from __future__ import annotations

import argparse
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from bridge_common import BrokerClient, new_request_id
from runtime_config import ensure_config_is_usable, load_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate broker reachability and run repeatable remote Codex probes."
    )
    parser.add_argument("--config-file", default="")
    parser.add_argument("--peer", default="")
    parser.add_argument("--timeout-secs", type=int, default=180)
    parser.add_argument("--prompt", default="Reply with exactly DOCTOR_OK.")
    parser.add_argument("--roundtrips", type=int, default=1)
    parser.add_argument("--conversation-turns", type=int, default=1)
    parser.add_argument("--poll-wait-seconds", type=int, default=20)
    return parser.parse_args()


def choose_default_peer(config, agents: List[Dict[str, Any]]) -> str:
    preferred = "windows-codex" if config.machine_role == "ubuntu" else "ubuntu-codex"
    for agent in agents:
        if agent.get("agent_id") == preferred:
            return preferred
    return ""


def build_instance_token() -> str:
    return f"{int(time.time()):x}.{new_request_id()[:8]}"


def derive_runtime_agent_id(base_agent_id: str, instance_token: str) -> str:
    suffix = f".{instance_token}"
    if len(base_agent_id) + len(suffix) <= 64:
        return base_agent_id + suffix
    prefix = base_agent_id[: max(1, 64 - len(suffix))].rstrip("._-")
    if not prefix:
        prefix = "doctor"
    return prefix + suffix


def derive_runtime_state_path(base_path: Path, instance_token: str) -> Path:
    suffix = base_path.suffix or ".json"
    stem = base_path.stem if base_path.suffix else base_path.name
    return base_path.with_name(f"{stem}.{instance_token}{suffix}")


def summarize_agents_for_display(agents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bridge_groups: Dict[str, Dict[str, Any]] = {}
    display_agents: List[Dict[str, Any]] = []
    for agent in agents:
        metadata = agent.get("metadata") or {}
        base_agent_id = str(metadata.get("base_agent_id") or "").strip()
        if agent.get("role") != "mcp-bridge" or not base_agent_id:
            display_agents.append(agent)
            continue
        group = bridge_groups.setdefault(
            base_agent_id,
            {
                "agent_id": base_agent_id,
                "role": "mcp-bridge",
                "host": agent.get("host", ""),
                "platform": agent.get("platform", ""),
                "metadata": {
                    "service": metadata.get("service", "broker-mcp-server"),
                    "base_agent_id": base_agent_id,
                    "runtime_agent_ids": [],
                    "instance_count": 0,
                    "alive_instance_count": 0,
                },
                "last_seen": agent.get("last_seen", ""),
                "alive": False,
                "age_seconds": float(agent.get("age_seconds", 999999.0)),
            },
        )
        runtime_agent_id = str(metadata.get("runtime_agent_id") or agent.get("agent_id", ""))
        group["metadata"]["runtime_agent_ids"].append(runtime_agent_id)
        group["metadata"]["instance_count"] += 1
        if agent.get("alive"):
            group["alive"] = True
            group["metadata"]["alive_instance_count"] += 1
        group["age_seconds"] = min(
            float(group.get("age_seconds", 999999.0)),
            float(agent.get("age_seconds", 999999.0)),
        )
        if str(agent.get("last_seen", "")) > str(group.get("last_seen", "")):
            group["last_seen"] = agent.get("last_seen", "")
        host = str(agent.get("host", ""))
        hosts = str(group.get("host", "")).split(", ") if group.get("host") else []
        if host and host not in hosts:
            if group["host"]:
                group["host"] += ", " + host
            else:
                group["host"] = host
    display_agents.extend(bridge_groups.values())
    display_agents.sort(key=lambda item: str(item.get("agent_id", "")))
    return display_agents


def format_agent_line(agent: Dict[str, Any]) -> str:
    marker = "alive" if agent.get("alive") else "stale"
    metadata = agent.get("metadata") or {}
    if agent.get("role") == "mcp-bridge" and metadata.get("base_agent_id"):
        return (
            f"  - {agent['agent_id']} [{agent['role']}] {marker} "
            f"instances={metadata.get('instance_count', 0)} "
            f"alive_instances={metadata.get('alive_instance_count', 0)} "
            f"host={agent['host']}"
        )
    return f"  - {agent['agent_id']} [{agent['role']}] {marker} host={agent['host']}"


def wait_for_response(
    client: BrokerClient,
    request_id: str,
    timeout_secs: int,
    poll_wait_seconds: int,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        remaining = max(1, int(deadline - time.time()))
        after_seq = client.get_last_seq()
        response = client.poll_messages(
            after_seq=after_seq,
            wait_seconds=min(max(1, poll_wait_seconds), remaining),
        )
        messages = response.get("messages", [])
        max_seq = after_seq
        for message in messages:
            max_seq = max(max_seq, int(message.get("seq", 0)))
            body = message.get("body", {})
            if body.get("request_id") != request_id:
                continue
            if str(message.get("kind", "")).strip() == "codex_event":
                continue
            client.set_last_seq(max_seq)
            return message
        client.set_last_seq(max_seq)
    raise TimeoutError(f"Timed out waiting for peer response to {request_id}")


def build_probe_prompt(
    base_prompt: str, round_index: int, turn_index: int, single_probe: bool
) -> str:
    if single_probe and round_index == 0 and turn_index == 0:
        return base_prompt
    token = f"DOCTOR_OK_{round_index + 1}_{turn_index + 1}"
    return f"Reply with exactly {token}."


def main() -> None:
    args = parse_args()
    config = load_runtime_config(args.config_file or None)
    ensure_config_is_usable(config)
    roundtrips = max(0, int(args.roundtrips))
    conversation_turns = max(1, int(args.conversation_turns))
    poll_wait_seconds = max(1, int(args.poll_wait_seconds))

    health_url = config.broker.url.rstrip("/") + "/health"
    try:
        with urllib.request.urlopen(health_url, timeout=10) as response:
            print(f"Broker health: {response.read().decode('utf-8').strip()}")
    except (urllib.error.URLError, OSError) as exc:
        raise SystemExit(f"Broker health check failed: {exc}") from exc

    base_agent_id = f"{config.bridge.agent_id}.doctor"
    runtime_token = build_instance_token()
    runtime_agent_id = derive_runtime_agent_id(base_agent_id, runtime_token)
    base_state_path = config.resolve_path(f"state/{config.machine_role}-doctor-state.json")
    runtime_state_path = derive_runtime_state_path(base_state_path, runtime_token)
    client = BrokerClient(
        broker_url=config.broker.url,
        token=config.broker.token,
        agent_id=runtime_agent_id,
        state_path=runtime_state_path,
    )
    client.register(
        role="doctor",
        metadata={
            "service": "doctor",
            "base_agent_id": base_agent_id,
            "runtime_agent_id": runtime_agent_id,
        },
    )
    agents = client.list_agents().get("agents", [])
    display_agents = summarize_agents_for_display(agents)
    print("Registered agents:")
    for agent in display_agents:
        print(format_agent_line(agent))

    bridge_visible = any(
        agent.get("agent_id") == config.bridge.agent_id and agent.get("role") == "mcp-bridge"
        for agent in display_agents
    )
    print(
        f"Configured bridge base '{config.bridge.agent_id}' visible: "
        f"{'yes' if bridge_visible else 'no'}"
    )

    peer = args.peer or choose_default_peer(config, display_agents)
    if roundtrips <= 0:
        print("Remote test skipped: --roundtrips 0")
        return
    if not peer:
        print("Remote test skipped: no peer specified and no default peer is registered.")
        return

    single_probe = roundtrips == 1 and conversation_turns == 1
    latencies: List[float] = []
    for round_index in range(roundtrips):
        thread_id = ""
        for turn_index in range(conversation_turns):
            prompt = build_probe_prompt(args.prompt, round_index, turn_index, single_probe)
            request_id = new_request_id()
            body: Dict[str, Any] = {
                "request_id": request_id,
                "prompt": prompt,
                "timeout_secs": args.timeout_secs,
                "sandbox": "read-only",
            }
            if thread_id:
                body["thread_id"] = thread_id
            client.send_message(
                to_agent=peer,
                kind="codex_request",
                body=body,
                conversation_id=request_id,
            )
            print(
                f"Sent probe request {request_id} to {peer} "
                f"(round {round_index + 1}/{roundtrips}, turn {turn_index + 1}/{conversation_turns})"
            )
            started = time.perf_counter()
            message = wait_for_response(
                client=client,
                request_id=request_id,
                timeout_secs=args.timeout_secs,
                poll_wait_seconds=poll_wait_seconds,
            )
            elapsed = time.perf_counter() - started
            latencies.append(elapsed)
            response_body = message.get("body", {})
            if message.get("kind") != "codex_response" or not response_body.get("ok"):
                raise SystemExit(response_body.get("error", "Remote test failed"))
            content = str(response_body.get("content", "")).strip()
            thread_id = str(response_body.get("thread_id", "") or thread_id)
            print(f"Remote response in {elapsed:.2f}s:")
            print(content)
            if thread_id:
                print(f"Remote thread id: {thread_id}")

    if latencies:
        print(
            "Latency summary: "
            f"count={len(latencies)} min={min(latencies):.2f}s "
            f"avg={sum(latencies) / len(latencies):.2f}s "
            f"max={max(latencies):.2f}s"
        )


if __name__ == "__main__":
    main()
