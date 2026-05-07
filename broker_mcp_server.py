from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from bridge_common import (
    BrokerClient,
    DEFAULT_PROTOCOL_VERSION,
    HttpError,
    ensure_parent_dir,
    load_json_file,
    new_request_id,
    summarize_text,
    utc_now,
    validate_agent_id,
    write_json_file,
)


REMOTE_CODEX_MAX_SYNC_WAIT_SECONDS = 110
REMOTE_CODEX_TIMEOUT_GRACE_SECONDS = 5


TOOL_LIST_REMOTE_AGENTS = {
    "name": "list_remote_agents",
    "title": "List Remote Agents",
    "description": "List Codex LAN bridge agents currently registered with the broker.",
    "inputSchema": {
        "type": "object",
        "properties": {},
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "agents": {"type": "array"},
        },
        "required": ["agents"],
    },
}


TOOL_REMOTE_CODEX = {
    "name": "remote_codex",
    "title": "Remote Codex",
    "description": (
        "Send a prompt to a remote Codex endpoint over the LAN broker. "
        "Returns the final reply if it completes quickly; otherwise returns "
        "a running conversation snapshot you can resume with "
        "remote_codex_wait or remote_codex_status."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "peer_agent": {"type": "string"},
            "prompt": {"type": "string"},
            "thread_id": {"type": "string"},
            "cwd": {"type": "string"},
            "sandbox": {
                "type": "string",
                "enum": ["read-only", "workspace-write", "danger-full-access"],
            },
            "model": {"type": "string"},
            "profile": {"type": "string"},
            "approval_policy": {
                "type": "string",
                "enum": ["untrusted", "on-failure", "on-request", "never"],
            },
            "developer_instructions": {"type": "string"},
            "base_instructions": {"type": "string"},
            "timeout_secs": {"type": "integer", "minimum": 1, "maximum": 3600},
        },
        "required": ["peer_agent", "prompt"],
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "requestId": {"type": "string"},
            "conversationId": {"type": "string"},
            "status": {"type": "string"},
            "done": {"type": "boolean"},
            "threadId": {"type": "string"},
            "content": {"type": "string"},
            "peerAgent": {"type": "string"},
            "effectiveSandbox": {"type": "string"},
            "effectiveCwd": {"type": "string"},
            "submittedSeq": {"type": "integer"},
            "submittedAt": {"type": "string"},
            "lastSeq": {"type": "integer"},
            "messageCount": {"type": "integer"},
            "latestMessageKind": {"type": "string"},
            "latestEvent": {"type": "object"},
            "response": {"type": "object"},
            "stderrTail": {"type": "array"},
        },
        "required": [
            "requestId",
            "conversationId",
            "status",
            "done",
            "content",
            "peerAgent",
            "lastSeq",
            "messageCount",
        ],
    },
}


TOOL_REMOTE_CODEX_START = {
    "name": "remote_codex_start",
    "title": "Start Remote Codex",
    "description": "Send a prompt to a remote Codex endpoint over the LAN broker and return immediately with conversation metadata for follow-up monitoring.",
    "inputSchema": TOOL_REMOTE_CODEX["inputSchema"],
    "outputSchema": {
        "type": "object",
        "properties": {
            "requestId": {"type": "string"},
            "conversationId": {"type": "string"},
            "peerAgent": {"type": "string"},
            "submittedSeq": {"type": "integer"},
            "submittedAt": {"type": "string"},
        },
        "required": [
            "requestId",
            "conversationId",
            "peerAgent",
            "submittedSeq",
        ],
    },
}


TOOL_REMOTE_CODEX_STATUS = {
    "name": "remote_codex_status",
    "title": "Remote Codex Status",
    "description": "Read the current state of a remote Codex conversation by request or conversation id.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "conversation_id": {"type": "string"},
            "request_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "conversationId": {"type": "string"},
            "requestId": {"type": "string"},
            "status": {"type": "string"},
            "done": {"type": "boolean"},
            "peerAgent": {"type": "string"},
            "lastSeq": {"type": "integer"},
            "messageCount": {"type": "integer"},
            "latestMessageKind": {"type": "string"},
            "latestEvent": {"type": "object"},
            "response": {"type": "object"},
            "messages": {"type": "array"},
        },
        "required": [
            "conversationId",
            "requestId",
            "status",
            "done",
            "lastSeq",
            "messageCount",
            "messages",
        ],
    },
}


TOOL_REMOTE_CODEX_WAIT = {
    "name": "remote_codex_wait",
    "title": "Wait For Remote Codex Update",
    "description": "Wait for new messages on a remote Codex conversation after a known sequence number.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "conversation_id": {"type": "string"},
            "request_id": {"type": "string"},
            "after_seq": {"type": "integer", "minimum": 0},
            "wait_seconds": {"type": "integer", "minimum": 1, "maximum": 300},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "conversationId": {"type": "string"},
            "requestId": {"type": "string"},
            "status": {"type": "string"},
            "done": {"type": "boolean"},
            "peerAgent": {"type": "string"},
            "lastSeq": {"type": "integer"},
            "messageCount": {"type": "integer"},
            "latestMessageKind": {"type": "string"},
            "latestEvent": {"type": "object"},
            "afterSeq": {"type": "integer"},
            "newMessageCount": {"type": "integer"},
            "messagesAfter": {"type": "array"},
            "response": {"type": "object"},
            "messages": {"type": "array"},
        },
        "required": [
            "conversationId",
            "requestId",
            "status",
            "done",
            "lastSeq",
            "messageCount",
            "afterSeq",
            "newMessageCount",
            "messagesAfter",
            "messages",
        ],
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MCP server that turns a remote Codex endpoint into a local tool."
    )
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument(
        "--state-file",
        default=".broker_mcp_state.json",
        help="Base cursor state file for the bridge instance.",
    )
    parser.add_argument("--heartbeat-seconds", type=int, default=30)
    parser.add_argument("--poll-wait-seconds", type=int, default=20)
    parser.add_argument(
        "--owner-pid",
        type=int,
        default=0,
        help="Owning MCP session PID. Defaults to the current parent process.",
    )
    parser.add_argument("--owner-check-seconds", type=int, default=5)
    return parser.parse_args()


def _linux_process_identity(pid: int) -> str:
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        fields = stat_path.read_text(encoding="utf-8").split()
    except OSError:
        return ""
    if len(fields) <= 21:
        return ""
    return fields[21]


def _windows_process_identity(pid: int) -> str:
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    synchronize = 0x00100000
    wait_object_0 = 0x00000000
    wait_timeout = 0x00000102
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(
        process_query_limited_information | synchronize,
        False,
        pid,
    )
    if not handle:
        return ""
    try:
        wait_result = kernel32.WaitForSingleObject(handle, 0)
        if wait_result == wait_object_0:
            return ""
        if wait_result != wait_timeout:
            return ""
        creation = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel_time = wintypes.FILETIME()
        user_time = wintypes.FILETIME()
        ok = kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        )
        if not ok:
            return ""
        return f"{creation.dwHighDateTime:08x}{creation.dwLowDateTime:08x}"
    finally:
        kernel32.CloseHandle(handle)


def capture_process_identity(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        return _windows_process_identity(pid)
    if platform.system() == "Linux":
        identity = _linux_process_identity(pid)
        if identity:
            return identity
    try:
        os.kill(pid, 0)
    except OSError:
        return ""
    return str(pid)


def current_owner_pid(configured_owner_pid: int) -> int:
    if configured_owner_pid > 0:
        return configured_owner_pid
    parent_pid = os.getppid()
    return parent_pid if parent_pid > 0 else 0


def build_instance_token() -> str:
    return f"{os.getpid():x}.{uuid.uuid4().hex[:8]}"


def derive_runtime_agent_id(base_agent_id: str, instance_token: str) -> str:
    suffix = f".{instance_token}"
    if len(base_agent_id) + len(suffix) <= 64:
        return base_agent_id + suffix
    prefix = base_agent_id[: max(1, 64 - len(suffix))].rstrip("._-")
    if not prefix:
        prefix = "bridge"
    return prefix + suffix


def derive_runtime_state_path(base_path: Path, instance_token: str) -> Path:
    suffix = base_path.suffix or ".json"
    stem = base_path.stem if base_path.suffix else base_path.name
    return base_path.with_name(f"{stem}.{instance_token}{suffix}")


def derive_pending_path(runtime_state_path: Path) -> Path:
    suffix = runtime_state_path.suffix or ".json"
    stem = runtime_state_path.stem if runtime_state_path.suffix else runtime_state_path.name
    return runtime_state_path.with_name(f"{stem}.pending{suffix}")


def build_owner_lock_path(base_agent_id: str, owner_pid: int) -> Path:
    owner_token = owner_pid if owner_pid > 0 else "no-owner"
    return Path(tempfile.gettempdir()) / (
        f"codex-lan-bridge-{base_agent_id}-{owner_token}.lock.json"
    )


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
                    "preferred_runtime_agent_id": "",
                },
                "last_seen": agent.get("last_seen", ""),
                "alive": False,
                "age_seconds": float(agent.get("age_seconds", 999999.0)),
            },
        )
        runtime_agent_id = str(metadata.get("runtime_agent_id") or agent.get("agent_id", ""))
        group["metadata"]["runtime_agent_ids"].append(runtime_agent_id)
        group["metadata"]["instance_count"] += 1
        preferred_runtime = str(group["metadata"].get("preferred_runtime_agent_id", ""))
        candidate_age = float(agent.get("age_seconds", 999999.0))
        preferred_age = float(group["metadata"].get("_preferred_runtime_age", 999999.0))
        if not preferred_runtime or candidate_age < preferred_age:
            group["metadata"]["preferred_runtime_agent_id"] = runtime_agent_id
            group["metadata"]["_preferred_runtime_age"] = candidate_age
        if agent.get("alive"):
            group["alive"] = True
            group["metadata"]["alive_instance_count"] += 1
            preferred_alive = bool(group["metadata"].get("_preferred_runtime_alive"))
            if (not preferred_alive) or candidate_age < preferred_age:
                group["metadata"]["preferred_runtime_agent_id"] = runtime_agent_id
                group["metadata"]["_preferred_runtime_age"] = candidate_age
                group["metadata"]["_preferred_runtime_alive"] = True
        group["age_seconds"] = min(
            float(group.get("age_seconds", 999999.0)),
            float(agent.get("age_seconds", 999999.0)),
        )
        if str(agent.get("last_seen", "")) > str(group.get("last_seen", "")):
            group["last_seen"] = agent.get("last_seen", "")
        if str(agent.get("host", "")) not in str(group.get("host", "")).split(", "):
            if group["host"]:
                group["host"] += ", " + str(agent.get("host", ""))
            else:
                group["host"] = agent.get("host", "")
    for group in bridge_groups.values():
        metadata = group.get("metadata", {})
        if isinstance(metadata, dict):
            metadata.pop("_preferred_runtime_age", None)
            metadata.pop("_preferred_runtime_alive", None)
    display_agents.extend(bridge_groups.values())
    display_agents.sort(key=lambda item: str(item.get("agent_id", "")))
    return display_agents


class BrokerMcpServer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.base_agent_id = args.agent_id
        self.instance_token = build_instance_token()
        self.runtime_agent_id = derive_runtime_agent_id(
            self.base_agent_id, self.instance_token
        )
        self.owner_pid = current_owner_pid(int(args.owner_pid))
        self.owner_check_seconds = max(1, int(args.owner_check_seconds))
        self.owner_identity = capture_process_identity(self.owner_pid)
        self.process_identity = capture_process_identity(os.getpid()) or str(os.getpid())
        self.base_state_path = Path(args.state_file).resolve()
        self.runtime_state_path = derive_runtime_state_path(
            self.base_state_path, self.instance_token
        )
        self.pending_state_path = derive_pending_path(self.runtime_state_path)
        self.owner_lock_path = build_owner_lock_path(
            self.base_agent_id, self.owner_pid
        )
        self.client = BrokerClient(
            broker_url=args.broker_url,
            token=args.token,
            agent_id=self.runtime_agent_id,
            state_path=self.runtime_state_path,
        )
        self.heartbeat_seconds = max(5, int(args.heartbeat_seconds))
        self.poll_wait_seconds = max(1, int(args.poll_wait_seconds))
        self.pending_condition = threading.Condition(threading.RLock())
        self.pending_by_request_id = self._load_pending_messages()
        self.stdout_lock = threading.Lock()
        self.running = True
        self.reconnect_retry_seconds = 2
        self.cleanup_requested = False
        self._shutdown_lock = threading.Lock()
        self._shutdown_requested = False
        self._shutdown_complete = threading.Event()
        self.background_threads: List[threading.Thread] = []
        self._claim_owner_slot()
        self._start_background_threads()

    def _log(self, text: str) -> None:
        print(f"[{self.runtime_agent_id}] {text}", file=sys.stderr)

    def _sleep_before_retry(self, context: str, exc: HttpError) -> bool:
        self._log(f"{context}: {exc}")
        wait_seconds = max(1, self.reconnect_retry_seconds)
        self._log(f"retrying broker connection in {wait_seconds}s")
        for _ in range(wait_seconds):
            if not self.running:
                return False
            time.sleep(1)
        return self.running

    def _metadata(self) -> Dict[str, Any]:
        metadata = {
            "service": "broker-mcp-server",
            "base_agent_id": self.base_agent_id,
            "runtime_agent_id": self.runtime_agent_id,
            "instance_token": self.instance_token,
        }
        if self.owner_pid > 0:
            metadata["owner_pid"] = self.owner_pid
        return metadata

    def _load_pending_messages(self) -> Dict[str, Dict[str, Any]]:
        payload = load_json_file(self.pending_state_path, {})
        if not isinstance(payload, dict):
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        for request_id, message in payload.items():
            if isinstance(request_id, str) and isinstance(message, dict):
                result[request_id] = message
        return result

    def _persist_pending_messages_locked(self) -> None:
        if self.cleanup_requested:
            return
        write_json_file(self.pending_state_path, self.pending_by_request_id)

    def _list_agents(self) -> List[Dict[str, Any]]:
        agents = self.client.list_agents().get("agents", [])
        return agents if isinstance(agents, list) else []

    def _resolve_remote_codex_peer(self, requested_peer_agent: str) -> str:
        agents = self._list_agents()
        exact_matches = [
            agent
            for agent in agents
            if str(agent.get("agent_id", "")).strip() == requested_peer_agent
        ]
        if exact_matches:
            exact_matches.sort(
                key=lambda agent: (
                    not bool(agent.get("alive")),
                    float(agent.get("age_seconds", 999999.0)),
                )
            )
            agent = exact_matches[0]
            role = str(agent.get("role", "")).strip()
            if role != "endpoint":
                raise ValueError(
                    f"{requested_peer_agent} is a {role or 'non-endpoint'} agent. "
                    "remote_codex requires a live endpoint agent such as "
                    "`windows-codex` or `ubuntu-codex`."
                )
            if not agent.get("alive"):
                raise ValueError(
                    f"Endpoint agent {requested_peer_agent} is registered but not alive."
                )
            return requested_peer_agent
        bridge_matches = [
            agent
            for agent in agents
            if agent.get("role") == "mcp-bridge"
            and str((agent.get("metadata") or {}).get("base_agent_id", "")).strip()
            == requested_peer_agent
        ]
        if bridge_matches:
            raise ValueError(
                f"{requested_peer_agent} is a bridge agent, not a remote Codex endpoint. "
                "Use the peer machine's endpoint id such as `windows-codex` or "
                "`ubuntu-codex`."
            )
        raise ValueError(f"Peer agent {requested_peer_agent} is not registered.")

    def _lock_payload(self) -> Dict[str, Any]:
        return {
            "pid": os.getpid(),
            "pid_identity": self.process_identity,
            "owner_pid": self.owner_pid,
            "owner_identity": self.owner_identity,
            "runtime_agent_id": self.runtime_agent_id,
            "updated_at": utc_now(),
        }

    @staticmethod
    def _matches_live_process(pid: int, process_identity: str) -> bool:
        if pid <= 0:
            return False
        current_identity = capture_process_identity(pid)
        if not current_identity:
            return False
        if process_identity:
            return current_identity == process_identity
        return True

    def _claim_owner_slot(self) -> None:
        if self.owner_pid <= 0:
            return
        ensure_parent_dir(self.owner_lock_path)
        while True:
            try:
                fd = os.open(
                    self.owner_lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                existing = load_json_file(self.owner_lock_path, {})
                if not isinstance(existing, dict):
                    existing = {}
                existing_pid = int(existing.get("pid") or 0)
                existing_identity = str(existing.get("pid_identity") or "")
                if self._matches_live_process(existing_pid, existing_identity):
                    if existing_pid == os.getpid():
                        return
                    print(
                        f"[{self.runtime_agent_id}] owner-scoped bridge already "
                        f"running as pid {existing_pid}; exiting duplicate",
                        file=sys.stderr,
                    )
                    raise RuntimeError("duplicate_owner_bridge")
                try:
                    self.owner_lock_path.unlink()
                except FileNotFoundError:
                    continue
                except OSError:
                    time.sleep(0.1)
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._lock_payload(), handle, indent=2, sort_keys=True)
                handle.write("\n")
            return

    def _release_owner_slot(self) -> None:
        if self.owner_pid <= 0:
            return
        payload = load_json_file(self.owner_lock_path, {})
        if not isinstance(payload, dict):
            payload = {}
        if int(payload.get("pid") or 0) != os.getpid():
            return
        if str(payload.get("pid_identity") or "") != self.process_identity:
            return
        try:
            self.owner_lock_path.unlink()
        except FileNotFoundError:
            return
        except OSError as exc:
            self._log(f"could not remove owner lock {self.owner_lock_path}: {exc}")

    def _register(self) -> None:
        self.client.register(role="mcp-bridge", metadata=self._metadata())

    def _owner_monitor_loop(self) -> None:
        if self.owner_pid <= 0:
            return
        if not self.owner_identity:
            self._log(
                f"owner identity missing for {self.owner_pid}; terminating bridge instance"
            )
            self._request_shutdown("owner_identity_missing")
            return
        while self.running:
            current_identity = capture_process_identity(self.owner_pid)
            if current_identity != self.owner_identity:
                self._log(
                    f"owner process {self.owner_pid} is gone; shutting down bridge instance"
                )
                self._request_shutdown("owner_exited")
                return
            for _ in range(self.owner_check_seconds):
                if not self.running:
                    return
                time.sleep(1)

    def _heartbeat_loop(self) -> None:
        while self.running:
            try:
                self._register()
            except HttpError as exc:
                if not self._sleep_before_retry("heartbeat/register failed", exc):
                    return
                continue
            except Exception as exc:
                self._log(f"unexpected heartbeat error: {exc}")
                self._request_shutdown("heartbeat_error")
                return
            for _ in range(self.heartbeat_seconds):
                if not self.running:
                    return
                time.sleep(1)

    def _receiver_loop(self) -> None:
        while self.running:
            try:
                after_seq = self.client.get_last_seq()
                response = self.client.poll_messages(
                    after_seq=after_seq,
                    wait_seconds=self.poll_wait_seconds,
                )
                messages = response.get("messages", [])
                if not isinstance(messages, list):
                    messages = []
                max_seq = after_seq
                changed = False
                with self.pending_condition:
                    for message in messages:
                        seq = int(message.get("seq", 0))
                        max_seq = max(max_seq, seq)
                        body = message.get("body", {})
                        request_id = str(body.get("request_id") or "").strip()
                        if not request_id:
                            continue
                        existing = self.pending_by_request_id.get(request_id)
                        existing_seq = int(existing.get("seq", 0)) if existing else -1
                        if seq >= existing_seq:
                            self.pending_by_request_id[request_id] = message
                            changed = True
                    if changed:
                        self._persist_pending_messages_locked()
                    if max_seq != after_seq:
                        self.client.set_last_seq(max_seq)
                    if messages:
                        self.pending_condition.notify_all()
            except HttpError as exc:
                if not self._sleep_before_retry("receiver poll failed", exc):
                    return
                continue
            except Exception as exc:
                self._log(f"unexpected receiver error: {exc}")
                self._request_shutdown("receiver_error")
                return

    def _start_background_threads(self) -> None:
        for name, target in (
            ("bridge-heartbeat", self._heartbeat_loop),
            ("bridge-receiver", self._receiver_loop),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self.background_threads.append(thread)
        owner_thread = threading.Thread(
            target=self._owner_monitor_loop,
            name="bridge-owner-monitor",
            daemon=True,
        )
        owner_thread.start()
        self.background_threads.append(owner_thread)

    def _request_shutdown(self, reason: str) -> None:
        with self._shutdown_lock:
            if self._shutdown_requested:
                return
            self._shutdown_requested = True
        self._log(f"shutdown requested: {reason}")
        if reason.startswith("owner_"):
            self.stop()
            try:
                self.cleanup_runtime_files()
            finally:
                try:
                    sys.stdout.flush()
                except Exception:
                    pass
                try:
                    sys.stderr.flush()
                except Exception:
                    pass
                os._exit(0)
        self.stop()
        self._close_stdin()
        force_exit_thread = threading.Thread(
            target=self._force_exit_after_grace,
            name="bridge-force-exit",
            daemon=True,
        )
        force_exit_thread.start()
        try:
            signal.raise_signal(signal.SIGTERM)
        except Exception:
            os._exit(0)

    def _close_stdin(self) -> None:
        streams = [getattr(sys, "stdin", None)]
        stdin = getattr(sys, "stdin", None)
        buffer = getattr(stdin, "buffer", None)
        if buffer is not None:
            streams.append(buffer)
        for stream in streams:
            if not stream:
                continue
            try:
                stream.close()
            except Exception:
                continue

    def _force_exit_after_grace(self) -> None:
        if self._shutdown_complete.wait(timeout=2):
            return
        os._exit(0)

    def stop(self) -> None:
        self.cleanup_requested = True
        self.running = False
        self.client.state_path = None
        with self.pending_condition:
            self.pending_condition.notify_all()

    def cleanup_runtime_files(self) -> None:
        self.cleanup_requested = True
        self.client.state_path = None
        for thread in self.background_threads:
            if thread is threading.current_thread():
                continue
            thread.join(timeout=0.2)
        for path in (self.pending_state_path, self.runtime_state_path):
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                self._log(f"could not remove runtime state {path}: {exc}")
        self._release_owner_slot()
        self._shutdown_complete.set()

    def emit(self, payload: Dict[str, Any]) -> None:
        with self.stdout_lock:
            sys.stdout.write(json.dumps(payload) + "\n")
            sys.stdout.flush()

    def handle_initialize(self, request_id: Any) -> None:
        self.emit(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": DEFAULT_PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "codex-lan-broker-mcp",
                        "version": "1.1",
                    },
                },
            }
        )

    def handle_tools_list(self, request_id: Any) -> None:
        self.emit(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        TOOL_LIST_REMOTE_AGENTS,
                        TOOL_REMOTE_CODEX,
                        TOOL_REMOTE_CODEX_START,
                        TOOL_REMOTE_CODEX_STATUS,
                        TOOL_REMOTE_CODEX_WAIT,
                    ],
                },
            }
        )

    def _wrap_tool_result(
        self, request_id: Any, text: str, structured: Dict[str, Any]
    ) -> None:
        self.emit(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": text}],
                    "structuredContent": structured,
                },
            }
        )

    def _wrap_error(self, request_id: Any, message: str) -> None:
        self.emit(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": message,
                },
            }
        )

    def _wait_for_request(self, request_id: str, timeout_secs: int) -> Dict[str, Any]:
        deadline = time.time() + timeout_secs
        with self.pending_condition:
            while self.running:
                cached = self.pending_by_request_id.pop(request_id, None)
                if cached:
                    self._persist_pending_messages_locked()
                    return cached
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self.pending_condition.wait(timeout=min(1.0, remaining))
        raise TimeoutError(f"Timed out waiting for remote request {request_id}")

    def _normalize_conversation_id(self, arguments: Dict[str, Any]) -> str:
        conversation_id = str(
            arguments.get("conversation_id") or arguments.get("request_id") or ""
        ).strip()
        if not conversation_id:
            raise ValueError("conversation_id or request_id is required")
        return conversation_id

    def _conversation_messages(
        self, conversation_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        response = self.client.get_conversation(
            conversation_id=conversation_id,
            limit=max(1, min(int(limit), 500)),
        )
        messages = response.get("messages", [])
        if not isinstance(messages, list):
            return []
        return messages

    def _conversation_snapshot(
        self, conversation_id: str, limit: int = 100
    ) -> Dict[str, Any]:
        messages = self._conversation_messages(conversation_id, limit=limit)
        request_id = conversation_id
        peer_agent = ""
        latest_kind = ""
        latest_event: Dict[str, Any] = {}
        response_payload: Optional[Dict[str, Any]] = None
        status = "pending"
        done = False
        for message in messages:
            body = message.get("body", {})
            if not isinstance(body, dict):
                body = {}
            if body.get("request_id"):
                request_id = str(body.get("request_id"))
            if message.get("kind") == "codex_request":
                peer_agent = str(message.get("to_agent", "")).strip()
            if message.get("kind") == "codex_event" and not latest_event:
                latest_event = body
            latest_kind = str(message.get("kind", "")).strip()
            if message.get("kind") == "codex_event":
                latest_event = body
        for message in reversed(messages):
            body = message.get("body", {})
            if not isinstance(body, dict):
                body = {}
            kind = str(message.get("kind", "")).strip()
            if kind == "codex_response":
                done = True
                if body.get("ok"):
                    status = "completed"
                    response_payload = {
                        "threadId": body.get("thread_id", ""),
                        "content": body.get("content", ""),
                        "stderrTail": body.get("stderr_tail", []),
                        "effectiveSandbox": body.get("effective_sandbox", ""),
                        "effectiveCwd": body.get("effective_cwd", ""),
                    }
                else:
                    status = "failed"
                    response_payload = {
                        "error": body.get("error", "remote Codex request failed"),
                    }
                break
            if kind == "codex_error":
                done = True
                status = "failed"
                response_payload = {
                    "error": body.get("error", "remote Codex request failed"),
                }
                break
        last_seq = max([0] + [int(message.get("seq", 0)) for message in messages])
        return {
            "conversationId": conversation_id,
            "requestId": request_id,
            "status": status,
            "done": done,
            "peerAgent": peer_agent,
            "lastSeq": last_seq,
            "messageCount": len(messages),
            "latestMessageKind": latest_kind,
            "latestEvent": latest_event,
            "response": response_payload or {},
            "messages": messages,
        }

    def _format_event_text(self, event: Dict[str, Any]) -> str:
        event_type = str(event.get("type", "")).strip()
        if not event_type:
            return ""
        if event_type in {"agent_message_content_delta", "agent_message_delta"}:
            delta = str(event.get("delta", "")).strip()
            return f"{event_type}: {delta}" if delta else event_type
        if event_type == "agent_message":
            message = str(event.get("message", "")).strip()
            return f"agent_message: {message}" if message else event_type
        if event_type == "task_complete":
            message = str(event.get("last_agent_message", "")).strip()
            return f"task_complete: {message}" if message else event_type
        if event_type == "mcp_startup_update":
            return (
                f"mcp_startup_update: server={event.get('server', '')} "
                f"state={event.get('state', '')}"
            ).strip()
        if event_type in {"item_started", "item_completed"}:
            return (
                f"{event_type}: {event.get('item_type', '')}"
            ).strip()
        return event_type

    def _wait_for_conversation_update(
        self,
        conversation_id: str,
        *,
        after_seq: int,
        wait_seconds: int,
        limit: int,
    ) -> Dict[str, Any]:
        deadline = time.time() + max(1, wait_seconds)
        while True:
            snapshot = self._conversation_snapshot(conversation_id, limit=limit)
            new_messages = [
                message
                for message in snapshot["messages"]
                if int(message.get("seq", 0)) > after_seq
            ]
            if new_messages or snapshot["done"] or time.time() >= deadline:
                snapshot["afterSeq"] = after_seq
                snapshot["newMessageCount"] = len(new_messages)
                snapshot["messagesAfter"] = new_messages
                return snapshot
            time.sleep(min(1.0, max(0.0, deadline - time.time())))

    def _format_conversation_text(self, snapshot: Dict[str, Any]) -> str:
        line = (
            f"{snapshot['status']} conversation={snapshot['conversationId']} "
            f"messages={snapshot['messageCount']} last_seq={snapshot['lastSeq']}"
        )
        if snapshot.get("peerAgent"):
            line += f" peer={snapshot['peerAgent']}"
        response = snapshot.get("response") or {}
        if snapshot["status"] == "completed":
            line += f" thread={response.get('threadId', '-')}"
            content = str(response.get("content", "")).strip()
            if content:
                line += f"\n{content}"
        elif snapshot["status"] == "failed":
            line += f"\n{response.get('error', 'remote Codex request failed')}"
        elif snapshot.get("latestEvent"):
            event_text = self._format_event_text(snapshot["latestEvent"])
            if event_text:
                line += f"\n{event_text}"
        return line

    @staticmethod
    def _remote_codex_sync_wait_seconds(request_timeout_secs: int) -> int:
        return max(
            1,
            min(
                REMOTE_CODEX_MAX_SYNC_WAIT_SECONDS,
                int(request_timeout_secs) + REMOTE_CODEX_TIMEOUT_GRACE_SECONDS,
            ),
        )

    def _wait_for_remote_codex_snapshot(
        self,
        conversation_id: str,
        *,
        wait_seconds: int,
        limit: int,
    ) -> Dict[str, Any]:
        deadline = time.time() + max(1, wait_seconds)
        snapshot = self._conversation_snapshot(conversation_id, limit=limit)
        while not snapshot["done"] and time.time() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.time())))
            snapshot = self._conversation_snapshot(conversation_id, limit=limit)
        return snapshot

    def _build_remote_codex_structured_result(
        self,
        submitted: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        response = snapshot.get("response") or {}
        latest_event = snapshot.get("latestEvent") or {}
        thread_id = str(
            response.get("threadId")
            or latest_event.get("thread_id")
            or ""
        )
        return {
            "requestId": submitted["requestId"],
            "conversationId": submitted["conversationId"],
            "status": snapshot.get("status", "pending"),
            "done": bool(snapshot.get("done")),
            "threadId": thread_id,
            "content": str(response.get("content", "") or ""),
            "peerAgent": submitted["peerAgent"],
            "requestedPeerAgent": submitted.get("requestedPeerAgent", submitted["peerAgent"]),
            "effectiveSandbox": str(response.get("effectiveSandbox", "") or ""),
            "effectiveCwd": str(response.get("effectiveCwd", "") or ""),
            "submittedSeq": submitted["submittedSeq"],
            "submittedAt": submitted["submittedAt"],
            "lastSeq": int(snapshot.get("lastSeq", submitted["submittedSeq"])),
            "messageCount": int(snapshot.get("messageCount", 0)),
            "latestMessageKind": str(snapshot.get("latestMessageKind", "") or ""),
            "latestEvent": latest_event,
            "response": response,
            "stderrTail": response.get("stderrTail", []),
        }

    def _format_remote_codex_running_text(
        self,
        snapshot: Dict[str, Any],
        *,
        wait_seconds: int,
    ) -> str:
        line = self._format_conversation_text(snapshot)
        if line:
            line += "\n"
        line += (
            "Remote Codex is still running after "
            f"{wait_seconds}s. Use remote_codex_wait with "
            f"conversation_id=\"{snapshot['conversationId']}\" and "
            f"after_seq={snapshot['lastSeq']} to continue waiting."
        )
        return line

    def _submit_remote_codex(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        peer_agent = str(arguments.get("peer_agent", "")).strip()
        prompt = str(arguments.get("prompt", "")).strip()
        if not peer_agent or not prompt:
            raise ValueError("peer_agent and prompt are required")
        timeout_secs = int(arguments.get("timeout_secs") or 900)
        try:
            resolved_peer_agent = self._resolve_remote_codex_peer(peer_agent)
        except ValueError as exc:
            # Allow optimistic dispatch when the endpoint has not finished
            # heartbeating into broker state yet. This avoids false negatives
            # during startup races and transient broker liveness skew.
            validate_agent_id(peer_agent)
            self._log(
                "peer resolution warning; dispatching anyway: "
                f"{peer_agent} ({exc})"
            )
            resolved_peer_agent = peer_agent
        outbound_request_id = new_request_id()
        self._log(
            f"-> {resolved_peer_agent} request={outbound_request_id} "
            f"thread={arguments.get('thread_id') or '-'} "
            f"prompt={summarize_text(prompt)}"
        )
        outbound_body: Dict[str, Any] = {
            "request_id": outbound_request_id,
            "prompt": prompt,
        }
        for key in (
            "thread_id",
            "cwd",
            "sandbox",
            "model",
            "profile",
            "approval_policy",
            "developer_instructions",
            "base_instructions",
            "timeout_secs",
        ):
            value = arguments.get(key)
            if value not in (None, ""):
                outbound_body[key] = value
        submitted = self.client.send_message(
            to_agent=resolved_peer_agent,
            kind="codex_request",
            body=outbound_body,
            conversation_id=outbound_request_id,
        )
        return {
            "requestId": outbound_request_id,
            "conversationId": outbound_request_id,
            "peerAgent": resolved_peer_agent,
            "requestedPeerAgent": peer_agent,
            "timeoutSecs": timeout_secs,
            "submittedSeq": int(submitted.get("seq", 0)),
            "submittedAt": submitted.get("created_at", ""),
        }

    def _handle_remote_codex_async(
        self, request_id: Any, arguments: Dict[str, Any]
    ) -> None:
        try:
            submitted = self._submit_remote_codex(arguments)
            snapshot = self._wait_for_remote_codex_snapshot(
                submitted["conversationId"],
                wait_seconds=self._remote_codex_sync_wait_seconds(
                    int(submitted["timeoutSecs"])
                ),
                limit=200,
            )
            if snapshot["status"] == "failed":
                response = snapshot.get("response") or {}
                self._wrap_error(
                    request_id,
                    str(response.get("error", "remote Codex request failed")),
                )
                return
            structured = self._build_remote_codex_structured_result(
                submitted, snapshot
            )
            if snapshot["status"] == "completed":
                self._wrap_tool_result(
                    request_id, structured.get("content", ""), structured
                )
                return
            self._wrap_tool_result(
                request_id,
                self._format_remote_codex_running_text(
                    snapshot,
                    wait_seconds=self._remote_codex_sync_wait_seconds(
                        int(submitted["timeoutSecs"])
                    ),
                ),
                structured,
            )
        except TimeoutError as exc:
            self._wrap_error(request_id, str(exc))
        except HttpError as exc:
            self._wrap_error(request_id, str(exc))
        except Exception as exc:
            self._wrap_error(request_id, str(exc))

    def handle_remote_codex(self, request_id: Any, arguments: Dict[str, Any]) -> None:
        worker = threading.Thread(
            target=self._handle_remote_codex_async,
            args=(request_id, dict(arguments)),
            name=f"remote-codex-{request_id}",
            daemon=True,
        )
        worker.start()

    def handle_remote_codex_start(
        self, request_id: Any, arguments: Dict[str, Any]
    ) -> None:
        try:
            structured = self._submit_remote_codex(arguments)
            text = (
                f"submitted conversation={structured['conversationId']} "
                f"peer={structured['peerAgent']} seq={structured['submittedSeq']}"
            )
            self._wrap_tool_result(request_id, text, structured)
        except Exception as exc:
            self._wrap_error(request_id, str(exc))

    def handle_list_agents(self, request_id: Any) -> None:
        agents = self._list_agents()
        display_agents = summarize_agents_for_display(agents)
        text_lines = []
        for agent in display_agents:
            agent["callable"] = agent.get("role") == "endpoint" and bool(agent.get("alive"))
            marker = "alive" if agent.get("alive") else "stale"
            metadata = agent.get("metadata") or {}
            if agent.get("role") == "mcp-bridge" and metadata.get("base_agent_id"):
                instance_count = int(metadata.get("instance_count", 0))
                alive_count = int(metadata.get("alive_instance_count", 0))
                preferred_runtime = str(
                    metadata.get("preferred_runtime_agent_id", "")
                ).strip()
                text_lines.append(
                    f"{agent['agent_id']} [{agent['role']}] {marker} "
                    f"instances={instance_count} alive_instances={alive_count} "
                    f"host={agent['host']} bridge-only"
                    + (
                        f" preferred_runtime={preferred_runtime}"
                        if preferred_runtime
                        else ""
                    )
                )
                continue
            text_lines.append(
                f"{agent['agent_id']} [{agent['role']}] {marker} host={agent['host']}"
                + (" callable" if agent["callable"] else "")
            )
        self._wrap_tool_result(
            request_id,
            "\n".join(text_lines) if text_lines else "No agents registered.",
            {"agents": display_agents, "rawAgents": agents},
        )

    def handle_remote_codex_status(
        self, request_id: Any, arguments: Dict[str, Any]
    ) -> None:
        try:
            conversation_id = self._normalize_conversation_id(arguments)
            limit = int(arguments.get("limit") or 100)
            snapshot = self._conversation_snapshot(conversation_id, limit=limit)
            self._wrap_tool_result(
                request_id,
                self._format_conversation_text(snapshot),
                snapshot,
            )
        except Exception as exc:
            self._wrap_error(request_id, str(exc))

    def handle_remote_codex_wait(
        self, request_id: Any, arguments: Dict[str, Any]
    ) -> None:
        try:
            conversation_id = self._normalize_conversation_id(arguments)
            after_seq = int(arguments.get("after_seq") or 0)
            wait_seconds = int(arguments.get("wait_seconds") or 20)
            limit = int(arguments.get("limit") or 100)
            snapshot = self._wait_for_conversation_update(
                conversation_id,
                after_seq=after_seq,
                wait_seconds=wait_seconds,
                limit=limit,
            )
            text = (
                self._format_conversation_text(snapshot)
                + f"\nnew_messages={snapshot['newMessageCount']} after_seq={after_seq}"
            )
            self._wrap_tool_result(request_id, text, snapshot)
        except Exception as exc:
            self._wrap_error(request_id, str(exc))

    def handle_tool_call(self, request_id: Any, params: Dict[str, Any]) -> None:
        try:
            name = params.get("name")
            arguments = params.get("arguments", {})
            if name == "list_remote_agents":
                self.handle_list_agents(request_id)
                return
            if name == "remote_codex":
                self.handle_remote_codex(request_id, arguments)
                return
            if name == "remote_codex_start":
                self.handle_remote_codex_start(request_id, arguments)
                return
            if name == "remote_codex_status":
                self.handle_remote_codex_status(request_id, arguments)
                return
            if name == "remote_codex_wait":
                self.handle_remote_codex_wait(request_id, arguments)
                return
            self._wrap_error(request_id, f"unknown tool: {name}")
        except Exception as exc:
            self._wrap_error(request_id, str(exc))

    def serve(self) -> None:
        for line in sys.stdin:
            text = line.strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError:
                continue
            method = message.get("method")
            request_id = message.get("id")
            params = message.get("params", {})
            try:
                if method == "initialize":
                    self.handle_initialize(request_id)
                    continue
                if method == "tools/list":
                    self.handle_tools_list(request_id)
                    continue
                if method == "tools/call":
                    self.handle_tool_call(request_id, params)
                    continue
                if method == "ping":
                    self.emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
                    continue
                if request_id is not None:
                    self._wrap_error(request_id, f"unsupported method: {method}")
            except Exception as exc:
                if request_id is not None:
                    self._wrap_error(request_id, str(exc))


def main() -> None:
    args = parse_args()
    try:
        server = BrokerMcpServer(args)
    except RuntimeError as exc:
        if str(exc) == "duplicate_owner_bridge":
            return
        raise

    def stop_handler(signum: int, frame: Optional[Any]) -> None:
        del signum, frame
        server.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    try:
        server.serve()
    finally:
        server.stop()
        server.cleanup_runtime_files()


if __name__ == "__main__":
    main()
