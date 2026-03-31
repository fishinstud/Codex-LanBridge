from __future__ import annotations

import argparse
import os
import sys

from runtime_config import ensure_config_is_usable, load_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the endpoint using the resolved local LAN bridge config."
    )
    parser.add_argument("--config-file", default="")
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--session-idle-seconds", type=int, default=900)
    parser.add_argument("--max-sessions", type=int, default=8)
    parser.add_argument("--warm-clients", type=int, default=1)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_runtime_config(args.config_file or None)
    ensure_config_is_usable(config)
    state_path = config.resolve_path(config.endpoint.state_file)
    role_file = config.resolve_path(config.endpoint.role_instructions_file)
    command = [
        sys.executable,
        str(config.root_dir / "agent_endpoint.py"),
        "--broker-url",
        config.broker.url,
        "--token",
        config.broker.token,
        "--agent-id",
        config.endpoint.agent_id,
        "--state-file",
        str(state_path),
        "--codex-bin",
        config.endpoint.codex_bin,
        "--default-cwd",
        config.endpoint.default_cwd,
        "--default-sandbox",
        config.endpoint.default_sandbox,
        "--default-approval-policy",
        config.endpoint.default_approval_policy,
        "--role-instructions-file",
        str(role_file),
        "--max-concurrency",
        str(max(1, args.max_concurrency)),
        "--session-idle-seconds",
        str(max(60, args.session_idle_seconds)),
        "--max-sessions",
        str(max(1, args.max_sessions)),
        "--warm-clients",
        str(max(0, args.warm_clients)),
    ]
    if args.run_once:
        command.append("--run-once")
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
