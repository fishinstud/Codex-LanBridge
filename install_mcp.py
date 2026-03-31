from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from typing import List

from runtime_config import ensure_config_is_usable, load_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install or refresh the local lanBridge MCP server configuration."
    )
    parser.add_argument("--config-file", default="")
    return parser.parse_args()


def _run_command(command: List[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    if os.name == "nt" and command[0].lower().endswith((".cmd", ".bat")):
        wrapped = ["cmd.exe", "/c", *command]
    else:
        wrapped = command
    return subprocess.run(
        wrapped,
        text=True,
        capture_output=True,
        check=check,
    )


def main() -> None:
    args = parse_args()
    config = load_runtime_config(args.config_file or None)
    ensure_config_is_usable(config)
    codex_cli = shutil.which(config.endpoint.codex_bin) or config.endpoint.codex_bin
    state_path = config.resolve_path(config.bridge.state_file)
    mcp_name = config.bridge.mcp_server_name
    try:
        _run_command([codex_cli, "mcp", "remove", mcp_name], check=False)
        result = _run_command(
            [
                codex_cli,
                "mcp",
                "add",
                mcp_name,
                "--",
                sys.executable,
                str(config.root_dir / "broker_mcp_server.py"),
                "--broker-url",
                config.broker.url,
                "--token",
                config.broker.token,
                "--agent-id",
                config.bridge.agent_id,
                "--state-file",
                str(state_path),
            ]
        )
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Could not find Codex CLI executable '{config.endpoint.codex_bin}'."
        ) from exc
    print(result.stdout.strip() or f"Installed MCP server '{mcp_name}'.")
    print(f"State file: {state_path}")


if __name__ == "__main__":
    main()
