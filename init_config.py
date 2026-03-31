from __future__ import annotations

import argparse
from pathlib import Path

from runtime_config import (
    DEFAULT_CONFIG_PATH,
    default_config_path_for_role,
    default_config_for_role,
    detect_machine_role,
    generate_token,
    next_steps_for_role,
    write_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a local Codex LAN Bridge config for this machine."
    )
    parser.add_argument(
        "--role",
        choices=["ubuntu", "windows"],
        default=detect_machine_role(),
    )
    parser.add_argument(
        "--config-file",
        default="",
        help=(
            "Config path to write. Defaults to the role-specific local config, "
            "for example config.windows.local.json."
        ),
    )
    parser.add_argument("--broker-url", default="")
    parser.add_argument("--token", default="")
    parser.add_argument("--broker-host", default="0.0.0.0")
    parser.add_argument("--broker-port", type=int, default=8765)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = default_config_for_role(args.role)
    payload["broker"]["host"] = args.broker_host
    payload["broker"]["port"] = args.broker_port
    if args.broker_url:
        payload["broker"]["url"] = args.broker_url
    if args.token:
        payload["broker"]["token"] = args.token
    elif args.role == "windows":
        payload["broker"]["token"] = "CHANGE-ME"
    if args.role == "ubuntu" and not args.token:
        payload["broker"]["token"] = generate_token()
    config_path = (
        Path(args.config_file).resolve()
        if args.config_file
        else default_config_path_for_role(args.role).resolve()
    )
    write_config(config_path, payload, force=args.force)
    print(f"Wrote {config_path}")
    print(f"Role: {args.role}")
    print(f"Broker URL: {payload['broker']['url']}")
    if payload["broker"]["token"] == "CHANGE-ME":
        print("Token: CHANGE-ME")
    else:
        print(f"Token: {payload['broker']['token']}")
    print(next_steps_for_role(args.role))


if __name__ == "__main__":
    main()
