from __future__ import annotations

import argparse
import os
import sys

from runtime_config import ensure_config_is_usable, load_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the broker using the resolved local LAN bridge config."
    )
    parser.add_argument("--config-file", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_runtime_config(args.config_file or None)
    ensure_config_is_usable(config)
    if not config.broker.enabled:
        raise SystemExit(
            "Broker is disabled in this config. Run this only on the machine hosting the broker."
        )
    command = [
        sys.executable,
        str(config.root_dir / "broker_server.py"),
        "--host",
        config.broker.host,
        "--port",
        str(config.broker.port),
        "--db",
        str(config.resolve_path(config.broker.db)),
        "--token",
        config.broker.token,
    ]
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
