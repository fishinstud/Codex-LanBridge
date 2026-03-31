from __future__ import annotations

import json
import os
import secrets
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT_DIR / "config.local.json"


def default_sandbox_for_role(role: str) -> str:
    return "danger-full-access" if role == "windows" else "workspace-write"


def default_config_path_for_role(role: str) -> Path:
    return ROOT_DIR / f"config.{role}.local.json"


def default_config_candidates(role: str) -> list[Path]:
    candidates = [default_config_path_for_role(role), DEFAULT_CONFIG_PATH]
    deduped: list[Path] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def resolve_default_config_path(role: Optional[str] = None) -> Path:
    effective_role = role or detect_machine_role()
    for candidate in default_config_candidates(effective_role):
        if candidate.exists():
            return candidate
    return DEFAULT_CONFIG_PATH


@dataclass
class BrokerConfig:
    enabled: bool
    host: str
    port: int
    url: str
    token: str
    db: str


@dataclass
class EndpointConfig:
    agent_id: str
    codex_bin: str
    default_cwd: str
    default_sandbox: str
    default_approval_policy: str
    role_instructions_file: str
    state_file: str


@dataclass
class BridgeConfig:
    agent_id: str
    mcp_server_name: str
    state_file: str


@dataclass
class RuntimeConfig:
    root_dir: Path
    config_path: Path
    machine_role: str
    broker: BrokerConfig
    endpoint: EndpointConfig
    bridge: BridgeConfig

    def resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return (self.root_dir / path).resolve()

    def as_dict(self) -> Dict[str, Any]:
        return {
            "machine_role": self.machine_role,
            "broker": self.broker.__dict__,
            "endpoint": self.endpoint.__dict__,
            "bridge": self.bridge.__dict__,
        }


def default_codex_bin() -> str:
    return "codex.cmd" if os.name == "nt" else "codex"


def detect_machine_role() -> str:
    return "windows" if os.name == "nt" else "ubuntu"


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def detect_local_ip() -> str:
    probe_address = ("192.0.2.1", 80)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(probe_address)
        ip = sock.getsockname()[0]
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    finally:
        sock.close()
    try:
        hostname_ip = socket.gethostbyname(socket.gethostname())
        if hostname_ip and not hostname_ip.startswith("127."):
            return hostname_ip
    except OSError:
        pass
    return "127.0.0.1"


def default_config_for_role(role: str) -> Dict[str, Any]:
    if role not in {"ubuntu", "windows"}:
        raise ValueError("role must be ubuntu or windows")
    broker_port = 8765
    broker_ip = detect_local_ip() if role == "ubuntu" else "CHANGE-ME"
    default_cwd = "/root" if role == "ubuntu" else "C:\\"
    role_file = (
        "examples/ubuntu-endpoint-instructions.txt"
        if role == "ubuntu"
        else "examples/windows-endpoint-instructions.txt"
    )
    return {
        "machine_role": role,
        "broker": {
            "enabled": role == "ubuntu",
            "host": "0.0.0.0",
            "port": broker_port,
            "url": f"http://{broker_ip}:{broker_port}",
            "token": generate_token() if role == "ubuntu" else "CHANGE-ME",
            "db": "broker.sqlite3",
        },
        "endpoint": {
            "agent_id": f"{role}-codex",
            "codex_bin": default_codex_bin(),
            "default_cwd": default_cwd,
            "default_sandbox": default_sandbox_for_role(role),
            "default_approval_policy": "never",
            "role_instructions_file": role_file,
            "state_file": f"state/{role}-endpoint-state.json",
        },
        "bridge": {
            "agent_id": f"{role}-ui",
            "mcp_server_name": "lanBridge",
            "state_file": f"state/{role}-bridge-state.json",
        },
    }


def write_config(config_path: Path, payload: Dict[str, Any], force: bool = False) -> None:
    if config_path.exists() and not force:
        raise FileExistsError(f"{config_path} already exists")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_runtime_config(config_path: Optional[str] = None) -> RuntimeConfig:
    path = (
        Path(config_path).resolve()
        if config_path
        else resolve_default_config_path(detect_machine_role())
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Missing config file: {path}. Run init_config.py first."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    role = payload.get("machine_role") or detect_machine_role()
    broker = payload.get("broker", {})
    endpoint = payload.get("endpoint", {})
    bridge = payload.get("bridge", {})
    root_dir = path.parent
    return RuntimeConfig(
        root_dir=root_dir,
        config_path=path,
        machine_role=role,
        broker=BrokerConfig(
            enabled=bool(broker.get("enabled", role == "ubuntu")),
            host=str(broker.get("host", "0.0.0.0")),
            port=int(broker.get("port", 8765)),
            url=str(broker.get("url", f"http://127.0.0.1:{broker.get('port', 8765)}")),
            token=str(broker.get("token", "")),
            db=str(broker.get("db", "broker.sqlite3")),
        ),
        endpoint=EndpointConfig(
            agent_id=str(endpoint.get("agent_id", f"{role}-codex")),
            codex_bin=str(endpoint.get("codex_bin", default_codex_bin())),
            default_cwd=str(endpoint.get("default_cwd", "/root" if role == "ubuntu" else "C:\\")),
            default_sandbox=str(
                endpoint.get("default_sandbox", default_sandbox_for_role(role))
            ),
            default_approval_policy=str(endpoint.get("default_approval_policy", "never")),
            role_instructions_file=str(
                endpoint.get(
                    "role_instructions_file",
                    "examples/ubuntu-endpoint-instructions.txt"
                    if role == "ubuntu"
                    else "examples/windows-endpoint-instructions.txt",
                )
            ),
            state_file=str(endpoint.get("state_file", f"state/{role}-endpoint-state.json")),
        ),
        bridge=BridgeConfig(
            agent_id=str(bridge.get("agent_id", f"{role}-ui")),
            mcp_server_name=str(bridge.get("mcp_server_name", "lanBridge")),
            state_file=str(bridge.get("state_file", f"state/{role}-bridge-state.json")),
        ),
    )


def ensure_config_is_usable(config: RuntimeConfig) -> None:
    if not config.broker.token or config.broker.token == "CHANGE-ME":
        raise ValueError(
            f"{config.config_path}: broker.token is missing. Run init_config.py or edit the file."
        )
    if not config.broker.url or "CHANGE-ME" in config.broker.url:
        raise ValueError(
            f"{config.config_path}: broker.url is missing. Run init_config.py or edit the file."
        )


def next_steps_for_role(role: str) -> str:
    if role == "ubuntu":
        return "\n".join(
            [
                "Next steps:",
                "1. Start the broker: python3 run_broker.py",
                "2. Start the endpoint: python3 run_endpoint.py",
                "3. Install the MCP bridge: python3 install_mcp.py",
                "4. Test from this machine: python3 doctor.py",
            ]
        )
    return "\n".join(
        [
            "Next steps:",
            "1. Start the endpoint: py run_endpoint.py",
            "2. Install the MCP bridge: py install_mcp.py",
            "3. Test against the Ubuntu broker: py doctor.py --peer ubuntu-codex",
        ]
    )


def platform_python_command() -> str:
    return "py" if os.name == "nt" else Path(sys.executable).name
