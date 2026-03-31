# Codex LAN Bridge

Codex LAN Bridge lets one Codex installation call another Codex installation over a local network.

The typical setup is:

- Ubuntu hosts the broker.
- Ubuntu and Windows each run a named endpoint.
- The machine where you are actively using Codex installs the local MCP bridge so the remote machine shows up as a tool.

This repository is built for practical cross-machine work, not just toy RPC. It supports real remote Codex sessions, thread reuse, async status polling, machine-specific instructions, Windows startup automation, and Windows network-drive preparation before the endpoint starts.

## What It Can Do

- Discover live remote Codex agents with `list_remote_agents`.
- Send synchronous remote prompts with `remote_codex`.
- Start long-running remote jobs with `remote_codex_start`.
- Poll or wait for progress with `remote_codex_status` and `remote_codex_wait`.
- Reuse a remote `threadId` so follow-up prompts continue the same conversation.
- Pass through host-specific execution options such as `cwd`, `sandbox`, `model`, `profile`, `approval_policy`, and instruction overrides.
- Inject different instruction files for Windows and Ubuntu endpoints.
- Run repeatable health checks with `doctor.py`.
- On Windows, start the endpoint automatically at startup and logon.
- On Windows, map required SMB drives before the endpoint launches.

## Architecture

The package has three main runtime pieces:

1. `broker_server.py`
   Central HTTP broker. It tracks live agents and routes messages between them.
2. `agent_endpoint.py`
   Runs on each machine and wraps that machine's local `codex mcp-server`.
3. `broker_mcp_server.py`
   The local MCP bridge. This is what exposes `remote_codex*` tools to your interactive Codex session.

The important behavior is that the endpoint talks to the remote machine's real Codex MCP server. That means remote work can continue across turns by reusing the returned `threadId`.

## Repository Layout

Core files:

- `broker_server.py`: LAN message broker
- `agent_endpoint.py`: remote Codex endpoint wrapper
- `broker_mcp_server.py`: MCP bridge exposed to Codex
- `codex_rpc.py`: JSON-RPC client for talking to `codex mcp-server`
- `bridge_common.py`: shared broker/client helpers
- `runtime_config.py`: config loading, defaults, and role-specific behavior
- `init_config.py`: generate local config files
- `install_mcp.py`: install or refresh the local `lanBridge` MCP server
- `doctor.py`: smoke test and roundtrip validation

Platform folders:

- `ubuntu/`: shell helpers and systemd unit templates
- `windows/`: PowerShell launchers, startup installer, and Windows-side bootstrap logic
- `examples/`: endpoint instruction files and AGENTS example snippet

## Requirements

- Python 3.10+
- Codex CLI installed and authenticated on every machine
- Network reachability from each endpoint to the Ubuntu broker host
- PowerShell 5.1+ on Windows for the included scripts

Optional requirements:

- OpenSSH on Windows if you also use SSH access to the machine
- SMB reachability if you use the Windows `network_drives` feature

## Security And GitHub Hygiene

This repo uses a shared bearer token and plain HTTP on the LAN. Treat it as trusted-network infrastructure.

Important before publishing to GitHub:

- Do not commit `config.local.json`, `config.windows.local.json`, or `config.ubuntu.local.json` if they contain live tokens, machine IPs, paths, or share credentials.
- Do not commit runtime state such as `state/`, broker databases, logs, or PID files.
- If you use `network_drives` on Windows, those config files may contain SMB credentials.

Recommended `.gitignore` entries:

```gitignore
config.local.json
config.*.local.json
state/
*.sqlite3
*.sqlite3-shm
*.sqlite3-wal
*.log
*.pid
__pycache__/
*.pyc
```

If the LAN is not trusted, tunnel the broker over SSH or put it behind a VPN instead of exposing it directly.

## Configuration Model

By default, config generation is role-specific:

- Ubuntu: `config.ubuntu.local.json`
- Windows: `config.windows.local.json`

At runtime, the scripts prefer the role-specific local config first and then fall back to `config.local.json`.

Key config sections:

- `machine_role`: `ubuntu` or `windows`
- `broker`: broker host, port, URL, token, database path, and whether the machine hosts the broker
- `endpoint`: endpoint agent id, Codex binary, default cwd, default sandbox, default approval policy, instruction file, and endpoint state file
- `bridge`: local MCP bridge agent id, MCP server name, and bridge state file

Windows also supports an optional `network_drives` section. Example:

```json
"network_drives": [
  {
    "letter": "M",
    "path": "\\\\SERVER\\SHARE",
    "persistent": true,
    "username": "shareuser",
    "password": "secret",
    "credential_target": "SERVER_OR_IP"
  }
]
```

`windows/Start-CodexWindowsSide.ps1` will try to map those drives before the endpoint starts. The current Windows launcher also contains fallback support for deployment-specific `BF6TOOLS_*` environment variables.

## Setup

### 1. Generate The Ubuntu Config

Run this on the Ubuntu machine that will host the broker:

```bash
python3 init_config.py --role ubuntu
```

That writes `config.ubuntu.local.json`, chooses a likely LAN IP for the broker URL, and generates a fresh shared token.

### 2. Generate The Windows Config

Copy the Ubuntu broker URL and token into the Windows config:

```powershell
py .\init_config.py `
  --role windows `
  --broker-url http://UBUNTU_IP:8765 `
  --token TOKEN_FROM_UBUNTU
```

That writes `config.windows.local.json`.

### 3. Start The Ubuntu Side

```bash
python3 run_broker.py --config-file config.ubuntu.local.json
python3 run_endpoint.py --config-file config.ubuntu.local.json
python3 install_mcp.py --config-file config.ubuntu.local.json
python3 doctor.py --config-file config.ubuntu.local.json
```

Notes:

- `run_broker.py` should only be run on the broker host.
- `install_mcp.py` registers the local `lanBridge` MCP server with Codex.
- `doctor.py` validates broker reachability and can probe a peer endpoint.

### 4. Start The Windows Side

```powershell
py .\run_endpoint.py --config-file .\config.windows.local.json
py .\install_mcp.py --config-file .\config.windows.local.json
py .\doctor.py --config-file .\config.windows.local.json --peer ubuntu-codex
```

If you want the Windows endpoint attached to the current terminal for debugging:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\windows\Start-CodexEndpoint.ps1 -Foreground
```

## Helper Scripts

Ubuntu helpers:

```bash
./ubuntu/init-config.sh
./ubuntu/start-broker.sh
./ubuntu/start-endpoint.sh
./ubuntu/start-mcp-bridge.sh
./ubuntu/doctor.sh
```

Windows helpers:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\windows\Initialize-CodexLanBridge.ps1 -BrokerUrl http://UBUNTU_IP:8765 -Token TOKEN_FROM_UBUNTU
PowerShell -ExecutionPolicy Bypass -File .\windows\Start-CodexEndpoint.ps1
PowerShell -ExecutionPolicy Bypass -File .\windows\Start-CodexBridge.ps1
PowerShell -ExecutionPolicy Bypass -File .\windows\Test-CodexLanBridge.ps1 --peer ubuntu-codex
PowerShell -ExecutionPolicy Bypass -File .\windows\Install-CodexWindowsEndpointStartup.ps1
```

What those scripts do:

- `Initialize-CodexLanBridge.ps1`: generate the Windows config
- `Start-CodexEndpoint.ps1`: start the endpoint now, detached by default
- `Start-CodexBridge.ps1`: install or refresh the local `lanBridge` MCP registration
- `Install-CodexWindowsEndpointStartup.ps1`: register a scheduled task for startup and logon, or fall back to a Startup shortcut if task registration is denied
- `Start-CodexWindowsSide.ps1`: boot/logon launcher that maps configured network drives and then starts the endpoint

Windows endpoint logs are written to:

- `state/windows-endpoint.stdout.log`
- `state/windows-endpoint.stderr.log`

## Using It From Codex

After `install_mcp.py` runs successfully, the local Codex installation gets these tools:

- `list_remote_agents`
- `remote_codex`
- `remote_codex_start`
- `remote_codex_status`
- `remote_codex_wait`

### Synchronous Example

```text
remote_codex(
  peer_agent="windows-codex",
  cwd="C:\\repo",
  prompt="Inspect the failing tests and summarize the root cause."
)
```

If the remote task finishes quickly, `remote_codex` returns the final reply. If it is still running, it returns a structured snapshot with conversation metadata and the latest known status.

### Asynchronous Example

Start the job:

```text
remote_codex_start(
  peer_agent="ubuntu-codex",
  cwd="/srv/app",
  prompt="Run the service diagnostics and tell me why startup fails."
)
```

Then poll or wait:

```text
remote_codex_wait(request_id="...", after_seq=0, wait_seconds=30)
remote_codex_status(request_id="...")
```

### Continuing A Remote Conversation

When a remote call returns a `threadId`, reuse it on later prompts:

```text
remote_codex(
  peer_agent="windows-codex",
  thread_id="...",
  prompt="Apply the fix and rerun the same failing tests."
)
```

Use endpoint agent ids for remote execution:

- `ubuntu-codex`
- `windows-codex`

Do not target the MCP bridge ids (`ubuntu-ui`, `windows-ui`) with `remote_codex*`.

## Endpoint Instruction Files

The endpoint injects a machine-specific instruction file when a new remote thread starts:

- `examples/ubuntu-endpoint-instructions.txt`
- `examples/windows-endpoint-instructions.txt`

Replace these with instructions that match your own repos, tools, and operational rules.

## Diagnostics

`doctor.py` is the main smoke-test tool. It checks broker health, registers a temporary doctor agent, lists live agents, and can run repeatable remote probes.

Examples:

```bash
python3 doctor.py --config-file config.ubuntu.local.json --peer windows-codex
python3 doctor.py --config-file config.ubuntu.local.json --peer windows-codex --roundtrips 3 --conversation-turns 2
```

```powershell
py .\doctor.py --config-file .\config.windows.local.json --peer ubuntu-codex
```

If something is wrong, check:

- broker reachability
- broker token mismatch
- endpoint agent registration in `list_remote_agents`
- `state/` logs and state files
- local Codex CLI path in the endpoint config
- Windows scheduled task state if the Windows endpoint should auto-start
- SMB reachability and credentials if you use `network_drives`

## Ubuntu Service Templates

Systemd templates live in `ubuntu/`:

- `codex-lan-broker.service`
- `codex-lan-endpoint.service`

Update paths and usernames before installing them.

## Notes For This Combined Package

This package currently contains both host-specific config variants:

- `config.windows.local.json`
- `config.ubuntu.local.json`

That is useful for operational reference, but those files should normally be removed or scrubbed before a public GitHub push.
