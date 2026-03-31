import shlex
import os
import sys

import paramiko


HOST = os.environ.get("JETSON_FIX_HOST", "CHANGE-ME")
USER = os.environ.get("JETSON_FIX_USER", "CHANGE-ME")
PASSWORD = os.environ.get("JETSON_FIX_PASSWORD", "")


def require_settings():
    missing = []
    if not HOST or HOST == "CHANGE-ME":
        missing.append("JETSON_FIX_HOST")
    if not USER or USER == "CHANGE-ME":
        missing.append("JETSON_FIX_USER")
    if not PASSWORD:
        missing.append("JETSON_FIX_PASSWORD")
    if missing:
        raise SystemExit("Missing required environment variables: " + ", ".join(missing))


def run(client, command, *, sudo=False, timeout=300):
    wrapped = f"bash -lc {shlex.quote(command)}"
    if sudo:
        wrapped = f"sudo -S {wrapped}"
    stdin, stdout, stderr = client.exec_command(wrapped, timeout=timeout, get_pty=sudo)
    if sudo:
        stdin.write(PASSWORD + "\n")
        stdin.flush()
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def print_result(name, code, out, err):
    print(f"=== {name} exit={code} ===")
    if out.strip():
        print(out.rstrip())
    if err.strip():
        print("--- stderr ---")
        print(err.rstrip())


def main():
    require_settings()
    remote_home = f"/home/{USER}"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASSWORD, timeout=30)
    try:
        steps = [
            (
                "prepare_runtime",
                f"install -d -m 755 {remote_home}/.local/bin && "
                f"if [ ! -d {remote_home}/.local/node-v22.21.1 ]; then "
                f"cp -a /root/.nvm/versions/node/v22.21.1 {remote_home}/.local/node-v22.21.1; "
                "fi && "
                f"chown -R {USER}:{USER} {remote_home}/.local/node-v22.21.1 {remote_home}/.local/bin && "
                "for name in node npm npx codex; do "
                f"ln -sfn {remote_home}/.local/node-v22.21.1/bin/$name {remote_home}/.local/bin/$name; "
                "done",
                True,
            ),
            (
                "ensure_path_profile",
                "for f in ~/.profile ~/.bashrc; do "
                "touch \"$f\"; "
                "grep -Fqx 'export PATH=\"$HOME/.local/bin:$PATH\"' \"$f\" || "
                "printf '\\nexport PATH=\"$HOME/.local/bin:$PATH\"\\n' >> \"$f\"; "
                "done",
                False,
            ),
            (
                "verify_path",
                "source ~/.profile >/dev/null 2>&1; "
                "command -v node; command -v npm; command -v npx; command -v codex; "
                "codex --version",
                False,
            ),
            (
                "verify_reverse_launch",
                "source ~/.profile >/dev/null 2>&1; "
                "cd ~/.codex/memories/codex_lan_bridge && "
                "codex exec --dangerously-bypass-approvals-and-sandbox "
                "--skip-git-repo-check --json "
                "\"Use the lanBridge MCP server. First call list_remote_agents and confirm windows-codex is available. "
                "Then call remote_codex_start targeting windows-codex with a trivial prompt asking it to reply with the hostname only. "
                "Use remote_codex_wait and/or remote_codex_status to monitor until there is a final result. "
                "Do not modify files. Return the remote agent used, the conversation id, the thread id, and the final hostname result only.\"",
                False,
            ),
            (
                "verify_ubuntu_ui_alive",
                "python3 - <<'PY'\n"
                "import json, re, urllib.request\n"
                f"text = open('{remote_home}/.codex/config.toml', 'r', encoding='utf-8').read()\n"
                "match = re.search(r'--token\"?,\\s*\"([^\"]+)\"', text)\n"
                "if not match:\n"
                "    raise SystemExit('token not found in config.toml')\n"
                "req = urllib.request.Request(\n"
                "    'http://127.0.0.1:8765/v1/agents',\n"
                "    headers={'Authorization': f'Bearer {match.group(1)}'},\n"
                ")\n"
                "data = json.load(urllib.request.urlopen(req))\n"
                "records = data.get('rawAgents', data.get('agents', data)) if isinstance(data, dict) else data\n"
                "alive = [a for a in records if isinstance(a, dict) and a.get('agent_id','').startswith('ubuntu-ui') and a.get('alive')]\n"
                "print(json.dumps({'alive_count': len(alive), 'alive_agents': [a.get('agent_id') for a in alive]}, indent=2))\n"
                "PY",
                False,
            ),
        ]

        for name, command, sudo in steps:
            code, out, err = run(client, command, sudo=sudo, timeout=1200 if name == "verify_reverse_launch" else 300)
            print_result(name, code, out, err)
            if code != 0:
                return code
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
