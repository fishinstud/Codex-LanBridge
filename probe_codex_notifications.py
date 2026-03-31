import json
import subprocess
import sys
import time


def send(proc, msg):
    proc.stdin.write(json.dumps(msg) + "\n")
    proc.stdin.flush()


def main():
    prompt = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "Think for a few seconds, then answer with the word done."
    )
    proc = subprocess.Popen(
        ["codex", "mcp-server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    send(
        proc,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "probe", "version": "1.0"},
            },
        },
    )
    send(proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    send(
        proc,
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "codex", "arguments": {"prompt": prompt}},
        },
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        print(line.rstrip())
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") == 3:
            break
    err = proc.stderr.read().strip()
    if err:
        print("STDERR:", err, file=sys.stderr)
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()


if __name__ == "__main__":
    main()
