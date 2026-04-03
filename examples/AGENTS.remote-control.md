Use the `lanBridge` MCP tools whenever work must happen on the other machine.

Rules:

- Call `list_remote_agents` if you are not sure the peer is online.
- Use `remote_codex` with `peer_agent="windows-codex"` for Windows work.
- Use `remote_codex` with `peer_agent="ubuntu-codex"` for Ubuntu work.
- Pass a concrete `cwd` that exists on the remote machine whenever the task is repo-specific.
- Reuse the returned `threadId` for follow-up prompts in the same remote conversation.
- Treat remote filesystem edits and command results as happening only on the remote machine.
- Ask the remote Codex to verify locally there instead of assuming parity with this host.

Examples:

- `remote_codex(peer_agent="windows-codex", cwd="C:\\Users\\me\\project", prompt="Inspect the failing tests and summarize the root cause.")`
- `remote_codex(peer_agent="windows-codex", thread_id="<prior thread id>", prompt="Apply the fix and rerun the failing tests.")`
- `remote_codex(peer_agent="ubuntu-codex", cwd="/srv/app", prompt="Check the service logs and tell me why startup failed.")`
