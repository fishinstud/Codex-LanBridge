from __future__ import annotations

import json
import queue
import subprocess
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from bridge_common import (
    DEFAULT_PROTOCOL_VERSION,
    extract_text_from_mcp_content,
)

RESUME_UNSUPPORTED_OPTIONS = (
    "cwd",
    "sandbox",
    "model",
    "profile",
    "approval_policy",
    "developer_instructions",
    "base_instructions",
)


class JsonRpcError(RuntimeError):
    pass


def _has_explicit_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def find_resume_unsupported_options(arguments: Dict[str, Any]) -> list[str]:
    return [
        option
        for option in RESUME_UNSUPPORTED_OPTIONS
        if _has_explicit_value(arguments.get(option))
    ]


class StdioJsonRpcClient:
    def __init__(self, command: list[str], cwd: Optional[Path] = None) -> None:
        self.command = command
        self.cwd = cwd
        self.process: Optional[subprocess.Popen[str]] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._next_id = 1
        self._pending: Dict[int, queue.Queue[Dict[str, Any]]] = {}
        self._notification_handlers: Dict[int, Callable[[Dict[str, Any]], None]] = {}
        self.stderr_tail: list[str] = []

    def start(self) -> None:
        if self.process and self.process.poll() is None:
            return
        self.close()
        self.stderr_tail = []
        self.process = subprocess.Popen(
            self.command,
            cwd=str(self.cwd) if self.cwd else None,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()
        self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True)
        self._stderr_thread.start()

    def close(self) -> None:
        process = self.process
        if not process:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        self._fail_pending("JSON-RPC subprocess closed")
        self.process = None

    def _fail_pending(self, message: str) -> None:
        pending = list(self._pending.items())
        self._pending.clear()
        for _, response_queue in pending:
            try:
                response_queue.put_nowait(
                    {
                        "error": {
                            "code": -32001,
                            "message": message,
                        }
                    }
                )
            except queue.Full:
                pass

    def _stderr_loop(self) -> None:
        process = self.process
        assert process is not None
        assert process.stderr is not None
        for line in process.stderr:
            line = line.rstrip("\n")
            if not line:
                continue
            self.stderr_tail.append(line)
            if len(self.stderr_tail) > 200:
                self.stderr_tail = self.stderr_tail[-200:]

    def _reader_loop(self) -> None:
        process = self.process
        assert process is not None
        assert process.stdout is not None
        for line in process.stdout:
            text = line.strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError:
                continue
            message_id = message.get("id")
            if message_id is None:
                self._handle_notification(message)
                continue
            pending = self._pending.get(int(message_id))
            if pending:
                pending.put(message)
        returncode = process.poll()
        detail = (
            f"JSON-RPC subprocess exited with code {returncode}"
            if returncode is not None
            else "JSON-RPC subprocess stopped producing output"
        )
        self._fail_pending(detail)

    def _handle_notification(self, message: Dict[str, Any]) -> None:
        params = message.get("params", {})
        if not isinstance(params, dict):
            return
        meta = params.get("_meta", {})
        if not isinstance(meta, dict):
            meta = {}
        request_id = meta.get("requestId")
        try:
            request_id_int = int(request_id)
        except (TypeError, ValueError):
            return
        handler = self._notification_handlers.get(request_id_int)
        if handler is None:
            return
        try:
            handler(message)
        except Exception:
            return

    def call(
        self,
        method: str,
        params: Dict[str, Any],
        timeout_seconds: int = 120,
        notification_handler: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        self.start()
        assert self.process is not None
        assert self.process.stdin is not None
        if self.process.poll() is not None:
            raise JsonRpcError("JSON-RPC subprocess is not running")
        with self._lock:
            request_id = self._next_id
            self._next_id += 1
            response_queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=1)
            self._pending[request_id] = response_queue
            if notification_handler is not None:
                self._notification_handlers[request_id] = notification_handler
            payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
            try:
                self.process.stdin.write(json.dumps(payload) + "\n")
                self.process.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                self._pending.pop(request_id, None)
                raise JsonRpcError(f"Could not send {method}: {exc}") from exc
        try:
            response = response_queue.get(timeout=timeout_seconds)
        except queue.Empty as exc:
            self.close()
            raise JsonRpcError(f"Timed out waiting for {method}") from exc
        finally:
            self._pending.pop(request_id, None)
            self._notification_handlers.pop(request_id, None)
        if "error" in response:
            raise JsonRpcError(json.dumps(response["error"], sort_keys=True))
        return response.get("result", {})


class CodexMcpClient:
    def __init__(self, codex_bin: str = "codex") -> None:
        self.rpc = StdioJsonRpcClient([codex_bin, "mcp-server"])
        self._initialized = False

    def start(self) -> None:
        self.rpc.start()
        if self._initialized:
            return
        try:
            self.rpc.call(
                "initialize",
                {
                    "protocolVersion": DEFAULT_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "codex-lan-endpoint",
                        "version": "1.0",
                    },
                },
                timeout_seconds=30,
            )
            self.rpc.call("tools/list", {}, timeout_seconds=30)
            self._initialized = True
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        self.rpc.close()
        self._initialized = False

    def run_codex(
        self,
        prompt: str,
        *,
        thread_id: Optional[str] = None,
        cwd: Optional[str] = None,
        sandbox: Optional[str] = None,
        model: Optional[str] = None,
        profile: Optional[str] = None,
        approval_policy: Optional[str] = None,
        developer_instructions: Optional[str] = None,
        base_instructions: Optional[str] = None,
        timeout_seconds: int = 1800,
        event_handler: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        if thread_id:
            rejected = find_resume_unsupported_options(
                {
                    "cwd": cwd,
                    "sandbox": sandbox,
                    "model": model,
                    "profile": profile,
                    "approval_policy": approval_policy,
                    "developer_instructions": developer_instructions,
                    "base_instructions": base_instructions,
                }
            )
            if rejected:
                raise ValueError(
                    "thread_id follow-up requests do not support: "
                    + ", ".join(rejected)
                )
            tool_name = "codex-reply"
            arguments: Dict[str, Any] = {
                "threadId": thread_id,
                "prompt": prompt,
            }
        else:
            tool_name = "codex"
            arguments = {
                "prompt": prompt,
            }
            if cwd:
                arguments["cwd"] = cwd
            if sandbox:
                arguments["sandbox"] = sandbox
            if model:
                arguments["model"] = model
            if profile:
                arguments["profile"] = profile
            if approval_policy:
                arguments["approval-policy"] = approval_policy
            if developer_instructions:
                arguments["developer-instructions"] = developer_instructions
            if base_instructions:
                arguments["base-instructions"] = base_instructions
        self.start()
        try:
            result = self.rpc.call(
                "tools/call",
                {
                    "name": tool_name,
                    "arguments": arguments,
                },
                timeout_seconds=timeout_seconds,
                notification_handler=event_handler,
            )
        except Exception:
            self.close()
            raise
        structured = result.get("structuredContent", {})
        content_text = structured.get("content")
        if not content_text:
            content_text = extract_text_from_mcp_content(result.get("content", []))
        return {
            "thread_id": structured.get("threadId") or thread_id,
            "content": content_text or "",
            "raw": result,
            "stderr_tail": self.rpc.stderr_tail[-20:],
        }
