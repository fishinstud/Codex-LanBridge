from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from bridge_common import BrokerClient, HttpError, new_request_id, summarize_text
from codex_rpc import CodexMcpClient, JsonRpcError, find_resume_unsupported_options


WINDOWS_SAFE_SANDBOX = "danger-full-access"
WINDOWS_SANDBOX_ERROR_SNIPPETS = (
    "windows sandbox",
    "setup refresh failed",
    "createprocesswithlogonw failed",
    "refusing to run unsandboxed",
    "timed out waiting for tools/call",
)
THREAD_RESUME_NOT_FOUND_SNIPPETS = (
    "session not found for thread_id",
    "session not found",
    "thread not found",
)
FORWARDED_CODEX_EVENT_TYPES = {
    "session_configured",
    "mcp_startup_update",
    "mcp_startup_complete",
    "task_started",
    "item_started",
    "item_completed",
    "token_count",
    "agent_message_content_delta",
    "agent_message_delta",
    "agent_message",
    "task_complete",
}
REQUEST_TIMEOUT_GRACE_SECONDS = 5
ORPHAN_REQUEST_MIN_AGE_SECONDS = 30
AGENT_LIVENESS_CACHE_SECONDS = 15


def _parse_utc_timestamp(timestamp: Any) -> Optional[datetime]:
    text = str(timestamp or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Endpoint daemon that exposes a local Codex instance through the LAN broker."
    )
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument(
        "--state-file",
        default=".agent_endpoint_state.json",
        help="Cursor state file for broker polling.",
    )
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--default-cwd", default="")
    parser.add_argument(
        "--default-sandbox",
        default="workspace-write",
        choices=["read-only", "workspace-write", "danger-full-access"],
    )
    parser.add_argument(
        "--default-approval-policy",
        default="never",
        choices=["untrusted", "on-failure", "on-request", "never"],
    )
    parser.add_argument("--role-instructions-file", default="")
    parser.add_argument("--heartbeat-seconds", type=int, default=30)
    parser.add_argument("--poll-wait-seconds", type=int, default=25)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--session-idle-seconds", type=int, default=900)
    parser.add_argument("--max-sessions", type=int, default=8)
    parser.add_argument("--warm-clients", type=int, default=1)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


@dataclass
class EndpointSession:
    thread_id: str
    client: CodexMcpClient
    created_at: float
    last_used_at: float
    in_use: bool = False


class EndpointDaemon:
    def __init__(self, args: argparse.Namespace) -> None:
        state_path = Path(args.state_file).resolve()
        self.client = BrokerClient(
            broker_url=args.broker_url,
            token=args.token,
            agent_id=args.agent_id,
            state_path=state_path,
        )
        self.codex_bin = args.codex_bin
        self.is_windows = os.name == "nt"
        self.default_cwd = self._normalize_startup_cwd(args.default_cwd or None)
        self.default_sandbox = self._normalize_default_sandbox(args.default_sandbox)
        self.default_approval_policy = args.default_approval_policy
        self.poll_wait_seconds = args.poll_wait_seconds
        self.heartbeat_seconds = args.heartbeat_seconds
        self.max_concurrency = max(1, args.max_concurrency)
        self.session_idle_seconds = max(60, args.session_idle_seconds)
        self.max_sessions = max(self.max_concurrency, args.max_sessions)
        self.warm_clients = max(0, min(args.warm_clients, self.max_sessions))
        self.last_heartbeat = 0.0
        self.run_once = args.run_once
        self.role_instructions = self._load_instructions(args.role_instructions_file)
        self.should_stop = False
        self.register_retry_seconds = 2
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_concurrency,
            thread_name_prefix=f"{args.agent_id}-worker",
        )
        committed_seq = self.client.get_last_seq()
        self._state_lock = threading.Lock()
        self._progress = threading.Event()
        self._inflight: set[Future[None]] = set()
        self._lane_queues: Dict[str, Deque[Dict[str, Any]]] = {}
        self._active_lanes: set[str] = set()
        self._active_codex: Dict[int, CodexMcpClient] = {}
        self._pending_message_seqs: set[int] = set()
        self._committed_seq = committed_seq
        self._polled_seq = committed_seq
        self._sessions: Dict[str, EndpointSession] = {}
        self._warm_pool: Deque[CodexMcpClient] = deque()
        self._alive_agents_cache: Dict[str, bool] = {}
        self._alive_agents_checked_at = 0.0
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name=f"{args.agent_id}-session-maint",
            daemon=True,
        )
        self._maintenance_thread.start()

    def _handle_broker_unavailable(self, context: str, exc: HttpError) -> bool:
        print(f"[{self.client.agent_id}] {context}: {exc}", file=sys.stderr)
        if self.run_once:
            self._request_shutdown(f"{context}: {exc}")
            return False
        retry_seconds = max(1, self.register_retry_seconds)
        print(
            f"[{self.client.agent_id}] retrying broker connection in "
            f"{retry_seconds}s",
            file=sys.stderr,
        )
        for _ in range(retry_seconds):
            if self.should_stop:
                return False
            time.sleep(1)
        return not self.should_stop

    def _normalize_startup_cwd(self, cwd: Optional[str]) -> Optional[str]:
        normalized = self._resolve_cwd(cwd, fallback=None)
        if cwd and normalized is None:
            print(
                f"[{self.client.agent_id}] ignoring unusable default cwd: {cwd}",
                file=sys.stderr,
            )
        return normalized

    def _normalize_default_sandbox(self, sandbox: str) -> str:
        requested = (sandbox or "workspace-write").strip() or "workspace-write"
        if self.is_windows and requested != WINDOWS_SAFE_SANDBOX:
            print(
                f"[{self.client.agent_id}] Windows endpoint forcing default sandbox "
                f"from {requested} to {WINDOWS_SAFE_SANDBOX}",
                file=sys.stderr,
            )
            return WINDOWS_SAFE_SANDBOX
        return requested

    def _resolve_cwd(self, cwd: Optional[str], *, fallback: Optional[str]) -> Optional[str]:
        raw_value = (cwd or "").strip()
        if not raw_value:
            return fallback
        expanded = os.path.expanduser(os.path.expandvars(raw_value))
        try:
            candidate = Path(expanded)
        except (TypeError, ValueError):
            return fallback
        if not candidate.is_absolute():
            base = Path(fallback) if fallback else Path.cwd()
            candidate = base / candidate
        try:
            candidate = candidate.resolve(strict=False)
        except OSError:
            pass
        try:
            if candidate.is_dir():
                return str(candidate)
        except OSError:
            pass
        return fallback

    def _normalize_request_cwd(self, cwd: Optional[str]) -> Optional[str]:
        normalized = self._resolve_cwd(cwd, fallback=self.default_cwd)
        raw_value = (cwd or "").strip()
        if raw_value and normalized != raw_value and normalized != cwd:
            if normalized:
                print(
                    f"[{self.client.agent_id}] falling back from unusable cwd "
                    f"{raw_value!r} to {normalized!r}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[{self.client.agent_id}] leaving cwd unset because {raw_value!r} "
                    "is not a usable directory",
                    file=sys.stderr,
                )
        return normalized

    def _sandbox_candidates(
        self, requested_sandbox: Optional[str]
    ) -> list[str]:
        primary = (requested_sandbox or self.default_sandbox or "").strip()
        if not primary:
            primary = WINDOWS_SAFE_SANDBOX if self.is_windows else "workspace-write"
        if self.is_windows:
            if primary != WINDOWS_SAFE_SANDBOX:
                print(
                    f"[{self.client.agent_id}] overriding requested sandbox "
                    f"{primary} to {WINDOWS_SAFE_SANDBOX} on Windows",
                    file=sys.stderr,
                )
            return [WINDOWS_SAFE_SANDBOX]
        candidates = [primary]
        deduped: list[str] = []
        for candidate in candidates:
            if candidate not in deduped:
                deduped.append(candidate)
        return deduped

    def _is_windows_sandbox_failure(
        self,
        *,
        attempted_sandbox: str,
        error_text: str = "",
        stderr_tail: Optional[list[str]] = None,
    ) -> bool:
        if not self.is_windows or attempted_sandbox == WINDOWS_SAFE_SANDBOX:
            return False
        combined = "\n".join([error_text, *(stderr_tail or [])]).lower()
        return any(snippet in combined for snippet in WINDOWS_SANDBOX_ERROR_SNIPPETS)

    @staticmethod
    def _is_missing_thread_error(error_text: str) -> bool:
        combined = (error_text or "").lower()
        return any(snippet in combined for snippet in THREAD_RESUME_NOT_FOUND_SNIPPETS)

    def _run_new_thread_request(
        self,
        *,
        message: Dict[str, Any],
        request_id: str,
        prompt: str,
        body: Dict[str, Any],
        cwd: Optional[str],
        effective_sandbox: str,
        approval_policy: str,
        developer_instructions: str,
    ) -> bool:
        last_error = "Could not start Codex session"
        for sandbox in [effective_sandbox]:
            for attempt in range(2):
                codex = self._load_or_create_new_client()
                try:
                    result = codex.run_codex(
                        prompt=prompt,
                        thread_id=None,
                        cwd=cwd,
                        sandbox=sandbox,
                        model=body.get("model") or None,
                        profile=body.get("profile") or None,
                        approval_policy=approval_policy,
                        developer_instructions=developer_instructions or None,
                        base_instructions=body.get("base_instructions") or None,
                        timeout_seconds=int(body.get("timeout_secs") or 1800),
                        event_handler=lambda event: self._forward_codex_event(
                            message, request_id, event
                        ),
                    )
                except (JsonRpcError, OSError) as exc:
                    last_error = str(exc)
                    retry_with_safe_sandbox = self._is_windows_sandbox_failure(
                        attempted_sandbox=sandbox,
                        error_text=last_error,
                    )
                    self._close_client(codex)
                    if retry_with_safe_sandbox:
                        print(
                            f"[{self.client.agent_id}] retrying {request_id} with "
                            f"{WINDOWS_SAFE_SANDBOX} after Windows sandbox failure: "
                            f"{last_error}",
                            file=sys.stderr,
                        )
                        break
                    if attempt == 0:
                        continue
                    break
                except Exception as exc:
                    self._close_client(codex)
                    return self._send_error(
                        message, request_id, f"Unhandled endpoint error: {exc}"
                    )

                if self._is_windows_sandbox_failure(
                    attempted_sandbox=sandbox,
                    stderr_tail=result["stderr_tail"],
                ):
                    last_error = (
                        "Windows sandbox failed during request execution; retrying "
                        f"with {WINDOWS_SAFE_SANDBOX}"
                    )
                    self._close_client(codex)
                    print(
                        f"[{self.client.agent_id}] retrying {request_id} with "
                        f"{WINDOWS_SAFE_SANDBOX} after stderr reported a Windows "
                        "sandbox failure",
                        file=sys.stderr,
                    )
                    break

                result_thread_id = result["thread_id"]
                if result_thread_id:
                    self._bind_session_client(result_thread_id, codex)
                    self._release_session_client(
                        thread_id=result_thread_id,
                        codex=codex,
                        keep=True,
                    )
                else:
                    self._return_warm_client(codex)
                payload = {
                    "ok": True,
                    "request_id": request_id,
                    "thread_id": result_thread_id,
                    "content": result["content"],
                    "stderr_tail": result["stderr_tail"],
                    "effective_cwd": cwd or "",
                    "effective_sandbox": sandbox,
                }
                return self._send_response(message, payload)
        return self._send_error(message, request_id, last_error)

    @staticmethod
    def _load_instructions(path_value: str) -> str:
        if not path_value:
            return ""
        return Path(path_value).read_text(encoding="utf-8").strip()

    def register(self) -> None:
        self.client.register(
            role="endpoint",
            metadata={
                "service": "codex-endpoint",
                "default_cwd": self.default_cwd,
                "default_sandbox": self.default_sandbox,
                "default_approval_policy": self.default_approval_policy,
                "session_idle_seconds": self.session_idle_seconds,
                "max_sessions": self.max_sessions,
                "warm_clients": self.warm_clients,
            },
        )
        self.last_heartbeat = time.time()

    def maybe_register(self) -> None:
        now = time.time()
        if now - self.last_heartbeat >= self.heartbeat_seconds:
            self.register()

    def _next_poll_wait(self) -> int:
        if self.run_once:
            return 0
        if self.heartbeat_seconds <= 0:
            return max(1, self.poll_wait_seconds)
        heartbeat_wait = max(
            0.0, self.last_heartbeat + self.heartbeat_seconds - time.time()
        )
        return max(1, min(self.poll_wait_seconds, int(math.ceil(heartbeat_wait))))

    def _lane_key(self, message: Dict[str, Any]) -> str:
        thread_id = message.get("body", {}).get("thread_id") or None
        if thread_id:
            return f"thread:{thread_id}"
        return f"seq:{int(message['seq'])}"

    def _track_codex(self, codex: CodexMcpClient) -> None:
        with self._state_lock:
            self._active_codex[id(codex)] = codex

    def _release_codex(self, codex: CodexMcpClient) -> None:
        with self._state_lock:
            self._active_codex.pop(id(codex), None)
            self._progress.set()

    def _close_client(self, codex: CodexMcpClient) -> None:
        self._release_codex(codex)
        codex.close()

    def _close_all_managed_clients(self) -> None:
        with self._state_lock:
            active = list(self._active_codex.values())
            sessions = [session.client for session in self._sessions.values()]
            warm = list(self._warm_pool)
            self._sessions.clear()
            self._warm_pool.clear()
        seen: set[int] = set()
        for codex in active + sessions + warm:
            token = id(codex)
            if token in seen:
                continue
            seen.add(token)
            codex.close()

    def _managed_client_count_locked(self) -> int:
        return len(self._sessions) + len(self._warm_pool)

    def _create_client(self) -> CodexMcpClient:
        codex = CodexMcpClient(codex_bin=self.codex_bin)
        codex.start()
        return codex

    def _borrow_warm_client(self) -> Optional[CodexMcpClient]:
        with self._state_lock:
            if not self._warm_pool:
                return None
            codex = self._warm_pool.popleft()
        self._track_codex(codex)
        return codex

    def _borrow_fresh_client(self) -> CodexMcpClient:
        codex = self._create_client()
        self._track_codex(codex)
        return codex

    def _load_or_create_new_client(self) -> CodexMcpClient:
        warm = self._borrow_warm_client()
        if warm is not None:
            return warm
        return self._borrow_fresh_client()

    def _borrow_session_client(self, thread_id: str) -> CodexMcpClient:
        with self._state_lock:
            session = self._sessions.get(thread_id)
            if session is None:
                raise KeyError(thread_id)
            session.in_use = True
            session.last_used_at = time.time()
            codex = session.client
        self._track_codex(codex)
        return codex

    def _bind_session_client(self, thread_id: str, codex: CodexMcpClient) -> None:
        now = time.time()
        to_close: Optional[CodexMcpClient] = None
        with self._state_lock:
            existing = self._sessions.get(thread_id)
            if existing and existing.client is not codex:
                to_close = existing.client
            self._sessions[thread_id] = EndpointSession(
                thread_id=thread_id,
                client=codex,
                created_at=now,
                last_used_at=now,
                in_use=False,
            )
            self._progress.set()
        if to_close is not None:
            to_close.close()

    def _release_session_client(
        self,
        *,
        thread_id: str,
        codex: CodexMcpClient,
        keep: bool,
    ) -> None:
        stale_client: Optional[CodexMcpClient] = None
        with self._state_lock:
            session = self._sessions.get(thread_id)
            if session and session.client is codex:
                if keep:
                    session.in_use = False
                    session.last_used_at = time.time()
                else:
                    stale_client = self._sessions.pop(thread_id).client
            self._progress.set()
        self._release_codex(codex)
        if stale_client is not None:
            stale_client.close()

    def _return_warm_client(self, codex: CodexMcpClient) -> None:
        keep_warm = False
        with self._state_lock:
            if (
                not self.should_stop
                and len(self._warm_pool) < self.warm_clients
                and self._managed_client_count_locked() < self.max_sessions
            ):
                self._warm_pool.append(codex)
                keep_warm = True
            self._progress.set()
        self._release_codex(codex)
        if not keep_warm:
            codex.close()

    def _evict_idle_sessions(self) -> None:
        stale_sessions: list[EndpointSession] = []
        stale_warm: list[CodexMcpClient] = []
        now = time.time()
        with self._state_lock:
            for thread_id, session in list(self._sessions.items()):
                if session.in_use:
                    continue
                if now - session.last_used_at < self.session_idle_seconds:
                    continue
                stale_sessions.append(self._sessions.pop(thread_id))
            while len(self._warm_pool) > self.warm_clients:
                stale_warm.append(self._warm_pool.pop())
        for session in stale_sessions:
            session.client.close()
        for codex in stale_warm:
            codex.close()

    def _ensure_warm_pool(self) -> None:
        while not self.should_stop:
            with self._state_lock:
                if len(self._warm_pool) >= self.warm_clients:
                    return
                if self._managed_client_count_locked() >= self.max_sessions:
                    return
            try:
                codex = self._create_client()
            except Exception:
                return
            with self._state_lock:
                if (
                    self.should_stop
                    or len(self._warm_pool) >= self.warm_clients
                    or self._managed_client_count_locked() >= self.max_sessions
                ):
                    codex.close()
                    return
                self._warm_pool.append(codex)
                self._progress.set()

    def _maintenance_loop(self) -> None:
        while not self.should_stop:
            self._evict_idle_sessions()
            self._ensure_warm_pool()
            self._progress.wait(timeout=5)
            self._progress.clear()

    def _send_message_with_retry(
        self,
        *,
        message: Dict[str, Any],
        kind: str,
        payload: Dict[str, Any],
    ) -> bool:
        request_id = payload.get("request_id", "-")
        while not self.should_stop:
            try:
                self.client.send_message(
                    to_agent=message["from_agent"],
                    kind=kind,
                    body=payload,
                    conversation_id=message["conversation_id"],
                    reply_to_seq=message["seq"],
                )
                return True
            except HttpError as exc:
                if not self._handle_broker_unavailable(
                    f"could not send {kind} for {request_id}", exc
                ):
                    return False
        return False

    def _send_response(self, message: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        return self._send_message_with_retry(
            message=message,
            kind="codex_response",
            payload=payload,
        )

    def _sanitize_codex_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        params = event.get("params", {})
        if not isinstance(params, dict):
            return None
        meta = params.get("_meta", {})
        if not isinstance(meta, dict):
            meta = {}
        msg = params.get("msg", {})
        if not isinstance(msg, dict):
            return None
        event_type = str(msg.get("type", "")).strip()
        if event_type not in FORWARDED_CODEX_EVENT_TYPES:
            return None
        payload: Dict[str, Any] = {
            "type": event_type,
            "thread_id": str(meta.get("threadId") or ""),
        }
        if event_type == "session_configured":
            payload["model"] = msg.get("model", "")
            payload["approval_policy"] = msg.get("approval_policy", "")
            payload["cwd"] = msg.get("cwd", "")
            sandbox_policy = msg.get("sandbox_policy", {})
            if isinstance(sandbox_policy, dict):
                payload["sandbox"] = sandbox_policy.get("type", "")
            return payload
        if event_type == "mcp_startup_update":
            payload["server"] = msg.get("server", "")
            status = msg.get("status", {})
            if isinstance(status, dict):
                payload["state"] = status.get("state", "")
            return payload
        if event_type == "mcp_startup_complete":
            payload["ready"] = msg.get("ready", [])
            payload["failed"] = msg.get("failed", [])
            payload["cancelled"] = msg.get("cancelled", [])
            return payload
        if event_type == "task_started":
            payload["turn_id"] = msg.get("turn_id", "")
            return payload
        if event_type in {"item_started", "item_completed"}:
            item = msg.get("item", {})
            if not isinstance(item, dict):
                return None
            item_type = str(item.get("type", "")).strip()
            if item_type == "UserMessage":
                return None
            payload["item_type"] = item_type
            payload["item_id"] = item.get("id", "")
            if item.get("phase"):
                payload["phase"] = item.get("phase", "")
            return payload
        if event_type == "token_count":
            info = msg.get("info", {})
            if isinstance(info, dict):
                usage = info.get("last_token_usage") or info.get("total_token_usage") or {}
                if isinstance(usage, dict) and usage:
                    payload["usage"] = usage
            return payload
        if event_type in {"agent_message_content_delta", "agent_message_delta"}:
            payload["delta"] = msg.get("delta", "")
            if msg.get("item_id"):
                payload["item_id"] = msg.get("item_id", "")
            return payload
        if event_type == "agent_message":
            payload["message"] = msg.get("message", "")
            if msg.get("phase"):
                payload["phase"] = msg.get("phase", "")
            return payload
        if event_type == "task_complete":
            payload["turn_id"] = msg.get("turn_id", "")
            payload["last_agent_message"] = msg.get("last_agent_message", "")
            return payload
        return None

    def _forward_codex_event(
        self,
        message: Dict[str, Any],
        request_id: str,
        event: Dict[str, Any],
    ) -> bool:
        payload = self._sanitize_codex_event(event)
        if not payload:
            return True
        payload["ok"] = True
        payload["request_id"] = request_id
        return self._send_message_with_retry(
            message=message,
            kind="codex_event",
            payload=payload,
        )

    def handle_request(self, message: Dict[str, Any]) -> bool:
        body = message.get("body", {})
        request_id = body.get("request_id") or new_request_id()
        prompt = body.get("prompt", "").strip()
        if not prompt:
            return self._send_error(message, request_id, "prompt is required")
        short_circuit = self._maybe_short_circuit_request(message, request_id, body)
        if short_circuit is not None:
            return short_circuit
        return self._run_request(
            message, request_id, prompt, body.get("thread_id") or None, body
        )

    def _run_request(
        self,
        message: Dict[str, Any],
        request_id: str,
        prompt: str,
        thread_id: Optional[str],
        body: Dict[str, Any],
    ) -> bool:
        cwd = self._normalize_request_cwd(body.get("cwd") or self.default_cwd)
        requested_sandbox = body.get("sandbox") or self.default_sandbox
        effective_sandbox = self._sandbox_candidates(requested_sandbox)[0]
        approval_policy = body.get("approval_policy") or self.default_approval_policy
        developer_instructions = body.get("developer_instructions") or ""
        restart_developer_instructions = developer_instructions
        if self.role_instructions:
            if restart_developer_instructions:
                restart_developer_instructions = (
                    self.role_instructions
                    + "\n\n"
                    + restart_developer_instructions
                )
            else:
                restart_developer_instructions = self.role_instructions
        if not thread_id:
            developer_instructions = restart_developer_instructions
        print(
            f"[{self.client.agent_id}] request {request_id} from {message['from_agent']} "
            f"thread={thread_id or '-'} prompt={summarize_text(prompt)}"
        )

        if thread_id:
            restart_context = find_resume_unsupported_options(body)
            reused_session = True
            try:
                codex = self._borrow_session_client(thread_id)
            except KeyError:
                reused_session = False
                codex = self._load_or_create_new_client()
            try:
                result = codex.run_codex(
                    prompt=prompt,
                    thread_id=thread_id,
                    timeout_seconds=int(body.get("timeout_secs") or 1800),
                    event_handler=lambda event: self._forward_codex_event(
                        message, request_id, event
                    ),
                )
            except (JsonRpcError, OSError, ValueError) as exc:
                error_text = str(exc)
                if reused_session:
                    self._release_session_client(thread_id=thread_id, codex=codex, keep=False)
                else:
                    self._close_client(codex)
                if restart_context and self._is_missing_thread_error(error_text):
                    print(
                        f"[{self.client.agent_id}] restarting missing thread "
                        f"{thread_id} for {request_id} as a fresh session",
                        file=sys.stderr,
                    )
                    return self._run_new_thread_request(
                        message=message,
                        request_id=request_id,
                        prompt=prompt,
                        body=body,
                        cwd=cwd,
                        effective_sandbox=effective_sandbox,
                        approval_policy=approval_policy,
                        developer_instructions=restart_developer_instructions,
                    )
                return self._send_error(message, request_id, error_text)
            except Exception as exc:
                if reused_session:
                    self._release_session_client(thread_id=thread_id, codex=codex, keep=False)
                else:
                    self._close_client(codex)
                return self._send_error(
                    message, request_id, f"Unhandled endpoint error: {exc}"
                )
            result_thread_id = result.get("thread_id") or thread_id
            resume_failure_text = "\n".join(
                [result.get("content", ""), *result.get("stderr_tail", [])]
            )
            if restart_context and self._is_missing_thread_error(resume_failure_text):
                if reused_session:
                    self._release_session_client(
                        thread_id=thread_id, codex=codex, keep=False
                    )
                else:
                    self._close_client(codex)
                print(
                    f"[{self.client.agent_id}] received missing-thread reply for "
                    f"{thread_id}; restarting {request_id} as a fresh session",
                    file=sys.stderr,
                )
                return self._run_new_thread_request(
                    message=message,
                    request_id=request_id,
                    prompt=prompt,
                    body=body,
                    cwd=cwd,
                    effective_sandbox=effective_sandbox,
                    approval_policy=approval_policy,
                    developer_instructions=restart_developer_instructions,
                )
            if result_thread_id:
                if not reused_session:
                    self._bind_session_client(result_thread_id, codex)
                self._release_session_client(
                    thread_id=result_thread_id,
                    codex=codex,
                    keep=True,
                )
            else:
                if reused_session:
                    self._release_session_client(thread_id=thread_id, codex=codex, keep=False)
                else:
                    self._close_client(codex)
            payload = {
                "ok": True,
                "request_id": request_id,
                "thread_id": result_thread_id,
                "content": result["content"],
                "stderr_tail": result["stderr_tail"],
                "effective_cwd": cwd or "",
                "effective_sandbox": effective_sandbox,
            }
            return self._send_response(message, payload)

        return self._run_new_thread_request(
            message=message,
            request_id=request_id,
            prompt=prompt,
            body=body,
            cwd=cwd,
            effective_sandbox=effective_sandbox,
            approval_policy=approval_policy,
            developer_instructions=developer_instructions,
        )

    def _send_error(self, message: Dict[str, Any], request_id: str, error_text: str) -> bool:
        print(f"[{self.client.agent_id}] error for {request_id}: {error_text}", file=sys.stderr)
        payload = {
            "ok": False,
            "request_id": request_id,
            "error": error_text,
        }
        return self._send_message_with_retry(
            message=message,
            kind="codex_error",
            payload=payload,
        )

    def _message_age_seconds(self, message: Dict[str, Any]) -> float:
        created_at = _parse_utc_timestamp(message.get("created_at"))
        if created_at is None:
            return 0.0
        return max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds())

    def _refresh_alive_agents_cache(self, *, force: bool = False) -> None:
        now = time.time()
        if (
            not force
            and self._alive_agents_cache
            and now - self._alive_agents_checked_at < AGENT_LIVENESS_CACHE_SECONDS
        ):
            return
        response = self.client.list_agents()
        agents = response.get("agents", [])
        cache: Dict[str, bool] = {}
        if isinstance(agents, list):
            for agent in agents:
                if not isinstance(agent, dict):
                    continue
                agent_id = str(agent.get("agent_id", "")).strip()
                if not agent_id:
                    continue
                cache[agent_id] = bool(agent.get("alive"))
        self._alive_agents_cache = cache
        self._alive_agents_checked_at = now

    def _requester_agent_alive(self, agent_id: str) -> bool:
        try:
            self._refresh_alive_agents_cache()
        except HttpError:
            return True
        return self._alive_agents_cache.get(str(agent_id).strip(), False)

    def _drop_orphaned_request(
        self,
        message: Dict[str, Any],
        request_id: str,
        *,
        age_seconds: float,
        reason: str,
    ) -> bool:
        print(
            f"[{self.client.agent_id}] dropping orphaned request {request_id} "
            f"from {message.get('from_agent', '-')} age={age_seconds:.1f}s: {reason}",
            file=sys.stderr,
        )
        return True

    def _maybe_short_circuit_request(
        self,
        message: Dict[str, Any],
        request_id: str,
        body: Dict[str, Any],
    ) -> Optional[bool]:
        age_seconds = self._message_age_seconds(message)
        timeout_seconds = int(body.get("timeout_secs") or 1800)
        requester_agent = str(message.get("from_agent", "")).strip()
        if age_seconds > timeout_seconds + REQUEST_TIMEOUT_GRACE_SECONDS:
            error_text = (
                "Request expired in broker queue after "
                f"{age_seconds:.1f}s (timeout {timeout_seconds}s)"
            )
            if not self._requester_agent_alive(requester_agent):
                return self._drop_orphaned_request(
                    message,
                    request_id,
                    age_seconds=age_seconds,
                    reason=error_text,
                )
            return self._send_error(message, request_id, error_text)
        if age_seconds < ORPHAN_REQUEST_MIN_AGE_SECONDS:
            return None
        if self._requester_agent_alive(requester_agent):
            return None
        return self._drop_orphaned_request(
            message,
            request_id,
            age_seconds=age_seconds,
            reason="requester agent is no longer alive",
        )

    def _mark_message_done(self, seq: int, *, delivered: bool) -> None:
        with self._state_lock:
            if delivered:
                self._pending_message_seqs.discard(seq)
            if delivered:
                if self._pending_message_seqs:
                    next_pending = min(self._pending_message_seqs)
                    new_committed_seq = max(self._committed_seq, next_pending - 1)
                else:
                    new_committed_seq = max(self._committed_seq, self._polled_seq)
                if new_committed_seq > self._committed_seq:
                    self._committed_seq = new_committed_seq
                    self.client.set_last_seq(self._committed_seq)
            self._progress.set()

    def _run_lane(self, lane_key: str) -> None:
        while True:
            with self._state_lock:
                queue = self._lane_queues.get(lane_key)
                if not queue:
                    self._active_lanes.discard(lane_key)
                    self._lane_queues.pop(lane_key, None)
                    self._progress.set()
                    return
                message = queue.popleft()
            delivered = self.handle_request(message)
            self._mark_message_done(int(message["seq"]), delivered=delivered)
            if self.should_stop:
                with self._state_lock:
                    if not self._lane_queues.get(lane_key):
                        self._active_lanes.discard(lane_key)
                        self._lane_queues.pop(lane_key, None)
                    self._progress.set()
                return

    def _submit_message(self, message: Dict[str, Any]) -> None:
        lane_key = self._lane_key(message)
        future: Optional[Future[None]] = None
        with self._state_lock:
            self._lane_queues.setdefault(lane_key, deque()).append(message)
            if lane_key not in self._active_lanes:
                self._active_lanes.add(lane_key)
                future = self.executor.submit(self._run_lane, lane_key)
                self._inflight.add(future)
            self._progress.set()
        if future is not None:
            future.add_done_callback(self._future_done)

    def _future_done(self, future: Future[None]) -> None:
        with self._state_lock:
            self._inflight.discard(future)
            self._progress.set()
        exc = future.exception()
        if exc is not None:
            print(
                f"[{self.client.agent_id}] request worker crashed: {exc}",
                file=sys.stderr,
            )

    def _request_shutdown(self, reason: str) -> None:
        if self.should_stop:
            return
        print(
            f"[{self.client.agent_id}] shutdown requested: {reason}",
            file=sys.stderr,
        )
        self.stop()

    def stop(self) -> None:
        self.should_stop = True
        self._progress.set()
        self._close_all_managed_clients()

    def _wait_for_inflight(self) -> None:
        while True:
            with self._state_lock:
                done = (
                    not self._inflight
                    and not self._active_lanes
                    and self._committed_seq >= self._polled_seq
                )
            if done or self.should_stop:
                return
            try:
                self.maybe_register()
            except HttpError as exc:
                if not self._handle_broker_unavailable(
                    "broker unavailable while waiting", exc
                ):
                    return
            self._progress.wait(timeout=1)
            self._progress.clear()

    def run(self) -> None:
        try:
            while not self.should_stop:
                try:
                    self.register()
                    break
                except HttpError as exc:
                    if not self._handle_broker_unavailable(
                        "broker unavailable during register", exc
                    ):
                        break
            while not self.should_stop:
                try:
                    self.maybe_register()
                    with self._state_lock:
                        after_seq = self._polled_seq
                    response = self.client.poll_messages(
                        after_seq=after_seq,
                        wait_seconds=self._next_poll_wait(),
                    )
                    messages = response.get("messages", [])
                    max_seq = max(
                        [after_seq] + [int(message["seq"]) for message in messages]
                    )
                    with self._state_lock:
                        self._polled_seq = max(self._polled_seq, max_seq)
                        for message in messages:
                            self._pending_message_seqs.add(int(message["seq"]))
                        self._progress.set()
                    for message in messages:
                        seq = int(message["seq"])
                        if message.get("kind") != "codex_request":
                            self._mark_message_done(seq, delivered=True)
                            continue
                        self._submit_message(message)
                    if self.run_once:
                        self._wait_for_inflight()
                        break
                except HttpError as exc:
                    if not self._handle_broker_unavailable(
                        "broker unavailable during poll", exc
                    ):
                        break
        finally:
            self.stop()
            self.executor.shutdown(wait=True)
            self._maintenance_thread.join(timeout=2)


def main() -> None:
    args = parse_args()
    daemon = EndpointDaemon(args)

    def stop_handler(signum: int, frame: Optional[Any]) -> None:
        del signum, frame
        daemon.stop()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    daemon.run()


if __name__ == "__main__":
    main()
