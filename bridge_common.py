from __future__ import annotations

import http.client
import json
import os
import platform
import re
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


AGENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
DEFAULT_PROTOCOL_VERSION = "2025-03-26"
STATE_WRITE_RETRIES = 6
STATE_WRITE_RETRY_DELAY_SECONDS = 0.05


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_agent_id(agent_id: str) -> str:
    if not AGENT_ID_PATTERN.match(agent_id):
        raise ValueError(
            "agent_id must match /^[A-Za-z0-9_.-]{1,64}$/"
        )
    return agent_id


def new_request_id() -> str:
    return uuid.uuid4().hex


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _preserve_corrupt_json_file(
    path: Path,
    raw_bytes: bytes,
    expected_file_id: Optional[tuple[int, int]],
) -> str:
    preserved_path = path.with_name(
        f"{path.name}.corrupt-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    )
    rename_error: Optional[OSError] = None
    try:
        current_stat = path.stat()
    except FileNotFoundError:
        current_file_id = None
    else:
        current_file_id = (current_stat.st_dev, current_stat.st_ino)
    if expected_file_id is not None and current_file_id == expected_file_id:
        try:
            os.replace(path, preserved_path)
            return f"; preserved at {preserved_path}"
        except OSError as exc:
            rename_error = exc
    try:
        with preserved_path.open("wb") as handle:
            handle.write(raw_bytes)
        return f"; copied to {preserved_path}"
    except OSError as exc:
        if rename_error is not None:
            return (
                "; failed to preserve corrupt contents: "
                f"rename={rename_error}; copy={exc}"
            )
        return f"; failed to preserve corrupt contents: {exc}"


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw_bytes = b""
    expected_file_id: Optional[tuple[int, int]] = None
    try:
        with path.open("rb") as handle:
            file_stat = os.fstat(handle.fileno())
            expected_file_id = (file_stat.st_dev, file_stat.st_ino)
            raw_bytes = handle.read()
        return json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(
            "[state] ignoring corrupt JSON in "
            f"{path}: {exc}"
            f"{_preserve_corrupt_json_file(path, raw_bytes, expected_file_id)}",
            file=sys.stderr,
        )
        return default


def write_json_file(path: Path, payload: Any) -> None:
    ensure_parent_dir(path)
    last_error: Optional[BaseException] = None
    for attempt in range(STATE_WRITE_RETRIES):
        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_name, path)
            return
        except PermissionError as exc:
            last_error = exc
            if attempt + 1 >= STATE_WRITE_RETRIES:
                raise
            time.sleep(STATE_WRITE_RETRY_DELAY_SECONDS * (attempt + 1))
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
    if last_error is not None:
        raise last_error


def compact_json(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def normalize_text(text: Optional[str]) -> str:
    return (text or "").strip()


def summarize_text(text: str, limit: int = 240) -> str:
    clean = " ".join(text.split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 3] + "..."


def extract_text_from_mcp_content(content: Iterable[Dict[str, Any]]) -> str:
    chunks = []
    for item in content:
        if item.get("type") == "text":
            chunks.append(item.get("text", ""))
    return "".join(chunks).strip()


class HttpError(RuntimeError):
    pass


class BrokerClient:
    def __init__(
        self,
        broker_url: str,
        token: str,
        agent_id: str,
        state_path: Optional[Path] = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.broker_url = broker_url.rstrip("/")
        self.token = token
        self.agent_id = validate_agent_id(agent_id)
        self.state_path = state_path
        self.timeout_seconds = timeout_seconds
        self._state_cache: Optional[Dict[str, Any]] = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "codex-lan-bridge/1.0",
        }

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        query_string = ""
        if query:
            query_string = "?" + urllib.parse.urlencode(query)
        url = f"{self.broker_url}{path}{query_string}"
        body = None
        if payload is not None:
            body = compact_json(payload).encode("utf-8")
        request = urllib.request.Request(
            url=url,
            data=body,
            headers=self._headers(),
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request, timeout=timeout_seconds or self.timeout_seconds
            ) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise HttpError(f"{method} {url} failed: {exc.code} {detail}") from exc
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            OSError,
        ) as exc:
            reason = getattr(exc, "reason", exc)
            raise HttpError(f"{method} {url} failed: {reason}") from exc

    def register(self, role: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {
            "agent_id": self.agent_id,
            "role": role,
            "host": socket.gethostname(),
            "platform": platform.platform(),
            "metadata": metadata or {},
            "seen_at": utc_now(),
        }
        return self._request("POST", "/v1/register", payload=payload)

    def list_agents(self) -> Dict[str, Any]:
        return self._request("GET", "/v1/agents")

    def send_message(
        self,
        to_agent: str,
        kind: str,
        body: Dict[str, Any],
        conversation_id: Optional[str] = None,
        reply_to_seq: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload = {
            "from_agent": self.agent_id,
            "to_agent": validate_agent_id(to_agent),
            "kind": kind,
            "body": body,
            "conversation_id": conversation_id or body.get("request_id") or new_request_id(),
            "reply_to_seq": reply_to_seq,
        }
        return self._request("POST", "/v1/messages", payload=payload)

    def poll_messages(
        self,
        after_seq: int,
        wait_seconds: int = 25,
        limit: int = 50,
    ) -> Dict[str, Any]:
        query = {
            "agent_id": self.agent_id,
            "after_seq": after_seq,
            "wait_seconds": max(0, wait_seconds),
            "limit": max(1, min(limit, 200)),
        }
        return self._request(
            "GET",
            "/v1/messages",
            query=query,
            timeout_seconds=max(self.timeout_seconds, wait_seconds + 5),
        )

    def get_conversation(self, conversation_id: str, limit: int = 100) -> Dict[str, Any]:
        query = {
            "conversation_id": conversation_id,
            "limit": max(1, min(limit, 500)),
        }
        return self._request("GET", "/v1/conversations", query=query)

    def _load_state(self) -> Dict[str, Any]:
        if self.state_path is None:
            return {"last_seq": 0}
        if self._state_cache is None:
            self._state_cache = load_json_file(self.state_path, {"last_seq": 0})
        return self._state_cache

    def get_last_seq(self) -> int:
        return int(self._load_state().get("last_seq", 0))

    def set_last_seq(self, last_seq: int) -> None:
        if self.state_path is None:
            return
        state = self._load_state()
        state["last_seq"] = int(last_seq)
        write_json_file(self.state_path, state)
