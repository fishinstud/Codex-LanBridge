from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from bridge_common import utc_now, validate_agent_id


AGENT_ALIVE_SECONDS = 90
STALE_AGENT_RETENTION_SECONDS = 15 * 60
MESSAGE_RETENTION_SECONDS = 7 * 24 * 60 * 60
CLEANUP_INTERVAL_SECONDS = 5 * 60
SQLITE_BUSY_TIMEOUT_MS = 5000
MAX_POLL_WAIT_SECONDS = 60
MAX_POLL_LIMIT = 500
MAX_CONVERSATION_LIMIT = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local-network broker for Codex agents.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="broker.sqlite3")
    parser.add_argument("--token", required=True, help="Shared bearer token for all clients.")
    return parser.parse_args()


class BrokerStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.connection = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
        self.connection.row_factory = sqlite3.Row
        self._last_cleanup_at = 0.0
        self._configure_connection()
        self._init_schema()

    def _configure_connection(self) -> None:
        self.connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA wal_autocheckpoint = 1000")
        self.connection.execute("PRAGMA temp_store = MEMORY")

    def _init_schema(self) -> None:
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    host TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL,
                    from_agent TEXT NOT NULL,
                    to_agent TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    body_json TEXT NOT NULL,
                    reply_to_seq INTEGER
                )
                """
            )
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_target_seq
                ON messages (to_agent, seq)
                """
            )
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_conversation_seq
                ON messages (conversation_id, seq)
                """
            )
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agents_last_seen
                ON agents (last_seen)
                """
            )
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_created_at
                ON messages (created_at)
                """
            )

    def _maybe_cleanup(self) -> None:
        now = time.monotonic()
        if now - self._last_cleanup_at < CLEANUP_INTERVAL_SECONDS:
            return
        self._last_cleanup_at = now
        agents_before = (
            datetime.now(timezone.utc) - timedelta(seconds=STALE_AGENT_RETENTION_SECONDS)
        )
        messages_before = (
            datetime.now(timezone.utc) - timedelta(seconds=MESSAGE_RETENTION_SECONDS)
        )
        self.connection.execute(
            "DELETE FROM agents WHERE last_seen < ?",
            (agents_before.isoformat().replace("+00:00", "Z"),),
        )
        self.connection.execute(
            "DELETE FROM messages WHERE created_at < ?",
            (messages_before.isoformat().replace("+00:00", "Z"),),
        )

    def register_agent(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        agent_id = validate_agent_id(payload["agent_id"])
        role = str(payload["role"])
        host = str(payload["host"])
        platform_name = str(payload["platform"])
        metadata_json = json.dumps(payload.get("metadata", {}), sort_keys=True)
        last_seen = payload.get("seen_at") or utc_now()
        with self.lock, self.connection:
            self.connection.execute(
                """
                INSERT INTO agents (agent_id, role, host, platform, metadata_json, last_seen)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    role = excluded.role,
                    host = excluded.host,
                    platform = excluded.platform,
                    metadata_json = excluded.metadata_json,
                    last_seen = excluded.last_seen
                """,
                (agent_id, role, host, platform_name, metadata_json, last_seen),
            )
            self._maybe_cleanup()
        return {"ok": True, "agent_id": agent_id, "seen_at": last_seen}

    def list_agents(self) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT agent_id, role, host, platform, metadata_json, last_seen
                FROM agents
                ORDER BY agent_id
                """
            ).fetchall()
        now = time.time()
        result = []
        for row in rows:
            last_seen = row["last_seen"]
            try:
                parsed = last_seen.replace("Z", "+00:00")
                age_seconds = max(
                    0.0,
                    now - __import__("datetime").datetime.fromisoformat(parsed).timestamp(),
                )
            except Exception:
                age_seconds = 999999.0
            result.append(
                {
                    "agent_id": row["agent_id"],
                    "role": row["role"],
                    "host": row["host"],
                    "platform": row["platform"],
                    "metadata": json.loads(row["metadata_json"]),
                    "last_seen": last_seen,
                    "alive": age_seconds <= AGENT_ALIVE_SECONDS,
                    "age_seconds": round(age_seconds, 1),
                }
            )
        return result

    def add_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        conversation_id = str(payload["conversation_id"])
        from_agent = validate_agent_id(payload["from_agent"])
        to_agent = validate_agent_id(payload["to_agent"])
        kind = str(payload["kind"])
        body_json = json.dumps(payload.get("body", {}), sort_keys=True)
        reply_to_seq = payload.get("reply_to_seq")
        created_at = utc_now()
        with self.condition, self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO messages (
                    conversation_id,
                    from_agent,
                    to_agent,
                    kind,
                    created_at,
                    body_json,
                    reply_to_seq
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    from_agent,
                    to_agent,
                    kind,
                    created_at,
                    body_json,
                    reply_to_seq,
                ),
            )
            seq = int(cursor.lastrowid)
            self._maybe_cleanup()
            self.condition.notify_all()
        return {"ok": True, "seq": seq, "created_at": created_at}

    def _fetch_messages(self, agent_id: str, after_seq: int, limit: int) -> List[Dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT seq, conversation_id, from_agent, to_agent, kind, created_at, body_json, reply_to_seq
            FROM messages
            WHERE to_agent = ? AND seq > ?
            ORDER BY seq ASC
            LIMIT ?
            """,
            (agent_id, after_seq, limit),
        ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def poll_messages(
        self, agent_id: str, after_seq: int, wait_seconds: int, limit: int
    ) -> List[Dict[str, Any]]:
        deadline = time.monotonic() + max(0, wait_seconds)
        with self.condition:
            while True:
                messages = self._fetch_messages(agent_id, after_seq, limit)
                if messages:
                    return messages
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                self.condition.wait(timeout=remaining)

    def get_conversation(self, conversation_id: str, limit: int) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT seq, conversation_id, from_agent, to_agent, kind, created_at, body_json, reply_to_seq
                FROM messages
                WHERE conversation_id = ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (conversation_id, limit),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "seq": row["seq"],
            "conversation_id": row["conversation_id"],
            "from_agent": row["from_agent"],
            "to_agent": row["to_agent"],
            "kind": row["kind"],
            "created_at": row["created_at"],
            "body": json.loads(row["body_json"]),
            "reply_to_seq": row["reply_to_seq"],
        }


class BrokerHandler(BaseHTTPRequestHandler):
    server_version = "CodexLanBroker/1.0"

    @property
    def broker(self) -> BrokerStore:
        return self.server.broker  # type: ignore[attr-defined]

    @property
    def shared_token(self) -> str:
        return self.server.shared_token  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        print(
            "%s - - [%s] %s"
            % (self.address_string(), self.log_date_time_string(), format % args)
        )

    def _require_auth(self) -> bool:
        header = self.headers.get("Authorization", "")
        expected = f"Bearer {self.shared_token}"
        if header == expected:
            return True
        self._send_json(
            HTTPStatus.UNAUTHORIZED,
            {"ok": False, "error": "missing or invalid bearer token"},
        )
        return False

    def _read_json(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length < 0:
            raise ValueError("invalid Content-Length")
        if content_length > 5 * 1024 * 1024:
            raise ValueError("request body too large")
        raw = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
        return json.loads(raw)

    @staticmethod
    def _parse_bounded_int(
        raw_value: str,
        *,
        field: str,
        minimum: int,
        maximum: int,
    ) -> int:
        try:
            parsed = int(raw_value)
        except ValueError as exc:
            raise ValueError(f"{field} must be an integer") from exc
        if parsed < minimum or parsed > maximum:
            raise ValueError(
                f"{field} must be between {minimum} and {maximum}"
            )
        return parsed

    def _send_json(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(HTTPStatus.OK, {"ok": True, "time": utc_now()})
            return
        if not self._require_auth():
            return
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/v1/agents":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "agents": self.broker.list_agents()},
                )
                return
            if parsed.path == "/v1/messages":
                agent_id = validate_agent_id(query.get("agent_id", [""])[0])
                after_seq = self._parse_bounded_int(
                    query.get("after_seq", ["0"])[0],
                    field="after_seq",
                    minimum=0,
                    maximum=2_147_483_647,
                )
                wait_seconds = self._parse_bounded_int(
                    query.get("wait_seconds", ["0"])[0],
                    field="wait_seconds",
                    minimum=0,
                    maximum=MAX_POLL_WAIT_SECONDS,
                )
                limit = self._parse_bounded_int(
                    query.get("limit", ["50"])[0],
                    field="limit",
                    minimum=1,
                    maximum=MAX_POLL_LIMIT,
                )
                messages = self.broker.poll_messages(
                    agent_id, after_seq, wait_seconds, limit
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "messages": messages,
                        "last_seq": max([after_seq] + [m["seq"] for m in messages]),
                    },
                )
                return
            if parsed.path == "/v1/conversations":
                conversation_id = query.get("conversation_id", [""])[0]
                if not conversation_id:
                    raise ValueError("conversation_id is required")
                limit = self._parse_bounded_int(
                    query.get("limit", ["100"])[0],
                    field="limit",
                    minimum=1,
                    maximum=MAX_CONVERSATION_LIMIT,
                )
                messages = self.broker.get_conversation(conversation_id, limit)
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "messages": messages},
                )
                return
        except (ValueError, KeyError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return
        except Exception as exc:
            print(
                f"Broker GET {parsed.path} failed: {exc}",
                file=sys.stderr,
            )
            traceback.print_exc()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": "internal broker error"},
            )
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not self._require_auth():
            return
        try:
            payload = self._read_json()
            if parsed.path == "/v1/register":
                result = self.broker.register_agent(payload)
                self._send_json(HTTPStatus.OK, result)
                return
            if parsed.path == "/v1/messages":
                result = self.broker.add_message(payload)
                self._send_json(HTTPStatus.OK, result)
                return
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return
        except Exception as exc:
            print(
                f"Broker POST {parsed.path} failed: {exc}",
                file=sys.stderr,
            )
            traceback.print_exc()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": "internal broker error"},
            )
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})


def main() -> None:
    args = parse_args()
    broker = BrokerStore(Path(args.db).resolve())
    server = ThreadingHTTPServer((args.host, args.port), BrokerHandler)
    server.daemon_threads = True
    server.broker = broker  # type: ignore[attr-defined]
    server.shared_token = args.token  # type: ignore[attr-defined]
    print(f"Broker listening on http://{args.host}:{args.port}")
    print(f"SQLite DB: {Path(args.db).resolve()}")
    server.serve_forever()


if __name__ == "__main__":
    main()
