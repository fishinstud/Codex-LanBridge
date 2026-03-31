import types
import threading
import unittest
from unittest.mock import ANY, Mock, patch

from agent_endpoint import EndpointDaemon
from bridge_common import HttpError
from broker_mcp_server import BrokerMcpServer
from codex_rpc import CodexMcpClient, JsonRpcError


class CodexRpcRegressionTests(unittest.TestCase):
    def test_run_codex_rejects_resume_overrides_before_start(self) -> None:
        client = CodexMcpClient.__new__(CodexMcpClient)
        client.start = Mock()

        with self.assertRaisesRegex(
            ValueError,
            "thread_id follow-up requests do not support: sandbox",
        ):
            client.run_codex(
                prompt="follow up",
                thread_id="thread-123",
                sandbox="danger-full-access",
            )

        client.start.assert_not_called()


class AgentEndpointRegressionTests(unittest.TestCase):
    def _make_daemon(self) -> EndpointDaemon:
        daemon = EndpointDaemon.__new__(EndpointDaemon)
        daemon.client = types.SimpleNamespace(agent_id="endpoint-agent")
        daemon.default_cwd = None
        daemon.default_sandbox = "workspace-write"
        daemon.default_approval_policy = "never"
        daemon.role_instructions = ""
        daemon._normalize_request_cwd = Mock(side_effect=lambda cwd: cwd)
        daemon._sandbox_candidates = Mock(side_effect=lambda sandbox: [sandbox or "workspace-write"])
        daemon._borrow_session_client = Mock()
        daemon._load_or_create_new_client = Mock()
        daemon._bind_session_client = Mock()
        daemon._release_session_client = Mock()
        daemon._close_client = Mock()
        daemon._forward_codex_event = Mock(return_value=True)
        daemon._send_response = Mock(return_value=True)
        daemon._send_error = Mock(return_value=False)
        return daemon

    def test_run_request_rehydrates_missing_session_for_thread(self) -> None:
        daemon = self._make_daemon()
        daemon._borrow_session_client.side_effect = KeyError("thread-123")
        codex = Mock()
        codex.run_codex.return_value = {
            "thread_id": "thread-123",
            "content": "done",
            "stderr_tail": [],
        }
        daemon._load_or_create_new_client.return_value = codex

        result = daemon._run_request(
            {"from_agent": "peer"},
            "req-1",
            "follow up",
            "thread-123",
            {"thread_id": "thread-123"},
        )

        self.assertTrue(result)
        codex.run_codex.assert_called_once_with(
            prompt="follow up",
            thread_id="thread-123",
            timeout_seconds=1800,
            event_handler=ANY,
        )
        daemon._bind_session_client.assert_called_once_with("thread-123", codex)
        daemon._release_session_client.assert_called_once_with(
            thread_id="thread-123",
            codex=codex,
            keep=True,
        )
        daemon._send_error.assert_not_called()

    def test_run_request_rejects_explicit_resume_overrides(self) -> None:
        daemon = self._make_daemon()
        daemon._borrow_session_client.side_effect = KeyError("thread-123")
        codex = Mock()
        codex.run_codex.side_effect = JsonRpcError(
            "thread_id follow-up requests do not support: sandbox"
        )
        daemon._load_or_create_new_client.return_value = codex

        result = daemon._run_request(
            {"from_agent": "peer"},
            "req-2",
            "follow up",
            "thread-123",
            {
                "thread_id": "thread-123",
                "sandbox": "danger-full-access",
            },
        )

        self.assertFalse(result)
        daemon._borrow_session_client.assert_called_once_with("thread-123")
        daemon._load_or_create_new_client.assert_called_once_with()
        daemon._close_client.assert_called_once_with(codex)
        daemon._send_error.assert_called_once_with(
            {"from_agent": "peer"},
            "req-2",
            "thread_id follow-up requests do not support: sandbox",
        )

    def test_run_request_forwards_incremental_codex_events(self) -> None:
        daemon = self._make_daemon()
        daemon._borrow_session_client.side_effect = KeyError("thread-123")
        codex = Mock()

        def _run_codex(**kwargs):
            kwargs["event_handler"](
                {
                    "params": {
                        "_meta": {"threadId": "thread-123"},
                        "msg": {
                            "type": "agent_message_content_delta",
                            "delta": "hel",
                            "item_id": "msg-1",
                        },
                    }
                }
            )
            return {
                "thread_id": "thread-123",
                "content": "hello",
                "stderr_tail": [],
            }

        codex.run_codex.side_effect = _run_codex
        daemon._load_or_create_new_client.return_value = codex

        result = daemon._run_request(
            {"from_agent": "peer", "conversation_id": "conv-1", "seq": 10},
            "req-3",
            "follow up",
            "thread-123",
            {"thread_id": "thread-123"},
        )

        self.assertTrue(result)
        daemon._forward_codex_event.assert_called_once()
        forwarded_args = daemon._forward_codex_event.call_args.args
        self.assertEqual(forwarded_args[1], "req-3")

    def test_broker_unavailable_retries_when_not_run_once(self) -> None:
        daemon = self._make_daemon()
        daemon.run_once = False
        daemon.should_stop = False
        daemon.register_retry_seconds = 2
        daemon._request_shutdown = Mock()

        with patch("agent_endpoint.time.sleep") as sleep_mock:
            result = daemon._handle_broker_unavailable(
                "broker unavailable during register",
                HttpError("connection refused"),
            )

        self.assertTrue(result)
        self.assertEqual(sleep_mock.call_count, 2)
        daemon._request_shutdown.assert_not_called()

    def test_broker_unavailable_stops_run_once_mode(self) -> None:
        daemon = self._make_daemon()
        daemon.run_once = True
        daemon.should_stop = False
        daemon.register_retry_seconds = 2
        daemon._request_shutdown = Mock()

        result = daemon._handle_broker_unavailable(
            "broker unavailable during register",
            HttpError("connection refused"),
        )

        self.assertFalse(result)
        daemon._request_shutdown.assert_called_once()


class BrokerMcpServerRegressionTests(unittest.TestCase):
    def _make_server(self) -> BrokerMcpServer:
        server = BrokerMcpServer.__new__(BrokerMcpServer)
        server.client = Mock()
        server.emit = Mock()
        server._log = Mock()
        server.running = True
        server.pending_condition = threading.Condition(threading.RLock())
        server.pending_by_request_id = {}
        server._persist_pending_messages_locked = Mock()
        return server

    def _structured_result(self, server: BrokerMcpServer) -> dict:
        return server.emit.call_args.args[0]["result"]["structuredContent"]

    def test_sleep_before_retry_waits_without_requesting_shutdown(self) -> None:
        server = self._make_server()
        server.running = True
        server.reconnect_retry_seconds = 2

        with patch("broker_mcp_server.time.sleep") as sleep_mock:
            result = server._sleep_before_retry(
                "heartbeat/register failed",
                HttpError("connection refused"),
            )

        self.assertTrue(result)
        self.assertEqual(sleep_mock.call_count, 2)
        self.assertGreaterEqual(server._log.call_count, 2)

    @patch("broker_mcp_server.new_request_id", return_value="req-123")
    def test_remote_codex_start_returns_submission_metadata(
        self, _: Mock
    ) -> None:
        server = self._make_server()
        server.client.send_message.return_value = {
            "seq": 7,
            "created_at": "2026-03-27T10:00:00Z",
        }

        server.handle_remote_codex_start(
            "call-1",
            {"peer_agent": "ubuntu-codex", "prompt": "run diagnostics"},
        )

        structured = self._structured_result(server)
        self.assertEqual(structured["requestId"], "req-123")
        self.assertEqual(structured["conversationId"], "req-123")
        self.assertEqual(structured["peerAgent"], "ubuntu-codex")
        self.assertEqual(structured["submittedSeq"], 7)
        server.client.send_message.assert_called_once()

    def test_remote_codex_status_reports_completed_response(self) -> None:
        server = self._make_server()
        server.client.get_conversation.return_value = {
            "messages": [
                {
                    "seq": 11,
                    "conversation_id": "req-123",
                    "from_agent": "windows-ui",
                    "to_agent": "ubuntu-codex",
                    "kind": "codex_request",
                    "created_at": "2026-03-27T10:00:00Z",
                    "body": {"request_id": "req-123", "prompt": "run diagnostics"},
                    "reply_to_seq": None,
                },
                {
                    "seq": 12,
                    "conversation_id": "req-123",
                    "from_agent": "ubuntu-codex",
                    "to_agent": "windows-ui",
                    "kind": "codex_response",
                    "created_at": "2026-03-27T10:00:05Z",
                    "body": {
                        "ok": True,
                        "request_id": "req-123",
                        "thread_id": "thread-9",
                        "content": "finished",
                        "stderr_tail": [],
                        "effective_cwd": "/workspace",
                        "effective_sandbox": "workspace-write",
                    },
                    "reply_to_seq": 11,
                },
            ]
        }

        server.handle_remote_codex_status("call-2", {"request_id": "req-123"})

        structured = self._structured_result(server)
        self.assertEqual(structured["status"], "completed")
        self.assertTrue(structured["done"])
        self.assertEqual(structured["lastSeq"], 12)
        self.assertEqual(structured["response"]["threadId"], "thread-9")
        self.assertEqual(structured["response"]["content"], "finished")

    def test_remote_codex_status_surfaces_latest_progress_event(self) -> None:
        server = self._make_server()
        server.client.get_conversation.return_value = {
            "messages": [
                {
                    "seq": 31,
                    "conversation_id": "req-123",
                    "from_agent": "windows-ui",
                    "to_agent": "ubuntu-codex",
                    "kind": "codex_request",
                    "created_at": "2026-03-27T10:00:00Z",
                    "body": {"request_id": "req-123", "prompt": "run diagnostics"},
                    "reply_to_seq": None,
                },
                {
                    "seq": 32,
                    "conversation_id": "req-123",
                    "from_agent": "ubuntu-codex",
                    "to_agent": "windows-ui",
                    "kind": "codex_event",
                    "created_at": "2026-03-27T10:00:02Z",
                    "body": {
                        "ok": True,
                        "request_id": "req-123",
                        "type": "agent_message_content_delta",
                        "delta": "hel",
                        "thread_id": "thread-9",
                    },
                    "reply_to_seq": 31,
                },
            ]
        }

        server.handle_remote_codex_status("call-2b", {"request_id": "req-123"})

        structured = self._structured_result(server)
        self.assertEqual(structured["status"], "pending")
        self.assertFalse(structured["done"])
        self.assertEqual(structured["latestMessageKind"], "codex_event")
        self.assertEqual(structured["latestEvent"]["type"], "agent_message_content_delta")
        self.assertEqual(structured["latestEvent"]["delta"], "hel")

    def test_remote_codex_wait_returns_only_new_messages_after_cursor(self) -> None:
        server = self._make_server()
        server.client.get_conversation.side_effect = [
            {
                "messages": [
                    {
                        "seq": 21,
                        "conversation_id": "req-123",
                        "from_agent": "windows-ui",
                        "to_agent": "ubuntu-codex",
                        "kind": "codex_request",
                        "created_at": "2026-03-27T10:00:00Z",
                        "body": {"request_id": "req-123", "prompt": "run diagnostics"},
                        "reply_to_seq": None,
                    }
                ]
            },
            {
                "messages": [
                    {
                        "seq": 21,
                        "conversation_id": "req-123",
                        "from_agent": "windows-ui",
                        "to_agent": "ubuntu-codex",
                        "kind": "codex_request",
                        "created_at": "2026-03-27T10:00:00Z",
                        "body": {"request_id": "req-123", "prompt": "run diagnostics"},
                        "reply_to_seq": None,
                    },
                    {
                        "seq": 22,
                        "conversation_id": "req-123",
                        "from_agent": "ubuntu-codex",
                        "to_agent": "windows-ui",
                        "kind": "codex_response",
                        "created_at": "2026-03-27T10:00:05Z",
                        "body": {
                            "ok": True,
                            "request_id": "req-123",
                            "thread_id": "thread-9",
                            "content": "finished",
                            "stderr_tail": [],
                            "effective_cwd": "/workspace",
                            "effective_sandbox": "workspace-write",
                        },
                        "reply_to_seq": 21,
                    },
                ]
            },
        ]

        with patch("broker_mcp_server.time.sleep") as sleep_mock:
            server.handle_remote_codex_wait(
                "call-3",
                {"conversation_id": "req-123", "after_seq": 21, "wait_seconds": 5},
            )

        structured = self._structured_result(server)
        self.assertEqual(structured["status"], "completed")
        self.assertEqual(structured["newMessageCount"], 1)
        self.assertEqual(structured["messagesAfter"][0]["seq"], 22)
        self.assertEqual(sleep_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
