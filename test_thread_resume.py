import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from agent_endpoint import EndpointDaemon
from codex_rpc import CodexMcpClient


class CodexRpcFollowupValidationTests(unittest.TestCase):
    def test_run_codex_rejects_unsupported_followup_overrides(self) -> None:
        client = CodexMcpClient.__new__(CodexMcpClient)
        client.rpc = Mock()
        client.start = Mock()

        with self.assertRaises(ValueError) as exc_info:
            client.run_codex(
                "follow up",
                thread_id="thread-1",
                cwd="C:\\work",
                sandbox="danger-full-access",
            )

        self.assertEqual(
            str(exc_info.exception),
            "thread_id follow-up requests do not support: "
            "cwd, sandbox",
        )
        client.rpc.call.assert_not_called()


class EndpointDaemonFollowupResumeTests(unittest.TestCase):
    def _make_daemon(self) -> EndpointDaemon:
        daemon = EndpointDaemon.__new__(EndpointDaemon)
        daemon.default_cwd = None
        daemon.default_sandbox = "danger-full-access"
        daemon.default_approval_policy = "never"
        daemon.role_instructions = ""
        daemon.client = SimpleNamespace(agent_id="test-endpoint")
        daemon._normalize_request_cwd = Mock(side_effect=lambda value: value)
        daemon._sandbox_candidates = Mock(side_effect=lambda value: [value])
        daemon._borrow_session_client = Mock(side_effect=KeyError("thread-1"))
        daemon._load_or_create_new_client = Mock()
        daemon._bind_session_client = Mock()
        daemon._release_session_client = Mock()
        daemon._close_client = Mock()
        daemon._return_warm_client = Mock()
        daemon._send_response = Mock(return_value=True)
        daemon._send_error = Mock(return_value=False)
        return daemon

    def test_run_request_reuses_thread_with_new_client_when_session_missing(self) -> None:
        daemon = self._make_daemon()
        codex = Mock()
        codex.run_codex.return_value = {
            "thread_id": "thread-1",
            "content": "ok",
            "raw": {},
            "stderr_tail": [],
        }
        daemon._load_or_create_new_client.return_value = codex

        result = daemon._run_request(
            {"from_agent": "caller", "body": {}},
            "req-1",
            "follow up",
            "thread-1",
            {},
        )

        self.assertTrue(result)
        daemon._load_or_create_new_client.assert_called_once_with()
        codex.run_codex.assert_called_once_with(
            prompt="follow up",
            thread_id="thread-1",
            timeout_seconds=1800,
        )
        daemon._bind_session_client.assert_called_once_with("thread-1", codex)
        daemon._release_session_client.assert_called_once_with(
            thread_id="thread-1",
            codex=codex,
            keep=True,
        )
        daemon._send_error.assert_not_called()
        daemon._send_response.assert_called_once()

    def test_run_request_returns_validation_error_for_unsupported_followup_options(self) -> None:
        daemon = self._make_daemon()
        daemon._borrow_session_client.side_effect = KeyError("thread-1")
        codex = Mock()
        codex.run_codex.side_effect = ValueError(
            "thread_id follow-up requests do not support: sandbox"
        )
        daemon._load_or_create_new_client.return_value = codex

        result = daemon._run_request(
            {"from_agent": "caller", "body": {"sandbox": "danger-full-access"}},
            "req-2",
            "follow up",
            "thread-1",
            {"sandbox": "danger-full-access"},
        )

        self.assertFalse(result)
        daemon._close_client.assert_called_once_with(codex)
        daemon._send_response.assert_not_called()
        daemon._send_error.assert_called_once_with(
            {"from_agent": "caller", "body": {"sandbox": "danger-full-access"}},
            "req-2",
            "thread_id follow-up requests do not support: sandbox",
        )


if __name__ == "__main__":
    unittest.main()
