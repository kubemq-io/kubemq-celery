"""Tests for kubemq_celery.backend.KubeMQResultBackend."""

from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from celery import Celery
from kubemq.core.exceptions import KubeMQChannelError

from kubemq_celery.backend import KubeMQResultBackend

# ---------------------------------------------------------------------------
# Helper: create a backend with mocked KubeMQ client
# ---------------------------------------------------------------------------


def _make_backend(mock_client, result_expires=None):
    """Create a KubeMQResultBackend with a mocked QueuesClient."""
    app = Celery("test")
    config = {
        "result_backend": "kubemq://localhost:50000",
        "task_always_eager": False,
    }
    if result_expires is not None:
        config["result_expires"] = result_expires
    app.config_from_object(config)

    backend = KubeMQResultBackend(app=app, url="kubemq://localhost:50000")
    # Inject mock client into the cached_property slot
    backend.__dict__["_queues_client"] = mock_client
    return backend


class TestKubeMQResultBackend:
    """Tests for KubeMQResultBackend."""

    def test_store_result(self, mock_queues_client):
        """Verify backend stores result as QueueMessage on celery-result-{task_id}."""
        backend = _make_backend(mock_queues_client)

        backend._store_result(
            task_id="abc-123",
            result=42,
            state="SUCCESS",
        )

        # ack_all called first (purge old result)
        mock_queues_client.ack_all_queue_messages.assert_called_once_with(
            channel="celery-result-abc-123",
            wait_time_seconds=1,
        )

        # send_queue_message called with correct channel and body
        mock_queues_client.send_queue_message.assert_called_once()
        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]

        assert sent_msg.channel == "celery-result-abc-123"

        body = json.loads(sent_msg.body)
        assert body["task_id"] == "abc-123"
        assert body["status"] == "SUCCESS"
        assert body["result"] is not None  # encoded result
        assert "date_done" in body

    def test_get_result_peek(self, mock_queues_client):
        """Verify backend retrieves result via peek_queue_messages (non-destructive)."""
        meta = {
            "task_id": "abc-456",
            "status": "SUCCESS",
            "result": 99,
            "traceback": None,
            "children": [],
            "date_done": "2026-04-03T12:00:00+00:00",
            "group_id": None,
        }

        mock_peek_msg = MagicMock()
        mock_peek_msg.body = json.dumps(meta).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_peek_msg],
        )

        backend = _make_backend(mock_queues_client)
        result = backend._get_task_meta_for("abc-456")

        mock_queues_client.peek_queue_messages.assert_called_once_with(
            channel="celery-result-abc-456",
            max_messages=1,
            wait_timeout_in_seconds=1,
        )
        assert result["status"] == "SUCCESS"
        assert result["task_id"] == "abc-456"

    def test_result_expiry(self, mock_queues_client):
        """Verify result messages have correct expiration_in_seconds."""
        backend = _make_backend(mock_queues_client, result_expires=3600)

        backend._store_result(
            task_id="exp-1",
            result="ok",
            state="SUCCESS",
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 3600

    def test_result_expiry_capped_at_12h(self, mock_queues_client):
        """Verify result_expires > 43200 is capped."""
        backend = _make_backend(mock_queues_client, result_expires=100_000)

        backend._store_result(
            task_id="exp-cap-1",
            result="ok",
            state="SUCCESS",
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 43200

    def test_state_transition_purges_old(self, mock_queues_client):
        """Verify state update purges old message and writes new one."""
        backend = _make_backend(mock_queues_client)

        # First store (STARTED)
        backend._store_result(task_id="st-1", result=None, state="STARTED")
        # Second store (SUCCESS) -- should purge the STARTED message first
        backend._store_result(task_id="st-1", result=42, state="SUCCESS")

        # ack_all_queue_messages should have been called twice (once per store)
        assert mock_queues_client.ack_all_queue_messages.call_count == 2
        # send_queue_message should have been called twice
        assert mock_queues_client.send_queue_message.call_count == 2

        # Verify the last stored state is SUCCESS
        last_sent = mock_queues_client.send_queue_message.call_args_list[-1][0][0]
        body = json.loads(last_sent.body)
        assert body["status"] == "SUCCESS"

    def test_group_save_restore(self, mock_queues_client):
        """Verify _save_group and _restore_group via queue peek."""
        backend = _make_backend(mock_queues_client)

        group_data = {"group_id": "grp-1", "result": [1, 2, 3]}

        # Test _save_group
        backend._save_group("grp-1", [1, 2, 3])
        mock_queues_client.send_queue_message.assert_called_once()
        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.channel == "celery-group-grp-1"
        body = json.loads(sent_msg.body)
        assert body["group_id"] == "grp-1"
        assert body["result"] == [1, 2, 3]

        # Test _restore_group
        mock_peek_msg = MagicMock()
        mock_peek_msg.body = json.dumps(group_data).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_peek_msg],
        )

        restored = backend._restore_group("grp-1")
        assert restored == group_data
        mock_queues_client.peek_queue_messages.assert_called_once_with(
            channel="celery-group-grp-1",
            max_messages=1,
            wait_timeout_in_seconds=1,
        )

    def test_get_result_pending_on_missing(self, mock_queues_client):
        """Verify PENDING returned when channel doesn't exist."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQChannelError("channel not found")

        backend = _make_backend(mock_queues_client)
        result = backend._get_task_meta_for("missing-task-id")

        assert result["status"] == "PENDING"
        assert result["result"] is None

    def test_backend_registration(self):
        """Verify backend registered in BACKEND_ALIASES."""
        from celery.app.backends import BACKEND_ALIASES

        import kubemq_celery  # noqa: F401

        assert "kubemq" in BACKEND_ALIASES

    def test_store_result_expires_none_default(self, mock_queues_client):
        """Verify result_expires=None uses 24h default."""
        backend = _make_backend(mock_queues_client)
        # Force result_expires to None to hit the elif branch
        backend.app.conf.result_expires = None

        backend._store_result(
            task_id="exp-none-1",
            result="ok",
            state="SUCCESS",
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        # None -> 86400, capped at 43200
        assert sent_msg.expiration_in_seconds == 43200

    def test_store_result_ack_all_failure_ignored(self, mock_queues_client):
        """Verify _store_result handles ack_all_queue_messages failure gracefully."""
        mock_queues_client.ack_all_queue_messages.side_effect = RuntimeError("channel not found")
        backend = _make_backend(mock_queues_client)

        # Should not raise
        backend._store_result(
            task_id="ack-fail-1",
            result="ok",
            state="SUCCESS",
        )

        # send_queue_message should still be called
        mock_queues_client.send_queue_message.assert_called_once()

    def test_get_task_meta_propagates_unexpected_peek_error(self, mock_queues_client):
        """Verify _get_task_meta_for propagates non-KubeMQ peek failures (spec §5.3.2)."""
        mock_queues_client.peek_queue_messages.side_effect = RuntimeError("unexpected error")
        backend = _make_backend(mock_queues_client)

        with pytest.raises(RuntimeError, match="unexpected error"):
            backend._get_task_meta_for("err-task-1")

    def test_delete_group(self, mock_queues_client):
        """Verify _delete_group calls ack_all on the group channel."""
        backend = _make_backend(mock_queues_client)

        backend._delete_group("grp-del-1")

        mock_queues_client.ack_all_queue_messages.assert_called_once_with(
            channel="celery-group-grp-del-1",
            wait_time_seconds=1,
        )

    def test_delete_group_handles_error(self, mock_queues_client):
        """Verify _delete_group handles errors gracefully."""
        mock_queues_client.ack_all_queue_messages.side_effect = RuntimeError("not found")
        backend = _make_backend(mock_queues_client)

        backend._delete_group("grp-del-2")  # should not raise

    def test_save_group_expires_none_default(self, mock_queues_client):
        """Verify _save_group with result_expires=None uses 24h default (capped at 12h)."""
        backend = _make_backend(mock_queues_client)
        # Force result_expires to None to hit the elif branch
        backend.app.conf.result_expires = None

        backend._save_group("grp-exp-1", [1, 2])

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        # None -> 86400, capped at 43200
        assert sent_msg.expiration_in_seconds == 43200

    def test_save_group_expires_timedelta(self, mock_queues_client):
        """Verify _save_group with timedelta result_expires."""
        backend = _make_backend(mock_queues_client, result_expires=timedelta(hours=1))

        backend._save_group("grp-exp-2", [3, 4])

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 3600

    def test_save_group_expires_int(self, mock_queues_client):
        """Verify _save_group with integer result_expires."""
        backend = _make_backend(mock_queues_client, result_expires=7200)

        backend._save_group("grp-exp-3", [5, 6])

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 7200

    def test_restore_group_returns_none_on_error(self, mock_queues_client):
        """Verify _restore_group returns None on exception."""
        mock_queues_client.peek_queue_messages.side_effect = RuntimeError("error")
        backend = _make_backend(mock_queues_client)

        result = backend._restore_group("grp-err-1")
        assert result is None

    def test_restore_group_returns_none_when_empty(self, mock_queues_client):
        """Verify _restore_group returns None when no messages."""
        mock_queues_client.peek_queue_messages.return_value = MagicMock(messages=[])
        backend = _make_backend(mock_queues_client)

        result = backend._restore_group("grp-empty-1")
        assert result is None


class TestKubeMQResultBackendClientCreation:
    """Tests for KubeMQResultBackend._queues_client cached_property."""

    @patch("kubemq_celery.backend.QueuesClient")
    def test_queues_client_creation_basic(self, MockQueuesClient):
        """Verify _queues_client creates QueuesClient from URL."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://localhost:50000",
                "task_always_eager": False,
            }
        )

        backend = KubeMQResultBackend(app=app, url="kubemq://myhost:50001")
        _ = backend._queues_client

        MockQueuesClient.assert_called_once()
        config = MockQueuesClient.call_args[1]["config"]
        assert config.address == "myhost:50001"

    @patch("kubemq_celery.backend.QueuesClient")
    def test_queues_client_creation_with_auth(self, MockQueuesClient):
        """Verify _queues_client extracts auth token from URL."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://:mytoken@localhost:50000",
                "task_always_eager": False,
            }
        )

        backend = KubeMQResultBackend(app=app, url="kubemq://:mytoken@myhost:50001")
        _ = backend._queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.auth_token == "mytoken"

    @patch("kubemq_celery.backend.QueuesClient")
    def test_queues_client_creation_with_tls(self, MockQueuesClient):
        """Verify _queues_client sets TLS from kubemq+tls:// URL."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq+tls://localhost:50000",
                "task_always_eager": False,
            }
        )

        backend = KubeMQResultBackend(app=app, url="kubemq+tls://myhost:50001")
        _ = backend._queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.tls.enabled is True

    @patch("kubemq_celery.backend.TLSConfig")
    @patch("kubemq_celery.backend.QueuesClient")
    def test_queues_client_creation_with_transport_options(self, MockQueuesClient, MockTLSConfig):
        """Verify _queues_client uses result_backend_transport_options."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://localhost:50000",
                "task_always_eager": False,
                "result_backend_transport_options": {
                    "auth_token": "opts-token",
                    "tls_enabled": True,
                    "tls_cert_file": "/cert.pem",
                    "tls_key_file": "/key.pem",
                    "tls_ca_file": "/ca.pem",
                },
            }
        )

        backend = KubeMQResultBackend(app=app, url="kubemq://localhost:50000")
        _ = backend._queues_client

        MockTLSConfig.assert_called_once_with(
            enabled=True,
            cert_file="/cert.pem",
            key_file="/key.pem",
            ca_file="/ca.pem",
        )
