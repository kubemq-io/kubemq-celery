"""Tests for kubemq_celery.backend.KubeMQResultBackend."""

from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from celery import Celery
from kubemq.core.exceptions import ErrorCode, KubeMQChannelError, KubeMQTimeoutError

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

    def test_result_expiry_capped_at_24h(self, mock_queues_client):
        """Verify result_expires > 43200 is capped."""
        backend = _make_backend(mock_queues_client, result_expires=100_000)

        backend._store_result(
            task_id="exp-cap-1",
            result="ok",
            state="SUCCESS",
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 86400

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
        """Verify _save_group and _restore_group via queue peek.

        _save_group uses backend.encode() to serialize the result,
        and _restore_group uses backend.decode() to deserialize it.
        """
        backend = _make_backend(mock_queues_client)

        # Test _save_group -- encode() produces a JSON string
        backend._save_group("grp-1", [1, 2, 3])
        mock_queues_client.send_queue_message.assert_called_once()
        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.channel == "celery-group-grp-1"
        body = json.loads(sent_msg.body)
        assert body["group_id"] == "grp-1"
        encoded_result = backend.encode([1, 2, 3])
        assert body["result"] == encoded_result

        # Test _restore_group -- decode() reverses encode()
        stored_data = {"group_id": "grp-1", "result": encoded_result}
        mock_peek_msg = MagicMock()
        mock_peek_msg.body = json.dumps(stored_data).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_peek_msg],
        )

        restored = backend._restore_group("grp-1")
        assert restored == [1, 2, 3]
        mock_queues_client.peek_queue_messages.assert_called_once_with(
            channel="celery-group-grp-1",
            max_messages=1,
            wait_timeout_in_seconds=1,
        )

    def test_get_result_pending_on_missing(self, mock_queues_client):
        """Verify PENDING returned when channel doesn't exist (NOT_FOUND code)."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQChannelError(
            "channel not found", code=ErrorCode.NOT_FOUND
        )

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
        # None -> 86400 (24h default, within KubeMQ max)
        assert sent_msg.expiration_in_seconds == 86400

    def test_store_result_ack_all_failure_ignored(self, mock_queues_client):
        """Verify _store_result handles ack_all_queue_messages failure gracefully.

        C3: The backend now catches specific KubeMQ exceptions (not broad Exception).
        """
        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQChannelError(
            "channel not found"
        )
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
        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQChannelError("not found")
        backend = _make_backend(mock_queues_client)

        backend._delete_group("grp-del-2")  # should not raise

    def test_save_group_expires_none_default(self, mock_queues_client):
        """Verify _save_group with result_expires=None uses 24h default."""
        backend = _make_backend(mock_queues_client)
        # Force result_expires to None to hit the elif branch
        backend.app.conf.result_expires = None

        backend._save_group("grp-exp-1", [1, 2])

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        # None -> 86400 (24h default, within KubeMQ max)
        assert sent_msg.expiration_in_seconds == 86400

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

    def test_restore_group_returns_none_on_not_found(self, mock_queues_client):
        """Verify _restore_group returns None on NOT_FOUND."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQChannelError(
            "not found", code=ErrorCode.NOT_FOUND
        )
        backend = _make_backend(mock_queues_client)

        result = backend._restore_group("grp-err-1")
        assert result is None

    def test_restore_group_propagates_non_not_found_error(self, mock_queues_client):
        """Verify _restore_group propagates non-NOT_FOUND errors."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQChannelError("server error")
        backend = _make_backend(mock_queues_client)

        with pytest.raises(KubeMQChannelError, match="server error"):
            backend._restore_group("grp-err-2")

    def test_restore_group_returns_none_when_empty(self, mock_queues_client):
        """Verify _restore_group returns None when no messages."""
        mock_queues_client.peek_queue_messages.return_value = MagicMock(messages=[])
        backend = _make_backend(mock_queues_client)

        result = backend._restore_group("grp-empty-1")
        assert result is None


class TestBackendExceptionNarrowing:
    """Tests for backend exception narrowing (C3).

    Verifies that _store_result and _get_task_meta_for handle
    specific KubeMQ exceptions rather than broad Exception.
    """

    def test_store_result_narrows_exceptions(self, mock_queues_client):
        """C3: ack_all failure uses specific KubeMQ exceptions, not Exception."""
        from kubemq.core.exceptions import (
            KubeMQChannelError,
        )

        # Simulate specific exception on ack_all (purge before store)
        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQChannelError("channel error")
        backend = _make_backend(mock_queues_client)

        # Should not raise -- exception is caught and logged
        backend._store_result(
            task_id="narrow-1",
            result="ok",
            state="SUCCESS",
        )

        # send_queue_message should still be called
        mock_queues_client.send_queue_message.assert_called_once()

    def test_store_result_connection_error_logged(self, mock_queues_client):
        """C3: Connection error during purge is logged as warning."""
        from kubemq.core.exceptions import KubeMQConnectionError

        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQConnectionError(
            "connection lost"
        )
        backend = _make_backend(mock_queues_client)

        # Should not raise
        backend._store_result(
            task_id="narrow-2",
            result="ok",
            state="SUCCESS",
        )

        mock_queues_client.send_queue_message.assert_called_once()

    def test_get_task_meta_returns_pending_on_not_found(self, mock_queues_client):
        """C3: _get_task_meta_for returns PENDING on NOT_FOUND channel error."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQChannelError(
            "not found", code=ErrorCode.NOT_FOUND
        )
        backend = _make_backend(mock_queues_client)

        result = backend._get_task_meta_for("not-found-task")
        assert result["status"] == "PENDING"

    def test_get_task_meta_propagates_non_not_found_error(self, mock_queues_client):
        """C3: _get_task_meta_for propagates non-NOT_FOUND KubeMQ errors."""
        mock_queues_client.peek_queue_messages.side_effect = KubeMQTimeoutError("timeout")
        backend = _make_backend(mock_queues_client)

        with pytest.raises(KubeMQTimeoutError, match="timeout"):
            backend._get_task_meta_for("timeout-task")

    def test_get_task_meta_propagates_json_error(self, mock_queues_client):
        """C3: JSON decode errors are propagated (not silently swallowed)."""
        mock_msg = MagicMock()
        mock_msg.body = b"not-valid-json"
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )
        backend = _make_backend(mock_queues_client)

        with pytest.raises(json.JSONDecodeError):
            backend._get_task_meta_for("json-err-task")


class TestRestoreGroupCache:
    """Tests for _restore_group LRU cache (C12).

    Verifies bounded cache with TTL-based expiry and LRU eviction.
    """

    def _make_stored_group(self, backend, group_id, result_value):
        """Build a stored group message body matching _save_group format."""
        encoded = backend.encode(result_value)
        return {"group_id": group_id, "result": encoded}

    def test_restore_group_cache_hit(self, mock_queues_client):
        """C12: Second call returns cached result, no broker peek."""
        backend = _make_backend(mock_queues_client)
        stored = self._make_stored_group(backend, "grp-cache-1", [1, 2])

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(stored).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        # First call: fetches from broker, decodes result
        result1 = backend._restore_group("grp-cache-1")
        assert result1 == [1, 2]
        assert mock_queues_client.peek_queue_messages.call_count == 1

        # Second call: should hit cache, no additional broker call
        result2 = backend._restore_group("grp-cache-1")
        assert result2 == [1, 2]
        assert mock_queues_client.peek_queue_messages.call_count == 1

    def test_restore_group_cache_miss(self, mock_queues_client):
        """C12: Different group_id triggers broker peek."""
        backend = _make_backend(mock_queues_client)
        stored1 = self._make_stored_group(backend, "grp-cache-2a", [1])
        stored2 = self._make_stored_group(backend, "grp-cache-2b", [2])

        def peek_side_effect(**kwargs):
            channel = kwargs.get("channel", "")
            if "grp-cache-2a" in channel:
                msg = MagicMock()
                msg.body = json.dumps(stored1).encode("utf-8")
                return MagicMock(messages=[msg])
            elif "grp-cache-2b" in channel:
                msg = MagicMock()
                msg.body = json.dumps(stored2).encode("utf-8")
                return MagicMock(messages=[msg])
            return MagicMock(messages=[])

        mock_queues_client.peek_queue_messages.side_effect = peek_side_effect

        result1 = backend._restore_group("grp-cache-2a")
        result2 = backend._restore_group("grp-cache-2b")

        assert result1 == [1]
        assert result2 == [2]
        assert mock_queues_client.peek_queue_messages.call_count == 2

    def test_restore_group_cache_eviction(self, mock_queues_client):
        """C12: Cache evicts oldest entries when over max size."""
        backend = _make_backend(mock_queues_client)
        # Set a small cache for testing
        backend._GROUP_CACHE_MAX_SIZE = 3

        # Pre-populate cache directly (values are already decoded)
        import time

        for i in range(5):
            backend._group_cache[f"grp-evict-{i}"] = (time.monotonic(), [i])

        # Cache should have been bounded, but since we're inserting directly,
        # verify eviction logic by triggering _restore_group
        mock_queues_client.peek_queue_messages.return_value = MagicMock(messages=[])

        assert len(backend._group_cache) == 5  # manually inserted

        # Trigger a restore that caches, which should evict old entries
        stored = self._make_stored_group(backend, "grp-evict-new", [99])
        mock_msg = MagicMock()
        mock_msg.body = json.dumps(stored).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        backend._restore_group("grp-evict-new")

        # Cache should be evicted down to max size (3)
        assert len(backend._group_cache) <= 3 + 1  # +1 for the new entry

    def test_restore_group_cache_ttl_expiry(self, mock_queues_client):
        """C12: Expired cache entries are refreshed from broker."""
        import time

        backend = _make_backend(mock_queues_client)
        stored = self._make_stored_group(backend, "grp-ttl-1", [42])

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(stored).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        backend._GROUP_CACHE_TTL = 0.01  # 10ms TTL for fast expiry

        # First call: populates cache
        result1 = backend._restore_group("grp-ttl-1")
        assert result1 == [42]
        assert mock_queues_client.peek_queue_messages.call_count == 1

        # Wait for TTL to expire
        time.sleep(0.02)

        # Second call: cache expired, fetches from broker again
        result2 = backend._restore_group("grp-ttl-1")
        assert result2 == [42]
        assert mock_queues_client.peek_queue_messages.call_count == 2

    def test_restore_group_cache_bypass(self, mock_queues_client):
        """C12: cache=False always fetches from broker."""
        backend = _make_backend(mock_queues_client)
        stored = self._make_stored_group(backend, "grp-bypass-1", [1])

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(stored).encode("utf-8")
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        backend = _make_backend(mock_queues_client)

        # Both calls should query broker
        backend._restore_group("grp-bypass-1", cache=False)
        backend._restore_group("grp-bypass-1", cache=False)

        assert mock_queues_client.peek_queue_messages.call_count == 2

    def test_delete_group_clears_cache(self, mock_queues_client):
        """C12: _delete_group removes entry from cache."""
        import time

        backend = _make_backend(mock_queues_client)
        backend._group_cache["grp-del-cache"] = (time.monotonic(), [1])

        backend._delete_group("grp-del-cache")

        assert "grp-del-cache" not in backend._group_cache


class TestBackendCleanup:
    """Tests for backend cleanup method."""

    def test_cleanup_closes_client(self, mock_queues_client):
        """Verify cleanup closes the queues client."""
        backend = _make_backend(mock_queues_client)

        backend.cleanup()

        mock_queues_client.close.assert_called_once()
        assert "_queues_client" not in backend.__dict__

    def test_cleanup_handles_close_error(self, mock_queues_client):
        """Verify cleanup handles errors during close."""
        mock_queues_client.close.side_effect = RuntimeError("close error")
        backend = _make_backend(mock_queues_client)

        backend.cleanup()  # should not raise
        assert "_queues_client" not in backend.__dict__

    def test_cleanup_no_client_created(self):
        """Verify cleanup is safe when no client was created."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://localhost:50000",
                "task_always_eager": False,
            }
        )
        backend = KubeMQResultBackend(app=app, url="kubemq://localhost:50000")
        backend.cleanup()  # should not raise


class TestBackendGetTaskMetaEdgeCases:
    """Edge case tests for _get_task_meta_for."""

    def test_get_task_meta_peek_error_flag_propagates(self, mock_queues_client):
        """Verify peek error flag raises KubeMQChannelError (not swallowed)."""
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            is_error=True,
            error="peek failed",
            messages=[],
        )
        backend = _make_backend(mock_queues_client)

        with pytest.raises(KubeMQChannelError, match="peek failed"):
            backend._get_task_meta_for("err-task-peek")

    def test_store_result_with_request_group_id(self, mock_queues_client):
        """Verify group_id from request is stored in meta."""
        backend = _make_backend(mock_queues_client)

        mock_request = MagicMock()
        mock_request.group = "my-group"

        backend._store_result(
            task_id="grp-store-1",
            result="ok",
            state="SUCCESS",
            request=mock_request,
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        body = json.loads(sent_msg.body)
        assert body["group_id"] == "my-group"

    def test_store_result_expires_zero(self, mock_queues_client):
        """Verify result_expires=0 uses max expiration."""
        backend = _make_backend(mock_queues_client, result_expires=0)

        backend._store_result(
            task_id="exp-zero-1",
            result="ok",
            state="SUCCESS",
        )

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 86400


class TestBackendResultChannelPrefix:
    """Tests for result channel prefix configuration."""

    def test_default_prefix(self, mock_queues_client):
        """Verify default result channel prefix."""
        backend = _make_backend(mock_queues_client)
        channel = backend._result_channel("task-123")
        assert channel == "celery-result-task-123"

    def test_custom_prefix(self, mock_queues_client):
        """Verify custom result channel prefix from transport options."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://localhost:50000",
                "task_always_eager": False,
                "result_backend_transport_options": {
                    "result_channel_prefix": "custom-result-",
                },
            }
        )
        backend = KubeMQResultBackend(app=app, url="kubemq://localhost:50000")
        backend.__dict__["_queues_client"] = mock_queues_client

        channel = backend._result_channel("task-456")
        assert channel == "custom-result-task-456"


class TestBackendRestoreGroupEdgeCases:
    """Edge cases for _restore_group -- covers backend.py L297, L310-317."""

    def test_restore_group_peek_error_returns_none(self, mock_queues_client):
        """Verify _restore_group returns None when peek is_error=True (L297)."""
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            is_error=True,
            messages=[],
        )
        backend = _make_backend(mock_queues_client)
        result = backend._restore_group("grp-peek-err")
        assert result is None

    def test_restore_group_connection_error(self, mock_queues_client):
        """Verify _restore_group returns None on connection error (L310-317)."""
        from kubemq.core.exceptions import KubeMQConnectionError

        mock_queues_client.peek_queue_messages.side_effect = KubeMQConnectionError("refused")
        backend = _make_backend(mock_queues_client)
        result = backend._restore_group("grp-conn-err")
        assert result is None

    def test_restore_group_json_decode_error(self, mock_queues_client):
        """Verify _restore_group returns None on corrupt JSON (L316-322)."""
        mock_msg = MagicMock()
        mock_msg.body = b"not valid json"
        mock_queues_client.peek_queue_messages.return_value = MagicMock(
            is_error=False,
            messages=[mock_msg],
        )
        backend = _make_backend(mock_queues_client)
        result = backend._restore_group("grp-json-err")
        assert result is None


class TestBackendDeleteGroupConnectionError:
    """Edge case for _delete_group connection error -- covers backend.py L339-340."""

    def test_delete_group_connection_error_logged(self, mock_queues_client):
        """Verify _delete_group handles connection error (L339-340)."""
        from kubemq.core.exceptions import KubeMQConnectionError

        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQConnectionError("refused")
        backend = _make_backend(mock_queues_client)
        backend._delete_group("grp-del-conn")  # should not raise


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

    @patch("kubemq_celery.backend.QueuesClient")
    def test_queues_client_creation_with_connection_timeout(self, MockQueuesClient):
        """Verify _queues_client passes connection_timeout when set (L87, L115)."""
        app = Celery("test")
        app.config_from_object(
            {
                "result_backend": "kubemq://localhost:50000",
                "task_always_eager": False,
                "result_backend_transport_options": {
                    "connection_timeout": "5.0",
                },
            }
        )

        backend = KubeMQResultBackend(app=app, url="kubemq://localhost:50000")
        _ = backend._queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.connection_timeout == 5.0

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
