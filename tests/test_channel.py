"""Tests for kubemq_celery.transport.Channel."""

from __future__ import annotations

import json
from queue import Empty
from unittest.mock import MagicMock, patch

import pytest
from kubemq.core.exceptions import ErrorCode, KubeMQChannelError

from kubemq_celery.transport import Channel

# ---------------------------------------------------------------------------
# Helper: create a Channel instance with mocked KubeMQ clients
# ---------------------------------------------------------------------------


def _make_channel(queues_client=None, pubsub_client=None, **transport_opts):
    """Create a Channel with mocked connection and optional SDK clients.

    Properly calls Channel.__init__ via a fully-mocked Kombu connection,
    then injects the mock SDK clients into the cached_property dict slots.
    """
    mock_conn = MagicMock()
    mock_conn.client.hostname = "localhost"
    mock_conn.client.port = 50000
    mock_conn.client.password = None
    mock_conn.client.ssl = False
    mock_conn.client.transport_options = transport_opts
    mock_conn._used_channel_ids = []
    mock_conn.channel_max = 65535

    channel = Channel(mock_conn)

    # Apply transport options that would normally come from broker_transport_options
    if "dead_letter_queue" in transport_opts:
        channel.dead_letter_queue = transport_opts["dead_letter_queue"]
    if "max_receive_count" in transport_opts:
        channel.max_receive_count = transport_opts["max_receive_count"]

    # Inject mocked SDK clients into the cached_property dict slots
    if queues_client is not None:
        channel.__dict__["_kubemq_queues_client"] = queues_client
    if pubsub_client is not None:
        channel.__dict__["_kubemq_pubsub_client"] = pubsub_client

    return channel


# ===========================================================================
# TestChannelPut
# ===========================================================================


class TestChannelPut:
    """Tests for Channel._put()."""

    def test_put_serializes_message(self, mock_queues_client):
        """Verify _put() JSON-serializes message dict and creates correct QueueMessage."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"task": "myapp.tasks.add"},
            "properties": {"delivery_tag": "tag-1", "priority": 5},
        }

        channel._put("celery", message)

        mock_queues_client.send_queue_message.assert_called_once()
        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]

        # Channel name sanitized
        assert sent_msg.channel == "celery"
        # Body is JSON-encoded bytes
        assert json.loads(sent_msg.body) == message
        # Metadata is JSON-encoded headers
        assert json.loads(sent_msg.metadata) == message["headers"]
        # Tags include priority
        assert sent_msg.tags["priority"] == "5"

    def test_put_with_delay(self, mock_queues_client):
        """Verify countdown/ETA maps to delay_in_seconds."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 10},
            "properties": {"delivery_tag": "tag-2", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.delay_in_seconds == 10

    def test_put_with_dlq(self, mock_queues_client):
        """Verify DLQ options set on QueueMessage."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            dead_letter_queue="my-dlq",
            max_receive_count=3,
        )
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-3", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.max_receive_count == 3
        assert sent_msg.max_receive_queue == "my-dlq"

    def test_put_delay_capped_at_24h(self, mock_queues_client):
        """Verify delay > 86400s is capped."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 100_000},
            "properties": {"delivery_tag": "tag-4", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.delay_in_seconds == 86400


# ===========================================================================
# TestChannelGet
# ===========================================================================


class TestChannelGet:
    """Tests for Channel._get()."""

    def test_get_deserializes_message(self, mock_queues_client):
        """Verify _get() returns correct message dict from QueueMessage.body."""
        original = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-10"},
        }

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(original).encode("utf-8")
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        channel = _make_channel(queues_client=mock_queues_client)
        # Queue is in _no_ack_queues -> auto_ack=True (default Celery behavior)
        channel._no_ack_queues.add("celery")

        result = channel._get("celery")
        assert result == original

    def test_get_raises_empty(self, mock_queues_client):
        """Verify _get() raises Empty when no messages."""
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[],
        )

        channel = _make_channel(queues_client=mock_queues_client)

        with pytest.raises(Empty):
            channel._get("celery")

    def test_get_stores_msg_ref(self, mock_queues_client):
        """Verify KubeMQ message ref stored for native ack when auto_ack=False."""
        original = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-20"},
        }

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(original).encode("utf-8")
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        channel = _make_channel(queues_client=mock_queues_client)
        # Queue is NOT in _no_ack_queues -> auto_ack=False (task_acks_late=True)

        channel._get("celery")

        # Verify msg ref stored under the delivery_tag
        assert "tag-20" in channel._kubemq_msg_refs
        assert channel._kubemq_msg_refs["tag-20"] is mock_msg

    def test_get_auto_ack_skips_ref_storage(self, mock_queues_client):
        """Verify no msg ref stored when auto_ack=True."""
        original = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-30"},
        }

        mock_msg = MagicMock()
        mock_msg.body = json.dumps(original).encode("utf-8")
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")  # auto_ack=True

        channel._get("celery")

        # No ref should be stored
        assert "tag-30" not in channel._kubemq_msg_refs
        assert len(channel._kubemq_msg_refs) == 0


# ===========================================================================
# TestChannelPurge
# ===========================================================================


class TestChannelPurge:
    """Tests for Channel._purge()."""

    def test_purge_calls_ack_all(self, mock_queues_client):
        """Verify _purge() calls ack_all_queue_messages()."""
        mock_queues_client.ack_all_queue_messages.return_value = 0
        channel = _make_channel(queues_client=mock_queues_client)

        channel._purge("celery")

        mock_queues_client.ack_all_queue_messages.assert_called_once_with(
            channel="celery",
            wait_time_seconds=1,
        )

    def test_purge_returns_actual_count(self, mock_queues_client):
        """Verify _purge() returns actual count from server."""
        mock_queues_client.ack_all_queue_messages.return_value = 5
        channel = _make_channel(queues_client=mock_queues_client)

        count = channel._purge("celery")

        assert count == 5


# ===========================================================================
# TestChannelSize
# ===========================================================================


class TestChannelSize:
    """Tests for Channel._size()."""

    def test_size_returns_count(self, mock_queues_client):
        """Verify _size() returns queue depth from channel stats."""
        mock_ch = MagicMock()
        mock_ch.name = "celery"
        mock_ch.incoming.waiting = 42

        mock_queues_client.list_queues_channels.return_value = [mock_ch]
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._size("celery") == 42

    def test_size_exact_match(self, mock_queues_client):
        """Verify _size() matches channel name exactly."""
        # Two channels with similar names -- only exact match should count
        mock_ch_similar = MagicMock()
        mock_ch_similar.name = "celery-priority"
        mock_ch_similar.incoming.waiting = 99

        mock_ch_exact = MagicMock()
        mock_ch_exact.name = "celery"
        mock_ch_exact.incoming.waiting = 7

        mock_queues_client.list_queues_channels.return_value = [
            mock_ch_similar,
            mock_ch_exact,
        ]
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._size("celery") == 7

    def test_size_peek_fallback_when_waiting_reports_zero(self, mock_queues_client):
        """Broker may report waiting=0 while messages are visible — use peek count."""
        mock_ch = MagicMock()
        mock_ch.name = "celery"
        mock_ch.incoming.waiting = 0
        mock_queues_client.list_queues_channels.return_value = [mock_ch]

        mock_peek = MagicMock()
        mock_peek.is_error = False
        mock_peek.messages = [MagicMock(), MagicMock(), MagicMock()]
        mock_queues_client.peek_queue_messages.return_value = mock_peek

        channel = _make_channel(queues_client=mock_queues_client)
        assert channel._size("celery") == 3
        mock_queues_client.peek_queue_messages.assert_called_once_with(
            channel="celery",
            max_messages=1024,
            wait_timeout_in_seconds=1,
        )


# ===========================================================================
# TestChannelAck
# ===========================================================================


class TestChannelAck:
    """Tests for Channel.basic_ack() and basic_reject()."""

    def test_basic_ack_native(self):
        """Verify basic_ack() calls QueueMessageReceived.ack()."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.ack.return_value = None
        channel._kubemq_msg_refs["tag-ack-1"] = mock_msg_ref

        # Patch super().basic_ack to avoid virtual layer side effects
        with patch.object(Channel.__bases__[1], "basic_ack"):
            channel.basic_ack("tag-ack-1")

        mock_msg_ref.ack.assert_called_once()
        # Ref should be removed after ack
        assert "tag-ack-1" not in channel._kubemq_msg_refs

    def test_basic_ack_keyerror_skips(self):
        """Verify basic_ack() handles KeyError gracefully (reconnection)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        # No ref stored for this tag -- should not raise
        with patch.object(Channel.__bases__[1], "basic_ack"):
            channel.basic_ack("nonexistent-tag")
        # If we reach here, no error was raised
        assert True

    def test_basic_reject_nack(self):
        """Verify basic_reject(requeue=False) calls nack()."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.nack.return_value = None
        channel._kubemq_msg_refs["tag-nack-1"] = mock_msg_ref

        with patch.object(Channel.__bases__[1], "basic_reject"):
            channel.basic_reject("tag-nack-1", requeue=False)

        mock_msg_ref.nack.assert_called_once()
        assert "tag-nack-1" not in channel._kubemq_msg_refs

    def test_basic_reject_requeue(self):
        """Verify basic_reject(requeue=True) calls re_queue()."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.re_queue.return_value = None
        mock_msg_ref.channel = "test-queue"
        channel._kubemq_msg_refs["tag-rq-1"] = mock_msg_ref

        with patch.object(Channel.__bases__[1], "basic_reject"):
            channel.basic_reject("tag-rq-1", requeue=True)

        mock_msg_ref.re_queue.assert_called_once_with("test-queue")
        assert "tag-rq-1" not in channel._kubemq_msg_refs

    def test_basic_reject_keyerror_skips(self):
        """Verify basic_reject() handles KeyError gracefully (reconnection)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        # No ref stored -- should not raise
        with patch.object(Channel.__bases__[1], "basic_reject"):
            channel.basic_reject("nonexistent-tag", requeue=False)
        # If we reach here, no error was raised
        assert True

    def test_basic_ack_valueerror_handled(self):
        """Verify basic_ack() handles ValueError (transaction already completed)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.ack.side_effect = ValueError("transaction already completed")
        channel._kubemq_msg_refs["tag-val-1"] = mock_msg_ref

        with patch.object(Channel.__bases__[1], "basic_ack"):
            channel.basic_ack("tag-val-1")
        # Should not raise; ref should still be removed via pop
        assert "tag-val-1" not in channel._kubemq_msg_refs

    def test_basic_reject_valueerror_handled(self):
        """Verify basic_reject() handles ValueError (transaction already completed)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.nack.side_effect = ValueError("transaction already completed")
        channel._kubemq_msg_refs["tag-val-2"] = mock_msg_ref

        with patch.object(Channel.__bases__[1], "basic_reject"):
            channel.basic_reject("tag-val-2", requeue=False)
        assert "tag-val-2" not in channel._kubemq_msg_refs


# ===========================================================================
# TestChannelBasicConsume
# ===========================================================================


class TestChannelRejectEdgeCases:
    """Edge case tests for Channel.basic_reject()."""

    def test_reject_requeue_broker_success_qos_ack(self):
        """Verify requeue success does QoS ack (not super reject)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        mock_msg_ref.re_queue.return_value = None
        mock_msg_ref.channel = "test-queue"
        channel._kubemq_msg_refs["rq-qos-1"] = mock_msg_ref

        # Simulate QoS tracking
        channel.qos._delivered["rq-qos-1"] = MagicMock()

        with patch.object(channel.qos, "ack") as mock_qos_ack:
            channel.basic_reject("rq-qos-1", requeue=True)

        mock_msg_ref.re_queue.assert_called_once_with("test-queue")
        mock_qos_ack.assert_called_once_with("rq-qos-1")

    def test_reject_delivery_tag_not_in_qos(self):
        """Verify reject returns early when tag not in QoS delivered."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        mock_msg_ref = MagicMock()
        channel._kubemq_msg_refs["orphan-1"] = mock_msg_ref

        # delivery_tag NOT in qos._delivered
        channel.basic_reject("orphan-1", requeue=False)
        mock_msg_ref.nack.assert_called_once()


class TestChannelEstablishConnectionEdgeCases:
    """Edge case tests for Transport.establish_connection()."""

    def test_establish_connection_network_error(self):
        """Verify establish_connection wraps OSError."""
        from kubemq_celery.exceptions import KubeMQCeleryConnectionError
        from kubemq_celery.transport import Transport

        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 50000
        mock_conninfo.password = None
        mock_conninfo.ssl = False
        transport.client = mock_conninfo

        mock_conn = MagicMock()

        with (
            patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn),
            patch(
                "kubemq_celery.transport.QueuesClient",
                side_effect=OSError("network unreachable"),
            ),
        ):
            with pytest.raises(KubeMQCeleryConnectionError, match="Network error"):
                transport.establish_connection()


class TestChannelBasicConsume:
    """Tests for Channel.basic_consume() no_ack tracking."""

    def test_basic_consume_no_ack_true(self, mock_queues_client):
        """Verify basic_consume with no_ack=True adds queue to _no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)

        with patch.object(Channel.__bases__[1], "basic_consume", return_value="tag-1"):
            channel.basic_consume("test-queue", no_ack=True)

        assert "test-queue" in channel._no_ack_queues

    def test_basic_consume_no_ack_false(self, mock_queues_client):
        """Verify basic_consume with no_ack=False does not add to _no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)

        with patch.object(Channel.__bases__[1], "basic_consume", return_value="tag-2"):
            channel.basic_consume("test-queue", no_ack=False)

        assert "test-queue" not in channel._no_ack_queues
        assert "tag-2" not in channel._no_ack_tags


# ===========================================================================
# TestChannelBasicCancel
# ===========================================================================


class TestChannelBasicCancel:
    """Tests for Channel.basic_cancel() no_ack cleanup."""

    def test_basic_cancel_removes_no_ack(self, mock_queues_client):
        """Verify basic_cancel cleans up _no_ack_queues when no other consumers."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("test-queue")
        channel._tag_to_queue = {"tag-cancel-1": "test-queue"}
        channel._consumers = {}

        with patch.object(Channel.__bases__[1], "basic_cancel", return_value=None):
            channel.basic_cancel("tag-cancel-1")

        assert "test-queue" not in channel._no_ack_queues

    def test_basic_cancel_keeps_no_ack_with_other_consumers(self, mock_queues_client):
        """Verify basic_cancel keeps _no_ack_queues if other no_ack consumers remain."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("test-queue")
        channel._no_ack_tags.add("tag-c1")
        channel._no_ack_tags.add("tag-c2")
        channel._tag_to_queue = {"tag-c1": "test-queue", "tag-c2": "test-queue"}
        channel._consumers = {"tag-c1": MagicMock(), "tag-c2": MagicMock()}

        with patch.object(Channel.__bases__[1], "basic_cancel", return_value=None):
            channel.basic_cancel("tag-c1")

        # Other consumer tag-c2 still consuming this queue with no_ack
        assert "test-queue" in channel._no_ack_queues

    def test_basic_cancel_unknown_tag(self, mock_queues_client):
        """Verify basic_cancel handles unknown consumer tag gracefully."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._tag_to_queue = {}
        channel._consumers = {}

        with patch.object(Channel.__bases__[1], "basic_cancel", return_value=None):
            channel.basic_cancel("nonexistent-tag")

        # Should not raise


# ===========================================================================
# TestChannelClose
# ===========================================================================


class TestChannelClose:
    """Tests for Channel.close()."""

    def test_close_cancels_fanout_subscriptions(self, mock_queues_client, mock_pubsub_client):
        """Verify close() cancels all fanout subscription tokens."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )

        cancel_token1 = MagicMock()
        cancel_token2 = MagicMock()
        channel._fanout_subscriptions = {"exchange1": cancel_token1, "exchange2": cancel_token2}

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()

        cancel_token1.cancel.assert_called_once()
        cancel_token2.cancel.assert_called_once()
        assert len(channel._fanout_subscriptions) == 0

    def test_close_handles_cancel_error(self, mock_queues_client, mock_pubsub_client):
        """Verify close() handles errors during fanout cancellation."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )

        cancel_token = MagicMock()
        cancel_token.cancel.side_effect = RuntimeError("cancel failed")
        channel._fanout_subscriptions = {"exchange1": cancel_token}

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()  # should not raise

        assert len(channel._fanout_subscriptions) == 0

    def test_close_closes_kubemq_clients(self, mock_queues_client, mock_pubsub_client):
        """Verify close() closes both KubeMQ clients."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()

        mock_queues_client.close.assert_called_once()
        mock_pubsub_client.close.assert_called_once()

    def test_close_handles_client_close_error(self, mock_queues_client):
        """Verify close() handles errors during client close."""
        channel = _make_channel(queues_client=mock_queues_client)
        mock_queues_client.close.side_effect = RuntimeError("close failed")

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()  # should not raise

    def test_close_clears_state(self, mock_queues_client):
        """Verify close() clears msg refs and no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._kubemq_msg_refs["tag-1"] = MagicMock()
        channel._no_ack_queues.add("test-queue")

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()

        assert len(channel._kubemq_msg_refs) == 0
        assert len(channel._no_ack_queues) == 0


# ===========================================================================
# TestChannelSubscribeFanout
# ===========================================================================


class TestChannelSubscribeFanout:
    """Tests for Channel._subscribe_fanout()."""

    def test_subscribe_fanout_creates_subscription(self, mock_pubsub_client):
        """Verify _subscribe_fanout() creates an EventsSubscription."""
        channel = _make_channel(pubsub_client=mock_pubsub_client)

        channel._subscribe_fanout("celeryev")

        mock_pubsub_client.subscribe_to_events.assert_called_once()
        assert "celeryev" in channel._fanout_subscriptions

    def test_subscribe_fanout_idempotent(self, mock_pubsub_client):
        """Verify _subscribe_fanout() is idempotent -- duplicate calls are no-ops."""
        channel = _make_channel(pubsub_client=mock_pubsub_client)

        channel._subscribe_fanout("celeryev")
        channel._subscribe_fanout("celeryev")

        # Only one subscription created
        mock_pubsub_client.subscribe_to_events.assert_called_once()


# ===========================================================================
# TestChannelFanoutEvent
# ===========================================================================


class TestChannelFanoutEvent:
    """Tests for Channel._on_fanout_event() and _on_fanout_error()."""

    def test_on_fanout_event_dispatches_message(self, mock_queues_client, mock_pubsub_client):
        """Verify _on_fanout_event() decodes event and dispatches to bound queues."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )
        message = {"body": "fanout-data", "headers": {}}
        mock_event = MagicMock()
        mock_event.body = json.dumps(message).encode("utf-8")

        with (
            patch.object(channel, "_lookup", return_value=["queue1"]) as mock_lookup,
            patch.object(channel, "_send_fanout_queue_message") as mock_send,
        ):
            channel._on_fanout_event("celeryev", mock_event)
            mock_lookup.assert_called_once_with("celeryev", "")
            mock_send.assert_called_once_with("queue1", message)

    def test_on_fanout_event_handles_decode_error(self, mock_queues_client):
        """Verify _on_fanout_event() handles invalid JSON gracefully."""
        channel = _make_channel(queues_client=mock_queues_client)
        mock_event = MagicMock()
        mock_event.body = b"not valid json"

        # Should not raise
        channel._on_fanout_event("celeryev", mock_event)

    def test_on_fanout_error_logs_warning(self, mock_queues_client, mock_pubsub_client):
        """Verify _on_fanout_error() removes old subscription and retries."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 1

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("connection lost"))

        cancel.cancel.assert_called_once()
        mock_sub.assert_called_once_with("celeryev")


# ===========================================================================
# TestChannelDelete
# ===========================================================================


class TestChannelDelete:
    """Tests for Channel._delete()."""

    def test_delete_calls_delete_channel(self, mock_queues_client):
        """Verify _delete() calls delete_queues_channel."""
        channel = _make_channel(queues_client=mock_queues_client)

        channel._delete("celery")

        mock_queues_client.delete_queues_channel.assert_called_once_with(
            channel="celery",
        )

    def test_delete_handles_not_found(self, mock_queues_client):
        """Verify _delete() ignores NOT_FOUND only (spec §4.4.2a)."""
        mock_queues_client.delete_queues_channel.side_effect = KubeMQChannelError(
            "not found",
            code=ErrorCode.NOT_FOUND,
        )
        channel = _make_channel(queues_client=mock_queues_client)

        channel._delete("nonexistent")  # should not raise


# ===========================================================================
# TestChannelNewQueue
# ===========================================================================


class TestChannelNewQueue:
    """Tests for Channel._new_queue()."""

    def test_new_queue_is_noop(self, mock_queues_client):
        """Verify _new_queue() is a no-op (KubeMQ creates on first use)."""
        channel = _make_channel(queues_client=mock_queues_client)

        channel._new_queue("new-queue")  # should not raise or call anything


# ===========================================================================
# TestChannelHasQueue
# ===========================================================================


class TestChannelHasQueue:
    """Tests for Channel._has_queue()."""

    def test_has_queue_returns_true_when_exists(self, mock_queues_client):
        """Verify _has_queue() returns True when channel exists."""
        mock_ch = MagicMock()
        mock_ch.name = "celery"
        mock_queues_client.list_queues_channels.return_value = [mock_ch]

        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._has_queue("celery") is True

    def test_has_queue_returns_false_when_missing(self, mock_queues_client):
        """Verify _has_queue() returns False when channel does not exist."""
        mock_queues_client.list_queues_channels.return_value = []
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._has_queue("celery") is False

    def test_has_queue_returns_false_on_not_found(self, mock_queues_client):
        """Verify _has_queue() returns False for NOT_FOUND (spec §4.4.2a)."""
        mock_queues_client.list_queues_channels.side_effect = KubeMQChannelError(
            "not found",
            code=ErrorCode.NOT_FOUND,
        )
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._has_queue("celery") is False

    def test_has_queue_propagates_operational_error(self, mock_queues_client):
        """Verify _has_queue() propagates non-NOT_FOUND errors."""
        mock_queues_client.list_queues_channels.side_effect = RuntimeError("err")
        channel = _make_channel(queues_client=mock_queues_client)

        with pytest.raises(RuntimeError, match="err"):
            channel._has_queue("celery")

    def test_has_queue_exact_match_only(self, mock_queues_client):
        """Verify _has_queue() uses exact name matching."""
        mock_ch = MagicMock()
        mock_ch.name = "celery-priority"
        mock_queues_client.list_queues_channels.return_value = [mock_ch]

        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._has_queue("celery") is False


# ===========================================================================
# TestChannelSizeEdgeCases
# ===========================================================================


class TestChannelSizeEdgeCases:
    """Additional edge case tests for Channel._size()."""

    def test_size_propagates_non_not_found(self, mock_queues_client):
        """Verify _size() propagates operational errors (not NOT_FOUND)."""
        mock_queues_client.list_queues_channels.side_effect = RuntimeError("error")
        channel = _make_channel(queues_client=mock_queues_client)

        with pytest.raises(RuntimeError, match="error"):
            channel._size("celery")

    def test_size_returns_zero_when_no_match(self, mock_queues_client):
        """Verify _size() returns 0 when no channel matches exactly."""
        mock_ch = MagicMock()
        mock_ch.name = "other-queue"
        mock_ch.incoming.waiting = 10
        mock_queues_client.list_queues_channels.return_value = [mock_ch]
        mock_peek = MagicMock()
        mock_peek.is_error = False
        mock_peek.messages = []
        mock_queues_client.peek_queue_messages.return_value = mock_peek

        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._size("celery") == 0


# ===========================================================================
# TestChannelIsAutoAckQueue
# ===========================================================================


class TestChannelGrpcAddress:
    """Tests for Channel._grpc_address() and _connection_timeout_value()."""

    def test_grpc_address(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        assert channel._grpc_address() == "localhost:50000"

    def test_connection_timeout_value_none(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        channel.connection.client.connect_timeout = None
        assert channel._connection_timeout_value() is None

    def test_connection_timeout_value_set(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        channel.connection_timeout = 10.0
        assert channel._connection_timeout_value() == 10.0

    def test_connection_timeout_from_conninfo(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        channel.connection.client.connect_timeout = 5.0
        assert channel._connection_timeout_value() == 5.0


class TestChannelGetEdgeCases:
    """Edge case tests for Channel._get()."""

    def test_get_error_response_raises(self, mock_queues_client):
        """Verify _get raises KubeMQMessageError on error response."""
        from kubemq.core.exceptions import KubeMQMessageError

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[],
            is_error=True,
            error="receive failed",
        )

        channel = _make_channel(queues_client=mock_queues_client)

        with pytest.raises(KubeMQMessageError):
            channel._get("celery")

    def test_get_invalid_json_skipped(self, mock_queues_client):
        """Verify invalid JSON messages are skipped."""
        bad_msg = MagicMock()
        bad_msg.body = b"not valid json"

        good_payload = {
            "body": "good",
            "headers": {},
            "properties": {"delivery_tag": "good-1"},
        }
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")

        result = channel._get("celery")
        assert result["body"] == "good"

    def test_get_non_dict_payload_skipped(self, mock_queues_client):
        """Verify non-dict JSON payloads are skipped."""
        list_msg = MagicMock()
        list_msg.body = json.dumps([1, 2, 3]).encode("utf-8")

        good_payload = {
            "body": "good",
            "headers": {},
            "properties": {"delivery_tag": "good-2"},
        }
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[list_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")

        result = channel._get("celery")
        assert result["body"] == "good"

    def test_get_all_invalid_raises_empty(self, mock_queues_client):
        """Verify Empty raised when all messages in batch are invalid."""
        bad_msg = MagicMock()
        bad_msg.body = b"invalid"

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")

        with pytest.raises(Empty):
            channel._get("celery")

    def test_get_missing_delivery_tag_manual_ack_raises(self, mock_queues_client):
        """Verify missing delivery_tag in manual ack mode raises."""
        from kubemq.core.exceptions import KubeMQChannelError

        payload = {
            "body": "test",
            "headers": {},
            "properties": {},  # no delivery_tag
        }
        mock_msg = MagicMock()
        mock_msg.body = json.dumps(payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[mock_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        # NOT in _no_ack_queues -> manual ack mode

        with pytest.raises(KubeMQChannelError, match="missing delivery_tag"):
            channel._get("celery")

    def test_get_closed_channel_raises(self, mock_queues_client):
        """Verify _get on closed channel raises."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        channel = _make_channel(queues_client=mock_queues_client)
        channel._closed = True

        with pytest.raises(KubeMQClientClosedError):
            channel._get("celery")


class TestChannelPurgeEdgeCases:
    """Edge case tests for Channel._purge()."""

    def test_purge_not_found_returns_zero(self, mock_queues_client):
        """Verify _purge returns 0 on NOT_FOUND."""
        exc = KubeMQChannelError("not found", code=ErrorCode.NOT_FOUND)
        mock_queues_client.ack_all_queue_messages.side_effect = exc

        channel = _make_channel(queues_client=mock_queues_client)
        count = channel._purge("nonexistent")
        assert count == 0


class TestChannelSendFanoutQueueMessage:
    """Tests for Channel._send_fanout_queue_message()."""

    def test_send_fanout_queue_message(self, mock_queues_client):
        """Verify _send_fanout_queue_message sends correct QueueMessage."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {"body": "fanout-data", "headers": {}}

        channel._send_fanout_queue_message("test-queue", message)

        mock_queues_client.send_queue_message.assert_called_once()
        sent = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent.channel == "test-queue"
        assert json.loads(sent.body) == message


class TestChannelSizeNotFound:
    """Tests for _size NOT_FOUND handling."""

    def test_size_returns_zero_on_not_found(self, mock_queues_client):
        """Verify _size returns 0 for NOT_FOUND."""
        exc = KubeMQChannelError("not found", code=ErrorCode.NOT_FOUND)
        mock_queues_client.list_queues_channels.side_effect = exc

        channel = _make_channel(queues_client=mock_queues_client)
        assert channel._size("celery") == 0


class TestChannelIsAutoAckQueue:
    """Tests for Channel._is_auto_ack_queue()."""

    def test_returns_true_when_in_no_ack_queues(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("my-queue")
        assert channel._is_auto_ack_queue("my-queue") is True

    def test_returns_false_when_not_in_no_ack_queues(self, mock_queues_client):
        channel = _make_channel(queues_client=mock_queues_client)
        assert channel._is_auto_ack_queue("my-queue") is False


# ===========================================================================
# TestChannelClientCreation
# ===========================================================================


class TestChannelClientCreation:
    """Tests for Channel._kubemq_queues_client and _kubemq_pubsub_client cached_property."""

    @patch("kubemq_celery.transport.QueuesClient")
    def test_kubemq_queues_client_creation(self, MockQueuesClient):
        """Verify _kubemq_queues_client creates QueuesClient with correct config."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50001
        mock_conn.client.password = "mytoken"
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        # Access the property to trigger creation
        _ = channel._kubemq_queues_client

        MockQueuesClient.assert_called_once()
        config = MockQueuesClient.call_args[1]["config"]
        assert config.address == "myhost:50001"
        assert config.auth_token == "mytoken"

    @patch("kubemq_celery.transport.QueuesClient")
    def test_kubemq_queues_client_default_port(self, MockQueuesClient):
        """Verify _kubemq_queues_client uses default port 50000 when None."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = None
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        _ = channel._kubemq_queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.address == "myhost:50000"

    @patch("kubemq_celery.transport.PubSubClient")
    def test_kubemq_pubsub_client_creation(self, MockPubSubClient):
        """Verify _kubemq_pubsub_client creates PubSubClient with correct config."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50001
        mock_conn.client.password = "pubtoken"
        mock_conn.client.ssl = True
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        _ = channel._kubemq_pubsub_client

        MockPubSubClient.assert_called_once()
        config = MockPubSubClient.call_args[1]["config"]
        assert config.address == "myhost:50001"
        assert config.auth_token == "pubtoken"
        assert config.tls.enabled is True

    @patch("kubemq_celery.transport.QueuesClient")
    def test_kubemq_queues_client_auth_token_override(self, MockQueuesClient):
        """Verify auth_token transport option overrides URL password."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50000
        mock_conn.client.password = "url-token"
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        channel.auth_token = "override-token"
        _ = channel._kubemq_queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.auth_token == "override-token"

    @patch("kubemq_celery.transport.TLSConfig")
    @patch("kubemq_celery.transport.QueuesClient")
    def test_kubemq_queues_client_tls_config(self, MockQueuesClient, MockTLSConfig):
        """Verify TLS config is passed through from channel attributes."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50000
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        channel.tls_enabled = True
        channel.tls_cert_file = "/path/cert.pem"
        channel.tls_key_file = "/path/key.pem"
        channel.tls_ca_file = "/path/ca.pem"
        _ = channel._kubemq_queues_client

        MockTLSConfig.assert_called_once_with(
            enabled=True,
            cert_file="/path/cert.pem",
            key_file="/path/key.pem",
            ca_file="/path/ca.pem",
        )


# ===========================================================================
# TestFanoutErrorRecovery (T2)
# ===========================================================================


class TestFanoutErrorRecovery:
    """T2: Fanout error recovery tests with exponential backoff."""

    def test_fanout_error_resubscribes(self, mock_pubsub_client):
        """T2-resubscribe: After error, re-subscription is attempted."""
        channel = _make_channel(pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 3

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("stream broken"))

        cancel.cancel.assert_called_once()
        mock_sub.assert_called_once_with("celeryev")

    def test_fanout_error_max_retries(self, mock_pubsub_client):
        """T2-max-retries: After max_retries exhausted, gives up."""
        channel = _make_channel(pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 2

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=RuntimeError("fail")),
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("error"))

        assert "celeryev" not in channel._fanout_subscriptions

    def test_fanout_error_permanent_failure_logs_error(self, mock_pubsub_client, caplog):
        """T2-permanent: Permanent failure logs ERROR."""
        import logging

        channel = _make_channel(pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 1

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=RuntimeError("fail")),
            patch.object(channel, "_backoff_sleep"),
            caplog.at_level(logging.ERROR, logger="kubemq_celery"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("error"))

        assert any("permanently lost" in r.message for r in caplog.records)

    def test_fanout_error_recovery_success_logs_info(self, mock_pubsub_client, caplog):
        """T2-recovery-log: Successful recovery logs INFO."""
        import logging

        channel = _make_channel(pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 3

        with (
            patch.object(channel, "_subscribe_fanout"),
            patch.object(channel, "_backoff_sleep"),
            caplog.at_level(logging.INFO, logger="kubemq_celery"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("error"))

        assert any("recovered" in r.message for r in caplog.records)


# ===========================================================================
# TestChannelPutWithTTL (C1)
# ===========================================================================


class TestChannelPutWithTTL:
    """Tests for Channel._put() with TTL/expiration (C1)."""

    def test_put_with_message_expiration(self, mock_queues_client):
        """Verify message_expiration transport option sets expiration_in_seconds."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel.message_expiration = 300
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-ttl-1", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 300

    def test_put_with_task_expires_header(self, mock_queues_client):
        """Verify per-task expires header overrides global message_expiration."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel.message_expiration = 300
        message = {
            "body": "dGVzdA==",
            "headers": {"expires": 60},
            "properties": {"delivery_tag": "tag-ttl-2", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 60

    def test_expiration_capped_at_24h(self, mock_queues_client):
        """Verify expiration > 86400s is capped."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel.message_expiration = 200_000
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-ttl-3", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 86400


# ===========================================================================
# TestChannelBatchGet (C6)
# ===========================================================================


class TestChannelBatchGet:
    """Tests for Channel._get() batch receive (C6)."""

    def test_batch_get_returns_first(self, mock_queues_client):
        """Verify batch _get returns first message."""
        messages = []
        for i in range(3):
            payload = {
                "body": f"batch-{i}",
                "headers": {},
                "properties": {"delivery_tag": f"btag-{i}"},
            }
            msg = MagicMock()
            msg.body = json.dumps(payload).encode("utf-8")
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")
        channel.max_batch_size = 10

        result = channel._get("celery")
        assert result["body"] == "batch-0"

    def test_batch_get_buffers_rest(self, mock_queues_client):
        """Verify remaining messages buffered for subsequent _get calls."""
        messages = []
        for i in range(3):
            payload = {
                "body": f"batch-{i}",
                "headers": {},
                "properties": {"delivery_tag": f"btag-{i}"},
            }
            msg = MagicMock()
            msg.body = json.dumps(payload).encode("utf-8")
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")
        channel.max_batch_size = 10

        r1 = channel._get("celery")
        r2 = channel._get("celery")
        r3 = channel._get("celery")

        assert r1["body"] == "batch-0"
        assert r2["body"] == "batch-1"
        assert r3["body"] == "batch-2"

        # Only 1 gRPC call
        assert mock_queues_client.receive_queue_messages.call_count == 1

    def test_drain_batch_buffers_nacks(self, mock_queues_client):
        """Verify _drain_batch_buffers nacks buffered messages."""
        messages = []
        for i in range(3):
            payload = {
                "body": f"batch-{i}",
                "headers": {},
                "properties": {"delivery_tag": f"btag-{i}"},
            }
            msg = MagicMock()
            msg.body = json.dumps(payload).encode("utf-8")
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")
        channel.max_batch_size = 10

        channel._get("celery")  # takes first, buffers rest
        channel._drain_batch_buffers()

        # 2 buffered messages should have been nacked
        for msg in messages[1:]:
            msg.nack.assert_called_once()

    def test_close_drains_buffers(self, mock_queues_client):
        """Verify close() drains batch buffers."""
        messages = []
        for i in range(2):
            payload = {
                "body": f"batch-{i}",
                "headers": {},
                "properties": {"delivery_tag": f"btag-{i}"},
            }
            msg = MagicMock()
            msg.body = json.dumps(payload).encode("utf-8")
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")
        channel.max_batch_size = 10

        channel._get("celery")  # takes first, buffers rest

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()

        assert channel._closed is True
        assert len(channel._batch_buffers) == 0

    def test_batch_mixed_task_types(self, mock_queues_client):
        """Verify batch works with different task payloads."""
        payloads = [
            {"body": "t-a", "headers": {"task": "add"}, "properties": {"delivery_tag": "mt-1"}},
            {"body": "t-b", "headers": {"task": "mul"}, "properties": {"delivery_tag": "mt-2"}},
        ]
        messages = []
        for p in payloads:
            msg = MagicMock()
            msg.body = json.dumps(p).encode("utf-8")
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("celery")
        channel.max_batch_size = 10

        r1 = channel._get("celery")
        r2 = channel._get("celery")

        assert r1["headers"]["task"] == "add"
        assert r2["headers"]["task"] == "mul"


# ===========================================================================
# TestChannelGetManualAckEdgeCases -- covers L285-288, L320-323, L329-332, L348-349
# ===========================================================================


class TestChannelGetManualAckEdgeCases:
    """Edge cases for _get() in manual ack mode (no_ack=False)."""

    def test_buffer_hit_stores_msg_ref_manual_ack(self, mock_queues_client):
        """Verify buffered message in manual ack mode stores msg ref (L285-288)."""
        payloads = [
            {"body": "m0", "headers": {}, "properties": {"delivery_tag": "buf-ack-0"}},
            {"body": "m1", "headers": {}, "properties": {"delivery_tag": "buf-ack-1"}},
        ]
        messages = []
        for p in payloads:
            msg = MagicMock()
            msg.body = json.dumps(p).encode("utf-8")
            msg.channel = "test"
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        # NOT in _no_ack_queues -> manual ack mode
        channel.max_batch_size = 10

        # First call: fetches batch, returns first, buffers second
        r1 = channel._get("test")
        assert r1["body"] == "m0"
        assert "buf-ack-0" in channel._kubemq_msg_refs

        # Second call: hits buffer, should store ref for buf-ack-1
        r2 = channel._get("test")
        assert r2["body"] == "m1"
        assert "buf-ack-1" in channel._kubemq_msg_refs

    def test_json_decode_failure_nacked_manual_ack(self, mock_queues_client):
        """Invalid JSON messages are nacked in manual ack mode (L320-323)."""
        bad_msg = MagicMock()
        bad_msg.body = b"not valid json"

        good_payload = {"body": "ok", "headers": {}, "properties": {"delivery_tag": "jd-1"}}
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        result = channel._get("test")
        assert result["body"] == "ok"
        bad_msg.nack.assert_called_once()

    def test_json_decode_nack_exception_swallowed(self, mock_queues_client):
        """Nack failure after JSON decode is logged but not raised (L322-323)."""
        bad_msg = MagicMock()
        bad_msg.body = b"not valid json"
        bad_msg.nack.side_effect = RuntimeError("nack failed")

        good_payload = {"body": "ok", "headers": {}, "properties": {"delivery_tag": "nf-1"}}
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        result = channel._get("test")
        assert result["body"] == "ok"

    def test_non_dict_payload_nacked_manual_ack(self, mock_queues_client):
        """Non-dict JSON payloads are nacked in manual ack mode (L329-332)."""
        list_msg = MagicMock()
        list_msg.body = json.dumps([1, 2, 3]).encode("utf-8")

        good_payload = {"body": "ok", "headers": {}, "properties": {"delivery_tag": "nd-1"}}
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[list_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        result = channel._get("test")
        assert result["body"] == "ok"
        list_msg.nack.assert_called_once()

    def test_non_dict_nack_exception_swallowed(self, mock_queues_client):
        """Nack failure after non-dict payload is logged but not raised (L331-332)."""
        list_msg = MagicMock()
        list_msg.body = json.dumps([1, 2, 3]).encode("utf-8")
        list_msg.nack.side_effect = RuntimeError("nack failed")

        good_payload = {"body": "ok", "headers": {}, "properties": {"delivery_tag": "nde-1"}}
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[list_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        result = channel._get("test")
        assert result["body"] == "ok"

    def test_missing_delivery_tag_nacks_and_raises(self, mock_queues_client):
        """First message without delivery_tag is nacked and raises error (L348-349)."""
        # Payload without delivery_tag
        bad_payload = {"body": "no-tag", "headers": {}, "properties": {}}
        bad_msg = MagicMock()
        bad_msg.body = json.dumps(bad_payload).encode("utf-8")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        with pytest.raises(KubeMQChannelError, match="missing delivery_tag"):
            channel._get("test")

        bad_msg.nack.assert_called_once()

    def test_missing_delivery_tag_nack_exception_swallowed(self, mock_queues_client):
        """Nack failure on missing delivery_tag is logged but not swallowed (L348-349)."""
        bad_payload = {"body": "no-tag", "headers": {}, "properties": {}}
        bad_msg = MagicMock()
        bad_msg.body = json.dumps(bad_payload).encode("utf-8")
        bad_msg.nack.side_effect = RuntimeError("nack failed")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel.max_batch_size = 10

        with pytest.raises(KubeMQChannelError, match="missing delivery_tag"):
            channel._get("test")


# ===========================================================================
# TestChannelCloseEdgeCases -- covers L475-476, L491-492, L515-516
# ===========================================================================


class TestChannelCloseEdgeCases:
    """Edge cases for Channel.close()."""

    def test_drain_nack_exception_swallowed(self, mock_queues_client):
        """Nack exception during drain is swallowed (L475-476)."""
        payload = {"body": "d", "headers": {}, "properties": {"delivery_tag": "dn-1"}}
        msg = MagicMock()
        msg.body = json.dumps(payload).encode("utf-8")
        msg.nack.side_effect = RuntimeError("nack failed")

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[
                MagicMock(
                    body=json.dumps(
                        {"body": "first", "headers": {}, "properties": {"delivery_tag": "dn-0"}}
                    ).encode("utf-8")
                ),
                msg,
            ],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client)
        channel._no_ack_queues.add("test")
        channel.max_batch_size = 10
        channel._get("test")  # takes first, buffers second

        # drain should not raise even though nack fails
        channel._drain_batch_buffers()
        assert len(channel._batch_buffers) == 0

    def test_close_drain_exception_swallowed(self, mock_queues_client):
        """Exception during drain in close() is swallowed (L491-492)."""
        channel = _make_channel(queues_client=mock_queues_client)

        with (
            patch.object(channel, "_drain_batch_buffers", side_effect=RuntimeError("drain error")),
            patch.object(Channel.__bases__[1], "close"),
        ):
            channel.close()  # should not raise

        assert channel._closed is True

    def test_close_pubsub_exception_swallowed(self, mock_queues_client, mock_pubsub_client):
        """Exception closing pubsub client is swallowed (L515-516)."""
        mock_pubsub_client.close.side_effect = RuntimeError("close error")
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()  # should not raise

        assert channel._closed is True


# ===========================================================================
# TestChannelBasicRejectSuper -- covers L456
# ===========================================================================


class TestChannelBasicRejectSuper:
    """Test basic_reject super() delegation."""

    def test_reject_no_requeue_calls_super(self, mock_queues_client):
        """Reject without requeue delegates to super().basic_reject (L456)."""
        channel = _make_channel(queues_client=mock_queues_client)

        mock_ref = MagicMock()
        mock_ref.channel = "test"
        channel._kubemq_msg_refs["rej-sup-1"] = mock_ref
        channel.qos._delivered["rej-sup-1"] = MagicMock()

        with patch.object(Channel.__bases__[1], "basic_reject") as mock_super_reject:
            channel.basic_reject("rej-sup-1", requeue=False)

        mock_ref.nack.assert_called_once()
        mock_super_reject.assert_called_once_with("rej-sup-1", requeue=False)


# ===========================================================================
# TestChannelBackoffSleep -- covers L226
# ===========================================================================


class TestChannelBackoffSleep:
    """Test _backoff_sleep calls time.sleep."""

    def test_backoff_sleep(self, mock_queues_client):
        """Verify _backoff_sleep calls time.sleep (L226)."""
        channel = _make_channel(queues_client=mock_queues_client)

        with patch("kubemq_celery.transport.time.sleep") as mock_sleep:
            channel._backoff_sleep(2.5)

        mock_sleep.assert_called_once_with(2.5)


# ===========================================================================
# TestChannelPurgeRaise -- covers L382
# ===========================================================================


class TestChannelPurgeRaise:
    """Test _purge raises for non-not-found errors."""

    def test_purge_raises_non_not_found(self, mock_queues_client):
        """Purge re-raises channel error that's not 'not found' (L382)."""
        mock_queues_client.ack_all_queue_messages.side_effect = KubeMQChannelError(
            "permission denied"
        )

        channel = _make_channel(queues_client=mock_queues_client)

        with pytest.raises(KubeMQChannelError, match="permission denied"):
            channel._purge("test")


# ===========================================================================
# TestChannelFanoutEvent -- covers L585, L592-598
# ===========================================================================


class TestChannelFanoutEventDispatch:
    """Tests for _on_fanout_event dispatch logic."""

    def test_fanout_event_dispatches_to_queues(self, mock_queues_client, mock_pubsub_client):
        """Verify fanout event decoded and dispatched to bound queues (L585-598)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)

        # Mock the _lookup to return bound queues
        with patch.object(channel, "_lookup", return_value=["worker-1", "worker-2"]):
            event = MagicMock()
            event.body = json.dumps({"body": "fanout-msg"}).encode("utf-8")
            channel._on_fanout_event("celeryev", event)

        # send_queue_message called for each bound queue
        assert mock_queues_client.send_queue_message.call_count == 2

    def test_fanout_event_closed_channel_skipped(self, mock_queues_client, mock_pubsub_client):
        """Closed channel skips fanout event dispatch (L584-585)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)
        channel._closed = True

        event = MagicMock()
        event.body = json.dumps({"body": "msg"}).encode("utf-8")
        channel._on_fanout_event("celeryev", event)

        mock_queues_client.send_queue_message.assert_not_called()

    def test_fanout_event_dispatch_error_swallowed(self, mock_queues_client, mock_pubsub_client):
        """Dispatch error to individual queue is swallowed (L597-598)."""
        mock_queues_client.send_queue_message.side_effect = RuntimeError("send failed")
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)

        with patch.object(channel, "_lookup", return_value=["worker-1"]):
            event = MagicMock()
            event.body = json.dumps({"body": "msg"}).encode("utf-8")
            channel._on_fanout_event("celeryev", event)  # should not raise

    def test_fanout_event_lookup_error_swallowed(self, mock_queues_client, mock_pubsub_client):
        """Lookup error during fanout event is swallowed (L592-593)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)

        with patch.object(channel, "_lookup", side_effect=RuntimeError("lookup failed")):
            event = MagicMock()
            event.body = json.dumps({"body": "msg"}).encode("utf-8")
            channel._on_fanout_event("celeryev", event)  # should not raise


# ===========================================================================
# TestChannelFanoutErrorCancelException -- covers L615-616
# ===========================================================================


class TestChannelFanoutErrorCancelException:
    """Test _on_fanout_error cancel token exception handling."""

    def test_cancel_token_exception_swallowed(self, mock_queues_client, mock_pubsub_client):
        """Exception from cancel_token.cancel() is swallowed (L615-616)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        cancel.cancel.side_effect = RuntimeError("cancel failed")
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 0

        # Should not raise despite cancel failure
        channel._on_fanout_error("celeryev", RuntimeError("error"))


# ===========================================================================
# TestChannelFanoutErrorClosedDuringBackoff -- covers L628, L634
# ===========================================================================


class TestChannelFanoutErrorClosedDuringBackoff:
    """Test _on_fanout_error when channel closes during backoff."""

    def test_closed_before_retry_loop(self, mock_queues_client, mock_pubsub_client):
        """Channel closed before retry loop starts (L628)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 3
        channel._closed = True

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("error"))

        mock_sub.assert_not_called()

    def test_closed_during_backoff_sleep(self, mock_queues_client, mock_pubsub_client):
        """Channel closed during backoff sleep (L634)."""
        channel = _make_channel(queues_client=mock_queues_client, pubsub_client=mock_pubsub_client)
        cancel = MagicMock()
        channel._fanout_subscriptions["celeryev"] = cancel
        channel.fanout_max_retries = 3

        def close_during_sleep(secs):
            channel._closed = True

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep", side_effect=close_during_sleep),
        ):
            channel._on_fanout_error("celeryev", RuntimeError("error"))

        mock_sub.assert_not_called()


# ===========================================================================
# TestChannelSizeEdgeCases -- covers L691, L696
# ===========================================================================


class TestChannelSizeEdgeCasesExtended:
    """Edge cases for Channel._size()."""

    def test_size_peek_error_returns_zero(self, mock_queues_client):
        """Verify _size returns 0 when peek returns is_error=True (L691)."""
        mock_queues_client.list_queues_channels.return_value = []
        mock_queues_client.peek_queue_messages.return_value = MagicMock(is_error=True)

        channel = _make_channel(queues_client=mock_queues_client)
        assert channel._size("test") == 0

    def test_size_raises_non_not_found(self, mock_queues_client):
        """Verify _size re-raises non-not-found channel error (L696)."""
        mock_queues_client.list_queues_channels.side_effect = KubeMQChannelError(
            "permission denied"
        )

        channel = _make_channel(queues_client=mock_queues_client)
        with pytest.raises(KubeMQChannelError, match="permission denied"):
            channel._size("test")


# ===========================================================================
# TestChannelDeleteAndHasQueue -- covers L707, L724
# ===========================================================================


class TestChannelDeleteAndHasQueue:
    """Tests for _delete and _has_queue edge cases."""

    def test_delete_channel(self, mock_queues_client):
        """Verify _delete calls delete_queues_channel (L707)."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._delete("test")
        mock_queues_client.delete_queues_channel.assert_called_once()

    def test_has_queue_raises_non_not_found(self, mock_queues_client):
        """Verify _has_queue re-raises non-not-found error (L724)."""
        mock_queues_client.list_queues_channels.side_effect = KubeMQChannelError(
            "permission denied"
        )

        channel = _make_channel(queues_client=mock_queues_client)
        with pytest.raises(KubeMQChannelError, match="permission denied"):
            channel._has_queue("test")


class TestDLQConfigValidation:
    """Tests for DLQ configuration validation (M-2)."""

    def test_max_receive_count_without_dlq_raises(self):
        """Verify max_receive_count without dead_letter_queue raises at init."""
        from kubemq_celery.exceptions import KubeMQCeleryConfigError

        with pytest.raises(KubeMQCeleryConfigError, match="requires dead_letter_queue"):
            _make_channel(max_receive_count=5)

    def test_max_receive_count_with_dlq_succeeds(self, mock_queues_client):
        """Verify max_receive_count with dead_letter_queue is accepted."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            dead_letter_queue="my-dlq",
            max_receive_count=5,
        )
        assert channel.max_receive_count == 5
        assert channel.dead_letter_queue == "my-dlq"

    def test_max_receive_count_zero_no_validation(self, mock_queues_client):
        """Verify max_receive_count=0 does not require dead_letter_queue."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            max_receive_count=0,
        )
        assert channel.max_receive_count == 0
