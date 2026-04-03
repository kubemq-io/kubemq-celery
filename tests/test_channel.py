"""Tests for kubemq_celery.transport.Channel."""

from __future__ import annotations

import json
from queue import Empty
from unittest.mock import MagicMock, patch

import pytest

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

    def test_put_delay_capped_at_12h(self, mock_queues_client):
        """Verify delay > 43200s is capped."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 100_000},
            "properties": {"delivery_tag": "tag-4", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.delay_in_seconds == 43200


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


# ===========================================================================
# TestChannelFanout
# ===========================================================================


class TestChannelFanout:
    """Tests for Channel._put_fanout()."""

    def test_put_fanout_uses_events(self, mock_pubsub_client):
        """Verify _put_fanout() sends EventMessage."""
        channel = _make_channel(pubsub_client=mock_pubsub_client)
        message = {"body": "broadcast-data", "headers": {}}

        channel._put_fanout("celeryev", message, routing_key="worker1")

        mock_pubsub_client.send_event.assert_called_once()
        sent_event = mock_pubsub_client.send_event.call_args[0][0]

        assert sent_event.channel == "celeryev"
        assert json.loads(sent_event.body) == message
        meta = json.loads(sent_event.metadata)
        assert meta["routing_key"] == "worker1"


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
        with patch.object(Channel.__bases__[0], "basic_ack"):
            channel.basic_ack("tag-ack-1")

        mock_msg_ref.ack.assert_called_once()
        # Ref should be removed after ack
        assert "tag-ack-1" not in channel._kubemq_msg_refs

    def test_basic_ack_keyerror_skips(self):
        """Verify basic_ack() handles KeyError gracefully (reconnection)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        # No ref stored for this tag -- should not raise
        with patch.object(Channel.__bases__[0], "basic_ack"):
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

        with patch.object(Channel.__bases__[0], "basic_reject"):
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

        with patch.object(Channel.__bases__[0], "basic_reject"):
            channel.basic_reject("tag-rq-1", requeue=True)

        mock_msg_ref.re_queue.assert_called_once_with("test-queue")
        assert "tag-rq-1" not in channel._kubemq_msg_refs

    def test_basic_reject_keyerror_skips(self):
        """Verify basic_reject() handles KeyError gracefully (reconnection)."""
        mock_queues_client = MagicMock()
        channel = _make_channel(queues_client=mock_queues_client)

        # No ref stored -- should not raise
        with patch.object(Channel.__bases__[0], "basic_reject"):
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

        with patch.object(Channel.__bases__[0], "basic_ack"):
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

        with patch.object(Channel.__bases__[0], "basic_reject"):
            channel.basic_reject("tag-val-2", requeue=False)
        assert "tag-val-2" not in channel._kubemq_msg_refs


# ===========================================================================
# TestChannelBasicConsume
# ===========================================================================


class TestChannelBasicConsume:
    """Tests for Channel.basic_consume() no_ack tracking."""

    def test_basic_consume_no_ack_true(self, mock_queues_client):
        """Verify basic_consume with no_ack=True adds queue to _no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)

        with patch.object(Channel.__bases__[0], "basic_consume", return_value="tag-1"):
            channel.basic_consume("test-queue", no_ack=True)

        assert "test-queue" in channel._no_ack_queues

    def test_basic_consume_no_ack_false(self, mock_queues_client):
        """Verify basic_consume with no_ack=False does not add to _no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)

        with patch.object(Channel.__bases__[0], "basic_consume", return_value="tag-2"):
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

        with patch.object(Channel.__bases__[0], "basic_cancel", return_value=None):
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

        with patch.object(Channel.__bases__[0], "basic_cancel", return_value=None):
            channel.basic_cancel("tag-c1")

        # Other consumer tag-c2 still consuming this queue with no_ack
        assert "test-queue" in channel._no_ack_queues

    def test_basic_cancel_unknown_tag(self, mock_queues_client):
        """Verify basic_cancel handles unknown consumer tag gracefully."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._tag_to_queue = {}
        channel._consumers = {}

        with patch.object(Channel.__bases__[0], "basic_cancel", return_value=None):
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

        with patch.object(Channel.__bases__[0], "close"):
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

        with patch.object(Channel.__bases__[0], "close"):
            channel.close()  # should not raise

        assert len(channel._fanout_subscriptions) == 0

    def test_close_closes_kubemq_clients(self, mock_queues_client, mock_pubsub_client):
        """Verify close() closes both KubeMQ clients."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            pubsub_client=mock_pubsub_client,
        )

        with patch.object(Channel.__bases__[0], "close"):
            channel.close()

        mock_queues_client.close.assert_called_once()
        mock_pubsub_client.close.assert_called_once()

    def test_close_handles_client_close_error(self, mock_queues_client):
        """Verify close() handles errors during client close."""
        channel = _make_channel(queues_client=mock_queues_client)
        mock_queues_client.close.side_effect = RuntimeError("close failed")

        with patch.object(Channel.__bases__[0], "close"):
            channel.close()  # should not raise

    def test_close_clears_state(self, mock_queues_client):
        """Verify close() clears msg refs and no_ack_queues."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._kubemq_msg_refs["tag-1"] = MagicMock()
        channel._no_ack_queues.add("test-queue")

        with patch.object(Channel.__bases__[0], "close"):
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
            patch.object(channel, "_put") as mock_put,
        ):
            channel._on_fanout_event("celeryev", mock_event)
            mock_lookup.assert_called_once_with("celeryev", "")
            mock_put.assert_called_once_with("queue1", message)

    def test_on_fanout_event_handles_decode_error(self, mock_queues_client):
        """Verify _on_fanout_event() handles invalid JSON gracefully."""
        channel = _make_channel(queues_client=mock_queues_client)
        mock_event = MagicMock()
        mock_event.body = b"not valid json"

        # Should not raise
        channel._on_fanout_event("celeryev", mock_event)

    def test_on_fanout_error_logs_warning(self, mock_queues_client):
        """Verify _on_fanout_error() does not raise."""
        channel = _make_channel(queues_client=mock_queues_client)

        # Should not raise
        channel._on_fanout_error("celeryev", RuntimeError("connection lost"))


# ===========================================================================
# TestChannelPutFanoutMessage
# ===========================================================================


class TestChannelPutFanoutMessage:
    """Tests for Channel._put_fanout_message()."""

    def test_put_fanout_message_dispatches_to_bound_queues(self, mock_queues_client):
        """Verify _put_fanout_message() sends to queues bound to exchange."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {"body": "fanout-msg", "headers": {}}

        with patch.object(channel, "_lookup", return_value=["q1", "q2"]):
            with patch.object(channel, "_put") as mock_put:
                channel._put_fanout_message("celeryev", message)
                assert mock_put.call_count == 2
                mock_put.assert_any_call("q1", message)
                mock_put.assert_any_call("q2", message)

    def test_put_fanout_message_handles_lookup_error(self, mock_queues_client):
        """Verify _put_fanout_message() handles _lookup() errors."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {"body": "fanout-msg", "headers": {}}

        with patch.object(channel, "_lookup", side_effect=KeyError("no exchange")):
            with patch.object(channel, "_put") as mock_put:
                channel._put_fanout_message("nonexistent", message)
                mock_put.assert_not_called()


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

    def test_delete_handles_error(self, mock_queues_client):
        """Verify _delete() handles errors gracefully."""
        mock_queues_client.delete_queues_channel.side_effect = RuntimeError("not found")
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

    def test_has_queue_returns_false_on_error(self, mock_queues_client):
        """Verify _has_queue() returns False on exception."""
        mock_queues_client.list_queues_channels.side_effect = RuntimeError("err")
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._has_queue("celery") is False

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

    def test_size_returns_zero_on_exception(self, mock_queues_client):
        """Verify _size() returns 0 on exception."""
        mock_queues_client.list_queues_channels.side_effect = RuntimeError("error")
        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._size("celery") == 0

    def test_size_returns_zero_when_no_match(self, mock_queues_client):
        """Verify _size() returns 0 when no channel matches exactly."""
        mock_ch = MagicMock()
        mock_ch.name = "other-queue"
        mock_ch.incoming.waiting = 10
        mock_queues_client.list_queues_channels.return_value = [mock_ch]

        channel = _make_channel(queues_client=mock_queues_client)

        assert channel._size("celery") == 0


# ===========================================================================
# TestChannelIsAutoAckQueue
# ===========================================================================


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
