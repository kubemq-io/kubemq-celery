"""Batch receive unit tests for Channel._get() (C6).

Tests the batch receive buffer logic: multi-message gRPC receive,
first-message return, buffer-rest pattern, and drain-on-close behavior.
"""

from __future__ import annotations

import json
from queue import Empty
from unittest.mock import MagicMock, patch

import pytest

from kubemq_celery.transport import Channel


def _make_channel(queues_client=None, **transport_opts):
    """Create a Channel with mocked connection."""
    mock_conn = MagicMock()
    mock_conn.client.hostname = "localhost"
    mock_conn.client.port = 50000
    mock_conn.client.password = None
    mock_conn.client.ssl = False
    mock_conn.client.transport_options = transport_opts
    mock_conn._used_channel_ids = []
    mock_conn.channel_max = 65535

    channel = Channel(mock_conn)
    for k, v in transport_opts.items():
        if hasattr(channel, k):
            setattr(channel, k, v)

    if queues_client is not None:
        channel.__dict__["_kubemq_queues_client"] = queues_client

    return channel


def _make_mock_messages(count, queue="test-queue", auto_ack=True):
    """Create count mock QueueMessageReceived objects."""
    messages = []
    for i in range(count):
        payload = {
            "body": f"msg-{i}",
            "headers": {},
            "properties": {"delivery_tag": f"tag-batch-{i}"},
        }
        mock_msg = MagicMock()
        mock_msg.body = json.dumps(payload).encode("utf-8")
        mock_msg.channel = queue
        messages.append(mock_msg)
    return messages


class TestBatchReceive:
    """Tests for batch receive buffer (C6)."""

    def test_batch_get_returns_first(self, mock_queues_client):
        """Verify _get returns first message from batch."""
        messages = _make_mock_messages(3)
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        result = channel._get("test-queue")
        assert result["body"] == "msg-0"

    def test_batch_get_buffers_rest(self, mock_queues_client):
        """Verify remaining messages are buffered for subsequent _get calls."""
        messages = _make_mock_messages(5)
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        # First call triggers gRPC, returns first message
        result1 = channel._get("test-queue")
        assert result1["body"] == "msg-0"

        # Buffer should have 4 remaining
        assert len(channel._batch_buffers.get("test-queue", [])) == 4

        # Subsequent calls drain from buffer (no new gRPC call)
        result2 = channel._get("test-queue")
        assert result2["body"] == "msg-1"

        result3 = channel._get("test-queue")
        assert result3["body"] == "msg-2"

        # Only one gRPC call should have been made
        assert mock_queues_client.receive_queue_messages.call_count == 1

    def test_batch_buffer_empty_triggers_new_fetch(self, mock_queues_client):
        """After buffer is drained, next _get fetches from broker again."""
        messages1 = _make_mock_messages(2)

        call_count = [0]

        def receive_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return MagicMock(messages=messages1, is_error=False)
            return MagicMock(messages=[], is_error=False)

        mock_queues_client.receive_queue_messages.side_effect = receive_side_effect

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        # Drain all buffered messages
        channel._get("test-queue")  # msg-0
        channel._get("test-queue")  # msg-1

        # Next call should trigger new gRPC fetch
        with pytest.raises(Empty):
            channel._get("test-queue")

        assert mock_queues_client.receive_queue_messages.call_count == 2

    def test_drain_batch_buffers_nacks(self, mock_queues_client):
        """Verify _drain_batch_buffers nacks all buffered messages."""
        messages = _make_mock_messages(3)
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        # Fetch batch, only take first
        channel._get("test-queue")

        # Drain should nack the 2 remaining buffered messages
        channel._drain_batch_buffers()

        assert len(channel._batch_buffers) == 0
        # The 2 buffered message refs should have had nack() called
        for msg in messages[1:]:
            msg.nack.assert_called_once()

    def test_close_drains_buffers(self, mock_queues_client):
        """Verify close() drains batch buffers before closing."""
        messages = _make_mock_messages(3)
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        # Fetch batch, only take first
        channel._get("test-queue")

        with patch.object(Channel.__bases__[1], "close"):
            channel.close()

        assert channel._closed is True
        assert len(channel._batch_buffers) == 0

    def test_batch_mixed_task_types(self, mock_queues_client):
        """Verify batch receive handles different task payloads correctly."""
        payloads = [
            {
                "body": "task-a",
                "headers": {"task": "app.add"},
                "properties": {"delivery_tag": "a1"},
            },
            {
                "body": "task-b",
                "headers": {"task": "app.mul"},
                "properties": {"delivery_tag": "b1"},
            },
            {
                "body": "task-c",
                "headers": {"task": "app.sub"},
                "properties": {"delivery_tag": "c1"},
            },
        ]
        messages = []
        for p in payloads:
            msg = MagicMock()
            msg.body = json.dumps(p).encode("utf-8")
            msg.channel = "celery"
            messages.append(msg)

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("celery")

        result1 = channel._get("celery")
        result2 = channel._get("celery")
        result3 = channel._get("celery")

        assert result1["headers"]["task"] == "app.add"
        assert result2["headers"]["task"] == "app.mul"
        assert result3["headers"]["task"] == "app.sub"

    def test_batch_size_1_no_buffering(self, mock_queues_client):
        """With max_batch_size=1, each _get triggers a separate gRPC call."""
        msg = _make_mock_messages(1)[0]
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=1)
        channel._no_ack_queues.add("test-queue")

        channel._get("test-queue")
        channel._get("test-queue")

        # Should have made 2 separate gRPC calls
        assert mock_queues_client.receive_queue_messages.call_count == 2

    def test_batch_max_size_capped(self, mock_queues_client):
        """Verify max_batch_size is capped at MAX_BATCH_SIZE (100)."""
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=500)
        channel._no_ack_queues.add("test-queue")

        with pytest.raises(Empty):
            channel._get("test-queue")

        # Verify the actual batch size passed to SDK was capped at 100
        call_kwargs = mock_queues_client.receive_queue_messages.call_args
        assert call_kwargs.kwargs.get("max_messages", call_kwargs[1].get("max_messages")) == 100

    def test_batch_manual_ack_stores_refs(self, mock_queues_client):
        """Verify batch receive stores msg refs for manual ack mode."""
        messages = _make_mock_messages(3)
        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=messages,
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        # NOT in _no_ack_queues -> manual ack mode

        channel._get("test-queue")

        # First message ref should be stored
        assert "tag-batch-0" in channel._kubemq_msg_refs
        # Buffered messages also have refs stored
        assert "tag-batch-1" in channel._kubemq_msg_refs
        assert "tag-batch-2" in channel._kubemq_msg_refs

    def test_batch_invalid_json_skipped(self, mock_queues_client):
        """Invalid JSON messages in batch are skipped, valid ones returned."""
        good_payload = {
            "body": "good",
            "headers": {},
            "properties": {"delivery_tag": "good-tag"},
        }
        good_msg = MagicMock()
        good_msg.body = json.dumps(good_payload).encode("utf-8")

        bad_msg = MagicMock()
        bad_msg.body = b"not valid json"

        mock_queues_client.receive_queue_messages.return_value = MagicMock(
            messages=[bad_msg, good_msg],
            is_error=False,
        )

        channel = _make_channel(queues_client=mock_queues_client, max_batch_size=10)
        channel._no_ack_queues.add("test-queue")

        result = channel._get("test-queue")
        assert result["body"] == "good"
