"""Tests for Channel._put with TTL/delay refactoring.

Tests the refactored _put method that delegates to
BaseKubeMQChannel._build_queue_message_kwargs() for TTL, delay,
and DLQ handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from kubemq.core.exceptions import KubeMQClientClosedError

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


class TestPutWithTTL:
    """Tests for _put with message_expiration (C1)."""

    def test_put_with_message_expiration(self, mock_queues_client):
        """Verify _put uses message_expiration transport option."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            message_expiration=300,
        )
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-ttl-1", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 300

    def test_put_with_task_expires_header(self, mock_queues_client):
        """Verify _put uses per-task expires header over global option."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            message_expiration=300,
        )
        message = {
            "body": "dGVzdA==",
            "headers": {"expires": 60},
            "properties": {"delivery_tag": "tag-ttl-2", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 60

    def test_put_expiration_capped_at_24h(self, mock_queues_client):
        """Verify expiration > 86400s is capped."""
        channel = _make_channel(
            queues_client=mock_queues_client,
            message_expiration=200_000,
        )
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-ttl-3", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.expiration_in_seconds == 86400

    def test_put_no_expiration_default(self, mock_queues_client):
        """Verify no expiration when message_expiration=0 and no header."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-ttl-4", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert not hasattr(sent_msg, "expiration_in_seconds") or (
            getattr(sent_msg, "expiration_in_seconds", None) in (None, 0)
        )


class TestPutWithDelay:
    """Tests for _put with delay (countdown/eta)."""

    def test_put_with_countdown(self, mock_queues_client):
        """Verify _put with countdown header sets delay_in_seconds."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 15},
            "properties": {"delivery_tag": "tag-delay-1", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.delay_in_seconds == 15

    def test_put_delay_capped_at_24h(self, mock_queues_client):
        """Verify delay > 86400s is capped."""
        channel = _make_channel(queues_client=mock_queues_client)
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 100_000},
            "properties": {"delivery_tag": "tag-delay-2", "priority": 0},
        }

        channel._put("celery", message)

        sent_msg = mock_queues_client.send_queue_message.call_args[0][0]
        assert sent_msg.delay_in_seconds == 86400


class TestPutClosedChannel:
    """Tests for _put on a closed channel."""

    def test_put_on_closed_channel_raises(self, mock_queues_client):
        """Verify _put raises KubeMQClientClosedError when channel is closed."""
        channel = _make_channel(queues_client=mock_queues_client)
        channel._closed = True

        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-closed-1", "priority": 0},
        }

        with pytest.raises(KubeMQClientClosedError, match="channel closed"):
            channel._put("celery", message)
