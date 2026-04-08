"""Tests for kubemq_celery.base.BaseKubeMQChannel ABC.

Tests the concrete methods inherited by both Channel and AsyncChannel:
- _build_queue_message_kwargs
- _calculate_delay_seconds
- _calculate_expiration_seconds
- _decode_fanout_event
- _deserialize_message
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from kubemq_celery.transport import Channel


def _make_channel(**transport_opts):
    """Create a Channel with mocked connection for base class testing."""
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
    return channel


class TestBuildQueueMessageKwargs:
    """Tests for BaseKubeMQChannel._build_queue_message_kwargs()."""

    def test_basic_message(self):
        """Build kwargs for a simple message without delay or expiration."""
        channel = _make_channel()
        message = {
            "body": "dGVzdA==",
            "headers": {"task": "app.add"},
            "properties": {"delivery_tag": "tag-1", "priority": 3},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert kwargs["channel"] == "test-queue"
        assert json.loads(kwargs["body"]) == message
        assert json.loads(kwargs["metadata"]) == message["headers"]
        assert kwargs["tags"]["priority"] == "3"
        assert "delay_in_seconds" not in kwargs
        assert "expiration_in_seconds" not in kwargs

    def test_with_countdown_delay(self):
        """Build kwargs with countdown sets delay_in_seconds."""
        channel = _make_channel()
        message = {
            "body": "dGVzdA==",
            "headers": {"countdown": 30},
            "properties": {"delivery_tag": "tag-2", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert kwargs["delay_in_seconds"] == 30

    def test_with_message_expiration(self):
        """Build kwargs with message_expiration option sets expiration_in_seconds."""
        channel = _make_channel(message_expiration=600)
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-3", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert kwargs["expiration_in_seconds"] == 600

    def test_with_task_expires_header(self):
        """Per-task expires header takes priority over global message_expiration."""
        channel = _make_channel(message_expiration=600)
        message = {
            "body": "dGVzdA==",
            "headers": {"expires": 120},
            "properties": {"delivery_tag": "tag-4", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert kwargs["expiration_in_seconds"] == 120

    def test_with_dlq_options(self):
        """Build kwargs with DLQ options."""
        channel = _make_channel(dead_letter_queue="my-dlq", max_receive_count=5)
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-5", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert kwargs["max_receive_count"] == 5
        assert kwargs["max_receive_queue"] == "my-dlq"

    def test_no_dlq_when_zero_count(self):
        """DLQ options not set when max_receive_count=0."""
        channel = _make_channel(dead_letter_queue="my-dlq", max_receive_count=0)
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-6", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        assert "max_receive_count" not in kwargs
        assert "max_receive_queue" not in kwargs

    def test_empty_headers_metadata_none(self):
        """Empty headers produce metadata=None."""
        channel = _make_channel()
        message = {
            "body": "dGVzdA==",
            "headers": {},
            "properties": {"delivery_tag": "tag-7", "priority": 0},
        }

        kwargs = channel._build_queue_message_kwargs("test-queue", message)

        # Empty dict is falsy so metadata should be None
        assert kwargs["metadata"] is None


class TestCalculateDelaySeconds:
    """Tests for BaseKubeMQChannel._calculate_delay_seconds()."""

    def test_countdown_integer(self):
        channel = _make_channel()
        assert channel._calculate_delay_seconds({"countdown": 10}, {}) == 10

    def test_countdown_zero(self):
        channel = _make_channel()
        assert channel._calculate_delay_seconds({"countdown": 0}, {}) == 0

    def test_countdown_negative(self):
        channel = _make_channel()
        # Negative countdown returns negative; _build_queue_message_kwargs
        # only applies delay when > 0, so negative is effectively ignored
        assert channel._calculate_delay_seconds({"countdown": -5}, {}) == -5

    def test_eta_future(self):
        channel = _make_channel()
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        delay = channel._calculate_delay_seconds({"eta": future}, {})
        assert 55 <= delay <= 65  # within tolerance

    def test_eta_past(self):
        channel = _make_channel()
        past = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
        delay = channel._calculate_delay_seconds({"eta": past}, {})
        assert delay == 0

    def test_countdown_takes_priority_over_eta(self):
        channel = _make_channel()
        future = (datetime.now(timezone.utc) + timedelta(seconds=600)).isoformat()
        delay = channel._calculate_delay_seconds({"countdown": 10, "eta": future}, {})
        assert delay == 10

    def test_delay_capped_at_24h(self):
        channel = _make_channel()
        delay = channel._calculate_delay_seconds({"countdown": 100_000}, {})
        assert delay == 86400

    def test_invalid_countdown_value(self):
        channel = _make_channel()
        delay = channel._calculate_delay_seconds({"countdown": "invalid"}, {})
        assert delay == 0

    def test_invalid_eta_value(self):
        channel = _make_channel()
        delay = channel._calculate_delay_seconds({"eta": "not-a-date"}, {})
        assert delay == 0

    def test_no_delay_headers(self):
        channel = _make_channel()
        assert channel._calculate_delay_seconds({}, {}) == 0


class TestCalculateExpirationSeconds:
    """Tests for BaseKubeMQChannel._calculate_expiration_seconds()."""

    def test_task_expires_integer(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({"expires": 300}, 0)
        assert exp == 300

    def test_task_expires_float(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({"expires": 300.5}, 0)
        assert exp == 300

    def test_task_expires_iso_string(self):
        channel = _make_channel()
        future = (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat()
        exp = channel._calculate_expiration_seconds({"expires": future}, 0)
        assert 115 <= exp <= 125

    def test_global_message_expiration_fallback(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({}, 600)
        assert exp == 600

    def test_task_expires_overrides_global(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({"expires": 120}, 600)
        assert exp == 120

    def test_expiration_capped_at_24h(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({"expires": 200_000}, 0)
        assert exp == 86400

    def test_no_expiration_returns_zero(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({}, 0)
        assert exp == 0

    def test_invalid_expires_string(self):
        channel = _make_channel()
        exp = channel._calculate_expiration_seconds({"expires": "garbage"}, 0)
        assert exp == 0

    def test_task_expires_iso_string_naive(self):
        """Verify naive datetime string (no tzinfo) is treated as UTC."""
        channel = _make_channel()
        # Create a naive datetime string (no tz suffix)
        future = (datetime.now(timezone.utc) + timedelta(seconds=120)).strftime("%Y-%m-%dT%H:%M:%S")
        exp = channel._calculate_expiration_seconds({"expires": future}, 0)
        assert 110 <= exp <= 130  # L152: naive datetime gets utc tzinfo

    def test_eta_naive_datetime(self):
        """Verify naive ETA datetime (no tzinfo) is treated as UTC."""
        channel = _make_channel()
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%S")
        delay = channel._calculate_delay_seconds({"eta": future}, {})
        assert 55 <= delay <= 65  # L116: naive datetime gets utc tzinfo


class TestDecodeFanoutEvent:
    """Tests for BaseKubeMQChannel._decode_fanout_event()."""

    def test_valid_json_dict(self):
        channel = _make_channel()
        body = json.dumps({"body": "test"}).encode("utf-8")
        result = channel._decode_fanout_event(body)
        assert result == {"body": "test"}

    def test_invalid_json(self):
        channel = _make_channel()
        result = channel._decode_fanout_event(b"not valid json")
        assert result is None

    def test_non_dict_json(self):
        channel = _make_channel()
        result = channel._decode_fanout_event(json.dumps([1, 2, 3]).encode("utf-8"))
        assert result is None

    def test_empty_dict(self):
        channel = _make_channel()
        result = channel._decode_fanout_event(json.dumps({}).encode("utf-8"))
        assert result == {}


class TestDeserializeMessage:
    """Tests for BaseKubeMQChannel._deserialize_message()."""

    def test_valid_json(self):
        channel = _make_channel()
        body = json.dumps({"key": "value"}).encode("utf-8")
        result = channel._deserialize_message(body)
        assert result == {"key": "value"}

    def test_invalid_json_raises(self):
        channel = _make_channel()
        with pytest.raises(json.JSONDecodeError):
            channel._deserialize_message(b"not json")
