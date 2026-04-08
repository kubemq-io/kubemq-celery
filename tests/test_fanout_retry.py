"""Dedicated fanout retry unit tests.

Tests the _on_fanout_error() exponential backoff re-subscription logic
in isolation from a live broker.

Spec: T2-resubscribe, T2-max-retries, T2-permanent, T2-recovery-log
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from kubemq_celery.transport import Channel


def _make_channel(queues_client=None, pubsub_client=None, **transport_opts):
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
    if pubsub_client is not None:
        channel.__dict__["_kubemq_pubsub_client"] = pubsub_client

    return channel


class TestFanoutRetry:
    """Unit tests for _on_fanout_error retry logic."""

    def test_fanout_error_resubscribes_on_first_try(self):
        """T2-resubscribe: After error, _subscribe_fanout is called again."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=3)

        # Put an existing subscription to be removed
        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        # Mock _subscribe_fanout to succeed (after initial error removed old sub)
        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("stream broken"))

        cancel.cancel.assert_called_once()
        # _subscribe_fanout should have been called (retry attempt)
        mock_sub.assert_called_once_with("myexchange")

    def test_fanout_error_retries_with_exponential_backoff(self):
        """Verify exponential backoff sleep values: 1, 2, 4 seconds."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=3)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        sleep_values = []

        # _subscribe_fanout fails on first 2 attempts, succeeds on 3rd
        call_count = [0]

        def side_effect(exchange):
            call_count[0] += 1
            if call_count[0] < 3:
                raise RuntimeError(f"attempt {call_count[0]} failed")

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=side_effect),
            patch.object(channel, "_backoff_sleep", side_effect=lambda s: sleep_values.append(s)),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("initial error"))

        # Sleep values should be exponential: 1.0, 2.0, 4.0
        assert sleep_values == [1.0, 2.0, 4.0]

    def test_fanout_error_max_retries_exhausted(self):
        """T2-max-retries: After max_retries, gives up without raising."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=2)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=RuntimeError("fail")),
            patch.object(channel, "_backoff_sleep"),
        ):
            # Should not raise
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        assert "myexchange" not in channel._fanout_subscriptions

    def test_fanout_error_permanent_failure_logs_error(self, caplog):
        """T2-permanent: After max retries, logs ERROR about permanent loss."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=1)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=RuntimeError("fail")),
            patch.object(channel, "_backoff_sleep"),
            caplog.at_level(logging.ERROR, logger="kubemq_celery"),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        assert any("permanently lost" in r.message for r in caplog.records)

    def test_fanout_error_recovery_success_logs_info(self, caplog):
        """T2-recovery-log: Successful recovery logs INFO message."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=3)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        with (
            patch.object(channel, "_subscribe_fanout"),
            patch.object(channel, "_backoff_sleep"),
            caplog.at_level(logging.INFO, logger="kubemq_celery"),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        assert any("recovered" in r.message for r in caplog.records)

    def test_fanout_error_skips_retry_when_closed(self):
        """No retry attempts when channel is already closed."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=5)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel
        channel._closed = True

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        mock_sub.assert_not_called()

    def test_fanout_error_zero_max_retries(self):
        """With fanout_max_retries=0, gives up immediately."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=0)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        with (
            patch.object(channel, "_subscribe_fanout") as mock_sub,
            patch.object(channel, "_backoff_sleep"),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        mock_sub.assert_not_called()

    def test_fanout_error_backoff_capped_at_30s(self):
        """Backoff sleep should never exceed 30 seconds."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub, fanout_max_retries=10)

        cancel = MagicMock()
        channel._fanout_subscriptions["myexchange"] = cancel

        sleep_values = []

        with (
            patch.object(channel, "_subscribe_fanout", side_effect=RuntimeError("fail")),
            patch.object(channel, "_backoff_sleep", side_effect=lambda s: sleep_values.append(s)),
        ):
            channel._on_fanout_error("myexchange", RuntimeError("error"))

        # backoff: 1, 2, 4, 8, 16, 30, 30, 30, 30, 30
        for s in sleep_values:
            assert s <= 30.0
