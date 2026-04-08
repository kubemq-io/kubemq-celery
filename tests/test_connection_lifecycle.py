"""Tests for connection lifecycle (C8, C11).

Verifies lifecycle logging (INFO-level connect/disconnect, client creation)
and connection verification using SDK ping().
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from kubemq_celery.transport import Channel, Transport


def _make_channel(queues_client=None, pubsub_client=None):
    """Create a Channel with mocked connection."""
    mock_conn = MagicMock()
    mock_conn.client.hostname = "localhost"
    mock_conn.client.port = 50000
    mock_conn.client.password = None
    mock_conn.client.ssl = False
    mock_conn.client.transport_options = {}
    mock_conn._used_channel_ids = []
    mock_conn.channel_max = 65535

    channel = Channel(mock_conn)

    if queues_client is not None:
        channel.__dict__["_kubemq_queues_client"] = queues_client
    if pubsub_client is not None:
        channel.__dict__["_kubemq_pubsub_client"] = pubsub_client

    return channel


class TestConnectionLifecycleLogging:
    """Tests for lifecycle logging (C8)."""

    @patch("kubemq_celery.transport.QueuesClient")
    def test_queues_client_creation_logs_info(self, MockQueuesClient, caplog):
        """Verify QueuesClient creation logs INFO with client_id."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50001
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)

        with caplog.at_level(logging.INFO, logger="kubemq_celery"):
            _ = channel._kubemq_queues_client

        assert any("QueuesClient created" in r.message for r in caplog.records)
        assert any("client_id" in r.message for r in caplog.records)

    @patch("kubemq_celery.transport.PubSubClient")
    def test_pubsub_client_creation_logs_info(self, MockPubSubClient, caplog):
        """Verify PubSubClient creation logs INFO with client_id."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "myhost"
        mock_conn.client.port = 50001
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)

        with caplog.at_level(logging.INFO, logger="kubemq_celery"):
            _ = channel._kubemq_pubsub_client

        assert any("PubSubClient created" in r.message for r in caplog.records)

    def test_subscribe_fanout_logs_info(self, caplog):
        """Verify _subscribe_fanout logs INFO."""
        mock_pubsub = MagicMock()
        channel = _make_channel(pubsub_client=mock_pubsub)

        with caplog.at_level(logging.INFO, logger="kubemq_celery"):
            channel._subscribe_fanout("celeryev")

        assert any("Subscribed to fanout" in r.message for r in caplog.records)

    def test_close_connection_logs_info(self, caplog):
        """Verify close_connection logs INFO."""
        transport = Transport.__new__(Transport)

        with (
            caplog.at_level(logging.INFO, logger="kubemq_celery"),
            patch.object(Transport.__bases__[0], "close_connection"),
        ):
            transport.close_connection(MagicMock())

        assert any("connection closed" in r.message for r in caplog.records)


class TestConnectionVerification:
    """Tests for connection verification (C11)."""

    def test_verify_connection_uses_ping(self):
        """Verify verify_connection uses SDK ping() directly."""
        transport = Transport.__new__(Transport)
        mock_channel = MagicMock()
        mock_channel._kubemq_queues_client.ping.return_value = True
        transport._avail_channels = {mock_channel}

        result = transport.verify_connection(MagicMock())

        assert result is True
        mock_channel._kubemq_queues_client.ping.assert_called_once()

    def test_verify_connection_returns_false_on_ping_failure(self):
        """Verify verify_connection returns False when ping fails."""
        transport = Transport.__new__(Transport)
        mock_channel = MagicMock()
        mock_channel._kubemq_queues_client.ping.side_effect = RuntimeError("unreachable")
        transport._avail_channels = {mock_channel}

        result = transport.verify_connection(MagicMock())

        assert result is False

    def test_verify_connection_returns_false_no_channels(self):
        """Verify verify_connection returns False when no channels available."""
        transport = Transport.__new__(Transport)
        transport._avail_channels = set()

        result = transport.verify_connection(MagicMock())

        assert result is False

    def test_establish_connection_ping_check(self):
        """Verify establish_connection pings the broker."""
        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.transport_cls = "kubemq"
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 50000
        mock_conninfo.password = None
        mock_conninfo.ssl = False
        transport.client = mock_conninfo

        mock_conn = MagicMock()

        with (
            patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn),
            patch("kubemq_celery.transport.QueuesClient") as MockClient,
        ):
            mock_ping_client = MagicMock()
            MockClient.return_value = mock_ping_client

            result = transport.establish_connection()

        # Ping should have been called on the temporary client
        mock_ping_client.ping.assert_called_once()
        mock_ping_client.close.assert_called_once()
        assert result is mock_conn
