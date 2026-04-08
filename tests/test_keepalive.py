"""Tests for gRPC keepalive configuration (C10).

Verifies that keepalive transport options are passed through
to KubeMQ SDK KeepAliveConfig.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kubemq_celery.transport import Channel


class TestKeepaliveConfig:
    """Tests for gRPC keepalive transport options."""

    def test_default_keepalive_values(self):
        """Verify default keepalive values on Channel."""
        assert Channel.grpc_keepalive_time == 30
        assert Channel.grpc_keepalive_timeout == 10
        assert Channel.grpc_permit_without_calls is True

    @patch("kubemq_celery.transport.QueuesClient")
    def test_keepalive_passed_to_queues_client(self, MockQueuesClient):
        """Verify keepalive config is passed to QueuesClient creation."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "localhost"
        mock_conn.client.port = 50000
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        channel.grpc_keepalive_time = 60
        channel.grpc_keepalive_timeout = 20
        channel.grpc_permit_without_calls = False

        _ = channel._kubemq_queues_client

        config = MockQueuesClient.call_args[1]["config"]
        assert config.keep_alive.enabled is True
        assert config.keep_alive.ping_interval_in_seconds == 60
        assert config.keep_alive.ping_timeout_in_seconds == 20
        assert config.keep_alive.permit_without_calls is False

    @patch("kubemq_celery.transport.PubSubClient")
    def test_keepalive_passed_to_pubsub_client(self, MockPubSubClient):
        """Verify keepalive config is passed to PubSubClient creation."""
        mock_conn = MagicMock()
        mock_conn.client.hostname = "localhost"
        mock_conn.client.port = 50000
        mock_conn.client.password = None
        mock_conn.client.ssl = False
        mock_conn.client.transport_options = {}
        mock_conn._used_channel_ids = []
        mock_conn.channel_max = 65535

        channel = Channel(mock_conn)
        channel.grpc_keepalive_time = 45
        channel.grpc_keepalive_timeout = 15

        _ = channel._kubemq_pubsub_client

        config = MockPubSubClient.call_args[1]["config"]
        assert config.keep_alive.enabled is True
        assert config.keep_alive.ping_interval_in_seconds == 45
        assert config.keep_alive.ping_timeout_in_seconds == 15

    def test_keepalive_in_from_transport_options(self):
        """Verify keepalive options are in from_transport_options tuple."""
        assert "grpc_keepalive_time" in Channel.from_transport_options
        assert "grpc_keepalive_timeout" in Channel.from_transport_options
        assert "grpc_permit_without_calls" in Channel.from_transport_options
