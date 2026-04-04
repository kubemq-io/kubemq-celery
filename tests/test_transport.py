"""Tests for kubemq_celery.transport.Transport."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from kubemq_celery.transport import Transport


class TestTransport:
    """Tests for Transport class."""

    def test_connection_errors_include_kubemq(self):
        """Verify KubeMQ exceptions in connection_errors tuple."""
        from kubemq.core.exceptions import (
            KubeMQAuthenticationError,
            KubeMQConnectionError,
            KubeMQConnectionNotReadyError,
        )

        assert KubeMQConnectionError in Transport.connection_errors
        assert KubeMQAuthenticationError in Transport.connection_errors
        assert KubeMQConnectionNotReadyError in Transport.connection_errors

    def test_channel_errors_include_kubemq(self):
        """Verify KubeMQ exceptions in channel_errors tuple."""
        from kubemq.core.exceptions import (
            KubeMQChannelError,
            KubeMQMessageError,
            KubeMQStreamBrokenError,
            KubeMQTimeoutError,
            KubeMQTransactionError,
        )

        assert KubeMQTimeoutError in Transport.channel_errors
        assert KubeMQStreamBrokenError in Transport.channel_errors
        assert KubeMQChannelError in Transport.channel_errors
        assert KubeMQMessageError in Transport.channel_errors
        assert KubeMQTransactionError in Transport.channel_errors

    def test_auto_registration(self):
        """Verify import kubemq_celery registers kubemq:// in TRANSPORT_ALIASES."""
        from kombu.transport import TRANSPORT_ALIASES

        import kubemq_celery  # noqa: F401

        assert "kubemq" in TRANSPORT_ALIASES
        assert "kubemq+tls" in TRANSPORT_ALIASES

    def test_driver_type(self):
        assert Transport.driver_type == "kubemq"
        assert Transport.driver_name == "kubemq"
        assert Transport.default_port == 50000

    def test_polling_interval(self):
        assert Transport.polling_interval == 0.1

    def test_driver_version(self):
        """Verify driver_version() returns kubemq package version."""
        mock_client = MagicMock()
        transport = Transport.__new__(Transport)
        transport.client = mock_client

        version = transport.driver_version()
        assert isinstance(version, str)
        assert len(version) > 0

    def test_default_connection_params(self):
        """Verify default_connection_params returns correct defaults."""
        transport = Transport.__new__(Transport)

        params = transport.default_connection_params
        assert params["hostname"] == "localhost"
        assert params["port"] == 50000

    def test_as_uri_masks_password(self):
        """Verify as_uri() masks credentials when include_password=False."""
        transport = Transport.__new__(Transport)

        result = transport.as_uri("kubemq://:mytoken@localhost:50000", include_password=False)
        assert "mytoken" not in result
        assert "**" in result
        assert "localhost:50000" in result

    def test_as_uri_shows_password(self):
        """Verify as_uri() shows credentials when include_password=True."""
        transport = Transport.__new__(Transport)

        result = transport.as_uri("kubemq://:mytoken@localhost:50000", include_password=True)
        assert "mytoken" in result

    def test_as_uri_no_at_sign(self):
        """Verify as_uri() returns URI unchanged when no @ present."""
        transport = Transport.__new__(Transport)

        result = transport.as_uri("kubemq://localhost:50000", include_password=False)
        assert result == "kubemq://localhost:50000"

    def test_as_uri_custom_mask(self):
        """Verify as_uri() uses custom mask string."""
        transport = Transport.__new__(Transport)

        result = transport.as_uri(
            "kubemq://:mytoken@localhost:50000",
            include_password=False,
            mask="[HIDDEN]",
        )
        assert "[HIDDEN]" in result
        assert "mytoken" not in result

    def test_close_connection(self):
        """Verify close_connection() delegates to super()."""
        transport = Transport.__new__(Transport)
        mock_connection = MagicMock()

        with patch.object(Transport.__bases__[0], "close_connection"):
            transport.close_connection(mock_connection)

    def test_verify_connection_true(self):
        """Verify verify_connection() returns True when ping succeeds."""
        transport = Transport.__new__(Transport)
        mock_channel = MagicMock()
        mock_channel._kubemq_queues_client.ping.return_value = True
        transport._avail_channels = {mock_channel}

        result = transport.verify_connection(MagicMock())
        assert result is True

    def test_verify_connection_false_on_error(self):
        """Verify verify_connection() returns False when ping fails."""
        transport = Transport.__new__(Transport)
        mock_channel = MagicMock()
        mock_channel._kubemq_queues_client.ping.side_effect = RuntimeError("unreachable")
        transport._avail_channels = {mock_channel}

        result = transport.verify_connection(MagicMock())
        assert result is False

    def test_verify_connection_no_channels(self):
        """Verify verify_connection() returns False when no channels (no broker check)."""
        transport = Transport.__new__(Transport)
        transport._avail_channels = set()

        result = transport.verify_connection(MagicMock())
        assert result is False

    def test_establish_connection_success(self):
        """Verify establish_connection() creates and verifies a channel."""
        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.transport_cls = "kubemq"
        transport.client = mock_conninfo

        mock_conn = MagicMock()
        mock_channel = MagicMock()

        with patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn):
            with patch.object(transport, "create_channel", return_value=mock_channel):
                result = transport.establish_connection()

        assert result is mock_conn
        # Channel should have been closed after verification
        mock_channel.close.assert_called_once()

    def test_establish_connection_tls_from_url(self):
        """Verify establish_connection() sets ssl=True when +tls in transport string."""
        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.transport = "kubemq+tls"
        transport.client = mock_conninfo

        mock_conn = MagicMock()
        mock_channel = MagicMock()

        with patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn):
            with patch.object(transport, "create_channel", return_value=mock_channel):
                transport.establish_connection()

        assert mock_conninfo.ssl is True

    def test_establish_connection_failure_closes_channel(self):
        """Verify establish_connection() closes channel on failure and re-raises."""
        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.transport_cls = "kubemq"
        transport.client = mock_conninfo

        mock_conn = MagicMock()
        mock_channel = MagicMock()
        # Accessing the queues client property triggers an error
        type(mock_channel)._kubemq_queues_client = PropertyMock(
            side_effect=RuntimeError("connection refused")
        )

        with patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn):
            with patch.object(transport, "create_channel", return_value=mock_channel):
                with pytest.raises(RuntimeError, match="connection refused"):
                    transport.establish_connection()

        mock_channel.close.assert_called_once()
