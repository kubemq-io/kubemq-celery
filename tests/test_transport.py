"""Tests for kubemq_celery.transport.Transport."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
        """Verify establish_connection() pings broker via temporary QueuesClient."""
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

        assert result is mock_conn
        mock_ping_client.ping.assert_called_once()
        mock_ping_client.close.assert_called_once()

    def test_establish_connection_tls_from_url(self):
        """Verify establish_connection() sets ssl=True when +tls in transport string."""
        transport = Transport.__new__(Transport)
        mock_conninfo = MagicMock()
        mock_conninfo.transport = "kubemq+tls"
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 50000
        mock_conninfo.password = None
        transport.client = mock_conninfo

        mock_conn = MagicMock()

        with (
            patch.object(Transport.__bases__[0], "establish_connection", return_value=mock_conn),
            patch("kubemq_celery.transport.QueuesClient") as MockClient,
        ):
            mock_ping_client = MagicMock()
            MockClient.return_value = mock_ping_client
            transport.establish_connection()

        assert mock_conninfo.ssl is True

    def test_establish_connection_failure_raises(self):
        """Verify establish_connection() raises on connection failure."""
        from kubemq.core.exceptions import KubeMQConnectionError

        from kubemq_celery.exceptions import KubeMQCeleryConnectionError

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
            patch(
                "kubemq_celery.transport.QueuesClient",
                side_effect=KubeMQConnectionError("refused"),
            ),
        ):
            with pytest.raises(KubeMQCeleryConnectionError, match="Failed to connect"):
                transport.establish_connection()

    def test_lifecycle_logging(self, caplog):
        """C8: Verify lifecycle INFO logging on connection events."""
        import logging

        transport = Transport.__new__(Transport)

        with (
            caplog.at_level(logging.INFO, logger="kubemq_celery"),
            patch.object(Transport.__bases__[0], "close_connection"),
        ):
            transport.close_connection(MagicMock())

        assert any("connection closed" in r.message for r in caplog.records)
