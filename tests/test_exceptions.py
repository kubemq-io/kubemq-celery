"""Tests for kubemq_celery.exceptions module.

Verifies the exception hierarchy: all custom exceptions inherit
from KubeMQCeleryError which inherits from KubeMQ SDK's KubeMQError.
"""

from __future__ import annotations

import pytest
from kubemq.core.exceptions import KubeMQError

from kubemq_celery.exceptions import (
    KubeMQCeleryBackendError,
    KubeMQCeleryChannelError,
    KubeMQCeleryConnectionError,
    KubeMQCeleryError,
    KubeMQCelerySerializationError,
    KubeMQCeleryTimeoutError,
    KubeMQCeleryTransportError,
)


class TestExceptionHierarchy:
    """Verify exception inheritance chain."""

    def test_celery_error_inherits_kubemq_error(self):
        """KubeMQCeleryError should inherit from KubeMQError."""
        assert issubclass(KubeMQCeleryError, KubeMQError)

    def test_transport_error_inherits_celery_error(self):
        """KubeMQCeleryTransportError should inherit from KubeMQCeleryError."""
        assert issubclass(KubeMQCeleryTransportError, KubeMQCeleryError)

    def test_connection_error_inherits_transport_error(self):
        """KubeMQCeleryConnectionError inherits from KubeMQCeleryTransportError."""
        assert issubclass(KubeMQCeleryConnectionError, KubeMQCeleryTransportError)

    def test_channel_error_inherits_transport_error(self):
        """KubeMQCeleryChannelError inherits from KubeMQCeleryTransportError."""
        assert issubclass(KubeMQCeleryChannelError, KubeMQCeleryTransportError)

    def test_timeout_error_inherits_transport_error(self):
        """KubeMQCeleryTimeoutError inherits from KubeMQCeleryTransportError."""
        assert issubclass(KubeMQCeleryTimeoutError, KubeMQCeleryTransportError)

    def test_backend_error_inherits_celery_error(self):
        """KubeMQCeleryBackendError inherits from KubeMQCeleryError."""
        assert issubclass(KubeMQCeleryBackendError, KubeMQCeleryError)

    def test_serialization_error_inherits_celery_error(self):
        """KubeMQCelerySerializationError inherits from KubeMQCeleryError."""
        assert issubclass(KubeMQCelerySerializationError, KubeMQCeleryError)


class TestExceptionInstantiation:
    """Verify exceptions can be raised and caught correctly."""

    def test_catch_celery_error_catches_subtypes(self):
        """Catching KubeMQCeleryError should catch all subtypes."""
        subtypes = [
            KubeMQCeleryTransportError,
            KubeMQCeleryConnectionError,
            KubeMQCeleryChannelError,
            KubeMQCeleryTimeoutError,
            KubeMQCeleryBackendError,
            KubeMQCelerySerializationError,
        ]
        for exc_cls in subtypes:
            with pytest.raises(KubeMQCeleryError):
                raise exc_cls(f"test {exc_cls.__name__}")

    def test_catch_kubemq_error_catches_celery_errors(self):
        """Catching KubeMQError should catch all KubeMQCelery* exceptions."""
        with pytest.raises(KubeMQError):
            raise KubeMQCeleryConnectionError("connection lost")

    def test_error_message_preserved(self):
        """Verify error message is preserved through the hierarchy."""
        exc = KubeMQCeleryConnectionError("broker unreachable")
        assert "broker unreachable" in str(exc)

    def test_transport_error_not_backend_error(self):
        """Transport errors should not be caught by backend error handler."""
        exc = KubeMQCeleryTransportError("transport issue")
        assert not isinstance(exc, KubeMQCeleryBackendError)

    def test_backend_error_not_transport_error(self):
        """Backend errors should not be caught by transport error handler."""
        exc = KubeMQCeleryBackendError("backend issue")
        assert not isinstance(exc, KubeMQCeleryTransportError)
