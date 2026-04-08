"""Transport-specific exceptions for kubemq-celery.

Exception hierarchy inheriting from KubeMQ SDK exceptions.
Used for narrowed error handling in backend (C3), async transport (C7),
and batch receive (C6).
"""

from __future__ import annotations

from kubemq.core.exceptions import KubeMQError


class KubeMQCeleryError(KubeMQError):
    """Base exception for all kubemq-celery transport errors."""

    def __init__(self, message: str = "", **kwargs) -> None:
        super().__init__(message=message, **kwargs)


class KubeMQCeleryTransportError(KubeMQCeleryError):
    """Transport-layer error (send, receive, subscribe)."""

    pass


class KubeMQCeleryConnectionError(KubeMQCeleryTransportError):
    """Connection to KubeMQ broker failed or was lost."""

    pass


class KubeMQCeleryChannelError(KubeMQCeleryTransportError):
    """Channel-level error (queue not found, permission denied)."""

    pass


class KubeMQCeleryTimeoutError(KubeMQCeleryTransportError):
    """Operation timed out."""

    pass


class KubeMQCeleryBackendError(KubeMQCeleryError):
    """Result backend error (store, retrieve, group operations)."""

    pass


class KubeMQCelerySerializationError(KubeMQCeleryError):
    """JSON serialization/deserialization error."""

    pass


class KubeMQCeleryConfigError(KubeMQCeleryError):
    """Invalid transport configuration."""

    pass
