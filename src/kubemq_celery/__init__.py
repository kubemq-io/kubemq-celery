"""KubeMQ Celery Transport -- kubemq:// broker URL support."""

from __future__ import annotations

from kubemq_celery.backend import KubeMQResultBackend
from kubemq_celery.transport import Transport

__version__ = "1.0.0"
__all__ = ["Transport", "KubeMQResultBackend"]

# Auto-register transport aliases
from kombu.transport import TRANSPORT_ALIASES

TRANSPORT_ALIASES["kubemq"] = "kubemq_celery.transport:Transport"
TRANSPORT_ALIASES["kubemq+tls"] = "kubemq_celery.transport:Transport"

# Auto-register result backend alias
# Uses same kubemq:// scheme as broker -- Celery distinguishes by setting name
from celery.app.backends import BACKEND_ALIASES  # noqa: E402

BACKEND_ALIASES["kubemq"] = "kubemq_celery.backend:KubeMQResultBackend"
# Fallback (version-safe): result_backend = 'kubemq_celery.backend:KubeMQResultBackend'
