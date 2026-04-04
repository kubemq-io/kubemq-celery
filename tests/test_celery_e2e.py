"""End-to-end Celery tests (requires live KubeMQ broker).

Run: pytest tests/test_celery_e2e.py -m e2e
Env: KUBEMQ_BROKER_ADDRESS (default: kubemq://localhost:50000)
"""

from __future__ import annotations

import os

import pytest
from celery import Celery
from kombu.transport import TRANSPORT_ALIASES

import kubemq_celery  # noqa: F401 — register transport/backend aliases

pytestmark = pytest.mark.e2e


def _broker_url() -> str:
    return os.environ.get("KUBEMQ_BROKER_ADDRESS", "kubemq://localhost:50000")


class TestCeleryE2E:
    def test_kubemq_transport_registered(self):
        """Importing kubemq_celery registers the Kombu transport alias."""
        assert TRANSPORT_ALIASES.get("kubemq") == "kubemq_celery.transport:Transport"
        assert TRANSPORT_ALIASES.get("kubemq+tls") == "kubemq_celery.transport:Transport"

    def test_celery_app_ensure_broker_connection(self):
        """Celery app can open a read connection to the live broker."""
        app = Celery("e2e", broker=_broker_url())
        app.conf.update(broker_connection_retry_on_startup=True)
        with app.connection_for_read() as conn:
            conn.ensure_connection(max_retries=3)

    def test_celery_task_can_be_declared(self):
        """Tasks bind to the configured app without eager execution."""
        app = Celery("e2e-tasks", broker=_broker_url())

        @app.task(name="e2e.echo")
        def echo(x: int) -> int:
            return x

        assert echo.name == "e2e.echo"
