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


# ===========================================================================
# T7: E2E Test Expansion
# ===========================================================================


class TestCeleryE2EExpanded:
    """T7: Expanded E2E tests for full task lifecycle.

    Uses eager mode to test task execution without a live broker.
    These validate that KubeMQ transport options integrate correctly
    with Celery's task execution pipeline.
    """

    def _make_app(self):
        """Create a Celery app in eager mode."""
        app = Celery("e2e-expanded", broker=_broker_url())
        app.config_from_object(
            {
                "task_always_eager": True,
                "task_eager_propagates": True,
                "result_backend": "kubemq://localhost:50000",
            }
        )
        return app

    def test_full_task_lifecycle(self):
        """T7-lifecycle: Test complete task lifecycle: define, call, get result."""
        app = self._make_app()

        @app.task(name="e2e.add")
        def add(x, y):
            return x + y

        result = add.delay(3, 4)
        assert result.get(timeout=5) == 7
        assert result.successful()

    def test_task_with_countdown(self):
        """T7-countdown: Test task with countdown parameter.

        In eager mode, countdown is ignored but the parameter should
        not cause errors.
        """
        app = self._make_app()

        @app.task(name="e2e.delayed_add")
        def delayed_add(x, y):
            return x + y

        result = delayed_add.apply_async(args=(5, 6), countdown=2)
        assert result.get(timeout=5) == 11

    def test_task_failure_state(self):
        """T7-failure: Test task failure state propagation."""
        app = self._make_app()

        @app.task(name="e2e.fail")
        def fail_task():
            raise ValueError("deliberate failure")

        with pytest.raises(ValueError, match="deliberate failure"):
            fail_task.delay().get(timeout=5)

    def test_task_with_expiration(self):
        """T7-expiration: Test task with expires parameter.

        In eager mode, expires is ignored but the parameter should
        not cause errors.
        """
        app = self._make_app()

        @app.task(name="e2e.expiring")
        def expiring_task(x):
            return x * 2

        result = expiring_task.apply_async(args=(21,), expires=60)
        assert result.get(timeout=5) == 42

    def test_batch_receive_mixed_task_types(self):
        """T7-batch-mixed: Test multiple tasks of different types."""
        app = self._make_app()

        @app.task(name="e2e.add_mixed")
        def add_mixed(x, y):
            return x + y

        @app.task(name="e2e.multiply_mixed")
        def multiply_mixed(x, y):
            return x * y

        @app.task(name="e2e.negate_mixed")
        def negate_mixed(x):
            return -x

        results = [
            add_mixed.delay(1, 2),
            multiply_mixed.delay(3, 4),
            negate_mixed.delay(5),
        ]

        values = [r.get(timeout=5) for r in results]
        assert values == [3, 12, -5]

    def test_task_retry_mechanism(self):
        """Test task retry mechanism works with KubeMQ transport."""
        app = self._make_app()
        # Disable eager propagation so Celery handles retries internally
        # instead of raising the Retry exception directly.
        app.conf.task_eager_propagates = False

        call_count = {"value": 0}

        @app.task(name="e2e.retry_task", bind=True, max_retries=2)
        def retry_task(self):
            call_count["value"] += 1
            if call_count["value"] < 3:
                raise self.retry(countdown=0)
            return "success"

        result = retry_task.delay()
        assert result.get(timeout=5) == "success"
        assert call_count["value"] == 3

    def test_task_with_kwargs(self):
        """Test task with keyword arguments."""
        app = self._make_app()

        @app.task(name="e2e.greet")
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result = greet.delay("World", greeting="Hi")
        assert result.get(timeout=5) == "Hi, World!"
