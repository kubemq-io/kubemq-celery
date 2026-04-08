"""On Failure Callback — KubeMQ Celery Transport.

Demonstrates:
- Overriding on_failure() in a task class for custom error handling
- Using the task_failure signal for cross-cutting error logging
- Both approaches work independently and can be combined

Usage:
    # Start a worker:
    celery -A examples.error_handling.on_failure_callback worker --loglevel=info

    # Run the example:
    python examples/error_handling/on_failure_callback.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from typing import Any

from billiard.einfo import ExceptionInfo
from celery import Celery
from celery.signals import task_failure

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "on_failure_callback",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


# --- Method 1: Override on_failure() via Task subclass ---


class AlertingTask(app.Task):
    """Custom base task that hooks into failure lifecycle."""

    def on_failure(
        self,
        exc: Exception,
        task_id: str,
        args: tuple,
        kwargs: dict,
        einfo: ExceptionInfo,
    ) -> None:
        print(f"[on_failure] Task {task_id} failed: {exc}")
        print(f"  args={args}, kwargs={kwargs}")


@app.task(base=AlertingTask, bind=True, max_retries=0)
def risky_operation(self, item_id: str) -> dict:
    """A task that fails and triggers on_failure()."""
    raise RuntimeError(f"Cannot process item {item_id}")


# --- Method 2: task_failure signal (cross-cutting) ---


@task_failure.connect
def handle_task_failure(
    sender: Any = None,
    task_id: str | None = None,
    exception: Exception | None = None,
    args: tuple | None = None,
    kwargs: dict | None = None,
    traceback: Any = None,
    einfo: ExceptionInfo | None = None,
    **kw: Any,
) -> None:
    """Signal handler called when ANY task fails.

    Use this for centralized error tracking (e.g., Sentry, Datadog).
    """
    print(f"[task_failure signal] Task {sender.name}[{task_id}] failed: {exception}")


@app.task(max_retries=0)
def divide(x: int, y: int) -> float:
    """Divide x by y — will fail on division by zero."""
    return x / y


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("On Failure Callback Example")
    print("=" * 40)

    print("\n--- Task with on_failure override ---")
    try:
        result = risky_operation.delay("item-42")
        result.get(timeout=10)
    except Exception as exc:
        print(f"Caught: {type(exc).__name__}: {exc}")

    print("\n--- Division by zero (triggers signal) ---")
    try:
        result2 = divide.delay(10, 0)
        result2.get(timeout=10)
    except Exception as exc:
        print(f"Caught: {type(exc).__name__}: {exc}")

    print("\nDone!")
