"""Task Inheritance — KubeMQ Celery Transport.

Demonstrates:
- Base task class with on_failure, on_retry, on_success hooks
- Inheriting common behavior across multiple tasks via base=BaseTask
- Centralized error logging and success tracking
- Shared retry configuration through base class

Usage:
    celery -A examples.advanced_patterns.task_inheritance worker --loglevel=info
    python examples/advanced_patterns/task_inheritance.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery, Task

import kubemq_celery  # noqa: F401

app = Celery(
    "task_inheritance",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


class BaseTask(Task):
    """Base task class with lifecycle hooks for all derived tasks."""

    abstract = True
    autoretry_for = (ConnectionError, TimeoutError)
    max_retries = 3
    default_retry_delay = 5

    def on_success(self, retval, task_id, args, kwargs):
        print(f"[BaseTask.on_success] Task {self.name} ({task_id}) succeeded")
        print(f"  Result: {retval}")
        print(f"  Args: {args}, Kwargs: {kwargs}")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(f"[BaseTask.on_failure] Task {self.name} ({task_id}) FAILED")
        print(f"  Exception: {exc}")
        print(f"  Args: {args}, Kwargs: {kwargs}")

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        print(f"[BaseTask.on_retry] Task {self.name} ({task_id}) retrying")
        print(f"  Exception: {exc}")

    def before_start(self, task_id, args, kwargs):
        print(f"[BaseTask.before_start] Task {self.name} ({task_id}) starting")
        self._start_time = time.monotonic()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        elapsed = time.monotonic() - getattr(self, "_start_time", time.monotonic())
        print(f"[BaseTask.after_return] Task {self.name} ({task_id})")
        print(f"  Status: {status}, Elapsed: {elapsed:.3f}s")


@app.task(base=BaseTask)
def process_order(order_id: str, items: list[dict]) -> dict:
    """Process an order — inherits BaseTask lifecycle hooks."""
    total = sum(item.get("price", 0) * item.get("qty", 1) for item in items)
    time.sleep(0.5)
    return {"order_id": order_id, "total": total, "status": "processed"}


@app.task(base=BaseTask)
def send_notification(user_id: str, message: str) -> dict:
    """Send notification — inherits BaseTask lifecycle hooks."""
    time.sleep(0.2)
    return {"user_id": user_id, "message": message, "status": "sent"}


@app.task(base=BaseTask, max_retries=5)
def unreliable_task(fail_count: int = 0) -> dict:
    """Task that can be configured to fail N times before succeeding."""
    attempt = unreliable_task.request.retries + 1
    if attempt <= fail_count:
        raise ConnectionError(f"Simulated failure on attempt {attempt}")
    return {"attempt": attempt, "status": "success"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Task Inheritance Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Base class: {BaseTask.__name__}")
    print(f"  autoretry_for: {BaseTask.autoretry_for}")
    print(f"  max_retries: {BaseTask.max_retries}")

    print("\n--- Process order (success path) ---")
    items = [{"name": "Widget", "price": 9.99, "qty": 2}]
    result = process_order.delay("ORD-001", items)
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Send notification (success path) ---")
    result = send_notification.delay("user-42", "Your order shipped!")
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Unreliable task (no failures) ---")
    result = unreliable_task.delay(fail_count=0)
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\nAll task inheritance examples completed!")
