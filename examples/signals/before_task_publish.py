"""Before Task Publish Signal — KubeMQ Celery Transport.

Demonstrates:
- before_task_publish signal for injecting custom headers
- Adding tracing/correlation IDs to task messages
- Modifying task headers before they reach the KubeMQ broker
- Request metadata propagation across task chains

Usage:
    celery -A examples.signals.before_task_publish worker --loglevel=info
    python examples/signals/before_task_publish.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

from celery import Celery
from celery.signals import after_task_publish, before_task_publish

import kubemq_celery  # noqa: F401

app = Celery(
    "before_task_publish",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600

_published_tasks: list[dict] = []


@before_task_publish.connect
def on_before_task_publish(
    sender: str = "",
    headers: dict | None = None,
    body: Any = None,
    exchange: str = "",
    routing_key: str = "",
    **kwargs: Any,
) -> None:
    """Called just before a task message is published to the broker.

    This is the ideal place to:
    - Inject correlation/trace IDs for distributed tracing
    - Add custom metadata headers
    - Log outgoing task messages
    - Modify routing before the message reaches KubeMQ
    """
    if headers is None:
        return

    # Inject a correlation ID for distributed tracing
    if "correlation_id" not in headers:
        headers["correlation_id"] = str(uuid.uuid4())

    # Add a publish timestamp
    headers["published_at"] = time.time()

    # Add source service identifier
    headers["source_service"] = "example-publisher"

    print(
        f"[before_publish] Task {sender} -> "
        f"queue={routing_key}, correlation_id={headers['correlation_id'][:8]}..."
    )


@after_task_publish.connect
def on_after_task_publish(
    sender: str = "",
    headers: dict | None = None,
    body: Any = None,
    exchange: str = "",
    routing_key: str = "",
    **kwargs: Any,
) -> None:
    """Called after the task message has been sent to KubeMQ."""
    record = {
        "task": sender,
        "exchange": exchange,
        "routing_key": routing_key,
        "time": time.time(),
    }
    _published_tasks.append(record)
    print(f"[after_publish]  Task {sender} published to KubeMQ")


@app.task(bind=True)
def traced_task(self, data: str) -> dict:
    """A task that reads custom headers injected by the signal."""
    request = self.request
    headers = getattr(request, "headers", {}) or {}
    return {
        "data": data,
        "task_id": request.id,
        "correlation_id": headers.get("correlation_id"),
        "published_at": headers.get("published_at"),
        "source_service": headers.get("source_service"),
    }


@app.task
def simple_add(x: int, y: int) -> int:
    """Simple task to show signal fires for all tasks."""
    return x + y


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Before Task Publish Signal — KubeMQ Celery Transport ===\n")

    # Send task with auto-injected headers
    print("[1] Sending traced_task('hello')...")
    print("    before_task_publish injects correlation_id, published_at, source_service")
    result = traced_task.delay("hello")
    print(f"    Task ID: {result.id}")
    value = result.get(timeout=30)
    print(f"    Result:  {value}\n")

    # Send multiple tasks to show signal fires for each
    print("[2] Sending 3 simple_add tasks...")
    results = []
    for i in range(3):
        r = simple_add.delay(i, i + 10)
        results.append(r)
    for r in results:
        print(f"    Task {r.id[:8]}... = {r.get(timeout=30)}")
    print()

    # Show publish log
    print(f"[log] {len(_published_tasks)} tasks published with custom headers")
    for record in _published_tasks:
        print(f"    {record['task']} -> {record['routing_key']}")
    print()

    print("=== Before task publish signal demo complete ===")
    print("NOTE: before_task_publish fires on the CLIENT side (publisher).")
    print("      Use it for tracing, auditing, and header injection.")
    print("      Headers are included in the KubeMQ queue message metadata.")
