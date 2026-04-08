"""Idempotent Tasks — KubeMQ Celery Transport.

Demonstrates:
- Deduplication with custom task IDs via task_id= in apply_async()
- Deterministic task ID generation for idempotent dispatch
- Checking existing results before dispatching duplicates
- Safe retry patterns for at-least-once delivery

Usage:
    celery -A examples.advanced_patterns.idempotent_tasks worker --loglevel=info
    python examples/advanced_patterns/idempotent_tasks.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any

from celery import Celery, Task
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

app = Celery(
    "idempotent_tasks",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


def deterministic_task_id(task_name: str, *args, **kwargs) -> str:
    """Generate a deterministic task ID from the task name and arguments.

    Same arguments always produce the same ID, enabling deduplication.
    """
    key = f"{task_name}:{args}:{sorted(kwargs.items())}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


@app.task(bind=True)
def process_payment(self, order_id: str, amount: float) -> dict:
    """Process a payment — must be idempotent (safe to call multiple times)."""
    print(f"[process_payment] order={order_id} amount=${amount:.2f} task_id={self.request.id}")
    time.sleep(0.5)
    return {
        "order_id": order_id,
        "amount": amount,
        "status": "charged",
        "task_id": self.request.id,
    }


@app.task(bind=True)
def send_webhook(self, url: str, payload: dict) -> dict:
    """Send a webhook — idempotent with dedup key."""
    print(f"[send_webhook] url={url} task_id={self.request.id}")
    time.sleep(0.3)
    return {"url": url, "status": "delivered", "task_id": self.request.id}


def dispatch_idempotent(task: Task, *args: Any, **kwargs: Any) -> AsyncResult:
    """Dispatch a task idempotently using a deterministic task ID.

    If a result already exists for this task+args combination,
    return the existing result instead of dispatching again.
    """
    task_id = deterministic_task_id(task.name, *args, **kwargs)

    try:
        existing = AsyncResult(task_id, app=app)
        if existing.state not in ("PENDING",):
            print(f"  Dedup hit: task {task_id[:12]}... already in state {existing.state}")
            return existing
    except (ConnectionError, TimeoutError, OSError):
        pass
    except Exception as exc:
        print(f"  Dedup check warning: {type(exc).__name__}: {exc}")

    print(f"  Dispatching new task: {task_id[:12]}...")
    return task.apply_async(args=args, kwargs=kwargs, task_id=task_id)


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Idempotent Tasks Example ===")
    print(f"Broker: {app.conf.broker_url}")

    print("\n--- Deterministic task ID generation ---")
    id1 = deterministic_task_id("process_payment", "ORD-001", 99.99)
    id2 = deterministic_task_id("process_payment", "ORD-001", 99.99)
    id3 = deterministic_task_id("process_payment", "ORD-002", 99.99)
    print(f"Same args produce same ID: {id1 == id2}")
    print(f"Different args produce different ID: {id1 != id3}")
    assert id1 == id2, "Same args should produce same ID"
    assert id1 != id3, "Different args should produce different ID"

    print("\n--- First dispatch (new task) ---")
    result1 = dispatch_idempotent(process_payment, "ORD-001", 49.99)
    value1 = result1.get(timeout=10)
    print(f"Result: {value1}")

    print("\n--- Second dispatch (same args — dedup) ---")
    result2 = dispatch_idempotent(process_payment, "ORD-001", 49.99)
    print(f"Same task ID: {result1.id == result2.id}")
    assert result1.id == result2.id, "Dedup should return same task ID"

    print("\n--- Different args (new task) ---")
    result3 = dispatch_idempotent(process_payment, "ORD-002", 79.99)
    value3 = result3.get(timeout=10)
    print(f"Result: {value3}")
    assert result3.id != result1.id, "Different args should produce new task"

    print("\n--- Custom task_id with apply_async ---")
    custom_id = "payment-ORD-003-v1"
    result = process_payment.apply_async(
        args=["ORD-003", 29.99],
        task_id=custom_id,
    )
    value = result.get(timeout=10)
    print(f"Custom task ID: {result.id}")
    print(f"Result: {value}")
    assert result.id == custom_id

    print("\nAll idempotent task examples completed!")
