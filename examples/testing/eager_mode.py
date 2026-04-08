"""Eager Mode Testing — KubeMQ Celery Transport.

Demonstrates:
- task_always_eager=True for synchronous in-process execution
- Testing tasks without a running broker or worker
- Immediate result availability without .get()
- Eager mode limitations and when to use it

Usage:
    python examples/testing/eager_mode.py

Requirements:
    - kubemq-celery installed
    - No broker needed — tasks execute in-process
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "eager_mode",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task(bind=True, max_retries=3)
def divide(self, x: float, y: float) -> float:
    """Divide x by y with error handling."""
    if y == 0:
        raise ZeroDivisionError("Cannot divide by zero")
    return x / y


@app.task
def process_items(items: list[dict]) -> dict:
    """Process a list of items and return summary."""
    total = sum(item.get("value", 0) for item in items)
    return {"count": len(items), "total": total}


if __name__ == "__main__":
    print("=== Eager Mode Testing Example ===")
    print(f"task_always_eager: {app.conf.task_always_eager}")
    print(f"task_eager_propagates: {app.conf.task_eager_propagates}")
    print("No broker connection needed!\n")

    print("--- Basic task execution ---")
    result = add.delay(2, 3)
    print(f"add(2, 3) = {result.result}")
    print(f"  Status: {result.status}")
    print(f"  Ready:  {result.ready()}")
    assert result.result == 5
    assert result.status == "SUCCESS"
    assert result.ready() is True

    print("\n--- Chained computation ---")
    r1 = add.delay(10, 20)
    r2 = multiply.delay(r1.result, 3)
    print(f"add(10, 20) = {r1.result}")
    print(f"multiply(30, 3) = {r2.result}")
    assert r2.result == 90

    print("\n--- Error propagation ---")
    try:
        divide.delay(10, 0)
        print("ERROR: Expected ZeroDivisionError!")
    except ZeroDivisionError as e:
        print(f"Caught expected error: {e}")
        print("  task_eager_propagates=True raises exceptions directly")

    print("\n--- Successful division ---")
    result = divide.delay(10, 3)
    print(f"divide(10, 3) = {result.result:.4f}")

    print("\n--- Complex data ---")
    items = [{"name": "a", "value": 10}, {"name": "b", "value": 20}]
    result = process_items.delay(items)
    print(f"process_items -> {result.result}")
    assert result.result["count"] == 2
    assert result.result["total"] == 30

    print("\nAll eager mode tests passed!")
    print("\nNOTE: Eager mode is for testing only. In production, tasks")
    print("      are dispatched to the KubeMQ broker and run by workers.")
