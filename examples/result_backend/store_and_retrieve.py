"""Store and Retrieve Results — KubeMQ Celery Transport.

Demonstrates:
- Storing task results via KubeMQ's queue-peek result backend
- Retrieving results with result.get() from multiple readers
- Non-destructive peek allowing concurrent result access
- AsyncResult lookups by task ID

Usage:
    celery -A examples.result_backend.store_and_retrieve worker --loglevel=info
    python examples/result_backend/store_and_retrieve.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

app = Celery(
    "store_and_retrieve",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task
def compute_factorial(n: int) -> int:
    """Compute factorial of n."""
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


@app.task
def build_report(title: str, items: list[str]) -> dict:
    """Build a structured report."""
    return {
        "title": title,
        "item_count": len(items),
        "items": items,
        "generated_at": time.time(),
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Store and Retrieve Results — KubeMQ Queue-Peek Backend ===\n")

    # Send a task and get its result
    print("[1] Sending compute_factorial(10)...")
    result = compute_factorial.delay(10)
    task_id = result.id
    print(f"    Task ID: {task_id}")
    value = result.get(timeout=30)
    print(f"    Result:  {value}")
    print(f"    State:   {result.state}\n")

    # Multiple readers can peek the same result (non-destructive)
    if app.conf.task_always_eager:
        print("[2] AsyncResult re-read skipped (eager mode — results stored in-process only)")
        print(f"    Original result: {value}\n")
    else:
        print("[2] Re-reading the same result via AsyncResult (simulates second reader)...")
        second_reader = AsyncResult(task_id, app=app)
        value2 = second_reader.get(timeout=10)
        print(f"    Reader 2 got: {value2}")
        print(f"    Same result:  {value == value2}\n")

    # Structured result
    print("[3] Sending build_report(...)...")
    result3 = build_report.delay("Weekly Summary", ["item-a", "item-b", "item-c"])
    print(f"    Task ID: {result3.id}")
    report = result3.get(timeout=30)
    print(f"    Report title: {report['title']}")
    print(f"    Item count:   {report['item_count']}")
    print(f"    State:        {result3.state}\n")

    # Batch results
    print("[4] Sending batch of 5 factorial tasks...")
    results = [compute_factorial.delay(i) for i in range(5, 10)]
    for r in results:
        val = r.get(timeout=30)
        print(f"    Task {r.id[:8]}... = {val}")

    print("\n=== All results stored and retrieved via KubeMQ queue-peek backend ===")
