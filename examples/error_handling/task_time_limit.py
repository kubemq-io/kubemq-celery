"""Task Time Limit — KubeMQ Celery Transport.

Demonstrates:
- time_limit: hard time limit (kills the task worker process)
- soft_time_limit: soft time limit (raises SoftTimeLimitExceeded)
- Catching SoftTimeLimitExceeded for graceful cleanup

Usage:
    # Start a worker:
    celery -A examples.error_handling.task_time_limit worker --loglevel=info

    # Run the example:
    python examples/error_handling/task_time_limit.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "task_time_limit",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task(soft_time_limit=5, time_limit=10)
def long_running_task(duration: int) -> dict:
    """A task with both soft and hard time limits.

    soft_time_limit=5:  raises SoftTimeLimitExceeded after 5s
    time_limit=10:      kills worker process after 10s (hard limit)

    Catches the soft limit exception for graceful cleanup.
    """
    try:
        print(f"  Starting work (requested {duration}s)...")
        for i in range(duration):
            time.sleep(1)
            print(f"  Progress: {i + 1}/{duration}s")
        return {"duration": duration, "status": "completed"}
    except SoftTimeLimitExceeded:
        print("  Soft time limit reached — performing cleanup...")
        return {"duration": duration, "status": "partial", "reason": "soft_time_limit"}


@app.task(time_limit=3)
def hard_limit_task(duration: int) -> dict:
    """A task with only a hard time limit.

    If the task exceeds 3 seconds, the worker process is terminated
    and the task is marked as failed with WorkerLostError.
    """
    time.sleep(duration)
    return {"duration": duration, "status": "completed"}


@app.task(soft_time_limit=2)
def quick_task(item: str) -> dict:
    """A fast task with a conservative soft limit."""
    time.sleep(0.5)
    return {"item": item, "status": "processed"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Task Time Limit Example")
    print("=" * 40)

    # Task that completes within the limit
    print("\n--- Quick task (within limits) ---")
    result = quick_task.delay("fast-item")
    value = result.get(timeout=10)
    print(f"Result: {value}")

    # Task that hits the soft time limit
    print("\n--- Long task (hits soft limit at 5s) ---")
    result2 = long_running_task.delay(8)
    try:
        value2 = result2.get(timeout=15)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"Failed: {type(exc).__name__}: {exc}")

    # Task that completes within hard limit
    print("\n--- Hard limit task (within 3s) ---")
    result3 = hard_limit_task.delay(1)
    try:
        value3 = result3.get(timeout=10)
        print(f"Result: {value3}")
    except Exception as exc:
        print(f"Failed: {type(exc).__name__}: {exc}")

    print("\nDone!")
