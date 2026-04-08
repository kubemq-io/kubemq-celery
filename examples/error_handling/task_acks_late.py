"""Task Acks Late — KubeMQ Celery Transport.

Demonstrates:
- task_acks_late=True: message is acknowledged AFTER task completes
- Default (False): message is acknowledged BEFORE task runs
- Late ack prevents message loss if the worker crashes mid-task
- KubeMQ uses native ack/nack for manual acknowledgment

Usage:
    # Start a worker:
    celery -A examples.error_handling.task_acks_late worker --loglevel=info

    # Run the example:
    python examples/error_handling/task_acks_late.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("task_acks_late")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        # Global setting: acknowledge messages AFTER task completion
        "task_acks_late": True,
        # Reject and requeue if worker is killed mid-task
        "task_reject_on_worker_lost": True,
        # Only prefetch 1 task at a time (pairs well with acks_late)
        "worker_prefetch_multiplier": 1,
    }
)


@app.task
def critical_operation(item_id: str) -> dict:
    """A task where message loss is unacceptable.

    With acks_late=True:
    1. Worker receives message (not acked yet)
    2. Task executes
    3. If task succeeds -> ack (message consumed)
    4. If worker crashes -> nack (message redelivered by KubeMQ)
    """
    print(f"  Processing critical item: {item_id}")
    time.sleep(1.0)
    return {"item_id": item_id, "status": "completed"}


@app.task(acks_late=False)
def non_critical_task(data: str) -> dict:
    """A task where early ack is acceptable.

    Per-task override: acks_late=False (default Celery behavior).
    Message is acked before execution — faster but risks message
    loss if the worker crashes during processing.
    """
    return {"data": data, "status": "processed"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Task Acks Late Example")
    print("=" * 40)

    print(f"\nGlobal task_acks_late: {app.conf.task_acks_late}")
    print(f"task_reject_on_worker_lost: {app.conf.task_reject_on_worker_lost}")
    print(f"worker_prefetch_multiplier: {app.conf.worker_prefetch_multiplier}")

    print("\n--- Critical operation (acks_late=True) ---")
    result = critical_operation.delay("order-12345")
    value = result.get(timeout=15)
    print(f"Result: {value}")

    print("\n--- Non-critical task (acks_late=False override) ---")
    result2 = non_critical_task.delay("cache-refresh")
    value2 = result2.get(timeout=10)
    print(f"Result: {value2}")

    print("\nDone!")
