"""Reject and Requeue — KubeMQ Celery Transport.

Demonstrates:
- Reject(requeue=True) to put a message back on the queue
- Reject(requeue=False) to permanently discard a message
- Useful for conditional processing based on external state

Usage:
    # Start a worker:
    celery -A examples.error_handling.reject_and_requeue worker --loglevel=info

    # Run the example:
    python examples/error_handling/reject_and_requeue.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import random

from celery import Celery
from celery.exceptions import Reject

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("reject_and_requeue")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "task_acks_late": True,
    }
)


@app.task(bind=True, max_retries=0)
def process_if_ready(self, job_id: str) -> dict:
    """Process a job only if the external system is ready.

    If not ready, requeue the message so another worker (or the same
    worker later) can try again. Uses Reject(requeue=True) which
    calls KubeMQ's native re_queue() on the message.
    """
    is_ready = random.random() > 0.5

    if not is_ready:
        print(f"  Job {job_id}: system not ready, requeueing...")
        raise Reject(reason="external system not ready", requeue=True)

    return {"job_id": job_id, "status": "processed"}


@app.task(bind=True, max_retries=0)
def validate_and_process(self, data: dict) -> dict:
    """Validate data and either process or permanently reject.

    Invalid data is rejected WITHOUT requeue (discarded).
    """
    if not data.get("required_field"):
        print(f"  Rejecting invalid data (no requeue): {data}")
        raise Reject(reason="missing required_field", requeue=False)

    return {"data": data, "status": "valid_and_processed"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Reject and Requeue Example")
    print("=" * 40)
    print("NOTE: eager mode executes tasks in-process and does NOT exercise")
    print("      real broker ack/requeue paths. Run with a live worker to")
    print("      verify KubeMQ re_queue() and ack() behaviour.")
    print()

    print("\n--- Requeue if not ready ---")
    result = process_if_ready.delay("job-42")
    try:
        value = result.get(timeout=15)
        print(f"Result: {value}")
    except Reject:
        print("  Task was requeued (will be retried by a worker)")
    except Exception as exc:
        print(f"  Task rejected: {exc}")

    print("\n--- Permanent reject (invalid data) ---")
    result2 = validate_and_process.delay({"name": "test"})
    try:
        value2 = result2.get(timeout=10)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"  Rejected: {type(exc).__name__}: {exc}")

    print("\n--- Valid data ---")
    result3 = validate_and_process.delay({"required_field": "present", "name": "test"})
    try:
        value3 = result3.get(timeout=10)
        print(f"Result: {value3}")
    except Exception as exc:
        print(f"  Failed: {exc}")

    print("\nDone!")
