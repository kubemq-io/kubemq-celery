"""Error Callbacks — KubeMQ Celery Transport.

Demonstrates:
- link_error: attach an error callback to a task
- Error callbacks receive the task ID of the failed task
- Chaining error handlers for notification and cleanup

Usage:
    # Start a worker:
    celery -A examples.error_handling.error_callbacks worker --loglevel=info

    # Run the example:
    python examples/error_handling/error_callbacks.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "error_callbacks",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task
def process_payment(order_id: str, amount: float) -> dict:
    """Process a payment — fails for amounts over 10000."""
    if amount > 10000:
        raise ValueError(f"Amount {amount} exceeds limit for order {order_id}")
    return {"order_id": order_id, "amount": amount, "status": "charged"}


@app.task
def notify_failure(task_id: str) -> dict:
    """Error callback for link_error — receives the failed task's ID as a string."""
    print(f"  [notify_failure] Task {task_id} failed — alerting ops team")
    return {"task_id": task_id, "notification": "sent"}


@app.task
def rollback_payment(task_id: str) -> dict:
    """Error callback for link_error — receives the failed task's ID as a string."""
    print(f"  [rollback_payment] Rolling back for failed task {task_id}")
    return {"task_id": task_id, "rollback": "completed"}


@app.task
def successful_callback(result: dict) -> dict:
    """Success callback: called when process_payment succeeds."""
    print(f"  [success] Payment processed: {result}")
    return {"confirmed": True, "result": result}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Error Callbacks Example")
    print("=" * 40)

    # Successful task with success callback
    print("\n--- Successful payment ---")
    result = process_payment.apply_async(
        args=("ORD-001", 99.99),
        link=successful_callback.s(),
    )
    value = result.get(timeout=10)
    print(f"Result: {value}")

    # Failed task with error callbacks
    print("\n--- Failed payment (triggers error callbacks) ---")
    try:
        result2 = process_payment.apply_async(
            args=("ORD-002", 15000.00),
            link_error=[
                notify_failure.s(),
                rollback_payment.s(),
            ],
        )
        result2.get(timeout=10)
    except Exception as exc:
        print(f"Task failed: {type(exc).__name__}: {exc}")
        print("  Error callbacks (notify + rollback) were triggered")

    print("\nDone!")
