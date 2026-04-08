"""Dead Letter Queue — KubeMQ Celery Transport.

Demonstrates:
- KubeMQ's native dead letter queue (DLQ) support
- max_receive_count: messages move to DLQ after N failed deliveries
- dead_letter_queue: the channel name where poison messages are sent
- IMPORTANT: max_receive_count > 0 REQUIRES dead_letter_queue to be set

Usage:
    # Start a worker:
    celery -A examples.error_handling.dead_letter_queue worker --loglevel=info

    # Run the example:
    python examples/error_handling/dead_letter_queue.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("dead_letter_queue")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "broker_transport_options": {
            # After 3 failed receive attempts, the message moves to the DLQ.
            # max_receive_count > 0 REQUIRES dead_letter_queue to be set;
            # otherwise KubeMQCeleryConfigError is raised at startup.
            "max_receive_count": 3,
            "dead_letter_queue": "celery-dead-letters",
        },
    }
)


@app.task
def process_order(order_id: str) -> dict:
    """Process an order — may fail on certain inputs."""
    if order_id.startswith("BAD"):
        raise ValueError(f"Invalid order: {order_id}")
    return {"order_id": order_id, "status": "processed"}


@app.task
def process_dlq_message(message: dict) -> dict:
    """Process a message from the dead letter queue.

    In production, you'd read from the DLQ channel and handle
    poison messages (log, alert, manual review, etc.).
    """
    return {"original": message, "action": "logged_for_review"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Dead Letter Queue Example")
    print("=" * 40)

    opts = app.conf.broker_transport_options
    print(f"\nmax_receive_count: {opts['max_receive_count']}")
    print(f"dead_letter_queue: {opts['dead_letter_queue']}")
    print()
    print("Validation rule:")
    print("  max_receive_count > 0 REQUIRES dead_letter_queue to be set.")
    print("  Setting max_receive_count=3 without dead_letter_queue raises:")
    print("  KubeMQCeleryConfigError at startup.")

    # Good order — processes normally
    print("\n--- Normal order ---")
    result = process_order.delay("ORD-12345")
    try:
        value = result.get(timeout=10)
        print(f"Result: {value}")
    except Exception as exc:
        print(f"Failed: {exc}")

    # Bad order — will fail and eventually be sent to DLQ after
    # max_receive_count exhausted
    print("\n--- Poison message (will go to DLQ after 3 attempts) ---")
    try:
        result2 = process_order.delay("BAD-99999")
        value2 = result2.get(timeout=10)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"Task failed: {type(exc).__name__}: {exc}")
        print(f"  After {opts['max_receive_count']} delivery attempts,")
        print(f"  message moves to '{opts['dead_letter_queue']}' channel.")

    print("\nDone!")
