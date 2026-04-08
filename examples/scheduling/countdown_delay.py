"""Countdown Delay — KubeMQ Celery Transport.

Demonstrates:
- task.apply_async(countdown=N) for delayed execution
- KubeMQ native delay_in_seconds (no client-side polling)
- Multiple tasks with different countdown values
- Countdown precision and behavior

Usage:
    celery -A examples.scheduling.countdown_delay worker --loglevel=info
    python examples/scheduling/countdown_delay.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("countdown_delay")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
    }
)


@app.task
def delayed_greeting(name: str) -> dict:
    """A greeting that arrives after a countdown delay."""
    return {
        "message": f"Hello, {name}!",
        "delivered_at": time.time(),
    }


@app.task
def scheduled_cleanup(resource_id: str) -> dict:
    """Clean up a resource after a delay."""
    return {
        "resource_id": resource_id,
        "action": "cleaned_up",
        "at": time.time(),
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("NOTE: Running in eager mode — broker-side delays are not observed.")
    print("=== Countdown Delay — KubeMQ Celery Transport ===\n")

    print("countdown=N tells KubeMQ to hold the message for N seconds")
    print("using native delay_in_seconds — no client-side polling.\n")

    # Countdown examples
    publish_time = time.time()

    print("[1] Sending delayed_greeting with countdown=5...")
    r1 = delayed_greeting.apply_async(args=("Alice",), countdown=5)
    print(f"    Task ID: {r1.id}")
    print(f"    Published at:  {publish_time:.2f}")
    print("    Waiting for delivery...\n")

    print("[2] Sending delayed_greeting with countdown=3...")
    r2 = delayed_greeting.apply_async(args=("Bob",), countdown=3)
    print(f"    Task ID: {r2.id}")
    print("    This should arrive BEFORE the 5-second task.\n")

    print("[3] Sending scheduled_cleanup with countdown=10...")
    r3 = scheduled_cleanup.apply_async(args=("res-001",), countdown=10)
    print(f"    Task ID: {r3.id}\n")

    # Wait for results in order of expected delivery
    print("Waiting for results...\n")

    val2 = r2.get(timeout=30)
    delay2 = val2["delivered_at"] - publish_time
    print(f"    [3s countdown] {val2['message']} (actual delay: {delay2:.1f}s)")

    val1 = r1.get(timeout=30)
    delay1 = val1["delivered_at"] - publish_time
    print(f"    [5s countdown] {val1['message']} (actual delay: {delay1:.1f}s)")

    val3 = r3.get(timeout=30)
    delay3 = val3["at"] - publish_time
    print(f"    [10s countdown] {val3['resource_id']} cleaned up (actual delay: {delay3:.1f}s)")

    print("\n=== Countdown delay demo complete ===")
    print("NOTE: KubeMQ holds delayed messages server-side.")
    print("      Maximum delay: 86400 seconds (24 hours).")
    print("      Delays exceeding 24h are capped with a warning.")
