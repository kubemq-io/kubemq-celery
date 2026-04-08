"""Task Annotations — KubeMQ Celery Transport.

Demonstrates:
- task_annotations config for cross-cutting concerns
- Applying rate limits, time limits, and retries via annotations
- Per-task and wildcard annotations
- Separating task logic from operational configuration

Usage:
    celery -A examples.advanced_patterns.task_annotations worker --loglevel=info
    python examples/advanced_patterns/task_annotations.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "task_annotations",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_annotations={
        "*": {
            "rate_limit": "100/m",
        },
        "examples.advanced_patterns.task_annotations.send_email": {
            "rate_limit": "10/m",
            "max_retries": 5,
            "default_retry_delay": 30,
        },
        "examples.advanced_patterns.task_annotations.process_image": {
            "rate_limit": "5/m",
            "soft_time_limit": 60,
            "time_limit": 120,
        },
        "examples.advanced_patterns.task_annotations.critical_task": {
            "rate_limit": None,
            "acks_late": True,
            "reject_on_worker_lost": True,
        },
    },
)


@app.task
def send_email(to: str, subject: str) -> dict:
    """Send email — rate limited to 10/min via annotations."""
    print(f"[send_email] to={to} subject={subject}")
    time.sleep(0.2)
    return {"to": to, "subject": subject, "status": "sent"}


@app.task
def process_image(image_id: str, operation: str) -> dict:
    """Process image — rate limited to 5/min, time-limited via annotations."""
    print(f"[process_image] image={image_id} op={operation}")
    time.sleep(0.5)
    return {"image_id": image_id, "operation": operation, "status": "processed"}


@app.task
def critical_task(payload: dict) -> dict:
    """Critical task — no rate limit, acks_late for reliability."""
    print(f"[critical_task] payload keys={list(payload.keys())}")
    time.sleep(0.1)
    return {"status": "completed", "payload_size": len(payload)}


@app.task
def default_task(value: int) -> dict:
    """Default task — inherits wildcard annotation (100/min rate limit)."""
    print(f"[default_task] value={value}")
    return {"value": value, "doubled": value * 2}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Task Annotations Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print()

    annotations = app.conf.task_annotations
    print("Configured annotations:")
    for pattern, config in annotations.items():
        print(f"  {pattern}:")
        for key, val in config.items():
            print(f"    {key}: {val}")

    print("\n--- Send email (rate limited: 10/min) ---")
    result = send_email.delay("alice@example.com", "Hello")
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Process image (rate limited: 5/min, time-limited) ---")
    result = process_image.delay("img-001", "resize")
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Critical task (no rate limit, acks_late) ---")
    result = critical_task.delay({"order_id": "ORD-001", "priority": "high"})
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Default task (wildcard: 100/min) ---")
    result = default_task.delay(42)
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\nAll task annotation examples completed!")
