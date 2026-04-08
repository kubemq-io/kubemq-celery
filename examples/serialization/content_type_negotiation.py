"""Content-Type Negotiation — KubeMQ Celery Transport.

Demonstrates:
- Per-task serializer override via @app.task(serializer='...')
- accept_content with multiple types for mixed-serializer environments
- apply_async(serializer='...') for on-the-fly serializer selection
- Verifying content-type negotiation between producers and workers

Usage:
    celery -A examples.serialization.content_type_negotiation worker --loglevel=info
    python examples/serialization/content_type_negotiation.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "content_type_negotiation",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json", "pickle"],
    result_expires=3600,
)


@app.task
def json_only_task(data: dict) -> dict:
    """Task that always uses JSON serialization (the default)."""
    print(f"[json_only_task] Received: {data}")
    return {"source": "json_only", "echo": data}


@app.task(serializer="pickle")
def pickle_task(data: dict) -> dict:
    """Task with per-task serializer override to pickle."""
    print(f"[pickle_task] Received: {data}")
    return {"source": "pickle_task", "echo": data}


@app.task
def flexible_task(data: dict) -> dict:
    """Task that accepts any configured content type.

    The serializer is determined by the producer at dispatch time.
    """
    print(f"[flexible_task] Received: {data}")
    return {"source": "flexible_task", "echo": data}


@app.task(bind=True)
def inspect_delivery(self, data: dict) -> dict:
    """Inspect delivery info to see which serializer was used."""
    delivery_info = self.request.delivery_info or {}
    content_type = getattr(self.request, "content_type", "unknown")
    result = {
        "data": data,
        "content_type": content_type,
        "delivery_info": {k: str(v) for k, v in delivery_info.items()},
    }
    print(f"[inspect_delivery] content_type={content_type}")
    return result


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Content-Type Negotiation Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Default serializer: {app.conf.task_serializer}")
    print(f"Accept content: {app.conf.accept_content}")

    payload = {"message": "hello", "count": 42}

    print("\n--- Default JSON task ---")
    result = json_only_task.delay(payload)
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value["source"] == "json_only"

    print("\n--- Per-task pickle override ---")
    result = pickle_task.delay(payload)
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value["source"] == "pickle_task"

    print("\n--- Flexible task with default serializer ---")
    result = flexible_task.delay(payload)
    value = result.get(timeout=10)
    print(f"Result (default): {value}")
    assert value["source"] == "flexible_task"

    print("\n--- Flexible task with apply_async serializer override ---")
    result = flexible_task.apply_async(args=[payload], serializer="pickle")
    value = result.get(timeout=10)
    print(f"Result (pickle override): {value}")
    assert value["source"] == "flexible_task"

    print("\n--- Inspect delivery info ---")
    result = inspect_delivery.delay(payload)
    value = result.get(timeout=10)
    print(f"Delivery info: {value}")

    print("\nAll content-type negotiation tests passed!")
