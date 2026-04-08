"""Map Workflow — KubeMQ Celery Transport.

Demonstrates:
- task.map(iterable): apply a task to each item sequentially
- Each item is passed as the sole argument
- Runs as a single task that processes items one by one

Usage:
    # Start a worker:
    celery -A examples.canvas.map worker --loglevel=info

    # Run the example:
    python examples/canvas/map.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("map_example")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }
)


@app.task
def double(n: int) -> int:
    """Double a number."""
    return n * 2


@app.task
def to_upper(text: str) -> str:
    """Convert text to uppercase."""
    return text.upper()


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Map Workflow Example")
    print("=" * 40)

    # Map: apply double() to each number
    print("\n--- double.map([1, 2, 3, 4, 5]) ---")
    result = double.map([1, 2, 3, 4, 5]).apply_async()
    values = result.get(timeout=10)
    print(f"Results: {values}")
    assert values == [2, 4, 6, 8, 10], f"Expected [2, 4, 6, 8, 10], got {values}"

    # Map: apply to_upper() to each string
    print("\n--- to_upper.map(['hello', 'kubemq', 'celery']) ---")
    result2 = to_upper.map(["hello", "kubemq", "celery"]).apply_async()
    values2 = result2.get(timeout=10)
    print(f"Results: {values2}")
    assert values2 == ["HELLO", "KUBEMQ", "CELERY"]

    print("\nDone!")
