"""Starmap Workflow — KubeMQ Celery Transport.

Demonstrates:
- task.starmap(iterable): apply a task to each item in an iterable
- Each item is unpacked as positional arguments to the task
- Runs as a single task that iterates locally (not parallel)

Usage:
    # Start a worker:
    celery -A examples.canvas.starmap worker --loglevel=info

    # Run the example:
    python examples/canvas/starmap.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("starmap_example")
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
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task
def power(base: int, exp: int) -> int:
    """Raise base to the power of exp."""
    return base**exp


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Starmap Workflow Example")
    print("=" * 40)

    # starmap: apply add() to each pair
    print("\n--- add.starmap() ---")
    pairs = [(1, 2), (3, 4), (5, 6), (7, 8)]
    result = add.starmap(pairs).apply_async()
    values = result.get(timeout=10)
    print(f"Pairs:   {pairs}")
    print(f"Results: {values}")
    assert values == [3, 7, 11, 15], f"Expected [3, 7, 11, 15], got {values}"

    # starmap with power function
    print("\n--- power.starmap() ---")
    bases = [(2, 1), (2, 2), (2, 3), (2, 4), (2, 5)]
    result2 = power.starmap(bases).apply_async()
    values2 = result2.get(timeout=10)
    print(f"Pairs:   {bases}")
    print(f"Results: {values2}")
    assert values2 == [2, 4, 8, 16, 32], f"Expected [2, 4, 8, 16, 32], got {values2}"

    print("\nDone!")
