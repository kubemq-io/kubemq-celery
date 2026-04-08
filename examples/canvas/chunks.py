"""Chunks Workflow — KubeMQ Celery Transport.

Demonstrates:
- task.chunks(items, chunk_size): split work into batches
- Each chunk runs as a separate task for parallel processing
- Useful for processing large datasets without overwhelming workers

Usage:
    # Start a worker:
    celery -A examples.canvas.chunks worker --loglevel=info

    # Run the example:
    python examples/canvas/chunks.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("chunks_example")
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
def square(n: int) -> int:
    """Square a number."""
    return n * n


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Chunks Workflow Example")
    print("=" * 40)

    # Split 10 additions into chunks of 3
    print("\n--- add.chunks (10 items, chunk_size=3) ---")
    items = [(i, i + 1) for i in range(10)]
    result = add.chunks(items, 3).apply_async()
    values = result.get(timeout=15)
    print(f"Items:   {items}")
    print(f"Results: {values}")
    flat = [v for chunk in values for v in chunk]
    expected = [i + (i + 1) for i in range(10)]
    assert flat == expected, f"Expected {expected}, got {flat}"

    # Chunks with single-arg task
    print("\n--- square.chunks (8 items, chunk_size=4) ---")
    items2 = [(i,) for i in range(1, 9)]
    result2 = square.chunks(items2, 4).apply_async()
    values2 = result2.get(timeout=15)
    print(f"Items:   {[i[0] for i in items2]}")
    print(f"Results: {values2}")
    flat2 = [v for chunk in values2 for v in chunk]
    expected2 = [i * i for i in range(1, 9)]
    assert flat2 == expected2, f"Expected {expected2}, got {flat2}"

    print("\nDone!")
