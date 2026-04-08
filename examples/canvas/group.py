"""Group Workflow — KubeMQ Celery Transport.

Demonstrates:
- Parallel task execution with group()
- Collecting results from all group members
- Dynamic group creation from iterables

Usage:
    # Start a worker:
    celery -A examples.canvas.group worker --loglevel=info

    # Run the example:
    python examples/canvas/group.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, group

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("group_example")
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
    print("Group Workflow Example")
    print("=" * 40)

    # Static group: run 4 additions in parallel
    print("\n--- Static group ---")
    workflow = group(
        add.s(1, 2),
        add.s(3, 4),
        add.s(5, 6),
        add.s(7, 8),
    )
    result = workflow.apply_async()
    values = result.get(timeout=10)
    print(f"Results: {values}")
    assert sorted(values) == [3, 7, 11, 15]

    # Dynamic group from iterable
    print("\n--- Dynamic group (squares of 1..5) ---")
    workflow2 = group(square.s(i) for i in range(1, 6))
    result2 = workflow2.apply_async()
    values2 = result2.get(timeout=10)
    print(f"Results: {values2}")
    assert sorted(values2) == [1, 4, 9, 16, 25]

    # Large group
    print("\n--- Large group (20 tasks) ---")
    workflow3 = group(add.s(i, i) for i in range(20))
    result3 = workflow3.apply_async()
    values3 = result3.get(timeout=30)
    print(f"Results: {values3}")
    assert len(values3) == 20

    print("\nDone!")
