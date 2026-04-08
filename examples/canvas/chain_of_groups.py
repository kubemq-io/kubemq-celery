"""Chain of Groups — KubeMQ Celery Transport.

Demonstrates:
- Chaining groups together for multi-stage parallel processing
- Stage 1: parallel computation -> Stage 2: parallel transformation
- Each stage waits for all tasks in the previous group to complete

Usage:
    # Start a worker:
    celery -A examples.canvas.chain_of_groups worker --loglevel=info

    # Run the example:
    python examples/canvas/chain_of_groups.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, chain, chord, group

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("chain_of_groups_example")
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
def double(n: int) -> int:
    """Double a number."""
    return n * 2


@app.task
def collect(values: list[int]) -> list[int]:
    """Pass-through collector (identity function for chaining)."""
    return values


@app.task
def double_all(values: list[int]) -> list[int]:
    """Double every element in the list."""
    return [v * 2 for v in values]


@app.task
def sum_all(values: list[int]) -> int:
    """Sum all values in the list."""
    return sum(values)


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Chain of Groups Example")
    print("=" * 40)

    # Stage 1 (chord): parallel additions -> collect results
    # Stage 2 (task):  double all collected results
    # Stage 3 (task):  sum everything
    print("\nPipeline:")
    print("  Stage 1: group(add(1,2), add(3,4), add(5,6)) -> collect")
    print("  Stage 2: double_all(collected)")
    print("  Stage 3: sum_all(doubled)")

    workflow = chain(
        chord(
            group(add.s(1, 2), add.s(3, 4), add.s(5, 6)),
            collect.s(),
        ),
        double_all.s(),
        sum_all.s(),
    )

    result = workflow.apply_async()
    value = result.get(timeout=20)
    # Stage 1: [3, 7, 11]
    # Stage 2: [6, 14, 22]
    # Stage 3: 42
    print(f"Result: {value}")
    assert value == 42, f"Expected 42, got {value}"

    print("\nDone!")
