"""Canvas workflow examples with KubeMQ Celery transport.

Demonstrates all five Celery canvas primitives:
- Chain: sequential pipeline with result passing
- Group: parallel execution with result aggregation
- Chord: group + callback (uses polling fallback with KubeMQ)
- Starmap: parallel execution over an iterable
- Chunks: splitting large iterables into batches

Run:
    # Terminal 1: start worker
    celery -A canvas_workflows worker --loglevel=info

    # Terminal 2: run examples
    python examples/canvas_workflows.py

NOTE: Chords use Celery's polling fallback (chord_unlock task) with KubeMQ.
This is functionally correct but has slightly higher latency (~1-2s) than
Redis's native chord unlock mechanism.
"""

from __future__ import annotations

import os
import sys

import kubemq_celery  # noqa: F401
from celery import Celery, chain, chord, group

app = Celery("canvas_workflows")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 86400,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }
)


# --- Task Definitions ---


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task
def subtract(x: int, y: int) -> int:
    """Subtract y from x."""
    return x - y


@app.task
def sum_results(values: list[int]) -> int:
    """Sum a list of integers (used as chord callback)."""
    return sum(values)


@app.task
def square(x: int) -> int:
    """Square a number."""
    return x * x


@app.task
def process_batch(items: list[int]) -> list[int]:
    """Process a batch of items (double each)."""
    return [item * 2 for item in items]


# --- Workflow Examples ---


def example_chain():
    """Chain: sequential pipeline with result passing.

    Pipeline: add(2, 3) -> multiply(result, 10) -> subtract(result, 5)
    Expected: (2+3) * 10 - 5 = 45
    """
    print("\n=== Chain Example ===")
    workflow = chain(
        add.s(2, 3),           # step 1: 2 + 3 = 5
        multiply.s(10),        # step 2: 5 * 10 = 50
        subtract.s(5),         # step 3: 50 - 5 = 45
    )
    result = workflow.apply_async()
    value = result.get(timeout=10)
    print(f"Chain result: {value}")  # Expected: 45
    assert value == 45, f"Expected 45, got {value}"


def example_group():
    """Group: parallel execution with result aggregation.

    Execute 4 additions in parallel and collect all results.
    Expected: [3, 7, 11, 15]
    """
    print("\n=== Group Example ===")
    workflow = group(
        add.s(1, 2),   # 3
        add.s(3, 4),   # 7
        add.s(5, 6),   # 11
        add.s(7, 8),   # 15
    )
    result = workflow.apply_async()
    values = result.get(timeout=10)
    print(f"Group results: {values}")  # Expected: [3, 7, 11, 15]
    assert sorted(values) == [3, 7, 11, 15], f"Unexpected: {values}"


def example_chord():
    """Chord: group + callback.

    Execute group of additions in parallel, then sum all results.
    Expected: sum([3, 7, 11]) = 21

    NOTE: KubeMQ uses Celery's polling fallback for chord unlock.
    The chord_unlock task polls the result backend until all group
    results are available. This adds ~1-2s latency vs Redis.
    """
    print("\n=== Chord Example ===")
    workflow = chord(
        group(
            add.s(1, 2),   # 3
            add.s(3, 4),   # 7
            add.s(5, 6),   # 11
        ),
        sum_results.s(),   # callback: sum([3, 7, 11]) = 21
    )
    result = workflow.apply_async()
    value = result.get(timeout=15)  # extra timeout for polling
    print(f"Chord result: {value}")  # Expected: 21
    assert value == 21, f"Expected 21, got {value}"


def example_starmap():
    """Starmap: parallel execution over an iterable.

    Apply square() to each value in the list.
    Expected: [1, 4, 9, 16, 25]
    """
    print("\n=== Starmap Example ===")
    items = [(1,), (2,), (3,), (4,), (5,)]
    workflow = group(square.s(x) for (x,) in items)
    result = workflow.apply_async()
    values = result.get(timeout=10)
    print(f"Starmap results: {values}")  # Expected: [1, 4, 9, 16, 25]
    assert sorted(values) == [1, 4, 9, 16, 25], f"Unexpected: {values}"


def example_chunks():
    """Chunks: splitting large iterables into batches.

    Split 10 items into batches of 3, process each batch.
    Expected: 4 batches -> [[0,2,4], [6,8,10], [12,14,16], [18]]
    """
    print("\n=== Chunks Example ===")
    items = list(range(10))
    workflow = process_batch.chunks(
        [(batch,) for batch in [items[i:i + 3] for i in range(0, len(items), 3)]],
        1,
    )
    result = workflow.apply_async()
    values = result.get(timeout=10)
    print(f"Chunks results: {values}")
    total_items = sum(len(batch) for batch in values)
    assert total_items == 10, f"Expected 10 items total, got {total_items}"


if __name__ == "__main__":
    print("Running Canvas workflow examples...")
    print(f"Broker: {app.conf.broker_url}")

    example_chain()
    example_group()
    example_chord()
    example_starmap()
    example_chunks()

    print("\nAll canvas workflow examples completed successfully!")
