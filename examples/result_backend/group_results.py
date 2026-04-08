"""Group Results — KubeMQ Celery Transport.

Demonstrates:
- Chord and group result aggregation
- Group metadata stored on celery-group-{id} KubeMQ channels
- Collecting results from parallel task execution
- Chord callback triggered after all group tasks complete

Usage:
    celery -A examples.result_backend.group_results worker --loglevel=info
    python examples/result_backend/group_results.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, chord, group

import kubemq_celery  # noqa: F401

app = Celery(
    "group_results",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task
def square(n: int) -> int:
    """Compute n squared."""
    return n * n


@app.task
def cube(n: int) -> int:
    """Compute n cubed."""
    return n * n * n


@app.task
def aggregate(results: list[int]) -> dict:
    """Aggregate results from a group — used as chord callback."""
    return {
        "count": len(results),
        "sum": sum(results),
        "min": min(results),
        "max": max(results),
        "values": results,
    }


@app.task
def format_results(results: list[int]) -> str:
    """Format group results as a summary string."""
    return f"Processed {len(results)} items, total={sum(results)}"


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Group Results — KubeMQ Celery Transport ===\n")

    # Basic group: parallel execution, collect all results
    print("[1] Group: square(1..5) in parallel...")
    g = group(square.s(i) for i in range(1, 6))
    result = g.apply_async()
    print(f"    Group ID: {result.id}")
    values = result.get(timeout=60)
    print(f"    Results:  {values}")
    print(f"    (Group metadata stored on celery-group-{result.id[:8]}... channel)\n")

    # Chord: group + callback when all complete
    print("[2] Chord: cube(1..4) -> aggregate(results)...")
    c = chord(
        [cube.s(i) for i in range(1, 5)],
        aggregate.s(),
    )
    result2 = c.apply_async()
    print(f"    Chord ID: {result2.id}")
    summary = result2.get(timeout=60)
    print(f"    Aggregated: {summary}\n")

    # Chord with string callback
    print("[3] Chord: square(1..6) -> format_results(...)...")
    c2 = chord(
        [square.s(i) for i in range(1, 7)],
        format_results.s(),
    )
    result3 = c2.apply_async()
    print(f"    Chord ID: {result3.id}")
    formatted = result3.get(timeout=60)
    print(f"    Formatted: {formatted}\n")

    # Nested group
    print("[4] Multiple groups collected sequentially...")
    g1 = group(square.s(i) for i in range(1, 4))
    g2 = group(cube.s(i) for i in range(1, 4))
    r1 = g1.apply_async()
    r2 = g2.apply_async()
    print(f"    Squares: {r1.get(timeout=60)}")
    print(f"    Cubes:   {r2.get(timeout=60)}\n")

    print("=== Group results aggregation complete ===")
    print("NOTE: Group metadata uses celery-group-{group_id} KubeMQ queue channels.")
    print("      Chord callbacks poll group results until all members complete.")
