"""Chord Workflow — KubeMQ Celery Transport.

Demonstrates:
- chord(group, callback): run tasks in parallel, then aggregate
- KubeMQ uses Celery's polling fallback for chord unlock (chord_unlock task)
- Polling adds ~1-2s latency vs Redis's native chord unlock

Usage:
    # Start a worker:
    celery -A examples.canvas.chord worker --loglevel=info

    # Run the example:
    python examples/canvas/chord.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, chord, group

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("chord_example")
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
def sum_results(values: list[int]) -> int:
    """Sum a list of integers (chord callback)."""
    return sum(values)


@app.task
def format_total(total: int) -> str:
    """Format the total as a string (chord callback)."""
    return f"Grand total: {total}"


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Chord Workflow Example")
    print("=" * 40)
    print("NOTE: KubeMQ uses Celery's polling fallback (chord_unlock task).")
    print("      This adds ~1-2s latency vs Redis's native chord unlock.")

    # Basic chord: parallel additions, then sum
    print("\n--- Basic chord ---")
    workflow = chord(
        group(
            add.s(1, 2),
            add.s(3, 4),
            add.s(5, 6),
        ),
        sum_results.s(),
    )
    result = workflow.apply_async()
    value = result.get(timeout=15)
    print(f"sum([3, 7, 11]) = {value}")
    assert value == 21, f"Expected 21, got {value}"

    # Chord with dynamic group
    print("\n--- Dynamic chord ---")
    workflow2 = chord(
        group(add.s(i, i * 2) for i in range(1, 6)),
        sum_results.s(),
    )
    result2 = workflow2.apply_async()
    value2 = result2.get(timeout=15)
    print(f"sum([3, 6, 9, 12, 15]) = {value2}")
    assert value2 == 45, f"Expected 45, got {value2}"

    print("\nDone!")
