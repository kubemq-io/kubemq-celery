"""Chord Error Handling — KubeMQ Celery Transport.

Demonstrates:
- What happens when a chord member task fails
- Using link_error to catch chord failures
- The chord callback is not executed if any member fails

Usage:
    # Start a worker:
    celery -A examples.canvas.chord_error_handling worker --loglevel=info

    # Run the example:
    python examples/canvas/chord_error_handling.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, Task, chord, group

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("chord_error_handling_example")
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
def failing_task(x: int) -> int:
    """A task that always raises an error."""
    raise ValueError(f"Intentional failure for input {x}")


@app.task
def sum_results(values: list[int]) -> int:
    """Sum a list of integers (chord callback)."""
    return sum(values)


@app.task
def on_chord_error(request: Task, exc: Exception, traceback: str | None) -> None:
    """Error callback for chord failures.

    Called when any chord member fails, preventing the callback
    from executing.
    """
    print(f"Chord error! Task {request.id} failed: {exc}")


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Chord Error Handling Example")
    print("=" * 40)

    # Successful chord (baseline)
    print("\n--- Successful chord ---")
    workflow = chord(
        group(add.s(1, 2), add.s(3, 4)),
        sum_results.s(),
    )
    result = workflow.apply_async()
    try:
        value = result.get(timeout=15)
        print(f"Result: {value}")
        assert value == 10
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    # Chord with a failing member + error callback
    print("\n--- Chord with failing member ---")
    workflow2 = chord(
        group(
            add.s(1, 2),
            failing_task.s(99),
            add.s(5, 6),
        ),
        sum_results.s(),
    )
    workflow2.link_error(on_chord_error.s())

    try:
        result2 = workflow2.apply_async()
        value2 = result2.get(timeout=15, propagate=True)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"Chord failed as expected: {type(exc).__name__}: {exc}")

    print("\nDone!")
