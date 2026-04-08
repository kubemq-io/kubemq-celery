"""Chain Workflow — KubeMQ Celery Transport.

Demonstrates:
- Sequential task execution with chain()
- Result passing between chained tasks via .s() (signature)
- The output of each task becomes the first argument of the next

Usage:
    # Start a worker:
    celery -A examples.canvas.chain worker --loglevel=info

    # Run the example:
    python examples/canvas/chain.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, chain

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("chain_example")
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
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task
def subtract(x: int, y: int) -> int:
    """Subtract y from x."""
    return x - y


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Chain Workflow Example")
    print("=" * 40)

    # Pipeline: add(2, 3) -> multiply(result, 10) -> subtract(result, 5)
    # Expected: (2+3) * 10 - 5 = 45
    workflow = chain(
        add.s(2, 3),
        multiply.s(10),
        subtract.s(5),
    )

    print("Pipeline: add(2,3) -> multiply(·,10) -> subtract(·,5)")
    result = workflow.apply_async()
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value == 45, f"Expected 45, got {value}"

    # Longer chain
    workflow2 = chain(
        add.s(1, 1),
        multiply.s(3),
        add.s(4),
        multiply.s(2),
    )

    print("\nPipeline: add(1,1) -> multiply(·,3) -> add(·,4) -> multiply(·,2)")
    result2 = workflow2.apply_async()
    value2 = result2.get(timeout=10)
    print(f"Result: {value2}")
    assert value2 == 20, f"Expected 20, got {value2}"

    print("\nDone!")
