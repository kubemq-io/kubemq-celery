"""Hello World — KubeMQ Celery Transport.

Demonstrates:
- Minimal task definition and execution
- Sending a task with .delay()
- Retrieving the result with .get()

Usage:
    # Start a worker:
    celery -A examples.quickstart.hello_world worker --loglevel=info

    # Run the example:
    python examples/quickstart/hello_world.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "hello_world",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Sending add(4, 6) to KubeMQ broker...")
    result = add.delay(4, 6)
    try:
        value = result.get(timeout=10)
        print(f"Result: {value}")
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")
    print("\nDone!")
