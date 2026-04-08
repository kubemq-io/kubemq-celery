"""Send and Get Result — KubeMQ Celery Transport.

Demonstrates:
- .delay() vs .apply_async() for dispatching tasks
- result.get(timeout=N) for blocking retrieval
- result.id for task identification
- result.state / result.status for polling task progress

Usage:
    # Start a worker:
    celery -A examples.quickstart.send_and_get_result worker --loglevel=info

    # Run the example:
    python examples/quickstart/send_and_get_result.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "send_and_get_result",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers (simulates a short computation)."""
    time.sleep(0.5)
    return x * y


@app.task
def greet(name: str) -> str:
    """Return a greeting message."""
    return f"Hello, {name}!"


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    # --- Method 1: .delay() (simple positional args) ---
    print("=== .delay() ===")
    r1 = multiply.delay(6, 7)
    print(f"Task ID: {r1.id}")
    print(f"State immediately: {r1.state}")
    try:
        value = r1.get(timeout=10)
        print(f"Result: {value}")
        print(f"State after completion: {r1.state}")
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    # --- Method 2: .apply_async() (full control) ---
    print("\n=== .apply_async() ===")
    r2 = greet.apply_async(args=("KubeMQ",), countdown=0)
    print(f"Task ID: {r2.id}")

    # Poll state until done
    while not r2.ready():
        print(f"  Polling... state={r2.state}")
        time.sleep(0.5)

    try:
        value = r2.get(timeout=10)
        print(f"Result: {value}")
        print(f"Successful: {r2.successful()}")
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    # --- Method 3: .apply_async() with keyword arguments ---
    print("\n=== .apply_async() with kwargs ===")
    r3 = multiply.apply_async(kwargs={"x": 10, "y": 5})
    print(f"Task ID: {r3.id}")
    try:
        value = r3.get(timeout=10)
        print(f"Result: {value}")
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    print("\nDone!")
