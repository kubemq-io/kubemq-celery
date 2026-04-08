"""Immutable Signatures — KubeMQ Celery Transport.

Demonstrates:
- .si() (immutable signature) vs .s() (mutable signature)
- .s(): previous task result is prepended as the first argument
- .si(): previous task result is NOT passed — arguments are fixed

Usage:
    # Start a worker:
    celery -A examples.canvas.immutable_signatures worker --loglevel=info

    # Run the example:
    python examples/canvas/immutable_signatures.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery, chain

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("immutable_signatures_example")
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
def log_and_return(value: int) -> int:
    """Log a value and return it unchanged."""
    print(f"  [log_and_return] received: {value}")
    return value


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Immutable Signatures Example")
    print("=" * 40)

    # Mutable (.s): result flows through the chain
    # add(2,3)=5 -> multiply(5, 10)=50
    print("\n--- Mutable chain: .s() ---")
    workflow = chain(
        add.s(2, 3),
        multiply.s(10),
    )
    result = workflow.apply_async()
    value = result.get(timeout=10)
    print(f"add(2,3) -> multiply(result, 10) = {value}")
    assert value == 50, f"Expected 50, got {value}"

    # Immutable (.si): result is NOT passed
    # add(2,3)=5 -> multiply(100, 10)=1000  (5 is discarded)
    print("\n--- Immutable chain: .si() ---")
    workflow2 = chain(
        add.s(2, 3),
        multiply.si(100, 10),
    )
    result2 = workflow2.apply_async()
    value2 = result2.get(timeout=10)
    print(f"add(2,3) -> multiply(100, 10) = {value2}")
    print("  (result of add was discarded — .si() ignores previous output)")
    assert value2 == 1000, f"Expected 1000, got {value2}"

    # Mixed chain: mutable then immutable then mutable
    print("\n--- Mixed chain ---")
    workflow3 = chain(
        add.s(5, 5),
        log_and_return.s(),
        multiply.si(7, 3),
    )
    result3 = workflow3.apply_async()
    value3 = result3.get(timeout=10)
    print(f"add(5,5)=10 -> log(10) -> multiply(7,3) = {value3}")
    assert value3 == 21, f"Expected 21, got {value3}"

    print("\nDone!")
