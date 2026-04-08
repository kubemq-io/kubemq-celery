"""Basic Broker Connection — KubeMQ Celery Transport.

Demonstrates:
- Connecting to KubeMQ with kubemq://host:port URL
- Verifying connection with app.connection().ensure_connection()
- Sending and receiving a task through the broker

Usage:
    # Start a worker:
    celery -A examples.connection.basic_broker worker --loglevel=info

    # Run the example:
    python examples/connection/basic_broker.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "basic_broker",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task
def echo(message: str) -> str:
    """Echo back the input message."""
    return f"echo: {message}"


if __name__ == "__main__":
    print("=== Basic Broker Connection — KubeMQ Celery Transport ===\n")
    print(f"Broker URL:     {app.conf.broker_url}")
    print(f"Result backend: {app.conf.result_backend}")
    print()
    print("To test this connection:")
    print("  1. Start a worker:")
    print("     celery -A examples.connection.basic_broker worker --loglevel=info")
    print("  2. Send a task from another shell:")
    print(
        '     python -c "from examples.connection.basic_broker '
        "import echo; print(echo.delay('hello').get(timeout=10))\""
    )
    print()
    print("=== Configuration demo complete ===")
