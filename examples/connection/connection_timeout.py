"""Connection Timeout — KubeMQ Celery Transport.

Demonstrates:
- Setting connection_timeout for initial broker connection
- Controls how long the gRPC client waits to establish a connection
- Useful for slow networks or remote brokers

Usage:
    # Start a worker:
    celery -A examples.connection.connection_timeout worker --loglevel=info

    # Run the example:
    python examples/connection/connection_timeout.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("connection_timeout")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "broker_transport_options": {
            # Wait up to 10 seconds to establish the initial gRPC connection
            "connection_timeout": 10.0,
        },
        "result_backend_transport_options": {
            "connection_timeout": 10.0,
        },
    }
)


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


if __name__ == "__main__":
    print("=== Connection Timeout — KubeMQ Celery Transport ===\n")

    timeout = app.conf.broker_transport_options["connection_timeout"]
    print(f"Connection timeout: {timeout}s")
    print()
    print("The connection_timeout controls how long the gRPC client waits")
    print("to establish the initial connection to the KubeMQ broker.")
    print()
    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.connection.connection_timeout worker --loglevel=info")
    print("  2. The worker will use the configured timeout for connecting.")
    print()
    print("=== Configuration demo complete ===")
