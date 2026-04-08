"""Result Backend Configuration — KubeMQ Celery Transport.

Demonstrates:
- result_backend_transport_options for fine-tuning the result backend
- result_channel_prefix: custom prefix for result queue channel names
- peek_timeout: how long to wait when peeking for results (seconds)

Usage:
    # Start a worker:
    celery -A examples.connection.result_backend_config worker --loglevel=info

    # Run the example:
    python examples/connection/result_backend_config.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("result_backend_config")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "result_backend_transport_options": {
            # Custom prefix for result channels (default: "celery-result-")
            # Results stored on channels: myapp-result-<task_id>
            "result_channel_prefix": "myapp-result-",
            # Peek timeout: how long to wait when checking for results (default: 1s)
            # Higher values reduce polling frequency but increase latency
            "peek_timeout": 2,
        },
    }
)


@app.task
def compute(x: int, y: int) -> dict:
    """Perform a computation and return structured results."""
    return {
        "x": x,
        "y": y,
        "sum": x + y,
        "product": x * y,
    }


if __name__ == "__main__":
    print("=== Result Backend Configuration — KubeMQ Celery Transport ===\n")

    opts = app.conf.result_backend_transport_options
    print(f"Result channel prefix: {opts['result_channel_prefix']}")
    print(f"Peek timeout:          {opts['peek_timeout']}s")
    print(f"Result expires:        {app.conf.result_expires}s")
    print()
    print("Result channels are named: <prefix><task_id>")
    print(f"  Example: {opts['result_channel_prefix']}<task-uuid>")
    print()
    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.connection.result_backend_config worker --loglevel=info")
    print("  2. Send a task and observe result channel creation.")
    print()
    print("=== Configuration demo complete ===")
