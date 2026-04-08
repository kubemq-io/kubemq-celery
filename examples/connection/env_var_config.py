"""Environment Variable Configuration — KubeMQ Celery Transport.

Demonstrates:
- Full Celery + KubeMQ configuration via environment variables
- Production-ready pattern for containerized deployments
- All connection, transport, and result backend options via env

Usage:
    # Set environment variables:
    export CELERY_BROKER_URL=kubemq://broker.example.com:50000
    export CELERY_RESULT_BACKEND=kubemq://broker.example.com:50000
    export KUBEMQ_AUTH_TOKEN=my-secret-token
    export KUBEMQ_CONNECTION_TIMEOUT=15
    export KUBEMQ_MAX_SEND_SIZE=8388608
    export KUBEMQ_WAIT_TIMEOUT=2
    export KUBEMQ_DEAD_LETTER_QUEUE=my-dlq
    export KUBEMQ_MAX_RECEIVE_COUNT=5

    # Start a worker:
    celery -A examples.connection.env_var_config worker --loglevel=info

    # Run the example:
    python examples/connection/env_var_config.py

Requirements:
    - Running KubeMQ broker (configure via CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport


def _safe_int(raw: str, default: int) -> int:
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


def _safe_float(raw: str, default: float) -> float:
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


app = Celery("env_var_config")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": _safe_int(os.environ.get("CELERY_RESULT_EXPIRES", "3600"), 3600),
        "task_serializer": os.environ.get("CELERY_TASK_SERIALIZER", "json"),
        "result_serializer": os.environ.get("CELERY_RESULT_SERIALIZER", "json"),
        "accept_content": ["json"],
        "timezone": os.environ.get("CELERY_TIMEZONE", "UTC"),
        "broker_transport_options": {
            "auth_token": os.environ.get("KUBEMQ_AUTH_TOKEN", None),
            "connection_timeout": _safe_float(
                os.environ.get("KUBEMQ_CONNECTION_TIMEOUT", "10"), 10.0
            ),
            "wait_timeout": _safe_int(os.environ.get("KUBEMQ_WAIT_TIMEOUT", "1"), 1),
            "max_send_size": _safe_int(os.environ.get("KUBEMQ_MAX_SEND_SIZE", "4194304"), 4194304),
            "max_receive_size": _safe_int(
                os.environ.get("KUBEMQ_MAX_RECEIVE_SIZE", "4194304"), 4194304
            ),
            "grpc_keepalive_time": _safe_int(os.environ.get("KUBEMQ_KEEPALIVE_TIME", "30"), 30),
            "grpc_keepalive_timeout": _safe_int(
                os.environ.get("KUBEMQ_KEEPALIVE_TIMEOUT", "10"), 10
            ),
            "dead_letter_queue": os.environ.get("KUBEMQ_DEAD_LETTER_QUEUE", ""),
            "max_receive_count": _safe_int(os.environ.get("KUBEMQ_MAX_RECEIVE_COUNT", "0"), 0),
            "message_expiration": _safe_int(os.environ.get("KUBEMQ_MESSAGE_EXPIRATION", "0"), 0),
            "max_batch_size": _safe_int(os.environ.get("KUBEMQ_MAX_BATCH_SIZE", "10"), 10),
        },
        "result_backend_transport_options": {
            "auth_token": os.environ.get("KUBEMQ_AUTH_TOKEN", None),
            "connection_timeout": _safe_float(
                os.environ.get("KUBEMQ_CONNECTION_TIMEOUT", "10"), 10.0
            ),
            "result_channel_prefix": os.environ.get("KUBEMQ_RESULT_PREFIX", "celery-result-"),
            "peek_timeout": _safe_int(os.environ.get("KUBEMQ_PEEK_TIMEOUT", "1"), 1),
        },
    }
)


@app.task
def process(item: str) -> dict:
    """Process an item using the environment-configured transport."""
    return {"item": item, "status": "processed"}


if __name__ == "__main__":
    print("=== Environment Variable Configuration — KubeMQ Celery Transport ===\n")

    print(f"Broker URL:     {app.conf.broker_url}")
    print(f"Result backend: {app.conf.result_backend}")
    print(f"Result expires: {app.conf.result_expires}s")

    broker_opts = app.conf.broker_transport_options
    print("\nBroker transport options:")
    for key, value in sorted(broker_opts.items()):
        display = "****" if key == "auth_token" and value else value
        print(f"  {key}: {display}")

    backend_opts = app.conf.result_backend_transport_options
    print("\nResult backend transport options:")
    for key, value in sorted(backend_opts.items()):
        display = "****" if key == "auth_token" and value else value
        print(f"  {key}: {display}")

    print()
    print("To test:")
    print("  1. Set environment variables (see module docstring)")
    print("  2. Start a worker:")
    print("     celery -A examples.connection.env_var_config worker --loglevel=info")
    print()
    print("=== Configuration demo complete ===")
