"""Result Backend Transport Options — KubeMQ Celery Transport.

Demonstrates:
- result_backend_transport_options configuration
- Custom result_channel_prefix for channel naming
- peek_timeout tuning for result retrieval latency
- TLS and authentication options for result backend

Usage:
    celery -A examples.result_backend.result_backend_options worker --loglevel=info
    python examples/result_backend/result_backend_options.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "result_backend_options",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    # Result backend transport options specific to KubeMQ
    result_backend_transport_options={
        # Channel name prefix for result queues (default: "celery-result-")
        # Results stored on: {prefix}{task_id}
        "result_channel_prefix": "celery-result-",
        # Peek timeout in seconds (default: 1)
        # How long peek_queue_messages waits for a result before returning empty.
        # Lower = faster polling cycle, higher CPU. Higher = slower first check.
        "peek_timeout": 2,
        # gRPC message size limits (default: 4MB each)
        "max_send_size": 4_194_304,
        "max_receive_size": 4_194_304,
        # gRPC keepalive settings for the result backend client
        "grpc_keepalive_time": 30,
        "grpc_keepalive_timeout": 10,
        "grpc_permit_without_calls": True,
        # Authentication (alternative to URL-based auth)
        # "auth_token": "your-token-here",
        # TLS settings for result backend connection
        # "tls_enabled": True,
        # "tls_cert_file": "/path/to/client.crt",
        # "tls_key_file": "/path/to/client.key",
        # "tls_ca_file": "/path/to/ca.crt",
        # Connection timeout (None = use default)
        # "connection_timeout": 10.0,
    },
)


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task
def build_payload(name: str, count: int) -> dict:
    """Build a structured payload."""
    return {
        "name": name,
        "items": [f"item-{i}" for i in range(count)],
        "count": count,
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Result Backend Transport Options — KubeMQ Celery Transport ===\n")

    # Display current options
    opts = app.conf.get("result_backend_transport_options", {})
    print("[config] result_backend_transport_options:")
    for key, value in opts.items():
        print(f"    {key}: {value}")
    print()

    # Explain each option
    options_info = {
        "result_channel_prefix": (
            "Prefix for result queue channel names. "
            "Default 'celery-result-'. Results stored on {prefix}{task_id}."
        ),
        "peek_timeout": (
            "Seconds to wait during peek_queue_messages. "
            "Default 1. Higher values reduce polling frequency."
        ),
        "max_send_size": (
            "Maximum gRPC send message size in bytes. Default 4MB. Increase for large task results."
        ),
        "max_receive_size": (
            "Maximum gRPC receive message size in bytes. Default 4MB. Must match max_send_size."
        ),
        "auth_token": "Authentication token for the result backend KubeMQ client.",
        "tls_enabled": "Enable TLS for the result backend gRPC connection.",
        "connection_timeout": "Connection timeout in seconds. None = SDK default.",
    }
    print("[reference] Available options:")
    for key, desc in options_info.items():
        print(f"    {key}: {desc}")
    print()

    # Send tasks with configured backend
    print("[1] Sending multiply(6, 7)...")
    result = multiply.delay(6, 7)
    print(f"    Task ID: {result.id}")
    print(f"    Result channel: celery-result-{result.id}")
    value = result.get(timeout=30)
    print(f"    Result: {value}\n")

    print("[2] Sending build_payload('test', 3)...")
    result2 = build_payload.delay("test", 3)
    print(f"    Task ID: {result2.id}")
    payload = result2.get(timeout=30)
    print(f"    Payload: {payload}\n")

    print("=== Result backend options demo complete ===")
