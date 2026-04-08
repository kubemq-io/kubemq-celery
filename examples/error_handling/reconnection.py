"""Reconnection — KubeMQ Celery Transport.

Demonstrates:
- Automatic reconnection via Kombu's connection recovery
- Celery workers automatically reconnect when the broker restarts
- Transport-level connection_errors trigger Kombu's retry logic
- broker_connection_retry_on_startup for initial connection resilience

Usage:
    # Start a worker (will auto-reconnect if broker restarts):
    celery -A examples.error_handling.reconnection worker --loglevel=info

    # Run the example:
    python examples/error_handling/reconnection.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("reconnection")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        # Retry connecting to broker on worker startup
        "broker_connection_retry_on_startup": True,
        # Kombu connection retry settings
        "broker_connection_max_retries": 10,
        "broker_connection_retry": True,
        "broker_transport_options": {
            "connection_timeout": 10.0,
            # Keepalive helps detect broken connections faster
            "grpc_keepalive_time": 15,
            "grpc_keepalive_timeout": 5,
        },
    }
)


@app.task(bind=True, max_retries=3)
def resilient_task(self, data: str) -> dict:
    """A task that handles connection errors gracefully.

    Celery classifies KubeMQ connection errors as recoverable.
    The worker automatically re-establishes the connection and
    retries the operation.
    """
    return {"data": data, "status": "processed"}


if __name__ == "__main__":
    print("Reconnection Example")
    print("=" * 40)

    print("\nCelery reconnection settings:")
    print(f"  broker_connection_retry_on_startup: {app.conf.broker_connection_retry_on_startup}")
    print(f"  broker_connection_max_retries:      {app.conf.broker_connection_max_retries}")
    print(f"  broker_connection_retry:             {app.conf.broker_connection_retry}")

    opts = app.conf.broker_transport_options
    print("\nTransport options:")
    print(f"  connection_timeout:      {opts['connection_timeout']}s")
    print(f"  grpc_keepalive_time:     {opts['grpc_keepalive_time']}s")
    print(f"  grpc_keepalive_timeout:  {opts['grpc_keepalive_timeout']}s")

    print("\nKubeMQ transport auto-reconnect flow:")
    print("  1. Worker detects connection loss (keepalive timeout or send/recv error)")
    print("  2. KubeMQConnectionError raised (classified as connection_error)")
    print("  3. Kombu's connection recovery kicks in")
    print("  4. New gRPC channel established to broker")
    print("  5. Worker resumes consuming tasks")
    print()

    print("To test reconnection:")
    print("  1. Start a worker:")
    print("     celery -A examples.error_handling.reconnection worker --loglevel=info")
    print("  2. Restart the KubeMQ broker while the worker is running.")
    print("  3. The worker will auto-reconnect and resume processing.")
    print()
    print("=== Configuration demo complete ===")
