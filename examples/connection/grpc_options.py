"""gRPC Options — KubeMQ Celery Transport.

Demonstrates:
- gRPC keepalive configuration for long-lived connections
- grpc_keepalive_time: interval between keepalive pings (seconds)
- grpc_keepalive_timeout: max wait for a keepalive response (seconds)
- max_send_size / max_receive_size: gRPC message size limits (bytes)

Usage:
    # Start a worker:
    celery -A examples.connection.grpc_options worker --loglevel=info

    # Run the example:
    python examples/connection/grpc_options.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("grpc_options")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "broker_transport_options": {
            # Keepalive: send a ping every 15 seconds to detect broken connections
            "grpc_keepalive_time": 15,
            # Wait up to 5 seconds for a keepalive response before considering dead
            "grpc_keepalive_timeout": 5,
            # Allow 8MB messages (default is 4MB)
            "max_send_size": 8_388_608,
            "max_receive_size": 8_388_608,
        },
        "result_backend_transport_options": {
            "grpc_keepalive_time": 15,
            "grpc_keepalive_timeout": 5,
            "max_send_size": 8_388_608,
            "max_receive_size": 8_388_608,
        },
    }
)


@app.task
def process_large_payload(data: list) -> dict:
    """Process a potentially large list of items."""
    return {"items_processed": len(data), "total": sum(data)}


if __name__ == "__main__":
    print("=== gRPC Options — KubeMQ Celery Transport ===\n")

    opts = app.conf.broker_transport_options
    print(f"Keepalive time:    {opts['grpc_keepalive_time']}s")
    print(f"Keepalive timeout: {opts['grpc_keepalive_timeout']}s")
    print(f"Max send size:     {opts['max_send_size'] / 1048576:.0f} MB")
    print(f"Max receive size:  {opts['max_receive_size'] / 1048576:.0f} MB")
    print()
    print("To test with these gRPC options:")
    print("  1. Start a worker:")
    print("     celery -A examples.connection.grpc_options worker --loglevel=info")
    print("  2. The worker will use the configured keepalive and message size limits.")
    print()
    print("=== Configuration demo complete ===")
