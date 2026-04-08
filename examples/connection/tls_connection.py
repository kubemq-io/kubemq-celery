"""TLS Connection — KubeMQ Celery Transport.

Demonstrates:
- Connecting to KubeMQ over TLS using kubemq+tls:// URL scheme
- TLS encrypts the gRPC channel between Celery and the KubeMQ broker
- No client certificates required (server-side TLS only)

Usage:
    # Start a worker:
    celery -A examples.connection.tls_connection worker --loglevel=info

    # Run the example:
    python examples/connection/tls_connection.py

Requirements:
    - Running KubeMQ broker with TLS enabled
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "tls_connection",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq+tls://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq+tls://localhost:50000"),
)


@app.task
def secure_echo(message: str) -> str:
    """Echo back a message over the TLS-secured connection."""
    return f"secure echo: {message}"


if __name__ == "__main__":
    print("=== TLS Connection — KubeMQ Celery Transport ===\n")
    print(f"Broker URL:     {app.conf.broker_url}")
    print(f"Result backend: {app.conf.result_backend}")
    print("  (kubemq+tls:// enables gRPC TLS encryption)")
    print()
    print("To test this connection:")
    print("  1. Ensure KubeMQ broker has TLS enabled")
    print("  2. Start a worker:")
    print("     celery -A examples.connection.tls_connection worker --loglevel=info")
    print("  3. The worker will establish a TLS-encrypted gRPC connection.")
    print()
    print("=== Configuration demo complete ===")
