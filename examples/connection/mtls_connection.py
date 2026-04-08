"""Mutual TLS (mTLS) Connection — KubeMQ Celery Transport.

Demonstrates:
- Mutual TLS authentication using client certificates
- broker_transport_options: tls_cert_file, tls_key_file, tls_ca_file
- Both broker and result backend share the same TLS configuration

Usage:
    # Start a worker:
    celery -A examples.connection.mtls_connection worker --loglevel=info

    # Run the example:
    python examples/connection/mtls_connection.py

Requirements:
    - Running KubeMQ broker with mTLS enabled
    - Client certificate, key, and CA certificate files
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

CERT_DIR = os.environ.get("CERT_DIR", "/etc/kubemq/certs")

app = Celery("mtls_connection")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq+tls://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq+tls://localhost:50000"),
        "broker_transport_options": {
            "tls_cert_file": os.environ.get("TLS_CERT_FILE", f"{CERT_DIR}/client.crt"),
            "tls_key_file": os.environ.get("TLS_KEY_FILE", f"{CERT_DIR}/client.key"),
            "tls_ca_file": os.environ.get("TLS_CA_FILE", f"{CERT_DIR}/ca.crt"),
        },
        "result_backend_transport_options": {
            "tls_enabled": True,
            "tls_cert_file": os.environ.get("TLS_CERT_FILE", f"{CERT_DIR}/client.crt"),
            "tls_key_file": os.environ.get("TLS_KEY_FILE", f"{CERT_DIR}/client.key"),
            "tls_ca_file": os.environ.get("TLS_CA_FILE", f"{CERT_DIR}/ca.crt"),
        },
    }
)


@app.task
def secure_add(x: int, y: int) -> int:
    """Add two numbers over an mTLS-secured connection."""
    return x + y


if __name__ == "__main__":
    print("=== Mutual TLS (mTLS) Connection — KubeMQ Celery Transport ===\n")
    print(f"Broker URL: {app.conf.broker_url}")
    opts = app.conf.broker_transport_options
    print(f"  cert: {opts.get('tls_cert_file')}")
    print(f"  key:  {opts.get('tls_key_file')}")
    print(f"  CA:   {opts.get('tls_ca_file')}")
    print()
    print("To test this connection:")
    print("  1. Ensure KubeMQ broker has mTLS enabled")
    print("  2. Place client cert, key, and CA files in the cert directory")
    print("  3. Start a worker:")
    print("     celery -A examples.connection.mtls_connection worker --loglevel=info")
    print()
    print("=== Configuration demo complete ===")
