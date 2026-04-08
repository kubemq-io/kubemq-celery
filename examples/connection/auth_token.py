"""Auth Token — KubeMQ Celery Transport.

Demonstrates:
- Two methods for authenticating with KubeMQ
- Method 1: Auth token in the URL (as password field)
- Method 2: auth_token in broker_transport_options

Usage:
    # Start a worker:
    celery -A examples.connection.auth_token worker --loglevel=info

    # Run the example:
    python examples/connection/auth_token.py

Requirements:
    - Running KubeMQ broker with authentication enabled
    - Valid auth token
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

TOKEN = os.environ.get("KUBEMQ_AUTH_TOKEN", "my-secret-token")


def create_app_url_auth() -> Celery:
    """Method 1: Auth token embedded in the broker URL.

    Format: kubemq://:token@host:port
    The token is placed in the password field of the URL.
    """
    fallback = os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000")
    host_port = fallback.rsplit("://", 1)[-1].split("@")[-1]
    url = f"kubemq://:{TOKEN}@{host_port}"

    a = Celery(
        "auth_token_url",
        broker=url,
        result_backend=url,
    )
    return a


def create_app_transport_option_auth() -> Celery:
    """Method 2: Auth token via broker_transport_options.

    Keeps the URL clean; the token is passed as a transport option.
    This method is preferred when managing tokens via environment variables.
    """
    a = Celery("auth_token_opts")
    a.config_from_object(
        {
            "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
            "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
            "broker_transport_options": {
                "auth_token": TOKEN,
            },
            "result_backend_transport_options": {
                "auth_token": TOKEN,
            },
        }
    )
    return a


# Use the transport-option method by default
app = create_app_transport_option_auth()


@app.task
def authenticated_task(data: str) -> dict:
    """A task that runs over an authenticated connection."""
    return {"data": data, "authenticated": True}


if __name__ == "__main__":
    print("=== Auth Token — KubeMQ Celery Transport ===\n")

    print("Method 1: Token in URL")
    print("  Format: kubemq://:token@host:port")
    print("  Broker URL: kubemq://:****@localhost:50000")
    print("  (Token is placed in the password field of the URL)")
    print()

    print("Method 2: Token in transport options (recommended)")
    print(f"  Broker URL: {app.conf.broker_url}")
    print(f"  auth_token: ****{TOKEN[-4:]}")
    print("  (Keeps the URL clean; token managed via env vars)")
    print()

    print("To test this connection:")
    print("  1. Set KUBEMQ_AUTH_TOKEN environment variable")
    print("  2. Start a worker:")
    print("     celery -A examples.connection.auth_token worker --loglevel=info")
    print()
    print("=== Configuration demo complete ===")
