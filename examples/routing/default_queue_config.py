"""Default Queue Configuration — KubeMQ Celery Transport.

Demonstrates:
- task_default_queue for setting the default queue name
- task_create_missing_queues for auto-creating KubeMQ channels
- task_default_exchange and task_default_routing_key
- Queue naming conventions with KubeMQ

Usage:
    celery -A examples.routing.default_queue_config worker --loglevel=info
    python examples/routing/default_queue_config.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("default_queue_config")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        # Default queue name (default is "celery")
        # Tasks without explicit routing go here
        "task_default_queue": "default",
        # Auto-create queues that don't exist (default: True)
        # KubeMQ creates channels on first use, so this is always effective
        "task_create_missing_queues": True,
        # Default exchange (used by Kombu virtual transport)
        "task_default_exchange": "default",
        "task_default_exchange_type": "direct",
        # Default routing key (typically matches queue name)
        "task_default_routing_key": "default",
    }
)


@app.task
def task_a(data: str) -> dict:
    """Task with no explicit route — uses default queue."""
    return {"data": data, "queue": "default (task_default_queue)"}


@app.task
def task_b(data: str) -> dict:
    """Another task using the default queue."""
    return {"data": data, "queue": "default (task_default_queue)"}


if __name__ == "__main__":
    print("=== Default Queue Configuration — KubeMQ Celery Transport ===\n")

    # Show current config
    print("[config] Queue settings:")
    print(f"  task_default_queue:          {app.conf.task_default_queue}")
    print(f"  task_create_missing_queues:  {app.conf.task_create_missing_queues}")
    print(f"  task_default_exchange:       {app.conf.task_default_exchange}")
    print(f"  task_default_exchange_type:  {app.conf.task_default_exchange_type}")
    print(f"  task_default_routing_key:    {app.conf.task_default_routing_key}")
    print()

    print("[info] KubeMQ queue channel behavior:")
    print("  - KubeMQ creates queue channels on first message (auto-create)")
    print("  - task_create_missing_queues=True is the natural KubeMQ behavior")
    print("  - Queue names are sanitized: dots/slashes converted to dashes")
    print("  - Default queue 'celery' can be renamed via task_default_queue")
    print()

    print("[info] Naming conventions:")
    print("  task_default_queue='celery'   -> KubeMQ channel: celery")
    print("  task_default_queue='default'  -> KubeMQ channel: default")
    print("  task_default_queue='app.main' -> KubeMQ channel: app-main")
    print("  (Dots are converted to dashes for KubeMQ compatibility)")
    print()

    print("--- Common configurations ---")
    print("  # Production: explicit default queue name")
    print('  task_default_queue = "myapp-tasks"')
    print()
    print("  # Prevent accidental queue creation")
    print("  task_create_missing_queues = False")
    print("  # (Tasks sent to unknown queues will fail)")
    print()
    print("  # Multi-app deployment: prefix queues per app")
    print('  task_default_queue = "app1-default"')
    print('  task_routes = {"app1.tasks.*": {"queue": "app1-tasks"}}')
    print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.routing.default_queue_config worker --loglevel=info")
    print("  2. Tasks without explicit routing go to the default queue.")
    print()
    print("=== Configuration demo complete ===")
