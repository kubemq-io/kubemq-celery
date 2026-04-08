"""Broadcast Tasks — KubeMQ Celery Transport.

Demonstrates:
- Fanout via app.control.broadcast() to all workers
- KubeMQ Events (PubSub) for broadcast delivery
- Built-in broadcast commands (shutdown, ping, pool controls)
- Fanout exchange with backoff on subscription errors

Usage:
    celery -A examples.routing.broadcast_tasks worker --loglevel=info
    python examples/routing/broadcast_tasks.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "broadcast_tasks",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task
def normal_task(data: str) -> dict:
    """A regular task for testing alongside broadcasts."""
    return {"data": data, "pid": os.getpid()}


if __name__ == "__main__":
    print("=== Broadcast Tasks — KubeMQ Celery Transport ===\n")

    print("Broadcasts use KubeMQ Events (PubSub fanout) to reach ALL workers.")
    print("Unlike queue tasks, every worker receives every broadcast.\n")

    print("Available broadcast commands:\n")
    commands = [
        ("app.control.ping(timeout=5)", "Check if workers are alive"),
        ("app.control.broadcast('shutdown')", "Graceful shutdown of all workers"),
        ("app.control.broadcast('pool_grow', arguments={'n': 2})", "Add 2 pool processes"),
        ("app.control.broadcast('pool_shrink', arguments={'n': 1})", "Remove 1 pool process"),
        (
            "app.control.broadcast('rate_limit', "
            "arguments={'task_name': 'x', 'rate_limit': '10/m'})",
            "Set rate limit at runtime",
        ),
        (
            "app.control.broadcast('add_consumer', arguments={'queue': 'new-queue'})",
            "Start consuming a new queue",
        ),
        (
            "app.control.broadcast('cancel_consumer', arguments={'queue': 'old-queue'})",
            "Stop consuming a queue",
        ),
    ]
    for cmd, desc in commands:
        print(f"  {cmd}")
        print(f"    -> {desc}\n")

    print("--- KubeMQ broadcast implementation ---")
    print("  Broadcasts use KubeMQ Events (PubSub) with fanout exchange.")
    print("  Worker control (pidbox) subscribes via PubSubClient.")
    print("  If subscription breaks, exponential backoff retries")
    print("  (1s, 2s, 4s, ... up to 30s, max fanout_max_retries=5).")
    print()
    print("  Unlike queue tasks, broadcasts are fire-and-forget —")
    print("  they are NOT persisted and NOT retried by KubeMQ.")
    print("  Workers must be online to receive broadcasts.")
    print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.routing.broadcast_tasks worker --loglevel=info")
    print("  2. Use the broadcast commands above from another shell.")
    print()
    print("=== Configuration demo complete ===")
