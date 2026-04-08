"""Worker Control — KubeMQ Celery Transport.

Demonstrates:
- app.control.shutdown() for graceful worker shutdown
- app.control.ping() for health checks
- app.control.add_consumer() / cancel_consumer() for dynamic queue management
- Worker pool management commands

Usage:
    celery -A examples.monitoring.worker_control worker --loglevel=info
    python examples/monitoring/worker_control.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "worker_control",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task
def echo(message: str) -> str:
    """Echo task for testing worker connectivity."""
    return f"echo: {message}"


if __name__ == "__main__":
    print("=== Worker Control — KubeMQ Celery Transport ===\n")

    print("Worker control commands use KubeMQ Events (pidbox fanout)")
    print("to send commands to all workers simultaneously.\n")

    commands = [
        {
            "section": "Health & Status",
            "cmds": [
                ("app.control.ping(timeout=5)", "Check if workers are alive"),
                ("app.control.inspect().active()", "List running tasks"),
                ("app.control.inspect().reserved()", "List prefetched tasks"),
                ("app.control.inspect().stats()", "Worker pool statistics"),
            ],
        },
        {
            "section": "Queue Management",
            "cmds": [
                ("app.control.add_consumer('new-queue')", "Start consuming a new KubeMQ queue"),
                ("app.control.cancel_consumer('old-queue')", "Stop consuming a queue"),
                ("app.control.inspect().active_queues()", "List consumed queues"),
            ],
        },
        {
            "section": "Pool Management",
            "cmds": [
                ("app.control.pool_grow(n=2)", "Add 2 worker processes to pool"),
                ("app.control.pool_shrink(n=1)", "Remove 1 worker process from pool"),
                ("app.control.autoscale(max=10, min=2)", "Set autoscale bounds"),
            ],
        },
        {
            "section": "Rate Limiting",
            "cmds": [
                ("app.control.rate_limit('task.name', '10/m')", "Set rate limit at runtime"),
                ("app.control.rate_limit('task.name', 0)", "Remove rate limit"),
            ],
        },
        {
            "section": "Lifecycle",
            "cmds": [
                ("app.control.shutdown()", "Graceful shutdown of all workers"),
                (
                    "app.control.broadcast('shutdown', destination=['w1@host'])",
                    "Shutdown specific worker",
                ),
            ],
        },
    ]

    for group in commands:
        print(f"  --- {group['section']} ---")
        for cmd, desc in group["cmds"]:
            print(f"  {cmd}")
            print(f"    -> {desc}")
        print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.monitoring.worker_control worker --loglevel=info")
    print("  2. Use the control commands above from a Python shell.")
    print()
    print("NOTE: All control commands are broadcast via KubeMQ Events.")
    print("      Workers must be online to receive and act on commands.")
    print("      Commands are fire-and-forget — not persisted.")
    print()
    print("=== Configuration demo complete ===")
