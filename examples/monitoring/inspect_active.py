"""Inspect Active Tasks — KubeMQ Celery Transport.

Demonstrates:
- app.control.inspect() for querying worker state
- Inspecting active, reserved, and scheduled tasks
- Worker statistics and configuration
- Ping for worker health checks

Usage:
    celery -A examples.monitoring.inspect_active worker --loglevel=info
    python examples/monitoring/inspect_active.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "inspect_active",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task(bind=True)
def long_task(self, seconds: int = 30) -> dict:
    """Long task to be visible in active inspection."""
    for i in range(seconds):
        time.sleep(1)
        self.update_state(state="PROGRESS", meta={"second": i + 1, "total": seconds})
    return {"seconds": seconds, "done": True}


@app.task
def quick_task(x: int) -> int:
    """Quick task for inspection demo."""
    return x * 2


if __name__ == "__main__":
    print("=== Inspect Active Tasks — KubeMQ Celery Transport ===\n")

    print("--- Inspect commands reference ---")
    print("  inspector = app.control.inspect(timeout=5.0)")
    print()
    print("  inspector.ping()       -> Check worker health")
    print("  inspector.active()     -> Currently executing tasks")
    print("  inspector.reserved()   -> Prefetched (waiting) tasks")
    print("  inspector.scheduled()  -> ETA/countdown pending tasks")
    print("  inspector.stats()      -> Worker pool statistics")
    print("  inspector.registered() -> Registered task names")
    print("  inspector.conf()       -> Worker configuration")
    print("  inspector.report()     -> Full worker report")
    print()
    print("  All inspect commands use KubeMQ Events (pidbox fanout).")
    print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.monitoring.inspect_active worker --loglevel=info")
    print("  2. Use the inspect commands above from a Python shell or script.")
    print()
    print("=== Configuration demo complete ===")
