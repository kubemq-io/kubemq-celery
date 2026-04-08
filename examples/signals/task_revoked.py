"""Task Revoked Signal — KubeMQ Celery Transport.

Demonstrates:
- task_revoked signal handler for cleanup on cancellation
- app.control.revoke() to cancel a running or pending task
- Revocation with terminate=True for immediate kill
- Tracking revoked tasks via signal handlers

Usage:
    celery -A examples.signals.task_revoked worker --loglevel=info
    python examples/signals/task_revoked.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time
from typing import Any

from celery import Celery
from celery.signals import task_revoked

import kubemq_celery  # noqa: F401

app = Celery(
    "task_revoked",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    task_track_started=True,
)


@task_revoked.connect
def on_task_revoked(
    sender: Any = None,
    request: Any = None,
    terminated: bool = False,
    signum: Any = None,
    expired: bool = False,
    **kwargs: Any,
) -> None:
    """Called when a task is revoked/cancelled.

    Args:
        sender: The task class.
        request: The task request with id, args, kwargs.
        terminated: True if the task was killed (SIGTERM/SIGKILL).
        signum: The signal used to terminate.
        expired: True if revoked due to expiration.
    """
    task_id = request.id if request else "unknown"
    task_name = sender.name if sender else "unknown"
    print(
        f"[REVOKED] Task {task_name} ({task_id[:8]}...) — "
        f"terminated={terminated}, signal={signum}, expired={expired}"
    )


@app.task(bind=True)
def long_computation(self, iterations: int) -> dict:
    """A long-running task that can be revoked mid-execution."""
    results = []
    for i in range(iterations):
        time.sleep(1)
        results.append(i * i)
        self.update_state(
            state="PROGRESS",
            meta={"current": i + 1, "total": iterations},
        )
    return {"iterations": iterations, "results": results}


@app.task
def quick_task(x: int) -> int:
    """A quick task for revoking before execution."""
    return x * x


if __name__ == "__main__":
    print("=== Task Revoked Signal — KubeMQ Celery Transport ===\n")
    print("NOTE: Revocation requires a running worker with pidbox support.\n")

    print("Revocation API reference:")
    print("  app.control.revoke(task_id)")
    print("    -> Revoke task (worker won't execute if pending)")
    print()
    print("  app.control.revoke(task_id, terminate=True)")
    print("    -> Terminate running task with SIGTERM")
    print()
    print("  app.control.revoke(task_id, terminate=True, signal='SIGKILL')")
    print("    -> Force-kill running task")
    print()
    print("  app.control.revoke([id1, id2, id3])")
    print("    -> Batch revoke multiple tasks")
    print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.signals.task_revoked worker --loglevel=info")
    print("  2. Send a long task, then revoke it from another shell.")
    print("  3. The task_revoked signal handler logs the revocation on the worker.")
    print()
    print("NOTE: task_revoked signal fires on the worker process.")
    print("      Revoke commands sent via KubeMQ Events (pidbox fanout).")
    print()
    print("=== Configuration demo complete ===")
