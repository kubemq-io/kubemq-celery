"""Celery Events — KubeMQ Celery Transport.

Demonstrates:
- worker_send_task_events=True for event broadcasting
- task_send_sent_event for publish-time events
- Event types: task-sent, task-received, task-started, task-succeeded, task-failed
- Events broadcast via KubeMQ Events (PubSub fanout)

Usage:
    celery -A examples.monitoring.celery_events worker --loglevel=info -E
    celery -A examples.monitoring.celery_events events --loglevel=info
    python examples/monitoring/celery_events.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("celery_events")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        # Enable all event types
        "worker_send_task_events": True,  # Worker sends task state change events
        "task_send_sent_event": True,  # Client sends task-sent event on publish
        "event_queue_expires": 60,  # Event queue TTL (seconds)
    }
)


@app.task
def fast_task(n: int) -> int:
    """Quick task — generates task-received, task-started, task-succeeded events."""
    return n * n


@app.task
def slow_task(seconds: float) -> str:
    """Slow task — events visible over time."""
    time.sleep(seconds)
    return f"slept {seconds}s"


@app.task
def error_task() -> None:
    """Failing task — generates task-failed event."""
    raise ValueError("intentional error")


if __name__ == "__main__":
    print("=== Celery Events — KubeMQ Celery Transport ===\n")

    print("Celery event types:\n")
    events = [
        ("task-sent", "Published by client when task is sent to broker"),
        ("task-received", "Worker received the task from KubeMQ queue"),
        ("task-started", "Worker started executing the task"),
        ("task-succeeded", "Task completed successfully"),
        ("task-failed", "Task raised an exception"),
        ("task-rejected", "Task was rejected by worker"),
        ("task-revoked", "Task was revoked/cancelled"),
        ("task-retried", "Task scheduled for retry"),
        ("worker-online", "Worker came online"),
        ("worker-offline", "Worker went offline"),
        ("worker-heartbeat", "Periodic worker heartbeat"),
    ]
    for name, desc in events:
        print(f"  {name:20s} — {desc}")
    print()

    print("Event flow with KubeMQ:\n")
    print("  1. Events are published via KubeMQ Events (PubSub fanout)")
    print("  2. All subscribers (Flower, celery events, custom) receive them")
    print("  3. Events are fire-and-forget (not persisted by KubeMQ)")
    print("  4. Enable with: worker_send_task_events=True\n")

    print("Monitor events in real-time:")
    print("  celery -A examples.monitoring.celery_events events --loglevel=info\n")

    print("To test:")
    print("  1. Start a worker with events enabled:")
    print("     celery -A examples.monitoring.celery_events worker --loglevel=info -E")
    print("  2. In another terminal, start the event monitor:")
    print("     celery -A examples.monitoring.celery_events events --loglevel=info")
    print("  3. Send tasks and watch events flow in the monitor.")
    print()
    print("NOTE: Use 'celery events' CLI or Flower to view event stream.")
    print()
    print("=== Configuration demo complete ===")
