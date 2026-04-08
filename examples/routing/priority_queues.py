"""Priority Queues — KubeMQ Celery Transport.

Demonstrates:
- Priority-based task routing using dedicated queues
- Workers consuming queues in priority order
- Priority metadata in task messages
- KubeMQ queue-per-priority pattern

Usage:
    celery -A examples.routing.priority_queues worker --loglevel=info -Q critical,high,default,low
    python examples/routing/priority_queues.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

app = Celery("priority_queues")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_default_queue": "default",
        "task_routes": {
            "examples.routing.priority_queues.critical_task": {"queue": "critical"},
            "examples.routing.priority_queues.high_priority_task": {"queue": "high"},
            "examples.routing.priority_queues.low_priority_task": {"queue": "low"},
        },
    }
)

PRIORITY_QUEUES = ["critical", "high", "default", "low"]


@app.task
def critical_task(message: str) -> dict:
    """Critical priority task — processed first."""
    return {"priority": "critical", "message": message, "at": time.time()}


@app.task
def high_priority_task(message: str) -> dict:
    """High priority task."""
    return {"priority": "high", "message": message, "at": time.time()}


@app.task
def default_task(message: str) -> dict:
    """Default priority task."""
    return {"priority": "default", "message": message, "at": time.time()}


@app.task
def low_priority_task(message: str) -> dict:
    """Low priority task — processed last."""
    return {"priority": "low", "message": message, "at": time.time()}


def send_with_priority(message: str, priority: str = "default") -> AsyncResult:
    """Send a task to the appropriate priority queue."""
    queue_map = {
        "critical": ("critical", critical_task),
        "high": ("high", high_priority_task),
        "default": ("default", default_task),
        "low": ("low", low_priority_task),
    }
    if priority not in queue_map:
        print(f"  WARNING: unknown priority {priority!r}, falling back to 'default'")
    queue, task = queue_map.get(priority, queue_map["default"])
    return task.apply_async(args=(message,), queue=queue)


if __name__ == "__main__":
    print("=== Priority Queues — KubeMQ Celery Transport ===\n")

    print("Priority queue pattern: separate KubeMQ queue per priority level.\n")
    print("Queue consumption order (workers with -Q flag):")
    for i, q in enumerate(PRIORITY_QUEUES, 1):
        print(f"  {i}. {q}")
    print()
    print("Workers consume from queues in the order listed with -Q.")
    print("Start worker: celery -A app worker -Q critical,high,default,low\n")

    print("Task routing:")
    for task_name, route in app.conf.task_routes.items():
        print(f"  {task_name.split('.')[-1]:20s} -> queue='{route['queue']}'")
    print()

    print("--- Priority queue strategies ---")
    print("  1. Separate queues (this example):")
    print("     Each priority gets its own KubeMQ queue channel.")
    print("     Workers consume high-priority queues first.")
    print()
    print("  2. Dedicated workers:")
    print("     celery -A app worker -Q critical      # Only critical")
    print("     celery -A app worker -Q high,default   # High + default")
    print("     celery -A app worker -Q low            # Only low")
    print()
    print("  3. Weighted consumption:")
    print("     Run more worker processes for high-priority queues.")
    print()

    print("To test:")
    print("  1. Start a worker consuming all priority queues:")
    print(
        "     celery -A examples.routing.priority_queues worker "
        "-Q critical,high,default,low --loglevel=info"
    )
    print()
    print("=== Configuration demo complete ===")
