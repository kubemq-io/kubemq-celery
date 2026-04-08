"""Prefetch Multiplier — KubeMQ Celery Transport.

Demonstrates:
- worker_prefetch_multiplier=1 for fair scheduling
- How prefetch affects KubeMQ queue message distribution
- Trade-off between throughput and fair task distribution
- Impact on long-running tasks mixed with short tasks

Usage:
    celery -A examples.rate_limiting.prefetch_multiplier worker --loglevel=info
    python examples/rate_limiting/prefetch_multiplier.py

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
    "prefetch_multiplier",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    # Fair scheduling: prefetch only 1 task per worker process at a time.
    # Default is 4, meaning each worker prefetches 4 tasks ahead.
    worker_prefetch_multiplier=1,
    # Combined with task_acks_late=True for optimal fair scheduling
    task_acks_late=True,
)


@app.task
def short_task(task_num: int) -> dict:
    """A quick task (~100ms)."""
    time.sleep(0.1)
    return {"task_num": task_num, "type": "short", "pid": os.getpid()}


@app.task
def long_task(task_num: int) -> dict:
    """A slow task (~3s)."""
    time.sleep(3)
    return {"task_num": task_num, "type": "long", "pid": os.getpid()}


if __name__ == "__main__":
    print("=== Prefetch Multiplier — KubeMQ Celery Transport ===\n")

    print("Prefetch multiplier controls how many tasks a worker reserves ahead:\n")
    print("  worker_prefetch_multiplier = 4 (default)")
    print("    -> Each worker process reserves 4 tasks from KubeMQ")
    print("    -> Higher throughput but unfair when tasks have mixed durations")
    print("    -> Short tasks queue behind long tasks in the same worker")
    print()
    print("  worker_prefetch_multiplier = 1 (fair scheduling)")
    print("    -> Each worker process reserves 1 task at a time")
    print("    -> Tasks distributed more evenly across workers")
    print("    -> Slightly lower throughput due to more frequent fetches")
    print()
    print("  Combined with task_acks_late = True:")
    print("    -> Tasks acknowledged after execution (not on receive)")
    print("    -> Failed tasks are redelivered by KubeMQ")
    print()
    print(f"  Current config: prefetch_multiplier={app.conf.worker_prefetch_multiplier}")
    print(f"                  task_acks_late={app.conf.task_acks_late}\n")

    print("To test prefetch behavior:")
    print("  1. Start a worker:")
    print("     celery -A examples.rate_limiting.prefetch_multiplier worker -c 4 --loglevel=info")
    print("  2. Send a mix of short and long tasks to observe fair scheduling.")
    print()
    print("TIP: With multiple workers (-c 4) and prefetch=1, short tasks")
    print("     complete without waiting behind long tasks.")
    print()
    print("=== Configuration demo complete ===")
