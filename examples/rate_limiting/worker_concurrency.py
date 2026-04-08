"""Worker Concurrency — KubeMQ Celery Transport.

Demonstrates:
- Worker --concurrency flag for controlling parallel task execution
- Pool types: prefork (multiprocess), eventlet, gevent, threads
- Impact of concurrency on KubeMQ queue consumption rate
- Choosing concurrency level for different workload types

Usage:
    celery -A examples.rate_limiting.worker_concurrency worker --loglevel=info --concurrency=4
    celery -A examples.rate_limiting.worker_concurrency worker --loglevel=info -c 2 -P threads
    python examples/rate_limiting/worker_concurrency.py

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
    "worker_concurrency",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    task_track_started=True,
)


@app.task(bind=True)
def cpu_bound_task(self, iterations: int) -> dict:
    """CPU-intensive task — benefits from prefork concurrency."""
    start = time.monotonic()
    total = sum(i * i for i in range(iterations))
    elapsed = time.monotonic() - start
    return {
        "iterations": iterations,
        "result": total,
        "duration": round(elapsed, 3),
        "pid": os.getpid(),
    }


@app.task(bind=True)
def io_bound_task(self, sleep_seconds: float) -> dict:
    """I/O-bound task — benefits from eventlet/gevent/threads concurrency."""
    start = time.monotonic()
    time.sleep(sleep_seconds)
    elapsed = time.monotonic() - start
    return {
        "sleep_seconds": sleep_seconds,
        "actual_duration": round(elapsed, 3),
        "pid": os.getpid(),
    }


if __name__ == "__main__":
    print("=== Worker Concurrency — KubeMQ Celery Transport ===\n")

    print("Worker concurrency controls how many tasks run in parallel:\n")

    pool_configs = [
        (
            "prefork (default)",
            "celery -A ... worker -c 4 -P prefork",
            "Multiprocess. Best for CPU-bound tasks. Each child has its own GIL.",
        ),
        (
            "threads",
            "celery -A ... worker -c 8 -P threads",
            "Thread pool. Good for I/O-bound tasks. Shares GIL.",
        ),
        (
            "eventlet",
            "celery -A ... worker -c 100 -P eventlet",
            "Green threads. Very high concurrency for I/O. Requires eventlet.",
        ),
        (
            "gevent",
            "celery -A ... worker -c 100 -P gevent",
            "Green threads. Similar to eventlet. Requires gevent.",
        ),
        (
            "solo",
            "celery -A ... worker -P solo",
            "No pool. Single task at a time. Useful for debugging.",
        ),
    ]

    for name, cmd, desc in pool_configs:
        print(f"  [{name}]")
        print(f"    Command: {cmd}")
        print(f"    Notes:   {desc}")
        print()

    print("To test concurrency:")
    print("  1. Start a worker with desired pool/concurrency:")
    print("     celery -A examples.rate_limiting.worker_concurrency worker -c 4 --loglevel=info")
    print("  2. Observe parallel task execution in worker logs.")
    print()
    print("TIP: Match concurrency to workload type:")
    print("     CPU-bound -> prefork, -c = CPU cores")
    print("     I/O-bound -> threads/eventlet/gevent, -c = 10-100+")
    print()
    print("=== Configuration demo complete ===")
