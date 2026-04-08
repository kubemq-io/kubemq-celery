"""Worker Pool Options — KubeMQ Celery Transport.

Demonstrates:
- Pool types: prefork, solo, threads
- Choosing the right pool for different workloads
- Pool-specific behaviors and KubeMQ interaction
- Configuration via CLI -P flag

Usage:
    celery -A examples.rate_limiting.worker_pool_options worker --loglevel=info -P prefork
    celery -A examples.rate_limiting.worker_pool_options worker --loglevel=info -P solo
    celery -A examples.rate_limiting.worker_pool_options worker --loglevel=info -P threads
    python examples/rate_limiting/worker_pool_options.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import threading
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "worker_pool_options",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task
def identify_pool() -> dict:
    """Report the execution environment of the current worker."""
    return {
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        "thread_count": threading.active_count(),
    }


@app.task
def io_work(duration: float) -> dict:
    """Simulate I/O-bound work."""
    time.sleep(duration)
    return {"pid": os.getpid(), "thread": threading.current_thread().name}


@app.task
def cpu_work(n: int) -> dict:
    """Simulate CPU-bound work."""
    total = sum(i * i for i in range(n))
    return {"pid": os.getpid(), "result": total}


if __name__ == "__main__":
    print("=== Worker Pool Options — KubeMQ Celery Transport ===\n")

    pools = [
        {
            "name": "prefork (default)",
            "flag": "-P prefork",
            "desc": "Multiprocess pool using fork(). Each child process has its own GIL.",
            "best_for": "CPU-bound tasks, task isolation, memory-heavy tasks",
            "kubemq_note": "Each child has its own KubeMQ gRPC connection",
            "concurrency": "-c N (default: CPU count)",
            "limits": "Hard time limits supported (SIGKILL)",
        },
        {
            "name": "solo",
            "flag": "-P solo",
            "desc": "No pool — runs tasks in the main worker process sequentially.",
            "best_for": "Debugging, development, single-task workers",
            "kubemq_note": "Single KubeMQ connection shared with worker",
            "concurrency": "Always 1 (ignored)",
            "limits": "No hard time limits (no child process to kill)",
        },
        {
            "name": "threads",
            "flag": "-P threads",
            "desc": "Thread pool using threading module. Shares GIL.",
            "best_for": "I/O-bound tasks (HTTP calls, file I/O, database queries)",
            "kubemq_note": "Threads share KubeMQ connection (thread-safe gRPC)",
            "concurrency": "-c N (default: CPU count)",
            "limits": "No hard time limits",
        },
        {
            "name": "eventlet",
            "flag": "-P eventlet",
            "desc": "Green thread pool using eventlet. Cooperative multitasking.",
            "best_for": "Very high concurrency I/O (thousands of concurrent tasks)",
            "kubemq_note": "Requires eventlet-compatible gRPC (may need patching)",
            "concurrency": "-c N (can be 100-1000+)",
            "limits": "No hard time limits, requires 'pip install eventlet'",
        },
        {
            "name": "gevent",
            "flag": "-P gevent",
            "desc": "Green thread pool using gevent. Cooperative multitasking.",
            "best_for": "Very high concurrency I/O (similar to eventlet)",
            "kubemq_note": "Requires gevent-compatible gRPC (may need patching)",
            "concurrency": "-c N (can be 100-1000+)",
            "limits": "No hard time limits, requires 'pip install gevent'",
        },
    ]

    for pool in pools:
        print(f"  [{pool['name']}]")
        print(f"    Flag:        {pool['flag']}")
        print(f"    Description: {pool['desc']}")
        print(f"    Best for:    {pool['best_for']}")
        print(f"    KubeMQ:      {pool['kubemq_note']}")
        print(f"    Concurrency: {pool['concurrency']}")
        print(f"    Limits:      {pool['limits']}")
        print()

    print("To test:")
    print("  1. Start a worker with a specific pool:")
    print(
        "     celery -A examples.rate_limiting.worker_pool_options "
        "worker -P prefork -c 4 --loglevel=info"
    )
    print(
        "     celery -A examples.rate_limiting.worker_pool_options worker -P solo --loglevel=info"
    )
    print(
        "     celery -A examples.rate_limiting.worker_pool_options "
        "worker -P threads -c 8 --loglevel=info"
    )
    print()
    print("RECOMMENDATION for KubeMQ:")
    print("  CPU-bound tasks -> prefork (default)")
    print("  I/O-bound tasks -> threads or prefork")
    print("  Debugging       -> solo")
    print()
    print("=== Configuration demo complete ===")
