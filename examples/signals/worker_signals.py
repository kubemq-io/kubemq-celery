"""Worker Signals — KubeMQ Celery Transport.

Demonstrates:
- worker_ready signal when worker is fully initialized
- worker_shutting_down signal for graceful shutdown hooks
- worker_process_init for per-process initialization (prefork pool)
- Using signals for connection setup, warmup, and cleanup

Usage:
    celery -A examples.signals.worker_signals worker --loglevel=info
    python examples/signals/worker_signals.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery
from celery.signals import (
    worker_init,
    worker_process_init,
    worker_ready,
    worker_shutting_down,
)

import kubemq_celery  # noqa: F401

app = Celery(
    "worker_signals",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@worker_init.connect
def on_worker_init(sender: Any = None, **kwargs: Any) -> None:
    """Called when the worker process is initializing.

    Fires before connections are established. Good for:
    - Loading configuration files
    - Setting up logging
    - Initializing shared state
    """
    print("[worker_init] Worker process initializing...")
    print("[worker_init] Good place to load configs, set up logging")


@worker_ready.connect
def on_worker_ready(sender: Any = None, **kwargs: Any) -> None:
    """Called when the worker is fully initialized and ready.

    The KubeMQ connection is established and queues are bound.
    Good for:
    - Warmup operations (pre-load caches, etc.)
    - Health check registration
    - Sending "worker online" notifications
    """
    print("[worker_ready] Worker is fully initialized and connected to KubeMQ!")
    print("[worker_ready] Ready to process tasks.")


@worker_process_init.connect
def on_worker_process_init(sender: Any = None, **kwargs: Any) -> None:
    """Called in each child process after fork (prefork pool only).

    IMPORTANT: This fires in each pool worker process, not the main process.
    Good for:
    - Creating per-process database connections
    - Initializing thread-local state
    - Setting up per-process resources

    Does NOT fire with solo, threads, or eventlet/gevent pools.
    """
    pid = os.getpid()
    print(f"[worker_process_init] Child process {pid} initialized")
    print("[worker_process_init] Set up per-process resources here")


@worker_shutting_down.connect
def on_worker_shutting_down(
    sender: Any = None, sig: Any = None, how: str = "", exitcode: int = 0, **kwargs: Any
) -> None:
    """Called when the worker begins shutting down.

    Good for:
    - Closing external connections
    - Flushing metrics/logs
    - Sending "worker offline" notifications
    - Saving state before exit
    """
    print(f"[worker_shutting_down] Worker shutting down (signal={sig}, how={how})")
    print("[worker_shutting_down] Cleaning up resources...")


@app.task
def health_check() -> dict:
    """Simple health check task."""
    return {"status": "healthy", "pid": os.getpid()}


@app.task
def echo(message: str) -> str:
    """Echo back a message."""
    return f"echo: {message}"


if __name__ == "__main__":
    print("=== Worker Signals — KubeMQ Celery Transport ===\n")
    print("Worker signals fire on the WORKER process, not the client.")
    print("Start a worker to see signal handler output:\n")
    print("  celery -A examples.signals.worker_signals worker --loglevel=info\n")

    print("Signal execution order on worker startup:")
    print("  1. worker_init        -> Process initializing")
    print("  2. worker_process_init -> Each child process (prefork only)")
    print("  3. worker_ready       -> Fully initialized, accepting tasks")
    print()
    print("Signal execution on worker shutdown:")
    print("  1. worker_shutting_down -> Graceful shutdown begins")
    print()

    print("To test:")
    print("  1. Start a worker and observe signal handler output:")
    print("     celery -A examples.signals.worker_signals worker --loglevel=info")
    print("  2. Press Ctrl+C on the worker to see worker_shutting_down signal.")
    print()
    print("=== Configuration demo complete ===")
