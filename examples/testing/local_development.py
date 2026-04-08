"""Local Development Setup — KubeMQ Celery Transport.

Demonstrates:
- Development-friendly Celery worker configuration
- --pool=solo for single-threaded debugging
- --loglevel=debug for verbose output
- --autoreload for automatic code reloading
- Environment-based configuration switching

Usage:
    # Start worker with dev-friendly settings:
    celery -A examples.testing.local_development worker \
        --pool=solo --loglevel=debug --autoreload

    # Or use the built-in launcher:
    python examples/testing/local_development.py

    # Send test tasks:
    python -c "
    from examples.testing.local_development import app, add, greet
    print(add.delay(2, 3).get(timeout=10))
    print(greet.delay('Dev').get(timeout=10))
    "

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import logging
import os
import sys

from celery import Celery, signals

import kubemq_celery  # noqa: F401

IS_DEV = os.environ.get("CELERY_ENV", "development") == "development"

app = Celery(
    "local_development",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

if IS_DEV:
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        result_expires=3600,
        worker_prefetch_multiplier=1,
        worker_concurrency=1,
        task_track_started=True,
        worker_hijack_root_logger=False,
        task_acks_late=True,
        broker_transport_options={
            "wait_timeout": 1,
        },
    )
else:
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        result_expires=3600,
        worker_prefetch_multiplier=4,
    )


@signals.setup_logging.connect
def setup_logging(**kwargs):
    """Configure structured logging for development."""
    logging.basicConfig(
        level=logging.DEBUG if IS_DEV else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    print(f"[add] Computing {x} + {y}")
    return x + y


@app.task
def greet(name: str) -> str:
    """Return a greeting."""
    print(f"[greet] Greeting {name}")
    return f"Hello, {name}!"


@app.task(bind=True)
def debug_task(self) -> dict:
    """Return debug information about the task and worker."""
    info = {
        "task_id": self.request.id,
        "task_name": self.name,
        "hostname": self.request.hostname,
        "delivery_info": dict(self.request.delivery_info or {}),
        "is_dev": IS_DEV,
        "broker": str(app.conf.broker_url),
    }
    print(f"[debug_task] {info}")
    return info


@app.task(bind=True, max_retries=2)
def flaky_task(self, succeed_on_attempt: int = 2) -> dict:
    """Task that fails a configurable number of times (for retry testing)."""
    attempt = self.request.retries + 1
    print(f"[flaky_task] Attempt {attempt}/{succeed_on_attempt}")
    if attempt < succeed_on_attempt:
        raise self.retry(countdown=1, exc=RuntimeError(f"Attempt {attempt} failed"))
    return {"attempt": attempt, "status": "success"}


if __name__ == "__main__":
    print("=== Local Development Setup ===")
    print(f"Environment: {'development' if IS_DEV else 'production'}")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Concurrency: {app.conf.worker_concurrency}")
    print(f"Prefetch: {app.conf.worker_prefetch_multiplier}")
    print()
    print("Recommended dev worker command:")
    print("  celery -A examples.testing.local_development worker \\")
    print("      --pool=solo --loglevel=debug --autoreload")
    print()
    print("Configuration details:")
    print(f"  task_acks_late:             {app.conf.task_acks_late}")
    print(f"  task_track_started:         {app.conf.task_track_started}")
    print(f"  worker_prefetch_multiplier: {app.conf.worker_prefetch_multiplier}")
    print(f"  worker_concurrency:         {app.conf.worker_concurrency}")
    print()

    if "--worker" in sys.argv:
        print("Starting development worker...")
        app.worker_main(
            [
                "worker",
                "--pool=solo",
                "--loglevel=debug",
                "--without-heartbeat",
                "-Q",
                "celery",
            ]
        )
    else:
        print("To start the worker, run:")
        print("  python examples/testing/local_development.py --worker")
        print()
        print("Or use celery CLI:")
        print("  celery -A examples.testing.local_development worker --pool=solo --loglevel=debug")
