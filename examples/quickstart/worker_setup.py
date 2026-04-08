"""Worker Setup — KubeMQ Celery Transport.

Demonstrates:
- Celery worker CLI flags and configuration
- Concurrency settings
- Log levels and queue selection
- Task definitions that the worker will consume

Usage:
    # Start a worker (basic):
    celery -A examples.quickstart.worker_setup worker --loglevel=info

    # Start with custom concurrency:
    celery -A examples.quickstart.worker_setup worker --loglevel=info --concurrency=4

    # Start consuming specific queues:
    celery -A examples.quickstart.worker_setup worker --loglevel=info -Q celery,reports

    # Start with a custom worker name:
    celery -A examples.quickstart.worker_setup worker --loglevel=info -n worker1@%h

    # Run the example (sends tasks):
    python examples/quickstart/worker_setup.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "worker_setup",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    worker_prefetch_multiplier=1,
    task_acks_late=False,
    worker_max_tasks_per_child=1000,
)


@app.task
def compute(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task(queue="reports")
def generate_report(name: str) -> dict:
    """Generate a named report (routed to 'reports' queue)."""
    return {"report": name, "status": "generated", "rows": 100}


@app.task
def ping() -> str:
    """Health-check task that confirms the worker is alive."""
    return "pong"


if __name__ == "__main__":
    print("Worker Setup Example")
    print("=" * 50)
    print()
    print("Common worker CLI commands:")
    print()
    print("  # Basic worker:")
    print("  celery -A examples.quickstart.worker_setup worker --loglevel=info")
    print()
    print("  # With 4 worker processes:")
    print("  celery -A examples.quickstart.worker_setup worker -c 4 --loglevel=info")
    print()
    print("  # Listen on specific queues:")
    print("  celery -A examples.quickstart.worker_setup worker -Q celery,reports --loglevel=info")
    print()
    print("  # Named worker (useful for multi-worker setups):")
    print("  celery -A examples.quickstart.worker_setup worker -n worker1@%h --loglevel=info")
    print()
    print("  # Debug-level logging:")
    print("  celery -A examples.quickstart.worker_setup worker --loglevel=debug")
    print()

    print("Worker configuration:")
    print(f"  Broker URL:              {app.conf.broker_url}")
    print(f"  Result backend:          {app.conf.result_backend}")
    print(f"  Prefetch multiplier:     {app.conf.worker_prefetch_multiplier}")
    print(f"  Acks late:               {app.conf.task_acks_late}")
    print(f"  Max tasks per child:     {app.conf.worker_max_tasks_per_child}")
    print()
    print("Done!")
