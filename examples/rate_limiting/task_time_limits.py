"""Task Time Limits — KubeMQ Celery Transport.

Demonstrates:
- Worker-enforced time limits via CLI and app config
- --time-limit (hard) and --soft-time-limit (soft) CLI flags
- task_time_limit and task_soft_time_limit app configuration
- Per-task time limits via @app.task(time_limit=N, soft_time_limit=N)

Usage:
    celery -A examples.rate_limiting.task_time_limits worker --loglevel=info --time-limit=30
    celery -A examples.rate_limiting.task_time_limits worker --loglevel=info --soft-time-limit=10
    python examples/rate_limiting/task_time_limits.py

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
    "task_time_limits",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    # Global time limits (applied to all tasks without per-task overrides)
    task_time_limit=300,  # Hard limit: 5 minutes (SIGKILL)
    task_soft_time_limit=240,  # Soft limit: 4 minutes (SoftTimeLimitExceeded)
)


@app.task(time_limit=10, soft_time_limit=8)
def quick_with_limits(seconds: float) -> dict:
    """Task with per-task time limits.

    soft_time_limit=8: raises SoftTimeLimitExceeded after 8s
    time_limit=10: worker kills task after 10s (SIGKILL in prefork)
    """
    time.sleep(seconds)
    return {"slept": seconds, "status": "completed"}


@app.task
def uses_global_limits(seconds: float) -> dict:
    """Task using global time limits from app config.

    Inherits task_time_limit=300 and task_soft_time_limit=240.
    """
    time.sleep(seconds)
    return {"slept": seconds, "status": "completed"}


@app.task(time_limit=60)
def hard_limit_only(seconds: float) -> dict:
    """Task with only a hard time limit, no soft limit.

    Worker kills the task after 60 seconds with no warning.
    """
    time.sleep(seconds)
    return {"slept": seconds, "status": "completed"}


if __name__ == "__main__":
    print("=== Task Time Limits — KubeMQ Celery Transport ===\n")

    print("Time limit types:\n")
    print("  Soft time limit (task_soft_time_limit):")
    print("    -> Raises SoftTimeLimitExceeded exception in the task")
    print("    -> Task can catch it for graceful cleanup")
    print("    -> Set via: @app.task(soft_time_limit=N) or app.conf")
    print("    -> CLI: --soft-time-limit=N")
    print()
    print("  Hard time limit (task_time_limit):")
    print("    -> Worker kills the task process (SIGKILL in prefork)")
    print("    -> Cannot be caught — immediate termination")
    print("    -> Set via: @app.task(time_limit=N) or app.conf")
    print("    -> CLI: --time-limit=N")
    print()

    print(f"[config] Global task_time_limit      = {app.conf.task_time_limit}s")
    print(f"[config] Global task_soft_time_limit = {app.conf.task_soft_time_limit}s\n")

    print("Configured tasks:")
    print("  quick_with_limits   -> time_limit=10, soft_time_limit=8")
    print("  uses_global_limits  -> inherits global limits (300s / 240s)")
    print("  hard_limit_only     -> time_limit=60, no soft limit")
    print()

    print("--- CLI time limit flags ---")
    print("  celery -A app worker --time-limit=30       # Hard limit all tasks")
    print("  celery -A app worker --soft-time-limit=20  # Soft limit all tasks")
    print("  celery -A app worker --time-limit=30 --soft-time-limit=20  # Both")
    print()
    print("  CLI flags override app.conf but NOT per-task decorators.")
    print("  Per-task @app.task(time_limit=N) always takes priority.")
    print()

    print("To test:")
    print("  1. Start a worker:")
    print("     celery -A examples.rate_limiting.task_time_limits worker --loglevel=info")
    print("  2. Send tasks with various durations to observe time limit enforcement.")
    print()
    print("NOTE: Time limits are enforced by the worker process, not KubeMQ.")
    print("      Hard limits only work with the prefork pool (not solo/threads).")
    print()
    print("=== Configuration demo complete ===")
