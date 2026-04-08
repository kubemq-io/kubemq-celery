"""Dynamic Periodic Tasks — KubeMQ Celery Transport.

Demonstrates:
- Runtime manipulation of beat_schedule
- Adding and removing periodic tasks dynamically
- Modifying schedule intervals at runtime
- Programmatic beat_schedule management

Usage:
    celery -A examples.scheduling.dynamic_periodic_tasks worker --loglevel=info
    celery -A examples.scheduling.dynamic_periodic_tasks beat --loglevel=info
    python examples/scheduling/dynamic_periodic_tasks.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("dynamic_periodic_tasks")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "enable_utc": True,
        "beat_schedule": {
            "initial-heartbeat": {
                "task": "examples.scheduling.dynamic_periodic_tasks.heartbeat",
                "schedule": timedelta(seconds=30),
            },
        },
    }
)


@app.task
def heartbeat() -> dict:
    """Periodic heartbeat task."""
    return {"beat": True, "at": datetime.now(timezone.utc).isoformat()}


@app.task
def monitor_service(service_name: str) -> dict:
    """Monitor a specific service."""
    return {
        "service": service_name,
        "status": "up",
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@app.task
def custom_periodic(job_id: str, params: dict) -> dict:
    """A generic periodic task with custom parameters."""
    return {
        "job_id": job_id,
        "params": params,
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }


def add_periodic_task(
    name: str,
    task: str,
    schedule: timedelta,
    args: tuple | None = None,
    kwargs: dict | None = None,
) -> None:
    """Add a periodic task to the beat schedule at runtime."""
    entry = {
        "task": task,
        "schedule": schedule,
    }
    if args:
        entry["args"] = args
    if kwargs:
        entry["kwargs"] = kwargs
    app.conf.beat_schedule[name] = entry
    print(f"  Added: {name} -> {task} every {schedule}")


def remove_periodic_task(name: str) -> None:
    """Remove a periodic task from the beat schedule."""
    if name in app.conf.beat_schedule:
        del app.conf.beat_schedule[name]
        print(f"  Removed: {name}")
    else:
        print(f"  Not found: {name}")


def list_periodic_tasks() -> None:
    """List all configured periodic tasks."""
    print("  Current beat_schedule:")
    for name, config in app.conf.beat_schedule.items():
        print(f"    {name}: {config['task']} every {config['schedule']}")


if __name__ == "__main__":
    print("=== Dynamic Periodic Tasks — KubeMQ Celery Transport ===\n")

    # Show initial schedule
    print("[1] Initial beat_schedule:")
    list_periodic_tasks()
    print()

    # Add new periodic tasks dynamically
    print("[2] Adding periodic tasks at runtime...")
    add_periodic_task(
        "monitor-api",
        "examples.scheduling.dynamic_periodic_tasks.monitor_service",
        timedelta(seconds=15),
        args=("api-gateway",),
    )
    add_periodic_task(
        "monitor-db",
        "examples.scheduling.dynamic_periodic_tasks.monitor_service",
        timedelta(minutes=1),
        args=("postgres",),
    )
    add_periodic_task(
        "custom-job-1",
        "examples.scheduling.dynamic_periodic_tasks.custom_periodic",
        timedelta(seconds=45),
        kwargs={"job_id": "job-001", "params": {"threshold": 95}},
    )
    print()

    # Show updated schedule
    print("[3] Updated beat_schedule:")
    list_periodic_tasks()
    print()

    # Modify an existing task's schedule
    print("[4] Modifying heartbeat interval from 30s to 60s...")
    app.conf.beat_schedule["initial-heartbeat"]["schedule"] = timedelta(seconds=60)
    print()

    # Remove a periodic task
    print("[5] Removing monitor-db task...")
    remove_periodic_task("monitor-db")
    print()

    # Show final schedule
    print("[6] Final beat_schedule:")
    list_periodic_tasks()
    print()

    print("--- Runtime schedule management notes ---")
    print("  Modifying app.conf.beat_schedule at runtime works when Beat")
    print("  is running in the same process (e.g., worker -B flag).")
    print()
    print("  For separate Beat processes, consider:")
    print("    - celery-beat-redis or django-celery-beat for DB-backed schedules")
    print("    - Restarting Beat after config changes")
    print("    - Using app.control to signal Beat to reload")
    print()
    print("  IMPORTANT: Beat state is LOCAL. Changes are lost on restart")
    print("  unless persisted to a database or config file.")

    print("\n=== Dynamic periodic tasks demo complete ===")
