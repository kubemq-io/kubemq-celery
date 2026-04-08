"""Beat Timedelta Schedule — KubeMQ Celery Transport.

Demonstrates:
- Celery Beat with timedelta interval schedules
- Fixed-interval periodic task execution
- Combining timedelta schedules with task arguments
- Beat scheduler publishing to KubeMQ at regular intervals

Usage:
    celery -A examples.scheduling.beat_timedelta worker --loglevel=info
    celery -A examples.scheduling.beat_timedelta beat --loglevel=info
    python examples/scheduling/beat_timedelta.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("beat_timedelta")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "enable_utc": True,
        "beat_schedule": {
            # Run every 10 seconds
            "heartbeat-10s": {
                "task": "examples.scheduling.beat_timedelta.heartbeat",
                "schedule": timedelta(seconds=10),
            },
            # Run every 30 seconds with arguments
            "sensor-reading-30s": {
                "task": "examples.scheduling.beat_timedelta.read_sensor",
                "schedule": timedelta(seconds=30),
                "args": ("temperature",),
            },
            # Run every 2 minutes with keyword arguments
            "cache-refresh-2m": {
                "task": "examples.scheduling.beat_timedelta.refresh_cache",
                "schedule": timedelta(minutes=2),
                "kwargs": {"cache_name": "user_profiles", "ttl": 300},
            },
            # Run every 5 minutes
            "stats-snapshot-5m": {
                "task": "examples.scheduling.beat_timedelta.snapshot_stats",
                "schedule": timedelta(minutes=5),
            },
            # Run every hour
            "hourly-sync": {
                "task": "examples.scheduling.beat_timedelta.sync_data",
                "schedule": timedelta(hours=1),
            },
        },
    }
)


@app.task
def heartbeat() -> dict:
    """Periodic heartbeat — runs every 10 seconds."""
    return {"beat": True, "at": datetime.now(timezone.utc).isoformat()}


@app.task
def read_sensor(sensor_type: str) -> dict:
    """Read a sensor value periodically."""
    import random

    return {
        "sensor": sensor_type,
        "value": round(random.uniform(20.0, 30.0), 2),
        "unit": "°C",
        "at": datetime.now(timezone.utc).isoformat(),
    }


@app.task
def refresh_cache(cache_name: str, ttl: int = 600) -> dict:
    """Refresh a cache periodically."""
    return {
        "cache": cache_name,
        "ttl": ttl,
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }


@app.task
def snapshot_stats() -> dict:
    """Take a statistics snapshot."""
    return {"snapshot": True, "at": datetime.now(timezone.utc).isoformat()}


@app.task
def sync_data() -> dict:
    """Sync data from external source."""
    return {"synced": True, "at": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    print("=== Beat Timedelta Schedule — KubeMQ Celery Transport ===\n")

    print("Timedelta schedules run tasks at fixed intervals:\n")
    for name, config in app.conf.beat_schedule.items():
        sched = config["schedule"]
        args = config.get("args", ())
        kwargs = config.get("kwargs", {})
        print(f"  {name}:")
        print(f"    task:     {config['task']}")
        print(f"    interval: {sched}")
        if args:
            print(f"    args:     {args}")
        if kwargs:
            print(f"    kwargs:   {kwargs}")
        print()

    print("To run periodic tasks:")
    print("  # Terminal 1: Worker")
    print("  celery -A examples.scheduling.beat_timedelta worker --loglevel=info")
    print()
    print("  # Terminal 2: Beat scheduler")
    print("  celery -A examples.scheduling.beat_timedelta beat --loglevel=info")
    print()
    print("Timedelta vs Crontab:")
    print("  timedelta(seconds=30)  -> Fixed 30-second interval from last run")
    print("  crontab(minute='*/1')  -> At specific clock times (every minute)")
    print()
    print("IMPORTANT: Beat state is LOCAL. Run exactly ONE Beat process.")

    print("\n=== Beat timedelta demo complete ===")
