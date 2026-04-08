"""Beat Crontab Schedule — KubeMQ Celery Transport.

Demonstrates:
- Celery Beat with crontab schedules for recurring tasks
- crontab expressions for minute, hour, day_of_week, etc.
- Running the Beat scheduler alongside workers
- Beat state is LOCAL (not stored in KubeMQ)

Usage:
    celery -A examples.scheduling.beat_crontab worker --loglevel=info
    celery -A examples.scheduling.beat_crontab beat --loglevel=info
    python examples/scheduling/beat_crontab.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from celery import Celery
from celery.schedules import crontab

import kubemq_celery  # noqa: F401

app = Celery("beat_crontab")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "enable_utc": True,
        "beat_schedule": {
            # Run every minute
            "every-minute-health": {
                "task": "examples.scheduling.beat_crontab.health_check",
                "schedule": crontab(),  # default: every minute
            },
            # Run every 5 minutes
            "every-5min-metrics": {
                "task": "examples.scheduling.beat_crontab.collect_metrics",
                "schedule": crontab(minute="*/5"),
            },
            # Run every hour at minute 0
            "hourly-cleanup": {
                "task": "examples.scheduling.beat_crontab.cleanup_stale_data",
                "schedule": crontab(minute=0, hour="*"),
            },
            # Run daily at midnight UTC
            "daily-report": {
                "task": "examples.scheduling.beat_crontab.generate_daily_report",
                "schedule": crontab(minute=0, hour=0),
            },
            # Run every Monday at 9:00 AM UTC
            "weekly-digest": {
                "task": "examples.scheduling.beat_crontab.send_weekly_digest",
                "schedule": crontab(minute=0, hour=9, day_of_week="monday"),
            },
            # Run on the 1st of every month at 6:00 AM UTC
            "monthly-billing": {
                "task": "examples.scheduling.beat_crontab.run_monthly_billing",
                "schedule": crontab(minute=0, hour=6, day_of_month=1),
            },
        },
    }
)


@app.task
def health_check() -> dict:
    """Periodic health check (every minute)."""
    return {"status": "healthy", "at": datetime.now(timezone.utc).isoformat()}


@app.task
def collect_metrics() -> dict:
    """Collect system metrics (every 5 minutes)."""
    return {"metrics": "collected", "at": datetime.now(timezone.utc).isoformat()}


@app.task
def cleanup_stale_data() -> dict:
    """Clean up stale data (every hour)."""
    return {"cleaned": True, "at": datetime.now(timezone.utc).isoformat()}


@app.task
def generate_daily_report() -> dict:
    """Generate daily report (midnight UTC)."""
    return {"report": "daily", "at": datetime.now(timezone.utc).isoformat()}


@app.task
def send_weekly_digest() -> dict:
    """Send weekly digest (Monday 9 AM UTC)."""
    return {"digest": "weekly", "at": datetime.now(timezone.utc).isoformat()}


@app.task
def run_monthly_billing() -> dict:
    """Run monthly billing (1st of month, 6 AM UTC)."""
    return {"billing": "monthly", "at": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    print("=== Beat Crontab Schedule — KubeMQ Celery Transport ===\n")

    print("Crontab schedule examples:\n")
    examples = [
        ("crontab()", "Every minute"),
        ("crontab(minute='*/5')", "Every 5 minutes"),
        ("crontab(minute=0, hour='*')", "Every hour at :00"),
        ("crontab(minute=0, hour=0)", "Daily at midnight UTC"),
        ("crontab(minute=0, hour=9, day_of_week='monday')", "Mondays at 9 AM"),
        ("crontab(minute=0, hour=6, day_of_month=1)", "1st of month at 6 AM"),
        ("crontab(minute='*/15', hour='8-17')", "Every 15min during business hours"),
        ("crontab(minute=0, hour=0, day_of_week='1,3,5')", "Mon/Wed/Fri at midnight"),
    ]
    for expr, desc in examples:
        print(f"  {expr}")
        print(f"    -> {desc}")
        print()

    # Show configured schedules
    print("Configured beat_schedule entries:")
    for name, config in app.conf.beat_schedule.items():
        print(f"  {name}: {config['task']}")
        print(f"    schedule: {config['schedule']}")
    print()

    print("To start Beat scheduler and worker:")
    print("  # Terminal 1: Start worker")
    print("  celery -A examples.scheduling.beat_crontab worker --loglevel=info")
    print()
    print("  # Terminal 2: Start beat scheduler")
    print("  celery -A examples.scheduling.beat_crontab beat --loglevel=info")
    print()
    print("  # Or combined (development only):")
    print("  celery -A examples.scheduling.beat_crontab worker -B --loglevel=info")
    print()
    print("IMPORTANT: Beat state is LOCAL (celerybeat-schedule file).")
    print("           Running multiple Beat instances causes duplicate tasks.")
    print("           In production, run exactly ONE Beat instance.")

    print("\n=== Beat crontab demo complete ===")
