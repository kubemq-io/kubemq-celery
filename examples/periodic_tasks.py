"""Celery Beat periodic tasks with KubeMQ transport.

Demonstrates:
- Static beat_schedule dict with crontab and timedelta
- 4 periodic tasks with different schedule types
- Both embedded beat (worker --beat) and standalone beat process

Run (standalone beat + separate worker -- RECOMMENDED for production):
    celery -A periodic_tasks beat --loglevel=info
    celery -A periodic_tasks worker --loglevel=info

Run (embedded beat -- single process, development only):
    celery -A periodic_tasks worker --beat --loglevel=info

NOTE: Beat schedule state is stored locally (celerybeat-schedule file),
NOT in the KubeMQ broker. This means only ONE beat scheduler should run
at a time to avoid duplicate task dispatch.

For dynamic schedules (add/modify periodic tasks at runtime), use
django-celery-beat with a database backend:
    pip install django-celery-beat
    # In Django settings:
    # INSTALLED_APPS += ["django_celery_beat"]
    # CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
"""

from __future__ import annotations

import logging
import os
import time
from datetime import timedelta

import kubemq_celery  # noqa: F401 -- registers transport
from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

app = Celery("periodic_tasks")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 86400,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "broker_transport_options": {
            "wait_timeout": 1,
            "message_expiration": 3600,
        },
    }
)

# --- Static Beat Schedule ---

app.conf.beat_schedule = {
    # Daily cleanup at midnight UTC
    "cleanup-old-data": {
        "task": "periodic_tasks.cleanup_old_data",
        "schedule": crontab(hour=0, minute=0),
        "args": (30,),  # delete records older than 30 days
        "options": {"queue": "maintenance"},
    },
    # Hourly email summary
    "email-daily-summary": {
        "task": "periodic_tasks.send_email_summary",
        "schedule": crontab(minute=0),  # every hour at :00
        "kwargs": {"report_type": "hourly"},
    },
    # Health check every 5 minutes
    "health-check": {
        "task": "periodic_tasks.health_check",
        "schedule": timedelta(minutes=5),
    },
    # Weekly report every Monday at 9:00 AM UTC
    "weekly-report": {
        "task": "periodic_tasks.generate_weekly_report",
        "schedule": crontab(hour=9, minute=0, day_of_week=1),
        "kwargs": {"format": "pdf"},
    },
}
app.conf.timezone = "UTC"


# --- Task Definitions ---


@app.task(bind=True, max_retries=3)
def cleanup_old_data(self, days_threshold: int = 30) -> dict:
    """Delete records older than N days."""
    logger.info("Cleaning up data older than %d days", days_threshold)
    time.sleep(1.0)  # simulate DB cleanup
    deleted_count = 42  # simulated
    logger.info("Deleted %d old records", deleted_count)
    return {"deleted": deleted_count, "threshold_days": days_threshold}


@app.task(bind=True, max_retries=3)
def send_email_summary(self, report_type: str = "hourly") -> dict:
    """Send periodic email summary."""
    logger.info("Sending %s email summary", report_type)
    time.sleep(0.5)
    return {"report_type": report_type, "recipients": 15, "status": "sent"}


@app.task
def health_check() -> dict:
    """Check system health (lightweight, no retries needed)."""
    checks = {
        "database": "ok",
        "cache": "ok",
        "broker": "ok",
        "timestamp": time.time(),
    }
    logger.info("Health check: all systems operational")
    return checks


@app.task(bind=True, max_retries=2)
def generate_weekly_report(self, format: str = "pdf") -> dict:
    """Generate comprehensive weekly report."""
    logger.info("Generating weekly report in %s format", format)
    time.sleep(3.0)  # simulate report generation
    return {"format": format, "pages": 24, "url": "/reports/weekly-latest.pdf"}
