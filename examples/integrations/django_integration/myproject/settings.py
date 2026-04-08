"""Django Settings — KubeMQ Celery Django Integration.

Demonstrates:
- Celery broker and result backend configured with kubemq:// URLs
- KubeMQ-specific broker transport options (TTL, dead-letter, batching)
- Static beat schedule and django-celery-beat dynamic scheduler
- django-celery-results for storing task results in the database

Usage:
    Set CELERY_BROKER_URL and CELERY_RESULT_BACKEND env vars to override defaults.
    Add periodic tasks via Django admin at /admin/django_celery_beat/.

Requirements:
    - Django, django-celery-beat, django-celery-results
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import sys

if __name__ == "__main__":
    print("=== Django Settings — KubeMQ Celery Integration ===\n")
    print("This module contains Django settings. Run via manage.py:\n")
    print("  cd examples/integrations/django_integration")
    print("  python manage.py runserver")
    print("\n=== Configuration demo complete ===")
    sys.exit(0)
from datetime import timedelta

from celery.schedules import crontab

# --- Django Core Settings ---

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "change-me-in-production")
DEBUG = os.environ.get("DEBUG", "true").lower() == "true"
ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")
ROOT_URLCONF = "myproject.urls"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "myapp",
    "django_celery_beat",
    "django_celery_results",
]

# --- Celery Configuration (KubeMQ transport) ---

CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000")
CELERY_RESULT_EXPIRES = 86400  # 24 hours (KubeMQ maximum)

CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TIMEZONE = "UTC"
CELERY_TASK_TRACK_STARTED = True

# Production: drive these via environment variables (e.g. KUBEMQ_WAIT_TIMEOUT)
# rather than hard-coding values here.
CELERY_BROKER_TRANSPORT_OPTIONS = {
    "wait_timeout": 1,
    "dead_letter_queue": "celery-dead-letters",
    "max_receive_count": 3,
    "message_expiration": 3600,  # 1 hour default TTL
    "max_batch_size": 10,
}

# --- Static Beat Schedule ---
# For tasks that run on a fixed schedule. Defined at deploy time.

CELERY_BEAT_SCHEDULE = {
    "cleanup-old-data": {
        "task": "myapp.tasks.cleanup_old_data",
        "schedule": crontab(hour=0, minute=0),
        "args": (30,),
    },
    "health-check": {
        "task": "myapp.tasks.health_check",
        "schedule": timedelta(minutes=5),
    },
    "warm-cache": {
        "task": "myapp.tasks.warm_cache",
        "schedule": timedelta(minutes=30),
    },
    "daily-digest": {
        "task": "myapp.tasks.send_daily_digest",
        "schedule": crontab(hour=8, minute=0),
    },
}

# --- django-celery-beat (Dynamic Schedules) ---
# Enables runtime-editable periodic tasks via Django admin.
# Add/modify periodic tasks in Django admin at /admin/django_celery_beat/
# without restarting the beat scheduler.

CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
