"""Django settings for myproject (KubeMQ Celery example).

This is an excerpt showing the Celery/KubeMQ-specific settings.
A full Django settings file would include DATABASES, TEMPLATES, etc.
"""

from __future__ import annotations

import os
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
    # django-celery-beat for dynamic schedules (optional)
    # Uncomment to enable runtime-editable periodic tasks via Django admin:
    # "django_celery_beat",
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
    # Daily cleanup at midnight UTC
    "cleanup-old-data": {
        "task": "myapp.tasks.cleanup_old_data",
        "schedule": crontab(hour=0, minute=0),
        "args": (30,),  # delete records older than 30 days
    },
    # Health check every 5 minutes
    "health-check": {
        "task": "myapp.tasks.health_check",
        "schedule": timedelta(minutes=5),
    },
}

# --- django-celery-beat (Dynamic Schedules) ---
# Uncomment to enable runtime-editable periodic tasks via Django admin.
# Requires: pip install django-celery-beat
# Add "django_celery_beat" to INSTALLED_APPS above, then run:
#   python manage.py migrate
#
# CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
#
# Then add/modify periodic tasks in Django admin at /admin/django_celery_beat/
# without restarting the beat scheduler.
