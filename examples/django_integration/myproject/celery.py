"""Celery application factory for Django + KubeMQ.

This module creates and configures the Celery app instance that Django
uses for background task processing. It auto-discovers tasks from all
installed Django apps.

Usage:
    # In myproject/__init__.py, add:
    from myproject.celery import app as celery_app
    __all__ = ("celery_app",)
"""

from __future__ import annotations

import os

import kubemq_celery  # noqa: F401 -- registers kubemq:// transport
from celery import Celery

# Set the default Django settings module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

app = Celery("myproject")

# Load Celery config from Django settings (variables prefixed with CELERY_)
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks in all installed Django apps
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task that prints the request info."""
    print(f"Request: {self.request!r}")
