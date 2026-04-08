"""Celery App Factory — KubeMQ Celery Django Integration.

Demonstrates:
- Creating a Celery app instance configured from Django settings
- Auto-discovering tasks from all installed Django apps
- Registering the kubemq:// transport via kubemq_celery import

Usage:
    # In myproject/__init__.py, add:
    from myproject.celery import app as celery_app
    __all__ = ("celery_app",)

Requirements:
    - Django, django-celery-beat, django-celery-results
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import sys

if __name__ == "__main__":
    print("=== Celery App Factory — Django Integration ===\n")
    print("This module configures the Celery app for Django.")
    print("It is imported by myproject/__init__.py, not run directly.\n")
    print("Usage:")
    print("  1. cd examples/integrations/django_integration")
    print("  2. python manage.py migrate")
    print("  3. celery -A myproject worker --loglevel=info")
    print("\n=== Configuration demo complete ===")
    sys.exit(0)

from celery import Celery

import kubemq_celery  # noqa: F401 -- registers kubemq:// transport

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

app = Celery("myproject")

app.config_from_object("django.conf:settings", namespace="CELERY")

app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task that prints the request info."""
    print(f"Request: {self.request!r}")
