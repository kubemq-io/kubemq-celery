#!/usr/bin/env python
"""Django Management Script — KubeMQ Celery Django Integration.

Demonstrates:
- Standard Django management entry point configured for KubeMQ Celery

Usage:
    python manage.py runserver
    python manage.py migrate
    celery -A myproject worker --loglevel=info

Requirements:
    - Django, django-celery-beat, django-celery-results
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import sys


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError("Couldn't import Django. Install it with: pip install django") from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
