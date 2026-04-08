"""URL Configuration — KubeMQ Celery Django Integration.

Demonstrates:
- Django URL routes for dispatching Celery tasks (email, report, upload)
- Task status polling endpoint
- Health check endpoint

Usage:
    Include in ROOT_URLCONF. POST to /tasks/send-email/, /tasks/generate-report/,
    or /tasks/process-upload/ to dispatch tasks. GET /tasks/status/<task_id>/ to poll.

Requirements:
    - Django, django-celery-beat, django-celery-results
    - kubemq-celery installed
"""

from __future__ import annotations

import sys

if __name__ == "__main__":
    print("=== URL Configuration — Django Integration ===\n")
    print("This module defines Django URL routes. Run via manage.py:\n")
    print("  cd examples/integrations/django_integration")
    print("  python manage.py runserver")
    print("\n=== Configuration demo complete ===")
    sys.exit(0)

from django.contrib import admin
from django.urls import path
from myapp import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("tasks/send-email/", views.dispatch_send_email, name="send-email"),
    path("tasks/generate-report/", views.dispatch_generate_report, name="generate-report"),
    path("tasks/process-upload/", views.dispatch_process_upload, name="process-upload"),
    path("tasks/status/<str:task_id>/", views.task_status, name="task-status"),
    path("health/", views.health_check, name="health"),
]
