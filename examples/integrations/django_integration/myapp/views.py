"""Django Views — KubeMQ Celery Django Integration.

Demonstrates:
- Dispatching Celery tasks from Django views via .delay()
- Polling task status and results with AsyncResult
- Returning JSON responses with task IDs for async tracking

Usage:
    POST /tasks/send-email/, /tasks/generate-report/, /tasks/process-upload/
    GET /tasks/status/<task_id>/ to poll results. GET /health/ for health check.

Requirements:
    - Django, django-celery-beat, django-celery-results
    - kubemq-celery installed
"""

from __future__ import annotations

import sys

if __name__ == "__main__":
    print("=== Django Views — KubeMQ Celery Integration ===\n")
    print("This module defines Django views. Run via manage.py:\n")
    print("  cd examples/integrations/django_integration")
    print("  python manage.py runserver")
    print("\n=== Configuration demo complete ===")
    sys.exit(0)

import json

from celery.result import AsyncResult
from django.http import HttpRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from myapp.tasks import generate_report, process_upload, send_email


@csrf_exempt
@require_POST
def dispatch_send_email(request: HttpRequest) -> JsonResponse:
    """POST /tasks/send-email/ -- dispatch an email task.

    Request body (JSON):
        {"to": "user@example.com", "subject": "Hello", "body": "..."}
    """
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    result = send_email.delay(
        to=data["to"],
        subject=data["subject"],
        body=data.get("body", ""),
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@csrf_exempt
@require_POST
def dispatch_generate_report(request: HttpRequest) -> JsonResponse:
    """POST /tasks/generate-report/ -- dispatch a report generation task.

    Request body (JSON):
        {"report_type": "sales", "params": {"start": "2026-01-01"}}
    """
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    result = generate_report.delay(
        report_type=data["report_type"],
        params=data.get("params"),
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@csrf_exempt
@require_POST
def dispatch_process_upload(request: HttpRequest) -> JsonResponse:
    """POST /tasks/process-upload/ -- dispatch a file processing task.

    Request body (JSON):
        {"file_id": "abc-123", "filename": "report.csv"}
    """
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    result = process_upload.delay(
        file_id=data["file_id"],
        filename=data["filename"],
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@require_GET
def task_status(request: HttpRequest, task_id: str) -> JsonResponse:
    """GET /tasks/status/<task_id>/ -- check task status and result."""
    result = AsyncResult(task_id)
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }
    if result.failed():
        response["error"] = str(result.result)
    if result.status == "PROGRESS":
        response["progress"] = result.info
    return JsonResponse(response)


@require_GET
def health_check(request: HttpRequest) -> JsonResponse:
    """GET /health/ -- application health check."""
    return JsonResponse({"status": "ok", "service": "django-kubemq-celery"})
