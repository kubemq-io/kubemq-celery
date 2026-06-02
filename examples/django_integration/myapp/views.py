"""Django views that dispatch Celery tasks and check results."""

from __future__ import annotations

import json

from celery.result import AsyncResult
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from myapp.tasks import generate_report, process_upload, send_email


@csrf_exempt
@require_POST
def dispatch_send_email(request):
    """POST /tasks/send-email/ -- dispatch an email task.

    Request body (JSON):
        {"to": "user@example.com", "subject": "Hello", "body": "..."}
    """
    data = json.loads(request.body)
    result = send_email.delay(
        to=data["to"],
        subject=data["subject"],
        body=data.get("body", ""),
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@csrf_exempt
@require_POST
def dispatch_generate_report(request):
    """POST /tasks/generate-report/ -- dispatch a report generation task.

    Request body (JSON):
        {"report_type": "sales", "params": {"start": "2026-01-01"}}
    """
    data = json.loads(request.body)
    result = generate_report.delay(
        report_type=data["report_type"],
        params=data.get("params"),
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@csrf_exempt
@require_POST
def dispatch_process_upload(request):
    """POST /tasks/process-upload/ -- dispatch a file processing task.

    Request body (JSON):
        {"file_id": "abc-123", "filename": "report.csv"}
    """
    data = json.loads(request.body)
    result = process_upload.delay(
        file_id=data["file_id"],
        filename=data["filename"],
    )
    return JsonResponse({"task_id": result.id, "status": "queued"}, status=202)


@require_GET
def task_status(request, task_id):
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
def health_check(request):
    """GET /health/ -- application health check."""
    return JsonResponse({"status": "ok", "service": "django-kubemq-celery"})
