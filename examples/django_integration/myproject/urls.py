"""URL configuration for myproject."""

from __future__ import annotations

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
