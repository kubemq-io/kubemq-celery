"""Django app tasks for KubeMQ Celery example.

Three production-realistic tasks demonstrating:
- Email sending with retry logic
- Report generation with progress tracking
- File upload processing with idempotency
"""

from __future__ import annotations

import logging
import time

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def send_email(self, to: str, subject: str, body: str) -> dict:
    """Send an email notification with automatic retry on failure.

    Args:
        to: Recipient email address.
        subject: Email subject line.
        body: Email body text.

    Returns:
        Dictionary with send status and metadata.
    """
    logger.info("Sending email to %s: %s", to, subject)
    try:
        # Simulate email sending (replace with real SMTP/API call)
        time.sleep(0.5)
        return {
            "to": to,
            "subject": subject,
            "status": "sent",
            "task_id": self.request.id,
        }
    except ConnectionError as exc:
        # Retry with exponential backoff: 10s, 20s, 40s
        raise self.retry(exc=exc, countdown=10 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=2)
def generate_report(self, report_type: str, params: dict | None = None) -> dict:
    """Generate a data report with progress tracking.

    Args:
        report_type: Type of report (e.g., "sales", "inventory", "users").
        params: Optional report parameters (date range, filters, etc.).

    Returns:
        Dictionary with report URL and metadata.
    """
    params = params or {}
    logger.info("Generating %s report with params: %s", report_type, params)

    # Simulate report generation phases
    self.update_state(state="PROGRESS", meta={"step": "gathering_data", "percent": 20})
    time.sleep(1.0)

    self.update_state(state="PROGRESS", meta={"step": "processing", "percent": 60})
    time.sleep(1.0)

    self.update_state(state="PROGRESS", meta={"step": "formatting", "percent": 90})
    time.sleep(0.5)

    return {
        "report_type": report_type,
        "params": params,
        "url": f"/reports/{report_type}-latest.pdf",
        "pages": 24,
        "status": "complete",
    }


@shared_task(bind=True, max_retries=3)
def process_upload(self, file_id: str, filename: str) -> dict:
    """Process an uploaded file (validate, transform, store).

    Args:
        file_id: Unique file identifier.
        filename: Original filename.

    Returns:
        Dictionary with processing results.
    """
    logger.info("Processing upload: %s (%s)", filename, file_id)

    # Simulate file processing
    time.sleep(2.0)

    return {
        "file_id": file_id,
        "filename": filename,
        "size_bytes": 1048576,
        "status": "processed",
        "thumbnail_url": f"/thumbnails/{file_id}.jpg",
    }


@shared_task
def cleanup_old_data(days_threshold: int = 30) -> dict:
    """Delete records older than N days (periodic task).

    Args:
        days_threshold: Delete records older than this many days.

    Returns:
        Dictionary with deletion count.
    """
    logger.info("Cleaning up data older than %d days", days_threshold)
    time.sleep(1.0)
    deleted_count = 42  # simulated
    logger.info("Deleted %d old records", deleted_count)
    return {"deleted": deleted_count, "threshold_days": days_threshold}


@shared_task
def health_check() -> dict:
    """Lightweight health check (periodic task)."""
    return {
        "database": "ok",
        "cache": "ok",
        "broker": "ok",
        "status": "healthy",
    }
