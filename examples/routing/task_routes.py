"""Task Routes — KubeMQ Celery Transport.

Demonstrates:
- task_routes configuration for automatic task-to-queue mapping
- Routing tasks to specific KubeMQ queue channels
- Pattern-based routing with wildcards
- Multiple routing strategies (dict, list, function)

Usage:
    celery -A examples.routing.task_routes worker --loglevel=info -Q default,email,analytics
    python examples/routing/task_routes.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("task_routes")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_routes": {
            # Route specific tasks to named queues (KubeMQ queue channels)
            "examples.routing.task_routes.send_email": {"queue": "email"},
            "examples.routing.task_routes.send_sms": {"queue": "email"},
            "examples.routing.task_routes.track_event": {"queue": "analytics"},
            "examples.routing.task_routes.generate_report": {"queue": "analytics"},
            # Wildcard pattern: all tasks matching pattern go to a queue
            "examples.routing.task_routes.critical_*": {"queue": "high-priority"},
        },
    }
)


@app.task
def send_email(to: str, subject: str) -> dict:
    """Send email — routed to 'email' queue."""
    return {"to": to, "subject": subject, "queue": "email", "status": "sent"}


@app.task
def send_sms(phone: str, message: str) -> dict:
    """Send SMS — routed to 'email' queue (notifications)."""
    return {"phone": phone, "message": message, "queue": "email", "status": "sent"}


@app.task
def track_event(event: str, data: dict) -> dict:
    """Track analytics event — routed to 'analytics' queue."""
    return {"event": event, "data": data, "queue": "analytics"}


@app.task
def generate_report(report_type: str) -> dict:
    """Generate report — routed to 'analytics' queue."""
    return {"report_type": report_type, "queue": "analytics", "status": "generated"}


@app.task
def critical_alert(message: str) -> dict:
    """Critical alert — routed to 'high-priority' queue."""
    return {"message": message, "queue": "high-priority", "priority": "critical"}


@app.task
def default_task(data: str) -> dict:
    """Task with no route — goes to default queue ('celery')."""
    return {"data": data, "queue": "celery (default)"}


if __name__ == "__main__":
    print("=== Task Routes — KubeMQ Celery Transport ===\n")

    print("Configured task_routes:")
    for pattern, route in app.conf.task_routes.items():
        print(f"  {pattern} -> queue='{route['queue']}'")
    print()

    print("Each queue maps to a KubeMQ queue channel.\n")

    print("--- Worker queue consumption ---")
    print("  Workers only process tasks from queues they consume:")
    print("  celery -A app worker -Q default          # Only 'default' queue")
    print("  celery -A app worker -Q email             # Only 'email' queue")
    print("  celery -A app worker -Q default,email,analytics  # Multiple queues")
    print()
    print("  Each queue name maps to a KubeMQ queue channel.")
    print("  Tasks routed to unconsumed queues wait in KubeMQ until a")
    print("  worker starts consuming that queue.")
    print()

    print("To test:")
    print("  1. Start a worker consuming all queues:")
    print(
        "     celery -A examples.routing.task_routes worker "
        "-Q default,email,analytics,high-priority --loglevel=info"
    )
    print()
    print("=== Configuration demo complete ===")
