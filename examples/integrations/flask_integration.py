"""Flask Integration — KubeMQ Celery Transport.

Demonstrates:
- Flask routes dispatching Celery tasks via KubeMQ
- Task submission and status polling endpoints
- Flask application factory pattern with Celery init
- JSON response handling

Usage:
    # Terminal 1: Start the Flask app
    flask --app examples.integrations.flask_integration:app run --port 5001

    # Terminal 2: Start the Celery worker
    celery -A examples.integrations.flask_integration:celery_app worker --loglevel=info

    # Terminal 3: Test
    curl -X POST http://localhost:5001/tasks/notify \
        -H "Content-Type: application/json" \
        -d '{"user_id": "42", "message": "Hello"}'
    curl http://localhost:5001/tasks/status/<task_id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install flask
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

try:
    from flask import Flask, Response, jsonify, request
except ImportError:
    raise ImportError("Flask is required. Install with: pip install flask")

celery_app = Celery(
    "flask_integration",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


@celery_app.task(bind=True, max_retries=3)
def send_notification(self, user_id: str, message: str) -> dict:
    """Send a notification (simulated)."""
    print(f"[send_notification] user={user_id} msg={message}")
    time.sleep(0.5)
    return {"user_id": user_id, "message": message, "status": "sent"}


@celery_app.task
def process_data(dataset_id: str) -> dict:
    """Process a dataset (simulated)."""
    print(f"[process_data] dataset={dataset_id}")
    time.sleep(1.0)
    return {"dataset_id": dataset_id, "rows_processed": 1000, "status": "complete"}


@celery_app.task
def generate_report(report_id: str, template: str = "default") -> dict:
    """Generate a report (simulated)."""
    print(f"[generate_report] report={report_id} template={template}")
    time.sleep(0.8)
    return {"report_id": report_id, "url": f"/reports/{report_id}.pdf", "pages": 12}


app = Flask(__name__)


@app.post("/tasks/notify")
def dispatch_notification() -> tuple[Response, int]:
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON in request body"}), 400
    user_id = data.get("user_id", "unknown")
    message = data.get("message", "")
    result = send_notification.delay(user_id, message)
    return jsonify({"task_id": result.id, "status": "queued"}), 202


@app.post("/tasks/process")
def dispatch_processing() -> tuple[Response, int]:
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON in request body"}), 400
    dataset_id = data.get("dataset_id", "default")
    result = process_data.delay(dataset_id)
    return jsonify({"task_id": result.id, "status": "queued"}), 202


@app.post("/tasks/report")
def dispatch_report() -> tuple[Response, int]:
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON in request body"}), 400
    report_id = data.get("report_id", "1")
    template = data.get("template", "default")
    result = generate_report.delay(report_id, template)
    return jsonify({"task_id": result.id, "status": "queued"}), 202


@app.get("/tasks/status/<task_id>")
def task_status(task_id: str) -> Response:
    result = AsyncResult(task_id, app=celery_app)
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }
    if result.failed():
        response["error"] = str(result.result)
    return jsonify(response)


@app.get("/health")
def health() -> Response:
    return jsonify({"status": "ok", "service": "flask-kubemq-celery"})


if __name__ == "__main__":
    print("=== Flask + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.flask_integration:celery_app worker --loglevel=info"
    )
    print("  2. Start the Flask server:")
    print("     flask --app examples.integrations.flask_integration:app run --port 5001")
    print()
    print("=== Configuration demo complete ===")
