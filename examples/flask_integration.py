"""Flask 3.x + KubeMQ Celery integration example.

Requirements:
    flask>=3.0
    celery>=5.4
    kubemq-celery-transport>=1.1.0

Run:
    # Terminal 1: Flask app
    flask --app flask_integration run --port 5001

    # Terminal 2: Celery worker
    celery -A flask_integration.celery_app worker --loglevel=info

    # Terminal 3: Test
    curl -X POST http://localhost:5001/tasks/notify -H "Content-Type: application/json" \
         -d '{"user_id": "42", "message": "Hello"}'
    curl http://localhost:5001/tasks/status/<task_id>
"""

from __future__ import annotations

import logging
import os
import time

import kubemq_celery  # noqa: F401 -- registers transport
from celery import Celery
from celery.result import AsyncResult
from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)


def celery_init_app(app: Flask) -> Celery:
    """Create and configure Celery app from Flask config (Flask 3.x pattern)."""

    class FlaskCelery(Celery):
        def on_init(self) -> None:
            self.set_default()

    celery_app = FlaskCelery(app.import_name)
    celery_app.config_from_object(app.config, namespace="CELERY")
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app


def create_app() -> Flask:
    app = Flask(__name__)

    # --- Celery Configuration ---
    app.config.from_mapping(
        CELERY=dict(
            broker_url=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
            result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
            result_expires=86400,
            task_serializer="json",
            result_serializer="json",
            accept_content=["json"],
            broker_transport_options={
                "wait_timeout": 1,
                "dead_letter_queue": "flask-dead-letters",
                "max_receive_count": 3,
                "message_expiration": 3600,
                "max_batch_size": 10,
            },
        ),
    )

    celery_app = celery_init_app(app)

    # --- Tasks ---

    @celery_app.task(bind=True, max_retries=3)
    def send_notification(self, user_id: str, message: str) -> dict:
        """Send a notification to a user (simulated)."""
        logger.info("Sending notification to user %s: %s", user_id, message)
        time.sleep(0.5)  # simulate I/O
        return {"user_id": user_id, "status": "sent", "message": message}

    @celery_app.task(bind=True, max_retries=5)
    def process_data(self, dataset_id: str) -> dict:
        """Process a dataset (simulated CPU work)."""
        logger.info("Processing dataset %s", dataset_id)
        time.sleep(2.0)  # simulate processing
        return {"dataset_id": dataset_id, "rows_processed": 1000, "status": "complete"}

    @celery_app.task(bind=True)
    def generate_pdf(self, report_id: str, template: str = "default") -> dict:
        """Generate a PDF report (simulated)."""
        logger.info("Generating PDF for report %s with template %s", report_id, template)
        time.sleep(1.0)
        return {"report_id": report_id, "url": f"/reports/{report_id}.pdf", "pages": 12}

    # --- Routes ---

    @app.post("/tasks/notify")
    def dispatch_notification():
        data = request.get_json(force=True)
        user_id = data.get("user_id", "unknown")
        message = data.get("message", "")
        result = send_notification.delay(user_id, message)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @app.post("/tasks/process")
    def dispatch_processing():
        data = request.get_json(force=True)
        dataset_id = data.get("dataset_id", "default")
        result = process_data.delay(dataset_id)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @app.post("/tasks/report")
    def dispatch_report():
        data = request.get_json(force=True)
        report_id = data.get("report_id", "1")
        template = data.get("template", "default")
        result = generate_pdf.delay(report_id, template)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @app.get("/tasks/status/<task_id>")
    def task_status(task_id: str):
        result = AsyncResult(task_id, app=celery_app)
        response = {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None,
        }
        if result.failed():
            response["error"] = str(result.result)
        return jsonify(response)

    return app


app = create_app()
celery_app = app.extensions["celery"]
