"""Flask Application Factory + Celery — KubeMQ Celery Transport.

Demonstrates:
- Flask application factory pattern (create_app)
- Celery initialization within the factory
- Flask 3.x Celery integration pattern
- Config-driven broker selection via Flask config namespace

Usage:
    # Terminal 1: Start the Flask app
    flask --app examples.integrations.flask_factory_pattern:app run --port 5002

    # Terminal 2: Start the Celery worker
    celery -A examples.integrations.flask_factory_pattern:celery_app worker --loglevel=info

    # Terminal 3: Test
    curl -X POST http://localhost:5002/tasks/compute \
        -H "Content-Type: application/json" \
        -d '{"x": 10, "y": 20}'
    curl http://localhost:5002/tasks/status/<task_id>

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
    from flask import Flask, jsonify, request
except ImportError:
    raise ImportError("Flask is required. Install with: pip install flask")


def celery_init_app(flask_app: Flask) -> Celery:
    """Create and configure Celery from Flask config (Flask 3.x pattern)."""

    class FlaskCelery(Celery):
        def on_init(self) -> None:
            self.set_default()

    cel = FlaskCelery(flask_app.import_name)
    cel.config_from_object(flask_app.config, namespace="CELERY")
    cel.set_default()
    flask_app.extensions["celery"] = cel
    return cel


def create_app(config: dict | None = None) -> Flask:
    """Application factory for the Flask + Celery app."""
    flask_app = Flask(__name__)

    flask_app.config.from_mapping(
        CELERY=dict(
            broker_url=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
            result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
            result_expires=3600,
            task_serializer="json",
            result_serializer="json",
            accept_content=["json"],
            broker_transport_options={
                "wait_timeout": 1,
                "max_batch_size": 10,
            },
        ),
    )

    if config:
        flask_app.config.from_mapping(config)

    cel = celery_init_app(flask_app)

    @cel.task(bind=True)
    def compute(self, x: int, y: int) -> dict:
        """Compute sum and product."""
        print(f"[compute] x={x} y={y}")
        time.sleep(0.3)
        return {"sum": x + y, "product": x * y}

    @cel.task
    def greet(name: str) -> dict:
        """Return a greeting."""
        print(f"[greet] name={name}")
        return {"greeting": f"Hello, {name}!", "source": "flask-factory"}

    @cel.task(bind=True, max_retries=3)
    def process_item(self, item_id: str) -> dict:
        """Process an item."""
        print(f"[process_item] item_id={item_id}")
        time.sleep(0.5)
        return {"item_id": item_id, "status": "processed"}

    @flask_app.post("/tasks/compute")
    def dispatch_compute():
        data = request.get_json(force=True)
        x = data.get("x", 0)
        y = data.get("y", 0)
        result = compute.delay(x, y)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @flask_app.post("/tasks/greet")
    def dispatch_greet():
        data = request.get_json(force=True)
        name = data.get("name", "World")
        result = greet.delay(name)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @flask_app.post("/tasks/process")
    def dispatch_process():
        data = request.get_json(force=True)
        item_id = data.get("item_id", "unknown")
        result = process_item.delay(item_id)
        return jsonify({"task_id": result.id, "status": "queued"}), 202

    @flask_app.get("/tasks/status/<task_id>")
    def task_status(task_id: str):
        result = AsyncResult(task_id, app=cel)
        response = {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None,
        }
        if result.failed():
            response["error"] = str(result.result)
        return jsonify(response)

    @flask_app.get("/health")
    def health():
        return jsonify({"status": "ok", "service": "flask-factory-kubemq"})

    return flask_app


app = create_app()
celery_app = app.extensions["celery"]


if __name__ == "__main__":
    print("=== Flask Factory Pattern + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.flask_factory_pattern:celery_app "
        "worker --loglevel=info"
    )
    print("  2. Start the Flask server:")
    print("     flask --app examples.integrations.flask_factory_pattern:app run --port 5002")
    print()
    print("=== Configuration demo complete ===")
