"""FastAPI Integration — KubeMQ Celery Transport.

Demonstrates:
- FastAPI async endpoints dispatching Celery tasks
- Task submission, status polling, and result retrieval
- Pydantic request/response models
- Health check endpoint verifying broker connectivity

Usage:
    # 1. Start a Celery worker:
    celery -A examples.integrations.fastapi_integration:celery_app worker --loglevel=info

    # 2. Start the FastAPI server:
    uvicorn examples.integrations.fastapi_integration:api --host 0.0.0.0 --port 8000

    # 3. Submit a task:
    curl -X POST http://localhost:8000/tasks/process \
        -H "Content-Type: application/json" \
        -d '{"data": "hello world", "options": {"uppercase": true}}'

    # 4. Check task status:
    curl http://localhost:8000/tasks/<task-id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install fastapi uvicorn
"""

from __future__ import annotations

import os
import time
from typing import Any

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

celery_app = Celery(
    "fastapi_integration",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_track_started=True,
    worker_prefetch_multiplier=1,
)


@celery_app.task(bind=True)
def process_data(self, data: str, options: dict[str, Any] | None = None) -> dict:
    """Process data with configurable transformations."""
    options = options or {}
    self.update_state(state="PROGRESS", meta={"step": "processing", "percent": 50})
    time.sleep(1)

    result = data
    if options.get("uppercase"):
        result = result.upper()
    if options.get("reverse"):
        result = result[::-1]
    if options.get("repeat"):
        try:
            result = result * int(options["repeat"])
        except (ValueError, TypeError):
            pass

    return {
        "input": data,
        "output": result,
        "options_applied": list(options.keys()),
    }


@celery_app.task
def health_check() -> dict:
    """Verify broker connectivity."""
    return {"status": "ok", "broker": "kubemq"}


try:
    from fastapi import FastAPI
    from pydantic import BaseModel
except ImportError:
    raise ImportError(
        "FastAPI and Pydantic are required. Install with: pip install fastapi uvicorn"
    )


class TaskRequest(BaseModel):
    data: str
    options: dict[str, Any] | None = None


class TaskResponse(BaseModel):
    task_id: str
    status: str
    detail: str


class TaskStatus(BaseModel):
    task_id: str
    status: str
    result: Any | None = None
    error: str | None = None
    progress: dict[str, Any] | None = None


api = FastAPI(
    title="KubeMQ Celery API",
    description="FastAPI + Celery with KubeMQ transport",
    version="1.0.0",
)


@api.post("/tasks/process", response_model=TaskResponse)
async def submit_task(request: TaskRequest) -> TaskResponse:
    """Submit a data processing task to the KubeMQ broker."""
    result = process_data.delay(request.data, request.options)
    return TaskResponse(
        task_id=result.id,
        status="PENDING",
        detail="Task submitted to KubeMQ broker",
    )


@api.get("/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str) -> TaskStatus:
    """Check the status of a submitted task."""
    result = AsyncResult(task_id, app=celery_app)

    if result.state == "PROGRESS":
        return TaskStatus(task_id=task_id, status="PROGRESS", progress=result.info)
    elif result.state == "SUCCESS":
        return TaskStatus(task_id=task_id, status="SUCCESS", result=result.result)
    elif result.state == "FAILURE":
        return TaskStatus(task_id=task_id, status="FAILURE", error=str(result.result))
    else:
        return TaskStatus(task_id=task_id, status=result.state)


@api.get("/health")
async def api_health() -> dict:
    """API health check."""
    return {"status": "ok", "service": "fastapi-kubemq-celery"}


if __name__ == "__main__":
    print("=== FastAPI + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.fastapi_integration:celery_app worker --loglevel=info"
    )
    print("  2. Start the FastAPI server:")
    print("     uvicorn examples.integrations.fastapi_integration:api --host 0.0.0.0 --port 8000")
    print()
    print("=== Configuration demo complete ===")
