"""FastAPI + Celery integration with KubeMQ.

Production-ready pattern demonstrating:
- FastAPI async endpoints for task submission
- Celery task execution via KubeMQ broker
- Queue-peek result backend for task result retrieval
- Suitable for ML inference pipelines, data processing, etc.

Usage:
    # 1. Start a Celery worker:
    celery -A fastapi_integration.celery_app worker --loglevel=info

    # 2. Start the FastAPI server:
    uvicorn fastapi_integration:api --host 0.0.0.0 --port 8000

    # 3. Submit a task:
    curl -X POST http://localhost:8000/tasks/process \
        -H "Content-Type: application/json" \
        -d '{"data": "hello world", "options": {"uppercase": true}}'

    # 4. Check task status:
    curl http://localhost:8000/tasks/<task-id>

Requirements:
    pip install fastapi uvicorn kubemq-celery
"""

from __future__ import annotations

import time
from typing import Any

import kubemq_celery
from celery import Celery
from celery.result import AsyncResult

# ---------------------------------------------------------------------------
# Celery Application
# ---------------------------------------------------------------------------

celery_app = Celery(
    "fastapi_integration",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

celery_app.conf.update(
    result_expires=3600,  # results expire after 1 hour
    task_track_started=True,  # enable STARTED state for progress tracking
    worker_prefetch_multiplier=1,
)


@celery_app.task(bind=True)
def process_data(self, data: str, options: dict[str, Any] | None = None) -> dict:
    """Process data -- simulates a CPU/IO-intensive operation.

    In a real application, this could be:
    - ML model inference
    - PDF generation
    - Image processing
    - Data transformation pipeline
    """
    options = options or {}
    self.update_state(state="PROGRESS", meta={"step": "preprocessing", "percent": 10})

    # Simulate processing time
    time.sleep(2)
    self.update_state(state="PROGRESS", meta={"step": "processing", "percent": 50})

    result = data
    if options.get("uppercase"):
        result = result.upper()
    if options.get("reverse"):
        result = result[::-1]
    if options.get("repeat"):
        result = result * int(options["repeat"])

    time.sleep(1)
    self.update_state(state="PROGRESS", meta={"step": "finalizing", "percent": 90})

    return {
        "input": data,
        "output": result,
        "options_applied": list(options.keys()),
        "processing_time_ms": 3000,
    }


@celery_app.task
def health_check() -> dict:
    """Simple health check task to verify broker connectivity."""
    return {"status": "ok", "broker": "kubemq"}


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except ImportError:
    raise ImportError(
        "FastAPI and Pydantic are required for this example. "
        "Install with: pip install fastapi uvicorn"
    )


class TaskRequest(BaseModel):
    """Request body for task submission."""
    data: str
    options: dict[str, Any] | None = None


class TaskResponse(BaseModel):
    """Response body for task submission."""
    task_id: str
    status: str
    detail: str


class TaskStatus(BaseModel):
    """Response body for task status check."""
    task_id: str
    status: str
    result: Any | None = None
    error: str | None = None
    progress: dict[str, Any] | None = None


api = FastAPI(
    title="KubeMQ Celery API",
    description="FastAPI + Celery with KubeMQ broker",
    version="1.0.0",
)


@api.post("/tasks/process", response_model=TaskResponse)
async def submit_task(request: TaskRequest) -> TaskResponse:
    """Submit a data processing task.

    The task is sent to KubeMQ via the Celery transport and processed
    by an available worker. Returns immediately with a task ID.
    """
    result = process_data.delay(request.data, request.options)
    return TaskResponse(
        task_id=result.id,
        status="PENDING",
        detail="Task submitted to KubeMQ broker",
    )


@api.get("/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str) -> TaskStatus:
    """Check the status of a submitted task.

    Uses the KubeMQ queue-peek result backend to retrieve the task
    status without consuming the result message. Multiple callers
    can check the same task.
    """
    result = AsyncResult(task_id, app=celery_app)

    if result.state == "PENDING":
        return TaskStatus(
            task_id=task_id,
            status="PENDING",
            progress={"detail": "Task is waiting to be processed"},
        )
    elif result.state == "STARTED":
        return TaskStatus(
            task_id=task_id,
            status="STARTED",
            progress={"detail": "Task is being processed"},
        )
    elif result.state == "PROGRESS":
        return TaskStatus(
            task_id=task_id,
            status="PROGRESS",
            progress=result.info,
        )
    elif result.state == "SUCCESS":
        return TaskStatus(
            task_id=task_id,
            status="SUCCESS",
            result=result.result,
        )
    elif result.state == "FAILURE":
        return TaskStatus(
            task_id=task_id,
            status="FAILURE",
            error=str(result.result),
        )
    else:
        return TaskStatus(
            task_id=task_id,
            status=result.state,
        )


@api.post("/tasks/health", response_model=TaskResponse)
async def submit_health_check() -> TaskResponse:
    """Submit a health check task to verify the broker and worker are running."""
    result = health_check.delay()
    return TaskResponse(
        task_id=result.id,
        status="PENDING",
        detail="Health check task submitted",
    )


@api.get("/health")
async def api_health() -> dict:
    """API health check (does not verify broker connectivity)."""
    return {"status": "ok", "service": "fastapi-kubemq-celery"}
