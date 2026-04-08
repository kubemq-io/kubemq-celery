"""Starlette Integration — KubeMQ Celery Transport.

Demonstrates:
- Starlette ASGI app dispatching Celery tasks
- kubemq+async:// URL scheme for native async transport
- Lifespan context manager for startup/shutdown
- Route-based task submission and status endpoints

Usage:
    # Terminal 1: Start the Celery worker
    celery -A examples.integrations.starlette_integration:celery_app worker \
        --pool=asyncio --loglevel=info

    # Terminal 2: Start the Starlette app
    uvicorn examples.integrations.starlette_integration:app --port 8000

    # Terminal 3: Test
    curl -X POST http://localhost:8000/tasks/analyze \
        -H "Content-Type: application/json" -d '{"dataset": "sales-q4"}'
    curl http://localhost:8000/tasks/status/<task_id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install starlette uvicorn

Note:
    The kubemq+async:// URL scheme is intended for async worker pools only
    (e.g. ``--pool=asyncio``). Use the plain kubemq:// scheme for prefork
    or solo pool workers.
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
except ImportError:
    raise ImportError("Starlette is required. Install with: pip install starlette uvicorn")

celery_app = Celery("starlette_integration")
celery_app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq+async://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "broker_transport_options": {
            "wait_timeout": 1,
            "max_batch_size": 10,
        },
    }
)


@celery_app.task(bind=True, max_retries=3)
def analyze_dataset(self, dataset: str) -> dict:
    """Analyze a dataset (simulated)."""
    time.sleep(2.0)
    return {"dataset": dataset, "rows": 50000, "anomalies": 3, "status": "complete"}


@celery_app.task
def generate_report(dataset: str, fmt: str = "json") -> dict:
    """Generate an analysis report."""
    time.sleep(1.0)
    return {"dataset": dataset, "format": fmt, "url": f"/reports/{dataset}.{fmt}"}


async def dispatch_analysis(request: Request) -> JSONResponse:
    """POST /tasks/analyze — dispatch dataset analysis."""
    body = await request.json()
    dataset = body.get("dataset", "default")
    result = analyze_dataset.delay(dataset)
    return JSONResponse(
        {"task_id": result.id, "status": "queued", "dataset": dataset},
        status_code=202,
    )


async def dispatch_report(request: Request) -> JSONResponse:
    """POST /tasks/report — dispatch report generation."""
    body = await request.json()
    dataset = body.get("dataset", "default")
    fmt = body.get("format", "json")
    result = generate_report.delay(dataset, fmt)
    return JSONResponse(
        {"task_id": result.id, "status": "queued"},
        status_code=202,
    )


async def task_status(request: Request) -> JSONResponse:
    """GET /tasks/status/{task_id} — check task status."""
    task_id = request.path_params["task_id"]
    result = AsyncResult(task_id, app=celery_app)
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }
    if result.failed():
        response["error"] = str(result.result)
    return JSONResponse(response)


async def health(request: Request) -> JSONResponse:
    """GET /health — application health check."""
    return JSONResponse({"status": "ok", "transport": "kubemq+async"})


@asynccontextmanager
async def lifespan(app: Starlette):
    print("Starlette app starting with KubeMQ async transport")
    yield
    print("Starlette app shutting down")


app = Starlette(
    debug=os.environ.get("DEBUG", "false").lower() == "true",
    routes=[
        Route("/tasks/analyze", dispatch_analysis, methods=["POST"]),
        Route("/tasks/report", dispatch_report, methods=["POST"]),
        Route("/tasks/status/{task_id}", task_status, methods=["GET"]),
        Route("/health", health, methods=["GET"]),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    print("=== Starlette + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.starlette_integration:celery_app "
        "worker --pool=asyncio --loglevel=info"
    )
    print("  2. Start the Starlette server:")
    print("     uvicorn examples.integrations.starlette_integration:app --port 8000")
    print()
    print("=== Configuration demo complete ===")
