"""Starlette + KubeMQ async Celery transport example.

Uses kubemq+async:// URL scheme for native async I/O.

Requirements:
    starlette>=0.37
    uvicorn>=0.29
    celery>=5.4
    kubemq-celery-transport>=1.1.0

Run:
    # Terminal 1: Celery async worker
    celery -A starlette_integration.celery_app worker --pool=asyncio --loglevel=info

    # Terminal 2: Starlette app
    uvicorn starlette_integration:app --port 8000

    # Terminal 3: Test
    curl -X POST http://localhost:8000/tasks/analyze \
         -H "Content-Type: application/json" -d '{"dataset": "sales-q4"}'
    curl http://localhost:8000/tasks/status/<task_id>
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import kubemq_celery  # noqa: F401
from celery import Celery
from celery.result import AsyncResult
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

# --- Celery App (async transport) ---

celery_app = Celery("starlette_integration")
celery_app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq+async://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 86400,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "broker_transport_options": {
            "wait_timeout": 1,
            "max_batch_size": 10,
            "message_expiration": 3600,
        },
    }
)


@celery_app.task(bind=True, max_retries=3)
def analyze_dataset(self, dataset: str) -> dict:
    """Analyze a dataset (simulated async-dispatched work)."""
    import time

    time.sleep(2.0)
    return {"dataset": dataset, "rows": 50000, "anomalies": 3, "status": "complete"}


@celery_app.task
def generate_report(dataset: str, format: str = "json") -> dict:
    """Generate analysis report."""
    import time

    time.sleep(1.0)
    return {"dataset": dataset, "format": format, "url": f"/reports/{dataset}.{format}"}


# --- Starlette Routes ---


async def dispatch_analysis(request: Request) -> JSONResponse:
    """POST /tasks/analyze -- dispatch dataset analysis task."""
    body = await request.json()
    dataset = body.get("dataset", "default")
    result = analyze_dataset.delay(dataset)
    return JSONResponse(
        {"task_id": result.id, "status": "queued", "dataset": dataset},
        status_code=202,
    )


async def dispatch_report(request: Request) -> JSONResponse:
    """POST /tasks/report -- dispatch report generation."""
    body = await request.json()
    dataset = body.get("dataset", "default")
    fmt = body.get("format", "json")
    result = generate_report.delay(dataset, fmt)
    return JSONResponse(
        {"task_id": result.id, "status": "queued"},
        status_code=202,
    )


async def task_status(request: Request) -> JSONResponse:
    """GET /tasks/status/{task_id} -- check task status."""
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
    """GET /health -- application health check."""
    return JSONResponse({"status": "ok", "transport": "kubemq+async"})


# --- Lifespan ---


@asynccontextmanager
async def lifespan(app: Starlette):
    """Application lifespan: startup/shutdown hooks."""
    print("Starlette app starting with KubeMQ async transport")
    yield
    print("Starlette app shutting down")


# --- App ---

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
