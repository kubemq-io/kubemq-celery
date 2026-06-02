"""Litestar + KubeMQ async Celery transport example.

Uses kubemq+async:// URL scheme for native async I/O.

Requirements:
    litestar>=2.6
    uvicorn>=0.29
    celery>=5.4
    kubemq-celery-transport>=1.1.0

Run:
    # Terminal 1: Celery async worker
    celery -A litestar_integration.celery_app worker --pool=asyncio --loglevel=info

    # Terminal 2: Litestar app
    uvicorn litestar_integration:app --port 8001

    # Terminal 3: Test
    curl -X POST http://localhost:8001/tasks/process \
         -H "Content-Type: application/json" -d '{"item_id": "abc-123"}'
    curl http://localhost:8001/tasks/status/abc-123
"""

from __future__ import annotations

import os

import kubemq_celery  # noqa: F401
from celery import Celery
from celery.result import AsyncResult
from litestar import Litestar, get, post

# --- Celery App (async transport) ---

celery_app = Celery("litestar_integration")
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
def process_item(self, item_id: str) -> dict:
    """Process an item (simulated work)."""
    import time

    time.sleep(1.5)
    return {"item_id": item_id, "status": "processed", "score": 0.95}


@celery_app.task
def batch_process(item_ids: list[str]) -> dict:
    """Process multiple items in batch."""
    import time

    time.sleep(len(item_ids) * 0.5)
    return {"processed": len(item_ids), "item_ids": item_ids}


@celery_app.task(bind=True, max_retries=2)
def export_data(self, format: str, filters: dict | None = None) -> dict:
    """Export data in specified format."""
    import time

    time.sleep(3.0)
    return {"format": format, "rows": 10000, "url": f"/exports/data.{format}"}


# --- Litestar Handlers ---


@post("/tasks/process")
async def dispatch_process(data: dict) -> dict:
    """POST /tasks/process -- dispatch item processing task."""
    item_id = data.get("item_id", "unknown")
    result = process_item.delay(item_id)
    return {"task_id": result.id, "status": "queued", "item_id": item_id}


@post("/tasks/batch")
async def dispatch_batch(data: dict) -> dict:
    """POST /tasks/batch -- dispatch batch processing task."""
    item_ids = data.get("item_ids", [])
    result = batch_process.delay(item_ids)
    return {"task_id": result.id, "status": "queued", "count": len(item_ids)}


@post("/tasks/export")
async def dispatch_export(data: dict) -> dict:
    """POST /tasks/export -- dispatch data export task."""
    fmt = data.get("format", "csv")
    filters = data.get("filters")
    result = export_data.delay(fmt, filters)
    return {"task_id": result.id, "status": "queued", "format": fmt}


@get("/tasks/status/{task_id:str}")
async def task_status(task_id: str) -> dict:
    """GET /tasks/status/{task_id} -- check task status."""
    result = AsyncResult(task_id, app=celery_app)
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }
    if result.failed():
        response["error"] = str(result.result)
    return response


@get("/health")
async def health() -> dict:
    """GET /health -- application health check."""
    return {"status": "ok", "transport": "kubemq+async"}


# --- App ---

app = Litestar(
    route_handlers=[dispatch_process, dispatch_batch, dispatch_export, task_status, health],
    debug=os.environ.get("DEBUG", "false").lower() == "true",
)
