"""Litestar Integration — KubeMQ Celery Transport.

Demonstrates:
- Litestar ASGI app dispatching Celery tasks
- kubemq+async:// URL scheme for native async transport
- Litestar @get/@post decorators with type-safe handlers
- Task submission, batch processing, and status endpoints

Usage:
    # Terminal 1: Start the Celery worker
    celery -A examples.integrations.litestar_integration:celery_app worker \
        --pool=asyncio --loglevel=info

    # Terminal 2: Start the Litestar app
    uvicorn examples.integrations.litestar_integration:app --port 8001

    # Terminal 3: Test
    curl -X POST http://localhost:8001/tasks/process \
        -H "Content-Type: application/json" -d '{"item_id": "abc-123"}'
    curl http://localhost:8001/tasks/status/<task_id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install litestar uvicorn

Note:
    The kubemq+async:// URL scheme is intended for async worker pools only
    (e.g. ``--pool=asyncio``). Use the plain kubemq:// scheme for prefork
    or solo pool workers.
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

try:
    from litestar import Litestar, get, post
except ImportError:
    raise ImportError("Litestar is required. Install with: pip install litestar uvicorn")

celery_app = Celery("litestar_integration")
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
def process_item(self, item_id: str) -> dict:
    """Process a single item."""
    time.sleep(1.5)
    return {"item_id": item_id, "status": "processed", "score": 0.95}


@celery_app.task
def batch_process(item_ids: list[str]) -> dict:
    """Process multiple items in batch."""
    time.sleep(len(item_ids) * 0.5)
    return {"processed": len(item_ids), "item_ids": item_ids}


@celery_app.task(bind=True, max_retries=2)
def export_data(self, fmt: str, filters: dict | None = None) -> dict:
    """Export data in the specified format."""
    time.sleep(2.0)
    return {"format": fmt, "rows": 10000, "url": f"/exports/data.{fmt}"}


@post("/tasks/process")
async def dispatch_process(data: dict) -> dict:
    """POST /tasks/process — dispatch item processing."""
    item_id = data.get("item_id", "unknown")
    result = process_item.delay(item_id)
    return {"task_id": result.id, "status": "queued", "item_id": item_id}


@post("/tasks/batch")
async def dispatch_batch(data: dict) -> dict:
    """POST /tasks/batch — dispatch batch processing."""
    item_ids = data.get("item_ids", [])
    result = batch_process.delay(item_ids)
    return {"task_id": result.id, "status": "queued", "count": len(item_ids)}


@post("/tasks/export")
async def dispatch_export(data: dict) -> dict:
    """POST /tasks/export — dispatch data export."""
    fmt = data.get("format", "csv")
    filters = data.get("filters")
    result = export_data.delay(fmt, filters)
    return {"task_id": result.id, "status": "queued", "format": fmt}


@get("/tasks/status/{task_id:str}")
async def task_status(task_id: str) -> dict:
    """GET /tasks/status/{task_id} — check task status."""
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
    """GET /health — application health check."""
    return {"status": "ok", "transport": "kubemq+async"}


app = Litestar(
    route_handlers=[dispatch_process, dispatch_batch, dispatch_export, task_status, health],
    debug=os.environ.get("DEBUG", "false").lower() == "true",
)


if __name__ == "__main__":
    print("=== Litestar + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.litestar_integration:celery_app "
        "worker --pool=asyncio --loglevel=info"
    )
    print("  2. Start the Litestar server:")
    print("     uvicorn examples.integrations.litestar_integration:app --port 8001")
    print()
    print("=== Configuration demo complete ===")
