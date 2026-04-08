"""aiohttp Integration — KubeMQ Celery Transport.

Demonstrates:
- aiohttp ASGI web server dispatching Celery tasks
- Async route handlers with JSON request/response
- Task submission and status polling endpoints
- Cleanup and graceful shutdown

Usage:
    # Terminal 1: Start the Celery worker
    celery -A examples.integrations.aiohttp_integration:celery_app worker --loglevel=info

    # Terminal 2: Start the aiohttp server
    python examples/integrations/aiohttp_integration.py

    # Terminal 3: Test
    curl -X POST http://localhost:8080/tasks/process \
        -H "Content-Type: application/json" \
        -d '{"data": "hello", "operation": "uppercase"}'
    curl http://localhost:8080/tasks/status/<task_id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install aiohttp
"""

from __future__ import annotations

import os
import time

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

try:
    from aiohttp import web
except ImportError:
    raise ImportError("aiohttp is required. Install with: pip install aiohttp")

celery_app = Celery(
    "aiohttp_integration",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


@celery_app.task
def process_data(data: str, operation: str = "echo") -> dict:
    """Process data with the specified operation."""
    print(f"[process_data] data={data!r} op={operation}")
    time.sleep(0.5)
    if operation == "uppercase":
        result = data.upper()
    elif operation == "reverse":
        result = data[::-1]
    elif operation == "repeat":
        result = data * 3
    else:
        result = data
    return {"input": data, "operation": operation, "output": result}


@celery_app.task
def aggregate(values: list[int]) -> dict:
    """Aggregate a list of values."""
    print(f"[aggregate] {len(values)} values")
    time.sleep(0.3)
    numeric = [v for v in values if isinstance(v, (int, float))]
    if len(numeric) != len(values):
        print(f"[aggregate] WARNING: dropped {len(values) - len(numeric)} non-numeric elements")
    return {
        "count": len(numeric),
        "sum": sum(numeric),
        "mean": sum(numeric) / len(numeric) if numeric else 0,
        "min": min(numeric) if numeric else 0,
        "max": max(numeric) if numeric else 0,
    }


@celery_app.task(bind=True, max_retries=3)
def send_webhook(self, url: str, payload: dict) -> dict:
    """Send a webhook notification (simulated)."""
    print(f"[send_webhook] url={url}")
    time.sleep(0.4)
    return {"url": url, "status": "delivered", "payload_size": len(str(payload))}


async def handle_process(request: web.Request) -> web.Response:
    """POST /tasks/process — dispatch data processing."""
    body = await request.json()
    data = body.get("data", "")
    operation = body.get("operation", "echo")
    result = process_data.delay(data, operation)
    return web.json_response(
        {"task_id": result.id, "status": "queued"},
        status=202,
    )


async def handle_aggregate(request: web.Request) -> web.Response:
    """POST /tasks/aggregate — dispatch value aggregation."""
    body = await request.json()
    values = body.get("values", [])
    result = aggregate.delay(values)
    return web.json_response(
        {"task_id": result.id, "status": "queued"},
        status=202,
    )


async def handle_webhook(request: web.Request) -> web.Response:
    """POST /tasks/webhook — dispatch webhook notification."""
    body = await request.json()
    url = body.get("url", "https://httpbin.org/post")
    payload = body.get("payload", {})
    result = send_webhook.delay(url, payload)
    return web.json_response(
        {"task_id": result.id, "status": "queued"},
        status=202,
    )


async def handle_task_status(request: web.Request) -> web.Response:
    """GET /tasks/status/{task_id} — check task status."""
    task_id = request.match_info["task_id"]
    result = AsyncResult(task_id, app=celery_app)
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }
    if result.failed():
        response["error"] = str(result.result)
    return web.json_response(response)


async def handle_health(request: web.Request) -> web.Response:
    """GET /health — application health check."""
    return web.json_response({"status": "ok", "service": "aiohttp-kubemq-celery"})


def create_app() -> web.Application:
    """Create the aiohttp application."""
    app = web.Application()
    app.router.add_post("/tasks/process", handle_process)
    app.router.add_post("/tasks/aggregate", handle_aggregate)
    app.router.add_post("/tasks/webhook", handle_webhook)
    app.router.add_get("/tasks/status/{task_id}", handle_task_status)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    print("=== aiohttp + KubeMQ Celery Integration ===\n")
    print(f"Broker: {celery_app.conf.broker_url}")
    print()
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.aiohttp_integration:celery_app worker --loglevel=info"
    )
    print("  2. Start the aiohttp server:")
    print(
        '     python -c "from examples.integrations.aiohttp_integration'
        " import create_app; from aiohttp import web;"
        ' web.run_app(create_app(), port=8080)"'
    )
    print("  3. Test:")
    print("     curl http://localhost:8080/health")
    print()
    print("=== Configuration demo complete ===")
