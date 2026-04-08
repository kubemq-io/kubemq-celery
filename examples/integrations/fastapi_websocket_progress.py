"""FastAPI WebSocket Progress — KubeMQ Celery Transport.

Demonstrates:
- WebSocket endpoint for real-time task progress streaming
- Celery task with self.update_state() for progress reporting
- Client receives live progress updates without polling
- Graceful connection handling and task completion notification

Usage:
    # 1. Start a Celery worker:
    celery -A examples.integrations.fastapi_websocket_progress:celery_app worker --loglevel=info

    # 2. Start the FastAPI server:
    uvicorn examples.integrations.fastapi_websocket_progress:api --host 0.0.0.0 --port 8000

    # 3. Test with websocat or browser:
    # Submit task:
    #   curl -X POST http://localhost:8000/tasks/process \
    #     -H "Content-Type: application/json" -d '{"steps": 5}'
    # Watch progress: websocat ws://localhost:8000/ws/progress/<task-id>

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - pip install fastapi uvicorn websockets
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

celery_app = Celery(
    "fastapi_ws_progress",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_track_started=True,
)


@celery_app.task(bind=True)
def long_running_task(self, steps: int = 10) -> dict:
    """Task that reports progress at each step."""
    for i in range(1, steps + 1):
        self.update_state(
            state="PROGRESS",
            meta={
                "current": i,
                "total": steps,
                "percent": int(i / steps * 100),
                "message": f"Processing step {i}/{steps}",
            },
        )
        time.sleep(1)

    return {"steps_completed": steps, "status": "done"}


@celery_app.task(bind=True)
def data_pipeline(self, records: int = 100) -> dict:
    """Simulate a data processing pipeline with stage-based progress."""
    stages = ["validating", "transforming", "aggregating", "writing"]
    for idx, stage in enumerate(stages):
        self.update_state(
            state="PROGRESS",
            meta={
                "stage": stage,
                "stage_num": idx + 1,
                "total_stages": len(stages),
                "percent": int((idx + 1) / len(stages) * 100),
            },
        )
        time.sleep(1.5)

    return {"records_processed": records, "stages": stages, "status": "complete"}


try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from pydantic import BaseModel
except ImportError:
    raise ImportError("FastAPI required. Install with: pip install fastapi uvicorn websockets")


class TaskRequest(BaseModel):
    steps: int = 5


api = FastAPI(
    title="KubeMQ Celery WebSocket Progress",
    description="Real-time task progress via WebSocket",
    version="1.0.0",
)


@api.post("/tasks/process")
async def submit_task(request: TaskRequest) -> dict:
    """Submit a long-running task."""
    result = long_running_task.delay(request.steps)
    return {"task_id": result.id, "status": "PENDING", "ws_url": f"/ws/progress/{result.id}"}


@api.post("/tasks/pipeline")
async def submit_pipeline(records: int = 100) -> dict:
    """Submit a data pipeline task."""
    result = data_pipeline.delay(records)
    return {"task_id": result.id, "status": "PENDING", "ws_url": f"/ws/progress/{result.id}"}


@api.websocket("/ws/progress/{task_id}")
async def websocket_progress(websocket: WebSocket, task_id: str):
    """Stream task progress updates over WebSocket."""
    await websocket.accept()
    print(f"[WS] Client connected for task {task_id}")

    max_iterations = 300
    try:
        for _ in range(max_iterations):
            result = AsyncResult(task_id, app=celery_app)

            if result.state == "PROGRESS":
                await websocket.send_json(
                    {
                        "status": "PROGRESS",
                        "progress": result.info,
                    }
                )
            elif result.state == "SUCCESS":
                await websocket.send_json(
                    {
                        "status": "SUCCESS",
                        "result": result.result,
                    }
                )
                break
            elif result.state == "FAILURE":
                await websocket.send_json(
                    {
                        "status": "FAILURE",
                        "error": str(result.result),
                    }
                )
                break
            elif result.state == "STARTED":
                await websocket.send_json(
                    {
                        "status": "STARTED",
                        "message": "Task is being processed",
                    }
                )
            else:
                await websocket.send_json(
                    {
                        "status": result.state,
                    }
                )

            await asyncio.sleep(0.5)
        else:
            await websocket.send_json({"status": "TIMEOUT", "error": "Polling limit reached"})

    except WebSocketDisconnect:
        print(f"[WS] Client disconnected for task {task_id}")
    finally:
        print(f"[WS] Connection closed for task {task_id}")


@api.get("/tasks/{task_id}")
async def get_task_status(task_id: str) -> dict:
    """Poll-based task status (alternative to WebSocket)."""
    result = AsyncResult(task_id, app=celery_app)
    response: dict[str, Any] = {"task_id": task_id, "status": result.state}

    if result.state == "PROGRESS":
        response["progress"] = result.info
    elif result.state == "SUCCESS":
        response["result"] = result.result
    elif result.state == "FAILURE":
        response["error"] = str(result.result)

    return response


@api.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "fastapi-ws-progress"}


if __name__ == "__main__":
    print("=== FastAPI WebSocket Progress + KubeMQ Celery Integration ===\n")
    print("To run this example:")
    print("  1. Start a Celery worker:")
    print(
        "     celery -A examples.integrations.fastapi_websocket_progress:celery_app "
        "worker --loglevel=info"
    )
    print("  2. Start the FastAPI server:")
    print(
        "     uvicorn examples.integrations.fastapi_websocket_progress:api "
        "--host 0.0.0.0 --port 8000"
    )
    print("  3. Submit a task and watch progress via WebSocket:")
    print(
        "     curl -X POST http://localhost:8000/tasks/process "
        "-H 'Content-Type: application/json' -d '{\"steps\": 5}'"
    )
    print("     websocat ws://localhost:8000/ws/progress/<task-id>")
    print()
    print("=== Configuration demo complete ===")
