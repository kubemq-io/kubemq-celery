"""Task Binding — KubeMQ Celery Transport.

Demonstrates:
- @app.task(bind=True) to access task instance as self
- self.request.id for the current task ID
- self.request.retries for the current retry count
- self.update_state() for custom state reporting
- self.retry() for manual retry with countdown

Usage:
    celery -A examples.advanced_patterns.task_binding worker --loglevel=info
    python examples/advanced_patterns/task_binding.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "task_binding",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_track_started=True,
)


@app.task(bind=True)
def inspect_request(self, data: str) -> dict:
    """Inspect the task request object and return its attributes."""
    info = {
        "task_id": self.request.id,
        "task_name": self.name,
        "retries": self.request.retries,
        "hostname": self.request.hostname,
        "delivery_info": dict(self.request.delivery_info or {}),
        "data": data,
    }
    print(f"[inspect_request] Task ID: {self.request.id}")
    print(f"  Name: {self.name}")
    print(f"  Retries: {self.request.retries}")
    print(f"  Hostname: {self.request.hostname}")
    return info


@app.task(bind=True)
def progress_task(self, steps: int) -> dict:
    """Task that reports progress via self.update_state()."""
    print(f"[progress_task] Starting {steps}-step task (ID: {self.request.id})")
    for i in range(1, steps + 1):
        self.update_state(
            state="PROGRESS",
            meta={"current": i, "total": steps, "percent": int(i / steps * 100)},
        )
        print(f"  Step {i}/{steps} ({int(i / steps * 100)}%)")
        time.sleep(0.3)

    return {"steps_completed": steps, "task_id": self.request.id}


@app.task(bind=True, max_retries=3)
def retry_task(self, succeed_on: int = 3) -> dict:
    """Task that retries until a specific attempt number."""
    attempt = self.request.retries + 1
    print(f"[retry_task] Attempt {attempt}/{succeed_on} (ID: {self.request.id})")

    if attempt < succeed_on:
        raise self.retry(
            countdown=1,
            exc=RuntimeError(f"Simulated failure #{attempt}"),
        )

    return {"attempt": attempt, "task_id": self.request.id, "status": "success"}


@app.task(bind=True)
def chained_context(self, value: int) -> dict:
    """Task in a chain that shows how binding works with result passing."""
    parent_id = self.request.parent_id
    root_id = self.request.root_id
    print(f"[chained_context] value={value}, task_id={self.request.id}")
    print(f"  parent_id: {parent_id}")
    print(f"  root_id:   {root_id}")
    return {
        "value": value * 2,
        "task_id": self.request.id,
        "parent_id": parent_id,
        "root_id": root_id,
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True, result_backend="disabled")
    print("=== Task Binding Example ===")
    print(f"Broker: {app.conf.broker_url}")

    print("\n--- Inspect request ---")
    result = inspect_request.delay("hello-binding")
    try:
        value = result.get(timeout=10)
        print(f"Result: {value}")
        assert value["data"] == "hello-binding"
        assert value["retries"] == 0
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    print("\n--- Progress tracking ---")
    result = progress_task.delay(5)
    try:
        value = result.get(timeout=30)
        print(f"Result: {value}")
        assert value["steps_completed"] == 5
    except Exception as exc:
        print(f"Task failed or timed out: {exc}")

    print("\nAll task binding examples completed!")
