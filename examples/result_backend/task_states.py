"""Task States — KubeMQ Celery Transport.

Demonstrates:
- Tracking task state transitions: PENDING → STARTED → PROGRESS → SUCCESS
- self.update_state() for custom progress reporting
- Polling task state from the client side
- Result backend storing each state transition

Usage:
    celery -A examples.result_backend.task_states worker --loglevel=info
    python examples/result_backend/task_states.py

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
    "task_states",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    task_track_started=True,  # Required for STARTED state
)


@app.task(bind=True)
def process_items(self, items: list[str]) -> dict:
    """Process items with progress tracking via state updates.

    State transitions:
      PENDING  -> task queued, not yet picked up
      STARTED  -> worker received task (requires task_track_started=True)
      PROGRESS -> custom state via self.update_state()
      SUCCESS  -> task completed, result stored
    """
    total = len(items)
    processed = []

    for i, item in enumerate(items):
        # Simulate processing each item
        time.sleep(0.5)
        processed.append(item.upper())

        # Report progress via custom PROGRESS state
        self.update_state(
            state="PROGRESS",
            meta={
                "current": i + 1,
                "total": total,
                "percent": int((i + 1) / total * 100),
                "last_item": item,
            },
        )

    return {
        "processed": processed,
        "total": total,
        "status": "completed",
    }


@app.task(bind=True)
def multi_stage_pipeline(self, data: str) -> dict:
    """Pipeline task with named stages tracked via state updates."""
    stages = ["VALIDATING", "TRANSFORMING", "ENRICHING", "FINALIZING"]

    for i, stage in enumerate(stages):
        self.update_state(
            state="PROGRESS",
            meta={
                "stage": stage,
                "stage_number": i + 1,
                "total_stages": len(stages),
                "percent": int((i + 1) / len(stages) * 100),
            },
        )
        time.sleep(1)

    return {"data": data, "stages_completed": len(stages), "status": "done"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True, result_backend="disabled")
    print("=== Task States — KubeMQ Celery Transport ===\n")

    # Standard state flow
    print("Standard Celery task states:")
    print("  PENDING  -> Task published, not yet received by worker")
    print("  STARTED  -> Worker picked up the task (task_track_started=True)")
    print("  PROGRESS -> Custom state via self.update_state()")
    print("  SUCCESS  -> Task completed successfully")
    print("  FAILURE  -> Task raised an exception")
    print("  RETRY    -> Task scheduled for retry")
    print("  REVOKED  -> Task was revoked/cancelled")
    print()

    # Send a task with progress tracking
    print("[1] Sending process_items with 6 items...")
    items = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
    result = process_items.delay(items)
    print(f"    Task ID: {result.id}")

    # Poll for state changes
    print("    Polling task state...")
    last_state = None
    while not result.ready():
        state = result.state
        info = result.info
        if state != last_state or state == "PROGRESS":
            if state == "PROGRESS" and isinstance(info, dict):
                pct = info.get("percent", "?")
                current = info.get("current", "?")
                total = info.get("total", "?")
                print(f"    State: {state} — {current}/{total} ({pct}%)")
            else:
                print(f"    State: {state}")
            last_state = state
        time.sleep(0.3)

    print(f"    State: {result.state}")
    print(f"    Result: {result.get(timeout=10)}\n")

    # Multi-stage pipeline
    print("[2] Sending multi_stage_pipeline...")
    result2 = multi_stage_pipeline.delay("sample-data-payload")
    print(f"    Task ID: {result2.id}")

    print("    Polling pipeline stages...")
    last_stage = None
    while not result2.ready():
        state = result2.state
        info = result2.info
        if state == "PROGRESS" and isinstance(info, dict):
            stage = info.get("stage", "?")
            if stage != last_stage:
                pct = info.get("percent", "?")
                print(f"    Stage: {stage} ({pct}%)")
                last_stage = stage
        time.sleep(0.3)

    print(f"    State: {result2.state}")
    print(f"    Result: {result2.get(timeout=10)}\n")

    print("=== Task states demo complete ===")
    print("NOTE: Each state update overwrites the previous result on the")
    print("      celery-result-{task_id} KubeMQ channel (purge + write).")
