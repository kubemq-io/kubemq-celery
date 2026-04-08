"""Custom State Updates — KubeMQ Celery Transport.

Demonstrates:
- self.update_state(state='PROGRESS', meta={...}) for real-time progress
- Custom state names beyond standard Celery states
- Rich metadata in state updates (progress bars, stage info, ETAs)
- Client-side polling of custom states

Usage:
    celery -A examples.signals.custom_state_updates worker --loglevel=info
    python examples/signals/custom_state_updates.py

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
    "custom_state_updates",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    result_expires=3600,
    task_track_started=True,
)


@app.task(bind=True)
def file_upload_processor(self, filename: str, chunk_count: int) -> dict:
    """Simulates processing an uploaded file with detailed progress."""
    for chunk in range(1, chunk_count + 1):
        time.sleep(0.5)
        self.update_state(
            state="PROGRESS",
            meta={
                "phase": "processing",
                "filename": filename,
                "chunk": chunk,
                "total_chunks": chunk_count,
                "percent": int(chunk / chunk_count * 100),
            },
        )

    # Final validation phase
    self.update_state(
        state="PROGRESS",
        meta={
            "phase": "validating",
            "filename": filename,
            "percent": 100,
        },
    )
    time.sleep(1)

    return {"filename": filename, "chunks": chunk_count, "status": "complete"}


@app.task(bind=True)
def data_migration(self, table_name: str, row_count: int) -> dict:
    """Simulates data migration with custom state names."""
    # Custom states beyond standard PROGRESS
    stages = [
        ("EXTRACTING", "Reading source data"),
        ("TRANSFORMING", "Applying transformations"),
        ("LOADING", "Writing to destination"),
        ("VERIFYING", "Checking data integrity"),
    ]

    rows_per_stage = row_count // len(stages)

    for i, (state_name, description) in enumerate(stages):
        for row in range(rows_per_stage):
            time.sleep(0.1)

        self.update_state(
            state=state_name,
            meta={
                "description": description,
                "stage": i + 1,
                "total_stages": len(stages),
                "rows_processed": (i + 1) * rows_per_stage,
                "total_rows": row_count,
                "percent": int((i + 1) / len(stages) * 100),
            },
        )

    return {
        "table": table_name,
        "rows_migrated": row_count,
        "status": "migration_complete",
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True, result_backend="disabled")
    print("=== Custom State Updates — KubeMQ Celery Transport ===\n")

    # File upload processing with PROGRESS state
    print("[1] Sending file_upload_processor('report.csv', 4)...")
    result = file_upload_processor.delay("report.csv", 4)
    print(f"    Task ID: {result.id}")

    last_info = None
    while not result.ready():
        state = result.state
        info = result.info
        if isinstance(info, dict) and info != last_info:
            phase = info.get("phase", "?")
            pct = info.get("percent", 0)
            chunk = info.get("chunk", "")
            total = info.get("total_chunks", "")
            extra = f"chunk {chunk}/{total}" if chunk else ""
            print(f"    [{state}] {phase} — {pct}% {extra}")
            last_info = info
        time.sleep(0.3)

    final = result.get(timeout=10)
    print(f"    [{result.state}] {final}\n")

    # Data migration with custom state names
    print("[2] Sending data_migration('users', 40)...")
    result2 = data_migration.delay("users", 40)
    print(f"    Task ID: {result2.id}")

    last_state = None
    while not result2.ready():
        state = result2.state
        info = result2.info
        if state != last_state and isinstance(info, dict):
            desc = info.get("description", "")
            pct = info.get("percent", 0)
            rows = info.get("rows_processed", 0)
            print(f"    [{state}] {desc} — {pct}% ({rows} rows)")
            last_state = state
        time.sleep(0.3)

    final2 = result2.get(timeout=10)
    print(f"    [{result2.state}] {final2}\n")

    print("=== Custom state updates demo complete ===")
    print("NOTE: Each update_state() overwrites the previous result on the")
    print("      KubeMQ result channel. Custom state names (EXTRACTING, etc.)")
    print("      are fully supported — they're stored as the 'status' field.")
