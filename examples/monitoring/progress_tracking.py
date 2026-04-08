"""Progress Tracking — KubeMQ Celery Transport.

Demonstrates:
- self.update_state() for fine-grained progress reporting
- Client-side polling loop with progress display
- Progress bar rendering from task metadata
- Multiple concurrent tasks with independent progress

Usage:
    celery -A examples.monitoring.progress_tracking worker --loglevel=info
    python examples/monitoring/progress_tracking.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import sys
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("progress_tracking")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_track_started": True,
    }
)


@app.task(bind=True)
def process_dataset(self, dataset_name: str, total_records: int) -> dict:
    """Process a dataset with per-record progress tracking."""
    processed = 0
    for i in range(total_records):
        time.sleep(0.3)
        processed += 1
        self.update_state(
            state="PROGRESS",
            meta={
                "dataset": dataset_name,
                "current": processed,
                "total": total_records,
                "percent": int(processed / total_records * 100),
            },
        )
    return {
        "dataset": dataset_name,
        "records_processed": total_records,
        "status": "complete",
    }


@app.task(bind=True)
def multi_phase_job(self, job_name: str) -> dict:
    """Job with distinct named phases."""
    phases = [
        ("downloading", 3),
        ("parsing", 2),
        ("analyzing", 4),
        ("writing_output", 1),
    ]
    total_steps = sum(d for _, d in phases)
    completed_steps = 0

    for phase_name, duration in phases:
        for step in range(duration):
            time.sleep(0.5)
            completed_steps += 1
            self.update_state(
                state="PROGRESS",
                meta={
                    "job": job_name,
                    "phase": phase_name,
                    "phase_step": step + 1,
                    "phase_total": duration,
                    "overall_percent": int(completed_steps / total_steps * 100),
                },
            )

    return {"job": job_name, "phases_completed": len(phases), "status": "done"}


def render_progress_bar(percent: int, width: int = 30) -> str:
    """Render a text progress bar."""
    filled = int(width * percent / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percent:3d}%"


def poll_progress(result, label: str):
    """Poll a task's progress and display it."""
    last_percent = -1
    while not result.ready():
        info = result.info
        if isinstance(info, dict):
            percent = info.get("percent", info.get("overall_percent", 0))
            extra = ""
            if "phase" in info:
                extra = f" ({info['phase']})"
            elif "dataset" in info:
                extra = f" ({info['current']}/{info['total']})"
            if percent != last_percent:
                bar = render_progress_bar(percent)
                sys.stdout.write(f"\r    {label}: {bar}{extra}  ")
                sys.stdout.flush()
                last_percent = percent
        time.sleep(0.3)
    sys.stdout.write(f"\r    {label}: {render_progress_bar(100)} DONE\n")
    sys.stdout.flush()


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True, result_backend="disabled")
    print("=== Progress Tracking — KubeMQ Celery Transport ===\n")

    # Single task progress
    print("[1] Tracking process_dataset progress...")
    r1 = process_dataset.delay("users.csv", 10)
    poll_progress(r1, "Dataset")
    final1 = r1.get(timeout=10)
    print(f"    Result: {final1}\n")

    # Multi-phase job progress
    print("[2] Tracking multi_phase_job progress...")
    r2 = multi_phase_job.delay("report-2024")
    poll_progress(r2, "Job    ")
    final2 = r2.get(timeout=10)
    print(f"    Result: {final2}\n")

    # Multiple concurrent tasks
    print("[3] Tracking 3 concurrent dataset tasks...")
    tasks = [
        ("DS-A", process_dataset.delay("alpha.csv", 5)),
        ("DS-B", process_dataset.delay("beta.csv", 7)),
        ("DS-C", process_dataset.delay("gamma.csv", 4)),
    ]

    while any(not r.ready() for _, r in tasks):
        parts = []
        for label, r in tasks:
            info = r.info
            if r.ready():
                parts.append(f"{label}:DONE")
            elif isinstance(info, dict):
                pct = info.get("percent", 0)
                parts.append(f"{label}:{pct:3d}%")
            else:
                parts.append(f"{label}:  ?%")
        sys.stdout.write(f"\r    {' | '.join(parts)}    ")
        sys.stdout.flush()
        time.sleep(0.3)

    print(f"\r    {' | '.join(f'{lbl}:DONE' for lbl, _ in tasks)}    ")
    for label, r in tasks:
        print(f"    {label}: {r.get(timeout=10)}")
    print()

    print("=== Progress tracking demo complete ===")
    print("NOTE: Each update_state() overwrites the result on the KubeMQ")
    print("      result channel. Client polls via peek_queue_messages().")
