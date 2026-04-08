"""ETA Scheduling — KubeMQ Celery Transport.

Demonstrates:
- task.apply_async(eta=datetime) for execution at a specific time
- KubeMQ converts ETA to delay_in_seconds internally
- UTC timezone handling for ETA calculations
- Comparing ETA vs countdown approaches

Usage:
    celery -A examples.scheduling.eta_scheduling worker --loglevel=info
    python examples/scheduling/eta_scheduling.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("eta_scheduling")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "enable_utc": True,
    }
)


@app.task
def run_at_time(label: str) -> dict:
    """Task scheduled to run at a specific ETA."""
    return {
        "label": label,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "timestamp": time.time(),
    }


@app.task
def timed_report(report_name: str) -> dict:
    """Generate a report at a scheduled time."""
    return {
        "report_name": report_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("NOTE: Running in eager mode — broker-side delays are not observed.")
    print("=== ETA Scheduling — KubeMQ Celery Transport ===\n")

    now = datetime.now(timezone.utc)
    print(f"Current UTC time: {now.isoformat()}\n")

    print("ETA scheduling: specify exact execution time as a datetime object.")
    print("KubeMQ calculates delay_in_seconds = (eta - now).total_seconds()\n")

    # Schedule task 10 seconds from now
    eta1 = now + timedelta(seconds=10)
    print(f"[1] Scheduling run_at_time for ETA={eta1.isoformat()}...")
    r1 = run_at_time.apply_async(args=("10s-from-now",), eta=eta1)
    print(f"    Task ID: {r1.id}\n")

    # Schedule task 5 seconds from now
    eta2 = now + timedelta(seconds=5)
    print(f"[2] Scheduling run_at_time for ETA={eta2.isoformat()}...")
    r2 = run_at_time.apply_async(args=("5s-from-now",), eta=eta2)
    print(f"    Task ID: {r2.id}")
    print("    Scheduled AFTER [1] but will execute BEFORE (earlier ETA)\n")

    # Schedule task 20 seconds from now
    eta3 = now + timedelta(seconds=20)
    print(f"[3] Scheduling timed_report for ETA={eta3.isoformat()}...")
    r3 = timed_report.apply_async(args=("daily-summary",), eta=eta3)
    print(f"    Task ID: {r3.id}\n")

    # Wait for results
    print("Waiting for scheduled tasks...\n")

    val2 = r2.get(timeout=30)
    print(f"    [5s ETA]  {val2['label']} executed at {val2['executed_at']}")

    val1 = r1.get(timeout=30)
    print(f"    [10s ETA] {val1['label']} executed at {val1['executed_at']}")

    val3 = r3.get(timeout=60)
    print(f"    [20s ETA] {val3['report_name']} generated at {val3['generated_at']}")

    print("\n--- ETA vs Countdown ---")
    print("  eta=datetime      -> Execute at specific time (timezone-aware)")
    print("  countdown=seconds -> Execute after N seconds from now")
    print("  Both use KubeMQ native delay_in_seconds internally")
    print()
    print("  IMPORTANT: Always use timezone-aware datetimes (UTC recommended)")
    print("  IMPORTANT: Max delay is 24 hours (86400s). ETA > now+24h is capped.")

    print("\n=== ETA scheduling demo complete ===")
