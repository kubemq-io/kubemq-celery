"""Beat Max Delay Warning — KubeMQ Celery Transport.

Demonstrates:
- KubeMQ's 24-hour maximum delay cap (86400 seconds)
- countdown=172800 (48h) being capped to 86400 (24h) with a warning
- How the transport handles over-limit delays
- Best practices for scheduling beyond 24 hours

Usage:
    celery -A examples.scheduling.beat_max_delay_warning worker --loglevel=info
    python examples/scheduling/beat_max_delay_warning.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("beat_max_delay_warning")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
    }
)


@app.task
def delayed_task(label: str) -> dict:
    """A task that may be delayed."""
    return {
        "label": label,
        "executed_at": time.time(),
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("NOTE: Running in eager mode — broker-side delays are not observed.")
    print("=== Beat Max Delay Warning — KubeMQ Celery Transport ===\n")

    MAX_DELAY = 86400  # 24 hours in seconds

    print(f"KubeMQ maximum delay: {MAX_DELAY}s ({MAX_DELAY // 3600}h)\n")

    delay_tests = [
        ("within-limit-10s", 10, "Within limit — delivered after 10s"),
        ("within-limit-1h", 3600, "Within limit — delivered after 1 hour"),
        ("within-limit-12h", 43200, "Within limit — delivered after 12 hours"),
        ("at-limit-24h", 86400, "At limit — delivered after exactly 24 hours"),
        ("over-limit-48h", 172800, "CAPPED — 48h requested, delivered after 24h"),
        ("over-limit-7d", 604800, "CAPPED — 7 days requested, delivered after 24h"),
    ]

    print("Countdown delay behavior:\n")
    for label, delay, note in delay_tests:
        effective = min(delay, MAX_DELAY)
        capped = delay > MAX_DELAY
        status = "CAPPED" if capped else "OK"
        print(f"  [{status:6s}] countdown={delay:>7d}s ({delay / 3600:.1f}h)")
        print(f"           effective={effective:>7d}s ({effective / 3600:.1f}h)")
        print(f"           {note}")
        if capped:
            print(f"           WARNING: delay {delay}s capped to {MAX_DELAY}s")
        print()

    print("[1] Sending task with countdown=5 (within limit)...")
    r1 = delayed_task.apply_async(args=("normal-5s",), countdown=5)
    print(f"    Task ID: {r1.id}")
    val = r1.get(timeout=30)
    print(f"    Result: {val}\n")

    print("--- Scheduling beyond the limit ---")
    print(f"  For delays > {MAX_DELAY}s, use Celery Beat instead:")
    print()
    print("  from celery.schedules import crontab")
    print("  beat_schedule = {")
    print("      'daily-at-noon': {")
    print("          'task': 'myapp.tasks.daily_job',")
    print("          'schedule': crontab(minute=0, hour=12),")
    print("      },")
    print("  }")
    print()
    print("  Beat handles recurring schedules without delay limits.")

    print("\n=== Max delay warning demo complete ===")
