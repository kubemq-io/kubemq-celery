"""Flower Monitoring Setup — KubeMQ Celery Transport.

Demonstrates:
- Setting up Flower web UI with KubeMQ backend
- Enabling worker events for task monitoring
- Flower configuration options
- Real-time task monitoring dashboard

Usage:
    celery -A examples.monitoring.flower_setup worker --loglevel=info -E
    celery -A examples.monitoring.flower_setup flower --port=5555
    python examples/monitoring/flower_setup.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - flower installed (pip install flower)
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("flower_setup")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        # Events required for Flower monitoring
        "worker_send_task_events": True,
        "task_send_sent_event": True,
        # Optional: tune event settings
        "event_queue_expires": 60,
    }
)


@app.task(bind=True)
def monitored_task(self, duration: float = 1.0) -> dict:
    """A task that reports progress — visible in Flower."""
    for i in range(int(duration * 2)):
        time.sleep(0.5)
        self.update_state(
            state="PROGRESS",
            meta={"step": i + 1, "total": int(duration * 2)},
        )
    return {"duration": duration, "status": "done"}


@app.task
def simple_compute(x: int, y: int) -> int:
    """Simple computation — visible in Flower task list."""
    return x + y


@app.task
def failing_example() -> None:
    """Task that fails — shows as FAILURE in Flower."""
    raise RuntimeError("intentional failure for monitoring demo")


if __name__ == "__main__":
    print("=== Flower Monitoring Setup — KubeMQ Celery Transport ===\n")

    print("Flower is a real-time web-based monitoring tool for Celery.\n")

    print("Setup steps:\n")
    print("  1. Install Flower:")
    print("     pip install flower\n")
    print("  2. Start worker with events enabled (-E flag):")
    print("     celery -A examples.monitoring.flower_setup worker -E --loglevel=info\n")
    print("  3. Start Flower:")
    print("     celery -A examples.monitoring.flower_setup flower --port=5555\n")
    print("  4. Open browser:")
    print("     http://localhost:5555\n")

    print("Flower features with KubeMQ:")
    print("  - Real-time task monitoring (state, duration, result)")
    print("  - Worker status and statistics")
    print("  - Task rate graphs")
    print("  - Task detail with traceback on failure")
    print("  - Worker pool management (grow/shrink)")
    print()
    print("  Events are broadcast via KubeMQ Events (PubSub fanout).")
    print("  Flower subscribes to the celery event exchange.\n")

    print("Flower CLI options:")
    print("  --port=5555           Web UI port")
    print("  --address=0.0.0.0     Bind address")
    print("  --basic-auth=user:pwd HTTP basic auth")
    print("  --persistent=True     Persistent storage for task history")
    print("  --max-tasks=10000     Max tasks to keep in memory")
    print()

    # Send sample tasks
    print("[1] Sending sample tasks for Flower dashboard...")
    r1 = monitored_task.delay(2.0)
    print(f"    monitored_task(2.0) -> {r1.id[:8]}...")

    for i in range(5):
        simple_compute.delay(i, i * 2)
    print("    5x simple_compute sent")

    r3 = failing_example.delay()
    print(f"    failing_example -> {r3.id[:8]}...")
    print()

    print("Open http://localhost:5555 to see these tasks in Flower.")
    print("\n=== Flower setup demo complete ===")
