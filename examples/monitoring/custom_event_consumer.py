"""Custom Event Consumer — KubeMQ Celery Transport.

Demonstrates:
- Programmatic Celery event consumer using app.events.Receiver
- Processing task events (task-sent, task-succeeded, task-failed)
- Building custom monitoring/alerting from event stream
- Events delivered via KubeMQ Events (PubSub fanout)

Usage:
    celery -A examples.monitoring.custom_event_consumer worker --loglevel=info -E
    python examples/monitoring/custom_event_consumer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import threading
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("custom_event_consumer")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "worker_send_task_events": True,
        "task_send_sent_event": True,
    }
)


@app.task
def sample_task(x: int) -> int:
    """Sample task for event generation."""
    return x * x


@app.task
def failing_task() -> None:
    """Task that always fails."""
    raise RuntimeError("intentional failure")


def run_event_monitor(duration: float = 15.0):
    """Run a custom event monitor for a specified duration.

    This demonstrates how to programmatically consume Celery events
    delivered via KubeMQ Events (PubSub fanout).
    """
    event_log: list[dict] = []
    stop_event = threading.Event()

    def on_task_sent(event):
        info = {"type": "task-sent", "task": event.get("name", "?"), "uuid": event.get("uuid", "?")}
        event_log.append(info)
        print(f"  [EVENT] task-sent: {info['task']} ({info['uuid'][:8]}...)")

    def on_task_received(event):
        info = {
            "type": "task-received",
            "task": event.get("name", "?"),
            "uuid": event.get("uuid", "?"),
        }
        event_log.append(info)
        print(f"  [EVENT] task-received: {info['task']}")

    def on_task_succeeded(event):
        info = {
            "type": "task-succeeded",
            "uuid": event.get("uuid", "?"),
            "runtime": event.get("runtime", 0),
        }
        event_log.append(info)
        print(f"  [EVENT] task-succeeded: {info['uuid'][:8]}... (runtime={info['runtime']:.3f}s)")

    def on_task_failed(event):
        info = {
            "type": "task-failed",
            "uuid": event.get("uuid", "?"),
            "exception": str(event.get("exception", ""))[:50],
        }
        event_log.append(info)
        print(f"  [EVENT] task-failed: {info['uuid'][:8]}... ({info['exception']})")

    print(f"\n  Starting event monitor for {duration}s...")
    print("  Events delivered via KubeMQ Events (PubSub fanout).\n")

    def capture_events():
        with app.connection() as conn:
            recv = app.events.Receiver(
                conn,
                handlers={
                    "task-sent": on_task_sent,
                    "task-received": on_task_received,
                    "task-succeeded": on_task_succeeded,
                    "task-failed": on_task_failed,
                },
            )
            try:
                recv.capture(limit=None, timeout=duration, wakeup=True)
            except Exception as exc:
                print(f"Monitor error: {exc}")

    monitor_thread = threading.Thread(target=capture_events, daemon=True)
    monitor_thread.start()

    # Give monitor time to connect
    time.sleep(2)

    return event_log, monitor_thread, stop_event


if __name__ == "__main__":
    print("=== Custom Event Consumer — KubeMQ Celery Transport ===\n")

    print("Programmatic event consumption:")
    print("  Use app.events.Receiver to process events in your own code.")
    print("  Events flow via KubeMQ Events (PubSub) — all consumers see all events.\n")

    print("Event handler pattern:")
    print("  with app.connection() as conn:")
    print("      recv = app.events.Receiver(conn, handlers={")
    print("          'task-sent': on_task_sent,")
    print("          'task-succeeded': on_task_succeeded,")
    print("          'task-failed': on_task_failed,")
    print("      })")
    print("      recv.capture(limit=None, timeout=30, wakeup=True)")
    print()

    print("--- Custom consumer patterns ---")
    print("  Alerting:  on_task_failed -> send alert to Slack/PagerDuty")
    print("  Metrics:   on_task_succeeded -> record runtime to Prometheus")
    print("  Auditing:  on_task_sent -> log task submissions")
    print("  Dashboard: Aggregate events for custom monitoring UI")
    print()

    print("To test:")
    print("  1. Start a worker with events enabled:")
    print("     celery -A examples.monitoring.custom_event_consumer worker --loglevel=info -E")
    print("  2. Run this module's run_event_monitor() function to capture events.")
    print()
    print("=== Configuration demo complete ===")
