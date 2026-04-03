"""Delayed task execution with KubeMQ.

Demonstrates:
- `countdown` parameter -- deliver message after N seconds
- `eta` parameter -- deliver message at a specific datetime
- KubeMQ's native `delay_in_seconds` (no client-side polling)
- 12-hour maximum delay limitation

Usage:
    # Start a worker:
    celery -A delayed_tasks worker --loglevel=info

    # Send delayed tasks (in another terminal):
    python delayed_tasks.py
"""

from datetime import datetime, timedelta, timezone

import kubemq_celery
from celery import Celery

app = Celery(
    "delayed_tasks",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)


@app.task(bind=True)
def send_reminder(self, user_id: str, message: str) -> dict:
    """Send a reminder after a delay.

    When called with countdown or eta, the message is held by KubeMQ
    using native `delay_in_seconds` -- no client-side polling needed.
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "user_id": user_id,
        "message": message,
        "delivered_at": now,
        "task_id": self.request.id,
    }


@app.task
def process_batch(batch_id: str, items: list[str]) -> dict:
    """Process a batch of items after a scheduled delay."""
    return {
        "batch_id": batch_id,
        "items_processed": len(items),
        "status": "completed",
    }


@app.task(bind=True, max_retries=3)
def retry_with_backoff(self, url: str) -> dict:
    """Demonstrate retry with increasing countdown (exponential backoff).

    Each retry uses countdown to delay redelivery via KubeMQ's
    native delay_in_seconds.
    """
    try:
        # Simulate an operation that might fail
        if self.request.retries < 2:
            raise ConnectionError(f"Failed to connect to {url}")
        return {"url": url, "status": "success", "attempts": self.request.retries + 1}
    except ConnectionError as exc:
        # Exponential backoff: 10s, 40s, 160s
        backoff = 10 * (4 ** self.request.retries)
        raise self.retry(exc=exc, countdown=backoff)


if __name__ == "__main__":
    print("Sending delayed tasks to KubeMQ...\n")

    # --- countdown: deliver after N seconds ---

    # Deliver after 10 seconds
    r1 = send_reminder.apply_async(
        args=("user-1", "Meeting in 10 minutes"),
        countdown=10,
    )
    print(f"[countdown=10s]  send_reminder -> id={r1.id}")
    print("  Message held by KubeMQ for 10 seconds before delivery.\n")

    # Deliver after 30 seconds
    r2 = send_reminder.apply_async(
        args=("user-2", "Your trial expires soon"),
        countdown=30,
    )
    print(f"[countdown=30s]  send_reminder -> id={r2.id}")
    print("  Message held by KubeMQ for 30 seconds before delivery.\n")

    # --- eta: deliver at a specific time ---

    # Deliver 1 minute from now
    eta_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    r3 = send_reminder.apply_async(
        args=("user-3", "Scheduled check-in"),
        eta=eta_time,
    )
    print(f"[eta={eta_time.isoformat()}]")
    print(f"  send_reminder -> id={r3.id}")
    print("  KubeMQ calculates delay from ETA and holds the message.\n")

    # --- Batch processing with delay ---

    r4 = process_batch.apply_async(
        args=("batch-001", ["item-a", "item-b", "item-c"]),
        countdown=5,
    )
    print(f"[countdown=5s]   process_batch -> id={r4.id}")
    print("  Batch processing starts after 5 second delay.\n")

    # --- Retry with exponential backoff ---

    r5 = retry_with_backoff.delay("https://api.example.com/data")
    print(f"[retry+backoff]  retry_with_backoff -> id={r5.id}")
    print("  Each retry uses increasing countdown (10s, 40s, 160s).\n")

    # --- Important: 12-hour maximum delay ---
    print("NOTE: KubeMQ maximum delay is 43200 seconds (12 hours).")
    print("Delays exceeding this limit are capped at 12 hours with a warning.")
    print("For longer scheduling, use Celery Beat.\n")

    # Example of a long delay (capped at 12 hours)
    r6 = send_reminder.apply_async(
        args=("user-4", "Daily digest"),
        countdown=86400,  # 24 hours -- will be capped to 43200 (12h)
    )
    print(f"[countdown=86400s -> capped to 43200s]")
    print(f"  send_reminder -> id={r6.id}")
    print("  24h delay capped to 12h by KubeMQ transport.\n")

    # Wait for the quick results
    print("Waiting for results (tasks with short delays)...")
    print(f"  process_batch: {r4.get(timeout=30)}")
    print(f"  retry_with_backoff: {r5.get(timeout=120)}")
    print(f"  send_reminder (10s): {r1.get(timeout=30)}")
