"""One-Shot Scheduled Task — KubeMQ Celery Transport.

Demonstrates:
- Scheduling a task to run once at a specific future time
- Using apply_async(eta=...) for one-off scheduling
- Using apply_async(countdown=...) for relative delay
- Difference from periodic Beat tasks

Usage:
    celery -A examples.scheduling.one_shot_scheduled worker --loglevel=info
    python examples/scheduling/one_shot_scheduled.py

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

app = Celery("one_shot_scheduled")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "enable_utc": True,
    }
)


@app.task
def send_email(to: str, subject: str, body: str) -> dict:
    """Send an email at a scheduled time (simulated)."""
    return {
        "to": to,
        "subject": subject,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "status": "sent",
    }


@app.task
def expire_offer(offer_id: str) -> dict:
    """Expire a promotional offer after a delay."""
    return {
        "offer_id": offer_id,
        "expired_at": datetime.now(timezone.utc).isoformat(),
        "status": "expired",
    }


@app.task
def release_reservation(reservation_id: str) -> dict:
    """Release a held reservation after timeout."""
    return {
        "reservation_id": reservation_id,
        "released_at": datetime.now(timezone.utc).isoformat(),
        "status": "released",
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("NOTE: Running in eager mode — broker-side delays are not observed.")
    print("=== One-Shot Scheduled Task — KubeMQ Celery Transport ===\n")

    now = datetime.now(timezone.utc)
    print(f"Current UTC time: {now.isoformat()}\n")

    # Schedule email for 10 seconds from now
    eta1 = now + timedelta(seconds=10)
    print(f"[1] Scheduling email delivery at {eta1.isoformat()}...")
    r1 = send_email.apply_async(
        kwargs={
            "to": "user@example.com",
            "subject": "Your order confirmation",
            "body": "Thank you for your purchase!",
        },
        eta=eta1,
    )
    print(f"    Task ID: {r1.id}")
    print("    KubeMQ holds message for ~10 seconds\n")

    # Schedule offer expiration via countdown
    print("[2] Scheduling offer expiration in 15 seconds (countdown)...")
    r2 = expire_offer.apply_async(
        args=("PROMO-2024-SPRING",),
        countdown=15,
    )
    print(f"    Task ID: {r2.id}")
    print("    Uses countdown=15 (relative delay)\n")

    # Schedule reservation release via ETA
    release_time = now + timedelta(seconds=20)
    print(f"[3] Scheduling reservation release at {release_time.isoformat()}...")
    r3 = release_reservation.apply_async(
        args=("RES-12345",),
        eta=release_time,
    )
    print(f"    Task ID: {r3.id}\n")

    # Wait for results
    print("Waiting for scheduled tasks to execute...\n")

    publish_time = time.time()

    val1 = r1.get(timeout=60)
    elapsed1 = time.time() - publish_time
    print(f"    [{elapsed1:.1f}s] Email sent to {val1['to']} at {val1['sent_at']}")

    val2 = r2.get(timeout=60)
    elapsed2 = time.time() - publish_time
    print(f"    [{elapsed2:.1f}s] Offer {val2['offer_id']} expired at {val2['expired_at']}")

    val3 = r3.get(timeout=60)
    elapsed3 = time.time() - publish_time
    print(f"    [{elapsed3:.1f}s] Reservation {val3['reservation_id']} released")

    print("\n--- One-shot vs periodic scheduling ---")
    print("  One-shot (this example):")
    print("    task.apply_async(eta=datetime)  -> runs ONCE at specific time")
    print("    task.apply_async(countdown=N)   -> runs ONCE after N seconds")
    print()
    print("  Periodic (Beat):")
    print("    beat_schedule with crontab/timedelta -> runs REPEATEDLY")
    print()
    print("  Both use KubeMQ native delay_in_seconds (max 24h).")
    print("  For one-shot tasks > 24h, store in DB and use Beat to check.")

    print("\n=== One-shot scheduled task demo complete ===")
