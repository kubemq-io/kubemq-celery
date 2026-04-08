"""Result Expiration — KubeMQ Celery Transport.

Demonstrates:
- Configuring result_expires for automatic result cleanup
- KubeMQ's 24-hour maximum expiration cap (MAX_EXPIRATION_SECONDS=86400)
- Behavior when results expire before retrieval
- timedelta vs integer expiration configuration

Usage:
    celery -A examples.result_backend.result_expiration worker --loglevel=info
    python examples/result_backend/result_expiration.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from datetime import timedelta

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "result_expiration",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

# Result expiration: KubeMQ caps at 86400 seconds (24 hours).
# Values exceeding 24h are silently capped. 0 or None defaults to 24h.
app.conf.result_expires = 3600  # 1 hour


@app.task
def quick_computation(x: int, y: int) -> int:
    """A fast task whose result expires after the configured period."""
    return x * y + x


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Result Expiration — KubeMQ Celery Transport ===\n")

    # Show current expiration config
    expires = app.conf.result_expires
    print(f"[config] result_expires = {expires}")
    print("         KubeMQ max expiration = 86400 seconds (24 hours)\n")

    # Demonstrate different expiration values
    configs = [
        ("1 hour (integer seconds)", 3600),
        ("30 minutes (timedelta)", timedelta(minutes=30)),
        ("24 hours (max allowed)", 86400),
        ("48 hours (capped to 24h)", 172800),
        ("None (defaults to 24h)", None),
        ("0 (treated as max=24h)", 0),
    ]

    for label, value in configs:
        effective = value
        if isinstance(value, timedelta):
            effective = int(value.total_seconds())
        elif value is None or value == 0:
            effective = 86400
        capped = min(effective, 86400) if effective > 0 else 86400
        print(f"  {label}")
        print(f"    -> Configured: {value}")
        print(f"    -> Effective:  {capped}s ({capped // 3600}h {(capped % 3600) // 60}m)")
        if isinstance(value, int) and value > 86400:
            print(f"    -> WARNING: Capped from {value}s to 86400s by KubeMQ")
        print()

    # Send a task with current expiration
    print("[1] Sending quick_computation(7, 8) with result_expires=3600...")
    result = quick_computation.delay(7, 8)
    print(f"    Task ID: {result.id}")
    try:
        value = result.get(timeout=30)
        print(f"    Result:  {value}")
    except Exception as exc:
        print(f"    Task failed or timed out: {exc}")
    print(f"    Expires in: {app.conf.result_expires}s")
    print("    After expiration, result.get() returns PENDING state.\n")

    print("=== Expiration demo complete ===")
    print("NOTE: Results stored as KubeMQ queue messages with expiration_in_seconds.")
    print("      After expiry, the message is automatically removed by KubeMQ.")
