"""Custom Retry Policy — KubeMQ Celery Transport.

Demonstrates:
- retry_backoff=True for automatic exponential backoff
- retry_jitter=True to add randomness and prevent thundering herd
- retry_backoff_max to cap the maximum delay
- Celery handles all backoff math automatically

Usage:
    # Start a worker:
    celery -A examples.error_handling.custom_retry_policy worker --loglevel=info

    # Run the example:
    python examples/error_handling/custom_retry_policy.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import random

from celery import Celery

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery(
    "custom_retry_policy",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task(
    autoretry_for=(ConnectionError, TimeoutError),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
)
def resilient_http_call(url: str) -> dict:
    """HTTP call with Celery's built-in backoff + jitter.

    retry_backoff=True:       enables exponential backoff (1, 2, 4, 8, 16...)
    retry_backoff_max=60:     caps delay at 60 seconds
    retry_jitter=True:        adds random jitter to prevent thundering herd
    """
    if random.random() < 0.4:
        raise ConnectionError(f"Connection to {url} refused")

    return {"url": url, "status": "ok"}


@app.task(
    autoretry_for=(ValueError,),
    max_retries=3,
    retry_backoff=2,
    retry_backoff_max=30,
    retry_jitter=False,
)
def validate_data(payload: dict) -> dict:
    """Validate incoming data with a fixed backoff multiplier.

    retry_backoff=2:          base delay is 2s (then 4, 8, 16...)
    retry_jitter=False:       deterministic delays (no randomness)
    """
    if random.random() < 0.3:
        raise ValueError("Temporary validation service error")

    return {"payload": payload, "valid": True}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Custom Retry Policy Example")
    print("=" * 40)

    print("\n--- Backoff + Jitter ---")
    print("  Policy: autoretry, backoff=True, max=60s, jitter=True")
    try:
        result = resilient_http_call.delay("https://api.example.com/data")
        value = result.get(timeout=120)
        print(f"  Result: {value}")
    except Exception as exc:
        print(f"  Failed: {exc}")

    print("\n--- Fixed backoff multiplier ---")
    print("  Policy: autoretry, backoff=2, max=30s, jitter=False")
    try:
        result2 = validate_data.delay({"user": "test", "action": "create"})
        value2 = result2.get(timeout=60)
        print(f"  Result: {value2}")
    except Exception as exc:
        print(f"  Failed: {exc}")

    print("\nDone!")
