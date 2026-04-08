"""Basic Retry — KubeMQ Celery Transport.

Demonstrates:
- max_retries for limiting retry attempts
- autoretry_for to automatically retry on specific exceptions
- Celery's built-in retry mechanism with countdown

Usage:
    # Start a worker:
    celery -A examples.error_handling.basic_retry worker --loglevel=info

    # Run the example:
    python examples/error_handling/basic_retry.py

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
    "basic_retry",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task(bind=True, max_retries=3)
def unreliable_api_call(self, endpoint: str) -> dict:
    """Simulate an unreliable API call with manual retry.

    Fails randomly ~50% of the time. Retries up to 3 times
    with a 2-second countdown between attempts.
    """
    if random.random() < 0.5:
        print(f"  Attempt {self.request.retries + 1}: API call failed, retrying...")
        raise self.retry(countdown=2, exc=ConnectionError("API unavailable"))

    return {"endpoint": endpoint, "status": "success", "attempt": self.request.retries + 1}


@app.task(
    autoretry_for=(ConnectionError, TimeoutError),
    max_retries=3,
    default_retry_delay=1,
)
def auto_retry_task(url: str) -> dict:
    """Task with autoretry_for — Celery automatically retries on listed exceptions.

    No need to call self.retry() manually.
    """
    if random.random() < 0.3:
        raise ConnectionError(f"Connection to {url} refused")

    return {"url": url, "status": "fetched"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Basic Retry Example")
    print("=" * 40)

    # Manual retry
    print("\n--- Manual retry (max_retries=3) ---")
    try:
        result = unreliable_api_call.delay("https://api.example.com/data")
        value = result.get(timeout=30)
        print(f"Result: {value}")
    except Exception as exc:
        print(f"Task failed after all retries: {exc}")

    # Auto retry
    print("\n--- Auto retry (autoretry_for) ---")
    try:
        result2 = auto_retry_task.delay("https://api.example.com/users")
        value2 = result2.get(timeout=30)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"Task failed after all retries: {exc}")

    print("\nDone!")
