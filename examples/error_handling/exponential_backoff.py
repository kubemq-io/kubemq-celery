"""Exponential Backoff — KubeMQ Celery Transport.

Demonstrates:
- Manual exponential backoff with self.retry(countdown=...)
- Doubling the delay on each retry attempt
- Useful for rate-limited APIs or temporarily unavailable services

Usage:
    # Start a worker:
    celery -A examples.error_handling.exponential_backoff worker --loglevel=info

    # Run the example:
    python examples/error_handling/exponential_backoff.py

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
    "exponential_backoff",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)


@app.task(bind=True, max_retries=5)
def call_rate_limited_api(self, resource_id: str) -> dict:
    """Simulate calling a rate-limited API with exponential backoff.

    Backoff schedule: 2s, 4s, 8s, 16s, 32s (base=2, factor=2^retries).
    """
    if random.random() < 0.6:
        backoff = 2 ** (self.request.retries + 1)
        print(f"  Attempt {self.request.retries + 1}: rate limited, retrying in {backoff}s...")
        raise self.retry(
            countdown=backoff,
            exc=ConnectionError("429 Too Many Requests"),
        )

    return {
        "resource_id": resource_id,
        "status": "fetched",
        "attempts": self.request.retries + 1,
    }


@app.task(bind=True, max_retries=4)
def sync_external_service(self, service_name: str) -> dict:
    """Sync with an external service using capped exponential backoff.

    Backoff: min(2^retries * 3, 30) seconds — caps at 30s.
    """
    if random.random() < 0.5:
        backoff = min(3 * (2**self.request.retries), 30)
        print(f"  Attempt {self.request.retries + 1}: sync failed, retrying in {backoff}s...")
        raise self.retry(
            countdown=backoff,
            exc=TimeoutError(f"{service_name} timed out"),
        )

    return {
        "service": service_name,
        "status": "synced",
        "attempts": self.request.retries + 1,
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Exponential Backoff Example")
    print("=" * 40)

    print("\n--- Rate-limited API (backoff: 2, 4, 8, 16, 32s) ---")
    try:
        result = call_rate_limited_api.delay("resource-42")
        value = result.get(timeout=120)
        print(f"Result: {value}")
    except Exception as exc:
        print(f"Failed after all retries: {exc}")

    print("\n--- External service sync (capped at 30s) ---")
    try:
        result2 = sync_external_service.delay("payment-gateway")
        value2 = result2.get(timeout=120)
        print(f"Result: {value2}")
    except Exception as exc:
        print(f"Failed after all retries: {exc}")

    print("\nDone!")
