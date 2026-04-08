"""Task Rate Limiting — KubeMQ Celery Transport.

Demonstrates:
- @app.task(rate_limit='10/m') to limit task execution rate
- Per-task rate limiting with different formats
- Rate limit enforcement by the worker (token bucket)
- Rate limit formats: '10/s', '100/m', '1000/h'

Usage:
    celery -A examples.rate_limiting.rate_limit worker --loglevel=info
    python examples/rate_limiting/rate_limit.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "rate_limit",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task(rate_limit="10/m")
def rate_limited_api_call(endpoint: str) -> dict:
    """Call an external API — limited to 10 per minute.

    The worker enforces the rate limit using a token bucket.
    Tasks exceeding the limit are held in the worker's ready queue
    until a token becomes available.
    """
    return {
        "endpoint": endpoint,
        "called_at": time.time(),
        "status": "ok",
    }


@app.task(rate_limit="2/s")
def fast_rate_limited(item_id: int) -> int:
    """Process items at max 2 per second."""
    return item_id * 2


@app.task(rate_limit="100/h")
def hourly_limited_report(report_id: str) -> dict:
    """Generate a report — limited to 100 per hour."""
    return {"report_id": report_id, "generated_at": time.time()}


@app.task
def unlimited_task(x: int) -> int:
    """No rate limit — processed as fast as possible."""
    return x * x


if __name__ == "__main__":
    print("=== Task Rate Limiting — KubeMQ Celery Transport ===\n")

    print("Rate limit formats:")
    print("  '10/s'   -> 10 tasks per second")
    print("  '10/m'   -> 10 tasks per minute")
    print("  '100/h'  -> 100 tasks per hour")
    print("  None     -> no rate limit (default)")
    print()

    print("Configured tasks:")
    print("  rate_limited_api_call  -> rate_limit='10/m'")
    print("  fast_rate_limited      -> rate_limit='2/s'")
    print("  hourly_limited_report  -> rate_limit='100/h'")
    print("  unlimited_task         -> no rate limit")
    print()

    print("To test rate limiting:")
    print("  1. Start a worker:")
    print("     celery -A examples.rate_limiting.rate_limit worker --loglevel=info")
    print("  2. Send tasks and observe rate-limited execution in worker logs.")
    print()
    print("NOTE: Rate limits are enforced per-worker. With multiple workers,")
    print("      the effective rate is multiplied by the number of workers.")
    print("      Rate limits apply to execution, not to publishing.")
    print()
    print("=== Configuration demo complete ===")
