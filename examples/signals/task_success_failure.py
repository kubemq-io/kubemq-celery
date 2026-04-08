"""Task Success/Failure Signals — KubeMQ Celery Transport.

Demonstrates:
- task_success signal for alerting on successful completion
- task_failure signal for alerting on task errors
- Building an alerting/notification system via signals
- Accessing exception info in failure handler

Usage:
    celery -A examples.signals.task_success_failure worker --loglevel=info
    python examples/signals/task_success_failure.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery
from celery.signals import task_failure, task_success

import kubemq_celery  # noqa: F401

app = Celery(
    "task_success_failure",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600

# Counters for demo (in production, use a proper metrics system)
_alert_log: list[dict] = []


@task_success.connect
def on_task_success(sender: Any = None, result: Any = None, **kwargs: Any) -> None:
    """Called when a task completes successfully."""
    alert = {
        "type": "SUCCESS",
        "task": sender.name,
        "result_preview": str(result)[:100],
    }
    _alert_log.append(alert)
    print(f"[SUCCESS ALERT] Task {sender.name} completed: {str(result)[:80]}")


@task_failure.connect
def on_task_failure(
    sender: Any = None,
    task_id: str = "",
    exception: BaseException | None = None,
    traceback: Any = None,
    einfo: Any = None,
    **kwargs: Any,
) -> None:
    """Called when a task fails with an exception."""
    alert = {
        "type": "FAILURE",
        "task": sender.name,
        "task_id": task_id,
        "exception_type": type(exception).__name__ if exception else "Unknown",
        "exception_msg": str(exception)[:200] if exception else "",
    }
    _alert_log.append(alert)
    print(
        f"[FAILURE ALERT] Task {sender.name} ({task_id[:8]}...) "
        f"failed: {type(exception).__name__}: {exception}"
    )


@app.task
def divide(x: float, y: float) -> float:
    """Divide x by y — may fail with ZeroDivisionError."""
    return x / y


@app.task
def validate_email(email: str) -> dict:
    """Validate an email address (simplified)."""
    if "@" not in email:
        raise ValueError(f"Invalid email: {email}")
    local, domain = email.rsplit("@", 1)
    if not domain or "." not in domain:
        raise ValueError(f"Invalid email domain: {domain}")
    return {"email": email, "local": local, "domain": domain, "valid": True}


@app.task
def process_order(order_id: str, amount: float) -> dict:
    """Process an order — fails if amount is negative."""
    if amount < 0:
        raise ValueError(f"Invalid amount: {amount}")
    return {"order_id": order_id, "amount": amount, "status": "processed"}


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Task Success/Failure Signals — KubeMQ Celery Transport ===\n")
    print("Signal handlers print alerts on the WORKER side.\n")

    # Successful tasks
    print("[1] Sending divide(100, 4) — expecting SUCCESS...")
    r1 = divide.delay(100, 4)
    print(f"    Result: {r1.get(timeout=30)}\n")

    print("[2] Sending validate_email('user@example.com') — expecting SUCCESS...")
    r2 = validate_email.delay("user@example.com")
    print(f"    Result: {r2.get(timeout=30)}\n")

    # Failing tasks
    print("[3] Sending divide(10, 0) — expecting FAILURE (ZeroDivisionError)...")
    try:
        r3 = divide.delay(10, 0)
        r3.get(timeout=30)
    except Exception as exc:
        print(f"    Expected error: {type(exc).__name__}: {exc}\n")

    print("[4] Sending validate_email('invalid') — expecting FAILURE...")
    try:
        r4 = validate_email.delay("invalid")
        r4.get(timeout=30)
    except Exception as exc:
        print(f"    Expected error: {type(exc).__name__}: {exc}\n")

    print("[5] Sending process_order('ORD-1', -50) — expecting FAILURE...")
    try:
        r5 = process_order.delay("ORD-1", -50)
        r5.get(timeout=30)
    except Exception as exc:
        print(f"    Expected error: {type(exc).__name__}: {exc}\n")

    print("=== Success/failure signals demo complete ===")
    print("NOTE: Check worker logs for [SUCCESS ALERT] and [FAILURE ALERT] messages.")
    print("      In production, route alerts to Slack, PagerDuty, email, etc.")
