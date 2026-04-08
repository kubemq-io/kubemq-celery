"""Ignore Result (Fire-and-Forget) — KubeMQ Celery Transport.

Demonstrates:
- @app.task(ignore_result=True) for fire-and-forget tasks
- Skipping result storage to reduce KubeMQ channel usage
- Mixing ignore_result tasks with result-returning tasks
- Performance benefit of not storing results

Usage:
    celery -A examples.result_backend.ignore_result worker --loglevel=info
    python examples/result_backend/ignore_result.py

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
    "ignore_result",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task(ignore_result=True)
def send_notification(user_id: str, message: str) -> None:
    """Fire-and-forget notification — no result stored in KubeMQ.

    With ignore_result=True, no celery-result-{task_id} channel is created.
    The caller cannot retrieve the return value.
    """
    print(f"[worker] Notification sent to user {user_id}: {message}")


@app.task(ignore_result=True)
def log_event(event_type: str, payload: dict) -> None:
    """Fire-and-forget event logging — result discarded."""
    print(f"[worker] Event logged: type={event_type}, payload={payload}")


@app.task(ignore_result=True)
def cleanup_temp_files(directory: str) -> None:
    """Fire-and-forget cleanup — no result needed."""
    print(f"[worker] Cleanup initiated for directory: {directory}")


@app.task
def compute_with_result(x: int, y: int) -> int:
    """Regular task that stores its result for comparison."""
    return x + y


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Ignore Result (Fire-and-Forget) — KubeMQ Celery Transport ===\n")

    # Fire-and-forget tasks — no result channel created
    print("[1] Sending fire-and-forget tasks (ignore_result=True)...")
    r1 = send_notification.delay("user-42", "Your order has shipped!")
    print(f"    send_notification -> Task ID: {r1.id}")
    print(f"    r1.result = {r1.result}  (always None)")
    print(f"    r1.state  = {r1.state}   (always PENDING)\n")

    r2 = log_event.delay("page_view", {"url": "/dashboard", "user": "admin"})
    print(f"    log_event -> Task ID: {r2.id}")
    print(f"    No celery-result-{r2.id[:8]}... channel created\n")

    r3 = cleanup_temp_files.delay("/tmp/uploads")
    print(f"    cleanup_temp_files -> Task ID: {r3.id}")
    print("    Cannot call r3.get() — would block forever\n")

    # Regular task with result for comparison
    print("[2] Sending regular task (ignore_result=False)...")
    r4 = compute_with_result.delay(10, 20)
    print(f"    compute_with_result -> Task ID: {r4.id}")
    value = r4.get(timeout=30)
    print(f"    Result: {value}")
    print(f"    State:  {r4.state}")
    print(f"    Result stored on celery-result-{r4.id[:8]}... channel\n")

    # Batch fire-and-forget
    print("[3] Sending batch of 10 fire-and-forget notifications...")
    start = time.time()
    for i in range(10):
        send_notification.delay(f"user-{i}", f"Batch message #{i}")
    elapsed = time.time() - start
    print(f"    10 tasks dispatched in {elapsed:.3f}s")
    print("    No result channels created — reduced KubeMQ overhead\n")

    print("=== Fire-and-forget demo complete ===")
    print("NOTE: ignore_result=True skips result storage entirely.")
    print("      No celery-result-* KubeMQ queue channels are created.")
    print("      Use for tasks where the caller doesn't need the return value.")
