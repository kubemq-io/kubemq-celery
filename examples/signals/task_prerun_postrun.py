"""Task Prerun/Postrun Signals — KubeMQ Celery Transport.

Demonstrates:
- task_prerun signal for capturing task start time
- task_postrun signal for capturing task end time and duration
- Signal-based timing instrumentation without modifying task code
- Signal handler registration via @receiver decorator

Usage:
    celery -A examples.signals.task_prerun_postrun worker --loglevel=info
    python examples/signals/task_prerun_postrun.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time
from typing import Any

from celery import Celery
from celery.signals import task_postrun, task_prerun

import kubemq_celery  # noqa: F401

app = Celery(
    "task_prerun_postrun",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600

# Store timing data (in production, use a proper metrics store)
_task_timings: dict[str, float] = {}


@task_prerun.connect
def on_task_prerun(sender: Any = None, task_id: str = "", task: Any = None, **kwargs: Any) -> None:
    """Called just before a task is executed by the worker."""
    _task_timings[task_id] = time.monotonic()
    print(f"[prerun]  Task {task.name} ({task_id[:8]}...) starting")


@task_postrun.connect
def on_task_postrun(
    sender: Any = None,
    task_id: str = "",
    task: Any = None,
    retval: Any = None,
    state: str = "",
    **kwargs: Any,
) -> None:
    """Called after a task has been executed by the worker."""
    start_time = _task_timings.pop(task_id, None)
    duration = (time.monotonic() - start_time) if start_time else 0.0
    print(
        f"[postrun] Task {task.name} ({task_id[:8]}...) finished in {duration:.3f}s — state={state}"
    )


@app.task
def fast_task(x: int, y: int) -> int:
    """A quick computation."""
    return x + y


@app.task
def slow_task(seconds: float) -> str:
    """A task that takes a specified number of seconds."""
    time.sleep(seconds)
    return f"completed after {seconds}s"


@app.task
def failing_task() -> None:
    """A task that raises an error — postrun still fires."""
    raise ValueError("intentional failure for signal demo")


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Task Prerun/Postrun Signals — KubeMQ Celery Transport ===\n")
    print("Signal handlers print timing info on the WORKER side.")
    print("Start a worker to see [prerun] and [postrun] messages.\n")

    print("[1] Sending fast_task(3, 4)...")
    r1 = fast_task.delay(3, 4)
    print(f"    Task ID: {r1.id}")
    print(f"    Result:  {r1.get(timeout=30)}\n")

    print("[2] Sending slow_task(2.0)...")
    r2 = slow_task.delay(2.0)
    print(f"    Task ID: {r2.id}")
    print(f"    Result:  {r2.get(timeout=30)}\n")

    print("[3] Sending failing_task (postrun fires with state=FAILURE)...")
    try:
        r3 = failing_task.delay()
        print(f"    Task ID: {r3.id}")
        r3.get(timeout=30)
    except Exception as exc:
        print(f"    Expected error: {exc}\n")

    print("=== Prerun/postrun signals demo complete ===")
    print("NOTE: Signals fire on the worker process. Check worker logs for")
    print("      [prerun] and [postrun] timing messages.")
