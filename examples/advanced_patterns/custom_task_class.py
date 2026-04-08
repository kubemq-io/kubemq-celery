"""Custom Task Class — KubeMQ Celery Transport.

Demonstrates:
- Extending celery.Task with before/after hooks
- Custom __call__ method for wrapping task execution
- Timing, logging, and metrics collection via base class
- Task class composition patterns

Usage:
    celery -A examples.advanced_patterns.custom_task_class worker --loglevel=info
    python examples/advanced_patterns/custom_task_class.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time
from collections import defaultdict

from celery import Celery, Task

import kubemq_celery  # noqa: F401

app = Celery(
    "custom_task_class",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)

_metrics: dict[str, list[float]] = defaultdict(list)


class InstrumentedTask(Task):
    """Task class that instruments execution with timing and metrics."""

    abstract = True

    def __call__(self, *args, **kwargs):
        task_name = self.name
        print(f"[InstrumentedTask] >>> Starting {task_name}")
        start = time.monotonic()

        try:
            result = super().__call__(*args, **kwargs)
            elapsed = time.monotonic() - start
            _metrics[task_name].append(elapsed)
            print(f"[InstrumentedTask] <<< {task_name} completed in {elapsed:.3f}s")
            return result
        except Exception as exc:
            elapsed = time.monotonic() - start
            print(f"[InstrumentedTask] !!! {task_name} failed after {elapsed:.3f}s: {exc}")
            raise

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(f"[InstrumentedTask] FAILURE {self.name}: {exc}")

    def on_success(self, retval, task_id, args, kwargs):
        print(f"[InstrumentedTask] SUCCESS {self.name}")


class ValidatedTask(Task):
    """Task class that validates inputs before execution."""

    abstract = True
    required_keys: list[str] = []

    def __call__(self, *args, **kwargs):
        if args and isinstance(args[0], dict) and self.required_keys:
            data = args[0]
            missing = [k for k in self.required_keys if k not in data]
            if missing:
                raise ValueError(f"Missing required keys: {missing}")
        for v in kwargs.values():
            if isinstance(v, dict) and self.required_keys:
                missing = [k for k in self.required_keys if k not in v]
                if missing:
                    raise ValueError(f"Missing required keys in kwargs payload: {missing}")
        return super().__call__(*args, **kwargs)


class InstrumentedValidatedTask(InstrumentedTask, ValidatedTask):
    """Combined task class: validation + instrumentation."""

    abstract = True


@app.task(base=InstrumentedTask)
def compute(x: int, y: int) -> dict:
    """A simple compute task with instrumentation."""
    time.sleep(0.2)
    return {"sum": x + y, "product": x * y}


@app.task(base=InstrumentedTask)
def slow_task(duration: float) -> dict:
    """A task that sleeps for the given duration."""
    time.sleep(duration)
    return {"slept_for": duration}


class ProcessDataTask(ValidatedTask):
    """Validated task requiring specific keys."""

    name = "custom_task_class.process_data"
    required_keys = ["id", "value"]


@app.task(base=ProcessDataTask)
def process_data(data: dict) -> dict:
    """Process data with input validation via custom task class."""
    return {
        "id": data["id"],
        "processed_value": data["value"] * 2,
        "status": "ok",
    }


def get_metrics_summary() -> dict:
    """Return a summary of collected timing metrics."""
    summary = {}
    for task_name, timings in _metrics.items():
        if timings:
            summary[task_name] = {
                "count": len(timings),
                "mean_ms": round(sum(timings) / len(timings) * 1000, 1),
                "min_ms": round(min(timings) * 1000, 1),
                "max_ms": round(max(timings) * 1000, 1),
            }
    return summary


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Custom Task Class Example ===")
    print(f"Broker: {app.conf.broker_url}")

    print("\n--- Instrumented task ---")
    result = compute.delay(3, 4)
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Slow instrumented task ---")
    result = slow_task.delay(0.5)
    value = result.get(timeout=10)
    print(f"Result: {value}")

    print("\n--- Validated task (valid input) ---")
    result = process_data.delay({"id": "item-1", "value": 42})
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value["processed_value"] == 84

    print("\n--- Validated task (invalid input) ---")
    try:
        result = process_data.delay({"value": 42})
        result.get(timeout=10)
        print("ERROR: Expected validation error!")
    except Exception as e:
        print(f"Caught validation error: {e}")

    print("\n--- Metrics summary ---")
    summary = get_metrics_summary()
    for task_name, stats in summary.items():
        print(f"  {task_name}: {stats}")

    print("\nAll custom task class examples completed!")
