"""Pickle Serialization — KubeMQ Celery Transport.

Demonstrates:
- Pickle serializer for complex Python objects
- task_serializer='pickle' with accept_content=['pickle']
- Sending datetime, set, custom class instances
- SECURITY WARNING: pickle can deserialize arbitrary code

Usage:
    celery -A examples.serialization.pickle_serializer worker --loglevel=info
    python examples/serialization/pickle_serializer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed

Security Warning:
    Pickle deserialization can execute arbitrary code. Only use pickle
    when you fully trust the message source. Never accept pickle from
    untrusted producers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "pickle_serializer",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="pickle",
    result_serializer="pickle",
    accept_content=["pickle"],
    result_expires=3600,
)


@dataclass
class ProcessingResult:
    """Custom dataclass that requires pickle for serialization."""

    job_id: str
    timestamp: datetime
    values: set[int]
    metadata: dict


@app.task
def process_complex_data(data: dict) -> ProcessingResult:
    """Process data and return a complex Python object."""
    result = ProcessingResult(
        job_id=data.get("job_id", "unknown"),
        timestamp=datetime.now(timezone.utc),
        values={i * 2 for i in range(data.get("count", 5))},
        metadata={"source": "pickle_example", "version": 1},
    )
    print(f"[process_complex_data] Created result: {result}")
    return result


@app.task
def echo_datetime(dt: datetime) -> datetime:
    """Round-trip a datetime object (not JSON-serializable)."""
    print(f"[echo_datetime] Received: {dt} (type={type(dt).__name__})")
    return dt


@app.task
def echo_set(values: set) -> set:
    """Round-trip a Python set (not JSON-serializable)."""
    print(f"[echo_set] Received: {values} (type={type(values).__name__})")
    return values


@app.task
def merge_sets(set_a: set, set_b: set) -> set:
    """Merge two sets and return the union."""
    result = set_a | set_b
    print(f"[merge_sets] {set_a} | {set_b} = {result}")
    return result


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Pickle Serialization Example ===")
    print("WARNING: Pickle can deserialize arbitrary code.")
    print("         Only use with trusted message sources.\n")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Serializer: {app.conf.task_serializer}")

    print("\n--- Complex object round-trip ---")
    result = process_complex_data.delay({"job_id": "test-001", "count": 5})
    value = result.get(timeout=10)
    print(f"Result type: {type(value).__name__}")
    print(f"Result: {value}")
    assert isinstance(value, ProcessingResult)
    assert value.job_id == "test-001"
    assert len(value.values) == 5

    print("\n--- Datetime round-trip ---")
    now = datetime.now(timezone.utc)
    result = echo_datetime.delay(now)
    value = result.get(timeout=10)
    print(f"Sent:     {now}")
    print(f"Received: {value}")
    assert isinstance(value, datetime)

    print("\n--- Set round-trip ---")
    test_set = {1, 2, 3, 4, 5}
    result = echo_set.delay(test_set)
    value = result.get(timeout=10)
    print(f"Sent:     {test_set}")
    print(f"Received: {value}")
    assert value == test_set

    print("\n--- Set merge ---")
    result = merge_sets.delay({1, 2, 3}, {3, 4, 5})
    value = result.get(timeout=10)
    print(f"Merged: {value}")
    assert value == {1, 2, 3, 4, 5}

    print("\nAll pickle serialization tests passed!")
