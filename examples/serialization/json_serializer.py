"""JSON Serialization — KubeMQ Celery Transport.

Demonstrates:
- Default JSON serializer configuration
- task_serializer='json' with accept_content=['json']
- Sending and receiving JSON-serializable data types
- Verifying round-trip fidelity for dicts, lists, strings, numbers

Usage:
    celery -A examples.serialization.json_serializer worker --loglevel=info
    python examples/serialization/json_serializer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "json_serializer",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


@app.task
def echo_dict(data: dict) -> dict:
    """Return the input dict unchanged to verify JSON round-trip."""
    print(f"[echo_dict] Received: {data}")
    return data


@app.task
def echo_list(data: list) -> list:
    """Return the input list unchanged."""
    print(f"[echo_list] Received: {data}")
    return data


@app.task
def transform_record(record: dict) -> dict:
    """Transform a record: uppercase name, double the value."""
    result = {
        "name": record.get("name", "").upper(),
        "value": record.get("value", 0) * 2,
        "tags": record.get("tags", []),
        "processed": True,
    }
    print(f"[transform_record] {record} -> {result}")
    return result


@app.task
def aggregate_values(items: list[dict]) -> dict:
    """Aggregate numeric values from a list of dicts."""
    total = sum(item.get("value", 0) for item in items)
    count = len(items)
    result = {"total": total, "count": count, "average": total / count if count else 0}
    print(f"[aggregate_values] {count} items -> {result}")
    return result


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== JSON Serialization Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Serializer: {app.conf.task_serializer}")
    print(f"Accept: {app.conf.accept_content}")

    print("\n--- Round-trip dict ---")
    payload = {"name": "Alice", "score": 95.5, "tags": ["admin", "active"]}
    result = echo_dict.delay(payload)
    value = result.get(timeout=10)
    print(f"Sent:     {payload}")
    print(f"Received: {value}")
    assert value == payload, f"Mismatch: {value}"

    print("\n--- Round-trip list ---")
    payload_list = [1, "two", 3.0, None, True, {"nested": "dict"}]
    result = echo_list.delay(payload_list)
    value = result.get(timeout=10)
    print(f"Sent:     {payload_list}")
    print(f"Received: {value}")

    print("\n--- Transform record ---")
    record = {"name": "sensor-a", "value": 42, "tags": ["temperature"]}
    result = transform_record.delay(record)
    value = result.get(timeout=10)
    print(f"Transformed: {value}")
    assert value["name"] == "SENSOR-A"
    assert value["value"] == 84

    print("\n--- Aggregate values ---")
    items = [{"value": 10}, {"value": 20}, {"value": 30}]
    result = aggregate_values.delay(items)
    value = result.get(timeout=10)
    print(f"Aggregated: {value}")
    assert value["total"] == 60
    assert value["average"] == 20.0

    print("\nAll JSON serialization tests passed!")
