"""MessagePack Serialization — KubeMQ Celery Transport.

Demonstrates:
- MessagePack serializer for compact binary encoding
- task_serializer='msgpack' with accept_content=['msgpack']
- Smaller payload sizes compared to JSON
- Faster serialization/deserialization for numeric-heavy data

Usage:
    celery -A examples.serialization.msgpack_serializer worker --loglevel=info
    python examples/serialization/msgpack_serializer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - msgpack package: pip install msgpack
"""

from __future__ import annotations

import json
import os
import sys

from celery import Celery

import kubemq_celery  # noqa: F401

try:
    import msgpack
except ImportError:
    msgpack = None  # type: ignore[assignment]

app = Celery(
    "msgpack_serializer",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

if msgpack is not None:
    app.conf.update(
        task_serializer="msgpack",
        result_serializer="msgpack",
        accept_content=["msgpack"],
        result_expires=3600,
    )
else:
    app.conf.update(result_expires=3600)


@app.task
def process_numeric_data(matrix: list[list[float]]) -> dict:
    """Process a numeric matrix — msgpack excels at compact numeric encoding."""
    rows = len(matrix)
    cols = len(matrix[0]) if matrix else 0
    flat = [v for row in matrix for v in row]
    total = sum(flat)
    result = {
        "rows": rows,
        "cols": cols,
        "sum": total,
        "mean": total / len(flat) if flat else 0,
        "min": min(flat) if flat else 0,
        "max": max(flat) if flat else 0,
    }
    print(f"[process_numeric_data] {rows}x{cols} matrix -> {result}")
    return result


@app.task
def echo_payload(data: dict) -> dict:
    """Echo payload to verify msgpack round-trip."""
    print(f"[echo_payload] keys={list(data.keys())}")
    return data


@app.task
def batch_sum(batches: list[list[int]]) -> list[int]:
    """Sum each batch in a list of batches."""
    result = [sum(batch) for batch in batches]
    print(f"[batch_sum] {len(batches)} batches -> {result}")
    return result


def compare_sizes(data: dict) -> None:
    """Compare JSON vs msgpack serialized sizes."""
    json_bytes = len(json.dumps(data).encode("utf-8"))
    msgpack_bytes = len(msgpack.packb(data))  # type: ignore[union-attr]
    savings = (1 - msgpack_bytes / json_bytes) * 100 if json_bytes else 0
    print(f"  JSON:    {json_bytes:>6} bytes")
    print(f"  msgpack: {msgpack_bytes:>6} bytes")
    print(f"  Savings: {savings:.1f}%")


if __name__ == "__main__":
    print("=== MessagePack Serialization Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Serializer: {app.conf.task_serializer}")

    if msgpack is None:
        print("\nNOTE: msgpack package not installed.")
        print("      Install with: pip install msgpack")
        print("\nSkipping live tasks (msgpack not available).")
        print("\nDone!")
        sys.exit(0)

    app.conf.update(task_always_eager=True, task_eager_propagates=True)

    print("\n--- Size comparison ---")
    sample_data = {
        "sensors": [{"id": i, "value": i * 1.5, "status": "ok"} for i in range(20)],
        "timestamp": 1700000000,
        "batch_id": "batch-001",
    }
    compare_sizes(sample_data)

    print("\n--- Numeric matrix processing ---")
    matrix = [[float(i + j) for j in range(5)] for i in range(4)]
    result = process_numeric_data.delay(matrix)
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value["rows"] == 4
    assert value["cols"] == 5

    print("\n--- Payload echo ---")
    payload = {"integers": [1, 2, 3], "floats": [1.1, 2.2], "text": "hello"}
    result = echo_payload.delay(payload)
    value = result.get(timeout=10)
    print(f"Echoed: {value}")

    print("\n--- Batch sum ---")
    batches = [[1, 2, 3], [10, 20], [100, 200, 300, 400]]
    result = batch_sum.delay(batches)
    value = result.get(timeout=10)
    print(f"Sums: {value}")
    assert value == [6, 30, 1000]

    print("\nAll msgpack serialization tests passed!")
