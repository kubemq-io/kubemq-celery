"""Custom Serializer — KubeMQ Celery Transport.

Demonstrates:
- Registering a custom serializer with kombu.serialization.register()
- Using orjson (or stdlib json fallback) for faster JSON encoding
- Custom content type and encoder/decoder functions
- Applying the custom serializer to tasks

Usage:
    celery -A examples.serialization.custom_serializer worker --loglevel=info
    python examples/serialization/custom_serializer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - Optional: orjson package (pip install orjson) for best performance
"""

from __future__ import annotations

import json
import os

from celery import Celery
from kombu.serialization import register

import kubemq_celery  # noqa: F401

try:
    import orjson

    def _orjson_dumps(obj: object) -> bytes:
        return orjson.dumps(obj)

    def _orjson_loads(data: bytes | str) -> object:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    SERIALIZER_NAME = "orjson"
    CONTENT_TYPE = "application/x-orjson"
    print("[custom_serializer] Using orjson for fast JSON encoding")
except ImportError:

    def _orjson_dumps(obj: object) -> bytes:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def _orjson_loads(data: bytes | str) -> object:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    SERIALIZER_NAME = "orjson"
    CONTENT_TYPE = "application/x-orjson"
    print("[custom_serializer] orjson not installed, falling back to stdlib json")

register(
    SERIALIZER_NAME,
    _orjson_dumps,
    _orjson_loads,
    content_type=CONTENT_TYPE,
    content_encoding="utf-8",
)

app = Celery(
    "custom_serializer",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer=SERIALIZER_NAME,
    result_serializer=SERIALIZER_NAME,
    accept_content=[SERIALIZER_NAME, "json"],
    result_expires=3600,
)


@app.task
def process_event(event: dict) -> dict:
    """Process an event using the custom serializer."""
    result = {
        "event_type": event.get("type", "unknown"),
        "payload_keys": list(event.get("payload", {}).keys()),
        "processed": True,
    }
    print(f"[process_event] {event.get('type')} -> {result}")
    return result


@app.task
def echo_data(data: dict) -> dict:
    """Echo data to verify custom serializer round-trip."""
    print(f"[echo_data] Received {len(data)} keys")
    return data


@app.task
def transform_batch(records: list[dict]) -> list[dict]:
    """Transform a batch of records."""
    result = []
    for record in records:
        transformed = {k.upper(): v for k, v in record.items()}
        result.append(transformed)
    print(f"[transform_batch] Transformed {len(result)} records")
    return result


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Custom Serializer Example ===")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Serializer: {SERIALIZER_NAME} ({CONTENT_TYPE})")

    print("\n--- Process event ---")
    event = {
        "type": "user.signup",
        "payload": {"user_id": "u-123", "email": "alice@example.com"},
        "timestamp": 1700000000,
    }
    result = process_event.delay(event)
    value = result.get(timeout=10)
    print(f"Result: {value}")
    assert value["event_type"] == "user.signup"
    assert value["processed"] is True

    print("\n--- Echo round-trip ---")
    payload = {
        "string": "hello",
        "integer": 42,
        "float": 3.14,
        "list": [1, 2, 3],
        "nested": {"a": 1, "b": 2},
        "null": None,
        "bool": True,
    }
    result = echo_data.delay(payload)
    value = result.get(timeout=10)
    print(f"Echoed: {value}")
    assert value["string"] == "hello"
    assert value["integer"] == 42

    print("\n--- Batch transform ---")
    records = [
        {"name": "alice", "role": "admin"},
        {"name": "bob", "role": "user"},
    ]
    result = transform_batch.delay(records)
    value = result.get(timeout=10)
    print(f"Transformed: {value}")
    assert value[0]["NAME"] == "alice"

    print("\nAll custom serializer tests passed!")
