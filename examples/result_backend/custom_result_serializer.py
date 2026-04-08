"""Custom Result Serializer — KubeMQ Celery Transport.

Demonstrates:
- Configuring result_serializer='json' (default, human-readable)
- Configuring result_serializer='pickle' (Python objects, not portable)
- Configuring result_serializer='msgpack' (compact binary, requires msgpack)
- accept_content for controlling accepted serialization formats
- Serializer impact on result payload size and compatibility

Usage:
    celery -A examples.result_backend.custom_result_serializer worker --loglevel=info
    python examples/result_backend/custom_result_serializer.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - msgpack (optional, for msgpack serializer)
"""

from __future__ import annotations

import os
import sys

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "custom_result_serializer",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

# Default: JSON serializer for results (human-readable, cross-language)
app.conf.update(
    result_serializer="json",
    task_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


@app.task
def process_data(data: dict) -> dict:
    """Process data and return a structured result."""
    return {
        "input_keys": list(data.keys()),
        "total_values": sum(v for v in data.values() if isinstance(v, (int, float))),
        "processed": True,
    }


@app.task
def compute_stats(numbers: list[float]) -> dict:
    """Compute basic statistics."""
    if not numbers:
        return {"error": "empty input"}
    return {
        "count": len(numbers),
        "sum": sum(numbers),
        "mean": sum(numbers) / len(numbers),
        "min": min(numbers),
        "max": max(numbers),
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Custom Result Serializer — KubeMQ Celery Transport ===\n")

    # Show available serializers
    serializers = {
        "json": {
            "pros": "Human-readable, cross-language, safe (no code execution)",
            "cons": "Larger payload, limited Python types (no datetime, set, bytes)",
            "config": 'result_serializer="json", accept_content=["json"]',
        },
        "pickle": {
            "pros": "Supports all Python types (datetime, set, custom classes)",
            "cons": "Security risk (arbitrary code execution), Python-only",
            "config": 'result_serializer="pickle", accept_content=["pickle"]',
        },
        "msgpack": {
            "pros": "Compact binary format, fast, cross-language",
            "cons": "Requires msgpack package, limited types (similar to JSON)",
            "config": 'result_serializer="msgpack", accept_content=["msgpack"]',
        },
    }

    print("Available result serializers:\n")
    for name, info in serializers.items():
        print(f"  [{name}]")
        print(f"    Pros:   {info['pros']}")
        print(f"    Cons:   {info['cons']}")
        print(f"    Config: {info['config']}")
        print()

    # Current config
    print(f"[config] result_serializer = {app.conf.result_serializer}")
    print(f"[config] task_serializer   = {app.conf.task_serializer}")
    print(f"[config] accept_content    = {app.conf.accept_content}\n")

    # Send task with JSON serializer (default)
    print("[1] Sending process_data with JSON serializer...")
    result = process_data.delay({"alpha": 10, "beta": 20, "gamma": 30})
    print(f"    Task ID: {result.id}")
    value = result.get(timeout=30)
    print(f"    Result:  {value}")
    print(f"    Type:    {type(value).__name__}\n")

    # Send task returning numeric results
    print("[2] Sending compute_stats with JSON serializer...")
    result2 = compute_stats.delay([1.5, 2.7, 3.14, 4.0, 5.5])
    print(f"    Task ID: {result2.id}")
    stats = result2.get(timeout=30)
    print(f"    Stats:   {stats}\n")

    # Check if msgpack is available
    try:
        import msgpack  # noqa: F401

        msgpack_available = True
    except ImportError:
        msgpack_available = False

    print("--- Serializer switching notes ---")
    print("To switch serializer, update app.conf before starting workers:")
    print()
    print("  # For pickle (Python-only, supports all types):")
    print('  app.conf.update(result_serializer="pickle", accept_content=["pickle", "json"])')
    print()
    if msgpack_available:
        print("  # For msgpack (compact binary, installed):")
    else:
        print("  # For msgpack (requires: pip install msgpack):")
    print('  app.conf.update(result_serializer="msgpack", accept_content=["msgpack", "json"])')
    print()
    print("IMPORTANT: Workers and clients must use matching accept_content settings.")
    print(f"           msgpack available: {msgpack_available}")
    print(f"           Python version:    {sys.version.split()[0]}")
    print("\n=== Serializer demo complete ===")
