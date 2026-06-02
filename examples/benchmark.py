"""KubeMQ Celery Transport Benchmark.

A self-contained benchmark script for measuring KubeMQ transport performance.

Run:
    python examples/benchmark.py [--broker kubemq://localhost:50000] [--tasks 1000]

Measures:
    - Task dispatch rate (messages/sec)
    - Task round-trip latency (p50, p95, p99)

Requirements:
    - Running KubeMQ broker
    - kubemq-celery-transport installed
    - A Celery worker running: celery -A benchmark worker --loglevel=info
"""

from __future__ import annotations

import argparse
import os
import statistics
import time

import kubemq_celery  # noqa: F401
from celery import Celery

# --- Celery App Setup ---

app = Celery("benchmark")


def configure_app(broker_url: str) -> None:
    app.config_from_object(
        {
            "broker_url": broker_url,
            "result_backend": broker_url,
            "result_expires": 86400,
            "task_serializer": "json",
            "result_serializer": "json",
            "accept_content": ["json"],
        }
    )


@app.task
def noop_task(payload: str = "") -> str:
    """Minimal task for benchmarking dispatch/receive overhead."""
    return payload


@app.task
def echo_task(data: dict) -> dict:
    """Echo task for round-trip latency measurement."""
    return data


# --- Benchmark Functions ---


def benchmark_dispatch_rate(num_tasks: int) -> dict:
    """Measure task dispatch rate (messages/sec)."""
    print(f"\n--- Dispatch Rate Benchmark ({num_tasks} tasks) ---")

    start = time.monotonic()
    for i in range(num_tasks):
        noop_task.delay(f"payload-{i}")
    elapsed = time.monotonic() - start

    rate = num_tasks / elapsed if elapsed > 0 else 0
    print(f"Dispatched {num_tasks} tasks in {elapsed:.2f}s")
    print(f"Rate: {rate:.0f} tasks/sec")
    return {"tasks": num_tasks, "elapsed_s": round(elapsed, 3), "rate_per_sec": round(rate, 1)}


def benchmark_round_trip(num_tasks: int) -> dict:
    """Measure task round-trip latency (dispatch -> result)."""
    print(f"\n--- Round-Trip Latency Benchmark ({num_tasks} tasks) ---")

    latencies = []
    for i in range(num_tasks):
        start = time.monotonic()
        result = echo_task.delay({"seq": i, "ts": time.time()})
        try:
            result.get(timeout=30)
            latency = time.monotonic() - start
            latencies.append(latency)
        except Exception as exc:
            print(f"  Task {i} failed: {exc}")

    if not latencies:
        print("  No successful round-trips!")
        return {"error": "all tasks failed"}

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    mean = statistics.mean(latencies)

    print(f"Completed {len(latencies)}/{num_tasks} tasks")
    print(f"  Mean:  {mean * 1000:.1f} ms")
    print(f"  p50:   {p50 * 1000:.1f} ms")
    print(f"  p95:   {p95 * 1000:.1f} ms")
    print(f"  p99:   {p99 * 1000:.1f} ms")

    return {
        "completed": len(latencies),
        "total": num_tasks,
        "mean_ms": round(mean * 1000, 1),
        "p50_ms": round(p50 * 1000, 1),
        "p95_ms": round(p95 * 1000, 1),
        "p99_ms": round(p99 * 1000, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="KubeMQ Celery Transport Benchmark")
    parser.add_argument(
        "--broker",
        default=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        help="Broker URL (default: kubemq://localhost:50000)",
    )
    parser.add_argument(
        "--tasks", type=int, default=100, help="Number of tasks per benchmark (default: 100)"
    )
    args = parser.parse_args()

    configure_app(args.broker)
    print("KubeMQ Celery Transport Benchmark")
    print(f"Broker: {args.broker}")
    print(f"Tasks per test: {args.tasks}")

    results = {}
    results["dispatch"] = benchmark_dispatch_rate(args.tasks)
    results["round_trip"] = benchmark_round_trip(min(args.tasks, 50))

    print("\n=== Benchmark Summary ===")
    print(f"Dispatch rate: {results['dispatch'].get('rate_per_sec', 'N/A')} tasks/sec")
    rt = results["round_trip"]
    if "error" not in rt:
        print(
            f"Round-trip p50: {rt['p50_ms']} ms, "
            f"p95: {rt['p95_ms']} ms, "
            f"p99: {rt['p99_ms']} ms"
        )


if __name__ == "__main__":
    main()
