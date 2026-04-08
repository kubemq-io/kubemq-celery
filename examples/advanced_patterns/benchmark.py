"""Performance Benchmark — KubeMQ Celery Transport.

Demonstrates:
- Task dispatch rate measurement (messages/sec)
- Round-trip latency percentiles (p50, p95, p99)
- Configurable number of tasks and broker URL via argparse CLI
- Structured results output

Usage:
    # Start a worker first:
    celery -A examples.advanced_patterns.benchmark worker --loglevel=info

    # Run benchmark:
    python examples/advanced_patterns/benchmark.py
    python examples/advanced_patterns/benchmark.py --tasks 500 --broker kubemq://myhost:50000

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - A running Celery worker for round-trip tests
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("benchmark")


def configure_app(broker_url: str) -> None:
    """Configure the Celery app with the given broker URL."""
    app.config_from_object(
        {
            "broker_url": broker_url,
            "result_backend": os.environ.get("CELERY_RESULT_BACKEND", broker_url),
            "result_expires": 3600,
            "task_serializer": "json",
            "result_serializer": "json",
            "accept_content": ["json"],
        }
    )


@app.task
def noop_task(payload: str = "") -> str:
    """Minimal task for dispatch overhead measurement."""
    return payload


@app.task
def echo_task(data: dict) -> dict:
    """Echo task for round-trip latency measurement."""
    return data


def benchmark_dispatch_rate(num_tasks: int) -> dict:
    """Measure fire-and-forget dispatch rate."""
    print(f"\n--- Dispatch Rate ({num_tasks} tasks) ---")

    start = time.monotonic()
    for i in range(num_tasks):
        noop_task.delay(f"payload-{i}")
    elapsed = time.monotonic() - start

    rate = num_tasks / elapsed if elapsed > 0 else 0
    print(f"  Dispatched {num_tasks} tasks in {elapsed:.2f}s")
    print(f"  Rate: {rate:.0f} tasks/sec")
    return {
        "tasks": num_tasks,
        "elapsed_s": round(elapsed, 3),
        "rate_per_sec": round(rate, 1),
    }


def benchmark_round_trip(num_tasks: int) -> dict:
    """Measure task round-trip latency (dispatch -> result)."""
    print(f"\n--- Round-Trip Latency ({num_tasks} tasks) ---")

    latencies: list[float] = []
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
    stdev = statistics.stdev(latencies) if len(latencies) > 1 else 0

    print(f"  Completed {len(latencies)}/{num_tasks} tasks")
    print(f"  Mean:   {mean * 1000:.1f} ms")
    print(f"  Stdev:  {stdev * 1000:.1f} ms")
    print(f"  p50:    {p50 * 1000:.1f} ms")
    print(f"  p95:    {p95 * 1000:.1f} ms")
    print(f"  p99:    {p99 * 1000:.1f} ms")
    print(f"  Min:    {latencies[0] * 1000:.1f} ms")
    print(f"  Max:    {latencies[-1] * 1000:.1f} ms")

    return {
        "completed": len(latencies),
        "total": num_tasks,
        "mean_ms": round(mean * 1000, 1),
        "stdev_ms": round(stdev * 1000, 1),
        "p50_ms": round(p50 * 1000, 1),
        "p95_ms": round(p95 * 1000, 1),
        "p99_ms": round(p99 * 1000, 1),
        "min_ms": round(latencies[0] * 1000, 1),
        "max_ms": round(latencies[-1] * 1000, 1),
    }


def benchmark_burst(num_tasks: int) -> dict:
    """Measure burst dispatch + collect pattern."""
    print(f"\n--- Burst Pattern ({num_tasks} tasks) ---")

    start = time.monotonic()
    pending = []
    for i in range(num_tasks):
        pending.append(echo_task.delay({"seq": i}))
    dispatch_elapsed = time.monotonic() - start

    collected = 0
    failures: list[str] = []
    for result in pending:
        try:
            result.get(timeout=30)
            collected += 1
        except Exception as exc:
            failures.append(str(exc))
    total_elapsed = time.monotonic() - start

    print(f"  Dispatch: {dispatch_elapsed:.2f}s ({num_tasks / dispatch_elapsed:.0f}/sec)")
    print(f"  Total:    {total_elapsed:.2f}s")
    print(f"  Collected: {collected}/{num_tasks}")
    if failures:
        print(f"  Failures: {len(failures)}")
        for reason in failures[:5]:
            print(f"    - {reason}")
        if len(failures) > 5:
            print(f"    ... and {len(failures) - 5} more")

    return {
        "tasks": num_tasks,
        "dispatch_s": round(dispatch_elapsed, 3),
        "total_s": round(total_elapsed, 3),
        "dispatch_rate": round(num_tasks / dispatch_elapsed, 1),
        "collected": collected,
    }


def main():
    parser = argparse.ArgumentParser(description="KubeMQ Celery Transport Benchmark")
    parser.add_argument(
        "--broker",
        default=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        help="Broker URL (default: kubemq://localhost:50000)",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=100,
        help="Number of tasks per benchmark (default: 100)",
    )
    parser.add_argument(
        "--skip-roundtrip",
        action="store_true",
        help="Skip round-trip tests (no worker needed)",
    )
    parser.add_argument(
        "--eager",
        action="store_true",
        help="Run tasks eagerly in-process (no worker needed)",
    )
    args = parser.parse_args()

    configure_app(args.broker)
    if args.eager:
        app.conf.update(task_always_eager=True, task_eager_propagates=True)
    else:
        print("=== KubeMQ Celery Transport Benchmark ===")
        print()
        print("This benchmark requires a running Celery worker.")
        print("  1. Start a worker:")
        print(
            "     celery -A examples.advanced_patterns.benchmark worker --pool=solo --loglevel=info"
        )
        print("  2. Re-run with --eager for in-process mode:")
        print("     python examples/advanced_patterns/benchmark.py --eager")
        print()
        print("=== Benchmark demo complete ===")
        sys.exit(0)
    print("=== KubeMQ Celery Transport Benchmark (eager mode) ===")
    print(f"Broker: {args.broker}")
    print(f"Tasks per test: {args.tasks}")

    results: dict = {}
    results["dispatch"] = benchmark_dispatch_rate(args.tasks)

    if not args.skip_roundtrip:
        rt_count = min(args.tasks, 50)
        results["round_trip"] = benchmark_round_trip(rt_count)
        results["burst"] = benchmark_burst(min(args.tasks, 50))

    print("\n=== Summary ===")
    print(f"Dispatch rate: {results['dispatch']['rate_per_sec']} tasks/sec")
    if "round_trip" in results and "error" not in results["round_trip"]:
        rt = results["round_trip"]
        print(f"Round-trip p50={rt['p50_ms']}ms p95={rt['p95_ms']}ms p99={rt['p99_ms']}ms")
    if "burst" in results:
        burst = results["burst"]
        print(
            f"Burst: {burst['dispatch_rate']} dispatch/sec, "
            f"{burst['total_s']}s total for {burst['tasks']} tasks"
        )


if __name__ == "__main__":
    main()
