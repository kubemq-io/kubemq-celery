"""Dedicated Workers — KubeMQ Celery Transport.

Demonstrates:
- Workers consuming specific queues with -Q flag
- Multiple worker instances for different workloads
- Queue isolation for resource-intensive tasks
- Worker naming with -n flag

Usage:
    celery -A examples.routing.dedicated_workers worker -Q high,default -n high@%h --loglevel=info
    celery -A examples.routing.dedicated_workers worker -Q low -n low@%h --loglevel=info
    python examples/routing/dedicated_workers.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery("dedicated_workers")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_default_queue": "default",
        "task_routes": {
            "examples.routing.dedicated_workers.gpu_task": {"queue": "gpu"},
            "examples.routing.dedicated_workers.memory_intensive": {"queue": "high-memory"},
            "examples.routing.dedicated_workers.quick_response": {"queue": "high"},
            "examples.routing.dedicated_workers.batch_process": {"queue": "low"},
        },
    }
)


@app.task
def gpu_task(model_name: str, data_size: int) -> dict:
    """GPU-intensive task — needs dedicated GPU worker."""
    time.sleep(0.5)
    return {"model": model_name, "data_size": data_size, "pid": os.getpid()}


@app.task
def memory_intensive(dataset: str) -> dict:
    """Memory-intensive task — needs high-memory worker."""
    time.sleep(0.3)
    return {"dataset": dataset, "pid": os.getpid()}


@app.task
def quick_response(query: str) -> dict:
    """Low-latency task — needs fast-response worker."""
    return {"query": query, "pid": os.getpid()}


@app.task
def batch_process(items: list[str]) -> dict:
    """Batch processing — can run on low-priority worker."""
    time.sleep(0.2)
    return {"items_count": len(items), "pid": os.getpid()}


@app.task
def general_task(data: str) -> dict:
    """General task — runs on default queue."""
    return {"data": data, "pid": os.getpid()}


if __name__ == "__main__":
    print("=== Dedicated Workers — KubeMQ Celery Transport ===\n")

    print("Dedicated worker deployment pattern:\n")
    workers = [
        {
            "name": "gpu@worker1",
            "queues": "gpu",
            "flags": "-c 1 -P solo",
            "purpose": "GPU tasks (single process, dedicated GPU)",
        },
        {
            "name": "highmem@worker2",
            "queues": "high-memory",
            "flags": "-c 2 --max-memory-per-child=500000",
            "purpose": "Memory-intensive tasks (memory limit per child)",
        },
        {
            "name": "fast@worker3",
            "queues": "high,default",
            "flags": "-c 8 -P threads",
            "purpose": "Fast-response tasks (high concurrency threads)",
        },
        {
            "name": "batch@worker4",
            "queues": "low",
            "flags": "-c 4 -P prefork",
            "purpose": "Batch processing (standard concurrency)",
        },
    ]

    for w in workers:
        cmd = (
            f"celery -A examples.routing.dedicated_workers worker "
            f"-n {w['name']} -Q {w['queues']} {w['flags']} --loglevel=info"
        )
        print(f"  Worker: {w['name']}")
        print(f"  Queues: {w['queues']}")
        print(f"  Purpose: {w['purpose']}")
        print(f"  Command: {cmd}")
        print()

    print("Task routing:")
    for task_name, route in app.conf.task_routes.items():
        print(f"  {task_name.split('.')[-1]:20s} -> queue='{route['queue']}'")
    print()

    print("--- Deployment benefits ---")
    print("  - Resource isolation: GPU tasks don't compete with CPU tasks")
    print("  - Independent scaling: scale workers per queue demand")
    print("  - Failure isolation: crashed GPU worker doesn't affect batch worker")
    print("  - Each KubeMQ queue channel acts as an independent buffer")
    print()
    print("=== Configuration demo complete ===")
