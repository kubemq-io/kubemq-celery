"""Dynamic Routing — KubeMQ Celery Transport.

Demonstrates:
- apply_async(queue='specific-queue') for per-call routing
- Overriding task_routes with explicit queue parameter
- Routing decisions at publish time based on runtime conditions
- KubeMQ queue channel creation on first use

Usage:
    celery -A examples.routing.dynamic_routing worker \
        --loglevel=info -Q default,urgent,batch,region-us,region-eu
    python examples/routing/dynamic_routing.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os

from celery import Celery
from celery.result import AsyncResult

import kubemq_celery  # noqa: F401

app = Celery(
    "dynamic_routing",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.result_expires = 3600


@app.task
def process_order(order_id: str, priority: str = "normal") -> dict:
    """Process an order — queue determined at publish time."""
    return {"order_id": order_id, "priority": priority, "status": "processed"}


@app.task
def sync_data(region: str, table: str) -> dict:
    """Sync data to a specific region — routed dynamically by region."""
    return {"region": region, "table": table, "status": "synced"}


@app.task
def run_job(job_type: str, payload: dict) -> dict:
    """Run a job — queue based on job type."""
    return {"job_type": job_type, "payload": payload, "status": "completed"}


def route_by_priority(order_id: str, priority: str) -> AsyncResult:
    """Route an order to the appropriate queue based on priority."""
    queue_map = {
        "urgent": "urgent",
        "high": "urgent",
        "normal": "default",
        "low": "batch",
    }
    if priority not in queue_map:
        print(f"  WARNING: unknown priority {priority!r}, falling back to 'default'")
    queue = queue_map.get(priority, "default")
    print(f"  Routing order {order_id} with priority={priority} to queue='{queue}'")
    return process_order.apply_async(
        args=(order_id, priority),
        queue=queue,
    )


def route_by_region(region: str, table: str) -> AsyncResult:
    """Route data sync to a region-specific queue."""
    known_regions = {"us", "eu", "ap", "sa"}
    if region.lower() not in known_regions:
        print(f"  WARNING: unknown region {region!r}, routing to 'region-{region.lower()}'")
    queue = f"region-{region.lower()}"
    print(f"  Routing sync({table}) to queue='{queue}'")
    return sync_data.apply_async(
        args=(region, table),
        queue=queue,
    )


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("=== Dynamic Routing — KubeMQ Celery Transport ===\n")

    print("Dynamic routing: specify queue at publish time via apply_async(queue=...)")
    print("KubeMQ creates queue channels on first use — no pre-creation needed.\n")

    # Route by priority
    print("[1] Routing orders by priority...")
    r1 = route_by_priority("ORD-001", "urgent")
    r2 = route_by_priority("ORD-002", "normal")
    r3 = route_by_priority("ORD-003", "low")
    print()

    # Route by region
    print("[2] Routing data sync by region...")
    r4 = route_by_region("US", "users")
    r5 = route_by_region("EU", "orders")
    print()

    # Direct queue specification
    print("[3] Direct queue specification...")
    r6 = run_job.apply_async(
        args=("import", {"file": "data.csv"}),
        queue="batch",
    )
    print("  run_job('import') -> queue='batch'")

    r7 = run_job.apply_async(
        args=("alert", {"level": "critical"}),
        queue="urgent",
    )
    print("  run_job('alert') -> queue='urgent'\n")

    # Collect results
    print("Waiting for results...\n")
    for label, r in [
        ("ORD-001 (urgent)", r1),
        ("ORD-002 (normal)", r2),
        ("ORD-003 (low)", r3),
        ("sync US/users", r4),
        ("sync EU/orders", r5),
        ("import job", r6),
        ("alert job", r7),
    ]:
        try:
            val = r.get(timeout=30)
            print(f"  {label}: {val}")
        except Exception as exc:
            print(f"  {label}: ERROR — {exc}")
    print()

    print("=== Dynamic routing demo complete ===")
    print("NOTE: Workers must consume the target queue (-Q flag).")
    print("      KubeMQ queue channels are created on first message.")
