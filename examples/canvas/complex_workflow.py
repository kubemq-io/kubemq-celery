"""Complex Workflow (ETL Pipeline) — KubeMQ Celery Transport.

Demonstrates:
- Combining chain + group + chord for a realistic ETL pipeline
- Extract: fetch data from multiple sources in parallel (group)
- Transform: process each dataset (chord callback merges)
- Load: write the combined result (final chain step)

Usage:
    # Start a worker:
    celery -A examples.canvas.complex_workflow worker --loglevel=info

    # Run the example:
    python examples/canvas/complex_workflow.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
"""

from __future__ import annotations

import os
import time

from celery import Celery, chain, chord, group

import kubemq_celery  # noqa: F401 — registers kubemq:// transport

app = Celery("complex_workflow_example")
app.config_from_object(
    {
        "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
        "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
        "result_expires": 3600,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }
)


# --- Extract phase tasks ---


@app.task
def fetch_sales_data(region: str) -> dict:
    """Simulate fetching sales data from a regional database."""
    time.sleep(0.3)
    data = {
        "us": {"region": "us", "revenue": 50000, "orders": 120},
        "eu": {"region": "eu", "revenue": 35000, "orders": 85},
        "apac": {"region": "apac", "revenue": 28000, "orders": 65},
    }
    return data.get(region, {"region": region, "revenue": 0, "orders": 0})


# --- Transform phase tasks ---


@app.task
def merge_datasets(datasets: list[dict]) -> dict:
    """Merge extracted datasets into a single summary."""
    total_revenue = sum(d["revenue"] for d in datasets)
    total_orders = sum(d["orders"] for d in datasets)
    return {
        "regions": [d["region"] for d in datasets],
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "datasets": datasets,
    }


@app.task
def enrich_summary(summary: dict) -> dict:
    """Add computed metrics to the merged summary."""
    summary["avg_order_value"] = round(
        summary["total_revenue"] / max(summary["total_orders"], 1), 2
    )
    summary["region_count"] = len(summary["regions"])
    return summary


# --- Load phase tasks ---


@app.task
def save_report(report: dict) -> dict:
    """Simulate saving the final report to a data warehouse."""
    time.sleep(0.2)
    return {
        "status": "saved",
        "region_count": report["region_count"],
        "total_revenue": report["total_revenue"],
        "total_orders": report["total_orders"],
        "avg_order_value": report["avg_order_value"],
    }


if __name__ == "__main__":
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    print("Complex ETL Workflow Example")
    print("=" * 40)
    print("\nPipeline:")
    print("  1. Extract: fetch data from US, EU, APAC in parallel")
    print("  2. Transform: merge datasets -> enrich with metrics")
    print("  3. Load: save final report")

    workflow = chain(
        # Extract: parallel data fetching from 3 regions
        chord(
            group(
                fetch_sales_data.s("us"),
                fetch_sales_data.s("eu"),
                fetch_sales_data.s("apac"),
            ),
            merge_datasets.s(),
        ),
        # Transform: enrich the merged data
        enrich_summary.s(),
        # Load: save to warehouse
        save_report.s(),
    )

    print("\nRunning pipeline...")
    result = workflow.apply_async()
    report = result.get(timeout=30)

    print(f"\nFinal report: {report}")
    assert report["status"] == "saved"
    assert report["region_count"] == 3
    assert report["total_revenue"] == 113000
    assert report["total_orders"] == 270

    print("\nDone!")
