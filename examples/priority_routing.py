"""Multi-queue priority routing with KubeMQ.

Demonstrates:
- Multiple task queues (high, default, low priority)
- Task routing based on task name patterns
- Priority metadata in message tags
- Running workers for specific queues

Usage:
    # Start a worker for all queues:
    celery -A priority_routing worker --loglevel=info -Q high-priority,celery,low-priority

    # Or start separate workers per priority:
    celery -A priority_routing worker --loglevel=info -Q high-priority --concurrency=8
    celery -A priority_routing worker --loglevel=info -Q celery --concurrency=4
    celery -A priority_routing worker --loglevel=info -Q low-priority --concurrency=2

    # Send tasks (in another terminal):
    python priority_routing.py
"""

import kubemq_celery
from celery import Celery

app = Celery(
    "priority_routing",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

# Configure task routing: route tasks to queues based on name patterns
app.conf.update(
    task_routes={
        "priority_routing.process_payment": {"queue": "high-priority"},
        "priority_routing.send_notification": {"queue": "celery"},  # default queue
        "priority_routing.generate_report": {"queue": "low-priority"},
        "priority_routing.cleanup_old_data": {"queue": "low-priority"},
    },
    # Priority values stored in KubeMQ message tags (metadata, not server-enforced)
    task_default_priority=5,
    task_queue_max_priority=10,
    # Use prefetch=1 for fair distribution across workers
    worker_prefetch_multiplier=1,
)


@app.task(priority=9)
def process_payment(order_id: str, amount: float) -> dict:
    """High-priority: Process a payment immediately.

    Routed to 'high-priority' queue with priority=9 in message tags.
    """
    return {
        "order_id": order_id,
        "amount": amount,
        "status": "processed",
    }


@app.task(priority=5)
def send_notification(user_id: str, message: str) -> dict:
    """Default priority: Send a notification to a user.

    Routed to default 'celery' queue with priority=5 in message tags.
    """
    return {
        "user_id": user_id,
        "message": message,
        "status": "sent",
    }


@app.task(priority=1)
def generate_report(report_type: str, date_range: str) -> dict:
    """Low priority: Generate a batch report.

    Routed to 'low-priority' queue with priority=1 in message tags.
    """
    return {
        "report_type": report_type,
        "date_range": date_range,
        "status": "generated",
        "rows": 1500,
    }


@app.task(priority=1)
def cleanup_old_data(days_old: int) -> dict:
    """Low priority: Clean up data older than N days.

    Routed to 'low-priority' queue with priority=1 in message tags.
    """
    return {
        "days_old": days_old,
        "records_cleaned": 42,
        "status": "completed",
    }


if __name__ == "__main__":
    print("Sending tasks with priority routing...\n")

    # High-priority task -- goes to 'high-priority' queue
    r1 = process_payment.delay("ORD-12345", 99.99)
    print(f"[HIGH]    process_payment -> queue=high-priority, id={r1.id}")

    # Default-priority task -- goes to 'celery' queue
    r2 = send_notification.delay("user-42", "Your order is confirmed!")
    print(f"[DEFAULT] send_notification -> queue=celery, id={r2.id}")

    # Low-priority tasks -- go to 'low-priority' queue
    r3 = generate_report.delay("sales", "2026-Q1")
    print(f"[LOW]     generate_report -> queue=low-priority, id={r3.id}")

    r4 = cleanup_old_data.delay(90)
    print(f"[LOW]     cleanup_old_data -> queue=low-priority, id={r4.id}")

    # You can also route dynamically with apply_async
    r5 = send_notification.apply_async(
        args=("user-99", "Flash sale starting!"),
        queue="high-priority",  # override default routing
        priority=9,
    )
    print(f"[HIGH]    send_notification (override) -> queue=high-priority, id={r5.id}")

    # Wait for results
    print("\nWaiting for results...")
    for label, result in [
        ("process_payment", r1),
        ("send_notification", r2),
        ("generate_report", r3),
        ("cleanup_old_data", r4),
        ("send_notification (override)", r5),
    ]:
        print(f"  {label}: {result.get(timeout=30)}")

    print("\nAll tasks completed!")
