"""Multiple Celery Apps — KubeMQ Celery Transport.

Demonstrates:
- Multiple Celery() app instances with different KubeMQ brokers
- Routing tasks to separate broker instances
- Independent configuration per app
- Cross-app task dispatching

Usage:
    # Start workers for each app:
    celery -A examples.advanced_patterns.multi_app:orders_app worker \
        --loglevel=info -Q orders -n orders@%%h
    celery -A examples.advanced_patterns.multi_app:notifications_app worker \
        --loglevel=info -Q notifications -n notifications@%%h

    python examples/advanced_patterns/multi_app.py

Requirements:
    - Running KubeMQ broker(s) on localhost:50000 (or set env vars)
    - kubemq-celery installed

Note:
    This example intentionally uses ORDERS_BROKER_URL and
    NOTIFICATIONS_BROKER_URL instead of a single CELERY_BROKER_URL.
    Each Celery app targets a separate KubeMQ broker (multi-broker design),
    so they require independent connection strings.
"""

from __future__ import annotations

import os
import time

from celery import Celery

import kubemq_celery  # noqa: F401

orders_broker = os.environ.get("ORDERS_BROKER_URL", "kubemq://localhost:50000")
notifications_broker = os.environ.get("NOTIFICATIONS_BROKER_URL", "kubemq://localhost:50000")

orders_app = Celery(
    "orders",
    broker=orders_broker,
    result_backend=orders_broker,
)
orders_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_default_queue="orders",
    broker_transport_options={
        "wait_timeout": 1,
    },
)

notifications_app = Celery(
    "notifications",
    broker=notifications_broker,
    result_backend=notifications_broker,
)
notifications_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_default_queue="notifications",
    broker_transport_options={
        "wait_timeout": 1,
    },
)


@orders_app.task
def create_order(order_id: str, items: list[dict]) -> dict:
    """Create an order on the orders broker."""
    total = sum(item.get("price", 0) * item.get("qty", 1) for item in items)
    print(f"[orders] Created order {order_id}, total=${total:.2f}")
    time.sleep(0.3)
    return {"order_id": order_id, "total": total, "status": "created"}


@orders_app.task
def fulfill_order(order_id: str) -> dict:
    """Fulfill an order."""
    print(f"[orders] Fulfilling order {order_id}")
    time.sleep(0.5)
    return {"order_id": order_id, "status": "fulfilled"}


@notifications_app.task
def send_email(to: str, subject: str, body: str) -> dict:
    """Send email via the notifications broker."""
    print(f"[notifications] Email to {to}: {subject}")
    time.sleep(0.2)
    return {"to": to, "subject": subject, "status": "sent"}


@notifications_app.task
def send_sms(phone: str, message: str) -> dict:
    """Send SMS via the notifications broker."""
    print(f"[notifications] SMS to {phone}: {message}")
    time.sleep(0.2)
    return {"phone": phone, "status": "sent"}


def process_new_order(order_id: str, items: list[dict], customer_email: str) -> dict:
    """Orchestrate order processing across both apps."""
    order_result = create_order.delay(order_id, items)

    total = sum(item.get("price", 0) * item.get("qty", 1) for item in items)
    email_result = send_email.delay(
        customer_email,
        f"Order {order_id} Confirmed",
        f"Your order total is ${total:.2f}",
    )

    return {
        "order_task_id": order_result.id,
        "email_task_id": email_result.id,
    }


if __name__ == "__main__":
    print("=== Multiple Celery Apps — KubeMQ Celery Transport ===\n")
    print(f"Orders broker:        {orders_broker}")
    print(f"Notifications broker: {notifications_broker}")
    print(f"Orders queue:         {orders_app.conf.task_default_queue}")
    print(f"Notifications queue:  {notifications_app.conf.task_default_queue}")
    print()

    print("To test:")
    print("  1. Start an orders worker:")
    print("     celery -A examples.advanced_patterns.multi_app:orders_app worker \\")
    print("       --loglevel=info -Q orders -n orders@%h")
    print("  2. Start a notifications worker:")
    print("     celery -A examples.advanced_patterns.multi_app:notifications_app worker \\")
    print("       --loglevel=info -Q notifications -n notifications@%h")
    print("  3. Send tasks from a Python shell using the app-specific tasks.")
    print()
    print("=== Configuration demo complete ===")
