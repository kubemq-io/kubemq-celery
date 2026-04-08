"""Mock Tasks — KubeMQ Celery Transport.

Demonstrates:
- unittest.mock.patch for mocking Celery task calls
- Mocking .delay() and .apply_async() return values
- Verifying task was called with expected arguments
- Testing application logic without dispatching to broker

Usage:
    python examples/testing/mock_tasks.py
    pytest examples/testing/mock_tasks.py -v

Requirements:
    - kubemq-celery installed
    - No broker needed — all calls are mocked
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "mock_tasks",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@app.task
def send_email(to: str, subject: str, body: str) -> dict:
    """Send an email notification (simulated)."""
    return {"to": to, "subject": subject, "status": "sent"}


@app.task
def process_order(order_id: str, items: list[dict]) -> dict:
    """Process an order."""
    total = sum(item.get("price", 0) * item.get("qty", 1) for item in items)
    return {"order_id": order_id, "total": total, "status": "processed"}


@app.task(bind=True, max_retries=3)
def charge_payment(self, order_id: str, amount: float) -> dict:
    """Charge payment for an order."""
    return {"order_id": order_id, "amount": amount, "status": "charged"}


def create_order(order_id: str, items: list[dict]) -> dict:
    """Application-level function that dispatches tasks."""
    order_result = process_order.delay(order_id, items)

    total = sum(item.get("price", 0) * item.get("qty", 1) for item in items)
    charge_result = charge_payment.delay(order_id, total)

    send_email.delay(
        to="customer@example.com",
        subject=f"Order {order_id} confirmed",
        body=f"Your order total is ${total:.2f}",
    )

    return {
        "order_task_id": order_result.id,
        "charge_task_id": charge_result.id,
    }


def test_mock_delay():
    """Test mocking .delay() calls."""
    print("--- test_mock_delay ---")

    mock_result = MagicMock()
    mock_result.id = "fake-task-id-001"
    mock_result.result = {"to": "test@example.com", "status": "sent"}
    mock_result.status = "SUCCESS"

    with patch.object(send_email, "delay", return_value=mock_result) as mock_delay:
        result = send_email.delay("test@example.com", "Test", "Hello")

        mock_delay.assert_called_once_with("test@example.com", "Test", "Hello")
        assert result.id == "fake-task-id-001"
        assert result.status == "SUCCESS"
        print("  send_email.delay called with correct args: OK")
        print(f"  Returned mock task ID: {result.id}")


def test_mock_apply_async():
    """Test mocking .apply_async() calls."""
    print("\n--- test_mock_apply_async ---")

    mock_result = MagicMock()
    mock_result.id = "fake-task-id-002"

    with patch.object(process_order, "apply_async", return_value=mock_result) as mock_apply:
        result = process_order.apply_async(
            args=["ORD-001", [{"name": "widget", "price": 9.99, "qty": 2}]],
            queue="orders",
        )

        mock_apply.assert_called_once()
        call_kwargs = mock_apply.call_args
        assert call_kwargs.kwargs["queue"] == "orders"
        print("  apply_async called with queue='orders': OK")
        print(f"  Returned mock task ID: {result.id}")


def test_mock_full_workflow():
    """Test mocking an entire workflow with multiple tasks."""
    print("\n--- test_mock_full_workflow ---")

    mock_order = MagicMock(id="order-task-001")
    mock_charge = MagicMock(id="charge-task-001")
    mock_email = MagicMock(id="email-task-001")

    with (
        patch.object(process_order, "delay", return_value=mock_order),
        patch.object(charge_payment, "delay", return_value=mock_charge),
        patch.object(send_email, "delay", return_value=mock_email),
    ):
        items = [{"name": "widget", "price": 10.0, "qty": 2}]
        result = create_order("ORD-100", items)

        assert result["order_task_id"] == "order-task-001"
        assert result["charge_task_id"] == "charge-task-001"

        process_order.delay.assert_called_once_with("ORD-100", items)
        charge_payment.delay.assert_called_once_with("ORD-100", 20.0)
        send_email.delay.assert_called_once()

        email_args = send_email.delay.call_args
        assert email_args.kwargs["to"] == "customer@example.com"
        assert "ORD-100" in email_args.kwargs["subject"]

        print("  process_order called: OK")
        print("  charge_payment called with $20.00: OK")
        print("  send_email called to customer: OK")


def test_mock_task_failure():
    """Test mocking a task that fails."""
    print("\n--- test_mock_task_failure ---")

    mock_result = MagicMock()
    mock_result.id = "fail-task-001"
    mock_result.status = "FAILURE"
    mock_result.result = ValueError("Payment declined")
    mock_result.failed.return_value = True

    with patch.object(charge_payment, "delay", return_value=mock_result):
        result = charge_payment.delay("ORD-999", 1000.0)

        assert result.failed()
        assert result.status == "FAILURE"
        assert isinstance(result.result, ValueError)
        print(f"  Mocked failure status: {result.status}")
        print(f"  Mocked error: {result.result}")


if __name__ == "__main__":
    print("=== Mock Tasks Example ===")
    print("All task calls are mocked — no broker needed\n")

    test_mock_delay()
    test_mock_apply_async()
    test_mock_full_workflow()
    test_mock_task_failure()

    print("\nAll mock task tests passed!")
