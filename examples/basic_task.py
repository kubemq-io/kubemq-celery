"""Basic Celery task with KubeMQ broker.

Minimal example demonstrating:
- KubeMQ broker connection
- Simple task definition and execution
- Result retrieval with queue-peek backend

Usage:
    # Start a worker:
    celery -A basic_task worker --loglevel=info

    # Send a task (in another terminal):
    python basic_task.py
"""

import kubemq_celery  # registers kubemq:// transport
from celery import Celery

# Create Celery app with KubeMQ as both broker and result backend
app = Celery(
    "basic_task",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

# Optional: configure result expiration (max 12 hours for KubeMQ)
app.conf.result_expires = 3600  # 1 hour


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task
def greet(name: str) -> str:
    """Return a greeting message."""
    return f"Hello, {name}!"


if __name__ == "__main__":
    # Send tasks and retrieve results
    print("Sending tasks to KubeMQ broker...")

    # Simple task execution
    result = add.delay(4, 6)
    print(f"add(4, 6) -> Task ID: {result.id}")
    print(f"add(4, 6) -> Result: {result.get(timeout=10)}")

    # Another task
    result = multiply.delay(3, 7)
    print(f"multiply(3, 7) -> Task ID: {result.id}")
    print(f"multiply(3, 7) -> Result: {result.get(timeout=10)}")

    # String result
    result = greet.delay("KubeMQ")
    print(f"greet('KubeMQ') -> Task ID: {result.id}")
    print(f"greet('KubeMQ') -> Result: {result.get(timeout=10)}")

    print("\nAll tasks completed successfully!")
