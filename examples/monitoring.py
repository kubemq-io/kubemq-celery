"""Flower monitoring integration with KubeMQ.

Demonstrates:
- Celery events for task monitoring
- Flower web UI setup
- Celery inspect and control commands
- Real-time task tracking

Usage:
    # 1. Start a worker with events enabled:
    celery -A monitoring worker --loglevel=info -E

    # 2. Start Flower monitoring:
    celery -A monitoring flower --port=5555

    # 3. Open Flower dashboard:
    #    http://localhost:5555

    # 4. Send tasks to see them in Flower:
    python monitoring.py

    # Useful celery CLI commands:
    celery -A monitoring inspect active       # list active tasks
    celery -A monitoring inspect reserved     # list reserved tasks
    celery -A monitoring inspect stats        # worker statistics
    celery -A monitoring inspect ping         # check worker connectivity
    celery -A monitoring control shutdown     # graceful worker shutdown
"""

import time

import kubemq_celery
from celery import Celery, chain, group

app = Celery(
    "monitoring",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

# Enable task events for Flower monitoring
# Events are broadcast via KubeMQ Events (fanout) to all subscribers
app.conf.update(
    # Enable worker to send task events
    worker_send_task_events=True,
    # Enable task to send state change events (PENDING, STARTED, SUCCESS, etc.)
    task_send_sent_event=True,
    # Event rate limit (events per second per worker)
    event_queue_expires=60,
)


@app.task(bind=True)
def long_running_task(self, seconds: int = 5) -> dict:
    """A task that takes some time to complete.

    Visible in Flower as STARTED while running, then SUCCESS.
    """
    self.update_state(state="PROGRESS", meta={"step": 0, "total": seconds})
    for i in range(seconds):
        time.sleep(1)
        # Update progress -- visible in Flower's task detail view
        self.update_state(
            state="PROGRESS",
            meta={"step": i + 1, "total": seconds, "percent": (i + 1) * 100 // seconds},
        )
    return {"duration": seconds, "status": "completed"}


@app.task
def compute(x: int, y: int, op: str = "add") -> int:
    """Simple compute task for monitoring demonstration."""
    if op == "add":
        return x + y
    elif op == "multiply":
        return x * y
    elif op == "power":
        return x ** y
    raise ValueError(f"Unknown operation: {op}")


@app.task
def always_fails():
    """A task that always fails -- visible as FAILURE in Flower."""
    raise RuntimeError("This task is designed to fail for monitoring demo")


if __name__ == "__main__":
    print("Sending tasks for monitoring demonstration...\n")
    print("Open Flower at http://localhost:5555 to watch task execution.\n")

    # Send a long-running task -- track progress in Flower
    r1 = long_running_task.delay(10)
    print(f"[1] long_running_task(10s) -> id={r1.id}")
    print("    Watch progress updates in Flower task detail view.\n")

    # Send a batch of fast tasks -- visible as a burst in Flower graphs
    results = []
    for i in range(20):
        r = compute.delay(i, i + 1, "add")
        results.append(r)
    print(f"[2] Sent 20 compute tasks (batch)")
    print("    Visible as a burst in Flower's task rate graph.\n")

    # Send a chain -- shows task dependencies in Flower
    r3 = chain(
        compute.s(2, 3, "add"),      # 2 + 3 = 5
        compute.s(4, "multiply"),     # 5 * 4 = 20
    ).apply_async()
    print(f"[3] chain(add(2,3), multiply(_, 4)) -> id={r3.id}")
    print("    Shows as linked tasks in Flower.\n")

    # Send a group -- parallel execution visible in Flower
    r4 = group(
        compute.s(i, 2, "power") for i in range(5)
    ).apply_async()
    print(f"[4] group(power(i, 2) for i in range(5)) -> id={r4.id}")
    print("    Shows parallel task execution in Flower.\n")

    # Send a failing task -- visible as FAILURE in Flower
    r5 = always_fails.delay()
    print(f"[5] always_fails -> id={r5.id}")
    print("    Shows as FAILURE with traceback in Flower.\n")

    # Demonstrate inspect commands
    print("--- Celery Inspect Commands ---")
    print("Run these in a separate terminal while workers are running:\n")
    print("  celery -A monitoring inspect ping")
    print("    -> Checks if workers are alive via KubeMQ Events (pidbox)\n")
    print("  celery -A monitoring inspect active")
    print("    -> Lists currently executing tasks\n")
    print("  celery -A monitoring inspect reserved")
    print("    -> Lists tasks reserved (prefetched) by workers\n")
    print("  celery -A monitoring inspect stats")
    print("    -> Shows detailed worker statistics\n")
    print("  celery -A monitoring control shutdown")
    print("    -> Sends graceful shutdown to all workers via KubeMQ Events\n")

    # Wait for batch results
    print("Waiting for batch results...")
    batch_results = [r.get(timeout=30) for r in results]
    print(f"  Batch complete: {len(batch_results)} tasks, sum={sum(batch_results)}")

    # Wait for chain result
    print(f"  Chain result: {r3.get(timeout=30)}")

    # Wait for long-running task
    print(f"  Long-running task: {r1.get(timeout=60)}")

    print("\nDone! Check Flower for full task history and graphs.")
