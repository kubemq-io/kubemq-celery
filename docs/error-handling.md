# Error Handling Guide

Comprehensive guide to error handling patterns with KubeMQ Celery Transport, including Sentry integration.

## 1. Task Retry with `self.retry()`

Use `self.retry()` to retry tasks on transient failures. The retry dispatches a new message through KubeMQ with an optional countdown delay (using native `delay_in_seconds`).

```python
@app.task(bind=True, max_retries=3)
def send_email(self, to: str, subject: str, body: str) -> dict:
    """Send email with automatic retry on transient failures."""
    try:
        result = email_service.send(to=to, subject=subject, body=body)
        return {"status": "sent", "message_id": result.id}
    except ConnectionError as exc:
        # Retry with exponential backoff: 10s, 20s, 40s
        raise self.retry(exc=exc, countdown=10 * (2 ** self.request.retries))
    except ValueError as exc:
        # Permanent error -- do NOT retry
        logger.error("Invalid email params: %s", exc)
        raise
```

**Key points:**
- `bind=True` gives access to `self` for retry control.
- `max_retries=3` limits retry attempts.
- `countdown` uses KubeMQ's native server-side delay (no client-side polling).
- Distinguish transient errors (retry) from permanent errors (raise immediately).

## 2. Exponential Backoff Pattern

For external API calls, use exponential backoff with jitter to avoid thundering herd:

```python
@app.task(bind=True, max_retries=5, default_retry_delay=60)
def call_external_api(self, endpoint: str, payload: dict) -> dict:
    """Call external API with exponential backoff and jitter."""
    import random

    try:
        response = requests.post(endpoint, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        backoff = min(60 * (2 ** self.request.retries), 600)  # max 10 min
        jitter = random.uniform(0, backoff * 0.1)  # 10% jitter
        raise self.retry(exc=exc, countdown=backoff + jitter)
```

**Backoff schedule:** 60s, 120s, 240s, 480s, 600s (capped).

**Note:** KubeMQ's maximum delay is 86400 seconds (24 hours). Countdown values exceeding this are capped automatically.

## 3. Dead Letter Queue Configuration

Configure a dead letter queue (DLQ) to capture messages that fail repeatedly at the broker level:

```python
# In Celery configuration:
app.conf.broker_transport_options = {
    "dead_letter_queue": "celery-dead-letters",
    "max_receive_count": 5,  # move to DLQ after 5 failed receives
}
```

**Monitoring DLQ messages** via the KubeMQ dashboard at `http://localhost:9090`, or programmatically:

```python
from kubemq.queues.client import Client as QueuesClient
from kubemq.core import ClientConfig

client = QueuesClient(config=ClientConfig(address="localhost:50000"))
response = client.peek_queue_messages(
    channel="celery-dead-letters",
    max_messages=10,
    wait_timeout_in_seconds=1,
)
for msg in response.messages:
    print(f"DLQ message: {msg.body.decode()}")
```

**Note:** DLQ operates at the broker level (receive count), separate from Celery's task retry mechanism (`max_retries`). Both can be used together.

## 4. `acks_late` vs `acks_early` with KubeMQ

### acks_early (DEFAULT -- recommended for most tasks)

Message is acknowledged **before** task executes. If the worker crashes during execution, the task is NOT retried automatically (use `self.retry()` for application-level retries).

```python
app.conf.task_acks_late = False  # default
```

### acks_late (at-least-once delivery)

Message is acknowledged **after** task completes successfully. If the worker crashes, KubeMQ re-delivers the message.

```python
app.conf.task_acks_late = True
app.conf.task_reject_on_worker_lost = True  # nack on worker crash
```

**Warning:** KubeMQ's transaction timeout may expire for long-running tasks (> 60s). For long tasks, use `acks_early` or ensure idempotency.

### Per-task override

```python
@app.task(acks_late=True, reject_on_worker_lost=True)
def critical_payment(payment_id: str) -> dict:
    """Process payment -- must not be lost."""
    # Ensure idempotency via payment_id dedup
    ...
```

## 5. `task_reject_on_worker_lost`

When both `task_reject_on_worker_lost=True` and `acks_late=True` are set:

- If the worker process is killed (OOM, SIGKILL), Celery nacks the message.
- KubeMQ re-delivers the nacked message to another worker.
- This provides crash recovery at the cost of potential duplicate execution.

```python
app.conf.task_reject_on_worker_lost = True
```

**Always pair with idempotent task design** (see section 7).

## 6. Handling `MaxRetriesExceededError`

When all retry attempts are exhausted, handle the final failure gracefully:

```python
from celery.exceptions import MaxRetriesExceededError

@app.task(bind=True, max_retries=3)
def process_order(self, order_id: str) -> dict:
    try:
        return do_process(order_id)
    except TransientError as exc:
        try:
            raise self.retry(exc=exc, countdown=30)
        except MaxRetriesExceededError:
            # All retries exhausted -- log and escalate
            logger.error("Order %s failed after %d retries", order_id, self.max_retries)
            notify_ops_team(order_id=order_id, error=str(exc))
            raise  # let Celery mark task as FAILURE
```

## 7. Idempotent Task Design

Design tasks to be safely re-executed (critical when using `acks_late` or broker-level redelivery):

```python
import hashlib

@app.task(bind=True, acks_late=True)
def process_upload(self, file_id: str, checksum: str) -> dict:
    """Idempotent file processing -- safe for re-delivery.

    Uses file checksum as idempotency key. If already processed,
    returns cached result without re-processing.
    """
    cache_key = f"processed:{checksum}"
    cached = cache.get(cache_key)
    if cached:
        logger.info("File %s already processed (idempotent skip)", file_id)
        return cached

    result = do_expensive_processing(file_id)
    cache.set(cache_key, result, timeout=86400)
    return result
```

**Idempotency strategies:**
- **Cache-based dedup:** Check a cache/DB before processing.
- **Database constraints:** Use unique constraints to prevent duplicate writes.
- **Conditional updates:** Use `UPDATE ... WHERE version = N` patterns.

## 8. Error Monitoring with Celery Events

Use Celery signals to monitor task failures and retries:

```python
from celery import signals

@signals.task_failure.connect
def on_task_failure(sender=None, task_id=None, exception=None,
                    traceback=None, **kwargs):
    """Log task failures for monitoring."""
    logger.error(
        "Task %s[%s] failed: %s",
        sender.name if sender else "unknown",
        task_id,
        exception,
    )

@signals.task_retry.connect
def on_task_retry(sender=None, request=None, reason=None, **kwargs):
    """Track retry events."""
    logger.warning(
        "Task %s[%s] retrying: %s (attempt %d)",
        sender.name if sender else "unknown",
        request.id if request else "?",
        reason,
        request.retries if request else 0,
    )
```

These signals work with any monitoring backend (Prometheus, Datadog, custom logging).

## 9. Sentry Integration

[Sentry](https://sentry.io/) provides automatic error capture, performance monitoring, and distributed tracing for Celery tasks.

### Setup

```python
"""Sentry integration for Celery error tracking.

Provides automatic error capture, performance monitoring,
and distributed tracing across task chains.
"""

import os

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration

sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    integrations=[CeleryIntegration()],
    traces_sample_rate=0.1,  # 10% of transactions
    profiles_sample_rate=0.1,
    environment=os.environ.get("ENVIRONMENT", "development"),
    release=os.environ.get("APP_VERSION", "unknown"),
)
```

### What Sentry captures automatically

- Task failures (with full traceback)
- Task retries
- Performance spans for task execution
- Distributed tracing across chain/chord/group

### Custom breadcrumbs for debugging

```python
@app.task(bind=True)
def process_with_context(self, item_id: str) -> dict:
    sentry_sdk.add_breadcrumb(
        category="task",
        message=f"Processing item {item_id}",
        level="info",
    )
    try:
        return do_process(item_id)
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        raise
```

### Filtering noise

```python
sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    integrations=[CeleryIntegration()],
    before_send=lambda event, hint: (
        None if "MaxRetriesExceededError" in str(hint.get("exc_info", ""))
        else event
    ),
)
```

This filters out `MaxRetriesExceededError` from Sentry (since retries are expected behavior), while still capturing unexpected errors.
