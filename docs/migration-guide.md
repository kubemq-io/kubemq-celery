# Migration Guide: Redis/RabbitMQ to KubeMQ

Migrate your existing Celery application from Redis or RabbitMQ to KubeMQ with minimal changes.

## One-Line Migration

```python
# Before (Redis):
app = Celery("myapp", broker="redis://localhost:6379/0")

# Before (RabbitMQ):
app = Celery("myapp", broker="amqp://guest:guest@localhost:5672//")

# After (KubeMQ):
import kubemq_celery
app = Celery("myapp", broker="kubemq://localhost:50000")
```

That's it. One import and one URL change.

## Step-by-Step Migration

### 1. Install kubemq-celery

```bash
pip install kubemq-celery
# or
uv add kubemq-celery
```

### 2. Start a KubeMQ broker

```bash
# Docker
docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq:latest

# Kubernetes
kubectl apply -f https://get.kubemq.io/deploy
```

### 3. Update your Celery configuration

```python
import kubemq_celery  # add this import

# Change broker_url
app.conf.broker_url = "kubemq://localhost:50000"

# Optional: change result backend too
app.conf.result_backend = "kubemq://localhost:50000"
```

### 4. Remove old broker dependencies (optional)

```bash
# If no longer needed
pip uninstall redis
# or
pip uninstall amqp
```

### 5. Start your workers

```bash
celery -A myapp worker --loglevel=info
```

Workers connect to KubeMQ and start processing tasks immediately.

## Feature Comparison

| Feature | Redis | RabbitMQ | KubeMQ |
|---------|-------|----------|--------|
| **Setup** | External process | External process + Erlang | Docker or K8s-native |
| **Kubernetes-native** | No | No | Yes (StatefulSet, auto-clustering) |
| **Message acknowledgment** | Visibility timeout (can cause duplicates) | Native ack/nack | Native ack/nack |
| **Delayed delivery** | Client-side polling | Plugin (rabbitmq_delayed_message) | Native `delay_in_seconds` |
| **Dead letter queue** | Manual implementation | Exchange-based DLQ | Native `max_receive_count` + DLQ channel |
| **Priority queues** | Separate lists per priority | Server-enforced priority | Metadata tags (use separate queues for enforcement) |
| **Fanout/broadcast** | Pub/Sub channels | Fanout exchange | KubeMQ Events |
| **Monitoring (Flower)** | Full | Full | Full |
| **Remote control** | Full (pidbox via Redis Pub/Sub) | Full (pidbox via AMQP) | Full (pidbox via KubeMQ Events) |
| **Result backend** | Redis GET/SET | RPC or DB | Queue-peek (non-destructive read) |
| **Protocol** | TCP | AMQP (TCP) | gRPC (HTTP/2) |
| **Connection stability** | Connection resets under load | Stable | gRPC keep-alive, auto-reconnect |
| **Max delay** | Unlimited (client polling) | Unlimited (plugin) | 24 hours (86400 seconds) |
| **Max result expiry** | Unlimited | N/A (RPC: 24h default) | 24 hours (86400 seconds) |

## What Works the Same

These Celery features work identically with KubeMQ:

- **Task definition** -- `@app.task` decorators, task classes
- **Task invocation** -- `task.delay()`, `task.apply_async()`, `task.s()` signatures
- **Task routing** -- `task_routes`, `task_default_queue`, custom routing
- **Chains, groups, chords** -- all canvas primitives work
- **Task retries** -- `self.retry()` and `autoretry_for`
- **Worker management** -- `celery worker`, `celery multi`, concurrency settings
- **Flower monitoring** -- all Flower features work (task list, graphs, worker info)
- **Remote control** -- `celery inspect`, `celery control` commands
- **Celery Beat** -- periodic task scheduling
- **Serialization** -- JSON, pickle, msgpack, YAML serializers
- **Error handling** -- `task_acks_on_failure_or_timeout`, `task_reject_on_worker_lost`
- **Prefetch** -- `worker_prefetch_multiplier` via Kombu virtual QoS

## What's Different

### Acknowledgment Model

**Redis:** Uses a visibility timeout. If a worker crashes without acking within the timeout, the message becomes visible again. Setting the timeout too low causes duplicate delivery; too high causes delayed reprocessing.

**KubeMQ:** Uses native ack/nack via gRPC stream. Messages are explicitly acknowledged or rejected. No timing-based guesswork.

```python
# KubeMQ handles this natively -- no configuration needed
# The following settings work but the underlying mechanism is different:
app.conf.task_acks_late = True   # KubeMQ native ack (not visibility timeout)
app.conf.task_reject_on_worker_lost = True  # Maps to KubeMQ nack()
```

### Delayed Tasks

**Redis:** Celery implements countdown/ETA client-side using a polling loop.

**RabbitMQ:** Requires the `rabbitmq_delayed_message_exchange` plugin.

**KubeMQ:** Native `delay_in_seconds` on queue messages. Zero polling overhead.

```python
# Works the same from the API perspective:
task.apply_async(countdown=60)          # delivers after 60 seconds
task.apply_async(eta=future_datetime)   # delivers at specific time

# Limitation: max delay is 24 hours (86400 seconds)
# Delays > 24h are capped at 24h with a warning log
```

### Dead Letter Queue

**Redis:** Requires manual DLQ implementation.

**RabbitMQ:** Requires exchange-level DLQ configuration.

**KubeMQ:** One-line transport option.

```python
app.conf.broker_transport_options = {
    "dead_letter_queue": "celery-dead-letters",
    "max_receive_count": 3,
}
```

### Result Backend

**Redis:** Uses GET/SET with TTL expiry. Fast but requires a separate Redis connection.

**KubeMQ:** Uses queue-peek (non-destructive read). Results are stored in the same KubeMQ broker. No additional infrastructure needed.

```python
# Pure KubeMQ stack -- broker and results in one place
app.conf.result_backend = "kubemq://localhost:50000"
app.conf.result_expires = 86400  # max 24 hours
```

### Queue Name Sanitization

KubeMQ channel names have stricter character rules than Redis keys or AMQP queue names. The transport automatically sanitizes names:

| Celery Name | KubeMQ Channel |
|------------|----------------|
| `celery` | `celery` |
| `celery@worker1.celery.pidbox` | `celery.worker1.celery.pidbox` |
| `reply/celery/pidbox` | `reply.celery.pidbox` |

This is transparent -- no configuration changes needed.

## Common Gotchas

### 1. `import kubemq_celery` is required

The transport must be imported to register the `kubemq://` URL scheme. Without it, Celery will raise an "unknown transport" error.

```python
import kubemq_celery  # must be imported before Celery uses the broker URL
from celery import Celery
```

### 2. `broker_pool_limit` is ignored

KubeMQ uses a single gRPC client per Channel. Kombu's connection pool handles scaling. Setting `broker_pool_limit` has no effect.

### 3. Maximum delay is 24 hours

KubeMQ's `delay_in_seconds` caps at 86400 seconds. If your tasks use `countdown` or `eta` values exceeding 24 hours, they will be capped. Use Celery Beat for longer scheduling needs.

### 4. Result expiration is 24 hours max

KubeMQ queue message expiration caps at 86400 seconds. Celery's default `result_expires` of 24 hours matches the KubeMQ maximum. If you need longer result retention, use a database-backed result backend.

### 5. Priority is metadata only

KubeMQ does not enforce message priority at the server level. Priority values are stored in message tags but not used for ordering. Use separate queues for priority routing instead:

```python
app.conf.task_routes = {
    "myapp.tasks.critical_*": {"queue": "high-priority"},
    "myapp.tasks.batch_*": {"queue": "low-priority"},
}
```

### 6. Long-running tasks with `task_acks_late=True`

When using `task_acks_late=True` with tasks that run longer than ~60 seconds, the KubeMQ transaction may expire before the ack is sent, causing redelivery. For long tasks, use `task_acks_late=False` (the default) or ensure your tasks are idempotent.

## New in v1.1

### Per-Message TTL

Set a default message expiration for all tasks:

```python
app.conf.broker_transport_options = {
    "message_expiration": 3600,  # 1 hour TTL for all messages
}
```

Task-level `expires` header takes precedence. Maximum is 86400 seconds (24 hours).

### Batch Receive Optimization

Receive multiple messages per gRPC call to reduce round-trips:

```python
app.conf.broker_transport_options = {
    "max_batch_size": 10,  # up to 100
}
```

### Async Transport

For asyncio-based worker pools (Starlette, Litestar, etc.):

```python
app.conf.broker_url = "kubemq+async://localhost:50000"
# Start worker with: celery -A myapp worker --pool=asyncio
```

Also supports TLS: `kubemq+async+tls://`.

### gRPC Keepalive

Configurable keepalive for long-lived connections:

```python
app.conf.broker_transport_options = {
    "grpc_keepalive_time": 30,       # ping every 30s
    "grpc_keepalive_timeout": 10,    # wait 10s for response
    "grpc_permit_without_calls": True,
}
```

## Redis-Specific Settings to Remove

These Redis-specific settings are not applicable and can be removed:

```python
# Remove these:
# app.conf.redis_max_connections = ...
# app.conf.redis_socket_timeout = ...
# app.conf.redis_socket_connect_timeout = ...
# app.conf.redis_retry_on_timeout = ...
# app.conf.broker_transport_options = {"visibility_timeout": 3600}  # not needed with KubeMQ
```

## RabbitMQ-Specific Settings to Remove

These RabbitMQ-specific settings are not applicable:

```python
# Remove these:
# app.conf.broker_heartbeat = ...
# app.conf.broker_transport_options = {"confirm_publish": True}  # not applicable
# Exchange/queue declare options are handled automatically
```
