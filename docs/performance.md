# Performance Guide

Tuning guide for KubeMQ Celery Transport performance.

## Connection Efficiency

KubeMQ uses gRPC (HTTP/2) which provides significant connection efficiency compared to traditional AMQP or Redis connections:

| Aspect | KubeMQ (gRPC) | Redis | RabbitMQ (AMQP) |
|--------|--------------|-------|-----------------|
| Connections per worker | 2-3 (queues + pubsub + backend) | 6-8 (broker + backend + results) | 4-6 (channels + heartbeat) |
| Protocol | HTTP/2 multiplexed | TCP per connection | TCP per channel |
| Keepalive | Built-in gRPC keepalive | Manual PING/PONG | AMQP heartbeats |
| TLS overhead | Single handshake, multiplexed | Per-connection handshake | Per-connection handshake |

## Acknowledgment Model

KubeMQ provides native message-level acknowledgment:

- **acks_early (default):** Message auto-acked on receive. Zero overhead.
- **acks_late:** Manual ack after task success. Single gRPC call per message.
- No visibility timeout races (unlike Redis/SQS).

## Recommended Celery Settings

```python
# Optimal settings for KubeMQ transport
app.conf.update(
    worker_prefetch_multiplier=4,     # 4x concurrency (default 4)
    worker_concurrency=4,             # match CPU cores
    broker_transport_options={
        "wait_timeout": 1,            # 1s receive poll (default)
        "max_batch_size": 10,         # batch receive for throughput
        "message_expiration": 3600,   # 1h default TTL
    },
)
```

## Tuning Guidelines

### worker_prefetch_multiplier

Controls how many messages each worker prefetches (buffered in the worker).

| Workload | Recommended Value | Reason |
|----------|-------------------|--------|
| CPU-bound | 1 | Minimize buffering, maximize responsiveness |
| I/O-bound | 4 (default) | Pipeline effect while waiting for I/O |
| High-throughput I/O | 8-16 | Maximize worker utilization |

With `max_batch_size > 1`, the effective in-flight messages are `prefetch_multiplier * concurrency`.

### worker_concurrency

Controls the number of concurrent worker processes/threads.

| Workload | Recommended Value | Reason |
|----------|-------------------|--------|
| CPU-bound | Number of CPU cores | Avoid oversubscription |
| I/O-bound | 2-4x CPU cores | Overlap I/O waits |
| Mixed | CPU cores + 2 | Balance compute and I/O |

### max_batch_size

Controls how many messages are fetched per gRPC call. Reduces round-trips at the cost of increased memory.

| Setting | Behavior |
|---------|----------|
| 1 | One message per gRPC call (lowest latency per message) |
| 10 (default) | Good balance of throughput and memory |
| 20-50 | High-throughput workloads |
| 100 (maximum) | Maximum throughput, higher memory usage |

### wait_timeout

Controls how long the receive call blocks waiting for messages.

| Setting | Behavior |
|---------|----------|
| 0 | Non-blocking (higher CPU, lowest latency) |
| 1 (default) | 1 second block (good balance) |
| 5-10 | Lower CPU usage, slower response to new messages |

**Note:** Must be less than Celery's `drain_events` timeout (default 2s) to avoid transport deadlocks.

### message_expiration

Per-message TTL in seconds. Messages that exceed this age are discarded by KubeMQ.

| Setting | Behavior |
|---------|----------|
| 0 (default) | No expiration |
| 3600 | 1 hour TTL (good for time-sensitive tasks) |
| 86400 | 24 hours (maximum) |

### gRPC Keepalive

Prevents idle connections from being dropped by load balancers or firewalls:

```python
app.conf.broker_transport_options = {
    "grpc_keepalive_time": 30,       # ping every 30s
    "grpc_keepalive_timeout": 10,    # wait 10s for response
    "grpc_permit_without_calls": True,  # keepalive even when idle
}
```

## Workload Profiles

### Profile 1: Low-latency API tasks

Tasks dispatched from web requests that need fast execution.

```python
app.conf.update(
    worker_concurrency=8,
    worker_prefetch_multiplier=1,
    broker_transport_options={
        "wait_timeout": 1,
        "max_batch_size": 1,       # minimize latency
    },
)
```

### Profile 2: High-throughput batch processing

Processing large volumes of tasks where throughput matters more than latency.

```python
app.conf.update(
    worker_concurrency=4,
    worker_prefetch_multiplier=8,
    broker_transport_options={
        "wait_timeout": 1,
        "max_batch_size": 50,      # maximize throughput
        "message_expiration": 86400,
    },
)
```

### Profile 3: Mixed workload with priority queues

Different settings per queue using task routing.

```python
app.conf.update(
    worker_concurrency=4,
    worker_prefetch_multiplier=4,
    task_routes={
        "myapp.tasks.critical_*": {"queue": "high-priority"},
        "myapp.tasks.batch_*": {"queue": "low-priority"},
    },
    broker_transport_options={
        "wait_timeout": 1,
        "max_batch_size": 10,
        "message_expiration": 3600,
    },
)

# Run separate workers per queue:
# celery -A myapp worker -Q high-priority --concurrency=4 --prefetch-multiplier=1
# celery -A myapp worker -Q low-priority --concurrency=2 --prefetch-multiplier=8
```

## Benchmarking

Use the provided benchmark script to measure your specific environment's performance:

```bash
# Start a worker
celery -A benchmark worker --loglevel=info

# Run benchmark (default: 100 tasks)
python examples/advanced_patterns/benchmark.py

# Custom settings
python examples/advanced_patterns/benchmark.py --broker kubemq://kubemq:50000 --tasks 1000
```

The benchmark measures:
- **Dispatch rate:** Messages sent per second (producer throughput)
- **Round-trip latency:** Time from dispatch to result retrieval (p50, p95, p99)

See [examples/advanced_patterns/benchmark.py](../examples/advanced_patterns/benchmark.py) for the full benchmark script.

## Monitoring Performance

### KubeMQ Dashboard

The KubeMQ dashboard at port 9090 shows real-time queue metrics:
- Queue depth (messages waiting)
- Message rates (sent/received per second)
- Channel statistics

### Flower

Flower provides task-level metrics:
- Task execution times
- Success/failure rates
- Worker utilization

```bash
celery -A myapp flower --broker=kubemq://localhost:50000
```

### Celery Inspect

Built-in Celery commands for checking worker state:

```bash
# Worker stats (uptime, task counts, prefetch)
celery -A myapp inspect stats

# Active tasks
celery -A myapp inspect active

# Reserved (prefetched) tasks
celery -A myapp inspect reserved
```
