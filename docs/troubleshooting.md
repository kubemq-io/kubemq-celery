# Troubleshooting & FAQ

Common issues and solutions for KubeMQ Celery Transport.

## 1. Connection Issues

### "Connection refused" on startup

**Symptoms:** Worker fails to start with `KubeMQCeleryConnectionError: Failed to connect to KubeMQ broker`.

**Cause:** KubeMQ broker is not running or not accessible at the configured address.

**Solution:**

```bash
# Verify the broker is running
docker ps | grep kubemq

# Start if not running
docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq:latest

# Check connectivity
curl -s http://localhost:9090/health
```

### "Authentication failed"

**Symptoms:** `KubeMQAuthenticationError` on connection.

**Cause:** Invalid or missing auth token.

**Solution:**

```python
# Option 1: token in URL
app.conf.broker_url = "kubemq://:my-token@kubemq.default.svc:50000"

# Option 2: token in transport options
app.conf.broker_transport_options = {
    "auth_token": "my-token",
}
```

Verify the token matches the KubeMQ broker configuration.

### "TLS handshake failed"

**Symptoms:** Connection fails with TLS/SSL error.

**Cause:** Certificate mismatch, expired cert, or wrong CA.

**Solution:**

```python
app.conf.broker_url = "kubemq+tls://kubemq.default.svc:50000"
app.conf.broker_transport_options = {
    "tls_cert_file": "/certs/client.pem",
    "tls_key_file": "/certs/client-key.pem",
    "tls_ca_file": "/certs/ca.pem",
}
```

Verify certificate files are readable and not expired:

```bash
openssl x509 -in /certs/client.pem -noout -dates
```

### "Connection lost during operation"

**Symptoms:** Intermittent `KubeMQStreamBrokenError` or `KubeMQConnectionError`.

**Cause:** Network instability, broker restart, or load balancer timeout.

**Solution:** Configure gRPC keepalive to detect and recover from stale connections:

```python
app.conf.broker_transport_options = {
    "grpc_keepalive_time": 30,       # ping every 30s
    "grpc_keepalive_timeout": 10,    # wait 10s for response
    "grpc_permit_without_calls": True,
}
```

Celery's built-in `broker_connection_retry` handles automatic reconnection.

## 2. Task Issues

### "Tasks not executing"

**Symptoms:** Tasks are dispatched but never picked up by workers.

**Cause:** Queue name mismatch, worker not consuming the right queue, or worker not connected.

**Solution:**

```bash
# Check worker is connected and consuming
celery -A myapp inspect active_queues

# Verify tasks are in the queue (KubeMQ dashboard)
# Open http://localhost:9090 and check queue depth

# Ensure queue names match between producer and consumer
celery -A myapp inspect registered
```

### "Tasks executing twice"

**Symptoms:** Duplicate task execution.

**Cause:** With `task_acks_late=True`, if the KubeMQ transaction timeout expires before the task completes, the message is redelivered.

**Solution:**

```python
# Option 1: Use acks_early (default, recommended for most tasks)
app.conf.task_acks_late = False

# Option 2: If acks_late is required, ensure tasks are idempotent
@app.task(acks_late=True, reject_on_worker_lost=True)
def idempotent_task(item_id: str):
    # Check if already processed
    if cache.get(f"processed:{item_id}"):
        return  # skip duplicate
    # ... process ...
    cache.set(f"processed:{item_id}", True, timeout=86400)
```

**KubeMQ advantage over Redis:** With `acks_early=True` (default), KubeMQ's native ack eliminates the visibility timeout race condition that causes duplicates with Redis.

### "Task results not found"

**Symptoms:** `result.get()` raises timeout or returns `None`.

**Cause:** Result backend not configured, result expired, or result channel not created.

**Solution:**

```python
# Ensure result backend is configured
app.conf.result_backend = "kubemq://localhost:50000"
app.conf.result_expires = 86400  # 24 hours (KubeMQ maximum)
```

Results expire after `result_expires` seconds. If you need the result after 24 hours, retrieve it before expiration.

### "Tasks stuck in queue"

**Symptoms:** Queue depth keeps growing but workers are idle.

**Cause:** Worker is processing tasks on a different queue, or worker is stuck.

**Solution:**

```bash
# Check which queues the worker is consuming
celery -A myapp inspect active_queues

# Start a worker on a specific queue
celery -A myapp worker -Q myqueue --loglevel=info

# Purge a stuck queue (CAUTION: deletes all messages)
celery -A myapp purge -Q myqueue
```

## 3. Configuration Issues

### "Unknown transport: kubemq"

**Symptoms:** `ValueError: Unknown transport 'kubemq'` or `No module named 'kubemq_celery'`.

**Cause:** The `kubemq_celery` package is not imported before Celery uses the broker URL.

**Solution:**

```python
import kubemq_celery  # MUST be imported before Celery uses the broker URL
from celery import Celery

app = Celery("myapp", broker="kubemq://localhost:50000")
```

The import registers the `kubemq://` URL scheme with Kombu's transport registry.

### "Import error: No module named 'kubemq_celery'"

**Symptoms:** `ModuleNotFoundError` when importing.

**Cause:** Package not installed in the active Python environment.

**Solution:**

```bash
# Install with pip
pip install kubemq-celery

# Or with uv
uv add kubemq-celery

# Verify installation
python -c "import kubemq_celery; print(kubemq_celery.__version__)"
```

## 4. Performance Issues

### "Slow task dispatch"

**Symptoms:** High latency between `task.delay()` and worker receiving the task.

**Cause:** Network latency, large message payloads, or suboptimal batch settings.

**Solution:**

```python
app.conf.broker_transport_options = {
    "wait_timeout": 1,        # reduce if latency is acceptable
    "max_batch_size": 10,     # batch receive for throughput
    "message_expiration": 3600,
}
```

For large payloads, increase gRPC message size limits:

```python
app.conf.broker_transport_options = {
    "max_send_size": 8_388_608,     # 8MB
    "max_receive_size": 8_388_608,  # 8MB
}
```

### "Worker consuming slowly"

**Symptoms:** Queue depth grows faster than the worker can process.

**Cause:** Low concurrency, CPU-bound tasks blocking the worker, or low batch size.

**Solution:**

```python
# Increase concurrency for I/O-bound tasks
# celery -A myapp worker --concurrency=8

# Use batch receive to reduce gRPC round-trips
app.conf.broker_transport_options = {
    "max_batch_size": 20,  # up to 100, default 10
}

# Tune prefetch multiplier
app.conf.worker_prefetch_multiplier = 4  # 4x concurrency
```

See [Performance Guide](performance.md) for detailed tuning recommendations.

## 5. Monitoring Issues

### "Flower shows no workers"

**Symptoms:** Flower dashboard is empty or shows workers as offline.

**Cause:** Flower cannot connect to the KubeMQ broker, or fanout events are not being delivered.

**Solution:**

```bash
# Start Flower with the correct broker URL
celery -A myapp flower --broker=kubemq://localhost:50000

# Verify workers are running
celery -A myapp inspect ping
```

Flower uses Celery's event system, which requires the KubeMQ PubSub (Events) transport for fanout. Verify PubSub is working:

```bash
# Enable worker events
celery -A myapp worker --loglevel=info -E
```

### "`celery inspect` times out"

**Symptoms:** `celery inspect ping` hangs or times out.

**Cause:** Worker pidbox (control channel) subscription not active, or broker not reachable.

**Solution:**

```bash
# Check with explicit timeout
celery -A myapp inspect ping --timeout=10

# Verify the broker is accessible
curl -s http://localhost:9090/health

# Restart the worker (re-establishes pidbox subscription)
celery -A myapp control shutdown
celery -A myapp worker --loglevel=info
```

## 6. KubeMQ vs Redis: Common Issues Comparison

| Issue | Redis Behavior | KubeMQ Behavior |
|-------|---------------|-----------------|
| Message loss on restart | Data lost unless AOF/RDB persistence configured | Persistent storage by default |
| Duplicate task execution | Visibility timeout race causes duplicates | Native ack/nack (no duplicates with acks_early) |
| Worker connection drops | Silent failure, manual recovery needed | gRPC keepalive, auto-reconnect via Celery retry |
| DLQ for failed tasks | Manual implementation required | Native `max_receive_count` + `dead_letter_queue` |
| Delayed task OOM | Tasks held in worker memory (client-side polling) | Server-side `delay_in_seconds` (zero worker memory) |
| Connection exhaustion | 6-8 TCP connections per worker | 2-3 gRPC connections (HTTP/2 multiplexed) |
| Queue depth monitoring | Requires custom scripts or Redis CLI | Built-in dashboard at port 9090 |
| Broker failover | Sentinel or Cluster mode required | Kubernetes-native StatefulSet auto-clustering |

### When to choose KubeMQ over Redis

- **Kubernetes-native deployments**: KubeMQ runs as a StatefulSet with auto-clustering.
- **Message durability**: Persistent storage without AOF/RDB configuration.
- **Delayed tasks**: Server-side delay without client-side polling overhead.
- **Dead letter queues**: Built-in DLQ with `max_receive_count`.
- **Connection stability**: gRPC keepalive handles network instability.

### When Redis may still be appropriate

- **Sub-millisecond latency**: Redis in-memory operations are faster for small payloads.
- **Existing Redis infrastructure**: If Redis is already deployed and managed.
- **Result retention > 24 hours**: Redis has no expiration cap.
- **Native chord support**: Redis provides O(1) chord unlock (KubeMQ uses polling fallback).
