# Configuration Reference

Complete reference for all `kubemq-celery` configuration options.

## Broker URL

### Format

```
kubemq://[:token@]host[:port]
kubemq+tls://[:token@]host[:port]
kubemq+async://[:token@]host[:port]
kubemq+async+tls://[:token@]host[:port]
```

### Examples

```python
# Basic connection (localhost, default port 50000)
app.conf.broker_url = "kubemq://localhost:50000"

# In-cluster Kubernetes service
app.conf.broker_url = "kubemq://kubemq.default.svc:50000"

# With authentication token
app.conf.broker_url = "kubemq://:my-secret-token@kubemq.default.svc:50000"

# With TLS
app.conf.broker_url = "kubemq+tls://kubemq.default.svc:50000"

# TLS + authentication
app.conf.broker_url = "kubemq+tls://:my-token@kubemq.default.svc:50000"

# Async transport (for asyncio worker pools)
app.conf.broker_url = "kubemq+async://localhost:50000"

# Async + TLS
app.conf.broker_url = "kubemq+async+tls://kubemq.default.svc:50000"
```

| Component | Description | Default |
|-----------|-------------|---------|
| Scheme | `kubemq://`, `kubemq+tls://`, `kubemq+async://`, or `kubemq+async+tls://` | Required |
| Token | Authentication token (after `:`, before `@`) | None |
| Host | KubeMQ broker hostname | `localhost` |
| Port | KubeMQ gRPC port | `50000` |

The `kubemq+async://` and `kubemq+async+tls://` schemes use native async KubeMQ clients (`AsyncQueuesClient`, `AsyncPubSubClient`) for non-blocking I/O. Use with Celery's asyncio worker pool (`--pool=asyncio`).

## Transport Options

Configure via `broker_transport_options`:

```python
app.conf.broker_transport_options = {
    "wait_timeout": 1,
    "auth_token": "my-token",
    "dead_letter_queue": "celery-dead-letters",
    "max_receive_count": 3,
    "client_id_prefix": "celery",
    "tls_enabled": False,
    "tls_cert_file": "/path/to/cert.pem",
    "tls_key_file": "/path/to/key.pem",
    "tls_ca_file": "/path/to/ca.pem",
    "max_send_size": 4_194_304,
    "max_receive_size": 4_194_304,
    "message_expiration": 3600,
    "max_batch_size": 10,
    "fanout_max_retries": 5,
    "grpc_keepalive_time": 30,
    "grpc_keepalive_timeout": 10,
    "grpc_permit_without_calls": True,
}
```

### Option Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wait_timeout` | `int` | `1` | Blocking receive timeout in seconds. Controls how long `_get()` waits for a message before returning empty. Must be less than Celery's `drain_events` timeout (default 2s). Increase for higher-latency environments. |
| `auth_token` | `str \| None` | `None` | KubeMQ authentication token. Overrides the token in the broker URL if both are set. |
| `dead_letter_queue` | `str` | `""` | KubeMQ channel name for dead letter messages. Messages that exceed `max_receive_count` are routed here. |
| `max_receive_count` | `int` | `0` | Maximum receive attempts before routing to the dead letter queue. Set to `0` to disable DLQ (messages redelivered indefinitely). Requires `dead_letter_queue` to be set. |
| `client_id_prefix` | `str` | `"celery"` | Prefix for KubeMQ client IDs. Each worker gets a unique ID: `{prefix}-queues-{random8}` and `{prefix}-pubsub-{random8}`. |
| `tls_enabled` | `bool` | `False` | Enable TLS for gRPC connections. Automatically set to `True` when using `kubemq+tls://` URL scheme. Set explicitly to override URL-based detection. |
| `tls_cert_file` | `str` | `""` | Path to client certificate file for mTLS authentication. |
| `tls_key_file` | `str` | `""` | Path to client private key file for mTLS authentication. |
| `tls_ca_file` | `str` | `""` | Path to CA certificate file for custom certificate authority verification. |
| `max_send_size` | `int` | `4_194_304` | Maximum gRPC send message size in bytes (default 4MB). Increase for large task payloads. |
| `max_receive_size` | `int` | `4_194_304` | Maximum gRPC receive message size in bytes (default 4MB). Increase for large task results. |
| `message_expiration` | `int` | `0` | Per-message TTL in seconds. Messages older than this are discarded by KubeMQ. Set to `0` to disable (no expiration). Maximum 86400 (24 hours). Task-level `expires` header takes precedence if set. |
| `max_batch_size` | `int` | `10` | Maximum messages per gRPC receive call. Higher values reduce round-trips but increase memory. Range: 1-100. |
| `fanout_max_retries` | `int` | `5` | Maximum re-subscription attempts when a fanout subscription (Events) encounters an error. Uses exponential backoff (1s, 2s, 4s, ... max 30s). |
| `grpc_keepalive_time` | `int` | `30` | Seconds between gRPC keepalive pings. Prevents idle connections from being dropped by load balancers or firewalls. |
| `grpc_keepalive_timeout` | `int` | `10` | Seconds to wait for a keepalive ping response before considering the connection dead. |
| `grpc_permit_without_calls` | `bool` | `True` | Send keepalive pings even when there are no active RPCs. Set to `True` for long-lived connections that may be idle between task bursts. |

### TLS Configuration Examples

**Server-only TLS (encrypted connection, no client auth):**

```python
app.conf.broker_url = "kubemq+tls://kubemq.default.svc:50000"
```

**mTLS (mutual authentication):**

```python
app.conf.broker_url = "kubemq+tls://kubemq.default.svc:50000"
app.conf.broker_transport_options = {
    "tls_cert_file": "/certs/client.pem",
    "tls_key_file": "/certs/client-key.pem",
    "tls_ca_file": "/certs/ca.pem",
}
```

**Custom CA (self-signed broker certificate):**

```python
app.conf.broker_transport_options = {
    "tls_enabled": True,
    "tls_ca_file": "/certs/custom-ca.pem",
}
```

### Dead Letter Queue Configuration

```python
app.conf.broker_transport_options = {
    "dead_letter_queue": "celery-dead-letters",
    "max_receive_count": 3,  # route to DLQ after 3 failed receive attempts
}
```

Messages that fail processing more than `max_receive_count` times are automatically routed to the `dead_letter_queue` channel by KubeMQ. You can consume DLQ messages with a separate worker or inspect them via the KubeMQ dashboard.

## Result Backend Configuration

The queue-peek result backend stores task results as KubeMQ Queue messages and retrieves them via non-destructive peek.

```python
app.conf.update(
    result_backend="kubemq://localhost:50000",
    result_expires=86400,  # 24 hours (KubeMQ maximum)
)
```

### Result Backend Transport Options

```python
app.conf.result_backend_transport_options = {
    "auth_token": "my-token",
    "tls_enabled": False,
    "tls_cert_file": "/path/to/cert.pem",
    "tls_key_file": "/path/to/key.pem",
    "tls_ca_file": "/path/to/ca.pem",
}
```

### Result Backend Notes

- Results are stored on per-task channels: `celery-result-{task_id}`
- Retrieval uses `peek_queue_messages()` (non-destructive) -- multiple callers can read the same result
- Maximum result expiration is 86400 seconds (24 hours), which is a KubeMQ limitation
- Celery's default `result_expires` of 24 hours matches the KubeMQ maximum
- State transitions (PENDING -> STARTED -> SUCCESS) purge and rewrite the result message
- Chord support uses Celery's polling fallback (`chord_unlock` task)

## Celery Settings Compatibility

| Celery Setting | Support | Notes |
|---------------|---------|-------|
| `broker_url` | Full | `kubemq://`, `kubemq+tls://`, `kubemq+async://`, `kubemq+async+tls://` schemes |
| `broker_transport_options` | Full | All options listed above |
| `broker_pool_limit` | Ignored | Single client per Channel; Kombu connection pool handles scaling |
| `broker_connection_timeout` | Full | Passed to SDK connection timeout |
| `broker_connection_retry` | Full | Standard Celery reconnection behavior |
| `broker_connection_max_retries` | Full | Standard Celery reconnection behavior |
| `broker_failover_strategy` | Ignored | Not supported in v1.0; KubeMQ cluster handles HA internally |
| `task_acks_late` | Full | See caveats below |
| `task_acks_on_failure_or_timeout` | Full | Standard Celery behavior |
| `task_reject_on_worker_lost` | Full | Maps to KubeMQ `nack()` |
| `task_default_queue` | Full | Queue name is sanitized for KubeMQ compatibility |
| `task_routes` | Full | Virtual exchange layer handles routing |
| `task_default_priority` | Stored | Priority stored in message tags (not enforced server-side) |
| `task_queue_max_priority` | Stored | Priority stored in message tags |
| `worker_prefetch_multiplier` | Full | Managed by Kombu's virtual QoS layer |
| `worker_concurrency` | Full | Standard Celery behavior |
| `result_backend` | Full | `kubemq://` for queue-peek result backend |
| `result_expires` | Full | Controls result message expiration (max 24 hours) |

### `task_acks_late` Caveats

When `task_acks_late=True`, messages are acknowledged after task completion rather than on receipt. This interacts with KubeMQ's transaction model:

- **Short tasks (< 60s):** Works as expected.
- **Long tasks (> 60s):** The KubeMQ server-side transaction timeout may expire before the task completes, causing the message to be redelivered. For long-running tasks, either:
  - Use `task_acks_late=False` (default) to ack on receipt
  - Accept at-least-once delivery semantics and ensure task idempotency

When `task_acks_late=False` (default), the transport uses `auto_ack=True` on receive, which is the safest and most performant option.

## Environment Variable Configuration

Celery supports environment-based configuration. Common patterns for KubeMQ:

```python
import os
import kubemq_celery
from celery import Celery

app = Celery("myapp")
app.conf.update(
    broker_url=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
    broker_transport_options={
        "auth_token": os.environ.get("KUBEMQ_AUTH_TOKEN"),
    },
)
```

## Full Configuration Example

```python
import kubemq_celery
from celery import Celery

app = Celery("myapp")

app.conf.update(
    # Broker
    broker_url="kubemq://kubemq.default.svc:50000",
    broker_transport_options={
        "wait_timeout": 1,
        "dead_letter_queue": "celery-dead-letters",
        "max_receive_count": 5,
        "client_id_prefix": "myapp",
        "max_send_size": 8_388_608,      # 8MB
        "max_receive_size": 8_388_608,   # 8MB
    },

    # Result backend
    result_backend="kubemq://kubemq.default.svc:50000",
    result_expires=86400,  # 24 hours

    # Task settings
    task_acks_late=False,
    task_default_queue="myapp-tasks",
    task_routes={
        "myapp.tasks.high_priority": {"queue": "high-priority"},
        "myapp.tasks.low_priority": {"queue": "low-priority"},
    },

    # Worker settings
    worker_prefetch_multiplier=1,
    worker_concurrency=4,
)
```
