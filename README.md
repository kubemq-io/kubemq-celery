# kubemq-celery

KubeMQ transport and result backend for [Celery](https://docs.celeryq.dev/) task queues.

Use KubeMQ as your Celery message broker with a one-line configuration change. The only Kubernetes-native, in-cluster Celery broker.

## Install

```bash
pip install kubemq-celery
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add kubemq-celery
```

## Quickstart

```python
import kubemq_celery
from celery import Celery

app = Celery("myapp", broker="kubemq://localhost:50000")

@app.task
def add(x, y):
    return x + y
```

Start a worker:

```bash
celery -A myapp worker --loglevel=info
```

Send a task:

```python
add.delay(4, 6)
```

## Features

| Feature | Description |
|---------|-------------|
| **One-line setup** | `broker="kubemq://host:50000"` -- drop-in replacement for Redis/RabbitMQ |
| **Native ack/nack** | No visibility timeout bugs -- messages are explicitly acknowledged via gRPC |
| **Delayed delivery** | `countdown` and `eta` map to KubeMQ's native `delay_in_seconds` (no polling) |
| **Per-message TTL** | `message_expiration` transport option for automatic message expiration |
| **Dead letter queue** | Built-in DLQ via `max_receive_count` + `dead_letter_queue` transport options |
| **Batch receive** | `max_batch_size` fetches multiple messages per gRPC call for higher throughput |
| **Queue-peek results** | Optional result backend using non-destructive peek -- no external Redis/DB needed |
| **Full monitoring** | Flower, `celery inspect`, `celery control` -- all work via KubeMQ Events fanout |
| **TLS support** | `kubemq+tls://` for encrypted gRPC connections, mTLS for mutual authentication |
| **Async transport** | `kubemq+async://` for native asyncio I/O with `--pool=asyncio` workers |
| **Kubernetes-native** | In-cluster broker with auto-clustering, gRPC keep-alive, KEDA autoscaling |
| **Auto-registration** | `import kubemq_celery` registers the `kubemq://` URL scheme automatically |

## Result Backend

Enable the queue-peek result backend for a pure KubeMQ stack:

```python
app = Celery(
    "myapp",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

result = add.delay(4, 6)
print(result.get(timeout=10))  # 10
```

Results are stored as KubeMQ Queue messages and retrieved via non-destructive peek. Multiple callers can read the same result.

## Configuration

```python
app.conf.broker_transport_options = {
    "wait_timeout": 1,                          # receive timeout (seconds)
    "dead_letter_queue": "celery-dead-letters",  # DLQ channel
    "max_receive_count": 3,                      # attempts before DLQ
    "auth_token": "my-token",                    # KubeMQ auth
}
```

See [docs/configuration.md](docs/configuration.md) for the full reference.

## Why KubeMQ over Redis/RabbitMQ?

| | Redis | RabbitMQ | KubeMQ |
|---|---|---|---|
| **Acknowledgment** | Visibility timeout | Native AMQP ack | Native gRPC ack |
| **Delayed delivery** | Client-side polling | Plugin required | Native `delay_in_seconds` |
| **Kubernetes** | External StatefulSet | External + Erlang | K8s-native, auto-clustering |
| **Connection stability** | TCP reset under load | Stable | gRPC keep-alive |
| **Setup complexity** | Moderate | High | Low |

## Documentation

- [Getting Started](docs/getting-started.md) -- 5-minute quickstart guide
- [Configuration Reference](docs/configuration.md) -- all transport and backend options
- [Migration Guide](docs/migration-guide.md) -- switching from Redis or RabbitMQ
- [Kubernetes Deployment](docs/kubernetes.md) -- production K8s with KEDA autoscaling
- [Troubleshooting & FAQ](docs/troubleshooting.md) -- common issues and vs-Redis comparison
- [Error Handling Guide](docs/error-handling.md) -- retries, DLQ, acks, Sentry integration
- [Performance Guide](docs/performance.md) -- tuning, benchmarking, workload profiles

## Examples

See [examples/README.md](examples/README.md) for the full guide with 95+ examples across 14 categories.

### Quick Start
- [quickstart/hello_world.py](examples/quickstart/hello_world.py) — minimal Celery + KubeMQ
- [quickstart/send_and_get_result.py](examples/quickstart/send_and_get_result.py) — task dispatch and results

### Connection & Configuration
- [connection/basic_broker.py](examples/connection/basic_broker.py) — basic `kubemq://` connection
- [connection/tls_connection.py](examples/connection/tls_connection.py) — TLS encryption
- [connection/auth_token.py](examples/connection/auth_token.py) — authentication

### Core Features
- [canvas/](examples/canvas/) — chain, group, chord, starmap, chunks (10 scripts)
- [error_handling/](examples/error_handling/) — retry, DLQ, acks_late, reconnection (10 scripts)
- [result_backend/](examples/result_backend/) — store, expiry, states (7 scripts)
- [scheduling/](examples/scheduling/) — countdown, ETA, Beat crontab (8 scripts)
- [routing/](examples/routing/) — task routes, priority, broadcast (6 scripts)

### Monitoring & Operations
- [monitoring/](examples/monitoring/) — Flower, inspect, events (6 scripts)
- [signals/](examples/signals/) — task lifecycle signals (6 scripts)
- [rate_limiting/](examples/rate_limiting/) — concurrency, prefetch (5 scripts)
- [serialization/](examples/serialization/) — JSON, pickle, msgpack, custom (5 scripts)

### Testing & Advanced
- [testing/](examples/testing/) — eager mode, pytest, mocking (5 scripts)
- [advanced_patterns/](examples/advanced_patterns/) — inheritance, idempotency, benchmark (7 scripts)

### Framework Integrations
- [integrations/fastapi_integration.py](examples/integrations/fastapi_integration.py) — FastAPI + Celery
- [integrations/flask_integration.py](examples/integrations/flask_integration.py) — Flask + Celery
- [integrations/django_integration/](examples/integrations/django_integration/) — Django + Beat + Results
- [integrations/starlette_integration.py](examples/integrations/starlette_integration.py) — Starlette
- [integrations/litestar_integration.py](examples/integrations/litestar_integration.py) — Litestar
- [integrations/aiohttp_integration.py](examples/integrations/aiohttp_integration.py) — aiohttp

### Kubernetes & Deployment
- [kubernetes/](examples/kubernetes/) — Dockerfile, Docker Compose, K8s, KEDA

## Known Limitations

- **Priority is metadata-only**: KubeMQ does not enforce message priority at the server level.
- **Max delay/expiration**: 24 hours (86400 seconds). Capped with warning log.
- **Chord uses polling fallback**: chord_unlock task polls for group completion.
- **Long-running tasks with task_acks_late=True**: KubeMQ transaction timeout may expire.

## Requirements

- Python >= 3.10
- KubeMQ broker (Docker, Kubernetes, or standalone)
- Celery >= 5.4
- Kombu >= 5.4

## License

MIT
