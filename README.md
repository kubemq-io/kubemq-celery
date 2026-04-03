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
| **Dead letter queue** | Built-in DLQ via `max_receive_count` + `dead_letter_queue` transport options |
| **Queue-peek results** | Optional result backend using non-destructive peek -- no external Redis/DB needed |
| **Full monitoring** | Flower, `celery inspect`, `celery control` -- all work via KubeMQ Events fanout |
| **TLS support** | `kubemq+tls://` for encrypted gRPC connections, mTLS for mutual authentication |
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

## Examples

- [basic_task.py](examples/basic_task.py) -- minimal Celery app
- [priority_routing.py](examples/priority_routing.py) -- multi-queue with priority routing
- [delayed_tasks.py](examples/delayed_tasks.py) -- ETA and countdown tasks
- [monitoring.py](examples/monitoring.py) -- Flower monitoring integration
- [fastapi_integration.py](examples/fastapi_integration.py) -- FastAPI + Celery async pattern
- [kubernetes/](examples/kubernetes/) -- Docker Compose, K8s deployment, KEDA autoscaling

## Requirements

- Python >= 3.11
- KubeMQ broker (Docker, Kubernetes, or standalone)
- Celery >= 5.4
- Kombu >= 5.4

## License

MIT
