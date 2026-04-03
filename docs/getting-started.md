# Getting Started with kubemq-celery

Get a Celery app running on KubeMQ in under 5 minutes.

## Prerequisites

- **Python >= 3.11**
- **KubeMQ broker** running and accessible (default: `localhost:50000`)
  - Docker: `docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq:latest`
  - Kubernetes: see [Kubernetes Deployment Guide](kubernetes.md)

## Install

```bash
# pip
pip install kubemq-celery

# uv (recommended)
uv add kubemq-celery
```

## Quickstart

### 1. Create your Celery app

Create a file called `tasks.py`:

```python
import kubemq_celery  # registers kubemq:// transport
from celery import Celery

app = Celery("myapp", broker="kubemq://localhost:50000")

@app.task
def add(x, y):
    return x + y
```

### 2. Start a worker

```bash
celery -A tasks worker --loglevel=info
```

You should see output like:

```
[config]
.> broker:      kubemq://localhost:50000
.> results:     disabled://

[queues]
.> celery       exchange=celery(direct) key=celery

[2026-04-03 12:00:00,000: INFO/MainProcess] Connected to kubemq://localhost:50000
[2026-04-03 12:00:00,100: INFO/MainProcess] celery@hostname ready.
```

### 3. Send a task

Open a Python shell (or another script):

```python
from tasks import add

result = add.delay(4, 6)
print(f"Task ID: {result.id}")
```

In the worker terminal, you should see the task execute:

```
[2026-04-03 12:00:05,000: INFO/MainProcess] Task tasks.add[abc123] received
[2026-04-03 12:00:05,010: INFO/MainProcess] Task tasks.add[abc123] succeeded in 0.01s: 10
```

## Enable Result Backend

To retrieve task results, enable the queue-peek result backend:

```python
import kubemq_celery
from celery import Celery

app = Celery(
    "myapp",
    broker="kubemq://localhost:50000",
    result_backend="kubemq://localhost:50000",
)

@app.task
def add(x, y):
    return x + y
```

Now you can get results:

```python
from tasks import add

result = add.delay(4, 6)
print(result.get(timeout=10))  # Output: 10
```

The result backend uses KubeMQ's non-destructive `peek_queue_messages()` for retrieval, so multiple callers can read the same result without consuming it.

## Docker Compose Quickstart

For a zero-install demo with KubeMQ broker + worker + task sender, use the provided Docker Compose file:

```bash
cd examples/kubernetes
docker compose up -d
```

This starts:
- KubeMQ broker on port 50000 (dashboard on port 9090)
- A Celery worker connected to the broker

See [examples/kubernetes/docker-compose.yaml](../examples/kubernetes/docker-compose.yaml) for the full configuration.

## Next Steps

- [Configuration Reference](configuration.md) -- all transport options, TLS, result backend settings
- [Migration Guide](migration-guide.md) -- switching from Redis or RabbitMQ
- [Kubernetes Deployment](kubernetes.md) -- production K8s deployment with KEDA autoscaling
- [Examples](../examples/) -- priority routing, delayed tasks, FastAPI integration, and more
