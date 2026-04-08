# Kubernetes Deployment Guide

Deploy Celery workers with KubeMQ broker on Kubernetes.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│ Kubernetes Cluster                               │
│                                                  │
│  ┌──────────────┐    gRPC     ┌──────────────┐  │
│  │ Celery Worker │ ─────────► │   KubeMQ     │  │
│  │ Deployment    │            │   StatefulSet │  │
│  │ (N replicas)  │ ◄───────── │   (HA cluster)│  │
│  └──────────────┘             └──────────────┘  │
│         │                           │            │
│  ┌──────▼──────┐             ┌──────▼──────┐    │
│  │ KEDA         │             │ KubeMQ       │    │
│  │ ScaledObject │             │ Dashboard    │    │
│  │ (autoscale)  │             │ :9090        │    │
│  └─────────────┘             └─────────────┘    │
└─────────────────────────────────────────────────┘
```

## 1. Deploy KubeMQ Broker

### Quick Deploy

```bash
kubectl apply -f https://get.kubemq.io/deploy
```

This creates a KubeMQ StatefulSet in the `default` namespace with:
- gRPC service on port 50000
- Dashboard on port 9090
- Persistent storage

### Custom Deployment

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kubemq
  namespace: default
spec:
  serviceName: kubemq
  replicas: 3  # HA cluster
  selector:
    matchLabels:
      app: kubemq
  template:
    metadata:
      labels:
        app: kubemq
    spec:
      containers:
        - name: kubemq
          image: kubemq/kubemq:latest
          ports:
            - containerPort: 50000
              name: grpc
            - containerPort: 9090
              name: dashboard
          env:
            - name: KUBEMQ_TOKEN
              valueFrom:
                secretKeyRef:
                  name: kubemq-license
                  key: token
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 1Gi
          readinessProbe:
            tcpSocket:
              port: 50000
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            tcpSocket:
              port: 50000
            initialDelaySeconds: 15
            periodSeconds: 20
---
apiVersion: v1
kind: Service
metadata:
  name: kubemq
  namespace: default
spec:
  selector:
    app: kubemq
  ports:
    - name: grpc
      port: 50000
      targetPort: 50000
    - name: dashboard
      port: 9090
      targetPort: 9090
  type: ClusterIP
```

The KubeMQ service is accessible within the cluster at `kubemq.default.svc:50000`.

## 2. Deploy Celery Workers

### Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celery-worker
  namespace: default
  labels:
    app: celery-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: celery-worker
  template:
    metadata:
      labels:
        app: celery-worker
    spec:
      containers:
        - name: worker
          image: myapp:latest
          command:
            - celery
            - -A
            - myapp
            - worker
            - --loglevel=info
            - --concurrency=4
          env:
            - name: CELERY_BROKER_URL
              value: "kubemq://kubemq.default.svc:50000"
            - name: CELERY_RESULT_BACKEND
              value: "kubemq://kubemq.default.svc:50000"
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 512Mi
          readinessProbe:
            exec:
              command:
                - celery
                - -A
                - myapp
                - inspect
                - ping
                - --timeout=5
            initialDelaySeconds: 30
            periodSeconds: 30
            timeoutSeconds: 10
          livenessProbe:
            exec:
              command:
                - celery
                - -A
                - myapp
                - inspect
                - ping
                - --timeout=5
            initialDelaySeconds: 60
            periodSeconds: 60
            timeoutSeconds: 10
```

### Worker Configuration

Your Celery app should read the broker URL from environment variables:

```python
import os
import kubemq_celery
from celery import Celery

app = Celery("myapp")
app.conf.update(
    broker_url=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
    worker_prefetch_multiplier=1,
    task_acks_late=False,
)
```

### Worker Dockerfile

A production-ready Dockerfile is provided at [`examples/kubernetes/Dockerfile`](../examples/kubernetes/Dockerfile):

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install uv and dependencies
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml .
RUN uv pip install --system --no-cache .

# Copy application code
COPY src/ src/
COPY examples/ examples/

ENV PYTHONPATH=/app/src:/app/examples

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import kubemq_celery; print('ok')" || exit 1

CMD ["celery", "-A", "basic_task", "worker", "--loglevel=info"]
```

### Multi-Worker Docker Compose

The provided [`examples/kubernetes/docker-compose.yaml`](../examples/kubernetes/docker-compose.yaml) includes a multi-worker setup with:
- 2 workers on different queues (default + high-priority)
- 1 Beat scheduler for periodic tasks
- 1 KubeMQ broker with health checks

```bash
cd examples/kubernetes
docker compose up -d
```

## 3. Service Discovery

Workers connect to the KubeMQ broker via the Kubernetes service DNS name:

```
kubemq://<service-name>.<namespace>.svc:<port>
```

Common patterns:

| Scenario | Broker URL |
|----------|-----------|
| Same namespace | `kubemq://kubemq:50000` |
| Explicit namespace | `kubemq://kubemq.default.svc:50000` |
| FQDN | `kubemq://kubemq.default.svc.cluster.local:50000` |
| With auth | `kubemq://:my-token@kubemq.default.svc:50000` |
| With TLS | `kubemq+tls://kubemq.default.svc:50000` |

## 4. KEDA Autoscaling

[KEDA](https://keda.sh/) can autoscale Celery workers based on KubeMQ queue depth. This is more efficient than HPA (which only scales on CPU/memory).

### Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace
```

### ScaledObject for Queue Depth

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: celery-worker-scaler
  namespace: default
spec:
  scaleTargetRef:
    name: celery-worker
  minReplicaCount: 1
  maxReplicaCount: 20
  pollingInterval: 10       # check queue depth every 10 seconds
  cooldownPeriod: 60        # wait 60s before scaling down
  triggers:
    - type: kubemq
      metadata:
        address: "kubemq.default.svc:50000"
        channel: "celery"
        queueLength: "10"   # scale up when > 10 messages queued
```

### Multi-Queue Scaling

For applications with priority routing, create separate ScaledObjects per queue:

```yaml
# High-priority queue -- aggressive scaling
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: celery-high-priority-scaler
spec:
  scaleTargetRef:
    name: celery-worker-high
  minReplicaCount: 2    # always keep 2 replicas
  maxReplicaCount: 50
  triggers:
    - type: kubemq
      metadata:
        address: "kubemq.default.svc:50000"
        channel: "high-priority"
        queueLength: "5"  # scale earlier for high priority
---
# Low-priority queue -- conservative scaling
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: celery-low-priority-scaler
spec:
  scaleTargetRef:
    name: celery-worker-low
  minReplicaCount: 0    # scale to zero when idle
  maxReplicaCount: 10
  triggers:
    - type: kubemq
      metadata:
        address: "kubemq.default.svc:50000"
        channel: "low-priority"
        queueLength: "50"  # more tolerant for low priority
```

## 5. Resource Recommendations

### KubeMQ Broker

| Load | CPU Request | CPU Limit | Memory Request | Memory Limit | Replicas |
|------|-------------|-----------|----------------|--------------|----------|
| Low (< 100 msg/s) | 250m | 500m | 256Mi | 512Mi | 1 |
| Medium (100-1000 msg/s) | 500m | 1000m | 512Mi | 1Gi | 3 |
| High (> 1000 msg/s) | 1000m | 2000m | 1Gi | 2Gi | 3 |

### Celery Workers

| Concurrency | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-------------|-------------|-----------|----------------|--------------|
| 1 (I/O bound) | 100m | 500m | 128Mi | 256Mi |
| 4 (default) | 250m | 1000m | 256Mi | 512Mi |
| 8 (CPU bound) | 500m | 2000m | 512Mi | 1Gi |

## 6. Health Checks

### Celery Worker Health

The transport implements `verify_connection()` which pings the KubeMQ broker. Celery's built-in inspect commands use this:

```bash
# Check if workers are alive
celery -A myapp inspect ping

# List active tasks
celery -A myapp inspect active

# Check worker stats
celery -A myapp inspect stats
```

### KubeMQ Broker Health

KubeMQ exposes health endpoints:

```yaml
readinessProbe:
  tcpSocket:
    port: 50000
  initialDelaySeconds: 5
  periodSeconds: 10
livenessProbe:
  tcpSocket:
    port: 50000
  initialDelaySeconds: 15
  periodSeconds: 20
```

## 7. Monitoring

### Flower (Celery Monitoring)

Deploy Flower alongside your workers:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flower
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flower
  template:
    metadata:
      labels:
        app: flower
    spec:
      containers:
        - name: flower
          image: mher/flower:latest
          command:
            - celery
            - -A
            - myapp
            - flower
            - --port=5555
          env:
            - name: CELERY_BROKER_URL
              value: "kubemq://kubemq.default.svc:50000"
          ports:
            - containerPort: 5555
---
apiVersion: v1
kind: Service
metadata:
  name: flower
spec:
  selector:
    app: flower
  ports:
    - port: 5555
      targetPort: 5555
  type: ClusterIP
```

### KubeMQ Dashboard

The KubeMQ dashboard provides broker-level monitoring at port 9090:

```bash
# Port-forward to access dashboard locally
kubectl port-forward svc/kubemq 9090:9090

# Open http://localhost:9090 in your browser
```

The dashboard shows queue depth, message rates, and channel statistics -- complementing Flower's task-level monitoring.

## 8. Production Checklist

- [ ] KubeMQ broker deployed with >= 3 replicas for HA
- [ ] Persistent volume claims configured for KubeMQ data
- [ ] Authentication token configured (broker + workers)
- [ ] TLS enabled for gRPC connections (`kubemq+tls://`)
- [ ] Worker resource requests and limits set
- [ ] Readiness and liveness probes configured
- [ ] KEDA ScaledObject configured for queue-depth autoscaling
- [ ] Flower deployed for task monitoring
- [ ] Dead letter queue configured for failed messages
- [ ] `result_expires` set to <= 86400 (24 hours)
- [ ] Worker `--concurrency` matched to workload type (I/O vs CPU bound)
- [ ] Network policies restrict broker access to worker namespaces
