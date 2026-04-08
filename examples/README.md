# KubeMQ Celery Transport — Examples

Comprehensive example collection demonstrating the KubeMQ Celery transport integration. 93 standalone example scripts covering everything from basic usage to production patterns.

## Prerequisites

- **Python 3.10+**
- **KubeMQ broker** running on `localhost:50000` (or set `CELERY_BROKER_URL`)
- **kubemq-celery** installed: `pip install kubemq-celery`

**Note:** Examples require cloning the repo — they are not included in the `pip install kubemq-celery` package.

## Quick Start

```bash
# 1. Clone the repo and install the transport
git clone https://github.com/kubemq/kubemq-celery.git && cd kubemq-celery
pip install kubemq-celery

# 2. Start a KubeMQ broker (Docker)
docker run -d --name kubemq -p 50000:50000 kubemq/kubemq-community:latest

# 3. Run a basic example (no worker needed for eager mode)
python examples/testing/eager_mode.py

# 4. Or start a worker + send tasks
celery -A examples.quickstart.hello_world worker --loglevel=info
python examples/quickstart/hello_world.py
```

## Examples by Category

### Quickstart (`quickstart/`)

| Script | Description |
|--------|-------------|
| `hello_world.py` | Minimal hello-world task with KubeMQ broker |
| `send_and_get_result.py` | Send a task and retrieve its result |
| `worker_setup.py` | Worker configuration and startup options |

### Connection & Configuration (`connection/`)

| Script | Description |
|--------|-------------|
| `basic_broker.py` | Basic `kubemq://` broker connection |
| `auth_token.py` | Authentication token configuration |
| `tls_connection.py` | TLS-encrypted broker connection |
| `mtls_connection.py` | Mutual TLS (mTLS) client certificate auth |
| `connection_timeout.py` | Connection timeout and retry settings |
| `env_var_config.py` | Environment variable-based configuration |
| `grpc_options.py` | Custom gRPC channel options |
| `result_backend_config.py` | Result backend configuration options |

### Result Backend (`result_backend/`)

| Script | Description |
|--------|-------------|
| `store_and_retrieve.py` | Store and retrieve task results |
| `result_expiration.py` | Result TTL and expiration settings |
| `result_backend_options.py` | Backend transport options |
| `custom_result_serializer.py` | Custom result serializer configuration |
| `group_results.py` | Group result collection and aggregation |
| `ignore_result.py` | `ignore_result=True` for fire-and-forget tasks |
| `task_states.py` | Task state transitions (PENDING → STARTED → SUCCESS) |

### Canvas Workflows (`canvas/`)

| Script | Description |
|--------|-------------|
| `chain.py` | Sequential task pipeline with result passing |
| `group.py` | Parallel task execution with result collection |
| `chord.py` | Group + callback pattern |
| `chord_error_handling.py` | Error handling within chord workflows |
| `chain_of_groups.py` | Nested chain-of-groups composition |
| `chunks.py` | Splitting large iterables into batches |
| `starmap.py` | Parallel execution over an iterable |
| `map.py` | Map pattern for parallel transforms |
| `immutable_signatures.py` | Immutable signatures (`.si()`) that ignore parent results |
| `complex_workflow.py` | Multi-stage production workflow combining primitives |

### Error Handling (`error_handling/`)

| Script | Description |
|--------|-------------|
| `basic_retry.py` | Basic task retry with `max_retries` |
| `exponential_backoff.py` | Exponential backoff retry strategy |
| `custom_retry_policy.py` | Custom retry policies and exception filtering |
| `dead_letter_queue.py` | Dead letter queue for failed messages |
| `error_callbacks.py` | Error callbacks with `link_error` |
| `on_failure_callback.py` | `on_failure` hook for error notification |
| `reconnection.py` | Broker reconnection handling |
| `reject_and_requeue.py` | Message rejection and requeue patterns |
| `task_acks_late.py` | Late acknowledgment for reliability |
| `task_time_limit.py` | Soft and hard time limits |

### Scheduling (`scheduling/`)

| Script | Description |
|--------|-------------|
| `countdown_delay.py` | `countdown=` parameter for delayed execution |
| `eta_scheduling.py` | `eta=` parameter for exact-time scheduling |
| `beat_crontab.py` | Celery Beat with crontab schedules |
| `beat_timedelta.py` | Celery Beat with timedelta intervals |
| `beat_solar.py` | Solar event-based scheduling (sunrise/sunset) |
| `beat_max_delay_warning.py` | KubeMQ max delay warning (86400s limit) |
| `dynamic_periodic_tasks.py` | Runtime-modifiable periodic task schedules |
| `one_shot_scheduled.py` | One-shot scheduled task at a future time |

### Signals (`signals/`)

| Script | Description |
|--------|-------------|
| `task_prerun_postrun.py` | `task_prerun` / `task_postrun` signal handlers |
| `task_success_failure.py` | `task_success` / `task_failure` signals |
| `task_revoked.py` | `task_revoked` signal handling |
| `before_task_publish.py` | `before_task_publish` signal for message interception |
| `worker_signals.py` | Worker lifecycle signals (init, ready, shutdown) |
| `custom_state_updates.py` | Custom state updates via `update_state()` |

### Rate Limiting (`rate_limiting/`)

| Script | Description |
|--------|-------------|
| `rate_limit.py` | Per-task rate limiting (`rate_limit='10/m'`) |
| `worker_concurrency.py` | Worker concurrency configuration |
| `prefetch_multiplier.py` | Prefetch multiplier tuning |
| `task_time_limits.py` | Soft/hard time limits per task |
| `worker_pool_options.py` | Worker pool types (prefork, solo, threads) |

### Serialization (`serialization/`)

| Script | Description |
|--------|-------------|
| `json_serializer.py` | Default JSON serializer with round-trip validation |
| `pickle_serializer.py` | Pickle for complex Python objects (with security warning) |
| `msgpack_serializer.py` | MessagePack for compact binary encoding (requires `msgpack`) |
| `custom_serializer.py` | Custom serializer via `kombu.serialization.register()` (e.g., orjson) |
| `content_type_negotiation.py` | Per-task serializer override and content-type negotiation |

### Testing (`testing/`)

| Script | Description |
|--------|-------------|
| `eager_mode.py` | `task_always_eager=True` for synchronous testing without broker |
| `pytest_fixtures.py` | `celery.contrib.pytest` fixtures with KubeMQ config |
| `mock_tasks.py` | `unittest.mock.patch` for mocking task calls |
| `test_canvas.py` | Testing canvas workflows (chain, group, chord) in eager mode |
| `local_development.py` | Dev setup: `--pool=solo`, `--loglevel=debug`, `--autoreload` |

### Advanced Patterns (`advanced_patterns/`)

| Script | Description |
|--------|-------------|
| `task_inheritance.py` | Base task class with `on_failure`, `on_retry`, `on_success` hooks |
| `task_binding.py` | `bind=True`, `self.request.id`, `self.request.retries`, `update_state()` |
| `idempotent_tasks.py` | Deduplication with deterministic `task_id` in `apply_async()` |
| `task_annotations.py` | `task_annotations` config for cross-cutting rate limits and time limits |
| `multi_app.py` | Multiple `Celery()` apps with different KubeMQ brokers |
| `custom_task_class.py` | Extended `celery.Task` with before/after hooks and metrics |
| `benchmark.py` | Performance benchmark: dispatch rate, p50/p95/p99 latency with argparse CLI |

### Monitoring (`monitoring/`)

| Script | Description |
|--------|-------------|
| `flower_setup.py` | Flower real-time web monitor setup for KubeMQ |
| `celery_events.py` | Celery events consumer for task lifecycle tracking |
| `inspect_active.py` | Inspect active, reserved, and scheduled tasks |
| `progress_tracking.py` | Custom state progress tracking with `update_state()` |
| `custom_event_consumer.py` | Custom event consumer for task analytics |
| `worker_control.py` | Runtime worker control commands (pool size, rate limits) |

### Routing (`routing/`)

| Script | Description |
|--------|-------------|
| `task_routes.py` | Static task-to-queue routing configuration |
| `dynamic_routing.py` | Dynamic queue routing based on task arguments |
| `priority_queues.py` | Priority queue configuration and task dispatch |
| `dedicated_workers.py` | Dedicated workers consuming from specific queues |
| `broadcast_tasks.py` | Broadcast tasks to all workers |
| `default_queue_config.py` | Default queue and exchange configuration |

### Framework Integrations (`integrations/`)

| Script | Description |
|--------|-------------|
| `fastapi_integration.py` | FastAPI async endpoints + Celery task dispatch and status |
| `fastapi_websocket_progress.py` | WebSocket endpoint for real-time task progress streaming |
| `flask_integration.py` | Flask routes + Celery task dispatch |
| `flask_factory_pattern.py` | Flask application factory + Celery init (Flask 3.x pattern) |
| `starlette_integration.py` | Starlette ASGI + Celery with `kubemq+async://` |
| `litestar_integration.py` | Litestar ASGI + Celery with `kubemq+async://` |
| `aiohttp_integration.py` | aiohttp web server + Celery task dispatch |

### Django Integration (`integrations/django_integration/`)

| File | Description |
|------|-------------|
| `myproject/settings.py` | Django settings with KubeMQ Celery config |
| `myproject/celery.py` | Celery app initialization for Django |
| `myproject/urls.py` | URL routing for task endpoints |
| `myapp/tasks.py` | Task definitions |
| `myapp/views.py` | View-based task dispatch |
| `manage.py` | Django management entry point |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CELERY_BROKER_URL` | `kubemq://localhost:50000` | KubeMQ broker URL |
| `CELERY_RESULT_BACKEND` | `kubemq://localhost:50000` | Result backend URL |
| `CELERY_ENV` | `development` | Environment (affects logging/config in some examples) |

## Optional Dependencies

Some examples require additional packages:

```bash
# Framework integrations
pip install fastapi uvicorn          # FastAPI examples
pip install flask                     # Flask examples
pip install starlette                 # Starlette examples
pip install litestar                  # Litestar examples
pip install aiohttp                   # aiohttp examples
pip install django                    # Django examples

# Serialization
pip install msgpack                   # MessagePack serializer
pip install orjson                    # Custom serializer (orjson)

# Testing
pip install pytest                    # Pytest fixtures example

# WebSocket
pip install websockets                # FastAPI WebSocket progress
```
