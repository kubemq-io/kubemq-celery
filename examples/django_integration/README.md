# Django + KubeMQ Celery Integration

Complete Django project example using KubeMQ as the Celery broker and result backend.

## Features

- Three production-realistic tasks (email, report generation, file processing)
- Static `beat_schedule` for periodic tasks (crontab + timedelta)
- django-celery-beat support for dynamic schedules (optional)
- Docker Compose for full-stack local development
- Environment-variable configuration for production deployment

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
# or
uv pip install -r requirements.txt
```

### 2. Start KubeMQ broker

```bash
docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq:latest
```

### 3. Run Django

```bash
python manage.py runserver
```

### 4. Start Celery worker

```bash
celery -A myproject worker --loglevel=info
```

### 5. Start Celery Beat (periodic tasks)

```bash
celery -A myproject beat --loglevel=info
```

### 6. Test

```bash
# Dispatch an email task
curl -X POST http://localhost:8000/tasks/send-email/ \
     -H "Content-Type: application/json" \
     -d '{"to": "user@example.com", "subject": "Hello", "body": "Test email"}'

# Check task status
curl http://localhost:8000/tasks/status/<task_id>/
```

## Docker Compose (Full Stack)

Run everything in containers:

```bash
docker compose up -d
```

This starts:
- KubeMQ broker (dashboard at http://localhost:9090)
- Django web server (http://localhost:8000)
- Celery worker
- Celery Beat scheduler

## Static vs Dynamic Schedules

### Static (beat_schedule in settings.py)

Defined at deploy time in `myproject/settings.py`:

```python
CELERY_BEAT_SCHEDULE = {
    "cleanup-old-data": {
        "task": "myapp.tasks.cleanup_old_data",
        "schedule": crontab(hour=0, minute=0),
    },
    "health-check": {
        "task": "myapp.tasks.health_check",
        "schedule": timedelta(minutes=5),
    },
}
```

### Dynamic (django-celery-beat)

For runtime-editable periodic tasks via Django admin:

1. Install: `pip install django-celery-beat`
2. Add `"django_celery_beat"` to `INSTALLED_APPS`
3. Set `CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"`
4. Run `python manage.py migrate`
5. Start beat: `celery -A myproject beat --loglevel=info`
6. Add/edit schedules in Django admin at `/admin/django_celery_beat/`

## Project Structure

```
django_integration/
  myproject/
    __init__.py
    celery.py        # Celery app factory with autodiscover_tasks()
    settings.py      # Django settings with KubeMQ Celery config
    urls.py          # URL routing
  myapp/
    __init__.py
    tasks.py         # Task definitions (send_email, generate_report, process_upload)
    views.py         # Views that dispatch tasks and check results
  manage.py          # Django management script
  requirements.txt   # Dependencies
  docker-compose.yaml # Full-stack Docker setup
  README.md          # This file
```
