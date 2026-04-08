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
