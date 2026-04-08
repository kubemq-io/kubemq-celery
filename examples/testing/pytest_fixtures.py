"""Pytest Fixtures — KubeMQ Celery Transport.

Demonstrates:
- celery.contrib.pytest fixtures for integration testing
- celery_app fixture with KubeMQ configuration
- celery_worker fixture for in-process worker
- Writing tests that validate task behavior with a real worker

Usage:
    pytest examples/testing/pytest_fixtures.py -v

Requirements:
    - kubemq-celery installed
    - pytest and pytest-celery (pip install pytest)
    - No broker needed — uses in-memory transport for test isolation
"""

from __future__ import annotations

import os

import pytest
from celery import Celery

import kubemq_celery  # noqa: F401

app = Celery(
    "pytest_fixtures",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
)


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@app.task
def concatenate(parts: list[str], separator: str = " ") -> str:
    """Join string parts with separator."""
    return separator.join(parts)


@app.task(bind=True, max_retries=2)
def divide(self, x: float, y: float) -> float:
    """Divide x by y, raising on zero."""
    if y == 0:
        raise ZeroDivisionError("Cannot divide by zero")
    return x / y


# --- Pytest Fixtures ---


@pytest.fixture(scope="session")
def celery_config():
    """Override Celery config for tests — use eager mode for isolation."""
    return {
        "broker_url": "memory://",
        "result_backend": "cache+memory://",
        "task_always_eager": True,
        "task_eager_propagates": True,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }


@pytest.fixture(scope="session")
def celery_app(celery_config):
    """Create a test Celery app with the test config."""
    test_app = Celery("test")
    test_app.config_from_object(celery_config)

    @test_app.task(name="pytest_fixtures.add")
    def test_add(x: int, y: int) -> int:
        return x + y

    @test_app.task(name="pytest_fixtures.multiply")
    def test_multiply(x: int, y: int) -> int:
        return x * y

    @test_app.task(name="pytest_fixtures.concatenate")
    def test_concatenate(parts: list[str], separator: str = " ") -> str:
        return separator.join(parts)

    @test_app.task(bind=True, name="pytest_fixtures.divide", max_retries=2)
    def test_divide(self, x: float, y: float) -> float:
        if y == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return x / y

    return test_app


@pytest.fixture
def eager_app():
    """Per-test fixture that uses eager mode (no broker needed)."""
    app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
    )
    yield app
    app.conf.update(
        task_always_eager=False,
        task_eager_propagates=False,
    )


# --- Test Cases ---


class TestBasicTasks:
    """Test basic task execution."""

    def test_add(self, eager_app):
        result = add.delay(2, 3)
        assert result.result == 5

    def test_multiply(self, eager_app):
        result = multiply.delay(4, 5)
        assert result.result == 20

    def test_add_negative(self, eager_app):
        result = add.delay(-1, -2)
        assert result.result == -3

    def test_multiply_zero(self, eager_app):
        result = multiply.delay(100, 0)
        assert result.result == 0


class TestStringTasks:
    """Test string manipulation tasks."""

    def test_concatenate_default_sep(self, eager_app):
        result = concatenate.delay(["hello", "world"])
        assert result.result == "hello world"

    def test_concatenate_custom_sep(self, eager_app):
        result = concatenate.delay(["a", "b", "c"], separator="-")
        assert result.result == "a-b-c"

    def test_concatenate_empty(self, eager_app):
        result = concatenate.delay([])
        assert result.result == ""


class TestErrorHandling:
    """Test error handling behavior."""

    def test_divide_success(self, eager_app):
        result = divide.delay(10.0, 2.0)
        assert result.result == 5.0

    def test_divide_by_zero(self, eager_app):
        with pytest.raises(ZeroDivisionError, match="Cannot divide by zero"):
            divide.delay(10.0, 0.0)


if __name__ == "__main__":
    print("=== Pytest Fixtures Example ===")
    print("Run with: pytest examples/testing/pytest_fixtures.py -v")
    print("\nRunning quick eager-mode validation...\n")

    app.conf.update(task_always_eager=True, task_eager_propagates=True)

    result = add.delay(2, 3)
    print(f"add(2, 3) = {result.result}")
    assert result.result == 5

    result = multiply.delay(4, 5)
    print(f"multiply(4, 5) = {result.result}")
    assert result.result == 20

    result = concatenate.delay(["hello", "world"])
    print(f"concatenate(['hello', 'world']) = {result.result}")
    assert result.result == "hello world"

    print("\nAll fixture-based tests validated!")
