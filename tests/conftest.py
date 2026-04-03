"""Test fixtures for kubemq-celery tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import kubemq_celery  # noqa: F401 -- triggers auto-registration


@pytest.fixture
def mock_queues_client():
    """Mock QueuesClient for unit tests."""
    client = MagicMock()
    client.send_queue_message.return_value = MagicMock(is_error=False)
    client.receive_queue_messages.return_value = MagicMock(messages=[])
    client.ack_all_queue_messages.return_value = 0
    client.list_queues_channels.return_value = []
    client.peek_queue_messages.return_value = MagicMock(messages=[])
    client.ping.return_value = MagicMock()
    client.close.return_value = None
    return client


@pytest.fixture
def mock_pubsub_client():
    """Mock PubSubClient for unit tests."""
    client = MagicMock()
    client.send_event.return_value = None
    client.subscribe_to_events.return_value = None
    client.close.return_value = None
    return client


@pytest.fixture
def celery_app():
    """Create a Celery app configured with KubeMQ transport for testing."""
    from celery import Celery

    app = Celery("test")
    app.config_from_object(
        {
            "broker_url": "kubemq://localhost:50000",
            "result_backend": "kubemq://localhost:50000",
            "task_always_eager": False,
        }
    )
    return app
