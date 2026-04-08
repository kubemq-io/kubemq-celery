"""Test fixtures for kubemq-celery tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from kubemq.queues import QueueMessage

import kubemq_celery  # noqa: F401 -- triggers auto-registration
from kubemq_celery.transport import Channel

# ---------------------------------------------------------------------------
# Relax QueueMessage SDK validation for unit tests.
# The SDK's MAX_EXPIRATION_SECONDS/MAX_DELAY_SECONDS may be lower (43200)
# than the values used by the transport code (86400). Since unit tests use
# mocked clients, we raise the SDK limits to match the transport.
# ---------------------------------------------------------------------------
_orig_post_init = QueueMessage.__post_init__


def _relaxed_post_init(self):
    """Allow up to 86400s expiration/delay for tests."""
    saved_exp = QueueMessage.MAX_EXPIRATION_SECONDS
    saved_del = QueueMessage.MAX_DELAY_SECONDS
    QueueMessage.MAX_EXPIRATION_SECONDS = max(saved_exp, 86400)
    QueueMessage.MAX_DELAY_SECONDS = max(saved_del, 86400)
    try:
        _orig_post_init(self)
    finally:
        QueueMessage.MAX_EXPIRATION_SECONDS = saved_exp
        QueueMessage.MAX_DELAY_SECONDS = saved_del


@pytest.fixture(autouse=True, scope="session")
def _relax_queue_message_validation():
    """Session-wide: relax QueueMessage validation limits."""
    QueueMessage.__post_init__ = _relaxed_post_init
    yield
    QueueMessage.__post_init__ = _orig_post_init


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


def _make_channel(queues_client=None, pubsub_client=None, **transport_opts):
    """Create a Channel with mocked connection and optional SDK clients.

    Properly calls Channel.__init__ via a fully-mocked Kombu connection,
    then injects the mock SDK clients into the cached_property dict slots.
    """
    mock_conn = MagicMock()
    mock_conn.client.hostname = "localhost"
    mock_conn.client.port = 50000
    mock_conn.client.password = None
    mock_conn.client.ssl = False
    mock_conn.client.transport_options = transport_opts
    mock_conn._used_channel_ids = []
    mock_conn.channel_max = 65535

    channel = Channel(mock_conn)

    # Apply transport options that would normally come from broker_transport_options
    if "dead_letter_queue" in transport_opts:
        channel.dead_letter_queue = transport_opts["dead_letter_queue"]
    if "max_receive_count" in transport_opts:
        channel.max_receive_count = transport_opts["max_receive_count"]

    # Inject mocked SDK clients into the cached_property dict slots
    if queues_client is not None:
        channel.__dict__["_kubemq_queues_client"] = queues_client
    if pubsub_client is not None:
        channel.__dict__["_kubemq_pubsub_client"] = pubsub_client

    return channel


def _make_mock_message(payload: dict, *, delivery_tag: str | None = None) -> MagicMock:
    """Create a mock QueueMessageReceived with JSON body."""
    msg = MagicMock()
    msg.body = json.dumps(payload).encode("utf-8")
    msg.channel = "test-queue"
    if delivery_tag:
        payload.setdefault("properties", {})["delivery_tag"] = delivery_tag
        msg.body = json.dumps(payload).encode("utf-8")
    return msg
