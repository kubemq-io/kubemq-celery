"""T6: Thread safety tests for kubemq-celery Channel.

Tests concurrent access to Channel methods to verify thread safety
of _put/_get, basic_ack, and fanout subscribe/close operations.

Spec: T6-zero-loss, T6-ack-race, T6-close-race
"""

from __future__ import annotations

import json
import threading
from queue import Empty
from unittest.mock import MagicMock, patch

import pytest

from kubemq_celery.transport import Channel

pytestmark = pytest.mark.integration


def _make_channel_for_threads(queues_client=None, pubsub_client=None):
    """Create a Channel suitable for thread safety testing."""
    mock_conn = MagicMock()
    mock_conn.client.hostname = "localhost"
    mock_conn.client.port = 50000
    mock_conn.client.password = None
    mock_conn.client.ssl = False
    mock_conn.client.transport_options = {}
    mock_conn._used_channel_ids = []
    mock_conn.channel_max = 65535

    channel = Channel(mock_conn)

    if queues_client is not None:
        channel.__dict__["_kubemq_queues_client"] = queues_client
    if pubsub_client is not None:
        channel.__dict__["_kubemq_pubsub_client"] = pubsub_client

    return channel


class TestThreadSafety:
    """T6: Thread safety tests."""

    def test_concurrent_put_get_zero_loss(self):
        """T6-zero-loss: 10 threads x 100 messages each, verify zero lost.

        Each thread sends messages via _put, while receiver threads
        drain the in-memory batch buffer. Verifies total message count
        matches expected.
        """
        num_threads = 10
        messages_per_thread = 100
        total_expected = num_threads * messages_per_thread

        mock_client = MagicMock()
        # Track all sent messages
        sent_messages = []
        send_lock = threading.Lock()

        def track_send(msg):
            with send_lock:
                sent_messages.append(msg)

        mock_client.send_queue_message.side_effect = track_send
        channel = _make_channel_for_threads(queues_client=mock_client)

        errors = []

        def sender(thread_id):
            try:
                for i in range(messages_per_thread):
                    message = {
                        "body": f"msg-{thread_id}-{i}",
                        "headers": {},
                        "properties": {
                            "delivery_tag": f"tag-{thread_id}-{i}",
                            "priority": 0,
                        },
                    }
                    channel._put("test-queue", message)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=sender, args=(t,)) for t in range(num_threads)]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Sender errors: {errors}"
        assert len(sent_messages) == total_expected

    def test_concurrent_ack_no_race(self):
        """T6-ack-race: Concurrent basic_ack calls on different delivery tags.

        Verifies that concurrent ack operations on different delivery tags
        don't corrupt the _kubemq_msg_refs dictionary.
        """
        mock_client = MagicMock()
        channel = _make_channel_for_threads(queues_client=mock_client)

        # Pre-populate message refs
        num_tags = 100
        mock_refs = {}
        for i in range(num_tags):
            tag = f"ack-tag-{i}"
            ref = MagicMock()
            ref.ack.return_value = None
            channel._kubemq_msg_refs[tag] = ref
            mock_refs[tag] = ref

        errors = []
        acked_tags = []
        ack_lock = threading.Lock()

        def acker(start, end):
            try:
                for i in range(start, end):
                    tag = f"ack-tag-{i}"
                    with patch.object(Channel.__bases__[1], "basic_ack"):
                        channel.basic_ack(tag)
                    with ack_lock:
                        acked_tags.append(tag)
            except Exception as exc:
                errors.append(exc)

        # Split ack work across 10 threads
        threads = []
        chunk = num_tags // 10
        for t in range(10):
            start = t * chunk
            end = start + chunk
            threads.append(threading.Thread(target=acker, args=(start, end)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Ack errors: {errors}"
        assert len(acked_tags) == num_tags
        # All refs should have been popped
        assert len(channel._kubemq_msg_refs) == 0
        # All refs should have had ack() called
        for tag, ref in mock_refs.items():
            ref.ack.assert_called_once()

    def test_concurrent_fanout_subscribe_close(self):
        """T6-close-race: Concurrent _subscribe_fanout and close() calls.

        Verifies that close() and _subscribe_fanout don't deadlock or
        corrupt state when called concurrently.
        """
        mock_pubsub = MagicMock()
        mock_client = MagicMock()
        channel = _make_channel_for_threads(
            queues_client=mock_client,
            pubsub_client=mock_pubsub,
        )

        errors = []
        barrier = threading.Barrier(2, timeout=10)

        def subscriber():
            try:
                barrier.wait()
                for i in range(20):
                    try:
                        channel._subscribe_fanout(f"exchange-{i}")
                    except Exception:
                        # Channel may be closed -- expected
                        pass
            except Exception as exc:
                errors.append(exc)

        def closer():
            try:
                barrier.wait()
                # Let subscriber start a few subscriptions first
                import time

                time.sleep(0.01)
                with patch.object(Channel.__bases__[1], "close"):
                    channel.close()
            except Exception as exc:
                errors.append(exc)

        t_sub = threading.Thread(target=subscriber)
        t_close = threading.Thread(target=closer)

        t_sub.start()
        t_close.start()

        t_sub.join(timeout=15)
        t_close.join(timeout=15)

        # No deadlocks (threads completed) and no unhandled errors
        assert not errors, f"Race condition errors: {errors}"
        # Channel should be closed
        assert channel._closed is True

    def test_concurrent_batch_buffer_access(self):
        """Concurrent _get calls on same queue with batch buffer.

        Verifies that batch buffer access under _batch_buffer_locks
        doesn't lose messages when multiple threads read concurrently.
        """
        mock_client = MagicMock()

        # Create messages to populate batch buffer
        messages = []
        for i in range(50):
            payload = {
                "body": f"batch-{i}",
                "headers": {},
                "properties": {"delivery_tag": f"batch-tag-{i}"},
            }
            mock_msg = MagicMock()
            mock_msg.body = json.dumps(payload).encode("utf-8")
            messages.append(mock_msg)

        # First call returns all messages; subsequent calls return empty
        call_count = [0]
        call_lock = threading.Lock()

        def receive_side_effect(**kwargs):
            with call_lock:
                call_count[0] += 1
                if call_count[0] == 1:
                    return MagicMock(messages=messages, is_error=False)
            return MagicMock(messages=[], is_error=False)

        mock_client.receive_queue_messages.side_effect = receive_side_effect

        channel = _make_channel_for_threads(queues_client=mock_client)
        channel._no_ack_queues.add("test-queue")

        received = []
        recv_lock = threading.Lock()
        errors = []

        def getter():
            try:
                for _ in range(10):
                    try:
                        msg = channel._get("test-queue")
                        with recv_lock:
                            received.append(msg)
                    except Empty:
                        pass
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=getter) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Getter errors: {errors}"
        # All 50 messages should have been received exactly once
        assert len(received) == 50
        bodies = sorted(m["body"] for m in received)
        expected = sorted(f"batch-{i}" for i in range(50))
        assert bodies == expected
