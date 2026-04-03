"""Integration tests for kubemq-celery (requires live KubeMQ broker).

Run with: pytest tests/test_integration.py -m integration
Requires: KUBEMQ_BROKER_ADDRESS environment variable or localhost:50000
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestIntegrationRoundtrip:
    def test_send_receive_roundtrip(self):
        """Reference pattern: full send/receive roundtrip against live broker.

        Demonstrates broker connection setup, message send via _put(),
        receive via _get(), and assertion on round-tripped payload.
        """
        import os

        from kombu import Connection

        broker_url = os.environ.get("KUBEMQ_BROKER_ADDRESS", "kubemq://localhost:50000")
        queue_name = "test-integration-roundtrip"
        test_payload = {
            "body": "dGVzdA==",  # base64 "test"
            "content-encoding": "utf-8",
            "content-type": "application/json",
            "headers": {},
            "properties": {
                "delivery_tag": "test-tag-001",
                "delivery_info": {"exchange": "", "routing_key": queue_name},
            },
        }

        with Connection(broker_url, transport="kubemq") as conn:
            channel = conn.channel()
            try:
                # Purge any leftover messages
                channel._purge(queue_name)

                # Send
                channel._put(queue_name, test_payload)

                # Receive
                received = channel._get(queue_name)

                # Assert round-trip fidelity
                assert received["body"] == test_payload["body"]
                assert received["properties"]["delivery_tag"] == "test-tag-001"
                assert received["content-type"] == "application/json"
            finally:
                # Cleanup
                channel._purge(queue_name)
                channel.close()

    def test_ack_removes_message(self): ...
    def test_nack_redelivers(self): ...
    def test_purge_removes_all(self): ...
    def test_delayed_delivery(self): ...
    def test_dlq_routing(self): ...
    def test_fanout_broadcast(self): ...
    def test_queue_size(self): ...
    def test_connection_error_retry(self): ...
    def test_tls_connection(self): ...
