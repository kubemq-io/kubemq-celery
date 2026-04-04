"""Integration tests for kubemq-celery (requires live KubeMQ broker).

Run: pytest tests/test_integration.py -m integration
Env: KUBEMQ_BROKER_ADDRESS (default: kubemq://localhost:50000)
"""

from __future__ import annotations

import os
import time
import uuid
from queue import Empty

import pytest
from kombu import Connection

pytestmark = pytest.mark.integration


def _broker_url() -> str:
    return os.environ.get("KUBEMQ_BROKER_ADDRESS", "kubemq://localhost:50000")


def _payload(delivery_tag: str, queue_name: str, body_b64: str = "dGVzdA==") -> dict:
    return {
        "body": body_b64,
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {},
        "properties": {
            "delivery_tag": delivery_tag,
            "delivery_info": {"exchange": "", "routing_key": queue_name},
        },
    }


class TestIntegrationRoundtrip:
    def test_send_receive_roundtrip(self):
        """Full send/receive roundtrip against live broker."""
        queue_name = "test-integration-roundtrip"
        test_payload = _payload("test-tag-001", queue_name)

        with Connection(_broker_url(), transport="kubemq") as conn:
            channel = conn.channel()
            try:
                channel._purge(queue_name)
                channel.basic_consume(queue_name, True, lambda _m: None, "ct-rt")
                channel._put(queue_name, test_payload)
                received = channel._get(queue_name)
                assert received["body"] == test_payload["body"]
                assert received["properties"]["delivery_tag"] == "test-tag-001"
                assert received["content-type"] == "application/json"
            finally:
                channel.basic_cancel("ct-rt")
                channel._purge(queue_name)
                channel.close()

    def test_ack_removes_message(self):
        """Manual ack: after basic_ack, queue is empty."""
        queue_name = "test-int-ack"
        pl = _payload("dt-ack-1", queue_name)

        with Connection(_broker_url(), transport="kubemq") as conn:
            ch = conn.channel()
            try:
                ch._purge(queue_name)
                ch.basic_consume(queue_name, False, lambda _m: None, "ct-ack")
                ch._put(queue_name, pl)
                msg = ch._get(queue_name)
                tag = msg["properties"]["delivery_tag"]
                ch.basic_ack(tag)
                with pytest.raises(Empty):
                    ch._get(queue_name)
            finally:
                ch.basic_cancel("ct-ack")
                ch._purge(queue_name)
                ch.close()

    def test_nack_redelivers(self):
        """Reject with requeue=True returns the same payload on next receive."""
        queue_name = "test-int-nack"
        pl = _payload("dt-nack-1", queue_name)

        with Connection(_broker_url(), transport="kubemq") as conn:
            ch = conn.channel()
            try:
                ch._purge(queue_name)
                ch.basic_consume(queue_name, False, lambda _m: None, "ct-nack")
                ch._put(queue_name, pl)
                first = ch._get(queue_name)
                tag = first["properties"]["delivery_tag"]
                ch.basic_reject(tag, requeue=True)
                second = ch._get(queue_name)
                assert second["body"] == first["body"]
                ch.basic_ack(second["properties"]["delivery_tag"])
            finally:
                ch.basic_cancel("ct-nack")
                ch._purge(queue_name)
                ch.close()

    def test_purge_removes_all(self):
        """Purge clears multiple messages."""
        queue_name = "test-int-purge"
        a = _payload("dt-p-1", queue_name, body_b64="YQ==")
        b = _payload("dt-p-2", queue_name, body_b64="Yg==")

        with Connection(_broker_url(), transport="kubemq") as conn:
            ch = conn.channel()
            try:
                ch._purge(queue_name)
                ch._put(queue_name, a)
                ch._put(queue_name, b)
                n = ch._purge(queue_name)
                assert n >= 2
                ch.basic_consume(queue_name, True, lambda _m: None, "ct-purge")
                with pytest.raises(Empty):
                    ch._get(queue_name)
            finally:
                ch.basic_cancel("ct-purge")
                ch._purge(queue_name)
                ch.close()

    def test_queue_size(self):
        """_size reflects waiting messages after put."""
        queue_name = f"test-int-size-{uuid.uuid4().hex[:10]}"

        with Connection(_broker_url(), transport="kubemq") as conn:
            ch = conn.channel()
            try:
                ch._purge(queue_name)
                assert ch._size(queue_name) == 0
                ch._put(queue_name, _payload("dt-sz-1", queue_name))
                ch.basic_consume(queue_name, True, lambda _m: None, "ct-size")
                assert ch._size(queue_name) >= 1
            finally:
                ch.basic_cancel("ct-size")
                ch._purge(queue_name)
                ch.close()

    def test_delayed_delivery_countdown(self):
        """Countdown in headers delays visibility (auto_ack consumer)."""
        queue_name = f"test-int-delay-{uuid.uuid4().hex[:10]}"
        pl = _payload("dt-del-1", queue_name)
        pl["headers"] = {"countdown": 2}

        with Connection(_broker_url(), transport="kubemq") as conn:
            ch = conn.channel()
            try:
                ch._purge(queue_name)
                ch.basic_consume(queue_name, True, lambda _m: None, "ct-delay")
                ch._put(queue_name, pl)
                with pytest.raises(Empty):
                    ch._get(queue_name)
                deadline = time.monotonic() + 15.0
                got = None
                while time.monotonic() < deadline:
                    time.sleep(0.25)
                    try:
                        got = ch._get(queue_name)
                        break
                    except Empty:
                        continue
                assert got is not None
                assert got["body"] == pl["body"]
            finally:
                ch.basic_cancel("ct-delay")
                ch._purge(queue_name)
                ch.close()

    def test_connection_refused_invalid_port(self):
        """Unreachable broker port fails during connection (no silent pass)."""
        bad_url = "kubemq://127.0.0.1:1"
        conn = Connection(bad_url, transport="kubemq")
        with pytest.raises(Exception):
            conn.connect()
