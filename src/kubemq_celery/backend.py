"""KubeMQ Queue-peek result backend for Celery."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from functools import cached_property
from uuid import uuid4

from celery.backends.base import BaseBackend
from kubemq.core import ClientConfig
from kubemq.core.config import TLSConfig
from kubemq.core.exceptions import KubeMQChannelError, KubeMQTimeoutError
from kubemq.queues import QueueMessage
from kubemq.queues.client import Client as QueuesClient

from kubemq_celery.transport import DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS
from kubemq_celery.utils import format_grpc_address, parse_result_url, sanitize_queue_name

logger = logging.getLogger("kubemq_celery.backend")


class KubeMQResultBackend(BaseBackend):
    """KubeMQ Queue-peek result backend for Celery.

    Stores task results as KubeMQ Queue messages on per-task channels.
    Retrieves results via non-destructive peek_queue_messages().

    Works with multiple workers -- results stored in broker, not worker memory.
    """

    # Backend capabilities
    supports_autoexpire = True  # KubeMQ expiration_in_seconds
    supports_native_join = False  # chords use polling fallback

    # Channel prefix for result queues
    RESULT_CHANNEL_PREFIX = "celery-result-"

    # Max expiration in KubeMQ (12 hours = 43200 seconds)
    MAX_EXPIRATION_SECONDS = 43200

    def __init__(self, app, url=None, **kwargs):
        super().__init__(app, url=url, **kwargs)
        self._url = url

    def _backend_opts(self) -> dict:
        return getattr(self.app.conf, "result_backend_transport_options", {}) or {}

    @cached_property
    def _queues_client(self) -> QueuesClient:
        """Lazy QueuesClient creation from result_backend URL."""
        url = self._url or self.app.conf.result_backend
        url_params = parse_result_url(url or "")
        hostname = url_params["hostname"]
        port = url_params["port"]
        auth_token = url_params["auth_token"]
        tls_enabled = url_params["tls_enabled"]
        opts = self._backend_opts()

        max_send = int(opts.get("max_send_size", 4_194_304))
        max_recv = int(opts.get("max_receive_size", 4_194_304))
        conn_timeout = opts.get("connection_timeout")
        if conn_timeout is not None:
            conn_timeout = float(conn_timeout)

        cfg_kw: dict = {
            "address": format_grpc_address(hostname, port),
            "client_id": f"celery-result-{uuid4().hex[:8]}",
            "auth_token": auth_token or opts.get("auth_token"),
            "tls": TLSConfig(
                enabled=tls_enabled or opts.get("tls_enabled", False),
                cert_file=opts.get("tls_cert_file") or None,
                key_file=opts.get("tls_key_file") or None,
                ca_file=opts.get("tls_ca_file") or None,
            ),
            "max_send_size": max_send,
            "max_receive_size": max_recv,
        }
        if conn_timeout is not None:
            cfg_kw["connection_timeout"] = conn_timeout
        config = ClientConfig(**cfg_kw)
        return QueuesClient(config=config)

    @cached_property
    def _result_channel_prefix(self) -> str:
        opts = self._backend_opts()
        return str(opts.get("result_channel_prefix", self.RESULT_CHANNEL_PREFIX))

    @cached_property
    def _peek_timeout(self) -> int:
        opts = self._backend_opts()
        return int(opts.get("peek_timeout", 1))

    def _result_channel(self, task_id: str) -> str:
        """Generate result queue channel name for a task."""
        return f"{self._result_channel_prefix}{sanitize_queue_name(task_id)}"

    def _store_result(self, task_id, result, state, traceback=None, request=None, **kwargs):
        """Store task result as a KubeMQ Queue message.

        The result is stored on channel 'celery-result-{task_id}'
        with expiration matching Celery's result_expires setting.
        Each state update overwrites the previous (purge then write).
        """
        meta = {
            "task_id": task_id,
            "status": state,
            "result": self.encode_result(result, state),
            "traceback": traceback,
            "children": [],
            "date_done": datetime.now(UTC).isoformat(),
            "group_id": getattr(request, "group", None) if request else None,
        }

        # Calculate expiration
        expires = self.app.conf.result_expires
        if isinstance(expires, timedelta):
            expires_seconds = int(expires.total_seconds())
        elif expires is None:
            expires_seconds = 86400  # 24h default
        else:
            expires_seconds = int(expires)

        # Cap at KubeMQ max (12 hours); treat 0 as "use max" since
        # KubeMQ does not support infinite expiration
        if expires_seconds <= 0:
            expires_seconds = self.MAX_EXPIRATION_SECONDS
        expires_seconds = min(expires_seconds, self.MAX_EXPIRATION_SECONDS)

        body = json.dumps(meta).encode("utf-8")
        channel = self._result_channel(task_id)

        # Purge any previous result for this task (state transitions)
        try:
            self._queues_client.ack_all_queue_messages(
                channel=channel,
                wait_time_seconds=DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS,
            )
        except Exception:
            pass  # channel may not exist yet

        # NOTE: During state transitions, there is a brief window between
        # ack_all (purge) and send (write) where peek returns PENDING.
        # Celery's polling loop handles this gracefully by retrying.

        msg = QueueMessage(
            channel=channel,
            body=body,
            metadata=json.dumps({"task_id": task_id, "status": state}),
            expiration_in_seconds=expires_seconds,
        )
        self._queues_client.send_queue_message(msg)
        return result

    def _get_task_meta_for(self, task_id):
        """Retrieve task result via non-destructive peek.

        Uses peek_queue_messages() which reads without consuming,
        allowing multiple callers to read the same result.
        """
        channel = self._result_channel(task_id)
        peek_timeout = self._peek_timeout
        try:
            result = self._queues_client.peek_queue_messages(
                channel=channel,
                max_messages=1,
                wait_timeout_in_seconds=peek_timeout,
            )
            if getattr(result, "is_error", False) is True:
                logger.warning(
                    "Peek error on %s: %s",
                    channel,
                    getattr(result, "error", ""),
                )
                raise KubeMQChannelError(getattr(result, "error", "") or "peek failed")
            if result.messages:
                meta = json.loads(result.messages[0].body)
                return self.meta_from_decoded(meta)
        except (KubeMQChannelError, KubeMQTimeoutError):
            pass  # channel doesn't exist or timeout -- task still pending
        except json.JSONDecodeError as exc:
            logger.error(
                "Corrupt result body for task %s on %s: %s",
                task_id,
                channel,
                exc,
            )
            raise

        return {"status": "PENDING", "result": None}

    def _save_group(self, group_id, result):
        """Store group metadata as a queue message."""
        channel = f"celery-group-{sanitize_queue_name(group_id)}"
        body = json.dumps({"group_id": group_id, "result": result}).encode("utf-8")

        # Calculate expiration -- aligned with _store_result's pattern
        # to correctly handle result_expires=0 (which means "never expire"
        # in some Celery contexts). The (x or default) pattern would
        # silently convert 0 to the default.
        expires = self.app.conf.result_expires
        if isinstance(expires, timedelta):
            expires_seconds = int(expires.total_seconds())
        elif expires is None:
            expires_seconds = 86400  # 24h default
        else:
            expires_seconds = int(expires)

        # Cap at KubeMQ max (12 hours); treat 0 as "use max"
        if expires_seconds <= 0:
            expires_seconds = self.MAX_EXPIRATION_SECONDS
        expires_seconds = min(expires_seconds, self.MAX_EXPIRATION_SECONDS)

        msg = QueueMessage(
            channel=channel,
            body=body,
            expiration_in_seconds=expires_seconds,
        )
        self._queues_client.send_queue_message(msg)

    def _restore_group(self, group_id, cache=True):
        """Retrieve group metadata via peek."""
        channel = f"celery-group-{sanitize_queue_name(group_id)}"
        try:
            result = self._queues_client.peek_queue_messages(
                channel=channel,
                max_messages=1,
                wait_timeout_in_seconds=self._peek_timeout,
            )
            if getattr(result, "is_error", False) is True:
                return None
            if result.messages:
                return json.loads(result.messages[0].body)
        except Exception:
            pass
        return None

    def _delete_group(self, group_id):
        """Delete group metadata."""
        channel = f"celery-group-{sanitize_queue_name(group_id)}"
        try:
            self._queues_client.ack_all_queue_messages(
                channel=channel,
                wait_time_seconds=DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS,
            )
        except Exception:
            pass

    def cleanup(self):
        """Close the KubeMQ client connection."""
        if "_queues_client" in self.__dict__:
            try:
                self._queues_client.close()
            except Exception:
                pass
            del self.__dict__["_queues_client"]
        super().cleanup()
