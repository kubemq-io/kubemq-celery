"""KubeMQ Queue-peek result backend for Celery."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Any
from uuid import uuid4

from celery.backends.base import BaseBackend
from kubemq.core import ClientConfig
from kubemq.core.config import TLSConfig
from kubemq.core.exceptions import (
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
    KubeMQTimeoutError,
)
from kubemq.queues import QueueMessage
from kubemq.queues.client import Client as QueuesClient

from kubemq_celery.base import DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS
from kubemq_celery.utils import (
    format_grpc_address,
    is_not_found,
    parse_result_url,
    sanitize_queue_name,
)

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

    # Max expiration in KubeMQ (24 hours = 86400 seconds)
    # Requires KubeMQ Python SDK v4.1.1+ with MAX_EXPIRATION_SECONDS=86400
    MAX_EXPIRATION_SECONDS = 86400

    _GROUP_CACHE_MAX_SIZE: int = 1000
    _GROUP_CACHE_TTL: float = 5.0  # seconds

    def __init__(self, app, url=None, **kwargs):
        super().__init__(app, url=url, **kwargs)
        self._url = url
        self._group_cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._group_cache_lock = threading.Lock()

    def _backend_opts(self) -> dict:
        return getattr(self.app.conf, "result_backend_transport_options", {}) or {}

    @cached_property
    def _queues_client(self) -> QueuesClient:
        """Lazy QueuesClient creation from result_backend URL."""
        from kubemq.core.config import KeepAliveConfig

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

        grpc_keepalive_time = int(opts.get("grpc_keepalive_time", 30))
        grpc_keepalive_timeout = int(opts.get("grpc_keepalive_timeout", 10))
        grpc_permit_without_calls = bool(opts.get("grpc_permit_without_calls", True))

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
            "keep_alive": KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=grpc_keepalive_time,
                ping_timeout_in_seconds=grpc_keepalive_timeout,
                permit_without_calls=grpc_permit_without_calls,
            ),
        }
        if conn_timeout is not None:
            cfg_kw["connection_timeout"] = conn_timeout
        config = ClientConfig(**cfg_kw)
        client = QueuesClient(config=config)
        logger.info("KubeMQ result backend client created (client_id: %s)", cfg_kw["client_id"])
        return client

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

    def _result_expires_seconds(self) -> int:
        """Calculate result expiration in seconds from Celery config.

        Handles timedelta, None (24h default), and int values.
        Caps at MAX_EXPIRATION_SECONDS (24h); treats 0 as "use max"
        since KubeMQ does not support infinite expiration.
        """
        expires = self.app.conf.result_expires
        if isinstance(expires, timedelta):
            seconds = int(expires.total_seconds())
        elif expires is None:
            seconds = 86400  # 24h default
        else:
            seconds = int(expires)
        if seconds <= 0:
            seconds = self.MAX_EXPIRATION_SECONDS
        return min(seconds, self.MAX_EXPIRATION_SECONDS)

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
            "date_done": datetime.now(timezone.utc).isoformat(),
            "group_id": getattr(request, "group", None) if request else None,
        }

        expires_seconds = self._result_expires_seconds()

        body = json.dumps(meta).encode("utf-8")
        channel = self._result_channel(task_id)

        # Purge any previous result for this task (state transitions)
        try:
            self._queues_client.ack_all_queue_messages(
                channel=channel,
                wait_time_seconds=DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS,
            )
        except (KubeMQChannelError, KubeMQTimeoutError) as exc:
            if not is_not_found(exc):
                logger.warning(
                    "Purge before store_result failed for task %s: %s",
                    task_id,
                    exc,
                )
        except (KubeMQConnectionError, KubeMQConnectionNotReadyError) as exc:
            logger.warning(
                "Connection error during result purge for task %s: %s",
                task_id,
                exc,
            )

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
                if meta.get("task_id") != task_id:
                    logger.warning(
                        "Task ID mismatch on %s: expected %s, got %s "
                        "(possible channel name collision)",
                        channel,
                        task_id,
                        meta.get("task_id"),
                    )
                    return {"status": "PENDING", "result": None}
                return self.meta_from_decoded(meta)
        except (KubeMQChannelError, KubeMQTimeoutError) as exc:
            if not is_not_found(exc):
                raise
            # Channel doesn't exist -- task still pending
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
        encoded_result = self.encode(result)
        body = json.dumps({"group_id": group_id, "result": encoded_result}).encode("utf-8")
        expires_seconds = self._result_expires_seconds()

        msg = QueueMessage(
            channel=channel,
            body=body,
            expiration_in_seconds=expires_seconds,
        )
        self._queues_client.send_queue_message(msg)

    def _restore_group(self, group_id, cache=True):
        """Retrieve group metadata via peek with optional LRU cache.

        When cache=True, returns cached result if available and not expired
        (TTL: 5s, max 1000 entries). When cache=False, always queries broker.
        """
        if cache:
            with self._group_cache_lock:
                entry = self._group_cache.get(group_id)
                if entry is not None:
                    cached_time, cached_data = entry
                    if time.monotonic() - cached_time < self._GROUP_CACHE_TTL:
                        # Move to end (LRU)
                        self._group_cache.move_to_end(group_id)
                        return cached_data
                    else:
                        # Expired -- remove
                        del self._group_cache[group_id]

        # Fetch from broker
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
                data = json.loads(result.messages[0].body)
                if data.get("group_id") != group_id:
                    logger.warning(
                        "Group ID mismatch on %s: expected %s, got %s "
                        "(possible channel name collision)",
                        channel,
                        group_id,
                        data.get("group_id"),
                    )
                    return None
                restored = self.decode(data.get("result"))
                # Cache the restored result
                if cache:
                    with self._group_cache_lock:
                        self._group_cache[group_id] = (time.monotonic(), restored)
                        # Evict oldest if over limit
                        while len(self._group_cache) > self._GROUP_CACHE_MAX_SIZE:
                            self._group_cache.popitem(last=False)
                return restored
        except (KubeMQChannelError, KubeMQTimeoutError) as exc:
            if not is_not_found(exc):
                raise
            # Channel doesn't exist -- group not ready yet
        except (KubeMQConnectionError, KubeMQConnectionNotReadyError) as exc:
            logger.warning(
                "Connection error during _restore_group for group %s: %s",
                group_id,
                exc,
            )
        except json.JSONDecodeError as exc:
            logger.error(
                "Corrupt group body for group %s on %s: %s",
                group_id,
                channel,
                exc,
            )
        return None

    def _delete_group(self, group_id):
        """Delete group metadata and clear cache entry."""
        # Clear cache
        with self._group_cache_lock:
            self._group_cache.pop(group_id, None)

        channel = f"celery-group-{sanitize_queue_name(group_id)}"
        try:
            self._queues_client.ack_all_queue_messages(
                channel=channel,
                wait_time_seconds=DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS,
            )
        except (KubeMQChannelError, KubeMQTimeoutError):
            pass  # channel doesn't exist or timeout -- group not ready yet
        except (KubeMQConnectionError, KubeMQConnectionNotReadyError) as exc:
            logger.warning(
                "Connection error during _delete_group for group %s: %s",
                group_id,
                exc,
            )

    def cleanup(self):
        """Close the KubeMQ client connection."""
        if "_queues_client" in self.__dict__:
            try:
                self._queues_client.close()
            except Exception:
                pass
            del self.__dict__["_queues_client"]
        super().cleanup()
