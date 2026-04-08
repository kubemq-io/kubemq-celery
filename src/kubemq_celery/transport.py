"""KubeMQ Kombu Transport for Celery."""

from __future__ import annotations

import atexit
import json
import logging
import threading
import time
from collections import deque
from functools import cached_property
from queue import Empty
from typing import Any
from uuid import uuid4

from kombu.transport import virtual
from kubemq.common.cancellation_token import CancellationToken
from kubemq.core import ClientConfig
from kubemq.core.config import TLSConfig
from kubemq.core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQChannelError,
    KubeMQClientClosedError,
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
    KubeMQMessageError,
    KubeMQStreamBrokenError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
)
from kubemq.pubsub import EventMessage, EventsSubscription
from kubemq.pubsub.client import Client as PubSubClient
from kubemq.queues import QueueMessage
from kubemq.queues.client import Client as QueuesClient

from kubemq_celery.base import DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS, BaseKubeMQChannel
from kubemq_celery.utils import format_grpc_address, is_not_found, sanitize_queue_name

logger = logging.getLogger("kubemq_celery")


class Channel(BaseKubeMQChannel, virtual.Channel):
    """KubeMQ Channel -- implements storage primitives for Kombu virtual transport.

    Inherits shared business logic from BaseKubeMQChannel (ABC) and
    storage primitives from Kombu's virtual.Channel.
    """

    supports_fanout = True
    do_restore = False  # KubeMQ handles redelivery natively

    # --- Transport options (configurable via broker_transport_options) ---
    from_transport_options = virtual.Channel.from_transport_options + (
        "wait_timeout",
        "auth_token",
        "dead_letter_queue",
        "max_receive_count",
        "client_id_prefix",
        "tls_enabled",
        "tls_cert_file",
        "tls_key_file",
        "tls_ca_file",
        "max_send_size",
        "max_receive_size",
        "connection_timeout",
        "purge_wait_seconds",
        "message_expiration",
        "max_batch_size",
        "fanout_max_retries",
        "grpc_keepalive_time",
        "grpc_keepalive_timeout",
        "grpc_permit_without_calls",
    )

    wait_timeout: int = 1  # seconds -- blocking receive timeout
    auth_token: str | None = None  # override URL-based auth
    dead_letter_queue: str = ""  # DLQ channel name
    max_receive_count: int = 0  # max receive attempts before DLQ
    client_id_prefix: str = "celery"  # client ID prefix
    tls_enabled: bool = False  # TLS override (set True by kubemq+tls://)
    tls_cert_file: str = ""  # mTLS client cert
    tls_key_file: str = ""  # mTLS client key
    tls_ca_file: str = ""  # CA cert
    max_send_size: int = 4_194_304  # 4MB default
    max_receive_size: int = 4_194_304  # 4MB default
    connection_timeout: float | None = None  # None = infer from conninfo / transport options
    # ack_all wait (SDK default 60s is too slow for empty-queue purge)
    purge_wait_seconds: int = DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS

    # v1.1 options
    message_expiration: int = 0  # seconds; 0 = no expiration (C1)
    max_batch_size: int = 10  # max messages per gRPC receive call (C6)
    fanout_max_retries: int = 5  # max re-subscription attempts (C2)
    grpc_keepalive_time: int = 30  # seconds between keepalive pings (C10)
    grpc_keepalive_timeout: int = 10  # seconds to wait for response (C10)
    grpc_permit_without_calls: bool = True  # keepalive without active RPCs (C10)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Validate DLQ configuration (M-2)
        if self.max_receive_count > 0 and not self.dead_letter_queue:
            from kubemq_celery.exceptions import KubeMQCeleryConfigError

            raise KubeMQCeleryConfigError(
                f"max_receive_count={self.max_receive_count} requires dead_letter_queue "
                f"to be set. Without a DLQ, poison messages will be retried indefinitely."
            )
        self._closed: bool = False
        self._fanout_subscriptions: dict[str, CancellationToken] = {}
        self._kubemq_msg_refs: dict[str, Any] = {}
        # Track which queues were registered with no_ack=True
        self._no_ack_queues: set[str] = set()
        self._no_ack_tags: set[str] = set()  # consumer_tags registered with no_ack=True
        # Batch receive buffers (C6) -- per-queue deque of (payload, msg_ref) tuples
        self._batch_buffers: dict[str, deque[tuple[dict, Any]]] = {}
        self._batch_buffer_locks: dict[str, threading.Lock] = {}
        atexit.register(self._drain_batch_buffers)
        # Lock for fanout subscription dict and fanout buffer writes.
        # subscribe_to_events() runs callbacks in a background thread,
        # so _subscribe_fanout() and _on_fanout_event() need explicit
        # synchronization to avoid races on _fanout_subscriptions and
        # the fanout message delivery path. Python's GIL provides basic
        # safety for _kubemq_msg_refs simple dict ops, but check-then-act
        # patterns (e.g., "if exchange in _fanout_subscriptions: return")
        # need the lock to be atomic.
        self._fanout_lock = threading.Lock()

    def _grpc_address(self) -> str:
        conninfo = self.connection.client
        host = conninfo.hostname or "localhost"
        port = int(conninfo.port or 50000)
        return format_grpc_address(host, port)

    def _connection_timeout_value(self) -> float | None:
        if self.connection_timeout is not None:
            return float(self.connection_timeout)
        conninfo = self.connection.client
        ct = getattr(conninfo, "connect_timeout", None)
        if ct is not None:
            return float(ct)
        return None

    # --- Lazy client creation ---

    def _build_client_config_kwargs(self, prefix: str = "queues") -> dict[str, Any]:
        """Build ClientConfig kwargs (shared between queues and pubsub clients)."""
        from kubemq.core.config import KeepAliveConfig

        conninfo = self.connection.client
        cfg_kw: dict[str, Any] = {
            "address": self._grpc_address(),
            "client_id": f"{self.client_id_prefix}-{prefix}-{uuid4().hex[:8]}",
            "auth_token": self.auth_token or conninfo.password or None,
            "tls": TLSConfig(
                enabled=self.tls_enabled or bool(conninfo.ssl),
                cert_file=self.tls_cert_file or None,
                key_file=self.tls_key_file or None,
                ca_file=self.tls_ca_file or None,
            ),
            "max_send_size": self.max_send_size,
            "max_receive_size": self.max_receive_size,
            "keep_alive": KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=self.grpc_keepalive_time,
                ping_timeout_in_seconds=self.grpc_keepalive_timeout,
                permit_without_calls=self.grpc_permit_without_calls,
            ),
        }
        ct = self._connection_timeout_value()
        if ct is not None:
            cfg_kw["connection_timeout"] = ct
        return cfg_kw

    @cached_property
    def _kubemq_queues_client(self) -> QueuesClient:
        """Lazy KubeMQ QueuesClient -- created on first use."""
        cfg_kw = self._build_client_config_kwargs(prefix="queues")
        config = ClientConfig(**cfg_kw)
        client = QueuesClient(config=config)
        logger.info("KubeMQ QueuesClient created (client_id: %s)", cfg_kw["client_id"])
        return client

    @cached_property
    def _kubemq_pubsub_client(self) -> PubSubClient:
        """Lazy KubeMQ PubSubClient -- created on first fanout use."""
        cfg_kw = self._build_client_config_kwargs(prefix="pubsub")
        config = ClientConfig(**cfg_kw)
        client = PubSubClient(config=config)
        logger.info("KubeMQ PubSubClient created (client_id: %s)", cfg_kw["client_id"])
        return client

    # --- Helper methods ---

    def _is_auto_ack_queue(self, queue: str) -> bool:
        """Determine if a queue should use auto_ack.

        Returns True when the consumer was registered with no_ack=True
        (Celery default with task_acks_late=False). Returns False when
        task_acks_late=True (manual ack mode).
        """
        return queue in self._no_ack_queues

    def _backoff_sleep(self, seconds: float) -> None:
        """Sync backoff sleep using time.sleep()."""
        time.sleep(seconds)

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        """Override to track no_ack setting per queue."""
        result = super().basic_consume(queue, no_ack, *args, **kwargs)
        # result is the consumer_tag assigned by super()
        if no_ack:
            self._no_ack_tags.add(result)
            self._no_ack_queues.add(queue)
        return result

    def basic_cancel(self, consumer_tag):
        """Override to clean up no_ack tracking."""
        queue = self._tag_to_queue.get(consumer_tag)
        self._no_ack_tags.discard(consumer_tag)
        result = super().basic_cancel(consumer_tag)
        if queue:
            # Only remove from no_ack_queues if no remaining consumer
            # for this queue has no_ack=True
            remaining_no_ack = any(
                tag in self._no_ack_tags and self._tag_to_queue.get(tag) == queue
                for tag in self._consumers
            )
            if not remaining_no_ack:
                self._no_ack_queues.discard(queue)
        return result

    # --- Required methods (MUST implement) ---

    def _put(self, queue: str, message: dict, **kwargs: Any) -> None:
        """Send a Celery message to a KubeMQ Queue channel."""
        if self._closed:
            raise KubeMQClientClosedError("channel closed")

        msg_kwargs = self._build_queue_message_kwargs(queue, message)
        msg = QueueMessage(**msg_kwargs)
        self._kubemq_queues_client.send_queue_message(msg)
        logger.debug("Message sent to queue '%s'", sanitize_queue_name(queue))

    def _get(self, queue: str, timeout: int | None = None) -> dict:
        """Receive a Celery message from a KubeMQ Queue channel.

        When batch mode is enabled (max_batch_size > 1), receives up to
        max_batch_size messages in a single gRPC call. Returns the first
        message immediately and buffers the rest for subsequent _get() calls.
        """
        if self._closed:
            raise KubeMQClientClosedError("channel closed")

        sanitized = sanitize_queue_name(queue)

        # Check buffer first
        buffer_lock = self._batch_buffer_locks.setdefault(sanitized, threading.Lock())
        with buffer_lock:
            buffer = self._batch_buffers.get(sanitized)
            if buffer:
                payload, msg_ref = buffer.popleft()
                # Store msg ref for ack (if not auto-ack)
                auto_ack = self._is_auto_ack_queue(queue)
                if not auto_ack:
                    delivery_tag = payload.get("properties", {}).get("delivery_tag")
                    if delivery_tag:
                        self._kubemq_msg_refs[delivery_tag] = msg_ref
                return payload

        # Buffer empty -- fetch from broker
        auto_ack = self._is_auto_ack_queue(queue)
        wait_secs = max(0, int(self.wait_timeout))
        if timeout is not None:
            wait_secs = max(0, min(wait_secs, int(timeout)))

        batch_size = max(1, min(int(self.max_batch_size), self.MAX_BATCH_SIZE))

        response = self._kubemq_queues_client.receive_queue_messages(
            channel=sanitized,
            max_messages=batch_size,
            wait_timeout_in_seconds=wait_secs,
            auto_ack=auto_ack,
        )

        if getattr(response, "is_error", False) is True:
            err = getattr(response, "error", "") or "receive failed"
            raise KubeMQMessageError(err)

        if not response.messages:
            raise Empty()

        # Process all messages
        parsed_messages: list[tuple[dict, Any]] = []
        for msg in response.messages:
            try:
                payload = self._deserialize_message(msg.body)
            except json.JSONDecodeError as exc:
                if not auto_ack:
                    try:
                        msg.nack()
                    except Exception:
                        logger.exception("nack after JSON decode failure")
                logger.error("Invalid JSON body on queue %s: %s", sanitized, exc)
                continue  # skip bad messages

            if not isinstance(payload, dict):
                if not auto_ack:
                    try:
                        msg.nack()
                    except Exception:
                        logger.exception("nack after non-dict JSON payload")
                continue

            parsed_messages.append((payload, msg))

        if not parsed_messages:
            raise Empty()

        # Return first, buffer rest
        first_payload, first_msg = parsed_messages[0]

        if not auto_ack:
            delivery_tag = first_payload.get("properties", {}).get("delivery_tag")
            if not delivery_tag:
                try:
                    first_msg.nack()
                except Exception:
                    logger.exception("nack after missing delivery_tag")
                raise KubeMQChannelError("missing delivery_tag in manual ack mode")
            self._kubemq_msg_refs[delivery_tag] = first_msg

        # Buffer remaining messages
        if len(parsed_messages) > 1:
            with buffer_lock:
                buffer = self._batch_buffers.setdefault(sanitized, deque())
                for payload, msg_ref in parsed_messages[1:]:
                    if not auto_ack:
                        # Store msg refs for buffered messages too
                        dt = payload.get("properties", {}).get("delivery_tag")
                        if dt:
                            self._kubemq_msg_refs[dt] = msg_ref
                    buffer.append((payload, msg_ref))

        logger.debug("Message received from queue '%s'", sanitized)
        return first_payload

    def _purge(self, queue: str) -> int:
        """Purge all messages from a KubeMQ Queue channel.

        Returns the actual count of purged messages from the server.
        Returns 0 if the queue is empty or does not exist.
        """
        try:
            return self._kubemq_queues_client.ack_all_queue_messages(
                channel=sanitize_queue_name(queue),
                wait_time_seconds=max(0, int(self.purge_wait_seconds)),
            )
        except (KubeMQMessageError, KubeMQChannelError) as exc:
            if is_not_found(exc):
                return 0
            raise

    # --- Should-implement methods ---

    def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        """Acknowledge a message via native KubeMQ ack.

        Retrieves the stored QueueMessageReceived reference and calls
        its ack() method. If the reference is missing (KeyError -- cleared
        during reconnection), skips native ack. KubeMQ will re-deliver
        the message (at-least-once semantics). Always calls super() to
        keep virtual QoS consistent.
        """
        try:
            msg_ref = self._kubemq_msg_refs.pop(delivery_tag)
            msg_ref.ack()
        except KeyError:
            # Reference cleared during reconnection -- skip native ack.
            # KubeMQ will re-deliver (at-least-once semantics).
            logger.debug(
                "No KubeMQ msg ref for delivery_tag=%s (likely reconnection), skipping native ack",
                delivery_tag,
            )
        except ValueError:
            # Transaction already completed (auto-acked or previously handled)
            logger.debug(
                "KubeMQ transaction already completed for delivery_tag=%s",
                delivery_tag,
            )
        # Forward multiple to super() so the virtual QoS layer can
        # handle it. Note: KubeMQ only supports per-message ack, so
        # multiple=True semantics depend on the virtual layer's
        # implementation. If multiple=True is ever sent by Celery,
        # the virtual layer handles iterating over unacked messages.
        super().basic_ack(delivery_tag, multiple)

    def basic_reject(self, delivery_tag: str, requeue: bool = False) -> None:
        """Reject a message via native KubeMQ nack/requeue.

        If requeue=True, calls re_queue() to put the message back.
        If requeue=False, calls nack() to reject permanently.
        KeyError handling same as basic_ack.

        When native re_queue succeeds, Kombu's virtual QoS must not run
        ``reject(requeue=True)`` — that path calls ``_restore`` which would
        ``_put`` the message again and duplicate it on the broker.
        """
        broker_requeued = False
        try:
            msg_ref = self._kubemq_msg_refs.pop(delivery_tag)
            if requeue:
                msg_ref.re_queue(msg_ref.channel)
                broker_requeued = True
            else:
                msg_ref.nack()
        except KeyError:
            logger.debug(
                "No KubeMQ msg ref for delivery_tag=%s (likely reconnection), "
                "skipping native reject",
                delivery_tag,
            )
        except ValueError:
            logger.debug(
                "KubeMQ transaction already completed for delivery_tag=%s",
                delivery_tag,
            )

        if delivery_tag not in self.qos._delivered:
            return

        if broker_requeued:
            # Broker already returned the message to the queue; only drop QoS state.
            self.qos.ack(delivery_tag)
        else:
            super().basic_reject(delivery_tag, requeue=requeue)

    def _drain_batch_buffers(self) -> None:
        """Nack all buffered messages on shutdown.

        Called by atexit handler and close(). Provides crash safety for
        buffered messages that haven't been delivered to Celery yet.
        Uses nack() only -- KubeMQ's native visibility timeout handles
        redelivery of nacked messages automatically.
        """
        for queue_name, buffer in list(self._batch_buffers.items()):
            lock = self._batch_buffer_locks.get(queue_name)
            acquired = lock.acquire(timeout=2.0) if lock else False
            try:
                while buffer:
                    _payload, msg_ref = buffer.popleft()
                    try:
                        msg_ref.nack()
                    except Exception:
                        logger.debug(
                            "Failed to nack buffered message on %s during drain",
                            queue_name,
                        )
            finally:
                if acquired:
                    lock.release()
        self._batch_buffers.clear()

    def close(self) -> None:
        """Clean up KubeMQ client resources."""
        self._closed = True
        # Unregister atexit handler to avoid stale callback leak (M-3)
        atexit.unregister(self._drain_batch_buffers)

        # Drain batch buffers first (nack undelivered messages)
        try:
            self._drain_batch_buffers()
        except Exception:
            logger.debug("Error draining batch buffers during close")

        # Cancel all fanout subscriptions (lock protects against
        # concurrent _subscribe_fanout / _on_fanout_event calls)
        with self._fanout_lock:
            for exchange, cancel_token in self._fanout_subscriptions.items():
                try:
                    cancel_token.cancel()
                except Exception:
                    pass  # best-effort cleanup
            self._fanout_subscriptions.clear()

        # Close KubeMQ clients (if created via cached_property)
        if "_kubemq_queues_client" in self.__dict__:
            try:
                self._kubemq_queues_client.close()
            except Exception:
                pass
            del self.__dict__["_kubemq_queues_client"]

        if "_kubemq_pubsub_client" in self.__dict__:
            try:
                self._kubemq_pubsub_client.close()
            except Exception:
                pass
            del self.__dict__["_kubemq_pubsub_client"]

        # Clear message references
        self._kubemq_msg_refs.clear()
        self._no_ack_queues.clear()
        self._no_ack_tags.clear()

        # MUST call super().close() -- cancels consumers, restores unacked
        super().close()

    # --- Queue binding (required when supports_fanout = True) ---

    def _queue_bind(self, exchange: str, routing_key: str, pattern: Any, queue: str) -> None:
        """Bind a queue to an exchange.

        Called by virtual.Channel.queue_bind() when supports_fanout is True.
        For fanout exchanges, subscribes to KubeMQ Events on the exchange channel.
        For direct/topic exchanges, this is a no-op (KubeMQ queues handle
        routing natively).
        """
        try:
            exchange_type = self.typeof(exchange).type
        except (KeyError, AttributeError):
            exchange_type = "direct"

        if exchange_type == "fanout":
            self._subscribe_fanout(exchange)

    def _put_fanout(
        self,
        exchange: str,
        message: dict,
        routing_key: str = "",
        **kwargs: Any,
    ) -> None:
        """Publish a message to a fanout exchange via KubeMQ Events.

        Kombu calls this for fanout exchanges (broadcast, remote control,
        pidbox, inspect). Publishes as a fire-and-forget KubeMQ event so
        all subscribed workers receive the message.
        """
        body = json.dumps(message).encode("utf-8")
        event = EventMessage(
            channel=sanitize_queue_name(exchange),
            body=body,
        )
        self._kubemq_pubsub_client.publish_event(event)
        logger.debug("Fanout message published to exchange '%s'", exchange)

    # --- Fanout methods ---

    def _subscribe_fanout(self, exchange: str) -> None:
        """Subscribe to fanout exchange via KubeMQ Events.

        IMPORTANT: No consumer group -- true fan-out to all workers.
        Pidbox requires every worker to receive every control message.
        Celery events require every monitoring tool to see all events.

        Thread-safe: guarded by _fanout_lock to prevent duplicate
        subscriptions from concurrent calls.
        """
        with self._fanout_lock:
            if exchange in self._fanout_subscriptions:
                return  # already subscribed

            cancel = CancellationToken()
            subscription = EventsSubscription(
                channel=sanitize_queue_name(exchange),
                # group omitted (default None) = no group = true fan-out to ALL subscribers
                on_receive_event_callback=lambda event: self._on_fanout_event(exchange, event),
                on_error_callback=lambda err: self._on_fanout_error(exchange, err),
            )
            self._kubemq_pubsub_client.subscribe_to_events(subscription, cancel)
            self._fanout_subscriptions[exchange] = cancel
            logger.info("Subscribed to fanout exchange '%s'", exchange)

    def _send_fanout_queue_message(self, queue: str, message: dict) -> None:
        """Deliver fanout to a worker queue without task delay/DLQ policies."""
        body = json.dumps(message).encode("utf-8")
        qmsg = QueueMessage(
            channel=sanitize_queue_name(queue),
            body=body,
            metadata=None,
        )
        self._kubemq_queues_client.send_queue_message(qmsg)

    def _on_fanout_event(self, exchange: str, event: Any) -> None:
        """Handle incoming fanout event -- decode and dispatch to virtual layer.

        Decodes the event body and dispatches to bound queues.
        The _lookup() call is protected by _fanout_lock (reads shared
        binding state), but the actual _put() network calls happen
        outside the lock to avoid blocking subscribe/close operations.
        """
        if self._closed:
            return
        message = self._decode_fanout_event(event.body)
        if message is None:
            return
        try:
            with self._fanout_lock:
                queues = self._lookup(exchange, "")
        except Exception:
            queues = []
        for queue in queues:
            try:
                self._send_fanout_queue_message(queue, message)
            except Exception as exc:
                logger.warning("Error dispatching fanout message to %s: %s", queue, exc)

    def _on_fanout_error(self, exchange: str, err: Any) -> None:
        """Handle fanout subscription error with exponential backoff re-subscription.

        Attempts re-subscription up to fanout_max_retries times with backoff:
        1s, 2s, 4s, 8s, max 30s. After max retries exhausted, gives up until
        worker restart (interview decision #10).
        """
        logger.warning("Fanout subscription error on %s: %s", exchange, err)

        # Cancel the failed subscription
        with self._fanout_lock:
            cancel_token = self._fanout_subscriptions.pop(exchange, None)
        if cancel_token:
            try:
                cancel_token.cancel()
            except Exception:
                pass

        if self._closed:
            return

        # Attempt re-subscription with exponential backoff
        max_retries = max(0, int(self.fanout_max_retries))
        backoff = 1.0  # initial backoff in seconds
        max_backoff = 30.0

        for attempt in range(1, max_retries + 1):
            if self._closed:
                return

            self._backoff_sleep(min(backoff, max_backoff))

            # Re-check after sleep -- channel may have been closed during backoff
            if self._closed:
                return

            try:
                self._subscribe_fanout(exchange)
                logger.info(
                    "Fanout subscription recovered for exchange '%s' after %d retries",
                    exchange,
                    attempt,
                )
                return
            except Exception as retry_err:
                logger.warning(
                    "Fanout re-subscription attempt %d/%d failed for '%s': %s",
                    attempt,
                    max_retries,
                    exchange,
                    retry_err,
                )
                backoff *= 2  # exponential backoff

        # Max retries exhausted -- give up until restart
        logger.error(
            "Fanout subscription permanently lost for exchange '%s'. "
            "Worker monitoring/control may be unavailable. "
            "Restart worker to recover.",
            exchange,
        )

    # --- Advanced feature methods ---

    def _size(self, queue: str) -> int:
        """Return the number of waiting messages in a KubeMQ Queue channel.

        Uses list_queues_channels with exact name matching (the search
        is wildcard-based, so we filter results by exact channel name).

        Some broker versions report ``incoming.waiting`` as 0 while messages
        are visible; in that case fall back to a bounded peek (counts up to
        1024 visible messages).
        """
        sanitized = sanitize_queue_name(queue)
        try:
            channels = self._kubemq_queues_client.list_queues_channels(
                channel_search=sanitized,
            )
            for ch in channels:
                if ch.name == sanitized:
                    w = int(getattr(ch.incoming, "waiting", 0) or 0)
                    if w > 0:
                        return w
                    break
            peek = self._kubemq_queues_client.peek_queue_messages(
                channel=sanitized,
                max_messages=1024,
                wait_timeout_in_seconds=1,
            )
            if getattr(peek, "is_error", False) is True:
                return 0
            return len(getattr(peek, "messages", []) or [])
        except KubeMQChannelError as exc:
            if is_not_found(exc):
                return 0
            raise

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        """Delete a KubeMQ Queue channel. Ignores 'not found' errors."""
        try:
            self._kubemq_queues_client.delete_queues_channel(
                channel=sanitize_queue_name(queue),
            )
        except KubeMQChannelError as exc:
            if is_not_found(exc):
                return
            raise

    def _new_queue(self, queue: str, **kwargs: Any) -> None:
        """No-op. KubeMQ creates channels on first use."""
        pass

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        """Check if a KubeMQ Queue channel exists."""
        sanitized = sanitize_queue_name(queue)
        try:
            channels = self._kubemq_queues_client.list_queues_channels(
                channel_search=sanitized,
            )
            return any(ch.name == sanitized for ch in channels)
        except KubeMQChannelError as exc:
            if is_not_found(exc):
                return False
            raise


class Transport(virtual.Transport):
    """KubeMQ Transport for Kombu/Celery."""

    Channel = Channel

    driver_type = "kubemq"
    driver_name = "kubemq"
    default_port = 50000

    polling_interval = 0.1  # 100ms (10x faster than Kombu default 1.0s)

    # Error classification for Celery auto-retry
    connection_errors = virtual.Transport.connection_errors + (
        KubeMQConnectionError,
        KubeMQAuthenticationError,
        KubeMQConnectionNotReadyError,
    )
    channel_errors = virtual.Transport.channel_errors + (
        KubeMQTimeoutError,
        KubeMQStreamBrokenError,
        KubeMQChannelError,
        KubeMQMessageError,
        KubeMQTransactionError,
        KubeMQClientClosedError,
    )

    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset(["direct", "topic", "fanout"]),
        heartbeats=False,
    )

    def driver_version(self) -> str:
        import kubemq

        return kubemq.__version__

    @property
    def default_connection_params(self) -> dict:
        return {"hostname": "localhost", "port": self.default_port}

    def establish_connection(self) -> Transport:
        """Validate connection by pinging KubeMQ broker.

        Uses SDK ping() method directly via a temporary QueuesClient
        without creating a full Kombu channel.
        """
        from kubemq_celery.exceptions import KubeMQCeleryConnectionError

        conninfo = self.client
        # Parse TLS from URL scheme
        if (
            hasattr(conninfo, "transport")
            and conninfo.transport
            and "+tls" in str(conninfo.transport)
        ):
            conninfo.ssl = True

        conn = super().establish_connection()

        host = conninfo.hostname or "localhost"
        port = int(conninfo.port or 50000)
        tls = bool(conninfo.ssl)
        address = format_grpc_address(host, port)

        # Build TLS config from transport options (same material as runtime channels)
        transport_options = getattr(conninfo, "transport_options", None)
        if not isinstance(transport_options, dict):
            transport_options = {}
        tls_config = TLSConfig(
            enabled=tls,
            cert_file=transport_options.get("tls_cert_file") or None,
            key_file=transport_options.get("tls_key_file") or None,
            ca_file=transport_options.get("tls_ca_file") or None,
        )

        try:
            ping_client = QueuesClient(
                config=ClientConfig(
                    address=address,
                    client_id=f"celery-ping-{uuid4().hex[:8]}",
                    auth_token=conninfo.password or None,
                    tls=tls_config,
                )
            )
            try:
                ping_client.ping()
            finally:
                ping_client.close()
        except (
            KubeMQConnectionError,
            KubeMQAuthenticationError,
            KubeMQConnectionNotReadyError,
            KubeMQTimeoutError,
        ) as exc:
            raise KubeMQCeleryConnectionError(
                f"Failed to connect to KubeMQ broker at {address}: {exc}"
            ) from exc
        except (ConnectionError, TimeoutError, OSError) as exc:
            raise KubeMQCeleryConnectionError(
                f"Network error connecting to KubeMQ broker at {address}: {exc}"
            ) from exc

        logger.info(
            "KubeMQ connection established to %s:%d (TLS: %s)",
            host,
            port,
            tls,
        )
        return conn

    def close_connection(self, connection) -> None:
        """Close connection and all channels."""
        logger.info("KubeMQ connection closed")
        super().close_connection(connection)

    def verify_connection(self, connection) -> bool:
        """Check if connection is alive via ping.

        Checks active channels first (Kombu moves healthy channels from
        _avail_channels to self.channels), then falls back to available.
        """
        try:
            channels = self.channels or self._avail_channels
            if not channels:
                return False
            channel = next(iter(channels))
            channel._kubemq_queues_client.ping()
            return True
        except Exception:
            return False

    def as_uri(self, uri: str, include_password: bool = False, mask: str = "**") -> str:
        """Format URI for display, masking auth token."""
        if not include_password and "@" in uri:
            scheme, rest = uri.split("://", 1)
            if "@" in rest:
                _creds, host = rest.rsplit("@", 1)
                return f"{scheme}://:{mask}@{host}"
        return uri
