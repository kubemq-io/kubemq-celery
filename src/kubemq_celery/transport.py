"""KubeMQ Kombu Transport for Celery."""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
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

from kubemq_celery.utils import sanitize_queue_name

logger = logging.getLogger("kubemq_celery")


class Channel(virtual.Channel):
    """KubeMQ Channel -- implements storage primitives for Kombu virtual transport."""

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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._fanout_subscriptions: dict[str, CancellationToken] = {}
        self._kubemq_msg_refs: dict[str, Any] = {}
        # Track which queues were registered with no_ack=True
        self._no_ack_queues: set[str] = set()
        self._no_ack_tags: set[str] = set()  # consumer_tags registered with no_ack=True
        # Lock for fanout subscription dict and fanout buffer writes.
        # subscribe_to_events() runs callbacks in a background thread,
        # so _subscribe_fanout() and _on_fanout_event() need explicit
        # synchronization to avoid races on _fanout_subscriptions and
        # the fanout message delivery path. Python's GIL provides basic
        # safety for _kubemq_msg_refs simple dict ops, but check-then-act
        # patterns (e.g., "if exchange in _fanout_subscriptions: return")
        # need the lock to be atomic.
        self._fanout_lock = threading.Lock()

    # --- Lazy client creation ---

    @cached_property
    def _kubemq_queues_client(self) -> QueuesClient:
        """Lazy KubeMQ QueuesClient -- created on first use."""
        conninfo = self.connection.client
        config = ClientConfig(
            address=f"{conninfo.hostname}:{conninfo.port or 50000}",
            client_id=f"{self.client_id_prefix}-queues-{uuid4().hex[:8]}",
            auth_token=self.auth_token or conninfo.password or None,
            tls=TLSConfig(
                enabled=self.tls_enabled or bool(conninfo.ssl),
                cert_file=self.tls_cert_file or None,
                key_file=self.tls_key_file or None,
                ca_file=self.tls_ca_file or None,
            ),
            max_send_size=self.max_send_size,
            max_receive_size=self.max_receive_size,
        )
        return QueuesClient(config=config)

    @cached_property
    def _kubemq_pubsub_client(self) -> PubSubClient:
        """Lazy KubeMQ PubSubClient -- created on first fanout use."""
        conninfo = self.connection.client
        config = ClientConfig(
            address=f"{conninfo.hostname}:{conninfo.port or 50000}",
            client_id=f"{self.client_id_prefix}-pubsub-{uuid4().hex[:8]}",
            auth_token=self.auth_token or conninfo.password or None,
            tls=TLSConfig(
                enabled=self.tls_enabled or bool(conninfo.ssl),
                cert_file=self.tls_cert_file or None,
                key_file=self.tls_key_file or None,
                ca_file=self.tls_ca_file or None,
            ),
        )
        return PubSubClient(config=config)

    # --- Helper methods ---

    def _is_auto_ack_queue(self, queue: str) -> bool:
        """Determine if a queue should use auto_ack.

        Returns True when the consumer was registered with no_ack=True
        (Celery default with task_acks_late=False). Returns False when
        task_acks_late=True (manual ack mode).
        """
        return queue in self._no_ack_queues

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
        body = json.dumps(message).encode("utf-8")

        # Extract delay from Celery message headers
        headers = message.get("headers", {}) or {}
        properties = message.get("properties", {}) or {}
        countdown = headers.get("countdown")
        eta = headers.get("eta")

        delay_seconds = 0
        if countdown:
            delay_seconds = int(countdown)
        elif eta:
            try:
                eta_dt = datetime.fromisoformat(eta)
                now = datetime.now(UTC)
                delay_seconds = max(0, int((eta_dt - now).total_seconds()))
            except (ValueError, TypeError):
                delay_seconds = 0

        # Cap at KubeMQ max (12 hours = 43200 seconds)
        if delay_seconds > 43200:
            logger.warning(
                "Delay %ds exceeds KubeMQ max 43200s (12h), capping at 43200s",
                delay_seconds,
            )
            delay_seconds = 43200

        # Extract priority for tags
        priority = str(properties.get("priority", 0))

        # Extract headers as metadata for debugging visibility
        metadata = json.dumps(headers) if headers else ""

        # Build QueueMessage
        msg_kwargs: dict[str, Any] = {
            "channel": sanitize_queue_name(queue),
            "body": body,
            "metadata": metadata or None,
            "tags": {"priority": priority},
        }
        if delay_seconds > 0:
            msg_kwargs["delay_in_seconds"] = delay_seconds
        if self.max_receive_count > 0 and self.dead_letter_queue:
            msg_kwargs["max_receive_count"] = self.max_receive_count
            msg_kwargs["max_receive_queue"] = sanitize_queue_name(self.dead_letter_queue)

        msg = QueueMessage(**msg_kwargs)
        self._kubemq_queues_client.send_queue_message(msg)

    def _get(self, queue: str, timeout: int | None = None) -> dict:
        """Receive a Celery message from a KubeMQ Queue channel.

        Uses auto_ack=True when the consumer was registered with no_ack=True
        (Celery default, task_acks_late=False). Uses auto_ack=False when
        task_acks_late=True for manual ack mode. See spec section 6.3.
        """
        auto_ack = self._is_auto_ack_queue(queue)
        response = self._kubemq_queues_client.receive_queue_messages(
            channel=sanitize_queue_name(queue),
            max_messages=1,
            wait_timeout_in_seconds=self.wait_timeout,
            auto_ack=auto_ack,
        )

        if not response.messages:
            raise Empty()

        msg = response.messages[0]
        payload = json.loads(msg.body)

        # Store KubeMQ message reference for native ack (only if not auto-acked)
        if not auto_ack:
            delivery_tag = payload.get("properties", {}).get("delivery_tag")
            if delivery_tag:
                self._kubemq_msg_refs[delivery_tag] = msg

        return payload

    def _purge(self, queue: str) -> int:
        """Purge all messages from a KubeMQ Queue channel.

        Returns the actual count of purged messages from the server.
        Returns 0 if the queue is empty or does not exist.
        """
        try:
            return self._kubemq_queues_client.ack_all_queue_messages(
                channel=sanitize_queue_name(queue),
            )
        except (KubeMQMessageError, KubeMQChannelError):
            return 0

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
        """
        try:
            msg_ref = self._kubemq_msg_refs.pop(delivery_tag)
            if requeue:
                msg_ref.re_queue(msg_ref.channel)
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
        super().basic_reject(delivery_tag, requeue)

    def close(self) -> None:
        """Clean up KubeMQ client resources."""
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

    # --- Fanout methods ---

    def _put_fanout(
        self, exchange: str, message: dict, routing_key: str | None = None, **kwargs: Any
    ) -> None:
        """Publish to all subscribers via KubeMQ Events."""
        body = json.dumps(message).encode("utf-8")
        event = EventMessage(
            channel=sanitize_queue_name(exchange),
            body=body,
            metadata=json.dumps({"routing_key": routing_key or ""}),
        )
        self._kubemq_pubsub_client.send_event(event)

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

    def _on_fanout_event(self, exchange: str, event: Any) -> None:
        """Handle incoming fanout event -- decode and dispatch to virtual layer.

        Decodes the event body and dispatches to bound queues.
        The _lookup() call is protected by _fanout_lock (reads shared
        binding state), but the actual _put() network calls happen
        outside the lock to avoid blocking subscribe/close operations.
        """
        try:
            message = json.loads(event.body)
        except Exception as exc:
            logger.warning("Error decoding fanout event on %s: %s", exchange, exc)
            return
        try:
            with self._fanout_lock:
                queues = self._lookup(exchange, "")
        except Exception:
            queues = []
        for queue in queues:
            try:
                self._put(queue, message)
            except Exception as exc:
                logger.warning("Error dispatching fanout message to %s: %s", queue, exc)

    def _on_fanout_error(self, exchange: str, err: Any) -> None:
        """Handle fanout subscription error."""
        logger.warning("Fanout subscription error on %s: %s", exchange, err)

    def _put_fanout_message(self, exchange: str, message: dict) -> None:
        """Inject a received fanout message into the virtual layer's buffer.

        Uses the public self._put(queue, message) method for each queue
        bound to this exchange, which is the standard Kombu virtual
        transport approach for delivering fanout messages.

        IMPLEMENTATION NOTE: Verify the exact Kombu virtual.Channel
        mechanism for looking up exchange->queue bindings. In Kombu >=5.4,
        check virtual.Channel._lookup() or the exchange-to-queue binding
        table. The _lookup(exchange, routing_key) method returns the list
        of queues bound to an exchange and is the public API for this.
        Adjust the binding lookup below if Kombu internals differ.
        """
        # Use Kombu's exchange-to-queue binding lookup to find
        # all queues bound to this fanout exchange, then deliver
        # the message to each via the public _put() method.
        try:
            queues = self._lookup(exchange, "")
        except Exception:
            queues = []
        for queue in queues:
            self._put(queue, message)

    # --- Advanced feature methods ---

    def _size(self, queue: str) -> int:
        """Return the number of waiting messages in a KubeMQ Queue channel.

        Uses list_queues_channels with exact name matching (the search
        is wildcard-based, so we filter results by exact channel name).
        """
        sanitized = sanitize_queue_name(queue)
        try:
            channels = self._kubemq_queues_client.list_queues_channels(
                channel_search=sanitized,
            )
            for ch in channels:
                if ch.name == sanitized:
                    return ch.incoming.waiting
        except Exception:
            pass
        return 0

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        """Delete a KubeMQ Queue channel. Ignores 'not found' errors."""
        try:
            self._kubemq_queues_client.delete_queues_channel(
                channel=sanitize_queue_name(queue),
            )
        except Exception:
            pass  # ignore not-found or other errors

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
        except Exception:
            return False


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
        """Validate connection by pinging KubeMQ broker."""
        conninfo = self.client
        # Parse TLS from URL scheme
        if conninfo.transport_cls and "+tls" in str(conninfo.transport_cls):
            conninfo.ssl = True
        conn = super().establish_connection()
        # Trigger channel creation to verify connectivity
        channel = self.create_channel(conn)
        try:
            _ = channel._kubemq_queues_client  # triggers lazy creation + ping
        except Exception:
            channel.close()
            raise
        # Close the temporary verification channel to avoid leaking
        # a gRPC connection. The real working channel is created later
        # by Kombu when it calls create_channel() on the returned conn.
        channel.close()
        return conn

    def close_connection(self, connection) -> None:
        """Close connection and all channels."""
        super().close_connection(connection)

    def verify_connection(self, connection) -> bool:
        """Check if connection is alive via ping."""
        try:
            if self._avail_channels:
                channel = next(iter(self._avail_channels))
                channel._kubemq_queues_client.ping()
                return True
        except Exception:
            return False
        return True

    def as_uri(self, uri: str, include_password: bool = False, mask: str = "**") -> str:
        """Format URI for display, masking auth token."""
        if not include_password and "@" in uri:
            scheme, rest = uri.split("://", 1)
            if "@" in rest:
                _creds, host = rest.rsplit("@", 1)
                return f"{scheme}://:{mask}@{host}"
        return uri
