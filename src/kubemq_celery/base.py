"""Shared business logic for KubeMQ Celery channels.

Provides a structural ABC base class (BaseKubeMQChannel) that is inherited
by Channel (transport.py) via cooperative multiple inheritance.

CONCRETE methods (inherited as-is, contain full implementation):
- _build_queue_message_kwargs() -- build QueueMessage kwargs from Celery message
- _calculate_delay_seconds() -- extract delay from countdown/eta headers
- _calculate_expiration_seconds() -- calculate per-message TTL
- _decode_fanout_event() -- decode fanout event JSON body
- _deserialize_message() -- deserialize received message body

ABSTRACT methods (must be overridden by sync/async subclasses):
- _backoff_sleep(seconds) -- sync: time.sleep(); async: await asyncio.sleep()
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from kubemq_celery.utils import sanitize_queue_name

logger = logging.getLogger("kubemq_celery")

# KubeMQ Python SDK defaults ack_all_queue_messages(wait_time_seconds=60). For purge
# (empty queues), that blocks up to 60s per call — use a short wait for Kombu/Celery semantics.
DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS = 1


class BaseKubeMQChannel(ABC):
    """Shared business logic for sync and async KubeMQ channels.

    Inherited by both Channel and AsyncChannel via cooperative
    multiple inheritance (MRO: Channel -> BaseKubeMQChannel -> virtual.Channel).
    """

    # Constants
    MAX_DELAY_SECONDS: int = 86400
    MAX_EXPIRATION_SECONDS: int = 86400
    DEFAULT_BATCH_SIZE: int = 10
    MAX_BATCH_SIZE: int = 100

    # Subclass-provided attributes (set via from_transport_options in Channel/AsyncChannel).
    # Declared here for type checker compatibility (PY-7) and API contract clarity.
    message_expiration: int  # seconds; 0 = no expiration (C1)
    max_receive_count: int  # DLQ max receive count
    dead_letter_queue: str  # DLQ channel name

    # --- CONCRETE methods (full implementation, inherited by subclasses) ---

    def _build_queue_message_kwargs(
        self,
        queue: str,
        message: dict,
    ) -> dict[str, Any]:
        """Build QueueMessage constructor kwargs from a Celery message dict.

        Handles:
        - JSON serialization of message body
        - Delay extraction from headers (countdown/eta)
        - Per-message TTL from headers['expires'] or transport option message_expiration
        - DLQ policy from transport options
        - Priority tag extraction

        Returns:
            dict suitable for QueueMessage(**kwargs)
        """
        body = json.dumps(message).encode("utf-8")
        headers = message.get("headers", {}) or {}
        properties = message.get("properties", {}) or {}

        msg_kwargs: dict[str, Any] = {
            "channel": sanitize_queue_name(queue),
            "body": body,
            "metadata": json.dumps(headers) if headers else None,
            "tags": {"priority": str(properties.get("priority", 0))},
        }

        delay = self._calculate_delay_seconds(headers, properties)
        if delay > 0:
            msg_kwargs["delay_in_seconds"] = delay

        expiration = self._calculate_expiration_seconds(headers, self.message_expiration)
        if expiration > 0:
            msg_kwargs["expiration_in_seconds"] = expiration

        if self.max_receive_count > 0 and self.dead_letter_queue:
            msg_kwargs["max_receive_count"] = self.max_receive_count
            msg_kwargs["max_receive_queue"] = sanitize_queue_name(self.dead_letter_queue)

        return msg_kwargs

    def _calculate_delay_seconds(self, headers: dict, properties: dict) -> int:
        """Extract delay from Celery message headers.

        Checks countdown first, then eta. Caps at MAX_DELAY_SECONDS (86400s).
        """
        countdown = headers.get("countdown")
        eta = headers.get("eta")

        delay_seconds = 0
        if countdown is not None:
            try:
                delay_seconds = int(countdown)
            except (ValueError, TypeError):
                delay_seconds = 0
        elif eta:
            try:
                eta_dt = datetime.fromisoformat(eta)
                if eta_dt.tzinfo is None:
                    eta_dt = eta_dt.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                delay_seconds = max(0, int((eta_dt - now).total_seconds()))
            except (ValueError, TypeError):
                delay_seconds = 0

        if delay_seconds > self.MAX_DELAY_SECONDS:
            logger.warning(
                "Delay %ds exceeds KubeMQ max %ds (24h), capping",
                delay_seconds,
                self.MAX_DELAY_SECONDS,
            )
            delay_seconds = self.MAX_DELAY_SECONDS

        return delay_seconds

    def _calculate_expiration_seconds(
        self,
        headers: dict,
        message_expiration: int,
    ) -> int:
        """Calculate per-message TTL.

        Priority: per-task expires header > global message_expiration option.
        Caps at MAX_EXPIRATION_SECONDS (86400s) with WARNING log.
        Returns 0 if no expiration.
        """
        expiration_seconds = 0
        task_expires = headers.get("expires")
        task_expires_present = task_expires is not None
        if task_expires_present:
            if isinstance(task_expires, (int, float)):
                expiration_seconds = int(task_expires)
            elif isinstance(task_expires, str):
                try:
                    exp_dt = datetime.fromisoformat(task_expires)
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    expiration_seconds = max(0, int((exp_dt - now).total_seconds()))
                except (ValueError, TypeError):
                    expiration_seconds = 0

        # Only fall back to global expiration when no per-task expires was
        # supplied. If a per-task expires was present but already elapsed
        # (expiration_seconds == 0), respect that — don't override with
        # the global TTL. Celery workers will discard expired tasks anyway.
        if expiration_seconds <= 0 and message_expiration > 0 and not task_expires_present:
            expiration_seconds = message_expiration

        if expiration_seconds > self.MAX_EXPIRATION_SECONDS:
            logger.warning(
                "Message expiration %ds exceeds KubeMQ max %ds (24h), capping",
                expiration_seconds,
                self.MAX_EXPIRATION_SECONDS,
            )
            expiration_seconds = self.MAX_EXPIRATION_SECONDS

        return expiration_seconds

    def _decode_fanout_event(self, event_body: bytes) -> dict | None:
        """Decode a fanout event body. Returns None on decode failure."""
        try:
            message = json.loads(event_body)
        except json.JSONDecodeError as exc:
            logger.warning("Error decoding fanout event body: %s", exc)
            return None
        if not isinstance(message, dict):
            logger.warning("Fanout event body is not a JSON object")
            return None
        return message

    def _deserialize_message(self, body: bytes) -> dict:
        """Deserialize a received message body from JSON."""
        return json.loads(body)

    # --- ABSTRACT methods (must be overridden by subclasses) ---

    @abstractmethod
    def _backoff_sleep(self, seconds: float) -> None:
        """Sleep for the given number of seconds during backoff.

        Sync Channel: ``def _backoff_sleep`` using ``time.sleep(seconds)``
        Async Channel: ``async def _backoff_sleep`` using ``await asyncio.sleep(seconds)``

        Note: The async subclass overrides this as ``async def``, changing
        the method signature from sync to async. This means the ABC is
        **structural** (shared-logic base), not Liskov-compliant -- callers
        must know whether they hold a sync or async subclass. This is
        acceptable because Channel and AsyncChannel are never used
        interchangeably; each is bound to its own Transport class.
        """
        ...
