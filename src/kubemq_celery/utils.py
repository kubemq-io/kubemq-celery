"""Utility functions for kubemq-celery transport."""

from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

from kubemq.core.exceptions import ErrorCode


def is_not_found(exc: BaseException) -> bool:
    """Check if a KubeMQ exception has a NOT_FOUND error code."""
    return getattr(exc, "code", None) == ErrorCode.NOT_FOUND


# Characters that need replacement in KubeMQ channel names
_SANITIZE_MAP = str.maketrans(
    {
        "@": ".",
        "/": ".",
        "#": ".",
        " ": "_",
        "\x06": ".",  # Redis priority separator
    }
)

_VALID_CHANNEL_RE = re.compile(r"^[a-zA-Z0-9._\-]+$")


def sanitize_queue_name(name: str) -> str:
    """Sanitize Celery queue name to valid KubeMQ channel name.

    Celery conventions:
    - celery@worker1.celery.pidbox -> celery.worker1.celery.pidbox
    - celery\\x060 (Redis priority) -> celery.0
    - reply/celery/pidbox -> reply.celery.pidbox

    Rules:
    - Replace @ with .
    - Replace / with .
    - Replace # with .
    - Replace spaces with _
    - Replace \\x06 (Redis sep) with .
    - Collapse consecutive dots
    - Strip leading/trailing dots
    """
    result = name.translate(_SANITIZE_MAP)
    result = re.sub(r"\.{2,}", ".", result)  # collapse dots
    result = result.strip(".")
    # Validate that the sanitized result contains only valid KubeMQ
    # channel name characters. If not (unexpected input), strip any
    # remaining invalid characters.
    if result and not _VALID_CHANNEL_RE.match(result):
        result = re.sub(r"[^a-zA-Z0-9._\-]", "", result)
    if not result:
        raise ValueError(f"Queue name {name!r} sanitizes to empty string")
    return result


def format_grpc_address(hostname: str, port: int) -> str:
    """Build gRPC `host:port` with RFC 3986 bracketed IPv6 when needed."""
    if not hostname:
        hostname = "localhost"
    hn = hostname.strip()
    if hn.startswith("[") and hn.endswith("]"):
        inner = hn[1:-1]
        return f"[{inner}]:{port}"
    if ":" in hn:
        return f"[{hn}]:{port}"
    return f"{hn}:{port}"


def parse_result_url(url: str) -> dict:
    """Parse kubemq:// or kubemq+tls:// result backend URL.

    Format: kubemq://[:token@]host[:port][/vhost]

    Returns dict with: hostname (str), port (int), auth_token (str|None),
    tls_enabled (bool).

    Accepts both raw URLs (``kubemq+tls://...``) and Celery-normalized
    URLs (``tls://...``) for TLS detection.

    Raises ``ValueError`` on malformed host or port values.
    """
    if not url:
        return {
            "hostname": "localhost",
            "port": 50000,
            "auth_token": None,
            "tls_enabled": False,
        }

    # urlsplit needs a recognized scheme to parse authority correctly.
    # Replace kubemq[-variant]:// with http:// for parsing, but remember the scheme.
    original_scheme = url.split("://")[0] if "://" in url else ""
    tls_enabled = "tls" in original_scheme.lower()

    # Normalize scheme for urlsplit
    if "://" in url:
        normalized = "http://" + url.split("://", 1)[1]
    else:
        normalized = "http://" + url

    parts = urlsplit(normalized)

    # Extract auth token (percent-decoded)
    auth_token = None
    if parts.password:
        auth_token = unquote(parts.password)
    elif parts.username:
        auth_token = unquote(parts.username)

    # Extract hostname
    hostname = parts.hostname or "localhost"

    # Extract port
    port = parts.port or 50000

    return {
        "hostname": hostname,
        "port": port,
        "auth_token": auth_token,
        "tls_enabled": tls_enabled,
    }
