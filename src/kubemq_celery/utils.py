"""Utility functions for kubemq-celery transport."""

from __future__ import annotations

import re

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


def parse_broker_url(url: str) -> dict:
    """Parse kubemq:// or kubemq+tls:// URL into connection params.

    Format: kubemq://[:token@]host[:port][/vhost]

    Returns dict with: tls_enabled (bool).
    Kombu handles most URL parsing via conninfo -- this extracts
    transport-specific URL logic only.
    """
    tls_enabled = "+tls" in url.split("://")[0] if "://" in url else False
    return {"tls_enabled": tls_enabled}


def parse_result_url(url: str) -> dict:
    """Parse kubemq:// or kubemq+tls:// result backend URL.

    Format: kubemq://[:token@]host[:port][/vhost]

    Returns dict with: hostname (str), port (int), auth_token (str|None),
    tls_enabled (bool).

    Used by KubeMQResultBackend to extract connection parameters from
    the result_backend URL (spec section 5.3).
    """
    hostname = "localhost"
    port = 50000
    auth_token = None
    tls_enabled = "+tls" in url.split("://")[0] if "://" in url else False

    if url and "://" in url:
        _scheme, rest = url.split("://", 1)
        # Handle auth: kubemq://:token@host:port
        if "@" in rest:
            creds_part, host_part = rest.rsplit("@", 1)
            auth_token = creds_part.lstrip(":")
        else:
            host_part = rest
        # Strip vhost
        host_part = host_part.split("/")[0]
        # IPv6: [::1]:50000
        if host_part.startswith("["):
            end = host_part.find("]")
            if end != -1:
                hostname = host_part[1:end]
                port = 50000
                if len(host_part) > end + 1 and host_part[end + 1] == ":":
                    try:
                        port = int(host_part[end + 2 :])
                    except ValueError:
                        port = 50000
            else:
                hostname = host_part
        elif ":" in host_part:
            hostname, port_str = host_part.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                port = 50000
        else:
            hostname = host_part

    return {
        "hostname": hostname,
        "port": port,
        "auth_token": auth_token,
        "tls_enabled": tls_enabled,
    }
