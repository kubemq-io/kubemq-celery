"""Tests for kubemq_celery.utils module."""

from __future__ import annotations

import pytest

from kubemq_celery.utils import parse_result_url, sanitize_queue_name


class TestSanitizeQueueName:
    def test_at_sign_replaced(self):
        assert sanitize_queue_name("celery@worker1") == "celery.worker1"

    def test_slash_replaced(self):
        assert sanitize_queue_name("reply/celery/pidbox") == "reply.celery.pidbox"

    def test_hash_replaced(self):
        assert sanitize_queue_name("queue#1") == "queue.1"

    def test_redis_separator_replaced(self):
        assert sanitize_queue_name("celery\x060") == "celery.0"

    def test_space_replaced(self):
        assert sanitize_queue_name("queue name") == "queue_name"

    def test_consecutive_dots_collapsed(self):
        assert sanitize_queue_name("a..b...c") == "a.b.c"

    def test_leading_trailing_dots_stripped(self):
        assert sanitize_queue_name(".celery.") == "celery"

    def test_complex_celery_name(self):
        assert sanitize_queue_name("celery@worker1.celery.pidbox") == (
            "celery.worker1.celery.pidbox"
        )

    def test_already_valid_name(self):
        assert sanitize_queue_name("celery") == "celery"

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="sanitizes to empty"):
            sanitize_queue_name("")


class TestParseResultUrl:
    def test_basic_url(self):
        result = parse_result_url("kubemq://localhost:50000")
        assert result["hostname"] == "localhost"
        assert result["port"] == 50000
        assert result["auth_token"] is None
        assert result["tls_enabled"] is False

    def test_tls_url(self):
        result = parse_result_url("kubemq+tls://myhost:50001")
        assert result["hostname"] == "myhost"
        assert result["port"] == 50001
        assert result["tls_enabled"] is True

    def test_auth_url(self):
        result = parse_result_url("kubemq://:mytoken@localhost:50000")
        assert result["auth_token"] == "mytoken"
        assert result["hostname"] == "localhost"
        assert result["port"] == 50000

    def test_default_port(self):
        result = parse_result_url("kubemq://myhost")
        assert result["hostname"] == "myhost"
        assert result["port"] == 50000

    def test_empty_url(self):
        result = parse_result_url("")
        assert result["hostname"] == "localhost"
        assert result["port"] == 50000

    def test_invalid_port_raises(self):
        """Verify invalid port raises ValueError (fail-fast, not silent default)."""
        with pytest.raises(ValueError, match="Port"):
            parse_result_url("kubemq://localhost:notaport")

    def test_celery_normalized_tls_url(self):
        """Verify Celery-normalized tls:// scheme enables TLS."""
        result = parse_result_url("tls://myhost:50001")
        assert result["hostname"] == "myhost"
        assert result["port"] == 50001
        assert result["tls_enabled"] is True

    def test_percent_encoded_auth(self):
        """Verify percent-encoded auth token is decoded."""
        result = parse_result_url("kubemq://:my%40token@localhost:50000")
        assert result["auth_token"] == "my@token"

    def test_url_with_vhost(self):
        """Verify vhost is stripped from URL."""
        result = parse_result_url("kubemq://localhost:50000/vhost")
        assert result["hostname"] == "localhost"
        assert result["port"] == 50000

    def test_ipv6_url(self):
        result = parse_result_url("kubemq://[::1]:50001")
        assert result["hostname"] == "::1"
        assert result["port"] == 50001


class TestFormatGrpcAddress:
    """Tests for format_grpc_address."""

    def test_basic(self):
        from kubemq_celery.utils import format_grpc_address

        assert format_grpc_address("localhost", 50000) == "localhost:50000"

    def test_ipv6_bare(self):
        from kubemq_celery.utils import format_grpc_address

        assert format_grpc_address("::1", 50000) == "[::1]:50000"

    def test_ipv6_bracketed(self):
        from kubemq_celery.utils import format_grpc_address

        assert format_grpc_address("[::1]", 50000) == "[::1]:50000"

    def test_empty_hostname(self):
        from kubemq_celery.utils import format_grpc_address

        assert format_grpc_address("", 50000) == "localhost:50000"


class TestSanitizeQueueNameEdgeCases:
    """Edge case tests for sanitize_queue_name."""

    def test_invalid_chars_stripped(self):
        """Verify characters outside [a-zA-Z0-9._-] are stripped."""
        result = sanitize_queue_name("queue!name$test")
        assert result == "queuenametest"
        # Verify the result is valid
        import re

        assert re.match(r"^[a-zA-Z0-9._\-]+$", result)

    def test_mixed_invalid_and_known_chars(self):
        """Verify mix of known replacements and unknown chars."""
        result = sanitize_queue_name("q@a!b#c")
        # @ -> ., # -> ., ! stripped
        assert result == "q.a.b.c" or result == "q.ab.c"


class TestParseResultUrlIPv6Edge:
    """Edge case for IPv6 bracket parsing in parse_result_url."""

    def test_ipv6_with_invalid_port(self):
        """Verify IPv6 URL with invalid port raises ValueError."""
        with pytest.raises(ValueError, match="Port"):
            parse_result_url("kubemq://[::1]:badport")

    def test_ipv6_no_port(self):
        """Verify IPv6 URL without port uses default 50000."""
        result = parse_result_url("kubemq://[::1]")
        assert result["hostname"] == "::1"
        assert result["port"] == 50000

    def test_ipv6_unclosed_bracket(self):
        """Verify IPv6 URL with unclosed bracket raises ValueError."""
        with pytest.raises(ValueError):
            parse_result_url("kubemq://[::1")
