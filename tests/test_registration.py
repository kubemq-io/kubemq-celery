"""Tests for transport and backend registration.

Verifies that importing kubemq_celery registers all transport aliases
and backend aliases correctly.
"""

from __future__ import annotations

import pytest


class TestTransportRegistration:
    """Tests for transport alias registration."""

    def test_kubemq_alias_registered(self):
        """Verify kubemq:// alias is registered."""
        from kombu.transport import TRANSPORT_ALIASES

        import kubemq_celery  # noqa: F401

        assert "kubemq" in TRANSPORT_ALIASES
        assert TRANSPORT_ALIASES["kubemq"] == "kubemq_celery.transport:Transport"

    def test_kubemq_tls_alias_registered(self):
        """Verify kubemq+tls:// alias is registered."""
        from kombu.transport import TRANSPORT_ALIASES

        import kubemq_celery  # noqa: F401

        assert "kubemq+tls" in TRANSPORT_ALIASES
        assert TRANSPORT_ALIASES["kubemq+tls"] == "kubemq_celery.transport:Transport"


class TestBackendRegistration:
    """Tests for backend alias registration."""

    def test_kubemq_backend_alias_registered(self):
        """Verify kubemq:// backend alias is registered."""
        from celery.app.backends import BACKEND_ALIASES

        import kubemq_celery  # noqa: F401

        assert "kubemq" in BACKEND_ALIASES
        assert BACKEND_ALIASES["kubemq"] == "kubemq_celery.backend:KubeMQResultBackend"


class TestVersion:
    """Tests for package version."""

    def test_version_string(self):
        """Verify __version__ is set correctly."""
        import kubemq_celery

        assert kubemq_celery.__version__ == "1.1.0"

    def test_all_exports(self):
        """Verify __all__ includes expected names."""
        import kubemq_celery

        assert "Transport" in kubemq_celery.__all__
        assert "KubeMQResultBackend" in kubemq_celery.__all__

    def test_getattr_unknown_raises(self):
        """Verify __getattr__ raises AttributeError for unknown names."""
        import kubemq_celery

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = kubemq_celery.NonExistentThing
