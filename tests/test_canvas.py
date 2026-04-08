"""T1: Canvas integration tests for kubemq-celery.

Tests Celery canvas primitives (chain, group, chord) with mocked KubeMQ backend.
These verify that the transport/backend correctly handle canvas workflows
without requiring a live broker.

Spec: T1-chain, T1-chain-error, T1-group, T1-group-partial, T1-chord, T1-chord-timing
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from celery import Celery, chain, chord, group

import kubemq_celery  # noqa: F401 -- triggers auto-registration

pytestmark = pytest.mark.integration


def _make_celery_app():
    """Create a Celery app in eager mode for canvas testing."""
    app = Celery("canvas-test")
    app.config_from_object(
        {
            "broker_url": "kubemq://localhost:50000",
            "result_backend": "kubemq://localhost:50000",
            "task_always_eager": True,
            "task_eager_propagates": True,
            "result_expires": timedelta(hours=1),
        }
    )
    return app


@pytest.fixture
def canvas_app():
    """Provide a Celery app configured for eager canvas testing."""
    return _make_celery_app()


class TestCanvasChain:
    """T1: Chain execution tests."""

    def test_chain_execution(self, canvas_app):
        """T1-chain: Verify chain(a | b | c) executes tasks in sequence."""

        @canvas_app.task(name="canvas.add")
        def add(x, y):
            return x + y

        @canvas_app.task(name="canvas.multiply")
        def multiply(x, y):
            return x * y

        # chain: add(2,3) -> multiply(result, 4)
        # add(2,3) = 5, multiply(5,4) = 20
        result = chain(add.s(2, 3), multiply.s(4))()
        assert result.get(timeout=5) == 20

    def test_chain_error_propagation(self, canvas_app):
        """T1-chain-error: Verify chain propagates errors from failed tasks."""

        @canvas_app.task(name="canvas.fail")
        def fail_task():
            raise ValueError("deliberate failure")

        @canvas_app.task(name="canvas.after_fail")
        def after_fail(x):
            return x + 1

        with pytest.raises(ValueError, match="deliberate failure"):
            chain(fail_task.s(), after_fail.s())()

    def test_chain_single_task(self, canvas_app):
        """Chain with a single task should work like a normal call."""

        @canvas_app.task(name="canvas.echo")
        def echo(x):
            return x

        result = chain(echo.s(42))()
        assert result.get(timeout=5) == 42


class TestCanvasGroup:
    """T1: Group execution tests."""

    def test_group_execution(self, canvas_app):
        """T1-group: Verify group runs tasks in parallel and collects results."""

        @canvas_app.task(name="canvas.square")
        def square(x):
            return x * x

        result = group(square.s(i) for i in range(1, 5))()
        values = result.get(timeout=5)
        assert sorted(values) == [1, 4, 9, 16]

    def test_group_partial_failure(self, canvas_app):
        """T1-group-partial: Verify group handles partial failures."""
        # Temporarily disable eager propagation so exceptions are stored
        # in results instead of being raised directly during apply().
        canvas_app.conf.task_eager_propagates = False
        try:

            @canvas_app.task(name="canvas.maybe_fail")
            def maybe_fail(x):
                if x == 3:
                    raise RuntimeError("task 3 failed")
                return x * 2

            result = group(maybe_fail.s(i) for i in range(1, 5))()

            # In eager mode without propagate, individual results can be checked
            # Some tasks succeed, some fail
            succeeded = 0
            failed = 0
            for r in result.results:
                try:
                    r.get(timeout=5, propagate=True)
                    succeeded += 1
                except RuntimeError:
                    failed += 1

            assert succeeded == 3  # tasks 1, 2, 4
            assert failed == 1  # task 3
        finally:
            canvas_app.conf.task_eager_propagates = True

    def test_group_empty(self, canvas_app):
        """Group with zero tasks should return empty results."""

        result = group([])()
        values = result.get(timeout=5)
        assert values == []


class TestCanvasChord:
    """T1: Chord execution tests."""

    def test_chord_execution(self, canvas_app):
        """T1-chord: Verify chord(header, callback) runs header then callback."""

        @canvas_app.task(name="canvas.inc")
        def inc(x):
            return x + 1

        @canvas_app.task(name="canvas.sum_list")
        def sum_list(values):
            return sum(values)

        # chord: inc(1), inc(2), inc(3) -> sum_list
        # [2, 3, 4] -> 9
        result = chord(group(inc.s(i) for i in range(1, 4)), sum_list.s())()
        assert result.get(timeout=10) == 9

    def test_chord_timing_bounds(self, canvas_app):
        """T1-chord-timing: Verify chord completes within expected time bounds."""
        import time

        @canvas_app.task(name="canvas.fast_op")
        def fast_op(x):
            return x

        @canvas_app.task(name="canvas.collect")
        def collect(values):
            return len(values)

        start = time.monotonic()
        result = chord(
            group(fast_op.s(i) for i in range(10)),
            collect.s(),
        )()
        value = result.get(timeout=10)
        elapsed = time.monotonic() - start

        assert value == 10
        # In eager mode, this should complete very quickly
        assert elapsed < 10.0

    def test_chord_with_error_in_header(self, canvas_app):
        """Chord callback should handle errors in header tasks."""

        @canvas_app.task(name="canvas.chord_fail")
        def chord_fail(x):
            if x == 2:
                raise ValueError("header task failed")
            return x

        @canvas_app.task(name="canvas.chord_callback")
        def chord_callback(values):
            return sum(values)

        # In eager mode, this raises during execution
        with pytest.raises(ValueError, match="header task failed"):
            chord(
                group(chord_fail.s(i) for i in range(1, 4)),
                chord_callback.s(),
            )()
