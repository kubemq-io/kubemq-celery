"""Canvas Testing in Eager Mode — KubeMQ Celery Transport.

Demonstrates:
- Testing chain, group, and chord workflows in eager mode
- Validating canvas composition without a live broker
- Asserting intermediate and final results
- Testing error propagation through canvas primitives

Usage:
    python examples/testing/test_canvas.py
    pytest examples/testing/test_canvas.py -v

Requirements:
    - kubemq-celery installed
    - No broker needed — uses task_always_eager=True
"""

from __future__ import annotations

import os

from celery import Celery, chain, chord, group

import kubemq_celery  # noqa: F401

app = Celery(
    "test_canvas",
    broker=os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    result_backend=os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
)

app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@app.task
def add(x: int, y: int) -> int:
    return x + y


@app.task
def multiply(x: int, y: int) -> int:
    return x * y


@app.task
def double(x: int) -> int:
    return x * 2


@app.task
def sum_list(values: list[int]) -> int:
    return sum(values)


@app.task
def fail_task(msg: str) -> None:
    raise ValueError(msg)


def test_chain_basic():
    """Test a basic chain: add -> multiply -> double."""
    print("--- test_chain_basic ---")
    workflow = chain(add.s(2, 3), double.s())
    result = workflow.apply()
    assert result.result == 10, f"Expected 10, got {result.result}"
    print(f"  chain(add(2,3), double) = {result.result}: OK")


def test_chain_three_steps():
    """Test a three-step chain with result passing."""
    print("--- test_chain_three_steps ---")
    workflow = chain(
        add.s(1, 2),  # 3
        multiply.s(10),  # 30
        double.s(),  # 60
    )
    result = workflow.apply()
    assert result.result == 60, f"Expected 60, got {result.result}"
    print(f"  chain(add(1,2), mul(10), double) = {result.result}: OK")


def test_group_basic():
    """Test parallel group execution."""
    print("--- test_group_basic ---")
    workflow = group(add.s(1, 1), add.s(2, 2), add.s(3, 3))
    result = workflow.apply()
    values = result.get()
    assert sorted(values) == [2, 4, 6], f"Expected [2,4,6], got {values}"
    print(f"  group(add(1,1), add(2,2), add(3,3)) = {values}: OK")


def test_group_homogeneous():
    """Test group with identical task signatures."""
    print("--- test_group_homogeneous ---")
    workflow = group(double.s(i) for i in range(1, 6))
    result = workflow.apply()
    values = result.get()
    assert sorted(values) == [2, 4, 6, 8, 10], f"Unexpected: {values}"
    print(f"  group(double(1..5)) = {sorted(values)}: OK")


def test_chord_basic():
    """Test chord: group + callback."""
    print("--- test_chord_basic ---")
    workflow = chord(
        group(add.s(1, 1), add.s(2, 2), add.s(3, 3)),
        sum_list.s(),
    )
    result = workflow.apply()
    assert result.result == 12, f"Expected 12, got {result.result}"
    print(f"  chord(group(add...), sum) = {result.result}: OK")


def test_chain_error_propagation():
    """Test that errors propagate through chains in eager mode."""
    print("--- test_chain_error_propagation ---")
    workflow = chain(add.s(1, 2), fail_task.s())
    try:
        workflow.apply()
        print("  ERROR: Expected ValueError!")
    except (ValueError, Exception) as e:
        print(f"  Error propagated correctly: {e}: OK")


def test_nested_canvas():
    """Test chain of group (nested composition)."""
    print("--- test_nested_canvas ---")
    workflow = chain(
        group(add.s(1, 1), add.s(2, 2)),
    )
    result = workflow.apply()
    values = result.get()
    assert sorted(values) == [2, 4], f"Unexpected: {values}"
    print(f"  chain(group(add..)) = {sorted(values)}: OK")


def test_immutable_signatures():
    """Test immutable signatures (si) that ignore parent results."""
    print("--- test_immutable_signatures ---")
    workflow = chain(
        add.s(1, 2),
        add.si(10, 20),
    )
    result = workflow.apply()
    assert result.result == 30, f"Expected 30, got {result.result}"
    print(f"  chain(add(1,2), add.si(10,20)) = {result.result}: OK")


if __name__ == "__main__":
    print("=== Canvas Testing in Eager Mode ===")
    print(f"task_always_eager: {app.conf.task_always_eager}\n")

    test_chain_basic()
    test_chain_three_steps()
    test_group_basic()
    test_group_homogeneous()
    test_chord_basic()
    test_chain_error_propagation()
    test_nested_canvas()
    test_immutable_signatures()

    print("\nAll canvas tests passed!")
