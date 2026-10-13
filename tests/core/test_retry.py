"""Unit tests for the shared :mod:`backend.core.retry` policy.

Covers the invariants the migrated bespoke loops depend on: retryable
exceptions are retried up to the budget, non-retryable ones re-raise
immediately (never broadened), the backoff sequence is exactly
``base_delay * multiplier ** (n - 1)`` clamped to ``max_delay``, the
``before_retry`` hook fires between attempts, and both the attempt and
wall-clock budgets are honored.
"""

from __future__ import annotations

import time

import pytest

from backend.core.retry import retry_call


class _Transient(Exception):
    pass


class _Fatal(Exception):
    pass


def _is_transient(exc: BaseException) -> bool:
    return isinstance(exc, _Transient)


def test_success_passthrough_no_sleep() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        return "ok"

    result = retry_call(fn, retry_on=_is_transient, attempts=5, sleep=sleeps.append)

    assert result == "ok"
    assert calls["n"] == 1
    assert sleeps == []


def test_retryable_retried_to_budget_then_reraises() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=4,
            base_delay=1.0,
            multiplier=2.0,
            sleep=sleeps.append,
        )

    # 4 attempts -> 3 sleeps between them
    assert calls["n"] == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_fatal_not_retried() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []


def test_eventual_success_stops_retrying() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("retry me")
        return "done"

    result = retry_call(
        fn,
        retry_on=_is_transient,
        attempts=5,
        base_delay=1.0,
        multiplier=2.0,
        sleep=sleeps.append,
    )

    assert result == "done"
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_max_delay_clamps_backoff() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            multiplier=10.0,
            max_delay=5.0,
            sleep=sleeps.append,
        )

    # 1, min(10,5)=5, min(100,5)=5, min(1000,5)=5
    assert sleeps == [1.0, 5.0, 5.0, 5.0]


def test_constant_backoff_via_multiplier_one() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            multiplier=1.0,
            sleep=sleeps.append,
        )

    assert sleeps == [1.0, 1.0]


def test_before_retry_fires_between_attempts_with_exception() -> None:
    sleeps: list[float] = []
    seen: list[BaseException] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient(f"attempt-{calls['n']}")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            before_retry=seen.append,
            sleep=sleeps.append,
        )

    # Hook runs once per retry (not after the final give-up attempt).
    assert len(seen) == 2
    assert all(isinstance(e, _Transient) for e in seen)
    assert [str(e) for e in seen] == ["attempt-1", "attempt-2"]


def test_before_retry_not_called_on_fatal() -> None:
    seen: list[BaseException] = []

    def fn() -> None:
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            before_retry=seen.append,
            sleep=lambda _d: None,
        )

    assert seen == []


def test_max_elapsed_budget_stops_early() -> None:
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    # tenacity's stop_after_delay reads real monotonic time, so a real (tiny)
    # sleep is required for the wall-clock budget to advance. Kept small so the
    # test stays fast while still proving the budget cuts off well before the
    # attempt cap.
    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1000,
            base_delay=0.02,
            multiplier=1.0,
            max_elapsed=0.05,
            sleep=time.sleep,
        )

    # Budget cut it off well before the 1000-attempt cap.
    assert calls["n"] < 1000


def test_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        retry_call(lambda: None, retry_on=_is_transient, attempts=0)


def test_attempts_one_calls_once_and_propagates() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []
