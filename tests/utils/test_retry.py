"""Tests for the centralised retry-policy decorators in
:mod:`backend.utils.retry`.

These cover the policy contracts (which exceptions retry, which don't,
how many attempts, exponential backoff). Per-call-site migration of
existing retry loops to these decorators is a separate per-site review
(see the module docstring's "Migration checklist").
"""

from __future__ import annotations

import socket
import sqlite3
import urllib.error
from unittest.mock import patch

import pytest

from backend.utils import retry as retry_mod
from backend.utils.retry import (
    HttpRetryable,
    generic_network_retry,
    http_api_retry,
    sqlite_busy_retry,
)


# ── http_api_retry ───────────────────────────────────────────────────────────


def test_http_api_retry_retries_on_HttpRetryable():
    """HttpRetryable is the canonical marker for retryable HTTP errors
    (429 + 5xx). The decorator should retry it up to max_attempts."""
    calls = {"n": 0}

    @http_api_retry(max_attempts=3, min_wait=0, max_wait=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise HttpRetryable("HTTP 500")
        return "ok"

    with patch("time.sleep"):  # tenacity uses time.sleep for waits
        assert flaky() == "ok"
    assert calls["n"] == 3


def test_http_api_retry_retries_on_url_error():
    """URLError (DNS / connection refused) is in the retry set."""
    calls = {"n": 0}

    @http_api_retry(max_attempts=3, min_wait=0, max_wait=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise urllib.error.URLError("connection refused")
        return "ok"

    with patch("time.sleep"):
        assert flaky() == "ok"
    assert calls["n"] == 2


def test_http_api_retry_retries_on_timeout_and_connection_errors():
    for exc in [TimeoutError("read timed out"), ConnectionError("reset"), socket.timeout()]:
        calls = {"n": 0}

        @http_api_retry(max_attempts=3, min_wait=0, max_wait=0)
        def flaky():
            calls["n"] += 1
            if calls["n"] < 2:
                raise exc
            return "ok"

        with patch("time.sleep"):
            assert flaky() == "ok"
        assert calls["n"] == 2, f"failed for {type(exc).__name__}"


def test_http_api_retry_does_not_retry_on_generic_exception():
    """An unrelated exception (e.g. ValueError) bubbles immediately."""

    @http_api_retry(max_attempts=3, min_wait=0, max_wait=0)
    def boom():
        raise ValueError("bad input")

    with pytest.raises(ValueError, match="bad input"):
        boom()


def test_http_api_retry_reraises_underlying_on_exhaustion():
    """After max_attempts the original exception bubbles up (reraise=True)."""

    @http_api_retry(max_attempts=2, min_wait=0, max_wait=0)
    def always_fail():
        raise HttpRetryable("HTTP 503")

    with patch("time.sleep"):
        with pytest.raises(HttpRetryable, match="HTTP 503"):
            always_fail()


def test_http_api_retry_logs_each_retry(monkeypatch, caplog):
    """before_sleep_log emits a `[retry] attempt=N` log line per retry."""
    calls = {"n": 0}

    @http_api_retry(max_attempts=3, min_wait=0, max_wait=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise HttpRetryable("HTTP 502")
        return "ok"

    with patch("time.sleep"), caplog.at_level("WARNING", logger="backend.utils.retry"):
        flaky()

    retry_log_lines = [r for r in caplog.records if "[retry]" in r.message]
    assert len(retry_log_lines) >= 1
    assert any("attempt=" in r.message for r in retry_log_lines)


# ── sqlite_busy_retry ────────────────────────────────────────────────────────


def test_sqlite_busy_retry_retries_on_sqlite_operational_error():
    calls = {"n": 0}

    @sqlite_busy_retry(max_attempts=4, min_wait=0, max_wait=0)
    def flaky_write():
        calls["n"] += 1
        if calls["n"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "wrote"

    with patch("time.sleep"):
        assert flaky_write() == "wrote"
    assert calls["n"] == 3


def test_sqlite_busy_retry_does_not_retry_on_integrity_error():
    """IntegrityError (constraint violation) is NOT a busy condition and
    must NOT be retried — retrying a UNIQUE constraint violation 5 times
    just delays the inevitable."""

    @sqlite_busy_retry(max_attempts=4, min_wait=0, max_wait=0)
    def boom():
        raise sqlite3.IntegrityError("UNIQUE constraint failed")

    with pytest.raises(sqlite3.IntegrityError):
        boom()


def test_sqlite_busy_retry_reraises_on_exhaustion():
    @sqlite_busy_retry(max_attempts=2, min_wait=0, max_wait=0)
    def always_busy():
        raise sqlite3.OperationalError("database is locked")

    with patch("time.sleep"):
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            always_busy()


# ── generic_network_retry ────────────────────────────────────────────────────


def test_generic_network_retry_retries_on_oserror():
    calls = {"n": 0}

    @generic_network_retry(max_attempts=3, min_wait=0, max_wait=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise OSError("network unreachable")
        return "ok"

    with patch("time.sleep"):
        assert flaky() == "ok"
    assert calls["n"] == 2


def test_generic_network_retry_does_not_retry_on_value_error():
    @generic_network_retry(max_attempts=3, min_wait=0, max_wait=0)
    def boom():
        raise ValueError("bad input")

    with pytest.raises(ValueError):
        boom()


# ── before_sleep_log shape ───────────────────────────────────────────────────


def test_before_sleep_log_handles_missing_outcome_safely():
    """The hook reads retry_state.outcome which CAN be None mid-construction.
    Verify it doesn't crash. Important because tenacity 9.x reworked
    RetryCallState semantics and the hook is forward-compatible.
    """

    class _FakeAction:
        sleep = 1.5

    class _FakeState:
        outcome = None
        attempt_number = 2

        def __init__(self):
            self.fn = lambda: None
            self.fn.__name__ = "test_fn"
            self.next_action = _FakeAction()

    # Should not raise
    retry_mod._before_sleep_log(_FakeState())  # type: ignore[arg-type]
