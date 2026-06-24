"""Tests for the process-level OOM stopgap (backend/core/memory_guard.py).

The guard SIGTERMs its own process when RSS crosses the restart threshold so
docker restarts it cleanly before the cgroup OOM-SIGKILL. ``os.kill`` is
mocked in every test — a real SIGTERM would kill the pytest process.
"""

import signal

import pytest

from backend.core import memory_guard


@pytest.fixture(autouse=True)
def _no_real_kill(monkeypatch):
    """Hard safety net: never let a real signal escape this module under test."""
    calls = []
    monkeypatch.setattr(memory_guard.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    return calls


def _set_rss(monkeypatch, value):
    """Patch the lazily-imported current_rss_bytes to return ``value`` bytes."""
    monkeypatch.setattr("backend.core.duckdb.current_rss_bytes", lambda: value)


def test_disabled_by_default(monkeypatch, _no_real_kill):
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    _set_rss(monkeypatch, 50_000 * 1024 * 1024)  # huge, but guard is off
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_below_threshold_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 5000 * 1024 * 1024)  # 5GB < 9GB
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_at_or_above_threshold_sends_sigterm(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 9500 * 1024 * 1024)  # 9.5GB >= 9GB
    assert memory_guard.maybe_graceful_restart() is True
    assert len(_no_real_kill) == 1
    pid, sig = _no_real_kill[0]
    assert sig == signal.SIGTERM
    assert pid > 0


def test_unreadable_rss_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, None)  # /proc unreadable (e.g. off-Linux)
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_threshold_parse_bytes(monkeypatch):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 9000 * 1024 * 1024
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "garbage")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0
