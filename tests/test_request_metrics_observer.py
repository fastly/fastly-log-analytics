from __future__ import annotations

import copy
import threading
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from backend import request_metrics_observer as module


@pytest.fixture
def observer_state(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(module.config, "list_configs", lambda: [{"name": "test-service"}])
    monkeypatch.setattr(module.config, "config_to_source", lambda cfg: cfg)
    state = {"request": {"total_rows": 1, "latest_log_at": "2026-09-07T00:00:00Z", "last_sync_at": None}}
    persisted = {}
    calls = []

    def refresh(source, **kwargs):
        persisted.clear()
        persisted.update(copy.deepcopy(state))
        calls.append("persist")
        return copy.deepcopy(state)

    def publish(sid, snapshot):
        assert snapshot == persisted
        calls.append("publish")

    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh)
    monkeypatch.setattr(module, "compute_sync_status_cached", lambda sid: copy.deepcopy(persisted))
    monkeypatch.setattr(module.publisher, "publish", publish)
    return module.RequestMetricsObserver(), state, persisted, calls


def test_persists_before_publishing_only_changed_observations(observer_state):
    observer, state, _, calls = observer_state
    observer.reconcile()
    observer.reconcile()
    assert calls == ["persist", "publish", "persist"]
    state["request"]["last_sync_at"] = "2026-09-08T18:00:00Z"
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]
    state["request"].update(latest_log_at=None, total_rows=0)
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]


def test_failed_observation_does_not_publish_or_discard_last_success(observer_state, monkeypatch, caplog):
    observer, _, persisted, calls = observer_state
    observer.reconcile()
    before = copy.deepcopy(persisted)
    monkeypatch.setattr(module, "refresh_durable_request_metrics", MagicMock(side_effect=RuntimeError("unavailable")))
    observer.reconcile()
    assert persisted == before
    assert calls == ["persist", "publish"]
    assert "request_metrics.refresh_failed" in caplog.text


def test_concurrent_passes_coalesce_without_overlapping(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    entered, release = threading.Event(), threading.Event()
    refresh = module.refresh_durable_request_metrics

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return refresh(*args, **kwargs)

    monkeypatch.setattr(module, "refresh_durable_request_metrics", slow)
    thread = threading.Thread(target=observer.reconcile)
    thread.start()
    try:
        assert entered.wait(2)
        observer.reconcile()
        assert calls == []
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive()
    assert calls == ["persist", "publish"]


def test_lifecycle_disabled_in_file_mode(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: False)
    observer = module.RequestMetricsObserver()
    observer.start()
    assert observer._thread is None
    observer.stop()


def test_lifecycle_runs_with_paused_crons_and_stops(observer_state, monkeypatch):
    observer, _, _, _ = observer_state
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    entered = threading.Event()
    monkeypatch.setattr(observer, "reconcile", entered.set)
    observer.start()
    first_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.start()
        assert observer._thread is first_thread
    finally:
        observer.stop()
    assert not first_thread.is_alive()
    assert observer._thread is None


def test_periodic_reconciliation_continues_without_worker_notifications(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    monkeypatch.setattr(module, "RECONCILE_INTERVAL_S", 0.01)
    twice = threading.Event()
    reconcile = observer.reconcile

    def observed_pass():
        reconcile()
        if calls.count("persist") >= 2:
            twice.set()

    monkeypatch.setattr(observer, "reconcile", observed_pass)
    observer.start()
    try:
        assert twice.wait(2)
    finally:
        observer.stop()
    assert calls.count("publish") == 1


def test_bounded_stop_does_not_allow_overlapping_restart(observer_state, monkeypatch, caplog):
    observer, _, _, _ = observer_state
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(module, "STOP_TIMEOUT_S", 0.01)

    def blocked_dependency():
        entered.set()
        release.wait(2)

    monkeypatch.setattr(observer, "reconcile", blocked_dependency)
    observer.start()
    first_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.stop()
        assert first_thread.is_alive()
        observer.start()
        assert observer._thread is first_thread
        assert "request_metrics.stop_timeout" in caplog.text
    finally:
        release.set()
        first_thread.join(2)
        observer.stop()
    assert observer._thread is None


@pytest.mark.asyncio
async def test_application_lifespan_owns_observer_order(monkeypatch):
    from backend import main

    calls = []

    @asynccontextmanager
    async def application(app):
        calls.append("application_start")
        yield
        calls.append("application_stop")

    monkeypatch.setattr(main, "_application_lifespan", application)
    monkeypatch.setattr(module.observer, "start", lambda: calls.append("observer_start"))
    monkeypatch.setattr(module.observer, "stop", lambda: calls.append("observer_stop"))
    monkeypatch.setattr(main.request_telemetry, "force_flush", lambda: None)
    monkeypatch.setattr(main.clickhouse_observer, "stop_clickhouse_observer", lambda: None)
    monkeypatch.setattr(main.clickhouse_client, "close_clickhouse_client", lambda: None)
    async with main.lifespan(main.app):
        calls.append("serving")
    assert calls == ["application_start", "observer_start", "serving", "observer_stop", "application_stop"]
