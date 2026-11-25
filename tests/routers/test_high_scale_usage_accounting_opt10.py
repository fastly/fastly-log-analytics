"""Focused unit tests for Round 10 High-Scale log-accounting and usage/current-storage optimizations."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# ── 1. Admin log-accounting ──────────────────────────────────────────────────


def test_log_accounting_caches_are_swr_and_registered():
    from backend.routers.admin import log_accounting as _la
    from backend.utils.bounded_cache import CacheRegistry, SWRTTLCache

    for cache_name in (
        "_FASTLY_COUNTS_CACHE",
        "_CLICKHOUSE_COUNTS_CACHE",
        "_LOCAL_COUNTS_CACHE",
        "_LATEST_INGEST_CACHE",
    ):
        cache = getattr(_la, cache_name)
        assert isinstance(cache, SWRTTLCache), f"{cache_name} must be an SWRTTLCache"
        assert f"routers.admin.log_accounting.{cache_name}" in CacheRegistry.names()


def test_log_accounting_runs_latest_ingest_concurrently_with_counts(monkeypatch):
    from backend import config as svcconfig
    from backend.high_scale import postgres_control
    from backend.routers.admin import log_accounting as _la

    for c in (
        _la._FASTLY_COUNTS_CACHE,
        _la._CLICKHOUSE_COUNTS_CACHE,
        _la._LOCAL_COUNTS_CACHE,
        _la._LATEST_INGEST_CACHE,
    ):
        c.clear()

    barrier = threading.Barrier(4, timeout=2.0)
    now_hr = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    lbl = (now_hr - timedelta(hours=1)).strftime("%Y-%m-%dT%H")

    class _FakePG:
        def request_flush_accounting(self, sid, start, end, width):
            barrier.wait()
            return {lbl: (100, 2)}

        def latest_request_ingest(self, sid):
            barrier.wait()
            return datetime.now(UTC) - timedelta(seconds=30)

    def _fake_fastly(*_a, **_kw):
        barrier.wait()
        return {lbl: 100}

    def _fake_ch(*_a, **_kw):
        barrier.wait()
        return {lbl: 100}

    class _FakeReg:
        def resolve(self, _sid):
            return object()

    monkeypatch.setattr(_la, "get_high_scale_service_registry", lambda: _FakeReg())
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(_la, "_fetch_fastly_request_counts", _fake_fastly)
    monkeypatch.setattr(_la, "_clickhouse_row_counts_per_bucket", _fake_ch)
    monkeypatch.setattr(svcconfig, "get_fastly_api_key", lambda _sid: "tok")
    monkeypatch.setattr(svcconfig, "load_config", lambda _sid: {})

    src = {"name": "svc-la-par", "service_id": "svc-la-par", "logging_service_id": "log-la-par"}
    result = _la.compute_log_accounting(src, hours=24, by="hour")
    assert result["totals"].fastly_requests == 100
    assert result["catchup"]["status"] == "caught_up"


def test_log_accounting_coalesces_cold_misses_and_serves_stale_while_revalidating(monkeypatch):
    from backend.routers.admin import log_accounting as _la
    from backend.utils.bounded_cache import SWRTTLCache

    short_cache = SWRTTLCache(maxsize=16, ttl_seconds=0.05, stale_ttl_seconds=60.0)
    calls = 0
    started = threading.Event()
    release = threading.Event()
    refresh_done = threading.Event()

    def _fetch():
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            release.wait(timeout=2.0)
            return {"2026-11-25T00": 100}
        time.sleep(0.05)
        refresh_done.set()
        return {"2026-11-25T00": 250}

    results: list[dict[str, int]] = []

    def _worker():
        results.append(_la._swr_fetch(short_cache, ("k",), _fetch, label="test"))

    t1 = threading.Thread(target=_worker)
    t2 = threading.Thread(target=_worker)
    t1.start()
    assert started.wait(timeout=2.0)
    t2.start()
    time.sleep(0.03)
    release.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert calls == 1
    assert results == [{"2026-11-25T00": 100}, {"2026-11-25T00": 100}]

    time.sleep(0.08)
    t0 = time.monotonic()
    stale = _la._swr_fetch(short_cache, ("k",), _fetch, label="test")
    assert stale == {"2026-11-25T00": 100}
    assert time.monotonic() - t0 < 0.04
    assert refresh_done.wait(timeout=2.0)

    deadline = time.monotonic() + 1.0
    while short_cache.get(("k",)) != {"2026-11-25T00": 250} and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _la._swr_fetch(short_cache, ("k",), _fetch, label="test") == {"2026-11-25T00": 250}


def test_prewarm_log_accounting_warms_and_swallows_errors():
    from backend.routers.admin import log_accounting as _la

    with (
        patch("backend.core.fos_client.get_source_for_service", return_value={"name": "svc-1", "service_id": "svc-1"}),
        patch.object(_la, "compute_log_accounting", side_effect=RuntimeError("fastly down")) as mock_compute,
    ):
        _la.prewarm_log_accounting("svc-1")
    mock_compute.assert_called_once_with({"name": "svc-1", "service_id": "svc-1"}, hours=24, by="hour")


# ── 2. Postgres storage_billing_summary split branch & covering indexes ──────


def test_storage_billing_summary_splits_updated_branch_to_bound_to_start_utc():
    from backend.high_scale.postgres_control.control_plane import (
        _CONCURRENT_BILLING_INDEXES,
        ARCHIVE_MANIFESTS_EXPIRED_BILLING_INDEX,
        SOURCE_OBJECTS_UPDATED_BILLING_INDEX,
        PostgresControlPlane,
    )

    idx_map = dict(_CONCURRENT_BILLING_INDEXES)
    assert SOURCE_OBJECTS_UPDATED_BILLING_INDEX in idx_map
    assert "INCLUDE" in idx_map[SOURCE_OBJECTS_UPDATED_BILLING_INDEX]
    assert "WHERE status IN ('source_deleted', 'source_missing')" in idx_map[SOURCE_OBJECTS_UPDATED_BILLING_INDEX]
    assert ARCHIVE_MANIFESTS_EXPIRED_BILLING_INDEX in idx_map
    assert "INCLUDE" in idx_map[ARCHIVE_MANIFESTS_EXPIRED_BILLING_INDEX]
    assert "WHERE state <> 'artifact_uploading'" in idx_map[ARCHIVE_MANIFESTS_EXPIRED_BILLING_INDEX]

    executed: list[tuple[str, tuple]] = []

    class FakeConn:
        @contextmanager
        def transaction(self):
            yield

        def execute(self, sql, params=None):
            executed.append((sql, tuple(params or ())))
            res = MagicMock()
            res.fetchone.return_value = (1.5, 0.5, 0.25, 0)
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    now = datetime(2026, 11, 25, 8, 0, 0, tzinfo=UTC)
    start = now - timedelta(hours=24)
    min_cutoff = start - timedelta(days=30)

    live, deleted, minimum, complete, sources = cp.storage_billing_summary("svc-1", start, now)
    assert (live, deleted, minimum, complete) == (3.0, 1.0, 0.5, True)
    assert sources == ["high-scale:archive_manifests", "high-scale:source_objects"]

    src_sql, src_params = next((s, p) for s, p in executed if "FROM high_scale_source_objects" in s)
    arc_sql, arc_params = next((s, p) for s, p in executed if "FROM high_scale_archive_manifests" in s)

    assert src_sql.count("UNION ALL") == 3
    assert "AND updated_at > %s AND discovered_at <= %s" in src_sql
    assert "AND discovered_at IS NULL AND updated_at > %s" in src_sql
    assert arc_sql.count("UNION ALL") == 3
    assert "AND archive_expired_at > %s AND created_at <= %s" in arc_sql
    assert "AND created_at IS NULL AND archive_expired_at > %s" in arc_sql

    # Verify Branch 3a bounds updated_at / archive_expired_at to start_utc (not min_cutoff)
    assert start in src_params
    assert min_cutoff in src_params
    assert start in arc_params
    assert min_cutoff in arc_params


# ── 3. Usage current-storage rolling SWR cache + prewarm ─────────────────────


def test_usage_current_storage_rolling_swr_cache_and_invalidation(monkeypatch):
    from backend import config as svcconfig
    from backend.core.metadata import fos_usage, usage_log
    from backend.high_scale import postgres_control
    from backend.repositories.utils.response_cache import invalidate_service_cache
    from backend.routers import usage
    from backend.utils.bounded_cache import CacheRegistry, SWRTTLCache

    assert isinstance(usage._USAGE_ROLLING_STORAGE_CACHE, SWRTTLCache)
    assert "routers.usage._USAGE_ROLLING_STORAGE_CACHE" in CacheRegistry.names()

    short_swr = SWRTTLCache(maxsize=16, ttl_seconds=0.05, stale_ttl_seconds=60.0)
    monkeypatch.setattr(usage, "_USAGE_ROLLING_STORAGE_CACHE", short_swr)
    usage._USAGE_RESPONSE_MEMO.cache.clear()
    usage._STORAGE_INSPECTION_CACHE.clear()

    billing_calls = 0
    refresh_done = threading.Event()

    class _FakePG:
        def source_storage_window(self, sid, start, end):
            return {"files": 5, "bytes": 2000, "rum_bytes": 100}

        def archive_storage_summary(self, sid):
            return {"live_bytes": 500, "live_objects": 3, "expired_30d_bytes": 0, "live_rum_bytes": 0}

        def storage_billing_summary(self, sid, start, end, *, minimum_days=30):
            nonlocal billing_calls
            billing_calls += 1
            if billing_calls == 2:
                time.sleep(0.05)
                refresh_done.set()
            return (float(billing_calls), 0.0, 0.0, True, ["high-scale:archive_manifests", "high-scale:source_objects"])

    class _FakeReg:
        def resolve(self, _sid):
            return SimpleNamespace(service_id=_sid)

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _FakeReg())
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {"storage_bytes": 100, "storage_files": 1})
    monkeypatch.setattr("backend.core.metadata.ingest_log.get_quarantine_storage_total", lambda _n: 0)
    monkeypatch.setattr(usage_log, "cloud_copy_deleted_bytes_in_billing_window", lambda _n: 0)
    monkeypatch.setattr(fos_usage, "cloud_copy_deletion_records_in_window", lambda *_a, **_kw: [])

    src = {"name": "svc-roll", "service_id": "svc-roll", "bucket": "b", "prefix": ""}
    now = datetime.now(UTC)
    s1 = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:00Z")
    e1 = now.strftime("%Y-%m-%dT%H:%M:00Z")

    r1 = usage.usage_current_storage(start=s1, end=e1, source=src)
    assert r1.live_billed_gb_hours == 1.0
    assert billing_calls == 1

    # Rolling window shifted by 1 minute (different minute bucket in ResponseMemo) still hits rolling SWR cache
    s2 = (now - timedelta(hours=24) + timedelta(seconds=65)).strftime("%Y-%m-%dT%H:%M:00Z")
    e2 = (now + timedelta(seconds=65)).strftime("%Y-%m-%dT%H:%M:00Z")
    r2 = usage.usage_current_storage(start=s2, end=e2, source=src)
    assert r2.live_billed_gb_hours == 1.0
    assert r2.is_cached is True
    assert billing_calls == 1

    # After fresh TTL expires, serve stale immediately and refresh in background
    time.sleep(0.08)
    t0 = time.monotonic()
    r3 = usage.usage_current_storage(start=s2, end=e2, source=src)
    assert r3.live_billed_gb_hours == 1.0
    assert r3.is_cached is True
    assert time.monotonic() - t0 < 0.04
    assert refresh_done.wait(timeout=2.0)

    deadline = time.monotonic() + 1.0
    while True:
        usage._USAGE_RESPONSE_MEMO.cache.clear()
        r4 = usage.usage_current_storage(start=s2, end=e2, source=src)
        if r4.live_billed_gb_hours == 2.0 or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert r4.live_billed_gb_hours == 2.0

    # invalidate_service_cache clears rolling cache for svc-roll
    invalidate_service_cache("svc-roll")
    assert len(short_swr) == 0


def test_prewarm_usage_storage_warms_and_swallows_errors():
    from backend.routers import usage

    with (
        patch(
            "backend.core.fos_client.get_source_for_service",
            return_value={"name": "svc-u1", "service_id": "svc-u1", "bucket": "b"},
        ),
        patch.object(usage, "usage_current_storage", side_effect=RuntimeError("pg down")) as mock_storage,
    ):
        usage.prewarm_usage_storage("svc-u1")
    mock_storage.assert_called_once_with(
        start="",
        end="",
        source={"name": "svc-u1", "service_id": "svc-u1", "bucket": "b"},
    )


# ── 4. Lifespan startup prewarm ──────────────────────────────────────────────


def test_initialize_service_runs_all_four_prewarms_concurrently():
    from backend.core import lifespan

    barrier = threading.Barrier(4, timeout=2.0)
    passed: list[str] = []

    def fake_sync(_sid: str):
        barrier.wait()
        passed.append("sync")

    def fake_scoring(_sid: str):
        barrier.wait()
        passed.append("scoring")

    def fake_accounting(_sid: str):
        barrier.wait()
        passed.append("log_accounting")

    def fake_usage(_sid: str):
        barrier.wait()
        passed.append("usage_storage")

    modules = {
        "backend.routers.admin": MagicMock(compute_sync_status_cached=fake_sync),
        "backend.routers.session_scoring.scoring_vcl": MagicMock(prewarm_scoring_status=fake_scoring),
        "backend.routers.admin.log_accounting": MagicMock(prewarm_log_accounting=fake_accounting),
        "backend.routers.usage": MagicMock(prewarm_usage_storage=fake_usage),
    }

    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=0),
        patch.object(lifespan._fos, "get_source_for_service", return_value={"name": "svc-par"}),
        patch.object(lifespan.importlib, "import_module", side_effect=lambda name: modules[name]),
    ):
        lifespan._initialize_service({"service_id": "svc-par"})

    assert sorted(passed) == ["log_accounting", "scoring", "sync", "usage_storage"]
