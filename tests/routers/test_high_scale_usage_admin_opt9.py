"""Focused unit tests for Round 9 High-Scale scoring/config, usage/current-storage, and admin/celery/status optimizations."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

# ── 1. Scoring config ────────────────────────────────────────────────────────


def test_scoring_pragma_missing_cache_has_300s_ttl():
    from backend.routers.session_scoring import scoring_vcl

    assert scoring_vcl._scoring_pragma_missing_cache.ttl_seconds == 300.0


def test_cached_fastly_probe_serves_stale_while_revalidating_and_invalidate_clears_stale():
    from backend.routers.session_scoring import scoring_vcl
    from backend.utils.bounded_cache import SWRTTLCache

    cache = SWRTTLCache(maxsize=16, ttl_seconds=0.05, stale_ttl_seconds=60.0)
    calls = 0
    refresh_done = threading.Event()

    def probe():
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"number": 1}
        time.sleep(0.05)
        refresh_done.set()
        return {"number": 2}

    first = scoring_vcl._cached_fastly_probe(cache, "svc-swr", probe, "test")
    assert first == {"number": 1}
    assert calls == 1

    time.sleep(0.08)
    assert cache.get("svc-swr") is None
    assert cache.get_stale("svc-swr") == {"number": 1}

    t0 = time.monotonic()
    second = scoring_vcl._cached_fastly_probe(cache, "svc-swr", probe, "test")
    elapsed = time.monotonic() - t0
    assert second == {"number": 1}
    assert elapsed < 0.04
    assert refresh_done.wait(timeout=2.0)

    # Allow background writer to store refreshed value
    deadline = time.monotonic() + 1.0
    while cache.get("svc-swr") != {"number": 2} and time.monotonic() < deadline:
        time.sleep(0.01)
    assert cache.get("svc-swr") == {"number": 2}

    cache.clear()
    assert cache.get("svc-swr") is None
    assert cache.get_stale("svc-swr") is None


def test_scoring_config_composite_loads_service_config_once():
    from backend.routers.session_scoring import scoring_admin, scoring_vcl

    scoring_vcl.invalidate_scoring_status_cache("svc-once")
    cfg = {
        "service_id": "svc-once",
        "scoring": {
            "enabled": False,
            "operator_threshold": 65,
            "exclude_url_regex": "",
            "enforce_status_code": 429,
        },
    }
    load_calls = 0

    def fake_load_config(sid: str):
        nonlocal load_calls
        load_calls += 1
        return cfg

    with patch("backend.config.load_config", side_effect=fake_load_config):
        result = scoring_admin.scoring_config_composite("svc-once")

    assert load_calls == 1
    assert result["status"]["enabled"] is False
    assert result["threshold"]["threshold"] == 65
    assert result["enforce_status_code"]["effective"] == 429


def test_initialize_service_runs_sync_status_and_scoring_prewarm_concurrently():
    from backend.core import lifespan

    barrier = threading.Barrier(2, timeout=2.0)
    passed: list[str] = []

    def fake_sync(_sid: str):
        barrier.wait()
        passed.append("sync")

    def fake_prewarm(_sid: str):
        barrier.wait()
        passed.append("scoring")

    fake_admin = MagicMock(compute_sync_status_cached=fake_sync)
    fake_scoring = MagicMock(prewarm_scoring_status=fake_prewarm)

    def fake_import(name: str):
        if name == "backend.routers.admin":
            return fake_admin
        if name == "backend.routers.session_scoring.scoring_vcl":
            return fake_scoring
        raise AssertionError(f"unexpected import: {name}")

    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=0),
        patch.object(lifespan._fos, "get_source_for_service", return_value={"name": "svc-par"}),
        patch.object(lifespan.importlib, "import_module", side_effect=fake_import),
    ):
        lifespan._initialize_service({"service_id": "svc-par"})

    assert sorted(passed) == ["scoring", "sync"]


# ── 2. Usage current-storage ─────────────────────────────────────────────────


def test_storage_billing_summary_uses_bounded_union_all_branches():
    from backend.high_scale.postgres_control.control_plane import PostgresControlPlane

    executed_sql: list[str] = []

    class FakeConn:
        @contextmanager
        def transaction(self):
            yield

        def execute(self, sql, params=None):
            executed_sql.append(sql)
            res = MagicMock()
            res.fetchone.return_value = (1.5, 0.5, 0.25, 0)
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    now = datetime.now(UTC)
    live, deleted, minimum, complete, sources = cp.storage_billing_summary("svc-1", now - timedelta(hours=6), now)

    assert (live, deleted, minimum, complete) == (3.0, 1.0, 0.5, True)
    assert sources == ["high-scale:archive_manifests", "high-scale:source_objects"]

    src_sql = [s for s in executed_sql if "FROM high_scale_source_objects" in s][0]
    arc_sql = [s for s in executed_sql if "FROM high_scale_archive_manifests" in s][0]

    assert src_sql.count("UNION ALL") == 3
    assert "discovered_at > %s AND discovered_at < %s" in src_sql
    assert "updated_at > %s" in src_sql

    assert arc_sql.count("UNION ALL") == 3
    assert "created_at > %s AND created_at < %s" in arc_sql
    assert "archive_expired_at > %s" in arc_sql


def test_usage_current_storage_groups_metadata_reads_on_one_thread_and_releases_connection(monkeypatch):
    from backend import config as svcconfig
    from backend.core.metadata import fos_usage, pg_pool, usage_log
    from backend.high_scale import postgres_control
    from backend.routers import usage

    usage._USAGE_RESPONSE_MEMO.cache.clear()
    usage._USAGE_ROLLING_STORAGE_CACHE.clear()
    usage._STORAGE_INSPECTION_CACHE.clear()

    meta_threads: list[int] = []
    released_threads: list[int] = []

    class _FakePG:
        def source_storage_window(self, sid, start, end):
            return {"files": 2, "bytes": 1000, "rum_bytes": 100}

        def archive_storage_summary(self, sid):
            return {"live_bytes": 500, "live_objects": 3, "expired_30d_bytes": 0, "live_rum_bytes": 0}

        def storage_billing_summary(self, sid, start, end, *, minimum_days=30):
            return (1.0, 0.0, 0.0, True, ["high-scale:archive_manifests", "high-scale:source_objects"])

    class _FakeRegistry:
        def resolve(self, _sid):
            return object()

    def _quarantine(_n):
        meta_threads.append(threading.get_ident())
        return 10

    def _cc_deleted(_n):
        meta_threads.append(threading.get_ident())
        return 20

    def _cc_intervals(*_a, **_kw):
        meta_threads.append(threading.get_ident())
        return []

    def _release():
        released_threads.append(threading.get_ident())

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {"storage_bytes": 100, "storage_files": 1})
    monkeypatch.setattr("backend.core.metadata.ingest_log.get_quarantine_storage_total", _quarantine)
    monkeypatch.setattr(usage_log, "cloud_copy_deleted_bytes_in_billing_window", _cc_deleted)
    monkeypatch.setattr(fos_usage, "cloud_copy_deletion_records_in_window", _cc_intervals)
    monkeypatch.setattr(pg_pool, "release_thread_connection", _release)

    src = {"name": "svc-meta-thr", "service_id": "svc-meta-thr", "bucket": "b", "prefix": ""}
    resp = usage.usage_current_storage(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", source=src)

    assert resp.quarantine_bytes == 10
    assert resp.cloud_copy_deleted_bytes == 20
    assert len(meta_threads) == 3
    assert len(set(meta_threads)) == 1
    assert meta_threads[0] in released_threads


# ── 3. Admin celery status ───────────────────────────────────────────────────


def test_read_ingest_ledger_summary_batches_postgres_services_in_one_query():
    from backend import celery_app

    cur = MagicMock()
    cur.fetchall.return_value = [("committed", 200), ("quarantined", 2)]
    con = MagicMock()
    con.cursor.return_value = cur

    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}]),
        patch("backend.core.metadata.pg_pool.is_postgres", return_value=True),
        patch("backend.core.metadata.base.get_con", return_value=con) as mock_get_con,
    ):
        result = celery_app._read_ingest_ledger_summary()

    assert result == {"committed": 200, "quarantined": 2}
    assert mock_get_con.call_count == 1
    sql, params = cur.execute.call_args[0]
    assert "WHERE service_id IN (" in sql
    assert list(params) == ["svc-a", "svc-b"]


def test_ingest_ledger_summary_coalesces_concurrent_misses_and_serves_stale(monkeypatch):
    from backend import celery_app
    from backend.utils.bounded_cache import SWRTTLCache

    short_cache = SWRTTLCache(maxsize=1, ttl_seconds=0.05, stale_ttl_seconds=60.0)
    monkeypatch.setattr(celery_app, "_LEDGER_SUMMARY_CACHE", short_cache)

    calls = 0
    started = threading.Event()
    release = threading.Event()
    refresh_done = threading.Event()

    def fake_read():
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            release.wait(timeout=2.0)
            return {"committed": 10}
        time.sleep(0.05)
        refresh_done.set()
        return {"committed": 25}

    monkeypatch.setattr(celery_app, "_read_ingest_ledger_summary", fake_read)

    results: list[dict[str, int]] = []

    def worker():
        results.append(celery_app.ingest_ledger_summary())

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert started.wait(timeout=2.0)
    t2.start()
    time.sleep(0.03)
    release.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert calls == 1
    assert results == [{"committed": 10}, {"committed": 10}]

    # After fresh TTL expires, serve stale immediately and refresh in background
    time.sleep(0.08)
    t0 = time.monotonic()
    stale = celery_app.ingest_ledger_summary()
    assert stale == {"committed": 10}
    assert time.monotonic() - t0 < 0.04
    assert refresh_done.wait(timeout=2.0)

    deadline = time.monotonic() + 1.0
    while short_cache.get("ledger") != {"committed": 25} and time.monotonic() < deadline:
        time.sleep(0.01)
    assert celery_app.ingest_ledger_summary() == {"committed": 25}


def test_api_celery_status_cache_ttl_is_10s_and_coalesces_concurrent_misses(monkeypatch):
    from backend.routers.admin import health

    assert health._CELERY_STATUS_CACHE.ttl_seconds == 10.0

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    health._CELERY_STATUS_CACHE.clear()

    calls = 0
    started = threading.Event()
    release = threading.Event()

    def slow_status():
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=2.0)
        return {"ledger": {"committed": 5}, "inprocess_schedules": [], "inprocess_active_runs": []}

    monkeypatch.setattr(health, "get_celery_status", slow_status)

    results: list[dict] = []

    def worker():
        results.append(health.api_celery_status())

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert started.wait(timeout=2.0)
    t2.start()
    time.sleep(0.03)
    release.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert calls == 1
    assert len(results) == 2
    assert results[0]["ledger"] == {"committed": 5}
    assert results[1]["ledger"] == {"committed": 5}


def test_prewarm_celery_status_warms_and_swallows_errors():
    from backend import celery_app

    with patch.object(celery_app, "ingest_ledger_summary", side_effect=RuntimeError("db down")) as mock_summary:
        celery_app.prewarm_celery_status()
    mock_summary.assert_called_once_with()
