"""Focused unit tests for Round 11 High-Scale /api/usage/current-storage & log-activity optimizations."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from backend import config as svcconfig
from backend.core.metadata import fos_usage, usage_log
from backend.high_scale import postgres_control
from backend.repositories.utils.response_cache import ResponseMemo, invalidate_service_cache
from backend.routers import usage
from backend.utils.bounded_cache import CacheRegistry, SWRTTLCache


def _hs_reg(sids: set[str]):
    class _Reg:
        def resolve(self, sid: str):
            return SimpleNamespace(service_id=sid) if sid in sids else None

    return _Reg()


def test_ingest_scoped_invalidation_preserves_usage_memo_and_rolling_swr_caches():
    analytics_memo = ResponseMemo(ttl=60.0, maxsize=16)
    analytics_memo.put("svc-opt11:analytics", {"ok": True})
    usage._USAGE_RESPONSE_MEMO.put("svc-opt11:usage", {"ok": True})
    usage._USAGE_ROLLING_STORAGE_CACHE[("svc-opt11", "svc-opt11", "b", "", 6, True, 30)] = {"ok": True}
    usage._SOURCE_STORAGE_WINDOW_CACHE[("svc-opt11", "svc-opt11", 30)] = {"total_files": 1, "total_bytes": 10}
    usage._ARCHIVE_STORAGE_SUMMARY_CACHE["svc-opt11"] = (10, 3, 0, 0, True)

    try:
        # Routine periodic ingest invalidation clears analytics memo, NOT usage/rolling storage caches.
        invalidate_service_cache("svc-opt11", scope="ingest")
        assert analytics_memo.get("svc-opt11:analytics") is None
        assert usage._USAGE_RESPONSE_MEMO.get("svc-opt11:usage") is not None
        assert usage._USAGE_ROLLING_STORAGE_CACHE.get(("svc-opt11", "svc-opt11", "b", "", 6, True, 30)) is not None
        assert usage._SOURCE_STORAGE_WINDOW_CACHE.get(("svc-opt11", "svc-opt11", 30)) is not None
        assert usage._ARCHIVE_STORAGE_SUMMARY_CACHE.get("svc-opt11") is not None

        # Explicit full invalidation (default scope="all") clears everything for svc-opt11.
        invalidate_service_cache("svc-opt11")
        assert usage._USAGE_RESPONSE_MEMO.get("svc-opt11:usage") is None
        assert usage._USAGE_ROLLING_STORAGE_CACHE.get(("svc-opt11", "svc-opt11", "b", "", 6, True, 30)) is None
        assert usage._SOURCE_STORAGE_WINDOW_CACHE.get(("svc-opt11", "svc-opt11", 30)) is None
        assert usage._ARCHIVE_STORAGE_SUMMARY_CACHE.get("svc-opt11") is None
    finally:
        invalidate_service_cache("svc-opt11")


def test_log_discovery_cron_invalidation_scope_depends_on_force_and_discovered(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda _sid: {
            "service_id": "svc-disc",
            "log_period": 10,
            "provisioning": {"cron_sync": {"enabled": True, "polling_mode": "fixed"}},
        },
    )
    monkeypatch.setattr(
        sync_mod,
        "get_source_for_service",
        lambda _sid: {"name": "svc-disc", "service_id": "svc-disc", "bucket": "b"},
    )
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *_a, **_kw: True)
    monkeypatch.setattr("backend.cron.jobs._common.publish_sync_status_safe", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())

    @contextmanager
    def _fake_cron_run(*_a, **_kw):
        yield SimpleNamespace(ok=True, run_id=None, include_log_output=False, success=MagicMock(), fail=MagicMock())

    monkeypatch.setattr(sync_mod, "cron_run", _fake_cron_run)

    inv_mock = MagicMock()
    monkeypatch.setattr(sync_mod, "invalidate_service_cache", inv_mock)

    # 1. Periodic tick with 0 discovered files -> no cache invalidation at all.
    monkeypatch.setattr(sync_mod, "discover_prefix", lambda *_a, **_kw: 0)
    sync_mod._run_log_discovery_cron.__wrapped__("svc-disc", force=False)
    inv_mock.assert_not_called()

    # 2. Periodic tick with newly discovered files -> ingest-scoped invalidation only.
    monkeypatch.setattr(sync_mod, "discover_prefix", lambda *_a, **_kw: 2)
    sync_mod._run_log_discovery_cron.__wrapped__("svc-disc", force=False)
    inv_mock.assert_called_once_with("svc-disc", scope="ingest")

    # 3. Forced manual sync -> full invalidation.
    inv_mock.reset_mock()
    monkeypatch.setattr(sync_mod, "_claim_heavy_refresh", lambda _sid: False)
    sync_mod._run_log_discovery_cron.__wrapped__("svc-disc", force=True)
    inv_mock.assert_called_once_with("svc-disc")


def test_prewarm_usage_storage_warms_24h_6h_and_1h_sharing_30d_source_and_archive_caches(monkeypatch):
    assert isinstance(usage._SOURCE_STORAGE_WINDOW_CACHE, SWRTTLCache)
    assert isinstance(usage._ARCHIVE_STORAGE_SUMMARY_CACHE, SWRTTLCache)
    assert "routers.usage._SOURCE_STORAGE_WINDOW_CACHE" in CacheRegistry.names()
    assert "routers.usage._ARCHIVE_STORAGE_SUMMARY_CACHE" in CacheRegistry.names()

    invalidate_service_cache("svc-pw11")
    usage._STORAGE_INSPECTION_CACHE.clear()

    calls = {"source": 0, "archive": 0, "billing": 0}

    class _FakePG:
        def source_storage_window(self, sid, start, end):
            calls["source"] += 1
            return {"files": 10, "bytes": 5000, "rum_bytes": 500}

        def archive_storage_summary(self, sid):
            calls["archive"] += 1
            return {"live_bytes": 2000, "live_objects": 6, "expired_30d_bytes": 100, "live_rum_bytes": 200}

        def storage_billing_summary(self, sid, start, end, *, minimum_days=30):
            calls["billing"] += 1
            hours = round((end - start).total_seconds() / 3600.0)
            return (float(hours), 0.0, 0.0, True, ["high-scale:archive_manifests", "high-scale:source_objects"])

    src = {"name": "svc-pw11", "service_id": "svc-pw11", "bucket": "b", "prefix": ""}
    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_reg({"svc-pw11"}))
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {"storage_bytes": 100, "storage_files": 1})
    monkeypatch.setattr("backend.core.metadata.ingest_log.get_quarantine_storage_total", lambda _n: 0)
    monkeypatch.setattr(usage_log, "cloud_copy_deleted_bytes_in_billing_window", lambda _n: 0)
    monkeypatch.setattr(fos_usage, "cloud_copy_deletion_records_in_window", lambda *_a, **_kw: [])
    monkeypatch.setattr("backend.core.fos_client.get_source_for_service", lambda _sid: src)

    usage.prewarm_usage_storage("svc-pw11")

    # 30-day rolling source + archive queries ran ONCE across all three windows (24h, 6h, 1h);
    # billing ran once per window (3 total).
    assert calls == {"source": 1, "archive": 1, "billing": 3}

    # Even after a routine ingest tick, cold 24h, 6h, and 1h requests all hit the rolling SWR cache!
    invalidate_service_cache("svc-pw11", scope="ingest")
    now = datetime.now(UTC)
    for hours in (24, 6, 1):
        s = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:00Z")
        e = now.strftime("%Y-%m-%dT%H:%M:00Z")
        resp = usage.usage_current_storage(start=s, end=e, source=src)
        assert resp.is_cached is True
        assert resp.live_billed_gb_hours == float(hours)
    assert calls == {"source": 1, "archive": 1, "billing": 3}

    invalidate_service_cache("svc-pw11")


def test_swr_storage_fetch_serves_stale_immediately_and_refreshes_in_background():
    short_cache = SWRTTLCache(maxsize=8, ttl_seconds=0.05, stale_ttl_seconds=60.0)
    calls = 0
    refreshed = threading.Event()

    def _fetch():
        nonlocal calls
        calls += 1
        if calls == 2:
            time.sleep(0.05)
            refreshed.set()
        return {"v": calls}

    assert usage._swr_storage_fetch(short_cache, ("k",), _fetch, label="test-opt11") == {"v": 1}
    time.sleep(0.08)

    t0 = time.monotonic()
    stale = usage._swr_storage_fetch(short_cache, ("k",), _fetch, label="test-opt11")
    assert stale == {"v": 1}
    assert time.monotonic() - t0 < 0.04
    assert refreshed.wait(timeout=2.0)

    deadline = time.monotonic() + 1.0
    while short_cache.get(("k",)) != {"v": 2} and time.monotonic() < deadline:
        time.sleep(0.01)
    assert short_cache.get(("k",)) == {"v": 2}


def test_source_objects_activity_covering_index_registered_and_built():
    from backend.high_scale.postgres_control.control_plane import (
        _CONCURRENT_BILLING_INDEXES,
        SOURCE_OBJECTS_ACTIVITY_INDEX,
        PostgresControlPlane,
    )

    idx_map = dict(_CONCURRENT_BILLING_INDEXES)
    assert SOURCE_OBJECTS_ACTIVITY_INDEX in idx_map
    ddl = idx_map[SOURCE_OBJECTS_ACTIVITY_INDEX]
    assert "INCLUDE (accepted_rows, size_bytes)" in ddl
    assert "WHERE status NOT IN ('discovered', 'claimed')" in ddl

    executed: list[str] = []

    class FakeConn:
        autocommit = False

        def execute(self, sql, params=None):
            executed.append(sql)
            res = MagicMock()
            if "pg_try_advisory_lock" in sql:
                res.fetchone.return_value = (True,)
            else:
                res.fetchone.return_value = None
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    assert cp.ensure_committed_manifest_index() == "built"
    assert any(SOURCE_OBJECTS_ACTIVITY_INDEX in s and s.startswith("CREATE INDEX CONCURRENTLY") for s in executed)
