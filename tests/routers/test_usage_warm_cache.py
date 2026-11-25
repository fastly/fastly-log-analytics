"""Warm-cache, bucketing, service-isolation, and invalidation tests for /api/usage."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.core.fastly import client as fastly_client
from backend.core.metadata import fos_usage, usage_log
from backend.high_scale import postgres_control
from backend.high_scale import rum as hs_rum
from backend.repositories.utils.response_cache import invalidate_service_cache
from backend.routers import usage

pytestmark = pytest.mark.no_postgres


def _hs_registry(service_ids: set[str], client: Any = None) -> Any:
    class _Reg:
        def resolve(self, sid: str) -> Any:
            if sid in service_ids:
                return SimpleNamespace(service_id=sid, client=client)
            return None

    return _Reg()


def test_fastly_api_warm_hit_across_seconds_in_same_minute_and_misses_on_window_change(monkeypatch):
    calls: list[dict[str, Any]] = []

    def fake_metrics(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"data": [{"start_time": kwargs["from_epoch"], "requests": 100}]}

    monkeypatch.setattr(fastly_client, "fastly_platform_metrics", fake_metrics)

    # 10:00:05 .. 11:00:05 and 10:00:45 .. 11:00:45 fall in the same minute bucket
    base_from = 1_760_000_040  # divisible by 60
    base_to = base_from + 3600
    r1 = usage._fastly_api("svc-a", "hour", base_from + 5, base_to + 5, ("requests",), "tok-a")
    r2 = usage._fastly_api("svc-a", "hour", base_from + 45, base_to + 45, ("requests",), "tok-a")
    assert r1 == r2
    assert len(calls) == 1

    # Different minute window must miss
    usage._fastly_api("svc-a", "hour", base_from + 65, base_to + 65, ("requests",), "tok-a")
    assert len(calls) == 2

    # Different entity_id must miss
    usage._fastly_api("svc-b", "hour", base_from + 5, base_to + 5, ("requests",), "tok-a")
    assert len(calls) == 3

    # Different token must miss
    usage._fastly_api("svc-a", "hour", base_from + 5, base_to + 5, ("requests",), "tok-b")
    assert len(calls) == 4


def test_prefill_caches_endpoint_chain_stats_and_edge_ratio_while_reflecting_local_config(monkeypatch):
    source_a = svcconfig.config_to_source({"service_id": "svc-a", "cdn_service_id": "cdn-a"})
    source_b = svcconfig.config_to_source({"service_id": "svc-b", "cdn_service_id": "cdn-b"})

    counts = {"version": 0, "endpoint": 0, "condition": 0, "stats": 0, "ch": 0}
    snapshot_interval = {"value": 5}

    def fake_active_version(svc_id: str, api_key: str) -> int:
        counts["version"] += 1
        return 7

    def fake_log_endpoint(svc_id: str, ver: int, name: str, api_key: str) -> dict[str, Any]:
        counts["endpoint"] += 1
        return {"period": 60, "response_condition": "Log Sampling"}

    def fake_find_condition(cond_name: str, svc_id: str, ver: int, api_key: str) -> dict[str, Any]:
        counts["condition"] += 1
        return {"statement": "randombool(50, 100) && req.restarts == 0"}

    def fake_metrics(**kwargs: Any) -> dict[str, Any]:
        counts["stats"] += 1
        return {"data": [{"start_time": 1_767_225_600, "requests": 1000, "edge_requests": 800}]}

    class _FakeCH:
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            counts["ch"] += 1
            return [{"edge_ratio": 82.5}]

    class _FakePG:
        def request_files_per_flush(self, sid: str) -> float:
            return 4.0

    fake_ch = _FakeCH()
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "tok-secret")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda k: k)
    monkeypatch.setattr(fastly_client, "get_active_version", fake_active_version)
    monkeypatch.setattr(fastly_client, "get_log_endpoint", fake_log_endpoint)
    monkeypatch.setattr(fastly_client, "find_condition", fake_find_condition)
    monkeypatch.setattr(fastly_client, "fastly_platform_metrics", fake_metrics)
    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_registry({"svc-a", "svc-b"}, fake_ch))
    monkeypatch.setattr(usage, "visible_publication_fence_sql", lambda *a, **kw: "1=1")
    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda _name: {"provisioning": {"cron_sync": {"snapshot_interval_minutes": snapshot_interval["value"]}}},
    )
    monkeypatch.setattr(svcconfig, "get_status", lambda _name: {})
    monkeypatch.setattr(svcconfig, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(hs_rum, "rum_beacons_by_date", lambda *a, **kw: {"2026-11-20": 100})

    first = asyncio.run(usage.prefill(source_a))
    assert first.log_period_seconds == 60
    assert first.sample_rate == 50
    assert first.edge_only is True
    assert first.edge_ratio == 82.5
    assert first.requests_per_day == 1000
    assert first.snapshot_interval_minutes == 5
    assert counts == {"version": 1, "endpoint": 1, "condition": 1, "stats": 1, "ch": 1}

    # Warm reload with a local config change must reuse external/CH caches AND reflect local config immediately
    snapshot_interval["value"] = 60
    second = asyncio.run(usage.prefill(source_a))
    assert second.log_period_seconds == 60
    assert second.edge_ratio == 82.5
    assert second.requests_per_day == 1000
    assert second.snapshot_interval_minutes == 60
    assert counts == {"version": 1, "endpoint": 1, "condition": 1, "stats": 1, "ch": 1}

    # Different service must not share svc-a's cached endpoint chain, stats, or edge ratio
    asyncio.run(usage.prefill(source_b))
    assert counts == {"version": 2, "endpoint": 2, "condition": 2, "stats": 2, "ch": 2}


def test_usage_current_storage_warm_hit_service_isolation_time_range_and_invalidation(monkeypatch):
    src_a = {"name": "svc-a", "service_id": "svc-a", "bucket": "b", "prefix": ""}
    src_b = {"name": "svc-b", "service_id": "svc-b", "bucket": "b", "prefix": ""}

    pg_calls = {"storage": 0, "archive": 0, "billing": 0}
    billing_complete = {"value": True}

    class _FakePG:
        def source_storage_window(self, sid: str, start: Any, end: Any) -> dict[str, int]:
            pg_calls["storage"] += 1
            return {"files": 10, "bytes": 5000, "rum_bytes": 500}

        def archive_storage_summary(self, sid: str) -> dict[str, int]:
            pg_calls["archive"] += 1
            return {"live_bytes": 2000, "live_objects": 4, "expired_30d_bytes": 100, "live_rum_bytes": 200}

        def storage_billing_summary(self, sid: str, start: Any, end: Any, *, minimum_days: int = 30) -> Any:
            pg_calls["billing"] += 1
            return (1.5, 0.5, 0.25, billing_complete["value"], ["high-scale:postgres"])

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_registry({"svc-a", "svc-b"}))
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakePG)
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {"storage_bytes": 1000, "storage_files": 2})
    monkeypatch.setattr(usage_log, "cloud_copy_deleted_bytes_in_billing_window", lambda _n: 0)
    monkeypatch.setattr(fos_usage, "cloud_copy_deletion_records_in_window", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        "backend.core.metadata.ingest_log.get_quarantine_storage_total",
        lambda _n: 0,
    )

    r1 = usage.usage_current_storage(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", source=src_a)
    assert pg_calls == {"storage": 1, "archive": 1, "billing": 1}
    assert r1.archive_bytes == 2000

    # Warm reload 25s later in same minute must hit cache
    r2 = usage.usage_current_storage(start="2026-11-24T10:00:30Z", end="2026-11-24T11:00:30Z", source=src_a)
    assert pg_calls == {"storage": 1, "archive": 1, "billing": 1}
    assert r2.archive_bytes == 2000
    assert r2.is_cached is True

    # Different time window must miss
    usage.usage_current_storage(start="2026-11-24T09:00:05Z", end="2026-11-24T11:00:05Z", source=src_a)
    assert pg_calls == {"storage": 2, "archive": 2, "billing": 2}

    # Different service must miss
    usage.usage_current_storage(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", source=src_b)
    assert pg_calls == {"storage": 3, "archive": 3, "billing": 3}

    # Cache invalidation for svc-a must force recomputation for svc-a only
    invalidate_service_cache("svc-a")
    usage.usage_current_storage(start="2026-11-24T10:00:30Z", end="2026-11-24T11:00:30Z", source=src_a)
    assert pg_calls == {"storage": 4, "archive": 4, "billing": 4}
    # svc-b stays cached
    usage.usage_current_storage(start="2026-11-24T10:00:30Z", end="2026-11-24T11:00:30Z", source=src_b)
    assert pg_calls == {"storage": 4, "archive": 4, "billing": 4}

    # Incomplete billing result must NOT be cached
    invalidate_service_cache("svc-a")
    billing_complete["value"] = False
    usage.usage_current_storage(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", source=src_a)
    usage.usage_current_storage(start="2026-11-24T10:00:30Z", end="2026-11-24T11:00:30Z", source=src_a)
    assert pg_calls == {"storage": 6, "archive": 6, "billing": 6}


def test_usage_operations_and_log_activity_warm_cache_and_invalidation(monkeypatch):
    src_a = {
        "name": "svc-a",
        "service_id": "svc-a",
        "logging_service_id": "log-a",
        "cdn_service_id": "cdn-a",
    }
    ops_calls = 0

    def fake_ops(sid: str, start_str: str, end_str: str, gran: str) -> dict[str, Any]:
        nonlocal ops_calls
        ops_calls += 1
        return {
            "data": [{"date": "2026-11-24T10:00", "class_a": 10, "class_b": 20}],
            "total_class_a": 10,
            "total_class_b": 20,
        }

    monkeypatch.setattr(fos_usage, "get_usage_operation_buckets", fake_ops)

    o1 = usage.usage_operations(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="hour", source=src_a)
    o2 = usage.usage_operations(start="2026-11-24T10:00:40Z", end="2026-11-24T11:00:40Z", by="hour", source=src_a)
    assert ops_calls == 1
    assert o1.total_class_a == o2.total_class_a == 10
    assert o2.is_cached is True

    # Different granularity misses
    usage.usage_operations(start="2026-11-24T10:00:40Z", end="2026-11-24T11:00:40Z", by="day", source=src_a)
    assert ops_calls == 2

    invalidate_service_cache("svc-a")
    usage.usage_operations(start="2026-11-24T10:00:40Z", end="2026-11-24T11:00:40Z", by="hour", source=src_a)
    assert ops_calls == 3

    # log-activity
    activity_calls = {"pg": 0, "fastly": 0}

    class _FakeActivityPG:
        def source_activity(self, sid: str, start: Any, end: Any, by: str) -> list[tuple[str, int, int]]:
            activity_calls["pg"] += 1
            return [("2026-11-24T10:00", 50, 1024)]

    def fake_metrics(**kwargs: Any) -> dict[str, Any]:
        activity_calls["fastly"] += 1
        return {"data": [{"start_time": 1_795_514_400, "requests": 50, "log": 50}]}

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_registry({"svc-a"}))
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _FakeActivityPG)
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "tok-a")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda k: k)
    monkeypatch.setattr(fastly_client, "fastly_platform_metrics", fake_metrics)

    a1 = usage.usage_log_activity(source=src_a, start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="hour")
    a2 = usage.usage_log_activity(source=src_a, start="2026-11-24T10:00:40Z", end="2026-11-24T11:00:40Z", by="hour")
    assert activity_calls == {"pg": 1, "fastly": 1}
    assert a1.total_rows == a2.total_rows == 50
    assert a2.is_cached is True

    invalidate_service_cache("svc-a")
    usage.usage_log_activity(source=src_a, start="2026-11-24T10:00:40Z", end="2026-11-24T11:00:40Z", by="hour")
    assert activity_calls["pg"] == 2

    # Empty log-activity windows must not be cached so newly ingested rows appear immediately
    empty_calls = 0

    def fake_empty_then_seeded(src: dict[str, Any], start_str: str, end_str: str, by: str) -> dict[str, Any]:
        nonlocal empty_calls
        empty_calls += 1
        if empty_calls == 1:
            return {"granularity": by, "data": [], "total_rows": 0, "total_bytes": 0}
        return {
            "granularity": by,
            "data": [{"time": "2026-01-01T10:00", "row_count": 300, "bytes": 13000}],
            "total_rows": 300,
            "total_bytes": 13000,
        }

    src_empty = {"name": "svc-empty", "service_id": "svc-empty", "logging_service_id": ""}
    monkeypatch.setattr(usage, "_get_log_activity", fake_empty_then_seeded)
    e1 = usage.usage_log_activity(source=src_empty, start="2026-01-01", end="2026-01-02", by="hour")
    e2 = usage.usage_log_activity(source=src_empty, start="2026-01-01", end="2026-01-02", by="hour")
    assert (e1.total_rows, e2.total_rows, e2.total_bytes, empty_calls) == (0, 300, 13000, 2)


def test_usage_rum_breakdown_high_scale_warm_hit_across_seconds_in_same_minute(monkeypatch):
    src_a = {"name": "svc-a", "service_id": "svc-a"}
    src_b = {"name": "svc-b", "service_id": "svc-b"}
    ch_calls = 0

    def fake_beacons(hs_service: Any, start_str: str, end_str: str) -> dict[str, int]:
        nonlocal ch_calls
        ch_calls += 1
        return {"2026-11-24": 420}

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_registry({"svc-a", "svc-b"}))
    monkeypatch.setattr(hs_rum, "rum_beacons_by_date", fake_beacons)
    monkeypatch.setattr(svcconfig, "load_usage_logging_config", lambda: {"class_a_rate_per_1k": 0.0025})

    r1 = usage.usage_rum_breakdown(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="day", source=src_a)
    r2 = usage.usage_rum_breakdown(start="2026-11-24T10:00:45Z", end="2026-11-24T11:00:45Z", by="day", source=src_a)
    assert ch_calls == 1
    assert r1.total_beacons == r2.total_beacons == 420

    # Different minute window misses
    usage.usage_rum_breakdown(start="2026-11-24T10:01:05Z", end="2026-11-24T11:01:05Z", by="day", source=src_a)
    assert ch_calls == 2

    # Different service misses
    usage.usage_rum_breakdown(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="day", source=src_b)
    assert ch_calls == 3

    # Suppress unused import warning check
    assert metadata_db is not None


def test_usage_bandwidth_warm_cache_and_invalidation(monkeypatch):
    src_a = {"name": "svc-a", "service_id": "svc-a", "logging_service_id": "log-a", "cdn_service_id": "cdn-a"}
    src_b = {"name": "svc-b", "service_id": "svc-b", "logging_service_id": "log-b", "cdn_service_id": "cdn-b"}
    bw_calls = 0

    def fake_fastly_api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal bw_calls
        bw_calls += 1
        return {"data": [{"start_time": 1_795_514_400, "bandwidth": 1000, "requests": 50}]}

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "tok-a")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda k: k)
    monkeypatch.setattr(usage, "_fastly_api", fake_fastly_api)

    b1 = usage.usage_bandwidth(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="hour", source=src_a)
    b2 = usage.usage_bandwidth(start="2026-11-24T10:00:45Z", end="2026-11-24T11:00:45Z", by="hour", source=src_a)
    assert bw_calls == 1
    assert b1.total_bytes == b2.total_bytes == 1000
    assert b2.is_cached is True

    # Different granularity misses
    usage.usage_bandwidth(start="2026-11-24T10:00:45Z", end="2026-11-24T11:00:45Z", by="day", source=src_a)
    assert bw_calls == 2

    # Different service misses
    usage.usage_bandwidth(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", by="hour", source=src_b)
    assert bw_calls == 3

    invalidate_service_cache("svc-a")
    usage.usage_bandwidth(start="2026-11-24T10:00:45Z", end="2026-11-24T11:00:45Z", by="hour", source=src_a)
    assert bw_calls == 4


def test_usage_get_endpoints_emit_cache_control(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    src = {"name": "svc-a", "service_id": "svc-a", "logging_service_id": "log-a", "cdn_service_id": "cdn-a"}
    app = FastAPI()
    app.include_router(usage.router)
    app.dependency_overrides[usage.get_source] = lambda: src

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "tok-a")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda k: k)
    monkeypatch.setattr(usage, "_fastly_api", lambda *_a, **_kw: {"data": []})
    monkeypatch.setattr(
        fos_usage,
        "get_usage_operation_buckets",
        lambda *_a, **_kw: {"data": [], "total_class_a": 0, "total_class_b": 0},
    )
    monkeypatch.setattr(svcconfig, "load_usage_logging_config", lambda: {"class_a_rate_per_1k": 0.0025})

    client = TestClient(app)
    for path in (
        "/api/usage/prefill/rates",
        "/api/usage/operations?start=2026-11-24T10:00:00Z&end=2026-11-24T11:00:00Z&by=hour",
        "/api/usage/bandwidth?start=2026-11-24T10:00:00Z&end=2026-11-24T11:00:00Z&by=hour",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, (path, resp.text)
        assert resp.headers.get("cache-control") == usage._USAGE_CACHE_CONTROL


def test_usage_current_storage_runs_independent_postgres_queries_concurrently(monkeypatch):
    import threading

    src = {"name": "svc-par", "service_id": "svc-par", "bucket": "b", "prefix": ""}
    # Barrier of 4: source_storage_window, archive_storage_summary, storage_billing_summary,
    # and cloud_copy_deleted_bytes_in_billing_window must all enter concurrently.
    barrier = threading.Barrier(4, timeout=2.0)

    class _ConcurrentPG:
        def source_storage_window(self, sid: str, start: Any, end: Any) -> dict[str, int]:
            barrier.wait()
            return {"files": 10, "bytes": 5000, "rum_bytes": 500}

        def archive_storage_summary(self, sid: str) -> dict[str, int]:
            barrier.wait()
            return {"live_bytes": 2000, "live_objects": 4, "expired_30d_bytes": 100, "live_rum_bytes": 200}

        def storage_billing_summary(self, sid: str, start: Any, end: Any, *, minimum_days: int = 30) -> Any:
            barrier.wait()
            return (1.5, 0.5, 0.25, True, ["high-scale:postgres"])

    def _cloud_copy_deleted(_name: str) -> int:
        barrier.wait()
        return 123

    monkeypatch.setattr(usage, "get_high_scale_service_registry", lambda: _hs_registry({"svc-par"}))
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _ConcurrentPG)
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {"storage_bytes": 1000, "storage_files": 2})
    monkeypatch.setattr(usage_log, "cloud_copy_deleted_bytes_in_billing_window", _cloud_copy_deleted)
    monkeypatch.setattr(fos_usage, "cloud_copy_deletion_records_in_window", lambda *_a, **_kw: [])
    monkeypatch.setattr("backend.core.metadata.ingest_log.get_quarantine_storage_total", lambda _n: 0)

    resp = usage.usage_current_storage(start="2026-11-24T10:00:05Z", end="2026-11-24T11:00:05Z", source=src)
    assert resp.archive_bytes == 2000
    assert resp.cloud_copy_deleted_bytes == 123
    assert resp.storage_billing_complete is True


def test_prefill_runs_metadata_fastly_and_edge_ratio_concurrently(monkeypatch):
    import threading

    source = svcconfig.config_to_source({"service_id": "svc-par-prefill", "cdn_service_id": "cdn-par"})
    # Barrier of 4: request_files_per_flush, rum_beacons_by_date, fastly_platform_metrics, edge_ratio CH query.
    barrier = threading.Barrier(4, timeout=2.0)

    class _ConcurrentPG:
        def request_files_per_flush(self, sid: str) -> float:
            barrier.wait()
            return 6.0

    def _concurrent_rum(*_a: Any, **_kw: Any) -> dict[str, int]:
        barrier.wait()
        return {"2026-11-24": 250}

    def _concurrent_metrics(**_kw: Any) -> dict[str, Any]:
        barrier.wait()
        return {"data": [{"start_time": 1_767_225_600, "requests": 1000, "edge_requests": 800}]}

    class _ConcurrentCH:
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            barrier.wait()
            return [{"edge_ratio": 77.5}]

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "tok-secret")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda k: k)
    monkeypatch.setattr(fastly_client, "get_active_version", lambda *_a: None)
    monkeypatch.setattr(fastly_client, "fastly_platform_metrics", _concurrent_metrics)
    monkeypatch.setattr(
        usage,
        "get_high_scale_service_registry",
        lambda: _hs_registry({"svc-par-prefill"}, _ConcurrentCH()),
    )
    monkeypatch.setattr(usage, "visible_publication_fence_sql", lambda *a, **kw: "1=1")
    monkeypatch.setattr(svcconfig, "load_config", lambda _n: {})
    monkeypatch.setattr(svcconfig, "get_status", lambda _n: {})
    monkeypatch.setattr(svcconfig, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", _ConcurrentPG)
    monkeypatch.setattr(hs_rum, "rum_beacons_by_date", _concurrent_rum)

    resp = asyncio.run(usage.prefill(source))
    assert resp.avg_nodes_per_flush == 6
    assert resp.rum_beacons_per_day == 250
    assert resp.requests_per_day == 1000
    assert resp.edge_ratio == 77.5
