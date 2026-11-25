"""Focused unit tests for High-Scale Round 8 scoring/config, current-storage, and ClickHouse schema startup fixes."""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest


def test_cached_fastly_probe_coalesces_concurrent_misses():
    from backend.routers.session_scoring import scoring_vcl
    from backend.utils.bounded_cache import BoundedTTLCache

    cache = BoundedTTLCache(maxsize=16, ttl_seconds=60.0)
    calls = 0
    started = threading.Event()
    release = threading.Event()

    def slow_probe():
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=2.0)
        return {"number": 7, "updated_at": "2026-11-25T00:00:00Z"}

    results: list[dict | None] = []

    def worker():
        results.append(scoring_vcl._cached_fastly_probe(cache, "svc-coal", slow_probe, "test"))

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert started.wait(timeout=2.0)
    t2.start()
    time.sleep(0.05)
    release.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert calls == 1
    assert len(results) == 2
    assert results[0] == results[1] == {"number": 7, "updated_at": "2026-11-25T00:00:00Z"}


def test_scoring_status_caches_have_300s_ttl_and_invalidate_clears_them():
    from backend.routers.session_scoring import scoring_vcl

    assert scoring_vcl._scoring_pragma_cache.ttl_seconds == 300.0
    assert scoring_vcl._scoring_svc_version_cache.ttl_seconds == 300.0

    scoring_vcl._scoring_pragma_cache["svc-inv"] = True
    scoring_vcl._scoring_pragma_missing_cache["svc-inv"] = False
    scoring_vcl._scoring_svc_version_cache["scoring-inv"] = {"number": 1}

    scoring_vcl.invalidate_scoring_status_cache("svc-inv", "scoring-inv")

    assert scoring_vcl._scoring_pragma_cache.get("svc-inv") is None
    assert scoring_vcl._scoring_pragma_missing_cache.get("svc-inv") is None
    assert scoring_vcl._scoring_svc_version_cache.get("scoring-inv") is None


def test_get_generated_vcl_and_active_version_pass_bounded_timeouts():
    from backend.core.fastly import client

    captured: list[tuple[str, dict]] = []

    def fake_fastly(method, path, **kwargs):
        captured.append((path, kwargs))
        if path.endswith("/generated_vcl"):
            return {"content": "sub vcl_recv {}"}
        return {"number": 3, "active": True}

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        assert client.get_active_version("svc-1", "tok") == 3
        assert client.get_generated_vcl("svc-1", 3, "tok") == "sub vcl_recv {}"

    assert captured[0][1]["timeout"] == 8
    assert captured[0][1]["max_retries"] == 1
    assert captured[1][1]["timeout"] == 8
    assert captured[1][1]["max_retries"] == 0


def test_package_sha_caches_by_file_stat_and_invalidates_on_change(tmp_path: Path):
    from backend.provision.session_scoring import wasm_deploy

    wasm_deploy._PACKAGE_SHA_CACHE.clear()
    pkg = tmp_path / "scorer.tar.gz"
    pkg.write_bytes(b"first-payload")

    read_calls = 0
    orig_read_bytes = Path.read_bytes

    def counting_read_bytes(self: Path) -> bytes:
        nonlocal read_calls
        if self == pkg:
            read_calls += 1
        return orig_read_bytes(self)

    with patch.object(Path, "read_bytes", counting_read_bytes):
        sha1 = wasm_deploy._package_sha(pkg)
        sha2 = wasm_deploy._package_sha(pkg)
        assert sha1 == sha2
        assert read_calls == 1

        pkg.write_bytes(b"second-longer-payload")
        sha3 = wasm_deploy._package_sha(pkg)
        assert sha3 != sha1
        assert read_calls == 2


def test_read_l2_enforce_state_fetches_three_keys_in_parallel():
    from backend.routers.session_scoring import scoring_admin

    scoring_admin._l2_enforce_cache.clear()
    barrier = threading.Barrier(3, timeout=2.0)

    def parallel_get(_store_id: str, key: str, _token: str) -> str:
        barrier.wait()
        if key == scoring_admin._SCORING_ENABLED_AT_KEY:
            return "1700000000"
        return ""

    with patch.object(scoring_admin, "_l2_get_item", side_effect=parallel_get):
        state = scoring_admin._read_l2_enforce_state("svc-par", "store-1", "tok")

    assert state["enabled"] is False
    assert state["scoring_enabled_at"] == 1700000000


def test_prewarm_scoring_status_warms_and_swallows_errors():
    from backend.routers.session_scoring import scoring_vcl

    with patch.object(scoring_vcl, "scoring_status", side_effect=RuntimeError("boom")) as mock_status:
        scoring_vcl.prewarm_scoring_status("svc-prewarm")
    mock_status.assert_called_once_with("svc-prewarm")


def test_archive_storage_summary_filters_to_live_and_30d_expired():
    from backend.high_scale.postgres_control.control_plane import PostgresControlPlane

    executed_sql: list[str] = []

    class FakeConn:
        @contextmanager
        def transaction(self):
            yield

        def execute(self, sql, params=None):
            executed_sql.append(sql)
            res = MagicMock()
            res.fetchone.return_value = (100, 2, 50, 25)
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    summary = cp.archive_storage_summary("svc-1")

    assert summary == {"live_bytes": 100, "live_objects": 6, "expired_30d_bytes": 50, "live_rum_bytes": 25}
    select_sql = [s for s in executed_sql if "FROM high_scale_archive_manifests" in s][0]
    assert "UNION ALL" in select_sql
    assert "archive_expired_at IS NULL" in select_sql
    assert "archive_expired_at > statement_timestamp() - interval '30 days'" in select_sql
    assert " OR " not in select_sql


def test_storage_billing_summary_uses_any_status_predicate():
    from datetime import UTC, datetime, timedelta

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
    where_clause = src_sql.split("WHERE ", 1)[1]
    assert "status = ANY(ARRAY[" in where_clause
    assert "status NOT IN" not in where_clause


def test_ensure_committed_manifest_index_also_builds_covering_billing_indexes():
    from backend.high_scale.postgres_control.control_plane import (
        ARCHIVE_MANIFESTS_BILLING_INDEX,
        COMMITTED_MANIFEST_INDEX,
        SOURCE_OBJECTS_BILLING_INDEX,
        PostgresControlPlane,
    )

    executed: list[tuple[str, tuple | None]] = []

    class FakeConn:
        autocommit = False

        def execute(self, sql, params=None):
            executed.append((sql, params))
            res = MagicMock()
            if "pg_try_advisory_lock" in sql:
                res.fetchone.return_value = (True,)
            elif "SELECT indisvalid" in sql:
                res.fetchone.return_value = None
            else:
                res.fetchone.return_value = None
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    assert cp.ensure_committed_manifest_index() == "built"

    ddl_statements = [sql for sql, _ in executed if sql.startswith("CREATE INDEX CONCURRENTLY")]
    assert any(COMMITTED_MANIFEST_INDEX in s for s in ddl_statements)
    assert any(SOURCE_OBJECTS_BILLING_INDEX in s and "INCLUDE" in s for s in ddl_statements)
    assert any(ARCHIVE_MANIFESTS_BILLING_INDEX in s and "INCLUDE" in s for s in ddl_statements)


def test_ensure_committed_manifest_index_tolerates_billing_index_error():
    from backend.high_scale.postgres_control.control_plane import (
        SOURCE_OBJECTS_BILLING_INDEX,
        PostgresControlPlane,
    )

    class FakeConn:
        autocommit = False

        def execute(self, sql, params=None):
            if SOURCE_OBJECTS_BILLING_INDEX in sql and sql.startswith("CREATE INDEX"):
                raise psycopg.OperationalError("lock timeout")
            res = MagicMock()
            res.fetchone.return_value = (True,) if "pg_try_advisory_lock" in sql else None
            return res

    class FakePool:
        @contextmanager
        def connection(self, timeout=None):
            yield FakeConn()

    cp = PostgresControlPlane(pool=FakePool())
    assert cp.ensure_committed_manifest_index() == "built"


def test_install_clickhouse_schema_accepts_explicit_client_and_applies_sample_rate():
    from backend.high_scale.archive.clickhouse_batch import install_clickhouse_schema

    executed: list[str] = []

    class FakeCH:
        def execute(self, stmt: str) -> None:
            executed.append(stmt)

    install_clickhouse_schema(FakeCH())
    assert any(
        "rum_vitals_facts" in s and "ADD COLUMN IF NOT EXISTS sample_rate Float64 DEFAULT 1" in s for s in executed
    )
    assert any(
        "rum_error_facts" in s and "ADD COLUMN IF NOT EXISTS sample_rate Float64 DEFAULT 1" in s for s in executed
    )


@pytest.mark.parametrize("schema_raises", [False, True])
def test_lifespan_installs_clickhouse_schema_before_serving(schema_raises: bool):
    from backend import main
    from backend.core import clickhouse as ch

    events: list[str] = []
    fake_ch = MagicMock()

    def fake_install(client=None):
        events.append("install_schema")
        assert client is fake_ch
        if schema_raises:
            raise RuntimeError("readonly user")

    with (
        patch("backend.core.lifespan._enforce_data_dir_mounted"),
        patch("backend.core.lifespan._enforce_proxy_headers_configured"),
        patch("backend.core.metadata.base.ensure_pg_schema"),
        patch("backend.core.lifespan.register_high_scale_services_from_environment", return_value=("svc-1",)),
        patch("backend.core.lifespan.console_access.provisioning_client"),
        patch("backend.core.lifespan.console_access.provision_configured_services", return_value=()),
        patch("backend.high_scale.archive.clickhouse_batch.install_clickhouse_schema", side_effect=fake_install),
        patch("backend.utils.telemetry.start_proxy_server"),
        patch("backend.utils.tunnel.get_tunnel_manager"),
        patch("backend.core.lifespan._bounded_scheduler_shutdown"),
        patch.object(ch, "get_clickhouse_client", return_value=fake_ch),
        patch.object(ch, "close_clickhouse_client"),
    ):

        async def _run():
            async with main.lifespan(main.app):
                events.append("serving")

        asyncio.run(_run())

    assert events == ["install_schema", "serving"]
