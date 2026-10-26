"""Consolidated tests for test_e2e_pipeline."""

from __future__ import annotations

import glob
import gzip
import io
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from functools import partial
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pytest

from backend import config as svcconfig
from backend.core import iceberg as ice
from backend.core import ingest as ing
from backend.core import metadata as metadata_db
from backend.core.duckdb import _cache_dir
from backend.core.iceberg import _ducklake as _dl
from backend.core.iceberg import buffer as buffer_mod
from backend.core.metadata.pg_connection import is_postgres
from backend.repositories._base import _safe_table
from backend.repositories.dashboard import _dashboard_cache, get_aggregates
from backend.repositories.insights import _insights_cache, get_insights
from scripts.check_environment_health import (
    ENVIRONMENTS,
    check_backend_health,
    check_rbac,
    check_tenancy,
)
from tests._helpers.sources import make_source
from tests.conftest import MOCK_SERVICE_ID


@pytest.fixture
def pipeline_env(monkeypatch):
    """Stand up a full pipeline environment with a local-FS DuckLake warehouse."""
    tmpdir = tempfile.mkdtemp(prefix="e2e_pipeline_")
    warehouse_path = os.path.join(tmpdir, "warehouse")
    cache_path = os.path.join(tmpdir, "cache")
    for p in (warehouse_path, cache_path, os.path.join(tmpdir, "ducklake_data")):
        os.makedirs(p, exist_ok=True)

    sfx = uuid.uuid4().hex[:6]
    source = {
        "name": f"e2e_svc_{sfx}",
        "service_id": f"e2e-svc-{sfx}",
        "service_name": f"E2E Test {sfx}",
        "bucket": "e2e-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "duckdb_path": os.path.join(tmpdir, f"e2e_{sfx}.duckdb"),
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr("backend.config.DUCKLAKE_CATALOG", os.path.join(tmpdir, "ducklake"))
    monkeypatch.setattr("backend.config.DUCKLAKE_DATA_PATH", os.path.join(tmpdir, "ducklake_data/"))

    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()
    yield {"src": source, "tmpdir": tmpdir, "warehouse": warehouse_path, "cache": cache_path}
    shutil.rmtree(tmpdir, ignore_errors=True)
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()


def _simulate_sync(src, warehouse_path, cache_path):
    """Copy parquet files from the warehouse into the local cache/data/ dir."""
    data_dir = os.path.join(cache_path, "data")
    os.makedirs(data_dir, exist_ok=True)
    for src_path in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(src_path))
        if not os.path.exists(dst):
            shutil.copy2(src_path, dst)


def _make_log_batch(n: int = 20, *, ip_octet: int = 0, path_prefix: str | None = None) -> pa.Table:
    """Build a PyArrow table shaped like a buffer batch from real ingest."""
    base = datetime.now(UTC) - timedelta(hours=1)
    urls = (
        [f"/{path_prefix}/{i}" for i in range(n)] if path_prefix is not None else [f"/path/{i % 10}" for i in range(n)]
    )
    countries = ["US"] * n if path_prefix is not None else ["US" if i % 3 == 0 else "GB" for i in range(n)]
    pops = ["LAX"] * n if path_prefix is not None else [["LAX", "JFK", "LHR"][i % 3] for i in range(n)]
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i * 2) for i in range(n)], type=pa.timestamp("us", tz="UTC")
            ),
            "ip": pa.array([f"10.{ip_octet}.0.{i}" for i in range(n)]),
            "status": pa.array([200 if i % 5 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array(urls),
            "country": pa.array(countries),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array(pops),
        }
    )


def _seed_and_commit(src: dict, n: int, fname: str) -> dict:
    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=n), fname)
    return ice.commit_buffer(src)


def test_e2e_buffer_to_commit_produces_queryable_snapshot(pipeline_env):
    """Full lifecycle: write buffer parquet -> commit -> update DuckDB view -> query."""
    src = pipeline_env["src"]
    assert ice.init_iceberg_table(src) is not None
    ice.write_to_buffer(src, _make_log_batch(n=20), "batch_test_0.parquet")
    assert len(ice.buffer_files(src)) == 1

    result = ice.commit_buffer(src)
    assert result["files_committed"] >= 1
    assert result["rows_committed"] == 20
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(src) == []

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)
    row = con.execute(f"SELECT COUNT(*) FROM {_safe_table(src['name'])}").fetchone()
    assert row[0] == 20


def test_e2e_aggregates_via_full_pipeline(pipeline_env):
    """Pipeline -> get_aggregates returns the ingested row count."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 15, "batch_agg_0.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)

    _dashboard_cache.clear()
    out = get_aggregates(
        con=con, src=src, start_time=None, end_time=None, filters={}, chart_interval="1 minute", chart_metric="requests"
    )
    assert out["total_rows"] == 15
    assert out["total_rows_total"] >= 15


def test_e2e_insights_via_full_pipeline(pipeline_env):
    """Pipeline -> get_insights runs all registered insights without error."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 30, "batch_insights_0.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(con, src, window_hours=1, baseline_hours=1)

    errors = [i for i in result["insights"] if i.get("severity") == "error"]
    assert not errors, "Insights errored:\n" + "\n".join(f"  {i['id']}: {i.get('summary', '')[:120]}" for i in errors)


def test_commit_buffer_does_no_redundant_catalog_work_per_call(pipeline_env, monkeypatch):
    """Single-chunk commit attaches DuckLake RW once and does not re-enter catalog init."""
    src = pipeline_env["src"]
    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_load_pin_0.parquet")

    counts = {"public": 0, "locked": 0, "write_attach": 0}
    real_public, real_locked, real_write_conn = (
        ice.init_iceberg_table,
        ice._init_iceberg_table_locked,
        buffer_mod._ducklake_write_connection,
    )

    def counting_public(source, create=True, table_name="logs"):
        counts["public"] += 1
        return real_public(source, create=create, table_name=table_name)

    def counting_locked(source, create=True, table_name="logs"):
        counts["locked"] += 1
        return real_locked(source, create=create, table_name=table_name)

    def counting_write_conn(source):
        counts["write_attach"] += 1
        return real_write_conn(source)

    monkeypatch.setattr(ice, "init_iceberg_table", counting_public)
    monkeypatch.setattr(ice, "_init_iceberg_table_locked", counting_locked)
    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", counting_write_conn)

    result = ice.commit_buffer(src)
    assert result["rows_committed"] == 5
    assert counts == {"public": 0, "locked": 0, "write_attach": 1}


def test_e2e_empty_buffer_commit_is_noop(pipeline_env):
    """commit_buffer with no buffer files is a clean no-op."""
    src = pipeline_env["src"]
    ice.init_iceberg_table(src)
    result = ice.commit_buffer(src)
    assert (result["files_committed"], result["rows_committed"], result["snapshot_id"]) == (0, 0, None)


def test_e2e_two_commits_append_rather_than_overwrite(pipeline_env):
    """Two separate commit cycles append rather than overwrite."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 10, "batch_a.parquet")
    ice.write_to_buffer(src, _make_log_batch(n=12), "batch_b.parquet")
    ice.commit_buffer(src)

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)
    row = con.execute(f"SELECT COUNT(*) FROM {_safe_table(src['name'])}").fetchone()
    assert row[0] == 22


def test_full_pipeline_including_raw_gzip_ingest(s3_mock, fos_source, monkeypatch, tmp_path, pipeline_env):
    """End-to-end through the RAW .gz ingest path: upload to moto S3 -> ingest -> commit -> query."""
    cache_path, warehouse_path = str(tmp_path / "cache"), str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    fos_source["duckdb_path"] = os.path.join(tmp_path, "e2e.duckdb")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _CallerHintShim(s3_mock))
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    unique_id = uuid.uuid4().hex[:8]
    unique_prefix = f"pipe_{unique_id}"
    fos_source = {
        **fos_source,
        "prefix": unique_prefix,
        "service_id": f"svc_{unique_id}",
        "name": f"tbl_{unique_id}",
    }

    base = datetime.now(UTC) - timedelta(hours=2)
    rows_per_file = 5
    files = [
        (f"{unique_prefix}/raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz", base),
        (
            f"{unique_prefix}/raw/request/year=2026/month=05/day=20/hour=10/minute=05/2026-05-20T10-05-00.svc.gz",
            base + timedelta(minutes=5),
        ),
    ]
    for key, ts in files:
        rows = [
            {
                "timestamp": (ts + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
                "ip": f"10.0.0.{i}",
                "status": 200 if i % 2 == 0 else 404,
                "url": f"/path/{i}",
                "method": "GET",
                "cache": "HIT",
                "resp_bytes": 1024 + i,
                "elapsed": 1500 + i * 10,
            }
            for i in range(rows_per_file)
        ]
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    ice.init_iceberg_table(fos_source)
    events = list(ing.ingest(source=fos_source))
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["new_files"] == len(files)
    assert done["rows_inserted"] == rows_per_file * len(files)
    assert len(ice.buffer_files(fos_source)) >= 1

    result = ice.commit_buffer(fos_source)
    assert result["rows_committed"] == rows_per_file * len(files)
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(fos_source) == []

    _simulate_sync(fos_source, warehouse_path, cache_path)
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, fos_source)

    view_name = _safe_table(fos_source["name"])
    (count,) = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
    assert count == rows_per_file * len(files)

    sample = con.execute(
        f"SELECT status, url, method, cache, resp_bytes FROM {view_name} ORDER BY url LIMIT 1"
    ).fetchone()
    assert sample is not None
    status, url, method, cache, resp_bytes = sample
    assert isinstance(status, int) and status in (200, 404)
    assert url.startswith("/path/") and method == "GET" and cache == "HIT"
    assert isinstance(resp_bytes, int) and resp_bytes >= 1024

    service_name = fos_source["name"]
    ingested_names = {row["file_name"] for row in metadata_db.list_ingested_files(service_name)}
    for key, _ in files:
        assert any(name.endswith(key) for name in ingested_names)
    assert metadata_db.list_in_flight(service_name) == []


def test_e2e_readonly_dashboard_query_path(pipeline_env):
    """Writer commits buffer -> builds persistent view; separate RO connection hits fast-path cache."""
    src = pipeline_env["src"]
    db_path = os.path.join(pipeline_env["tmpdir"], "e2e.duckdb")

    _seed_and_commit(src, 8, "batch_ro_e2e.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    rw = duckdb.connect(db_path)
    ice.update_iceberg_view(rw, src)
    view_name = _safe_table(src["name"])
    assert rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
    rw.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT NULL::INTEGER AS x WHERE false")
    rw.close()

    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_iceberg_view(ro, src)
        assert ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
        _dashboard_cache.clear()
        out = get_aggregates(
            con=ro,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
            chart_interval="1 hour",
            chart_metric="requests",
        )
        assert out["total_rows"] == 8
    finally:
        ro.close()


def test_e2e_readonly_dashboard_cold_cache_path(pipeline_env):
    """Fresh RO connection + evicted caches rebuilds TEMP view via slow path."""
    src = pipeline_env["src"]
    db_path = os.path.join(pipeline_env["tmpdir"], "e2e_cold.duckdb")

    _seed_and_commit(src, 8, "batch_ro_cold.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    rw = duckdb.connect(db_path)
    ice.update_iceberg_view(rw, src)
    view_name = _safe_table(src["name"])
    assert rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
    rw.close()

    ice._view_cache.pop(src["name"], None)

    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_iceberg_view(ro, src)
        assert ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
        _dashboard_cache.clear()
        out = get_aggregates(
            con=ro,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
            chart_interval="1 hour",
            chart_metric="requests",
        )
        assert out["total_rows"] == 8
    finally:
        ro.close()


# ── Merged from test_multi_service_e2e.py ──────────────────────────────────────────────

_make_source = partial(
    make_source,
    kind="cloud",
    prefix="logs",
    fos_native_endpoint="us-east-1.object.fastlystorage.app",
    access_key_id="test-key",
    secret_access_key="test-secret",
)


@pytest.fixture
def two_service_env(monkeypatch):
    """Two local warehouses, one per service."""
    tmpdir = tempfile.mkdtemp(prefix="multi_svc_e2e_")
    warehouses = {
        "svc_a": os.path.join(tmpdir, "warehouse_a"),
        "svc_b": os.path.join(tmpdir, "warehouse_b"),
    }
    for p in warehouses.values():
        os.makedirs(p, exist_ok=True)
    src_a = _make_source(
        name="svc_a",
        service_id="svc-a-id",
        bucket="svc-a-bucket",
        service_name="Multi-Svc svc_a",
        duckdb_path=svcconfig.duckdb_path("svc-a-id"),
    )
    src_b = _make_source(
        name="svc_b",
        service_id="svc-b-id",
        bucket="svc-b-bucket",
        service_name="Multi-Svc svc_b",
        duckdb_path=svcconfig.duckdb_path("svc-b-id"),
    )

    monkeypatch.setattr(_dl, "_default_data_path", lambda s: warehouses[s["name"]])
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    yield {"src_a": src_a, "src_b": src_b, "warehouses": warehouses}
    shutil.rmtree(tmpdir, ignore_errors=True)


def _simulate_sync_one(source: dict, warehouse_path: str) -> None:
    data_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)


def test_concurrent_iceberg_commits_two_services_no_cross_contamination(two_service_env):
    """Two services run buffer-write + commit concurrently without cross-tenant contamination."""
    src_a, src_b = two_service_env["src_a"], two_service_env["src_b"]
    ice.init_iceberg_table(src_a)
    ice.init_iceberg_table(src_b)
    ice.write_to_buffer(src_a, _make_log_batch(n=12, ip_octet=1, path_prefix="alpha"), "svc_a.parquet")
    ice.write_to_buffer(src_b, _make_log_batch(n=18, ip_octet=2, path_prefix="bravo"), "svc_b.parquet")

    barrier = threading.Barrier(2)
    results: dict[str, dict] = {}
    rlock = threading.Lock()

    def commit_worker(label: str, src: dict) -> None:
        outcome: dict = {"label": label}
        try:
            barrier.wait()
            res = ice.commit_buffer(src)
            outcome["rows_committed"] = res.get("rows_committed")
            outcome["snapshot_id"] = res.get("snapshot_id")
        except Exception as exc:
            outcome["error"] = repr(exc)
        with rlock:
            results[label] = outcome

    threads = [
        threading.Thread(target=commit_worker, args=("a", src_a)),
        threading.Thread(target=commit_worker, args=("b", src_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    for label, expected in (("a", 12), ("b", 18)):
        r = results[label]
        assert "error" not in r, f"svc_{label} commit raised: {r['error']}"
        assert r["rows_committed"] == expected
        assert r["snapshot_id"] is not None
    assert ice.buffer_files(src_a) == [] and ice.buffer_files(src_b) == []

    _simulate_sync_one(src_a, two_service_env["warehouses"]["svc_a"])
    _simulate_sync_one(src_b, two_service_env["warehouses"]["svc_b"])
    con_a, con_b = duckdb.connect(":memory:"), duckdb.connect(":memory:")
    ice.update_iceberg_view(con_a, src_a)
    ice.update_iceberg_view(con_b, src_b)

    view_a, view_b = _safe_table(src_a["name"]), _safe_table(src_b["name"])
    assert con_a.execute(f"SELECT COUNT(*) FROM {view_a}").fetchone()[0] == 12
    assert con_b.execute(f"SELECT COUNT(*) FROM {view_b}").fetchone()[0] == 18
    assert (
        con_b.execute(f"SELECT COUNT(*) FROM {view_b} WHERE ip LIKE '10.1.%' OR url LIKE '/alpha/%'").fetchone()[0] == 0
    )
    assert (
        con_a.execute(f"SELECT COUNT(*) FROM {view_a} WHERE ip LIKE '10.2.%' OR url LIKE '/bravo/%'").fetchone()[0] == 0
    )


def test_concurrent_metadata_db_writes_two_services_no_cross_contamination():
    """Two services run independent metadata_db mutations on separate threads without cross-contamination."""
    svc_a, svc_b = "svc-md-a", "svc-md-b"
    seed_key, new_key = "raw/2026-06-10/00/seed.gz", "raw/2026-06-10/01/svc_b_only.gz"

    metadata_db.insert_ingested_files(svc_a, [(seed_key, 100, 1024)])
    metadata_db.insert_ingested_files(svc_b, [(seed_key, 100, 1024)])
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)} == {seed_key}
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)} == {seed_key}

    barrier = threading.Barrier(2)
    errors: list[str] = []
    elock = threading.Lock()

    def deleter_svc_a() -> None:
        try:
            barrier.wait()
            con = metadata_db.get_con(svc_a)
            con.execute("DELETE FROM ingested_files WHERE source_name = ? AND file_name = ?", (svc_a, seed_key))
            con.commit()
        except Exception as exc:
            with elock:
                errors.append(f"svc_a deleter: {exc!r}")

    def inserter_svc_b() -> None:
        try:
            barrier.wait()
            metadata_db.insert_ingested_files(svc_b, [(new_key, 250, 2048)])
        except Exception as exc:
            with elock:
                errors.append(f"svc_b inserter: {exc!r}")

    threads = [threading.Thread(target=deleter_svc_a), threading.Thread(target=inserter_svc_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive()
    assert not errors

    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)} == set()
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)} == {seed_key, new_key}

    path_a, path_b = metadata_db.db_path(svc_a), metadata_db.db_path(svc_b)
    if not is_postgres():
        assert path_a != path_b
        assert os.path.exists(path_a) and os.path.exists(path_b)


# ── Merged from test_smoke_end_to_end.py ──────────────────────────────────────────────


@pytest.fixture
def seeded_service(in_memory_duckdb, test_service_source):
    """Seed the test DuckDB with 50 log rows spanning the last 2 hours."""
    table = _safe_table(test_service_source["name"])
    catalog_fields = [
        ("timestamp", "TIMESTAMPTZ"),
        ("dt", "VARCHAR"),
        ("timestamp_hour", "VARCHAR"),
        ("status", "INTEGER"),
        ("country", "VARCHAR"),
        ("url", "VARCHAR"),
        ("ip", "VARCHAR"),
        ("method", "VARCHAR"),
        ("ua", "VARCHAR"),
        ("pop", "VARCHAR"),
        ("asn", "INTEGER"),
        ("city", "VARCHAR"),
        ("region", "VARCHAR"),
        ("elapsed", "INTEGER"),
        ("cache", "VARCHAR"),
        ("ottfb", "DOUBLE"),
        ("ttfb", "DOUBLE"),
        ("waf_sig", "VARCHAR"),
        ("edge", "BOOLEAN"),
        ("lat", "DOUBLE"),
        ("lon", "DOUBLE"),
        ("resp_bytes", "INTEGER"),
        ("oip", "VARCHAR"),
        ("ost", "INTEGER"),
    ]
    cols_def = ", ".join(f'"{n}" {t}' for n, t in catalog_fields)
    in_memory_duckdb.execute(f"CREATE TABLE {table} ({cols_def})")

    base = datetime.now(UTC) - timedelta(hours=2)
    statuses = [200, 200, 200, 404, 500] * 10
    pops = ["LAX", "JFK", "LHR"] * 17
    countries = ["US", "US", "GB", "DE", "JP"] * 10
    rows = [
        (
            (ts := base + timedelta(minutes=i * 2)),
            ts.strftime("%Y-%m-%d"),
            ts.strftime("%Y-%m-%d-%H"),
            statuses[i],
            countries[i],
            f"/path-{i % 10}",
            f"10.0.{i // 10}.{i % 10}",
            "GET",
            "Mozilla/5.0",
            pops[i % 3],
            15169 + (i % 5),
            "San Francisco",
            "CA",
            50 + i * 5,
            "HIT" if i % 3 == 0 else "MISS",
            50000.0 + i * 100,
            0.05 + i * 0.001,
            "",
            True,
            37.7749,
            -122.4194,
            500 + i * 50,
            "origin-1.example.com",
            statuses[i],
        )
        for i in range(50)
    ]
    placeholders = ", ".join(["?"] * len(catalog_fields))
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} ({', '.join(f'''\"{n}\"''' for n, _ in catalog_fields)}) VALUES ({placeholders})",
        rows,
    )
    return test_service_source


def test_smoke_dashboard_aggregates_returns_real_counts(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_rows"] > 0
    assert body["total_rows_total"] >= body["total_rows"]
    for key in ("data", "time_series", "interval", "metric"):
        assert key in body
    assert any(entry.get("top") for entry in body["data"].values())


def test_smoke_dashboard_aggregates_with_filter_narrows_results(client, seeded_service):
    unfiltered = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    ).json()
    gb_only = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": ["GB"]}}},
    ).json()
    assert 0 < gb_only["total_rows"] < unfiltered["total_rows"]


def test_smoke_dashboard_aggregates_with_numeric_filter_on_string_column_does_not_crash(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": [0]}}},
    )
    assert resp.status_code == 200
    assert resp.json()["total_rows"] == 0


def test_smoke_insights_endpoint_returns_insight_cards_with_distinct_ids(client, seeded_service):
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 1, "baseline_hours": 1, "filters": {}},
        )
    assert resp.status_code == 200
    insights = resp.json()["insights"]
    assert len(insights) >= 15
    for card in insights:
        for key in ("id", "title", "severity", "summary", "items"):
            assert key in card
    ids = [c["id"] for c in insights]
    assert len(ids) == len(set(ids))


def test_smoke_origin_timeseries_with_sub_minute_bucket(client, seeded_service):
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {},
            "bucket_minutes": 1.0 / 60.0,
            "split_by_leg": False,
            "metric": "ttfb",
            "percentile": "p95",
        },
    )
    assert resp.status_code == 200
    assert "has_data" in resp.json()


def test_query_errors_decorator_returns_documented_envelope_for_malformed_sql(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"status": {"mode": "", "values": [200]}}},
    )
    if resp.status_code != 200:
        assert "detail" in resp.json()


def test_query_errors_envelope_for_filter_type_mismatch(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": [0]}}},
    )
    if resp.status_code != 200:
        assert "detail" in resp.json()


# ── Merged from test_performance_smoke.py ──────────────────────────────────────────────


@pytest.fixture
def one_million_row_service(in_memory_duckdb, test_service_source):
    """Seed 1,000,000 rows into DuckDB using native generation for speed."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"""
        CREATE TABLE {table} (
            timestamp TIMESTAMPTZ, status INTEGER, country VARCHAR, pop VARCHAR,
            url VARCHAR, method VARCHAR, ip VARCHAR, elapsed INTEGER,
            cache VARCHAR, ttfb DOUBLE, edge BOOLEAN, resp_bytes INTEGER, backend VARCHAR
        )
    """)
    in_memory_duckdb.execute(f"""
        INSERT INTO {table}
        SELECT
            now() - interval (random() * 24) hour as timestamp,
            (case when random() > 0.95 then 500 when random() > 0.90 then 404 else 200 end) as status,
            (case when random() > 0.5 then 'US' when random() > 0.3 then 'GB' else 'DE' end) as country,
            (case when random() > 0.7 then 'LAX' when random() > 0.4 then 'JFK' else 'LHR' end) as pop,
            '/path/' || (random() * 100)::int as url,
            'GET' as method,
            '10.0.0.1' as ip,
            (random() * 1000)::int as elapsed,
            (case when random() > 0.3 then 'HIT' else 'MISS' end) as cache,
            random() as ttfb,
            true as edge,
            (random() * 10000)::int as resp_bytes,
            'origin-1' as backend
        FROM range(1000000)
    """)
    return test_service_source


def test_performance_dashboard_aggregates_1m_rows(in_memory_duckdb, one_million_row_service):
    start = time.time()
    res = get_aggregates(
        con=in_memory_duckdb,
        src=one_million_row_service,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
    )
    duration = time.time() - start
    assert res["total_rows"] == 1000000
    assert duration < 5.0


def test_performance_insights_1m_rows(in_memory_duckdb, one_million_row_service):
    start = time.time()
    with patch(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        return_value={"LAX": (33.9, -118.4), "JFK": (40.6, -73.7), "LHR": (51.4, -0.4)},
    ):
        res = get_insights(con=in_memory_duckdb, src=one_million_row_service, window_hours=1, baseline_hours=24)
    duration = time.time() - start
    assert len(res["insights"]) > 0
    assert duration < 10.0


# ── Merged from test_environment_connectivity.py ──────────────────────────────────────────────


@pytest.mark.skipif(
    bool(os.getenv("CI") or os.getenv("GITHUB_ACTIONS")),
    reason="Environment connectivity tests require local/remote port-forwards not present in CI",
)
@pytest.mark.skipif(
    any(not target.fastly_service_id for target in ENVIRONMENTS.values()),
    reason="Environment connectivity tests require all three service ID environment variables",
)
class TestEnvironmentConnectivity:
    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_backend_health(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_backend_health(target, timeout_s=3.0)
        assert result.backend_ok, f"{env_name} failed health check: {result.error}"
        assert result.status_code == 200
        assert result.version == "3.0.0-beta4"

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_service_and_tenancy(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_tenancy(target, timeout_s=5.0)
        assert result.service_match, f"{env_name} service mismatch: {result.error}"
        assert result.rejection_ok, f"{env_name} tenancy breach: {result.error}"

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_rbac(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_rbac(target, timeout_s=5.0)
        assert result.auth_config_ok, f"{env_name} auth config failed: {result.error}"
        assert result.unauth_analyst_blocked, f"{env_name} unauth analyst blocking failed: {result.error}"


# ── Merged from test_helpers_sources.py ──────────────────────────────────────────────


def test_lake_kind_creates_cache_dirs_and_duckdb_path(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", buffer_dir=True)
    assert src == {
        "name": "svc",
        "service_id": "svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache_svc"),
        "duckdb_path": str(tmp_path / "svc.duckdb"),
    }
    assert (tmp_path / "cache_svc" / "buffer").is_dir()


def test_lake_kind_without_buffer_dir_only_makes_the_cache_dir(tmp_path):
    make_source(tmp_path, "svc", kind="lake")
    assert (tmp_path / "cache_svc").is_dir()
    assert not (tmp_path / "cache_svc" / "buffer").exists()


def test_lake_kind_applies_overrides_last(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", fos_local_warehouse=False, extra=1)
    assert src["fos_local_warehouse"] is False and src["extra"] == 1


def test_lake_kind_requires_root_and_name():
    with pytest.raises(TypeError):
        make_source(kind="lake")


def test_cache_kind_defaults_and_custom_subdirs(tmp_path):
    assert make_source(str(tmp_path)) == {
        "name": "test_service",
        "service_id": "test-service-id",
        "_cache_dir_override": str(tmp_path),
    }
    root = tmp_path / "cache" / "x"
    src = make_source(root, "x", cache_key="_test_cache_root", mkdirs=("data",))
    assert src == {"name": "x", "service_id": "x", "_test_cache_root": str(root)}
    assert (root / "data").is_dir()


def test_cloud_kind_defaults_overrides_and_errors():
    src = make_source(kind="cloud", bucket="b2", access_key_id="k")
    assert src["name"] == src["service_id"] == "test"
    assert src["bucket"] == "b2" and src["access_key_id"] == "k"
    assert src["storage_mode"] == "cloud" and src["access_level"] == "read_write"
    assert make_source(kind="cloud", access_level="read_only")["access_level"] == "read_only"

    preset = partial(make_source, kind="cloud", provisioning={})
    preset()["provisioning"]["mutated"] = True
    assert preset()["provisioning"] == {}

    with pytest.raises(ValueError, match="unknown source kind"):
        make_source(kind="nope")
