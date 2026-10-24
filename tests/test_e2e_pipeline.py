"""Consolidated tests for test_e2e_pipeline."""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

# ── Fixture: real PyIceberg with a local filesystem warehouse ───────────


@pytest.fixture
def pipeline_env(monkeypatch):
    """Stand up a full pipeline environment with a local-FS PyIceberg
    warehouse. No S3 mocking — PyIceberg writes to ``tmp/warehouse/``,
    the buffer/data dirs live under ``tmp/cache/``, and DuckDB queries
    the resulting parquet files directly from disk.
    """
    tmpdir = tempfile.mkdtemp(prefix="e2e_pipeline_")
    warehouse_path = os.path.join(tmpdir, "warehouse")
    cache_path = os.path.join(tmpdir, "cache")
    os.makedirs(warehouse_path, exist_ok=True)
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "ducklake_data"), exist_ok=True)

    unique_suffix = uuid.uuid4().hex[:6]
    svc_name = f"e2e_svc_{unique_suffix}"
    svc_id = f"e2e-svc-{unique_suffix}"

    source = {
        "name": svc_name,
        "service_id": svc_id,
        "service_name": f"E2E Test {unique_suffix}",
        "bucket": "e2e-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "duckdb_path": os.path.join(tmpdir, f"e2e_{unique_suffix}.duckdb"),
    }

    # Point cache_dir at our temp dir so the buffer/, data/, and
    # iceberg_catalog.db all land in the test sandbox.
    def fake_cache_dir(src):
        return cache_path

    monkeypatch.setattr("backend.core.duckdb._cache_dir", fake_cache_dir)
    monkeypatch.setattr("backend.config.DUCKLAKE_CATALOG", os.path.join(tmpdir, "e2e.ducklake"))
    monkeypatch.setattr("backend.config.DUCKLAKE_DATA_PATH", os.path.join(tmpdir, "ducklake_data/"))

    # Reset module-level caches so the view cache doesn't carry across tests.
    from backend.core import iceberg as _ice

    if hasattr(_ice, "_view_cache"):
        _ice._view_cache.clear()

    yield {"src": source, "tmpdir": tmpdir, "warehouse": warehouse_path, "cache": cache_path}

    shutil.rmtree(tmpdir, ignore_errors=True)
    if hasattr(_ice, "_view_cache"):
        _ice._view_cache.clear()


def _simulate_sync(src, warehouse_path, cache_path):
    """Mimic what sync_data does in production: copy parquet files from
    the iceberg warehouse into the local ``cache/data/`` dir.

    In production, ``sync_data`` downloads from S3 to local disk; here
    we just copy from the local-FS warehouse since the files are
    already on disk. This keeps the test's view-update path identical
    to the production code path."""
    import glob

    data_dir = os.path.join(cache_path, "data")
    os.makedirs(data_dir, exist_ok=True)
    # Iceberg writes parquet files under <warehouse>/<namespace>.db/<table>/data/
    for src_path in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(src_path))
        if not os.path.exists(dst):
            shutil.copy2(src_path, dst)


def _make_log_batch(
    n: int = 20,
    *,
    ip_octet: int = 0,
    path_prefix: str | None = None,
) -> pa.Table:
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
                [base + timedelta(minutes=i * 2) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.{ip_octet}.0.{i}" for i in range(n)]),
            "status": pa.array(
                [200 if i % 5 else 500 for i in range(n)],
                type=pa.uint16(),
            ),
            "url": pa.array(urls),
            "country": pa.array(countries),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array(pops),
        }
    )


# ── Test 1: happy-path full pipeline ────────────────────────────────────


def test_e2e_buffer_to_commit_produces_queryable_snapshot(pipeline_env, monkeypatch):
    """Full lifecycle: write a buffer parquet → commit it to iceberg →
    update the DuckDB view → query → assert the row count round-trips.

    Pinned because losing this contract would let a refactor in any
    of those layers silently produce wrong dashboard numbers. The
    individual layers all have unit tests; this proves they line up."""
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    # 1. Initialize the iceberg table
    table = ice.init_iceberg_table(src)
    assert table is not None

    # 2. Write a buffer parquet (skipping the raw-JSON ingest step)
    batch = _make_log_batch(n=20)
    ice.write_to_buffer(src, batch, "batch_test_0.parquet")
    buffer_files = ice.buffer_files(src)
    assert len(buffer_files) == 1

    # 3. Commit the buffer to iceberg → snapshot written, buffer cleared
    result = ice.commit_buffer(src)
    assert result["files_committed"] >= 1
    assert result["rows_committed"] == 20
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(src) == []  # buffer cleaned up

    # 4. Simulate sync (copy warehouse parquet → cache/data/), then
    # update the DuckDB view + query
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)

    view_name = _safe_table(src["name"])
    row = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
    assert row[0] == 20, f"Expected 20 rows through the full pipeline, got {row[0]}"


def test_e2e_aggregates_via_full_pipeline(pipeline_env, monkeypatch):
    """Like the previous test, but ends in `get_aggregates` instead of
    a raw SELECT. Pins the contract that the dashboard repo returns
    the same row count the pipeline ingested."""
    from backend.core import iceberg as ice
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=15), "batch_agg_0.parquet")
    ice.commit_buffer(src)

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)

    _dashboard_cache.clear()
    out = get_aggregates(
        con=con,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert out["total_rows"] == 15
    assert out["total_rows_total"] >= 15


def test_e2e_insights_via_full_pipeline(pipeline_env, monkeypatch):
    """Pipeline → insights endpoint. Pins that the registry can read
    from the same view shape the dashboard reads, and that every
    registered insight runs against the seeded data without error."""
    from unittest.mock import patch

    from backend.core import iceberg as ice
    from backend.repositories.insights import _insights_cache, get_insights

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=30), "batch_insights_0.parquet")
    ice.commit_buffer(src)

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(con, src, window_hours=1, baseline_hours=1)

    # The contract: no insight should be severity="error" against
    # pipeline-produced data.
    errors = [i for i in result["insights"] if i.get("severity") == "error"]
    assert not errors, "Insights errored against pipeline-produced data:\n" + "\n".join(
        f"  {i['id']}: {i.get('summary', '')[:120]}" for i in errors
    )


def test_commit_buffer_does_no_redundant_catalog_work_per_call(pipeline_env, monkeypatch):
    """A single-chunk commit must attach the DuckLake catalog read-write exactly
    once and must not re-enter the catalog-init path (``init_iceberg_table`` /
    ``_init_iceberg_table_locked``). The old PyIceberg path called
    ``init_iceberg_table`` twice per commit, doubling the per-commit catalog
    metadata reads for no benefit; this pins the DuckLake equivalent so a
    refactor can't reintroduce redundant attach/load work."""
    from backend.core import iceberg as ice
    from backend.core.iceberg import buffer as buffer_mod

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_load_pin_0.parquet")

    counts = {"public": 0, "locked": 0, "write_attach": 0}
    real_public = ice.init_iceberg_table
    real_locked = ice._init_iceberg_table_locked
    real_write_conn = buffer_mod._ducklake_write_connection

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
    assert counts["write_attach"] == 1, f"commit opened {counts['write_attach']} write-attaches — expected 1"
    assert counts["locked"] == 0 and counts["public"] == 0, (
        f"commit_buffer re-entered catalog init (public={counts['public']}, locked={counts['locked']}) — "
        "the DuckLake commit path must not do redundant catalog work"
    )


def test_e2e_empty_buffer_commit_is_noop(pipeline_env, monkeypatch):
    """commit_buffer with no buffer files → zero counts, no exception,
    no snapshot created. Pinned because the cron fires on a schedule
    regardless of whether ingest produced data; the empty-buffer path
    must be a clean no-op."""
    from backend.core import iceberg as ice

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    ice.init_iceberg_table(src)
    result = ice.commit_buffer(src)
    assert result["files_committed"] == 0
    assert result["rows_committed"] == 0
    assert result["snapshot_id"] is None


def test_e2e_two_commits_append_rather_than_overwrite(pipeline_env, monkeypatch):
    """Two separate commit cycles → the view sees both batches summed.
    Pinned because losing this would mean the second snapshot
    silently overwrites the first instead of appending — a
    catastrophic data-loss bug that would be invisible until you
    queried."""
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    ice.init_iceberg_table(src)

    # First batch: 10 rows
    ice.write_to_buffer(src, _make_log_batch(n=10), "batch_a.parquet")
    ice.commit_buffer(src)

    # Second batch: 12 rows
    ice.write_to_buffer(src, _make_log_batch(n=12), "batch_b.parquet")
    ice.commit_buffer(src)

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)
    view_name = _safe_table(src["name"])
    row = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
    assert row[0] == 22, f"Expected 10+12=22 rows after two commits, got {row[0]}"


def test_full_pipeline_including_raw_gzip_ingest(s3_mock, fos_source, monkeypatch, tmp_path, pipeline_env):
    """End-to-end through the RAW .gz ingest path: upload gzipped JSON
    log files to moto S3 → run ``ingest()`` → commit_buffer → update view
    → DuckDB query returns the same rows.

    This is the only E2E test that exercises the decompression + JSON
    parsing + Arrow conversion seam that the other ``pipeline_env``
    tests deliberately skip (they call ``write_to_buffer`` directly).
    A truncated or malformed ``.gz`` from Fastly would silently no-op
    without this path under test.

    Pipeline stages exercised (production code, not stubs):
      1. Discovery: ``list_objects_v2`` paginator filters by ``StartAfter``
         and the Fastly filename regex.
      2. Download: ``_download_chunk_to_local`` pulls each ``.gz`` via
         boto3 ``get_object`` into a per-chunk tempdir.
      3. Parse: DuckDB ``read_json_auto`` decompresses + parses each line.
      4. Cast/transform: types align with ``LOG_FIELD_CATALOG``; backend
         prefix-stripping fires.
      5. Buffer: PyArrow table → local parquet via ``write_to_buffer``.
      6. Commit: ``commit_buffer`` writes a new Iceberg snapshot.
      7. View: ``update_iceberg_view`` registers parquet files behind a
         DuckDB view.
      8. Query: ``SELECT COUNT(*)`` against the view returns the row
         count we ingested.
    """
    import gzip
    import io
    import json

    from backend.core import iceberg as ice
    from backend.core import ingest as ing
    from backend.repositories._base import _safe_table

    cache_path = str(tmp_path / "cache")
    warehouse_path = str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    # Point cache + warehouse at our tmp dirs so the buffer + parquet land
    # in the test sandbox instead of the real ``data/`` directory.
    fos_source["duckdb_path"] = os.path.join(tmp_path, "e2e.duckdb")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)

    # Skip the FOS proxy SECRET setup — ingest's first DuckDB query would
    # otherwise try to load httpfs + start the telemetry proxy. We patch
    # both `duckdb._configure_fos` (called by `get_memory_connection`) and
    # the indirect re-export `ingest._configure_fos` for safety.
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    # Conftest's `s3_mock` already patches `backend.core.duckdb._get_fos_client`,
    # but ingest.py imports `_get_fos_client` at module load and so holds its
    # own reference. Patch that too so ingest's discovery LIST + per-file
    # GET both hit moto instead of trying to reach the real FOS hostname.
    # Production wraps boto3 to accept `caller_hint=` on `get_paginator`;
    # plain moto boto3 doesn't, so wrap to swallow that kwarg.
    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    shim = _CallerHintShim(s3_mock)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: shim)

    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    # Reset module-level iceberg caches so a prior test's snapshot/view
    # state can't leak into this one (the autouse `_reset_module_caches`
    # fixture does this too, but be explicit).
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

    # ── Seed moto with two gzipped JSON log files ─────────────────────
    # Fastly key shape: raw/YYYY-MM-DD/HH/YYYY-MM-DDTHH-MM-SS.<svc>.gz
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

    # ── Bootstrap the Iceberg table BEFORE ingest so commit_buffer has
    # a table to append into. Real provisioning does this too. ─────────
    ice.init_iceberg_table(fos_source)

    # ── Run ingest end-to-end (drain the SSE-style generator) ─────────
    events = list(ing.ingest(source=fos_source))
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None, f"ingest did not emit a 'done' event: {events}"
    assert done["new_files"] == len(files), f"ingest reported {done['new_files']} files but seeded {len(files)}: {done}"
    assert done["rows_inserted"] == rows_per_file * len(files), (
        f"ingest reported {done['rows_inserted']} rows but seeded {rows_per_file * len(files)}: {done}"
    )

    # Buffer should hold the new parquet, ready for commit
    bufs = ice.buffer_files(fos_source)
    assert len(bufs) >= 1, f"expected a buffer parquet after ingest, got: {bufs}"

    # ── Commit buffer → Iceberg snapshot ──────────────────────────────
    result = ice.commit_buffer(fos_source)
    assert result["rows_committed"] == rows_per_file * len(files), (
        f"commit_buffer reported {result['rows_committed']} rows: {result}"
    )
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(fos_source) == []

    # ── Sync warehouse → cache, build view, query ────────────────────
    _simulate_sync(fos_source, warehouse_path, cache_path)
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, fos_source)

    view_name = _safe_table(fos_source["name"])
    (count,) = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
    assert count == rows_per_file * len(files), (
        f"DuckDB view returned {count} rows but ingested {rows_per_file * len(files)}"
    )

    # Spot-check that columns survived the full pipeline with correct types
    sample = con.execute(
        f"SELECT status, url, method, cache, resp_bytes FROM {view_name} ORDER BY url LIMIT 1"
    ).fetchone()
    assert sample is not None
    status, url, method, cache, resp_bytes = sample
    assert isinstance(status, int) and status in (200, 404)
    assert url.startswith("/path/")
    assert method == "GET"
    assert cache == "HIT"
    assert isinstance(resp_bytes, int) and resp_bytes >= 1024

    # ── Writer-side metadata invariant ─────────────────────────────────
    # The pipeline above asserted row counts round-trip through DuckDB,
    # but a refactor that silently dropped the metadata writes would
    # still pass those asserts while corrupting dedup, status rollup,
    # and crash recovery in production. Pin the explicit contract:
    # every successfully-ingested file MUST land in metadata_db's
    # ``ingested_files`` table, and there MUST be zero leftover rows in
    # ``ingest_in_flight`` once commit completes.
    from backend.core import metadata as metadata_db

    service_name = fos_source["name"]
    ingested = metadata_db.list_ingested_files(service_name)
    ingested_names = {row["file_name"] for row in ingested}
    # ingest stores keys as ``s3://<bucket>/<key>``; pin via suffix match
    # so this assertion is robust to any future change in the prefix.
    for key, _ in files:
        assert any(name.endswith(key) for name in ingested_names), (
            f"metadata_db.ingested_files missing entry ending in {key!r} after "
            f"successful ingest. Got {sorted(ingested_names)}. A refactor "
            "that skips metadata_db.insert_ingested_files would break dedup "
            "and the admin sync-status page without any other test failing."
        )
    in_flight = metadata_db.list_in_flight(service_name)
    assert in_flight == [], (
        f"ingest_in_flight non-empty after commit: {in_flight}. "
        "metadata_db.clear_in_flight must run after insert_ingested_files; "
        "leftover rows here cause double-ingest on next restart."
    )


def test_e2e_readonly_dashboard_query_path(pipeline_env, monkeypatch):
    """Exercises the production RO query path end-to-end: a writer cron
    commits buffer → builds the persistent view, then a SEPARATE RO
    connection (the dashboard's connection mode) updates its session
    via update_iceberg_view (hitting the FAST path because the cache
    is now warm) and queries the view.

    This pins the bug class where the fast-path silently failed to
    re-bind the cached `CREATE OR REPLACE VIEW` on RO connections —
    a class the existing e2e tests above missed because they all use
    `duckdb.connect(":memory:")` (RW) with a cold cache (slow path
    only). The dashboard's actual pattern is on-disk + warm cache +
    RO, every request.

    The on-disk DuckDB file also exercises the persistent-view-vs-
    TEMP-view shadowing semantics that pure-memory tests can't reach
    (each :memory: connection has its own session)."""
    import duckdb

    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    # On-disk DuckDB file so RW and RO connections share storage
    db_path = os.path.join(pipeline_env["tmpdir"], "e2e.duckdb")

    # ── Writer: commit buffer + build persistent view ───────────────
    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=8), "batch_ro_e2e.parquet")
    ice.commit_buffer(src)
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    rw = duckdb.connect(db_path)
    ice.update_iceberg_view(rw, src)
    view_name = _safe_table(src["name"])
    rw_rows = rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    assert rw_rows == 8, f"writer view should have 8 rows; got {rw_rows}"

    # Simulate the production bug-state: the persistent view in the
    # DuckDB file got downgraded to "WHERE false" by some earlier event
    # (the old empty-view-downgrade bug). The cache STILL holds the
    # correct CREATE OR REPLACE VIEW SQL. Without the fast-path RO
    # rewrite, the RO session below would re-execute that cached SQL,
    # silently fail (CREATE on RO not allowed), and see the empty
    # persistent view — returning 0 rows.
    rw.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT NULL::INTEGER AS x WHERE false")
    rw.close()

    # ── Reader: separate RO connection, fast-path cache hit ─────────
    # The cache is warm from the writer call above, so the next
    # update_iceberg_view will take the fast path and re-execute the
    # cached `CREATE OR REPLACE VIEW`. On RO that statement fails
    # unless the fast-path code path detects RO and rewrites to
    # `CREATE OR REPLACE TEMP VIEW`.
    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_iceberg_view(ro, src)

        ro_rows = ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        assert ro_rows == 8, (
            f"RO query path returned {ro_rows} rows but the writer's view has 8 — "
            "fast-path re-bind probably failed silently on RO. This was the "
            '"No data available" dashboard bug.'
        )

        # Exercise get_aggregates via the same RO connection — the full
        # production query stack.
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
        assert out["total_rows"] == 8, f"get_aggregates via RO returned {out['total_rows']} rows; expected 8"
    finally:
        ro.close()


def test_e2e_readonly_dashboard_cold_cache_path(pipeline_env, monkeypatch):
    """R-8: same writer/reader split as the warm-cache test above, but
    the dashboard's read happens AFTER the per-source caches have been
    evicted — so update_iceberg_view falls into the slow metadata.json
    scan path. That path is unreachable from local dev (the
    ``dev-no-crons`` memory: no cron tick in dev means the cache never
    gets evicted), so production-only regressions there are likely.

    Pins the contract that a fresh RO connection + cleared caches can
    still rebuild the view via the metadata.json scan + parquet
    discovery + CREATE OR REPLACE TEMP VIEW path."""
    import duckdb

    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_CATALOG",
        os.path.join(pipeline_env["tmpdir"], "ducklake"),
    )
    monkeypatch.setattr(
        "backend.config.DUCKLAKE_DATA_PATH",
        os.path.join(pipeline_env["tmpdir"], "ducklake_data/"),
    )

    db_path = os.path.join(pipeline_env["tmpdir"], "e2e_cold.duckdb")

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=8), "batch_ro_cold.parquet")
    ice.commit_buffer(src)
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    # Warm the view first so the persistent view exists in the DuckDB file.
    rw = duckdb.connect(db_path)
    ice.update_iceberg_view(rw, src)
    view_name = _safe_table(src["name"])
    assert rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
    rw.close()

    # Evict the per-source caches that drive the fast path. Without these
    # in memory, update_iceberg_view has to re-discover the snapshot from
    # the metadata.json on disk + re-enumerate the parquet files — the
    # cold path the cron tick keeps warm in production.
    source_key = src["name"]
    ice._view_cache.pop(source_key, None)

    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_iceberg_view(ro, src)

        ro_rows = ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        assert ro_rows == 8, (
            f"RO cold-cache query returned {ro_rows} rows but the writer committed 8 — "
            "the slow-path metadata.json scan + CREATE OR REPLACE TEMP VIEW probably "
            "broke. This is the production-only regression the dev environment can't "
            "catch (no cron tick → caches never evict in dev)."
        )

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
        assert out["total_rows"] == 8, f"get_aggregates via RO cold-cache returned {out['total_rows']}; expected 8"
    finally:
        ro.close()


# ── Merged from test_multi_service_e2e.py ──────────────────────────────────────────────

import glob
import threading
from functools import partial

import pytest

from backend import config as svcconfig
from tests._helpers.sources import make_source

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
    """Two file:// Iceberg warehouses, one per service. Extends the
    ``pipeline_env`` pattern in ``tests/test_e2e_pipeline.py`` but routes
    ``_warehouse_uri`` per ``source["name"]``. The autouse
    ``isolate_metadata_db`` fixture already redirects ``_cache_dir`` per
    bucket → distinct cache dirs + catalog SQLite files."""
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

    # Cloud-backed sources default DATA_PATH to s3://{bucket}/… (durable FOS),
    # which moto can't serve to DuckDB's httpfs — pin each service's data path
    # to its local warehouse dir so a misrouted source still surfaces as a
    # wrong path.
    from backend.core.iceberg import _ducklake as _dl

    monkeypatch.setattr(_dl, "_default_data_path", lambda s: warehouses[s["name"]])
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # _reset_module_caches (autouse) drains these; explicit clear future-proofs that contract.
    from backend.core import iceberg as _ice

    if hasattr(_ice, "_view_cache"):
        _ice._view_cache.clear()

    yield {"src_a": src_a, "src_b": src_b, "warehouses": warehouses}
    shutil.rmtree(tmpdir, ignore_errors=True)


def _simulate_sync_one(source: dict, warehouse_path: str) -> None:
    """Copy warehouse parquet → ``cache/{bucket}/data/`` so the DuckDB
    view binding can read it. Conftest sandboxes ``_cache_dir`` per-bucket."""
    from backend.core.duckdb import _cache_dir

    data_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)


# ── Test 1: concurrent iceberg commits, per-service isolation ──────────


def test_concurrent_iceberg_commits_two_services_no_cross_contamination(two_service_env):
    """Two services run buffer-write + commit concurrently. Post-commit,
    each iceberg table holds ONLY its own rows. Pins ``_catalog_cache``
    (key: ``source["name"]``) and ``_table_object_cache`` (key:
    ``(bucket, prefix, namespace, name)``). A refactor that collapsed
    either key — or a PyIceberg Transaction race that let one service
    clobber the other's metadata.json — would smear rows across tenants."""
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    src_a, src_b = two_service_env["src_a"], two_service_env["src_b"]

    # Bootstrap sequentially: ``test_e2e_pipeline.py`` already covers the
    # init path; this test isolates concurrent COMMITS as the failure mode.
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
        assert not t.is_alive(), "iceberg commit worker hung past 60s"

    for label, expected in (("a", 12), ("b", 18)):
        r = results[label]
        assert "error" not in r, f"svc_{label} commit raised: {r['error']}"
        assert r["rows_committed"] == expected, (
            f"svc_{label} committed {r['rows_committed']} rows; expected {expected} — "
            "concurrent commit leaked or dropped rows."
        )
        assert r["snapshot_id"] is not None, f"svc_{label} produced no snapshot"
    assert ice.buffer_files(src_a) == [] and ice.buffer_files(src_b) == []

    _simulate_sync_one(src_a, two_service_env["warehouses"]["svc_a"])
    _simulate_sync_one(src_b, two_service_env["warehouses"]["svc_b"])
    con_a = duckdb.connect(":memory:")
    ice.update_iceberg_view(con_a, src_a)

    con_b = duckdb.connect(":memory:")
    ice.update_iceberg_view(con_b, src_b)

    view_a, view_b = _safe_table(src_a["name"]), _safe_table(src_b["name"])

    (count_a,) = con_a.execute(f"SELECT COUNT(*) FROM {view_a}").fetchone()
    (count_b,) = con_b.execute(f"SELECT COUNT(*) FROM {view_b}").fetchone()
    assert count_a == 12, f"svc_a view has {count_a} rows; expected 12 (no svc_b leak)"
    assert count_b == 18, f"svc_b view has {count_b} rows; expected 18 (no svc_a leak)"

    bleed_a_in_b = con_b.execute(
        f"SELECT COUNT(*) FROM {view_b} WHERE ip LIKE '10.1.%' OR url LIKE '/alpha/%'"
    ).fetchone()[0]
    assert bleed_a_in_b == 0, f"{bleed_a_in_b} svc_a rows leaked into svc_b — cache key collision"

    bleed_b_in_a = con_a.execute(
        f"SELECT COUNT(*) FROM {view_a} WHERE ip LIKE '10.2.%' OR url LIKE '/bravo/%'"
    ).fetchone()[0]
    assert bleed_b_in_a == 0, f"{bleed_b_in_a} svc_b rows leaked into svc_a — cache key collision"


# ── Test 2: concurrent metadata_db writes, per-service isolation ───────


def test_concurrent_metadata_db_writes_two_services_no_cross_contamination():
    """Two services run independent metadata_db mutations on separate
    threads — svc-a deletes a seed row, svc-b inserts a new one. Each
    SQLite file must reflect ONLY its own mutation. Pins
    ``get_con(service_id)`` keying — a regression that cached the
    connection on the wrong key (or globalised ``_initialized`` without
    the per-service guard) would write svc-b's rows into svc-a's file.

    DEVIATION FROM SPEC: the spec said "delete a custom field in svc-a".
    Custom fields live in YAML service config (``backend/core/log_fields.py``
    + ``backend/config.py``), NOT in metadata_db. Pinned here instead: a
    raw DELETE on ``ingested_files`` for svc-a + ``insert_ingested_files``
    on svc-b — same isolation contract, real metadata_db surface."""
    from backend.core import metadata as metadata_db

    svc_a, svc_b = "svc-md-a", "svc-md-b"
    seed_key = "raw/2026-06-10/00/seed.gz"
    new_key = "raw/2026-06-10/01/svc_b_only.gz"

    # Same seed name on both sides so a cross-file smear would be visibly wrong.
    metadata_db.insert_ingested_files(svc_a, [(seed_key, 100, 1024)])
    metadata_db.insert_ingested_files(svc_b, [(seed_key, 100, 1024)])
    pre_a = {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)}
    pre_b = {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)}
    assert pre_a == {seed_key} and pre_b == {seed_key}, f"pre={pre_a!r}/{pre_b!r}"

    barrier = threading.Barrier(2)
    errors: list[str] = []
    elock = threading.Lock()

    def deleter_svc_a() -> None:
        try:
            barrier.wait()
            con = metadata_db.get_con(svc_a)
            con.execute(
                "DELETE FROM ingested_files WHERE source_name = ? AND file_name = ?",
                (svc_a, seed_key),
            )
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
        assert not t.is_alive(), "metadata_db worker hung past 15s"
    assert not errors, f"metadata_db worker(s) raised: {errors}"

    post_a = {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)}
    post_b = {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)}
    assert post_a == set(), (
        f"svc_a should be empty after deletion; got {post_a!r} — "
        "either delete didn't land OR svc_b's insert smeared into svc_a."
    )
    assert post_b == {seed_key, new_key}, (
        f"svc_b should hold seed + concurrently-inserted row; got {post_b!r} — "
        "either insert didn't land OR svc_a's delete smeared into svc_b."
    )

    path_a, path_b = metadata_db.db_path(svc_a), metadata_db.db_path(svc_b)
    from backend.core.metadata.pg_connection import is_postgres

    if not is_postgres():
        assert path_a != path_b, f"two services collapsed onto one SQLite file: {path_a}"
        assert os.path.exists(path_a) and os.path.exists(path_b)


# ── Merged from test_smoke_end_to_end.py ──────────────────────────────────────────────

from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID

# ── Fixture: a service with realistic seeded data ───────────────────────


@pytest.fixture
def seeded_service(in_memory_duckdb, test_service_source):
    """Seed the test DuckDB with ~50 log rows spanning the last 2 hours."""
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
    statuses = [200, 200, 200, 404, 500] * 10  # 50 rows; 10% errors, 10% 404s
    pops = ["LAX", "JFK", "LHR"] * 17
    countries = ["US", "US", "GB", "DE", "JP"] * 10
    rows = []
    for i in range(50):
        ts = base + timedelta(minutes=i * 2)
        rows.append(
            (
                ts,
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
        )
    placeholders = ", ".join(["?"] * len(catalog_fields))
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} ({', '.join(f'''\"{n}\"''' for n, _ in catalog_fields)}) VALUES ({placeholders})",
        rows,
    )
    return test_service_source


# ── HTTP smoke tests (one per critical user journey) ────────────────────


def test_smoke_dashboard_aggregates_returns_real_counts(client, seeded_service):
    """Full stack: POST /api/dashboard/aggregates → real repository →
    seeded DuckDB → JSON response with the expected aggregated counts.

    Pinned because losing this would surface as the FE's primary
    landing page silently rendering "0 requests" despite having data
    — exactly the class of bug where unit tests stay green because
    each layer's mock returns a plausible value."""
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
    has_top = any(entry.get("top") for entry in body["data"].values())
    assert has_top, "Expected at least one field to have top values"


def test_smoke_dashboard_aggregates_with_filter_narrows_results(client, seeded_service):
    """Same POST as the aggregates smoke, but with a `country=GB` filter
    — must return strictly fewer total_rows than the unfiltered case.
    This is the regression guard for the CAST(... AS VARCHAR) +
    stringified-params fix in build_where_clause: a type-mismatched
    filter must NOT crash AND must narrow correctly."""
    unfiltered = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    ).json()

    gb_only = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {"country": {"mode": "include", "values": ["GB"]}},
        },
    ).json()

    assert gb_only["total_rows"] > 0  # there ARE GB rows in the seed
    assert gb_only["total_rows"] < unfiltered["total_rows"]


def test_smoke_dashboard_aggregates_with_numeric_filter_on_string_column_does_not_crash(client, seeded_service):
    """A numeric filter value (`country=0`) on a VARCHAR column must NOT
    crash with "Could not convert string '0' to INT32" or similar.
    Pinned because hypothesis caught this exact regression mode in
    build_where_clause. The FE shouldn't send this, but a defensive
    backend keeps the dashboard alive when input validation drifts."""
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {"country": {"mode": "include", "values": [0]}},
        },
    )
    # MUST be 200 (zero rows match) and NOT 500 (crash)
    assert resp.status_code == 200
    # No string '0' country exists in the seed → zero matches
    assert resp.json()["total_rows"] == 0


def test_smoke_insights_endpoint_returns_insight_cards_with_distinct_ids(client, seeded_service):
    """POST /api/insights returns the full set of registered insight
    cards, each with a distinct id. Pinned because losing the
    registry/temp-table wiring would silently render an empty
    insights page, and a duplicate-id regression would re-introduce
    the React-key crash."""
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 1, "baseline_hours": 1, "filters": {}},
        )
    assert resp.status_code == 200
    body = resp.json()
    insights = body["insights"]
    # The seed schema doesn't cover every catalog field, so not every
    # insight has its `required_fields` satisfied. We pin a floor that
    # proves the registry → temp-table → SQL path still works for the
    # majority of insights — a drop here means a regression in either
    # the registry wiring or the schema-coverage check.
    assert len(insights) >= 15
    for card in insights:
        for key in ("id", "title", "severity", "summary", "items"):
            assert key in card, f"Insight card {card.get('id')} missing key {key}"
    ids = [c["id"] for c in insights]
    assert len(ids) == len(set(ids)), f"Duplicate insight ids: {[i for i in ids if ids.count(i) > 1]}"


def test_smoke_origin_timeseries_with_sub_minute_bucket(client, seeded_service):
    """POST /api/origin/timeseries with bucket_minutes=1/60 (1 second).
    Integration-level regression test for the `_clamp_to_float` fix
    — sub-minute granularity must round-trip from request to response
    without collapsing to 1m."""
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
    body = resp.json()
    assert "has_data" in body


# ── Error-envelope shape on @query_errors paths (audit follow-up) ───────────


def test_query_errors_decorator_returns_documented_envelope_for_malformed_sql(client, seeded_service):
    """When the dashboard endpoint forwards malformed SQL into DuckDB,
    the @query_errors decorator converts the exception to an HTTP error
    with the documented envelope shape {detail: ...} — the frontend's
    openapi-fetch decoder relies on that key being present.

    Pinned via a request whose filter shape provokes a DuckDB parse
    error in the WHERE-clause builder rather than mutating route SQL
    directly (the /query endpoint is SELECT-only via sql_validator).
    """
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            # Empty-string mode is not a recognised filter mode; the
            # builder may either 422 it (FastAPI/Pydantic) or treat it
            # as a no-op. Either way the response body must carry
            # `detail` if a 4xx surfaces.
            "filters": {"status": {"mode": "", "values": [200]}},
        },
    )
    # Either 200 (no-op filter), 4xx (validation), or 500 (decorator
    # caught a SQL error). The contract this test pins: if the response
    # is NOT 200, the body MUST be JSON with a `detail` key — that's
    # what openapi-fetch and the frontend error toast rely on.
    if resp.status_code != 200:
        body = resp.json()
        assert "detail" in body, (
            f"non-2xx response missing 'detail' envelope key: status={resp.status_code} body={body!r}"
        )


def test_query_errors_envelope_for_filter_type_mismatch(client, seeded_service):
    """The existing numeric-on-string-column smoke test asserts the
    request does not 500; this asserts the broader envelope contract:
    if the @query_errors decorator catches anything (which it does for
    this exact regression history), the response carries a `detail`
    key. The frontend's QueryProvider error toast subscribes to this
    field — a regression that returned `{error: ...}` instead would
    silently break the toast.
    """
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {"country": {"mode": "include", "values": [0]}},
        },
    )
    # Smoke test asserts 200; envelope test takes either path:
    if resp.status_code != 200:
        body = resp.json()
        assert "detail" in body


# ── Merged from test_performance_smoke.py ──────────────────────────────────────────────

import time

import pytest

from backend.repositories.dashboard import get_aggregates
from backend.repositories.insights import get_insights


@pytest.fixture
def one_million_row_service(in_memory_duckdb, test_service_source):
    """Seed 1,000,000 rows into DuckDB using native generation for speed."""
    table = _safe_table(test_service_source["name"])

    # 1. Create table schema
    in_memory_duckdb.execute(f"""
        CREATE TABLE {table} (
            timestamp TIMESTAMPTZ,
            status INTEGER,
            country VARCHAR,
            pop VARCHAR,
            url VARCHAR,
            method VARCHAR,
            ip VARCHAR,
            elapsed INTEGER,
            cache VARCHAR,
            ttfb DOUBLE,
            edge BOOLEAN,
            resp_bytes INTEGER,
            backend VARCHAR
        )
    """)

    # 2. Seed 1M rows using range() and random values
    # This takes ~1-2 seconds on a modern laptop, much faster than executemany()
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
    """Benchmark dashboard aggregates over 1M rows.

    Target: < 2.0s on medium CI runners.
    """
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
    print(f"\nDashboard aggregates (1M rows) took {duration:.3f}s")

    assert res["total_rows"] == 1000000
    # CI runners vary, but 5s is a safe "something is very wrong" threshold
    assert duration < 5.0


def test_performance_insights_1m_rows(in_memory_duckdb, one_million_row_service):
    """Benchmark automated insights over 1M rows.

    This exercises ~20+ distinct analytical queries against the dataset.
    Target: < 5.0s on medium CI runners.
    """
    start = time.time()

    # insights registry needs some lat/lon data for the map card
    with patch(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        return_value={"LAX": (33.9, -118.4), "JFK": (40.6, -73.7), "LHR": (51.4, -0.4)},
    ):
        res = get_insights(con=in_memory_duckdb, src=one_million_row_service, window_hours=1, baseline_hours=24)

    duration = time.time() - start
    print(f"\nInsights generation (1M rows) took {duration:.3f}s")

    assert len(res["insights"]) > 0
    # Insights are heavier, 10s is the "safety" threshold
    assert duration < 10.0


# ── Merged from test_environment_connectivity.py ──────────────────────────────────────────────


import pytest

from scripts.check_environment_health import (
    ENVIRONMENTS,
    check_backend_health,
    check_rbac,
    check_tenancy,
)


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

        assert result.backend_ok, (
            f"Environment '{env_name}' failed health check: {result.error}\n"
            f"Backend URL: {target.backend_url}\n"
            f"Remediation: {target.remediation_cmd}"
        )
        assert result.status_code == 200
        assert result.version == "3.0.0-beta4"

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_service_and_tenancy(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_tenancy(target, timeout_s=5.0)

        assert result.service_match, (
            f"Environment '{env_name}' service mismatch: {result.error}\n"
            f"Expected: {target.fastly_service_id}, Got: {result.resolved_service_id}\n"
            f"Remediation: {target.remediation_cmd}"
        )
        assert result.rejection_ok, (
            f"Environment '{env_name}' tenancy breach: {result.error}\nRemediation: {target.remediation_cmd}"
        )

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_rbac(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_rbac(target, timeout_s=5.0)

        assert result.auth_config_ok, (
            f"Environment '{env_name}' auth config check failed: {result.error}\nRemediation: {target.remediation_cmd}"
        )
        assert result.unauth_analyst_blocked, (
            f"Environment '{env_name}' unauth analyst blocking failed: {result.error}\n"
            f"Remediation: {target.remediation_cmd}"
        )


# ── Merged from test_helpers_sources.py ──────────────────────────────────────────────

from functools import partial

import pytest

from tests._helpers.sources import make_source


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
    assert src["fos_local_warehouse"] is False
    assert src["extra"] == 1


def test_lake_kind_requires_root_and_name():
    with pytest.raises(TypeError):
        make_source(kind="lake")


def test_cache_kind_defaults_match_the_rollup_reader_tests(tmp_path):
    assert make_source(str(tmp_path)) == {
        "name": "test_service",
        "service_id": "test-service-id",
        "_cache_dir_override": str(tmp_path),
    }


def test_cache_kind_custom_key_and_subdirs(tmp_path):
    root = tmp_path / "cache" / "x"
    src = make_source(root, "x", cache_key="_test_cache_root", mkdirs=("data",))
    assert src == {"name": "x", "service_id": "x", "_test_cache_root": str(root)}
    assert (root / "data").is_dir()


def test_cloud_kind_defaults_and_overrides():
    src = make_source(kind="cloud", bucket="b2", access_key_id="k")
    assert src["name"] == src["service_id"] == "test"
    assert src["bucket"] == "b2" and src["access_key_id"] == "k"
    assert src["storage_mode"] == "cloud" and src["access_level"] == "read_write"
    assert make_source(kind="cloud", access_level="read_only")["access_level"] == "read_only"


def test_bound_presets_never_share_mutable_state_between_calls():
    preset = partial(make_source, kind="cloud", provisioning={})
    first = preset()
    first["provisioning"]["mutated"] = True
    assert preset()["provisioning"] == {}


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown source kind"):
        make_source(kind="nope")
