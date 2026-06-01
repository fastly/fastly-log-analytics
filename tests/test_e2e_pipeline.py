"""End-to-end pipeline test: buffer → commit → view → query → insights.

This is the only test in the suite that exercises real PyIceberg
catalog write + real DuckDB view-update + real DuckDB query in a
single flow. Every layer is real except the S3 backend (which we
replace with a local-filesystem warehouse, because PyIceberg's
fsspec/s3fs/aiobotocore stack doesn't play nicely with moto — the
async vs. sync runtime mismatch raises mid-write).

What this catches that unit tests miss:
  - Seam bugs between layers (commit writes a snapshot, the view-
    builder reads from the same metadata.json, the repo queries
    through the view — does the entire chain actually line up?)
  - Real PyIceberg integration regressions (parquet write codecs,
    snapshot metadata, manifest layout, schema alignment).
  - Wrong row counts: if any seam silently drops rows the test fails.

What we deliberately skip:
  - The raw-JSON `.gz` ingest path (would need DuckDB httpfs reading
    from a mocked S3, which is brittle). We call `write_to_buffer`
    directly — the same code path `ingest()` ultimately invokes.
  - `sync_data()` — it's an S3 → local file copy; with a local
    warehouse the files are already "local," so sync is a no-op
    and not exercised here. It has its own moto-based unit test.

Tests are intentionally heavyweight: one full lifecycle per test,
focused asserts. If a test fails, the failure is high-signal — a
real layer has broken its contract with the next.
"""

from __future__ import annotations

import os
import shutil
import tempfile
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

    source = {
        "name": "e2e_svc",
        "service_id": "e2e-svc-id",
        "service_name": "E2E Test",
        "bucket": "e2e-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }

    # Point cache_dir at our temp dir so the buffer/, data/, and
    # iceberg_catalog.db all land in the test sandbox.
    def fake_cache_dir(src):
        return cache_path

    monkeypatch.setattr("backend.core.duckdb._cache_dir", fake_cache_dir)

    # Override _warehouse_uri to point at our local-FS warehouse so
    # PyIceberg writes parquet files to local disk instead of S3.
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda src: f"file://{warehouse_path}")

    # Reset module-level caches so the catalog/snapshot caches don't
    # carry across tests.
    from backend.core import iceberg as _ice

    _ice._catalog_cache.clear()
    _ice._snapshot_files_cache.clear()
    _ice._table_object_cache.clear()
    if hasattr(_ice, "_view_cache"):
        _ice._view_cache.clear()

    yield {"src": source, "tmpdir": tmpdir, "warehouse": warehouse_path, "cache": cache_path}

    shutil.rmtree(tmpdir, ignore_errors=True)
    _ice._catalog_cache.clear()
    _ice._snapshot_files_cache.clear()
    _ice._table_object_cache.clear()
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


def _make_log_batch(n: int = 20) -> pa.Table:
    """Build a PyArrow table shaped like a buffer batch from real ingest.

    Matches the column types declared in LOG_FIELD_CATALOG / get_arrow_schema
    so commit_buffer can align without warnings.
    """
    base = datetime.now(UTC) - timedelta(hours=1)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i * 2) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
            "status": pa.array(
                [200 if i % 5 else 500 for i in range(n)],
                type=pa.uint16(),
            ),
            "url": pa.array([f"/path/{i % 10}" for i in range(n)]),
            "country": pa.array(["US" if i % 3 == 0 else "GB" for i in range(n)]),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array([["LAX", "JFK", "LHR"][i % 3] for i in range(n)]),
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

    # 1. Initialize the iceberg table
    table = ice.init_iceberg_table(src)
    assert table is not None
    assert table.schema() is not None

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


def test_commit_buffer_loads_table_once_per_call(pipeline_env, monkeypatch):
    """commit_buffer used to fall into init_iceberg_table twice within a
    single invocation — once via _init_iceberg_table_locked at the top,
    then a second redundant `init_iceberg_table(source)` mid-function
    just before the .append() call. Each public `init_iceberg_table`
    call hits the cloud (catalog.load_table → HEAD + GET on the current
    metadata.json), so the redundant call doubled the per-commit
    metadata.json read count for no benefit. Pin against regression."""
    from backend.core import iceberg as ice

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # Bootstrap once so the table exists before we start counting.
    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_load_pin_0.parquet")

    counts = {"public": 0, "locked": 0}
    real_public = ice.init_iceberg_table
    real_locked = ice._init_iceberg_table_locked

    def counting_public(source, create=True):
        counts["public"] += 1
        return real_public(source, create=create)

    def counting_locked(source, create=True):
        counts["locked"] += 1
        return real_locked(source, create=create)

    monkeypatch.setattr(ice, "init_iceberg_table", counting_public)
    monkeypatch.setattr(ice, "_init_iceberg_table_locked", counting_locked)

    ice.commit_buffer(src)

    # Pin: exactly one _init_iceberg_table_locked (top of commit_buffer),
    # zero public init_iceberg_table calls when the table already exists
    # (the L1141 fallback only fires for missing tables; the previous
    # redundant L1172 call has been removed). If this assert fails, a
    # refactor reintroduced a load_table call inside commit_buffer.
    assert counts["locked"] == 1, f"_init_iceberg_table_locked called {counts['locked']}× — expected 1"
    assert counts["public"] == 0, (
        f"init_iceberg_table called {counts['public']}× inside commit_buffer — "
        "expected 0 for an existing table. The removed L1172 redundant call "
        "may be back."
    )


def test_commit_buffer_preseeds_manifest_cache_before_async_summary(pipeline_env, monkeypatch):
    """Pin the call order in commit_buffer: `_update_snapshot_cache_from_delta`
    MUST run before `_write_metadata_pointer`. The pointer writer spawns the
    async table-summary thread that calls `_get_cached_or_scan_metadata`,
    which reads `_manifest_metadata_cache`. The delta path pre-seeds that
    cache for the new manifest. If the order is swapped, the async thread
    races ahead of the seed and re-GETs the new ~10 KB .avro every commit.

    Regression for commits e7f4d15 (Stream D — added pre-seed) + this fix
    (Stream F — fixed the race). The original Stream D wrote the seed AFTER
    spawning the async thread, so the seed never won the race."""
    from backend.core import iceberg as ice

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_order_pin_0.parquet")

    calls: list[str] = []
    real_delta = ice._update_snapshot_cache_from_delta
    real_pointer = ice._write_metadata_pointer

    def tracking_delta(source, table):
        calls.append("delta")
        return real_delta(source, table)

    def tracking_pointer(source, location, table=None):
        calls.append("pointer")
        return real_pointer(source, location, table=table)

    monkeypatch.setattr(ice, "_update_snapshot_cache_from_delta", tracking_delta)
    monkeypatch.setattr(ice, "_write_metadata_pointer", tracking_pointer)

    ice.commit_buffer(src)

    assert calls == ["delta", "pointer"], (
        f"commit_buffer called {calls} — expected ['delta', 'pointer']. "
        "If pointer runs before delta, the async table-summary thread spawned "
        "by _write_metadata_pointer can call _get_cached_or_scan_metadata "
        "before the per-manifest cache is seeded, re-GETting the new .avro "
        "every commit."
    )


def test_commit_buffer_populates_table_cache_so_metadata_sync_skips_load_table(pipeline_env, monkeypatch):
    """Pin Streams G+H: across a full commit cycle (commit_buffer +
    post-commit metadata_sync simulation), zero FOS-bound load_table
    calls should escape our Table cache.

    Stream G covers _init_iceberg_table_locked (callable from anywhere
    using init_iceberg_table). Stream H covers pyiceberg's INTERNAL
    SqlCatalog.commit_table which calls self.load_table inside
    table.append. Without H, every commit pays ~868 KB metadata.json GET
    for pyiceberg's CAS pre-check. Both wins together drop the steady-
    state per-commit cycle to zero metadata.json GETs (still 1 PUT — we
    write the new snapshot)."""
    from backend.core import iceberg as ice

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # In the pipeline_env fixture, the FOS endpoint is unreachable so the
    # real _write/_read_metadata_pointer GET/PUT both 403. The freshness
    # check needs a working pointer roundtrip, so patch both halves to
    # share an in-memory dict — production semantics without real FOS auth.
    pointer_store: dict[tuple, str] = {}

    def fake_write_pointer(source, location, table=None):
        identifier = ice._table_identifier(source)
        pointer_store[(source.get("bucket"), source.get("prefix", ""), identifier)] = location

    def fake_read_pointer(source, identifier):
        return pointer_store.get((source.get("bucket"), source.get("prefix", ""), identifier))

    monkeypatch.setattr(ice, "_write_metadata_pointer", fake_write_pointer)
    monkeypatch.setattr(ice, "_read_metadata_pointer", fake_read_pointer)

    # Bootstrap the table. The CREATE path doesn't populate the table cache
    # (only the LOAD path does), and fake_write_pointer doesn't fire from
    # create-table either — so we manually seed both, simulating the state
    # after one warmup commit cycle in production. This puts us in the
    # steady-state regime that Streams G+H optimize for.
    bootstrap_table = ice.init_iceberg_table(src)
    fake_write_pointer(src, bootstrap_table.metadata_location)
    ice._set_cached_table(src, ice._table_identifier(src), bootstrap_table)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_cache_pin_0.parquet")

    # Reset the Stream H fall-through counter. From here on, any increment
    # means our cache missed and pyiceberg had to refetch metadata.json
    # from FOS — the exact regression we're guarding against.
    ice._sql_load_table_real_calls["n"] = 0

    # Run commit_buffer end-to-end. The expected flow:
    #   1. _init_iceberg_table_locked → _load_table_cached → cache hit (Stream G)
    #   2. table.append → pyiceberg.commit_table → self.load_table → patched
    #      _cached_load_table → pointer matches cached → returns cached (Stream H)
    #   3. table.append writes new metadata + updates table.metadata_location
    #   4. _set_cached_table replaces cache entry with post-commit table
    ice.commit_buffer(src)

    after_commit = ice._sql_load_table_real_calls["n"]
    assert after_commit == 0, (
        f"commit_buffer triggered {after_commit} FOS-bound SqlCatalog.load_table "
        "call(s). Stream H's FosSqlCatalog subclass should have served them all "
        "from the Table cache. Check _get_fos_catalog_class is used by "
        "_get_catalog, _fos_source is attached to the catalog, and "
        "_read_metadata_pointer returns the just-written pointer location."
    )

    # Simulate metadata_sync's first action: init_iceberg_table(create=False).
    # Stream G ensures this resolves via _load_table_cached without even
    # calling self.load_table — but if it did call, Stream H would also
    # cache-hit. Either way: counter stays at 0.
    ice.init_iceberg_table(src, create=False)

    assert ice._sql_load_table_real_calls["n"] == 0, (
        f"Post-commit init_iceberg_table(create=False) triggered "
        f"{ice._sql_load_table_real_calls['n']} FOS-bound load_table call(s). "
        "Stream G's Table-object cache regressed — the post-commit metadata.json "
        "GET (~865 KB) is back. Check _set_cached_table is still called after "
        "table.append() in commit_buffer."
    )


def test_e2e_empty_buffer_commit_is_noop(pipeline_env, monkeypatch):
    """commit_buffer with no buffer files → zero counts, no exception,
    no snapshot created. Pinned because the cron fires on a schedule
    regardless of whether ingest produced data; the empty-buffer path
    must be a clean no-op."""
    from backend.core import iceberg as ice

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

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


def test_full_pipeline_including_raw_gzip_ingest(s3_mock, fos_source, monkeypatch, tmp_path):
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
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")

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

    # Reset module-level iceberg caches so a prior test's snapshot/view
    # state can't leak into this one (the autouse `_reset_module_caches`
    # fixture does this too, but be explicit).
    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # ── Seed moto with two gzipped JSON log files ─────────────────────
    # Fastly key shape: raw/YYYY-MM-DD/HH/YYYY-MM-DDTHH-MM-SS.<svc>.gz
    base = datetime.now(UTC) - timedelta(hours=2)
    rows_per_file = 5
    files = [
        ("raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz", base),
        ("raw/2026-05-20/10/2026-05-20T10-05-00.svc.gz", base + timedelta(minutes=5)),
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
