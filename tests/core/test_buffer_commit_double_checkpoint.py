"""Iceberg-side half of the buffer-commit double-checkpoint contract.

Sibling ``test_buffer_commit_idempotent.py`` pins the SQLite half. This
file pins the Iceberg-snapshot half: half-states where one of the two
durable channels lands but the other doesn't.

``backend/core/iceberg/buffer.py`` recovery is a UNION (see L500–L558):
either ``list_committed_basenames`` (SQLite) or
``_recent_snapshot_markers`` (Iceberg) marking a basename is enough
proof of a prior successful append; the sweep tombstone-and-skips it.

Test 2 OBSERVED behaviour diverges from the spec's "snapshot is source
of truth" wording: today's union semantics also rescue on SQLite alone.
That's intentional — re-appending whenever the snapshot lacked a
marker would dup-append every file once the snapshot aged past the
1-hour ``_COMMIT_MARKER_LOOKBACK_S`` window. See notes.

Audit finding: buffer-commit dup-row race (2026-06-12).
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest


@pytest.fixture
def pipeline_env(monkeypatch):
    """Local-FS PyIceberg warehouse + cache/buffer under tmpdir. Mirror
    of the same-named fixture in ``tests/test_e2e_pipeline.py`` (pytest
    fixtures don't cross file boundaries without conftest sharing).
    Autouse ``isolate_metadata_db`` sandboxes SQLite on top."""
    tmpdir = tempfile.mkdtemp(prefix="buffer_double_checkpoint_")
    warehouse_path = os.path.join(tmpdir, "warehouse")
    cache_path = os.path.join(tmpdir, "cache")
    os.makedirs(warehouse_path, exist_ok=True)
    os.makedirs(cache_path, exist_ok=True)

    source = {
        "name": "double_checkpoint_svc",
        "service_id": "double-checkpoint-svc-id",
        "service_name": "Double Checkpoint Test",
        "bucket": "double-checkpoint-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")

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


def _make_log_batch(n: int = 10) -> pa.Table:
    """Log-shaped Arrow batch matching get_arrow_schema so commit_buffer's
    _align_to_schema doesn't warn. Same column shape as the sibling
    fixture in tests/test_e2e_pipeline.py."""
    base = datetime.now(UTC) - timedelta(hours=1)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i * 2) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
            "status": pa.array([200 if i % 5 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array([f"/path/{i % 5}" for i in range(n)]),
            "country": pa.array(["US" if i % 2 == 0 else "GB" for i in range(n)]),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array([["LAX", "JFK", "LHR"][i % 3] for i in range(n)]),
        }
    )


def _table_row_count(src: dict) -> int:
    """Load the table fresh from the catalog and scan its row count.
    Bypasses any in-process cache so we observe the actual on-disk state."""
    from backend.core import iceberg as ice

    catalog = ice._get_catalog(src)
    table = catalog.load_table(ice._table_identifier(src))
    return table.scan().to_arrow().num_rows


# ── Test 1: snapshot marker present, SQLite marker missing ─────────────


@pytest.mark.skip(reason="Migrated to ducklake")
def test_snapshot_marker_present_but_sqlite_marker_missing_does_not_double_append(pipeline_env, monkeypatch):
    """Crash window between ``table.append`` and ``mark_buffers_committed``:
    snapshot carries the commit-marker, no SQLite row landed. The next
    commit tick must detect the marker via ``_recent_snapshot_markers``
    and SKIP the re-append — if the marker channel doesn't rescue,
    every crash in this window doubles the rows for the affected batch.
    """
    from backend.core import iceberg as ice
    from backend.core import metadata as _meta
    from backend.core.iceberg import buffer as buffer_mod
    from backend.core.iceberg.buffer import sweep_tombstoned_buffer_files

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # First commit: writes the snapshot marker AND the SQLite row.
    ice.init_iceberg_table(src)
    batch = _make_log_batch(n=10)
    ice.write_to_buffer(src, batch, "batch_orphan_snapshot.parquet")
    first_result = ice.commit_buffer(src)
    assert first_result["rows_committed"] == 10
    assert first_result["snapshot_id"] is not None

    rows_after_first_commit = _table_row_count(src)
    assert rows_after_first_commit == 10

    # Plant the half-state: drop the SQLite row (simulate
    # mark_buffers_committed never landing) and resurrect the buffer
    # parquet on disk so the next tick re-discovers it. Same basename
    # so the marker hash still matches what's in the snapshot.
    purged = _meta.purge_committed_buffer_rows(src["service_id"], ["batch_orphan_snapshot.parquet"])
    assert purged == 1, f"expected to drop the 1 committed_buffers row from the first commit, got {purged}"

    # Force any tombstones from the first commit out so the resurrected
    # file shows up in buffer_files() instead of being filtered.
    sweep_tombstoned_buffer_files(src, grace_seconds=0)
    ice.write_to_buffer(src, batch, "batch_orphan_snapshot.parquet")
    bufs_before_recovery = ice.buffer_files(src)
    assert any(p.endswith("batch_orphan_snapshot.parquet") for p in bufs_before_recovery), (
        f"resurrected buffer file not visible: {bufs_before_recovery}"
    )

    # Sanity: snapshot DOES carry the marker for our basename.
    catalog = ice._get_catalog(src)
    table = catalog.load_table(ice._table_identifier(src))
    markers = buffer_mod._recent_snapshot_markers(table, since_ms=0)
    expected_marker = buffer_mod._buffer_basename_marker("batch_orphan_snapshot.parquet")
    assert expected_marker in markers, (
        "first commit did NOT tag its snapshot with the buffer-commit marker — "
        "rest of this test cannot verify Iceberg-marker recovery."
    )

    # Recovery commit tick must skip the re-append.
    recovery_result = ice.commit_buffer(src)
    assert recovery_result["rows_committed"] == 0, (
        f"recovery commit re-appended {recovery_result['rows_committed']} rows — "
        "the Iceberg-snapshot-marker channel failed to rescue an orphaned buffer."
    )

    rows_after_recovery = _table_row_count(src)
    assert rows_after_recovery == 10, (
        f"row count {rows_after_first_commit} → {rows_after_recovery} — orphaned buffer "
        "got re-appended because the snapshot-marker recovery scan failed."
    )

    bufs_after_recovery = ice.buffer_files(src)
    assert not any(p.endswith("batch_orphan_snapshot.parquet") for p in bufs_after_recovery), (
        f"recovery did not tombstone the orphan buffer file: {bufs_after_recovery}"
    )


# ── Test 2: SQLite marker present, snapshot marker missing ─────────────


@pytest.mark.skip(reason="Migrated to ducklake")
def test_sqlite_marker_present_but_snapshot_missing_runs_full_commit(pipeline_env, monkeypatch):
    """Inverse half-state: SQLite committed_buffers says the basename
    landed, but the Iceberg table holds NO snapshot marker for it.

    OBSERVED behaviour: today's union semantics (buffer.py:L536) treat
    EITHER channel as sufficient → tombstone-and-skip on SQLite alone,
    NOT re-append. Compaction-dedup (PR #21) is the third layer that
    catches any genuine snapshot-missing case the sweep skips here.
    See module docstring for why diverging from "snapshot is source of
    truth" is the right call given the 1-hour marker-lookback window.
    """
    from backend.core import iceberg as ice
    from backend.core import metadata as _meta
    from backend.core.iceberg import buffer as buffer_mod

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # Seed the table so we have a known baseline.
    ice.init_iceberg_table(src)
    seed = _make_log_batch(n=4)
    ice.write_to_buffer(src, seed, "batch_seed.parquet")
    ice.commit_buffer(src)
    baseline_rows = _table_row_count(src)
    assert baseline_rows == 4

    # Plant the half-state: a fresh buffer parquet that has NEVER been
    # appended, plus a SQLite row claiming it WAS committed.
    test_batch = _make_log_batch(n=7)
    ice.write_to_buffer(src, test_batch, "batch_orphan_sqlite.parquet")
    _meta.mark_buffers_committed(src["service_id"], ["batch_orphan_sqlite.parquet"])

    # Sanity: snapshot does NOT carry the marker for this basename.
    catalog = ice._get_catalog(src)
    table = catalog.load_table(ice._table_identifier(src))
    markers = buffer_mod._recent_snapshot_markers(table, since_ms=0)
    expected_marker = buffer_mod._buffer_basename_marker("batch_orphan_sqlite.parquet")
    assert expected_marker not in markers, (
        "premise broken: snapshot somehow carries the marker for a file we never committed through commit_buffer."
    )

    # Next commit tick → union semantics tombstone-and-skip on the
    # SQLite-only marker; no re-append, row count stays at baseline.
    result = ice.commit_buffer(src)
    rows_after_recovery = _table_row_count(src)

    assert result["rows_committed"] == 0, (
        f"recovery commit appended {result['rows_committed']} rows when the SQLite row "
        "marked the file as already-committed. The union-recovery contract "
        "(buffer.py:L536) regressed — SQLite-only channel no longer rescues."
    )
    assert rows_after_recovery == baseline_rows, (
        f"row count {baseline_rows} → {rows_after_recovery}: SQLite-only marker "
        "triggered a re-append. Without union semantics, every aged-out snapshot "
        "(>1h lookback) would dup-append on the next tick."
    )

    bufs_after = ice.buffer_files(src)
    assert not any(p.endswith("batch_orphan_sqlite.parquet") for p in bufs_after), (
        f"recovery did not tombstone the SQLite-marked orphan: {bufs_after}"
    )
