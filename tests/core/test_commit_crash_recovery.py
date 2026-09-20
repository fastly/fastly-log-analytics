"""Crash-injection tests for the commit half + rollup atomic publish
(Deliverable 2, part 2).

``commit_buffer`` has TWO independent durable channels proving "this buffer
was already appended", so a crash anywhere in the
``table.append → mark_buffers_committed → tombstone_buffer_files`` window
can't double-append on the next tick:

  * SQLite ``committed_buffers`` (fast path) — written between append and
    tombstone.
  * Iceberg snapshot-summary markers (``app.buffer_commit_marker.*``) —
    written *inside* ``table.append`` itself, so they survive even if the
    SQLite write never happened.

The existing ``test_buffer_commit_idempotent`` / ``_double_checkpoint`` tests
exercise the metadata helpers and the marker parsing in isolation. These
tests crash the REAL ``commit_buffer`` against a real PyIceberg table at
each window and assert the table ends with the rows appended exactly once.

Also pins the rollup writer's atomic tmp+rename: a crash mid-publish must
never leave a half-written bundle the reader will trust.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from backend.core import iceberg as ice
from backend.repositories._base import _safe_table

# ── Harness: real PyIceberg on a local-FS warehouse ─────────────────────────


@pytest.fixture
def commit_env(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="commit_crash_")
    warehouse = os.path.join(tmpdir, "warehouse")
    cache = os.path.join(tmpdir, "cache")
    os.makedirs(warehouse, exist_ok=True)
    os.makedirs(cache, exist_ok=True)

    src = {
        "name": "commit_crash_svc",
        "service_id": "commit-crash-id",
        "service_name": "Commit Crash",
        "bucket": "cc-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "duckdb_path": os.path.join(tmpdir, "cc.duckdb"),
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse}")
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    for c in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        c.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    ice.init_iceberg_table(src)
    yield {"src": src, "warehouse": warehouse, "cache": cache, "tmpdir": tmpdir}

    shutil.rmtree(tmpdir, ignore_errors=True)
    for c in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        c.clear()


def _batch(n: int, tag: str) -> pa.Table:
    base = datetime.now(UTC) - timedelta(hours=2)
    return pa.table(
        {
            "timestamp": pa.array([base + timedelta(seconds=i) for i in range(n)], type=pa.timestamp("us", tz="UTC")),
            "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
            "status": pa.array([200] * n, type=pa.uint16()),
            "url": pa.array([f"/{tag}/{i}" for i in range(n)]),
        }
    )


def _count_via_view(env) -> int:
    """Row count through the production read path (sync warehouse→cache,
    build the DuckDB view, COUNT)."""
    import glob

    data_dir = os.path.join(env["cache"], "data")
    os.makedirs(data_dir, exist_ok=True)
    for p in glob.glob(os.path.join(env["warehouse"], "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(p))
        if not os.path.exists(dst):
            shutil.copy2(p, dst)
    con = duckdb.connect(":memory:")
    try:
        ice.update_iceberg_view(con, env["src"])
        return con.execute(f"SELECT COUNT(*) FROM {_safe_table(env['src']['name'])}").fetchone()[0]
    finally:
        con.close()


class _Boom(BaseException):
    """BaseException so it slips past ``commit_buffer``'s ``except Exception``
    guards — i.e. it models a real process death, not a handled error."""


def _raise_once(real, exc):
    """Return a callable that raises ``exc`` on its first call, then delegates
    to ``real``. Lets the crash patch self-disable so the restart tick runs
    healthy WITHOUT ``monkeypatch.undo()`` (which, sharing the function-scoped
    monkeypatch, would also revert the autouse metadata-isolation fixture)."""
    state = {"n": 0}

    def _f(*a, **kw):
        state["n"] += 1
        if state["n"] == 1:
            raise exc
        return real(*a, **kw)

    return _f


# ── Crash between table.append and mark_buffers_committed ────────────────────


@pytest.mark.skip(reason="Migrated to ducklake")
def test_crash_after_append_before_mark_iceberg_marker_prevents_double(commit_env, monkeypatch):
    """The SQLite checkpoint never lands, but the Iceberg snapshot marker
    (written inside table.append) must let the next commit tick recognise the
    buffer as already-appended and skip the re-append. If the marker channel
    doesn't round-trip, this DOUBLE-COUNTS — a silent 2× row duplication."""
    from backend.core import metadata as _meta_mark

    src = commit_env["src"]
    ice.write_to_buffer(src, _batch(6, "a"), "batch_marker_recovery.parquet")

    # Crash the moment after append returns, before the SQLite checkpoint.
    # One-shot: the restart tick's mark (if any) delegates to the real impl.
    monkeypatch.setattr(
        "backend.core.metadata.mark_buffers_committed",
        _raise_once(_meta_mark.mark_buffers_committed, _Boom("crash after append, before mark")),
    )
    with pytest.raises(_Boom):
        ice.commit_buffer(src)

    # Append landed (rows in Iceberg) but the buffer is still active (no
    # tombstone) and there's no committed_buffers row.
    from backend.core import metadata as _meta

    assert _meta.list_committed_basenames(src["service_id"], ["batch_marker_recovery.parquet"]) == set()
    assert ice.buffer_files(src), "buffer was tombstoned despite the crash before tombstone"
    # NB: a view count here would read iceberg(6) + the still-active buffer(6)
    # = 12 transiently — correct pre-recovery. The dedup is the restart's job;
    # assert the post-recovery count instead.

    # Restart: recovery must skip the re-append via the Iceberg marker (the
    # crash patch is one-shot, so mark is healthy now — no undo needed).
    result = ice.commit_buffer(src)
    assert result["rows_committed"] == 0, (
        "buffer was re-appended after a crash before the SQLite checkpoint — "
        "the Iceberg snapshot-marker recovery channel failed, producing duplicate rows"
    )
    assert _count_via_view(commit_env) == 6, "row count doubled — commit-recovery did not dedupe"


# ── Crash between mark_buffers_committed and tombstone ───────────────────────


@pytest.mark.skip(reason="Migrated to ducklake")
def test_crash_after_mark_before_tombstone_sqlite_prevents_double(commit_env, monkeypatch):
    """committed_buffers row lands, tombstone never runs. The next tick's
    SQLite recovery must tombstone-and-skip rather than re-append."""
    from backend.core.iceberg import buffer as _buf

    src = commit_env["src"]
    ice.write_to_buffer(src, _batch(7, "b"), "batch_sqlite_recovery.parquet")

    # One-shot: the loop tombstone (tick 1) crashes; the restart's recovery
    # tombstone delegates to the real impl.
    monkeypatch.setattr(
        "backend.core.iceberg.buffer.tombstone_buffer_files",
        _raise_once(_buf.tombstone_buffer_files, _Boom("crash after mark, before tombstone")),
    )
    with pytest.raises(_Boom):
        ice.commit_buffer(src)

    from backend.core import metadata as _meta

    assert _meta.list_committed_basenames(src["service_id"], ["batch_sqlite_recovery.parquet"]) == {
        "batch_sqlite_recovery.parquet"
    }
    assert ice.buffer_files(src), "buffer tombstoned despite crash before tombstone"
    # (view here = iceberg(7) + active buffer(7) = 14 transiently; assert post-recovery)

    # Restart: SQLite recovery tombstones-and-skips (one-shot patch is spent).
    result = ice.commit_buffer(src)
    assert result["rows_committed"] == 0, "re-append after a crash before tombstone — SQLite recovery failed"
    assert _count_via_view(commit_env) == 7, "row count changed — commit-recovery double-appended"


# ── Two clean commits never lose or duplicate (control) ─────────────────────


@pytest.mark.skip(reason="Migrated to ducklake")
def test_two_clean_commits_append_exactly_once_each(commit_env):
    src = commit_env["src"]
    ice.write_to_buffer(src, _batch(5, "c1"), "batch_clean_1.parquet")
    assert ice.commit_buffer(src)["rows_committed"] == 5
    ice.write_to_buffer(src, _batch(4, "c2"), "batch_clean_2.parquet")
    assert ice.commit_buffer(src)["rows_committed"] == 4
    assert _count_via_view(commit_env) == 9


# ── Self-heal: commit creates a missing table instead of crashing forever ────


@pytest.fixture
def commit_env_no_table(monkeypatch):
    """Same harness as ``commit_env`` but WITHOUT pre-creating the Iceberg
    table — models a fresh service whose provision-time table creation was
    skipped or failed. ``commit_buffer`` must self-heal by creating it."""
    tmpdir = tempfile.mkdtemp(prefix="commit_no_table_")
    warehouse = os.path.join(tmpdir, "warehouse")
    cache = os.path.join(tmpdir, "cache")
    os.makedirs(warehouse, exist_ok=True)
    os.makedirs(cache, exist_ok=True)

    src = {
        "name": "commit_no_table_svc",
        "service_id": "commit-no-table-id",
        "service_name": "Commit No Table",
        "bucket": "cnt-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "duckdb_path": os.path.join(tmpdir, "cc.duckdb"),
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse}")
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    for c in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        c.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # NOTE: deliberately NOT calling ice.init_iceberg_table(src) — the table
    # must not exist when commit_buffer runs.
    yield {"src": src, "warehouse": warehouse, "cache": cache, "tmpdir": tmpdir}

    shutil.rmtree(tmpdir, ignore_errors=True)
    for c in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        c.clear()


@pytest.mark.skip(reason="Migrated to ducklake")
def test_commit_self_heals_missing_table(commit_env_no_table):
    """A fresh service whose Iceberg table was never created (provision-time
    init skipped/failed) must not crash the commit cron forever. The first
    ``commit_buffer`` must CREATE the table and commit the buffered rows.

    Regression: the create=True fallback in commit_buffer was unreachable —
    the preceding ``_init_iceberg_table_locked(create=False)`` raised
    ``NoSuchTableError`` before it, so every commit tick crashed and buffered
    data never reached durable storage."""
    from pyiceberg.exceptions import NoSuchTableError

    src = commit_env_no_table["src"]

    # Sanity: the table genuinely does not exist yet.
    with pytest.raises(NoSuchTableError):
        ice.init_iceberg_table(src, create=False)

    # First-ever commit for the service: must create the table + commit rows.
    ice.write_to_buffer(src, _batch(8, "heal"), "batch_self_heal.parquet")
    result = ice.commit_buffer(src)

    assert result["rows_committed"] == 8, "commit_buffer did not create+commit against a missing table"
    # Table now exists and the rows are durably queryable via the read path.
    assert ice.init_iceberg_table(src, create=False) is not None
    assert _count_via_view(commit_env_no_table) == 8


# ── Rollup atomic publish: a crash mid-rename leaves no trusted half-bundle ──


def _seed_ts_table(con, table, hour_dt):
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    for k in range(5):
        con.execute(f"INSERT INTO {table} VALUES (?, 200, 'HIT', 100, 0.05)", [hour_dt + timedelta(minutes=k)])


def test_rollup_copy_failure_leaves_no_partial_bundle(tmp_path, monkeypatch):
    """If the COPY that writes the tmp parquet fails, the writer must remove
    the tmp and publish NOTHING — never a half-written ``time_series.parquet``
    that the reader would then trust as a complete hour."""
    from contextlib import contextmanager
    from unittest.mock import patch

    from backend.core.rollups import time_series

    @contextmanager
    def _noop(_k):
        yield

    src = {"name": "atomic-svc", "service_id": "atomic-id", "_cache_dir_override": str(tmp_path)}
    hour_dt = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
    hour = hour_dt.strftime("%Y-%m-%d-%H")
    con = duckdb.connect(":memory:")
    _seed_ts_table(con, "logs", hour_dt)

    class _CopyFailingCon:
        """Proxy whose ``execute`` raises on the COPY (tmp-parquet write) but
        delegates everything else — DuckDB connection objects don't allow
        attribute reassignment, hence the proxy. ``close()`` is a no-op so the
        writer's ``finally: con.close()`` doesn't orphan our table."""

        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **kw):
            if sql.strip().upper().startswith("COPY"):
                raise duckdb.IOException("crash: disk full writing tmp parquet")
            return self._real.execute(sql, *a, **kw)

        def close(self):
            pass

        def __getattr__(self, name):
            return getattr(self._real, name)

    proxy = _CopyFailingCon(con)

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value="logs"),
        patch("backend.core.duckdb.get_connection", return_value=proxy),
        patch("backend.core.iceberg.view._get_service_lock", _noop),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        n = time_series.build_time_series_bundles("atomic-id", src, [hour])

    bundle_dir = tmp_path / "rollups" / "hour_bundled" / f"hour={hour}"
    assert n == 0, "writer reported a publish despite the COPY failing"
    # No published bundle, and no orphan .tmp left behind.
    assert not (bundle_dir / "time_series.parquet").exists(), "half-written bundle published after COPY failure"
    if bundle_dir.exists():
        leftover = [p for p in os.listdir(bundle_dir) if p.startswith(".tmp")]
        assert leftover == [], f"orphan tmp parquet left after COPY failure: {leftover}"


def test_reader_ignores_stray_tmp_bundle(tmp_path):
    """A crash between tmp-write and os.replace can leave a ``.tmp_ts_*.parquet``
    in the bundle dir. The reader keys on the exact ``time_series.parquet``
    name, so a stray tmp must be invisible — never read as data."""
    from backend.core.rollups import TIME_SERIES_BUNDLE_FILENAME, _hour_bundled_root
    from backend.repositories._base import collect_hourly_bundle_paths

    src = {"name": "stray-svc", "service_id": "stray-id", "_cache_dir_override": str(tmp_path)}
    hour_dt = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
    hour = hour_dt.strftime("%Y-%m-%d-%H")
    bundled_root = _hour_bundled_root(src)
    hour_dir = os.path.join(bundled_root, f"hour={hour}")
    os.makedirs(hour_dir, exist_ok=True)

    # A valid bundle + a stray half-written tmp sibling.
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT TIMESTAMPTZ '{hour_dt.isoformat()}' AS bucket, 5 AS requests, 0 AS status_4xx, "
            f"0 AS status_5xx, 0 AS hits, 0 AS cache_total, 0 AS resp_bytes_sum, 0.0 AS ttfb_sum, 0 AS ttfb_count) "
            f"TO '{os.path.join(hour_dir, TIME_SERIES_BUNDLE_FILENAME)}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    # Garbage tmp file (not even valid parquet) — must be ignored.
    with open(os.path.join(hour_dir, ".tmp_ts_deadbeef.parquet"), "wb") as f:
        f.write(b"not a parquet file")

    result = collect_hourly_bundle_paths(
        src, hour_dt, hour_dt + timedelta(hours=1), bundled_root, TIME_SERIES_BUNDLE_FILENAME
    )
    assert result is not None
    paths, _crosses = result
    assert len(paths) == 1, f"reader picked up a stray/tmp file: {paths}"
    assert paths[0].endswith(TIME_SERIES_BUNDLE_FILENAME)
