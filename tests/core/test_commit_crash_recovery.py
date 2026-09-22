"""Tests for rollup atomic publish behavior and crash recovery.

Pins the rollup writer's atomic tmp+rename: a crash mid-publish must
never leave a half-written bundle the reader will trust.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import duckdb

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
