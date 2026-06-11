"""Tests for the hour-bundling layer.

Hour bundling collapses per-(field, hour) parquets into a single
per-hour parquet at ``rollups/hour_bundled/hour=H/all_fields.parquet``,
cutting parquet file-opens on a 24h dashboard query from ~984 to ~24.
The reader prefers the bundled file and falls back to per-field
parquets when a bundle is missing — so the bundling roll-out is
non-destructive and zero-risk on the read path.
"""

from __future__ import annotations

import os
import time
import uuid
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


def _write_per_field_hour(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    """Write a per-(field, hour) rollup parquet. Returns path."""
    d = os.path.join(cache_root, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    # PER-FIELD layout stores only (value, count) — field and hour come from
    # the hive path. Mirror that here so we test against the real layout.
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def test_bundle_hours_writes_one_parquet_per_hour(tmp_path):
    """Per-(field, hour) parquets get combined into a single
    rollups/hour_bundled/hour=H/all_fields.parquet containing rows for
    all fields. Schema: field, value, count."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-1"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Hour 2026-05-15-10: two fields, multiple rows each.
        _write_per_field_hour(
            str(cache_root),
            "url",
            "2026-05-15-10",
            [
                {"value": "/login", "count": 100},
                {"value": "/api", "count": 75},
            ],
        )
        _write_per_field_hour(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {"value": "US", "count": 80},
                {"value": "JP", "count": 20},
            ],
        )

        n = rollups.bundle_hours("svc-bundle-1", src, ["2026-05-15-10"])

    assert n == 1, f"expected 1 hour bundled; got {n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists(), f"bundled file missing at {bundle}"

    t = pq.read_table(str(bundle))
    # Bundle MUST include field/value/count. DuckDB's COPY may also
    # preserve the hour hive-partition value as an extra column — that's
    # benign (the reader projects only field/value/count via the explicit
    # SELECT list in execute_top_n_rollups).
    assert {"field", "value", "count"}.issubset(set(t.column_names)), (
        f"bundled parquet must carry field+value+count columns; got {t.column_names}"
    )
    rows = list(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("url", "/login", 100) in rows
    assert ("url", "/api", 75) in rows
    assert ("country", "US", 80) in rows
    assert ("country", "JP", 20) in rows


def test_bundle_hours_skips_active_hour(tmp_path):
    """Active (current UTC) hour must not be bundled — its per-field
    parquets are still being written by the post-sync rebuild and
    bundling would race them. The dashboard reader serves the active
    hour live anyway."""
    from datetime import UTC, datetime

    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-active"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", active, [{"value": "/x", "count": 1}])
        n = rollups.bundle_hours("svc-bundle-active", src, [active])

    assert n == 0, "active hour must be skipped"
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={active}" / "all_fields.parquet"
    assert not bundle.exists()


def test_bundle_hours_skips_when_bundle_is_up_to_date(tmp_path):
    """Re-running bundle_hours with no changes to source files must skip
    the rebuild. Without the mtime guard the post-sync hook would
    re-bundle every closed hour on every sync tick — wasted I/O."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-skip"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        n1 = rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"])
        assert n1 == 1

        bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
        mtime_first = os.path.getmtime(bundle)

        # Re-run with no source changes. Bundle must NOT be rebuilt
        # (mtime would jump if it were).
        n2 = rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"])
        assert n2 == 0, f"second run with no source changes should rebuild 0; got {n2}"
        assert os.path.getmtime(bundle) == mtime_first


def test_bundle_hours_rebuilds_when_source_files_newer(tmp_path):
    """If a per-field file is newer than the bundle, the bundle MUST be
    rebuilt — otherwise the bundle would miss a sync's worth of new
    top-K values."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-stale"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

        # Write a NEW per-field parquet for the SAME (field, hour) — must
        # have a strictly newer mtime than the bundle so bundle_hours'
        # freshness check (bundle_mtime >= max_src_mtime) re-runs. Force
        # mtime forward explicitly rather than sleeping past the FS timer.
        new_p = _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/y", "count": 2}])
        future = time.time() + 10
        os.utime(new_p, (future, future))

        n = rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

    assert n == 1, f"newer per-field file must trigger rebuild; got n={n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    values = set(t["value"].to_pylist())
    assert "/y" in values, "newly-written per-field row must appear in the rebuilt bundle"


def test_reader_uses_bundle_when_available_skipping_per_field_files(tmp_path):
    """When a bundled file exists for an hour, the reader's enumeration
    must skip the per-field parquets for that hour (since the bundle
    already covers them) — otherwise data is double-counted."""
    from backend.core import rollups
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-prefer-bundle", "bucket": "b", "prefix": "p"}

    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 50}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-prefer-bundle", src, ["2026-05-15-10"])

        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            # Window includes the bundled hour. should_query_live=False
            # because end_time is well before the active hour.
            rows, _ = runner.execute_top_n_rollups(
                ["url", "country"],
                "2026-05-15T10:00:00",
                "2026-05-15T11:00:00",
                limit=10,
            )

    by_field: dict[str, list[tuple]] = {}
    for f, v, c in rows:
        by_field.setdefault(f, []).append((v, c))
    # Each value must appear EXACTLY ONCE (count == 100/50). If the
    # reader read both bundle AND per-field files, we'd see 200/100.
    assert by_field.get("url") == [("/login", 100)], (
        f"url count must be 100 (single source); got {by_field.get('url')}. Double-count bug?"
    )
    assert by_field.get("country") == [("US", 50)], (
        f"country count must be 50 (single source); got {by_field.get('country')}. Double-count bug?"
    )


def test_reader_falls_back_to_per_field_when_bundle_missing(tmp_path):
    """When NO bundled file exists for an hour (cron hasn't run yet, or
    bundling failed), the reader must fall back to per-field files for
    that hour. Otherwise data for unbundled hours silently disappears."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-fallback", "bucket": "b", "prefix": "p"}

    # Per-field file exists, but NO bundle.
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            rows, _ = runner.execute_top_n_rollups(
                ["url"],
                "2026-05-15T10:00:00",
                "2026-05-15T11:00:00",
                limit=10,
            )

    assert rows == [("url", "/login", 100)], f"reader must fall back to per-field when bundle missing; got {rows}"


def test_reader_mixed_bundled_and_per_field_hours(tmp_path):
    """A query window spanning multiple hours where SOME are bundled
    and others aren't (newly-built bundle backlog) must return the
    correct unioned counts."""
    from backend.core import rollups
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-mixed", "bucket": "b", "prefix": "p"}

    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-11", [{"value": "/login", "count": 50}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Bundle ONLY hour 10. Hour 11 stays per-field.
        rollups.bundle_hours("svc-read-mixed", src, ["2026-05-15-10"])

        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            rows, _ = runner.execute_top_n_rollups(
                ["url"],
                "2026-05-15T10:00:00",
                "2026-05-15T12:00:00",
                limit=10,
            )

    # Hour 10 = 100, hour 11 = 50 → total 150. If reader double-counted
    # the bundled hour by also reading per-field, we'd see 250.
    assert rows == [("url", "/login", 150)], f"mixed bundled+per-field union must sum correctly; got {rows}"


def test_backfill_hour_bundles_processes_all_closed_hours(tmp_path):
    """backfill_hour_bundles enumerates the per-field tree and bundles
    every closed hour that doesn't have an up-to-date bundle. Pinned
    because this drives the one-shot migration that delivers the cold-
    path win on existing data."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-backfill"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Three closed hours across two days.
        for h in ["2026-05-15-10", "2026-05-15-11", "2026-05-16-09"]:
            _write_per_field_hour(str(cache_root), "url", h, [{"value": "/x", "count": 1}])
            _write_per_field_hour(str(cache_root), "country", h, [{"value": "US", "count": 1}])

        n = rollups.backfill_hour_bundles("svc-backfill", src)

    assert n == 3, f"expected 3 hour bundles built; got {n}"
    for h in ["2026-05-15-10", "2026-05-15-11", "2026-05-16-09"]:
        assert (cache_root / "rollups" / "hour_bundled" / f"hour={h}" / "all_fields.parquet").exists()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Second call is a no-op — all bundles already exist and are fresh.
        n2 = rollups.backfill_hour_bundles("svc-backfill", src)
    assert n2 == 0, "re-running backfill with no source changes must be a no-op"
