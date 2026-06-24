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


def _write_per_field_hour_ip_spread(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    """Write a per-(field, hour) IP-spread rollup parquet. Returns path.

    Schema mirrors the live writer's output (see _common.IP_SPREAD_*
    constants + recompute._run_ip_spread_per_field): (value,
    ip_sketch, ip_count_observed, sample_capped) — field+hour come
    from the hive partition path on read."""
    d = os.path.join(cache_root, "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
            "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
            "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
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
    top-K values.

    The replacement per-field write here mirrors production's
    `_run_per_field_copy` behaviour: a touched hour rewrites the per-
    field tree from a full re-scan of base data, so the new write
    contains both the old "/x" row AND the freshly-added "/y" row.
    (Pre-cleanup the test wrote only "/y" and relied on the prior
    bundle's "/x" surviving via the union of bundle + new per-field;
    the per-field-cleanup-after-bundle pass introduced in 2026-06-12
    deletes the per-field tree once bundled, so the rebuild reads only
    what the second per-field write provides — same invariant as
    production.)
    """
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-stale"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

        # Full-replacement per-field write (the production recompute path
        # is non-incremental). Force mtime strictly forward so the
        # freshness check fires regardless of FS timer resolution.
        new_p = _write_per_field_hour(
            str(cache_root),
            "url",
            "2026-05-15-10",
            [{"value": "/x", "count": 1}, {"value": "/y", "count": 2}],
        )
        future = time.time() + 10
        os.utime(new_p, (future, future))

        n = rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

    assert n == 1, f"newer per-field file must trigger rebuild; got n={n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    values = set(t["value"].to_pylist())
    assert "/y" in values, "newly-written per-field row must appear in the rebuilt bundle"
    assert "/x" in values, "previous-bundle row that the per-field rewrite preserved must be present"


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


def test_bundle_hours_cleans_up_per_field_files_after_publish(tmp_path):
    """After a fresh bundle is published, the per-field per-hour parquet
    files that fed into it are redundant — the reader prefers the bundle
    and the writer's recompute path is non-incremental (rewrites all
    per-field for any touched hour from base data). The cleanup pass
    inside bundle_hours sweeps the per-field dirs to keep the file
    count down on the active-day query window."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-cleanup"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
        _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}])

        n = rollups.bundle_hours("svc-cleanup", src, ["2026-05-15-10"])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists()
    # The per-field/hour dirs for the bundled hour should be gone.
    for f in ("url", "country"):
        per_field_hour_dir = cache_root / "rollups" / "hour" / f"field={f}" / "hour=2026-05-15-10"
        assert not per_field_hour_dir.exists(), f"per-field dir {per_field_hour_dir} must be swept after bundling"


def test_bundle_hours_cleanup_dry_run_logs_but_does_not_unlink(tmp_path, caplog):
    """ROLLUP_CLEANUP_DRY_RUN=1 makes the cleanup pass log the file
    count it WOULD delete without actually unlinking — first-deploy
    safety so an operator can confirm the math before flipping it off."""
    import logging

    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-dry-run"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.dict(os.environ, {"ROLLUP_CLEANUP_DRY_RUN": "1"}),
        caplog.at_level(logging.INFO, logger="backend.core.rollups"),
    ):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-dry-run", src, ["2026-05-15-10"])

    # Per-field dir survives the dry run.
    per_field_hour_dir = cache_root / "rollups" / "hour" / "field=url" / "hour=2026-05-15-10"
    assert per_field_hour_dir.exists()
    # And we logged what we would have deleted.
    assert any("ROLLUP_CLEANUP_DRY_RUN" in r.message for r in caplog.records), (
        "dry-run mode must emit a log line naming the file count it would unlink"
    )


# ── bundle_hours_ip_spread ───────────────────────────────────────────────────


def test_bundle_hours_ip_spread_writes_one_parquet_per_hour(tmp_path):
    """Per-(field, hour) IP-spread parquets get combined into a single
    rollups/hour_bundled/hour=H/all_fields_ip.parquet containing rows
    for all fields. Schema: field, value, ip_sketch, ip_count_observed,
    sample_capped. The HLL ``ip_sketch`` BLOB column must pass through
    byte-identical so the reader's deserialize-and-merge step sees the
    same sketches the writer produced."""
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-bundle"}

    # Build two sketches with known input sets so the test can verify
    # byte-for-byte preservation after the bundle round-trip.
    hll_us = HyperLogLog()
    hll_us.update([f"1.1.1.{i}" for i in range(50)])
    hll_jp = HyperLogLog()
    hll_jp.update([f"2.2.2.{i}" for i in range(30)])
    sketch_us = hll_us.to_bytes()
    sketch_jp = hll_jp.to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {
                    "value": "US",
                    "ip_sketch": sketch_us,
                    "ip_count_observed": 50,
                    "sample_capped": False,
                },
                {
                    "value": "JP",
                    "ip_sketch": sketch_jp,
                    "ip_count_observed": 30,
                    "sample_capped": False,
                },
            ],
        )
        # Second field on the same hour — to confirm the bundle merges
        # multiple field dirs into one file.
        hll_ja3 = HyperLogLog()
        hll_ja3.update([f"3.3.3.{i}" for i in range(10)])
        sketch_ja3 = hll_ja3.to_bytes()
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "ja3",
            "2026-05-15-10",
            [
                {
                    "value": "ja3-abc",
                    "ip_sketch": sketch_ja3,
                    "ip_count_observed": 10,
                    "sample_capped": False,
                },
            ],
        )

        from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

        n = bundle_hours_ip_spread("svc-ipsp-bundle", src, ["2026-05-15-10"])

    assert n == 1, f"expected 1 hour bundled; got {n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields_ip.parquet"
    assert bundle.exists(), f"bundled ip_spread file missing at {bundle}"

    t = pq.read_table(str(bundle))
    assert {"field", "value", "ip_sketch", "ip_count_observed", "sample_capped"}.issubset(set(t.column_names)), (
        f"bundle must carry all five columns; got {t.column_names}"
    )

    # Pull rows into a (field, value) -> row dict for assertion clarity.
    by_key: dict[tuple[str, str], dict] = {}
    for row in t.to_pylist():
        by_key[(row["field"], row["value"])] = row
    assert set(by_key.keys()) == {("country", "US"), ("country", "JP"), ("ja3", "ja3-abc")}

    # HLL sketches must pass through byte-identical so the reader's
    # merge produces the same estimates as if we'd added the inputs
    # directly. This is THE invariant the rollup tree relies on.
    assert by_key[("country", "US")]["ip_sketch"] == sketch_us
    assert by_key[("country", "JP")]["ip_sketch"] == sketch_jp
    assert by_key[("ja3", "ja3-abc")]["ip_sketch"] == sketch_ja3
    assert by_key[("country", "US")]["ip_count_observed"] == 50
    assert by_key[("country", "US")]["sample_capped"] is False

    # And the merged reader-side estimate must be close to the true count.
    # (For a single-hour bundle, the merge is a no-op — verify the
    # sketch deserializes and gives a sensible cardinality estimate.)
    restored = HyperLogLog.from_bytes(by_key[("country", "US")]["ip_sketch"])
    assert abs(restored.count() - 50) <= 10  # generous bound at p=8 small-range


def test_bundle_hours_ip_spread_skips_active_hour(tmp_path):
    """Active hour must be skipped — same race-with-writer reason the
    count bundler skips it."""
    import datetime as _dt

    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-active"}
    active_hour = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d-%H")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            active_hour,
            [
                {
                    "value": "US",
                    "ip_sketch": HyperLogLog().to_bytes(),
                    "ip_count_observed": 0,
                    "sample_capped": False,
                },
            ],
        )
        n = bundle_hours_ip_spread("svc-ipsp-active", src, [active_hour])

    assert n == 0
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={active_hour}" / "all_fields_ip.parquet"
    assert not bundle.exists()


def test_bundle_hours_ip_spread_skips_when_bundle_is_up_to_date(tmp_path):
    """If the bundle file is newer than every per-field source, the
    bundler must skip rebuilding (cheap mtime check, expensive COPY
    avoided). Mirrors bundle_hours' fast-path."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-fresh"}
    hour = "2026-05-15-10"
    sketch = HyperLogLog().to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            hour,
            [{"value": "US", "ip_sketch": sketch, "ip_count_observed": 0, "sample_capped": False}],
        )
        # First call produces the bundle.
        n_first = bundle_hours_ip_spread("svc-ipsp-fresh", src, [hour])
        # Touch the bundle to make it strictly newer than the per-field source.
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour}" / "all_fields_ip.parquet"
        time.sleep(0.05)
        os.utime(bundle, None)
        # Second call should NOT rebuild — the bundle is up-to-date.
        n_second = bundle_hours_ip_spread("svc-ipsp-fresh", src, [hour])

    assert n_first == 1
    assert n_second == 0


def test_bundle_hours_ip_spread_returns_zero_when_no_per_field_files(tmp_path):
    """An hour with NO per-field IP-spread parquets yields a 0 return
    and no bundle file — handles the case where ip_spread writer
    hasn't run yet for any field on that hour."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-empty"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = bundle_hours_ip_spread("svc-ipsp-empty", src, ["2026-05-15-10"])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10").exists()


def test_bundle_hours_ip_spread_returns_zero_when_root_missing(tmp_path):
    """When the ip_spread root dir doesn't exist at all (cold pool, no
    writer tick has run yet), the bundler returns 0 immediately
    without raising an OSError on the listdir."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-cold"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = bundle_hours_ip_spread("svc-ipsp-cold", src, ["2026-05-15-10"])

    assert n == 0
