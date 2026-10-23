"""Tests for the per-day bundle writer + its backfill driver.

``bundle_days`` collapses per-(field, day) parquets under ``rollups/day/``
into a single bundled parquet at ``rollups/day_bundled/day=D/all_fields.parquet``
with a top-K + __other__ aggregate per field. ``backfill_day_bundles``
walks the per-field day tree to discover candidate days.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from tests._helpers.locks import noop_lock as _noop_lock


def _write_per_field_day(cache_root: str, field: str, day: str, rows: list[tuple[str, int]]) -> str:
    """Write a per-(field, day) parquet at the expected layout."""
    d = os.path.join(cache_root, "rollups", "day", f"field={field}", f"day={day}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([field] * len(rows)),
            "value": pa.array([r[0] for r in rows]),
            "count": pa.array([r[1] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, p)
    return p


def _past_day(days_ago: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def test_bundle_days_writes_combined_parquet(tmp_path):
    """Per-(field, day) parquets get combined into a single
    day_bundled/day=D/all_fields.parquet with field/value/count columns."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day"}

    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 100), ("/b", 50)])
    _write_per_field_day(str(cache_root), "country", day, [("US", 80), ("JP", 20)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = day_bundles.bundle_days("svc-day", src, [day])

    assert n == 1
    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    rows = list(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("url", "/a", 100) in rows
    assert ("url", "/b", 50) in rows
    assert ("country", "US", 80) in rows
    assert ("country", "JP", 20) in rows


def test_bundle_days_top_k_truncation_with_other_synthetic_row(tmp_path):
    """When a (field, day) has more than DAY_BUNDLE_TOP_K values, the
    bundle keeps top-K AND a synthetic ``__other__`` row that sums the
    long tail — so the dashboard's per-field total stays correct."""
    from backend.core.rollups import day_bundles
    from backend.core.rollups._common import DAY_BUNDLE_TOP_K

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-topk"}
    day = _past_day(2)

    # Write DAY_BUNDLE_TOP_K + 50 values with counts (i+1) so
    # the bottom 50 collapse into __other__.
    rows = [(f"v{i}", i + 1) for i in range(DAY_BUNDLE_TOP_K + 50)]
    _write_per_field_day(str(cache_root), "url", day, rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = day_bundles.bundle_days("svc-topk", src, [day])

    assert n == 1
    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    records = t.to_pylist()
    other = [r for r in records if r["value"] == "__other__"]
    assert len(other) == 1, "exactly one __other__ row expected"
    non_other = [r for r in records if r["value"] != "__other__"]
    assert len(non_other) == DAY_BUNDLE_TOP_K, f"top-K must be exactly {DAY_BUNDLE_TOP_K}"
    # __other__ must equal SUM of the bottom 50 counts (1..50 = 1_275).
    assert other[0]["count"] == sum(range(1, 51))


def test_bundle_days_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    """When only one field's per-day parquet is rewritten after the initial
    day bundle swept per-field files, untouched fields in the existing bundle
    must be preserved rather than dropped."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-preserve"}
    day = _past_day(2)

    _write_per_field_day(str(cache_root), "ip", day, [("107.170.164.220", 500)])
    _write_per_field_day(str(cache_root), "url", day, [("/old", 100)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

        # Rewrite ONLY url (ip's per-field file was swept on the first bundle pass)
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/new", 250)])
        future = time.time() + 60
        os.utime(new_p, (future, future))

        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 500) in rows
    assert ("url", "/new", 250) in rows
    assert ("url", "/old", 100) not in rows


def test_bundle_days_skips_active_day(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d")
    _write_per_field_day(str(cache_root), "url", active, [("/x", 1)])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = day_bundles.bundle_days("svc", {"name": "svc"}, [active])

    assert n == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={active}" / "all_fields.parquet").exists()


def test_bundle_days_skips_when_bundle_up_to_date(tmp_path):
    """A bundle older than every source mtime is reused (no rebuild)."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        # Calling again with no source changes → 0 (skipped).
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_rebuilds_when_source_newer_than_bundle(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1

        # Write a new per-field file with future mtime → source > bundle.
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/b", 5)])
        future = time.time() + 60
        os.utime(new_p, (future, future))

        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1


def test_bundle_days_no_per_field_files_skipped(tmp_path):
    """A day with no per-field parquets → nothing to bundle, no entry."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    # Create only the day root, no field subdirs.
    (cache_root / "rollups" / "day").mkdir(parents=True)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_no_day_root_returns_zero(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache_missing"
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_empty_input_returns_zero():
    from backend.core.rollups import day_bundles

    assert day_bundles.bundle_days("svc", {"name": "svc"}, []) == 0


def test_bundle_days_malformed_day_token_skipped(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    (cache_root / "rollups" / "day").mkdir(parents=True)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, ["not-a-day"]) == 0


def test_backfill_day_bundles_discovers_and_caps(tmp_path):
    """backfill walks the per-field-day tree, skips days that already have
    a bundle, respects ``max_days``."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-bf"}

    days = [_past_day(i) for i in range(2, 6)]  # 4 closed days
    for d in days:
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])

    # Pre-seed one bundle so it gets skipped.
    pre_seeded = days[0]
    seeded_dir = cache_root / "rollups" / "day_bundled" / f"day={pre_seeded}"
    seeded_dir.mkdir(parents=True)
    (seeded_dir / "all_fields.parquet").write_bytes(b"present")

    seen: list[list[str]] = []

    def _fake_bundle(_sid, _src, days_in):
        seen.append(list(days_in))
        return len(days_in)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.day_bundles.bundle_days", _fake_bundle),
    ):
        rebuilt = day_bundles.backfill_day_bundles("svc-day-bf", src, max_days=2)

    assert rebuilt == 2
    # Should NOT include the pre-seeded day; should be capped at 2.
    assert len(seen[0]) == 2
    assert pre_seeded not in seen[0]


def test_backfill_day_bundles_no_root_returns_zero(tmp_path):
    from backend.core.rollups import day_bundles

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_backfill_day_bundles_all_present_returns_zero(tmp_path):
    """If every closed day already has a bundle, backfill returns 0
    without invoking the builder."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}

    for i in range(2, 4):
        d = _past_day(i)
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
        bd = cache_root / "rollups" / "day_bundled" / f"day={d}"
        bd.mkdir(parents=True)
        (bd / "all_fields.parquet").write_bytes(b"x")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(
            "backend.core.rollups.day_bundles.bundle_days",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        assert day_bundles.backfill_day_bundles("svc", src) == 0


def test_link_day_bundle_alias_hardlinks_and_leaves_no_tmp(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")

    _link_day_bundle_alias(str(src), str(dst))

    assert os.path.samefile(src, dst)
    assert dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_never_removes_existing_dst_on_failure(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")

    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=OSError("no copy")),
    ):
        _link_day_bundle_alias(str(src), str(dst))

    assert dst.read_bytes() == b"old"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_copy_fallback_is_atomic_replace(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    seen_during_copy: list[bool] = []
    real_copy = __import__("shutil").copyfile

    def spy_copy(s, d):
        seen_during_copy.append(dst.exists())
        return real_copy(s, d)

    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=spy_copy),
    ):
        _link_day_bundle_alias(str(src), str(dst))

    assert seen_during_copy == [True]
    assert dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]
