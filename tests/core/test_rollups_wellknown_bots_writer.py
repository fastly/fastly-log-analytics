"""Tests for the writer half of the wellknown_bots rollup.

The reader-side tests live in ``test_rollups_wellknown_bots.py``; this
file covers :func:`recompute_wellknown_bots_rollup`.

The writer reads each CLOSED hour's committed Iceberg data partition
directly — ``cache/<svc>/data/timestamp_hour=<H>/*.parquet`` — via a
private ``:memory:`` DuckDB connection, deliberately bypassing the
per-service iceberg *view* (which UNIONs the churning ``buffer/``
parquets and was the source of the 2026-06-27 backfill stall). So the
fixtures here seed real on-disk hive partitions rather than patching a
connection.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _write_data_partition(
    cache_root: str,
    hour: str,
    hour_dt: datetime,
    rows: list[tuple[str, str]],
    *,
    with_ua: bool = True,
) -> str:
    """Seed a committed Iceberg data partition at
    ``cache/<svc>/data/timestamp_hour=<hour>/batch_*.parquet`` with
    ``(timestamp, ua, ip)`` rows (drop ``ua`` when ``with_ua`` is False).
    Returns the parquet path."""
    d = os.path.join(cache_root, "data", f"timestamp_hour={hour}")
    os.makedirs(d, exist_ok=True)
    cols: dict[str, pa.Array] = {
        "timestamp": pa.array([hour_dt + timedelta(minutes=3) for _ in rows], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array([ip for _, ip in rows]),
    }
    if with_ua:
        cols["ua"] = pa.array([ua for ua, _ in rows])
    p = os.path.join(d, f"batch_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(pa.table(cols), p)
    return p


def test_recompute_no_pattern_set_version_returns_zero(tmp_path):
    """No source files cached → no version → return 0 without writing."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=""),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


def test_recompute_no_bot_pattern_returns_zero(tmp_path):
    """Version present but the regex compiler returned empty → bail
    cleanly (the pattern compiler caches across calls; an empty result
    means the cache is in an indeterminate state)."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


def test_recompute_empty_hours_input_returns_zero():
    """No hours → fast return without touching anything else."""
    from backend.core.rollups import wellknown_bots

    assert wellknown_bots.recompute_wellknown_bots_rollup("svc", {"name": "svc"}, []) == 0


def test_recompute_active_hour_filtered_out(tmp_path):
    """Active UTC hour is dropped from the parsed list — its data is
    still in flight (buffer) and the reader serves it live — even when a
    committed partition already exists on disk for it."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    _write_data_partition(str(cache_root), active, active_dt, [("Googlebot/2.1", "66.249.66.1")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [active])

    assert n == 0
    assert not (cache_root / "rollups" / "wellknown_bots").exists()


def test_recompute_malformed_hour_skipped(tmp_path):
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, ["nope"])

    assert n == 0


def test_recompute_no_data_partition_returns_zero(tmp_path):
    """A closed hour with no committed data partition on disk is skipped
    (genuinely empty or not yet flushed) — the reader's coverage floor +
    live fallback covers it."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)  # no partition seeded

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0
    assert not (cache_root / "rollups" / "wellknown_bots").exists()


def test_recompute_table_missing_ua_or_ip_returns_zero(tmp_path):
    """Service whose schema lacks ua → can't materialize bots, skip
    cleanly without writing partial / wrong-shape files."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ua"}
    h, hour_dt = _past_hour(2)
    # Partition exists but carries only (timestamp, ip).
    _write_data_partition(str(cache_root), h, hour_dt, [("", "66.249.66.1")], with_ua=False)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-no-ua", src, [h])

    assert n == 0
    # Critical: no parquet was written.
    assert not (cache_root / "rollups" / "wellknown_bots").exists() or not any(
        (cache_root / "rollups" / "wellknown_bots").rglob("compacted_*.parquet")
    )


def test_recompute_writes_filtered_rows_to_partition(tmp_path):
    """Happy path: a closed hour with bot-UA traffic in its committed
    data partition produces a ``rollups/wellknown_bots/hour=H/
    compacted_*.parquet`` with the expected (ua, ip, count, version)."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-write"}
    h, hour_dt = _past_hour(2)
    _write_data_partition(
        str(cache_root),
        h,
        hour_dt,
        [("Googlebot/2.1", "66.249.66.1"), ("Googlebot/2.1", "66.249.66.1"), ("curl/8.0", "10.0.0.9")],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v42"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="Googlebot"),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-write", src, [h])

    # One hour processed → one partition published.
    assert n == 1
    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    assert hour_dir.exists()
    parquets = list(hour_dir.glob("compacted_*.parquet"))
    assert len(parquets) == 1, f"expected one compacted parquet; got {parquets}"
    # Tmp files are renamed away cleanly.
    assert not list(hour_dir.glob(".tmp_*.parquet"))

    # Content: only the Googlebot rows survive the regex; the two
    # identical (ua, ip) rows collapse to one row with count=2; version
    # is stamped.
    tbl = pq.read_table(parquets[0]).to_pydict()
    assert tbl["ua"] == ["Googlebot/2.1"]
    assert tbl["ip"] == ["66.249.66.1"]
    assert tbl["request_count"] == [2]
    assert tbl["pattern_set_version"] == ["v42"]


def test_recompute_sweeps_stale_parquets_in_hour_dir(tmp_path):
    """Before publishing the fresh tmp parquet, the writer must sweep any
    pre-existing parquets in the rollup hour dir — otherwise a reader
    enumerating the dir could see a stale-version row alongside the fresh
    one for the same hour."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-sweep"}
    h, hour_dt = _past_hour(2)

    # Pre-seed a stale rollup parquet at the canonical location.
    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    hour_dir.mkdir(parents=True)
    stale = hour_dir / "compacted_stale.parquet"
    stale.write_bytes(b"stale content")

    _write_data_partition(str(cache_root), h, hour_dt, [("Googlebot/2.1", "66.249.66.1")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v_fresh"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-sweep", src, [h])

    assert n == 1
    # Stale gone; exactly one fresh compacted_ parquet present.
    assert not stale.exists()
    fresh = list(hour_dir.glob("compacted_*.parquet"))
    assert len(fresh) == 1


def test_recompute_copy_failure_skips_hour_without_raising(tmp_path):
    """A COPY failure for one hour leaves the rest of the hours untouched
    and returns the count successfully written. The bad hour's data
    partition holds a non-parquet file so its ``read_parquet`` raises,
    while the good (first) hour — used for the column probe — succeeds."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-fail"}
    h_ok, hour_ok_dt = _past_hour(2)
    h_bad, _ = _past_hour(3)

    _write_data_partition(str(cache_root), h_ok, hour_ok_dt, [("Googlebot/2.1", "66.249.66.1")])
    # Bad hour: a file ending .parquet so it counts as "has parquets",
    # but its bytes aren't a valid parquet → read_parquet raises.
    bad_part = cache_root / "data" / f"timestamp_hour={h_bad}"
    bad_part.mkdir(parents=True)
    (bad_part / "batch_corrupt.parquet").write_bytes(b"not a parquet")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-fail", src, [h_ok, h_bad])

    # Only the good hour succeeded.
    assert n == 1
    assert (cache_root / "rollups" / "wellknown_bots" / f"hour={h_ok}").exists()
    bad_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h_bad}"
    # Dir got created (mkdirs above the COPY) but no compacted parquet.
    if bad_dir.exists():
        assert not list(bad_dir.glob("compacted_*.parquet"))
