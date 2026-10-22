"""Tests for the writer half of the wellknown_bots rollup.

The reader-side tests live in ``test_rollups_wellknown_bots.py``; this
file covers :func:`recompute_wellknown_bots_rollup`.

The writer reads each CLOSED hour through the per-service session view (the
same source every other rollup writer uses), so the fixtures here seed an
in-memory DuckDB table and patch the view resolution instead of writing
on-disk hive partitions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.locks import noop_lock as _noop_lock

pytestmark = pytest.mark.usefixtures("frozen_clock")


def _seed_con(hour_dt: datetime, rows: list[tuple[str, str]], *, with_ua: bool = True) -> duckdb.DuckDBPyConnection:
    """In-memory ``logs_wk`` table with ``(timestamp, ip[, ua])`` rows inside ``hour_dt``."""
    con = duckdb.connect(":memory:")
    ua_col = ", ua VARCHAR" if with_ua else ""
    con.execute(f"CREATE TABLE logs_wk (timestamp TIMESTAMPTZ, ip VARCHAR{ua_col})")
    for ua, ip in rows:
        ts = hour_dt + timedelta(minutes=3)
        if with_ua:
            con.execute("INSERT INTO logs_wk VALUES (?, ?, ?)", [ts, ip, ua])
        else:
            con.execute("INSERT INTO logs_wk VALUES (?, ?)", [ts, ip])
    return con


def _view_patches(cache_root, con):
    """Patch cache dir, view resolution, the read-only connection and the retry hop."""
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.wellknown_bots._safe_table_for", return_value="logs_wk"),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _src, fn: fn(c)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    )


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
    still in flight (buffer) and the reader serves it live — even when
    the view already holds rows for it."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    con = _seed_con(active_dt, [("Googlebot/2.1", "66.249.66.1")])

    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
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


def test_recompute_hour_without_matching_rows_writes_empty_partition(tmp_path):
    """A closed hour with no bot traffic still publishes a zero-row partition:
    it is covered-and-empty, so the reader doesn't treat the hour as a gap."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(hour_dt, [("curl/8.0", "10.0.0.9")])

    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 1
    files = list((cache_root / "rollups" / "wellknown_bots" / f"hour={h}").glob("compacted_*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).num_rows == 0


def test_recompute_table_missing_ua_or_ip_returns_zero(tmp_path):
    """Service whose schema lacks ua → can't materialize bots, skip
    cleanly without writing partial / wrong-shape files."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ua"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(hour_dt, [("", "66.249.66.1")], with_ua=False)

    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
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
    """Happy path: a closed hour with bot-UA traffic produces a
    ``rollups/wellknown_bots/hour=H/compacted_*.parquet`` with the expected
    (ua, ip, count, version)."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-write"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(
        hour_dt,
        [("Googlebot/2.1", "66.249.66.1"), ("Googlebot/2.1", "66.249.66.1"), ("curl/8.0", "10.0.0.9")],
    )

    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v42"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-write", src, [h])

    assert n == 1
    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    assert hour_dir.exists()
    parquets = list(hour_dir.glob("compacted_*.parquet"))
    assert len(parquets) == 1, f"expected one compacted parquet; got {parquets}"
    assert not list(hour_dir.glob(".tmp_*.parquet"))

    # Only the Googlebot rows survive the regex; the two identical (ua, ip)
    # rows collapse to one row with count=2; version is stamped.
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

    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    hour_dir.mkdir(parents=True)
    stale = hour_dir / "compacted_stale.parquet"
    stale.write_bytes(b"stale content")

    con = _seed_con(hour_dt, [("Googlebot/2.1", "66.249.66.1")])
    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v_fresh"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-sweep", src, [h])

    assert n == 1
    assert not stale.exists()
    assert len(list(hour_dir.glob("compacted_*.parquet"))) == 1


def test_recompute_copy_failure_skips_hour_without_raising(tmp_path):
    """A COPY failure for one hour leaves the rest untouched and returns the
    count successfully written."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-fail"}
    h_ok, hour_ok_dt = _past_hour(2)
    h_bad, _ = _past_hour(3)
    con = _seed_con(hour_ok_dt, [("Googlebot/2.1", "66.249.66.1")])

    calls = {"n": 0}

    def _flaky_retry(c, _src, fn):
        calls["n"] += 1
        if calls["n"] == 2:  # first call is the column DESCRIBE, second is hour 1, third hour 2
            return fn(c)
        if calls["n"] == 3:
            raise duckdb.IOException("simulated COPY failure")
        return fn(c)

    p = _view_patches(cache_root, con)
    with (
        p[0],
        p[1],
        p[2],
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_flaky_retry),
        p[4],
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-fail", src, [h_ok, h_bad])

    assert n == 1
    assert (cache_root / "rollups" / "wellknown_bots" / f"hour={h_ok}").exists()
    bad_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h_bad}"
    if bad_dir.exists():
        assert not list(bad_dir.glob("compacted_*.parquet"))
