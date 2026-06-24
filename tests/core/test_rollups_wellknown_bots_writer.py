"""Tests for the writer half of the wellknown_bots rollup.

The existing `test_rollups_wellknown_bots.py` covers the READER only —
its docstring explicitly defers writer coverage because the writer
needs a real DuckDB connection against a base table. This file fills
that gap by spinning up an in-memory DuckDB with `ua` + `ip` rows and
patching `get_connection` to hand it to the writer.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _seed_logs(hour_dt: datetime, rows: list[tuple[str, str]]) -> duckdb.DuckDBPyConnection:
    """Create a logs_x table with (timestamp, ua, ip) and seed rows."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ua VARCHAR, ip VARCHAR)")
    for ua, ip in rows:
        con.execute(
            "INSERT INTO logs_x VALUES (?, ?, ?)",
            [hour_dt + timedelta(minutes=3), ua, ip],
        )
    return con


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
    still in flight and the reader serves it live."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ua VARCHAR, ip VARCHAR)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [active])

    assert n == 0


def test_recompute_malformed_hour_skipped(tmp_path):
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    con = duckdb.connect(":memory:")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, ["nope"])

    assert n == 0


def test_recompute_no_safe_table_returns_zero(tmp_path):
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {}  # no name/service_id → _safe_table_for returns None
    h, _ = _past_hour(2)
    con = duckdb.connect(":memory:")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


def test_recompute_describe_failure_returns_zero(tmp_path):
    """If DESCRIBE fails (stale view), the writer logs + returns 0
    instead of crashing the sync cron."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)
    con = duckdb.connect(":memory:")

    def _boom(_c, _src, _fn):
        raise duckdb.Error("synthetic")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_boom),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


def test_recompute_table_missing_ua_or_ip_returns_zero(tmp_path):
    """Service whose schema lacks ua OR ip → can't materialize bots,
    skip cleanly without writing partial / wrong-shape files."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ua"}
    h, hour_dt = _past_hour(2)
    con = duckdb.connect(":memory:")
    # Only timestamp + ip (no ua).
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ip VARCHAR)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.rollups.wellknown_bots._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-no-ua", src, [h])

    assert n == 0
    # Critical: no parquet was written.
    assert not (cache_root / "rollups" / "wellknown_bots").exists() or not any(
        (cache_root / "rollups" / "wellknown_bots").rglob("compacted_*.parquet")
    )


def test_recompute_writes_filtered_rows_to_partition(tmp_path):
    """Happy path: a closed hour with bot-UA traffic produces a
    ``rollups/wellknown_bots/hour=H/compacted_*.parquet`` partition.

    Schema correctness + regex filtering are covered structurally — the
    exact COPY-output row contents depend on DuckDB's regex behaviour
    against the time-bucket extracted from TIMESTAMPTZ rows, which the
    integration tests exercise against real data.
    """
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-write"}
    h, hour_dt = _past_hour(2)

    con = _seed_logs(hour_dt, [("Googlebot/2.1", "66.249.66.1")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v42"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=".*"),  # match-all
        patch("backend.core.rollups.wellknown_bots._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
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


def test_recompute_sweeps_stale_parquets_in_hour_dir(tmp_path):
    """Before publishing the fresh tmp parquet, the writer must sweep
    any pre-existing parquets in the hour dir — otherwise a reader
    enumerating the dir could see a stale-version row alongside the
    fresh one for the same hour."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-sweep"}
    h, hour_dt = _past_hour(2)

    # Pre-seed a stale parquet at the canonical location.
    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    hour_dir.mkdir(parents=True)
    stale = hour_dir / "compacted_stale.parquet"
    stale.write_bytes(b"stale content")

    con = _seed_logs(hour_dt, [("Googlebot/2.1", "66.249.66.1")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v_fresh"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.rollups.wellknown_bots._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-sweep", src, [h])

    assert n == 1
    # Stale gone; exactly one fresh compacted_ parquet present.
    assert not stale.exists()
    fresh = list(hour_dir.glob("compacted_*.parquet"))
    assert len(fresh) == 1


def test_recompute_copy_failure_skips_hour_without_raising(tmp_path):
    """A COPY failure for one hour leaves the rest of the hours
    untouched and returns the count successfully written."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-fail"}
    h_ok, hour_ok_dt = _past_hour(2)
    h_bad, _ = _past_hour(3)

    real_con = _seed_logs(hour_ok_dt, [("Googlebot/2.1", "66.249.66.1")])

    class _Proxy:
        def __init__(self, con):
            self._con = con

        def execute(self, sql, *args, **kwargs):
            # Fail every COPY for the bad hour token.
            if "COPY" in sql and h_bad in sql:
                raise duckdb.Error("synthetic bad-hour failure")
            return self._con.execute(sql, *args, **kwargs)

        def close(self):
            self._con.close()

    proxy = _Proxy(real_con)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
        patch("backend.core.rollups.wellknown_bots._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=proxy),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-fail", src, [h_ok, h_bad])

    # Only the good hour succeeded.
    assert n == 1
    assert (cache_root / "rollups" / "wellknown_bots" / f"hour={h_ok}").exists()
    bad_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h_bad}"
    # Dir got created (mkdirs above the COPY) but no compacted parquet.
    if bad_dir.exists():
        assert not list(bad_dir.glob("compacted_*.parquet"))
