"""Tests for the wellknown_bots rollup writer + reader.

The rollup pre-materialises the (ua, ip, count) tuples that
``/api/security/aggregates``'s wellknown_bots block would otherwise
compute via a 500-pattern RE2 prefilter on the request-scoped
temp_table. Writer runs from the sync cron after
``recompute_touched_hours``; reader is called from
``backend/repositories/security.py`` with a live-SQL fallback.

These tests pin the reader's serve / fall-back semantics. The reader
serves CLOSED hours only on windows >= 48 h (dropping the in-progress
active hour), with a >= 50 % closed-hour coverage floor — the same
posture as slow_urls / security_dims. Narrow windows (< 48 h) stay on
the live path. The reader is the correctness boundary (return wrong
data → wrong bot counts in the UI), so it gets the bulk of the coverage.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


def _write_rollup_hour(
    cache_root: str,
    hour: str,
    rows: list[tuple[str, str, int]],
    pattern_set_version: str = "v1700000000",
) -> str:
    """Write a synthetic wellknown_bots rollup parquet for a single hour.

    Returns the parquet path. Schema mirrors what
    :func:`backend.core.rollups.recompute_wellknown_bots_rollup` writes:
    ``(ua, ip, request_count, pattern_set_version)``.
    """
    d = os.path.join(cache_root, "rollups", "wellknown_bots", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "ua": pa.array([r[0] for r in rows]),
            "ip": pa.array([r[1] for r in rows]),
            "request_count": pa.array([r[2] for r in rows], type=pa.int64()),
            "pattern_set_version": pa.array([pattern_set_version] * len(rows)),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _past_hour_iso(hours_ago: int) -> tuple[str, str]:
    """Return ``(hour_token, iso_string)`` for a fully-closed hour N hours ago."""
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_block(
    cache_root: str,
    n_hours: int,
    first_hours_ago: int,
    rows_for,
    pattern_set_version: str = "v1700000000",
) -> tuple[str, str]:
    """Seed ``n_hours`` contiguous closed-hour partitions, the newest
    ``first_hours_ago`` hours back. ``rows_for(i)`` supplies the rows for the
    i-th hour. Returns ``(start_iso, end_iso)`` spanning the block (>= 48 h
    when n_hours >= 49)."""
    for i in range(n_hours):
        tok, _ = _past_hour_iso(first_hours_ago + i)
        _write_rollup_hour(cache_root, tok, rows_for(i), pattern_set_version)
    _, start_iso = _past_hour_iso(first_hours_ago + n_hours - 1)
    _, end_iso = _past_hour_iso(first_hours_ago)
    return start_iso, end_iso


def test_read_returns_rows_when_window_covered_and_versions_match(tmp_path):
    """Happy path: a >= 48 h window whose closed hours all have a
    fresh-version rollup returns the union of per-hour rows (the reader
    does not aggregate — the downstream Python loop does)."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-1"}

    # 50 closed hours, one distinct row each → a ~49 h span (>= 48 h).
    start_iso, end_iso = _seed_block(
        str(cache_root),
        n_hours=50,
        first_hours_ago=2,
        rows_for=lambda i: [(f"Bot{i}/1.0", f"10.0.0.{i}", 100 + i)],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is not None, "expected rollup hit; got None (fallback)"
    assert len(rows) == 50
    # Sorted DESC by request_count.
    counts = [r[2] for r in rows]
    assert counts == sorted(counts, reverse=True)


def test_read_falls_back_below_48h_window(tmp_path):
    """Narrow windows (< 48 h) stay on the live path — the closed-hour read
    doesn't amortise there and live also covers the active hour."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-narrow"}

    # Seed a couple of closed hours, query only a 24 h span.
    h2, _ = _past_hour_iso(2)
    h3, _ = _past_hour_iso(3)
    _, end_iso = _past_hour_iso(2)
    _, start_iso = _past_hour_iso(26)  # 24 h span

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 50)])
        _write_rollup_hour(str(cache_root), h3, [("Bingbot/2.0", "157.55.39.1", 20)])
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "windows < 48 h must fall back to live"


def test_read_falls_back_below_coverage_floor(tmp_path):
    """A >= 48 h window with < 50 % of its closed hours covered falls back to
    live rather than badly undercount the leaderboard."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-2"}

    # 50 h span, but only ~10 hours seeded (10 / 50 = 20% < 50%).
    _, start_iso = _past_hour_iso(51)
    _, end_iso = _past_hour_iso(2)
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        for i in range(10):
            tok, _ = _past_hour_iso(2 + i)
            _write_rollup_hour(str(cache_root), tok, [("Googlebot/2.1", "66.249.66.1", 50)])
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "below the 50% coverage floor must trigger fallback"


def test_read_serves_closed_hours_dropping_active_hour(tmp_path):
    """A >= 48 h window ending NOW serves its CLOSED hours (the in-progress
    active hour is dropped, not a fallback trigger). Previously any window
    touching the active hour bailed to live — which, since dashboard requests
    always end "now", meant the rollup never served.

    Also pins the cross-hour (ua, ip) aggregation: 50 closed hours each with
    the SAME (ua, ip, 5) collapse to ONE row summing to 250 — not 50 per-hour
    rows."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-4"}

    # Seed 50 closed hours (2..51 back); window runs from 51 h ago to NOW.
    _seed_block(
        str(cache_root),
        n_hours=50,
        first_hours_ago=2,
        rows_for=lambda i: [("Googlebot/2.1", "66.249.66.1", 5)],
    )
    _, start_iso = _past_hour_iso(51)
    end_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is not None, "closed hours must serve even when the window ends in the active hour"
    # 50 identical (ua, ip) hours aggregate to ONE row; active hour excluded.
    assert len(rows) == 1
    assert rows[0] == ("Googlebot/2.1", "66.249.66.1", 250)


def test_read_falls_back_when_pattern_set_version_stale(tmp_path):
    """A bot-sources refresh bumps ``get_pattern_set_version``; rollups
    written under the previous version must be ignored so a newly-added bot
    pattern doesn't silently miss recent traffic."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-3"}

    start_iso, end_iso = _seed_block(
        str(cache_root),
        n_hours=50,
        first_hours_ago=2,
        rows_for=lambda i: [("OldBot/1.0", "1.2.3.4", 10)],
        pattern_set_version="v1700000000",
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000999"),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "stale pattern_set_version must trigger fallback"


def test_read_returns_empty_list_when_window_covered_but_no_bot_traffic(tmp_path):
    """Empty parquets (closed hours with zero matches) over a covered >= 48 h
    window are a valid "0 bot rows" answer, not a fallback signal."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-5"}

    # 50 closed hours, each an EMPTY parquet (file exists, zero rows).
    for i in range(50):
        tok, _ = _past_hour_iso(2 + i)
        d = os.path.join(str(cache_root), "rollups", "wellknown_bots", f"hour={tok}")
        os.makedirs(d, exist_ok=True)
        empty_table = pa.table(
            {
                "ua": pa.array([], type=pa.string()),
                "ip": pa.array([], type=pa.string()),
                "request_count": pa.array([], type=pa.int64()),
                "pattern_set_version": pa.array([], type=pa.string()),
            }
        )
        pq.write_table(empty_table, os.path.join(d, "compacted_empty.parquet"))
    _, start_iso = _past_hour_iso(51)
    _, end_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows == [], "covered-but-empty window must return [] not None"


def test_read_falls_back_when_pattern_set_version_missing(tmp_path):
    """No source files cached yet → no version → no rollup possible.
    Reader must defer to live-SQL even if parquet files exist."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-6"}

    start_iso, end_iso = _seed_block(
        str(cache_root),
        n_hours=50,
        first_hours_ago=2,
        rows_for=lambda i: [("OldBot/1.0", "1.2.3.4", 10)],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=""),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None


def test_read_falls_back_when_time_range_exceeds_366_days():
    """Spans wider than 366 days short-circuit to None before the hour loop
    (guards against an attacker-influenced multi-year range)."""
    from backend.core import rollups

    _, end_iso = _past_hour_iso(2)
    start_dt = datetime.now(UTC) - timedelta(days=500)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    src = {"name": "svc-bot-range-guard"}

    rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)
    assert rows is None, "spans > 366 days must short-circuit to None"


def _write_data_dir(cache_root: str, hour: str) -> None:
    """Mark a closed hour as having a committed raw data partition. The
    backfill only probes for ``*.parquet`` presence here (recompute, which
    actually reads them, is mocked in these tests), so a dummy file suffices."""
    d = os.path.join(cache_root, "data", f"timestamp_hour={hour}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "batch_x.parquet"), "wb") as f:
        f.write(b"x")


def test_backfill_wellknown_rebuilds_data_backed_and_sweeps_unrebuildable_stale(tmp_path):
    """The self-heal pass is keyed on raw data partitions:

    - a data-backed hour with NO wellknown partition → rebuilt;
    - a data-backed hour with a STALE-version partition → rebuilt;
    - a data-backed hour already on the CURRENT version → skipped;
    - a STALE-version hour whose raw data has aged out → DELETED (it can't be
      rebuilt and would otherwise pin the whole window on the live path);
    - the in-progress active hour → never touched.
    """
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-bf"}

    h_missing, _ = _past_hour_iso(3)  # data + no wk partition → rebuild
    h_stale_data, _ = _past_hour_iso(4)  # data + stale wk → rebuild
    h_current, _ = _past_hour_iso(5)  # data + current wk → skip
    h_stale_nodata, _ = _past_hour_iso(6)  # stale wk, NO data → delete
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    for tok in (h_missing, h_stale_data, h_current):
        _write_data_dir(str(cache_root), tok)
    # active hour HAS data but must never be built.
    _write_data_dir(str(cache_root), active)

    _write_rollup_hour(
        str(cache_root), h_current, [("Googlebot/2.1", "66.249.66.1", 5)], pattern_set_version="vCURRENT"
    )
    _write_rollup_hour(str(cache_root), h_stale_data, [("Bingbot/2.0", "157.55.39.1", 3)], pattern_set_version="vOLD")
    _write_rollup_hour(str(cache_root), h_stale_nodata, [("OldBot/1.0", "1.2.3.4", 2)], pattern_set_version="vOLD")

    nodata_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h_stale_nodata}"
    assert nodata_dir.exists()

    seen: list[list[str]] = []

    def _spy_recompute(_sid, _src, hours):
        seen.append(sorted(hours))
        return len(hours)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="vCURRENT"),
        patch.object(wellknown_bots, "recompute_wellknown_bots_rollup", side_effect=_spy_recompute),
    ):
        n = wellknown_bots.backfill_wellknown_bots_rollup("svc-bot-bf", src)

    assert seen == [sorted([h_missing, h_stale_data])], f"rebuild set wrong; got {seen}"
    assert n == 2
    # The unrebuildable stale hour's partition files are swept.
    assert not list(nodata_dir.glob("*.parquet")), "unrebuildable stale hour must be deleted"
    # Current + data-backed-stale dirs are untouched by the sweep.
    assert list((cache_root / "rollups" / "wellknown_bots" / f"hour={h_current}").glob("*.parquet"))
    assert list((cache_root / "rollups" / "wellknown_bots" / f"hour={h_stale_data}").glob("*.parquet"))
