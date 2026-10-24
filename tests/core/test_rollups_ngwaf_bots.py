"""Tests for the ngwaf_bots per-hour bundle writer, the day-compaction merge,
and the get_top_bots reader (try_ngwaf_top_bots_from_rollup).

Mirrors test_rollups_security_dims.py in structure — same patch pattern, same
_noop_lock helper, same _build_patches stack. The writer's join reads the
SQLite ngwaf_bot_cache via ``sqlite_scan`` (no ATTACH), so the fixtures build
a real cache file with sqlite3 (same shape as tests/repositories/
test_security_ngwaf.py's ``_create_cache_db``).
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.rollups import build_writer_patches as _build_patches

pytestmark = pytest.mark.usefixtures("frozen_clock")


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, waf_req_id VARCHAR)")
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [r["timestamp"], r.get("waf_req_id")])


def _create_cache_db(db_path: str, entries: list[tuple]) -> None:
    """entries: (waf_req_id, bot_name, category[, wellknown_bot_name])."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ngwaf_bots ("
        " waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT,"
        " wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    for entry in entries:
        if len(entry) == 4:
            wid, name, cat, wk_name = entry
        else:
            wid, name, cat = entry
            wk_name = None
        con.execute(
            "INSERT INTO ngwaf_bots VALUES (?, ?, ?, NULL, ?, '2026-01-01T00:00:00Z')",
            (wid, name, cat, wk_name),
        )
    con.commit()
    con.close()


# ── Writer ───────────────────────────────────────────────────────────────────


def test_build_ngwaf_bots_writes_aggregated_join(tmp_path):
    """A closed hour's waf_req_ids join against the cache and aggregate to
    (bot_name, wellknown_bot_name, category, count); unmatched ids and NULLs drop out."""
    from backend.core.rollups import ngwaf_bots as nb

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(
        db_path,
        [
            ("w1", "GPTBot", "AI-CRAWLER", "GPTBot"),
            ("w2", "GPTBot", "AI-CRAWLER", "GPTBot"),
            ("w3", "AhrefsBot", "SEO", None),
        ],
    )
    src = {"name": "svc-nb-1"}
    hour_token, hour_dt = _past_hour(2)

    rows = [
        {"timestamp": hour_dt, "waf_req_id": "w1"},
        {"timestamp": hour_dt + timedelta(seconds=1), "waf_req_id": "w2"},
        {"timestamp": hour_dt + timedelta(seconds=2), "waf_req_id": "w3"},
        {"timestamp": hour_dt + timedelta(seconds=3), "waf_req_id": "unmatched"},
        {"timestamp": hour_dt + timedelta(seconds=4), "waf_req_id": None},
    ]

    def _fresh_con():
        c = duckdb.connect(":memory:")
        c.execute("INSTALL sqlite; LOAD sqlite;")
        _seed_logs(c, "logs_nb", rows)
        return c

    p = _build_patches(cache_root, "logs_nb", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4], patch("backend.config.ngwaf_db_path", return_value=db_path):
        n = nb.build_ngwaf_bots_bundles("svc-nb-1", src, [hour_token])

    assert n == 1
    out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "ngwaf_bots.parquet"
    assert out.is_file()
    got = {
        (r["bot_name"], r["wellknown_bot_name"], r["category"]): r["count"] for r in pq.read_table(str(out)).to_pylist()
    }
    assert got == {("GPTBot", "GPTBot", "AI-CRAWLER"): 2, ("AhrefsBot", None, "SEO"): 1}


def test_build_ngwaf_bots_zero_bot_hour_writes_empty_marker(tmp_path):
    """A covered hour with NO cache matches still writes an (empty) parquet —
    the covered-and-empty marker that keeps the reader off the live fallback."""
    from backend.core.rollups import ngwaf_bots as nb

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path, [])
    src = {"name": "svc-nb-2"}
    hour_token, hour_dt = _past_hour(2)

    def _fresh_con():
        c = duckdb.connect(":memory:")
        c.execute("INSTALL sqlite; LOAD sqlite;")
        _seed_logs(c, "logs_nb2", [{"timestamp": hour_dt, "waf_req_id": "wX"}])
        return c

    p = _build_patches(cache_root, "logs_nb2", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4], patch("backend.config.ngwaf_db_path", return_value=db_path):
        n = nb.build_ngwaf_bots_bundles("svc-nb-2", src, [hour_token])

    assert n == 1
    out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "ngwaf_bots.parquet"
    assert out.is_file()
    assert pq.read_table(str(out)).num_rows == 0


def test_build_ngwaf_bots_skips_without_column_or_cache(tmp_path):
    """No waf_req_id column → skip; no cache file on disk → skip."""
    from backend.core.rollups import ngwaf_bots as nb

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-nb-3"}
    hour_token, hour_dt = _past_hour(2)

    def _con_no_col():
        c = duckdb.connect(":memory:")
        c.execute("CREATE TABLE logs_nb3 (timestamp TIMESTAMPTZ)")
        return c

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path, [])
    p = _build_patches(cache_root, "logs_nb3", _con_no_col)
    with p[0], p[1], p[2], p[3], p[4], patch("backend.config.ngwaf_db_path", return_value=db_path):
        assert nb.build_ngwaf_bots_bundles("svc-nb-3", src, [hour_token]) == 0

    def _con_with_col():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_nb3b", [{"timestamp": hour_dt, "waf_req_id": "w1"}])
        return c

    p = _build_patches(cache_root, "logs_nb3b", _con_with_col)
    with p[0], p[1], p[2], p[3], p[4], patch("backend.config.ngwaf_db_path", return_value=str(tmp_path / "missing.db")):
        assert nb.build_ngwaf_bots_bundles("svc-nb-3", src, [hour_token]) == 0


# ── Day compaction ───────────────────────────────────────────────────────────


def test_compact_ngwaf_bots_days_sums_hours(tmp_path):
    """24 closed hours fold into one per-day file with SUMmed counts."""
    from backend.core.rollups.day_bundles import compact_ngwaf_bots_closed_days_to_daily

    cache_root = tmp_path / "cache"
    day = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    for h in range(24):
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={day}-{h:02d}"
        hour_dir.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "bot_name": ["GPTBot"],
                    "wellknown_bot_name": ["GPTBot"],
                    "category": ["AI-CRAWLER"],
                    "count": pa.array([2], type=pa.int64()),
                }
            ),
            str(hour_dir / "ngwaf_bots.parquet"),
        )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = compact_ngwaf_bots_closed_days_to_daily("svc-nb-4", {"name": "svc-nb-4"})

    assert n == 1
    out = cache_root / "rollups" / "day_bundled" / f"day={day}" / "ngwaf_bots.parquet"
    assert out.is_file()
    rows = pq.read_table(str(out)).to_pylist()
    # The shared compactor stamps a hive-style `day` column alongside the
    # feature columns; the reader selects columns by name so it's inert.
    assert [{k: r[k] for k in ("bot_name", "wellknown_bot_name", "category", "count")} for r in rows] == [
        {"bot_name": "GPTBot", "wellknown_bot_name": "GPTBot", "category": "AI-CRAWLER", "count": 48}
    ]


def test_compact_ngwaf_bots_ts_days_sums_hours(tmp_path):
    """24 closed hours of minute-bucketed ngwaf_bots_ts fold into one per-day file."""
    from backend.core.rollups.day_bundles import compact_ngwaf_bots_ts_closed_days_to_daily

    cache_root = tmp_path / "cache"
    day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    day = day_dt.strftime("%Y-%m-%d")
    for h in range(24):
        hour_dt = day_dt + timedelta(hours=h)
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={day}-{h:02d}"
        hour_dir.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "bucket_ts": pa.array([hour_dt], type=pa.timestamp("us", tz="UTC")),
                    "bot_name": ["GPTBot"],
                    "count": pa.array([3], type=pa.int64()),
                }
            ),
            str(hour_dir / "ngwaf_bots_ts.parquet"),
        )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = compact_ngwaf_bots_ts_closed_days_to_daily("svc-nb-4-ts", {"name": "svc-nb-4-ts"})

    assert n == 1
    out = cache_root / "rollups" / "day_bundled" / f"day={day}" / "ngwaf_bots_ts.parquet"
    assert out.is_file()
    rows = pq.read_table(str(out)).to_pylist()
    assert len(rows) == 24
    assert sum(r["count"] for r in rows) == 72


def test_stale_3col_ngwaf_bots_parquet_fallback_and_purge(tmp_path):
    """Trap #24: pre-rebundle 3-col ngwaf_bots.parquet (without wellknown_bot_name)
    causes try_ngwaf_verified_bots_from_rollup to return None (live fallback),
    and _purge_stale_ngwaf_bots_parquets removes it so backfill rewrites 4 cols."""
    from backend.core.rollups.ngwaf_bots import _purge_stale_ngwaf_bots_parquets
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    base = (datetime.now(UTC) - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0)
    for i in range(24):
        hour_dir = (
            cache_root / "rollups" / "hour_bundled" / f"hour={(base + timedelta(hours=i)).strftime('%Y-%m-%d-%H')}"
        )
        hour_dir.mkdir(parents=True)
        # Legacy 3-col schema without wellknown_bot_name
        pq.write_table(
            pa.table(
                {
                    "bot_name": ["GPTBot"],
                    "category": ["AI-CRAWLER"],
                    "count": pa.array([2], type=pa.int64()),
                }
            ),
            str(hour_dir / "ngwaf_bots.parquet"),
        )

    con = duckdb.connect(":memory:")
    src = {"name": "svc-nb-trap24"}
    runner = QueryRunner(con, src)
    st = base.isoformat()
    et = (base + timedelta(hours=24)).isoformat()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Reader detects missing wellknown_bot_name column and falls back
        assert runner.try_ngwaf_verified_bots_from_rollup(st, et, has_filters=False) is None
        # Purge unlinks all 24 legacy 3-col files
        purged = _purge_stale_ngwaf_bots_parquets(src)
        assert purged == 24


def test_ngwaf_verified_bots_and_ts_parity_with_live_sql_and_temp_skip(tmp_path):
    """End-to-end parity: writer → reader produces identical rows to
    SQL.NGWAF_VERIFIED_BOTS and SQL.NGWAF_VERIFIED_BOTS_TS (at 60s and 300s
    buckets, including active-hour top-up), and _build_security_response
    logs security:temp_skipped when both sections are rollup-served."""
    from backend.core.rollups import ngwaf_bots as nb
    from backend.repositories._base import QueryRunner, ensure_ngwaf_bots_materialized, safe_iso
    from backend.repositories._sql import security as SQL
    from backend.repositories.security import _build_security_response

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(
        db_path,
        [
            ("w1", "Google Feed Fetcher", "FEED_FETCHER", "Google FeedFetcher"),
            ("w2", "GPTBot", "AI-CRAWLER", "GPTBot"),
            ("w3", "Facebook Crawler", "SOCIAL", None),
        ],
    )

    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    base_dt = active_dt - timedelta(hours=24)
    closed_hours = [(base_dt + timedelta(hours=i)).strftime("%Y-%m-%d-%H") for i in range(24)]

    # Seed events across closed hours plus the active hour
    h0 = base_dt + timedelta(minutes=2, seconds=15)
    h1 = base_dt + timedelta(hours=1, minutes=7, seconds=40)
    h_active = active_dt + timedelta(minutes=4, seconds=10)
    raw_rows = [
        {"timestamp": h0, "waf_req_id": "w1"},
        {"timestamp": h0 + timedelta(seconds=10), "waf_req_id": "w1"},
        {"timestamp": h0 + timedelta(seconds=20), "waf_req_id": "w1"},
        {"timestamp": h0 + timedelta(minutes=1), "waf_req_id": "w2"},
        {"timestamp": h1, "waf_req_id": "w3"},
        {"timestamp": h1 + timedelta(seconds=20), "waf_req_id": "unmatched"},
        {"timestamp": h_active, "waf_req_id": "w2"},
    ]

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_parity", raw_rows)
        return c

    src = {"name": "svc-nb-parity", "service_id": "svc-nb-parity"}
    p = _build_patches(cache_root, "logs_parity", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4], patch("backend.config.ngwaf_db_path", return_value=db_path):
        assert nb.build_ngwaf_bots_bundles("svc-nb-parity", src, closed_hours) == 24
        assert nb.build_ngwaf_bots_ts_bundles("svc-nb-parity", src, closed_hours) == 24

    # Write active-hour row to live buffer so active-hour direct temp picks it up
    buffer_dir = cache_root / "buffer"
    buffer_dir.mkdir()
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array([h_active], type=pa.timestamp("us", tz="UTC")),
                "waf_req_id": pa.array(["w2"], type=pa.string()),
            }
        ),
        str(buffer_dir / "live.parquet"),
    )

    con = _fresh_con()
    st_iso = base_dt.isoformat()
    et_iso = (active_dt + timedelta(minutes=30)).isoformat()

    with (
        patch("backend.config.ngwaf_db_path", return_value=db_path),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
    ):
        assert ensure_ngwaf_bots_materialized(con, "ngwaf_cache")
        runner = QueryRunner(con, src)
        runner.get_schema_cols = lambda: ["timestamp", "waf_req_id"]  # type: ignore[method-assign]

        # Direct SQL reference over logs_parity (full window including active hour)
        live_table_rows = [
            {
                "bot_name": r[0],
                "wellknown_bot_name": r[1],
                "category": r[2],
                "request_count": r[3],
            }
            for r in con.execute(SQL.NGWAF_VERIFIED_BOTS.format(temp_table="logs_parity")).fetchall()
        ]
        rolled_table_rows = runner.try_ngwaf_verified_bots_from_rollup(st_iso, et_iso, has_filters=False)
        assert rolled_table_rows == live_table_rows

        for bucket_s in (60, 300):
            live_ts = [
                {"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]}
                for r in con.execute(
                    SQL.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=bucket_s, temp_table="logs_parity")
                ).fetchall()
            ]
            rolled_ts_raw = runner.try_ngwaf_verified_bots_ts_from_rollup(
                st_iso, et_iso, bucket_seconds=bucket_s, has_filters=False
            )
            assert rolled_ts_raw is not None
            rolled_ts = [{"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]} for r in rolled_ts_raw]
            assert rolled_ts == live_ts

        # Verify _build_security_response skips the catalog temp entirely
        section_timings: list[dict] = []
        resp = _build_security_response(
            runner,
            src,
            con,
            ["timestamp", "waf_req_id"],
            "logs_parity",
            "",
            None,
            60,
            section_timings,
            start_time=st_iso,
            end_time=et_iso,
            filters=None,
            sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
        )
        assert resp["ngwaf_verified_bots"] == live_table_rows
        phase_names = {entry.get("name") or entry.get("section") for entry in section_timings}
        assert "security:temp_skipped" in phase_names
        assert "ngwaf_verified_bots_rollup" in phase_names
        assert "ngwaf_verified_bots_ts_rollup" in phase_names
        assert "temp_table_create" not in phase_names
        assert "security:temp_ngwaf_narrowed" not in phase_names


# ── Reader ───────────────────────────────────────────────────────────────────


def test_reader_sums_and_ranks_closed_hours(tmp_path):
    """Closed-window read: SUM across hour files, ranked desc, capped at n;
    a covered-but-empty window returns [] (NOT None — no live fallback)."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    base = (datetime.now(UTC) - timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)
    for i in range(3):
        hour_dir = (
            cache_root / "rollups" / "hour_bundled" / f"hour={(base + timedelta(hours=i)).strftime('%Y-%m-%d-%H')}"
        )
        hour_dir.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "bot_name": ["GPTBot", "AhrefsBot"],
                    "category": ["AI-CRAWLER", "SEO"],
                    "count": pa.array([5, 1], type=pa.int64()),
                }
            ),
            str(hour_dir / "ngwaf_bots.parquet"),
        )

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, {"name": "svc-nb-5"})
    st = base.isoformat()
    et = (base + timedelta(hours=3)).isoformat()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        got = runner.try_ngwaf_top_bots_from_rollup(st, et, has_filters=False, n=10)
        top1 = runner.try_ngwaf_top_bots_from_rollup(st, et, has_filters=False, n=1)

    assert got == [
        {"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 15},
        {"name": "AhrefsBot", "category": "SEO", "request_count": 3},
    ]
    assert top1 == [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 15}]


def test_reader_eligibility_gates(tmp_path):
    """None on: filters present, window < 2h, no bundle coverage, malformed
    timestamps — caller falls back to the direct join."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, {"name": "svc-nb-6"})

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        call = lambda s, e, hf: runner.try_ngwaf_top_bots_from_rollup(s, e, has_filters=hf, n=10)  # noqa: E731
        assert call("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", True) is None
        assert call("2026-06-01T00:00:00Z", "2026-06-01T01:30:00Z", False) is None
        assert call("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", False) is None
        assert call("not-a-date", "2026-06-02T00:00:00Z", False) is None

        assert (
            runner.try_ngwaf_verified_bots_from_rollup("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", has_filters=True)
            is None
        )
        assert (
            runner.try_ngwaf_verified_bots_from_rollup(
                "2026-06-01T00:00:00Z", "2026-06-01T12:00:00Z", has_filters=False
            )
            is None
        )

        assert (
            runner.try_ngwaf_verified_bots_ts_from_rollup(
                "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", bucket_seconds=60, has_filters=True
            )
            is None
        )
        assert (
            runner.try_ngwaf_verified_bots_ts_from_rollup(
                "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", bucket_seconds=45, has_filters=False
            )
            is None
        )
        assert (
            runner.try_ngwaf_verified_bots_ts_from_rollup(
                "2026-06-01T00:00:00Z", "2026-06-01T12:00:00Z", bucket_seconds=60, has_filters=False
            )
            is None
        )


def test_reader_live_topup_joins_active_hour_buffer(tmp_path):
    """A window crossing the active hour merges live buffer waf_req_ids via
    the direct read + the caller-ATTACHed ngwaf_top cache."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    closed = [active_dt - timedelta(hours=2), active_dt - timedelta(hours=1)]
    for dt in closed:
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={dt.strftime('%Y-%m-%d-%H')}"
        hour_dir.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "bot_name": ["GPTBot"],
                    "category": ["AI-CRAWLER"],
                    "count": pa.array([2], type=pa.int64()),
                }
            ),
            str(hour_dir / "ngwaf_bots.parquet"),
        )

    buffer_dir = cache_root / "buffer"
    buffer_dir.mkdir()
    ts = active_dt + timedelta(minutes=5)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array([ts, ts], type=pa.timestamp("us", tz="UTC")),
                "waf_req_id": pa.array(["w1", "w9"], type=pa.string()),
            }
        ),
        str(buffer_dir / "live.parquet"),
    )

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path, [("w1", "GPTBot", "AI-CRAWLER")])

    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    from backend.repositories._base import attach_ngwaf_cache

    with (
        patch("backend.config.ngwaf_db_path", return_value=db_path),
        attach_ngwaf_cache(con, ["waf_req_id"], "ngwaf_top"),
    ):
        runner = QueryRunner(con, {"name": "svc-nb-7"})
        runner.get_schema_cols = lambda: ["timestamp", "waf_req_id"]  # type: ignore[method-assign]

        st = closed[0].isoformat()
        et = (active_dt + timedelta(minutes=30)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            got = runner.try_ngwaf_top_bots_from_rollup(st, et, has_filters=False, n=10)

        # Closed: 2 hours × 2; live buffer adds w1 (matched) but not w9 (unmatched).
        assert got == [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 5}]
