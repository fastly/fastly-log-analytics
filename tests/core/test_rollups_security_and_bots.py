"""Tests for the ngwaf_bots per-hour bundle writer, the day-compaction merge,
and the get_top_bots reader (try_ngwaf_top_bots_from_rollup).

Mirrors test_rollups_security_dims.py in structure — same patch pattern, same
_noop_lock helper, same _build_patches stack. The writer's join reads the
SQLite ngwaf_bot_cache via ``sqlite_scan`` (no ATTACH), so the fixtures build
a real cache file with sqlite3 (same shape as tests/repositories/
test_security_ngwaf.py's ``_create_cache_db``).
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.repositories._sql.dashboard import CONN_REQUESTS_BUCKET
from backend.repositories._sql.security import (
    CONN_REUSE_DIST,
    FINGERPRINT_COVERAGE_BULK,
    REQ_HEADER_SIZE_DIST,
    TOP_IPS_BY_MAX_HEADER,
)
from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.dates import three_days_ago_iso as _three_days_ago_iso
from tests._helpers.dates import two_days_ago_iso as _two_days_ago_iso
from tests._helpers.dates import yesterday_iso as _yesterday_iso
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.rollups import (
    build_writer_patches as _build_patches,
)
from tests._helpers.rollups import make_stub_runner as _stub_runner
from tests._helpers.rollups import write_hour_all_fields as _write_hour_all_fields
from tests._helpers.rollups import (
    write_hour_bundle as _write_hour_bundle,
)


# ── Merged from tests/core/test_rollups_ngwaf_bots.py ──
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


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNgwafBots:
    def test_build_ngwaf_bots_writes_aggregated_join(self, tmp_path):
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
            (r["bot_name"], r["wellknown_bot_name"], r["category"]): r["count"]
            for r in pq.read_table(str(out)).to_pylist()
        }
        assert got == {("GPTBot", "GPTBot", "AI-CRAWLER"): 2, ("AhrefsBot", None, "SEO"): 1}

    def test_build_ngwaf_bots_zero_bot_hour_writes_empty_marker(self, tmp_path):
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

    def test_build_ngwaf_bots_skips_without_column_or_cache(self, tmp_path):
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
        with (
            p[0],
            p[1],
            p[2],
            p[3],
            p[4],
            patch("backend.config.ngwaf_db_path", return_value=str(tmp_path / "missing.db")),
        ):
            assert nb.build_ngwaf_bots_bundles("svc-nb-3", src, [hour_token]) == 0

    def test_compact_ngwaf_bots_days_sums_hours(self, tmp_path):
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

    def test_compact_ngwaf_bots_ts_days_sums_hours(self, tmp_path):
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

    def test_stale_3col_ngwaf_bots_parquet_fallback_and_purge(self, tmp_path):
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

    def test_ngwaf_verified_bots_and_ts_parity_with_live_sql_and_temp_skip(self, tmp_path):
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

    def test_reader_sums_and_ranks_closed_hours(self, tmp_path):
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

    def test_reader_eligibility_gates(self, tmp_path):
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
                runner.try_ngwaf_verified_bots_from_rollup(
                    "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", has_filters=True
                )
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

    def test_reader_live_topup_joins_active_hour_buffer(self, tmp_path):
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


# ── Merged from tests/core/test_rollups_security_dims.py ──
def _seed_logs_rollups_security_dims(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the security_dims writers read and
    INSERT rows. Columns: timestamp, ip, req_header_bytes, conn_requests,
    tls_ciphers_sha."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, "
        f"  conn_requests BIGINT, tls_ciphers_sha VARCHAR"
        f")"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ip"),
                r.get("req_header_bytes"),
                r.get("conn_requests"),
                r.get("tls_ciphers_sha"),
            ],
        )


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsSecurityDims:
    def test_build_security_dims_writes_four_bundles(self, tmp_path):
        """A closed hour with the full column set produces all four parquet files
        with their documented schemas + exact aggregates."""
        from backend.core.rollups import security_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-1"}
        hour_token, hour_dt = _past_hour(2)

        # 6 rows: header sizes spanning two buckets; conn_requests reuse; two IPs;
        # 4 of 6 have a populated tls fingerprint (one empty string → not populated).
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=0),
                "ip": "10.0.0.1",
                "req_header_bytes": 100,
                "conn_requests": 1,
                "tls_ciphers_sha": "abc",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=1),
                "ip": "10.0.0.1",
                "req_header_bytes": 300,
                "conn_requests": 3,
                "tls_ciphers_sha": "abc",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=2),
                "ip": "10.0.0.2",
                "req_header_bytes": 5000,
                "conn_requests": 10,
                "tls_ciphers_sha": "def",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=3),
                "ip": "10.0.0.2",
                "req_header_bytes": 200,
                "conn_requests": 1,
                "tls_ciphers_sha": "def",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=4),
                "ip": "10.0.0.3",
                "req_header_bytes": 50,
                "conn_requests": 1,
                "tls_ciphers_sha": "",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=5),
                "ip": "10.0.0.3",
                "req_header_bytes": 50,
                "conn_requests": 1,
                "tls_ciphers_sha": None,
            },
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_security_dims(c, "logs_sd", rows)
            return c

        p = _build_patches(cache_root, "logs_sd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = security_dims.build_security_dims_bundles("svc-sd-1", src, [hour_token])

        assert n == 4
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        req_file = hour_dir / "security_req_size.parquet"
        conn_file = hour_dir / "security_conn_reuse.parquet"
        topips_file = hour_dir / "security_topips.parquet"
        cov_file = hour_dir / "security_cov.parquet"
        assert req_file.exists() and conn_file.exists() and topips_file.exists() and cov_file.exists()

        # Schemas. (pyarrow auto-derives the `hour=` hive column from the path when
        # reading a single file inside a hour=H dir; the parquet body itself carries
        # only the COPY's projected columns — assert via issubset like origin_dims.)
        assert {"bucket", "count", "min_val"}.issubset(set(pq.read_table(str(req_file)).column_names))
        assert {"bucket", "count", "min_val"}.issubset(set(pq.read_table(str(conn_file)).column_names))
        assert {"ip", "max_header"}.issubset(set(pq.read_table(str(topips_file)).column_names))
        assert {"total_rows", "tls_populated"}.issubset(set(pq.read_table(str(cov_file)).column_names))

        # req_size buckets: 100,200,50,50 → '0-256B' (count 4, min 50); 300 →
        # '256-512B' (count 1, min 300); 5000 → '4-6KB' (count 1, min 5000).
        req_rows = {r["bucket"]: r for r in pq.read_table(str(req_file)).to_pylist()}
        assert req_rows["0-256B"]["count"] == 4 and req_rows["0-256B"]["min_val"] == 50
        assert req_rows["256-512B"]["count"] == 1 and req_rows["256-512B"]["min_val"] == 300
        assert req_rows["4-6KB"]["count"] == 1 and req_rows["4-6KB"]["min_val"] == 5000

        # conn_reuse: 1×4 → '1 (None)' (count 4, min 1); 3 → '2-5' (count 1, min 3);
        # 10 → '6-20' (count 1, min 10).
        conn_rows = {r["bucket"]: r for r in pq.read_table(str(conn_file)).to_pylist()}
        assert conn_rows["1 (None)"]["count"] == 4 and conn_rows["1 (None)"]["min_val"] == 1
        assert conn_rows["2-5"]["count"] == 1 and conn_rows["2-5"]["min_val"] == 3
        assert conn_rows["6-20"]["count"] == 1 and conn_rows["6-20"]["min_val"] == 10

        # topips: MAX per ip. 10.0.0.1→300, 10.0.0.2→5000, 10.0.0.3→50.
        topips_rows = {r["ip"]: r["max_header"] for r in pq.read_table(str(topips_file)).to_pylist()}
        assert topips_rows == {"10.0.0.1": 300, "10.0.0.2": 5000, "10.0.0.3": 50}

        # cov: 6 total, 4 populated (abc×2, def×2; '' and NULL not populated).
        cov = pq.read_table(str(cov_file)).to_pylist()[0]
        assert cov["total_rows"] == 6 and cov["tls_populated"] == 4

    def test_build_security_dims_skips_active_hour(self, tmp_path):
        """The active UTC hour is still being written; the writer must skip it."""
        from backend.core.rollups import security_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-2"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_security_dims(c, "logs_sd", [])
            return c

        p = _build_patches(cache_root, "logs_sd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = security_dims.build_security_dims_bundles("svc-sd-2", src, [active_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
        assert not (hour_dir / "security_req_size.parquet").exists()

    def test_build_security_dims_missing_conn_requests_skips_that_bundle(self, tmp_path):
        """A service with no conn_requests column still gets req_size + topips + cov;
        only the conn_reuse bundle is skipped."""
        from backend.core.rollups import security_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-3"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_nocr (timestamp TIMESTAMPTZ, ip VARCHAR, "
                "req_header_bytes BIGINT, tls_ciphers_sha VARCHAR)"
            )
            for i in range(4):
                c.execute(
                    "INSERT INTO logs_nocr VALUES (?, ?, ?, ?)",
                    [hour_dt + timedelta(minutes=i), "10.0.0.1", 100 + i, "abc"],
                )
            return c

        p = _build_patches(cache_root, "logs_nocr", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = security_dims.build_security_dims_bundles("svc-sd-3", src, [hour_token])

        assert n == 3
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "security_req_size.parquet").exists()
        assert (hour_dir / "security_topips.parquet").exists()
        assert (hour_dir / "security_cov.parquet").exists()
        assert not (hour_dir / "security_conn_reuse.parquet").exists()

    def test_build_security_dims_missing_ip_skips_topips_only(self, tmp_path):
        """topips needs ip AND req_header_bytes — a service with no ip skips topips
        but still writes req_size (needs only req_header_bytes), conn_reuse, cov."""
        from backend.core.rollups import security_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-3b"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_noip (timestamp TIMESTAMPTZ, req_header_bytes BIGINT, "
                "conn_requests BIGINT, tls_ciphers_sha VARCHAR)"
            )
            for i in range(4):
                c.execute(
                    "INSERT INTO logs_noip VALUES (?, ?, ?, ?)",
                    [hour_dt + timedelta(minutes=i), 100 + i, 1, "abc"],
                )
            return c

        p = _build_patches(cache_root, "logs_noip", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = security_dims.build_security_dims_bundles("svc-sd-3b", src, [hour_token])

        assert n == 3
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "security_req_size.parquet").exists()
        assert (hour_dir / "security_conn_reuse.parquet").exists()
        assert (hour_dir / "security_cov.parquet").exists()
        assert not (hour_dir / "security_topips.parquet").exists()

    def test_compact_security_dims_day_merge(self, tmp_path):
        """Day-compaction over a full closed UTC day: req_size/conn_reuse SUM counts
        + MIN-of-MIN; topips MAX-of-MAX (NOT SUM); cov SUM to one row."""
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-4"}

        # A full closed day (yesterday) so the compactor doesn't skip it as active.
        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day_dt.strftime("%Y-%m-%d")
        bundled_root = cache_root / "rollups" / "hour_bundled"

        # Two hours of that day.
        for hh in (3, 4):
            hour_token = (day_dt + timedelta(hours=hh)).strftime("%Y-%m-%d-%H")
            hour_dir = str(bundled_root / f"hour={hour_token}")
            # req_size: '0-256B' appears in both hours (counts 4 + 6, mins 50 + 80).
            _write_hour_bundle(
                hour_dir,
                "security_req_size.parquet",
                "('0-256B', CAST(4 AS BIGINT), CAST(50 AS BIGINT))"
                if hh == 3
                else "('0-256B', CAST(6 AS BIGINT), CAST(80 AS BIGINT))",
                "bucket, count, min_val",
            )
            # conn_reuse: '1 (None)' counts 2 + 3, mins 1 + 1.
            _write_hour_bundle(
                hour_dir,
                "security_conn_reuse.parquet",
                "('1 (None)', CAST(2 AS BIGINT), CAST(1 AS BIGINT))"
                if hh == 3
                else "('1 (None)', CAST(3 AS BIGINT), CAST(1 AS BIGINT))",
                "bucket, count, min_val",
            )
            # topips: ip A max 100 (h3) then 900 (h4); ip B max 500 (h3 only).
            _write_hour_bundle(
                hour_dir,
                "security_topips.parquet",
                "('A', CAST(100 AS BIGINT)), ('B', CAST(500 AS BIGINT))" if hh == 3 else "('A', CAST(900 AS BIGINT))",
                "ip, max_header",
            )
            # cov: 10/4 (h3), 20/9 (h4) → day total 30/13.
            _write_hour_bundle(
                hour_dir,
                "security_cov.parquet",
                "(CAST(10 AS BIGINT), CAST(4 AS BIGINT))" if hh == 3 else "(CAST(20 AS BIGINT), CAST(9 AS BIGINT))",
                "total_rows, tls_populated",
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-4", src)

        assert n == 4  # one per bundle for the single closed day

        day_root = cache_root / "rollups" / "day_bundled" / f"day={day_str}"

        req = pq.read_table(str(day_root / "security_req_size.parquet")).to_pylist()[0]
        assert req["bucket"] == "0-256B" and req["count"] == 10 and req["min_val"] == 50  # 4+6, MIN(50,80)

        conn = pq.read_table(str(day_root / "security_conn_reuse.parquet")).to_pylist()[0]
        assert conn["count"] == 5 and conn["min_val"] == 1  # 2+3

        # topips MAX-of-MAX: A = max(100, 900) = 900 (NOT 1000 if SUM'd); B = 500.
        topips = {
            r["ip"]: r["max_header"] for r in pq.read_table(str(day_root / "security_topips.parquet")).to_pylist()
        }
        assert topips == {"A": 900, "B": 500}

        cov = pq.read_table(str(day_root / "security_cov.parquet")).to_pylist()[0]
        assert cov["total_rows"] == 30 and cov["tls_populated"] == 13

    def test_compact_security_dims_topips_recaps_above_500(self, tmp_path):
        """Day-compaction re-caps topips to 500 IPs by MAX-of-MAX across hours,
        keeping the highest-max IPs."""
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-5"}

        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day_dt.strftime("%Y-%m-%d")
        bundled_root = cache_root / "rollups" / "hour_bundled"

        # 600 distinct IPs in one closed hour; max_header = the IP index so the
        # ranking is deterministic. The day file must keep the top-500 by MAX.
        hour_token = (day_dt + timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        hour_dir = str(bundled_root / f"hour={hour_token}")
        values = ", ".join(f"('ip{i:04d}', CAST({i} AS BIGINT))" for i in range(600))
        _write_hour_bundle(hour_dir, "security_topips.parquet", values, "ip, max_header")

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-5", src)

        day_file = cache_root / "rollups" / "day_bundled" / f"day={day_str}" / "security_topips.parquet"
        rows = pq.read_table(str(day_file)).to_pylist()
        assert len(rows) == 500
        maxes = sorted(r["max_header"] for r in rows)
        # Kept the top 500 by max_header: indices 100..599.
        assert maxes[0] == 100 and maxes[-1] == 599

    def test_backfill_security_dims_heals_each_bundle_independently(self, tmp_path):
        """The self-heal driver walks EACH of the four bundles independently, so a
        partial prior run (req_size complete, topips/cov missing) still resumes the
        laggards. h2 has only the req_size file; backfill must skip h2 for req_size
        but still build conn_reuse/topips/cov there (the single-sentinel bug)."""
        from backend.core.rollups import security_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-6"}

        h1_token, _ = _past_hour(3)
        h2_token, _ = _past_hour(4)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        for tok in (h1_token, h2_token):
            (bundled_root / f"hour={tok}").mkdir(parents=True)
            (bundled_root / f"hour={tok}" / "all_fields.parquet").write_bytes(b"x")
        # h2 already has ONLY the req_size bundle — the other three are missing.
        (bundled_root / f"hour={h2_token}" / "security_req_size.parquet").write_bytes(b"x")

        # Spy on the shared per-hour driver so each (bundle_filename -> hours) walk
        # is captured without touching the real DuckDB COPY path.
        calls: list[tuple[str, set[str]]] = []

        def _spy_bphb(sid, src, hours, *, bundle_filename, **kw):
            hours = list(hours)
            calls.append((bundle_filename, set(hours)))
            return len(hours)

        with patch("backend.core.rollups.security_dims.build_per_hour_bundles", side_effect=_spy_bphb):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = security_dims.backfill_security_dims_bundles("svc-sd-6", src)

        by_file = dict(calls)
        # req_size: only h1 is missing it (h2 has the sentinel).
        assert by_file[security_dims.SECURITY_REQ_SIZE_BUNDLE_FILENAME] == {h1_token}
        # conn_reuse / topips / cov: BOTH hours are missing them — the laggards
        # the single-sentinel backfill used to strand.
        for fname in (
            security_dims.SECURITY_CONN_REUSE_BUNDLE_FILENAME,
            security_dims.SECURITY_TOPIPS_BUNDLE_FILENAME,
            security_dims.SECURITY_COV_BUNDLE_FILENAME,
        ):
            assert by_file[fname] == {h1_token, h2_token}, f"{fname} did not heal both hours"
        # 1 (req_size) + 2 + 2 + 2 = 7 files written.
        assert n == 7

    def test_readers_eligibility_gates(self, tmp_path):
        """All four readers return None for: filters present, window too short,
        missing closed-hour rollup file, malformed timestamps."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-7"}
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, src)

            readers = [
                lambda s, e, hf: runner.try_security_req_size_from_rollup(s, e, has_filters=hf),
                lambda s, e, hf: runner.try_security_conn_reuse_from_rollup(s, e, has_filters=hf),
                lambda s, e, hf: runner.try_security_top_ips_from_rollup(s, e, has_filters=hf),
                lambda s, e, hf: runner.try_security_coverage_from_rollup(s, e, has_filters=hf),
            ]

            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                for call in readers:
                    # has_filters → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None
                    # window < 48h → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None
                    # empty bundle dir (no coverage) → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None
                    # malformed timestamps → None
                    assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None

    def test_rollup_readers_match_live_sql(self, tmp_path):
        """Seed known rows across a 50-hour closed window; build the security_dims
        rollups; assert each reader's output is byte-equal to the equivalent live
        SQL run over the SAME rows (one temp table holding all 50 hours)."""
        from backend.core.rollups import security_dims
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-8"}

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]

        def _canonical_rows(hour_dt):
            rows = []
            # A spread of header sizes across several buckets + conn reuse tiers +
            # 3 IPs with distinct max headers + a mix of populated/empty TLS.
            specs = [
                ("10.0.0.1", 100, 1, "abc"),
                ("10.0.0.1", 400, 4, "abc"),
                ("10.0.0.2", 5000, 12, "def"),
                ("10.0.0.2", 9000, 1, ""),
                ("10.0.0.3", 200, 50, None),
                ("10.0.0.3", 200, 200, "ghi"),
            ]
            for j, (ip, rhb, cr, tls) in enumerate(specs):
                rows.append(
                    {
                        "timestamp": hour_dt + timedelta(seconds=j),
                        "ip": ip,
                        "req_header_bytes": rhb,
                        "conn_requests": cr,
                        "tls_ciphers_sha": tls,
                    }
                )
            return rows

        # Build the rollups: seed each closed hour and run the writer once per hour.
        for dt in hours:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs_rollups_security_dims(c, "logs_par", rows)
                return c

            p = _build_patches(cache_root, "logs_par", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                security_dims.build_security_dims_bundles("svc-sd-8", src, [hour_token])

        # Live SQL over one temp table holding ALL 50 hours of the same rows.
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            all_rows: list[dict] = []
            for dt in hours:
                all_rows.extend(_canonical_rows(dt))
            _seed_logs_rollups_security_dims(con, "t_par", all_rows)
            runner = QueryRunner(con, src)

            # ── req_size parity ──
            live_req = con.execute(REQ_HEADER_SIZE_DIST.format(temp_table="t_par")).fetchall()
            live_req_pairs = [(r[0], int(r[1])) for r in live_req]  # (bucket, count) in min_val order

            # ── conn_reuse parity ──
            live_conn = con.execute(CONN_REUSE_DIST.format(temp_table="t_par")).fetchall()
            live_conn_pairs = [(r[0], int(r[1])) for r in live_conn]

            # ── topips parity ── (live LIMIT 10)
            live_topips = con.execute(TOP_IPS_BY_MAX_HEADER.format(temp_table="t_par")).fetchall()
            live_topips_pairs = [(r[0], int(r[1])) for r in live_topips]

            # ── coverage parity ── (one populated col over tls_ciphers_sha)
            agg = "count(*) FILTER (WHERE tls_ciphers_sha IS NOT NULL AND tls_ciphers_sha != '') AS populated_0"
            live_cov = con.execute(FINGERPRINT_COVERAGE_BULK.format(temp_table="t_par", agg_cols=agg)).fetchone()
            live_total, live_pop = int(live_cov[0]), int(live_cov[1])

            # Rollup readers over the 50-hour closed window.
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                roll_req = runner.try_security_req_size_from_rollup(st_iso, et_iso, has_filters=False)
                roll_conn = runner.try_security_conn_reuse_from_rollup(st_iso, et_iso, has_filters=False)
                roll_topips = runner.try_security_top_ips_from_rollup(st_iso, et_iso, has_filters=False)
                roll_cov = runner.try_security_coverage_from_rollup(st_iso, et_iso, has_filters=False)

        assert roll_req is not None and roll_conn is not None and roll_topips is not None and roll_cov is not None

        # req_size: exact bucket→count parity AND same min_val ordering.
        assert [(r["bucket"], r["count"]) for r in roll_req] == live_req_pairs
        assert "_approx" not in {k for d in roll_req for k in d}  # EXACT — no _approx key

        # conn_reuse: exact bucket→count parity + ordering.
        assert [(r["bucket"], r["count"]) for r in roll_conn] == live_conn_pairs

        # topips: top-10 by MAX-of-MAX. Each hour has identical rows so MAX across
        # hours == per-hour MAX == the live MAX over all rows.
        assert [(r["ip"], r["max_header"]) for r in roll_topips] == live_topips_pairs

        # coverage: exact SUM parity.
        assert roll_cov == (live_total, live_pop)

    def test_conn_requests_hist_reader_parity_and_fold(self, tmp_path):
        """try_conn_requests_hist_from_rollup serves the DASHBOARD histogram from
        the security_conn_reuse parquets: labels remapped to the dashboard space
        ('1 (None)'→'1', en-dash ranges), the '21-100'/'>100' tail folded into
        '21+', fixed bucket order, and byte-parity with the dashboard's live
        CONN_REQUESTS_BUCKET SQL over the SAME rows (NULL/0 floor included)."""
        from backend.core.rollups import security_dims
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-cr-1"}

        # 3 closed hours, safely before the active hour so no live top-up fires.
        base_dt = (datetime.now(UTC) - timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(3)]

        def _canonical_rows(hour_dt):
            # One row per rollup bucket INCLUDING both tail buckets (50 → '21-100',
            # 200 → '>100') so the '21+' fold aggregates two source buckets, plus
            # a NULL and a 0 that the conn_requests > 0 floor must drop.
            vals = [1, 1, 3, 10, 50, 200, 0, None]
            return [
                {
                    "timestamp": hour_dt + timedelta(seconds=j),
                    "ip": "10.0.0.1",
                    "req_header_bytes": 100,
                    "conn_requests": v,
                    "tls_ciphers_sha": "abc",
                }
                for j, v in enumerate(vals)
            ]

        for dt in hours:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs_rollups_security_dims(c, "logs_cr", rows)
                return c

            p = _build_patches(cache_root, "logs_cr", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                security_dims.build_security_dims_bundles("svc-cr-1", src, [hour_token])

        # Live dashboard SQL over one table holding ALL hours of the same rows.
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            all_rows: list[dict] = []
            for dt in hours:
                all_rows.extend(_canonical_rows(dt))
            _seed_logs_rollups_security_dims(con, "t_par", all_rows)
            live = con.execute(
                CONN_REQUESTS_BUCKET.format(requests_metric="COUNT(*)", table_name="t_par", where_clause="1=1")
            ).fetchall()
            live_pairs = [(r[0], int(r[1])) for r in live]

            runner = QueryRunner(con, src)
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                hist = runner.try_conn_requests_hist_from_rollup(
                    st_iso, et_iso, has_filters=False, actual_cols=["conn_requests"]
                )

        assert hist is not None
        assert [(r["value"], r["count"]) for r in hist["top"]] == live_pairs
        # Dashboard label space, fixed order, tail folded (50 + 200 → one '21+').
        assert [r["value"] for r in hist["top"]] == ["1", "2–5", "6–20", "21+"]
        assert hist["total"] == sum(c for _b, c in live_pairs)

    def test_conn_requests_hist_reader_eligibility(self, tmp_path):
        """None on: filters present, window < 2h, no bundle coverage, malformed
        timestamps. (The 2h floor is deliberately LOWER than the 48h security
        default — the dashboard fallback is a dedicated window scan.)"""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-cr-2"}
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, src)

            call = lambda s, e, hf: runner.try_conn_requests_hist_from_rollup(s, e, has_filters=hf)  # noqa: E731
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                assert call("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", True) is None
                assert call("2026-06-01T00:00:00Z", "2026-06-01T01:30:00Z", False) is None
                assert call("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", False) is None
                assert call("not-a-date", "2026-06-02T00:00:00Z", False) is None

    def test_conn_requests_hist_reader_live_topup(self, tmp_path):
        """A window crossing the active hour merges the buffer's live rows via the
        direct active-hour read; closed hours stay rollup-served."""
        import pyarrow as pa

        from backend.core.rollups import security_dims
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-cr-3"}

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed = [active_dt - timedelta(hours=2), active_dt - timedelta(hours=1)]

        def _closed_rows(hour_dt):
            # Per closed hour: 2× '1', 1× '2-5'.
            return [
                {
                    "timestamp": hour_dt,
                    "ip": "10.0.0.1",
                    "req_header_bytes": 100,
                    "conn_requests": 1,
                    "tls_ciphers_sha": "x",
                },
                {
                    "timestamp": hour_dt + timedelta(seconds=1),
                    "ip": "10.0.0.1",
                    "req_header_bytes": 100,
                    "conn_requests": 1,
                    "tls_ciphers_sha": "x",
                },
                {
                    "timestamp": hour_dt + timedelta(seconds=2),
                    "ip": "10.0.0.1",
                    "req_header_bytes": 100,
                    "conn_requests": 3,
                    "tls_ciphers_sha": "x",
                },
            ]

        for dt in closed:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _closed_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs_rollups_security_dims(c, "logs_cr3", rows)
                return c

            p = _build_patches(cache_root, "logs_cr3", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                security_dims.build_security_dims_bundles("svc-cr-3", src, [hour_token])

        # Buffer parquet for the active hour: 1× '1', 2× '6–20'. Timestamps a few
        # minutes into the active hour; the window end extends past them so the
        # [live_start, live_end) clamp keeps them regardless of wall-clock offset.
        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir()
        ts = active_dt + timedelta(minutes=5)
        tbl = pa.table(
            {
                "timestamp": pa.array([ts, ts, ts], type=pa.timestamp("us", tz="UTC")),
                "conn_requests": pa.array([1, 7, 7], type=pa.int64()),
            }
        )
        pq.write_table(tbl, str(buffer_dir / "live.parquet"))

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, src)
            st_iso = closed[0].isoformat().replace("+00:00", "Z")
            et_iso = (active_dt + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                hist = runner.try_conn_requests_hist_from_rollup(
                    st_iso, et_iso, has_filters=False, actual_cols=["conn_requests"]
                )

        assert hist is not None
        counts = {r["value"]: r["count"] for r in hist["top"]}
        # Closed: 2 hours × (2× '1', 1× '2–5'); live buffer adds 1× '1', 2× '6–20'.
        assert counts == {"1": 5, "2–5": 2, "6–20": 2}
        assert hist["total"] == 9


# ── Merged from tests/core/test_rollups_security_proxies.py ──
_ROWS = [
    # direct, near its POP: never flagged
    ("10.0.0.1", 100, "IAD", 20000, 21000, 38.9, -77.0, "US", "Ashburn", "Mozilla/5.0", "https://www.site.com/a", 1000),
    # rtt_min/tcp_rtt = 0.02: tunnel
    ("10.0.0.2", 200, "IAD", 1000, 50000, 39.0, -77.1, "US", "Reston", "Mozilla/5.0", None, 500),
    # ratio 0.5: wifi/mobile, not flagged
    ("10.0.0.3", 300, "IAD", 1000, 2000, 39.1, -77.2, "US", "Herndon", "Mozilla/5.0", None, 300),
    # ratio 0.143: flagged (medium) but not a tunnel
    ("10.0.0.4", 400, "IAD", 10000, 70000, 39.2, -77.3, "US", "Sterling", "Mozilla/5.0", None, 200),
    # client in Tokyo claiming a 5 ms RTT to IAD: distance mismatch
    ("10.0.0.5", 500, "IAD", 5000, 6000, 35.0, 139.0, "JP", "Tokyo", "Googlebot/2.1", "https://www.news.com/x", 4000),
    # no RTT data: direct, never flagged
    ("10.0.0.6", 600, "IAD", None, None, 40.0, -75.0, "US", "Philadelphia", "GPTBot-User/1.0", None, 100),
]


def _seed_logs_rollups_security_proxies(
    con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], base: datetime
) -> None:
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ip VARCHAR, asn BIGINT, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, "
        f"  lat DOUBLE, lon DOUBLE, country VARCHAR, city VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT)"
    )
    for j, r in enumerate(rows):
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [base + timedelta(seconds=j), *r]
        )


def _build_hour(cache_root, src, table: str, hour_dt: datetime, rows: list[tuple]) -> int:
    from backend.core.rollups import security_proxies

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs_rollups_security_proxies(c, table, rows, hour_dt)
        return c

    p = _build_patches(cache_root, table, _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        return security_proxies.build_security_proxies_bundles(src["name"], src, [hour_dt.strftime("%Y-%m-%d-%H")])


def _build_window(tmp_path, name: str):
    """Build 50 closed hours of identical rows; return (cache_root, src, hours, live con, runner)."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": name}
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        _build_hour(cache_root, src, "logs_par", dt, _ROWS)

    con = duckdb.connect(":memory:")
    all_rows_table = "t_par"
    con.execute(
        f"CREATE TABLE {all_rows_table} (timestamp TIMESTAMPTZ, ip VARCHAR, asn BIGINT, pop VARCHAR, rtt_min BIGINT, "
        f"tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, country VARCHAR, city VARCHAR, ua VARCHAR, referer VARCHAR, "
        f"resp_bytes BIGINT)"
    )
    for dt in hours:
        for j, r in enumerate(_ROWS):
            con.execute(
                f"INSERT INTO {all_rows_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [dt + timedelta(seconds=j), *r],
            )
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    return cache_root, QueryRunner(con, src), con, st_iso, et_iso


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsSecurityProxies:
    def test_writer_emits_three_bundles_with_only_flagged_rows(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-1"}
        hour_token, hour_dt = _past_hour(2)

        assert _build_hour(cache_root, src, "logs_sp1", hour_dt, _ROWS) == 3

        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        quality = {
            r["type"]: r["count"] for r in pq.read_table(str(hour_dir / "security_proxy_quality.parquet")).to_pylist()
        }
        assert quality == {"Direct Connection": 3, "Active Tunnel / Proxy": 1, "WiFi / Mobile": 2}

        flagged = pq.read_table(str(hour_dir / "security_proxy_flagged.parquet")).to_pylist()
        assert sorted(r["ip"] for r in flagged) == ["10.0.0.2", "10.0.0.4", "10.0.0.5"]
        assert all(r["cnt"] == 1 for r in flagged)

        bot_eff = pq.read_table(str(hour_dir / "security_bot_eff.parquet")).to_pylist()
        site = [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"]
        assert site == [(6, 6100)]
        assert {r["key"] for r in bot_eff if r["kind"] == "ua"} == {"Mozilla/5.0", "Googlebot/2.1", "GPTBot-User/1.0"}

    def test_writer_skips_services_without_the_required_columns(self, tmp_path):
        from backend.core.rollups import security_proxies

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-2"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_sp2 (timestamp TIMESTAMPTZ, ua VARCHAR, ip VARCHAR)")
            c.execute("INSERT INTO logs_sp2 VALUES (?, 'x', '10.0.0.1')", [hour_dt])
            return c

        p = _build_patches(cache_root, "logs_sp2", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = security_proxies.build_security_proxies_bundles("svc-sp-2", src, [hour_token])

        # Only bot_eff needs just ua + ip; the proxy bundles need the RTT/geo columns.
        assert n == 1
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "security_bot_eff.parquet").exists()
        assert not (hour_dir / "security_proxy_flagged.parquet").exists()
        assert not (hour_dir / "security_proxy_quality.parquet").exists()

    def test_readers_eligibility_gates(self, tmp_path):
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-3"})
        readers = [
            lambda s, e, hf: runner.try_security_proxies_from_rollup(s, e, has_filters=hf),
            lambda s, e, hf: runner.try_security_bot_efficiency_from_rollup(s, e, has_filters=hf),
        ]
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            for call in readers:
                assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None  # filters
                assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None  # < 48 h
                assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None  # nothing built
                assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None

    def test_proxies_reader_matches_live_sql(self, tmp_path):
        from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED

        cache_root, runner, con, st_iso, et_iso = _build_window(tmp_path, "svc-sp-4")

        live = con.execute(
            GET_SECURITY_PROXIES_COMBINED.format(
                temp_table="t_par",
                select_country_city_inner=",\n            country,\n            city",
                select_country_city_outer=",\n                country,\n                city",
            )
        ).fetchall()
        live_stats = next(r for r in live if r[0] == "stats")
        live_quality = sorted((r[1], r[2]) for r in live if r[0] == "quality")
        live_isps = sorted((r[2], r[3]) for r in live if r[0] == "isp")
        # The live scan repeats a client once per request; the rollup groups identical tuples.
        live_clients = {tuple(r[1:3]) + tuple(r[5:]) for r in live if r[0] == "client"}

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled is not None
        assert rolled["stats"] == {
            "active_proxies_count": live_stats[2],
            "total_requests_count": live_stats[3],
            "distance_mismatches_count": live_stats[4],
        }
        assert sorted(rolled["quality_rows"]) == live_quality
        assert sorted((r["asn"], r["count"]) for r in rolled["isps"]) == live_isps
        rolled_clients = {
            (
                c["ip"],
                c["asn"],
                c["rtt_min_ms"],
                c["tcp_rtt_ms"],
                c["distance_km"],
                c["pop"],
                c["client_lat"],
                c["client_lon"],
                c["pop_lat"],
                c["pop_lon"],
                c["impossible_distance"],
                c["risk_level"],
                c["country"],
                c["city"],
            )
            for c in rolled["clients"]
        }
        # The live scan returns one row per request, so its 100-row cap fills with the 100 High-risk
        # repeats (50 hours x 2 IPs) and drops the Medium-risk client; the grouped rollup lists it once.
        assert live_clients <= rolled_clients
        assert {c[0] for c in rolled_clients} == {"10.0.0.2", "10.0.0.4", "10.0.0.5"}
        assert len(rolled["clients"]) == len(rolled_clients) == 3
        # Sanity on the seeded scenario: tunnel + tokyo mismatch counted, the 0.143 ratio row is not a tunnel.
        assert rolled["stats"]["active_proxies_count"] == 2
        assert rolled["stats"]["distance_mismatches_count"] == 1
        assert rolled["stats"]["total_requests_count"] == 2 * 50

    def test_bot_efficiency_reader_matches_live_sql_with_documented_ref_overcount(self, tmp_path):
        from backend.repositories._sql.security import BOT_EFFICIENCY

        cache_root, runner, con, st_iso, et_iso = _build_window(tmp_path, "svc-sp-5")

        live = con.execute(
            BOT_EFFICIENCY.format(
                temp_table="t_par", resp_bytes_expr="COALESCE(t.resp_bytes, 0)", referer_expr="t.referer"
            )
        ).fetchall()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_bot_efficiency_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled is not None
        live_by_kind: dict[str, dict[str, tuple[int, int]]] = {}
        for kind, key, cnt, rb in live:
            live_by_kind.setdefault(kind, {})[key] = (int(cnt), int(rb))
        rolled_by_kind: dict[str, dict[str, tuple[int, int]]] = {}
        for kind, key, cnt, rb in rolled:
            rolled_by_kind.setdefault(kind, {})[key] = (cnt, rb)

        assert rolled_by_kind["site"] == live_by_kind["site"]
        assert rolled_by_kind["ua"] == live_by_kind["ua"]
        # The same IPs recur every hour, so SUM(per-hour distinct) is exactly hours x the true distinct count.
        assert set(rolled_by_kind["ref"]) == set(live_by_kind["ref"])
        for stem, (cnt, _) in rolled_by_kind["ref"].items():
            assert cnt == live_by_kind["ref"][stem][0] * 50

    def test_day_compaction_merges_hours_exactly(self, tmp_path):
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-6"}
        day = (datetime.now(UTC) - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(24):
            _build_hour(cache_root, src, "logs_day", day + timedelta(hours=h), _ROWS)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_security_proxies_closed_days_to_daily("svc-sp-6", src)
        assert n == 3

        day_root = cache_root / "rollups" / "day_bundled" / f"day={day.strftime('%Y-%m-%d')}"
        quality = {
            r["type"]: r["count"] for r in pq.read_table(str(day_root / "security_proxy_quality.parquet")).to_pylist()
        }
        assert quality == {"Direct Connection": 3 * 24, "Active Tunnel / Proxy": 24, "WiFi / Mobile": 2 * 24}
        flagged = pq.read_table(str(day_root / "security_proxy_flagged.parquet")).to_pylist()
        assert sorted((r["ip"], r["cnt"]) for r in flagged) == [("10.0.0.2", 24), ("10.0.0.4", 24), ("10.0.0.5", 24)]
        bot_eff = pq.read_table(str(day_root / "security_bot_eff.parquet")).to_pylist()
        assert [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"] == [(6 * 24, 6100 * 24)]

    def test_proxies_reader_handles_sparse_and_empty_flagged_hours(self, tmp_path):
        """Hours with 0 flagged proxy rows write a .done marker instead of
        security_proxy_flagged.parquet; the reader must still serve from rollups."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-7"}
        base_dt = (datetime.now(UTC) - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(24)]
        direct_only = [_ROWS[0]]  # Direct connection, never flagged
        for dt in hours:
            _build_hour(cache_root, src, "logs_sparse", dt, direct_only)

        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)
        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled is not None
        assert rolled["stats"] == {
            "active_proxies_count": 0,
            "total_requests_count": 0,
            "distance_mismatches_count": 0,
        }
        assert rolled["quality_rows"] == [("Direct Connection", 24)]
        assert rolled["isps"] == []
        assert rolled["clients"] == []

    def test_blank_ip_requests_never_appear_as_suspicious_clients(self, tmp_path):
        """Requests logged before the capture snippet ran (redirects) carry no IP, UA or country but
        still have edge RTT/geo, so they used to pass the proxy flags and render as blank-IP rows."""
        from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED

        blank = ("", 700, "IAD", 1000, 50000, 39.0, -77.1, "", "", "", None, 0)
        rows = [*_ROWS, blank]
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        for dt in hours:
            _build_hour(cache_root, {"name": "svc-sp-blank"}, "logs_par", dt, rows)

        live_con = duckdb.connect(":memory:")
        _seed_logs_rollups_security_proxies(live_con, "t_blank", rows, hours[0])
        live = live_con.execute(
            GET_SECURITY_PROXIES_COMBINED.format(
                temp_table="t_blank",
                select_country_city_inner=",\n            country,\n            city",
                select_country_city_outer=",\n                country,\n                city",
            )
        ).fetchall()
        assert [r[1] for r in live if r[0] == "client" and not r[1]] == []
        assert next(r for r in live if r[0] == "stats")[2] == 2  # active proxies: the tunnel + the Tokyo mismatch

        from backend.repositories._base import QueryRunner

        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-blank"})
        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)
        assert rolled is not None
        assert all(c["ip"] for c in rolled["clients"])
        assert rolled["stats"]["active_proxies_count"] == 2


# ── Merged from tests/core/test_rollups_verified_bots_ts.py ──
def _write_hour_vbts(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "bucket_ts": pa.array([r["bucket_ts"] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "bot_type": pa.array([r["bot_type"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "verified_bots_ts.parquet")
    pq.write_table(table, p)
    return p


def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsVerifiedBotsTs:
    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT verified_bots_ts.parquet."""
        from backend.core.rollups import verified_bots_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-vbts-bf", "service_id": "svc-vbts-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_vbts(
            str(cache_root), f"{yday}-11", [{"bucket_ts": _hour_dt(yday, 11), "bot_type": "google", "count": 3}]
        )

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(verified_bots_ts, "build_verified_bots_ts_bundles", side_effect=_stub_build):
                n = verified_bots_ts.backfill_verified_bots_ts_bundles("svc-vbts-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-vbts-r", "service_id": "svc-vbts-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_verified_bots_ts_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            temp_table="logs_x_tmp",
            bucket_seconds=3600,
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-vbts-r2", "service_id": "svc-vbts-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_verified_bots_ts_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            temp_table="logs_x_tmp",
            bucket_seconds=3600,
            has_filters=False,
        )
        assert result is None

    def test_reader_returns_none_for_non_minute_bucket(self, tmp_path):
        """bucket_seconds not a multiple of 60 → minute-granular re-bucketing
        would be inexact, so the reader bails to the live path."""
        src = {"name": "svc-vbts-r2b", "service_id": "svc-vbts-r2b"}
        runner = _stub_runner(src, [], [])
        result = runner.try_verified_bots_ts_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            temp_table="logs_x_tmp",
            bucket_seconds=90,  # not a multiple of 60
            has_filters=False,
        )
        assert result is None

    def test_reader_serves_hybrid_rows(self, tmp_path):
        """Eligible window → reader builds a UNION ALL of read_parquet (closed
        hours) + a scoped live temp-table query (active hour), and returns the
        (bucket_ts, bot_type, count) rows."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-vbts-r3", "service_id": "svc-vbts-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_vbts(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [{"bucket_ts": _hour_dt(d_iso, h), "bot_type": "google", "count": 5}],
                )

        captured: list[str] = []
        stub_rows = [(_hour_dt(day_a, 0), "google", 120), (_hour_dt(day_a, 1), "bing", 40)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_verified_bots_ts_from_rollup(
                st_iso,
                end_iso,
                temp_table="logs_x_tmp",
                bucket_seconds=3600,
                has_filters=False,
            )

        assert result == [(_hour_dt(day_a, 0), "google", 120), (_hour_dt(day_a, 1), "bing", 40)]
        assert len(captured) == 1
        sql = captured[0]
        assert "verified_bots_ts.parquet" in sql
        assert "read_parquet" in sql
        assert "UNION ALL" in sql
        assert "logs_x_tmp" in sql
        assert "unnest" in sql
        assert "VERIFIED-BOT." in sql
        assert "time_bucket(INTERVAL '3600 seconds'" in sql

    def test_compact_writes_per_day_file_preserving_minutes(self, tmp_path):
        """24 hour files for a closed day → 1 day file that PRESERVES minute
        granularity (one bucket_ts per source minute), unlike the leaderboard
        compactors which collapse the time dimension."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-vbts-cd", "service_id": "svc-vbts-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_vbts(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {"bucket_ts": _hour_dt(day, h), "bot_type": "google", "count": 10},
                    {"bucket_ts": _hour_dt(day, h), "bot_type": "bing", "count": 4},
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_verified_bots_ts_closed_days_to_daily("svc-vbts-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "verified_bots_ts.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            n_rows, n_buckets, n_google, n_bing = con.execute(
                f"SELECT count(*), count(DISTINCT bucket_ts), "
                f"       SUM(count) FILTER (WHERE bot_type='google'), "
                f"       SUM(count) FILTER (WHERE bot_type='bing') "
                f"FROM read_parquet('{day_file}')"
            ).fetchone()
        finally:
            con.close()
        # 24 hours × 2 bot_types = 48 rows; 24 distinct minute buckets;
        # google total 24×10=240, bing 24×4=96.
        assert n_rows == 48
        assert n_buckets == 24
        assert n_google == 240
        assert n_bing == 96

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-vbts-active", "service_id": "svc-vbts-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_vbts(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"bucket_ts": _hour_dt(today, h), "bot_type": "google", "count": 5}],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_verified_bots_ts_closed_days_to_daily("svc-vbts-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_wellknown_bots.py ──
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


def _write_data_dir(cache_root: str, hour: str) -> None:
    """Mark a closed hour as touched by the recompute pipeline (it carries an
    ``all_fields.parquet`` hour bundle). The backfill only probes for the file's
    presence here (recompute, which actually reads the view, is mocked in these
    tests), so a dummy file suffices."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "all_fields.parquet"), "wb") as f:
        f.write(b"x")


class TestRollupsWellknownBots:
    def test_read_returns_rows_when_window_covered_and_versions_match(self, tmp_path):
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

    def test_read_falls_back_below_48h_window(self, tmp_path):
        """Narrow windows (< 48 h) stay on the live path — the closed-hour read
        doesn't amortise there and live also covers the active hour."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-bot-narrow"}

        # Seed a couple of closed hours, query only a 12 h span (< 23 h gate).
        h2, _ = _past_hour_iso(2)
        h3, _ = _past_hour_iso(3)
        _, end_iso = _past_hour_iso(2)
        _, start_iso = _past_hour_iso(14)  # 12 h span

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 50)])
            _write_rollup_hour(str(cache_root), h3, [("Bingbot/2.0", "157.55.39.1", 20)])
            rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

        assert rows is None, "windows < 48 h must fall back to live"

    def test_read_falls_back_below_coverage_floor(self, tmp_path):
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

    def test_read_serves_closed_hours_dropping_active_hour(self, tmp_path):
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

    def test_read_falls_back_when_pattern_set_version_stale(self, tmp_path):
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

    def test_read_returns_empty_list_when_window_covered_but_no_bot_traffic(self, tmp_path):
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

    def test_read_falls_back_when_pattern_set_version_missing(self, tmp_path):
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

    def test_read_falls_back_when_time_range_exceeds_366_days(self):
        """Spans wider than 366 days short-circuit to None before the hour loop
        (guards against an attacker-influenced multi-year range)."""
        from backend.core import rollups

        _, end_iso = _past_hour_iso(2)
        start_dt = datetime.now(UTC) - timedelta(days=500)
        start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        src = {"name": "svc-bot-range-guard"}

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)
        assert rows is None, "spans > 366 days must short-circuit to None"

    def test_backfill_wellknown_rebuilds_data_backed_and_sweeps_unrebuildable_stale(self, tmp_path):
        """The self-heal pass is keyed on closed hours that have an all_fields hour bundle:

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
        _write_rollup_hour(
            str(cache_root), h_stale_data, [("Bingbot/2.0", "157.55.39.1", 3)], pattern_set_version="vOLD"
        )
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
