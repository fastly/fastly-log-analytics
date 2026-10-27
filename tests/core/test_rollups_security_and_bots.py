"""Tests for ngwaf_bots, security_dims, security_proxies, verified_bots_ts, and wellknown_bots rollups."""

from __future__ import annotations

import contextlib
import os
import sqlite3
import urllib.parse
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vcr
from tests._helpers.rollups import (
    build_writer_patches as _build_patches,
)
from tests._helpers.rollups import (
    make_stub_runner as _stub_runner,
)
from tests._helpers.rollups import (
    noop_lock as _noop_lock,
)
from tests._helpers.rollups import (
    past_hour as _past_hour,
)
from tests._helpers.rollups import (
    three_days_ago_iso as _three_days_ago_iso,
)
from tests._helpers.rollups import (
    two_days_ago_iso as _two_days_ago_iso,
)
from tests._helpers.rollups import (
    write_hour_all_fields as _write_hour_all_fields,
)
from tests._helpers.rollups import (
    write_hour_bundle as _write_hour_bundle,
)
from tests._helpers.rollups import (
    yesterday_iso as _yesterday_iso,
)

import backend.core.metadata.base as pg_connection
import backend.core.rollups.security_dims as nb
import backend.core.rollups.security_dims as security_proxies
import backend.core.rollups.security_dims as verified_bots_ts
import backend.core.rollups.security_dims as wellknown_bots
import backend.repositories.security as SQL
from backend.core import rollups
from backend.core.metadata.usage_log import CronLeaseHeldError
from backend.core.rollups import day_bundles, security_dims
from backend.core.rollups.day_bundles import (
    compact_ngwaf_bots_closed_days_to_daily,
    compact_ngwaf_bots_ts_closed_days_to_daily,
)
from backend.core.rollups.security_dims import (
    _extract_bot_record,
    _get_relative_time_range,
    _purge_stale_ngwaf_bots_parquets,
    cleanup_old_bots,
    fetch_verified_bots_paged,
    get_cache_stats,
    get_last_timestamp,
    oldest_unenriched_timestamp,
    upsert_bots,
)
from backend.repositories._base import QueryRunner, attach_ngwaf_cache, ensure_ngwaf_bots_materialized, safe_iso
from backend.repositories.dashboard import CONN_REQUESTS_BUCKET
from backend.repositories.security import (
    BOT_EFFICIENCY,
    CONN_REUSE_DIST,
    FINGERPRINT_COVERAGE_BULK,
    GET_SECURITY_PROXIES_COMBINED,
    REQ_HEADER_SIZE_DIST,
    TOP_IPS_BY_MAX_HEADER,
    _build_security_response,
)

# ── Merged from tests/core/test_rollups_ngwaf_bots.py ──


@contextlib.contextmanager
def _writer_ctx(cache_root, table: str, con_fn, db_path: str | None = None):
    p = _build_patches(cache_root, table, con_fn)
    with contextlib.ExitStack() as stack:
        for cm in p:
            stack.enter_context(cm)
        if db_path is not None:
            stack.enter_context(patch("backend.config.ngwaf_db_path", return_value=db_path))
        yield


@contextlib.contextmanager
def _compact_ctx(cache_root):
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
    ):
        yield


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, waf_req_id VARCHAR)")
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [r["timestamp"], r.get("waf_req_id")])


def _create_cache_db(db_path: str, entries: list[tuple]) -> None:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ngwaf_bots ("
        " waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT,"
        " wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    for entry in entries:
        wid, name, cat = entry[:3]
        wk_name = entry[3] if len(entry) == 4 else None
        con.execute(
            "INSERT INTO ngwaf_bots VALUES (?, ?, ?, NULL, ?, '2026-01-01T00:00:00Z')", (wid, name, cat, wk_name)
        )
    con.commit()
    con.close()


def _sqlite_duckdb(table: str, rows: list[dict]):
    def _con():
        c = duckdb.connect(":memory:")
        c.execute("INSTALL sqlite; LOAD sqlite;")
        _seed_logs(c, table, rows)
        return c

    return _con


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNgwafBots:
    def test_build_ngwaf_bots_writes_aggregated_join(self, tmp_path):
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
        hour_token, hour_dt = _past_hour(2)
        rows = [
            {"timestamp": hour_dt + timedelta(seconds=i), "waf_req_id": wid}
            for i, wid in enumerate(["w1", "w2", "w3", "unmatched", None])
        ]
        with _writer_ctx(cache_root, "logs_nb", _sqlite_duckdb("logs_nb", rows), db_path):
            assert nb.build_ngwaf_bots_bundles("svc-nb-1", {"name": "svc-nb-1"}, [hour_token]) == 1

        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "ngwaf_bots.parquet"
        assert out.is_file()
        got = {
            (r["bot_name"], r["wellknown_bot_name"], r["category"]): r["count"]
            for r in pq.read_table(str(out)).to_pylist()
        }
        assert got == {("GPTBot", "GPTBot", "AI-CRAWLER"): 2, ("AhrefsBot", None, "SEO"): 1}

    def test_build_ngwaf_bots_zero_bot_and_skip_conditions(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        db_path = str(tmp_path / "ngwaf_bot_cache.db")
        _create_cache_db(db_path, [])
        hour_token, hour_dt = _past_hour(2)

        with _writer_ctx(
            cache_root, "logs_nb2", _sqlite_duckdb("logs_nb2", [{"timestamp": hour_dt, "waf_req_id": "wX"}]), db_path
        ):
            assert nb.build_ngwaf_bots_bundles("svc-nb-2", {"name": "svc-nb-2"}, [hour_token]) == 1
        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "ngwaf_bots.parquet"
        assert out.is_file() and pq.read_table(str(out)).num_rows == 0

        def _con_no_col():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_nb3 (timestamp TIMESTAMPTZ)")
            return c

        with _writer_ctx(cache_root, "logs_nb3", _con_no_col, db_path):
            assert nb.build_ngwaf_bots_bundles("svc-nb-3", {"name": "svc-nb-3"}, [hour_token]) == 0

        with _writer_ctx(
            cache_root,
            "logs_nb3b",
            _sqlite_duckdb("logs_nb3b", [{"timestamp": hour_dt, "waf_req_id": "w1"}]),
            str(tmp_path / "missing.db"),
        ):
            assert nb.build_ngwaf_bots_bundles("svc-nb-3", {"name": "svc-nb-3"}, [hour_token]) == 0

    def test_compact_ngwaf_bots_and_ts_days_sums_hours(self, tmp_path):
        cache_root = tmp_path / "cache"
        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day = day_dt.strftime("%Y-%m-%d")
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
                ),  # fmt: skip
                str(hour_dir / "ngwaf_bots.parquet"),
            )
            pq.write_table(
                pa.table(
                    {
                        "bucket_ts": pa.array([day_dt + timedelta(hours=h)], type=pa.timestamp("us", tz="UTC")),
                        "bot_name": ["GPTBot"],
                        "count": pa.array([3], type=pa.int64()),
                    }
                ),  # fmt: skip
                str(hour_dir / "ngwaf_bots_ts.parquet"),
            )

        with _compact_ctx(cache_root):
            assert compact_ngwaf_bots_closed_days_to_daily("svc-nb-4", {"name": "svc-nb-4"}) == 1
            assert compact_ngwaf_bots_ts_closed_days_to_daily("svc-nb-4-ts", {"name": "svc-nb-4-ts"}) == 1

        out = cache_root / "rollups" / "day_bundled" / f"day={day}" / "ngwaf_bots.parquet"
        rows = pq.read_table(str(out)).to_pylist()
        assert [{k: r[k] for k in ("bot_name", "wellknown_bot_name", "category", "count")} for r in rows] == [
            {"bot_name": "GPTBot", "wellknown_bot_name": "GPTBot", "category": "AI-CRAWLER", "count": 48}
        ]
        ts_rows = pq.read_table(
            str(cache_root / "rollups" / "day_bundled" / f"day={day}" / "ngwaf_bots_ts.parquet")
        ).to_pylist()
        assert len(ts_rows) == 24 and sum(r["count"] for r in ts_rows) == 72

    def test_stale_3col_ngwaf_bots_parquet_fallback_and_purge(self, tmp_path):
        cache_root = tmp_path / "cache"
        base = (datetime.now(UTC) - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0)
        for i in range(24):
            hour_dir = (
                cache_root / "rollups" / "hour_bundled" / f"hour={(base + timedelta(hours=i)).strftime('%Y-%m-%d-%H')}"
            )
            hour_dir.mkdir(parents=True)
            pq.write_table(
                pa.table({"bot_name": ["GPTBot"], "category": ["AI-CRAWLER"], "count": pa.array([2], type=pa.int64())}),
                str(hour_dir / "ngwaf_bots.parquet"),
            )

        src = {"name": "svc-nb-trap24"}
        runner = QueryRunner(duckdb.connect(":memory:"), src)
        st, et = base.isoformat(), (base + timedelta(hours=24)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            assert runner.try_ngwaf_verified_bots_from_rollup(st, et, has_filters=False) is None
            assert _purge_stale_ngwaf_bots_parquets(src) == 24

    def test_ngwaf_verified_bots_and_ts_parity_with_live_sql_and_temp_skip(self, tmp_path):
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
        with _writer_ctx(cache_root, "logs_parity", _fresh_con, db_path):
            assert nb.build_ngwaf_bots_bundles("svc-nb-parity", src, closed_hours) == 24
            assert nb.build_ngwaf_bots_ts_bundles("svc-nb-parity", src, closed_hours) == 24

        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir()
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([h_active], type=pa.timestamp("us", tz="UTC")),
                    "waf_req_id": pa.array(["w2"], type=pa.string()),
                }
            ),  # fmt: skip
            str(buffer_dir / "live.parquet"),
        )

        con = _fresh_con()
        st_iso, et_iso = base_dt.isoformat(), (active_dt + timedelta(minutes=30)).isoformat()
        with (
            patch("backend.config.ngwaf_db_path", return_value=db_path),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        ):
            assert ensure_ngwaf_bots_materialized(con, "ngwaf_cache")
            runner = QueryRunner(con, src)
            runner.get_schema_cols = lambda: ["timestamp", "waf_req_id"]  # type: ignore[method-assign]

            live_table_rows = [
                {"bot_name": r[0], "wellknown_bot_name": r[1], "category": r[2], "request_count": r[3]}
                for r in con.execute(SQL.NGWAF_VERIFIED_BOTS.format(temp_table="logs_parity")).fetchall()
            ]
            assert runner.try_ngwaf_verified_bots_from_rollup(st_iso, et_iso, has_filters=False) == live_table_rows

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
                assert [{"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]} for r in rolled_ts_raw] == live_ts

            section_timings: list[dict] = []
            resp = _build_security_response(
                runner, src, ["timestamp", "waf_req_id"], "logs_parity", "", None, 60, section_timings,
                start_time=st_iso, end_time=et_iso, filters=None, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
            )  # fmt: skip
            assert resp["ngwaf_verified_bots"] == live_table_rows
            phase_names = {entry.get("name") or entry.get("section") for entry in section_timings}
            assert {
                "security:temp_skipped",
                "ngwaf_verified_bots_rollup",
                "ngwaf_verified_bots_ts_rollup",
            } <= phase_names
            assert "temp_table_create" not in phase_names and "security:temp_ngwaf_narrowed" not in phase_names

    def test_reader_sums_ranks_eligibility_and_live_topup(self, tmp_path):
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
                ),  # fmt: skip
                str(hour_dir / "ngwaf_bots.parquet"),
            )

        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-nb-5"})
        st, et = base.isoformat(), (base + timedelta(hours=3)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            got = runner.try_ngwaf_top_bots_from_rollup(st, et, has_filters=False, n=10)
            top1 = runner.try_ngwaf_top_bots_from_rollup(st, et, has_filters=False, n=1)
        assert got == [
            {"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 15},
            {"name": "AhrefsBot", "category": "SEO", "request_count": 3},
        ]
        assert top1 == [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 15}]

        empty_cache = tmp_path / "empty_cache"
        empty_cache.mkdir()
        runner_empty = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-nb-6"})
        with patch("backend.core.duckdb._cache_dir", return_value=str(empty_cache)):
            for s, e, hf in [
                ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", True),
                ("2026-06-01T00:00:00Z", "2026-06-01T01:30:00Z", False),
                ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", False),
                ("not-a-date", "2026-06-02T00:00:00Z", False),
            ]:
                assert runner_empty.try_ngwaf_top_bots_from_rollup(s, e, has_filters=hf, n=10) is None
            assert (
                runner_empty.try_ngwaf_verified_bots_from_rollup(
                    "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", has_filters=True
                )
                is None
            )
            assert (
                runner_empty.try_ngwaf_verified_bots_from_rollup(
                    "2026-06-01T00:00:00Z", "2026-06-01T12:00:00Z", has_filters=False
                )
                is None
            )
            for bs, hf, e in [
                (60, True, "2026-06-02T00:00:00Z"),
                (45, False, "2026-06-02T00:00:00Z"),
                (60, False, "2026-06-01T12:00:00Z"),
            ]:
                assert (
                    runner_empty.try_ngwaf_verified_bots_ts_from_rollup(
                        "2026-06-01T00:00:00Z", e, bucket_seconds=bs, has_filters=hf
                    )
                    is None
                )

    def test_reader_live_topup_joins_active_hour_buffer(self, tmp_path):
        cache_root = tmp_path / "cache"
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed = [active_dt - timedelta(hours=2), active_dt - timedelta(hours=1)]
        for dt in closed:
            hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={dt.strftime('%Y-%m-%d-%H')}"
            hour_dir.mkdir(parents=True)
            pq.write_table(
                pa.table({"bot_name": ["GPTBot"], "category": ["AI-CRAWLER"], "count": pa.array([2], type=pa.int64())}),
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
            ),  # fmt: skip
            str(buffer_dir / "live.parquet"),
        )

        db_path = str(tmp_path / "ngwaf_bot_cache.db")
        _create_cache_db(db_path, [("w1", "GPTBot", "AI-CRAWLER")])
        con = duckdb.connect(":memory:")
        con.execute("INSTALL sqlite; LOAD sqlite;")
        with (
            patch("backend.config.ngwaf_db_path", return_value=db_path),
            attach_ngwaf_cache(con, ["waf_req_id"], "ngwaf_top"),
        ):
            runner = QueryRunner(con, {"name": "svc-nb-7"})
            runner.get_schema_cols = lambda: ["timestamp", "waf_req_id"]  # type: ignore[method-assign]
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                got = runner.try_ngwaf_top_bots_from_rollup(
                    closed[0].isoformat(), (active_dt + timedelta(minutes=30)).isoformat(), has_filters=False, n=10
                )
            assert got == [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 5}]


# ── Merged from tests/core/test_rollups_security_dims.py ──


def _seed_logs_rollups_security_dims(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, conn_requests BIGINT, tls_ciphers_sha VARCHAR)"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("ip"), r.get("req_header_bytes"), r.get("conn_requests"), r.get("tls_ciphers_sha")],
        )


def _sd_con_fn(table: str, rows: list[dict]):
    def _con():
        c = duckdb.connect(":memory:")
        _seed_logs_rollups_security_dims(c, table, rows)
        return c

    return _con


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsSecurityDims:
    def test_build_security_dims_writes_four_bundles(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        hour_token, hour_dt = _past_hour(2)
        specs = [("10.0.0.1", 100, 1, "abc"), ("10.0.0.1", 300, 3, "abc"), ("10.0.0.2", 5000, 10, "def"), ("10.0.0.2", 200, 1, "def"), ("10.0.0.3", 50, 1, ""), ("10.0.0.3", 50, 1, None)]  # fmt: skip
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "ip": ip,
                "req_header_bytes": rhb,
                "conn_requests": cr,
                "tls_ciphers_sha": tls,
            }  # fmt: skip
            for i, (ip, rhb, cr, tls) in enumerate(specs)
        ]

        with _writer_ctx(cache_root, "logs_sd", _sd_con_fn("logs_sd", rows)):
            assert security_dims.build_security_dims_bundles("svc-sd-1", {"name": "svc-sd-1"}, [hour_token]) == 4

        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        req_file, conn_file = hour_dir / "security_req_size.parquet", hour_dir / "security_conn_reuse.parquet"
        topips_file, cov_file = hour_dir / "security_topips.parquet", hour_dir / "security_cov.parquet"
        assert req_file.exists() and conn_file.exists() and topips_file.exists() and cov_file.exists()
        assert {"bucket", "count", "min_val"} <= set(pq.read_table(str(req_file)).column_names)
        assert {"bucket", "count", "min_val"} <= set(pq.read_table(str(conn_file)).column_names)
        assert {"ip", "max_header"} <= set(pq.read_table(str(topips_file)).column_names)
        assert {"total_rows", "tls_populated"} <= set(pq.read_table(str(cov_file)).column_names)

        req_rows = {r["bucket"]: (r["count"], r["min_val"]) for r in pq.read_table(str(req_file)).to_pylist()}
        assert req_rows["0-256B"] == (4, 50) and req_rows["256-512B"] == (1, 300) and req_rows["4-6KB"] == (1, 5000)

        conn_rows = {r["bucket"]: (r["count"], r["min_val"]) for r in pq.read_table(str(conn_file)).to_pylist()}
        assert conn_rows["1 (None)"] == (4, 1) and conn_rows["2-5"] == (1, 3) and conn_rows["6-20"] == (1, 10)

        assert {r["ip"]: r["max_header"] for r in pq.read_table(str(topips_file)).to_pylist()} == {"10.0.0.1": 300, "10.0.0.2": 5000, "10.0.0.3": 50}  # fmt: skip
        cov = pq.read_table(str(cov_file)).to_pylist()[0]
        assert cov["total_rows"] == 6 and cov["tls_populated"] == 4

    def test_build_security_dims_skips_active_hour_and_missing_cols(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        with _writer_ctx(cache_root, "logs_sd", _sd_con_fn("logs_sd", [])):
            assert security_dims.build_security_dims_bundles("svc-sd-2", {"name": "svc-sd-2"}, [active_token]) == 0
        assert not (
            cache_root / "rollups" / "hour_bundled" / f"hour={active_token}" / "security_req_size.parquet"
        ).exists()

        hour_token, hour_dt = _past_hour(2)

        def _con_nocr():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_nocr (timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, tls_ciphers_sha VARCHAR)"
            )
            for i in range(4):
                c.execute(
                    "INSERT INTO logs_nocr VALUES (?, ?, ?, ?)",
                    [hour_dt + timedelta(minutes=i), "10.0.0.1", 100 + i, "abc"],
                )
            return c

        c_nocr = tmp_path / "cache_nocr"
        c_nocr.mkdir()
        with _writer_ctx(c_nocr, "logs_nocr", _con_nocr):
            assert security_dims.build_security_dims_bundles("svc-sd-3", {"name": "svc-sd-3"}, [hour_token]) == 3
        d_nocr = c_nocr / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (d_nocr / "security_req_size.parquet").exists() and (d_nocr / "security_topips.parquet").exists()
        assert (d_nocr / "security_cov.parquet").exists() and not (d_nocr / "security_conn_reuse.parquet").exists()

        def _con_noip():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_noip (timestamp TIMESTAMPTZ, req_header_bytes BIGINT, conn_requests BIGINT, tls_ciphers_sha VARCHAR)"
            )
            for i in range(4):
                c.execute(
                    "INSERT INTO logs_noip VALUES (?, ?, ?, ?)", [hour_dt + timedelta(minutes=i), 100 + i, 1, "abc"]
                )
            return c

        c_noip = tmp_path / "cache_noip"
        c_noip.mkdir()
        with _writer_ctx(c_noip, "logs_noip", _con_noip):
            assert security_dims.build_security_dims_bundles("svc-sd-3b", {"name": "svc-sd-3b"}, [hour_token]) == 3
        d_noip = c_noip / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (d_noip / "security_req_size.parquet").exists() and (d_noip / "security_conn_reuse.parquet").exists()
        assert (d_noip / "security_cov.parquet").exists() and not (d_noip / "security_topips.parquet").exists()

    def test_compact_security_dims_day_merge_and_topips_recap(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day_dt.strftime("%Y-%m-%d")
        bundled_root = cache_root / "rollups" / "hour_bundled"

        for hh in (3, 4):
            hour_dir = str(bundled_root / f"hour={(day_dt + timedelta(hours=hh)).strftime('%Y-%m-%d-%H')}")
            _write_hour_bundle(
                hour_dir, "security_req_size.parquet",
                "('0-256B', CAST(4 AS BIGINT), CAST(50 AS BIGINT))" if hh == 3 else "('0-256B', CAST(6 AS BIGINT), CAST(80 AS BIGINT))",
                "bucket, count, min_val",
            )  # fmt: skip
            _write_hour_bundle(
                hour_dir, "security_conn_reuse.parquet",
                "('1 (None)', CAST(2 AS BIGINT), CAST(1 AS BIGINT))" if hh == 3 else "('1 (None)', CAST(3 AS BIGINT), CAST(1 AS BIGINT))",
                "bucket, count, min_val",
            )  # fmt: skip
            _write_hour_bundle(
                hour_dir, "security_topips.parquet",
                "('A', CAST(100 AS BIGINT)), ('B', CAST(500 AS BIGINT))" if hh == 3 else "('A', CAST(900 AS BIGINT))",
                "ip, max_header",
            )  # fmt: skip
            _write_hour_bundle(
                hour_dir, "security_cov.parquet",
                "(CAST(10 AS BIGINT), CAST(4 AS BIGINT))" if hh == 3 else "(CAST(20 AS BIGINT), CAST(9 AS BIGINT))",
                "total_rows, tls_populated",
            )  # fmt: skip

        with _compact_ctx(cache_root):
            assert day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-4", {"name": "svc-sd-4"}) == 4

        day_root = cache_root / "rollups" / "day_bundled" / f"day={day_str}"
        req = pq.read_table(str(day_root / "security_req_size.parquet")).to_pylist()[0]
        assert (req["bucket"], req["count"], req["min_val"]) == ("0-256B", 10, 50)
        conn = pq.read_table(str(day_root / "security_conn_reuse.parquet")).to_pylist()[0]
        assert (conn["count"], conn["min_val"]) == (5, 1)
        assert {
            r["ip"]: r["max_header"] for r in pq.read_table(str(day_root / "security_topips.parquet")).to_pylist()
        } == {"A": 900, "B": 500}
        cov = pq.read_table(str(day_root / "security_cov.parquet")).to_pylist()[0]
        assert (cov["total_rows"], cov["tls_populated"]) == (30, 13)

        cap_root = tmp_path / "cache_cap"
        cap_root.mkdir()
        hour_dir_cap = str(
            cap_root / "rollups" / "hour_bundled" / f"hour={(day_dt + timedelta(hours=5)).strftime('%Y-%m-%d-%H')}"
        )
        _write_hour_bundle(
            hour_dir_cap,
            "security_topips.parquet",
            ", ".join(f"('ip{i:04d}', CAST({i} AS BIGINT))" for i in range(600)),
            "ip, max_header",
        )
        with _compact_ctx(cap_root):
            day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-5", {"name": "svc-sd-5"})
        rows = pq.read_table(
            str(cap_root / "rollups" / "day_bundled" / f"day={day_str}" / "security_topips.parquet")
        ).to_pylist()
        maxes = sorted(r["max_header"] for r in rows)
        assert len(rows) == 500 and maxes[0] == 100 and maxes[-1] == 599

    def test_backfill_security_dims_heals_each_bundle_independently(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        h1_token, _ = _past_hour(3)
        h2_token, _ = _past_hour(4)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        for tok in (h1_token, h2_token):
            (bundled_root / f"hour={tok}").mkdir(parents=True)
            (bundled_root / f"hour={tok}" / "all_fields.parquet").write_bytes(b"x")
        (bundled_root / f"hour={h2_token}" / "security_req_size.parquet").write_bytes(b"x")

        calls: list[tuple[str, set[str]]] = []

        def _spy_bphb(sid, src, hours, *, bundle_filename, **kw):
            hours = list(hours)
            calls.append((bundle_filename, set(hours)))
            return len(hours)

        with (
            patch("backend.core.rollups.security_dims.build_per_hour_bundles", side_effect=_spy_bphb),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        ):
            assert security_dims.backfill_security_dims_bundles("svc-sd-6", {"name": "svc-sd-6"}) == 7

        by_file = dict(calls)
        assert by_file[security_dims.SECURITY_REQ_SIZE_BUNDLE_FILENAME] == {h1_token}
        for fname in (
            security_dims.SECURITY_CONN_REUSE_BUNDLE_FILENAME,
            security_dims.SECURITY_TOPIPS_BUNDLE_FILENAME,
            security_dims.SECURITY_COV_BUNDLE_FILENAME,
        ):
            assert by_file[fname] == {h1_token, h2_token}

    def test_readers_eligibility_gates(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, {"name": "svc-sd-7"})
            readers = [
                runner.try_security_req_size_from_rollup,
                runner.try_security_conn_reuse_from_rollup,
                runner.try_security_top_ips_from_rollup,
                runner.try_security_coverage_from_rollup,
                runner.try_conn_requests_hist_from_rollup,
            ]
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                for call in readers:
                    for s, e, hf in [
                        ("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True),
                        ("2026-06-01T00:00:00Z", "2026-06-01T01:30:00Z", False),
                        ("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False),
                        ("not-a-date", "2026-06-15T00:00:00Z", False),
                    ]:
                        assert call(s, e, has_filters=hf) is None

    def test_rollup_readers_match_live_sql(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sd-8"}
        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        specs = [("10.0.0.1", 100, 1, "abc"), ("10.0.0.1", 400, 4, "abc"), ("10.0.0.2", 5000, 12, "def"), ("10.0.0.2", 9000, 1, ""), ("10.0.0.3", 200, 50, None), ("10.0.0.3", 200, 200, "ghi")]  # fmt: skip

        def _canonical_rows(hour_dt):
            return [
                {
                    "timestamp": hour_dt + timedelta(seconds=j),
                    "ip": ip,
                    "req_header_bytes": rhb,
                    "conn_requests": cr,
                    "tls_ciphers_sha": tls,
                }  # fmt: skip
                for j, (ip, rhb, cr, tls) in enumerate(specs)
            ]

        for dt in hours:
            with _writer_ctx(cache_root, "logs_par", _sd_con_fn("logs_par", _canonical_rows(dt))):
                security_dims.build_security_dims_bundles("svc-sd-8", src, [dt.strftime("%Y-%m-%d-%H")])

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            _seed_logs_rollups_security_dims(con, "t_par", [r for dt in hours for r in _canonical_rows(dt)])
            runner = QueryRunner(con, src)
            live_req = [
                (r[0], int(r[1])) for r in con.execute(REQ_HEADER_SIZE_DIST.format(temp_table="t_par")).fetchall()
            ]
            live_conn = [(r[0], int(r[1])) for r in con.execute(CONN_REUSE_DIST.format(temp_table="t_par")).fetchall()]
            live_topips = [
                (r[0], int(r[1])) for r in con.execute(TOP_IPS_BY_MAX_HEADER.format(temp_table="t_par")).fetchall()
            ]
            agg = "count(*) FILTER (WHERE tls_ciphers_sha IS NOT NULL AND tls_ciphers_sha != '') AS populated_0"
            live_cov = con.execute(FINGERPRINT_COVERAGE_BULK.format(temp_table="t_par", agg_cols=agg)).fetchone()

            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                roll_req = runner.try_security_req_size_from_rollup(st_iso, et_iso, has_filters=False)
                roll_conn = runner.try_security_conn_reuse_from_rollup(st_iso, et_iso, has_filters=False)
                roll_topips = runner.try_security_top_ips_from_rollup(st_iso, et_iso, has_filters=False)
                roll_cov = runner.try_security_coverage_from_rollup(st_iso, et_iso, has_filters=False)

        assert roll_req is not None and roll_conn is not None and roll_topips is not None and roll_cov is not None
        assert [(r["bucket"], r["count"]) for r in roll_req] == live_req
        assert "_approx" not in {k for d in roll_req for k in d}
        assert [(r["bucket"], r["count"]) for r in roll_conn] == live_conn
        assert [(r["ip"], r["max_header"]) for r in roll_topips] == live_topips
        assert roll_cov == (int(live_cov[0]), int(live_cov[1]))

    def test_conn_requests_hist_reader_parity_and_live_topup(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-cr-1"}
        base_dt = (datetime.now(UTC) - timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(3)]

        def _canonical_rows(hour_dt, crs=(1, 1, 3, 10, 50, 200, 0, None)):
            return [
                {
                    "timestamp": hour_dt + timedelta(seconds=j),
                    "ip": "10.0.0.1",
                    "req_header_bytes": 100,
                    "conn_requests": v,
                    "tls_ciphers_sha": "abc",
                }  # fmt: skip
                for j, v in enumerate(crs)
            ]

        for dt in hours:
            with _writer_ctx(cache_root, "logs_cr", _sd_con_fn("logs_cr", _canonical_rows(dt))):
                security_dims.build_security_dims_bundles("svc-cr-1", src, [dt.strftime("%Y-%m-%d-%H")])

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            _seed_logs_rollups_security_dims(con, "t_par", [r for dt in hours for r in _canonical_rows(dt)])
            live_pairs = [
                (r[0], int(r[1]))
                for r in con.execute(
                    CONN_REQUESTS_BUCKET.format(requests_metric="COUNT(*)", table_name="t_par", where_clause="1=1")
                ).fetchall()
            ]
            runner = QueryRunner(con, src)
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                hist = runner.try_conn_requests_hist_from_rollup(
                    st_iso, et_iso, has_filters=False, actual_cols=["conn_requests"]
                )

        assert hist is not None
        assert [(r["value"], r["count"]) for r in hist["top"]] == live_pairs
        assert [r["value"] for r in hist["top"]] == ["1", "2–5", "6–20", "21+"]
        assert hist["total"] == sum(c for _b, c in live_pairs)

        topup_cache = tmp_path / "cache_topup"
        topup_cache.mkdir()
        src3 = {"name": "svc-cr-3"}
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed = [active_dt - timedelta(hours=2), active_dt - timedelta(hours=1)]
        for dt in closed:
            with _writer_ctx(topup_cache, "logs_cr3", _sd_con_fn("logs_cr3", _canonical_rows(dt, (1, 1, 3)))):
                security_dims.build_security_dims_bundles("svc-cr-3", src3, [dt.strftime("%Y-%m-%d-%H")])

        buffer_dir = topup_cache / "buffer"
        buffer_dir.mkdir()
        ts = active_dt + timedelta(minutes=5)
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([ts, ts, ts], type=pa.timestamp("us", tz="UTC")),
                    "conn_requests": pa.array([1, 7, 7], type=pa.int64()),
                }
            ),  # fmt: skip
            str(buffer_dir / "live.parquet"),
        )
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner3 = QueryRunner(con, src3)
            st_iso3 = closed[0].isoformat().replace("+00:00", "Z")
            et_iso3 = (active_dt + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(topup_cache)):
                hist3 = runner3.try_conn_requests_hist_from_rollup(
                    st_iso3, et_iso3, has_filters=False, actual_cols=["conn_requests"]
                )
        assert (
            hist3 is not None
            and {r["value"]: r["count"] for r in hist3["top"]} == {"1": 5, "2–5": 2, "6–20": 2}
            and hist3["total"] == 9
        )


# ── Merged from tests/core/test_rollups_security_proxies.py ──


_ROWS = [
    ("10.0.0.1", 100, "IAD", 20000, 21000, 38.9, -77.0, "US", "Ashburn", "Mozilla/5.0", "https://www.site.com/a", 1000),
    ("10.0.0.2", 200, "IAD", 1000, 50000, 39.0, -77.1, "US", "Reston", "Mozilla/5.0", None, 500),
    ("10.0.0.3", 300, "IAD", 1000, 2000, 39.1, -77.2, "US", "Herndon", "Mozilla/5.0", None, 300),
    ("10.0.0.4", 400, "IAD", 10000, 70000, 39.2, -77.3, "US", "Sterling", "Mozilla/5.0", None, 200),
    ("10.0.0.5", 500, "IAD", 5000, 6000, 35.0, 139.0, "JP", "Tokyo", "Googlebot/2.1", "https://www.news.com/x", 4000),
    ("10.0.0.6", 600, "IAD", None, None, 40.0, -75.0, "US", "Philadelphia", "GPTBot-User/1.0", None, 100),
]

_PROXIES_SCHEMA = (
    "timestamp TIMESTAMPTZ, ip VARCHAR, asn BIGINT, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, "
    "lat DOUBLE, lon DOUBLE, country VARCHAR, city VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT"
)


def _seed_logs_rollups_security_proxies(
    con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], base: datetime
) -> None:
    con.execute(f"CREATE TABLE {table} ({_PROXIES_SCHEMA})")
    for j, r in enumerate(rows):
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [base + timedelta(seconds=j), *r]
        )


def _build_hour(cache_root, src, table: str, hour_dt: datetime, rows: list[tuple]) -> int:
    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs_rollups_security_proxies(c, table, rows, hour_dt)
        return c

    p = _build_patches(cache_root, table, _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        return security_proxies.build_security_proxies_bundles(src["name"], src, [hour_dt.strftime("%Y-%m-%d-%H")])


def _build_window(tmp_path, name: str):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": name}
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        _build_hour(cache_root, src, "logs_par", dt, _ROWS)

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE t_par ({_PROXIES_SCHEMA})")
    for dt in hours:
        for j, r in enumerate(_ROWS):
            con.execute(
                "INSERT INTO t_par VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [dt + timedelta(seconds=j), *r]
            )
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    return cache_root, QueryRunner(con, src), con, st_iso, et_iso


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsSecurityProxies:
    def test_writer_emits_three_bundles_with_only_flagged_rows(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        hour_token, hour_dt = _past_hour(2)
        assert _build_hour(cache_root, {"name": "svc-sp-1"}, "logs_sp1", hour_dt, _ROWS) == 3

        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        quality = {
            r["type"]: r["count"] for r in pq.read_table(str(hour_dir / "security_proxy_quality.parquet")).to_pylist()
        }
        assert quality == {"Direct Connection": 3, "Active Tunnel / Proxy": 1, "WiFi / Mobile": 2}

        flagged = pq.read_table(str(hour_dir / "security_proxy_flagged.parquet")).to_pylist()
        assert sorted(r["ip"] for r in flagged) == ["10.0.0.2", "10.0.0.4", "10.0.0.5"] and all(
            r["cnt"] == 1 for r in flagged
        )

        bot_eff = pq.read_table(str(hour_dir / "security_bot_eff.parquet")).to_pylist()
        assert [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"] == [(6, 6100)]
        assert {r["key"] for r in bot_eff if r["kind"] == "ua"} == {"Mozilla/5.0", "Googlebot/2.1", "GPTBot-User/1.0"}

    def test_writer_skips_services_without_the_required_columns(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_sp2 (timestamp TIMESTAMPTZ, ua VARCHAR, ip VARCHAR)")
            c.execute("INSERT INTO logs_sp2 VALUES (?, 'x', '10.0.0.1')", [hour_dt])
            return c

        p = _build_patches(cache_root, "logs_sp2", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            assert security_proxies.build_security_proxies_bundles("svc-sp-2", {"name": "svc-sp-2"}, [hour_token]) == 1

        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "security_bot_eff.parquet").exists()
        assert (
            not (hour_dir / "security_proxy_flagged.parquet").exists()
            and not (hour_dir / "security_proxy_quality.parquet").exists()
        )

    def test_readers_eligibility_gates(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-3"})
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            for call in (runner.try_security_proxies_from_rollup, runner.try_security_bot_efficiency_from_rollup):
                assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", has_filters=True) is None
                assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", has_filters=False) is None
                assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", has_filters=False) is None
                assert call("not-a-date", "2026-06-15T00:00:00Z", has_filters=False) is None

    def test_proxies_reader_matches_live_sql(self, tmp_path):
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
        assert live_clients <= rolled_clients
        assert {c[0] for c in rolled_clients} == {"10.0.0.2", "10.0.0.4", "10.0.0.5"} and len(rolled["clients"]) == 3
        assert rolled["stats"]["active_proxies_count"] == 2 and rolled["stats"]["distance_mismatches_count"] == 1
        assert rolled["stats"]["total_requests_count"] == 100

    def test_bot_efficiency_reader_matches_live_sql_with_documented_ref_overcount(self, tmp_path):
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

        assert rolled_by_kind["site"] == live_by_kind["site"] and rolled_by_kind["ua"] == live_by_kind["ua"]
        assert set(rolled_by_kind["ref"]) == set(live_by_kind["ref"])
        for stem, (cnt, _) in rolled_by_kind["ref"].items():
            assert cnt == live_by_kind["ref"][stem][0] * 50

    def test_day_compaction_merges_hours_exactly(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-6"}
        day = (datetime.now(UTC) - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(24):
            _build_hour(cache_root, src, "logs_day", day + timedelta(hours=h), _ROWS)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.storage._core._get_service_lock", _noop_lock),
        ):
            assert day_bundles.compact_security_proxies_closed_days_to_daily("svc-sp-6", src) == 3

        day_root = cache_root / "rollups" / "day_bundled" / f"day={day.strftime('%Y-%m-%d')}"
        quality = {
            r["type"]: r["count"] for r in pq.read_table(str(day_root / "security_proxy_quality.parquet")).to_pylist()
        }
        assert quality == {"Direct Connection": 72, "Active Tunnel / Proxy": 24, "WiFi / Mobile": 48}
        flagged = pq.read_table(str(day_root / "security_proxy_flagged.parquet")).to_pylist()
        assert sorted((r["ip"], r["cnt"]) for r in flagged) == [("10.0.0.2", 24), ("10.0.0.4", 24), ("10.0.0.5", 24)]
        bot_eff = pq.read_table(str(day_root / "security_bot_eff.parquet")).to_pylist()
        assert [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"] == [(144, 146400)]

    def test_proxies_reader_handles_sparse_and_empty_flagged_hours(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-sp-7"}
        base_dt = (datetime.now(UTC) - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(24)]
        for dt in hours:
            _build_hour(cache_root, src, "logs_sparse", dt, [_ROWS[0]])

        runner = QueryRunner(duckdb.connect(":memory:"), src)
        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled == {
            "stats": {"active_proxies_count": 0, "total_requests_count": 0, "distance_mismatches_count": 0},
            "quality_rows": [("Direct Connection", 24)],
            "isps": [],
            "clients": [],
        }

    def test_blank_ip_requests_never_appear_as_suspicious_clients(self, tmp_path):
        rows = [*_ROWS, ("", 700, "IAD", 1000, 50000, 39.0, -77.1, "", "", "", None, 0)]
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
        assert next(r for r in live if r[0] == "stats")[2] == 2

        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-blank"})
        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)
        assert (
            rolled is not None
            and all(c["ip"] for c in rolled["clients"])
            and rolled["stats"]["active_proxies_count"] == 2
        )


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
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch.object(
                verified_bots_ts,
                "build_verified_bots_ts_bundles",
                side_effect=lambda _s, _src, h: (captured.append(list(h)), len(h))[1],
            ),
        ):
            assert verified_bots_ts.backfill_verified_bots_ts_bundles("svc-vbts-bf", src) == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        runner = _stub_runner({"name": "svc-vbts-r", "service_id": "svc-vbts-r"}, [], [])
        assert (
            runner.try_verified_bots_ts_from_rollup(
                "2026-05-01T00:00:00+00:00",
                "2026-05-30T00:00:00+00:00",
                temp_table="logs_x_tmp",
                bucket_seconds=3600,
                has_filters=True,
            )
            is None
        )

    def test_reader_returns_none_for_short_window(self, tmp_path):
        runner = _stub_runner({"name": "svc-vbts-r2", "service_id": "svc-vbts-r2"}, [], [])
        assert (
            runner.try_verified_bots_ts_from_rollup(
                "2026-05-01T00:00:00+00:00",
                "2026-05-01T12:00:00+00:00",
                temp_table="logs_x_tmp",
                bucket_seconds=3600,
                has_filters=False,
            )
            is None
        )

    def test_reader_returns_none_for_non_minute_bucket(self, tmp_path):
        runner = _stub_runner({"name": "svc-vbts-r2b", "service_id": "svc-vbts-r2b"}, [], [])
        assert (
            runner.try_verified_bots_ts_from_rollup(
                "2026-05-01T00:00:00+00:00",
                "2026-05-30T00:00:00+00:00",
                temp_table="logs_x_tmp",
                bucket_seconds=90,
                has_filters=False,
            )
            is None
        )

    def test_reader_serves_hybrid_rows(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-vbts-r3", "service_id": "svc-vbts-r3"}
        day_a, day_b = _three_days_ago_iso(), _two_days_ago_iso()
        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_vbts(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [{"bucket_ts": _hour_dt(day_a, h), "bot_type": "google", "count": 5}],
                )

        captured: list[str] = []
        stub_rows = [(_hour_dt(day_a, 0), "google", 120), (_hour_dt(day_a, 1), "bing", 40)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_verified_bots_ts_from_rollup(
                st_iso, end_iso, temp_table="logs_x_tmp", bucket_seconds=3600, has_filters=False
            )

        assert result == stub_rows and len(captured) == 1
        for token in (
            "verified_bots_ts.parquet",
            "read_parquet",
            "UNION ALL",
            "logs_x_tmp",
            "unnest",
            "VERIFIED-BOT.",
            "time_bucket(INTERVAL '3600 seconds'",
        ):
            assert token in captured[0]

    def test_compact_writes_per_day_file_preserving_minutes(self, tmp_path):
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

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.storage._core._get_service_lock", _noop_lock),
        ):
            assert rollups.compact_verified_bots_ts_closed_days_to_daily("svc-vbts-cd", src) == 1

        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "verified_bots_ts.parquet"
        assert day_file.exists()
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            row = con.execute(
                f"SELECT count(*), count(DISTINCT bucket_ts), SUM(count) FILTER (WHERE bot_type='google'), SUM(count) FILTER (WHERE bot_type='bing') FROM read_parquet('{day_file}')"
            ).fetchone()
        assert row == (48, 24, 240, 96)

    def test_compact_skips_active_day(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        for h in range(24):
            _write_hour_vbts(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"bucket_ts": _hour_dt(today, h), "bot_type": "google", "count": 5}],
            )

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.storage._core._get_service_lock", _noop_lock),
        ):
            assert (
                rollups.compact_verified_bots_ts_closed_days_to_daily(
                    "svc-vbts-active", {"name": "svc-vbts-active", "service_id": "svc-vbts-active"}
                )
                == 0
            )
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_wellknown_bots.py ──


def _write_rollup_hour(
    cache_root: str, hour: str, rows: list[tuple[str, str, int]], pattern_set_version: str = "v1700000000"
) -> str:
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
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_block(
    cache_root: str, n_hours: int, first_hours_ago: int, rows_for, pattern_set_version: str = "v1700000000"
) -> tuple[str, str]:
    for i in range(n_hours):
        tok, _ = _past_hour_iso(first_hours_ago + i)
        _write_rollup_hour(cache_root, tok, rows_for(i), pattern_set_version)
    _, start_iso = _past_hour_iso(first_hours_ago + n_hours - 1)
    _, end_iso = _past_hour_iso(first_hours_ago)
    return start_iso, end_iso


def _write_data_dir(cache_root: str, hour: str) -> None:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "all_fields.parquet"), "wb") as f:
        f.write(b"x")


class TestRollupsWellknownBots:
    def test_read_returns_rows_when_window_covered_and_versions_match(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        start_iso, end_iso = _seed_block(
            str(cache_root), n_hours=50, first_hours_ago=2, rows_for=lambda i: [(f"Bot{i}/1.0", f"10.0.0.{i}", 100 + i)]
        )

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            rows = rollups.read_wellknown_bots_rollup({"name": "svc-bot-1"}, start_iso, end_iso)

        assert rows is not None and len(rows) == 50
        counts = [r[2] for r in rows]
        assert counts == sorted(counts, reverse=True)

    def test_read_falls_back_below_48h_window(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        h2, end_iso = _past_hour_iso(2)
        h3, _ = _past_hour_iso(3)
        _, start_iso = _past_hour_iso(14)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 50)])
            _write_rollup_hour(str(cache_root), h3, [("Bingbot/2.0", "157.55.39.1", 20)])
            assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-narrow"}, start_iso, end_iso) is None

    def test_read_falls_back_below_coverage_floor(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        _, start_iso = _past_hour_iso(51)
        _, end_iso = _past_hour_iso(2)
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            for i in range(10):
                tok, _ = _past_hour_iso(2 + i)
                _write_rollup_hour(str(cache_root), tok, [("Googlebot/2.1", "66.249.66.1", 50)])
            assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-2"}, start_iso, end_iso) is None

    def test_read_serves_closed_hours_dropping_active_hour(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        _seed_block(
            str(cache_root), n_hours=50, first_hours_ago=2, rows_for=lambda i: [("Googlebot/2.1", "66.249.66.1", 5)]
        )
        _, start_iso = _past_hour_iso(51)
        end_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            rows = rollups.read_wellknown_bots_rollup({"name": "svc-bot-4"}, start_iso, end_iso)

        assert rows == [("Googlebot/2.1", "66.249.66.1", 250)]

    def test_read_falls_back_when_pattern_set_version_stale(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-3"}, start_iso, end_iso) is None

    def test_read_returns_empty_list_when_window_covered_but_no_bot_traffic(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        empty_table = pa.table(
            {
                "ua": pa.array([], type=pa.string()),
                "ip": pa.array([], type=pa.string()),
                "request_count": pa.array([], type=pa.int64()),
                "pattern_set_version": pa.array([], type=pa.string()),
            }
        )
        for i in range(50):
            tok, _ = _past_hour_iso(2 + i)
            d = os.path.join(str(cache_root), "rollups", "wellknown_bots", f"hour={tok}")
            os.makedirs(d, exist_ok=True)
            pq.write_table(empty_table, os.path.join(d, "compacted_empty.parquet"))
        _, start_iso = _past_hour_iso(51)
        _, end_iso = _past_hour_iso(2)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
        ):
            assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-5"}, start_iso, end_iso) == []

    def test_read_falls_back_when_pattern_set_version_missing(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        start_iso, end_iso = _seed_block(
            str(cache_root), n_hours=50, first_hours_ago=2, rows_for=lambda i: [("OldBot/1.0", "1.2.3.4", 10)]
        )

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value=""),
        ):
            assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-6"}, start_iso, end_iso) is None

    def test_read_falls_back_when_time_range_exceeds_366_days(self):
        _, end_iso = _past_hour_iso(2)
        start_iso = (datetime.now(UTC) - timedelta(days=500)).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert rollups.read_wellknown_bots_rollup({"name": "svc-bot-range-guard"}, start_iso, end_iso) is None

    def test_backfill_wellknown_rebuilds_data_backed_and_sweeps_unrebuildable_stale(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-bot-bf"}
        h_missing, _ = _past_hour_iso(3)
        h_stale_data, _ = _past_hour_iso(4)
        h_current, _ = _past_hour_iso(5)
        h_stale_nodata, _ = _past_hour_iso(6)
        active = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        for tok in (h_missing, h_stale_data, h_current, active):
            _write_data_dir(str(cache_root), tok)

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

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.utils.bot_sources.get_pattern_set_version", return_value="vCURRENT"),
            patch.object(
                wellknown_bots,
                "recompute_wellknown_bots_rollup",
                side_effect=lambda _s, _src, h: (seen.append(sorted(h)), len(h))[1],
            ),
        ):
            assert wellknown_bots.backfill_wellknown_bots_rollup("svc-bot-bf", src) == 2

        assert seen == [sorted([h_missing, h_stale_data])]
        assert not list(nodata_dir.glob("*.parquet"))
        assert list((cache_root / "rollups" / "wellknown_bots" / f"hour={h_current}").glob("*.parquet"))
        assert list((cache_root / "rollups" / "wellknown_bots" / f"hour={h_stale_data}").glob("*.parquet"))


# ── Merged from tests/utils/test_ngwaf.py ────────────────────────────────────


def _make_ngwaf_page(data: list, next_cursor: str = "") -> dict:
    return {"data": data, "meta": {"next_cursor": next_cursor}}


def _bot_request(req_id: str, bot_name: str = "TestBot") -> dict:
    return {
        "id": req_id,
        "signals": [
            {"id": "VERIFIED-BOT", "value": bot_name},
            {"id": "VERIFIED-BOT.SEO", "value": bot_name},
        ],
        "user_agent": f"{bot_name}/1.0",
        "server_name": "example.com",
        "timestamp": "2026-05-07T10:00:00Z",
    }


def _parse_q(mock_fastly: MagicMock) -> str:
    path = mock_fastly.call_args.args[1]
    parsed = urllib.parse.urlparse(path)
    return urllib.parse.parse_qs(parsed.query).get("q", [""])[0]


@pytest.mark.parametrize(
    "r",
    [
        {
            "id": "abc123",
            "signals": [{"id": "DATACENTER", "location": "Microsoft Azure"}],
            "user_agent": "SomeBot/1.0",
            "server_name": "example.com",
            "timestamp": "2026-05-07T00:00:00Z",
        },
        {"id": "abc123", "signals": []},
    ],
)
def test_extract_bot_record_returns_none_without_verified_bot_signal(r):
    assert _extract_bot_record(r) is None


def test_extract_bot_record_extracts_bot_name_and_category():
    r = {
        "id": "05daf2b7aedc405da50c000000000001",
        "signals": [
            {"id": "VERIFIED-BOT", "value": "OpenAI SearchBot"},
            {"id": "VERIFIED-BOT.AI-FETCHER", "value": "OpenAI SearchBot"},
            {"id": "DATACENTER", "location": "Microsoft Azure"},
        ],
        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
        "server_name": "www.example.com",
        "timestamp": "2026-05-07T10:00:00Z",
    }
    result = _extract_bot_record(r)
    assert result == {
        "waf_req_id": "05daf2b7aedc405da50c000000000001",
        "bot_name": "OpenAI SearchBot",
        "category": "AI-FETCHER",
        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
        "server_name": "www.example.com",
        "timestamp": "2026-05-07T10:00:00Z",
    }


@pytest.mark.parametrize(
    ("r", "expected_bot", "expected_cat", "expected_ua"),
    [
        (
            {"id": "abc", "signals": [{"id": "VERIFIED-BOT", "value": "SomeBot"}], "user_agent": "SomeBot/1.0"},
            "SomeBot",
            None,
            "SomeBot/1.0",
        ),
        (
            {
                "id": "abc",
                "signals": [{"id": "VERIFIED-BOT", "value": "AhrefsBot"}],
                "request_headers": [
                    {"name": "Accept", "value": "text/html"},
                    {"name": "User-Agent", "value": "AhrefsBot/7.0"},
                ],
            },
            "AhrefsBot",
            None,
            "AhrefsBot/7.0",
        ),
        (
            {
                "id": "abc",
                "signals": [{"id": "VERIFIED-BOT", "value": "AhrefsBot"}],
                "request_headers": [{"name": "user-agent", "value": "AhrefsBot/7.0"}],
            },
            "AhrefsBot",
            None,
            "AhrefsBot/7.0",
        ),
        (
            {
                "id": "abc",
                "signals": [
                    {"id": "VERIFIED-BOT", "value": "Googlebot"},
                    {"id": "VERIFIED-BOT.SEARCH-ENGINE", "value": "Googlebot"},
                ],
                "user_agent": "Googlebot/2.1",
            },
            "Googlebot",
            "SEARCH-ENGINE",
            "Googlebot/2.1",
        ),
        (
            {
                "id": "abc",
                "signals": [
                    {"id": "VERIFIED-BOT", "value": "Facebook Crawler"},
                    {"id": "BOT-ANALYSIS", "value": "something"},
                ],
                "user_agent": "facebookexternalhit/1.1",
            },
            "Facebook Crawler",
            None,
            "facebookexternalhit/1.1",
        ),
    ],
)
def test_extract_bot_record_variants(r, expected_bot, expected_cat, expected_ua):
    result = _extract_bot_record(r)
    assert result is not None
    assert result["bot_name"] == expected_bot
    assert result["category"] == expected_cat
    assert result["user_agent"] == expected_ua


def test_fetch_verified_bots_paged_single_page_and_routing():
    page1 = [_bot_request("req1"), _bot_request("req2")]
    with patch(
        "backend.core.fastly.client.fastly", side_effect=[_make_ngwaf_page(page1, next_cursor="")]
    ) as mock_fastly:
        pages = list(fetch_verified_bots_paged("api-key", "ws-id", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1
    records, _, raw_count = pages[0]
    assert [r["waf_req_id"] for r in records] == ["req1", "req2"] and raw_count == 2
    args, kwargs = mock_fastly.call_args
    assert args[0] == "GET" and args[1].startswith("/ngwaf/v1/workspaces/ws-id/requests?")
    assert kwargs["token"] == "api-key" and kwargs["timeout"] == 30


def test_fetch_verified_bots_paged_multi_page():
    responses = [
        _make_ngwaf_page([_bot_request("req1")], next_cursor="cursor-abc"),
        _make_ngwaf_page([_bot_request("req2")], next_cursor=""),
    ]
    with patch("backend.core.fastly.client.fastly", side_effect=responses) as mock_fastly, patch("time.sleep"):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 2
    assert pages[0][0][0]["waf_req_id"] == "req1" and pages[1][0][0]["waf_req_id"] == "req2"
    assert "cursor=cursor-abc" in mock_fastly.call_args_list[1].args[1]


def test_fetch_verified_bots_paged_filtering_and_timestamps():
    non_bot = {"id": "req_no_bot", "signals": [{"id": "DATACENTER", "location": "AWS"}], "user_agent": "curl/7.0"}
    earlier = {
        **_bot_request("req1"),
        "timestamp": "2026-05-07T09:00:00Z",
        "signals": [{"id": "VERIFIED-BOT", "value": "Bot"}],
    }
    later = {
        **_bot_request("req2"),
        "timestamp": "2026-05-07T11:00:00Z",
        "signals": [{"id": "VERIFIED-BOT", "value": "Bot"}],
    }
    with patch("backend.core.fastly.client.fastly", side_effect=[_make_ngwaf_page([non_bot, earlier, later])]):
        records, latest_ts, raw_count = next(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))
    assert len(records) == 2 and raw_count == 3 and latest_ts == "2026-05-07T11:00:00Z"

    with patch("backend.core.fastly.client.fastly", side_effect=[_make_ngwaf_page([])]):
        records, latest_ts, raw_count = next(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))
    assert records == [] and latest_ts is None and raw_count == 0


def test_fetch_verified_bots_paged_query_syntax_and_rounding():
    from_dt = datetime.now(UTC) - timedelta(days=4, hours=12)
    with patch("backend.core.fastly.client.fastly", return_value=_make_ngwaf_page([], "")) as mock_fastly:
        list(fetch_verified_bots_paged("api-key", "ws-id", from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")))
        params = urllib.parse.parse_qs(urllib.parse.urlparse(mock_fastly.call_args.args[1]).query)
        assert "from:-5d" in params.get("q", [""])[0] and "from" not in params and "until" not in params

    recent_dt = datetime.now(UTC) - timedelta(minutes=12)
    with patch("backend.core.fastly.client.fastly", return_value=_make_ngwaf_page([], "")) as mock_fastly:
        list(fetch_verified_bots_paged("api-key", "ws-id", recent_dt.strftime("%Y-%m-%dT%H:%M:%SZ")))
        assert "from:-15min" in _parse_q(mock_fastly)

    until_dt = datetime.now(UTC) - timedelta(days=1)
    with patch("backend.core.fastly.client.fastly", return_value=_make_ngwaf_page([], "")) as mock_fastly:
        list(
            fetch_verified_bots_paged(
                "k", "ws", from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), until_ts=until_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        )
        q = _parse_q(mock_fastly)
        assert "from:-" in q and "until:-" in q


def test_relative_time_range_branches():
    assert _get_relative_time_range("-3d") == "-3d" and _get_relative_time_range("-15min") == "-15min"
    assert _get_relative_time_range("not-a-timestamp") == "-1d" and _get_relative_time_range("") == "-1d"
    assert (
        _get_relative_time_range((datetime.now(UTC) - timedelta(hours=4, minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"))
        == "-5h"
    )
    assert (
        _get_relative_time_range((datetime.now(UTC) - timedelta(minutes=40)).strftime("%Y-%m-%dT%H:%M:%SZ")) == "-45min"
    )
    assert (
        _get_relative_time_range((datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")) == "-15min"
    )


def test_oldest_unenriched_timestamp_branches(tmp_path):
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(tmp_path / "missing.db")),
        patch("backend.core.duckdb.get_connection", side_effect=CronLeaseHeldError("no db")),
    ):
        assert oldest_unenriched_timestamp(src) is None

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.core.duckdb.get_connection", side_effect=CronLeaseHeldError("DB busy")),
    ):
        assert oldest_unenriched_timestamp(src) is None

    expected_dt = datetime(2026, 4, 15, 12, 30, 0, tzinfo=UTC)
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchone.return_value = (expected_dt,)
    attached_cm = MagicMock()
    attached_cm.__enter__.return_value = True
    attached_cm.__exit__.return_value = False
    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.repositories._base.attach_ngwaf_cache", return_value=attached_cm),
    ):
        assert oldest_unenriched_timestamp(src) == "2026-04-15T12:30:00Z"


@pytest.fixture
def isolated_ngwaf_db():
    con = pg_connection.get_pg_thread_connection()
    try:
        con.execute("DELETE FROM ngwaf_bots")
        con.execute("DELETE FROM ngwaf_sync_state")
        con.commit()
    except Exception:
        con.rollback()
    yield


@pytest.mark.usefixtures("isolated_ngwaf_db")
class TestNgwafBotCache:
    def test_get_last_timestamp_and_isolation(self):
        assert get_last_timestamp("ws-missing") is None
        upsert_bots([], "ws1", latest_timestamp="2026-05-06T12:00:00Z")
        assert get_last_timestamp("ws1") == "2026-05-06T12:00:01Z"
        assert get_last_timestamp("ws-b") is None

    def test_upsert_bots_records_idempotency_and_edge_cases(self):
        records = [
            {
                "waf_req_id": "req1",
                "bot_name": "OpenAI SearchBot",
                "category": "AI-FETCHER",
                "wellknown_bot_id": "openai-searchbot",
                "wellknown_bot_name": "OpenAI SearchBot crawler",
            },
            {
                "waf_req_id": "req-unknown",
                "bot_name": "UnknownNewBot",
                "category": "AI-FETCHER",
                "wellknown_bot_id": None,
                "wellknown_bot_name": None,
            },
            {"waf_req_id": None, "bot_name": "Phantom"},
        ]
        upsert_bots(records, "ws1", latest_timestamp="2026-05-07T08:00:00Z")
        upsert_bots(records[:1], "ws1", latest_timestamp="2026-05-07T09:00:00Z")
        assert get_last_timestamp("ws1") == "2026-05-07T09:00:01Z"
        upsert_bots([], "ws1", latest_timestamp=None)
        assert get_last_timestamp("ws1") == "2026-05-07T09:00:01Z"

    def test_cleanup_old_bots_and_cache_stats(self):
        assert cleanup_old_bots(retention_days=30) == 0
        upsert_bots(
            [
                {
                    "waf_req_id": "old-req",
                    "bot_name": "OldBot",
                    "category": None,
                    "wellknown_bot_id": None,
                    "wellknown_bot_name": None,
                },
                {
                    "waf_req_id": "new-req",
                    "bot_name": "Googlebot",
                    "category": None,
                    "wellknown_bot_id": None,
                    "wellknown_bot_name": None,
                },
            ],
            "ws-stats",
            latest_timestamp="2026-05-15T00:00:00Z",
        )
        old_ts = (datetime.now(UTC) - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
        con = pg_connection.get_pg_thread_connection()
        con.execute("UPDATE ngwaf_bots SET synced_at = ? WHERE waf_req_id = 'old-req'", (old_ts,))
        con.commit()

        assert cleanup_old_bots(retention_days=30) == 1
        assert cleanup_old_bots(retention_days=30) == 0
        stats = get_cache_stats()
        assert stats["total_cached_bots"] == 1 and "ws-stats" in stats["workspaces"]
        assert stats["top_bots"].get("Googlebot") == 1


_NGWAF_VCR = vcr.VCR(
    cassette_library_dir=os.path.join(os.path.dirname(__file__), "..", "cassettes"),
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def test_ngwaf_vcr_single_page_and_mismatch():
    with _NGWAF_VCR.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        pages = list(
            fetch_verified_bots_paged(
                api_key="test-token-not-real", workspace_id="ws-test-1", from_ts="-1h", page_limit=500
            )
        )
    assert len(pages) == 1
    records, latest_ts, raw_count = pages[0]
    assert raw_count == 3 and len(records) == 2
    assert sorted(r["bot_name"] for r in records) == ["bingbot", "googlebot"]
    assert sorted(r["category"] for r in records) == ["SEARCH-ENGINE", "SEARCH-ENGINE"]
    assert latest_ts == "2026-05-26T10:05:00Z"

    with _NGWAF_VCR.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        with pytest.raises(Exception) as excinfo:
            list(
                fetch_verified_bots_paged(
                    api_key="test-token-not-real", workspace_id="ws-DIFFERENT", from_ts="-1h", page_limit=500
                )
            )
        assert "no match" in str(excinfo.value).lower() or "cannot" in str(excinfo.value).lower()
