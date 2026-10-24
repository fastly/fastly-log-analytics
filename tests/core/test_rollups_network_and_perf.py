"""Tests for the per-hour network_rtt rollup writer + reader (Task A2).

Pinned here:

* ``build_network_rtt_bundles``: writes a parquet at
  ``rollups/hour_bundled/hour=H/network_rtt.parquet`` with one row per
  top-K ASN holding requests, rtt_count, p95_us, p99_us. Active hour
  skipped. Schema unchanged when ``tcp_rtt`` / ``asn`` columns missing
  (writer returns 0 — nothing to roll up).

* ``QueryRunner.try_network_rtt_from_rollup``: gates on unfiltered +
  window >= 48 h + >= 50% closed-hour coverage; returns the
  ``{asn: {p95_rtt_us, p99_rtt_us}}`` dict shape the live path
  produces. Skips ASNs not in the caller-supplied ``top_asns``.

* ``compact_network_rtt_closed_days_to_daily``: 24 per-hour parquets
  for a closed day → 1 per-day file with the same schema, weighted-
  average percentiles + SUM counts. Math associative because rtt_count
  travels alongside the percentile.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.dates import three_days_ago_iso as _three_days_ago_iso
from tests._helpers.dates import two_days_ago_iso as _two_days_ago_iso
from tests._helpers.dates import yesterday_iso as _yesterday_iso
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.rollups import (
    build_writer_patches as _build_patches,
)
from tests._helpers.rollups import (
    make_stub_runner,
)
from tests._helpers.rollups import (
    make_stub_runner as _stub_runner,
)
from tests._helpers.rollups import (
    write_hour_all_fields as _write_hour_all_fields,
)
from tests._helpers.rollups import (
    write_hour_bundle as _write_hour_bundle,
)


# ── Merged from tests/core/test_rollups_network_rtt.py ──
def _write_hour_rtt(cache_root: str, hour: str, rows: list[dict]) -> str:
    """Write a network_rtt.parquet for one hour with the given rows."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "asn": pa.array([r["asn"] for r in rows], type=pa.int64()),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "rtt_count": pa.array([r["rtt_count"] for r in rows], type=pa.int64()),
            "p95_us": pa.array([r["p95_us"] for r in rows], type=pa.float64()),
            "p99_us": pa.array([r["p99_us"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, "network_rtt.parquet")
    pq.write_table(table, p)
    return p


def _stub_runner_rtt(cache_root_str: str, src: dict, captured_sql: list[str], stub_rows: list[tuple]):
    return make_stub_runner(src, captured_sql, stub_rows, table="logs_svc_nr_reader")


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNetworkRtt:
    def test_backfill_skips_hours_already_built(self, tmp_path):
        """Backfill walks the bundle tree; only hours WITH all_fields.parquet
        AND WITHOUT network_rtt.parquet get queued. Already-built hours
        skip cleanly. Verified without a real duckdb path by stubbing the
        inner writer."""
        from backend.core.rollups import network_rtt

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-bf", "service_id": "svc-nr-bf"}
        yday = _yesterday_iso()

        # Hour A: has all_fields but no network_rtt → SHOULD be queued
        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        # Hour B: has all_fields AND network_rtt → SHOULD be skipped
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_rtt(
            str(cache_root), f"{yday}-11", [{"asn": 1, "requests": 5, "rtt_count": 5, "p95_us": 100.0, "p99_us": 200.0}]
        )
        # Hour C: no all_fields → SHOULD be skipped
        d = cache_root / "rollups" / "hour_bundled" / f"hour={yday}-12"
        d.mkdir(parents=True)

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(network_rtt, "build_network_rtt_bundles", side_effect=_stub_build):
                n = network_rtt.backfill_network_rtt_bundles("svc-nr-bf", src)

        assert n == 1, "exactly one hour eligible"
        assert captured == [[f"{yday}-10"]], f"got {captured}"

    def test_reader_returns_none_when_filtered(self, tmp_path):
        """has_filters=True → None (rollup is unfiltered-only)."""
        src = {"name": "svc-nr-r", "service_id": "svc-nr-r"}
        runner = _stub_runner_rtt(str(tmp_path), src, [], [])
        result = runner.try_network_rtt_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            top_asns=[1],
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        """Window < 48h → None (caller's live path handles short windows fine)."""
        src = {"name": "svc-nr-r2", "service_id": "svc-nr-r2"}
        runner = _stub_runner_rtt(str(tmp_path), src, [], [])
        result = runner.try_network_rtt_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            top_asns=[1],
            has_filters=False,
        )
        assert result is None

    def test_reader_serves_top_asn_dict_from_hour_files(self, tmp_path):
        """Eligible window with hour files → reader returns the dict shape
        the live path produces. Verify WHERE-prunes to caller's top_asns."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-r3", "service_id": "svc-nr-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_rtt(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"asn": 7922, "requests": 100, "rtt_count": 100, "p95_us": 50000.0, "p99_us": 80000.0},
                        {"asn": 15169, "requests": 200, "rtt_count": 200, "p95_us": 30000.0, "p99_us": 50000.0},
                    ],
                )

        captured: list[str] = []
        # Stub returns two rows shaped (asn, p95, p99) per the reader's SELECT.
        stub_rows = [(7922, 50000.0, 80000.0), (15169, 30000.0, 50000.0)]
        runner = _stub_runner_rtt(str(cache_root), src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_network_rtt_from_rollup(
                st_iso,
                end_iso,
                top_asns=[7922, 15169],
                has_filters=False,
            )

        assert result is not None
        assert len(captured) == 1
        sql = captured[0]
        assert "network_rtt.parquet" in sql
        assert "asn IN (?, ?)" in sql, f"WHERE should bind 2 placeholders, got: {sql}"
        assert result[7922]["p95_rtt_us"] == 50000.0
        assert result[7922]["p99_rtt_us"] == 80000.0
        assert result[15169]["p95_rtt_us"] == 30000.0

    def test_compact_writes_per_day_file_with_correct_aggregates(self, tmp_path):
        """24 hour files for a closed day → 1 day file. Per-day row weighted-
        averages the percentiles using rtt_count and SUMs the counts. Schema
        matches the hour file so the reader treats both interchangeably."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-cd", "service_id": "svc-nr-cd"}
        day = _yesterday_iso()

        # Uniform: every hour has the same 2 ASNs with same counts + percentiles.
        for h in range(24):
            _write_hour_rtt(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {"asn": 7922, "requests": 100, "rtt_count": 100, "p95_us": 50000.0, "p99_us": 80000.0},
                    {"asn": 15169, "requests": 50, "rtt_count": 50, "p95_us": 30000.0, "p99_us": 50000.0},
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_rtt_closed_days_to_daily("svc-nr-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "network_rtt.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT asn, requests, rtt_count, p95_us, p99_us FROM read_parquet('{day_file}') ORDER BY asn"
            ).fetchall()
        finally:
            con.close()
        # ASN 7922: 24 * 100 = 2400 reqs, p95 stays 50000 (uniform avg), p99 80000
        # ASN 15169: 24 * 50 = 1200 reqs, p95 30000, p99 50000
        assert rows == [
            (7922, 2400, 2400, 50000.0, 80000.0),
            (15169, 1200, 1200, 30000.0, 50000.0),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-active", "service_id": "svc-nr-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_rtt(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"asn": 1, "requests": 5, "rtt_count": 5, "p95_us": 100.0, "p99_us": 200.0}],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_rtt_closed_days_to_daily("svc-nr-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_network_speed.py ──
def _write_hour_speed(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "asn": pa.array([r["asn"] for r in rows], type=pa.int64()),
            "c_speed": pa.array([r["c_speed"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "network_speed.parquet")
    pq.write_table(table, p)
    return p


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNetworkSpeed:
    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT network_speed.parquet."""
        from backend.core.rollups import network_speed

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-bf", "service_id": "svc-ns-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_speed(str(cache_root), f"{yday}-11", [{"asn": 1, "c_speed": "fast", "count": 10}])

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(network_speed, "build_network_speed_bundles", side_effect=_stub_build):
                n = network_speed.backfill_network_speed_bundles("svc-ns-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-ns-r", "service_id": "svc-ns-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_network_speed_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            top_asns=[1],
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-ns-r2", "service_id": "svc-ns-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_network_speed_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            top_asns=[1],
            has_filters=False,
        )
        assert result is None

    def test_reader_serves_rows_with_prune_to_top_asns(self, tmp_path):
        """Eligible window → reader builds a read_parquet SQL with the ASN
        list bound, returns (asn, c_speed, count) tuples."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-r3", "service_id": "svc-ns-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_speed(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"asn": 7922, "c_speed": "fast", "count": 50},
                        {"asn": 15169, "c_speed": "slow", "count": 30},
                    ],
                )

        captured: list[str] = []
        stub_rows = [(7922, "fast", 2400), (15169, "slow", 1440)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_network_speed_from_rollup(
                st_iso,
                end_iso,
                top_asns=[7922, 15169],
                has_filters=False,
            )

        assert result == [(7922, "fast", 2400), (15169, "slow", 1440)]
        assert len(captured) == 1
        sql = captured[0]
        assert "network_speed.parquet" in sql
        assert "asn IN (?, ?)" in sql
        assert "GROUP BY asn, c_speed" in sql

    def test_compact_writes_per_day_file_with_correct_sums(self, tmp_path):
        """24 hour files for a closed day → 1 day file. Exact integer SUMs
        per (asn, c_speed); no weighted-average since counts are exact."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-cd", "service_id": "svc-ns-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_speed(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {"asn": 7922, "c_speed": "fast", "count": 100},
                    {"asn": 7922, "c_speed": "slow", "count": 25},
                    {"asn": 15169, "c_speed": "fast", "count": 50},
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "network_speed.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT asn, c_speed, count FROM read_parquet('{day_file}') ORDER BY asn, c_speed"
            ).fetchall()
        finally:
            con.close()
        # 24 hours × the per-hour counts: 7922/fast=2400, 7922/slow=600, 15169/fast=1200
        assert rows == [
            (7922, "fast", 2400),
            (7922, "slow", 600),
            (15169, "fast", 1200),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-active", "service_id": "svc-ns-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_speed(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"asn": 1, "c_speed": "fast", "count": 5}],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_perf_dims.py ──
def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the perf_dims writer reads and
    INSERT rows. Columns: timestamp, ttl."""
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, ttl BIGINT)")
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [r["timestamp"], r.get("ttl")])


def _write_hour_ttl(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "bucket": pa.array([r["bucket"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
            "min_ttl": pa.array([r["min_ttl"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "perf_ttl_dist.parquet")
    pq.write_table(table, p)
    return p


_LIVE_TTL_DIST_SQL = """
    SELECT
        CASE
            WHEN ttl <= 0 THEN '0s'
            WHEN ttl <= 10 THEN '<10s'
            WHEN ttl <= 30 THEN '<30s'
            WHEN ttl <= 60 THEN '<1m'
            WHEN ttl <= 300 THEN '<5m'
            WHEN ttl <= 600 THEN '<10m'
            WHEN ttl <= 1800 THEN '<30m'
            WHEN ttl <= 3600 THEN '<1h'
            WHEN ttl <= 10800 THEN '<3h'
            WHEN ttl <= 21600 THEN '<6h'
            WHEN ttl <= 43200 THEN '<12h'
            WHEN ttl <= 86400 THEN '<1d'
            WHEN ttl <= 259200 THEN '<3d'
            WHEN ttl <= 604800 THEN '<1w'
            WHEN ttl <= 1209600 THEN '<2w'
            WHEN ttl <= 2592000 THEN '<30d'
            WHEN ttl <= 7776000 THEN '<90d'
            WHEN ttl <= 31536000 THEN '<1y'
            ELSE '>1y'
        END as bucket,
        count(*) as count,
        min(ttl) as min_ttl
    FROM {temp_table}
    WHERE ttl IS NOT NULL
    GROUP BY 1 ORDER BY min_ttl
"""


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPerfDims:
    def test_build_perf_dims_writes_bundle(self, tmp_path):
        """A closed hour with the ttl column produces perf_ttl_dist.parquet with the
        documented schema + exact bucket counts + MIN(ttl)."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-1"}
        hour_token, hour_dt = _past_hour(2)

        # ttl values spanning three buckets: 0s (<=0), <10s (1..10), <30s (11..30).
        rows = [
            {"timestamp": hour_dt + timedelta(minutes=0), "ttl": 0},
            {"timestamp": hour_dt + timedelta(minutes=1), "ttl": -5},
            {"timestamp": hour_dt + timedelta(minutes=2), "ttl": 5},
            {"timestamp": hour_dt + timedelta(minutes=3), "ttl": 8},
            {"timestamp": hour_dt + timedelta(minutes=4), "ttl": 25},
            {"timestamp": hour_dt + timedelta(minutes=5), "ttl": None},  # NULL → excluded
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_pd", rows)
            return c

        p = _build_patches(cache_root, "logs_pd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-1", src, [hour_token])

        assert n == 1
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        ttl_file = hour_dir / "perf_ttl_dist.parquet"
        assert ttl_file.exists()

        assert {"bucket", "count", "min_ttl"}.issubset(set(pq.read_table(str(ttl_file)).column_names))

        by_bucket = {r["bucket"]: r for r in pq.read_table(str(ttl_file)).to_pylist()}
        # 0s: ttl 0 and -5 (min -5, count 2); <10s: 5,8 (min 5, count 2); <30s: 25.
        assert by_bucket["0s"]["count"] == 2 and by_bucket["0s"]["min_ttl"] == -5
        assert by_bucket["<10s"]["count"] == 2 and by_bucket["<10s"]["min_ttl"] == 5
        assert by_bucket["<30s"]["count"] == 1 and by_bucket["<30s"]["min_ttl"] == 25

    def test_build_perf_dims_skips_active_hour(self, tmp_path):
        """The active UTC hour is still being written; the writer must skip it."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-2"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_pd", [])
            return c

        p = _build_patches(cache_root, "logs_pd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-2", src, [active_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
        assert not (hour_dir / "perf_ttl_dist.parquet").exists()

    def test_build_perf_dims_missing_ttl_skips_bundle(self, tmp_path):
        """A service whose schema lacks the ttl column produces no bundle."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-3"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_nottl (timestamp TIMESTAMPTZ, other BIGINT)")
            for i in range(4):
                c.execute("INSERT INTO logs_nottl VALUES (?, ?)", [hour_dt + timedelta(minutes=i), i])
            return c

        p = _build_patches(cache_root, "logs_nottl", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-3", src, [hour_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert not (hour_dir / "perf_ttl_dist.parquet").exists()

    def test_backfill_perf_dims_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT perf_ttl_dist.parquet."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-bf", "service_id": "svc-pd-bf"}
        h1, _ = _past_hour(3)
        h2, _ = _past_hour(4)

        _write_hour_all_fields(str(cache_root), h1)
        _write_hour_all_fields(str(cache_root), h2)
        # h1 already has the ttl_dist bundle — only h2 should be queued.
        _write_hour_ttl(str(cache_root), h1, [{"bucket": "0s", "count": 1, "min_ttl": 0}])

        calls: list[set[str]] = []

        def _spy_bphb(sid, src, hours, *, bundle_filename, **kw):
            hours = list(hours)
            calls.append(set(hours))
            return len(hours)

        with patch("backend.core.rollups.perf_dims.build_per_hour_bundles", side_effect=_spy_bphb):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = perf_dims.backfill_perf_dims_bundles("svc-pd-bf", src)

        assert n == 1
        assert calls == [{h2}]

    def test_compact_perf_dims_day_merge(self, tmp_path):
        """Day-compaction over a full closed UTC day: SUM counts + MIN-of-MIN per
        bucket into one day file."""
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-4"}

        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day_dt.strftime("%Y-%m-%d")
        bundled_root = cache_root / "rollups" / "hour_bundled"

        for hh in (3, 4):
            hour_token = (day_dt + timedelta(hours=hh)).strftime("%Y-%m-%d-%H")
            hour_dir = str(bundled_root / f"hour={hour_token}")
            # '0s' appears in both hours (counts 4 + 6, mins -5 + 0); '<10s' only h4.
            _write_hour_bundle(
                hour_dir,
                "perf_ttl_dist.parquet",
                "('0s', CAST(4 AS BIGINT), CAST(-5 AS BIGINT))"
                if hh == 3
                else "('0s', CAST(6 AS BIGINT), CAST(0 AS BIGINT)), ('<10s', CAST(2 AS BIGINT), CAST(5 AS BIGINT))",
                "bucket, count, min_ttl",
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-4", src)

        assert n == 1  # one bundle for the single closed day
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day_str}" / "perf_ttl_dist.parquet"
        assert day_file.exists()

        by_bucket = {r["bucket"]: r for r in pq.read_table(str(day_file)).to_pylist()}
        assert by_bucket["0s"]["count"] == 10 and by_bucket["0s"]["min_ttl"] == -5  # 4+6, MIN(-5, 0)
        assert by_bucket["<10s"]["count"] == 2 and by_bucket["<10s"]["min_ttl"] == 5

    def test_compact_perf_dims_skips_active_day(self, tmp_path):
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_ttl(str(cache_root), f"{today}-{h:02d}", [{"bucket": "0s", "count": 5, "min_ttl": 0}])

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-active", src)

        assert n == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-pd-r", "service_id": "svc-pd-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_ttl_dist_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-pd-r2", "service_id": "svc-pd-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_ttl_dist_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            has_filters=False,
        )
        assert result is None

    def test_reader_returns_none_malformed_timestamps(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r2b", "service_id": "svc-pd-r2b"}
        runner = _stub_runner(src, [], [])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            assert runner.try_perf_ttl_dist_from_rollup("not-a-date", "2026-06-15T00:00:00Z", has_filters=False) is None

    def test_reader_returns_none_no_coverage(self, tmp_path):
        """Eligible window but no rollup files at all → None (fall back to live)."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r2c", "service_id": "svc-pd-r2c"}
        runner = _stub_runner(src, [], [])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_ttl_dist_from_rollup(
                "2026-05-01T00:00:00+00:00",
                "2026-05-30T00:00:00+00:00",
                has_filters=False,
            )
        assert result is None

    def test_reader_serves_rows_sum_per_bucket_min_ttl_order(self, tmp_path):
        """Eligible window → reader builds a read_parquet SQL with SUM(count) per
        bucket ORDER BY MIN(min_ttl), returns [{"bucket","count"}] ordered."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r3", "service_id": "svc-pd-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_ttl(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"bucket": "0s", "count": 3, "min_ttl": 0},
                        {"bucket": "<10s", "count": 7, "min_ttl": 5},
                    ],
                )

        captured: list[str] = []
        # The stub returns rows already in min_ttl order (0s then <10s).
        stub_rows = [("0s", 144), ("<10s", 336)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_ttl_dist_from_rollup(st_iso, end_iso, has_filters=False)

        assert result == [{"bucket": "0s", "count": 144}, {"bucket": "<10s", "count": 336}]
        assert len(captured) == 1
        sql = captured[0]
        assert "perf_ttl_dist.parquet" in sql
        assert "SUM(count)" in sql
        assert "ORDER BY MIN(min_ttl)" in sql

    def test_rollup_reader_matches_live_sql(self, tmp_path):
        """Seed known rows across a 50-hour closed window; build the perf_dims
        rollup; assert the reader's [{"bucket","count"}] output is byte-equal to the
        live ttl_dist SQL over the SAME rows (one temp table holding all 50 hours)."""
        from backend.core.rollups import perf_dims
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-8"}

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]

        def _canonical_rows(hour_dt):
            # ttl values spanning many buckets, including boundary + NULL + huge.
            ttls = [-1, 0, 7, 10, 30, 59, 200, 3600, 86400, 700000, 40000000, None]
            return [{"timestamp": hour_dt + timedelta(seconds=j), "ttl": t} for j, t in enumerate(ttls)]

        for dt in hours:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs(c, "logs_par", rows)
                return c

            p = _build_patches(cache_root, "logs_par", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                perf_dims.build_perf_dims_bundles("svc-pd-8", src, [hour_token])

        # Live SQL over one temp table holding ALL 50 hours of the same rows.
        con = duckdb.connect(":memory:")
        all_rows: list[dict] = []
        for dt in hours:
            all_rows.extend(_canonical_rows(dt))
        _seed_logs(con, "t_par", all_rows)
        runner = QueryRunner(con, src)

        live = con.execute(_LIVE_TTL_DIST_SQL.format(temp_table="t_par")).fetchall()
        live_pairs = [(r[0], int(r[1])) for r in live]  # (bucket, count) in min_ttl order

        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_perf_ttl_dist_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled is not None
        assert [(r["bucket"], r["count"]) for r in rolled] == live_pairs
        assert "_approx" not in {k for d in rolled for k in d}  # EXACT — no _approx key


# ── Merged from tests/core/test_rollups_perf_latency.py ──
_COLS = ("value", "requests", "elapsed_count", "elapsed_sum", "p50_us", "p95_us", "p99_us")


def _write_hour_perf(cache_root: str, hour: str, rows: list[dict], filename: str = "perf_top_urls.parquet") -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "elapsed_count": pa.array([r["elapsed_count"] for r in rows], type=pa.int64()),
            "elapsed_sum": pa.array([r["elapsed_sum"] for r in rows], type=pa.float64()),
            "p50_us": pa.array([r["p50_us"] for r in rows], type=pa.float64()),
            "p95_us": pa.array([r["p95_us"] for r in rows], type=pa.float64()),
            "p99_us": pa.array([r["p99_us"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, filename)
    pq.write_table(table, p)
    return p


def _row(value: str, requests: int, p50: float, p95: float, p99: float, avg_us: float = 1000.0) -> dict:
    return {
        "value": value,
        "requests": requests,
        "elapsed_count": requests,
        "elapsed_sum": avg_us * requests,
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
    }


def _serve_window(cache_root, filename: str):
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()
    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_perf(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [_row("/a", 50, 1000.0, 2000.0, 3000.0), _row("/b", 30, 500.0, 900.0, 1500.0)],
                filename=filename,
            )
    st_iso = f"{day_a}T00:00:00+00:00"
    end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    return st_iso, end_iso


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPerfLatency:
    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks hour_bundled and queues hours WITH all_fields.parquet
        but WITHOUT perf_top_urls.parquet (the sentinel)."""
        from backend.core.rollups import perf_latency

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-bf", "service_id": "svc-pl-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_perf(str(cache_root), f"{yday}-11", [_row("/a", 10, 1.0, 2.0, 3.0)])

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(perf_latency, "build_perf_latency_bundles", side_effect=_stub_build):
                n = perf_latency.backfill_perf_latency_bundles("svc-pl-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-pl-r", "service_id": "svc-pl-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_latency_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            dimension="url",
            sort_by="p99",
            has_filters=True,
            min_requests=5,
            limit=20,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-pl-r2", "service_id": "svc-pl-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_latency_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            dimension="url",
            sort_by="p99",
            has_filters=False,
            min_requests=5,
            limit=20,
        )
        assert result is None

    def test_reader_serves_url_rows_weighted_avg(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-r3", "service_id": "svc-pl-r3"}
        st_iso, end_iso = _serve_window(cache_root, "perf_top_urls.parquet")

        captured: list[str] = []
        # (value, requests, avg_us, p50_us_w, p95_us_w, p99_us_w)
        stub_rows = [("/a", 2400, 1000.0, 1000.0, 2000.0, 3000.0)]
        runner = _stub_runner(src, captured, stub_rows)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_latency_from_rollup(
                st_iso,
                end_iso,
                dimension="url",
                sort_by="p99",
                has_filters=False,
                min_requests=5,
                limit=20,
            )

        assert result == {
            "rows": [{"value": "/a", "requests": 2400, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}],
            "_approx": True,
        }
        assert len(captured) == 1
        sql = captured[0]
        assert "perf_top_urls.parquet" in sql
        assert "read_parquet" in sql
        assert "SUM(p99_us * requests)" in sql
        assert "GROUP BY value" in sql
        assert "ORDER BY p99_us_w DESC" in sql  # default sort_by=p99

    def test_reader_asn_dim_and_sort_by_p95(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-r4", "service_id": "svc-pl-r4"}
        st_iso, end_iso = _serve_window(cache_root, "perf_top_asns.parquet")

        captured: list[str] = []
        runner = _stub_runner(src, captured, [("7922", 100, 1000.0, 1.0, 2.0, 3.0)])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_latency_from_rollup(
                st_iso,
                end_iso,
                dimension="asn",
                sort_by="p95",
                has_filters=False,
                min_requests=10,
                limit=20,
            )
        assert result is not None
        sql = captured[0]
        assert "perf_top_asns.parquet" in sql
        assert "ORDER BY p95_us_w DESC" in sql  # sort_by=p95 honored

    def test_compact_weighted_avg_per_day(self, tmp_path):
        """24 hour files → 1 day file: requests summed, percentiles request-
        weight-averaged. Same value+p95 each hour → day p95 unchanged, requests
        summed."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-cd", "service_id": "svc-pl-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_perf(
                str(cache_root),
                f"{day}-{h:02d}",
                [_row("/a", 10, 100.0, 200.0, 300.0), _row("/b", 5, 50.0, 90.0, 150.0)],
                filename="perf_top_urls.parquet",
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-cd", src)

        assert rebuilt >= 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "perf_top_urls.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT value, requests, p95_us FROM read_parquet('{day_file}') ORDER BY value"
            ).fetchall()
        finally:
            con.close()
        # /a: 24×10 reqs = 240, p95 weighted-avg of identical 200 = 200
        # /b: 24×5 reqs = 120, p95 = 90
        assert rows == [("/a", 240, 200.0), ("/b", 120, 90.0)]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-active", "service_id": "svc-pl-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_perf(str(cache_root), f"{today}-{h:02d}", [_row("/a", 10, 100.0, 200.0, 300.0)])

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_pop_health.py ──
def _write_hour_pop_health(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "pop": pa.array([r["pop"] for r in rows]),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "errors": pa.array([r["errors"] for r in rows], type=pa.int64()),
            "cache_hits": pa.array([r["cache_hits"] for r in rows], type=pa.int64()),
            "bandwidth_bytes": pa.array([r["bandwidth_bytes"] for r in rows], type=pa.int64()),
            "p50_rtt_us": pa.array([r["p50_rtt_us"] for r in rows], type=pa.float64()),
            "p95_ttfb_ms": pa.array([r["p95_ttfb_ms"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, "pop_health.parquet")
    pq.write_table(table, p)
    return p


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPopHealth:
    def test_backfill_skips_built_hours(self, tmp_path):
        from backend.core.rollups import pop_health

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-bf", "service_id": "svc-ns-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_pop_health(
            str(cache_root),
            f"{yday}-11",
            [
                {
                    "pop": "IAD",
                    "requests": 100,
                    "errors": 10,
                    "cache_hits": 90,
                    "bandwidth_bytes": 1000,
                    "p50_rtt_us": 10.0,
                    "p95_ttfb_ms": 100.0,
                }
            ],
        )

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(pop_health, "build_pop_health_bundles", side_effect=_stub_build):
                n = pop_health.backfill_pop_health_bundles("svc-ns-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-ns-r", "service_id": "svc-ns-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_pop_health_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            has_filters=True,
        )
        assert result is None

    def test_reader_serves_rows_with_prune_to_top_asns(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-r3", "service_id": "svc-ns-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_pop_health(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {
                            "pop": "IAD",
                            "requests": 50,
                            "errors": 0,
                            "cache_hits": 50,
                            "bandwidth_bytes": 500,
                            "p50_rtt_us": 10.0,
                            "p95_ttfb_ms": 100.0,
                        },
                    ],
                )

        captured: list[str] = []
        stub_rows = [("IAD", 2400, 0, 2400, 24000, 10.0, 100.0)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_pop_health_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
            )

        assert result == stub_rows
        assert len(captured) == 1
        sql = captured[0]
        assert "pop_health.parquet" in sql

    def test_compact_writes_per_day_file_with_correct_sums(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-cd", "service_id": "svc-ns-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_pop_health(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {
                        "pop": "IAD",
                        "requests": 100,
                        "errors": 5,
                        "cache_hits": 80,
                        "bandwidth_bytes": 1000,
                        "p50_rtt_us": 10.0,
                        "p95_ttfb_ms": 50.0,
                    },
                    {
                        "pop": "LHR",
                        "requests": 50,
                        "errors": 0,
                        "cache_hits": 45,
                        "bandwidth_bytes": 500,
                        "p50_rtt_us": 20.0,
                        "p95_ttfb_ms": 100.0,
                    },
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_pop_health_closed_days_to_daily("svc-ns-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "pop_health.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms FROM read_parquet('{day_file}') ORDER BY pop"
            ).fetchall()
        finally:
            con.close()

        assert rows == [
            ("IAD", 2400, 120, 1920, 24000, 10.0, 50.0),
            ("LHR", 1200, 0, 1080, 12000, 20.0, 100.0),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-active", "service_id": "svc-ns-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_pop_health(
                str(cache_root),
                f"{today}-{h:02d}",
                [
                    {
                        "pop": "IAD",
                        "requests": 5,
                        "errors": 0,
                        "cache_hits": 5,
                        "bandwidth_bytes": 50,
                        "p50_rtt_us": 10.0,
                        "p95_ttfb_ms": 100.0,
                    }
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_pop_health_closed_days_to_daily("svc-ns-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()
