"""Tests for the per-hour origin_summary bundle writer + reader.

Mirrors test_rollups_slow_urls.py — same patch pattern, same fixture
helpers — so the two writers stay legible side-by-side.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the origin_summary writer reads
    and INSERT rows."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE,"
        f"  elapsed DOUBLE, ost INTEGER, obytes BIGINT, cache VARCHAR"
        f")"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ottfb"),
                r.get("ttfb"),
                r.get("ottlb"),
                r.get("elapsed"),
                r.get("ost"),
                r.get("obytes"),
                r.get("cache"),
            ],
        )


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def test_build_origin_summary_writes_expected_columns(tmp_path):
    """Happy path: one closed hour with rows produces one parquet row with
    the documented column set + correct counts."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    hour_token, hour_dt = _past_hour(2)

    rows = [
        {
            "timestamp": hour_dt + timedelta(minutes=i),
            "ottfb": 100_000.0 + i * 100,
            "ttfb": 0.1,
            "ottlb": 200_000.0 + i * 100,
            "elapsed": 300_000.0 + i * 100,
            "ost": 200 if i < 8 else 500,  # 2/10 are 5xx
            "obytes": 1000,
            "cache": "MISS" if i < 5 else "HIT",
        }
        for i in range(10)
    ]
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    cols = set(t.column_names)
    expected = {
        "requests",
        "total_misses",
        "total_passes",
        "lat_us_count",
        "ottfb_p50_us",
        "ottfb_p75_us",
        "ottfb_p95_us",
        "ottfb_p99_us",
        "ottlb_count",
        "ottlb_p50_us",
        "ottlb_p95_us",
        "cdn_ovh_count",
        "cdn_ovh_p50_us",
        "ost_5xx_count",
        "ost_total_count",
        "obytes_count",
        "obytes_p50",
    }
    assert expected.issubset(cols), f"missing required columns; got: {cols}"

    row = t.to_pylist()[0]
    assert row["requests"] == 10
    assert row["total_misses"] == 5
    # ost: 8 × 200, 2 × 500-599 ⇒ 2 5xx / 10 total
    assert row["ost_5xx_count"] == 2
    assert row["ost_total_count"] == 10
    # 10 rows all have lat_us non-null (ottfb is non-null)
    assert row["lat_us_count"] == 10


def test_build_origin_summary_skips_active_hour(tmp_path):
    """Active UTC hour must be skipped (live SQL serves it). Convention
    shared with time_series + slow_urls writers."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [active_token])

    assert n == 0


def test_backfill_origin_summary_walks_existing_bundle_hours(tmp_path):
    """Self-heal driver picks up closed hours with all_fields.parquet but
    no origin_summary.parquet."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}

    h1_token, _ = _past_hour(3)
    h2_token, _ = _past_hour(4)
    bundled_root = cache_root / "rollups" / "hour_bundled"
    (bundled_root / f"hour={h1_token}").mkdir(parents=True)
    (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")
    (bundled_root / f"hour={h2_token}").mkdir(parents=True)
    (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
    (bundled_root / f"hour={h2_token}" / "origin_summary.parquet").write_bytes(b"x")  # already-built

    written_hours: list[list[str]] = []

    def _spy_build(_sid, _src, hours):
        written_hours.append(list(hours))
        return len(hours)

    with patch("backend.core.rollups.origin_summary.build_origin_summary_bundles", side_effect=_spy_build):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", src)

    assert n == 1
    assert written_hours == [[h1_token]]


def test_try_origin_summary_from_rollup_eligibility_gates(tmp_path):
    """Reader returns None for has_filters=True, window<48h, missing
    coverage, malformed timestamps."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # has_filters → None
        assert (
            runner.try_origin_summary_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-15T00:00:00Z",
                has_filters=True,
                actual_cols={"ottfb", "ttfb"},
            )
            is None
        )
        # Window < 48h
        assert (
            runner.try_origin_summary_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-02T12:00:00Z",
                has_filters=False,
                actual_cols={"ottfb", "ttfb"},
            )
            is None
        )
        # Malformed timestamp
        assert (
            runner.try_origin_summary_from_rollup(
                "not-a-date",
                "2026-06-15T00:00:00Z",
                has_filters=False,
                actual_cols={"ottfb", "ttfb"},
            )
            is None
        )


def test_try_origin_summary_from_rollup_aggregates_across_hours(tmp_path):
    """End-to-end: build a 50-hour window of origin_summary files, verify
    counts SUM exactly + percentiles request-weighted-average."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}

    bundled_root = str(cache_root / "rollups" / "hour_bundled")

    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
        os.makedirs(hour_dir, exist_ok=True)
        # Each hour: requests=1000, misses=500, passes=200, 5xx=10/1000,
        # lat_us_count=1000, ottfb_p50_us=50000 (50 ms), ottfb_p95_us=200000,
        # ottlb_count=900, ottlb_p50_us=80000, cdn_ovh_count=850,
        # cdn_ovh_p50_us=12000, obytes_count=1000, obytes_p50=2048
        sql_path = f"{hour_dir}/origin_summary.parquet"
        wcon = duckdb.connect()
        try:
            wcon.execute(
                f"COPY (SELECT * FROM (VALUES ("
                f"CAST(1000 AS BIGINT), CAST(500 AS BIGINT), CAST(200 AS BIGINT),"
                f"CAST(1000 AS BIGINT),"
                f"50000.0, 100000.0, 200000.0, 500000.0,"
                f"CAST(900 AS BIGINT), 80000.0, 250000.0,"
                f"CAST(850 AS BIGINT), 12000.0,"
                f"CAST(10 AS BIGINT), CAST(1000 AS BIGINT),"
                f"CAST(1000 AS BIGINT), 2048.0"
                f")) AS t(requests, total_misses, total_passes, lat_us_count,"
                f"ottfb_p50_us, ottfb_p75_us, ottfb_p95_us, ottfb_p99_us,"
                f"ottlb_count, ottlb_p50_us, ottlb_p95_us,"
                f"cdn_ovh_count, cdn_ovh_p50_us,"
                f"ost_5xx_count, ost_total_count,"
                f"obytes_count, obytes_p50)) "
                f"TO '{sql_path}' (FORMAT PARQUET)"
            )
        finally:
            wcon.close()

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    actual_cols = {"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_origin_summary_from_rollup(
            st_iso,
            et_iso,
            has_filters=False,
            actual_cols=actual_cols,
        )

    assert result is not None, "reader returned None — eligibility gate fired"
    assert result["_approx"] is True
    assert result["has_data"] is True

    # Counts SUM exactly across 50 hours.
    assert result["total_misses"] == 500 * 50
    assert result["total_passes"] == 200 * 50

    # Percentile fields are request-weighted average — same per-hour
    # values everywhere → cross-hour value == per-hour value.
    assert abs(result["ottfb_p50_ms"] - 50.0) < 1e-6  # 50000us = 50ms
    assert abs(result["ottfb_p95_ms"] - 200.0) < 1e-6
    assert abs(result["ottlb_p50_ms"] - 80.0) < 1e-6
    assert abs(result["cdn_overhead_p50_ms"] - 12.0) < 1e-6
    assert abs(result["obytes_p50"] - 2048.0) < 1e-6

    # Error rate: SUM(5xx) / SUM(total) = 10*50 / 1000*50 = 0.01 — exact across hours.
    assert abs(result["origin_error_rate"] - 0.01) < 1e-9


# ── Early-return + skip branches ───────────────────────────────────────────────


def test_build_origin_summary_empty_hours_returns_zero():
    """Caller passes an empty list (no closed hours to rebuild). Must
    return 0 immediately — never opens a DuckDB connection."""
    from backend.core.rollups import origin_summary

    with patch("backend.core.duckdb.get_connection") as mock_conn:
        n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [])
    assert n == 0
    mock_conn.assert_not_called()


def test_build_origin_summary_skips_malformed_hour_tokens(tmp_path, caplog):
    """Malformed hour tokens (e.g. legacy artifacts, hand-edits) are
    skipped with a warning. The valid token in the same call still
    runs."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    valid_token, valid_dt = _past_hour(2)

    rows = [{"timestamp": valid_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        import logging as _logging

        with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_summary"):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, ["totally-not-an-hour", valid_token])

    assert n == 1
    assert any("malformed hour token" in r.message for r in caplog.records)


def test_build_origin_summary_returns_zero_when_safe_table_missing(tmp_path):
    """A service whose schema can't resolve to a SQL-safe table (no
    name match) short-circuits at 0 — no parquet ever written."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, _ = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=None),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
    assert n == 0


def test_build_origin_summary_returns_zero_when_describe_columns_fails(tmp_path):
    """If describe_columns returns None (schema lookup failed), the
    writer returns 0 and closes the connection in finally."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, _ = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.rollups._common.describe_columns", return_value=None),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
    assert n == 0


# ── Latency-expression branches: ottfb only / ttfb only / neither ─────────────


def _seed_logs_partial(con: duckdb.DuckDBPyConnection, table: str, cols: list[str], rows: list[dict]) -> None:
    """Create ``table`` with only the listed columns + a TIMESTAMPTZ ts."""
    col_defs = ", ".join(f'"{c}" DOUBLE' for c in cols)
    con.execute(f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, {col_defs})')
    placeholder = ", ".join(["?"] * (1 + len(cols)))
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES ({placeholder})",
            [r["timestamp"], *(r.get(c) for c in cols)],
        )


def test_build_origin_summary_ottfb_only_schema(tmp_path):
    """Schema with ottfb but no ttfb — latency expression uses ottfb
    directly (not the COALESCE form)."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs_partial(
        con,
        "logs_os",
        ["ottfb"],
        [{"timestamp": dt + timedelta(minutes=i), "ottfb": 100_000.0 + i * 1000} for i in range(5)],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
    row = pq.read_table(str(bundle)).to_pylist()[0]
    assert row["requests"] == 5
    assert row["lat_us_count"] == 5
    # All optional columns absent → 0 / NULL.
    assert row["ottlb_count"] == 0
    assert row["cdn_ovh_count"] == 0
    assert row["ost_5xx_count"] == 0
    assert row["obytes_count"] == 0
    assert row["total_misses"] == 0  # no cache column


def test_build_origin_summary_ttfb_only_schema(tmp_path):
    """Schema with ttfb but no ottfb — latency expression scales ttfb
    (seconds) to microseconds."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs_partial(
        con, "logs_os", ["ttfb"], [{"timestamp": dt + timedelta(minutes=i), "ttfb": 0.05 + i * 0.001} for i in range(5)]
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
    row = pq.read_table(str(bundle)).to_pylist()[0]
    # 0.05 s = 50_000 us — verify the *1e6 scaling kicked in.
    assert row["lat_us_count"] == 5
    assert row["ottfb_p50_us"] >= 49_999.0


def test_build_origin_summary_no_latency_columns_returns_zero(tmp_path):
    """A service with no ottfb AND no ttfb has nothing to summarise —
    return 0 without writing any bundle."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    # Schema with only an unrelated column.
    _seed_logs_partial(con, "logs_os", ["elapsed"], [{"timestamp": dt, "elapsed": 100_000.0}])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet").exists()


# ── COPY failure: warn + cleanup tmp + continue ───────────────────────────────


class _FailingFirstCopyCon:
    """Thin wrapper that delegates everything to a real DuckDB conn
    but fails the FIRST ``COPY (...)`` execute with duckdb.Error.
    Lets us hit the copy-failure exception branch without monkey-
    patching read-only DuckDB attributes."""

    def __init__(self, real):
        self._real = real
        self._copies_seen = 0

    def execute(self, sql, *args, **kwargs):
        if sql.startswith("COPY ("):
            self._copies_seen += 1
            if self._copies_seen == 1:
                raise duckdb.Error("simulated COPY failure")
        return self._real.execute(sql, *args, **kwargs)

    def close(self):
        return self._real.close()

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_build_origin_summary_logs_and_skips_on_copy_failure(tmp_path, caplog):
    """A duckdb.Error on the COPY for one hour must not abort the
    whole call — log a warning, clean up the tmp file, continue to
    the next hour."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    h1_token, _ = _past_hour(3)
    h2_token, h2_dt = _past_hour(2)

    rows = [
        {
            "timestamp": h2_dt + timedelta(minutes=1),
            "ottfb": 50_000.0,
            "ttfb": 0.05,
            "ottlb": None,
            "elapsed": None,
            "ost": None,
            "obytes": None,
            "cache": None,
        }
    ]
    real = duckdb.connect(":memory:")
    _seed_logs(real, "logs_os", rows)
    con = _FailingFirstCopyCon(real)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        import logging as _logging

        with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_summary"):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [h1_token, h2_token])

    # h1 failed, h2 succeeded.
    assert n == 1
    assert any("COPY failed for hour" in r.message for r in caplog.records)
    # Tmp file from h1 must NOT be left behind.
    h1_dir = cache_root / "rollups" / "hour_bundled" / f"hour={h1_token}"
    if h1_dir.exists():
        assert not any(p.name.startswith(".tmp_os_") for p in h1_dir.iterdir())


# ── Publish (os.replace) failure: warn + cleanup tmp ─────────────────────────


def test_build_origin_summary_logs_on_publish_failure(tmp_path, caplog):
    """A successful COPY followed by an OSError during the atomic
    os.replace publish must be caught, logged, and the tmp file cleaned
    up so the directory stays consistent."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}
    token, dt = _past_hour(2)

    rows = [
        {
            "timestamp": dt + timedelta(minutes=1),
            "ottfb": 50_000.0,
            "ttfb": 0.05,
            "ottlb": None,
            "elapsed": None,
            "ost": None,
            "obytes": None,
            "cache": None,
        }
    ]
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_os", rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("os.replace", side_effect=OSError("simulated rename failure")),
    ):
        import logging as _logging

        with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_summary"):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])

    assert n == 0
    assert any("could not publish origin_summary" in r.message for r in caplog.records)


# ── Backfill: branches the existing test doesn't cover ───────────────────────


def test_backfill_returns_zero_when_bundled_root_missing(tmp_path):
    """Brand-new service: bundled_root doesn't exist yet. backfill
    must return 0 immediately rather than calling listdir on a
    nonexistent path."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-empty"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = origin_summary.backfill_origin_summary_bundles("svc-empty", src)
    assert n == 0


def test_backfill_skips_non_hour_entries_and_malformed_tokens(tmp_path):
    """Bundled root may contain non-hour entries (.tmp files, junk
    from prior crashes) and malformed hour tokens. Both must be
    filtered out at the directory-walk stage."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}

    bundled_root = cache_root / "rollups" / "hour_bundled"
    bundled_root.mkdir(parents=True)

    # Junk entry that doesn't start with hour=.
    (bundled_root / "junk_file.txt").write_text("ignore me")
    (bundled_root / "metadata").mkdir()
    # Malformed hour token.
    (bundled_root / "hour=not-an-hour").mkdir()
    (bundled_root / "hour=not-an-hour" / "all_fields.parquet").write_bytes(b"x")
    # Valid hour but missing all_fields.parquet (not yet bundled).
    valid_token, _ = _past_hour(3)
    (bundled_root / f"hour={valid_token}").mkdir()
    # Note: no all_fields.parquet, so this should also be skipped.

    captured: list[list[str]] = []

    def _spy_build(_sid, _src, hours):
        captured.append(list(hours))
        return len(hours)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(
            "backend.core.rollups.origin_summary.build_origin_summary_bundles",
            side_effect=_spy_build,
        ),
    ):
        n = origin_summary.backfill_origin_summary_bundles("svc-os", src)
    # Nothing to build → returns 0 and never calls build.
    assert n == 0
    assert captured == []


def test_backfill_returns_zero_on_listdir_oserror(tmp_path):
    """If os.listdir raises (e.g. permission denied), backfill must
    return 0 rather than propagate the OSError up the cron scheduler."""
    from backend.core.rollups import origin_summary

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os"}

    bundled_root = cache_root / "rollups" / "hour_bundled"
    bundled_root.mkdir(parents=True)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("os.listdir", side_effect=OSError("permission denied")),
    ):
        n = origin_summary.backfill_origin_summary_bundles("svc-os", src)
    assert n == 0
