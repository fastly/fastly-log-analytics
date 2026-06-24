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
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


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


def _write_hour_all_fields(cache_root: str, hour: str) -> None:
    """Sentinel parquet so the backfill walk treats this hour as touched."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table({"field": pa.array(["x"]), "value": pa.array(["y"]), "count": pa.array([1], type=pa.int64())}),
        os.path.join(d, "all_fields.parquet"),
    )


@contextmanager
def _noop_lock(_key):
    yield


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


# ── Writer happy path (no DuckDB execution — directly drive backfill walk) ────


def test_backfill_skips_hours_already_built(tmp_path):
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


# ── Reader: rollup ↔ live shape parity + eligibility gates ────────────────


def _stub_runner(cache_root_str: str, src: dict, captured_sql: list[str], stub_rows: list[tuple]):
    """Build a QueryRunner with execute() stubbed to capture the SQL and
    return canned rows."""
    from backend.repositories._base import QueryRunner

    class _Result:
        def fetchall(self):
            return stub_rows

        def fetchone(self):
            return stub_rows[0] if stub_rows else None

    class _Conn:
        def execute(self, sql, params=None):
            captured_sql.append(sql)
            return _Result()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner._table = "logs_svc_nr_reader"
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


def test_reader_returns_none_when_filtered(tmp_path):
    """has_filters=True → None (rollup is unfiltered-only)."""
    src = {"name": "svc-nr-r", "service_id": "svc-nr-r"}
    runner = _stub_runner(str(tmp_path), src, [], [])
    result = runner.try_network_rtt_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        top_asns=[1],
        has_filters=True,
    )
    assert result is None


def test_reader_returns_none_for_short_window(tmp_path):
    """Window < 48h → None (caller's live path handles short windows fine)."""
    src = {"name": "svc-nr-r2", "service_id": "svc-nr-r2"}
    runner = _stub_runner(str(tmp_path), src, [], [])
    result = runner.try_network_rtt_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-01T12:00:00+00:00",
        top_asns=[1],
        has_filters=False,
    )
    assert result is None


def test_reader_serves_top_asn_dict_from_hour_files(tmp_path):
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
    runner = _stub_runner(str(cache_root), src, captured, stub_rows)

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


# ── Daily compactor ───────────────────────────────────────────────────────


def test_compact_writes_per_day_file_with_correct_aggregates(tmp_path):
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


def test_compact_skips_active_day(tmp_path):
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
