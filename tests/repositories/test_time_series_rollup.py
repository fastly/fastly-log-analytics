"""Regression tests for QueryRunner.try_time_series_from_rollup.

The function had zero direct test coverage when it shipped, and the cursor
iterating in the request's input timezone (instead of UTC) silently dropped
hours from the response when the FE sent timezone-offset strings — see the
2026-06-11 missing-tail bar-chart incident. These tests pin the contract
explicitly so a regression would fail at CI time.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from backend.repositories._base import QueryRunner


def _write_bundle(bundled_root: Path, hour_str: str, total_requests: int = 600) -> None:
    """Create a minimal time_series.parquet under bundled_root with rows for
    every minute of ``hour_str`` (UTC). One row per minute; the per-hour sum is
    deterministic at total_requests.
    """
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / "time_series.parquet"

    base = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    per_min = total_requests // 60
    rows_sql = ", ".join(
        f"(TIMESTAMPTZ '{(base + timedelta(minutes=m)).isoformat()}', {per_min}, 0, 0, 0, 0, 0, 0.0, 0, '{hour_str}')"
        for m in range(60)
    )
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) "
            f"AS t(bucket, requests, status_4xx, status_5xx, hits, cache_total, "
            f"resp_bytes_sum, ttfb_sum, ttfb_count, hour)) "
            f"TO '{out}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def _write_per_field_marker(per_field_root: Path, field: str, hour_str: str) -> None:
    """Create the per-field rollup dir so _hour_had_any_data sees the hour.

    The bundled-root reader iterates closed hours and, on a missing bundle,
    checks the per-field tree to decide between "skip (no data this hour)"
    and "fall back to raw (data exists but bundle is mid-build)". Tests
    create both halves so the reader behaves like in production.
    """
    (per_field_root / f"field={field}" / f"hour={hour_str}").mkdir(parents=True, exist_ok=True)


@pytest.fixture
def rollup_layout(tmp_path):
    """Build a fake rollup layout under tmp_path and return the bundled root."""
    cache_dir = tmp_path / "cache" / "test-bucket"
    bundled = cache_dir / "rollups" / "hour_bundled"
    per_field = cache_dir / "rollups" / "hour"
    bundled.mkdir(parents=True)
    per_field.mkdir(parents=True)
    return bundled, per_field, str(cache_dir)


def _make_source(cache_override: str) -> dict:
    """Source dict that pins _cache_dir to a temp path via the override hook."""
    return {
        "name": "test_service",
        "service_id": "test-service-id",
        "_cache_dir_override": cache_override,
    }


def _populate_past_window(
    bundled, per_field, *, hours_back_end: int, span_hours: int
) -> tuple[datetime, datetime, list[str]]:
    """Write bundles + per-field markers for ``span_hours`` UTC hours ending
    ``hours_back_end`` hours before "now". Returns (start_utc, end_utc, hours).

    Using a window in the past keeps the active-hour boundary irrelevant —
    every cursor string is < active_hour_str, so crosses_active stays False
    and the rollup reader doesn't need to invoke the live branch. This makes
    the cursor-iteration assertion deterministic without freezing time.
    """
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = now - timedelta(hours=hours_back_end)
    start = end - timedelta(hours=span_hours)
    hours = []
    cursor = start
    while cursor < end:
        hs = cursor.strftime("%Y-%m-%d-%H")
        hours.append(hs)
        _write_bundle(bundled, hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", hs)
        cursor += timedelta(hours=1)
    return start, end, hours


class TestTryTimeSeriesFromRollup:
    """Pin try_time_series_from_rollup's cursor + window semantics."""

    def test_cdt_offset_input_serves_full_utc_window(self, rollup_layout):
        """REGRESSION: the cursor must iterate in UTC, not in the input's TZ.

        Bug history: when start_time was a CDT-offset string like
        '...-05:00', the reader's cursor inherited tz=CDT from
        datetime.fromisoformat. cursor.strftime('%Y-%m-%d-%H') then
        produced CDT-named hour strings, but the bundles on disk are
        keyed by UTC hours. The names don't match — so this test (whose
        bundles exist only under UTC names) would have returned no rows
        if the bug were present.

        Pre-fix observed in prod: 5 hours dropped from the 24h chart.
        Pre-fix in this test: 0 hours returned (bundles never matched).
        """
        bundled, per_field, cache_dir = rollup_layout
        start_utc, end_utc, hours = _populate_past_window(bundled, per_field, hours_back_end=1, span_hours=24)
        assert len(hours) == 24

        # Re-express the same wall-clock instants with a CDT offset
        # (UTC-5) — that's the actual bug trigger.
        cdt = timezone(timedelta(hours=-5))
        start_cdt_iso = start_utc.astimezone(cdt).isoformat()
        end_cdt_iso = end_utc.astimezone(cdt).isoformat()

        src = _make_source(cache_dir)
        con = duckdb.connect()
        try:
            runner = QueryRunner(con, src)
            rows = runner.try_time_series_from_rollup(
                chart_metric="requests",
                interval="1 hour",
                start_time=start_cdt_iso,
                end_time=end_cdt_iso,
                table_name="not_used_when_only_rollup_hours",
                where_clause="1=1",
                params=[],
            )
        finally:
            con.close()

        assert rows is not None, (
            "rollup reader returned None — the eligibility check failed or "
            "bundles weren't found (cursor-tz bug would cause this)."
        )
        # All 24 closed-hour UTC bundles should be served. Pre-fix: 0
        # because cursor iterated CDT hours and the per-field markers
        # (also UTC-named) wouldn't match either, so every hour was
        # silently 'skipped' as "no data".
        assert len(rows) == 24, (
            f"expected 24 hourly buckets covering the full UTC window, got "
            f"{len(rows)}. Sample: first={rows[0] if rows else None}, "
            f"last={rows[-1] if rows else None}. "
            f"A length of 0 strongly suggests the cursor-tz regression."
        )
        # Spot-check the actual content: the first and last UTC hours
        # match what we wrote. DuckDB serializes TIMESTAMPTZ in the
        # session tz, so the response strings may carry a non-UTC offset
        # on dev machines — compare as parsed datetimes to be tz-agnostic.
        row_instants = {datetime.fromisoformat(r["time"]).astimezone(UTC) for r in rows}
        assert start_utc in row_instants, (
            f"first UTC hour {start_utc.isoformat()} missing from response — "
            f"likely cursor-tz regression (sample times: {[r['time'] for r in rows[:3]]})"
        )
        last_closed_utc = end_utc - timedelta(hours=1)
        assert last_closed_utc in row_instants, (
            f"last UTC hour {last_closed_utc.isoformat()} missing — "
            f"likely cursor never reached the end of the UTC window "
            f"(sample tail: {[r['time'] for r in rows[-3:]]})"
        )

    def test_utc_offset_input_also_serves_full_window(self, rollup_layout):
        """Sibling: UTC-offset input (+00:00) also yields the full window.

        The pre-fix code path happened to work for UTC-offset input because
        the cursor's tz was already UTC. Pin that the UTC path still works
        after the fix so a future 'force input tz' regression would also
        be caught.
        """
        bundled, per_field, cache_dir = rollup_layout
        start_utc, end_utc, hours = _populate_past_window(bundled, per_field, hours_back_end=1, span_hours=24)

        src = _make_source(cache_dir)
        con = duckdb.connect()
        try:
            runner = QueryRunner(con, src)
            rows = runner.try_time_series_from_rollup(
                chart_metric="requests",
                interval="1 hour",
                start_time=start_utc.isoformat(),  # +00:00
                end_time=end_utc.isoformat(),
                table_name="not_used",
                where_clause="1=1",
                params=[],
            )
        finally:
            con.close()

        assert rows is not None
        assert len(rows) == 24, f"UTC-offset path also expected 24 buckets, got {len(rows)}"


class TestActiveHourDirectLiveSlice:
    """The live active-hour slice must come from the direct buffer/hourly
    read when the caller declares the window unfiltered — never the bound
    view (which pays manifest/union overhead per load)."""

    def test_crosses_active_serves_live_slice_from_buffer_not_view(self, rollup_layout):
        """table_name deliberately does NOT exist: if the reader ever routes
        the live slice through the view branch, the final query raises and
        the reader returns None — failing this test loudly."""
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        # Two fully-bundled closed hours before the active hour.
        cursor = active_start - timedelta(hours=2)
        while cursor < active_start:
            hs = cursor.strftime("%Y-%m-%d-%H")
            _write_bundle(bundled, hs, total_requests=600)
            _write_per_field_marker(per_field, "requests", hs)
            cursor += timedelta(hours=1)

        # Buffer parquet carrying 3 active-hour rows.
        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        ts = (active_start + timedelta(minutes=5)).isoformat()
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES (TIMESTAMPTZ '{ts}'), (TIMESTAMPTZ '{ts}'), "
                f"(TIMESTAMPTZ '{ts}')) AS t(timestamp)) "
                f"TO '{buffer_dir / 'live.parquet'}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

        runner = QueryRunner(duckdb.connect(), _make_source(cache_dir))
        st = (active_start - timedelta(hours=2)).isoformat()
        et = (active_start + timedelta(minutes=30)).isoformat()
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=st,
            end_time=et,
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert rows is not None, "reader fell back to the (nonexistent) view for the live slice"
        # Key by parsed datetime — safe_iso's exact string form (offset vs Z)
        # is not part of this test's contract.
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        assert sum(by_time.values()) == 600 + 600 + 3
        assert by_time[active_start] == 3

    def test_filtered_window_never_uses_direct_live_read(self, rollup_layout):
        """Without unfiltered_window=True the live slice MUST go through the
        table/where_clause branch (row filters would be silently dropped by
        the direct read). Pinned the same way: a nonexistent table means the
        reader must return None instead of serving a filter-ignoring result."""
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", hs)

        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        ts = (active_start + timedelta(minutes=5)).isoformat()
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES (TIMESTAMPTZ '{ts}')) AS t(timestamp)) "
                f"TO '{buffer_dir / 'live.parquet'}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

        runner = QueryRunner(duckdb.connect(), _make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="url = ?",
            params=["/x"],
        )

        assert rows is None, "filtered window must not serve the live slice via the direct read"
