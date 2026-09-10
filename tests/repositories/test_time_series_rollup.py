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


class TestPartialHourMergeIntoRollupReaders:
    """The partial-hour speed layer (Task 7's ``_partial_hour_adjusted_live_start``)
    must be wired into BOTH the time_series and count readers' live branches:
    narrowing ``live_start`` to the partial rollup's watermark (so the direct
    live read never re-scans rows the partial rollup already folded in) AND
    summing the partial rollup's rows into the response.

    Each test is built so it fails if EITHER half of the wiring is missing:
    dropping the narrowing double-counts the watermark-covered buffer row,
    dropping the partial-rows merge undercounts by that same row — neither
    produces the expected total.
    """

    def _write_partial_hour(self, cache_dir: str, hour: str, *, watermark_epoch: float, count: int) -> None:
        from backend.core.rollups.partial_hour import _all_fields_path, _write_partial_hour_watermark

        _write_partial_hour_watermark({"_cache_dir_override": cache_dir}, hour, watermark_epoch)
        path = _all_fields_path({"_cache_dir_override": cache_dir}, hour)
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES ('requests', '', {count})) "
                f"AS t(field, value, count)) TO '{path}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

    def _write_buffer_rows(self, cache_dir: str, *timestamps: datetime) -> None:
        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        values_sql = ", ".join(f"(TIMESTAMPTZ '{ts.isoformat()}')" for ts in timestamps)
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(timestamp)) "
                f"TO '{buffer_dir / 'live.parquet'}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

    def test_time_series_merges_partial_hour_without_double_counting(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_hour_str = active_start.strftime("%Y-%m-%d-%H")

        # One closed hour before the active hour.
        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        # Buffer holds two active-hour rows: one BEFORE the partial rollup's
        # watermark (already folded into the partial rollup — must NOT be
        # re-counted by the live direct read) and one AFTER it (not yet
        # folded — must be picked up live).
        watermark_instant = active_start + timedelta(minutes=10)
        already_merged_row = active_start + timedelta(minutes=5)
        not_yet_merged_row = active_start + timedelta(minutes=20)
        self._write_buffer_rows(cache_dir, already_merged_row, not_yet_merged_row)

        # Partial rollup already folded in the one row before the watermark.
        self._write_partial_hour(cache_dir, active_hour_str, watermark_epoch=watermark_instant.timestamp(), count=1)

        runner = QueryRunner(duckdb.connect(), _make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert rows is not None, "reader fell back to raw — partial-hour wiring likely broke the live SQL"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        # Expected: 600 (closed hour) + 1 (partial rollup's already-merged
        # row) + 1 (live direct read's not-yet-merged row) = 602.
        # If narrowing were dropped: the live direct read would ALSO count
        # the already-merged row (its business timestamp is still >=
        # active_start), giving 603 (double count).
        # If the partial-rows merge were dropped: the already-merged row
        # would never surface at all (the narrowed live scan explicitly
        # excludes it), giving 601 (undercount).
        assert by_time[active_start] == 2, f"active-hour bucket should sum to 2 (1 partial + 1 live), got {by_time}"
        assert sum(by_time.values()) == 602, f"expected 602 total, got {sum(by_time.values())} ({by_time})"

    def test_count_merges_partial_hour_without_double_counting(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_hour_str = active_start.strftime("%Y-%m-%d-%H")

        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        watermark_instant = active_start + timedelta(minutes=10)
        already_merged_row = active_start + timedelta(minutes=5)
        not_yet_merged_row = active_start + timedelta(minutes=20)
        self._write_buffer_rows(cache_dir, already_merged_row, not_yet_merged_row)
        self._write_partial_hour(cache_dir, active_hour_str, watermark_epoch=watermark_instant.timestamp(), count=1)

        runner = QueryRunner(duckdb.connect(), _make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert total is not None, "reader fell back to raw — partial-hour wiring likely broke the live SQL"
        assert total == 602, f"expected 602 (600 closed + 1 partial + 1 live), got {total}"

    def test_non_requests_metric_keeps_full_live_scan_when_partial_hour_exists(self, rollup_layout):
        """REGRESSION (Task 8 review round 1): only "requests" has a
        compensating partial-rollup merge. Before this fix, live_start was
        narrowed for EVERY chart_metric whenever a partial-hour rollup
        watermark existed — for "5xx"/"4xx"/"hit_rate" (no compensating
        merge), that silently dropped the
        [original_live_start, watermark) range from BOTH the rollup branch
        (still-open hour, not covered) and the narrowed live branch
        (skipped) — real data loss, not just "no active-hour boost".

        This pins that a non-"requests" metric's live branch keeps scanning
        the FULL original live range regardless of whether a partial-hour
        rollup happens to exist for the active hour.
        """
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_hour_str = active_start.strftime("%Y-%m-%d-%H")

        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        # A partial-hour rollup watermark exists for the active hour (the
        # steady-state case) — this alone must NOT narrow live_start for a
        # metric with no compensating merge.
        watermark_instant = active_start + timedelta(minutes=10)
        self._write_partial_hour(cache_dir, active_hour_str, watermark_epoch=watermark_instant.timestamp(), count=1)

        before_watermark_row = active_start + timedelta(minutes=5)  # status 500 -> counts as 5xx
        after_watermark_row = active_start + timedelta(minutes=20)  # status 200 -> does not

        con = duckdb.connect()
        table_name = "live_5xx_regression_table"
        con.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM (VALUES "
            f"(TIMESTAMPTZ '{before_watermark_row.isoformat()}', 500), "
            f"(TIMESTAMPTZ '{after_watermark_row.isoformat()}', 200)) "
            f"AS t(timestamp, status)"
        )

        runner = QueryRunner(con, _make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="5xx",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name=table_name,
            where_clause="1=1",
            params=[],
        )

        assert rows is not None, "reader fell back to raw unexpectedly"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        # 1 of 2 active-hour rows is 5xx -> 50.0%. Pre-fix (unconditional
        # narrowing), the before_watermark_row would have been silently
        # excluded from the live scan with nothing compensating for it,
        # yielding 0.0 instead of 50.0.
        assert by_time[active_start] == 50.0, (
            f"expected 50.0 (1 of 2 active-hour rows is 5xx) — a value of 0.0 means the "
            f"before-watermark 5xx row was dropped by an unconditional narrowing, got {by_time}"
        )


class TestMissingHourLiveHealInTimeSeriesAndCountReaders:
    """REGRESSION (2026-09-09, verified live): unlike ``execute_top_n_rollups``
    (which live-heals a CLOSED hour with no rollup bundle), the time_series
    and count readers silently SKIP such an hour via
    ``collect_hourly_bundle_paths`` — contributing zero for it instead of
    live-healing. On a real deployment with a 9/24-hour writer-coverage gap,
    top-N panels stayed correct (they heal) while the traffic chart showed
    ``[]`` and the total-requests count showed ``0``, despite real traffic
    existing in those gap hours.

    Each test writes one CLOSED hour with a real rollup bundle (H1) and one
    CLOSED hour with NO bundle and NO per-field marker at all (H2) — a pure
    writer-coverage gap, not the mid-build case that already triggers
    ``collect_hourly_bundle_paths``'s "return None" fallback. H2's rows exist
    only in the raw base table (``logs_<service>``), so a correct reader must
    live-heal H2 to include them.
    """

    def test_time_series_heals_missing_bundle_hour(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        h1_start = active_start - timedelta(hours=2)
        h2_start = active_start - timedelta(hours=1)
        h1_str = h1_start.strftime("%Y-%m-%d-%H")

        # H1: normal, fully-bundled closed hour.
        _write_bundle(bundled, h1_str, total_requests=600)
        _write_per_field_marker(per_field, "requests", h1_str)

        # H2: writer-coverage gap — no bundle, no per-field marker either.
        # Its rows exist only in the raw base table.
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO logs_test_service VALUES (?), (?), (?), (?), (?)",
            [h2_start + timedelta(minutes=m) for m in (1, 2, 3, 4, 5)],
        )

        runner = QueryRunner(con, _make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=h1_start.isoformat(),
            end_time=(h2_start + timedelta(hours=1)).isoformat(),
            table_name="not_used",
            where_clause="1=1",
            params=[],
        )
        con.close()

        assert rows is not None, "reader fell back to raw unexpectedly"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        assert by_time.get(h2_start) == 5, (
            f"H2 (writer-coverage gap hour) contributed {by_time.get(h2_start)!r} instead of 5 — "
            f"the missing-hour heal did not run. Full response: {by_time}"
        )
        assert sum(by_time.values()) == 605, f"expected 600 (H1) + 5 (healed H2) = 605, got {by_time}"

    def test_count_heals_missing_bundle_hour(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        h1_start = active_start - timedelta(hours=2)
        h2_start = active_start - timedelta(hours=1)
        h1_str = h1_start.strftime("%Y-%m-%d-%H")

        _write_bundle(bundled, h1_str, total_requests=600)
        _write_per_field_marker(per_field, "requests", h1_str)

        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO logs_test_service VALUES (?), (?), (?), (?), (?)",
            [h2_start + timedelta(minutes=m) for m in (1, 2, 3, 4, 5)],
        )

        runner = QueryRunner(con, _make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=h1_start.isoformat(),
            end_time=(h2_start + timedelta(hours=1)).isoformat(),
            table_name="not_used",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )
        con.close()

        assert total is not None, "reader fell back to raw unexpectedly"
        assert total == 605, (
            f"expected 600 (H1 bundle) + 5 (healed H2) = 605, got {total} — "
            f"the missing-hour heal did not run, undercounting the writer-coverage gap hour."
        )
