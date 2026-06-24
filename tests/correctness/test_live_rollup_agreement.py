"""Numeric-agreement tests for the rollup read path (Deliverable 1).

The existing rollup tests verify the WRITER (logs → bundle) and the READER
(bundle → window) *separately, against hand-written fixtures*. Nothing
asserts that, for the **same raw logs and the same query**, the three views
the API can serve agree to the value:

    1. live      — aggregation straight over the raw rows
    2. rollup     — read of the per-hour bundle the writer produced
    3. merged     — the active-hour-live ⊕ closed-hour-rollup path the API
                    actually serves (``QueryRunner.try_*_from_rollup``)

This module seeds one synthetic raw-log table, runs the *real* rollup
writer over it, then drives the *real* reader and compares against a live
aggregation computed independently from the same table. Divergence ⇒ the
dashboard is showing a silently-wrong number.

Harness notes
-------------
* One in-memory DuckDB connection holds the base table ``logs`` AND reads
  the bundle parquets the writer emits — so writer, reader, and the live
  control all see byte-identical data.
* ``_cache_dir_override`` on the source dict points both the writer's
  ``_hour_bundled_root`` and the reader's at the same tmp tree (see
  ``backend.core.duckdb._cache_dir``), so no ``_cache_dir`` patch needed.
* "now" matters for the *active-hour merge seam* tests (closed-hour rollup ⊕
  active-hour live). Those use the ``frozen_clock`` fixture to pin "now" to a
  deterministic instant via ``time_machine`` (which patches ``datetime`` at the
  C level, so the reader's function-local ``datetime.now(UTC)`` is affected
  even though monkeypatch can't reach it). Freezing removes two problems the
  old real-clock anchoring had: the UTC-hour-00 skip, and a latent flake where
  an hour rollover mid-test turned the seeded "active" hour into a closed hour
  with no bundle (reader then declines). Closed-window tests don't need it —
  their windows are anchored well in the past regardless of the minute.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone

import duckdb
import pytest

from backend.repositories._base import QueryRunner

# ── Harness ────────────────────────────────────────────────────────────────


@contextmanager
def _noop_lock(_key):
    yield


class _NonClosing:
    """Delegating proxy whose ``close()`` is a no-op.

    The rollup writers call ``con.close()`` in their ``finally`` (e.g.
    ``time_series.py:179``). Our agreement harness shares ONE in-memory
    connection across writer, reader, and the live control — letting the
    writer close it would orphan the base table. This proxy forwards every
    attribute to the real connection but swallows ``close()``.
    """

    def __init__(self, con):
        self._con = con

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._con, name)


_BASE_COLS_SQL = (
    "timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, "
    "resp_bytes BIGINT, ttfb DOUBLE, url VARCHAR, ip VARCHAR, ja4 VARCHAR"
)


def _seed_base_table(con: duckdb.DuckDBPyConnection, rows: list[dict], table: str = "logs") -> None:
    """Create the base log table the writer + live control both read."""
    con.execute(f"CREATE TABLE {table} ({_BASE_COLS_SQL})")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("status", 200),
                r.get("cache"),
                r.get("resp_bytes", 0),
                r.get("ttfb"),
                r.get("url", "/"),
                r.get("ip", "10.0.0.1"),
                r.get("ja4", "ja4-x"),
            ],
        )


def _make_src(cache_dir: str) -> dict:
    return {
        "name": "agree_svc",
        "service_id": "agree-svc-id",
        "_cache_dir_override": cache_dir,
    }


def _build_time_series_bundles(con, src, hours: list[str], table: str = "logs") -> int:
    """Run the REAL ``build_time_series_bundles`` writer against ``table``."""
    from unittest.mock import patch

    from backend.core.rollups import time_series

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        return time_series.build_time_series_bundles(src["service_id"], src, hours)


def _hours_between(start: datetime, end: datetime) -> list[str]:
    out, cur = [], start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        out.append(cur.strftime("%Y-%m-%d-%H"))
        cur += timedelta(hours=1)
    return out


def _active_hour_start() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _live_requests(con, start: datetime, end: datetime, table: str = "logs") -> int:
    return con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE timestamp >= ? AND timestamp < ?",
        [start, end],
    ).fetchone()[0]


# Deterministic "now" for the active-hour seam tests: hour 12 (so "today" has a
# closed hour to merge — kills the UTC-hour-00 skip) and minute 10 (so a 20-min
# active window ends at :30, comfortably inside the hour — kills the latent
# hour-rollover flake). The date is arbitrary; the in-memory harness has no
# real-time dependency (bundles live in tmp; queries use seeded timestamps).
_FROZEN_NOW = "2026-06-15 12:10:00+00:00"


@pytest.fixture
def frozen_clock():
    """Freeze ``datetime.now`` (incl. the reader's function-local import) at
    :data:`_FROZEN_NOW` so the closed/active-hour seam is deterministic."""
    import time_machine

    with time_machine.travel(_FROZEN_NOW, tick=False) as traveller:
        yield traveller


# ── time_series: requests, closed-window agreement ──────────────────────────


@pytest.mark.parametrize("interval", ["1 minute", "1 hour", "1 day"])
def test_ts_requests_closed_window_rollup_equals_live(tmp_path, interval):
    """Window entirely in the past (no active-hour merge): the rollup read
    must equal the live aggregation, bucket-for-bucket, with no duplicate
    buckets, for every supported interval."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    # Two full UTC days, ending at the start of *yesterday's* last hour so
    # the whole window is closed (well before the active hour).
    end = (_active_hour_start() - timedelta(hours=2)).replace(hour=0)
    start = end - timedelta(days=2)

    rows = []
    cur = start
    n = 0
    while cur < end:
        # Deterministic, varying per-hour volume so a dropped/duplicated
        # bucket changes the totals.
        for k in range((cur.hour % 5) + 1):
            rows.append({"timestamp": cur + timedelta(minutes=3 * k + 1), "status": 200})
            n += 1
        cur += timedelta(hours=1)
    assert n > 0
    _seed_base_table(con, rows)

    built = _build_time_series_bundles(con, src, _hours_between(start, end))
    assert built > 0, "writer produced no bundles for a populated window"

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval=interval,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, "rollup reader unexpectedly declined an eligible window"

    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate buckets in rollup output: {times}"

    rollup_total = sum(m["value"] for m in merged)
    live_total = _live_requests(con, start, end)
    assert rollup_total == live_total, f"interval={interval}: rollup total {rollup_total} != live total {live_total}"


@pytest.mark.parametrize(
    "interval,delta",
    [("1 minute", timedelta(minutes=1)), ("1 hour", timedelta(hours=1))],
)
def test_ts_requests_closed_window_per_bucket_agrees(tmp_path, interval, delta):
    """SEAM-05: the closed-window test above asserts only the window TOTAL and
    no-duplicate-buckets — but its docstring claims "bucket-for-bucket". This
    pins the actual per-bucket contract: EACH emitted bucket's value equals the
    live count over exactly that bucket's [start, start+interval) range. Seed
    deliberately varies the count per minute AND per hour, so a wrong bucket
    boundary or a shifted/merged bucket changes a value and trips here even when
    the total still balances. '1 minute' is the resolution the prior test never
    exercised at the value level."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    # 3 fully-closed hours; per-minute count varies as ((hour_idx + minute) % 4)
    # so adjacent minutes and adjacent hours hold different totals.
    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    hour_idx = 0
    while cur < end:
        for minute in range(60):
            for _ in range((hour_idx + minute) % 4):
                rows.append({"timestamp": cur + timedelta(minutes=minute), "status": 200})
        cur += timedelta(hours=1)
        hour_idx += 1
    assert rows
    _seed_base_table(con, rows)

    _build_time_series_bundles(con, src, _hours_between(start, end))
    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval=interval,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, "rollup reader unexpectedly declined an eligible window"

    for m in merged:
        bstart = datetime.fromisoformat(m["time"]).astimezone(UTC)
        live_in_bucket = con.execute(
            "SELECT COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
            [bstart, bstart + delta],
        ).fetchone()[0]
        assert m["value"] == live_in_bucket, (
            f"interval={interval}: bucket {bstart.isoformat()} value {m['value']} != live {live_in_bucket}"
        )
    # Total still balances — together with the per-bucket check this rules out a
    # dropped non-empty bucket (its rows would vanish from the sum).
    assert sum(m["value"] for m in merged) == _live_requests(con, start, end)


# ── time_series: the active-hour MERGE SEAM ─────────────────────────────────


def _seed_seam_scenario(con):
    """Seed rows in: a prior closed day, an earlier *closed hour of today*,
    and the *active hour*. Returns (today_start, active_start, window_end).

    Skips at UTC hour 00 where today has no closed hour to collide with.
    """
    active_start = _active_hour_start()
    if active_start.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")

    today_start = active_start.replace(hour=0)
    prior_closed_hour = active_start - timedelta(hours=1)  # still today, closed
    window_end = active_start + timedelta(minutes=20)  # inside the active hour

    rows = []
    # Prior day (closed) — 4 rows, all 200.
    pday = today_start - timedelta(hours=20)
    for k in range(4):
        rows.append({"timestamp": pday + timedelta(minutes=2 * k), "status": 200})
    # Earlier closed hour of TODAY — 2 rows (one 5xx).
    rows.append({"timestamp": prior_closed_hour + timedelta(minutes=5), "status": 200})
    rows.append({"timestamp": prior_closed_hour + timedelta(minutes=6), "status": 500})
    # Active hour (live only) — 3 rows (one 5xx).
    rows.append({"timestamp": active_start + timedelta(minutes=2), "status": 200})
    rows.append({"timestamp": active_start + timedelta(minutes=4), "status": 500})
    rows.append({"timestamp": active_start + timedelta(minutes=6), "status": 200})
    _seed_base_table(con, rows)
    return today_start, active_start, window_end


def test_ts_merge_seam_hour_interval_no_double_count(tmp_path, frozen_clock):
    """interval='1 hour' crossing the active hour: each hour is its own
    bucket, so live (active hour) and rollup (closed hours) never collide.
    Pin the good behavior + value agreement."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate hour buckets across the seam: {times}"
    assert sum(m["value"] for m in merged) == _live_requests(con, start, window_end)


def test_ts_merge_seam_day_interval_no_double_count(tmp_path, frozen_clock):
    """interval='1 day' crossing the active hour (regression for DP-1).

    The reader UNIONs the rollup branch (today's *closed* hours → day bucket D)
    with the live branch (the *active* hour → the SAME day bucket D). Before the
    fix it did NOT re-aggregate, so "today" was emitted TWICE, each partial. The
    default 30-day dashboard chart (``useReportConfig.ts`` defaults to '1 day'
    for ≥720h windows) hits exactly this path. The fix wraps the UNION in an
    outer ``GROUP BY out_bucket`` that rebuilds the metric from summed raw
    num/den, so the straddling day collapses to one correct value.
    """
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 day",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None

    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), (
        "duplicate day bucket at the active-hour seam — 'today' is emitted "
        f"once from the rollup (closed hours) and once from live (active hour) "
        f"without being summed: {merged}"
    )
    # And the per-day value must equal the live full-day count.
    today_iso_day = today_start
    today_value = sum(m["value"] for m in merged if datetime.fromisoformat(m["time"]).astimezone(UTC) == today_iso_day)
    assert today_value == _live_requests(con, today_start, window_end), (
        "today's daily bucket value disagrees with the live full-day count"
    )


def test_ts_merge_seam_tz_offset_still_agrees(tmp_path, frozen_clock):
    """Same seam, but the FE sends offset (CDT, UTC-5) ISO strings — the
    reader must iterate UTC hours (2026-06-11 regression) AND still produce
    a clean, agreeing series across the live/rollup seam."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    cdt = timezone(timedelta(hours=-5))
    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=start.astimezone(cdt).isoformat(),
        end_time=window_end.astimezone(cdt).isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate buckets under tz-offset input: {times}"
    assert sum(m["value"] for m in merged) == _live_requests(con, start, window_end)


# ── time_series: rate-metric agreement (5xx) ────────────────────────────────


def test_ts_5xx_rate_closed_window_agrees(tmp_path):
    """The rollup expression ``SUM(status_5xx)*100/SUM(requests)`` must equal
    the raw ``COUNT(*) FILTER(status>=500)*100/COUNT(*)`` per bucket."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    while cur < end:
        # 10 rows/hour, 3 of them 5xx → exactly 30.00 %.
        for k in range(10):
            rows.append({"timestamp": cur + timedelta(minutes=k), "status": 500 if k < 3 else 200})
        cur += timedelta(hours=1)
    _seed_base_table(con, rows)
    _build_time_series_bundles(con, src, _hours_between(start, end))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="5xx",
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    assert merged, "expected hourly 5xx buckets"
    for m in merged:
        assert abs(m["value"] - 30.0) < 1e-9, f"5xx rate {m['value']} != 30.0 at {m['time']}"


# Rate metrics keyed by chart_metric → the live numerator expression. The
# denominator for every rate is the raw row count COUNT(*) (mirrors the rollup
# `SUM(requests)`), so a NULL-cache row still counts in the hit_rate denominator.
_RATE_METRIC_LIVE_NUM = {
    "5xx": "COUNT(*) FILTER (WHERE status >= 500)",
    "4xx": "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)",
    "hit_rate": "COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE'))",
}

# The reader rounds rate percentages to 2 decimals, so a full-precision live
# re-derivation (e.g. 27.2727…) differs from the served value (27.27) by up to
# half a unit-in-the-last-place. This tolerance absorbs that rounding while
# staying far below any real divergence: a value error is ≥ one row's worth
# (≥1–10pp here) and the averaging bug this guards against is tens of pp off.
_RATE_TOL = 0.01


@pytest.mark.parametrize("metric", ["5xx", "4xx", "hit_rate"])
def test_ts_rate_metrics_closed_window_per_bucket_agree(tmp_path, metric):
    """SEAM-04: only ``5xx`` had a value-agreement test, and only at a flat
    30%. ``4xx`` and ``hit_rate`` had NONE. Pin all three per hour bucket
    against the live re-derivation ``num*100/COUNT(*)``. The seed mixes
    statuses AND cache states — including NULL-cache rows — so:
      * hit_rate's denominator must be the full row count (rollup
        ``SUM(requests)``); if it were ``SUM(cache_total)`` (the unused
        non-null-cache column the writer also emits) the NULL-cache rows make
        it diverge and this fails, and
      * each bucket carries a distinct rate so a shifted bucket is caught."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    hour_idx = 0
    while cur < end:
        # 10 rows/hour; status + cache mix shifts per hour so each bucket's
        # 4xx / 5xx / hit_rate differs from its neighbours. Some rows carry a
        # NULL cache (k == 9) that must still count in the denominator.
        for k in range(10):
            if k < 2 + hour_idx:
                status = 500
            elif k < 4 + hour_idx:
                status = 404
            else:
                status = 200
            if k == 9:
                cache = None
            elif k < 3:
                cache = "HIT" if k == 0 else "HIT-STALE"
            else:
                cache = "MISS"
            rows.append({"timestamp": cur + timedelta(minutes=k), "status": status, "cache": cache})
        cur += timedelta(hours=1)
        hour_idx += 1
    _seed_base_table(con, rows)
    _build_time_series_bundles(con, src, _hours_between(start, end))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric=metric,
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, f"rollup reader declined an eligible window for {metric}"
    assert merged, f"expected hourly {metric} buckets"
    num_expr = _RATE_METRIC_LIVE_NUM[metric]
    for m in merged:
        bstart = datetime.fromisoformat(m["time"]).astimezone(UTC)
        num, den = con.execute(
            f"SELECT {num_expr}, COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
            [bstart, bstart + timedelta(hours=1)],
        ).fetchone()
        expected = (num * 100.0 / den) if den else 0.0
        assert abs(m["value"] - expected) < _RATE_TOL, (
            f"{metric} at {bstart.isoformat()}: rollup {m['value']} != live {expected}"
        )


@pytest.mark.parametrize("metric", ["5xx", "4xx", "hit_rate"])
def test_ts_rate_metrics_day_seam_reaggregate_not_average(tmp_path, frozen_clock, metric):
    """SEAM-03: a rate metric whose day bucket STRADDLES the live/rollup seam
    must re-derive ``SUM(num)/SUM(den)`` over the whole day — never average the
    closed-hour rate with the active-hour rate. The seed gives today's closed
    hour and the active hour deliberately DIFFERENT rates AND different row
    counts, so the weighted answer (correct) and the averaged answer (bug)
    diverge. Asserts today's day bucket equals the live re-derivation over the
    full day [today_start, window_end)."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    active_start = _active_hour_start()
    if active_start.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")
    today_start = active_start.replace(hour=0)
    prior_closed_hour = active_start - timedelta(hours=1)  # still today, closed
    window_end = active_start + timedelta(minutes=20)
    start = today_start - timedelta(days=1)

    rows = []
    # Prior day (closed, separate day bucket) — neutral filler.
    pday = today_start - timedelta(hours=20)
    for k in range(4):
        rows.append({"timestamp": pday + timedelta(minutes=2 * k), "status": 200, "cache": "MISS"})
    # Today CLOSED hour: 8 rows — 2x 5xx, 2x 4xx, 3x HIT/HIT-STALE. (25% 5xx)
    for k in range(8):
        status = 500 if k < 2 else (404 if k < 4 else 200)
        cache = ("HIT" if k == 4 else "HIT-STALE") if 4 <= k < 6 else ("HIT" if k == 6 else "MISS")
        rows.append({"timestamp": prior_closed_hour + timedelta(minutes=k), "status": status, "cache": cache})
    # ACTIVE hour (live only): 3 rows — all 5xx, all MISS. (100% 5xx, 0% hit)
    for k in range(3):
        rows.append({"timestamp": active_start + timedelta(minutes=k + 1), "status": 500, "cache": "MISS"})
    _seed_base_table(con, rows)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric=metric,
        interval="1 day",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, f"rollup reader declined the day-seam window for {metric}"
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate day buckets at the seam for {metric}: {merged}"

    today_bucket = [m for m in merged if datetime.fromisoformat(m["time"]).astimezone(UTC) == today_start]
    assert len(today_bucket) == 1, f"expected exactly one 'today' day bucket, got {today_bucket}"

    num_expr = _RATE_METRIC_LIVE_NUM[metric]
    num, den = con.execute(
        f"SELECT {num_expr}, COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
        [today_start, window_end],
    ).fetchone()
    expected = (num * 100.0 / den) if den else 0.0
    assert abs(today_bucket[0]["value"] - expected) < _RATE_TOL, (
        f"{metric}: day-seam bucket {today_bucket[0]['value']} != weighted live {expected} "
        "(averaging the closed and active rates instead of SUM(num)/SUM(den)?)"
    )


# ── Filtered fallback: filters must NEVER serve unfiltered rollup numbers ────


# ── verified_bots_ts: the OTHER time-series reader (correct seam handling) ───
#
# This is the executable counterpart to DP-1: try_verified_bots_ts_from_rollup
# merges rollup⊕live with an OUTER GROUP BY SUM, so a bucket straddling the
# closed/active boundary is summed (not duplicated). Proving it agrees with
# live across the seam confirms the correct pattern works — and that the
# time_series reader's omission of that step (DP-1) is the anomaly.


def _seed_vbts_table(con, rows, table="logs_vbts"):
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, waf_sig VARCHAR)")
    for ts, sig in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [ts, sig])


def _build_vbts_bundles(con, src, hours, table="logs_vbts"):
    from unittest.mock import patch

    from backend.core.rollups import verified_bots_ts

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return verified_bots_ts.build_verified_bots_ts_bundles(src["service_id"], src, hours)


def _live_vbts(con, st, et, bucket_seconds, table="logs_vbts"):
    """Canonical live verified_bots_ts series over [st, et) — the answer the
    rollup⊕live merge must reproduce exactly (integer SUM, no approximation)."""
    rows = con.execute(
        f"""
        SELECT time_bucket(INTERVAL '{bucket_seconds} seconds', timestamp) AS bucket,
               replace(tag, 'VERIFIED-BOT.', '') AS bot_type, COUNT(*) AS count
        FROM (
            SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag
            FROM {table}
            WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'
              AND timestamp >= ? AND timestamp < ?
        ) sub
        WHERE tag LIKE 'VERIFIED-BOT.%'
        GROUP BY 1, 2
        """,
        [st, et],
    ).fetchall()
    return {(b.astimezone(UTC), bt): int(c) for b, bt, c in rows}


def test_verified_bots_ts_seam_agrees_with_live(tmp_path, frozen_clock):
    """verified_bots_ts rollup⊕live (1-hour buckets) crossing the active hour
    must equal the canonical live series exactly — including the day-straddle
    that DP-1 gets wrong for time_series."""
    src = _make_src(str(tmp_path))
    src["name"] = "vbts_svc"  # distinct cache scoping from other tests
    con = duckdb.connect(":memory:")

    active = _active_hour_start()
    if active.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")
    start = active - timedelta(hours=50)  # ≥ 48h window (reader gate)
    window_end = active + timedelta(minutes=20)  # into the active hour

    rows = []
    # Closed hours: several bot types, incl. a multi-tag waf_sig (unnest) and
    # a tag that must be filtered out (RULE-X).
    rows.append((start + timedelta(hours=1, minutes=5), "VERIFIED-BOT.GOOGLEBOT"))
    rows.append((start + timedelta(hours=1, minutes=6), "VERIFIED-BOT.GOOGLEBOT,RULE-X"))
    rows.append((start + timedelta(hours=1, minutes=7), "VERIFIED-BOT.BINGBOT"))
    rows.append((active - timedelta(hours=1, minutes=10), "VERIFIED-BOT.GOOGLEBOT"))  # last closed hour today
    rows.append((active - timedelta(hours=1, minutes=11), "RULE-ONLY.NOTABOT"))  # excluded entirely
    # Active hour (live only): same + a new type.
    rows.append((active + timedelta(minutes=2), "VERIFIED-BOT.GOOGLEBOT"))
    rows.append((active + timedelta(minutes=3), "VERIFIED-BOT.DUCKDUCKBOT"))
    _seed_vbts_table(con, rows)

    built = _build_vbts_bundles(con, src, _hours_between(start, active))
    assert built > 0

    runner = QueryRunner(con, src)
    got = runner.try_verified_bots_ts_from_rollup(
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        temp_table="logs_vbts",
        bucket_seconds=3600,
        has_filters=False,
    )
    assert got is not None, "verified_bots_ts reader declined an eligible window"

    merged = {}
    for bucket_ts, bot_type, count in got:
        key = (bucket_ts.astimezone(UTC), bot_type)
        merged[key] = merged.get(key, 0) + int(count)
        # No duplicate (bucket, bot_type) rows — the outer GROUP BY SUM must
        # already have collapsed any straddle.
    assert len(got) == len(merged), f"duplicate (bucket, bot_type) rows from the seam merge: {got}"
    assert merged == _live_vbts(con, start, window_end, 3600), "verified_bots_ts merge disagrees with live"


# ── origin_summary: exact-SUM agreement over a closed window ─────────────────


def _seed_origin_table(con, rows, table="logs_origin"):
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, cache VARCHAR, ttfb DOUBLE, ost INTEGER)")
    for ts, cache, ttfb, ost in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", [ts, cache, ttfb, ost])


def _build_origin_bundles(con, src, hours, table="logs_origin"):
    from unittest.mock import patch

    from backend.core.rollups import origin_summary

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return origin_summary.build_origin_summary_bundles(src["service_id"], src, hours)


def test_origin_summary_closed_window_exact_sums_agree(tmp_path):
    """origin_summary's exact (non-percentile) fields — total_misses,
    total_passes, origin_error_rate — must equal the live aggregation over the
    same closed window. (Percentiles are request-weighted/approx by design and
    are NOT asserted here.)"""
    src = _make_src(str(tmp_path))
    src["name"] = "origin_svc"
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=50)  # ≥ 48h, fully closed

    rows = []
    cur = start
    while cur < end:
        # 10 rows/hour: 4 MISS, 2 PASS, 4 HIT; 3 origin-5xx out of 8 with ost.
        for k in range(10):
            cache = "MISS" if k < 4 else ("PASS" if k < 6 else "HIT")
            ost = (500 if k < 3 else 200) if k < 8 else None
            rows.append((cur + timedelta(minutes=k), cache, 0.05, ost))
        cur += timedelta(hours=1)
    _seed_origin_table(con, rows)

    built = _build_origin_bundles(con, src, _hours_between(start, end))
    assert built > 0

    runner = QueryRunner(con, src)
    got = runner.try_origin_summary_from_rollup(
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        has_filters=False,
        actual_cols={"timestamp", "cache", "ttfb", "ost"},
    )
    assert got is not None and got["has_data"], "origin_summary reader declined an eligible window"

    live = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE cache ILIKE 'MISS%') AS misses,
            COUNT(*) FILTER (WHERE cache ILIKE 'PASS%') AS passes,
            COUNT(*) FILTER (WHERE ost BETWEEN 500 AND 599) * 1.0
                / NULLIF(COUNT(*) FILTER (WHERE ost IS NOT NULL), 0) AS err_rate
        FROM logs_origin WHERE timestamp >= ? AND timestamp < ?
        """,
        [start, end],
    ).fetchone()

    assert got["total_misses"] == live[0], f"total_misses {got['total_misses']} != live {live[0]}"
    assert got["total_passes"] == live[1], f"total_passes {got['total_passes']} != live {live[1]}"
    assert abs(got["origin_error_rate"] - float(live[2])) < 1e-9, (
        f"origin_error_rate {got['origin_error_rate']} != live {live[2]}"
    )


def test_slow_urls_filtered_query_falls_back_to_live(tmp_path):
    """The slow_urls rollup is built unfiltered. A filtered request must
    return ``None`` (caller falls back to live) rather than silently serving
    the unfiltered rollup numbers under the user's filter chips."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    _seed_base_table(con, [])  # no rows needed; the gate fires first
    runner = QueryRunner(con, src)
    out = runner.try_slow_urls_from_rollup(
        start_time=(datetime.now(UTC) - timedelta(days=7)).isoformat(),
        end_time=datetime.now(UTC).isoformat(),
        has_filters=True,
        min_requests=1,
        limit=10,
    )
    assert out is None, "filtered slow_urls request must fall back to live, not serve unfiltered rollup"


def test_time_series_caller_gates_rollup_on_no_filters():
    """Pin the caller-side contract (dashboard.py:274): the time_series
    rollup fast-path is gated on ``not filters``. This is the only thing
    stopping a filtered request from being served unfiltered rollup numbers
    (try_time_series_from_rollup itself takes no filter argument). If a
    refactor drops the ``not filters`` clause, filtered charts would silently
    show unfiltered totals.
    """
    import inspect

    from backend.repositories import dashboard

    src_text = inspect.getsource(dashboard.get_aggregates)
    assert "not filters" in src_text, (
        "dashboard.get_aggregates no longer gates the time_series rollup on "
        "`not filters` — filtered charts could serve unfiltered rollup numbers"
    )


# ── Remaining metrics: exact-column agreement (writer→reader→live) ───────────
#
# The day-prefer readers (slow_urls / origin_summary / network_rtt /
# network_speed / perf_latency) carry request-weighted percentile columns
# (`_approx`) that can't be asserted exactly across hours. Their EXACT columns
# — counts and SUM-reconstructed means — CAN, as long as the synthetic data
# avoids the writers' per-hour top-K + min-requests pruning (each dimension
# value clears the floor every hour and the value count stays under TOP_K).
# These tests construct exactly that no-pruning regime so the exact columns are
# a clean live==rollup comparison; the approx percentile columns are left to
# their documented tolerance.


def _build_rollup(module_path: str, fn_name: str, con, src, hours: list[str], table: str) -> int:
    """Run any rollup writer ``module.fn(service_id, src, hours)`` against the
    shared in-memory connection (writer's ``con.close()`` is a no-op via the
    proxy; ``_safe_table_for`` and the iceberg view lock are patched)."""
    import importlib
    from unittest.mock import patch

    mod = importlib.import_module(module_path)
    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return getattr(mod, fn_name)(src["service_id"], src, hours)


def _closed_48h_window():
    """A ≥48h window entirely in the closed past (the day-prefer readers'
    minimum)."""
    active = _active_hour_start()
    end = active - timedelta(hours=1)
    start = active - timedelta(hours=50)
    return start, end


def _insert(con, table: str, rows: list[tuple]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)


# --- network_speed: exact cnt per (asn, c_speed) ---------------------------


def test_network_speed_closed_window_exact_counts_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "nspeed_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_ns (timestamp TIMESTAMPTZ, asn BIGINT, c_speed VARCHAR)")

    start, end = _closed_48h_window()
    asns = [100, 200]
    speeds = ["broadband", "cellular"]
    rows = []
    cur = start
    while cur < end:
        for asn in asns:
            for sp in speeds:
                for k in range(3):  # 6/asn/hour clears MIN_REQUESTS_PER_HOUR=5
                    rows.append((cur + timedelta(minutes=2 * k + (0 if sp == "broadband" else 1)), asn, sp))
        cur += timedelta(hours=1)
    _insert(con, "logs_ns", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.network_speed",
            "build_network_speed_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_ns",
        )
        > 0
    )

    got = QueryRunner(con, src).try_network_speed_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), top_asns=asns, has_filters=False
    )
    assert got is not None, "network_speed reader declined an eligible window"
    rollup = {(asn, sp): cnt for asn, sp, cnt in got}

    live = {
        (r[0], r[1]): int(r[2])
        for r in con.execute(
            "SELECT asn, c_speed, COUNT(*) FROM logs_ns "
            "WHERE timestamp >= ? AND timestamp < ? AND asn IN (100, 200) "
            "AND c_speed IS NOT NULL AND c_speed != '' GROUP BY asn, c_speed",
            [start, end],
        ).fetchall()
    }
    assert rollup == live, f"network_speed counts disagree: rollup={rollup} live={live}"


# --- slow_urls: exact requests per url (no per-hour pruning) -----------------


def test_slow_urls_closed_window_exact_requests_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "surls_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_su (timestamp TIMESTAMPTZ, url VARCHAR, ttfb DOUBLE)")

    start, end = _closed_48h_window()
    urls = ["/a", "/b", "/c"]
    rows = []
    cur = start
    while cur < end:
        for ui, url in enumerate(urls):
            for k in range(6):  # clears MIN_REQUESTS_PER_HOUR=5; 3 urls << TOP_K
                rows.append((cur + timedelta(minutes=k + ui), url, 0.01 * (k + 1)))
        cur += timedelta(hours=1)
    _insert(con, "logs_su", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.slow_urls", "build_slow_urls_bundles", con, src, _hours_between(start, end), "logs_su"
        )
        > 0
    )

    got = QueryRunner(con, src).try_slow_urls_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), has_filters=False, min_requests=1, limit=100
    )
    assert got is not None and got["has_data"], "slow_urls reader declined an eligible window"
    rollup_req = {r["url"]: r["requests"] for r in got["rows"]}

    live_req = {
        r[0]: int(r[1])
        for r in con.execute(
            "SELECT url, COUNT(*) FROM logs_su WHERE timestamp >= ? AND timestamp < ? AND url IS NOT NULL GROUP BY url",
            [start, end],
        ).fetchall()
    }
    assert rollup_req == live_req, f"slow_urls requests disagree: rollup={rollup_req} live={live_req}"


# --- perf_latency: exact requests + avg per value ---------------------------


def test_perf_latency_closed_window_exact_requests_and_avg_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "perf_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_pl (timestamp TIMESTAMPTZ, url VARCHAR, elapsed DOUBLE)")

    start, end = _closed_48h_window()
    urls = ["/x", "/y"]
    rows = []
    cur = start
    while cur < end:
        for ui, url in enumerate(urls):
            for k in range(6):  # clears PERF_URLS_MIN_REQUESTS_PER_HOUR=5
                rows.append((cur + timedelta(minutes=k + ui), url, float(1000 * (k + 1) + ui * 50)))
        cur += timedelta(hours=1)
    _insert(con, "logs_pl", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.perf_latency",
            "build_perf_latency_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_pl",
        )
        > 0
    )

    got = QueryRunner(con, src).try_perf_latency_from_rollup(
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        dimension="url",
        sort_by="p99",
        has_filters=False,
        min_requests=0,
        limit=100,
    )
    assert got is not None, "perf_latency reader declined an eligible window"
    rollup = {r["value"]: (r["requests"], r["avg_ms"]) for r in got["rows"]}

    live = {
        r[0]: (int(r[1]), float(r[2]) / 1000.0)
        for r in con.execute(
            "SELECT url, COUNT(*), AVG(elapsed) FROM logs_pl "
            "WHERE timestamp >= ? AND timestamp < ? AND url IS NOT NULL GROUP BY url",
            [start, end],
        ).fetchall()
    }
    assert set(rollup) == set(live)
    for value in live:
        assert rollup[value][0] == live[value][0], f"perf_latency requests disagree for {value}"
        assert abs(rollup[value][1] - live[value][1]) < 1e-6, f"perf_latency avg_ms disagrees for {value}"


# --- network_rtt: no exact column → path-selection + ballpark p95 ------------


def test_network_rtt_closed_window_path_and_ballpark(tmp_path):
    """network_rtt exposes only request-weighted percentile approximations —
    nothing exact to assert. Pin that the rollup IS selected for an eligible
    window, returns the requested ASNs, and the weighted p95 lands in a sane
    ballpark of the live APPROX_QUANTILE (within 2×)."""
    src = _make_src(str(tmp_path))
    src["name"] = "nrtt_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_nr (timestamp TIMESTAMPTZ, asn BIGINT, tcp_rtt DOUBLE)")

    start, end = _closed_48h_window()
    asns = [100, 200]
    rtt_dist = [1000, 1500, 2000, 3000, 4000, 5000, 6000, 8000, 10000, 20000]
    rows = []
    cur = start
    while cur < end:
        for asn in asns:
            for k, v in enumerate(rtt_dist):  # 10/asn/hour, fixed distribution
                rows.append((cur + timedelta(minutes=k), asn, float(v + asn)))
        cur += timedelta(hours=1)
    _insert(con, "logs_nr", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.network_rtt",
            "build_network_rtt_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_nr",
        )
        > 0
    )

    got = QueryRunner(con, src).try_network_rtt_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), top_asns=asns, has_filters=False
    )
    assert got is not None, "network_rtt reader declined an eligible window"
    assert set(got) == set(asns), f"network_rtt returned ASNs {set(got)} != {set(asns)}"

    for asn in asns:
        live_p95 = con.execute(
            "SELECT APPROX_QUANTILE(tcp_rtt, 0.95) FROM logs_nr "
            "WHERE timestamp >= ? AND timestamp < ? AND asn = ? AND tcp_rtt > 0",
            [start, end, asn],
        ).fetchone()[0]
        got_p95 = got[asn]["p95_rtt_us"]
        assert got_p95 is not None and got_p95 > 0
        assert 0.5 * live_p95 <= got_p95 <= 2.0 * live_p95, (
            f"network_rtt weighted p95 {got_p95} not in ballpark of live {live_p95} for asn {asn}"
        )


# --- sessions: exact session aggregates across the active-hour seam ----------


def _build_session_bundles(con, src, hours, table="logs_sess"):
    return _build_rollup("backend.core.rollups.sessions", "build_session_bundles", con, src, hours, table)


def test_sessions_seam_exact_aggregates_agree(tmp_path, frozen_clock):
    """sessions reader merges closed-hour rollup ⊕ active-hour live and stitches
    per (ip, ja4). For sessions that don't break the 30-min gap, the exact
    aggregates (req_count / reqs_4xx / reqs_5xx / total_bytes) must equal the
    live totals over the whole window — across the seam.

    live SEAM-01: the clock is frozen (``frozen_clock``) so the seam is
    deterministic. The previous guard (``if active.minute > 35: skip``) was
    DEAD — ``_active_hour_start()`` floors the minute to 0 — so it never fired
    and left the test exposed to an hour-rollover flake: if "now" crossed the
    hour between seeding and the read, the seeded active hour became a closed
    hour with no bundle and the reader declined."""
    src = _make_src(str(tmp_path))
    src["name"] = "sess_svc"
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_sess (timestamp TIMESTAMPTZ, ip VARCHAR, ja4 VARCHAR, "
        "status INTEGER, resp_bytes BIGINT, country VARCHAR, asn INTEGER)"
    )

    active = _active_hour_start()
    start = active - timedelta(hours=3)
    window_end = active + timedelta(minutes=20)
    closed_hours = [active - timedelta(hours=h) for h in (3, 2, 1)]

    # Two entities; each a single continuous session (hourly rollup rows are
    # <30min apart across the seam). Status mix gives nonzero 4xx/5xx.
    entities = [("1.1.1.1", "ja4-a", "US", 100, 110, 200), ("2.2.2.2", "ja4-b", "GB", 300, 90, 222)]
    rows = []
    for ip, ja4, country, b_ok, b_bad, asn in entities:
        for h in closed_hours:
            rows.append((h + timedelta(minutes=5), ip, ja4, 200, b_ok, country, asn))
            rows.append((h + timedelta(minutes=55), ip, ja4, 404, b_bad, country, asn))
        rows.append((active + timedelta(minutes=2), ip, ja4, 200, b_ok, country, asn))
        rows.append((active + timedelta(minutes=5), ip, ja4, 500, b_bad, country, asn))
    _insert(con, "logs_sess", rows)

    assert _build_session_bundles(con, src, [h.strftime("%Y-%m-%d-%H") for h in closed_hours]) == 3

    from backend.repositories.sessions import _get_sessions_from_rollup

    runner = QueryRunner(con, src)
    result = _get_sessions_from_rollup(
        runner=runner,
        con=con,
        src=src,
        table_name="logs_sess",
        actual_cols={"timestamp", "ip", "ja4", "status", "resp_bytes", "country", "asn"},
        start_dt=start,
        end_dt=window_end,
        page=1,
        limit=100,
        sort_by="req_count",
        sort_dir="DESC",
        flagged_only=False,
        min_reqs_flag=1000,
        min_4xx_pct_flag=20.0,
        has_ja4=True,
        has_rtt=False,
        has_edge=False,
        has_edge_sid=False,
        section_timings=[],
        rollup_filters=None,
    )
    assert result is not None, "sessions reader declined an eligible window"
    by_ip = {s["ip"]: s for s in result["sessions"]}
    assert set(by_ip) == {"1.1.1.1", "2.2.2.2"}, f"expected one stitched session per entity, got {by_ip}"

    live = {
        r[0]: (int(r[1]), int(r[2]), int(r[3]), int(r[4]))
        for r in con.execute(
            "SELECT ip, COUNT(*), "
            "  SUM(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END), "
            "  SUM(resp_bytes) "
            "FROM logs_sess WHERE timestamp >= ? AND timestamp < ? GROUP BY ip",
            [start, window_end],
        ).fetchall()
    }
    for ip, (req, r4, r5, tb) in live.items():
        s = by_ip[ip]
        assert s["req_count"] == req, f"{ip}: req_count {s['req_count']} != live {req}"
        assert s["reqs_4xx"] == r4, f"{ip}: reqs_4xx {s['reqs_4xx']} != live {r4}"
        assert s["reqs_5xx"] == r5, f"{ip}: reqs_5xx {s['reqs_5xx']} != live {r5}"
        assert s["total_bytes"] == tb, f"{ip}: total_bytes {s['total_bytes']} != live {tb}"


# --- ip_spread: HLL distinct-IP merge agreement (writer→from_bytes/merge/count→live) ---
#
# ip_spread is the only rollup reader whose merge primitive is *approximate*:
# QueryRunner.execute_ip_spread_rollups folds the per-hour HyperLogLog sketches
# via ``from_bytes(...).merge(...)`` (_base.py:2532-2559) and returns the
# cardinality estimate. The HLL unit tests (tests/utils/test_hll.py) cover the
# primitive and the hour-bundling tests cover the writer, but nothing drove the
# REAL writer → REAL reader merge → live ``COUNT(DISTINCT ip)`` end to end. This
# closes that gap — the last uncovered reader in the agreement harness.


def _build_ip_spread(con, src, table: str = "logs", fields=("ja4",)) -> None:
    """Run the REAL ``_run_ip_spread_per_field`` writer against ``table``.

    Unlike the other builders this writer takes ``table_ident`` / ``where_sql``
    directly (no ``_safe_table_for`` hop) and groups by hour itself, so there is
    no ``hours`` argument — it materializes a sketch for every hour present in
    the table. ``describe_columns`` runs through ``execute_with_stale_view_retry``
    so that passthrough patch is required too; the table must carry an ``ip``
    column or the writer returns early (recompute.py:618)."""
    from unittest.mock import patch

    from backend.core.rollups import recompute

    with (
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        recompute._run_ip_spread_per_field(src["service_id"], src, table, "1=1", list(fields))


def test_ip_spread_closed_window_distinct_count_merge_agrees(tmp_path):
    """The HLL merge reader path (``from_bytes``→``merge``→``count``) must agree
    with the live ``COUNT(DISTINCT ip)`` — within HLL's error bound — after
    merging sketches across multiple closed hours.

    The SAME distinct-IP set is seeded into every hour for each ``ja4`` value,
    so the cross-hour merge is a set UNION: a correct HLL merge returns ~N,
    while a merge that mistakenly SUMMED per-hour counts would return ~N×hours
    and blow past tolerance. That asymmetry is what makes this a real check of
    the merge primitive and not just the single-sketch count. IP strings are
    fixed, so the HLL estimate is deterministic (no per-run flake)."""
    src = _make_src(str(tmp_path))
    src["name"] = "ipspread_svc"
    con = duckdb.connect(":memory:")
    # Match prod: get_connection pins the session to UTC (duckdb.py:861). The
    # writer buckets hours via DuckDB strftime (session TZ) while the reader's
    # _in_window compares Python-UTC hour strings — they only agree on a UTC
    # connection, which is exactly what every prod connection is.
    con.execute("SET TimeZone='UTC'")

    # Fully-closed 3-hour window (the reader skips the active hour).
    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=3)
    closed_hours = [start + timedelta(hours=h) for h in range(3)]

    # Disjoint distinct-IP sets per value, REPEATED every hour so the merge is a
    # union (not a sum). Far under IP_SAMPLE_CAP=5000, so no value is capped.
    value_ips = {
        "ja4-A": [f"10.10.{i // 250}.{(i % 250) + 1}" for i in range(240)],
        "ja4-B": [f"10.20.{i // 250}.{(i % 250) + 1}" for i in range(60)],
    }
    rows = []
    for h in closed_hours:
        for ja4, ips in value_ips.items():
            for i, ip in enumerate(ips):
                rows.append({"timestamp": h + timedelta(seconds=i), "ja4": ja4, "ip": ip})
    _seed_base_table(con, rows)

    _build_ip_spread(con, src, table="logs", fields=("ja4",))

    spread, meta = QueryRunner(con, src).execute_ip_spread_rollups(["ja4"], start.isoformat(), end.isoformat())
    assert spread, "ip_spread reader returned no merged sketches for an eligible closed window"
    # Exactly the two seeded (field, value) pairs come back — no value silently
    # dropped from the merge, none spuriously invented.
    assert set(spread) == {("ja4", "ja4-A"), ("ja4", "ja4-B")}, set(spread)

    for ja4, ips in value_ips.items():
        live = con.execute(
            "SELECT COUNT(DISTINCT ip) FROM logs WHERE timestamp >= ? AND timestamp < ? AND ja4 = ?",
            [start, end, ja4],
        ).fetchone()[0]
        assert live == len(ips), "sanity: the repeated-hour union is the per-value distinct set"
        est = spread[("ja4", ja4)]
        assert abs(est - live) / live <= 0.15, (
            f"ip_spread HLL merge for {ja4}: estimate {est} not within 15% of live "
            f"distinct {live} (a sum-instead-of-union merge bug would land near {live * len(closed_hours)})"
        )

    # Per-field meta: every seeded closed hour contributed a sketch; nothing hit
    # the IP_SAMPLE_CAP, so no (field, value) is flagged capped.
    assert meta["ja4"]["coverage_hours"] == len(closed_hours)
    assert meta["ja4"]["capped_values"] == 0
