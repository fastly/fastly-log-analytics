from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import time
import uuid
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
from backend.utils.hll import HyperLogLog
from tests._helpers.rollups import build_writer_patches, write_hour_bundle, writer_patches
from tests._helpers.rollups import noop_lock as _noop_lock
from tests._helpers.rollups import past_hour as _past_hour
from tests._helpers.sources import make_source

from backend.core import rollups
from backend.core import rollups as rr
from backend.core.rollups import day_bundles, rollups_present
from backend.core.rollups import partial_hour as ph
from backend.core.rollups._common import DAY_BUNDLE_TOP_K, parquet_zstd_target, unlink_quiet, wavg
from backend.core.rollups.day_bundles import _link_day_bundle_alias
from backend.core.rollups.recompute import ADMIN_BACKFILLS, DAY_COMPACTORS, _bundle_metadata
from backend.cron.jobs import compaction as sch
from backend.repositories._base import QueryRunner

# ── From test_rollup_readiness.py ──


@pytest.mark.usefixtures("frozen_clock")
def test_defaults_to_not_ready():
    rr.reset_rollup_coverage_ready()
    assert rr.rollup_coverage_ready("svc-a") is False


@pytest.mark.usefixtures("frozen_clock")
def test_mark_ready_flips_only_that_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is True
    assert rr.rollup_coverage_ready("svc-b") is False


@pytest.mark.usefixtures("frozen_clock")
def test_reset_single_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    rr.mark_rollup_coverage_ready("svc-b")
    rr.reset_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is False
    assert rr.rollup_coverage_ready("svc-b") is True


# ── From test_rollup_spec_registries.py ──


@pytest.mark.usefixtures("frozen_clock")
def test_day_compactors_resolve_to_callables_with_unique_labels() -> None:
    assert len({s.label for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert len({s.compactor for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert all(callable(getattr(rollups, s.compactor)) for s in DAY_COMPACTORS)


@pytest.mark.usefixtures("frozen_clock")
def test_every_per_rollup_day_compactor_is_registered() -> None:
    shipped = {
        name
        for name in vars(day_bundles)
        if re.fullmatch(r"compact_\w+_closed_days_to_daily", name) and name != "compact_closed_days_to_daily"
    }
    assert shipped == {s.compactor for s in DAY_COMPACTORS}


@pytest.mark.usefixtures("frozen_clock")
def test_admin_backfills_resolve_to_callables_with_unique_keys() -> None:
    assert len({k for k, _ in ADMIN_BACKFILLS}) == len(ADMIN_BACKFILLS)
    assert all(callable(getattr(rollups, fn)) for _, fn in ADMIN_BACKFILLS)


@pytest.mark.usefixtures("frozen_clock")
def test_admin_response_keys_do_not_collide() -> None:
    day_keys = [s.admin_key for s in DAY_COMPACTORS if s.admin_key]
    backfill_keys = [k for k, _ in ADMIN_BACKFILLS]
    assert len(set(day_keys) | set(backfill_keys)) == len(day_keys) + len(backfill_keys)


# ── From test_rollups_presence.py ──


def _src(tmp_path) -> dict:
    return {"name": "svc", "bucket": "b", "_cache_dir_override": str(tmp_path)}


def _touch_parquet(root: str, partition: str, name: str = "all_fields.parquet") -> None:
    d = os.path.join(root, partition)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name), "wb") as f:
        f.write(b"PAR1")


@pytest.mark.usefixtures("frozen_clock")
def test_absent_rollups_tree_is_not_present(tmp_path):
    assert rollups_present(_src(tmp_path)) is False


@pytest.mark.usefixtures("frozen_clock")
def test_empty_rollups_root_is_not_present(tmp_path):
    os.makedirs(os.path.join(tmp_path, "rollups"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False


@pytest.mark.usefixtures("frozen_clock")
def test_day_bundled_only_is_present(tmp_path):
    # The regression shape: per-field hour tier compacted away, day bundles remain.
    _touch_parquet(os.path.join(tmp_path, "rollups", "day_bundled"), "day=2026-10-01")
    assert not os.path.isdir(os.path.join(tmp_path, "rollups", "hour"))
    assert rollups_present(_src(tmp_path)) is True


@pytest.mark.usefixtures("frozen_clock")
def test_hour_bundled_only_is_present(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour_bundled"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


@pytest.mark.usefixtures("frozen_clock")
def test_per_field_hour_tier_still_counts(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour", "field=ip"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


@pytest.mark.usefixtures("frozen_clock")
def test_empty_partition_dirs_without_parquet_are_not_present(tmp_path):
    # Defensive: a partition dir left behind with no parquet must not count.
    os.makedirs(os.path.join(tmp_path, "rollups", "day_bundled", "day=2026-10-01"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False


# ── From test_rollups_recompute_dispatch.py ──


EXPECTED_ORDER = (
    "build_time_series_bundles",
    "build_session_bundles",
    "build_slow_urls_bundles",
    "build_origin_summary_bundles",
    "build_network_rtt_bundles",
    "build_network_speed_bundles",
    "build_network_quality_bundles",
    "build_network_heatmap_bundles",
    "build_network_geo_bundles",
    "build_verified_bots_ts_bundles",
    "build_perf_latency_bundles",
    "build_pop_health_bundles",
    "build_origin_dims_bundles",
    "build_origin_latency_ts_bundles",
    "build_security_dims_bundles",
    "build_security_proxies_bundles",
    "build_perf_dims_bundles",
    "build_ngwaf_bots_bundles",
    "build_ngwaf_bots_ts_bundles",
    "build_overview_bundles",
    "build_network_summary_bundles",
    "recompute_wellknown_bots_rollup",
)


def _stub_core(stack: ExitStack) -> None:
    stack.enter_context(patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"))
    stack.enter_context(patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]))
    stack.enter_context(patch("backend.core.rollups.recompute._run_per_field_copy"))
    stack.enter_context(patch("backend.core.rollups.recompute._run_ip_spread_per_field"))
    stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours"))
    stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours_ip_spread"))


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_fires_every_hour_writer_in_order_with_touched_hours():
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    calls: list[tuple[str, str, list[str]]] = []
    with ExitStack() as stack:
        _stub_core(stack)
        for name in EXPECTED_ORDER:
            stack.enter_context(
                patch(
                    f"backend.core.rollups.recompute.{name}",
                    side_effect=lambda sid, src, hours, _n=name: calls.append((_n, sid, list(hours))),
                )
            )
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})

    assert [c[0] for c in calls] == list(EXPECTED_ORDER)
    assert all(c[1] == "svc" and c[2] == [h] for c in calls)


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.parametrize("failing", EXPECTED_ORDER)
def test_one_failing_writer_does_not_stop_the_others(failing, caplog):
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    called: list[str] = []
    with ExitStack() as stack:
        _stub_core(stack)
        for name in EXPECTED_ORDER:
            if name == failing:
                effect = RuntimeError("boom")
            else:

                def effect(sid, src, hours, _n=name):
                    called.append(_n)

            stack.enter_context(patch(f"backend.core.rollups.recompute.{name}", side_effect=effect))
        with caplog.at_level("WARNING"):
            recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})

    assert called == [n for n in EXPECTED_ORDER if n != failing]
    assert any("bundle failed" in r.getMessage() and "boom" in r.getMessage() for r in caplog.records)


@pytest.mark.usefixtures("frozen_clock")
def test_registry_matches_expected_order_and_resolves_on_recompute():
    from backend.core.rollups import recompute
    from backend.core.rollups.recompute import HOUR_WRITERS

    assert tuple(s.builder for s in HOUR_WRITERS) == EXPECTED_ORDER
    assert len({s.label for s in HOUR_WRITERS}) == len(HOUR_WRITERS)
    assert all(callable(getattr(recompute, s.builder)) for s in HOUR_WRITERS)


# ── From test_rollups_sql_helpers.py ──


@pytest.mark.usefixtures("frozen_clock")
def test_wavg_renders_weighted_average_sql():
    assert wavg("p95_us", "requests") == "SUM(p95_us * requests) / NULLIF(SUM(requests), 0)"


@pytest.mark.usefixtures("frozen_clock")
def test_parquet_zstd_target_renders_copy_tail():
    assert parquet_zstd_target("/tmp/x.parquet") == "TO '/tmp/x.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"


@pytest.mark.usefixtures("frozen_clock")
def test_unlink_quiet_removes_existing_file(tmp_path):
    f = tmp_path / "a.parquet"
    f.write_bytes(b"x")
    unlink_quiet(str(f))
    assert not f.exists()


@pytest.mark.usefixtures("frozen_clock")
def test_unlink_quiet_ignores_missing_file(tmp_path):
    unlink_quiet(str(tmp_path / "absent.parquet"))


@pytest.mark.usefixtures("frozen_clock")
def test_unlink_quiet_swallows_any_oserror(tmp_path):
    with patch("backend.core.rollups._common.os.remove", side_effect=PermissionError("denied")):
        unlink_quiet(str(tmp_path / "locked.parquet"))


# ── From test_rollups_slow_urls.py ──


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the slow_urls writer reads
    and INSERT rows."""
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, url VARCHAR, ottfb DOUBLE, ttfb DOUBLE, cache VARCHAR)")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("url"), r.get("ottfb"), r.get("ttfb"), r.get("cache")],
        )


@pytest.mark.usefixtures("frozen_clock")
def test_build_slow_urls_writes_top_k_per_hour(tmp_path):
    """Happy path: closed hour with rows for several URLs produces one
    row per URL with the documented schema, ranked by p95 DESC."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    hour_token, hour_dt = _past_hour(2)

    # Six rows for URL "/slow" (above min_requests=5) with high latency
    # and six rows for URL "/fast" with low latency. Below min_requests
    # the rollup drops the row, so add five for "/edge".
    rows = []
    for i in range(6):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/slow",
                "ottfb": 500_000.0 + i * 1000,  # ~500ms in us
                "ttfb": 0.5,
                "cache": "MISS",
            }
        )
    for i in range(6):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/fast",
                "ottfb": 20_000.0 + i * 100,  # ~20ms
                "ttfb": 0.02,
                "cache": "MISS",
            }
        )
    # /edge has only 4 rows — should be filtered out by HAVING
    for i in range(4):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/edge",
                "ottfb": 100_000.0,
                "ttfb": 0.1,
                "cache": "MISS",
            }
        )

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", rows)

    with writer_patches(cache_root, "logs_su", con):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "slow_urls.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    cols = set(t.column_names)
    # Required columns — match time_series test's issubset posture so a
    # future column add doesn't break the test.
    assert {"url", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us", "p99_us"}.issubset(cols), (
        f"missing required columns; got: {cols}"
    )

    urls_present = set(t.column("url").to_pylist())
    # /edge had 4 rows — below min_requests=5 — and should be dropped
    assert "/slow" in urls_present
    assert "/fast" in urls_present
    assert "/edge" not in urls_present
    assert len(urls_present) == 2

    # /slow should rank higher by p95
    by_url = {r["url"]: r for r in t.to_pylist()}
    assert by_url["/slow"]["requests"] == 6
    assert by_url["/slow"]["p95_us"] > by_url["/fast"]["p95_us"]


@pytest.mark.usefixtures("frozen_clock")
def test_build_slow_urls_skips_active_hour(tmp_path):
    """The active UTC hour is still being written; the writer must skip it
    (live SQL serves the active hour). Same convention as time_series."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_su"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [active_token])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}" / "slow_urls.parquet").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_build_slow_urls_no_url_column_skips(tmp_path):
    """Services whose schema lacks `url` have no panel input — writer
    must return 0 rather than fail or write an empty file."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    hour_token, _ = _past_hour(2)

    # Table with no url column
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_nourl (timestamp TIMESTAMPTZ, ottfb DOUBLE)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_nourl"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [hour_token])

    assert n == 0


@pytest.mark.usefixtures("frozen_clock")
def test_backfill_slow_urls_walks_existing_bundle_hours(tmp_path):
    """The self-heal driver should pick up closed hours that already have
    all_fields.parquet but no slow_urls.parquet, and build the missing
    bundle for each."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    # Seed two closed hours with all_fields.parquet — only one of them
    # already has slow_urls.parquet, so backfill should hit the other.
    h1_token, h1_dt = _past_hour(3)
    h2_token, h2_dt = _past_hour(4)
    bundled_root = cache_root / "rollups" / "hour_bundled"
    (bundled_root / f"hour={h1_token}").mkdir(parents=True)
    (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")  # marker only
    (bundled_root / f"hour={h2_token}").mkdir(parents=True)
    (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
    (bundled_root / f"hour={h2_token}" / "slow_urls.parquet").write_bytes(b"x")  # already-built

    # Logs covering h1 only — slow_urls should be writeable for h1.
    rows = [
        {"timestamp": h1_dt + timedelta(minutes=i), "url": "/u", "ottfb": 100_000.0, "ttfb": 0.1, "cache": "MISS"}
        for i in range(6)
    ]
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", rows)

    written_hours: list[list[str]] = []

    def _spy_build(_sid, _src, hours):
        written_hours.append(list(hours))
        # Don't actually write — only verify backfill driver picked
        # the right hour.
        return len(hours)

    with patch("backend.core.rollups.slow_urls.build_slow_urls_bundles", side_effect=_spy_build):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = slow_urls.backfill_slow_urls_bundles("svc-su", src)

    assert n == 1
    assert written_hours == [[h1_token]]


@pytest.mark.usefixtures("frozen_clock")
def test_try_slow_urls_from_rollup_eligibility_gates(tmp_path):
    """Reader returns None for: filters present, window too short, missing
    closed-hour rollup file. Confirms the explicit gates documented in
    the docstring."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    # has_filters → None
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-15T00:00:00Z",
                has_filters=True,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Window < 48h → None
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-02T12:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Empty bundle dir → None (the walk finds the first missing
        # closed hour and falls back).
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-15T00:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Malformed timestamps → None
        assert (
            runner.try_slow_urls_from_rollup(
                "not-a-date",
                "2026-06-15T00:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )


@pytest.mark.usefixtures("frozen_clock")
def test_try_slow_urls_from_rollup_returns_request_weighted_p95(tmp_path):
    """End-to-end: build a 50-hour window of slow_urls files (above the
    48-hour MIN_HOURS gate), then verify the reader returns request-
    weighted p95 values and the _approx flag."""

    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    bundled_root = str(cache_root / "rollups" / "hour_bundled")

    # Seed 50 consecutive closed hours starting 100h ago (well above the
    # 48h MIN_HOURS gate and clear of the active hour).
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
        write_hour_bundle(
            hour_dir,
            "slow_urls.parquet",
            "('/a', CAST(100 AS BIGINT), CAST(100 AS BIGINT), 100000.0, 500.0, 1000.0, 1500.0), "
            "('/b', CAST(10 AS BIGINT), CAST(10 AS BIGINT), 100000.0, 5000.0, 10000.0, 15000.0)",
            "url, requests, lat_us_count, lat_us_sum, p50_us, p95_us, p99_us",
        )

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    # Use start = first hour, end = last hour + 1h
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_slow_urls_from_rollup(
            st_iso,
            et_iso,
            has_filters=False,
            min_requests=1,
            limit=10,
        )

    assert result is not None, "reader returned None — eligibility gate fired unexpectedly"
    assert result["_approx"] is True
    assert result["has_data"] is True
    by_url = {r["url"]: r for r in result["rows"]}
    # Each hour contributes identical per-row p95 values, so the
    # request-weighted average across 50 hours == per-hour p95.
    # /a p95: 1000us = 1.0ms; /b p95: 10000us = 10.0ms
    assert abs(by_url["/b"]["p95_ms"] - 10.0) < 1e-6
    assert abs(by_url["/a"]["p95_ms"] - 1.0) < 1e-6
    # /b ranks first by p95
    assert result["rows"][0]["url"] == "/b"
    # Request totals are summed across the 50 hours
    assert by_url["/a"]["requests"] == 100 * 50
    assert by_url["/b"]["requests"] == 10 * 50


# ── From test_rollups_wellknown_bots_writer.py ──


def _seed_con(hour_dt: datetime, rows: list[tuple[str, str]], *, with_ua: bool = True) -> duckdb.DuckDBPyConnection:
    """In-memory ``logs_wk`` table with ``(timestamp, ip[, ua])`` rows inside ``hour_dt``."""
    con = duckdb.connect(":memory:")
    ua_col = ", ua VARCHAR" if with_ua else ""
    con.execute(f"CREATE TABLE logs_wk (timestamp TIMESTAMPTZ, ip VARCHAR{ua_col})")
    for ua, ip in rows:
        ts = hour_dt + timedelta(minutes=3)
        if with_ua:
            con.execute("INSERT INTO logs_wk VALUES (?, ?, ?)", [ts, ip, ua])
        else:
            con.execute("INSERT INTO logs_wk VALUES (?, ?)", [ts, ip])
    return con


def _view_patches(cache_root, con):
    """Patch cache dir, view resolution, the read-only connection and the retry hop."""
    return writer_patches(
        cache_root,
        "logs_wk",
        con,
        safe_table_target="backend.core.rollups.wellknown_bots._safe_table_for",
    )


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_no_pattern_set_version_returns_zero(tmp_path):
    """No source files cached → no version → return 0 without writing."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=""),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_no_bot_pattern_returns_zero(tmp_path):
    """Version present but the regex compiler returned empty → bail
    cleanly (the pattern compiler caches across calls; an empty result
    means the cache is in an indeterminate state)."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, _ = _past_hour(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_empty_hours_input_returns_zero():
    """No hours → fast return without touching anything else."""
    from backend.core.rollups import wellknown_bots

    assert wellknown_bots.recompute_wellknown_bots_rollup("svc", {"name": "svc"}, []) == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_active_hour_filtered_out(tmp_path):
    """Active UTC hour is dropped from the parsed list — its data is
    still in flight (buffer) and the reader serves it live — even when
    the view already holds rows for it."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    con = _seed_con(active_dt, [("Googlebot/2.1", "66.249.66.1")])

    with (
        _view_patches(cache_root, con),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [active])

    assert n == 0
    assert not (cache_root / "rollups" / "wellknown_bots").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_malformed_hour_skipped(tmp_path):
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, ["nope"])

    assert n == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_hour_without_matching_rows_writes_empty_partition(tmp_path):
    """A closed hour with no bot traffic still publishes a zero-row partition:
    it is covered-and-empty, so the reader doesn't treat the hour as a gap."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(hour_dt, [("curl/8.0", "10.0.0.9")])

    with (
        _view_patches(cache_root, con),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc", src, [h])

    assert n == 1
    files = list((cache_root / "rollups" / "wellknown_bots" / f"hour={h}").glob("compacted_*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).num_rows == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_table_missing_ua_or_ip_returns_zero(tmp_path):
    """Service whose schema lacks ua → can't materialize bots, skip
    cleanly without writing partial / wrong-shape files."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ua"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(hour_dt, [("", "66.249.66.1")], with_ua=False)

    with (
        _view_patches(cache_root, con),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-no-ua", src, [h])

    assert n == 0
    # Critical: no parquet was written.
    assert not (cache_root / "rollups" / "wellknown_bots").exists() or not any(
        (cache_root / "rollups" / "wellknown_bots").rglob("compacted_*.parquet")
    )


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_writes_filtered_rows_to_partition(tmp_path):
    """Happy path: a closed hour with bot-UA traffic produces a
    ``rollups/wellknown_bots/hour=H/compacted_*.parquet`` with the expected
    (ua, ip, count, version)."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-write"}
    h, hour_dt = _past_hour(2)
    con = _seed_con(
        hour_dt,
        [("Googlebot/2.1", "66.249.66.1"), ("Googlebot/2.1", "66.249.66.1"), ("curl/8.0", "10.0.0.9")],
    )

    with (
        _view_patches(cache_root, con),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v42"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-write", src, [h])

    assert n == 1
    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    assert hour_dir.exists()
    parquets = list(hour_dir.glob("compacted_*.parquet"))
    assert len(parquets) == 1, f"expected one compacted parquet; got {parquets}"
    assert not list(hour_dir.glob(".tmp_*.parquet"))

    # Only the Googlebot rows survive the regex; the two identical (ua, ip)
    # rows collapse to one row with count=2; version is stamped.
    tbl = pq.read_table(parquets[0]).to_pydict()
    assert tbl["ua"] == ["Googlebot/2.1"]
    assert tbl["ip"] == ["66.249.66.1"]
    assert tbl["request_count"] == [2]
    assert tbl["pattern_set_version"] == ["v42"]


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_sweeps_stale_parquets_in_hour_dir(tmp_path):
    """Before publishing the fresh tmp parquet, the writer must sweep any
    pre-existing parquets in the rollup hour dir — otherwise a reader
    enumerating the dir could see a stale-version row alongside the fresh
    one for the same hour."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-sweep"}
    h, hour_dt = _past_hour(2)

    hour_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h}"
    hour_dir.mkdir(parents=True)
    stale = hour_dir / "compacted_stale.parquet"
    stale.write_bytes(b"stale content")

    con = _seed_con(hour_dt, [("Googlebot/2.1", "66.249.66.1")])
    with (
        _view_patches(cache_root, con),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v_fresh"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-sweep", src, [h])

    assert n == 1
    assert not stale.exists()
    assert len(list(hour_dir.glob("compacted_*.parquet"))) == 1


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_copy_failure_skips_hour_without_raising(tmp_path):
    """A COPY failure for one hour leaves the rest untouched and returns the
    count successfully written."""
    from backend.core.rollups import wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bots-fail"}
    h_ok, hour_ok_dt = _past_hour(2)
    h_bad, _ = _past_hour(3)
    con = _seed_con(hour_ok_dt, [("Googlebot/2.1", "66.249.66.1")])

    calls = {"n": 0}

    def _flaky_retry(c, _src, fn):
        calls["n"] += 1
        if calls["n"] == 2:  # first call is the column DESCRIBE, second is hour 1, third hour 2
            return fn(c)
        if calls["n"] == 3:
            raise duckdb.IOException("simulated COPY failure")
        return fn(c)

    p = build_writer_patches(
        cache_root,
        "logs_wk",
        lambda: con,
        safe_table_target="backend.core.rollups.wellknown_bots._safe_table_for",
    )
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_flaky_retry),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1"),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="bot|Googlebot"),
    ):
        n = wellknown_bots.recompute_wellknown_bots_rollup("svc-bots-fail", src, [h_ok, h_bad])

    assert n == 1
    assert (cache_root / "rollups" / "wellknown_bots" / f"hour={h_ok}").exists()
    bad_dir = cache_root / "rollups" / "wellknown_bots" / f"hour={h_bad}"
    if bad_dir.exists():
        assert not list(bad_dir.glob("compacted_*.parquet"))


# ── Merged from test_commit_crash_recovery.py ──


# ── Rollup atomic publish: a crash mid-rename leaves no trusted half-bundle ──


def _seed_ts_table(con, table, hour_dt):
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    for k in range(5):
        con.execute(f"INSERT INTO {table} VALUES (?, 200, 'HIT', 100, 0.05)", [hour_dt + timedelta(minutes=k)])


@pytest.mark.usefixtures("frozen_clock")
def test_rollup_copy_failure_leaves_no_partial_bundle(tmp_path, monkeypatch):
    """If the COPY that writes the tmp parquet fails, the writer must remove
    the tmp and publish NOTHING — never a half-written ``time_series.parquet``
    that the reader would then trust as a complete hour."""
    from contextlib import contextmanager
    from unittest.mock import patch

    from backend.core.rollups import time_series

    @contextmanager
    def _noop(_k):
        yield

    src = {"name": "atomic-svc", "service_id": "atomic-id", "_cache_dir_override": str(tmp_path)}
    hour_dt = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
    hour = hour_dt.strftime("%Y-%m-%d-%H")
    con = duckdb.connect(":memory:")
    _seed_ts_table(con, "logs", hour_dt)

    class _CopyFailingCon:
        """Proxy whose ``execute`` raises on the COPY (tmp-parquet write) but
        delegates everything else — DuckDB connection objects don't allow
        attribute reassignment, hence the proxy. ``close()`` is a no-op so the
        writer's ``finally: con.close()`` doesn't orphan our table."""

        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **kw):
            if sql.strip().upper().startswith("COPY"):
                raise duckdb.IOException("crash: disk full writing tmp parquet")
            return self._real.execute(sql, *a, **kw)

        def close(self):
            pass

        def __getattr__(self, name):
            return getattr(self._real, name)

    proxy = _CopyFailingCon(con)

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value="logs"),
        patch("backend.core.duckdb.get_connection", return_value=proxy),
        patch("backend.core.iceberg.view._get_service_lock", _noop),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        n = time_series.build_time_series_bundles("atomic-id", src, [hour])

    bundle_dir = tmp_path / "rollups" / "hour_bundled" / f"hour={hour}"
    assert n == 0, "writer reported a publish despite the COPY failing"
    # No published bundle, and no orphan .tmp left behind.
    assert not (bundle_dir / "time_series.parquet").exists(), "half-written bundle published after COPY failure"
    if bundle_dir.exists():
        leftover = [p for p in os.listdir(bundle_dir) if p.startswith(".tmp")]
        assert leftover == [], f"orphan tmp parquet left after COPY failure: {leftover}"


@pytest.mark.usefixtures("frozen_clock")
def test_reader_ignores_stray_tmp_bundle(tmp_path):
    """A crash between tmp-write and os.replace can leave a ``.tmp_ts_*.parquet``
    in the bundle dir. The reader keys on the exact ``time_series.parquet``
    name, so a stray tmp must be invisible — never read as data."""
    from backend.repositories.rollup_coverage import collect_hourly_bundle_paths

    from backend.core.rollups import TIME_SERIES_BUNDLE_FILENAME, _hour_bundled_root

    src = {"name": "stray-svc", "service_id": "stray-id", "_cache_dir_override": str(tmp_path)}
    hour_dt = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
    hour = hour_dt.strftime("%Y-%m-%d-%H")
    bundled_root = _hour_bundled_root(src)
    hour_dir = os.path.join(bundled_root, f"hour={hour}")
    os.makedirs(hour_dir, exist_ok=True)

    # A valid bundle + a stray half-written tmp sibling.
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT TIMESTAMPTZ '{hour_dt.isoformat()}' AS bucket, 5 AS requests, 0 AS status_4xx, "
            f"0 AS status_5xx, 0 AS hits, 0 AS cache_total, 0 AS resp_bytes_sum, 0.0 AS ttfb_sum, 0 AS ttfb_count) "
            f"TO '{os.path.join(hour_dir, TIME_SERIES_BUNDLE_FILENAME)}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    # Garbage tmp file (not even valid parquet) — must be ignored.
    with open(os.path.join(hour_dir, ".tmp_ts_deadbeef.parquet"), "wb") as f:
        f.write(b"not a parquet file")

    result = collect_hourly_bundle_paths(
        src, hour_dt, hour_dt + timedelta(hours=1), bundled_root, TIME_SERIES_BUNDLE_FILENAME
    )
    assert result is not None
    paths, _crosses = result
    assert len(paths) == 1, f"reader picked up a stray/tmp file: {paths}"
    assert paths[0].endswith(TIME_SERIES_BUNDLE_FILENAME)


# ── Merged from test_step7_rollup_coverage.py ──


from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs

from backend.repositories import dashboard, network, security
from backend.repositories._base import _safe_table


@pytest.mark.usefixtures("frozen_clock")
def test_unfiltered_dashboard_queries_hit_rollups_and_skip_raw_scans(in_memory_duckdb, monkeypatch):
    """
    Verify that when unfiltered requests are made to all default dashboard views
    AND rollups exist, NO raw scans (temp tables) are created.
    """
    src = {"name": "test_service", "service_id": "test-service-id"}
    table_name = _safe_table(src["name"])
    logs = generate_mock_logs(src, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dashboard.os.path, "isdir", fake_isdir)

    from backend.repositories._base import QueryRunner

    monkeypatch.setattr(
        QueryRunner, "execute_top_n_rollups", lambda self, cols, *a, **k: ([(c, "val", 10) for c in cols], cols)
    )
    monkeypatch.setattr(
        QueryRunner, "try_time_series_from_rollup", lambda *a, **k: [{"time": "2026-01-01T00:00:00Z", "value": 1.0}]
    )
    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_security_coverage_from_rollup", lambda *a, **k: (100, 10))
    monkeypatch.setattr(QueryRunner, "try_count_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_security_top_ips_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_security_req_size_from_rollup", lambda *a, **k: [(500, 10)])
    monkeypatch.setattr(QueryRunner, "try_security_conn_reuse_from_rollup", lambda *a, **k: [(50, 10)])
    monkeypatch.setattr(
        QueryRunner, "try_verified_bots_ts_from_rollup", lambda *a, **k: [("2026-01-01T00:00:00Z", "Googlebot", 10)]
    )
    monkeypatch.setattr(
        QueryRunner, "try_ngwaf_top_bots_from_rollup", lambda *a, **k: [("BadBot", "1.2.3.4", "AS", 5, 5)]
    )
    monkeypatch.setattr(QueryRunner, "try_network_heatmap_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_network_geo_from_rollup", lambda *a, **k: ([], []))
    monkeypatch.setattr(QueryRunner, "try_network_speed_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_network_rtt_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_origin_latency_ts_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_origin_status_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_origin_path_breakdown_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_origin_pop_latency_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_slow_urls_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_origin_summary_from_rollup", lambda *a, **k: {"has_data": True})
    monkeypatch.setattr(QueryRunner, "try_origin_ip_health_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_perf_latency_from_rollup", lambda *a, **k: [])
    monkeypatch.setattr(QueryRunner, "try_perf_ttl_dist_from_rollup", lambda *a, **k: [])

    monkeypatch.setattr("backend.repositories.security._has_rollup_coverage", lambda *a, **k: True)
    monkeypatch.setattr("backend.repositories.security._ipv6_per_hour_from_rollups", lambda *a, **k: {"count": 10})
    monkeypatch.setattr("backend.repositories.security._proxy_dist_from_rollups", lambda *a, **k: [])
    monkeypatch.setattr("backend.repositories.security._window_eligible_for_rollup", lambda *a, **k: True)
    monkeypatch.setattr("backend.core.rollups.read_wellknown_bots_rollup", lambda *a, **k: [])
    monkeypatch.setattr(
        QueryRunner, "execute_ip_spread_rollups", lambda self, cols, *a, **k: ({(c, "val"): 10 for c in cols}, {})
    )

    res_dash = dashboard.get_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-02T00:00:00Z",
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
    )
    timings_dash = [t["section"] for t in res_dash.get("section_timings", [])]
    assert "temp_table_create" not in timings_dash, f"Dashboard raw scan! {timings_dash}"

    res_sec = security.get_security_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-02T00:00:00Z",
        filters={},
        bucket_seconds=3600,
    )
    timings_sec = [t["section"] for t in res_sec.get("section_timings", [])]
    assert "temp_table_create" in timings_sec, "Security verified_bots_ts requires temp table"

    res_net_health = network.get_health(
        con_factory=lambda: in_memory_duckdb,
        src=src,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-02T00:00:00Z",
        filters={},
        bucket_seconds=3600,
    )
    timings_net_health = [t["section"] for t in res_net_health.get("section_timings", [])]
    assert "temp_table_create" not in timings_net_health, f"Network health raw scan! {timings_net_health}"


# ── From test_time_series_day_repair.py ──

from pathlib import Path
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core.rollups import time_series as _ts_mod
from backend.models.admin import RebuildTimeSeriesRollupsResponse
from backend.routers.admin import compaction as _compaction_api

_REPAIR_DAY = "2020-01-02"
_REPAIR_START = datetime(2020, 1, 2, tzinfo=UTC)


def _stale_day(root: Path, day: str) -> Path:
    path = root / "rollups" / "day_bundled" / f"day={day}" / "time_series.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"bucket": [_REPAIR_START], "requests": [999]}), path)
    return path


def _raw_repair_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE logs_repair (timestamp TIMESTAMPTZ, status INTEGER, "
        "cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    con.executemany(
        "INSERT INTO logs_repair VALUES (?, 200, 'HIT', 100, 0.1)",
        [[_REPAIR_START + timedelta(hours=h, minutes=7)] for h in range(24) if h != 12]
        + [[_REPAIR_START + timedelta(days=1)]],
    )
    return con


@pytest.mark.usefixtures("frozen_clock")
def test_day_repair_recomputes_retired_hours_and_leaves_other_day_untouched(tmp_path):
    stale = _stale_day(tmp_path, _REPAIR_DAY)
    other = _stale_day(tmp_path, "2020-01-01")
    other_bytes = other.read_bytes()
    assert not (tmp_path / "rollups" / "hour_bundled").exists()
    with writer_patches(tmp_path, "logs_repair", _raw_repair_connection()):
        from backend.core.duckdb import get_connection

        rebuilt = day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [_REPAIR_DAY])
        get_connection.assert_called_once()
    assert rebuilt == 1
    rows = pq.ParquetFile(stale).read().to_pylist()
    assert sum(row["requests"] for row in rows) == 23
    assert {row["bucket"].hour for row in rows} == set(range(24)) - {12}
    assert all(_REPAIR_START <= row["bucket"] < _REPAIR_START + timedelta(days=1) for row in rows)
    assert other.read_bytes() == other_bytes
    assert not (tmp_path / "rollups" / "hour_bundled").exists()
    assert not list(stale.parent.glob(".tmp_ts_repair_*"))


@pytest.mark.usefixtures("frozen_clock")
def test_incomplete_hour_recompute_preserves_existing_day(tmp_path, monkeypatch):
    stale = _stale_day(tmp_path, _REPAIR_DAY)
    old_bytes = stale.read_bytes()
    original = _ts_mod.build_time_series_bundles

    def incomplete(sid, source, hours, **kwargs):
        return original(sid, source, hours[:-1], **kwargs)

    monkeypatch.setattr(_ts_mod, "build_time_series_bundles", incomplete)
    with writer_patches(tmp_path, "logs_repair", _raw_repair_connection()):
        with pytest.raises(RuntimeError, match="incomplete"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [_REPAIR_DAY])
    assert stale.read_bytes() == old_bytes
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


@pytest.mark.usefixtures("frozen_clock")
def test_interleaved_compactor_cannot_publish_incomplete_repair(tmp_path, monkeypatch):
    hour = tmp_path / "rollups" / "hour_bundled" / f"hour={_REPAIR_DAY}-00" / "time_series.parquet"
    hour.parent.mkdir(parents=True)
    con = _raw_repair_connection()
    con.execute(
        "COPY (SELECT timestamp AS bucket, 999::BIGINT AS requests, "
        "0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, 0::BIGINT AS hits, "
        "0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, 0.0::DOUBLE AS ttfb_sum, "
        "0::BIGINT AS ttfb_count FROM logs_repair LIMIT 1) TO ? (FORMAT PARQUET)",
        [str(hour)],
    )
    os.utime(hour, (1, 1))
    old_hour = hour.read_bytes()
    stale = _stale_day(tmp_path, _REPAIR_DAY)
    old_day = stale.read_bytes()
    original = _ts_mod.build_time_series_bundles
    observed = []

    def interrupted(sid, source, hours, **kwargs):
        written = original(sid, source, hours[:-1], **kwargs)
        observed.append(day_bundles.compact_time_series_closed_days_to_daily(sid, source))
        assert stale.read_bytes() == old_day
        return written

    monkeypatch.setattr(_ts_mod, "build_time_series_bundles", interrupted)
    with writer_patches(tmp_path, "logs_repair", con):
        with pytest.raises(RuntimeError, match="incomplete"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [_REPAIR_DAY])
    assert observed == [0]
    assert stale.read_bytes() == old_day
    assert hour.read_bytes() == old_hour
    assert not list(stale.parent.glob(".tmp_ts_repair_*"))


@pytest.mark.usefixtures("frozen_clock")
def test_day_copy_failure_preserves_existing_day(tmp_path, monkeypatch):
    stale = _stale_day(tmp_path, _REPAIR_DAY)
    old_bytes = stale.read_bytes()
    monkeypatch.setattr(day_bundles, "_time_series_copy_sql", lambda *_: "SELECT missing_column")
    with writer_patches(tmp_path, "logs_repair", _raw_repair_connection()):
        with pytest.raises(RuntimeError, match="day rebuild failed"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [_REPAIR_DAY])
    assert stale.read_bytes() == old_bytes
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


@pytest.mark.usefixtures("frozen_clock")
def test_delayed_compactor_cannot_overwrite_published_repair(tmp_path, monkeypatch):
    from backend.core.rollups import _common

    hour = tmp_path / "rollups" / "hour_bundled" / f"hour={_REPAIR_DAY}-00" / "time_series.parquet"
    hour.parent.mkdir(parents=True)
    stale = _stale_day(tmp_path, _REPAIR_DAY)
    con = _raw_repair_connection()
    con.execute(
        "COPY (SELECT timestamp AS bucket, 999::BIGINT AS requests, "
        "0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, 0::BIGINT AS hits, "
        "0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, 0.0::DOUBLE AS ttfb_sum, "
        "0::BIGINT AS ttfb_count FROM logs_repair LIMIT 1) TO ? (FORMAT PARQUET)",
        [str(hour)],
    )
    os.utime(stale, (1, 1))
    original_publish = _common.publish_temp_parquet
    repaired = []

    def repair_before_compactor_publish(tmp_file, day_file, lock_key, **kwargs):
        monkeypatch.setattr(_common, "publish_temp_parquet", original_publish)
        assert day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [_REPAIR_DAY]) == 1
        repaired.append(stale.read_bytes())
        return original_publish(tmp_file, day_file, lock_key, **kwargs)

    monkeypatch.setattr(_common, "publish_temp_parquet", repair_before_compactor_publish)
    with writer_patches(tmp_path, "logs_repair", con):
        assert day_bundles.compact_time_series_closed_days_to_daily("repair", {"name": "repair"}) == 0
    assert stale.read_bytes() == repaired[0]
    assert sum(pq.ParquetFile(stale).read()["requests"].to_pylist()) == 23
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


@pytest.fixture
def repair_client(monkeypatch):
    hours = Mock(return_value=1)
    days = Mock(return_value=1)
    monkeypatch.setattr(_compaction_api, "build_time_series_bundles", hours)
    monkeypatch.setattr(day_bundles, "rebuild_time_series_days", days, raising=False)
    app = FastAPI()
    app.add_api_route(
        "/repair",
        _compaction_api.rebuild_time_series_rollups,
        methods=["POST"],
        response_model=RebuildTimeSeriesRollupsResponse,
        response_model_exclude_unset=True,
    )
    app.dependency_overrides[_compaction_api.get_source] = lambda: {"name": "repair", "service_id": "repair"}
    app.dependency_overrides[_compaction_api.require_admin] = lambda: None
    with TestClient(app) as client:
        yield client, hours, days


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.parametrize("actual_days", [0, 1])
def test_endpoint_day_only_reports_actual_day_count(repair_client, actual_days):
    client, hours, days = repair_client
    days.return_value = actual_days
    response = client.post("/repair", json={"days": [_REPAIR_DAY]})
    assert response.status_code == 200
    assert response.json() == {
        "service_id": "repair",
        "requested_hours": [],
        "rebuilt": 0,
        "requested_days": [_REPAIR_DAY],
        "rebuilt_days": actual_days,
    }
    hours.assert_not_called()
    days.assert_called_once_with("repair", {"name": "repair", "service_id": "repair"}, [_REPAIR_DAY])


@pytest.mark.usefixtures("frozen_clock")
def test_endpoint_hour_only_preserves_contract(repair_client):
    client, hours, days = repair_client
    response = client.post("/repair", json={"hours": [f"{_REPAIR_DAY}-04"]})
    assert response.status_code == 200
    assert response.json() == {"service_id": "repair", "requested_hours": [f"{_REPAIR_DAY}-04"], "rebuilt": 1}
    hours.assert_called_once()
    days.assert_not_called()


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"hours": [], "days": []},
        {"days": [_REPAIR_DAY, _REPAIR_DAY]},
        {"days": ["2020-1-02"]},
        {"days": ["2020-02-30"]},
        {"days": [datetime.now(UTC).strftime("%Y-%m-%d")]},
        {"days": [(datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%d")]},
        {"days": [_REPAIR_DAY] * 8},
        {"hours": [f"{_REPAIR_DAY}-04", f"{_REPAIR_DAY}-04"], "days": [_REPAIR_DAY]},
        {"hours": ["bad"], "days": [_REPAIR_DAY]},
    ],
)
def test_endpoint_rejects_invalid_scope_before_writing(repair_client, payload):
    client, hours, days = repair_client
    response = client.post("/repair", json=payload)
    assert response.status_code in (400, 422)
    hours.assert_not_called()
    days.assert_not_called()


@pytest.mark.usefixtures("frozen_clock")
def test_endpoint_incomplete_recompute_fails_explicitly(repair_client):
    client, _, days = repair_client
    days.side_effect = RuntimeError("incomplete hourly recompute: private detail")
    response = client.post("/repair", json={"days": [_REPAIR_DAY]})
    assert response.status_code == 500
    assert response.json()["detail"]["error"] == "time_series_day_rebuild_failed"
    assert "private detail" not in response.text


# ── Merged from tests/core/test_rollups_bundling_and_compaction.py ──


class _FrozenNow(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)


def _query_mem(sql: str) -> list:
    con = duckdb.connect(":memory:")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _write_hour_rollup(buf: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(buf, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _write_hour_bundle(buf: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(buf, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "all_fields.parquet")
    tmp = os.path.join(d, f".tmp_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, tmp)
    os.replace(tmp, p)
    return p


def _setup_backfill_env(monkeypatch, view_sqls: str | list[str], recompute_fn=None):
    sql_list = [view_sqls] if isinstance(view_sqls, str) else view_sqls

    def _fake_update_iceberg_view(con, _src):
        for s in sql_list:
            con.execute(s)

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)
    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)
    captured_hours: list[set[str]] = []
    monkeypatch.setattr(
        "backend.core.rollups.recompute.recompute_touched_hours",
        recompute_fn or (lambda _sid, _src, hours: captured_hours.append(set(hours))),
    )
    return captured_hours


# ── Merged from tests/core/test_rollups_compaction.py ──


def test_compact_writes_per_day_file_summing_hour_counts(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-1"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(
            str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla/5.0", "count": 100}]
        )
        _write_hour_rollup(
            str(cache_root), "ua", "2026-06-04-11", [{"field": "ua", "value": "Mozilla/5.0", "count": 250}]
        )
        _write_hour_rollup(
            str(cache_root), "ua", f"{today}-12", [{"field": "ua", "value": "Mozilla/5.0", "count": 999}]
        )
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-1", src)

    assert rebuilt == 1
    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    assert day_file.exists()
    rows = _query_mem(f"SELECT field, value, count FROM read_parquet('{day_file}') ORDER BY field, value")
    assert rows == [("ua", "Mozilla/5.0", 350)]
    assert not (cache_root / "rollups" / "day" / "field=ua" / f"day={today}" / "compacted.parquet").exists()


def test_compact_uses_in_memory_duckdb_not_per_service_file(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "x", "count": 1}])

    connect_calls: list = []
    real_connect = duckdb.connect
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.object(
            duckdb, "connect", side_effect=lambda *a, **kw: (connect_calls.append((a, kw)), real_connect(*a, **kw))[1]
        ),
    ):
        rollups.compact_closed_days_to_daily("svc-compact-mem", {"name": "svc-compact-mem"})

    assert any(c[0] and c[0][0] == ":memory:" for c in connect_calls)
    assert not any(c[0] and isinstance(c[0][0], str) and c[0][0].endswith(".duckdb") for c in connect_calls)


def test_compact_skips_when_per_day_file_is_already_up_to_date(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-idem"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "y", "count": 5}])
        assert rollups.compact_closed_days_to_daily("svc-compact-idem", src) == 1
        day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
        os.utime(str(day_file), (time.time() + 60, time.time() + 60))
        assert rollups.compact_closed_days_to_daily("svc-compact-idem", src) == 0


def test_compact_falls_back_to_hour_bundled_when_per_field_hours_cleaned_up(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "method", "value": "POST", "count": 1000},
            {"field": "method", "value": "GET", "count": 2000},
            {"field": "country", "value": "US", "count": 3000},
        ],
    )
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-11",
        [{"field": "method", "value": "POST", "count": 1500}, {"field": "method", "value": "GET", "count": 2500}],
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-fallback", {"name": "svc-compact-fallback"})

    assert rebuilt >= 2
    method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
    country_day = cache_root / "rollups" / "day" / "field=country" / "day=2026-06-04" / "compacted.parquet"
    assert method_day.exists() and country_day.exists()
    assert sorted(_query_mem(f"SELECT value, count FROM read_parquet('{method_day}')")) == [
        ("GET", 4500),
        ("POST", 2500),
    ]
    assert _query_mem(f"SELECT value, count FROM read_parquet('{country_day}')") == [("US", 3000)]


def test_compact_unions_per_field_hour_and_bundled_hour_sources(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_rollup(str(cache_root), "method", "2026-06-04-12", [{"field": "method", "value": "POST", "count": 500}])
    _write_hour_bundle(str(cache_root), "2026-06-04-10", [{"field": "method", "value": "POST", "count": 200}])
    _write_hour_bundle(str(cache_root), "2026-06-04-11", [{"field": "method", "value": "POST", "count": 300}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.compact_closed_days_to_daily("svc-compact-mixed", {"name": "svc-compact-mixed"})

    method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
    assert method_day.exists()
    assert _query_mem(f"SELECT value, count FROM read_parquet('{method_day}')") == [("POST", 1000)]


def test_backfill_missing_hour_bundles_detects_gaps_via_view(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-heal", "service_id": "svc-heal"}
    _write_hour_bundle(str(cache_root), "2026-06-04-10", [{"field": "method", "value": "GET", "count": 100}])
    captured_hours = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_heal AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-04 09:30:00+00'), (TIMESTAMP '2026-06-04 09:45:00+00'), "
        "(TIMESTAMP '2026-06-04 10:15:00+00')) AS t(timestamp)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-heal", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1


def test_backfill_missing_hour_bundles_heals_partial_bundle_missing_core_fields(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-partial-heal", "service_id": "svc-partial-heal"}
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "ip", "value": "107.170.164.220", "count": 100},
            {"field": "status", "value": "200", "count": 100},
            {"field": "method", "value": "GET", "count": 100},
            {"field": "pop", "value": "IAD", "count": 100},
        ],
    )
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [{"field": "url", "value": "/only-url", "count": 120}])
    captured_hours = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_partial_heal AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-04 09:30:00+00', '107.170.164.220', 200, 'GET', 'IAD'), "
        "(TIMESTAMP '2026-06-04 10:15:00+00', '107.170.164.220', 200, 'GET', 'IAD')"
        ") AS t(timestamp, ip, status, method, pop)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-partial-heal", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1
    assert (
        os.path.getmtime(cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet") == 0.0
    )


def test_backfill_ignores_rum_child_views_when_finding_request_view(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-rum", "service_id": "svc-rum"}
    captured_hours = _setup_backfill_env(
        monkeypatch,
        [
            "CREATE OR REPLACE VIEW logs_svc_rum__client_errors AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
            "CREATE OR REPLACE VIEW logs_svc_rum AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:45:00+00')) AS t(timestamp)",
        ],
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-rum", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1


def test_backfill_missing_hour_bundles_noop_when_complete(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-noop", "service_id": "svc-noop"}
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [{"field": "method", "value": "GET", "count": 50}])
    dispatched = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_noop AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)
        result2 = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)

    assert dispatched == []
    assert (
        result["missing"] == 0
        and result["rebuilt_fields"] == 0
        and result["bundled"] == 0
        and result["stamped_empty"] == 47
    )
    assert result2 == {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0, "coverage_verified": True}


def test_backfill_rebuilds_empty_sentinel_when_late_rows_arrive(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-late", "service_id": "svc-late"}
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [])
    dispatched: list[set[str]] = []

    def _fake_recompute(_sid, _src, hours):
        dispatched.append(set(hours))
        field_dir = cache_root / "rollups" / "hour" / "field=method" / "hour=2026-06-04-09"
        field_dir.mkdir(parents=True, exist_ok=True)
        source_path = field_dir / "late.parquet"
        pq.write_table(
            pa.table({"field": ["method"], "value": ["GET"], "count": pa.array([7], type=pa.int64())}), source_path
        )
        os.utime(source_path, (source_path.stat().st_mtime + 2, source_path.stat().st_mtime + 2))
        assert rollups.bundle_hours("svc-late", src, list(hours)) == 1

    _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_late AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
        recompute_fn=_fake_recompute,
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-late", src, lookback_days=2)

    assert dispatched == [{"2026-06-04-09"}] and result["missing"] == 1
    assert (
        pq.read_metadata(cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet").num_rows
        == 1
    )


def test_corrupt_hour_bundle_is_not_treated_as_coverage(tmp_path):
    corrupt = tmp_path / "all_fields.parquet"
    corrupt.write_bytes(b"not parquet")
    assert _bundle_metadata(str(corrupt)) is None


def test_compact_returns_zero_when_rollups_dir_missing(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert rollups.compact_closed_days_to_daily("svc-compact-empty", {"name": "svc-compact-empty"}) == 0


def _mock_rollups(monkeypatch):
    for fn in (
        "backfill_day_bundles",
        "compact_network_rtt_closed_days_to_daily",
        "compact_network_speed_closed_days_to_daily",
        "compact_ngwaf_bots_closed_days_to_daily",
        "compact_origin_dims_closed_days_to_daily",
        "compact_origin_latency_ts_closed_days_to_daily",
        "compact_origin_summary_closed_days_to_daily",
        "compact_overview_closed_days_to_daily",
        "compact_perf_latency_closed_days_to_daily",
        "compact_security_dims_closed_days_to_daily",
        "compact_security_proxies_closed_days_to_daily",
        "compact_verified_bots_ts_closed_days_to_daily",
    ):
        monkeypatch.setattr(f"backend.core.rollups.{fn}", lambda *a, **kw: 0)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )


def test_run_rollup_compact_daily_passes_run_id_on_success(monkeypatch):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 4242)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda sid, src, lookback_days=30: 7)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw})
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-ok")
    assert len(log_calls) == 1
    assert log_calls[0]["kwargs"].get("run_id") == 4242 and log_calls[0]["args"][3] == "success"
    assert "Rebuilt 7" in (log_calls[0]["kwargs"].get("summary") or "")


def test_run_rollup_compact_daily_passes_run_id_on_error(monkeypatch):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 9999)
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily",
        lambda sid, src, lookback_days=30: (_ for _ in ()).throw(RuntimeError("simulated DB lock")),
    )
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw})
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-err")
    assert len(log_calls) == 1
    assert log_calls[0]["kwargs"].get("run_id") == 9999 and log_calls[0]["args"][3] == "error"
    assert "simulated DB lock" in (log_calls[0]["kwargs"].get("error_message") or "")


def test_compacted_day_file_has_bigint_count_column(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla", "count": 1}])
        rollups.compact_closed_days_to_daily("svc-compact-bigint", {"name": "svc-compact-bigint"})

    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    schema = _query_mem(f"DESCRIBE SELECT * FROM read_parquet('{day_file}')")
    assert next((row[1] for row in schema if row[0] == "count"), None) == "BIGINT"


def test_mixed_day_and_hour_read_via_union_all_does_not_hit_hive_partition_mismatch(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-mixed-read"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla", "count": 100}])
        rollups.compact_closed_days_to_daily("svc-mixed-read", src)
        _write_hour_rollup(str(cache_root), "ua", f"{today}-12", [{"field": "ua", "value": "Mozilla", "count": 50}])

    day_files = glob.glob(str(cache_root / "rollups" / "day" / "field=ua" / "day=*" / "*.parquet"))
    hour_files = glob.glob(str(cache_root / "rollups" / "hour" / "field=ua" / f"hour={today}-*" / "*.parquet"))
    assert day_files and hour_files

    paths_sql = ", ".join(f"'{p}'" for p in day_files + hour_files)
    with pytest.raises(duckdb.BinderException, match=r"Hive partition mismatch"):
        _query_mem(
            f"SELECT field, value, SUM(count) AS c FROM read_parquet([{paths_sql}], hive_partitioning=1) GROUP BY field, value"
        )

    day_sql = ", ".join(f"'{p}'" for p in day_files)
    hour_sql = ", ".join(f"'{p}'" for p in hour_files)
    rows = _query_mem(
        f"SELECT field, value, SUM(count) AS c FROM ("
        f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{day_sql}], hive_partitioning=1)"
        f"  UNION ALL "
        f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{hour_sql}], hive_partitioning=1)"
        f") GROUP BY field, value"
    )
    assert rows == [("ua", "Mozilla", 150)]


def test_run_rollup_compact_daily_returns_silently_when_start_cron_run_skips(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run", lambda src, task: (_ for _ in ()).throw(CronLeaseHeldError("busy"))
    )
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily", lambda sid, src: pytest.fail("must not run")
    )
    log_calls: list = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))

    sch._run_rollup_compact_daily("svc-run-busy")
    assert log_calls == []


# ── Merged from tests/core/test_rollups_day_bundles.py ──


def _write_per_field_day(cache_root: str, field: str, day: str, rows: list[tuple[str, int]]) -> str:
    d = os.path.join(cache_root, "rollups", "day", f"field={field}", f"day={day}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([field] * len(rows)),
            "value": pa.array([r[0] for r in rows]),
            "count": pa.array([r[1] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, p)
    return p


def _past_day(days_ago: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def test_bundle_days_writes_combined_parquet(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 100), ("/b", 50)])
    _write_per_field_day(str(cache_root), "country", day, [("US", 80), ("JP", 20)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day", {"name": "svc-day"}, [day]) == 1

    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert {("url", "/a", 100), ("url", "/b", 50), ("country", "US", 80), ("country", "JP", 20)} <= rows


def test_bundle_days_top_k_truncation_with_other_synthetic_row(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [(f"v{i}", i + 1) for i in range(DAY_BUNDLE_TOP_K + 50)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-topk", {"name": "svc-topk"}, [day]) == 1

    records = pq.read_table(
        str(cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet")
    ).to_pylist()
    other = [r for r in records if r["value"] == "__other__"]
    assert len(other) == 1 and other[0]["count"] == sum(range(1, 51))
    assert len([r for r in records if r["value"] != "__other__"]) == DAY_BUNDLE_TOP_K


def test_bundle_days_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-preserve"}
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "ip", day, [("107.170.164.220", 500)])
    _write_per_field_day(str(cache_root), "url", day, [("/old", 100)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/new", 250)])
        os.utime(new_p, (time.time() + 60, time.time() + 60))
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 500) in rows and ("url", "/new", 250) in rows and ("url", "/old", 100) not in rows


def test_bundle_days_skips_active_day(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d")
    _write_per_field_day(str(cache_root), "url", active, [("/x", 1)])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [active]) == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={active}" / "all_fields.parquet").exists()


def test_bundle_days_skips_when_bundle_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_rebuilds_when_source_newer_than_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/b", 5)])
        os.utime(new_p, (time.time() + 60, time.time() + 60))
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1


def test_bundle_days_no_per_field_files_skipped(tmp_path):
    cache_root = tmp_path / "cache"
    (cache_root / "rollups" / "day").mkdir(parents=True)
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_no_day_root_returns_zero(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "cache_missing")):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_empty_input_returns_zero():
    assert day_bundles.bundle_days("svc", {"name": "svc"}, []) == 0


def test_bundle_days_malformed_day_token_skipped(tmp_path):
    cache_root = tmp_path / "cache"
    (cache_root / "rollups" / "day").mkdir(parents=True)
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, ["not-a-day"]) == 0


def test_backfill_day_bundles_discovers_and_caps(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    days = [_past_day(i) for i in range(2, 6)]
    for d in days:
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
    seeded_dir = cache_root / "rollups" / "day_bundled" / f"day={days[0]}"
    seeded_dir.mkdir(parents=True)
    (seeded_dir / "all_fields.parquet").write_bytes(b"present")

    seen: list[list[str]] = []
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(
            "backend.core.rollups.day_bundles.bundle_days",
            lambda _sid, _src, days_in: (seen.append(list(days_in)), len(days_in))[1],
        ),
    ):
        rebuilt = day_bundles.backfill_day_bundles("svc-day-bf", {"name": "svc-day-bf"}, max_days=2)

    assert rebuilt == 2 and len(seen[0]) == 2 and days[0] not in seen[0]


def test_backfill_day_bundles_no_root_returns_zero(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_backfill_day_bundles_all_present_returns_zero(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    for i in range(2, 4):
        d = _past_day(i)
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
        bd = cache_root / "rollups" / "day_bundled" / f"day={d}"
        bd.mkdir(parents=True)
        (bd / "all_fields.parquet").write_bytes(b"x")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.day_bundles.bundle_days", side_effect=AssertionError("must not be invoked")),
    ):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_link_day_bundle_alias_hardlinks_and_leaves_no_tmp(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    _link_day_bundle_alias(str(src), str(dst))
    assert os.path.samefile(src, dst) and dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_never_removes_existing_dst_on_failure(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=OSError("no copy")),
    ):
        _link_day_bundle_alias(str(src), str(dst))
    assert dst.read_bytes() == b"old" and sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_copy_fallback_is_atomic_replace(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    seen_during_copy: list[bool] = []
    real_copy = shutil.copyfile
    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch(
            "backend.core.rollups.day_bundles.shutil.copyfile",
            side_effect=lambda s, dt: (seen_during_copy.append(dst.exists()), real_copy(s, dt))[1],
        ),
    ):
        _link_day_bundle_alias(str(src), str(dst))
    assert seen_during_copy == [True] and dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


# ── Merged from tests/core/test_rollups_hour_bundling.py ──


def _write_per_field_hour(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _write_per_field_hour_ip_spread(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
            "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
            "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def test_bundle_hours_writes_one_parquet_per_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(
            str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}, {"value": "/api", "count": 75}]
        )
        _write_per_field_hour(
            str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}, {"value": "JP", "count": 20}]
        )
        assert rollups.bundle_hours("svc-bundle-1", {"name": "svc-bundle-1"}, ["2026-05-15-10"]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {"field", "value", "count"}.issubset(set(t.column_names))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert {("url", "/login", 100), ("url", "/api", 75), ("country", "US", 80), ("country", "JP", 20)} <= rows


def test_bundle_hours_skips_active_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", active, [{"value": "/x", "count": 1}])
        assert rollups.bundle_hours("svc-bundle-active", {"name": "svc-bundle-active"}, [active]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active}" / "all_fields.parquet").exists()


def test_bundle_hours_skips_when_bundle_is_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-skip"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        assert rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
        mtime_first = os.path.getmtime(bundle)
        assert rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"]) == 0
        assert os.path.getmtime(bundle) == mtime_first


def test_bundle_hours_rebuilds_when_source_files_newer(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-stale"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])
        new_p = _write_per_field_hour(
            str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}, {"value": "/y", "count": 2}]
        )
        os.utime(new_p, (time.time() + 10, time.time() + 10))
        assert rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"))
    assert {"/x", "/y"} <= set(t["value"].to_pylist())


def test_bundle_hours_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-preserve"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "ip", "2026-05-15-10", [{"value": "107.170.164.220", "count": 120}])
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/a", "count": 10}])
        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1
        new_p = _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/b", "count": 25}])
        os.utime(new_p, (time.time() + 10, time.time() + 10))
        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 120) in rows and ("url", "/b", 25) in rows and ("url", "/a", 10) not in rows


def _exec_top_n(cache_root, src, fields, st, et):
    con = duckdb.connect(":memory:")
    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            rows, _ = QueryRunner(con, src).execute_top_n_rollups(fields, st, et, limit=10)
            return rows
    finally:
        con.close()


def test_reader_uses_bundle_when_available_skipping_per_field_files(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-prefer-bundle", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 50}])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-prefer-bundle", src, ["2026-05-15-10"])
    rows = _exec_top_n(cache_root, src, ["url", "country"], "2026-05-15T10:00:00", "2026-05-15T11:00:00")
    by_field: dict[str, list[tuple]] = {}
    for f, v, c in rows:
        by_field.setdefault(f, []).append((v, c))
    assert by_field.get("url") == [("/login", 100)] and by_field.get("country") == [("US", 50)]


def test_reader_falls_back_to_per_field_when_bundle_missing(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-fallback", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    assert _exec_top_n(cache_root, src, ["url"], "2026-05-15T10:00:00", "2026-05-15T11:00:00") == [
        ("url", "/login", 100)
    ]


def test_reader_mixed_bundled_and_per_field_hours(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-mixed", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-11", [{"value": "/login", "count": 50}])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-mixed", src, ["2026-05-15-10"])
    assert _exec_top_n(cache_root, src, ["url"], "2026-05-15T10:00:00", "2026-05-15T12:00:00") == [
        ("url", "/login", 150)
    ]


def test_bundle_hours_cleans_up_per_field_files_after_publish(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
        _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}])
        assert rollups.bundle_hours("svc-cleanup", {"name": "svc-cleanup"}, ["2026-05-15-10"]) == 1

    assert (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet").exists()
    for f in ("url", "country"):
        assert not (cache_root / "rollups" / "hour" / f"field={f}" / "hour=2026-05-15-10").exists()


def test_bundle_hours_cleanup_dry_run_logs_but_does_not_unlink(tmp_path, caplog):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.dict(os.environ, {"ROLLUP_CLEANUP_DRY_RUN": "1"}),
        caplog.at_level(logging.INFO, logger="backend.core.rollups"),
    ):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-dry-run", {"name": "svc-dry-run"}, ["2026-05-15-10"])

    assert (cache_root / "rollups" / "hour" / "field=url" / "hour=2026-05-15-10").exists()
    assert any("ROLLUP_CLEANUP_DRY_RUN" in r.message for r in caplog.records)


def test_bundle_hours_ip_spread_writes_one_parquet_per_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hll_us, hll_jp, hll_ja3 = HyperLogLog(), HyperLogLog(), HyperLogLog()
    hll_us.update([f"1.1.1.{i}" for i in range(50)])
    hll_jp.update([f"2.2.2.{i}" for i in range(30)])
    hll_ja3.update([f"3.3.3.{i}" for i in range(10)])
    sketch_us, sketch_jp, sketch_ja3 = hll_us.to_bytes(), hll_jp.to_bytes(), hll_ja3.to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {"value": "US", "ip_sketch": sketch_us, "ip_count_observed": 50, "sample_capped": False},
                {"value": "JP", "ip_sketch": sketch_jp, "ip_count_observed": 30, "sample_capped": False},
            ],
        )
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "ja3",
            "2026-05-15-10",
            [{"value": "ja3-abc", "ip_sketch": sketch_ja3, "ip_count_observed": 10, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-bundle", {"name": "svc-ipsp-bundle"}, ["2026-05-15-10"]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields_ip.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {"field", "value", "ip_sketch", "ip_count_observed", "sample_capped"}.issubset(set(t.column_names))
    by_key = {(r["field"], r["value"]): r for r in t.to_pylist()}
    assert set(by_key) == {("country", "US"), ("country", "JP"), ("ja3", "ja3-abc")}
    assert by_key[("country", "US")]["ip_sketch"] == sketch_us and by_key[("country", "JP")]["ip_sketch"] == sketch_jp
    assert by_key[("ja3", "ja3-abc")]["ip_sketch"] == sketch_ja3
    assert by_key[("country", "US")]["ip_count_observed"] == 50 and by_key[("country", "US")]["sample_capped"] is False
    assert abs(HyperLogLog.from_bytes(by_key[("country", "US")]["ip_sketch"]).count() - 50) <= 10


def test_bundle_hours_ip_spread_skips_active_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            active_hour,
            [{"value": "US", "ip_sketch": HyperLogLog().to_bytes(), "ip_count_observed": 0, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-active", {"name": "svc-ipsp-active"}, [active_hour]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_hour}" / "all_fields_ip.parquet").exists()


def test_bundle_hours_ip_spread_skips_when_bundle_is_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour = "2026-05-15-10"
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            hour,
            [{"value": "US", "ip_sketch": HyperLogLog().to_bytes(), "ip_count_observed": 0, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-fresh", {"name": "svc-ipsp-fresh"}, [hour]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour}" / "all_fields_ip.parquet"
        time.sleep(0.05)
        os.utime(bundle, None)
        assert bundle_hours_ip_spread("svc-ipsp-fresh", {"name": "svc-ipsp-fresh"}, [hour]) == 0


def test_bundle_hours_ip_spread_returns_zero_when_no_per_field_files(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert bundle_hours_ip_spread("svc-ipsp-empty", {"name": "svc-ipsp-empty"}, ["2026-05-15-10"]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10").exists()


def test_bundle_hours_ip_spread_returns_zero_when_root_missing(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert bundle_hours_ip_spread("svc-ipsp-cold", {"name": "svc-ipsp-cold"}, ["2026-05-15-10"]) == 0


# ── Merged from tests/core/test_rollups_partial_hour.py ──


def _write_hourly_parquet(path, rows, schema="timestamp TIMESTAMP, country VARCHAR"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(f"CREATE TABLE t ({schema})")
        placeholders = ", ".join(["?"] * len(rows[0]))
        con.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


class TestRollupsPartialHour:
    @pytest.fixture(autouse=True)
    def _patch_cache_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path / src["name"]))
        yield

    def _setup_active_hour(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(str(tmp_path / "svc-a"), "data", f"timestamp_hour={active_hour}")
        return src, active_hour, hourly_dir, datetime.now(UTC)

    def test_watermark_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_watermark(src, "2026-09-09-14") == 0.0

    def test_all_fields_defaults_to_empty(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_all_fields(src, "2026-09-09-14") == []

    def test_total_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_total(src, "2026-09-09-14") == 0

    def test_merge_partial_hour_writes_a_total_row_not_per_field_summable(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US", "GET"), (now, "US", "POST"), (now, "CA", "GET")],
            schema="timestamp TIMESTAMP, country VARCHAR, method VARCHAR",
        )
        assert ph.merge_partial_hour("svc-a", src, ["country", "method"])["new_files"] == 1
        per_field_rows = ph.read_partial_hour_all_fields(src, active_hour)
        assert sum(c for _, _, c in per_field_rows) == 6
        assert ph.read_partial_hour_total(src, active_hour) == 3
        assert all(field != ph.TOTAL_FIELD for field, _, _ in per_field_rows)

    def test_merge_partial_hour_excludes_a_custom_field_named_total_field(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US", "x"), (now, "CA", "y")],
            schema='timestamp TIMESTAMP, country VARCHAR, "__total__" VARCHAR',
        )
        assert ph.merge_partial_hour("svc-a", src, ["country", ph.TOTAL_FIELD])["new_files"] == 1
        assert ph.read_partial_hour_total(src, active_hour) == 2
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 1, "CA": 1}

    def test_read_partial_hour_functions_reuse_caller_connection(self, tmp_path, monkeypatch):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])

        real_connect = duckdb.connect
        connect_calls = []
        caller_con = duckdb.connect(":memory:")
        monkeypatch.setattr(
            duckdb, "connect", lambda *a, **kw: (connect_calls.append((a, kw)), real_connect(*a, **kw))[1]
        )
        try:
            rows = ph.read_partial_hour_all_fields(src, active_hour, con=caller_con)
            total = ph.read_partial_hour_total(src, active_hour, con=caller_con)
        finally:
            caller_con.close()

        assert dict((v, c) for _, v, c in rows) == {"US": 1, "CA": 1} and total == 2 and connect_calls == []

    def test_merge_partial_hour_total_accumulates_across_ticks(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 2

        time.sleep(0.05)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

    def test_merge_partial_hour_is_incremental_and_idempotent(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "US"), (now, "CA")])

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 2, "CA": 1}

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 0
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 2, "CA": 1}

        time.sleep(0.05)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US"), (now, "DE")])
        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {
            "US": 3,
            "CA": 1,
            "DE": 1,
        }

    def test_merge_partial_hour_excludes_rows_before_hour_start(self, tmp_path):
        src, active_hour, hourly_dir, inside_hour = self._setup_active_hour(tmp_path)
        just_before_hour = datetime.strptime(active_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC) - timedelta(seconds=5)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "straddling.parquet"), [(just_before_hour, "PREV_HOUR_ONLY"), (inside_hour, "US")]
        )

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 1}

    def test_gc_stale_partial_hours_removes_old_but_not_active(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        os.makedirs(ph.partial_hour_dir(src, active_hour))
        os.makedirs(ph.partial_hour_dir(src, stale_hour))

        assert ph.gc_stale_partial_hours(src, max_age_hours=2) == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))

    def test_gc_stale_partial_hours_ignores_malformed_hour_token(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        malformed_dir = os.path.join(ph.partial_hour_root(src), "hour=garbage")
        for d in (ph.partial_hour_dir(src, active_hour), ph.partial_hour_dir(src, stale_hour), malformed_dir):
            os.makedirs(d)

        assert ph.gc_stale_partial_hours(src, max_age_hours=2) == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour)) and os.path.isdir(malformed_dir)
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))
