from __future__ import annotations

import os
import re
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from backend.core import rollup_readiness as rr
from backend.core import rollups
from backend.core.rollups import day_bundles, rollups_present
from backend.core.rollups._common import parquet_zstd_target, unlink_quiet, wavg
from backend.core.rollups.spec import ADMIN_BACKFILLS, DAY_COMPACTORS
from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.rollups import build_writer_patches, write_hour_bundle, writer_patches

# ── From test_rollup_readiness.py ──


def test_defaults_to_not_ready():
    rr.reset_rollup_coverage_ready()
    assert rr.rollup_coverage_ready("svc-a") is False


def test_mark_ready_flips_only_that_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is True
    assert rr.rollup_coverage_ready("svc-b") is False


def test_reset_single_service():
    rr.reset_rollup_coverage_ready()
    rr.mark_rollup_coverage_ready("svc-a")
    rr.mark_rollup_coverage_ready("svc-b")
    rr.reset_rollup_coverage_ready("svc-a")
    assert rr.rollup_coverage_ready("svc-a") is False
    assert rr.rollup_coverage_ready("svc-b") is True


# ── From test_rollup_spec_registries.py ──


def test_day_compactors_resolve_to_callables_with_unique_labels() -> None:
    assert len({s.label for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert len({s.compactor for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert all(callable(getattr(rollups, s.compactor)) for s in DAY_COMPACTORS)


def test_every_per_rollup_day_compactor_is_registered() -> None:
    shipped = {
        name
        for name in vars(day_bundles)
        if re.fullmatch(r"compact_\w+_closed_days_to_daily", name) and name != "compact_closed_days_to_daily"
    }
    assert shipped == {s.compactor for s in DAY_COMPACTORS}


def test_admin_backfills_resolve_to_callables_with_unique_keys() -> None:
    assert len({k for k, _ in ADMIN_BACKFILLS}) == len(ADMIN_BACKFILLS)
    assert all(callable(getattr(rollups, fn)) for _, fn in ADMIN_BACKFILLS)


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


def test_absent_rollups_tree_is_not_present(tmp_path):
    assert rollups_present(_src(tmp_path)) is False


def test_empty_rollups_root_is_not_present(tmp_path):
    os.makedirs(os.path.join(tmp_path, "rollups"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False


def test_day_bundled_only_is_present(tmp_path):
    # The regression shape: per-field hour tier compacted away, day bundles remain.
    _touch_parquet(os.path.join(tmp_path, "rollups", "day_bundled"), "day=2026-10-01")
    assert not os.path.isdir(os.path.join(tmp_path, "rollups", "hour"))
    assert rollups_present(_src(tmp_path)) is True


def test_hour_bundled_only_is_present(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour_bundled"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


def test_per_field_hour_tier_still_counts(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour", "field=ip"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


def test_empty_partition_dirs_without_parquet_are_not_present(tmp_path):
    # Defensive: a partition dir left behind with no parquet must not count.
    os.makedirs(os.path.join(tmp_path, "rollups", "day_bundled", "day=2026-10-01"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False


# ── From test_rollups_recompute_dispatch.py ──

pytestmark = pytest.mark.usefixtures("frozen_clock")


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


def test_registry_matches_expected_order_and_resolves_on_recompute():
    from backend.core.rollups import recompute
    from backend.core.rollups.spec import HOUR_WRITERS

    assert tuple(s.builder for s in HOUR_WRITERS) == EXPECTED_ORDER
    assert len({s.label for s in HOUR_WRITERS}) == len(HOUR_WRITERS)
    assert all(callable(getattr(recompute, s.builder)) for s in HOUR_WRITERS)


# ── From test_rollups_sql_helpers.py ──


def test_wavg_renders_weighted_average_sql():
    assert wavg("p95_us", "requests") == "SUM(p95_us * requests) / NULLIF(SUM(requests), 0)"


def test_parquet_zstd_target_renders_copy_tail():
    assert parquet_zstd_target("/tmp/x.parquet") == "TO '/tmp/x.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"


def test_unlink_quiet_removes_existing_file(tmp_path):
    f = tmp_path / "a.parquet"
    f.write_bytes(b"x")
    unlink_quiet(str(f))
    assert not f.exists()


def test_unlink_quiet_ignores_missing_file(tmp_path):
    unlink_quiet(str(tmp_path / "absent.parquet"))


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


def test_recompute_empty_hours_input_returns_zero():
    """No hours → fast return without touching anything else."""
    from backend.core.rollups import wellknown_bots

    assert wellknown_bots.recompute_wellknown_bots_rollup("svc", {"name": "svc"}, []) == 0


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
