"""Phase 4 /dashboard performance & security audit golden and regression tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from backend.core import duckdb as db_core
from backend.core.metadata import usage_log
from backend.core.rollups._common import (
    OVERVIEW_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
)
from backend.core.share_db.validation import apply_pii_policy
from backend.high_scale import dashboard as high_scale_dashboard
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import AggregatesRequest
from backend.repositories import _base as repo_base
from backend.repositories import rollup_coverage
from backend.utils.telemetry import set_page_load_id


def _write_time_series_hour(con: duckdb.DuckDBPyConnection, hour_root: str, hour_str: str, reqs: int = 10) -> None:
    ts_dir = Path(hour_root) / f"hour={hour_str}"
    ts_dir.mkdir(parents=True, exist_ok=True)
    out = ts_dir / TIME_SERIES_BUNDLE_FILENAME
    bucket = f"{hour_str[:10]} {hour_str[11:13]}:00:00Z"
    con.execute(
        f"""
        COPY (
            SELECT
                TIMESTAMPTZ '{bucket}' AS bucket,
                CAST({reqs} AS BIGINT) AS requests,
                CAST(0 AS BIGINT) AS status_4xx,
                CAST(0 AS BIGINT) AS status_5xx,
                CAST(5 AS BIGINT) AS hits,
                CAST(8 AS BIGINT) AS cache_total,
                CAST(1000 AS BIGINT) AS resp_bytes_sum,
                CAST(2.0 AS DOUBLE) AS ttfb_sum,
                CAST(10 AS BIGINT) AS ttfb_count
        ) TO '{out}' (FORMAT PARQUET)
        """
    )


def _write_day_overview(con: duckdb.DuckDBPyConnection, day_root: str, day_str: str, reqs_per_hour: int = 10) -> None:
    day_dir = Path(day_root) / f"day={day_str}"
    day_dir.mkdir(parents=True, exist_ok=True)
    out = day_dir / OVERVIEW_BUNDLE_FILENAME
    rows = ", ".join(f"('{day_str}T{h:02d}', CAST({reqs_per_hour} AS BIGINT))" for h in range(24))
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES {rows}) AS t(hour_start, requests)
        ) TO '{out}' (FORMAT PARQUET)
        """
    )


def test_compact_time_series_closed_days_and_30d_rollup_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """30d time_series and count read from day_bundled (time_series.parquet or overview.parquet)
    + recent hour_bundled without disqualifying when old hourly time_series files were compacted."""
    from backend.core.rollups.day_bundles import compact_time_series_closed_days_to_daily

    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    con = duckdb.connect(":memory:")
    src = {"name": "svc_30d", "service_id": "svc_30d"}
    con.execute(
        "CREATE TABLE logs_svc_30d (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root = _hour_bundled_root(src)
    day_root = _day_bundled_root(src)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    st = (now - timedelta(days=29)).replace(hour=0)
    et = now

    # Seed 28 closed days via day_bundled/overview.parquet (simulating pre-existing daily bundles)
    # and 1 closed day via hourly time_series that we compact into day_bundled/time_series.parquet,
    # plus today's hours via hourly time_series.
    cur_day = st.date()
    yesterday = (now - timedelta(days=1)).date()
    while cur_day < yesterday:
        _write_day_overview(con, day_root, cur_day.isoformat(), reqs_per_hour=10)
        cur_day += timedelta(days=1)

    for h in range(24):
        _write_time_series_hour(con, hour_root, f"{yesterday.isoformat()}-{h:02d}", reqs=20)

    compacted = compact_time_series_closed_days_to_daily("svc_30d", src, lookback_days=5)
    assert compacted == 1
    assert (Path(day_root) / f"day={yesterday.isoformat()}" / TIME_SERIES_BUNDLE_FILENAME).is_file()

    # Write today's closed hours up to `now`
    today_str = now.date().isoformat()
    for h in range(now.hour):
        _write_time_series_hour(con, hour_root, f"{today_str}-{h:02d}", reqs=30)

    rollup_coverage.clear_listdir_cache()
    runner = repo_base.QueryRunner(con, src)

    total = runner.try_count_from_rollup(
        start_time=st.isoformat(),
        end_time=et.isoformat(),
        table_name="logs_svc_30d",
        where_clause="1=1",
        params=[],
        unfiltered_window=True,
    )
    assert total is not None
    assert total > 0

    ts_rows = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=st.isoformat(),
        end_time=et.isoformat(),
        table_name="logs_svc_30d",
        where_clause="1=1",
        params=[],
        unfiltered_window=True,
    )
    assert ts_rows is not None
    assert len(ts_rows) > 24


def test_clickhouse_aggregates_batch_and_30d_rollup_usage() -> None:
    """High-scale aggregates uses batched request_aggregates queries and sets used_rollups=True."""
    executed_queries: list[str] = []

    class FakeBatchClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            executed_queries.append(sql)
            p = params or {}
            dims = [v for k, v in sorted(p.items()) if k.startswith("dim_")]
            if "AS chart_bucket" in sql:
                return [{"chart_bucket": "2026-10-01 00:00:00", "value": 1000}]
            if "AS bucket_start" in sql or "SELECT bucket_start" in sql:
                return [{"bucket_start": "2026-10-01 00:00:00", "value": 1000}]
            if "GROUP BY dimension" in sql and "GROUP BY dimension, value" not in sql:
                return [{"dimension": d, "total_count": 2} for d in dims]
            if "LIMIT 10 BY dimension" in sql:
                return [
                    {
                        "dimension": d,
                        "value": f"{d}_v1",
                        "aggregate_count": 100,
                    }
                    for d in dims
                ]
            return [{"value": "US", "aggregate_count": 100, "total_count": 100}]

    service = HighScaleService(
        service_id="svc1",
        client=FakeBatchClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc1",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 20, tzinfo=UTC),
            coverage_end=datetime(2026, 10, 20, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    res = high_scale_dashboard.aggregates(
        service,
        AggregatesRequest(
            fields=["url", "status", "country"],
            chart_interval="1 hour",
            chart_metric="requests",
        ),
        "2026-09-20T00:00:00Z",
        "2026-10-20T00:00:00Z",
    )
    assert res.data["url"].total == 2
    assert res.data["url"].top[0].value == "url_v1"
    # Batched: time_series + (batch top-N + batch totals) + map_data <= 6 queries instead of 3*2+3 = 9 (or 68 for 34 dims)!
    assert len(executed_queries) <= 6


def test_pii_policy_masks_schema_min_max_ips() -> None:
    """apply_pii_policy masks IPv4/IPv6 addresses in schema `min` and `max` fields when mask_ips=True."""
    payload = {
        "schema": [
            {"column": "host", "type": "VARCHAR", "min": "example.com", "max": "z.example.com"},
            {"column": "client_ip", "type": "VARCHAR", "min": "100.1.232.227", "max": "199.232.65.91"},
        ],
        "services": [
            {
                "service_id": "svc1",
                "status": {
                    "schema": [
                        {"column": "client_ip", "type": "VARCHAR", "min": "99.95.121.49", "max": "199.232.65.91"}
                    ]
                },
            }
        ],
    }
    masked = apply_pii_policy(payload, {"mask_ips": True})
    assert masked["schema"][1]["min"] == "100.1.232.xxx"
    assert masked["schema"][1]["max"] == "199.232.65.xxx"
    assert masked["services"][0]["status"]["schema"][0]["min"] == "99.95.121.xxx"
    assert masked["services"][0]["status"]["schema"][0]["max"] == "199.232.65.xxx"
    assert masked["schema"][0]["min"] == "example.com"


def test_page_telemetry_records_query_runner_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    """QueryRunner.execute logs queries into usage_log.log_telemetry_query when X-Page-Load-ID is active."""
    recorded: list[dict] = []
    monkeypatch.setattr(
        usage_log,
        "log_telemetry_query",
        lambda **kwargs: recorded.append(kwargs),
    )
    con = duckdb.connect(":memory:")
    set_page_load_id("test-page-load-01")
    try:
        runner = repo_base.QueryRunner(con, {"name": "svc", "service_id": "svc"})
        runner.execute("SELECT 1 AS x")
    finally:
        set_page_load_id(None)

    assert len(recorded) == 1
    assert recorded[0]["engine"] == "duckdb"
    assert "SELECT 1 AS x" in recorded[0]["sql"]
    assert recorded[0]["page_load_id"] == "test-page-load-01"


def test_rolling_window_uses_daily_time_series_bundle_when_day0_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rolling windows (where Day 0 starts mid-day with < 24 hours) still read day_bundled time_series.parquet."""
    from backend.core.rollups.day_bundles import compact_time_series_closed_days_to_daily

    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    service = "svc_rolling_ts"
    src = {"name": service, "service_id": service}
    con = duckdb.connect(":memory:")
    con.execute(
        f"CREATE TABLE logs_{service} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root = _hour_bundled_root(src)

    now = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [(now - timedelta(days=i)).date().isoformat() for i in range(7, 0, -1)]
    for d in days:
        for h in range(24):
            _write_time_series_hour(con, hour_root, f"{d}-{h:02d}", reqs=5)
    compacted = compact_time_series_closed_days_to_daily(service, src, lookback_days=10)
    assert compacted == 7

    rollup_coverage.clear_listdir_cache()
    # Rolling 6.5-day window starting mid-day on days[0] (12:00Z -> 12 hours on day 0 + 6*24 hours on days 1..6 = 156 hours)
    runner = repo_base.QueryRunner(con, src)
    ts = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=f"{days[0]}T12:00:00Z",
        end_time=f"{now.date().isoformat()}T00:00:00Z",
        table_name=f"logs_{service}",
        where_clause="1=1",
        params=[],
        unfiltered_window=True,
    )
    assert ts is not None
    assert sum(int(pt["value"]) for pt in ts) == (12 + 6 * 24) * 5
    queries = [q["sql"] for q in runner.telemetry()["debug_queries"]]
    assert any("read_parquet([7 files])" in q for q in queries), f"Expected 7 daily files in queries: {queries}"


def test_invalidate_service_clears_aggregates_and_top_bots_cache() -> None:
    """dashboard.invalidate_service drops both aggregates and top_bots cache entries for that service."""
    from backend.repositories import dashboard as dashboard_repo

    dashboard_repo._dashboard_cache["abc123:svc_test"] = (1.0, {"total_rows": 10})
    dashboard_repo._dashboard_cache["top_bots:def456:svc_test"] = (1.0, {"bots": [{"name": "Bot"}]})
    dashboard_repo._dashboard_cache["other:svc_keep"] = (1.0, {"total_rows": 5})

    dashboard_repo.invalidate_service("svc_test")

    assert "abc123:svc_test" not in dashboard_repo._dashboard_cache
    assert "top_bots:def456:svc_test" not in dashboard_repo._dashboard_cache
    assert "other:svc_keep" in dashboard_repo._dashboard_cache
    dashboard_repo.invalidate_service("svc_keep")


def test_enrich_bot_metadata_binary_search_and_pair_dedup_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """enrich_bot_metadata uses binary-searched CIDR intervals and deduplicates (ua, ip) pairs."""
    import ipaddress
    import time

    import pandas as pd

    from backend.utils import bot_sources, rdns_cache

    fake_feeds = {
        "openai-gptbot": [ipaddress.ip_network(f"198.51.{i}.0/24") for i in range(100)],
        "anthropic-ai": [ipaddress.ip_network("203.0.113.0/24"), ipaddress.ip_network("2001:db8::/32")],
    }
    monkeypatch.setattr(bot_sources, "_get_compiled_ip_feeds", lambda: fake_feeds)
    monkeypatch.setattr(bot_sources, "_compiled_ip_feeds", {})
    monkeypatch.setattr(rdns_cache, "get_hostnames", lambda ips: {})

    rows = [
        {"ua": "Mozilla/5.0 (compatible; GPTBot/1.0)", "ip": "198.51.42.10", "waf_req_id": ""},
        {"ua": "Mozilla/5.0", "ip": "203.0.113.99", "waf_req_id": ""},
        {"ua": "Mozilla/5.0", "ip": "192.0.2.1", "waf_req_id": ""},
    ] * 2000  # 6,000 rows
    df = pd.DataFrame(rows)

    t0 = time.perf_counter()
    bot_sources.enrich_bot_metadata(df)
    elapsed = time.perf_counter() - t0

    assert elapsed < 0.5, f"Expected < 0.5s for 6,000 rows, took {elapsed:.3f}s"
    assert "openai-gptbot" in str(df.loc[0, "_bot_name"])
    assert "IP-Match" in str(df.loc[1, "_bot_name"])
    assert df.loc[2, "_bot_name"] == "null"


def test_high_scale_field_values_and_raw_df(monkeypatch: pytest.MonkeyPatch) -> None:
    """High-scale field_values supports fast aggregate and search paths; raw_df unpacks custom_fields."""
    from backend.models.dashboard import FieldValuesRequest, RawRequest
    from backend.utils import bot_sources

    monkeypatch.setattr(bot_sources, "enrich_bot_metadata", lambda df: None)

    class FakeClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "request_aggregates" in sql and "AS total_count" in sql:
                return [{"total_count": 2}]
            if "request_aggregates" in sql:
                return [
                    {"value": "/api/v1", "aggregate_count": 50},
                    {"value": "/health", "aggregate_count": 10},
                ]
            if "GROUP BY v" in sql:
                return [{"v": "/api/v1", "c": 50}]
            if "FROM request_facts" in sql:
                return [
                    {
                        "timestamp": datetime(2026, 10, 20, 12, 0, tzinfo=UTC),
                        "ip": "198.51.100.1",
                        "url": "/api/v1",
                        "country": "US",
                        "custom_fields": {"custom_header": "abc", "status": "200"},
                    }
                ]
            return []

    service = HighScaleService(
        service_id="svc1",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc1",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 10, 19, tzinfo=UTC),
            coverage_end=datetime(2026, 10, 20, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    unfiltered_fv = high_scale_dashboard.field_values(
        service,
        FieldValuesRequest(field="url", limit=10),
        "2026-10-19T00:00:00Z",
        "2026-10-20T00:00:00Z",
    )
    assert len(unfiltered_fv.values) == 2
    assert unfiltered_fv.values[0].value == "/api/v1"
    assert unfiltered_fv.values[0].count == 50

    search_fv = high_scale_dashboard.field_values(
        service,
        FieldValuesRequest(field="url", search="api", limit=10),
        "2026-10-19T00:00:00Z",
        "2026-10-20T00:00:00Z",
    )
    assert len(search_fv.values) == 1
    assert search_fv.values[0].value == "/api/v1"

    df = high_scale_dashboard.raw_df(
        service,
        RawRequest(limit=10),
        "2026-10-19T00:00:00Z",
        "2026-10-20T00:00:00Z",
        limit=10,
    )
    assert len(df) == 1
    assert df.loc[0, "url"] == "/api/v1"
    assert df.loc[0, "custom_header"] == "abc"
    assert "custom_fields" not in df.columns
