import os

from backend.repositories import dashboard, network, security
from backend.repositories._base import _safe_table
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


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
        con=in_memory_duckdb,
        src=src,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-02T00:00:00Z",
        filters={},
        bucket_seconds=3600,
    )
    timings_net_health = [t["section"] for t in res_net_health.get("section_timings", [])]
    assert "temp_table_create" not in timings_net_health, f"Network health raw scan! {timings_net_health}"
