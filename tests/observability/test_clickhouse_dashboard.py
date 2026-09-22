import json
from pathlib import Path


def test_clickhouse_dashboard_uses_exact_metrics_and_existing_datasource():
    dashboard = json.loads(Path("observability/dashboards/fla-multipod.json").read_text())
    panels = [p for p in dashboard["panels"] if p["title"].startswith("ClickHouse")]
    assert len(panels) >= 9
    queries = " ".join(target["expr"] for panel in panels for target in panel.get("targets", []))
    for name in (
        "app_clickhouse_query_duration_ms_milliseconds_bucket",
        "app_clickhouse_insert_duration_ms_milliseconds_bucket",
        "app_clickhouse_queries_total",
        "app_clickhouse_publications_total",
        "app_clickhouse_publication_lag_seconds",
        "app_clickhouse_up",
        "app_clickhouse_disk_free_bytes",
        "app_clickhouse_disk_total_bytes",
        "app_clickhouse_rows_inserted_total",
        "app_clickhouse_bytes_read_bytes_total",
        "docker_container_memory_usage_bytes",
    ):
        assert name in queries
    assert "histogram_quantile(0.95" in queries and "histogram_quantile(0.99" in queries
    for panel in panels:
        if panel["type"] != "row":
            assert panel["datasource"]["uid"] == "prometheus"
    assert "or vector(0)" not in queries
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            if "backend|worker" in target["expr"]:
                assert "|clickhouse" in target["expr"]
    assert len({p["id"] for p in dashboard["panels"]}) == len(dashboard["panels"])
