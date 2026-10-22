from pathlib import Path

import yaml


def test_prometheus_preserves_requested_bytes_series_via_recording_alias():
    prometheus = yaml.safe_load(Path("observability/prometheus.yml").read_text())
    assert "/etc/prometheus/clickhouse.rules.yml" in prometheus["rule_files"]
    overlay = yaml.safe_load(Path("docker-compose.observability.yml").read_text())
    assert (
        "./observability/clickhouse.rules.yml:/etc/prometheus/clickhouse.rules.yml:ro"
        in overlay["services"]["prometheus"]["volumes"]
    )
    rules = yaml.safe_load(Path("observability/clickhouse.rules.yml").read_text())
    assert rules["groups"][0]["rules"] == [
        {
            "record": "app_clickhouse_bytes_read_bytes_total",
            "expr": "app_clickhouse_bytes_read_total",
        }
    ]
