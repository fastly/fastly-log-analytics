import subprocess
from pathlib import Path

import yaml


def test_clickhouse_overlay_is_private_persistent_and_opt_in():
    overlay = yaml.safe_load(Path("docker-compose.clickhouse-prototype.yml").read_text())
    ch = overlay["services"]["clickhouse"]
    assert not ch.get("ports")
    assert ch["image"] == "clickhouse/clickhouse-server:25.8.4.13"
    assert ch["volumes"] == ["clickhouse-prototype-data:/var/lib/clickhouse"]
    assert ch["mem_limit"] == "2g"
    assert ch["restart"] == "unless-stopped"
    assert ch["networks"] == ["app-network"]
    assert ch["healthcheck"]["test"][-1] == "http://127.0.0.1:8123/ping"
    assert "CLICKHOUSE_ENABLED=${CLICKHOUSE_ENABLED:-false}" in overlay["services"]["backend"]["environment"]
    assert "clickhouse" not in yaml.safe_load(Path("docker-compose.multipod.yml").read_text())["services"]


def test_clickhouse_schema_init_reuses_backend_build_config_and_metadata():
    overlay = yaml.safe_load(Path("docker-compose.clickhouse-prototype.yml").read_text())
    backend = yaml.safe_load(Path("docker-compose.multipod.yml").read_text())["services"]["backend"]
    schema = overlay["services"]["clickhouse-schema-init"]
    assert schema["build"] == backend["build"]
    assert "./configs:/app/configs" in backend["volumes"]
    assert schema["volumes"] == ["./configs:/app/configs:ro"]
    metadata_dsn = next(value for value in backend["environment"] if value.startswith("METADATA_DSN="))
    assert metadata_dsn in schema["environment"]
    catalog = next(value for value in backend["environment"] if value.startswith("DUCKLAKE_CATALOG="))
    assert catalog in schema["environment"]
    assert (
        "COPY --chown=app:app scripts/generate_openapi.py scripts/clickhouse_replay.py scripts/"
        in Path("backend/Dockerfile").read_text()
    )
    assert "CLICKHOUSE_ENABLED=true" in schema["environment"]
    assert {value for value in schema["environment"] if value.startswith("CLICKHOUSE_")} == {
        value for value in overlay["services"]["backend"]["environment"] if not value.startswith("CLICKHOUSE_ENABLED=")
    } | {"CLICKHOUSE_ENABLED=true"}


def test_clickhouse_schema_init_is_private_one_shot_with_only_database_dependencies():
    overlay = yaml.safe_load(Path("docker-compose.clickhouse-prototype.yml").read_text())
    schema = overlay["services"]["clickhouse-schema-init"]
    assert schema["command"] == ["python", "-m", "backend.core.clickhouse_schema"]
    assert schema["restart"] == "no"
    assert not schema.get("ports")
    assert schema["networks"] == ["app-network"]
    assert schema["depends_on"] == {
        "postgres": {"condition": "service_healthy"},
        "clickhouse": {"condition": "service_healthy"},
    }
    base = yaml.safe_load(Path("docker-compose.multipod.yml").read_text())
    assert "clickhouse-schema-init" not in base["services"]
    assert "clickhouse-schema-init" not in base["services"]["backend"]["depends_on"]
    assert "depends_on" not in overlay["services"]["backend"]


def test_clickhouse_schema_target_builds_then_runs_fresh_init_without_backend_restart():
    makefile = Path("Makefile").read_text()
    assert any(
        "clickhouse-prototype-schema" in line.split()[1:]
        for line in makefile.splitlines()
        if line.startswith(".PHONY:")
    )
    compose = (
        "docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml "
        "-f docker-compose.clickhouse-prototype.yml"
    )
    expected = [
        f"{compose} build clickhouse-schema-init",
        f"{compose} run --rm clickhouse-schema-init",
    ]
    # Dry runs exercise Make expansion without touching the persistent Docker stack.
    for _ in range(2):
        result = subprocess.run(
            ["make", "--no-print-directory", "-n", "clickhouse-prototype-schema"],
            check=True,
            capture_output=True,
            text=True,
        )
        assert result.stdout.splitlines() == expected


def test_cli_exports_via_existing_prometheus_otlp_receiver():
    overlay = yaml.safe_load(Path("docker-compose.clickhouse-prototype.yml").read_text())
    env = overlay["services"]["clickhouse-schema-init"]["environment"]
    assert "OTEL_EXPORTER=otlp" in env
    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://prometheus:9090/api/v1/otlp/v1/metrics" in env
    observability = yaml.safe_load(Path("docker-compose.observability.yml").read_text())
    assert "--web.enable-otlp-receiver" in observability["services"]["prometheus"]["command"]


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
