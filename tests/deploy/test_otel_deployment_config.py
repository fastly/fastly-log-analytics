from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OTEL_ENV = (
    "OTEL_EXPORTER",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
)


def _render_prod_compose(otel_values: dict[str, str] | None = None) -> dict:
    env = os.environ.copy()
    for name in OTEL_ENV:
        env.pop(name, None)
    env.update(otel_values or {})
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.prod.yml",
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_prod_compose_keeps_otel_export_disabled_by_default() -> None:
    backend_env = _render_prod_compose()["services"]["backend"]["environment"]

    assert backend_env["OTEL_EXPORTER"] == "none"
    assert backend_env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == ""
    assert backend_env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == ""


def test_prod_compose_passes_operator_otel_settings_without_defaults() -> None:
    metrics = "metrics-endpoint-from-operator"
    traces = "traces-endpoint-from-operator"
    backend_env = _render_prod_compose(
        {
            "OTEL_EXPORTER": "otlp",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": metrics,
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": traces,
        }
    )["services"]["backend"]["environment"]

    assert backend_env["OTEL_EXPORTER"] == "otlp"
    assert backend_env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == metrics
    assert backend_env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == traces
