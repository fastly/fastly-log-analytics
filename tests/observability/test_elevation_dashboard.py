import json
from pathlib import Path

import pytest


def _dashboard() -> dict:
    dashboard_path = Path("local-docs/fla-multipod-elevation.json")
    if not dashboard_path.exists():
        pytest.skip("Elevation dashboard export is intentionally local-only")
    return json.loads(dashboard_path.read_text())


def test_elevation_dashboard_includes_high_scale_operational_panels() -> None:
    dashboard = _dashboard()
    panels = dashboard["panels"]
    panel_text = json.dumps(panels)

    expected_metrics = (
        "fla_high_scale_source_objects",
        "fla_high_scale_oldest_active_age_seconds",
        "fla_high_scale_publication_lag_seconds",
        "fla_high_scale_pending_manifests",
        "fla_high_scale_rows_published_total",
    )

    assert len({panel["id"] for panel in panels}) == len(panels)
    for metric in expected_metrics:
        assert metric in panel_text


def test_elevation_dashboard_keeps_variable_based_promql() -> None:
    dashboard = _dashboard()
    panel_text = json.dumps(dashboard["panels"])

    assert "$site" in panel_text
    assert "$namespace" in panel_text
