import json
import subprocess
from pathlib import Path

import pytest

_SOURCE = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


@pytest.mark.parametrize(
    ("charts", "expected"),
    [
        ([], False),
        ([{"width": 524, "height": 300, "data": [{"x": [], "y": []}]}], False),
        ([{"width": 1, "height": 1, "data": [{"x": [0], "y": [0]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [0]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"values": [10]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [None]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": ["10"]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [float("nan")]}]}], False),
    ],
)
def test_rum_chart_requires_real_geometry_and_points(charts: list[dict], expected: bool) -> None:
    source = _SOURCE.read_text()
    start = source.index("function hasPopulatedRumChart()")
    end = source.index("\n}", start) + 2
    script = (
        source[start:end]
        + "\nconst charts = "
        + json.dumps(charts)
        + ";\nglobal.document = {querySelectorAll: selector => {"
        + "if (selector !== 'main .js-plotly-plot') throw Error('Unscoped chart selector');"
        + "return charts.map(c => ({data:c.data,getBoundingClientRect:()=>c}));}};"
        + "\nconsole.log(JSON.stringify(hasPopulatedRumChart()));"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) is expected


def test_remote_rum_chart_failure_cannot_warn_and_proceed() -> None:
    source = _SOURCE.read_text()
    assert "waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 })" in source
    assert "No populated full-sized RUM chart" in source
    assert "No visible Plotly charts found on the RUM page for remote cloud environment" not in source
