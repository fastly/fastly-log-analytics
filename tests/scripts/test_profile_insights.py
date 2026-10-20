"""Tests for `scripts/dev/profile_insights.py`."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.dev.profile_insights import format_markdown_report, main, run_profile


def test_run_profile_collects_phases_coalesced_and_insights():
    res = run_profile(total_rows=1000, runs=2, num_files=3, window_hours=1.0, baseline_hours=24.0)

    assert res["runs"] == 2
    assert res["dataset"]["total_rows"] == 1000
    assert res["cold_total"]["wall_p50_ms"] > 0
    assert res["warm_total"]["wall_p50_ms"] >= 0
    assert len(res["top_10_by_wall_p50"]) == 10
    assert len(res["top_10_by_cpu_p50"]) == 10

    op_names = {op["operation"] for op in res["all_operations"]}
    assert "phase:scratch_table_build" in op_names
    assert "coalesced:city" in op_names
    assert "coalesced:url" in op_names
    assert "coalesced:ip" in op_names
    assert "coalesced:waf" in op_names
    assert any(name.startswith("insight:") for name in op_names)

    md = format_markdown_report(res)
    assert "# Insights Performance Profile" in md
    assert "Top 10 Costs by Wall-Clock p50" in md


def test_profile_insights_cli_writes_markdown_and_json(tmp_path: Path):
    md_out = tmp_path / "profile.md"
    json_out = tmp_path / "profile.json"
    rc = main(
        [
            "--rows",
            "500",
            "--runs",
            "1",
            "--files",
            "2",
            "--output",
            str(md_out),
            "--json-output",
            str(json_out),
        ]
    )
    assert rc == 0
    assert md_out.exists()
    assert json_out.exists()
    payload = json.loads(json_out.read_text())
    assert payload["dataset"]["total_rows"] == 500
