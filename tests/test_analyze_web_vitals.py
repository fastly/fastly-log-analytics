"""Tests for scripts/analyze_web_vitals.py — the dev/AI report generator.

The script lives under scripts/ (not an importable package), so it's loaded
by path with importlib.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "analyze_web_vitals.py"
_spec = importlib.util.spec_from_file_location("analyze_web_vitals", _SCRIPT)
assert _spec and _spec.loader
awv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(awv)


def test_percentile_interpolates():
    assert awv._percentile([], 0.5) == 0.0
    assert awv._percentile([42.0], 0.95) == 42.0
    assert awv._percentile([0.0, 10.0], 0.5) == 5.0
    # 0..100 step 10 → p50 == 50, p75 == 75 with linear interpolation.
    vals = [float(x) for x in range(0, 101, 10)]
    assert awv._percentile(vals, 0.5) == 50.0
    assert awv._percentile(vals, 0.75) == 75.0


def test_verdict_buckets_against_thresholds():
    assert awv._verdict("LCP", 2000.0) == "good"  # <= 2500
    assert awv._verdict("LCP", 3000.0) == "needs-improvement"  # 2500..4000
    assert awv._verdict("LCP", 5000.0) == "poor"  # > 4000
    assert awv._verdict("CLS", 0.05) == "good"
    assert awv._verdict("UNKNOWN", 1.0) == "unknown"


def test_aggregate_groups_by_route_and_metric():
    samples = [
        {"name": "LCP", "value": 1000.0, "rating": "good", "pathname": "/a"},
        {"name": "LCP", "value": 3000.0, "rating": "needs-improvement", "pathname": "/a"},
        {"name": "LCP", "value": 500.0, "rating": "good", "pathname": "/b"},
        {"name": "BOGUS", "value": 1.0, "rating": "good", "pathname": "/a"},  # dropped: not a vital
    ]
    stats = awv.aggregate(samples)
    assert ("/a", "LCP") in stats
    assert ("/b", "LCP") in stats
    assert ("/a", "BOGUS") not in stats
    a = stats[("/a", "LCP")]
    assert a["count"] == 2
    assert a["ratings"] == {"good": 1, "needs-improvement": 1, "poor": 0}


def test_build_report_data_focus_excludes_good_and_honors_min_samples():
    stats = {
        ("/slow", "LCP"): {
            "count": 10,
            "p50": 4000.0,
            "p75": 5000.0,
            "p95": 6000.0,
            "ratings": {"good": 0, "needs-improvement": 0, "poor": 10},
            "verdict": "poor",
        },
        ("/fast", "LCP"): {
            "count": 10,
            "p50": 800.0,
            "p75": 1000.0,
            "p95": 1200.0,
            "ratings": {"good": 10, "needs-improvement": 0, "poor": 0},
            "verdict": "good",
        },
        ("/rare", "LCP"): {
            "count": 1,
            "p50": 9000.0,
            "p75": 9000.0,
            "p95": 9000.0,
            "ratings": {"good": 0, "needs-improvement": 0, "poor": 1},
            "verdict": "poor",
        },
    }
    report = awv.build_report_data(stats, min_samples=3)
    routes_in_focus = {r["route"] for r in report["focus"]}
    assert "/slow" in routes_in_focus  # poor + enough samples
    assert "/fast" not in routes_in_focus  # good verdict, never in focus
    assert "/rare" not in routes_in_focus  # below min_samples
    # /fast is still present in the full rows table.
    assert any(r["route"] == "/fast" for r in report["rows"])


def test_load_samples_filters_cohort_and_tolerates_torn_lines(tmp_path):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":1,"rating":"good","cohort":"admin","ts":"2026-06-19T00:00:00Z"}\n'
        '{"name":"LCP","value":2,"rating":"good","cohort":"analyst","ts":"2026-06-19T00:00:00Z"}\n'
        '{"name":"LCP","value":3,  <-- torn line\n',
        encoding="utf-8",
    )
    admin_only = awv.load_samples(p, since=None, cohort="admin")
    assert len(admin_only) == 1 and admin_only[0]["cohort"] == "admin"
    all_valid = awv.load_samples(p, since=None, cohort="all")
    assert len(all_valid) == 2  # torn line skipped


def test_main_no_file_is_a_clean_noop(tmp_path, capsys):
    rc = awv.main(["--input", str(tmp_path / "missing.jsonl")])
    assert rc == 0
    assert "No web-vitals data" in capsys.readouterr().err


def test_main_purge_deletes_file_after_report(tmp_path, capsys):
    p = tmp_path / "wv.jsonl"
    lines = [
        json.dumps(
            {
                "name": "LCP",
                "value": v,
                "rating": "poor",
                "cohort": "admin",
                "pathname": "/x",
                "ts": "2026-06-19T00:00:00Z",
            }
        )
        for v in (5000, 5100, 5200, 5300)
    ]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rc = awv.main(["--input", str(p), "--min-samples", "1", "--purge"])
    assert rc == 0
    out = capsys.readouterr()
    assert "Web Vitals Analysis" in out.out
    assert "/x" in out.out
    assert not p.exists()  # purged
    assert "Purged" in out.err


def test_main_purge_off_keeps_file(tmp_path):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":5000,"rating":"poor","cohort":"admin","pathname":"/x","ts":"2026-06-19T00:00:00Z"}\n',
        encoding="utf-8",
    )
    awv.main(["--input", str(p), "--min-samples", "1"])
    assert p.exists()  # not purged without the flag


@pytest.mark.parametrize("fmt", ["md", "json"])
def test_main_writes_to_out_file(tmp_path, fmt, capsys):
    p = tmp_path / "wv.jsonl"
    p.write_text(
        '{"name":"LCP","value":5000,"rating":"poor","cohort":"admin","pathname":"/x","ts":"2026-06-19T00:00:00Z"}\n',
        encoding="utf-8",
    )
    out = tmp_path / "report.out"
    rc = awv.main(["--input", str(p), "--min-samples", "1", "--format", fmt, "--out", str(out)])
    assert rc == 0
    assert out.exists() and out.read_text(encoding="utf-8").strip()


def _rec(value: float, route: str) -> str:
    return json.dumps(
        {
            "name": "LCP",
            "value": value,
            "rating": "poor",
            "cohort": "admin",
            "pathname": route,
            "ts": "2026-06-19T00:00:00Z",
        }
    )


def test_main_reads_rotated_segment_and_purge_removes_both(tmp_path, capsys):
    """The analyzer includes the rotated .1 backup in both the report and
    the purge, so analysis spans the full retained window."""
    active = tmp_path / "wv.jsonl"
    rotated = tmp_path / "wv.jsonl.1"
    rotated.write_text("\n".join(_rec(v, "/old") for v in (5000, 5100, 5200)) + "\n", encoding="utf-8")
    active.write_text("\n".join(_rec(v, "/new") for v in (4800, 4900, 5300)) + "\n", encoding="utf-8")

    rc = awv.main(["--input", str(active), "--min-samples", "1", "--purge"])
    assert rc == 0
    out = capsys.readouterr()
    # Both segments contributed to the report.
    assert "/old" in out.out and "/new" in out.out
    # Both segments purged.
    assert not active.exists() and not rotated.exists()
