"""Pure-logic tests for the per-section latency sweep (no network)."""

from __future__ import annotations

from scripts.dev import section_sweep as sweep


def test_classify_section_buckets():
    assert sweep.classify_section("proxies_rollup") == "rollup"
    assert sweep.classify_section("req_size_dist_rollup") == "rollup"
    assert sweep.classify_section("security:temp_skipped") == "skipped"
    assert sweep.classify_section("connection_acquire") == "queue"
    assert sweep.classify_section("get_schema_cols") == "overhead"
    assert sweep.classify_section("temp_table_create") == "live"
    assert sweep.classify_section("wellknown_bots_query") == "live"


def test_summarize_sections_totals_and_temp_flag():
    summary = sweep.summarize_sections(
        [
            {"section": "connection_acquire", "time_ms": 120.0},
            {"section": "bot_efficiency_rollup", "time_ms": 9.0},
            {"section": "temp_table_create", "time_ms": 458.6},
            {"section": "wellknown_bots_query", "time_ms": 217.0},
            {"section": "security:temp_skipped", "time_ms": 0.0},
            {"not": "a timing"},
        ]
    )
    assert summary["totals_ms"]["queue"] == 120.0
    assert summary["totals_ms"]["rollup"] == 9.0
    assert summary["totals_ms"]["live"] == 675.6
    assert summary["counts"] == {"rollup": 1, "live": 2, "skipped": 1}
    assert summary["temp_built"] is True
    assert sweep.summarize_sections([])["temp_built"] is False


def _rec(endpoint, window, sections):
    return {
        "endpoint": endpoint,
        "window": window,
        "cold": sweep.summarize_sections(sections),
        "warm": sweep.summarize_sections([]),
        "cold_wall_ms": 1.0,
        "warm_wall_ms": 1.0,
    }


def test_find_offenders_ranks_live_sections_over_budget_only():
    records = [
        _rec("security", "7d", [{"section": "proxies", "time_ms": 1500}, {"section": "x_rollup", "time_ms": 9000}]),
        _rec("origin", "3d", [{"section": "slow_urls_query", "time_ms": 2500}, {"section": "tiny", "time_ms": 10}]),
        {"endpoint": "network", "window": "3d", "error": "HTTPError: 500"},
    ]
    offenders = sweep.find_offenders(records, budget_ms=500)
    assert [(o["endpoint"], o["section"]) for o in offenders] == [
        ("origin", "slow_urls_query"),
        ("security", "proxies"),
    ]


def test_render_report_lists_rows_errors_and_offenders():
    text = sweep.render_report(
        [
            _rec("security", "3d", [{"section": "temp_table_create", "time_ms": 900}]),
            {"endpoint": "origin", "window": "3d", "error": "URLError: timed out"},
        ],
        budget_ms=500,
    )
    assert "| security | 3d |" in text
    assert "error: URLError: timed out" in text
    assert "| security | 3d | temp_table_create | 900 |" in text
