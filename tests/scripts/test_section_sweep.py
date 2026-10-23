"""Pure-logic measurement-harness tests; no environment access."""

from __future__ import annotations

import stat
from copy import deepcopy

import pytest

from scripts.dev import section_sweep as sweep
from scripts.lib.section_measurements import describe_response


def test_classify_section_buckets() -> None:
    assert sweep.classify_section("proxies_rollup") == "rollup"
    assert sweep.classify_section("req_size_dist_rollup") == "rollup"
    assert sweep.classify_section("security:temp_skipped") == "skipped"
    assert sweep.classify_section("connection_acquire") == "queue"
    assert sweep.classify_section("get_schema_cols") == "overhead"
    assert sweep.classify_section("temp_table_create") == "unknown"
    assert sweep.classify_section("wellknown_bots_query") == "unknown"
    assert sweep.classify_section("summary_cache_hit") == "cache"


def test_summarize_sections_sources_and_temp_flag() -> None:
    summary = sweep.summarize_sections(
        [
            {"section": "connection_acquire", "time_ms": 120.0},
            {"section": "bot_efficiency_rollup", "time_ms": 9.0},
            {"section": "temp_table_create", "time_ms": 458.6},
            {"section": "wellknown_bots_query", "time_ms": 217.0, "source": "live"},
            {"section": "security:temp_skipped", "time_ms": 0.0},
            {"not": "a timing"},
        ]
    )
    assert summary["sections"][0]["activity"] == "queue"
    assert summary["sections"][1]["source"] == "rollup"
    assert summary["sections"][3]["source"] == "live"
    assert "totals_ms" not in summary
    assert summary["temp_built"] is True
    assert sweep.summarize_sections([])["temp_built"] is False


def _rec(endpoint: str, window: str, sections: list[dict]) -> dict:
    sample = {
        "sample_id": endpoint,
        "environment": "remote-standard",
        "role": "admin",
        "endpoint": endpoint,
        "window": window,
        "payload": {"range_token": window, "filters": {}},
        "response": describe_response({"_section_timings": sections}),
        "wall_ms": 1.0,
        "gaps": ["response_cache_unknown"],
    }
    return {"endpoint": endpoint, "window": window, "samples": [sample], "payload": sample["payload"]}


def test_find_offenders_ranks_sections_and_keeps_unknown_evidence() -> None:
    records = [
        _rec("security", "7d", [{"section": "proxies", "time_ms": 1500, "source": "live"}]),
        _rec("origin", "3d", [{"section": "slow_urls_query", "time_ms": 2500}, {"section": "tiny", "time_ms": 10}]),
        {"endpoint": "network", "window": "3d", "samples": [], "error": "HTTPError"},
    ]
    offenders = sweep.find_offenders(records, budget_ms=500)
    assert [(row["endpoint"], row["section"]) for row in offenders] == [
        ("origin", "slow_urls_query"),
        ("security", "proxies"),
    ]
    assert all(row["cache_state"] == "unknown" and not row["repeatable"] for row in offenders)


def test_render_report_lists_rows_errors_and_offenders() -> None:
    text = sweep.render_report(
        [
            _rec("security", "3d", [{"section": "temp_table_create", "time_ms": 900}]),
            {"endpoint": "origin", "window": "3d", "samples": [], "error": "URLError"},
        ],
        budget_ms=500,
    )
    assert "| security | 3d |" in text
    assert "URLError" in text
    assert "temp_table_create" in text
    assert "incomplete" in text
    assert "cold" not in text.lower()
    assert "warm" not in text.lower()


def test_repeated_sampling_keeps_exact_payload_errors_and_independent_cache_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = {
        "range_token": "7d",
        "anchor": "2026-10-21T00:00:00Z",
        "filters": {"status": {"mode": "include", "values": ["200"]}, "method": {"mode": "include", "values": ["GET"]}},
    }
    expected = deepcopy(body)
    responses = iter([({"_is_cached": True}, 10), OSError("PRIVATE URL token"), ({"_is_cached": False}, 900)])
    calls = []

    def post(*args: object, **kwargs: object) -> tuple[dict, float]:
        calls.append(deepcopy(args[3]))
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(sweep, "_post", post)
    result = sweep.sweep_one(
        "https://example.com",
        "TestLogSvcABC123",
        "security",
        "/api/security/aggregates",
        body,
        "7d",
        5,
        sample_count=3,
        environment="remote-standard",
    )
    assert calls == [expected] * 3
    assert result["payload"] == expected
    assert body == expected
    samples = result["samples"]
    assert len(samples) == 3
    assert samples[0]["response"]["components"][0]["cache_state"] == "hit"
    assert samples[1]["error"] == "OSError"
    assert samples[2]["response"]["components"][0]["cache_state"] == "unknown"
    assert "PRIVATE" not in str(result)
    assert result["qualification"] == "incomplete"
    assert result["visible_results_budget_ms"] == 10000
    assert result["whole_request_budget_policy"] == "diagnostic_only"
    assert result["whole_request_budget_ms"] is None
    assert "page_ready_unmeasured" in result["gaps"]
    assert not any("cold" in key or "warm" in key for key in result)


def test_mixed_component_cache_states_are_not_dropped_from_distributions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sweep, "_post", lambda *args, **kwargs: ({"_is_cached": False, "aggregates": {"_is_cached": True}}, 10)
    )
    result = sweep.sweep_one(
        "https://example.com",
        "TestLogSvcABC123",
        "dashboard",
        "/api/dashboard/bundle",
        {"filters": {}},
        "24h",
        5,
        sample_count=1,
    )
    assert result["request_distribution_by_cache_state_ms"]["mixed"]["count"] == 1
    assert sum(row["count"] for row in result["request_distribution_by_cache_state_ms"].values()) == 1


def test_sweep_cli_blocks_network_without_verified_prerequisites(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Prerequisite failure must happen before any live request")

    monkeypatch.setattr(sweep, "run_sweep", forbidden)
    assert sweep.main(["--env", "remote-standard", "--service-id", "TestLogSvcABC123"]) == 2
    evidence = tmp_path / "evidence.json"
    evidence.write_text('{"environment":"remote-standard", "checks":{}}')
    assert (
        sweep.main(["--env", "remote-standard", "--service-id", "TestLogSvcABC123", "--prerequisites", str(evidence)])
        == 2
    )


def test_gate_rejects_other_environment_missing_or_unverified_evidence() -> None:
    assert "environment_mismatch" in sweep.prerequisite_gaps({"environment": "local-standard"}, "remote-standard")
    assert "cache_reporting" in sweep.prerequisite_gaps(
        {"environment": "remote-standard", "checks": {"cache_reporting": {"status": "unknown"}}}, "remote-standard"
    )


def test_exact_filter_payload_artifacts_are_owner_only_and_symlinks_are_rejected(tmp_path) -> None:
    report = {"payload": {"filters": {"url": {"values": ["/video?signature=PRIVATE"]}}}}
    path = tmp_path / "report.json"
    path.write_text("old")
    path.chmod(0o644)
    sweep.write_report(path, report)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "signature=PRIVATE" in path.read_text()
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(OSError):
        sweep.write_report(link, {"changed": True})
    assert "signature=PRIVATE" in path.read_text()


def test_gate_rejects_claimed_checks_without_evidence() -> None:
    report = {"environment": "remote-standard", "checks": {key: {"status": "verified"} for key in sweep.PREREQUISITES}}
    assert set(sweep.prerequisite_gaps(report, "remote-standard")) == set(sweep.PREREQUISITES)
