from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.lib.section_measurements import describe_response


def test_controlled_sampling_flag_does_not_bypass_prerequisite_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.dev import section_sweep

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("live transport must not run")

    monkeypatch.setattr(section_sweep, "_post", forbidden)
    assert section_sweep.main(["--env", "local-standard", "--controlled-cache-pair"]) == 2


def test_follower_sections_are_not_current_execution() -> None:
    result = describe_response(
        {
            "_response_cache": {
                "state": "unknown",
                "ttl_seconds": 15,
                "prewarmer": False,
                "single_flight": "follower",
                "wait_ms": 8,
            },
            "_section_timings": [{"section": "query", "time_ms": 5, "current_execution": True, "source": "fact"}],
        }
    )
    component = result["components"][0]
    assert component["single_flight"] == "follower"
    assert component["wait_ms"] == 8
    assert component["sections"][0]["current_execution"] is False
    assert "shared_execution" in component["gaps"]


def test_self_scoped_health_hit_does_not_suppress_independent_shielding() -> None:
    child = {
        "_response_cache": {"state": "miss", "prewarmer": False},
        "_section_timings": [{"section": "shielding", "time_ms": 10, "current_execution": True, "source": "live"}],
    }
    payload = {"_response_cache": {"state": "hit", "scope": "self", "prewarmer": False}, "shielding_analysis": child}
    result = describe_response(payload)
    assert result["components"][1]["sections"][0]["current_execution"] is True
    enclosing = describe_response({"_is_cached": True, "health": payload})
    assert enclosing["components"][2]["sections"][0]["current_execution"] is False


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def _component(state: str, **extra: object) -> dict:
    return {"_response_cache": {"state": state, "ttl_seconds": 15, "prewarmer": False, **extra}}


def test_expiry_sampler_keeps_payload_exact_and_requires_miss_then_hit() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    payload = {"anchor": "2026-10-22T00:00:00Z", "filters": {"status": {"values": ["200"]}}}
    expected = deepcopy(payload)
    calls = []
    answers = iter([_component("hit"), _component("miss"), _component("hit")])

    def request(body: dict, timeout: float) -> dict:
        calls.append((deepcopy(body), timeout, clock.now))
        body.clear()
        return next(answers)

    result = sample_cache_pair(request, payload, max_seconds=60, monotonic=clock.monotonic, sleep=clock.sleep)
    assert result["status"] == "observed"
    assert result["components"]["response"] == {"status": "observed", "miss_sample": 2, "hit_sample": 3}
    assert [call[0] for call in calls] == [expected] * 3
    assert calls[1][2] >= 15
    assert payload == expected
    assert result["database_cache_state"] == "unknown"


@pytest.mark.parametrize(
    "answer", [_component("unknown"), _component("miss", single_flight="follower"), {"_is_cached": False}]
)
def test_unknown_and_shared_results_cannot_establish_a_pair(answer: dict) -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    result = sample_cache_pair(
        lambda body, timeout: answer, {}, max_seconds=30, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert result["status"] == "incomplete"
    assert len(result["samples"]) <= 4
    assert result["gaps"]


def test_repopulation_and_independent_components_do_not_manufacture_a_pair() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(
        [
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
            {"aggregates": _component("miss"), "top_bots": _component("hit")},
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
            {"aggregates": _component("hit"), "top_bots": _component("hit")},
        ]
    )
    result = sample_cache_pair(
        lambda body, timeout: next(answers), {}, max_seconds=45, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert result["components"]["aggregates"]["status"] == "observed"
    assert result["components"]["top_bots"]["status"] == "incomplete"
    assert result["status"] == "incomplete"


def test_bypass_is_not_a_miss_and_failures_are_safe() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    bypass = sample_cache_pair(
        lambda body, timeout: _component("bypass"), {}, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert bypass["status"] == "not_applicable"
    assert bypass["components"]["response"]["status"] == "bypass"

    def fail(body: dict, timeout: float) -> dict:
        raise OSError("secret URL or cookie")

    failed = sample_cache_pair(fail, {}, monotonic=clock.monotonic, sleep=clock.sleep)
    assert failed["status"] == "incomplete"
    assert failed["samples"][0]["error"] == "OSError"
    assert "secret" not in str(failed)


def test_initial_miss_is_only_a_baseline_until_ttl_expiry() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter([_component("miss"), _component("hit"), _component("miss"), _component("hit")])
    result = sample_cache_pair(
        lambda body, timeout: next(answers),
        {},
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result["status"] == "observed"
    assert result["components"]["response"] == {"status": "observed", "miss_sample": 3, "hit_sample": 4}
    assert sum(clock.waits) >= 15


def test_historical_child_hit_cannot_complete_a_current_miss_pair() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(
        [
            {"_response_cache": {"state": "bypass", "prewarmer": False}, "aggregates": _component("miss")},
            {
                "_response_cache": {"state": "hit", "ttl_seconds": 15, "prewarmer": False},
                "aggregates": _component("hit"),
            },
        ]
    )
    result = sample_cache_pair(
        lambda body, timeout: next(answers),
        {},
        max_samples=2,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result["components"]["aggregates"]["status"] == "incomplete"
    assert "hit_sample" not in result["components"]["aggregates"]


def test_expiry_sampler_honors_attempt_and_wall_time_bounds() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    timeouts = []

    def request(body: dict, timeout: float) -> dict:
        timeouts.append(timeout)
        clock.now += timeout
        return _component("miss")

    result = sample_cache_pair(request, {}, max_seconds=10, monotonic=clock.monotonic, sleep=clock.sleep)
    assert timeouts == [10]
    assert result["status"] == "incomplete"
    assert clock.now == 10
    with pytest.raises(ValueError):
        sample_cache_pair(request, {}, max_seconds=float("nan"))


# ── Merged from test_section_measurements.py ──────────────────────────────────────────────────────

import json
import math
import subprocess
from pathlib import Path

import pytest

from scripts.lib.section_measurements import (
    distribution,
    rank_sections,
    visible_results_budget_ms,
)


def test_false_cache_marker_does_not_establish_a_miss() -> None:
    result = describe_response({"_is_cached": False, "_section_timings": []})
    assert result["components"][0]["cache_state"] == "unknown"
    assert "response_cache_unknown" in result["components"][0]["gaps"]
    assert "section_timings_missing" in result["components"][0]["gaps"]


@pytest.mark.parametrize("state", ["hit", "miss", "bypass", "unknown"])
def test_explicit_cache_outcomes_are_separate_from_source(state: str) -> None:
    result = describe_response(
        {
            "_response_cache": {"state": state, "reason": "lookup", "prewarmer": False},
            "_section_timings": [{"section": "summary", "time_ms": 12, "source": "projection"}],
        }
    )
    component = result["components"][0]
    assert component["cache_state"] == state
    assert component["prewarmer"] is False
    assert component["sections"][0]["source"] == "projection"
    assert component["sections"][0]["current_execution"] is (False if state == "hit" else None)


def test_nested_components_keep_independent_cache_states_and_cached_timings() -> None:
    result = describe_response(
        {
            "_is_cached": False,
            "aggregates": {
                "_is_cached": True,
                "_section_timings": [{"section": "count_rollup", "time_ms": 900}],
            },
            "top_bots": {"_section_timings": [{"section": "ua_query", "elapsed_ms": 25}]},
        }
    )
    components = {row["component"]: row for row in result["components"]}
    assert set(components) == {"response", "aggregates", "top_bots"}
    assert components["response"]["cache_state"] == "unknown"
    assert components["aggregates"]["cache_state"] == "hit"
    assert components["aggregates"]["sections"][0]["current_execution"] is False
    assert components["top_bots"]["sections"][0]["ms"] == 25
    assert components["top_bots"]["sections"][0]["source"] == "unknown"
    assert "historical_or_unverified_timings" in components["aggregates"]["gaps"]


@pytest.mark.parametrize("value", [None, True, -1, math.nan, math.inf, "12"])
def test_invalid_durations_are_unknown_not_zero(value: object) -> None:
    section = describe_response({"_section_timings": [{"section": "scan", "time_ms": value}]})["components"][0][
        "sections"
    ][0]
    assert section["ms"] is None
    assert "duration_unknown" in section["gaps"]


def test_response_cache_hit_is_not_a_rollup_and_parallel_spans_are_not_summed() -> None:
    result = describe_response(
        {
            "_section_timings": [
                {"section": "summary_cache_hit", "time_ms": 1},
                {"section": "foo_rollup", "time_ms": 40},
                {"section": "bar_rollup", "time_ms": 50},
                {"section": "connection_acquire", "time_ms": 10},
            ]
        }
    )
    sections = result["components"][0]["sections"]
    assert sections[0]["source"] == "unknown"
    assert sections[0]["activity"] == "cache"
    assert sections[1]["source"] == "rollup"
    assert sections[1]["source_evidence"] == "section_name"
    assert sections[3]["activity"] == "queue"
    assert "totals_ms" not in result
    assert "totals_ms" not in result["components"][0]


def test_zero_and_elapsed_ms_are_supported_without_fallback_overwriting_zero() -> None:
    result = describe_response(
        {"section_timings": [{"section": "zero", "time_ms": 0, "elapsed_ms": 50}, {"section": "b", "elapsed_ms": 9}]}
    )
    assert [row["ms"] for row in result["components"][0]["sections"]] == [0, 9]


def test_report_never_copies_raw_debug_queries_headers_or_data() -> None:
    result = describe_response(
        {
            "_debug_queries": [{"sql": "SECRET SQL", "params": ["PRIVATE"]}],
            "headers": {"Cookie": "BEARER"},
            "data": {"ip": "192.0.2.1"},
            "_section_timings": [{"section": "summary", "time_ms": 1, "sql": "SECRET SQL"}],
        }
    )
    assert all(secret not in str(result) for secret in ("SECRET SQL", "PRIVATE", "BEARER", "192.0.2.1"))


def test_non_object_response_is_incomplete() -> None:
    assert describe_response([]) == {"components": [], "gaps": ["response_not_object"]}


def test_malformed_metadata_is_unknown_and_section_gaps_reach_top_level() -> None:
    result = describe_response(
        {"_response_cache": {"state": []}, "_section_timings": [{"section": "scan", "source": {}, "time_ms": 1}]}
    )
    assert result["components"][0]["cache_state"] == "unknown"
    assert "source_unverified" in result["gaps"]
    assert "execution_freshness_unknown" in result["gaps"]


def test_conflicting_cache_markers_are_not_accepted_as_miss() -> None:
    result = describe_response({"_response_cache": {"state": "miss"}, "_is_cached": True})
    assert result["components"][0]["cache_state"] == "unknown"
    assert "response_cache_conflict" in result["gaps"]


def test_enclosing_hit_and_conflicting_marker_cannot_manufacture_current_misses() -> None:
    cached = _sample(1)
    cached["response"] = describe_response(
        {
            "_is_cached": True,
            "aggregates": {
                "_response_cache": {"state": "miss"},
                "_section_timings": [{"section": "scan", "time_ms": 900, "current_execution": True}],
            },
        }
    )
    conflict = _sample(2)
    conflict["response"] = describe_response(
        {
            "_is_cached": True,
            "_response_cache": {"state": "miss"},
            "_section_timings": [{"section": "scan", "time_ms": 900, "current_execution": True}],
        }
    )
    assert cached["response"]["components"][1]["sections"][0]["current_execution"] is False
    assert conflict["response"]["components"][0]["sections"][0]["current_execution"] is False
    assert rank_sections([cached, conflict]) == []


@pytest.mark.parametrize(
    "filters",
    [
        {"status": {"values": []}},
        {"status": {"values": [""]}},
        {"status": {"values": ["200"], "mode": "ignored"}},
        {"status": ["200"]},
    ],
)
def test_inactive_or_invalid_filters_cannot_receive_filtered_budget(filters: dict) -> None:
    with pytest.raises(ValueError, match="filter"):
        visible_results_budget_ms("30d", filters)


def test_distribution_is_nearest_rank_and_reports_sample_support() -> None:
    stats = distribution(list(range(1, 101)))
    assert (stats["count"], stats["p50"], stats["p90"], stats["p95"], stats["p99"]) == (100, 50, 90, 95, 99)
    assert stats["mean"] == 50.5
    assert stats["min"] == 1
    assert stats["max"] == 100
    assert stats["p95_sample_floor_met"] is True
    assert stats["p99_sample_floor_met"] is True
    assert distribution([0])["p95_sample_floor_met"] is False
    assert distribution([])["p95"] is None


@pytest.mark.parametrize("values", [[-1], [math.inf], [math.nan], [True]])
def test_distribution_rejects_invalid_samples(values: list[float]) -> None:
    with pytest.raises(ValueError, match="finite nonnegative"):
        distribution(values)


def test_visible_results_budgets_are_not_request_or_section_budgets() -> None:
    assert visible_results_budget_ms("24h", {}) == 2000
    assert visible_results_budget_ms("3d", {}) == 5000
    assert visible_results_budget_ms("7d", {"status": {"values": ["200"], "mode": "include"}}) == 10000
    assert visible_results_budget_ms("30d", {"status": {"values": ["200"]}, "method": {"values": ["GET"]}}) == 30000
    with pytest.raises(ValueError, match="Unsupported window"):
        visible_results_budget_ms("1h", {})


def _sample(number: int, *, role: str = "admin", cache: str = "miss", ms: float = 900) -> dict:
    return {
        "sample_id": str(number),
        "environment": "remote-standard",
        "role": role,
        "endpoint": "security",
        "window": "7d",
        "payload": {"range_token": "7d", "filters": {}},
        "response": describe_response(
            {
                "_response_cache": {"state": cache},
                "_section_timings": [
                    {"section": "proxies", "time_ms": ms, "source": "live", "current_execution": True}
                ],
            }
        ),
    }


def test_ranking_preserves_role_filters_cache_and_repeated_evidence() -> None:
    samples = [_sample(1), _sample(2, ms=1100), _sample(3, role="analyst-no-pii"), _sample(4, cache="hit")]
    filtered = _sample(5)
    filtered["payload"]["filters"] = {"status": {"values": ["200"]}, "country": {"values": ["US"]}}
    samples.append(filtered)
    rows = rank_sections(samples)
    admin_unfiltered = next(row for row in rows if row["role"] == "admin" and not row["payload"]["filters"])
    assert admin_unfiltered["cache_state"] == "miss"
    assert admin_unfiltered["distribution_ms"]["count"] == 2
    assert admin_unfiltered["repeatable"] is True
    assert len(rows) == 3
    assert all(row["cache_state"] != "hit" for row in rows)


def test_sample_id_reuse_across_environments_does_not_drop_evidence() -> None:
    local = _sample(1)
    remote = _sample(1)
    local["environment"] = "local-standard"
    assert len(rank_sections([local, remote])) == 2


def test_python_and_browser_reporters_agree_on_evidence() -> None:
    payloads = [
        {},
        {"_is_cached": False},
        {
            "_response_cache": {"state": "miss"},
            "_debug_queries": [
                {
                    "sql": "PRIVATE",
                    "clickhouse": {
                        "query_id": "query-test",
                        "section_id": "section-test",
                        "request_id": "request-test",
                        "service_id": "service-test",
                        "section": "cmcd.overview",
                        "source": "projected_fact",
                        "outcome": "success",
                        "client_elapsed_ms": 100,
                        "network_ms": 50,
                        "admission_wait_ms": 20,
                        "decode_ms": 30,
                        "server_cpu_ms": None,
                    },
                }
            ],
            "_section_timings": [
                {
                    "section": "cmcd.overview",
                    "section_id": "section-test",
                    "time_ms": 110,
                    "source": "projected_fact",
                    "current_execution": True,
                }
            ],
        },
        {
            "_debug_queries": [
                {"clickhouse": {"query_id": "PRIVATE VALUE", "outcome": "success", "client_elapsed_ms": 5}}
            ]
        },
        {
            "_response_cache": {"state": "miss"},
            "_reader_coverage": [
                {
                    "reader": "dashboard.count",
                    "minimum_window_hours": None,
                    "closed_hour_heal": "used",
                    "scalar_plan": {
                        "reason": "planned",
                        "closed_hours": 168,
                        "prefix_hours": 144,
                        "trailing_start": "2026-01-07T00:00:00+00:00",
                        "trailing": {"reason": "planned", "closed_hours": 24, "bundle_hours": 24},
                    },
                }
            ],
            "_history": {
                "extent_source": "live_query",
                "extent_rows": 10,
                "window_rows": 5,
                "window_count_source": "raw",
                "earliest_event_at": "2026-01-01T00:00:00+00:00",
                "latest_event_at": "2026-01-08T00:00:00+00:00",
            },
        },
        {
            "_reader_coverage": [
                {
                    "reader": "dashboard.top_n",
                    "top_n_plan": {
                        "coverage_scope": "any_field_file_presence",
                        "field_complete_hours": None,
                        "any_field_covered_hours": 1,
                        "heal_missing_hours": 0,
                    },
                }
            ]
        },
        {"_history": {"extent_source": "status_cache", "earliest_event_at": "PRIVATE"}},
        {
            "_reader_coverage": [
                {
                    "reader": "dashboard.ngwaf_bots",
                    "minimum_window_hours": 2,
                    "outcome": "served",
                    "live_top_up": "unavailable",
                    "attempts": [{"bundle": "ngwaf_bots", "reason": "planned", "closed_hours": 4, "covered_hours": 2}],
                }
            ]
        },
        {"_reader_coverage": [{"reader": "unknown", "minimum_window_hours": True}]},
        {
            "_reader_coverage": [
                {"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2, "window_start": "PRIVATE"}
            ]
        },
        {
            "_response_cache": {"state": "hit"},
            "aggregates": {
                "_response_cache": {"state": "miss"},
                "_reader_coverage": [
                    {"reader": "dashboard.conn_requests_histogram", "minimum_window_hours": 2, "outcome": "served"}
                ],
            },
        },
        {
            "_response_cache": {"state": "unknown", "single_flight": "follower"},
            "_reader_coverage": [{"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2, "outcome": "served"}],
        },
        {"_response_cache": {"state": "miss"}, "_is_cached": True},
        {
            "_response_cache": {"state": []},
            "_section_timings": [{"section": "scan", "source": {}, "activity": [], "time_ms": "12"}],
        },
        {
            "aggregates": {"_is_cached": True, "_section_timings": [{"section": "foo_rollup", "elapsed_ms": 15}]},
            "top_bots": {},
        },
        {
            "_response_cache": {"state": "bypass", "prewarmer": False},
            "_section_timings": [
                {"section": "summary", "time_ms": 0, "source": "fact", "activity": "compute", "current_execution": True}
            ],
        },
    ]
    script = "const fs=require('node:fs');const {describeResponse}=require('./scripts/lib/section_measurements.cjs');process.stdout.write(JSON.stringify(JSON.parse(fs.readFileSync(0,'utf8')).map(describeResponse)));"
    result = subprocess.run(
        ["node", "-e", script],
        input=json.dumps(payloads),
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    assert json.loads(result.stdout) == [describe_response(payload) for payload in payloads]


@pytest.mark.parametrize("cache", ["miss", "bypass", "hit", "unknown"])
def test_reader_coverage_keeps_planning_distinct_from_history_and_execution(cache: str) -> None:
    observation = {
        "reader": "dashboard.conn_requests_histogram",
        "minimum_window_hours": 2,
        "eligible": True,
        "eligibility_reason": "eligible",
        "outcome": "served",
        "live_top_up": "failed",
        "attempts": [
            {
                "bundle": "security_conn_reuse",
                "reason": "planned",
                "closed_hours": 4,
                "covered_hours": 2,
                "coverage_fraction": 0.5,
                "path": "PRIVATE",
            }
        ],
        "sql": "PRIVATE",
    }
    report = describe_response({"_response_cache": {"state": cache}, "_reader_coverage": [observation]})
    reader = report["components"][0]["reader_coverage"][0]
    assert reader["observation"]["attempts"][0]["covered_hours"] == 2
    assert reader["current_observation"] is (False if cache == "hit" else True if cache in {"miss", "bypass"} else None)
    assert "history_evidence_missing" in report["gaps"]
    assert "reader_live_top_up_failed" in report["gaps"]
    assert "PRIVATE" not in json.dumps(report)
    assert "reader_coverage" not in observation


def test_clickhouse_observations_keep_safe_correlation_and_client_phases_only() -> None:
    operation = {
        "query_id": "query-test",
        "request_id": "request-test",
        "service_id": "service-test",
        "section_id": "section-test",
        "section": "cmcd.overview",
        "source": "projected_fact",
        "outcome": "success",
        "client_elapsed_ms": 100,
        "admission_wait_ms": 20,
        "network_ms": 50,
        "decode_ms": 30,
        "server_cpu_ms": None,
        "sql": "PRIVATE",
        "url": "PRIVATE",
    }
    report = describe_response(
        {
            "_response_cache": {"state": "miss"},
            "_debug_queries": [{"sql": "PRIVATE", "clickhouse": operation}],
            "_section_timings": [
                {
                    "section": "cmcd.overview",
                    "section_id": "section-test",
                    "time_ms": 105,
                    "source": "projected_fact",
                    "current_execution": True,
                }
            ],
        }
    )
    query = report["components"][0]["query_operations"][0]
    assert query["observation"]["query_id"] == "query-test"
    assert query["observation"]["network_ms"] == 50
    assert query["observation"]["server_cpu_ms"] is None
    assert query["current_observation"] is True
    assert report["components"][0]["sections"][0]["section_id"] == "section-test"
    assert "PRIVATE" not in json.dumps(report)
    assert "server_cpu_unobserved" in report["gaps"]
    hit = describe_response(
        {"_response_cache": {"state": "bypass"}, "_debug_queries": [{"is_cached": True, "clickhouse": operation}]}
    )
    assert hit["components"][0]["query_operations"][0]["current_observation"] is False


def test_history_retains_provenance_without_certifying_cached_or_rolled_counts() -> None:
    for state, source in [("hit", "live_query"), ("miss", "status_cache"), ("miss", "live_query")]:
        report = describe_response(
            {
                "_response_cache": {"state": state},
                "_history": {
                    "extent_source": source,
                    "extent_rows": 5,
                    "window_rows": 5,
                    "window_count_source": "rollup",
                    "earliest_event_at": "2026-01-01T00:00:00+00:00",
                    "latest_event_at": "2026-01-02T00:00:00+00:00",
                    "sql": "PRIVATE",
                },
            }
        )
        history = report["components"][0]["history"]
        assert history["observation"]["extent_rows"] == 5
        assert history["current_observation"] is (state == "miss")
        assert "history_window_rows_unverified" in report["gaps"]
        assert "history_continuity_unverified" in report["gaps"]
        assert "PRIVATE" not in json.dumps(report)
        if source == "status_cache":
            assert "history_extent_unverified" in report["gaps"]


def test_history_reports_when_raw_extent_starts_after_requested_window() -> None:
    report = describe_response(
        {
            "_response_cache": {"state": "miss"},
            "_history": {
                "extent_source": "live_query",
                "extent_rows": 5,
                "window_rows": 5,
                "window_count_source": "rollup",
                "window_start": "2026-09-25T05:00:00+00:00",
                "earliest_event_at": "2026-09-25T05:38:42+00:00",
                "raw_extent_reaches_window_start": False,
            },
        }
    )

    assert "history_raw_prefix_uncovered" in report["gaps"]
    assert "history_continuity_unverified" in report["gaps"]
    assert report["components"][0]["history"]["observation"]["raw_extent_reaches_window_start"] is False


def test_reader_observation_limit_does_not_truncate_normal_multi_reader_payload() -> None:
    rows = [
        {"reader": reader, "minimum_window_hours": None}
        for reader in (
            "dashboard.top_n",
            "dashboard.count",
            "dashboard.time_series",
            "dashboard.conn_requests_histogram",
        )
    ]
    report = describe_response({"_reader_coverage": rows})
    assert len(report["components"][0]["reader_coverage"]) == 4
    assert "reader_coverage_truncated" not in report["gaps"]


def test_bundle_wrapper_does_not_claim_missing_coverage_owned_by_children() -> None:
    report = describe_response(
        {
            "aggregates": {
                "_reader_coverage": [{"reader": "dashboard.conn_requests_histogram", "minimum_window_hours": 2}]
            },
            "top_bots": {"_reader_coverage": [{"reader": "dashboard.ngwaf_bots", "minimum_window_hours": 2}]},
        }
    )
    assert "reader_coverage_missing" not in report["components"][0]["gaps"]
    assert "reader_coverage_missing" not in report["gaps"]
    assert "history_evidence_missing" in report["gaps"]


def test_missing_reader_metadata_stays_a_gap() -> None:
    report = describe_response({"_response_cache": {"state": "miss"}})
    assert report["components"][0]["reader_coverage"] == []
    assert "reader_coverage_missing" in report["gaps"]


def test_duplicate_sample_ids_do_not_manufacture_repeatability() -> None:
    sample = _sample(1)
    assert rank_sections([sample, sample])[0]["distribution_ms"]["count"] == 1
    assert rank_sections([sample, sample])[0]["repeatable"] is False


# ── Merged from test_section_sweep.py ──────────────────────────────────────────────────────

"""Pure-logic measurement-harness tests; no environment access."""

import stat

import pytest

from scripts.dev import section_sweep as sweep


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
