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


@pytest.mark.parametrize("rows", [[], [{"pop": "PRIVATE_ROW_VALUE", "requests": 7}]])
@pytest.mark.parametrize("state", ["bypass", "hit"])
def test_list_component_remains_unknown_without_copying_rows(rows: list[dict], state: str) -> None:
    result = describe_response({"_response_cache": {"state": state}, "pop_health": rows})
    components = {row["component"]: row for row in result["components"]}
    pop = components["pop_health"]
    assert pop["cache_state"] == "unknown"
    assert pop["sections"] == []
    assert pop["reader_coverage"] == []
    assert pop["history"] is None
    assert "component_observation_envelope_missing" in pop["gaps"]
    assert "response_cache_unknown" in pop["gaps"]
    assert pop["execution_reused"] is (state == "hit")
    assert "PRIVATE_ROW_VALUE" not in str(result)


@pytest.mark.parametrize("child", [None, "absent"])
def test_absent_list_component_is_not_fabricated(child: object) -> None:
    payload = {"pop_health": child} if child is None else {}
    assert [row["component"] for row in describe_response(payload)["components"]] == ["response"]


@pytest.mark.parametrize("reuse", ["none", "hit", "follower"])
def test_list_component_uses_owned_sidecar_without_promoting_reused_execution(reuse: str) -> None:
    payload = {
        "_response_cache": {"state": "bypass", "prewarmer": False},
        "pop_health": [{"pop": "PRIVATE_ROW_VALUE"}],
        "_component_observations": {
            "pop_health": {
                "_response_cache": {"state": "bypass", "prewarmer": False},
                "_section_timings": [
                    {"section": "pop_health", "time_ms": 3, "source": "live", "current_execution": True}
                ],
            }
        },
    }
    if reuse == "hit":
        payload["_response_cache"]["state"] = "hit"
    elif reuse == "follower":
        payload["_response_cache"].update(state="unknown", single_flight="follower")
    result = describe_response(payload)
    pop = next(row for row in result["components"] if row["component"] == "pop_health")
    assert pop["cache_state"] == "bypass"
    assert pop["lookup_current"] is (reuse == "none")
    assert pop["sections"][0]["current_execution"] is (reuse == "none")
    assert "component_observation_envelope_missing" not in pop["gaps"]
    assert "reader_coverage_missing" in pop["gaps"]
    assert "history_evidence_missing" in pop["gaps"]
    assert "PRIVATE_ROW_VALUE" not in str(result)


def test_list_component_prevents_false_composite_cache_pair_success() -> None:
    from scripts.lib.section_measurements import sample_cache_pair

    clock = _Clock()
    answers = iter(["hit", "miss", "hit"])

    def request(body: dict, timeout: float) -> dict:
        return {
            "_response_cache": {"state": "bypass", "prewarmer": False},
            "health": _component(next(answers)),
            "pop_health": [],
        }

    result = sample_cache_pair(request, {}, max_seconds=60, monotonic=clock.monotonic, sleep=clock.sleep)
    assert result["status"] == "incomplete"
    assert result["components"]["health"]["status"] == "observed"
    assert result["components"]["pop_health"]["status"] == "incomplete"
    assert "miss_hit_pair_unobserved" in result["gaps"]


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


# ── Merged from tests/load/test_clickhouse_serving_benchmark.py ──

"""Safety and measurement contracts; no Docker or network required."""

import asyncio
import copy
from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest
from pydantic import ValidationError

from tests.load.clickhouse_serving_benchmark import (
    BenchmarkReport,
    Freshness,
    RecoveryEvidence,
    Sample,
    Stage,
    attempt,
    parser,
    quantile,
    request_for,
    runtime,
    summarize_payload,
    validate_args,
)


def args(*extra):
    return parser().parse_args(
        [
            "--engine",
            "ducklake",
            "--output",
            "local-docs/test-evidence.json",
            "--apply",
            "--allow-prototype",
            "--base-url",
            "http://127.0.0.1",
            "--backend-container",
            "local-backend",
            *extra,
        ]
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1",
        "http://localhost",
        "http://example.com",
        "http://127.0.0.1.example.com",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1/path",
        "http://127.0.0.1?q=x",
    ],
)
def test_nonlocal_and_ambiguous_origins_refused(url):
    with pytest.raises(ValueError):
        validate_args(args("--base-url", url))


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--attempts", "29"),
        ("--attempts", "257"),
        ("--timeout", "0"),
        ("--timeout", "61"),
        ("--timeout", "nan"),
        ("--stage-seconds", "601"),
        ("--memory-abort-mb", "2049"),
        ("--concurrency", "128"),
        ("--output", "report.json"),
    ],
)
def test_bounds_refused(flag, value):
    with pytest.raises(ValueError):
        validate_args(args(flag, value))


@pytest.mark.parametrize("flag", ["apply", "allow_prototype", "base_url", "backend_container"])
def test_explicit_optin_required(flag):
    a = args()
    setattr(a, flag, None)
    with pytest.raises(ValueError):
        validate_args(a)


def test_allowed_budget_and_quantiles():
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        validate_args(args())
    assert quantile([1.0, 2.0, 100.0], 0.95) == pytest.approx(90.2)


def test_live_hybrid_benchmark_refused_before_docker_or_network():
    with patch("subprocess.run") as run:
        with pytest.raises(ValueError, match="archived"):
            validate_args(args("--engine", "clickhouse_hybrid"))
        run.assert_not_called()


def test_removed_dashboard_route_cannot_be_reported_as_hybrid():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["error"] == "engine"
    assert summary["clickhouse_calls"] == 0


def test_current_ducklake_dashboard_needs_no_retired_dispatch_marker():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "ducklake")
    assert summary["error"] is None
    assert summary["dispatch"] == "ducklake"
    assert summary["duckdb_queries"] == 1


@pytest.mark.parametrize("enabled", ["true", "false"])
def test_ducklake_benchmark_does_not_require_disabling_diagnostics(enabled):
    container = {
        "Config": {
            "Env": [
                "DEPLOYMENT_MODE=high_scale",
                f"CLICKHOUSE_ENABLED={enabled}",
                "METADATA_DSN=postgresql://fixture",
                "DUCKLAKE_CATALOG=postgresql://fixture",
                "CLICKHOUSE_HOST=clickhouse",
            ],
            "Labels": {
                "com.docker.compose.project.config_files": (
                    "docker-compose.multipod.yml,docker-compose.observability.yml,"
                    "docker-compose.clickhouse-prototype.yml"
                )
            },
        },
        "Image": "sha256:" + "a" * 64,
        "HostConfig": {"Memory": 0},
    }
    with patch("subprocess.check_output", return_value=json.dumps([container])):
        assert runtime("local-backend") == (container["Image"], 0)


def test_mixed_corpus_preserves_narrow_adapter_support():
    assert request_for("bundle")[1]["sections"] == ["core", "topten"]
    assert request_for("top_n")[1]["sections"] == ["topten"]
    assert request_for("time_series")[1]["include_conn_requests"] is False
    assert request_for("empty")[1]["start_time"] == "2026-09-06T00:00:00Z"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_failures_retained_and_response_secrets_omitted(status):
    async def run():
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, json={"secret": "NEVER_COPY_SECRET", "sql": "SELECT PRIVATE"})
            ),
        ) as client:
            return await attempt(client, "PRIVATE_SERVICE", "bundle", "ducklake", 1)

    sample = asyncio.run(run())
    assert sample.error == "http" and sample.status_code == status and sample.duration_ms > 0
    assert sample.rows_read is None and sample.bytes_read is None and sample.readiness_ms is None
    assert "PRIVATE" not in sample.model_dump_json() and "SECRET" not in sample.model_dump_json()


def test_deadline_retains_slow_attempt():
    async def handler(_):
        await asyncio.sleep(1)
        return httpx.Response(200)

    async def run():
        async with httpx.AsyncClient(base_url="http://127.0.0.1", transport=httpx.MockTransport(handler)) as client:
            return await attempt(client, "fixture", "bundle", "ducklake", 0.01)

    sample = asyncio.run(run())
    assert sample.error == "timeout" and sample.duration_ms >= 10


def test_schema_rejects_arbitrary_payload_and_false_complete():
    with pytest.raises(ValidationError):
        Sample.model_validate({"sql": "PRIVATE"})
    with pytest.raises(ValidationError):
        Stage(
            concurrency=1,
            requested_attempts=30,
            peak_in_flight=1,
            status="complete",
            elapsed_seconds=1.0,
            backend_peak_memory_bytes=1,
            warmups=[],
            samples=[],
            report=None,
        )
    assert "sql" not in json.dumps(Sample.model_json_schema()["properties"])


def top_n_payload(dispatch="disabled"):
    return {
        "_is_cached": False,
        "_section_timings": [{"section": "engine:" + dispatch, "time_ms": 0.0}],
        "_debug_queries": [{"sql": "PRIVATE_SQL", "time_ms": 1.0}],
        "total_rows": 184374,
        "interval": "1 minute",
        "metric": "requests",
        "data": {"country": {"total": 184374, "top": [{"value": "US", "count": 184374, "label": None}]}},
        "time_series": [],
        "map_data": [],
    }


def test_top_n_has_no_chart_and_uses_existing_minute_metadata():
    summary = summarize_payload(top_n_payload(), "top_n", "ducklake")
    assert summary["error"] is None
    assert summary["semantic_digest"] is not None
    assert "PRIVATE" not in json.dumps(summary)


def test_disabled_engine_must_not_be_mislabeled_fallback():
    summary = summarize_payload(top_n_payload("unsupported_sections"), "top_n", "ducklake")
    assert summary["error"] == "engine"
    assert summarize_payload(top_n_payload("unsupported_sections"), "top_n", "clickhouse_hybrid")["error"] is None


def test_telemetry_counts_and_errors_are_retained_without_details():
    payload = top_n_payload("unsupported_sections")
    payload["_debug_calls"] = [
        {
            "service": "ClickHouse",
            "url": "PRIVATE_HOST",
            "details": json.dumps(
                {
                    "query_id": "PRIVATE_ID",
                    "outcome": "error",
                    "rows_read": 2,
                    "bytes_read": 100,
                }
            ),
        }
    ]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["clickhouse_errors"] == 1
    assert summary["rows_read"] == 2 and summary["bytes_read"] == 100
    assert "PRIVATE" not in json.dumps(summary)


@pytest.mark.parametrize("memory", [1024, 2 * 1024**3])
def test_scheduler_respects_budget_and_records_errors(monkeypatch, memory):
    from tests.load import clickhouse_serving_benchmark as module

    calls = 0

    async def fake_attempt(*_):
        nonlocal calls
        calls += 1
        n = calls
        await asyncio.sleep(0)
        return Sample(
            kind="bundle",
            duration_ms=float(n),
            status_code=200 if n <= 3 else 503,
            error=None if n <= 3 else "http",
            dispatch="disabled",
            response_cached=False,
            duckdb_queries=0,
            clickhouse_calls=0,
            clickhouse_errors=0,
            rows_read=0,
            bytes_read=0,
            readiness_ms=0.0,
            semantic_digest=None,
        )

    async def fake_memory(*_):
        return memory

    monkeypatch.setattr(module, "attempt", fake_attempt)
    monkeypatch.setattr(module, "memory_bytes", fake_memory)
    now = datetime(2026, 9, 7, tzinfo=UTC)
    fresh = Freshness(
        observed_at=now,
        event_watermark=now,
        window_start=datetime(2026, 9, 1, tzinfo=UTC),
        window_end=datetime(2026, 9, 2, tzinfo=UTC),
        last_commit_at=now,
        last_publication_at=now,
    )
    measured = asyncio.run(module.stage(None, "fixture", args(), 8, fresh, 0.0))
    if memory > 1024:
        assert measured.status == "memory_abort"
        assert measured.samples == [] and measured.report is None and calls == 0
    else:
        assert measured.status == "complete" and measured.peak_in_flight == 8
        assert len(measured.samples) == 64 and calls == 67
        assert measured.report.error_count == 64 and measured.report.achieved_throughput_rps == 0
        assert measured.max_ms == 67
        assert measured.report.p99_ms == quantile([float(n) for n in range(4, 68)], 0.99)


def test_recovery_contract_never_accepts_credentials_or_nonfinite_durations():
    with pytest.raises(ValidationError):
        RecoveryEvidence.model_validate({"password": "NEVER_COPY"})
    assert "password" not in RecoveryEvidence.model_json_schema()["properties"]


# ── Merged from tests/load/test_clickhouse_serving_contract.py ──


@pytest.fixture
def report():
    return {
        "schema_version": 1,
        "engine": "ducklake",
        "dataset_size_rows": 190334,
        "eligible_window_rows": 184374,
        "concurrency": 1,
        "warmup_requests": 3,
        "measured_requests": 30,
        "elapsed_seconds": 3.0,
        "achieved_throughput_rps": 10.0,
        "p50_ms": 90.0,
        "p95_ms": 110.0,
        "p99_ms": 120.0,
        "ingest_lag_seconds": 128.0,
        "error_count": 0,
        "freshness": {
            "observed_at": datetime(2026, 9, 7, 23, tzinfo=UTC),
            "event_watermark": datetime(2026, 9, 7, 19, tzinfo=UTC),
            "window_start": datetime(2026, 9, 1, tzinfo=UTC),
            "window_end": datetime(2026, 9, 2, tzinfo=UTC),
            "last_commit_at": datetime(2026, 9, 7, 20, tzinfo=UTC),
            "last_publication_at": datetime(2026, 9, 7, 21, tzinfo=UTC),
        },
        "clickhouse_ingest_lag_seconds": None,
        "clickhouse_rebuild_seconds": None,
    }


def test_report_json_roundtrip(report):
    validated = BenchmarkReport.model_validate(report)
    assert BenchmarkReport.model_validate_json(validated.model_dump_json()) == validated
    assert validated.model_dump()["clickhouse_rebuild_seconds"] is None


@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "engine",
        "dataset_size_rows",
        "eligible_window_rows",
        "concurrency",
        "warmup_requests",
        "measured_requests",
        "elapsed_seconds",
        "achieved_throughput_rps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "ingest_lag_seconds",
        "error_count",
        "freshness",
        "clickhouse_ingest_lag_seconds",
        "clickhouse_rebuild_seconds",
    ],
)
def test_missing_report_field_rejected(report, field):
    del report[field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf"), "1", True, None])
@pytest.mark.parametrize(
    "field", ["elapsed_seconds", "achieved_throughput_rps", "p50_ms", "p95_ms", "p99_ms", "ingest_lag_seconds"]
)
def test_invalid_measurement_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, 1.5, True, "30", None])
@pytest.mark.parametrize(
    "field",
    ["dataset_size_rows", "eligible_window_rows", "concurrency", "warmup_requests", "measured_requests", "error_count"],
)
def test_invalid_count_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "updates",
    [
        {"p50_ms": 111.0},
        {"p99_ms": 109.0},
        {"error_count": 31},
        {"eligible_window_rows": 190335},
        {"dataset_size_rows": 0},
        {"eligible_window_rows": 0},
        {"concurrency": 0},
        {"warmup_requests": 2},
        {"measured_requests": 29},
        {"elapsed_seconds": 0.0},
        {"achieved_throughput_rps": 0.0},
        {"clickhouse_ingest_lag_seconds": 0.0},
        {"clickhouse_rebuild_seconds": 0.0},
        {"schema_version": True},
        {"engine": "unknown"},
        {"unexpected": "not allowed"},
    ],
)
def test_inconsistent_report_rejected(report, updates):
    report.update(updates)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field", ["observed_at", "event_watermark", "window_start", "window_end", "last_commit_at", "last_publication_at"]
)
def test_missing_or_naive_freshness_rejected(report, field):
    missing = copy.deepcopy(report)
    del missing["freshness"][field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(missing)
    report["freshness"][field] = datetime(2026, 9, 1)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field,value",
    [
        ("window_end", datetime(2026, 9, 1, tzinfo=UTC)),
        ("event_watermark", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_commit_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_publication_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("unexpected", datetime(2026, 9, 1, tzinfo=UTC)),
    ],
)
def test_invalid_freshness_order_rejected(report, field, value):
    report["freshness"][field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_errors_reduce_successful_throughput(report):
    report.update(error_count=3, achieved_throughput_rps=9.0)
    assert BenchmarkReport.model_validate(report).error_count == 3
    report["achieved_throughput_rps"] = 10.0
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_equal_quantiles_and_all_failed_requests_are_representable(report):
    report.update(p50_ms=0.0, p95_ms=0.0, p99_ms=0.0, error_count=30, achieved_throughput_rps=0.0)
    assert BenchmarkReport.model_validate(report).error_count == 30


def test_later_hybrid_measurements_are_explicit(report):
    report.update(engine="clickhouse_hybrid", clickhouse_ingest_lag_seconds=1.0, clickhouse_rebuild_seconds=42.0)
    assert BenchmarkReport.model_validate(report).clickhouse_rebuild_seconds == 42.0
    report["clickhouse_rebuild_seconds"] = float("nan")
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_section_query_correlation_missing_flags_uncorrelated_current_fact_section() -> None:
    """A current fact/projection section with no matching current query op is a gap.

    Mirrors the cross-check at scripts/lib/section_measurements.py: a section whose
    source resolves to fact/projection/projected_fact/mixed AND is the current
    execution MUST have a correlated current ClickHouse query op whose section_id
    matches, or the harness cannot attribute the section's work to a real query.
    """
    base_payload = {
        "_response_cache": {"state": "miss"},
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

    # gap PRESENT: no current query op correlates to section-test
    uncorrelated = describe_response(base_payload)
    assert "section_query_correlation_missing" in uncorrelated["gaps"]

    # gap ABSENT: a valid strict current ClickHouse op whose section_id matches
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
    correlated_payload = deepcopy(base_payload)
    correlated_payload["_debug_queries"] = [{"sql": "PRIVATE", "clickhouse": operation}]
    correlated = describe_response(correlated_payload)
    assert "section_query_correlation_missing" not in correlated["gaps"]
    assert "PRIVATE" not in json.dumps(correlated)
