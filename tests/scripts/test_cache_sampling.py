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
    from scripts.lib.cache_sampling import sample_cache_pair

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
    from scripts.lib.cache_sampling import sample_cache_pair

    clock = _Clock()
    result = sample_cache_pair(
        lambda body, timeout: answer, {}, max_seconds=30, monotonic=clock.monotonic, sleep=clock.sleep
    )
    assert result["status"] == "incomplete"
    assert len(result["samples"]) <= 4
    assert result["gaps"]


def test_repopulation_and_independent_components_do_not_manufacture_a_pair() -> None:
    from scripts.lib.cache_sampling import sample_cache_pair

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
    from scripts.lib.cache_sampling import sample_cache_pair

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
    from scripts.lib.cache_sampling import sample_cache_pair

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
    from scripts.lib.cache_sampling import sample_cache_pair

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
    from scripts.lib.cache_sampling import sample_cache_pair

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
