from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from backend.models.common import BaseResponse
from backend.repositories.utils import response_cache as rc


def test_lookup_and_store_observations_are_request_local(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [10.0]
    monkeypatch.setattr("backend.utils.bounded_cache.time.monotonic", lambda: clock[0])
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    assert memo.get("private-key") is None
    original = {"rows": [1], "_response_cache": {"state": "miss"}, "is_cached": True}
    memo.put("private-key", original)
    hit = memo.get("private-key")
    assert hit["_response_cache"] == {"state": "hit", "ttl_seconds": 15.0, "prewarmer": False}
    assert hit["is_cached"] is True
    assert "_response_cache" not in memo.cache["private-key"]
    hit["_response_cache"]["state"] = "bypass"
    assert memo.get("private-key")["_response_cache"]["state"] == "hit"
    clock[0] = 25.0
    assert memo.get("private-key") is None
    assert original["_response_cache"] == {"state": "miss"}


def test_compute_miss_recheck_hit_and_bypass() -> None:
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    first = memo.coalesce("unique-observation-key", lambda: {"rows": [1]})
    assert first["_response_cache"] == {
        "state": "miss",
        "ttl_seconds": 15.0,
        "prewarmer": False,
        "single_flight": "leader",
    }
    memo.put("key", first)
    rechecked = memo.coalesce("unique-observation-key", lambda: memo.get("key"))
    assert rechecked["_response_cache"]["state"] == "hit"
    bypass = rc.cache_result({"rows": []}, state="bypass", prewarmer=False)
    assert bypass["_response_cache"] == {"state": "bypass", "prewarmer": False}
    assert bypass["is_cached"] is False


def test_follower_cannot_relabel_the_leaders_shared_result(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.repositories.utils import single_flight

    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    entered, waiting, finish = Event(), Event(), Event()
    real_wait = single_flight.threading.Event.wait

    def tracked_wait(event: Event, timeout: float | None = None) -> bool:
        slot = single_flight._registry.get("concurrent-observation-key")
        if slot is not None and event is slot.event:
            waiting.set()
        return real_wait(event, timeout)

    monkeypatch.setattr(single_flight.threading.Event, "wait", tracked_wait)
    payload = {"rows": [1], "section_timings": [{"section": "query", "time_ms": 5}]}

    def build() -> dict:
        entered.set()
        assert finish.wait(5)
        return payload

    with ThreadPoolExecutor(max_workers=2) as pool:
        leader = pool.submit(memo.coalesce, "concurrent-observation-key", build)
        assert entered.wait(5)
        waiting.clear()
        follower = pool.submit(memo.coalesce, "concurrent-observation-key", build)
        try:
            assert waiting.wait(5)
        finally:
            finish.set()
        a, b = leader.result(5), follower.result(5)
    assert a["_response_cache"]["state"] == "miss"
    assert a["_response_cache"]["single_flight"] == "leader"
    assert b["_response_cache"]["state"] == "unknown"
    assert b["_response_cache"]["single_flight"] == "follower"
    assert b["_response_cache"]["wait_ms"] >= 0
    assert "wait_ms" not in a["_response_cache"]
    assert "_response_cache" not in payload


def test_observation_survives_serialization_without_debug_privileges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    payload = rc.cache_result(
        {"debug_queries": [{"sql": "private", "time_ms": 1}]}, state="miss", ttl_seconds=15, prewarmer=False
    )
    wire = BaseResponse(**payload).model_dump(by_alias=True)
    assert wire["_response_cache"] == {"state": "miss", "ttl_seconds": 15.0, "prewarmer": False}
    assert "_debug_queries" not in wire
    assert "private" not in str(wire)
    assert "_response_cache" not in BaseResponse().model_dump(by_alias=True)


def test_fresh_observation_replaces_both_alias_spellings() -> None:
    payload = {"response_cache": {"state": "hit"}, "_response_cache": {"state": "hit"}, "_is_cached": True}
    fresh = rc.cache_result(payload, state="miss", prewarmer=False)
    assert "response_cache" not in fresh
    assert "_is_cached" not in fresh
    assert BaseResponse(**fresh).response_cache.state == "miss"
    assert payload["_response_cache"]["state"] == "hit"


@pytest.mark.parametrize("state", ["hit", "miss", "bypass", "unknown"])
def test_observation_wire_bytes_match_the_typed_response(state: str) -> None:
    payload = rc.cache_result({}, state=state, ttl_seconds=15, prewarmer=False)
    raw = json.dumps(payload["_response_cache"], separators=(",", ":"))
    serialized = BaseResponse(**payload).model_dump(by_alias=True)["_response_cache"]
    assert json.dumps(serialized, separators=(",", ":")) == raw


def test_failed_builder_propagates_without_manufacturing_an_observation() -> None:
    def fail() -> dict:
        raise ValueError("failure")

    with pytest.raises(ValueError, match="failure"):
        rc.ResponseMemo().coalesce("failure-observation-key", fail)
