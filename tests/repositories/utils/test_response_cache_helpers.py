from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from backend.models.common import BaseResponse
from backend.repositories.utils import response_cache as rc
from backend.repositories.utils.response_cache import ResponseMemo, coalesce


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
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    entered, waiting, finish = Event(), Event(), Event()
    real_wait = rc.threading.Event.wait

    def tracked_wait(event: Event, timeout: float | None = None) -> bool:
        slot = rc._registry.get("concurrent-observation-key")
        if slot is not None and event is slot.event:
            waiting.set()
        return real_wait(event, timeout)

    monkeypatch.setattr(rc.threading.Event, "wait", tracked_wait)
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


class _Runner:
    def __init__(self, n: int) -> None:
        self.n = n

    def telemetry(self) -> dict:
        return {"debug_queries": [f"q{self.n}"], "is_cached": False}


def _sha(payload: dict, svc: str) -> str:
    body = json.dumps(payload, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{body}:{svc}".encode()).hexdigest()


def test_store_returns_fresh_telemetry_and_caches_a_request_clean_copy():
    memo = ResponseMemo(ttl=30, maxsize=4)
    first = memo.store("k", {"rows": [1], "debug_queries": ["stale"], "is_cached": True}, _Runner(1))
    assert first["debug_queries"] == ["q1"]

    hit = memo.check("k", _Runner(2))
    assert hit is not None
    assert hit["rows"] == [1]
    assert hit["debug_queries"] == ["q2"]
    assert hit["is_cached"] is True
    assert hit["_response_cache"]["state"] == "hit"
    assert first["_response_cache"]["state"] == "miss"


def test_check_miss_returns_none():
    assert ResponseMemo().check("absent", _Runner(1)) is None


def test_put_strips_extra_keys():
    memo = ResponseMemo()
    memo.put("k", {"a": 1, "section_timings": [1]}, strip=("section_timings",))
    assert memo.get("k") == {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }


def test_get_with_telemetry_restores_cached_query_attribution():
    memo = ResponseMemo()
    memo.put(
        "k",
        {
            "a": 1,
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB"}],
            "debug_calls": [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}],
        },
    )
    assert memo.get("k") == {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }
    hit = memo.get_with_telemetry("k")
    assert hit is not None
    assert hit["a"] == 1
    assert hit["is_cached"] is True
    assert hit["debug_queries"] == [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB", "is_cached": True}]
    assert hit["debug_calls"] == [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}]


def test_origin_response_cache_key_is_byte_stable():
    from backend.repositories.origin import _response_cache_key

    key = _response_cache_key(
        "slow_urls", {"name": "svc"}, "2026-01-01T00:00:12Z", "2026-01-01T01:00:59Z", {}, limit=3, mi=False
    )
    expected = _sha(
        {"ep": "slow_urls", "s": "2026-01-01T00:00", "e": "2026-01-01T01:00", "f": {}, "limit": 3, "mi": False},
        "svc",
    )
    assert key == expected


def test_cmcd_response_cache_key_is_byte_stable():
    from backend.repositories.cmcd import _response_cache_key

    key = _response_cache_key({"name": "svc"}, "2026-01-01T00:00:12Z", None, {}, 300, 30, {"b", "a"})
    expected = _sha(
        {"s": "2026-01-01T00:00", "e": None, "f": {}, "bs": 300, "tn": 30, "sec": ["a", "b"]},
        "svc",
    )
    assert key == expected


# ── Single-flight request-coalescing primitive tests ──────────────────────


def test_leader_runs_build_once_and_follower_shares_the_same_result():
    """Two threads racing on the SAME key: only the leader invokes ``build``;
    the follower blocks and gets back the IDENTICAL result object (not just
    an equal one) — proof it never ran its own copy of the work."""
    call_count = 0
    lock = threading.Lock()
    build_started = threading.Event()
    release = threading.Event()

    def build():
        nonlocal call_count
        with lock:
            call_count += 1
        build_started.set()
        assert release.wait(timeout=5), "test deadlocked waiting for release"
        return object()

    outcomes: dict[str, tuple] = {}

    def leader():
        outcomes["leader"] = coalesce("same-key", build)

    def follower():
        outcomes["follower"] = coalesce("same-key", build)

    t1 = threading.Thread(target=leader)
    t1.start()
    assert build_started.wait(timeout=5), "build() was never entered"

    t2 = threading.Thread(target=follower)
    t2.start()
    time.sleep(0.2)
    release.set()

    t1.join(timeout=5)
    t2.join(timeout=5)

    assert call_count == 1, "follower ran its own build() instead of sharing the leader's"
    leader_result, leader_is_leader = outcomes["leader"]
    follower_result, follower_is_leader = outcomes["follower"]
    assert leader_is_leader is True
    assert follower_is_leader is False
    assert leader_result is follower_result


def test_different_keys_each_run_their_own_build():
    """No false sharing: two concurrent calls with DIFFERENT keys must each
    invoke ``build`` — a mismatch on any input must never alias onto the
    other caller's result."""
    calls: list[str] = []
    lock = threading.Lock()

    def build_for(tag: str):
        def _build():
            with lock:
                calls.append(tag)
            return tag

        return _build

    outcomes: dict[str, tuple] = {}

    def run(key: str):
        outcomes[key] = coalesce(key, build_for(key))

    t1 = threading.Thread(target=run, args=("key-a",))
    t2 = threading.Thread(target=run, args=("key-b",))
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert sorted(calls) == ["key-a", "key-b"]
    assert outcomes["key-a"][0] == "key-a"
    assert outcomes["key-b"][0] == "key-b"


def test_sequential_calls_for_the_same_key_each_rebuild():
    """This is single-flight, NOT a cache: once the leader's call has fully
    finished, the next call for the SAME key must redo the work — a stale
    shared result must never survive past the in-flight window."""
    call_count = 0

    def build():
        nonlocal call_count
        call_count += 1
        return call_count

    first, first_is_leader = coalesce("seq-key", build)
    second, second_is_leader = coalesce("seq-key", build)

    assert first_is_leader is True
    assert second_is_leader is True
    assert first == 1
    assert second == 2  # rebuilt, not replayed from a cache


def test_build_exception_propagates_to_the_follower():
    """A leader failure must surface identically to every waiter — a follower
    must never silently get a different (e.g. empty/partial) result when the
    leader's build actually failed."""
    build_started = threading.Event()
    release = threading.Event()

    def build():
        build_started.set()
        assert release.wait(timeout=5)
        raise ValueError("boom")

    outcomes: dict[str, BaseException] = {}

    def leader():
        try:
            coalesce("fail-key", build)
        except ValueError as e:
            outcomes["leader"] = e

    def follower():
        try:
            coalesce("fail-key", build)
        except ValueError as e:
            outcomes["follower"] = e

    t1 = threading.Thread(target=leader)
    t1.start()
    assert build_started.wait(timeout=5)

    t2 = threading.Thread(target=follower)
    t2.start()
    time.sleep(0.2)
    release.set()

    t1.join(timeout=5)
    t2.join(timeout=5)

    assert isinstance(outcomes.get("leader"), ValueError)
    assert isinstance(outcomes.get("follower"), ValueError)
    assert str(outcomes["leader"]) == "boom"
    assert str(outcomes["follower"]) == "boom"


def test_registry_entry_is_removed_after_completion():
    """No leak: the module-level registry must not retain a finished slot —
    otherwise a much-later, non-overlapping call for the same key would
    incorrectly find a (stale, already-consumed) entry."""
    coalesce("cleanup-key", lambda: 1)
    assert "cleanup-key" not in rc._registry


def test_registry_entry_is_removed_even_on_failure():
    def boom():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        coalesce("cleanup-fail-key", boom)
    assert "cleanup-fail-key" not in rc._registry
