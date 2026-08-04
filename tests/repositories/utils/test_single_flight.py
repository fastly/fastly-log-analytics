"""Tests for the generic single-flight request-coalescing primitive.

These pin the correctness contract that ``backend/repositories/network.py``
relies on to dedupe concurrent identical ``/api/network-health`` temp-table
builds: exactly one concurrent caller per key runs ``build``, every other
caller shares its result (or its exception), and callers with DIFFERENT keys
never share.
"""

import threading
import time

import pytest

from backend.repositories.utils.single_flight import coalesce


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
    # ``build_started`` firing proves the registry write already happened
    # (coalesce() registers BEFORE calling build), so t2 is guaranteed to see
    # the leader's slot once it reaches coalesce() — this sleep only bounds
    # the (sub-millisecond) time for t2's thread to actually get scheduled
    # and make that call before we release the leader. Same pattern as
    # tests/core/test_duckdb_recycle_barrier.py's waiter-thread handshake.
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
    from backend.repositories.utils import single_flight as sf

    coalesce("cleanup-key", lambda: 1)
    assert "cleanup-key" not in sf._registry


def test_registry_entry_is_removed_even_on_failure():
    from backend.repositories.utils import single_flight as sf

    def boom():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        coalesce("cleanup-fail-key", boom)
    assert "cleanup-fail-key" not in sf._registry
