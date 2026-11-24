from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from backend.core.metadata import base as pg_connection
from backend.models.common import BaseResponse, FilterSpec
from backend.repositories.utils import response_cache as rc
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import ResponseMemo, coalesce, invalidate_service_cache
from backend.repositories.utils.response_shaping import (
    CHART_INTERVAL_SECONDS,
    _safe_table,
    normalize_chart_interval,
)


@pytest.mark.parametrize("good", ["1 second", "1 minute", "1 hour", "1 day"])
def test_normalize_chart_interval_passes_through_known_values(good):
    assert normalize_chart_interval(good) == good


@pytest.mark.parametrize(
    ("val", "expected"),
    [
        ("'; DROP TABLE--", "1 minute"),
        ("2 weeks", "1 minute"),
        ("", "1 minute"),
    ],
)
def test_normalize_chart_interval_falls_back_for_unknown_values(val, expected):
    assert normalize_chart_interval(val) == expected


def test_chart_interval_seconds_contains_core_intervals():
    for k in ("1 second", "1 minute", "1 hour", "1 day"):
        assert k in CHART_INTERVAL_SECONDS


def test_safe_table_delegates_to_core_safe_table_name():
    assert _safe_table("my_service") == "logs_my_service"
    assert _safe_table("default") == "logs"


# ── From test_response_cache_helpers.py ──


def test_lookup_and_store_observations_are_request_local(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [10.0]
    monkeypatch.setattr("backend.utils.bounded_cache.time.monotonic", lambda: clock[0])
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    assert memo.get("private-key") is None
    original = {"rows": [1], "_response_cache": {"state": "miss"}, "is_cached": True}
    memo.put("private-key", original)
    hit = memo.get("private-key")
    assert hit["_response_cache"] == {"state": "hit", "ttl_seconds": 15.0, "prewarmer": False}
    assert hit["is_cached"] is True and "_response_cache" not in memo.cache["private-key"]
    hit["_response_cache"]["state"] = "bypass"
    assert memo.get("private-key")["_response_cache"]["state"] == "hit"
    clock[0] = 25.0
    assert memo.get("private-key") is None and original["_response_cache"] == {"state": "miss"}


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
    assert bypass["_response_cache"] == {"state": "bypass", "prewarmer": False} and bypass["is_cached"] is False


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
    assert a["_response_cache"]["state"] == "miss" and a["_response_cache"]["single_flight"] == "leader"
    assert b["_response_cache"]["state"] == "unknown" and b["_response_cache"]["single_flight"] == "follower"
    assert b["_response_cache"]["wait_ms"] >= 0 and "wait_ms" not in a["_response_cache"]
    assert "_response_cache" not in payload


def test_observation_survives_serialization_without_debug_privileges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    payload = rc.cache_result(
        {"debug_queries": [{"sql": "private", "time_ms": 1}]}, state="miss", ttl_seconds=15, prewarmer=False
    )
    wire = BaseResponse(**payload).model_dump(by_alias=True)
    assert wire["_response_cache"] == {"state": "miss", "ttl_seconds": 15.0, "prewarmer": False}
    assert "_debug_queries" not in wire and "private" not in str(wire)
    assert "_response_cache" not in BaseResponse().model_dump(by_alias=True)


def test_fresh_observation_replaces_both_alias_spellings() -> None:
    payload = {"response_cache": {"state": "hit"}, "_response_cache": {"state": "hit"}, "_is_cached": True}
    fresh = rc.cache_result(payload, state="miss", prewarmer=False)
    assert "response_cache" not in fresh and "_is_cached" not in fresh
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


class _CacheRunner:
    def __init__(self, n: int) -> None:
        self.n = n

    def telemetry(self) -> dict:
        return {"debug_queries": [f"q{self.n}"], "is_cached": False}


def _sha(payload: dict, svc: str) -> str:
    body = json.dumps(payload, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{body}:{svc}".encode()).hexdigest()


def test_put_and_get_with_telemetry():
    memo2 = ResponseMemo()
    memo2.put("k", {"a": 1, "section_timings": [1]}, strip=("section_timings",))
    expected_clean = {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }
    assert memo2.get("k") == expected_clean

    memo3 = ResponseMemo()
    memo3.put(
        "k",
        {
            "a": 1,
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "ClickHouse"}],
            "debug_calls": [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}],
        },
    )
    assert memo3.get("k") == expected_clean
    hit3 = memo3.get_with_telemetry("k")
    assert hit3 is not None and hit3["a"] == 1 and hit3["is_cached"] is True
    assert hit3["debug_queries"] == [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "ClickHouse", "is_cached": True}]
    assert hit3["debug_calls"] == [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}]


def test_digest_cache_key_and_invalidate_service_cache():
    payload = {"ep": "slow_urls", "s": "2026-01-01T00:00", "e": "2026-01-01T01:00", "f": {}, "limit": 3, "mi": False}
    assert rc.digest_cache_key(payload, {"name": "svc"}) == _sha(payload, "svc")

    memo = ResponseMemo(ttl=30, maxsize=4)
    memo.put("svc-key", {"rows": [1]})
    assert memo.get("svc-key") is not None
    invalidate_service_cache("svc")
    assert memo.get("svc-key") is None


def test_leader_runs_build_once_and_follower_shares_the_same_result():
    call_count = 0
    lock = threading.Lock()
    build_started, release = threading.Event(), threading.Event()

    def build():
        nonlocal call_count
        with lock:
            call_count += 1
        build_started.set()
        assert release.wait(timeout=5)
        return object()

    outcomes: dict[str, tuple] = {}
    t1 = threading.Thread(target=lambda: outcomes.__setitem__("leader", coalesce("same-key", build)))
    t1.start()
    assert build_started.wait(timeout=5)
    t2 = threading.Thread(target=lambda: outcomes.__setitem__("follower", coalesce("same-key", build)))
    t2.start()
    time.sleep(0.2)
    release.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert call_count == 1
    leader_result, leader_is_leader = outcomes["leader"]
    follower_result, follower_is_leader = outcomes["follower"]
    assert leader_is_leader is True and follower_is_leader is False and leader_result is follower_result


def test_different_keys_and_sequential_calls_rebuild():
    calls: list[str] = []
    lock = threading.Lock()

    def build_for(tag: str):
        def _build():
            with lock:
                calls.append(tag)
            return tag

        return _build

    outcomes: dict[str, tuple] = {}
    t1 = threading.Thread(target=lambda: outcomes.__setitem__("key-a", coalesce("key-a", build_for("key-a"))))
    t2 = threading.Thread(target=lambda: outcomes.__setitem__("key-b", coalesce("key-b", build_for("key-b"))))
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)
    assert sorted(calls) == ["key-a", "key-b"]
    assert outcomes["key-a"][0] == "key-a" and outcomes["key-b"][0] == "key-b"

    call_count = 0

    def seq_build():
        nonlocal call_count
        call_count += 1
        return call_count

    first, first_is_leader = coalesce("seq-key", seq_build)
    second, second_is_leader = coalesce("seq-key", seq_build)
    assert first_is_leader is True and second_is_leader is True and first == 1 and second == 2


def test_build_exception_propagates_to_the_follower_and_cleans_registry():
    build_started, release = threading.Event(), threading.Event()

    def build():
        build_started.set()
        assert release.wait(timeout=5)
        raise ValueError("boom")

    outcomes: dict[str, BaseException] = {}

    def run(role: str):
        try:
            coalesce("fail-key", build)
        except ValueError as e:
            outcomes[role] = e

    t1 = threading.Thread(target=run, args=("leader",))
    t1.start()
    assert build_started.wait(timeout=5)
    t2 = threading.Thread(target=run, args=("follower",))
    t2.start()
    time.sleep(0.2)
    release.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert isinstance(outcomes.get("leader"), ValueError) and isinstance(outcomes.get("follower"), ValueError)
    assert str(outcomes["leader"]) == "boom" and str(outcomes["follower"]) == "boom"

    coalesce("cleanup-key", lambda: 1)
    assert "cleanup-key" not in rc._registry

    def boom():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        coalesce("cleanup-fail-key", boom)
    assert "cleanup-fail-key" not in rc._registry


# ── Property-based & unit tests for backend.repositories.utils.filters.build_where_clause ──

_KNOWN_COLS = (
    "status",
    "country",
    "url",
    "ip",
    "method",
    "ua",
    "pop",
    "asn",
    "city",
    "region",
    "ottfb",
    "ottlb",
    "waf_sig",
    "edge",
)

_value_strategy = st.one_of(
    st.text(min_size=0, max_size=20),
    st.integers(min_value=-1000, max_value=1000),
    st.none(),
    st.sampled_from(["a*", "*b", "*foo*", "a*b*", "%lit", "''", '"', "\\", "a\nb"]),
)

_filter_spec_strategy_filters_properties = st.builds(
    FilterSpec,
    mode=st.sampled_from(["include", "exclude"]),
    values=st.lists(_value_strategy, min_size=0, max_size=4),
)


def _filter_key_strategy() -> st.SearchStrategy[str]:
    base = st.sampled_from(_KNOWN_COLS)
    return st.one_of(
        base,
        base.map(lambda c: f"filter_{c}"),
        base.map(lambda c: f"xfilter_{c}"),
        base.map(lambda c: f"{c}_2"),
        base.map(lambda c: f"filter_{c}_3"),
    )


_filters_strategy_filters_properties = st.dictionaries(
    keys=_filter_key_strategy(),
    values=_filter_spec_strategy_filters_properties,
    min_size=0,
    max_size=4,
)

_iso_time_strategy_filters_properties = st.one_of(
    st.none(),
    st.sampled_from(
        [
            "2026-05-15T00:00:00Z",
            "2026-05-15T12:30:45+00:00",
            "2026-05-15",
            "2026-01-01T00:00:00.000Z",
        ]
    ),
)


@given(
    filters=_filters_strategy_filters_properties,
    start=_iso_time_strategy_filters_properties,
    end=_iso_time_strategy_filters_properties,
)
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_param_count_matches_placeholder_count(filters, start, end):
    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    assert sql.count("?") == len(params), (
        f"Placeholder/param mismatch:\n  sql={sql!r}\n  params={params!r}\n  filters={filters!r}\n  start={start!r} end={end!r}"
    )


@given(
    filters=_filters_strategy_filters_properties,
    start=_iso_time_strategy_filters_properties,
    end=_iso_time_strategy_filters_properties,
)
@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_inlined_sql_has_zero_params(filters, start, end):
    params, sql = build_where_clause(
        start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"], inline_params=True
    )
    assert params == [], f"inline_params=True should yield zero params, got {params!r}"
    assert isinstance(sql, str) and len(sql) > 0


@given(actual_cols=st.one_of(st.none(), st.lists(st.sampled_from(_KNOWN_COLS), min_size=0, max_size=8)))
def test_no_args_returns_passthrough(actual_cols):
    params, sql = build_where_clause(None, None, {}, actual_cols=actual_cols, exclude_invalid_ips=False)
    assert sql == "1=1" and params == []


@given(col=st.sampled_from(_KNOWN_COLS), mode=st.sampled_from(["include", "exclude"]))
def test_empty_values_list_is_dropped(col, mode):
    filters = {col: FilterSpec(mode=mode, values=[])}
    params, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), exclude_invalid_ips=False)
    assert sql == "1=1" and params == []


def test_inline_strips_null_byte_and_control_chars():
    filters = {"status": FilterSpec(mode="include", values=["\x00bad\x01value"])}
    _, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), inline_params=True)
    assert "\x00" not in sql and "\x01" not in sql


@pytest.mark.parametrize(
    ("fake_bot", "mode", "values", "cols", "expected_in", "expected_not_in", "exact_sql"),
    [
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1", r"compatible; Googlebot"]}},
            "include",
            ["googlebot"],
            ["ua"],
            ["regexp_matches", "(?i)", "Googlebot"],
            [],
            None,
        ),
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1"]}},
            "exclude",
            ["googlebot"],
            ["ua"],
            ["NOT regexp_matches"],
            [],
            None,
        ),
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot"]}},
            "include",
            ["googlebot"],
            ["status"],
            [],
            ["regexp_matches"],
            "1=1",
        ),
        (
            {"id": "empty", "pattern": {"accepted": []}},
            "include",
            ["empty"],
            ["ua"],
            [],
            ["regexp_matches"],
            None,
        ),
        (
            {"id": "evil", "pattern": {"accepted": [r"foo'; DROP TABLE--"]}},
            "include",
            ["evil"],
            ["ua"],
            ["foo''"],
            [],
            None,
        ),
    ],
)
def test_bot_name_filter_variants(fake_bot, mode, values, cols, expected_in, expected_not_in, exact_sql):
    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode=mode, values=values)},
            actual_cols=cols,
        )
    for token in expected_in:
        assert token in sql
    for token in expected_not_in:
        assert token not in sql
    if exact_sql is not None:
        assert sql == exact_sql


def test_bot_name_filter_skips_unknown_bot_ids():
    def _lookup(bot_id):
        if bot_id == "known":
            return {"id": "known", "pattern": {"accepted": [r"KnownBot"]}}
        return None

    with patch("backend.utils.bot_sources.get_bot_by_id", side_effect=_lookup):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["known", "unknown"])},
            actual_cols=["ua"],
        )

    assert "KnownBot" in sql


def _seed_ngwaf_bots(entries: list[tuple[str, str]], category: str | None = None) -> None:
    with pg_connection.get_con("svc") as con:
        for waf_req_id, bot_name in entries:
            con.execute(
                """
                INSERT INTO ngwaf_bots (waf_req_id, bot_name, category)
                VALUES (?, ?, ?)
                ON CONFLICT (waf_req_id) DO UPDATE SET bot_name = EXCLUDED.bot_name, category = EXCLUDED.category
                """,
                (waf_req_id, bot_name, category),
            )
        con.commit()


def test_ngwaf_bot_name_filter_skipped_when_waf_req_id_absent_or_cache_empty():
    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["status"],
    )
    assert "sqlite_scan" not in sql and sql == "1=1"

    _, sql_missing = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )
    assert "sqlite_scan" not in sql_missing


@pytest.mark.parametrize(
    ("entries", "mode", "expected_in", "exact_sql"),
    [
        ([("w1", "GoogleBot"), ("w2", "GoogleBot")], "include", "waf_req_id IN", None),
        ([("w1", "GoogleBot")], "exclude", "waf_req_id NOT IN", None),
        ([("w1", "SomeOtherBot")], "include", None, "(FALSE)"),
        ([("w1", "SomeOtherBot")], "exclude", None, "1=1"),
    ],
)
def test_ngwaf_bot_name_filter_postgres_resolution(entries, mode, expected_in, exact_sql):
    _seed_ngwaf_bots(entries)

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode=mode, values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )
    assert "sqlite_scan" not in sql and "ngwaf_bots" not in sql
    if expected_in is not None:
        assert expected_in in sql
    if exact_sql is not None:
        assert sql == exact_sql
