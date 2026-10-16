"""ResponseMemo scaffolding + golden response-cache keys (key bytes must stay stable across deploys)."""

from __future__ import annotations

import hashlib
import json

from backend.repositories.utils.response_cache import ResponseMemo


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
    assert hit["is_cached"] is False


def test_check_miss_returns_none():
    assert ResponseMemo().check("absent", _Runner(1)) is None


def test_put_strips_extra_keys():
    memo = ResponseMemo()
    memo.put("k", {"a": 1, "section_timings": [1]}, strip=("section_timings",))
    assert memo.get("k") == {"a": 1, "is_cached": True}


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

    key = _response_cache_key({"name": "svc"}, "2026-01-01T00:00:12Z", None, {}, 300, 30, {"b", "a"}, True)
    expected = _sha(
        {"s": "2026-01-01T00:00", "e": None, "f": {}, "bs": 300, "tn": 30, "sec": ["a", "b"], "mi": True},
        "svc",
    )
    assert key == expected
