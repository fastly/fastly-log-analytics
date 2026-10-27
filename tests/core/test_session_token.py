"""Unit tests for opaque AES-GCM session tokens."""

from __future__ import annotations

import threading

import pytest
from backend.core.session_token import SessionTokenError, open_session_token, seal_session_token

from backend.core import session_token as _st_mod


def _run_barrier_threads(n: int, fn) -> None:
    barrier = threading.Barrier(n)

    def _worker(idx: int) -> None:
        barrier.wait()
        fn(idx)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


@pytest.fixture
def fixed_key(monkeypatch):
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "unit-test-secret-A")
    _st_mod._key = None
    yield
    _st_mod._key = None


def _seal(ip: str = "203.0.113.7", ja4: str | None = "ja4abc", svc: str = "svcA") -> str:
    return seal_session_token(
        ip,
        ja4,
        "2026-06-29T00:00:00+00:00",
        "2026-06-29T00:30:00+00:00",
        service_id=svc,
    )


def test_session_token_round_trip_and_opacity_and_rejections(fixed_key, monkeypatch):
    assert open_session_token(_seal(), service_id="svcA") == (
        "203.0.113.7",
        "ja4abc",
        "2026-06-29T00:00:00+00:00",
        "2026-06-29T00:30:00+00:00",
    )
    ip, ja4, _, _ = open_session_token(_seal(ja4=None), service_id="svcA")
    assert ip == "203.0.113.7"
    assert ja4 is None

    tok = _seal(ip="198.51.100.23")
    assert "198.51.100.23" not in tok
    assert ".23" not in tok

    token = _seal()
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    invalid_cases = (
        (tampered, "svcA"),
        (token, "svcB"),
        ("not-a-valid-token!!!", "svcA"),
        ("", "svcA"),
    )
    for bad_tok, svc in invalid_cases:
        with pytest.raises(SessionTokenError):
            open_session_token(bad_tok, service_id=svc)

    monkeypatch.setenv("SESSION_TOKEN_SECRET", "unit-test-secret-B")
    _st_mod._key = None
    with pytest.raises(SessionTokenError):
        open_session_token(token, service_id="svcA")


def test_concurrent_first_resolve_yields_single_key(monkeypatch):
    monkeypatch.delenv("SESSION_TOKEN_SECRET", raising=False)
    _st_mod._key = None
    tokens: list[str] = []
    tokens_lock = threading.Lock()

    def _seal_and_record(_idx: int) -> None:
        tok = _seal()
        with tokens_lock:
            tokens.append(tok)

    _run_barrier_threads(16, _seal_and_record)
    assert len(tokens) == 16
    assert all(open_session_token(t, service_id="svcA")[0] == "203.0.113.7" for t in tokens)
    _st_mod._key = None
