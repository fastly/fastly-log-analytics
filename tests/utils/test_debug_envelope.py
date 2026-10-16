"""``debug_envelope`` keeps both pre-existing gating semantics (RUM ungated, scoring gated)."""

from __future__ import annotations

from backend.utils.telemetry import debug_envelope


def test_ungated_always_injects_envelope_in_place(monkeypatch):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    res = {"ok": True}
    out = debug_envelope(res, gated=False)
    assert out is res
    assert set(res) == {"ok", "_debug_queries", "_debug_sqlite", "_debug_calls", "_is_cached"}
    assert res["_is_cached"] is False


def test_gated_strips_debug_keys_when_debug_responses_off(monkeypatch):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    src = {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_debug_sqlite": [3]}
    out = debug_envelope(src, gated=True, is_cached=True)
    assert out == {"a": 1, "_debug_sqlite": [3], "_is_cached": True}
    assert "_is_cached" not in src


def test_gated_keeps_debug_keys_when_debug_responses_on(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    out = debug_envelope({"a": 1, "_debug_queries": [1], "_debug_calls": [2]}, gated=True)
    assert out == {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_is_cached": False}


def test_gated_never_injects_collectors(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    assert debug_envelope({"a": 1}, gated=True) == {"a": 1, "_is_cached": False}
