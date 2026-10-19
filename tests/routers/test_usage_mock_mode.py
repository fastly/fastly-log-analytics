"""Usage endpoints must work in FASTLY_MOCK_MODE (the Playwright backend has no Fastly key)."""

from __future__ import annotations

from backend.routers import usage


def test_api_key_falls_back_to_placeholder_in_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage._api_key("svc") == "mock-api-key"


def test_api_key_stays_empty_outside_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert usage._api_key("svc") == ""


def test_api_key_prefers_the_configured_key(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "real-key")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage._api_key("svc") == "real-key"
