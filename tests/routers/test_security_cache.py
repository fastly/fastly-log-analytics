"""Focused tests for Security router HTTP cache headers and GET /bundle sections."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core.request_context import build_request_context
from backend.routers import security

pytestmark = pytest.mark.no_postgres


def _make_client() -> TestClient:
    app = FastAPI()
    app.include_router(security.router)

    fake_ctx = SimpleNamespace(
        analyst_session=None,
        resolve_window=lambda _req: ("2026-11-24T10:00:00Z", "2026-11-24T11:00:00Z"),
        clamp=lambda start, end: (start or "2026-11-24T10:00:00Z", end or "2026-11-24T11:00:00Z"),
    )
    app.dependency_overrides[build_request_context] = lambda: fake_ctx
    return TestClient(app)


def test_security_bundle_get_forwards_sections_and_sets_cache_control(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_aggregates(
        hs_service: Any, req: Any, start: str, end: str, *, sections: Any, mask_ips: bool
    ) -> dict[str, Any]:
        captured["req_sections"] = req.sections
        captured["expanded_sections"] = sections
        captured["bucket_seconds"] = req.bucket_seconds
        return {}

    monkeypatch.setattr(security, "require_high_scale", lambda _ctx: SimpleNamespace(service_id="svc-a"))
    monkeypatch.setattr(security, "hs_aggregates", fake_aggregates)

    client = _make_client()
    resp = client.get(
        "/api/security/bundle",
        params=[
            ("bucket_seconds", "60"),
            ("sections", "wellknown_bots"),
            ("sections", "tls_fingerprints"),
        ],
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers.get("cache-control") == security._SECURITY_CACHE_CONTROL
    assert captured["bucket_seconds"] == 60
    assert captured["req_sections"] == ["wellknown_bots", "tls_fingerprints"]
    # tls_fingerprints implies fingerprint_coverage via _expand_security_sections
    assert captured["expanded_sections"] == frozenset({"wellknown_bots", "tls_fingerprints", "fingerprint_coverage"})


def test_security_threat_intel_sets_cache_control(monkeypatch):
    monkeypatch.setattr(security, "require_high_scale", lambda _ctx: SimpleNamespace(service_id="svc-a"))
    monkeypatch.setattr(security, "hs_threat_intel", lambda _svc, _s, _e: {"data": []})

    client = _make_client()
    resp = client.get("/api/security/threat-intel?start_time=2026-11-24T10:00:00Z&end_time=2026-11-24T11:00:00Z")
    assert resp.status_code == 200, resp.text
    assert resp.headers.get("cache-control") == security._SECURITY_CACHE_CONTROL
