"""Campaign E security hardening regression tests.

Covers:
- SQLI-01 (Critical): ClickHouse SQL injection & cross-tenant exfiltration in
  backend/routers/session_scoring/scoring_analytics.py (_fetch_session_events,
  _threshold_preview_counts, scoring_session_events, scoring_labels_create).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.routers.session_scoring import scoring_analytics as sa_mod

pytestmark = pytest.mark.no_postgres


def test_sqli01_fetch_session_events_uses_typed_clickhouse_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, str, Any]] = []

    def _fake_query_logs(service_id: str, sql: str, params: dict | tuple = ()) -> list[dict]:
        captured.append((service_id, sql, params))
        return []

    monkeypatch.setattr(sa_mod, "_table_columns", lambda _sid: set())
    monkeypatch.setattr(sa_mod, "query_logs", _fake_query_logs)

    payload_sid = "1) UNION ALL SELECT * FROM system.tables --"
    sa_mod._fetch_session_events("svc-a", ["deadbeef1234", payload_sid])

    assert len(captured) == 1
    service_id, sql, params = captured[0]
    assert service_id == "svc-a"
    assert isinstance(params, dict)
    assert params == {"sid_0": "deadbeef1234", "sid_1": payload_sid}
    assert "{sid_0:String}" in sql
    assert "{sid_1:String}" in sql
    assert payload_sid not in sql
    assert "deadbeef1234" not in sql


def test_sqli01_threshold_preview_counts_uses_typed_clickhouse_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, str, Any]] = []

    def _fake_query_logs(service_id: str, sql: str, params: dict | tuple = ()) -> list[dict]:
        captured.append((service_id, sql, params))
        return [
            {"total": 2, "flagged_total": 1, "flagged_good": 0, "flagged_bad": 1, "passed_good": 1, "passed_bad": 0}
        ]

    monkeypatch.setattr(sa_mod, "query_logs", _fake_query_logs)

    evil_good = "x) OR 1=1 --"
    evil_bad = "y' UNION SELECT 1 --"
    sa_mod._threshold_preview_counts(
        "svc-a",
        "request_facts",
        "timestamp >= now() - INTERVAL 24 HOUR",
        60,
        [(evil_good, "good"), (evil_bad, "bad")],
    )

    assert len(captured) == 1
    service_id, sql, params = captured[0]
    assert service_id == "svc-a"
    assert isinstance(params, dict)
    assert params == {"good_sid_0": evil_good, "bad_sid_1": evil_bad}
    assert "{good_sid_0:String}" in sql
    assert "{bad_sid_1:String}" in sql
    assert evil_good not in sql
    assert evil_bad not in sql


def test_sqli01_scoring_routes_reject_malformed_sid_before_querying(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def _boom(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("query_logs must not be invoked for malformed sid")

    monkeypatch.setattr(sa_mod, "query_logs", _boom)
    monkeypatch.setattr(sa_mod, "_enforce_scoring_service_scope", lambda *_a, **_kw: None)

    app = FastAPI()

    @app.middleware("http")
    async def _inject_analyst(request: Request, call_next: Any):
        request.state.analyst_session = SimpleNamespace(session_id="sess-e", service_ids=["svc-a"], mask_ips=False)
        request.state.is_remote = True
        return await call_next(request)

    app.include_router(sa_mod.router, prefix="/api/services")
    client = TestClient(app)

    for bad_sid in (
        "1) UNION SELECT 1",
        "abc' OR '1'='1",
        "deadbeef;DROP",
        "a" * 65,
    ):
        resp = client.get(f"/api/services/svc-a/scoring/sessions/{quote(bad_sid, safe='')}/events")
        assert resp.status_code == 400, (bad_sid, resp.status_code, resp.text)
        assert resp.json()["detail"]["error"] == "invalid_sid"

        label_resp = client.post(
            "/api/services/svc-a/scoring/labels",
            json={"sid": bad_sid, "label": "bad", "notes": "test"},
        )
        assert label_resp.status_code == 400, (bad_sid, label_resp.status_code, label_resp.text)
        assert label_resp.json()["detail"]["error"] == "invalid_label"

    assert called is False
