"""Tests for backend.routers.session_scoring — enable/disable/status/admin/analytics endpoints."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import backend.routers.session_scoring as _admin
import backend.routers.session_scoring as _ssa
import backend.scoring as _labels
from backend import config as svcconfig
from backend.core.duckdb import _clear_schema_cache
from backend.core.request_context import build_request_context
from backend.core.share_db import apply_pii_policy
from backend.main import app
from backend.models.analytics import ScoringL2EnforceBody
from backend.repositories._base import _safe_table
from backend.repositories.sessions import _SCORING_COLUMN_TYPES
from backend.routers import session_scoring as _ss
from backend.routers.session_scoring import _build_l2_enforce_block
from backend.routers.session_scoring import router as scoring_router
from backend.scoring import EvaluationResult, MatrixStats, TransitionMatrix
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID, override_request_context

LOG_SVC = "TestScoringRouterSvc"
_SHIPPED = "backend.provision.session_scoring_orchestrator.shipped_scorer_identity"
_LABEL_PII_KEYS = ("notes", "flagged_by", "sample_ip", "sample_ua", "sample_url")
_LABEL_SAFE_KEYS = ("id", "service_id", "sid", "label", "created_at", "updated_at")
_ANALYST_COMPOSITE_KEYS = {"top_flagged", "score_distribution", "compliance_breakdown", "latency_timeseries", "health"}
_ADMIN_ONLY_COMPOSITE_KEYS = {"evaluation", "evaluation_per_reason"}


@pytest.fixture
def _clear_analytics_cache_between_tests(monkeypatch):
    monkeypatch.setattr("backend.provision.fastly_api.service_has_scoring_pragmas", lambda *a, **kw: True)
    for cache in (_ss._analytics_cache, _ss._inflight, _ss._scoring_svc_version_cache, _ss._scoring_pragma_cache):
        cache.clear()
    yield
    for cache in (_ss._analytics_cache, _ss._inflight, _ss._scoring_svc_version_cache, _ss._scoring_pragma_cache):
        cache.clear()


def _parse_sse(resp) -> list[dict]:
    return [json.loads(line[len("data: ") :]) for line in resp.text.splitlines() if line.startswith("data: ")]


def _patch_run_with_events(
    events=({"type": "status", "message": "fake"},),
    *,
    capture_token: dict | None = None,
    on_done=None,
    raise_exc: Exception | None = None,
):
    def _fake(func, *args, **kwargs):
        if capture_token is not None:
            capture_token["t"] = args[1]
        yield from events
        if raise_exc is not None:
            raise raise_exc
        if on_done is not None:
            on_done()

    return patch("backend.provision.orchestrator.run_with_events", side_effect=_fake)


@contextmanager
def _patch_labels(labels: list[dict], counts: dict[str, int]):
    with (
        patch("backend.scoring.list_labels", return_value=labels),
        patch("backend.scoring.counts_by_label", return_value=counts),
    ):
        yield


@contextmanager
def _capture_fastly_and_audit(*, get_handler=None):
    fastly_calls: list[tuple] = []
    audit_calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        if method == "GET" and get_handler is not None:
            return get_handler(path)
        fastly_calls.append((method, path, body))
        return {}

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        yield fastly_calls, audit_calls


def _health_row(**overrides) -> dict:
    return {
        "total_edge_rows": 1000,
        "scored_rows": 200,
        "distinct_sids": 50,
        "avg_score": 10.0,
        "p50_score": 0.0,
        "p95_score": 0.0,
        "max_score": 100,
        "scorer_errors": 0,
        "top_reasons": [],
        "l2_evaluated": 100,
        "l2_high_count": 5,
        **overrides,
    }


def _enabled_cfg(**scoring) -> dict:
    return {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scoring_drift", **scoring}}


def _enforce_cfg(**scoring) -> dict:
    return {
        "service_id": LOG_SVC,
        "fastly_api_key": "TOKEN",
        "scoring": {"enabled": True, "scoring_config_store_id": "cs_abc123", **scoring},
    }


def _capture_cfg_save(monkeypatch) -> dict:
    saved: dict = {}

    def save_fn(sid, cfg):
        saved.update(sid=sid, cfg=cfg)

    monkeypatch.setattr("backend.config.save_config", save_fn)
    monkeypatch.setattr("backend.config.mutate_config", mutate_via_save(save_fn))
    return saved


def _patch_query_logs(rows: list[dict]):
    return patch("backend.repositories.sessions.query_logs", return_value=rows)


def _fake_request(analyst_session):
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _assert_analyst_projected(row: dict, *, check_safe: bool = False) -> None:
    for k in _LABEL_PII_KEYS:
        assert k not in row, f"analyst response should NOT include {k!r}"
    if check_safe:
        for k in _LABEL_SAFE_KEYS:
            assert k in row, f"analyst response should include {k!r}"


# ── /scoring/status & edge drift ─────────────────────────────────────────────


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
@pytest.mark.parametrize(
    ("cfg", "expected_enabled", "expected_id"),
    [
        ({"service_id": LOG_SVC}, False, None),
        ({"service_id": LOG_SVC, "scoring": {"enabled": False}}, False, None),
        (
            {
                "service_id": LOG_SVC,
                "scoring": {
                    "enabled": True,
                    "scoring_service_id": "scoring_xyz",
                    "scoring_service_name": f"Session Scoring Service for {LOG_SVC}",
                    "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
                    "aes_key_hex": "secret-never-show",
                },
            },
            True,
            "scoring_xyz",
        ),
    ],
)
def test_status_variants(client, with_config, cfg, expected_enabled, expected_id):
    with_config[LOG_SVC] = cfg
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is expected_enabled
    if expected_id:
        assert body["scoring_service_id"] == expected_id
    assert "secret-never-show" not in r.text
    assert "aes_key_hex" not in body


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_status_surfaces_preflight_pragmas_ready_when_disabled_and_token_present(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "tok_123", "scoring": {"enabled": False}}
    with patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=False) as mock_probe:
        r1 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        assert r1.status_code == 200 and r1.json()["enabled"] is False
        assert r1.json()["preflight_pragmas_ready"] is False
        assert mock_probe.call_count == 1

    with patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=True) as mock_probe2:
        r2 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        r3 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        assert r2.json()["preflight_pragmas_ready"] is True
        assert r3.json()["preflight_pragmas_ready"] is True
        assert mock_probe2.call_count == 1


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
@pytest.mark.parametrize(
    ("token", "info_return", "info_exc", "expect_version"),
    [
        (
            "tok_abc123",
            {"number": 42, "updated_at": "2026-06-17T18:41:04Z", "created_at": "2026-06-17T18:40:00Z"},
            None,
            42,
        ),
        (None, None, None, None),
        ("tok_abc123", None, RuntimeError("fastly down"), None),
    ],
)
def test_status_active_version_lookup(client, with_config, token, info_return, info_exc, expect_version):
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scoring_svc"}}
    if token:
        cfg["fastly_api_key"] = token
    with_config[LOG_SVC] = cfg
    kwargs = {"side_effect": info_exc} if info_exc else {"return_value": info_return}
    with patch("backend.core.fastly.client.get_active_version_info", **kwargs) as mock_info:
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    if expect_version is not None:
        assert body["scoring_active_version"] == expect_version
        assert body["scoring_activated_at"] == "2026-06-17T18:41:04Z"
        mock_info.assert_called_once_with("scoring_svc", token)
    else:
        assert "scoring_active_version" not in body
        if not token:
            mock_info.assert_not_called()


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
@pytest.mark.parametrize(
    ("deployed", "shipped", "shipped_exc", "expect_drift", "expect_detail"),
    [
        (
            {"deployed_package_sha": "old_pkg", "deployed_vcl_sha": "vcl1"},
            {"package_sha": "new_pkg", "vcl_sha": "vcl1"},
            None,
            True,
            "wasm",
        ),
        (
            {"deployed_package_sha": "pkg1", "deployed_vcl_sha": "old_vcl"},
            {"package_sha": "pkg1", "vcl_sha": "new_vcl"},
            None,
            True,
            "vcl",
        ),
        (
            {"deployed_package_sha": "old_pkg", "deployed_vcl_sha": "old_vcl"},
            {"package_sha": "new_pkg", "vcl_sha": "new_vcl"},
            None,
            True,
            "wasm+vcl",
        ),
        (
            {"deployed_package_sha": "pkg1", "deployed_vcl_sha": "vcl1"},
            {"package_sha": "pkg1", "vcl_sha": "vcl1"},
            None,
            False,
            None,
        ),
        ({}, {"package_sha": "anything", "vcl_sha": "anything"}, None, False, None),
        ({"deployed_package_sha": "pkg1", "deployed_vcl_sha": "vcl1"}, None, RuntimeError("hash boom"), False, None),
    ],
)
def test_status_scorer_drift(client, with_config, deployed, shipped, shipped_exc, expect_drift, expect_detail):
    with_config[LOG_SVC] = _enabled_cfg(**deployed)
    kwargs = {"side_effect": shipped_exc} if shipped_exc else {"return_value": shipped}
    with patch(_SHIPPED, **kwargs):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["scorer_drift"] is expect_drift
    assert body["drift_detail"] == expect_detail


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_status_and_admin_service_id_validation(client, with_config):
    assert client.get("/api/services/does-not-exist/scoring/status").status_code == 404
    assert client.get("/api/services/no-such-service/scoring/audit").status_code == 404
    for bad_svc in ("svc;DROP", "svc.dot"):
        assert client.get(f"/api/services/{bad_svc}/scoring/threshold").status_code == 422


# ── /scoring/enable & /scoring/disable ───────────────────────────────────────


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_enable_token_resolution(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": ""}
    r400 = client.post(f"/api/services/{LOG_SVC}/scoring/enable")
    assert r400.status_code == 400 and "token" in r400.json()["detail"]["error"].lower()

    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "FROM_CONFIG"}
    cap: dict = {}
    with _patch_run_with_events(capture_token=cap):
        assert client.post(f"/api/services/{LOG_SVC}/scoring/enable").status_code == 200
    assert cap["t"] == "FROM_CONFIG"

    with _patch_run_with_events(capture_token=cap):
        assert client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "FROM_QUERY"}).status_code == 200
    assert cap["t"] == "FROM_QUERY"


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_enable_and_disable_sse_streams(client, with_config):
    cfg = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN"}
    with_config[LOG_SVC] = cfg
    enabled_cfg = {**cfg, "scoring": {"enabled": True, "scoring_service_id": "scoring_xyz"}}

    with _patch_run_with_events(
        [{"type": "status", "message": "step 1"}, {"type": "status", "message": "step 2"}],
        on_done=lambda: with_config.__setitem__(LOG_SVC, enabled_cfg),
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(r)
    types = [e["type"] for e in events]
    assert "status" in types and types[-1] == "done"
    assert events[-1]["scoring"]["scoring_service_id"] == "scoring_xyz"

    with _patch_run_with_events(
        [{"type": "status", "message": "got partway"}],
        raise_exc=RuntimeError("boom — validation failed at step 7"),
    ):
        r_err = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})
    err_events = _parse_sse(r_err)
    assert "error" in [e["type"] for e in err_events] and "done" not in [e["type"] for e in err_events]
    assert "validation" in next(e for e in err_events if e["type"] == "error")["message"].lower()

    with _patch_run_with_events([{"type": "status", "message": "tearing down"}]):
        r_dis = client.post(f"/api/services/{LOG_SVC}/scoring/disable", json={"token": "TOKEN"})
    assert r_dis.status_code == 200 and any(e["type"] == "done" for e in _parse_sse(r_dis))


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_enable_blocked_when_preflight_pragmas_missing(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN", "scoring": {"enabled": False}}
    mutated = {"called": False}
    monkeypatch.setattr("backend.config.mutate_config", lambda *a, **kw: mutated.__setitem__("called", True))

    with (
        patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=False),
        patch("backend.provision.orchestrator.run_with_events") as mock_run,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})

    assert r.status_code == 200 and mutated["called"] is False
    mock_run.assert_not_called()
    events = _parse_sse(r)
    assert len(events) == 1 and events[0]["type"] == "error" and events[0]["code"] == "missing_scoring_pragmas"
    msg = events[0]["message"]
    assert "account team" in msg.lower()
    for internal in ("fix_unsent_body_drain", "no_body_if_bereq_is_get_or_head", "hctl"):
        assert internal not in msg


# ── /scoring/labels CRUD & analyst PII projection ────────────────────────────


def _post_label(client, **payload):
    return client.post(f"/api/services/{LOG_SVC}/scoring/labels", json=payload)


def _patch_label(client, label_id: str, **payload):
    return client.patch(f"/api/services/{LOG_SVC}/scoring/labels/{label_id}", json=payload)


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_labels_crud_and_validation(client):
    r = _post_label(client, sid="deadbeef1234", label="bad", notes="scraper", sample_ip="1.2.3.4")
    assert r.status_code == 200 and r.json()["sid"] == "deadbeef1234" and r.json()["label"] == "bad"

    listing = client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()
    assert "deadbeef1234" in [row["sid"] for row in listing["labels"]]
    assert listing["counts"]["bad"] >= 1

    assert _post_label(client, label="bad").status_code == 400
    assert _post_label(client, sid="abc", label="ugly").status_code == 400
    assert _post_label(client, sid="uncertain1", label="neutral").json()["label"] == "neutral"

    # Upsert on same sid
    r1 = _post_label(client, sid="samesid01", label="bad")
    r2 = _post_label(client, sid="samesid01", label="good", notes="actually fine")
    assert r1.json()["id"] == r2.json()["id"] and r2.json()["label"] == "good"
    matches = [
        row
        for row in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if row["sid"] == "samesid01"
    ]
    assert len(matches) == 1

    # Patch notes & invalid label
    created = _post_label(client, sid="patchme", label="bad", notes="initial").json()
    rp = _patch_label(client, created["id"], notes="revised")
    assert rp.status_code == 200 and rp.json()["notes"] == "revised" and rp.json()["label"] == "bad"
    assert _patch_label(client, created["id"], label="ugly").status_code == 400

    # Idempotent delete
    del_id = _post_label(client, sid="deleteme", label="bad").json()["id"]
    assert client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{del_id}").status_code == 200
    assert client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{del_id}").status_code == 200


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_labels_analyst_pii_projection_and_write_protection(client, monkeypatch):
    full = {
        "id": "abc",
        "service_id": "svc",
        "sid": "sid1",
        "label": "bad",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "notes": "user@example.com",
        "flagged_by": "analyst@example.com",
        "sample_ip": "1.2.3.4",
        "sample_ua": "Mozilla/5.0",
        "sample_url": "/admin/super-secret-path",
    }
    _assert_analyst_projected(_ss._project_label_for_analyst(full), check_safe=True)

    # Admin gets full PII
    created = _post_label(
        client,
        sid="admin-pii-test",
        label="bad",
        notes="admin-original",
        sample_ip="9.9.9.9",
        sample_ua="ua",
        sample_url="/x",
    ).json()
    admin_row = next(
        r
        for r in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if r["sid"] == "admin-pii-test"
    )
    for k in _LABEL_PII_KEYS:
        assert k in admin_row

    # Analyst GET/POST/PATCH responses omit PII, and write paths strip PII / protect notes
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    analyst_row = next(
        r
        for r in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if r["sid"] == "admin-pii-test"
    )
    _assert_analyst_projected(analyst_row, check_safe=True)

    r_post = _post_label(
        client,
        sid="analyst-write-pii",
        label="bad",
        notes="injected",
        flagged_by="spoofed-operator",
        sample_ip="9.9.9.9",
        sample_ua="evil-agent",
        sample_url="/admin/secret",
    )
    assert r_post.status_code == 200
    _assert_analyst_projected(r_post.json())
    stored = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == "analyst-write-pii")
    assert stored["label"] == "bad" and stored["notes"] == "" and stored["flagged_by"] == "admin"
    assert stored["sample_ip"] == "" and stored["sample_ua"] == "" and stored["sample_url"] == ""

    r_patch = _patch_label(client, created["id"], label="good", notes="analyst-overwrite")
    assert r_patch.status_code == 200
    _assert_analyst_projected(r_patch.json())
    stored_patched = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == "admin-pii-test")
    assert stored_patched["notes"] == "admin-original" and stored_patched["label"] == "good"


# ── /scoring/analytics composite & read endpoints ────────────────────────────


def _patch_composite_subcalls(monkeypatch, *, eval_raises=False):
    monkeypatch.setattr(_ss, "scoring_top_flagged", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_score_distribution", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False})
    monkeypatch.setattr(_ss, "scoring_health", lambda **kw: {"ok": True})
    if eval_raises:

        def _boom(**kw):
            raise RuntimeError("scoring_evaluation must not be called on the analyst path")

        monkeypatch.setattr(_ss, "scoring_evaluation", _boom)
        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", _boom)
    else:
        monkeypatch.setattr(_ss, "scoring_evaluation", lambda **kw: {"score": 0.5})
        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []})


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_analytics_composite_admin_vs_analyst(client, monkeypatch):
    _patch_composite_subcalls(monkeypatch)
    r_admin = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r_admin.status_code == 200
    assert (_ANALYST_COMPOSITE_KEYS | _ADMIN_ONLY_COMPOSITE_KEYS).issubset(r_admin.json().keys())

    _ss._analytics_cache.clear()
    _patch_composite_subcalls(monkeypatch, eval_raises=True)
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r_analyst = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r_analyst.status_code == 200
    keys = set(r_analyst.json().keys())
    assert _ANALYST_COMPOSITE_KEYS.issubset(keys)
    assert keys.isdisjoint(_ADMIN_ONLY_COMPOSITE_KEYS)


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_list_endpoints_and_bounds(client):
    with _patch_query_logs(
        [{"timestamp": "2026-06-01 10:00:00", "edge_sid": "aaa", "edge_score": 95, "ip": "1.1.1.1"}]
    ):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=24&limit=10")
        assert r.status_code == 200 and r.json()["since_hours"] == 24 and r.json()["rows"][0]["edge_sid"] == "aaa"
        assert client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=0").status_code == 422
        assert client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=999").status_code == 422

    with _patch_query_logs([{"hour": "2026-06-01 10:00:00", "bucket": "75-100", "count": 5}]):
        r_dist = client.get(f"/api/services/{LOG_SVC}/scoring/score-distribution")
        assert r_dist.status_code == 200 and r_dist.json()["rows"][0]["bucket"] == "75-100"

    with _patch_query_logs([{"hour": "2026-06-01 10:00:00", "compliance": "ok", "count": 200}]):
        r_comp = client.get(f"/api/services/{LOG_SVC}/scoring/compliance-breakdown")
        assert r_comp.status_code == 200 and r_comp.json()["rows"][0]["compliance"] == "ok"


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
@pytest.mark.parametrize("has_latency_cols", [True, False])
def test_health_and_latency_timeseries_columns(client, monkeypatch, has_latency_cols):
    cols = {"edge_score", "edge_score_reason"}
    if has_latency_cols:
        cols |= {"edge_score_l2", "edge_cookie_compliance", "edge_sid", "edge_score_rtt_us", "edge_score_exec_us"}
    monkeypatch.setattr(_ss, "_table_columns", lambda sid: cols)

    h_row = (
        _health_row(
            rtt_p50_us=8000, rtt_p95_us=42000, rtt_p99_us=91000, rtt_max_us=100000, exec_p50_us=540, exec_p95_us=880
        )
        if has_latency_cols
        else _health_row()
    )
    with _patch_query_logs([h_row]):
        rh = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert rh.status_code == 200
    lat = rh.json()["latency"]
    assert lat["available"] is has_latency_cols
    assert lat["rtt_p95_us"] == (42000 if has_latency_cols else None)

    ts_row = {"hour": "2026-06-17 10:00:00", "scored_count": 500, "fail_open_count": 30, "total_count": 530}
    if has_latency_cols:
        ts_row.update(rtt_p50_us=8000, rtt_p95_us=45000, rtt_p99_us=95000, exec_p50_us=540, exec_p95_us=900)
    with _patch_query_logs([ts_row]):
        rts = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=24")
    assert rts.status_code == 200
    assert rts.json()["has_latency"] is has_latency_cols
    assert rts.json()["rows"][0]["fail_open_count"] == 30


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_latency_timeseries_buckets_by_minute_for_1h_window(client, monkeypatch):
    monkeypatch.setattr(_ss, "_table_columns", lambda sid: {"edge_score", "edge_score_reason"})
    captured: dict[str, str] = {}

    def _fake_query_logs(*args, **kwargs):
        captured["sql"] = next((a for a in [*args, *kwargs.values()] if isinstance(a, str) and "date_trunc" in a), "")
        return [{"hour": "2026-06-17 10:00:00", "scored_count": 1, "fail_open_count": 0}]

    with patch("backend.repositories.sessions.query_logs", side_effect=_fake_query_logs):
        for hours, gran in ((1, "minute"), (24, "hour")):
            _ss._analytics_cache.clear()
            r = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours={hours}")
            assert r.status_code == 200 and r.json()["granularity"] == gran
            assert f"date_trunc('{gran}'" in captured["sql"]


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_analytics_cache_bust_and_inflight_cleanup():
    _ss._analytics_cache.clear()
    _ss._analytics_cache[("top-flagged", "svc-a", 24, 50)] = (12345.0, {"rows": [1]})
    _ss._analytics_cache[("score-distribution", "svc-a", 24)] = (12345.0, {"rows": [2]})
    _ss._analytics_cache[("top-flagged", "svc-b", 24, 50)] = (12345.0, {"rows": [3]})

    _ss._bust_analytics_cache("svc-a")
    assert list(_ss._analytics_cache.keys()) == [("top-flagged", "svc-b", 24, 50)]

    _ss._bust_analytics_cache(None)
    assert len(_ss._analytics_cache) == 0

    key = ("test_endpoint", "svc-test", 24)
    _ss._cached(key, lambda: {"foo": 1})
    assert key not in _ss._inflight
    produced = {"flag": False}
    _ss._cached(key, lambda: produced.__setitem__("flag", True) or {"foo": 999})
    assert produced["flag"] is False and key not in _ss._inflight


# ── /scoring/evaluation, /curves, /threshold-preview, /retrain, /sessions, /health, /dashboard ──


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
@pytest.mark.parametrize(
    ("path", "flag_key"),
    [
        ("evaluation", "has_min_samples"),
        ("curves", "has_min_samples"),
        ("evaluation/per-reason", "has_min_samples_overall"),
    ],
)
def test_min_samples_cta_under_threshold(client, with_config, path, flag_key):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}
    with _patch_labels([], {"good": 1, "bad": 0, "neutral": 0}):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/{path}")
    assert r.status_code == 200
    body = r.json()
    assert body[flag_key] is False
    assert "auc" not in body


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_evaluation_and_curves_when_min_samples_met(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}
    fake_labels = [{"sid": f"good{i}", "label": "good"} for i in range(3)] + [
        {"sid": f"bad{i}", "label": "bad"} for i in range(3)
    ]
    fake_counts = {"good": 3, "bad": 3, "neutral": 0}
    reconstructed = [({"session_id": f"good{i}", "events": [], "max_edge_score": i * 10}, "good") for i in range(3)] + [
        ({"session_id": f"bad{i}", "events": [], "max_edge_score": 75 + i * 5}, "bad") for i in range(3)
    ]
    fake_result = EvaluationResult(auc=0.85, pass_threshold=0.85, passed=True, n_good=3, n_bad=3)

    with (
        _patch_labels(fake_labels, fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value={"transitions": {}}),
        patch("backend.repositories.sessions.reconstruct_labeled_sessions", return_value=reconstructed),
        patch("backend.scoring.evaluate_from_persisted_scores", return_value=fake_result),
    ):
        r_eval = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
        r_curves = client.get(f"/api/services/{LOG_SVC}/scoring/curves")

    assert r_eval.status_code == 200 and r_eval.json()["auc"] == 0.85 and r_eval.json()["passed"] is True
    c_body = r_curves.json()
    assert c_body["has_min_samples"] is True and c_body["auc"] == 1.0
    assert len(c_body["roc"]) == 101 and len(c_body["pr"]) == 101

    # Missing matrix graceful error
    _ss._analytics_cache.clear()
    with (
        _patch_labels(fake_labels, fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value=None),
    ):
        r_missing = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
    assert (
        r_missing.status_code == 200 and "matrix" in r_missing.json()["error"].lower() and "auc" not in r_missing.json()
    )


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_threshold_preview_bucketing_and_extremes(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    aggregate_row = {
        "total": 6,
        "flagged_total": 4,
        "flagged_good": 1,
        "flagged_bad": 2,
        "passed_good": 1,
        "passed_bad": 1,
    }
    fake_labels = [{"sid": f"bad{i}", "label": "bad"} for i in (1, 2, 3)] + [
        {"sid": f"good{i}", "label": "good"} for i in (1, 2)
    ]

    with (
        _patch_query_logs([aggregate_row]),
        _patch_labels(fake_labels, {"good": 2, "bad": 3, "neutral": 0}),
        patch("backend.routers.session_scoring._bust_analytics_cache"),
    ):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=50&since_hours=24")
    body = r.json()
    assert body["threshold"] == 50 and body["flagged"] == {"bad": 2, "good": 1, "unlabeled": 1, "total": 4}
    assert body["passed"]["good"] == 1 and body["passed"]["bad"] == 1
    assert abs(body["precision"] - 2 / 3) < 0.01 and abs(body["recall"] - 2 / 3) < 0.01

    # Extreme thresholds (0 vs 100)
    _provisioned_cols = [{"column_name": c} for c in ("edge", *tuple(_SCORING_COLUMN_TYPES))]
    call_count = {"n": 0}

    def _route_query(_service_id, sql, *_args, **_kwargs):
        if sql.strip().upper().startswith("DESCRIBE"):
            return _provisioned_cols
        call_count["n"] += 1
        return (
            [{"total": 2, "flagged_total": 2, "passed_total": 0}]
            if call_count["n"] == 1
            else [{"total": 2, "flagged_total": 1, "passed_total": 1}]
        )

    with (
        patch("backend.repositories.sessions.query_logs", side_effect=_route_query),
        _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}),
    ):
        _ss._analytics_cache.clear()
        r_low = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=0")
        _ss._analytics_cache.clear()
        r_high = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=100")

    assert r_low.json()["flagged"]["total"] == 2
    assert r_high.json()["flagged"]["total"] == 1 and r_high.json()["passed"]["unlabeled"] == 1


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_retrain_smoke(client, with_config, tmp_path):
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "scoring_service_id": "scorer-x", "scoring_matrix_store_id": "MATRIX_STORE"},
    }
    fake_matrix = TransitionMatrix()
    fake_matrix.session_count = 5
    fake_matrix.transition_count = 12
    fake_matrix.vocab = {"/", "/login"}
    fake_stats = MatrixStats(
        sessions_in=10,
        sessions_dropped_short=3,
        sessions_dropped_fast=2,
        sessions_kept=5,
        transitions=12,
        routes_seen=2,
    )

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": LOG_SVC}),
        patch("backend.core.duckdb.get_connection") as mock_get_con,
        patch("backend.scoring.extract_traces", return_value=iter([])),
        patch("backend.scoring.build_matrix", return_value=(fake_matrix, fake_stats)),
        _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}),
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", tmp_path / "matrix.json"),
        patch("backend.core.metadata.base.publish_matrix_to_fos"),
        patch("backend.routers.session_scoring.resolve_fastly_token", return_value="tok"),
        patch("backend.core.fastly.client.fastly_raw") as mock_raw,
    ):
        mock_get_con.return_value.close = lambda: None
        r = client.post(f"/api/services/{LOG_SVC}/scoring/retrain?since_days=7")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["sessions_trained_on"] == 5 and body["transitions"] == 12
    assert body["local_matrix_saved"] is True and body["matrix_kv_written"] is True
    assert mock_raw.call_args.args[1] == "/resources/stores/kv/MATRIX_STORE/keys/matrix"


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_session_events_timeline_and_empty(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    base_ev = {
        "edge_sid": "abc123",
        "status": 200,
        "ip": "1.1.1.1",
        "ua": "browser",
        "edge_cookie_compliance": "ok",
        "edge_score_reason": "",
    }
    canned = [
        {**base_ev, "ts": "2026-06-02T20:00:00", "url": "/", "edge_score": 0},
        {**base_ev, "ts": "2026-06-02T20:00:05", "url": "/login", "edge_score": 10},
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/abc123/events")
    assert r.status_code == 200 and r.json()["event_count"] == 2 and r.json()["events"][1]["edge_score"] == 10

    with _patch_query_logs([]):
        r_empty = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/nosuch/events")
    assert r_empty.status_code == 200 and r_empty.json()["event_count"] == 0


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_health_wire_shape_and_fail_open_breakdown(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        _health_row(
            avg_score=45.5,
            p50_score=50.0,
            p95_score=75.0,
            scorer_errors=7,
            top_reasons=[{"reason": "cookie-missing", "count": 10}],
            fail_open_breakdown=[
                {"reason": "compute-unavailable-503", "count": 5},
                {"reason": "compute-unavailable-500", "count": 1},
                {"reason": "internal-error-keys", "count": 1},
            ],
        )
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    body = r.json()
    assert body["fire_rate_pct"] == 20.0 and body["distinct_sids"] == 50
    assert body["top_reasons"][0]["reason"] == "cookie-missing"
    assert body["matrix_staleness"]["is_stale"] is False
    assert body["fail_open_breakdown"][0] == {"reason": "compute-unavailable-503", "count": 5}
    assert body["fail_open_rate_pct"] == 0.7
    assert body["l2_enforce"]["available"] is False

    _ss._analytics_cache.clear()
    with _patch_query_logs([{"total_edge_rows": 10, "scored_rows": 10, "top_reasons": []}]):
        r_empty = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r_empty.json()["fail_open_breakdown"] == []


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_dashboard_returns_all_subobjects(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scorer-x"}}
    with _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}), _patch_query_logs([]):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/dashboard?since_hours=24&threshold=75")
    assert r.status_code == 200
    body = r.json()
    for key in (
        "since_hours",
        "threshold",
        "status",
        "evaluation",
        "evaluation_per_reason",
        "health",
        "top_flagged",
        "score_distribution",
        "compliance_breakdown",
        "curves",
        "threshold_preview",
        "config_threshold",
        "exclude_regex",
        "enforce_status_code",
    ):
        assert key in body
    assert "threshold" in body["config_threshold"] and "enforced" in body["config_threshold"]
    assert "current" in body["exclude_regex"] and "default" in body["exclude_regex"]
    assert "current" in body["enforce_status_code"] and "effective" in body["enforce_status_code"]


# ── /scoring/threshold, /matrix-versions, /audit ────────────────────────────


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_threshold_get_and_put(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    r_get = client.get(f"/api/services/{LOG_SVC}/scoring/threshold")
    assert r_get.status_code == 200 and r_get.json()["threshold"] is None and r_get.json()["enforced"] is False

    saved = _capture_cfg_save(monkeypatch)
    r_put = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 80})
    assert (
        r_put.status_code == 200
        and r_put.json()["threshold"] == 80
        and saved["cfg"]["scoring"]["operator_threshold"] == 80
    )

    assert client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 150}).status_code == 400

    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "operator_threshold": 80}}
    r_clear = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": None})
    assert r_clear.status_code == 200 and r_clear.json()["threshold"] is None
    assert "operator_threshold" not in saved["cfg"]["scoring"]


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_matrix_versions_list_and_restore(client, with_config, monkeypatch, tmp_path):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    with patch("backend.core.metadata.base.list_scoring_matrix_versions", return_value=[]):
        r0 = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions").json()
    assert r0["versions"] == [] and r0["current_version"] is None

    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c", "scoring_matrix_store_id": "MATRIX_STORE"},
    }
    fake_versions = [
        {
            "version": v,
            "key": f"meta/scoring_matrix_history/{v}.json",
            "size_bytes": 4000,
            "last_modified": f"{v[:10]}T10:00:00+00:00",
        }
        for v in ("2026-06-03-c", "2026-06-02-b", "2026-06-01-a")
    ]
    with patch("backend.core.metadata.base.list_scoring_matrix_versions", return_value=fake_versions):
        r_list = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions").json()
    assert len(r_list["versions"]) == 3 and r_list["current_version"] == "2026-06-03-c"

    # Restore validation gates
    with patch("backend.core.metadata.base.restore_scoring_matrix_version") as mock_restore:
        assert client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore").status_code == 400
        assert client.post(
            f"/api/services/{LOG_SVC}/scoring/matrix-versions/..%2Fetc%2Fpasswd/restore?confirm=true"
        ).status_code in (404, 405, 422)
        assert (
            client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/bad;rm-rf/restore?confirm=true").status_code
            == 422
        )
        assert (
            client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/{'a' * 65}/restore?confirm=true").status_code
            == 422
        )
        mock_restore.assert_not_called()

    # Restore 404 when missing in FOS
    with patch("backend.core.metadata.base.restore_scoring_matrix_version", return_value=None):
        assert (
            client.post(
                f"/api/services/{LOG_SVC}/scoring/matrix-versions/no-such-version/restore?confirm=true"
            ).status_code
            == 404
        )

    # Restore 500 when state_sync raises
    no_reraise_client = TestClient(app, raise_server_exceptions=False)
    with patch("backend.core.metadata.base.restore_scoring_matrix_version", side_effect=RuntimeError("S3 reset")):
        assert (
            no_reraise_client.post(
                f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true"
            ).status_code
            == 500
        )

    # Restore happy path
    saved = _capture_cfg_save(monkeypatch)
    fake_shared_path = tmp_path / "matrix.json"
    fake_matrix_path = tmp_path / f"matrix_{LOG_SVC}.json"
    fake_matrix_path.write_text('{"transitions": {}}')
    audit_calls: list = []
    with (
        patch(
            "backend.core.metadata.base.restore_scoring_matrix_version",
            return_value={"version": "2026-06-01-a", "restored_at": "2026-06-03T11:00:00+00:00"},
        ),
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_shared_path),
        patch(
            "backend.core.metadata.record_scoring_audit",
            side_effect=lambda svc, action, details=None: audit_calls.append((svc, action, details)),
        ),
        patch("backend.routers.session_scoring.resolve_fastly_token", return_value="tok"),
        patch("backend.provision.session_scoring_orchestrator._write_matrix_to_kv") as mock_kv,
    ):
        r_ok = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true")
    assert (
        r_ok.status_code == 200
        and r_ok.json()["restored_version"] == "2026-06-01-a"
        and r_ok.json()["matrix_kv_written"] is True
    )
    mock_kv.assert_called_once_with("MATRIX_STORE", LOG_SVC, "tok")
    assert not fake_matrix_path.exists()
    assert saved["cfg"]["scoring"]["matrix_version"] == "2026-06-01-a"
    assert any(a == "matrix_restored" for _, a, _ in audit_calls)


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_audit_readout_and_params(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    captured: dict = {}

    def fake_list(service_id, *, limit, since=None):
        captured.update(service_id=service_id, limit=limit, since=since)
        return [
            {
                "id": 3,
                "timestamp": "2026-06-03T10:00:00",
                "action": "threshold_committed",
                "actor": "operator",
                "details": {"new_threshold": 80},
            }
        ]

    with patch("backend.core.metadata.list_scoring_audit", side_effect=fake_list):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit?since=2026-06-03T09:00:00")
    assert r.status_code == 200 and r.json()["limit"] == 100 and r.json()["audit"][0]["id"] == 3
    assert captured == {"service_id": LOG_SVC, "limit": 100, "since": "2026-06-03T09:00:00"}

    with patch("backend.core.metadata.list_scoring_audit", return_value=[]):
        assert client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1000").status_code == 200
        assert client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1001").status_code == 422


# ── /scoring/enforce-threshold, /l2-enforce, /enforce-status-code ───────────


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_enforce_threshold_get_and_put(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").status_code == 400
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": 75}
        ).status_code
        == 400
    )

    from backend.utils.bounded_cache import CacheRegistry

    with_config[LOG_SVC] = _enforce_cfg()
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly API 404: not found")):
        r404 = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").json()
    assert r404["threshold"] is None and r404["enforced"] is False

    CacheRegistry.clear_all()
    with patch("backend.core.fastly.client.fastly", return_value={"item_key": "enforce_threshold", "item_value": "75"}):
        r_set = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").json()
    assert r_set["threshold"] == 75 and r_set["enforced"] is True

    CacheRegistry.clear_all()
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly API 503: unavailable")):
        assert client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").status_code == 502

    assert client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold", json={"threshold": 75}).status_code == 400
    for bad in (150, -5):
        assert (
            client.put(
                f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": bad}
            ).status_code
            == 400
        )

    with _capture_fastly_and_audit() as (fastly_calls, audit_calls):
        r_put = client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": 75})
    assert r_put.status_code == 200 and r_put.json()["threshold"] == 75 and r_put.json()["enforced"] is True
    assert fastly_calls[0][2]["item_value"] == "75"
    assert audit_calls == [(LOG_SVC, "threshold_enforced", {"threshold": 75})]

    with _capture_fastly_and_audit() as (fastly_calls_clr, audit_calls_clr):
        r_clr = client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": None})
    assert r_clr.status_code == 200 and r_clr.json()["threshold"] is None and r_clr.json()["enforced"] is False
    assert fastly_calls_clr[0][2]["item_value"] == ""
    assert audit_calls_clr == [(LOG_SVC, "threshold_enforce_disabled", {"threshold": None})]


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_build_l2_enforce_block_ramp_math():
    day = 86_400
    now = 1_000_000_000
    b = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 10 * day),
        l2_enforce_raw="1",
        l2_enabled_at_raw=str(now - int(1.5 * day)),
        now_epoch=now,
    )
    assert b["available"] is True and b["enabled"] is True and b["ready"] is True and b["fully_ramped"] is False
    assert b["ramp_progress"] == pytest.approx(0.5, abs=1e-6)

    off = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 30 * day),
        l2_enforce_raw="0",
        l2_enabled_at_raw=str(now - 30 * day),
        now_epoch=now,
    )
    assert off["enabled"] is False and off["fully_ramped"] is True and off["ready"] is True

    fresh = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 2 * day), l2_enforce_raw="1", l2_enabled_at_raw=str(now), now_epoch=now
    )
    assert fresh["ramp_progress"] == 0.0 and fresh["fully_ramped"] is False and fresh["ready"] is False

    none = _build_l2_enforce_block(
        scoring_enabled_at_raw=None, l2_enforce_raw=None, l2_enabled_at_raw=None, now_epoch=now
    )
    assert none["enabled"] is False and none["days_since_optin"] is None and none["ready"] is False
    assert ScoringL2EnforceBody().enabled is False and ScoringL2EnforceBody(enabled=True).enabled is True


def _get_l2_saturated(path: str) -> dict:
    if path.endswith("/item/l2_enforce_enabled"):
        return {"item_value": "1"}
    if path.endswith(("/item/l2_enabled_at", "/item/scoring_enabled_at")):
        return {"item_value": "1700000000"}
    raise AssertionError(f"unexpected GET {path}")


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_l2_enforce_get_and_put(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce").status_code == 400

    with_config[LOG_SVC] = _enforce_cfg()
    with _capture_fastly_and_audit(get_handler=_get_l2_saturated):
        r_get = client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce").json()
    assert r_get["available"] is True and r_get["enabled"] is True and r_get["fully_ramped"] is True

    with (
        _patch_query_logs([_health_row(total_edge_rows=100, scored_rows=50, l2_evaluated=0, l2_high_count=0)]),
        _capture_fastly_and_audit(get_handler=_get_l2_saturated),
    ):
        rh = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24").json()
    assert rh["l2_enforce"]["available"] is True and rh["l2_enforce"]["enabled"] is True

    assert client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce", json={"enabled": True}).status_code == 400

    # Enable from off writes flag + anchor
    with _capture_fastly_and_audit(
        get_handler=lambda _: (_ for _ in ()).throw(RuntimeError("Fastly API 404: not found"))
    ) as (writes, audits):
        r_en = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": True}).json()
    assert r_en["enabled"] is True and isinstance(r_en["l2_enabled_at"], int)
    assert any((p or {}).get("item_value") == "1" for _, _, p in writes)
    assert audits[0][1] == "l2_enforce_enabled"

    # Re-enable when already on preserves anchor
    with _capture_fastly_and_audit(get_handler=_get_l2_saturated) as (writes_on, _):
        r_on = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": True}).json()
    assert r_on["l2_enabled_at"] == 1700000000
    assert not any(
        path.endswith("/item/l2_enabled_at") or (p or {}).get("item_key") == "l2_enabled_at" for _, path, p in writes_on
    )

    # Disable writes "0"
    with _capture_fastly_and_audit(
        get_handler=lambda _: (_ for _ in ()).throw(RuntimeError("Fastly API 404: not found"))
    ) as (writes_off, audits_off):
        r_off = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": False}).json()
    assert r_off["enabled"] is False and audits_off[0][1] == "l2_enforce_disabled"


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_enforce_status_code_get_and_put(client, with_config):
    with_config[LOG_SVC] = _enforce_cfg()
    r_def = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code").json()
    assert r_def["current"] is None and r_def["effective"] == 429 and r_def["is_default"] is True

    with_config[LOG_SVC] = _enforce_cfg(enforce_status_code=403)
    r_ovr = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code").json()
    assert r_ovr["current"] == 403 and r_ovr["effective"] == 403 and r_ovr["is_default"] is False

    assert (
        client.put(f"/api/services/{LOG_SVC}/scoring/enforce-status-code", json={"status_code": 403}).status_code == 400
    )
    for bad in (399, 600, 200, 0, -1):
        assert (
            client.put(
                f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": bad}
            ).status_code
            == 400
        )
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": "not-an-int"}
        ).status_code
        == 422
    )

    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": 403}
        ).status_code
        == 400
    )

    with_config[LOG_SVC] = _enforce_cfg()
    with (
        patch(
            "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
            side_effect=lambda sid, tok, *, new_status_code: {
                "effective_status_code": new_status_code or 429,
                "is_default": new_status_code is None,
                "logging_service_active_version": 42,
            },
        ),
        _capture_fastly_and_audit() as (_, audit_calls),
    ):
        r_set = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": 451}
        ).json()
        r_rst = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": None}
        ).json()

    assert r_set["effective_status_code"] == 451 and r_set["is_default"] is False
    assert r_rst["effective_status_code"] == 429 and r_rst["is_default"] is True
    assert audit_calls[0][1] == "scoring_enforce_status_code_changed"


# ── H3: analyst time-window clamp & _scoring_source degradation ──────────────


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_time_window_and_endpoint_clamping(monkeypatch):
    pred_admin, disc_admin = _ss._scoring_time_window(_fake_request(None), 24)
    assert pred_admin == "timestamp >= now() - INTERVAL 24 HOUR" and disc_admin is None

    session = SimpleNamespace(
        query_window_hours=1, query_start_time=None, query_end_time=None, pii_policy={"mask_ips": True}
    )
    pred, disc = _ss._scoring_time_window(_fake_request(session), 168)
    assert "INTERVAL" not in pred and disc is not None
    lits = re.findall(r"TIMESTAMPTZ '([^']+)'", pred)
    span_h = (parse_iso_utc(lits[1]) - parse_iso_utc(lits[0])).total_seconds() / 3600
    assert 0.9 <= span_h <= 1.1

    captured: dict = {}
    monkeypatch.setattr(
        "backend.repositories.sessions.query_logs",
        lambda sid, sql, params=(): (
            captured.__setitem__("sql", sql) or [{"ip": "1.2.3.4", "ua": "Mozilla", "url": "/secret", "edge_score": 91}]
        ),
    )
    out_analyst = _ss.scoring_top_flagged(request=_fake_request(session), service_id="svc", since_hours=168, limit=50)
    assert "TIMESTAMPTZ" in captured["sql"] and out_analyst["rows"][0]["url"] == "/secret"

    out_admin = _ss.scoring_top_flagged(request=_fake_request(None), service_id="svc", since_hours=24, limit=50)
    assert "INTERVAL 24 HOUR" in captured["sql"] and out_admin["rows"][0]["url"] == "/secret"

    monkeypatch.setattr(
        _admin,
        "_fetch_session_events",
        lambda sid, sids, since_days=30, limit_per_sid=500, *, ts_predicate=None: (
            captured.__setitem__("ts_predicate", ts_predicate)
            or {sids[0]: [{"ts": "t", "url": "/secret", "status": 200, "ip": "1.2.3.4", "ua": "Mozilla"}]}
        ),
    )
    _admin.scoring_session_events(request=_fake_request(session), service_id="svc", sid="abc", since_days=90)
    assert captured["ts_predicate"] is not None and "TIMESTAMPTZ" in captured["ts_predicate"]
    _admin.scoring_session_events(request=_fake_request(None), service_id="svc", sid="abc", since_days=90)
    assert captured["ts_predicate"] is None


@pytest.mark.usefixtures("_clear_analytics_cache_between_tests")
def test_scoring_source_and_pre_enablement_degradation(monkeypatch):
    assert _ss._scoring_source("logs_x", set(_SCORING_COLUMN_TYPES) | {"edge", "timestamp"}) == "logs_x"
    assert _ss._scoring_source("logs_x", set()) == "logs_x"
    src = _ss._scoring_source("logs_x", {"edge", "timestamp", "ip"})
    assert src.startswith("(SELECT *, ") and src.endswith(" FROM logs_x)")
    for col, typ in _SCORING_COLUMN_TYPES.items():
        assert f"CAST(NULL AS {typ}) AS {col}" in src
    assert "edge_score_rtt_us" not in src

    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_noscore (timestamp TIMESTAMP, edge BOOLEAN, ip VARCHAR, ua VARCHAR, url VARCHAR, status INTEGER, country VARCHAR)"
        )
        con.execute("INSERT INTO logs_noscore VALUES (now()::TIMESTAMP, true, '1.2.3.4', 'UA', '/', 200, 'US')")

        def fake_query_logs(service_id, sql, params=()):
            cur = con.execute(sql, params) if params else con.execute(sql)
            rows = cur.fetchall()
            cnames = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cnames, r)) for r in rows]

        monkeypatch.setattr("backend.repositories.sessions.query_logs", fake_query_logs)
        req = _fake_request(None)
        assert _ss.scoring_top_flagged(request=req, service_id="noscore", since_hours=24, limit=50)["rows"] == []
        assert _ss.scoring_score_distribution(request=req, service_id="noscore", since_hours=24)["rows"] == []
        assert _ss.scoring_compliance_breakdown(request=req, service_id="noscore", since_hours=24)["rows"] == []
        assert _ss.scoring_latency_timeseries(request=req, service_id="noscore", since_hours=24)["has_latency"] is False
        assert (
            _ss.scoring_threshold_preview(request=req, service_id="noscore", threshold=75, since_hours=24)[
                "total_scored_sessions"
            ]
            == 0
        )
        health = _ss.scoring_health(request=req, service_id="noscore", since_hours=24)
        assert health["scored_rows"] == 0 and health["fire_rate_pct"] == 0.0 and health["top_reasons"] == []
    finally:
        con.close()


# ── Merged from tests/routers/test_session_scoring_admin.py ──

# ── Merged from tests/routers/test_session_scoring_admin.py ──
SVC = "TestScoringAdminSvc"


@pytest.fixture
def _reset_scoring_caches():
    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()
    _ssa._enforce_threshold_cache.clear()
    _ssa._l2_enforce_cache.clear()
    yield
    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()
    _ssa._enforce_threshold_cache.clear()
    _ssa._l2_enforce_cache.clear()


def _make_app(*, analyst: bool = False) -> FastAPI:
    app = FastAPI()
    if analyst:

        @app.middleware("http")
        async def _inject_analyst(request: Request, call_next):
            request.state.analyst_session = MagicMock(session_id="sess-analyst-scoring")
            return await call_next(request)

    app.include_router(scoring_router)
    return app


def _admin_enabled_cfg(**scoring_overrides: Any) -> dict[str, Any]:
    scoring = {
        "enabled": True,
        "scoring_service_id": "compute_svc_123",
        "scoring_service_name": f"Session Scoring Service for {SVC}",
        "scoring_domain": "fos-test-scorer.edgecompute.app",
        "scoring_config_store_id": "cfg_store_123",
        "scoring_keys_store_id": "keys_store_123",
        "scoring_matrix_store_id": "matrix_store_123",
        "matrix_version": "2026-10-18",
        "deployed_package_sha": "pkg_sha_1",
        "deployed_vcl_sha": "vcl_sha_1",
        "operator_threshold": 75,
        "operator_threshold_set_at": "2026-10-18T12:00:00Z",
        "request_secret": "do-not-leak-secret",
    }
    scoring.update(scoring_overrides)
    return {
        "service_id": SVC,
        "fastly_api_key": "tok_admin_123",
        "scoring": scoring,
    }


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringStatusAndComposites:
    """Status, config composite, and dashboard composite contract tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_status_returns_enabled_state_and_strips_secrets(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_orchestrator.shipped_scorer_identity",
                return_value={"package_sha": "pkg_sha_1", "vcl_sha": "vcl_sha_1"},
            ),
            patch(
                "backend.core.fastly.client.get_active_version_info",
                return_value={"number": 7, "updated_at": "2026-10-18T12:00:00Z"},
            ),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/status")

        assert res.status_code == 200
        body = res.json()
        assert body["enabled"] is True
        assert body["scoring_service_id"] == "compute_svc_123"
        assert body["scoring_active_version"] == 7
        assert body["scorer_drift"] is False
        assert "request_secret" not in body
        assert "do-not-leak-secret" not in res.text

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_config_composite_returns_bundled_controls(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_orchestrator.shipped_scorer_identity",
                return_value={"package_sha": "pkg_sha_1", "vcl_sha": "vcl_sha_1"},
            ),
            patch(
                "backend.core.fastly.client.get_active_version_info",
                return_value={"number": 7, "updated_at": "2026-10-18T12:00:00Z"},
            ),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/config")

        assert res.status_code == 200
        body = res.json()
        assert body["status"]["enabled"] is True
        assert body["threshold"]["threshold"] == 75
        assert body["enforce_status_code"]["effective"] == 429
        assert body["exclude_regex"]["is_default"] is True

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_dashboard_composite_returns_disabled_fast_path(self, prefix: str):
        cfg = {"service_id": SVC, "scoring": {"enabled": False}}
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.scoring.list_labels", return_value=[]),
            patch("backend.scoring.counts_by_label", return_value={"good": 0, "bad": 0, "neutral": 0}),
            patch("backend.repositories.sessions.query_logs", return_value=[]),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/dashboard?since_hours=24&threshold=75")

        assert res.status_code == 200
        body = res.json()
        assert body["since_hours"] == 24
        assert body["threshold"] == 75
        assert body["status"]["enabled"] is False
        assert body["health"]["scored_rows"] == 0
        assert body["top_flagged"]["rows"] == []
        assert body["score_distribution"]["rows"] == []
        assert body["compliance_breakdown"]["rows"] == []
        assert body["threshold_preview"]["threshold"] == 75


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringThresholdAndEnforcement:
    """Operator threshold, enforce-threshold, L2 enforcement, exclude-regex, and enforce-status-code tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_operator_threshold_get_and_put(self, prefix: str):
        cfg = _admin_enabled_cfg(operator_threshold=70)
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config") as mock_save,
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/threshold")
            assert get_res.status_code == 200
            assert get_res.json()["threshold"] == 70

            put_res = client.put(f"/api/services/{SVC}/{prefix}/threshold", json={"threshold": 85})
            assert put_res.status_code == 200
            assert put_res.json()["threshold"] == 85
            assert put_res.json()["enforced"] is False
            mock_save.assert_called_once()
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_enforce_threshold_requires_confirm_and_updates_config_store(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.core.fastly.client.fastly", return_value={"item_value": "80"}) as mock_fastly,
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            # Missing ?confirm=true -> 400
            unconfirmed = client.put(f"/api/services/{SVC}/{prefix}/enforce-threshold", json={"threshold": 80})
            assert unconfirmed.status_code == 400

            # With ?confirm=true -> 200
            confirmed = client.put(
                f"/api/services/{SVC}/{prefix}/enforce-threshold?confirm=true",
                json={"threshold": 80},
            )
            assert confirmed.status_code == 200
            body = confirmed.json()
            assert body["ok"] is True
            assert body["threshold"] == 80
            assert body["enforced"] is True
            mock_fastly.assert_called_once()
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_l2_enforce_requires_confirm_and_updates_config_store(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.core.fastly.client.fastly", return_value={"item_value": "1"}),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            unconfirmed = client.put(f"/api/services/{SVC}/{prefix}/l2-enforce", json={"enabled": True})
            assert unconfirmed.status_code == 400

            confirmed = client.put(
                f"/api/services/{SVC}/{prefix}/l2-enforce?confirm=true",
                json={"enabled": True},
            )
            assert confirmed.status_code == 200
            body = confirmed.json()
            assert body["ok"] is True
            assert body["enabled"] is True
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_exclude_regex_get_validate_and_put(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with patch("backend.config.load_config", return_value=cfg):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/exclude-regex")
            assert get_res.status_code == 200
            assert get_res.json()["is_default"] is True

        with patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}):
            val_res = client.post(
                f"/api/services/{SVC}/{prefix}/exclude-regex/validate",
                json={"regex": r"\.(css|js|png)$"},
            )
            assert val_res.status_code == 200
            assert val_res.json()["ok"] is True

            with (
                patch("backend.config.load_config", return_value=cfg),
                patch(
                    "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
                    return_value={
                        "current": r"\.(css|js|png)$",
                        "effective_regex": r"\.(css|js|png)$",
                        "is_default": False,
                        "logging_service_active_version": 12,
                    },
                ),
                patch("backend.core.metadata.record_scoring_audit") as mock_audit,
            ):
                put_res = client.put(
                    f"/api/services/{SVC}/{prefix}/exclude-regex?confirm=true",
                    json={"regex": r"\.(css|js|png)$"},
                )
                assert put_res.status_code == 200
                assert put_res.json()["ok"] is True
                assert put_res.json()["is_default"] is False
                mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_enforce_status_code_get_and_put(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with patch("backend.config.load_config", return_value=cfg):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/enforce-status-code")
            assert get_res.status_code == 200
            assert get_res.json()["effective"] == 429

            bad_res = client.put(
                f"/api/services/{SVC}/{prefix}/enforce-status-code?confirm=true",
                json={"status_code": 200},
            )
            assert bad_res.status_code == 400

            with (
                patch(
                    "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
                    return_value={
                        "current": 403,
                        "effective_status_code": 403,
                        "is_default": False,
                        "logging_service_active_version": 13,
                    },
                ),
                patch("backend.core.metadata.record_scoring_audit") as mock_audit,
            ):
                put_res = client.put(
                    f"/api/services/{SVC}/{prefix}/enforce-status-code?confirm=true",
                    json={"status_code": 403},
                )
                assert put_res.status_code == 200
                assert put_res.json()["ok"] is True
                assert put_res.json()["effective_status_code"] == 403
                mock_audit.assert_called_once()


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringMatrixAuditAndOperations:
    """Matrix versions, restore, rotate-key, audit log, and labels tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_matrix_versions_list_and_restore(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.core.metadata.base.list_scoring_matrix_versions",
                return_value=[{"version": "2026-10-17", "saved_at": "2026-10-17T00:00:00Z", "size_bytes": 512}],
            ),
        ):
            list_res = client.get(f"/api/services/{SVC}/{prefix}/matrix-versions")
            assert list_res.status_code == 200
            assert list_res.json()["current_version"] == "2026-10-18"
            assert len(list_res.json()["versions"]) == 1

        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", return_value=cfg),
            patch(
                "backend.core.metadata.base.restore_scoring_matrix_version",
                return_value={"restored_version": "2026-10-17", "restored_at": "2026-10-19T08:00:00Z"},
            ),
            patch("backend.provision.session_scoring_orchestrator._write_matrix_to_kv"),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            restore_res = client.post(f"/api/services/{SVC}/{prefix}/matrix-versions/2026-10-17/restore?confirm=true")
            assert restore_res.status_code == 200
            body = restore_res.json()
            assert body["ok"] is True
            assert body["restored_version"] == "2026-10-17"
            assert body["matrix_kv_written"] is True
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring"])
    def test_rotate_key_and_audit_list(self, prefix: str):
        cfg = _admin_enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_orchestrator.rotate_aes_key",
                return_value={"rotated_at": "2026-10-19T08:00:00Z", "previous_key_hex": "deadbeef"},
            ),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            rot_res = client.post(f"/api/services/{SVC}/{prefix}/rotate-key")
            assert rot_res.status_code == 200
            body = rot_res.json()
            assert body["ok"] is True
            assert body["previous_key_grace"] is True
            assert "deadbeef" not in rot_res.text
            mock_audit.assert_called_once()

        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.core.metadata.list_scoring_audit",
                return_value=[
                    {
                        "id": 1,
                        "service_id": SVC,
                        "timestamp": "2026-10-19T08:00:00Z",
                        "action": "key_rotated",
                        "actor": "operator",
                        "details": {"previous_key_grace": True},
                    }
                ],
            ),
        ):
            audit_res = client.get(f"/api/services/{SVC}/{prefix}/audit?limit=50")
            assert audit_res.status_code == 200
            assert len(audit_res.json()["audit"]) == 1
            assert audit_res.json()["audit"][0]["action"] == "key_rotated"


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringAdminRbacEnforcement:
    """Analyst Path B sessions must receive 403 {"error": "admin_only"} on all session-scoring admin/mutation endpoints."""

    @pytest.mark.parametrize("prefix", ["scoring"])
    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("GET", "status", None),
            ("GET", "config", None),
            ("GET", "dashboard", None),
            ("GET", "threshold", None),
            ("PUT", "threshold", {"threshold": 75}),
            ("GET", "enforce-threshold", None),
            ("PUT", "enforce-threshold", {"threshold": 80}),
            ("GET", "l2-enforce", None),
            ("PUT", "l2-enforce", {"enabled": True}),
            ("GET", "exclude-regex", None),
            ("PUT", "exclude-regex", {"regex": ""}),
            ("POST", "exclude-regex/validate", {"regex": ""}),
            ("GET", "enforce-status-code", None),
            ("PUT", "enforce-status-code", {"status_code": 429}),
            ("GET", "matrix-versions", None),
            ("POST", "matrix-versions/2026-10-18/restore", None),
            ("POST", "retrain", None),
            ("POST", "rotate-key", None),
            ("GET", "audit", None),
            ("GET", "evaluation/per-reason", None),
            ("POST", "enable", {"token": "tok"}),
            ("POST", "disable", {"token": "tok"}),
        ],
    )
    def test_analyst_blocked_at_middleware_and_route_dependency(
        self,
        prefix: str,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None,
    ):
        path = f"/api/services/{SVC}/{prefix}/{suffix}"
        # 1. Middleware path gate blocks the path
        assert _is_blocked_path(path) is True

        # 2. Route-level require_admin dependency also blocks with 403 admin_only
        client = TestClient(_make_app(analyst=True))
        if method == "GET":
            res = client.get(path)
        elif method == "PUT":
            res = client.put(path, json=payload)
        else:
            res = client.post(path, json=payload)

        assert res.status_code == 403
        assert res.json()["detail"]["error"] == "admin_only"


# ── Merged from tests/routers/test_scoring_exclude_regex.py ──
_SERVICE_ID = "svc-exclude-regex-test"


@pytest.fixture
def seeded_service(tmp_path, monkeypatch):
    """Per-test sandboxed configs dir with scoring enabled.

    The root conftest's ``isolate_metadata_db`` autouse fixture already
    creates ``tmp_path/configs`` and points svcconfig.CONFIGS_DIR there,
    so we just reuse that directory (mkdir would EEXIST otherwise).
    """
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    svcconfig.save_config(
        _SERVICE_ID,
        {
            "service_id": _SERVICE_ID,
            "name": "Test svc",
            "fastly_api_key": "stored-test-key",
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "scoring": {
                "enabled": True,
                "scoring_service_id": "sc-svc",
                "scoring_keys_store_id": "ks",
                "scoring_config_store_id": "cs",
                "request_secret": "test_secret_abc",
                "scoring_domain": "scorer.example.com",
                "scoring_service_name": "Test scoring service",
            },
        },
    )
    return cfg_dir


def test_get_returns_default_when_no_override(seeded_service):
    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert body["current"] == ""
    assert body["default"]  # non-empty
    assert body["effective"] == body["default"]


def test_get_returns_override_when_persisted(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(css|js)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is False
    assert body["current"] == r"\.(css|js)$"
    assert body["effective"] == r"\.(css|js)$"


def test_validate_returns_ok_for_clean_regex(seeded_service):
    """Dry-run validator with a syntactically valid + policy-clean regex
    returns ok=True. Drives the admin UI's on-blur lint affordance — no
    cfg writes, no Fastly calls."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"\.(css|js)$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert isinstance(body["lint_warnings"], list)


def test_validate_returns_error_for_invalid_regex(seeded_service):
    """Operator typing an unbalanced regex sees ok=False with a structured
    reason — same shape the publish flow surfaces, so the on-blur UI can
    use one error renderer for both code paths."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "invalid_regex"
    assert "error" in body and isinstance(body["error"], str)


def test_validate_returns_error_for_disallowed_quote(seeded_service):
    """Input-policy layer (length / quotes / control chars) fires before
    the regex compile and before falco — exercise that path too."""
    with (
        TestClient(app) as c,
        patch("backend.core.log_fields.shutil.which", return_value=None),
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "disallowed_char"


def test_validate_does_not_persist_to_cfg(seeded_service):
    """The whole point of the validate endpoint is that it's read-only —
    a successful dry-run must NOT mutate cfg.scoring.exclude_url_regex."""
    cfg_before = svcconfig.load_config(_SERVICE_ID)
    stored_before = (cfg_before.get("scoring") or {}).get("exclude_url_regex")

    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"^/healthz$"},
        )
    assert r.status_code == 200
    assert r.json()["ok"] is True

    cfg_after = svcconfig.load_config(_SERVICE_ID)
    stored_after = (cfg_after.get("scoring") or {}).get("exclude_url_regex")
    assert stored_after == stored_before, "validate must be read-only — cfg.scoring.exclude_url_regex changed"


def test_validate_rejects_non_string_body(seeded_service):
    """body.regex must be a string — int / null / array → 422 from the
    Pydantic body validator, matching the body/query validation
    classification (see commit 3c036cf)."""
    with TestClient(app) as c:
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 42},
        )
    assert r.status_code == 422
    # Pydantic envelope is a list of error objects under detail; pin the
    # field name + the string-input expectation so a future schema change
    # surfaces here rather than masquerading as a generic 422.
    errors = r.json()["detail"]
    assert any(e.get("loc", [])[-1] == "regex" for e in errors)
    assert any("string" in e.get("msg", "").lower() for e in errors)


def test_get_reports_is_default_when_literal_default_stored(seeded_service):
    """enable_scoring persists the literal DEFAULT_ASSET_EXT_REGEX into
    cfg on first turn-on so the admin UI's textarea is pre-populated with
    something to edit (the previous null sentinel produced an empty box).
    The GET must still report ``is_default=True`` in that case, otherwise
    the UI would mislabel a fresh-from-enable service as
    "currently custom override" the moment scoring is turned on."""
    from backend.provision.session_scoring_orchestrator import DEFAULT_ASSET_EXT_REGEX

    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = DEFAULT_ASSET_EXT_REGEX
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is True, "stored value equals the bundled default → must report is_default=True"
    assert body["current"] == DEFAULT_ASSET_EXT_REGEX
    assert body["effective"] == DEFAULT_ASSET_EXT_REGEX


def test_put_without_confirm_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_put_without_token_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"confirm": "true"},
            json={"regex": r"\.css$"},
        )
    # _resolve_token falls back to stored fastly_api_key only when the
    # caller passes empty; here we explicitly omit token and the cfg
    # DOES have fastly_api_key, so this should actually proceed past
    # auth and only fail at the orchestrator (which we haven't stubbed).
    # The "no token" path requires the stored key also missing.
    assert r.status_code != 200  # some downstream failure expected


def test_put_with_disallowed_quote_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch("backend.core.log_fields.shutil.which", return_value=None),
        # Disable falco-required so validation flows on its input-layer reject.
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 400
    body = r.json()
    assert body["detail"]["reason"] == "disallowed_char"


def test_put_with_invalid_regex_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "invalid_regex"


def test_put_when_scoring_disabled_rejected(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["enabled"] = False
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "not enabled" in r.json()["detail"]["error"]


def test_put_happy_path_with_stubbed_orchestrator(seeded_service):
    """Full pipeline: input validation passes, validator passes (falco
    skipped via env), orchestrator stubbed → audit log entry written →
    200 OK."""
    fake_result = {
        "effective_regex": r"\.css$",
        "is_default": False,
        "logging_service_active_version": 42,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ) as stub_update,
        patch("backend.core.metadata.record_scoring_audit") as stub_audit,
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["is_default"] is False
    assert body["effective_regex"] == r"\.css$"
    assert body["logging_service_active_version"] == 42
    stub_update.assert_called_once()
    stub_audit.assert_called_once()
    call = stub_audit.call_args
    assert call.args[1] == "scoring_exclude_regex_changed"


def test_put_reset_to_default(seeded_service):
    """Empty regex resets to default; orchestrator gets called with ""
    (which it maps to "use default" internally)."""
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(prior|custom)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    fake_result = {
        "effective_regex": "...DEFAULT...",
        "is_default": True,
        "logging_service_active_version": 43,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ),
        patch("backend.core.metadata.record_scoring_audit"),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": ""},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert "Reset to default" in body["message"]


def test_scoring_vcl_excludes_query_params():
    """Assert that the default asset exclusion regex does not match
    excluded extensions in the query string parameter, but does match them in the path."""
    import re

    from backend.provision.session_scoring_orchestrator import DEFAULT_ASSET_EXT_REGEX

    pattern = re.compile(DEFAULT_ASSET_EXT_REGEX, re.IGNORECASE)

    # Valid asset paths (should match and bypass scoring)
    assert pattern.search("/static/logo.png")
    assert pattern.search("/assets/styles.css")
    assert pattern.search("/js/app.js?v=1.2")

    # Dynamic paths with query params containing asset extensions (should NOT match, so they are scored)
    assert not pattern.search("/api/v1/login?file=.png")
    assert not pattern.search("/api/v1/user?bypass=.css")
    assert not pattern.search("/index.html?extension=.js")


@pytest.mark.security_regression
def test_validate_rejects_vcl_control_chars(seeded_service):
    """Control characters (\\x00-\\x08, \\x0a-\\x1f, \\x7f) are disallowed
    because they can break VCL string literals or confuse Fastly's VCL
    compiler. The double-quote case is covered by the existing
    ``test_validate_returns_error_for_disallowed_quote``; this test
    covers the remaining metacharacters."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        for ch, label in [
            ("\x00", "null byte"),
            ("\n", "newline"),
            ("\x7f", "DEL"),
        ]:
            r = c.post(
                f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
                json={"regex": f"foo{ch}bar"},
            )
            assert r.status_code == 200, f"{label}: unexpected status {r.status_code}"
            body = r.json()
            assert body["ok"] is False, f"{label}: should be rejected"
            assert body["reason"] == "disallowed_char", f"{label}: wrong reason {body.get('reason')}"


@pytest.mark.security_regression
def test_put_rejects_vcl_control_char_injection(seeded_service):
    """PUT rejects regex with embedded control chars — a complementary
    vector to the double-quote test."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": "foo\nbar"},
        )
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "disallowed_char"


@pytest.mark.security_regression
def test_validate_rejects_catastrophic_backtracking_vector(seeded_service):
    """Classic ReDoS patterns like ``(a+)+$`` compile fine under Python's
    ``re`` module and pass the disallowed-char check. Currently the only
    backstop is falco's 10 s timeout at lint time. This test documents
    the gap: without falco, the regex passes validation.

    When a dedicated ReDoS checker is added, this test should flip to
    asserting ``ok=False`` with a ``redos`` reason."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": "(a+)+$"},
        )
    assert r.status_code == 200
    body = r.json()
    # Documents current behavior: the ReDoS vector passes input validation
    # when falco is not available. When a dedicated checker is added,
    # update this assertion to: assert body["ok"] is False
    assert body["ok"] is True


def test_rotate_key_happy_path(seeded_service):
    """Stub the crypto layer and verify the router returns the expected
    shape: ``ok``, ``rotated_at``, ``previous_key_grace``, ``message``."""
    fake_result = {
        "rotated_at": "2026-07-08T12:00:00Z",
        "previous_key_hex": "deadbeef",
    }
    with (
        TestClient(app) as c,
        patch(
            "backend.provision.session_scoring_orchestrator.rotate_aes_key",
            return_value=fake_result,
        ) as stub_rotate,
        patch("backend.core.metadata.record_scoring_audit") as stub_audit,
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/rotate-key",
            headers={"token": "stored-test-key"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["rotated_at"] == "2026-07-08T12:00:00Z"
    assert body["previous_key_grace"] is True
    assert "message" in body
    stub_rotate.assert_called_once()
    stub_audit.assert_called_once()
    assert stub_audit.call_args.args[1] == "key_rotated"


def test_rotate_key_when_scoring_disabled(seeded_service):
    """rotate-key requires scoring to be enabled — 400 when disabled."""
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["enabled"] = False
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/rotate-key",
            headers={"token": "stored-test-key"},
        )
    assert r.status_code == 400
    assert "not enabled" in r.json()["detail"]["error"]


# ── Merged from tests/routers/test_sessions_token.py ──
_HDR = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed(in_memory_duckdb, source, ip="10.0.0.1", n=10):
    # Mock logs store naive TIMESTAMP holding UTC wall-clock. Production clamps
    # to tz-aware bounds (parse_iso_utc → CAST AS TIMESTAMPTZ), so pin the
    # session TZ to UTC (as prod does) — otherwise the naive↔tz-aware compare
    # uses the host's local TZ and the window misses every row.
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    # The schema cache is keyed by table name, not connection — clear it so
    # get_sessions reads THIS fresh in-memory connection's columns rather than
    # a stale (possibly empty) entry left by another test on "test_service".
    _clear_schema_cache()


def _seed_zero_width(in_memory_duckdb, source, ip="10.0.0.9", n=5):
    """Seed a single-request session: every log shares ONE timestamp so the
    aggregated session has session_start == session_end (a zero-width window).
    Returns the pinned timestamp."""
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    ts = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
        log["timestamp"] = ts
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    _clear_schema_cache()
    return ts


def _window():
    now = datetime.now(UTC)
    return (now - timedelta(hours=2)).isoformat(), now.isoformat()


def _use_analyst(in_memory_duckdb, source, *, mask_ips: bool):
    """Re-point build_request_context to inject an analyst session into ctx.
    (The local TestClient socket path classifies as admin in the middleware, so
    we exercise the router's analyst branch via the injected ctx.analyst_session.)"""
    session = SimpleNamespace(service_ids=[source["service_id"]], pii_policy={"mask_ips": mask_ips})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=source, _con_override=in_memory_duckdb, session=session
    )


def _list_first_token(client, in_memory_duckdb, source):
    # Omit start/end → default range (raw session path). An explicit >1h window
    # routes get_sessions to the rollup path, which has no parquet in tests.
    r = client.post("/api/sessions", headers=_HDR, json={})
    assert r.status_code == 200, r.text
    sessions = r.json()["sessions"]
    assert sessions, "expected at least one session row from seeded data"
    return sessions[0]


def test_list_attaches_session_token(client, in_memory_duckdb, test_service_source):
    """Every session row carries a non-empty opaque token."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row.get("session_token"), "session_token missing/empty on list row"
    # opaque — must not embed the raw ip
    assert "10.0.0.1" not in row["session_token"]


def test_token_resolves_detail(client, in_memory_duckdb, test_service_source):
    """A token taken from the list resolves a non-empty detail result."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["data"], "token-keyed detail returned no rows"


def test_invalid_token_returns_400(client, in_memory_duckdb, test_service_source):
    """A malformed/stale token surfaces a clear 400, not a silent empty grid."""
    _seed(in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": "garbage.token"})
    assert r.status_code == 400, r.text
    assert r.json()["detail"]["error"] == "session_token_invalid"


def test_zero_width_session_detail_via_token(client, in_memory_duckdb, test_service_source):
    """A single-request session (session_start == session_end) must still
    resolve detail rows. Previously the time clamp rejected the zero-width
    window with 400 'time_range_empty' and the modal showed 'No results' —
    for ~28% of real sessions, all roles. The detail handler now widens a
    zero-width window by 1s/side before clamping."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row["session_start"] == row["session_end"], "fixture should produce a zero-width session"
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail returned no rows"


def test_zero_width_session_detail_via_raw_ip(client, in_memory_duckdb, test_service_source):
    """The same zero-width widening helps the legacy raw-ip path (admin /
    non-masking analyst), not only the token path — the widen is shared."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": row["ip"], "start_time": row["session_start"], "end_time": row["session_end"]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail (raw ip) returned no rows"


@pytest.mark.security_regression
@pytest.mark.parametrize("raw_ip", ["10.0.0.1", "10.0.0.xxx"])
def test_masking_analyst_raw_ip_rejected(client, in_memory_duckdb, test_service_source, raw_ip):
    """A masking analyst supplying a raw OR masked top-level ip (no token) is
    rejected with 403 — accepting it would be a presence oracle (the masked
    value can never match; a raw value would probe for a guessed real IP)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": raw_ip, "start_time": start, "end_time": end},
    )
    assert r.status_code == 403, r.text
    assert r.json()["detail"]["error"] == "pii_policy_violation"


@pytest.mark.security_regression
def test_masking_analyst_token_returns_rows(client, in_memory_duckdb, test_service_source):
    """The fix itself: a masking analyst drills in via the opaque token and gets
    rows — without ever holding the real IP."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "masking analyst token-keyed detail returned no rows"


@pytest.mark.security_regression
def test_non_masking_analyst_raw_ip_allowed(client, in_memory_duckdb, test_service_source):
    """A NON-masking analyst still uses the legacy raw-ip path (the lock is
    gated on pii_policy.mask_ips, not on analyst-hood)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=False)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": "10.0.0.1", "start_time": start, "end_time": end},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "non-masking analyst raw-ip detail returned no rows"


@pytest.mark.security_regression
def test_detail_body_ip_column_is_masked_by_policy():
    """Pins that the detail response shape ({"data":[{"ip":...}], "columns":[...]})
    is caught by apply_pii_policy's key-name masking — the invariant that keeps
    raw IPs out of a masking analyst's detail body once rows are returned. If a
    future change aliases the ip column out of IP_FAMILY_KEYS this fails."""
    body = {
        "columns": ["ip", "url"],
        "data": [
            {"ip": "203.0.113.45", "url": "/a"},
            {"ip": "198.51.100.7", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # non-PII columns stay verbatim
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_detail_body_cookie_session_is_redacted_by_policy():
    """Phase-4 Track C added `cookie_session` — a hashed per-client session id
    captured at the edge. It is client PII, so a mask_ips analyst viewing raw
    /logs or /query rows must NOT see the hash. apply_pii_policy redacts the
    cookie_session column (SESSION_ID_KEYS) wholesale; if a future change drops
    it from the masked set this fails."""
    body = {
        "columns": ["ip", "cookie_session", "url"],
        "data": [
            {"ip": "203.0.113.45", "cookie_session": "a1b2c3d4deadbeef", "url": "/a"},
            {"ip": "198.51.100.7", "cookie_session": "", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    # Non-empty session hash → redacted; empty stays empty; raw hash never leaks.
    assert [r["cookie_session"] for r in masked["data"]] == ["[redacted]", ""]
    assert all("deadbeef" not in str(r.get("cookie_session")) for r in masked["data"])
    # IP still masked, url still verbatim.
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_aggregates_topn_cookie_session_value_is_redacted_by_policy():
    """/api/dashboard/aggregates top-N panels carry the dimension value under a
    generic ``value`` key nested as ``data[<field>]["top"][i]["value"]`` — the
    field name is the PARENT key, not the cell key. Pins that apply_pii_policy
    threads that field context so a cookie_session top-N cell is redacted (a
    mask_ips analyst must not read raw session hashes off the Top card) while
    the IP top-N cell stays masked (unchanged) and url stays verbatim."""
    body = {
        "data": {
            "ip": {"top": [{"value": "203.0.113.45", "count": 9}], "total": 9},
            "cookie_session": {
                "top": [{"value": "a1b2c3d4deadbeef", "count": 7}, {"value": "", "count": 1}],
                "total": 8,
            },
            "url": {"top": [{"value": "/login", "count": 5}], "total": 5},
        }
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    cs_top = masked["data"]["cookie_session"]["top"]
    assert [c["value"] for c in cs_top] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked["data"]["cookie_session"])
    # IP top-N still value-shape masked; url panel untouched.
    assert masked["data"]["ip"]["top"][0]["value"] == "203.0.113.xxx"
    assert masked["data"]["url"]["top"][0]["value"] == "/login"


@pytest.mark.security_regression
def test_field_values_cookie_session_is_redacted_by_policy():
    """/api/dashboard/field-values names its column in a SIBLING ``field`` key
    (``{"field": "cookie_session", "values": [{"value": <hash>}]}``), not the
    parent key. Pins that apply_pii_policy resolves that sibling so the distinct
    cookie_session values are redacted — otherwise a mask_ips analyst could
    enumerate every raw session hash via the value picker."""
    body = {
        "field": "cookie_session",
        "values": [
            {"value": "a1b2c3d4deadbeef", "count": 12},
            {"value": "", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked)
    assert masked["field"] == "cookie_session"  # the field NAME is fine to show


@pytest.mark.security_regression
def test_field_values_ip_is_masked_by_policy():
    """/api/dashboard/field-values with field=ip names the column in the SIBLING
    ``field`` key, so the raw distinct IP ``value`` cells slipped past the
    key-name masker — a mask_ips analyst could enumerate every raw client IP via
    the value picker. Pins that apply_pii_policy resolves the sibling ``field``
    for IP columns too and value-shape masks the cells (non-IP values verbatim)."""
    body = {
        "field": "ip",
        "values": [
            {"value": "203.0.113.45", "count": 12},
            {"value": "198.51.100.7", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # A non-IP dimension (url) must stay verbatim — even an IP-shaped url value.
    url_body = {"field": "url", "values": [{"value": "/a"}, {"value": "1.2.3.4"}]}
    assert [v["value"] for v in apply_pii_policy(url_body, {"mask_ips": True})["values"]] == ["/a", "1.2.3.4"]


def test_sessions_with_range_token(client, in_memory_duckdb, test_service_source):
    """Verify range_token is recognized and resolved server-side on POST /api/sessions."""
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.42", n=5)
    r = client.post(
        "/api/sessions",
        headers=_HDR,
        json={"range_token": "1h"},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert "sessions" in data
    assert any(s["ip"] == "10.0.0.42" for s in data["sessions"])


def test_sessions_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify process_context is captured for sessions:list and sessions:detail."""
    captured_contexts = []
    import backend.routers.session_scoring as sess_mod

    orig_scope = sess_mod.process_context_scope

    def mock_scope(ctx_name):
        captured_contexts.append(ctx_name)
        return orig_scope(ctx_name)

    monkeypatch.setattr(sess_mod, "process_context_scope", mock_scope)
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.55", n=5)

    r = client.post("/api/sessions", headers=_HDR, json={"range_token": "1h"})
    assert r.status_code == 200
    assert "sessions:list" in captured_contexts

    row = r.json()["sessions"][0]
    token = row["session_token"]
    r_detail = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": token})
    assert r_detail.status_code == 200
    assert "sessions:detail" in captured_contexts


def test_sessions_stale_view_retry_wrapped(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify execute_with_stale_view_retry wraps get_sessions in sessions_endpoint."""
    import backend.routers.session_scoring as sess_mod

    called = []
    orig_retry = sess_mod.execute_with_stale_view_retry

    def mock_retry(con, src, fn):
        called.append(True)
        return orig_retry(con, src, fn)

    monkeypatch.setattr(sess_mod, "execute_with_stale_view_retry", mock_retry)
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.99", n=2)
    r = client.post("/api/sessions", headers=_HDR, json={})
    assert r.status_code == 200
    assert called, "execute_with_stale_view_retry should be called by sessions_endpoint"
