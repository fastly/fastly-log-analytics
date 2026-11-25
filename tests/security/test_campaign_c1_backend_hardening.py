"""Security regression tests for Campaign C1 (High-Scale Backend & Declarative VCL Hardening).

Covers:
- C1-1: Embedded-IP PII leakage to analyst_no_pii in scoring_top_flagged / scoring_analytics_composite
- C1-2: Unbounded _insights_sems dict & per-service concurrency cap bypass
- C1-3: Unredacted driver/upstream exception leakage in HTTP & SSE responses
- C1-4: Defense-in-depth Depends(require_admin) & require_service_in_scope in services/crud.py
- C1-5: Unbounded lookback_hours & unbounded _ALERT_PREVIEW_CACHE in alerts.py
- C2-1: Client-spoofable Fastly-Client-IP in High-Scale declarative VCL provisioning
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.core.request_context import require_admin
from backend.models.common import Alert, ServiceCronSettingsBody
from backend.provision import fastly_api
from backend.provision.declarative.models import FeatureState
from backend.provision.declarative.vcl_builder import desired_snippets, generate_consolidated_snippet
from backend.routers import alerts as alerts_mod
from backend.routers import insights as insights_mod
from backend.routers import provision as provision_mod
from backend.routers import root_system as root_system_mod
from backend.routers.admin import health as admin_health_mod
from backend.routers.rum import provisioning as rum_prov_mod
from backend.routers.services import crud as services_crud_mod
from backend.routers.services import vcl as services_vcl_mod
from backend.routers.session_scoring import scoring_analytics as scoring_mod
from backend.routers.session_scoring import scoring_vcl as scoring_vcl_mod
from backend.utils.bounded_cache import BoundedTTLCache

pytestmark = [pytest.mark.security_regression, pytest.mark.no_postgres]


def _fake_request(analyst_session: object | None = None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _analyst_no_pii(service_ids: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        invite_id="inv-test-1",
        service_ids=service_ids if service_ids is not None else ["svc-test"],
        pii_policy={"mask_ips": True},
        mask_ips=True,
        query_start_time=None,
        query_end_time=None,
        query_window_hours=None,
    )


async def _collect_sse(resp) -> str:
    """Drain an EventSourceResponse body_iterator into a single string."""
    chunks: list[str] = []
    it = resp.body_iterator
    if hasattr(it, "__aiter__"):
        async for chunk in it:
            chunks.append(chunk.decode() if isinstance(chunk, bytes) else str(chunk))
    else:
        for chunk in it:
            chunks.append(chunk.decode() if isinstance(chunk, bytes) else str(chunk))
    return "".join(chunks)


# ── C1-1: Embedded-IP PII Scrubbing in scoring_top_flagged / composite ────────


def test_c1_1_scoring_top_flagged_and_composite_scrub_embedded_ips_for_analyst_no_pii(monkeypatch) -> None:
    """Embedded IPv4/IPv6 addresses inside url and ua must be scrubbed for analyst_no_pii."""
    canned_rows = [
        {
            "timestamp": "2026-11-25T10:00:00Z",
            "ip": "203.0.113.42",
            "url": "/login?origin=198.51.100.7&peer=2001:db8:abcd::9",
            "country": "US",
            "ua": "Mozilla/5.0 (proxy 192.0.2.88; v6 2001:db8:1234::5)",
            "status": 200,
            "edge_sid": "sid-secret-1",
            "edge_score": 92.0,
            "edge_score_l1": 45.0,
            "edge_score_l2": 47.0,
            "edge_score_reason": "bot_pattern",
            "edge_cookie_compliance": "missing",
            "edge_score_rtt_us": 1200,
            "edge_score_exec_us": 400,
        }
    ]
    monkeypatch.setattr(scoring_mod, "_prepare_scoring_query", lambda req, sid, sh: ("src", set(), "1=1", "win"))
    monkeypatch.setattr(scoring_mod, "_table_columns", lambda sid: set())
    monkeypatch.setattr(scoring_mod, "_cached_scoring_rows", lambda key, sh, fn, ts_field=None: {"rows": fn()})
    monkeypatch.setattr(scoring_mod, "query_logs", lambda sid, sql, params=(): canned_rows)

    req = _fake_request(_analyst_no_pii(["svc-test"]))
    out = scoring_mod.scoring_top_flagged(request=req, service_id="svc-test", since_hours=24, limit=50)
    serialized = json.dumps(out)
    for leaked_ip in ("203.0.113.42", "198.51.100.7", "2001:db8:abcd::9", "192.0.2.88", "2001:db8:1234::5"):
        assert leaked_ip not in serialized, f"IP {leaked_ip} leaked in scoring_top_flagged"

    # Composite endpoint inherits top_flagged scrubbing
    monkeypatch.setattr(scoring_mod, "scoring_score_distribution", lambda **kw: {"rows": []})
    monkeypatch.setattr(scoring_mod, "scoring_compliance_breakdown", lambda **kw: {"rows": []})
    monkeypatch.setattr(scoring_mod, "scoring_latency_timeseries", lambda **kw: {"rows": []})
    monkeypatch.setattr(scoring_mod, "scoring_health", lambda **kw: {"total_edge_rows": 0})
    comp = scoring_mod.scoring_analytics_composite(request=req, service_id="svc-test", since_hours=24)
    comp_serialized = json.dumps(comp)
    for leaked_ip in ("203.0.113.42", "198.51.100.7", "2001:db8:abcd::9", "192.0.2.88", "2001:db8:1234::5"):
        assert leaked_ip not in comp_serialized, f"IP {leaked_ip} leaked in scoring_analytics_composite"


# ── C1-2: Per-Service Insights Semaphore Cap & Bounded Cache ──────────────────


def test_c1_2_insights_sem_shared_across_windows_and_bounded() -> None:
    """Different window/baseline args for the same service share one semaphore, and the map is bounded."""
    with insights_mod._insights_sems_lock:
        insights_mod._insights_sems.clear()

    sem1 = insights_mod._get_insights_sem("svc-cap", 1.0, 24.0)
    sem2 = insights_mod._get_insights_sem("svc-cap", 2.5, 48.0)
    sem3 = insights_mod._get_insights_sem("svc-cap", 6.0, 168.0)
    assert sem1 is sem2
    assert sem1 is sem3

    for i in range(400):
        insights_mod._get_insights_sem(f"svc-flood-{i}", float(i + 1), 24.0)

    assert len(insights_mod._insights_sems) <= 256


# ── C1-3: Unredacted Exception Leakage Across HTTP & SSE Surfaces ─────────────


def test_c1_3_scoring_query_logs_redacts_paths(monkeypatch) -> None:
    """query_logs must redact filesystem paths from ClickHouse driver exceptions."""
    fake_client = MagicMock()
    fake_client.execute.side_effect = RuntimeError("ClickHouse error reading /var/lib/fla-internal/secrets.conf")
    monkeypatch.setattr(scoring_mod, "get_source_for_service", lambda sid: {"name": sid})
    with (
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=fake_client),
        patch("backend.high_scale.ingest_controller.get_high_scale_service_registry") as reg,
    ):
        reg.return_value.resolve.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            scoring_mod.query_logs("svc-err", "SELECT 1")
    assert exc_info.value.status_code == 400
    detail_str = json.dumps(exc_info.value.detail)
    assert "/var/lib/fla-internal/secrets.conf" not in detail_str
    assert "<path>" in detail_str


def test_c1_3_root_system_deep_health_redacts_paths() -> None:
    """GET /health?deep=1 must redact filesystem paths when metadata_db raises."""
    req = _fake_request(None)
    with (
        patch("backend.utils.remote_access.is_request_remote", return_value=False),
        patch("backend.config.list_service_ids", return_value=["svc-h1"]),
        patch("backend.core.metadata.get_con", side_effect=RuntimeError("open /var/lib/fla/meta.db failed")),
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=None),
    ):
        resp = root_system_mod.health_check(req, deep=True)
    body = json.loads(resp.body.decode()) if hasattr(resp, "body") else resp
    text = json.dumps(body)
    assert "/var/lib/fla/meta.db" not in text
    assert "<path>" in text


def test_c1_3_admin_health_snapshot_redacts_fos_and_clickhouse_errors() -> None:
    """health_snapshot must redact filesystem paths from FOS and ClickHouse probe exceptions."""
    admin_health_mod._HEALTH_SNAPSHOT_CACHE.clear()
    fake_ch = MagicMock()
    fake_ch.health.side_effect = RuntimeError("TLS cert missing at /var/lib/clickhouse/client.pem")
    with (
        patch.object(admin_health_mod, "get_celery_status", return_value={}),
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-h2", "name": "svc-h2"}]),
        patch(
            "backend.config.config_to_source",
            side_effect=RuntimeError("bad keyfile /var/lib/fos/creds.json"),
        ),
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=fake_ch),
        patch("backend.high_scale.archive.disk_snapshot_payload", return_value={}),
        patch(
            "backend.high_scale.archive.topology_from_environment",
            side_effect=ValueError("unset"),
        ),
    ):
        out = admin_health_mod.health_snapshot(probe_fos=True)
    text = json.dumps(out)
    assert "/var/lib/fos/creds.json" not in text
    assert "/var/lib/clickhouse/client.pem" not in text
    assert "<path>" in text


def test_c1_3_services_crud_sse_redacts_errors() -> None:
    """SSE error events in services/crud.py must pass exceptions through redact_error_text."""
    req = _fake_request(None)
    body = ServiceCronSettingsBody()
    with patch(
        "backend.config.load_config",
        side_effect=RuntimeError("cannot read /var/lib/fla/service.toml"),
    ):
        resp = services_crud_mod.api_service_cron_settings(req, "svc-sse", body)
        output = asyncio.run(_collect_sse(resp))
    assert "/var/lib/fla/service.toml" not in output
    assert "<path>" in output


def test_c1_3_services_vcl_sse_redacts_errors() -> None:
    """SSE error events in services/vcl.py must pass exceptions through redact_error_text."""
    req = _fake_request(None)
    with (
        patch.object(services_vcl_mod, "load_service_config", return_value={"provisioning": {}, "log_period": 60}),
        patch.object(services_vcl_mod, "stored_fastly_token", return_value="tok-demo"),
        patch(
            "backend.provision.update_logging_endpoint",
            side_effect=RuntimeError("failed loading /var/lib/fla/vcl_cfg.json"),
        ),
    ):
        resp = services_vcl_mod.api_service_update_logging_settings(
            req,
            "svc-vcl",
            period=None,
            sample_rate=None,
            prefix=None,
            edge_only=None,
            custom_condition=None,
            update_format=False,
            cmcd_enabled=None,
            cmcd_mode=None,
            cmcd_version=None,
        )
        output = asyncio.run(_collect_sse(resp))
    assert "/var/lib/fla/vcl_cfg.json" not in output
    assert "<path>" in output


def test_c1_3_provision_reconcile_sse_redacts_errors() -> None:
    """SSE error events in provision.py reconcile endpoint must redact paths."""
    from backend.models.common import ProvisionReconcileRequest

    req_body = ProvisionReconcileRequest(service_id="svc-prov", token="tok-demo")
    with (
        patch.object(provision_mod, "_validate_provision_vcl_inputs"),
        patch("backend.config.load_config", return_value=None),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            side_effect=RuntimeError("reconcile failed on /var/run/fastly/state.json"),
        ),
    ):
        resp = provision_mod.provision_reconcile(req_body)
        output = asyncio.run(_collect_sse(resp))
    assert "/var/run/fastly/state.json" not in output
    assert "<path>" in output


def test_c1_3_rum_provisioning_sse_redacts_errors() -> None:
    """SSE error events in rum/provisioning.py must redact paths."""
    from backend.models.common import RumDisableRequest

    req = _fake_request(None)
    with (
        patch.object(rum_prov_mod, "require_fastly_token", return_value="tok-demo"),
        patch.object(
            rum_prov_mod,
            "run_with_events",
            side_effect=RuntimeError("rum failed at /opt/fla/rum_state.json"),
        ),
    ):
        resp = asyncio.run(rum_prov_mod.disable_rum_handler(req, "svc-rum", RumDisableRequest()))
        output = asyncio.run(_collect_sse(resp))
    assert "/opt/fla/rum_state.json" not in output
    assert "<path>" in output


def test_c1_3_scoring_vcl_sse_redacts_errors() -> None:
    """SSE error events in scoring_vcl.py scoring_enable must redact paths."""
    captured_error_event = {}

    def _fake_stream(*args, **kwargs):
        fn = kwargs["error_event"]
        captured_error_event["ev"] = fn(RuntimeError("scoring failed at /var/lib/scoring/matrix.bin"))
        return {"ok": True}

    with (
        patch.object(scoring_vcl_mod, "_require_fastly_token", return_value="tok-demo"),
        patch("backend.config.load_config", return_value={}),
        patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=True),
        patch("backend.config.mutate_config"),
        patch.object(scoring_vcl_mod, "stream_iac_reconcile", side_effect=_fake_stream),
    ):
        scoring_vcl_mod.scoring_enable("svc-sc")
    msg = captured_error_event["ev"]["message"]
    assert "/var/lib/scoring/matrix.bin" not in msg
    assert "<path>" in msg


# ── C1-4: Defense-in-Depth require_admin & require_service_in_scope ───────────


def test_c1_4_services_crud_admin_dependencies_and_scope() -> None:
    """All admin mutation/log-stream routes in services/crud.py declare Depends(require_admin),
    GET /services stays analyst-readable, and clear-time-range / ngwaf-sync enforce service scope."""
    admin_paths = {
        ("/services/{service_id}/cron-settings", "POST"),
        ("/services/{service_id}/time-range", "DELETE"),
        ("/cron-runs/{run_id}/stream", "GET"),
        ("/cron-schedule", "GET"),
        ("/services/{service_id}/credentials", "PATCH"),
        ("/services/{service_id}/ngwaf-sync", "POST"),
    }
    found: set[tuple[str, str]] = set()
    for route in services_crud_mod.router.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", set()) or set()
        deps = [d.dependency for d in getattr(route, "dependencies", [])]
        if path == "/services" and "GET" in methods:
            assert require_admin not in deps, "GET /api/services must remain analyst-readable"
        for m in methods:
            if (path, m) in admin_paths:
                assert require_admin in deps, f"{m} {path} is missing Depends(require_admin)"
                found.add((path, m))
    assert found == admin_paths

    out_of_scope_req = _fake_request(_analyst_no_pii(["svc-other"]))
    with pytest.raises(HTTPException) as exc1:
        services_crud_mod.api_service_clear_time_range(out_of_scope_req, "svc-forbidden")
    assert exc1.value.status_code == 403

    with pytest.raises(HTTPException) as exc2:
        services_crud_mod.api_ngwaf_sync(out_of_scope_req, "svc-forbidden")
    assert exc2.value.status_code == 403


# ── C1-5: Bounded lookback_hours & Bounded _ALERT_PREVIEW_CACHE ──────────────


def test_c1_5_alerts_preview_bounds_lookback_and_cache() -> None:
    """lookback_hours > 720 is rejected with 422 and _ALERT_PREVIEW_CACHE is a BoundedTTLCache."""
    assert isinstance(alerts_mod._ALERT_PREVIEW_CACHE, BoundedTTLCache)
    assert alerts_mod._ALERT_PREVIEW_CACHE.maxsize <= 256

    alerts_mod._ALERT_PREVIEW_CACHE.clear()
    for i in range(300):
        alerts_mod._ALERT_PREVIEW_CACHE[(f"k-{i}",)] = {"times": [], "values": []}
    assert len(alerts_mod._ALERT_PREVIEW_CACHE) <= 256

    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(alerts_mod.alerts_router)
    client = TestClient(app)
    payload = Alert(
        name="High 5xx",
        service_id="svc-alert",
        metric="5xx",
        operator=">",
        threshold=10.0,
        window_min=5,
    ).model_dump()
    resp = client.post("/api/alerts/preview?lookback_hours=721", json=payload)
    assert resp.status_code == 422


# ── C2-1: Client-Spoofable Fastly-Client-IP in High-Scale Declarative VCL ────


def test_c2_1_declarative_vcl_scrubs_fastly_client_ip_before_capture() -> None:
    """High-Scale declarative VCL must scrub client-supplied Fastly-Client-IP at ingress before capture."""
    assert "Fastly-Client-IP" in fastly_api._SCRUB_HEADERS
    scrub_stmts = fastly_api.get_scrub_vcl_statements({"groups": ["A"]})
    assert "unset req.http.Fastly-Client-IP;" in scrub_stmts

    state = FeatureState(
        service_id="svc-vcl",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="us-east.object.fastlystorage.app",
        logging_enabled=True,
    )
    recv_vcl = generate_consolidated_snippet(state, "vcl_recv")
    assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in recv_vcl
    unset_idx = recv_vcl.index("unset req.http.Fastly-Client-IP;")
    capture_idx = recv_vcl.index("set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;")
    guard_idx = recv_vcl.index("if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {")
    assert guard_idx < unset_idx < capture_idx

    snippets = desired_snippets(state)
    recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")
    assert "unset req.http.Fastly-Client-IP;" in recv_snippet.body
