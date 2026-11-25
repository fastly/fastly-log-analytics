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
from backend.provision.declarative.vcl_builder import desired_snippets
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
    """High-Scale declarative VCL must emit a priority -100 reset snippet for Fastly-Client-IP
    while keeping _SCRUB_HEADERS free of Fastly-Client-IP so operator VCL between -100 and 10
    is preserved."""
    assert "Fastly-Client-IP" not in fastly_api._SCRUB_HEADERS
    scrub_stmts = fastly_api.get_scrub_vcl_statements({"groups": ["A"]})
    assert "unset req.http.Fastly-Client-IP;" not in scrub_stmts

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
    snippets = desired_snippets(state)
    reset_snippet = next(s for s in snippets if s.name == "Fastly Log Analysis Reset Client IP")
    recv_snippet = next(s for s in snippets if s.name == "Fastly Log Analytics - vcl_recv")
    assert reset_snippet.subroutine == "vcl_recv"
    assert reset_snippet.priority == -100
    assert reset_snippet.priority < recv_snippet.priority
    assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in reset_snippet.body
    assert "unset req.http.Fastly-Client-IP;" in reset_snippet.body
    assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in recv_snippet.body


# ── Iteration 2 Wave 1 Verifier Hardening Regressions ─────────────────────────


def test_w1_1_redact_error_text_scrubs_credentials_headers_aws_and_paths() -> None:
    """redact_error_text must scrub DSN creds, query/kv secrets, auth headers, AWS keys, and OS paths."""
    from backend.utils.telemetry.logging_config import redact_error_text

    # Use low-entropy synthetic placeholders so secret scanners remain clean.
    raw = (
        "connect failed postgresql://dbuser:demopass00@db.internal:5432/fla "
        "https://svc.local/api?token=demotok00&api_key=demokey00&password=demopw00 "
        "Fastly-Key: demofastly00 Authorization: Bearer demobearer00 "
        "aws_secret_access_key=demoawssec00 AKIA0000000000000001 ASIA0000000000000002 "
        "paths /etc/shadow /root/.ssh/id /Users/dev/secret /run/secrets/db /proc/self/environ /private/tmp/a"
    )
    redacted = redact_error_text(raw)
    for secret in (
        "demopass00",
        "demotok00",
        "demokey00",
        "demopw00",
        "demofastly00",
        "demobearer00",
        "demoawssec00",
        "AKIA0000000000000001",
        "ASIA0000000000000002",
        "/etc/shadow",
        "/root/.ssh/id",
        "/Users/dev/secret",
        "/run/secrets/db",
        "/proc/self/environ",
        "/private/tmp/a",
    ):
        assert secret not in redacted, f"Sensitive fragment leaked: {secret!r} in {redacted!r}"
    assert "postgresql://dbuser:[redacted]@db.internal:5432/fla" in redacted
    assert "<path>" in redacted


def test_w1_2_remaining_sse_and_http_error_paths_are_redacted() -> None:
    """stream_iac_reconcile, provision teardown/fos/check-config, and orchestrate rollback_err must redact."""
    from backend.models.common import CheckFosRequest, ProvisionTeardownRequest
    from backend.provision.declarative import orchestrate as orch_mod
    from backend.utils.sse import stream_iac_reconcile

    # 1) stream_iac_reconcile default fallback error event
    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=RuntimeError("boom postgresql://u:demopass11@h/db /etc/fla.conf"),
    ):
        resp = stream_iac_reconcile("svc-sse-1", "tok", start_message="start", done_event={"type": "done"})
        out = asyncio.run(_collect_sse(resp))
    assert "demopass11" not in out
    assert "/etc/fla.conf" not in out
    assert "[redacted]" in out and "<path>" in out

    # 2) provision_teardown SSE error & warning events
    with (
        patch(
            "backend.config.load_config",
            return_value={"fos_bucket": "b", "fos_region": "us-east-1", "provisioning": {}},
        ),
        patch("backend.core.vcl_generator.validate_destructive_token"),
        patch("backend.config.config_path", return_value="/nonexistent-cfg-path"),
        patch("backend.provision._sync_crontab", side_effect=RuntimeError("cron /etc/crontab failed")),
        patch.object(provision_mod, "_invalidate_service_credentials"),
        patch(
            "backend.provision.perform_teardown",
            side_effect=RuntimeError("teardown failed Fastly-Key: demofastly22 /root/key"),
        ),
    ):
        resp_td = provision_mod.provision_teardown(
            _fake_request(None),
            ProvisionTeardownRequest(service_id="svc-td", token="tok", remove_cron=True),
        )
        out_td = asyncio.run(_collect_sse(resp_td))
    assert "/etc/crontab" not in out_td
    assert "demofastly22" not in out_td
    assert "/root/key" not in out_td

    # 3) FOS credential probe fallback error message
    fake_s3 = MagicMock()
    fake_s3.list_objects_v2.side_effect = RuntimeError("probe failed https://u:demopass33@s3.local /etc/certs.pem")
    with patch.object(provision_mod, "_get_fos_s3_client", return_value=fake_s3):
        fos_out = provision_mod.provision_check_fos(
            CheckFosRequest(
                bucket="demo-bucket",
                region="us-east-1",
                access_key="demo-ak",
                secret_key="demo-sk",
            )
        )
    fos_text = json.dumps(fos_out)
    assert "demopass33" not in fos_text
    assert "/etc/certs.pem" not in fos_text

    # 4) provision_check_config logging & cdn error branches
    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=RuntimeError("fastly fail Fastly-Key: demofastly44 /etc/fastly.ini"),
    ):
        chk_out = provision_mod.provision_check_config(
            authorization="Bearer tok",
            service_id="svc-chk",
            cdn_service_id="cdn-1",
            bucket="demo-bucket",
        )
    chk_text = json.dumps(chk_out)
    assert "demofastly44" not in chk_text
    assert "/etc/fastly.ini" not in chk_text
    assert "[redacted]" in chk_text and "<path>" in chk_text

    # 5) orchestrate.py rollback_err status_cb message
    cb_messages: list[str] = []
    fake_state = FeatureState(
        service_id="svc-orch",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="us-east.object.fastlystorage.app",
        logging_enabled=True,
    )
    fake_diff = MagicMock()
    fake_diff.is_empty.return_value = False
    fake_diff.snippets_to_remove = []
    fake_lock = MagicMock()
    fake_lock.__enter__ = MagicMock(return_value=None)
    fake_lock.__exit__ = MagicMock(return_value=False)
    with (
        patch.object(orch_mod, "acquire_vcl_lock", return_value=fake_lock),
        patch.object(orch_mod, "_load_or_bootstrap_feature_state", return_value=fake_state),
        patch.object(orch_mod, "_fetch_active_version", return_value=1),
        patch.object(orch_mod, "_fetch_snippets", return_value=[]),
        patch.object(orch_mod, "_fetch_logging_endpoints", return_value=[]),
        patch.object(orch_mod, "_fetch_backends", return_value=[]),
        patch.object(orch_mod, "_fetch_dictionaries", return_value=[]),
        patch.object(orch_mod, "compute_diff", return_value=fake_diff),
        patch.object(orch_mod, "_clone_active_version", return_value=2),
        patch.object(
            orch_mod,
            "_apply_diff",
            side_effect=RuntimeError("apply failed postgresql://u:demopass55@h/db /etc/vcl.conf"),
        ),
    ):
        with pytest.raises(RuntimeError):
            orch_mod.reconcile_vcl_state("svc-orch", "tok", dry_run=False, status_cb=cb_messages.append)
    joined_cb = "\n".join(cb_messages)
    assert "demopass55" not in joined_cb
    assert "/etc/vcl.conf" not in joined_cb
    assert "[redacted]" in joined_cb and "<path>" in joined_cb


def test_w1_4_alert_model_bounds_and_preview_lookback_clamping() -> None:
    """Alert model must enforce bounds on window_min, comparison_period_min, baseline_period_days,
    and _preview_alert_series must clamp lookback_mins and baseline days."""
    from datetime import UTC, datetime

    from pydantic import ValidationError

    base = dict(name="Bound Check", service_id="svc-b", metric="5xx", operator=">", threshold=1.0, window_min=5)
    with pytest.raises(ValidationError):
        Alert(**{**base, "window_min": 0})
    with pytest.raises(ValidationError):
        Alert(**{**base, "window_min": 10081})
    with pytest.raises(ValidationError):
        Alert(**{**base, "comparison_period_min": 0})
    with pytest.raises(ValidationError):
        Alert(**{**base, "comparison_period_min": 43201})
    with pytest.raises(ValidationError):
        Alert(**{**base, "baseline_period_days": 0})
    with pytest.raises(ValidationError):
        Alert(**{**base, "baseline_period_days": 91})

    # Even with max valid window_min + comparison_period_min (10080 + 43200 + 60 = 53340 > 43200),
    # _preview_alert_series must clamp lookback_mins to <= 720 * 60 (43200).
    captured_params: list[dict] = []

    class _FakeCH:
        def execute(self, sql, params=None):
            captured_params.append(dict(params or {}))
            return []

    anchor = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
    rel_alert = Alert(
        **{
            **base,
            "evaluation_type": "relative_increase",
            "window_min": 10080,
            "comparison_period_min": 43200,
        }
    )
    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_FakeCH()),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
        patch.object(alerts_mod.svcconfig, "get_status", return_value={"latest_log_at": anchor.isoformat()}),
    ):
        alerts_mod._preview_alert_series("svc-b", {"name": "svc-b"}, "tbl", rel_alert, lookback_hours=720)
    assert captured_params, "expected ClickHouse execute call"
    first_delta_mins = (captured_params[0]["end"] - captured_params[0]["start"]).total_seconds() / 60.0
    assert first_delta_mins <= 720 * 60

    # Also verify anomaly_zscore clamps baseline days to [1, 90] even if a non-validated object is passed.
    captured_params.clear()
    fake_z_alert = SimpleNamespace(
        metric="5xx",
        window_min=5,
        evaluation_type="anomaly_zscore",
        evaluation_scope="all",
        comparison_period_min=None,
        status_codes=None,
        baseline_period_days=9999,
        zscore_threshold=3.0,
    )
    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_FakeCH()),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
        patch.object(alerts_mod.svcconfig, "get_status", return_value={"latest_log_at": anchor.isoformat()}),
    ):
        alerts_mod._preview_alert_series("svc-b", {"name": "svc-b"}, "tbl", fake_z_alert, lookback_hours=24)  # type: ignore[arg-type]
    assert len(captured_params) >= 2
    z_days = (captured_params[1]["end"] - captured_params[1]["start"]).total_seconds() / 86400.0
    assert z_days <= 90.0


def test_w1_5_sessions_endpoint_scrubs_ua_and_scrub_ip_text_handles_pct_encoded_ipv6(monkeypatch) -> None:
    """POST /api/sessions must scrub embedded IPs in ua for analyst_no_pii, and scrub_ip_text must
    scrub percent-encoded IPv6 (%3A / %3a) without corrupting non-IP text."""
    from backend.core.share_db import scrub_ip_text
    from backend.models.analytics import SessionsRequest

    # 1) Percent-encoded IPv6 in scrub_ip_text
    url_upper = "/api/v1?peer=2001%3Adb8%3Aabcd%3A%3A9&ts=12%3A34%3A56&tag=foo%3Abar%3Abaz"
    scrubbed_upper = scrub_ip_text(url_upper)
    assert "2001%3Adb8%3Aabcd%3A%3A9" not in scrubbed_upper
    assert "abcd%3A%3A9" not in scrubbed_upper
    assert "ts=12%3A34%3A56" in scrubbed_upper
    assert "tag=foo%3Abar%3Abaz" in scrubbed_upper

    url_lower = "/trace?src=2001%3adb8%3a%3a1"
    scrubbed_lower = scrub_ip_text(url_lower)
    assert "2001%3adb8%3a%3a1" not in scrubbed_lower
    assert "2001:db8::" in scrubbed_lower

    # 2) POST /api/sessions ua scrubbing for analyst_no_pii
    fake_hs_result = {
        "sessions": [
            {
                "ip": "203.0.113.xxx",
                "ja4": "t13d1516h2_8daaf6152771_e5627efa2ab1",
                "country": "US",
                "ua": "CustomBot/1.0 (client 198.51.100.77; v6 2001:db8:abcd::9; enc 2001%3Adb8%3Aef01%3A%3A5)",
                "session_start": "2026-11-25T10:00:00Z",
                "session_end": "2026-11-25T10:05:00Z",
                "req_count": 10,
                "flagged": False,
            }
        ],
        "total": 1,
        "page": 1,
        "limit": 50,
        "has_rtt": False,
        "has_ja4": True,
        "has_edge": True,
    }
    fake_ctx = SimpleNamespace(
        service_id="svc-sess",
        analyst_session=_analyst_no_pii(["svc-sess"]),
        resolve_window=lambda r: ("2026-11-25T00:00:00Z", "2026-11-25T23:59:59Z"),
    )
    with (
        patch("backend.high_scale.sessions.sessions_endpoint", return_value=fake_hs_result),
        patch.object(scoring_mod, "require_high_scale", return_value=MagicMock()),
        patch.object(scoring_mod, "seal_session_token", return_value="sealed-tok-demo"),
    ):
        resp = scoring_mod.sessions_endpoint(SessionsRequest(service_id="svc-sess"), fake_ctx)  # type: ignore[arg-type]
    dumped = resp.model_dump_json()
    for leaked in ("198.51.100.77", "2001:db8:abcd::9", "2001%3Adb8%3Aef01%3A%3A5"):
        assert leaked not in dumped, f"Embedded IP {leaked} leaked in POST /api/sessions"


def test_w1_6_insights_cache_collapse_guarded_and_locked_semaphores_never_evicted() -> None:
    """cache_collapse_detail_endpoint must be async and acquire per-service semaphore,
    and _get_insights_sem must never evict an actively locked semaphore."""
    import inspect

    assert inspect.iscoroutinefunction(insights_mod.cache_collapse_detail_endpoint)

    # 1) Never evict actively locked semaphores at capacity
    async def _check_no_locked_eviction() -> None:
        with insights_mod._insights_sems_lock:
            insights_mod._insights_sems.clear()
        orig_max = insights_mod._INSIGHTS_SEMS_MAXSIZE
        try:
            insights_mod._INSIGHTS_SEMS_MAXSIZE = 2
            sem_a = insights_mod._get_insights_sem("svc-lock-a")
            sem_b = insights_mod._get_insights_sem("svc-lock-b")
            await sem_a.acquire()
            await sem_b.acquire()
            try:
                _ = insights_mod._get_insights_sem("svc-lock-c")
                assert insights_mod._insights_sems.get("svc-lock-a") is sem_a
                assert insights_mod._insights_sems.get("svc-lock-b") is sem_b
            finally:
                sem_a.release()
                sem_b.release()
        finally:
            insights_mod._INSIGHTS_SEMS_MAXSIZE = orig_max
            with insights_mod._insights_sems_lock:
                insights_mod._insights_sems.clear()

    asyncio.run(_check_no_locked_eviction())

    # 2) cache_collapse_detail_endpoint serializes under _get_insights_sem(service_id)
    from backend.models.analytics import CacheCollapseDetailRequest

    async def _check_detail_uses_sem() -> None:
        with insights_mod._insights_sems_lock:
            insights_mod._insights_sems.clear()
        sem = insights_mod._get_insights_sem("svc-cc-sem")
        await sem.acquire()
        called = False

        def _fake_hs_detail(*_args, **_kwargs):
            nonlocal called
            called = True
            return {"data": {}}

        fake_ctx = SimpleNamespace(service_id="svc-cc-sem", analyst_session=None)
        req = CacheCollapseDetailRequest(service_id="svc-cc-sem", url="/x", baseline_hours=24, window_size_hrs=1)
        with (
            patch.object(insights_mod, "record_insights_request"),
            patch.object(insights_mod, "require_high_scale", return_value=MagicMock()),
            patch.object(insights_mod, "hs_detail", side_effect=_fake_hs_detail),
        ):
            task = asyncio.create_task(insights_mod.cache_collapse_detail_endpoint(req, fake_ctx))  # type: ignore[arg-type]
            await asyncio.sleep(0.05)
            assert not called, "cache_collapse_detail_endpoint bypassed the per-service semaphore!"
            sem.release()
            await task
            assert called

    asyncio.run(_check_detail_uses_sem())


# ── Iteration 3 Wave 2 Verifier Hardening Regressions ─────────────────────────


def test_w2_1_url_creds_regex_linear_time_no_redos() -> None:
    """_URL_CREDS_RE must run in linear time (<50ms on 40K adversarial inputs) without ReDoS."""
    import time

    from backend.utils.telemetry.logging_config import redact_error_text

    # Sanity check: valid URL credentials are still redacted
    assert (
        redact_error_text("connect postgresql://dbuser:demopass00@db.internal:5432/fla")
        == "connect postgresql://dbuser:[redacted]@db.internal:5432/fla"
    )

    # Adversarial inputs that triggered O(N^2) backtracking in the unanchored scheme regex
    payload_alpha = "a" * 40_000
    t0 = time.perf_counter()
    out_alpha = redact_error_text(payload_alpha)
    dt_alpha = time.perf_counter() - t0
    assert out_alpha == payload_alpha
    assert dt_alpha < 0.05, f"ReDoS regression on alpha payload: {dt_alpha:.4f}s"

    payload_url = "https://" + "a" * 40_000 + ":" + "b" * 40_000
    t1 = time.perf_counter()
    out_url = redact_error_text(payload_url)
    dt_url = time.perf_counter() - t1
    assert out_url == payload_url
    assert dt_url < 0.05, f"ReDoS regression on unclosed URL payload: {dt_url:.4f}s"


def test_w2_2_scrub_ip_text_handles_pct_encoded_delimiters() -> None:
    """scrub_ip_text must scrub percent-encoded IPv6 and embedded IPv4 preceded by percent-encoded delimiters."""
    from backend.core.share_db import scrub_ip_text

    for delim in ("%3D", "%2C", "%20", "%2F", "%3A"):
        raw_v6 = f"/trace?q=ip{delim}2001%3Adb8%3Aabcd%3A%3A9&ok=1"
        scrubbed = scrub_ip_text(raw_v6)
        assert "2001%3Adb8%3Aabcd%3A%3A9" not in scrubbed, f"Failed to scrub IPv6 after {delim}: {scrubbed!r}"
        assert "abcd%3A%3A9" not in scrubbed, f"Partial IPv6 leaked after {delim}: {scrubbed!r}"
        assert "2001:db8:abcd::" in scrubbed

    raw_v4 = "/proxy?hdr=for%20198.51.100.7%2C203.0.113.42&ts=12%3A34%3A56"
    scrubbed_v4 = scrub_ip_text(raw_v4)
    assert "198.51.100.7" not in scrubbed_v4, f"IPv4 after %20 leaked: {scrubbed_v4!r}"
    assert "203.0.113.42" not in scrubbed_v4, f"IPv4 after %2C leaked: {scrubbed_v4!r}"
    assert "ts=12%3A34%3A56" in scrubbed_v4


def test_w2_3_teardown_removes_reset_client_ip_snippet() -> None:
    """remove_logging_endpoint(remove_logging=True) must remove the priority -100 Reset Client IP snippet."""
    from contextlib import contextmanager

    deleted_paths: list[str] = []

    @contextmanager
    def _fake_draft_version(service_id, token, **kwargs):
        yield 7

    def _fake_fastly(method, path, token=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [
                {"name": "Fastly Log Analysis Reset Client IP"},
                {"name": "Fastly Log Analytics - vcl_recv"},
                {"name": "user-custom-snippet"},
            ]
        if method == "GET":
            return []
        if method == "DELETE":
            deleted_paths.append(path)
            return None
        return {}

    with (
        patch.object(fastly_api, "get_active_version", return_value=6),
        patch.object(fastly_api, "list_log_endpoints", return_value=[]),
        patch.object(fastly_api, "draft_version", _fake_draft_version),
        patch.object(fastly_api, "fastly", _fake_fastly),
    ):
        fastly_api.remove_logging_endpoint(
            "svc-td-reset",
            "ep-main",
            "tok-demo",
            remove_logging=True,
            remove_rum=False,
        )

    assert any("Fastly%20Log%20Analysis%20Reset%20Client%20IP" in p for p in deleted_paths), (
        f"Reset Client IP snippet was orphaned on teardown; deleted={deleted_paths!r}"
    )
    assert not any("user-custom-snippet" in p for p in deleted_paths)


def test_w2_4a_orchestrate_cdn_and_infra_reconcile_redact_status_cb(tmp_path) -> None:
    """reconcile_cdn_service_state and reconcile_infrastructure must redact errors in status_cb."""
    from backend.provision.declarative import orchestrate as orch_mod

    # 1) reconcile_cdn_service_state rollback_err
    cdn_cb: list[str] = []
    cfg_file = tmp_path / "svc-log.json"
    cfg_file.write_text(json.dumps({"fos_proxy": {"service_id": "cdn-1", "domain": "https://cdn.example.com"}}))
    fake_lock = MagicMock()
    fake_lock.__enter__ = MagicMock(return_value=None)
    fake_lock.__exit__ = MagicMock(return_value=False)
    fake_diff = MagicMock()
    fake_diff.is_empty.return_value = False
    with (
        patch("backend.config.config_path", return_value=cfg_file),
        patch.object(orch_mod, "acquire_vcl_lock", return_value=fake_lock),
        patch.object(orch_mod.fastly_integration, "fetch_active_version", return_value=1),
        patch.object(orch_mod, "fastly", return_value={"content": ""}),
        patch.object(orch_mod, "_fetch_snippets", return_value=[]),
        patch.object(orch_mod, "_fetch_backends", return_value=[]),
        patch.object(orch_mod, "_fetch_dictionaries", return_value=[]),
        patch.object(orch_mod, "compute_diff", return_value=fake_diff),
        patch.object(orch_mod, "_clone_active_version", return_value=2),
        patch.object(
            orch_mod,
            "_apply_diff",
            side_effect=RuntimeError("cdn apply failed postgresql://u:demopass66@h/db /etc/cdn.conf"),
        ),
    ):
        with pytest.raises(RuntimeError):
            orch_mod.reconcile_cdn_service_state("svc-log", "tok", dry_run=False, status_cb=cdn_cb.append)
    cdn_joined = "\n".join(cdn_cb)
    assert "demopass66" not in cdn_joined
    assert "/etc/cdn.conf" not in cdn_joined
    assert "[redacted]" in cdn_joined and "<path>" in cdn_joined

    # 2) reconcile_infrastructure err
    infra_cb: list[str] = []
    fake_desired = SimpleNamespace(compute_scorer=SimpleNamespace(domains=["demo.global.ssl.fastly.net"]))
    with (
        patch.object(orch_mod, "build_desired_state", return_value=fake_desired),
        patch.object(orch_mod, "discover_current_state", return_value={"compute_scorer": None, "kv_stores": {}}),
        patch("backend.config.snapshot_config", return_value=json.dumps({"service_id": "svc-log"})),
        patch(
            "backend.provision.opaque_names.generate_opaque_scorer_domain",
            side_effect=RuntimeError("infra failed Fastly-Key: demofastly77 /etc/infra.conf"),
        ),
    ):
        fake_desired.compute_scorer.domains = []
        with pytest.raises(RuntimeError):
            orch_mod.reconcile_infrastructure("svc-log", "tok", dry_run=False, status_cb=infra_cb.append)
    infra_joined = "\n".join(infra_cb)
    assert "demofastly77" not in infra_joined
    assert "/etc/infra.conf" not in infra_joined
    assert "[redacted]" in infra_joined and "<path>" in infra_joined


def test_w2_4b_orchestrator_provision_and_teardown_redact_events() -> None:
    """provision() and perform_teardown() in orchestrator.py must redact raw exceptions in yielded events."""
    from backend.provision import orchestrator as orch_core

    # 1) perform_teardown warning events
    state = {
        "logging_service_id": "svc-td-orch",
        "provisioning": {"endpoint_name": "ep-1"},
        "fos_bucket_name": "demo-bucket",
        "fos_region": "us-east-1",
    }
    opts = {
        "remove_logging": True,
        "remove_rum": False,
        "remove_cdn": False,
        "remove_bucket": False,
        "remove_cloud_files": True,
        "remove_scoring": False,
        "remove_fos_tokens": False,
        "fos_key_ids": ["key-1"],
    }
    with (
        patch.object(
            orch_core,
            "remove_logging_endpoint",
            side_effect=RuntimeError("rm ep failed Fastly-Key: demofastly81 /etc/ep.conf"),
        ),
        patch.object(
            orch_core,
            "delete_fos_access_key",
            side_effect=RuntimeError("rm key failed postgresql://u:demopass82@h/db /etc/key.conf"),
        ),
        patch.object(
            orch_core,
            "ensure_fos_access_key",
            side_effect=RuntimeError("cloud del failed Authorization: Bearer demobearer83 /var/lib/cloud.json"),
        ),
    ):
        events = list(orch_core.perform_teardown(state, "tok", opts))
    serialized = json.dumps(events)
    for secret in (
        "demofastly81",
        "/etc/ep.conf",
        "demopass82",
        "/etc/key.conf",
        "demobearer83",
        "/var/lib/cloud.json",
    ):
        assert secret not in serialized, f"Leaked {secret!r} in perform_teardown events"
    assert "[redacted]" in serialized and "<path>" in serialized

    # 2) provision() _PreflightError event
    with (
        patch.object(orch_core, "validate_log_format", return_value=[]),
        patch.object(
            orch_core,
            "logging_endpoint_role_error",
            return_value="preflight Fastly-Key: demofastly84 /etc/preflight.conf",
        ),
    ):
        prov_events = list(orch_core.provision({"logging_service_id": "svc-pre", "admin_token": "tok"}))
    prov_serialized = json.dumps(prov_events)
    assert "demofastly84" not in prov_serialized
    assert "/etc/preflight.conf" not in prov_serialized
    assert "[redacted]" in prov_serialized and "<path>" in prov_serialized


def test_w2_4c_run_with_events_redacts_error_event() -> None:
    """run_with_events(raise_on_error=False) in cli.py must redact error messages."""
    from backend.provision.cli import run_with_events

    def _boom(status_cb=None):
        raise RuntimeError("cli failed Fastly-Key: demofastly99 /etc/cli.conf")

    events = list(run_with_events(_boom, raise_on_error=False))
    err_events = [e for e in events if e.get("type") == "error"]
    assert len(err_events) == 1
    msg = err_events[0]["message"]
    assert "demofastly99" not in msg
    assert "/etc/cli.conf" not in msg
    assert "[redacted]" in msg and "<path>" in msg


def test_w2_4d_provision_router_defensively_redacts_stream_and_reconcile_result() -> None:
    """provision.py must defensively redact streamed event messages and reconcile result.error."""
    from backend.models.common import ProvisionReconcileRequest, ProvisionTeardownRequest

    # 1) provision_teardown defensive redaction of yielded perform_teardown event messages
    with (
        patch(
            "backend.config.load_config",
            return_value={"fos_bucket": "b", "fos_region": "us-east-1", "provisioning": {}},
        ),
        patch("backend.core.vcl_generator.validate_destructive_token"),
        patch("backend.config.config_path", return_value="/nonexistent-cfg-path"),
        patch("backend.provision._sync_crontab"),
        patch.object(provision_mod, "_invalidate_service_credentials"),
        patch(
            "backend.provision.perform_teardown",
            return_value=iter(
                [{"type": "status", "message": "raw teardown Fastly-Key: demofastly01 /etc/td_raw.conf"}]
            ),
        ),
    ):
        resp_td = provision_mod.provision_teardown(
            _fake_request(None),
            ProvisionTeardownRequest(service_id="svc-td-raw", token="tok", remove_cron=False),
        )
        out_td = asyncio.run(_collect_sse(resp_td))
    assert "demofastly01" not in out_td
    assert "/etc/td_raw.conf" not in out_td
    assert "[redacted]" in out_td and "<path>" in out_td

    # 2) provision_reconcile redaction of result.error
    fake_result = SimpleNamespace(
        service_id="svc-rec",
        activated_version=None,
        changes_applied={},
        duration_ms=12,
        error="reconcile err postgresql://u:demopass02@h/db /etc/rec_res.conf",
    )
    with (
        patch.object(provision_mod, "_validate_provision_vcl_inputs"),
        patch("backend.config.load_config", return_value=None),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state", return_value=fake_result),
    ):
        resp_rec = provision_mod.provision_reconcile(ProvisionReconcileRequest(service_id="svc-rec", token="tok-demo"))
        out_rec = asyncio.run(_collect_sse(resp_rec))
    assert "demopass02" not in out_rec
    assert "/etc/rec_res.conf" not in out_rec
    assert "[redacted]" in out_rec and "<path>" in out_rec
