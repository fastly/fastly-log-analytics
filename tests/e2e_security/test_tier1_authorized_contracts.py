"""Tier 1 — Core Security & Authorized Feature Coverage (37 tests across F1-F6)."""

from __future__ import annotations

import json
import re
import socket
import time
import urllib.error
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.core import share_db
from backend.core.fastly.client import load_vcl, validate_rum_custom_condition
from backend.provision.declarative.models import FeatureState, ScoringConfig
from backend.provision.declarative.vcl_builder import (
    desired_backends,
    generate_consolidated_snippet,
)
from tests.e2e_security.cloud_harness import (
    DEFAULT_CDN_KEY,
    DEFAULT_FRONT_KEY,
    FIXTURE_22_SID,
    CloudTestServer,
    build_cloud_capture_vcl_wrapper,
    build_front_vcl_wrapper,
    cloud_desired_snippets,
    cloud_issue_cdn_cookie,
    make_flp_cdn_cookie,
)
from tests.e2e_security.conftest import (
    RecordingClickHouseClient,
    make_client,
    probe_sse_status,
    provision_analyst_session,
    run_falco_lint,
    run_falco_test,
)

pytestmark = pytest.mark.no_postgres


def _wrap_hs_cdn_vcl(cdn_auth_entries: dict[str, str]) -> str:
    items = "\n".join(f'  "{k}": "{v}",' for k, v in cdn_auth_entries.items())
    return f"""backend F_fos_backend {{ .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }}
table cdn_auth {{
{items}
}}
table fos_credentials {{
  "access_key": "test-sentinel-fos-ak-0001",
  "secret_key": "test-sentinel-fos-sk-0001",
  "bucket": "bucket-a",
  "region": "us-east-1",
}}
sub vcl_recv {{
  set req.backend = F_fos_backend;
}}
{load_vcl(rate_limiting=True)}
"""


def _wrap_hs_customer_vcl(state: FeatureState) -> str:
    blocks = [
        """backend F_origin { .host = "127.0.0.1"; .port = "8080"; }
backend F_fos_origin { .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }
backend F_session_scorer { .host = "scorer.edgecompute.app"; .port = "443"; }
backend ssl_shield_iad_va_us { .host = "127.0.0.2"; .port = "443"; }

table fos_credentials {
  "access_key": "test-sentinel-fos-ak-0001",
  "secret_key": "test-sentinel-fos-sk-0001",
  "bucket": "bucket-a",
  "region": "us-east-1",
}

sub vcl_recv {
  set req.backend = F_origin;
}"""
    ]
    for hook in ("vcl_recv", "vcl_miss", "vcl_pass", "vcl_fetch", "vcl_deliver", "vcl_error"):
        snip = generate_consolidated_snippet(state, hook)
        if snip:
            snip = re.sub(
                r"fastly_try_select_shield\(\s*[A-Za-z0-9_]+\s*,\s*([A-Za-z0-9_]+)\s*\)",
                r"\1",
                snip,
            )
            blocks.append(f"sub {hook} {{\n{snip}\n}}")
    return "\n\n".join(blocks) + "\n"


# ============================================================================
# Area F1 — Edge/Proxy Trust, Loopback Admin, mTLS Admin Gateway & /metrics (6)
# ============================================================================


def test_f1_raw_loopback_peer_without_proxy_headers_is_admin() -> None:
    client = make_client("127.0.0.1")
    r_boot = client.get("/api/bootstrap")
    assert r_boot.status_code == 200
    payload = r_boot.json()
    assert payload["settings"]["is_remote_analyst"] is False
    service_ids = {s["service_id"] for s in payload["services"]}
    assert {"svc-a", "svc-b"}.issubset(service_ids)

    r_admin = client.get("/api/admin/queries")
    assert r_admin.status_code == 200


def test_f1_mtls_admin_gateway_token_grants_admin_and_blocks_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gw_secret = "test-sentinel-admin-gateway-token-32bytes"
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", gw_secret)

    # Pod peer without gateway token is blocked
    denied = make_client("10.244.1.15").get("/api/admin/queries")
    assert denied.status_code in {401, 403}

    # Pod peer with valid mTLS admin-gateway token is recognized as Admin
    allowed = make_client(
        "10.244.1.15",
        headers={"X-Admin-Gateway-Token": gw_secret},
    ).get("/api/admin/queries")
    assert allowed.status_code == 200


def test_f1_mtls_admin_gateway_post_provision_reconcile_sse_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gw_secret = "test-sentinel-admin-gateway-token-32bytes"
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", gw_secret)

    monkeypatch.setattr(
        "backend.provision.declarative.reconciler.reconcile_vcl_state",
        lambda service_id, token, dry_run=False, status_cb=None: SimpleNamespace(
            service_id=service_id,
            activated_version=7,
            changes_applied=["updated snippet"],
            duration_ms=12,
            error=None,
        ),
    )
    client = make_client(
        "10.244.1.20",
        headers={
            "X-Admin-Gateway-Token": gw_secret,
            "Accept": "text/event-stream",
        },
    )
    resp = client.post(
        "/api/provision/reconcile",
        json={"service_id": "svc-a", "token": "test-sentinel-fastly-token-0001"},
    )
    assert resp.status_code == 200
    assert "reconciliation_result" in resp.text
    assert '"activated_version": 7' in resp.text or '"activated_version":7' in resp.text


def test_f1_direct_loopback_metrics_and_health_return_200() -> None:
    client = make_client("127.0.0.1")
    r_health = client.get("/api/health")
    assert r_health.status_code == 200

    r_metrics = client.get("/metrics")
    assert r_metrics.status_code == 200
    assert "python_" in r_metrics.text or "http_" in r_metrics.text or "HELP" in r_metrics.text


def test_f1_valid_active_service_cookie_selects_scoped_service(
    ch_recorder: RecordingClickHouseClient,
    isolated_e2e_security_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "backend.config.get_active_service_id",
        isolated_e2e_security_state.orig_get_active_service_id,
    )
    client = make_client(
        "127.0.0.1",
        cookies={"fla_active_service": "svc-b"},
    )
    resp = client.post(
        "/api/query",
        headers={"x-service-id": "svc-b"},
        json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
    )
    assert resp.status_code == 200
    assert any("svc-b" in str(settings or {}) or "svc-b" in sql for sql, _, settings in ch_recorder.calls)


def test_f1_authorized_same_origin_web_vitals_post_succeeds() -> None:
    client = make_client(
        "127.0.0.1",
        headers={"Origin": "http://testserver", "Referer": "http://testserver/dashboard"},
    )
    resp = client.post(
        "/api/web-vitals",
        json={
            "name": "LCP",
            "value": 412.5,
            "rating": "good",
            "id": "v4-1700000000000-1234567890123",
            "page": "/dashboard",
        },
    )
    assert resp.status_code in {200, 204}


# ============================================================================
# Area F2 — Share Lifecycle, Sanitized Analyst Bootstrap & Live Control Room (6)
# ============================================================================


def test_f2_active_share_passcode_login_and_session_validation() -> None:
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=False,
        client_ip="203.0.113.41",
    )
    client = make_client("203.0.113.41")
    resp = client.post(
        "/api/share/login",
        headers=bundle.headers(),
        json={"email": bundle.email, "passcode": bundle.passcode},
    )
    assert resp.status_code == 200
    assert "analyst_session_id=" in resp.headers.get("set-cookie", "")


def test_f2_share_claim_token_flow_issues_scoped_session() -> None:
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.42",
    )
    raw_token = share_db.create_claim_token(bundle.invite_id)
    client = make_client("203.0.113.42")
    resp = client.post(
        f"/api/share/claim/{raw_token}",
        headers=bundle.headers(),
        json={},
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["email"] == bundle.email

    # One-time claim token cannot be replayed
    replay = client.post(f"/api/share/claim/{raw_token}", headers=bundle.headers(), json={})
    assert replay.status_code == 404

    login_resp = client.post(
        "/api/share/login",
        headers=bundle.headers(),
        json={"email": bundle.email, "passcode": bundle.passcode},
    )
    assert login_resp.status_code == 200
    assert "analyst_session_id=" in login_resp.headers.get("set-cookie", "")


def test_f2_admin_bootstrap_returns_full_service_inventory() -> None:
    client = make_client("127.0.0.1")
    resp = client.get("/api/bootstrap")
    assert resp.status_code == 200
    body = resp.json()
    assert body["settings"]["is_remote_analyst"] is False
    by_id = {svc["service_id"]: svc for svc in body["services"]}
    assert "svc-a" in by_id and "svc-b" in by_id


def test_f2_valid_whitelisted_analyst_bootstrap_returns_sanitized_public_config() -> None:
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.50",
        ip_whitelist="203.0.113.0/24",
    )
    client = make_client("203.0.113.50", cookies=bundle.cookies())
    resp = client.get("/api/bootstrap", headers=bundle.headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["settings"]["is_remote_analyst"] is True
    assert [s["service_id"] for s in body["services"]] == ["svc-a"]
    svc = body["services"][0]
    for forbidden_key in ("fos_bucket", "cdn_url", "cdn_service_id", "cron_stats"):
        assert forbidden_key not in svc or svc[forbidden_key] in (None, "")
    rum_cfg = svc.get("rum") or {}
    assert "cid_salt" not in rum_cfg


def test_f2_open_window_analyst_permitted_on_realtime_seed() -> None:
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        query_start_time=None,
        query_end_time=None,
        client_ip="203.0.113.51",
    )
    client = make_client("203.0.113.51", cookies=bundle.cookies())
    resp = client.get("/api/services/svc-a/realtime-seed", headers=bundle.headers())
    assert resp.status_code == 200
    assert resp.json()["ticks"] == []


def test_f2_open_window_analyst_permitted_on_live_sse_streams(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    future_end = (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    past_start = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        query_start_time=past_start,
        query_end_time=future_end,
        client_ip="203.0.113.52",
    )
    status_rt, _ = probe_sse_status(
        "/api/services/svc-a/realtime-stream",
        peer_ip="203.0.113.52",
        headers=bundle.headers(),
        cookies=bundle.cookies(),
    )
    assert status_rt == 200

    status_err, _ = probe_sse_status(
        "/api/services/svc-a/control-room/error-stream",
        peer_ip="203.0.113.52",
        headers=bundle.headers(),
        cookies=bundle.cookies(),
    )
    assert status_err == 200


# ============================================================================
# Area F3 — Multi-Service Scoping, mask_ips, fdt SQL, Downloads & CSV (7)
# ============================================================================


def test_f3_admin_vs_unmasked_analyst_vs_masked_analyst_ip_scoping(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    sample_row = {
        "event_id": "evt-1",
        "event_timestamp": datetime(2026, 6, 12, 0, 10, tzinfo=UTC),
        "ingested_at": datetime(2026, 6, 12, 0, 11, tzinfo=UTC),
        "owner_epoch": 1,
        "client_ip": "203.0.113.55",
        "method": "GET",
        "url": "/checkout",
        "host": "shop.example.com",
        "status": 200,
        "pop": "IAD",
        "country": "US",
        "asn": 54113,
        "cache_status": "HIT",
        "resp_body_size": 1024,
        "time_elapsed_usec": 2500,
        "is_error": 0,
        "is_bot": 0,
        "ua": "Mozilla/5.0",
        "protocol": "HTTP/2",
        "tls_version": "TLSv1.3",
        "custom_fields": {},
    }
    ch_recorder.set_handler(lambda sql, params, settings: [dict(sample_row)])

    req_body = {
        "start_time": "2026-06-12T00:00:00Z",
        "end_time": "2026-06-12T01:00:00Z",
        "limit": 10,
    }
    admin_resp = make_client("127.0.0.1").post(
        "/api/high-scale/services/svc-a/request-facts",
        headers={"X-Fastly-Service-Id": "svc-a"},
        json=req_body,
    )
    assert admin_resp.status_code == 200
    assert admin_resp.json()["rows"][0]["client_ip"] == "203.0.113.55"

    unmasked = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=False,
        client_ip="203.0.113.60",
        email="unmasked@tenant.example.test",
    )
    r_unmasked = make_client("203.0.113.60", cookies=unmasked.cookies()).post(
        "/api/high-scale/services/svc-a/request-facts",
        headers=unmasked.headers({"X-Fastly-Service-Id": "svc-a"}),
        json=req_body,
    )
    assert r_unmasked.status_code == 200
    assert r_unmasked.json()["rows"][0]["client_ip"] == "203.0.113.55"

    masked = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.61",
        email="masked@tenant.example.test",
    )
    r_masked = make_client("203.0.113.61", cookies=masked.cookies()).post(
        "/api/high-scale/services/svc-a/request-facts",
        headers=masked.headers({"X-Fastly-Service-Id": "svc-a"}),
        json=req_body,
    )
    assert r_masked.status_code == 200
    masked_ip = r_masked.json()["rows"][0]["client_ip"]
    assert masked_ip != "203.0.113.55"


def test_f3_analyst_bound_to_svc_a_queries_svc_a_cleanly(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    ch_recorder.set_handler(lambda sql, params, settings: [{"n": 42}])
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.62",
    )
    resp = make_client("203.0.113.62", cookies=bundle.cookies()).post(
        "/api/query",
        headers=bundle.headers({"X-Fastly-Service-Id": "svc-a"}),
        json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == [{"n": 42}]


def test_f3_fdt_canonical_req_id_and_all_five_fla_target_sql_queries(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    ch_recorder.set_handler(lambda sql, params, settings: [{"lt_n": 1}])
    client = make_client(
        "127.0.0.1",
        headers={"X-Fastly-Service-Id": "svc-a", "X-FLA-Harness": "fdt"},
    )
    fdt_queries = [
        # Canonical fdt.load query from PROJECT.md § Interface Contracts
        "SELECT req_id, count() AS n FROM logs WHERE timestamp >= '2026-06-12T00:00:00Z' AND req_id LIKE 'fdlt-%' GROUP BY req_id",
        # 1. observed_breakdown_sql (UNION ALL + formatDateTime)
        (
            "SELECT custom_fields['lt_run_id'] AS lt_rid, 'status' AS lt_dim, custom_fields['status'] AS lt_key, count() AS lt_n "
            "FROM logs WHERE custom_fields['lt_run_id'] IN ('fdlt-run-1') AND timestamp >= '2026-06-12T00:00:00Z' GROUP BY lt_rid, lt_key "
            "UNION ALL "
            "SELECT custom_fields['lt_run_id'] AS lt_rid, 'minute' AS lt_dim, "
            "formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:00Z') AS lt_key, count() AS lt_n "
            "FROM logs WHERE custom_fields['lt_run_id'] IN ('fdlt-run-1') AND timestamp >= '2026-06-12T00:00:00Z' GROUP BY lt_rid, lt_key"
        ),
        # 2. request_id_stats_sql (uniqExact + countIf)
        (
            "SELECT '1' AS lt_nonce, custom_fields['lt_run_id'] AS lt_rid, count() AS lt_rows, "
            "uniqExact(custom_fields['lt_request_id']) AS lt_unique, "
            "countIf(custom_fields['lt_request_id'] = '') AS lt_blank "
            "FROM logs WHERE custom_fields['lt_run_id'] IN ('fdlt-run-1') AND timestamp >= '2026-06-12T00:00:00Z' GROUP BY lt_rid"
        ),
        # 3. out_of_run_rows_sql
        (
            "SELECT custom_fields['lt_run_id'] AS lt_rid, count() AS lt_n FROM logs "
            "WHERE timestamp >= '2026-06-12T00:00:00Z' AND custom_fields['lt_run_id'] != 'fdlt-run-1' GROUP BY lt_rid"
        ),
        # 4. request_ids_page_sql
        (
            "SELECT custom_fields['lt_request_id'] AS lt_req, count() AS lt_n, "
            "min(custom_fields['status']) AS lt_smin, max(custom_fields['status']) AS lt_smax, min(custom_fields['pop']) AS lt_pop "
            "FROM logs WHERE custom_fields['lt_run_id'] = 'fdlt-run-1' AND timestamp >= '2026-06-12T00:00:00Z' "
            "AND custom_fields['lt_request_id'] > '' GROUP BY lt_req ORDER BY lt_req LIMIT 5000"
        ),
        # 5. blank_ids_sql
        (
            "SELECT count() AS lt_n FROM logs WHERE custom_fields['lt_run_id'] = 'fdlt-run-1' "
            "AND timestamp >= '2026-06-12T00:00:00Z' AND custom_fields['lt_request_id'] = ''"
        ),
    ]
    for sql in fdt_queries:
        resp = client.post("/api/query", json={"sql": sql, "dataset": "logs"})
        assert resp.status_code == 200, f"fdt query failed ({resp.status_code}): {sql}\n{resp.text}"


def test_f3_fdt_rum_vitals_and_rum_errors_console_queries_succeed(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    ch_recorder.set_handler(lambda sql, params, settings: [{"n": 5}])
    client = make_client(
        "127.0.0.1",
        headers={"X-Fastly-Service-Id": "svc-a", "X-FLA-Harness": "fdt"},
    )
    for ds, tbl in (("client_vitals", "client_vitals"), ("client_errors", "client_errors")):
        resp = client.post(
            "/api/query",
            json={
                "sql": f"SELECT count() AS n FROM {tbl} WHERE timestamp >= '2026-06-12T00:00:00Z'",
                "dataset": ds,
            },
        )
        assert resp.status_code == 200, f"RUM query failed for {ds}: {resp.text}"


def test_f3_authorized_admin_download_folder_raw_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "backend.core.fos_client.list_objects_under_prefix",
        lambda source, prefix, max_keys=1000: [],
        raising=False,
    )
    monkeypatch.setattr(
        "backend.core.fos_client._get_fos_client",
        lambda source: SimpleNamespace(
            get_paginator=lambda op: SimpleNamespace(paginate=lambda **kw: [{"Contents": []}])
        ),
        raising=False,
    )
    client = make_client("127.0.0.1")
    resp = client.get("/api/download-folder?root=raw&prefix=request")
    assert resp.status_code == 200
    assert "application/zip" in resp.headers.get("content-type", "")


def test_f3_safe_csv_export_preserves_benign_strings_and_numbers(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    ch_recorder.set_handler(
        lambda sql, params, settings: [
            {
                "event_id": "e1",
                "event_timestamp": datetime(2026, 6, 12, 0, 15, tzinfo=UTC),
                "ingested_at": datetime(2026, 6, 12, 0, 16, tzinfo=UTC),
                "owner_epoch": 1,
                "client_ip": "198.51.100.8",
                "method": "GET",
                "url": "/catalog/items/sku-900",
                "host": "shop.example.com",
                "status": 200,
                "pop": "SJC",
                "country": "US",
                "asn": 54113,
                "cache_status": "HIT",
                "resp_body_size": 512,
                "time_elapsed_usec": 1200,
                "is_error": 0,
                "is_bot": 0,
                "ua": "Mozilla/5.0",
                "protocol": "HTTP/2",
                "tls_version": "TLSv1.3",
                "custom_fields": {},
            }
        ]
    )
    client = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"})
    resp = client.post(
        "/api/dashboard/raw/csv",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "columns": [],
        },
    )
    assert resp.status_code == 200
    assert "/catalog/items/sku-900" in resp.text


def test_f3_authorized_saved_views_roundtrip_for_admin_and_unmasked_analyst() -> None:
    admin_client = make_client("127.0.0.1")
    for name, fjson in (
        ("Status 5xx", json.dumps({"status": {"mode": "include", "values": ["500"]}})),
        ("VIP Client IP", json.dumps({"ip": {"mode": "include", "values": ["203.0.113.99"]}})),
    ):
        r_create = admin_client.post(
            "/api/views/",
            json={"service_id": "svc-a", "name": name, "filters_json": fjson},
        )
        assert r_create.status_code in {200, 201}

    unmasked = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=False,
        client_ip="203.0.113.65",
    )
    r_views = make_client("203.0.113.65", cookies=unmasked.cookies()).get(
        "/api/views/svc-a",
        headers=unmasked.headers(),
    )
    assert r_views.status_code == 200
    names = {v["name"] for v in r_views.json()}
    assert {"Status 5xx", "VIP Client IP"}.issubset(names)


# ============================================================================
# Area F4 — High-Scale Fastly/VCL, CDN secret_key/secret, RUM & Domain Check (6)
# ============================================================================


def test_f4_load_vcl_lints_and_authorizes_secret_key_stripping_header_upstream() -> None:
    main_vcl = _wrap_hs_cdn_vcl({"secret_key": "test-sentinel-cdn-secret-0001"})
    lint = run_falco_lint(main_vcl)
    assert lint.returncode == 0, f"falco lint failed:\n{lint.stdout}\n{lint.stderr}"

    test_vcl = """
// @scope: recv
sub test_authorized_secret_key {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.12", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/2026-06-12.log.gz";
  set req.http.x-fastly-key = "test-sentinel-cdn-secret-0001";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"falco test failed:\n{res.stdout}\n{res.stderr}"


def test_f4_load_vcl_legacy_secret_dictionary_key_fallback_works() -> None:
    main_vcl = _wrap_hs_cdn_vcl({"secret": "legacy-sentinel-cdn-secret-0002"})  # gitleaks:allow
    test_vcl = """
// @scope: recv
sub test_legacy_secret_fallback_get_and_head {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.13", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "HEAD";
  set req.url = "/processed/request/part-0.parquet";
  set req.http.x-fastly-key = "legacy-sentinel-cdn-secret-0002"; # gitleaks:allow
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"legacy secret fallback failed:\n{res.stdout}\n{res.stderr}"


def test_f4_high_scale_rum_vcl_permits_rum_js_and_faro_sdk_on_f_fos_origin() -> None:
    state = FeatureState(
        service_id="svc-a",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="us-east-1.object.fastlystorage.app",
        logging_enabled=True,
        rum_enabled=True,
        fos_bucket="bucket-a",
        fos_region="us-east-1",
        cdn_shield="none",
        faro_version="1.19.0",
    )
    main_vcl = _wrap_hs_customer_vcl(state)
    lint = run_falco_lint(main_vcl)
    assert lint.returncode == 0, f"HS customer VCL lint failed:\n{lint.stdout}\n{lint.stderr}"

    test_vcl = """
// @scope: recv
sub test_rum_js_recv {
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/js/rum.js";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.http.X-FOS-Request, "1");
  assert.equal(req.backend, F_fos_origin);
}

// @scope: miss
sub test_faro_sdk_miss_signs_on_f_fos_origin {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_fos_origin;
  set req.url = "/js/faro-sdk.js";
  set bereq.url = "/js/faro-sdk.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.contains(bereq.http.Authorization, "AWS4-HMAC-SHA256");
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"HS RUM VCL test failed:\n{res.stdout}\n{res.stderr}"


def test_f4_valid_public_domain_check_with_global_ip_succeeds() -> None:
    def _fake_getaddrinfo(host: str, port: Any, *args: Any, **kwargs: Any) -> list[Any]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("151.101.1.57", 443))]

    fastly_unclaimed_err = urllib.error.HTTPError(
        url="https://my-new-cdn.global.ssl.fastly.net",
        code=500,
        msg="Domain Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=BytesIO(b"Fastly error: unknown domain. Please check that this domain has been added to a service."),
    )
    with (
        patch("socket.getaddrinfo", side_effect=_fake_getaddrinfo),
        patch("urllib.request.OpenerDirector.open", side_effect=fastly_unclaimed_err),
    ):
        resp = make_client("127.0.0.1").get("/api/provision/check-domain?prefix=my-new-cdn&is_custom=false")
    assert resp.status_code == 200
    assert resp.json()["available"] is True


def test_f4_valid_custom_vcl_condition_and_lt_custom_fields_pass_validation() -> None:
    err = validate_rum_custom_condition('req.http.Host == "shop.example.com" && req.url.ext != "png"')
    assert err is None

    client = make_client("127.0.0.1")
    for hdr_expr in ("req.http.X-LT-Run-Id", "req.http.X-LT-Request-Id"):
        resp = client.post(
            "/api/services/svc-a/custom-fields/validate-vcl",
            json={"vcl_log_expression": hdr_expr, "collection_stage": "edge"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["valid"] is True


def test_f4_session_scorer_backend_tls_config_enforces_ssl_check_cert() -> None:
    state = FeatureState(
        service_id="svc-a",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="us-east-1.object.fastlystorage.app",
        logging_enabled=True,
        fos_bucket="bucket-a",
        fos_region="us-east-1",
        scoring=ScoringConfig(
            enabled=True,
            domain="scorer.edgecompute.app",
            request_secret="test-sentinel-scorer-sec-0001",
        ),
    )
    backends = {b.name: b for b in desired_backends(state)}
    assert "session_scorer" in backends
    scorer = backends["session_scorer"]
    assert scorer.use_ssl is True
    assert scorer.ssl_check_cert is True
    assert scorer.ssl_hostname == "scorer.edgecompute.app"
    assert scorer.override_host == "scorer.edgecompute.app"


# ============================================================================
# Area F5 — compute/cloud Front VCL, Crons, flp_cdn Refresh & Capture RUM (6)
# ============================================================================


def test_f5_cloud_front_vcl_lints_and_serves_unauth_version_and_root(
    cloud_server: CloudTestServer,
) -> None:
    main_vcl, extra = build_front_vcl_wrapper()
    lint = run_falco_lint(main_vcl, extra_files=extra)
    assert lint.returncode == 0, f"front/main.vcl lint failed:\n{lint.stdout}\n{lint.stderr}"

    test_vcl = """
// @scope: recv
sub test_unauth_version_and_root {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.20", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/version";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.http.x-flp-front-key, "test-sentinel-front-key-0001"); # gitleaks:allow
}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"front/main.vcl test failed:\n{res.stdout}\n{res.stderr}"

    r_ver = cloud_server.request("GET", "/version")
    assert r_ver.status_code == 200
    assert "build_sha" in r_ver.json()

    r_root = cloud_server.request("GET", "/")
    assert r_root.status_code == 200


def test_f5_cloud_front_vcl_and_shell_handle_tick_and_flush_crons(
    cloud_server: CloudTestServer,
) -> None:
    main_vcl, extra = build_front_vcl_wrapper()
    test_vcl = """
// @scope: recv
sub test_tick_and_flush_headers {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.21", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/_tick?ignored=1";
  set req.http.Cookie = "flp_session=should-be-stripped";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.http.x-flp-tick, "1");
  assert.equal(req.http.x-flp-front-key, "test-sentinel-front-key-0001"); # gitleaks:allow
  assert.is_notset(req.http.Cookie);
}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"/_tick VCL test failed:\n{res.stdout}\n{res.stderr}"

    for cron_path in ("/_tick", "/_flush"):
        r = cloud_server.request(
            "GET",
            cron_path,
            front_key=DEFAULT_FRONT_KEY,
            extra_headers={"x-flp-tick": "1"},
        )
        assert r.status_code == 200, f"{cron_path} returned {r.status_code}: {r.text}"


def test_f5_cloud_front_vcl_authorizes_fos_with_valid_x_fastly_key() -> None:
    main_vcl, extra = build_front_vcl_wrapper()
    test_vcl = f"""
// @scope: recv
sub test_fos_direct_valid_key {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.22", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/processed/request/2026-06-12.parquet";
  set req.http.x-fastly-key = "{DEFAULT_CDN_KEY}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
  assert.is_notset(req.http.x-flp-front-key);
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"Cloud FOS valid x-fastly-key test failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_authorizes_fos_pq_with_valid_flp_cdn_hmac() -> None:
    main_vcl, extra = build_front_vcl_wrapper()
    sess = "valid-session-token-for-hmac-test"
    cdn_cookie = make_flp_cdn_cookie(
        session_cookie=sess,
        cdn_key=DEFAULT_CDN_KEY,
        sid=FIXTURE_22_SID,
        role="admin",
        exp=int(time.time()) + 240,
    )
    test_vcl = f"""
// @scope: recv
sub test_fos_pq_valid_flp_cdn {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.23", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={cdn_cookie}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/{FIXTURE_22_SID}/processed/request/part-000.parquet");
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"Valid flp_cdn test failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_expired_or_missing_flp_cdn_falls_through_cleanly_without_429() -> None:
    main_vcl, extra = build_front_vcl_wrapper()
    sess = "browser-session-needing-cdn-refresh"
    expired_cdn = make_flp_cdn_cookie(
        session_cookie=sess,
        cdn_key=DEFAULT_CDN_KEY,
        sid=FIXTURE_22_SID,
        role="analyst",
        exp=1700000000,  # naturally expired in the past
    )
    # 25 consecutive expired-cookie AND 25 consecutive missing-flp_cdn requests must ALL pass through to /api/file without penaltybox 429
    expired_calls = "\n  ".join(
        [
            """testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-001.parquet";
  set req.http.Cookie = "flp_session="""
            + sess
            + "; flp_cdn="
            + expired_cdn
            + """";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-auth-fail);"""
        ]
        * 25
    )
    missing_calls = "\n  ".join(
        [
            f"""testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-002.parquet";
  set req.http.Cookie = "flp_session={sess}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-auth-fail);"""
        ]
        * 25
    )
    test_vcl = f"""
// @scope: recv
sub test_expired_flp_cdn_never_penalizes {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.24", "127.0.0.1"));
  {expired_calls}
  assert.contains(req.url, "/api/file?key=");
}}

// @scope: recv
sub test_missing_flp_cdn_never_penalizes {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.25", "127.0.0.1"));
  {missing_calls}
  assert.contains(req.url, "/api/file?key=");
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"Expired flp_cdn refresh fallback failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_capture_rum_vcl_allows_rum_js_and_faro_sdk_on_f_fos_origin() -> None:
    snippets = cloud_desired_snippets(
        {
            "logging_enabled": True,
            "rum_enabled": True,
            "fos_region": "us-east-1",
            "cdn_shield": "none",
        }
    )
    main_vcl = build_cloud_capture_vcl_wrapper(snippets)
    lint = run_falco_lint(main_vcl)
    assert lint.returncode == 0, f"Cloud capture VCL lint failed:\n{lint.stdout}\n{lint.stderr}"

    test_vcl = """
// @scope: recv
sub test_cloud_rum_js_recv {
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/js/rum.js";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.http.X-FOS-Request, "1");
  assert.equal(req.backend, F_fos_origin);
}

// @scope: miss
sub test_cloud_rum_js_miss_signs {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_fos_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.contains(bereq.http.Authorization, "AWS4-HMAC-SHA256");
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"Cloud capture RUM test failed:\n{res.stdout}\n{res.stderr}"


# ============================================================================
# Area F6 — compute/cloud RBAC, /api/views, Admin/Logs Pages & Validators (6)
# ============================================================================


def test_f6_cloud_admin_harness_flow_views_crud_and_audit_logs(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)

    r_create = cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        harness_header=True,
        json_body={
            "service_id": sid,
            "name": "5xx Edge Errors",
            "filters_json": json.dumps({"status": {"mode": "include", "values": ["503"]}}),
        },
    )
    assert r_create.status_code == 201, r_create.text
    view_id = r_create.json()["id"]

    r_list = cloud_server.request(
        "GET",
        f"/api/views/{sid}",
        session_cookie=admin.session_cookie,
        harness_header=True,
    )
    assert r_list.status_code == 200
    assert any(v["id"] == view_id for v in r_list.json())

    r_del = cloud_server.request(
        "DELETE",
        f"/api/views/{view_id}",
        session_cookie=admin.session_cookie,
        harness_header=True,
    )
    assert r_del.status_code == 204

    r_audit = cloud_server.request(
        "GET",
        "/api/admin/audit-logs",
        session_cookie=admin.session_cookie,
        harness_header=True,
    )
    assert r_audit.status_code == 200
    assert len(r_audit.json()["entries"]) >= 1


def test_f6_cloud_admin_accesses_admin_and_logs_html_and_nested_rsc(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    for path in (
        "/admin",
        "/admin/__next.admin.txt",
        "/admin/share/__next.admin.share.txt",
        "/logs",
        "/logs/__next.logs.txt",
    ):
        r = cloud_server.request("GET", path, session_cookie=admin.session_cookie)
        assert r.status_code == 200, f"Admin denied on {path}: {r.status_code}"


def test_f6_cloud_analyst_with_pii_reads_all_saved_views_and_extents(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="PII Analyst")

    for name, fjson in (
        ("Public Cache Misses", json.dumps({"cache_status": {"mode": "include", "values": ["MISS"]}})),
        ("Client IP Investigation", json.dumps({"client_ip": {"mode": "include", "values": ["203.0.113.7"]}})),
    ):
        rc = cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            json_body={"service_id": sid, "name": name, "filters_json": fjson},
        )
        assert rc.status_code == 201

    rv = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=analyst.session_cookie)
    assert rv.status_code == 200
    assert {v["name"] for v in rv.json()} == {"Public Cache Misses", "Client IP Investigation"}

    rext = cloud_server.request("GET", "/api/log-extents", session_cookie=analyst.session_cookie)
    assert rext.status_code == 200
    assert rext.json()["configured"] is True
    issued = cloud_issue_cdn_cookie(session_cookie=analyst.session_cookie, role="analyst")
    assert issued["set_cookie"] is not None and issued["set_cookie"].startswith("flp_cdn=")


def test_f6_cloud_analyst_no_pii_reads_non_pii_saved_views_and_dashboard(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPII Analyst")

    rc = cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        json_body={
            "service_id": sid,
            "name": "POP Latency",
            "filters_json": json.dumps({"pop": {"mode": "include", "values": ["IAD"]}}),
        },
    )
    assert rc.status_code == 201

    rv = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=nopii.session_cookie)
    assert rv.status_code == 200
    assert [v["name"] for v in rv.json()] == ["POP Latency"]

    rdash = cloud_server.request("GET", "/dashboard/__next.dashboard.txt", session_cookie=nopii.session_cookie)
    assert rdash.status_code == 200


def test_f6_cloud_valid_custom_vcl_condition_accepted_on_service_patch(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    gen = cloud_server.service_generation(admin, sid)
    r_patch = cloud_server.request(
        "PATCH",
        f"/api/admin/services/{sid}",
        session_cookie=admin.session_cookie,
        json_body={
            "generation": gen,
            "custom_condition": 'req.http.Host == "www.example.com" && req.url.ext != "ico"',
        },
    )
    assert r_patch.status_code == 200, r_patch.text


def test_f6_cloud_valid_fdt_lt_custom_fields_pass_validation_and_config(
    cloud_server: CloudTestServer,
) -> None:
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    custom_fields = [
        {
            "name": "lt_run_id",
            "label": "Load-test run id",
            "vcl_log_expression": "req.http.X-LT-Run-Id",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "collection_stage": "edge",
            "enabled": True,
            "bytes_estimate": 40,
            "byte_limit": 192,
            "show_in_logs": False,
        },
        {
            "name": "lt_request_id",
            "label": "Load-test request id",
            "vcl_log_expression": "req.http.X-LT-Request-Id",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "collection_stage": "edge",
            "enabled": True,
            "bytes_estimate": 40,
            "byte_limit": 252,
            "show_in_logs": False,
        },
    ]
    for cf in custom_fields:
        rv = cloud_server.request(
            "POST",
            "/api/admin/log-fields/custom/validate",
            session_cookie=admin.session_cookie,
            json_body={"field": cf, "existing_names": []},
        )
        assert rv.status_code == 200, rv.text
        assert rv.json()["valid"] is True

    gen = cloud_server.service_generation(admin, sid)
    rcfg = cloud_server.request(
        "PATCH",
        "/api/admin/log-fields/config",
        session_cookie=admin.session_cookie,
        json_body={
            "generation": gen,
            "service_id": sid,
            "log_fields": {
                "groups": ["A", "C"],
                "custom_fields": custom_fields,
                "field_overrides": {},
                "field_limits": {},
            },
        },
    )
    assert rcfg.status_code == 200, rcfg.text
