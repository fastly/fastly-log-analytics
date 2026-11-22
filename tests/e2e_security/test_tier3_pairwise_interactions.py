"""Tier 3 — Cross-Feature Security Interactions (12 pairwise interaction tests)."""

from __future__ import annotations

import http.client
import json
import re
import socket
import time
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.core import share_db
from backend.core.fastly.client import load_vcl
from backend.provision.declarative.models import FeatureState
from backend.provision.declarative.vcl_builder import generate_consolidated_snippet
from tests.e2e_security.cloud_harness import (
    CloudTestServer,
    build_cloud_capture_vcl_wrapper,
    build_front_vcl_wrapper,
    cloud_desired_snippets,
    cloud_issue_cdn_cookie,
    make_flp_cdn_cookie,
)
from tests.e2e_security.conftest import (
    SHARED_18_PII_KEYS,
    RecordingClickHouseClient,
    make_client,
    probe_sse_status,
    provision_analyst_session,
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


def test_t3_01_hs_analyst_mask_ips_bootstrap_ip_ua_whitelist_and_secret_stripping() -> None:
    """Pairwise 1: Analyst mask_ips=True x /api/bootstrap x IP/UA whitelist x secret non-leakage."""
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.101",
        ip_whitelist="203.0.113.100/30",
    )
    ok_resp = make_client("203.0.113.101", cookies=bundle.cookies()).get(
        "/api/bootstrap",
        headers=bundle.headers(),
    )
    assert ok_resp.status_code == 200
    ok_json = ok_resp.json()
    assert [s["service_id"] for s in ok_json["services"]] == ["svc-a"]
    svc = ok_json["services"][0]
    for stripped in ("fos_bucket", "cdn_url", "cdn_service_id", "cron_stats"):
        assert stripped not in svc or svc[stripped] in (None, "")
    assert "cid_salt" not in (svc.get("rum") or {})

    # Mismatched client IP outside 203.0.113.100/30 must NOT receive svc-a config on /api/bootstrap
    bad_ip_resp = make_client("198.51.100.55", cookies=bundle.cookies()).get(
        "/api/bootstrap",
        headers=bundle.headers({"Fastly-Client-IP": "198.51.100.55"}),
    )
    assert bad_ip_resp.status_code in {200, 401, 403}
    if bad_ip_resp.status_code == 200:
        assert bad_ip_resp.json()["services"] == [], "Mismatched IP leaked services on /api/bootstrap"

    # Mismatched User-Agent fingerprint must NOT receive svc-a config on /api/bootstrap
    bad_ua_resp = make_client("203.0.113.101", cookies=bundle.cookies()).get(
        "/api/bootstrap",
        headers=bundle.headers({"User-Agent": "Mozilla/5.0 (Windows NT 10.0) Firefox/129.0"}),
    )
    assert bad_ua_resp.status_code in {200, 401, 403}
    if bad_ua_resp.status_code == 200:
        assert bad_ua_resp.json()["services"] == [], "Mismatched UA leaked services on /api/bootstrap"

    # sharing_enabled=False must NOT receive svc-a config on /api/bootstrap
    share_db.set_setting("sharing_enabled", "false")
    off_resp = make_client("203.0.113.101", cookies=bundle.cookies()).get(
        "/api/bootstrap",
        headers=bundle.headers(),
    )
    assert off_resp.status_code in {200, 401, 403}
    if off_resp.status_code == 200:
        assert off_resp.json()["services"] == [], "Stopped sharing leaked services on /api/bootstrap"


def test_t3_02_hs_mask_ips_18_pii_saved_views_scoring_events_and_metrics(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """Pairwise 2: High-Scale mask_ips=True x all 18 shared PII keys in saved views x scoring events x /metrics."""
    admin = make_client("127.0.0.1")
    r_clean = admin.post(
        "/api/views/",
        json={
            "service_id": "svc-a",
            "name": "Clean Country View",
            "filters_json": json.dumps([{"field": "country", "op": "in", "values": ["US"]}]),
        },
    )
    assert r_clean.status_code in {200, 201}
    for idx, pii_key in enumerate(SHARED_18_PII_KEYS):
        sentinel = f"PII_SENTINEL_VAL_{idx:02d}_203.0.113.{idx + 10}"
        r = admin.post(
            "/api/views/",
            json={
                "service_id": "svc-a",
                "name": f"PII View {pii_key}",
                "filters_json": json.dumps([{"field": pii_key, "op": "in", "values": [sentinel]}]),
            },
        )
        assert r.status_code in {200, 201}

    ch_recorder.set_handler(
        lambda sql, params, settings: [
            {
                "edge_sid": "sid-raw-123",
                "ts": datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
                "timestamp": datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
                "event_timestamp": datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
                "ip": "203.0.113.222",
                "client_ip": "203.0.113.222",
                "method": "GET",
                "url": "/login",
                "status": 200,
                "ua": "Mozilla/5.0",
                "pop": "IAD",
                "country": "US",
            }
        ]
    )

    # Positive control: Admin receives non-empty raw session events containing 203.0.113.222
    r_admin_score = admin.get("/api/services/svc-a/scoring/sessions/sid-raw-123/events")
    assert r_admin_score.status_code == 200, (
        f"Expected 200 OK for admin scoring session events, got {r_admin_score.status_code}: {r_admin_score.text}"
    )
    assert "203.0.113.222" in r_admin_score.text, (
        f"Positive control failed: mocked ClickHouse row did not populate raw IP in events: {r_admin_score.text}"
    )

    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.105",
    )
    analyst = make_client("203.0.113.105", cookies=bundle.cookies())

    r_views = analyst.get("/api/views/svc-a", headers=bundle.headers())
    assert r_views.status_code == 200
    assert "Clean Country View" in r_views.text, (
        f"Positive non-PII view missing from /api/views/svc-a response: {r_views.text}"
    )
    assert "PII_SENTINEL_VAL_" not in r_views.text, (
        f"mask_ips=True leaked one or more of the 18 PII keys in /api/views: {r_views.text}"
    )

    r_boot = analyst.get("/api/bootstrap", headers=bundle.headers())
    assert r_boot.status_code == 200
    assert "PII_SENTINEL_VAL_" not in r_boot.text, "mask_ips=True leaked PII saved view filters in /api/bootstrap"

    r_score = analyst.get(
        "/api/services/svc-a/scoring/sessions/sid-raw-123/events",
        headers=bundle.headers(),
    )
    assert r_score.status_code == 403, (
        f"Expected 403 pii_policy_violation on scoring/sessions/{{sid}}/events for mask_ips=True, "
        f"got {r_score.status_code}: {r_score.text}"
    )
    assert "pii_policy_violation" in r_score.text, (
        f"Expected pii_policy_violation error code in 403 body, got: {r_score.text}"
    )
    assert "203.0.113.222" not in r_score.text, (
        "scoring/sessions/{sid}/events leaked unmasked IP to mask_ips=True analyst"
    )

    assert analyst.get("/metrics", headers=bundle.headers()).status_code in {401, 403}


def test_t3_03_hs_historical_window_allows_scoped_historical_query_but_blocks_live_sse(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """Pairwise 3: Historical time-window invite queries historical slice cleanly but gets 403 on live SSE."""
    ch_recorder.set_handler(
        lambda sql, params, settings: [
            {
                "event_id": "hist-1",
                "event_timestamp": datetime(2026, 6, 12, 0, 30, tzinfo=UTC),
                "ingested_at": datetime(2026, 6, 12, 0, 31, tzinfo=UTC),
                "owner_epoch": 1,
                "client_ip": "198.51.100.44",
                "method": "GET",
                "url": "/archive",
                "host": "shop.example.com",
                "status": 200,
                "pop": "IAD",
                "country": "US",
                "asn": 54113,
                "cache_status": "HIT",
                "resp_body_size": 256,
                "time_elapsed_usec": 800,
                "is_error": 0,
                "is_bot": 0,
                "ua": "Mozilla/5.0",
                "protocol": "HTTP/2",
                "tls_version": "TLSv1.3",
                "custom_fields": {},
            }
        ]
    )
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        query_start_time="2026-06-12T00:00:00Z",
        query_end_time="2026-06-12T01:00:00Z",
        client_ip="203.0.113.106",
    )
    analyst = make_client("203.0.113.106", cookies=bundle.cookies())

    r_hist = analyst.post(
        "/api/high-scale/services/svc-a/request-facts",
        headers=bundle.headers({"X-Fastly-Service-Id": "svc-a"}),
        json={
            "start_time": "2026-06-12T00:10:00Z",
            "end_time": "2026-06-12T00:50:00Z",
            "limit": 10,
        },
    )
    assert r_hist.status_code == 200
    assert r_hist.json()["rows"][0]["client_ip"] != "198.51.100.44"

    assert analyst.get("/api/services/svc-a/realtime-seed", headers=bundle.headers()).status_code == 403
    for sse in ("/api/services/svc-a/realtime-stream", "/api/services/svc-a/control-room/error-stream"):
        status, _ = probe_sse_status(sse, peer_ip="203.0.113.106", headers=bundle.headers(), cookies=bundle.cookies())
        assert status == 403


def test_t3_04_hs_active_share_stop_sharing_panic_ssr_marker_and_proxy_spoofing() -> None:
    """Pairwise 4: Active share session x Stop Sharing/Panic x SSR X-Remote-Analyst: 1 x Fastly-Client-IP spoof."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.107")
    admin = make_client("127.0.0.1")
    assert admin.post("/api/admin/share/panic", json={}).status_code == 200

    spoof_hdrs = bundle.headers(
        {
            "X-Proxied-By-Caddy": "true",
            "X-Remote-Analyst": "1",
            "Fastly-Client-IP": "127.0.0.1",
            "X-Forwarded-For": "127.0.0.1",
        }
    )
    ssr_client = make_client("127.0.0.1", cookies=bundle.cookies())
    assert ssr_client.get("/api/views/svc-a", headers=spoof_hdrs).status_code in {401, 403}
    assert ssr_client.get("/api/admin/queries", headers=spoof_hdrs).status_code in {401, 403}

    r_boot = ssr_client.get("/api/bootstrap", headers=spoof_hdrs)
    assert r_boot.status_code in {200, 401, 403}, f"Unexpected /api/bootstrap status: {r_boot.status_code}"
    if r_boot.status_code == 200:
        assert r_boot.json()["services"] == []

    r_login = ssr_client.post(
        "/api/share/login",
        headers=spoof_hdrs,
        json={"email": bundle.email, "passcode": bundle.passcode},
    )
    assert r_login.status_code in {401, 403}


def test_t3_05_hs_sql_console_string_literals_multi_service_scoping_and_wellknown_bots(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """Pairwise 5: SQL Console string literals x multi-service scoping x _wellknown_bots parameterization."""
    bot_ua = "Mozilla/5.0 (compatible; Bingbot/2.0) ' UNION ALL SELECT 1 --"
    ch_recorder._http = SimpleNamespace(_transport=object())  # type: ignore[attr-defined]

    def _handler(sql: str, params: dict[str, Any] | None, settings: dict[str, Any] | None) -> list[dict[str, Any]]:
        if "dimension='ua'" in sql:
            return [{"ua": bot_ua}]
        if "FROM request_facts" in sql:
            return [{"ua": bot_ua, "ip": "203.0.113.99", "cnt": 4}]
        return [{"n": 2}]

    ch_recorder.set_handler(_handler)
    client = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"})

    r_sql = client.post(
        "/api/query",
        json={
            "sql": "SELECT count() AS n FROM logs WHERE url LIKE '%FROM logs%' AND custom_fields['env'] = 'prod'",
            "dataset": "logs",
        },
    )
    assert r_sql.status_code == 200
    last_sql, _, last_settings = ch_recorder.calls[-1]
    assert "'%FROM logs%'" in last_sql
    assert "svc-b" not in str(last_settings)

    r_bots = client.post(
        "/api/security/aggregates",
        json={
            "start_time": "2026-06-12T00:05:00Z",
            "end_time": "2026-06-12T00:55:00Z",
            "sections": ["wellknown_bots"],
        },
    )
    assert r_bots.status_code == 200
    for sql, _params, _ in ch_recorder.calls:
        assert "' UNION ALL SELECT 1 --" not in sql


def test_t3_06_hs_cdn_load_vcl_legacy_vs_secret_key_ipv6_64_penaltybox_valid_key_lockout() -> None:
    """Pairwise 6: High-Scale load_vcl x legacy 'secret' vs 'secret_key' x IPv6 /64 lockout of valid key."""
    vcl_src = load_vcl(rate_limiting=True)
    m_pb = re.search(r"penaltybox_has[^\n]+\{\s*error\s+(\d+)", vcl_src, re.DOTALL)
    expected_err = int(m_pb.group(1)) if m_pb else 429

    for dict_entries, active_secret in (
        ({"secret_key": "curr-sentinel-01"}, "curr-sentinel-01"),
        ({"secret": "legacy-sentinel-02"}, "legacy-sentinel-02"),  # gitleaks:allow
    ):
        main_vcl = _wrap_hs_cdn_vcl(dict_entries)
        bad_burst = "\n  ".join(
            [
                """testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/f.gz";
  set req.http.x-fastly-key = "bad-guess";
  testing.call_subroutine("vcl_recv");"""
            ]
            * 25
        )
        test_vcl = f"""
// @scope: recv
sub test_subnet_lockout_and_clean_subnet_allow {{
  testing.inject_variable("client.ip", std.str2ip("2001:db8:cafe:42::10", "::1"));
  {bad_burst}
  testing.fixed_access_rate(1000);
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.x-fastly-key = "bad-guess";
  testing.call_subroutine("vcl_recv");
  testing.fixed_access_rate(0);

  // Another host in SAME /64 with VALID secret is locked out BEFORE secret check
  testing.inject_variable("client.ip", std.str2ip("2001:db8:cafe:42::9999", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/f.gz";
  set req.http.x-fastly-key = "{active_secret}";
  testing.call_subroutine("vcl_recv");
  assert.error({expected_err});

  // Clean /64 subnet with VALID secret succeeds and strips x-fastly-key
  testing.inject_variable("client.ip", std.str2ip("2001:db8:cafe:43::1", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/f.gz";
  set req.http.x-fastly-key = "{active_secret}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
}}
"""
        res = run_falco_test(main_vcl, test_vcl)
        assert res.returncode == 0, f"Pairwise 6 failed for {dict_entries}:\n{res.stdout}\n{res.stderr}"


def test_t3_07_customer_rum_vcl_hs_and_cloud_parity_shield_restarts_and_x_fos_request_spoof() -> None:
    """Pairwise 7: Customer RUM VCL parity (High-Scale & Cloud) x shield/restarts x X-FOS-Request: 1 spoofing."""
    hs_vcl = _wrap_hs_customer_vcl(
        FeatureState(
            service_id="svc-a",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            logging_enabled=False,
            rum_enabled=True,
            fos_bucket="bucket-a",
            fos_region="us-east-1",
            cdn_shield="iad-va-us",
            faro_version="1.19.0",
        )
    )
    cloud_vcl = build_cloud_capture_vcl_wrapper(
        cloud_desired_snippets(
            {
                "logging_enabled": False,
                "rum_enabled": True,
                "fos_region": "us-east-1",
                "cdn_shield": "iad-va-us",
            }
        )
    )
    parity_test = """
// @scope: recv
sub test_edge_spoof_on_private_api_scrubbed {
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/api/private";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.X-FOS-Request);
  assert.equal(req.backend, F_origin);
}

// @scope: miss
sub test_shield_hop_never_double_signs {
  testing.inject_variable("req.backend.is_shield", true);
  set req.backend = F_fos_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_non_shield_fos_origin_rum_js_signs {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_fos_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.contains(bereq.http.Authorization, "AWS4-HMAC-SHA256");
}
"""
    for label, vcl_str in (("High-Scale", hs_vcl), ("Cloud", cloud_vcl)):
        res = run_falco_test(vcl_str, parity_test)
        assert res.returncode == 0, f"Pairwise 7 {label} RUM parity failed:\n{res.stdout}\n{res.stderr}"


def test_t3_08_cloud_logout_multi_cookie_clear_server_revocation_and_flp_cdn_binding(
    cloud_server: CloudTestServer,
) -> None:
    """Pairwise 8: Cloud POST /api/auth/logout x multi-Set-Cookie clearing x server revocation x short flp_cdn TTL & epoch/session binding."""
    admin = cloud_server.register_admin()
    cloud_server.provision_fixture_service(admin)

    r_ext = cloud_server.request("GET", "/api/log-extents", session_cookie=admin.session_cookie)
    assert r_ext.status_code == 200
    pre_logout_cdn = cloud_issue_cdn_cookie(
        session_cookie=admin.session_cookie,
        role="admin",
        auth_epoch=1,
    )
    assert pre_logout_cdn["set_cookie"] is not None and 180 <= pre_logout_cdn["ttl"] <= 300
    m_cdn = re.search(r"flp_cdn=([^;]+)", pre_logout_cdn["set_cookie"])
    assert m_cdn is not None
    pre_logout_cdn_val = m_cdn.group(1)

    old_sess = admin.session_cookie
    r_out = cloud_server.request("POST", "/api/auth/logout", session_cookie=old_sess, json_body={})
    assert r_out.status_code == 200
    sc_list = r_out.headers.get_list("set-cookie")
    for cname in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
        assert any(h.startswith(cname) and "Max-Age=0" in h for h in sc_list), (
            f"Missing clear header for {cname}: {sc_list}"
        )

    # Server-side session + pre-logout flp_cdn replay on GET /api/file must fail 401 with x-flp-auth-fail + cookie clear
    r_file_replay = cloud_server.request(
        "GET",
        "/api/file",
        raw_cookie=f"flp_session={old_sess}; flp_cdn={pre_logout_cdn_val}",
    )
    assert r_file_replay.status_code == 401
    assert r_file_replay.headers.get("x-flp-auth-fail") == "1"
    sc_replay = r_file_replay.headers.get_list("set-cookie")
    for cname in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
        assert any(h.startswith(cname) and "Max-Age=0" in h for h in sc_replay), (
            f"GET /api/file replay did not clear {cname}: {sc_replay}"
        )

    assert cloud_server.request("GET", "/api/log-extents", session_cookie=old_sess).status_code == 401
    assert cloud_server.request("GET", "/api/admin/audit-logs", session_cookie=old_sess).status_code == 401

    # Front VCL refuses direct /fos/* rewrite on both bumped auth_epoch (M7) and mismatched flp_session hash (M8)
    epoch2_vcl, epoch2_extra = build_front_vcl_wrapper(extra_front_auth={"auth_epoch": "2"})
    epoch2_cdn = make_flp_cdn_cookie(session_cookie=old_sess, exp=int(time.time()) + 240, epoch="2")
    r_vcl = run_falco_test(
        epoch2_vcl,
        f"""
// @scope: recv
sub test_t3_08_epoch_and_session_binding {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.108", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/f.parquet";
  set req.http.Cookie = "flp_session={old_sess}; flp_cdn={pre_logout_cdn_val}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.backend, F_compute);
  assert.contains(req.url, "/api/file?key=");
  assert.is_notset(req.http.x-flp-api-file);

  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/f.parquet";
  set req.http.Cookie = "flp_session=other-browser-session; flp_cdn={epoch2_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.backend, F_compute);
  assert.contains(req.url, "/api/file?key=");
  assert.is_notset(req.http.x-flp-api-file);
}}
""",
        extra_files=epoch2_extra,
    )
    assert r_vcl.returncode == 0, f"Pairwise 8 VCL binding failed:\n{r_vcl.stdout}\n{r_vcl.stderr}"


def test_t3_09_cloud_front_vcl_expired_flp_cdn_passthrough_vs_tampered_flp_cdn_penaltybox() -> None:
    """Pairwise 9: Cloud front/main.vcl x expired flp_cdn clean refresh vs tampered flp_cdn penaltybox."""
    main_vcl, extra = build_front_vcl_wrapper()
    sess = "active-browser-sess-1"
    expired_cdn = make_flp_cdn_cookie(session_cookie=sess, exp=1700000000)
    tampered_cdn = make_flp_cdn_cookie(session_cookie=sess, exp=int(time.time()) + 240, tamper_sig=True)

    expired_loop = "\n  ".join(
        [
            f"""testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/f.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={expired_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-auth-fail);"""
        ]
        * 20
    )
    deliver_10 = "\n  ".join(
        [
            """testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "198.51.100.112";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
        ]
        * 10
    )
    test_vcl = f"""
// @scope: recv
sub test_expired_clean_never_penalized_while_tampered_raises_629 {{
  // Honest client with expired flp_cdn never gets 618 or 629
  testing.inject_variable("client.ip", std.str2ip("198.51.100.111", "127.0.0.1"));
  {expired_loop}

  // Attacker with tampered HMAC immediately raises 629
  testing.inject_variable("client.ip", std.str2ip("198.51.100.112", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/f.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={tampered_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(629);
}}

// @scope: error
sub test_error_629_sets_auth_fail_marker {{
  set obj.status = 629;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.http.x-flp-auth-fail, "1");
}}

// @scope: deliver
sub test_tampered_auth_fail_adds_to_penaltybox {{
  {deliver_10}
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "198.51.100.112"));
}}

// @scope: recv
sub test_tampered_auth_fail_penalizes_across_fos_pq_and_signin {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.112", 15m);
  testing.inject_variable("client.ip", std.str2ip("198.51.100.112", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/f.parquet";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/sign-in";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"Pairwise 9 failed:\n{res.stdout}\n{res.stderr}"


def test_t3_10_cloud_views_rbac_and_analyst_no_pii_18_key_redaction_parity(
    cloud_server: CloudTestServer,
) -> None:
    """Pairwise 10: Cloud /api/views RBAC x AnalystNoPii 18-key PII redaction parity."""
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="Full Analyst")
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPII Analyst")

    rc_safe = cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        json_body={
            "service_id": sid,
            "name": "Safe Method Filter",
            "filters_json": json.dumps({"method": {"mode": "include", "values": ["GET"]}}),
        },
    )
    assert rc_safe.status_code == 201
    safe_id = rc_safe.json()["id"]

    for idx, pii_key in enumerate(SHARED_18_PII_KEYS):
        sentinel = f"CLOUD_PII_SENTINEL_{idx:02d}"
        rc = cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            json_body={
                "service_id": sid,
                "name": f"PII {pii_key}",
                "filters_json": json.dumps({pii_key: {"mode": "include", "values": [sentinel]}}),
            },
        )
        assert rc.status_code == 201

    # Analyst with PII sees all 19 views, cannot POST/DELETE
    r_analyst = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=analyst.session_cookie)
    assert r_analyst.status_code == 200
    assert len(r_analyst.json()) == 19
    assert (
        cloud_server.request("DELETE", f"/api/views/{safe_id}", session_cookie=analyst.session_cookie).status_code
        == 403
    )

    # AnalystNoPii never sees any of the 18 PII sentinel values, cannot POST/DELETE
    r_nopii = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=nopii.session_cookie)
    assert r_nopii.status_code == 200
    assert "Safe Method Filter" in r_nopii.text, (
        f"Positive non-PII view missing from Cloud AnalystNoPii response: {r_nopii.text}"
    )
    assert "CLOUD_PII_SENTINEL_" not in r_nopii.text, (
        f"Cloud AnalystNoPii leaked one or more of the 18 PII keys: {r_nopii.text}"
    )
    assert (
        cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=nopii.session_cookie,
            json_body={"service_id": sid, "name": "Nope", "filters_json": "{}"},
        ).status_code
        == 403
    )


def test_t3_11_cloud_nested_rsc_subpaths_and_forged_flp_session_cookie_clearing(
    cloud_server: CloudTestServer,
) -> None:
    """Pairwise 11: Cloud nested RSC subpaths (/admin/__next.*.txt, /logs/__next.*.txt) x forged flp_session clearing & penalty signal."""
    admin = cloud_server.register_admin()
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="Analyst")

    for rsc_path in ("/admin/__next.admin.txt", "/logs/__next.logs.txt"):
        r_role = cloud_server.request("GET", rsc_path, session_cookie=analyst.session_cookie)
        assert r_role.status_code == 403
        assert "SECRET_PAYLOAD" not in r_role.text

        r_forged = cloud_server.request("GET", rsc_path, session_cookie="forged-rsc-probe-cookie")
        assert r_forged.status_code in {302, 303, 401, 403}
        assert "SECRET_PAYLOAD" not in r_forged.text
        sc_forged = r_forged.headers.get_list("set-cookie")
        for cname in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
            assert any(h.startswith(cname) and "Max-Age=0" in h for h in sc_forged), (
                f"Forged flp_session on {rsc_path} did not clear {cname}: {sc_forged}"
            )

    r_api_forged = cloud_server.request(
        "GET",
        "/api/admin/audit-logs",
        session_cookie="forged-rsc-probe-cookie",
    )
    assert r_api_forged.status_code == 401
    assert r_api_forged.headers.get("x-flp-auth-fail") == "1"
    sc_api = r_api_forged.headers.get_list("set-cookie")
    for cname in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
        assert any(h.startswith(cname) and "Max-Age=0" in h for h in sc_api)


def test_t3_12_provisioning_admin_guard_check_domain_ssrf_and_vcl_breakout_defense() -> None:
    """Pairwise 12: Provisioning require_admin x check-domain redirect-to-loopback SSRF x VCL table.lookup defense."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.120")
    analyst = make_client("203.0.113.120", cookies=bundle.cookies())
    assert analyst.get(
        "/api/provision/check-domain?prefix=rebind.example.com&is_custom=true",
        headers=bundle.headers(),
    ).status_code in {401, 403}
    assert analyst.get("/api/services/svc-a/logging-settings", headers=bundle.headers()).status_code in {401, 403}

    admin = make_client("127.0.0.1")
    visited: list[str] = []

    class _FakeRawSocket:
        def __init__(self, raw_http: bytes) -> None:
            self._bio = BytesIO(raw_http)

        def makefile(self, mode: str = "rb", *args: Any, **kwargs: Any) -> BytesIO:
            return self._bio

        def close(self) -> None:
            pass

    def _https_getresponse(self: Any, *args: Any, **kwargs: Any) -> http.client.HTTPResponse:
        resp = http.client.HTTPResponse(
            _FakeRawSocket(  # type: ignore[arg-type]
                b"HTTP/1.1 301 Moved Permanently\r\n"
                b"Location: http://127.0.0.1:8000/api/admin/queries\r\n"
                b"Content-Length: 0\r\n\r\n"
            )
        )
        resp.begin()
        return resp

    def _forbid_loopback_redirect(self: Any) -> None:
        visited.append(f"http://{self.host}")
        raise AssertionError(f"Followed redirect to {self.host}")

    with (
        patch(
            "socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("151.101.1.57", 443))],
        ),
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(
            http.client.HTTPSConnection,
            "request",
            lambda self, m, u, *a, **k: visited.append(f"https://{self.host}{u}"),
        ),
        patch.object(http.client.HTTPSConnection, "getresponse", _https_getresponse),
        patch.object(http.client.HTTPConnection, "connect", _forbid_loopback_redirect),
    ):
        r_dom = admin.get("/api/provision/check-domain?prefix=redir.example.com&is_custom=true")
    assert not any("127.0.0.1" in u for u in visited)
    assert r_dom.status_code in {200, 400}
    if r_dom.status_code == 200:
        assert r_dom.json()["available"] is False

    r_vcl = admin.post(
        "/api/services/svc-a/custom-fields",
        json={
            "name": "exfil_secret",
            "label": "Exfil",
            "vcl_log_expression": 'table.lookup(cdn_auth, "secret_key", "")',
            "value_type": "string",
            "collection_stage": "edge",
        },
    )
    assert r_vcl.status_code in {400, 422}
