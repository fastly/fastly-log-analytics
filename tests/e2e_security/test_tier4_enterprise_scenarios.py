"""Tier 4 — Enterprise Multi-Tenant & Preserved fdt End-to-End Scenarios (E1-E6)."""

from __future__ import annotations

import http.client
import json
import re
import socket
import time
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend import config as svcconfig
from backend.core import fos_client, share_db
from backend.core.fastly.client import load_vcl
from backend.provision.declarative.models import FeatureState, ScoringConfig
from backend.provision.declarative.vcl_builder import (
    desired_backends,
    generate_consolidated_snippet,
)
from backend.utils import tunnel
from backend.utils.terraform_gen import generate_terraform
from tests.e2e_security.cloud_harness import (
    DEFAULT_CDN_KEY,
    DEFAULT_FRONT_KEY,
    FIXTURE_22_SID,
    CloudTestServer,
    build_front_vcl_wrapper,
    cloud_issue_cdn_cookie,
    make_flp_cdn_cookie,
)
from tests.e2e_security.conftest import (
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


def test_scenario_e1_multi_customer_service_isolation_under_active_attack(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """Scenario E1: Customer A historical masked analyst probes Customer B & Admin surfaces while spoofing loopback headers."""
    ch_recorder.set_handler(
        lambda sql, params, settings: [
            {
                "event_id": "e-a-1",
                "event_timestamp": datetime(2026, 6, 12, 0, 20, tzinfo=UTC),
                "ingested_at": datetime(2026, 6, 12, 0, 21, tzinfo=UTC),
                "owner_epoch": 1,
                "client_ip": "198.51.100.88",
                "method": "GET",
                "url": "/customer-a/home",
                "host": "a.example.com",
                "status": 200,
                "pop": "IAD",
                "country": "US",
                "asn": 54113,
                "cache_status": "HIT",
                "resp_body_size": 512,
                "time_elapsed_usec": 1100,
                "is_error": 0,
                "is_bot": 0,
                "ua": "Mozilla/5.0",
                "protocol": "HTTP/2",
                "tls_version": "TLSv1.3",
                "custom_fields": {},
            }
        ]
    )

    admin = make_client("127.0.0.1")
    admin.post(
        "/api/views/",
        json={
            "service_id": "svc-b",
            "name": "Customer B Secret View",
            "filters_json": json.dumps([{"field": "ip", "op": "in", "values": ["203.0.113.250"]}]),
        },
    )

    cust_a = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        query_start_time="2026-06-12T00:00:00Z",
        query_end_time="2026-06-12T01:00:00Z",
        client_ip="203.0.113.140",
    )
    attacker = make_client("203.0.113.140", cookies=cust_a.cookies())

    # Spoofing loopback headers on Caddy/external peer must NOT elevate privileges or cross into svc-b
    spoof_b_hdrs = cust_a.headers(
        {
            "X-Fastly-Service-Id": "svc-b",
            "X-Forwarded-For": "127.0.0.1",
        }
    )
    assert (
        attacker.post(
            "/api/query",
            headers=spoof_b_hdrs,
            json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
        ).status_code
        == 403
    )
    assert (
        attacker.post(
            "/api/high-scale/services/svc-b/queries",
            headers=spoof_b_hdrs,
            json={
                "domain": "request",
                "start_time": "2026-06-12T00:00:00Z",
                "end_time": "2026-06-12T01:00:00Z",
                "limit": 10,
            },
        ).status_code
        == 403
    )
    assert (
        attacker.post(
            "/api/dashboard/raw/csv",
            headers=spoof_b_hdrs,
            json={
                "start_time": "2026-06-12T00:00:00Z",
                "end_time": "2026-06-12T01:00:00Z",
                "filters": {},
                "columns": [],
            },
        ).status_code
        == 403
    )
    assert attacker.get("/api/services/svc-b/scoring/sessions/s1/events", headers=spoof_b_hdrs).status_code == 403
    assert attacker.get("/api/views/svc-b", headers=spoof_b_hdrs).status_code == 403
    assert attacker.get("/api/download-folder?root=raw", headers=spoof_b_hdrs).status_code in {401, 403}
    assert attacker.get("/metrics", headers=spoof_b_hdrs).status_code in {401, 403}
    assert attacker.get("/api/debug/recent-postgres", headers=spoof_b_hdrs).status_code in {401, 403}
    assert attacker.get("/api/admin/queries", headers=spoof_b_hdrs).status_code in {401, 403}

    # Live SSE streams must be blocked (403) because Customer A analyst is historical-window-bound
    for sse_path in (
        "/api/services/svc-a/realtime-stream",
        "/api/services/svc-a/control-room/error-stream",
    ):
        st, _ = probe_sse_status(sse_path, peer_ip="203.0.113.140", headers=cust_a.headers(), cookies=cust_a.cookies())
        assert st == 403

    # Bootstrap must omit svc-b AND omit svc-a secrets
    r_boot = attacker.get("/api/bootstrap", headers=cust_a.headers())
    assert r_boot.status_code == 200
    boot = r_boot.json()
    assert [s["service_id"] for s in boot["services"]] == ["svc-a"]
    svc_a_boot = boot["services"][0]
    assert "fos_bucket" not in svc_a_boot or not svc_a_boot["fos_bucket"]
    assert "cid_salt" not in (svc_a_boot.get("rum") or {})

    # Authorized masked historical read on svc-a succeeds with masked IP
    r_auth_a = attacker.post(
        "/api/high-scale/services/svc-a/request-facts",
        headers=cust_a.headers({"X-Fastly-Service-Id": "svc-a"}),
        json={
            "start_time": "2026-06-12T00:05:00Z",
            "end_time": "2026-06-12T00:55:00Z",
            "limit": 10,
        },
    )
    assert r_auth_a.status_code == 200
    assert r_auth_a.json()["rows"][0]["client_ip"] != "198.51.100.88"

    # Admin Customer B fdt SQL query succeeds cleanly
    r_admin_b = admin.post(
        "/api/query",
        headers={"X-Fastly-Service-Id": "svc-b", "X-FLA-Harness": "fdt"},
        json={
            "sql": "SELECT req_id, count() AS n FROM logs WHERE timestamp >= '2026-06-12T00:00:00Z' AND req_id LIKE 'fdlt-%' GROUP BY req_id",
            "dataset": "logs",
        },
    )
    assert r_admin_b.status_code == 200


def test_scenario_e2_enterprise_incident_response_stop_sharing_panic_live_revocation(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """Scenario E2: Invite -> claim -> bootstrap & scoped query -> Stop Sharing / Panic -> full lockout -> controlled recovery."""
    ch_recorder.set_handler(lambda sql, params, settings: [{"n": 9}])
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.150",
    )
    claim_token = share_db.create_claim_token(bundle.invite_id)
    reclaim_token = share_db.create_claim_token(bundle.invite_id)

    ext = make_client("203.0.113.150")
    r_claim = ext.post(f"/api/share/claim/{claim_token}", headers=bundle.headers(), json={})
    assert r_claim.status_code == 200
    assert r_claim.json()["ok"] is True

    r_login = ext.post(
        "/api/share/login",
        headers=bundle.headers(),
        json={"email": bundle.email, "passcode": bundle.passcode},
    )
    assert r_login.status_code == 200
    m = re.search(r"analyst_session_id=([^;]+)", r_login.headers.get("set-cookie", ""))
    assert m is not None
    claimed_sid = m.group(1)

    active_analyst = make_client("203.0.113.150", cookies={"analyst_session_id": claimed_sid})
    r_boot1 = active_analyst.get("/api/bootstrap", headers=bundle.headers())
    assert r_boot1.status_code == 200
    assert [s["service_id"] for s in r_boot1.json()["services"]] == ["svc-a"]

    r_q1 = active_analyst.post(
        "/api/query",
        headers=bundle.headers({"X-Fastly-Service-Id": "svc-a"}),
        json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
    )
    assert r_q1.status_code == 200

    # Incident response: Admin triggers Stop Sharing + Panic
    admin = make_client("127.0.0.1")
    assert admin.post("/api/admin/share/stop", json={}).status_code == 200
    assert admin.post("/api/admin/share/panic", json={}).status_code == 200

    # Immediate revocation across cookie, SSR X-Remote-Analyst: 1, Host: localhost spoof, bootstrap, and re-claim
    assert active_analyst.get("/api/views/svc-a", headers=bundle.headers()).status_code in {401, 403}
    ssr_client = make_client("127.0.0.1", cookies={"analyst_session_id": claimed_sid})
    assert ssr_client.get("/api/views/svc-a", headers={"X-Remote-Analyst": "1"}).status_code in {401, 403}
    assert active_analyst.get(
        "/api/views/svc-a",
        headers=bundle.headers({"Host": "localhost", "Origin": "http://localhost"}),
    ).status_code in {400, 401, 403}

    r_boot_locked = active_analyst.get("/api/bootstrap", headers=bundle.headers())
    assert r_boot_locked.status_code in {200, 401, 403}, (
        f"Unexpected /api/bootstrap status after Stop Sharing: {r_boot_locked.status_code}"
    )
    if r_boot_locked.status_code == 200:
        assert r_boot_locked.json()["services"] == []

    assert ext.post(
        "/api/share/login",
        headers=bundle.headers(),
        json={"email": bundle.email, "passcode": bundle.passcode},
    ).status_code in {401, 403}
    assert ext.post(f"/api/share/claim/{reclaim_token}", headers=bundle.headers(), json={}).status_code in {
        401,
        403,
        404,
        410,
    }

    # Controlled recovery: Admin re-enables sharing with a fresh invite
    recovered = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.151",
        email="recovered@tenant.example.test",
    )
    rec_client = make_client("203.0.113.151", cookies=recovered.cookies())
    assert rec_client.get("/api/views/svc-a", headers=recovered.headers()).status_code == 200
    assert tunnel.get_tunnel_manager().is_sharing_active() is True


def test_scenario_e3_fos_log_delivery_cdn_brute_force_and_credential_exfiltration_defense() -> None:
    """Scenario E3: End-to-end High-Scale + Cloud CDN/Front/Customer VCL brute-force & X-FOS-Request defense."""
    # 1. High-Scale empty cdn_auth fails closed
    empty_hs = _wrap_hs_cdn_vcl({})
    r_empty = run_falco_test(
        empty_hs,
        """
// @scope: recv
sub test_hs_empty_fail_closed {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.160", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz";
  testing.call_subroutine("vcl_recv");
  assert.error(401);
}
""",
    )
    if r_empty.returncode != 0:
        r_empty = run_falco_test(
            empty_hs,
            """
// @scope: recv
sub test_hs_empty_fail_closed_403 {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.160", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz";
  testing.call_subroutine("vcl_recv");
  assert.error(403);
}
""",
        )
    assert r_empty.returncode == 0

    # 2. High-Scale brute-force lockout vs clean IP uninterrupted access
    hs_vcl_src = load_vcl(rate_limiting=True)
    m_pb = re.search(r"penaltybox_has[^\n]+\{\s*error\s+(\d+)", hs_vcl_src, re.DOTALL)
    hs_pb_code = int(m_pb.group(1)) if m_pb else 429
    hs_main = _wrap_hs_cdn_vcl({"secret_key": "test-sentinel-hs-cdn-0001"})
    hs_brute = "\n  ".join(
        [
            """testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz?key=test-sentinel-hs-cdn-0001";
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");"""
        ]
        * 25
    )
    r_hs = run_falco_test(
        hs_main,
        f"""
// @scope: recv
sub test_hs_bruteforce_and_clean_peer {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.161", "127.0.0.1"));
  {hs_brute}
  testing.fixed_access_rate(1000);
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.fixed_access_rate(0);

  // Attacker blocked even with valid key
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.x-fastly-key = "test-sentinel-hs-cdn-0001";
  testing.call_subroutine("vcl_recv");
  assert.error({hs_pb_code});

  // Legitimate worker on clean IP succeeds and strips x-fastly-key
  testing.inject_variable("client.ip", std.str2ip("198.51.100.162", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.x-fastly-key = "test-sentinel-hs-cdn-0001";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
}}
""",
    )
    assert r_hs.returncode == 0, f"E3 High-Scale CDN defense failed:\n{r_hs.stdout}\n{r_hs.stderr}"

    # 3. Cloud Front VCL tampered flp_cdn lockout vs clean browser session
    front_main, front_extra = build_front_vcl_wrapper()
    good_sess = "legit-browser-session"
    good_cdn = make_flp_cdn_cookie(session_cookie=good_sess, exp=int(time.time()) + 240)
    bad_cdn = make_flp_cdn_cookie(session_cookie=good_sess, exp=int(time.time()) + 240, tamper_sig=True)
    cloud_deliver_10 = "\n  ".join(
        [
            """testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "2001:db8:dead:1::1";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
        ]
        * 10
    )
    r_cloud = run_falco_test(
        front_main,
        f"""
// @scope: recv
sub test_cloud_tampered_raises_629 {{
  testing.inject_variable("client.ip", std.str2ip("2001:db8:dead:1::1", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={good_sess}; flp_cdn={bad_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(629);
}}

// @scope: error
sub test_cloud_629_sets_auth_fail {{
  set obj.status = 629;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.http.x-flp-auth-fail, "1");
}}

// @scope: deliver
sub test_cloud_tampered_deliver_adds_ipv6_64_penaltybox {{
  {cloud_deliver_10}
  declare local var.sib_ip IP;
  declare local var.sib_key STRING;
  set var.sib_ip = std.str2ip("2001:db8:dead:1::abcd", "::1");
  set var.sib_key = "v6:" + addr.extract_bits(var.sib_ip, 96, 32) + ":" + addr.extract_bits(var.sib_ip, 64, 32);
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, var.sib_key));
}}

// @scope: recv
sub test_cloud_tampered_lockout_and_clean_browser {{
  declare local var.atk_ip IP;
  declare local var.atk_key STRING;
  set var.atk_ip = std.str2ip("2001:db8:dead:1::1", "::1");
  set var.atk_key = "v6:" + addr.extract_bits(var.atk_ip, 96, 32) + ":" + addr.extract_bits(var.atk_ip, 64, 32);
  ratelimit.penaltybox_add(auth_fail_pb, var.atk_key, 15m);

  // Rotated host in same /64 locked out even with valid good_cdn
  testing.inject_variable("client.ip", std.str2ip("2001:db8:dead:1::abcd", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={good_sess}; flp_cdn={good_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  // Clean browser on separate IPv6 /64 succeeds
  testing.inject_variable("client.ip", std.str2ip("2001:db8:cafe:9::1", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={good_sess}; flp_cdn={good_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}}
""",
        extra_files=front_extra,
    )
    assert r_cloud.returncode == 0, f"E3 Cloud Front defense failed:\n{r_cloud.stdout}\n{r_cloud.stderr}"


def test_scenario_e4_cloud_multi_role_enterprise_lifecycle_and_logout_revocation(
    cloud_server: CloudTestServer,
) -> None:
    """Scenario E4: Cloud fdt health/crons -> Admin passkey + views -> AnalystNoPii isolation & RSC block -> expired flp_cdn refresh -> logout revocation."""
    assert cloud_server.request("GET", "/version").status_code == 200
    assert cloud_server.request("GET", "/").status_code == 200
    assert (
        cloud_server.request(
            "GET", "/_tick", front_key=DEFAULT_FRONT_KEY, extra_headers={"x-flp-tick": "1"}
        ).status_code
        == 200
    )
    assert (
        cloud_server.request(
            "GET", "/_flush", front_key=DEFAULT_FRONT_KEY, extra_headers={"x-flp-tick": "1"}
        ).status_code
        == 200
    )

    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="Enterprise NoPII")

    assert (
        cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            harness_header=True,
            json_body={
                "service_id": sid,
                "name": "Public Latency View",
                "filters_json": json.dumps({"pop": {"mode": "include", "values": ["IAD"]}}),
            },
        ).status_code
        == 201
    )
    assert (
        cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            harness_header=True,
            json_body={
                "service_id": sid,
                "name": "Restricted Session View",
                "filters_json": json.dumps({"sid": {"mode": "include", "values": ["secret-sid-42"]}}),
            },
        ).status_code
        == 201
    )

    # AnalystNoPii checks: PII view hidden/redacted, positive view preserved, POST /api/views 403, /admin/__next.admin.txt 403
    r_np_views = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=nopii.session_cookie)
    assert r_np_views.status_code == 200
    assert "Public Latency View" in r_np_views.text, (
        f"Positive non-PII view missing from AnalystNoPii response: {r_np_views.text}"
    )
    assert "secret-sid-42" not in r_np_views.text
    assert (
        cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=nopii.session_cookie,
            json_body={"service_id": sid, "name": "Blocked", "filters_json": "{}"},
        ).status_code
        == 403
    )
    assert (
        cloud_server.request("GET", "/admin/__next.admin.txt", session_cookie=nopii.session_cookie).status_code == 403
    )

    # Expired flp_cdn seamless refresh in Front VCL without 429
    front_main, front_extra = build_front_vcl_wrapper()
    expired = make_flp_cdn_cookie(session_cookie=admin.session_cookie, exp=1700000000)
    r_refresh = run_falco_test(
        front_main,
        f"""
// @scope: recv
sub test_e4_expired_refresh {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.170", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={admin.session_cookie}; flp_cdn={expired}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-auth-fail);
  assert.contains(req.url, "/api/file?key=");
}}
""",
        extra_files=front_extra,
    )
    assert r_refresh.returncode == 0

    # Logout clears flp_session, flp_cdn, __Host-flp_cdn and revokes /api/file + API replay
    pre_logout_issued = cloud_issue_cdn_cookie(
        session_cookie=admin.session_cookie,
        role="admin",
        auth_epoch=1,
    )
    assert 180 <= pre_logout_issued["ttl"] <= 300
    m_pre_cdn = re.search(r"flp_cdn=([^;]+)", pre_logout_issued["set_cookie"] or "")
    assert m_pre_cdn is not None
    pre_cdn_val = m_pre_cdn.group(1)

    # Admin revokes AnalystNoPii user -> immediate 401 on /api/file replay
    r_nopii_sess = cloud_server.request("GET", "/api/auth/session", session_cookie=nopii.session_cookie)
    assert r_nopii_sess.status_code == 200
    nopii_uid = r_nopii_sess.json()["user"]["id"]
    r_rev = cloud_server.request(
        "POST",
        f"/api/admin/users/{nopii_uid}/revoke",
        session_cookie=admin.session_cookie,
        json_body={},
    )
    assert r_rev.status_code == 200
    assert (
        cloud_server.request(
            "GET",
            "/api/file",
            raw_cookie=f"flp_session={nopii.session_cookie}; flp_cdn={pre_cdn_val}",
        ).status_code
        == 401
    )

    old_admin_cookie = admin.session_cookie
    r_logout = cloud_server.request(
        "POST",
        "/api/auth/logout",
        session_cookie=old_admin_cookie,
        json_body={},
    )
    assert r_logout.status_code == 200
    sc = r_logout.headers.get_list("set-cookie")
    for cname in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
        assert any(h.startswith(cname) and "Max-Age=0" in h for h in sc)
    r_file_after = cloud_server.request(
        "GET",
        "/api/file",
        raw_cookie=f"flp_session={old_admin_cookie}; flp_cdn={pre_cdn_val}",
    )
    assert r_file_after.status_code == 401
    assert r_file_after.headers.get("x-flp-auth-fail") == "1"
    assert cloud_server.request("GET", "/api/admin/audit-logs", session_cookie=old_admin_cookie).status_code == 401


def test_scenario_e5_malicious_tenant_provisioning_and_supply_chain_ssrf_containment(
    cloud_server: CloudTestServer,
    isolated_e2e_security_state: Any,
) -> None:
    """Scenario E5: Untrusted provisioning blocked -> check-domain SSRF/redirect blocked -> VCL exfiltration blocked -> valid TLS/Terraform succeeds."""
    provision_analyst_session(service_ids=["svc-a"], public_host="share.example.test")
    ext = make_client(
        "93.184.216.34",
        headers={"Host": "share.example.test", "Origin": "https://share.example.test"},
    )
    assert ext.get("/api/provision/check-domain?prefix=test&is_custom=false").status_code in {401, 403}
    assert ext.post("/api/services/svc-a/logging-settings/update", json={}).status_code in {401, 403}

    admin = make_client("127.0.0.1")
    outbound: list[str] = []

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
                b"HTTP/1.1 302 Found\r\n"
                b"Location: http://169.254.169.254/latest/meta-data/iam\r\n"
                b"Content-Length: 0\r\n\r\n"
            )
        )
        resp.begin()
        return resp

    def _forbid_http_redirect(self: Any) -> None:
        outbound.append(f"http://{self.host}")
        raise AssertionError(f"Followed redirect to {self.host}")

    with (
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(
            http.client.HTTPSConnection,
            "request",
            lambda self, m, u, *a, **k: outbound.append(f"https://{self.host}{u}"),
        ),
        patch.object(http.client.HTTPSConnection, "getresponse", _https_getresponse),
        patch.object(http.client.HTTPConnection, "connect", _forbid_http_redirect),
    ):
        r_meta = admin.get("/api/provision/check-domain?prefix=169.254.169.254&is_custom=true")
        assert r_meta.status_code in {200, 400, 422}
        if r_meta.status_code == 200:
            assert r_meta.json()["available"] is False
        with patch(
            "socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 443))],
        ):
            r_priv = admin.get("/api/provision/check-domain?prefix=internal-db.example.com&is_custom=true")
            assert r_priv.status_code in {200, 400, 422}
            if r_priv.status_code == 200:
                assert r_priv.json()["available"] is False
        with patch(
            "socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("151.101.1.57", 443))],
        ):
            r_redir = admin.get("/api/provision/check-domain?prefix=redir.example.com&is_custom=true")
            assert r_redir.status_code in {200, 400, 422}
            if r_redir.status_code == 200:
                assert r_redir.json()["available"] is False
    assert not any("169.254.169.254" in u or "10.1.2.3" in u for u in outbound)

    # VCL table.lookup exfiltration rejected across High-Scale RUM, Custom Fields, Terraform, and Cloud
    exfil = 'table.lookup(fos_credentials, "secret_key", "") != ""'
    assert (
        admin.post(
            "/api/services/svc-a/rum/settings",
            json={
                "capture_vitals": True,
                "capture_performance": True,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": exfil,
                "token": "test-sentinel-tok",
            },
        ).status_code
        == 400
    )
    assert admin.post(
        "/api/services/svc-a/custom-fields",
        json={
            "name": "leak_sk",
            "label": "Leak",
            "vcl_log_expression": 'table.lookup(fos_credentials, "secret_key", "")',
            "value_type": "string",
            "collection_stage": "edge",
        },
    ).status_code in {400, 422}
    assert admin.post(
        "/api/provision/terraform/preview",
        json={"service_id": "svc-a", "fos_bucket_name": "bucket-a", "custom_condition": exfil},
    ).status_code in {400, 422}

    cloud_admin = cloud_server.register_admin()
    cloud_sid = cloud_server.provision_fixture_service(cloud_admin)
    assert cloud_server.request(
        "PATCH",
        f"/api/admin/services/{cloud_sid}",
        session_cookie=cloud_admin.session_cookie,
        json_body={
            "generation": cloud_server.service_generation(cloud_admin, cloud_sid),
            "custom_condition": exfil,
        },
    ).status_code in {400, 422}

    # /api/download* traversal + metadata SSRF + redirecting-CDN SafeRedirectHandler blocked
    assert admin.get("/api/download-folder?root=../../etc&prefix=passwd").status_code == 400
    assert admin.get("/api/download?key=../../etc/passwd").status_code == 400
    cfg_path = Path(svcconfig.CONFIGS_DIR) / "svc-a.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["cdn_url"] = "http://169.254.169.254/latest"
    cfg["provisioning"]["cdn_url"] = "http://169.254.169.254/latest"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    with svcconfig._config_cache_lock:
        svcconfig._config_cache.clear()
    fos_client.reload_default_source()
    dl_calls: list[str] = []
    with patch("urllib.request.urlopen", side_effect=lambda r, *a, **k: dl_calls.append(str(r))):
        r_ssrf = admin.get("/api/download?key=raw/request/f.gz", headers={"X-Fastly-Service-Id": "svc-a"})
    assert r_ssrf.status_code == 400
    assert dl_calls == []

    # Valid Fastly CDN host returning 302 -> metadata IP blocked by SafeRedirectHandler (urlopen UNPATCHED)
    cfg["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"
    cfg["provisioning"]["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    with svcconfig._config_cache_lock:
        svcconfig._config_cache.clear()
    fos_client.reload_default_source()
    redir_hosts: list[str] = []
    with (
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(
            http.client.HTTPSConnection,
            "request",
            lambda self, m, u, *a, **k: redir_hosts.append(self.host),
        ),
        patch.object(http.client.HTTPSConnection, "getresponse", _https_getresponse),
        patch.object(http.client.HTTPConnection, "connect", _forbid_http_redirect),
    ):
        r_cdn_redir = admin.get("/api/download?key=raw/request/f.gz", headers={"X-Fastly-Service-Id": "svc-a"})
    assert r_cdn_redir.status_code == 502
    assert redir_hosts == ["cdn-svc-a.global.ssl.fastly.net"]

    # Valid TLS-verified backend & Terraform HCL generation succeed cleanly
    backends = {
        b.name: b
        for b in desired_backends(
            FeatureState(
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
                    request_secret="test-sentinel-sec",
                ),
            )
        )
    }
    assert backends["session_scorer"].ssl_check_cert is True
    tf = generate_terraform(
        {
            "service_id": "svc-a",
            "logging_service_id": "svc-a",
            "fos_bucket_name": "bucket-a",
            "fos_region": "us-east-1",
            "custom_condition": 'req.http.Host == "shop.example.com"',
        },
        "test-sentinel-ak",
        "test-sentinel-sk",
    )
    assert "cdn_proxy.tf.json" in tf


def test_scenario_e6_preserved_fdt_deployment_readiness_and_light_traffic_compatibility(
    monkeypatch: pytest.MonkeyPatch,
    ch_recorder: RecordingClickHouseClient,
    cloud_server: CloudTestServer,
) -> None:
    """Scenario E6: Complete local fdt compatibility simulation across fla-elevation, flp-cloud, and flp-se-demo."""
    # 1. fla-elevation: mTLS admin-gateway reconcile + loopback /metrics & /health + fdt SQL verification
    gw_secret = "test-sentinel-fdt-admin-gw-32bytes"
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", gw_secret)
    monkeypatch.setattr(
        "backend.provision.declarative.reconciler.reconcile_vcl_state",
        lambda service_id, token, dry_run=False, status_cb=None: SimpleNamespace(
            service_id=service_id,
            activated_version=12,
            changes_applied=[],
            duration_ms=8,
            error=None,
        ),
    )
    ch_recorder.set_handler(lambda sql, params, settings: [{"req_id": "fdlt-1", "n": 1}])

    gw_client = make_client(
        "10.244.2.5",
        headers={
            "X-Admin-Gateway-Token": gw_secret,
            "X-Fastly-Service-Id": "svc-a",
            "X-FLA-Harness": "fdt",
        },
    )
    r_rec = gw_client.post(
        "/api/provision/reconcile",
        json={"service_id": "svc-a", "token": "test-sentinel-fastly-token"},
    )
    assert r_rec.status_code == 200
    assert "reconciliation_result" in r_rec.text

    for sql, ds in (
        (
            "SELECT req_id, count() AS n FROM logs WHERE timestamp >= '2026-06-12T00:00:00Z' AND req_id LIKE 'fdlt-%' GROUP BY req_id",
            "logs",
        ),
        (
            "SELECT count() AS n FROM client_vitals WHERE timestamp >= '2026-06-12T00:00:00Z'",
            "client_vitals",
        ),
    ):
        assert gw_client.post("/api/query", json={"sql": sql, "dataset": ds}).status_code == 200

    # Direct in-cluster/loopback Prometheus scrape without gateway token when loopback peer is used
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    loopback = make_client("127.0.0.1")
    assert loopback.get("/api/health").status_code == 200
    assert loopback.get("/metrics").status_code == 200

    # High-Scale CDN VCL dual compatibility: both legacy "secret" and new "secret_key" dictionaries work
    for key_entry in ({"secret": "legacy-fdt-cdn-key"}, {"secret_key": "new-fdt-cdn-key"}):
        secret_val = next(iter(key_entry.values()))
        res_hs = run_falco_test(
            _wrap_hs_cdn_vcl(key_entry),
            f"""
// @scope: recv
sub test_fdt_dual_cdn_dict {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.190", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/fdlt.log.gz";
  set req.http.x-fastly-key = "{secret_val}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
}}
""",
        )
        assert res_hs.returncode == 0, f"Dual cdn_auth compatibility failed for {key_entry}:\n{res_hs.stdout}"

    # 2. flp-cloud & flp-se-demo: /version, /, /_tick, /_flush, flp_session + X-FLP-Harness, X-LT-* fields, flp_cdn refresh
    assert cloud_server.request("GET", "/version").status_code == 200
    assert cloud_server.request("GET", "/").status_code == 200
    assert (
        cloud_server.request(
            "GET", "/_tick", front_key=DEFAULT_FRONT_KEY, extra_headers={"x-flp-tick": "1"}
        ).status_code
        == 200
    )
    assert (
        cloud_server.request(
            "GET", "/_flush", front_key=DEFAULT_FRONT_KEY, extra_headers={"x-flp-tick": "1"}
        ).status_code
        == 200
    )

    cloud_admin = cloud_server.register_admin()
    cloud_sid = cloud_server.provision_fixture_service(cloud_admin)
    r_lt_cfg = cloud_server.request(
        "PATCH",
        "/api/admin/log-fields/config",
        session_cookie=cloud_admin.session_cookie,
        harness_header=True,
        json_body={
            "service_id": cloud_sid,
            "generation": cloud_server.service_generation(cloud_admin, cloud_sid),
            "log_fields": {
                "groups": ["A", "C"],
                "custom_fields": [
                    {
                        "name": "lt_run_id",
                        "label": "LT Run ID",
                        "vcl_log_expression": "req.http.X-LT-Run-Id",
                        "value_type": "string",
                        "duckdb_type": "VARCHAR",
                        "byte_limit": 192,
                        "bytes_estimate": 40,
                        "collection_stage": "edge",
                        "enabled": True,
                        "show_in_logs": False,
                    },
                    {
                        "name": "lt_request_id",
                        "label": "LT Request ID",
                        "vcl_log_expression": "req.http.X-LT-Request-Id",
                        "value_type": "string",
                        "duckdb_type": "VARCHAR",
                        "byte_limit": 252,
                        "bytes_estimate": 40,
                        "collection_stage": "edge",
                        "enabled": True,
                        "show_in_logs": False,
                    },
                ],
                "field_overrides": {},
                "field_limits": {},
            },
        },
    )
    assert r_lt_cfg.status_code == 200

    r_ext = cloud_server.request(
        "GET",
        "/api/log-extents",
        session_cookie=cloud_admin.session_cookie,
        harness_header=True,
    )
    assert r_ext.status_code == 200
    assert r_ext.json()["configured"] is True
    issued = cloud_issue_cdn_cookie(session_cookie=cloud_admin.session_cookie, role="admin")
    assert issued["set_cookie"] is not None and issued["set_cookie"].startswith("flp_cdn=")

    front_main, front_extra = build_front_vcl_wrapper()
    exp_cdn = make_flp_cdn_cookie(
        session_cookie=cloud_admin.session_cookie,
        sid=FIXTURE_22_SID,
        cdn_key=DEFAULT_CDN_KEY,
        exp=1700000000,
    )
    r_front_refresh = run_falco_test(
        front_main,
        f"""
// @scope: recv
sub test_e6_cloud_expired_flp_cdn_refresh {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.191", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/fdlt.parquet";
  set req.http.Cookie = "flp_session={cloud_admin.session_cookie}; flp_cdn={exp_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.is_notset(req.http.x-flp-auth-fail);
}}
""",
        extra_files=front_extra,
    )
    assert r_front_refresh.returncode == 0
