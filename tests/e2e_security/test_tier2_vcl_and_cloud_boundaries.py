"""Tier 2 (Part B) — High-Scale & Cloud VCL, CDN Rate-Limiting, SSRF & Cloud RBAC/RSC (28 test functions / 30 collected items across F4-F6)."""

from __future__ import annotations

import http.client
import json
import re
import socket
import time
from io import BytesIO
from typing import Any
from unittest.mock import patch

import pytest

from backend.core.catalog import validate_custom_field
from backend.core.fastly.client import (
    _generate_deliver_set_cookie_vcl,
    load_vcl,
    validate_rum_custom_condition,
)
from backend.core.metadata.state_sync import _cdn_get
from backend.core.request_context import require_admin
from backend.provision import fastly_api, rum_assets
from backend.provision.declarative.models import FeatureState
from backend.provision.declarative.vcl_builder import generate_consolidated_snippet
from backend.provision.session_scoring.wasm_deploy import pass_snippet
from backend.routers import provision as provision_router_mod
from backend.routers.services import vcl as services_vcl_router_mod
from backend.utils.terraform_gen import generate_terraform
from tests.e2e_security.cloud_harness import (
    CLOUD_DIR,
    DEFAULT_CDN_KEY,
    FIXTURE_22_SID,
    FRONT_MAIN_VCL,
    CloudTestServer,
    build_cloud_capture_vcl_wrapper,
    build_front_vcl_wrapper,
    cloud_check_audit_entries_rbac,
    cloud_check_revoke_epoch_bump,
    cloud_desired_snippets,
    cloud_issue_cdn_cookie,
    cloud_rum_tracker_js,
    make_flp_cdn_cookie,
)
from tests.e2e_security.conftest import (
    make_client,
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


def _iter_route_dep_calls(route: Any) -> set[Any]:
    seen: set[Any] = set()
    stack = [getattr(route, "dependant", None)]
    while stack:
        dep = stack.pop()
        if dep is None:
            continue
        if getattr(dep, "call", None) is not None:
            seen.add(dep.call)
        for child in getattr(dep, "dependencies", []) or []:
            stack.append(child)
    return seen


# ============================================================================
# Area F4 — High-Scale VCL, CDN Rate-Limiting, SSRF & Scorer/RUM Hardening (10)
# ============================================================================


def test_f4_hs_customer_vcl_scrubs_client_x_fos_request_in_logging_and_rum_only_modes() -> None:
    """SEC-11: Client-supplied X-FOS-Request: 1 is scrubbed in both full logging+RUM and RUM-only modes."""
    assert any(h.lower() == "x-fos-request" for h in fastly_api._SCRUB_HEADERS), (
        "X-FOS-Request missing from fastly_api._SCRUB_HEADERS"
    )
    for logging_enabled in (True, False):
        state = FeatureState(
            service_id="svc-a",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            logging_enabled=logging_enabled,
            rum_enabled=True,
            fos_bucket="bucket-a",
            fos_region="us-east-1",
            cdn_shield="none",
            faro_version="1.19.0",
        )
        main_vcl = _wrap_hs_customer_vcl(state)
        test_vcl = """
// @scope: recv
sub test_scrub_spoofed_x_fos_request {
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/account/profile";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.X-FOS-Request);
  assert.equal(req.backend, F_origin);
}
"""
        res = run_falco_test(main_vcl, test_vcl)
        assert res.returncode == 0, f"SEC-11 failed for logging_enabled={logging_enabled}:\n{res.stdout}\n{res.stderr}"


def test_f4_hs_rum_sigv4_refuses_non_rum_paths_wrong_backend_and_shield_hops() -> None:
    """SEC-11: vcl_miss SigV4 signing requires F_fos_origin && !is_shield && (/js/rum.js || /js/faro-sdk.js)."""
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
    test_vcl = """
// @scope: miss
sub test_refuse_wrong_backend {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_refuse_non_rum_path_on_fos_origin {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_fos_origin;
  set req.url = "/raw/request/secret.log.gz";
  set bereq.url = "/raw/request/secret.log.gz";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_refuse_shield_hop_double_sign {
  testing.inject_variable("req.backend.is_shield", true);
  set req.backend = F_fos_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"SEC-11 miss guard failed:\n{res.stdout}\n{res.stderr}"


def test_f4_load_vcl_deterministic_and_fails_closed_on_empty_dict_query_key_and_post() -> None:
    """SEC-12: load_vcl() is deterministic, fails closed when cdn_auth is empty, rejects ?key=, and 405s POST."""
    vcl_a = load_vcl(rate_limiting=True)
    vcl_b = load_vcl(rate_limiting=True)
    assert vcl_a == vcl_b, "load_vcl() contains non-deterministic random drift across calls"

    empty_vcl = _wrap_hs_cdn_vcl({})
    empty_test = """
// @scope: recv
sub test_empty_cdn_auth_fails_closed {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.31", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz";
  set req.http.x-fastly-key = "";
  testing.call_subroutine("vcl_recv");
  assert.error(401);
}
"""
    r_empty = run_falco_test(empty_vcl, empty_test)
    if r_empty.returncode != 0:
        # Also accept 403 fail-closed
        r_empty_403 = run_falco_test(empty_vcl, empty_test.replace("401", "403"))
        assert r_empty_403.returncode == 0, f"Empty cdn_auth did not fail closed:\n{r_empty.stdout}"

    populated_vcl = _wrap_hs_cdn_vcl({"secret_key": "test-sentinel-cdn-0001"})
    method_and_qs_test = """
// @scope: recv
sub test_query_key_without_header_rejected {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.32", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz?key=test-sentinel-cdn-0001";
  testing.call_subroutine("vcl_recv");
  assert.error(401);
}

// @scope: recv
sub test_post_rejected_with_405 {
  testing.inject_variable("client.ip", std.str2ip("198.51.100.33", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "POST";
  set req.url = "/raw/request/a.gz";
  set req.http.x-fastly-key = "test-sentinel-cdn-0001";
  testing.call_subroutine("vcl_recv");
  assert.error(405);
}
"""
    r_pop = run_falco_test(populated_vcl, method_and_qs_test)
    if r_pop.returncode != 0:
        r_pop_403 = run_falco_test(
            populated_vcl,
            method_and_qs_test.replace("assert.error(401);", "assert.error(403);"),
        )
        assert r_pop_403.returncode == 0, f"Query-key or POST check failed:\n{r_pop.stdout}"


def test_f4_load_vcl_enforces_pre_auth_penaltybox_429_no_store_15m_and_secure_is_equal() -> None:
    """SEC-12: load_vcl() checks penaltybox BEFORE secret check (429 no-store, 15m TTL, digest.secure_is_equal, IPv6 /64)."""
    vcl_src = load_vcl(rate_limiting=True)
    assert "digest.secure_is_equal(" in vcl_src, "load_vcl() must use digest.secure_is_equal"
    assert "15m" in vcl_src, "load_vcl() must enforce 15m penaltybox duration"
    assert vcl_src.index("penaltybox_has") < vcl_src.index("secure_is_equal"), (
        "penaltybox_has must execute BEFORE secret comparison in load_vcl()"
    )

    main_vcl = _wrap_hs_cdn_vcl({"secret_key": "test-sentinel-cdn-0001"})
    # Detect whether vcl_recv emits error 429 directly or synthetic 6xx rewritten in vcl_error
    m_pb = re.search(r"penaltybox_has[^\n]+\{\s*error\s+(\d+)", vcl_src, re.DOTALL)
    expected_err = int(m_pb.group(1)) if m_pb else 429

    bad_attempts = "\n  ".join(
        [
            """testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz";
  set req.http.x-fastly-key = "wrong-secret-attempt";
  testing.call_subroutine("vcl_recv");"""
        ]
        * 25
    )
    test_vcl = f"""
// @scope: recv
sub test_ipv6_64_pre_auth_lockout {{
  testing.inject_variable("client.ip", std.str2ip("2001:db8:abcd:1234::1", "::1"));
  {bad_attempts}
  testing.fixed_access_rate(1000);
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.x-fastly-key = "wrong-secret-attempt";
  testing.call_subroutine("vcl_recv");
  testing.fixed_access_rate(0);

  // Rotate to another host in the SAME /64 with a VALID secret -> must STILL be blocked!
  testing.inject_variable("client.ip", std.str2ip("2001:db8:abcd:1234:ffff:ffff:ffff:ffff", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/raw/request/a.gz";
  set req.http.x-fastly-key = "test-sentinel-cdn-0001";
  testing.call_subroutine("vcl_recv");
  assert.error({expected_err});
}}

// @scope: error
sub test_429_error_sets_no_store_and_retry_after {{
  set obj.status = 429;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 429);
  assert.contains(obj.http.Cache-Control, "no-store");
  assert.equal(obj.http.Retry-After, "900");
}}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"SEC-12 IPv6 /64 pre-auth penaltybox failed:\n{res.stdout}\n{res.stderr}"


def test_f4_state_sync_cdn_get_uses_header_only_and_terraform_cdn_auth_write_only() -> None:
    """SEC-12: state_sync._cdn_get sends x-fastly-key header (never ?key=) and Terraform sets write_only + secret_key."""
    captured: list[Any] = []

    class _FakeResp:
        status = 200
        headers: dict[str, str] = {}

        def read(self) -> bytes:
            return b'{"ok":true}'

        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *args: Any) -> bool:
            return False

    def _spy_urlopen(req: Any, *args: Any, **kwargs: Any) -> _FakeResp:
        captured.append(req)
        return _FakeResp()

    source = {
        "cdn_url": "https://test-cdn.global.ssl.fastly.net",
        "cdn_secret": "test-sentinel-header-only-secret",
        "prefix": "logs",
    }
    with patch("urllib.request.urlopen", side_effect=_spy_urlopen):
        body = _cdn_get(source, "meta/admin_state.json")
    assert body == b'{"ok":true}'
    assert len(captured) == 1
    req = captured[0]
    assert "test-sentinel-header-only-secret" not in req.full_url
    assert "key=" not in req.full_url
    hdrs = {k.lower(): v for k, v in req.header_items()}
    assert hdrs.get("x-fastly-key") == "test-sentinel-header-only-secret"

    tf_files = generate_terraform(
        {
            "service_id": "svc-a",
            "logging_service_id": "svc-a",
            "fos_bucket_name": "bucket-a",
            "fos_region": "us-east-1",
            "cdn_prefix": "fos-svc-a-logs",
        },
        "test-sentinel-ak",
        "test-sentinel-sk",
    )
    cdn_tf = json.loads(tf_files["cdn_proxy.tf.json"])
    dicts = cdn_tf["resource"]["fastly_service_vcl"]["cdn_proxy"]["dictionary"]
    cdn_auth_dict = next(d for d in dicts if d["name"] == "cdn_auth")
    assert cdn_auth_dict["write_only"] is True
    dict_items_res = cdn_tf["resource"]["fastly_service_dictionary_items"]
    cdn_auth_res = dict_items_res.get("cdn_auth") or dict_items_res.get("cdn_auth_items")
    assert cdn_auth_res is not None, f"Missing cdn_auth dictionary items resource in {list(dict_items_res)}"
    items = cdn_auth_res["items"]
    assert "secret_key" in items, f"Expected secret_key in Terraform cdn_auth items, got {list(items)}"


def test_f4_check_domain_rejects_ip_literals_internal_tlds_and_private_dns() -> None:
    """SEC-13: GET /api/provision/check-domain blocks IP literals, internal suffixes, and non-global DNS answers."""
    client = make_client("127.0.0.1")
    http_calls: list[str] = []

    def _fail_urlopen(req: Any, *args: Any, **kwargs: Any) -> Any:
        http_calls.append(str(req))
        raise AssertionError(f"Outbound HTTP must not fire for SSRF target: {req}")

    with patch("urllib.request.OpenerDirector.open", side_effect=_fail_urlopen):
        for literal_or_internal in (
            "127.0.0.1",
            "169.254.169.254",
            "[::1]",
            "metadata.internal",
            "admin.localhost",
            "printer.local",
            "kubernetes.default.svc.cluster.local",
        ):
            r = client.get(f"/api/provision/check-domain?prefix={literal_or_internal}&is_custom=true")
            assert r.status_code in {200, 400, 422}
            if r.status_code == 200:
                assert r.json()["available"] is False, f"{literal_or_internal} was reported available!"

        for private_ip in ("127.0.0.1", "10.0.0.5", "169.254.169.254", "100.64.0.1"):
            with patch(
                "socket.getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", (private_ip, 443))],
            ):
                r_dns = client.get("/api/provision/check-domain?prefix=rebind.example.com&is_custom=true")
                assert r_dns.status_code in {200, 400, 422}
                if r_dns.status_code == 200:
                    assert r_dns.json()["available"] is False, f"Private DNS IP {private_ip} was not rejected!"
    assert http_calls == []


def test_f4_check_domain_blocks_http_redirects_and_sanitizes_internal_errors() -> None:
    """SEC-13: check-domain never follows 301/302 redirects and never leaks raw internal exception details."""
    client = make_client("127.0.0.1")
    visited: list[str] = []

    class _FakeRawSocket:
        def __init__(self, raw_http: bytes) -> None:
            self._bio = BytesIO(raw_http)

        def makefile(self, mode: str = "rb", *args: Any, **kwargs: Any) -> BytesIO:
            return self._bio

        def close(self) -> None:
            pass

    def _https_request(self: Any, method: str, url: str, *args: Any, **kwargs: Any) -> None:
        visited.append(f"https://{self.host}{url}")

    def _https_getresponse(self: Any, *args: Any, **kwargs: Any) -> http.client.HTTPResponse:
        resp = http.client.HTTPResponse(
            _FakeRawSocket(  # type: ignore[arg-type]
                b"HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/\r\nContent-Length: 0\r\n\r\n"
            )
        )
        resp.begin()
        return resp

    def _forbid_http_redirect_connect(self: Any) -> None:
        visited.append(f"http://{self.host}")
        raise AssertionError(f"check-domain followed HTTP redirect to {self.host}")

    with (
        patch(
            "socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("151.101.1.57", 443))],
        ),
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(http.client.HTTPSConnection, "request", _https_request),
        patch.object(http.client.HTTPSConnection, "getresponse", _https_getresponse),
        patch.object(http.client.HTTPConnection, "connect", _forbid_http_redirect_connect),
    ):
        resp = client.get("/api/provision/check-domain?prefix=redir.example.com&is_custom=true")
    assert not any("169.254.169.254" in u for u in visited)
    assert resp.status_code in {200, 400}
    if resp.status_code == 200:
        assert resp.json()["available"] is False

    with (
        patch(
            "socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("151.101.1.57", 443))],
        ),
        patch(
            "urllib.request.OpenerDirector.open",
            side_effect=RuntimeError("SENSITIVE_INTERNAL_STACK_DETAIL_10.99.88.77"),
        ),
    ):
        r_err = client.get("/api/provision/check-domain?prefix=err.example.com&is_custom=true")
    assert "SENSITIVE_INTERNAL_STACK_DETAIL_10.99.88.77" not in r_err.text


def test_f4_provision_and_vcl_routers_enforce_router_level_require_admin() -> None:
    """SEC-13: provision.py and services/vcl.py attach Depends(require_admin) and block analyst callers."""
    for label, rtr in (
        ("provision.router", provision_router_mod.router),
        ("services.vcl.router", services_vcl_router_mod.router),
    ):
        router_deps = {d.dependency for d in (rtr.dependencies or [])}
        assert require_admin in router_deps or all(require_admin in _iter_route_dep_calls(r) for r in rtr.routes), (
            f"{label} missing Depends(require_admin)"
        )

    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.81")
    analyst_client = make_client("203.0.113.81", cookies=bundle.cookies())
    assert analyst_client.get(
        "/api/provision/check-domain?prefix=testcdn&is_custom=false",
        headers=bundle.headers(),
    ).status_code in {401, 403}
    assert analyst_client.get(
        "/api/services/svc-a/logging-settings",
        headers=bundle.headers(),
    ).status_code in {401, 403}


def test_f4_scorer_pass_snippet_strips_customer_cookies_preserving_x_session_state_and_rum_hardening() -> None:
    """SEC-14: pass_snippet strips customer cookies (session_id, other) forwarding only X-Session-State (and _ess); rum_cid has Secure; rum_assets uses json.dumps; deploy_remote_frontend sets ssl_check_cert."""
    snip = pass_snippet("svc-a", "test-sentinel-scorer-sec-0001")
    main_vcl = f"""backend F_origin {{ .host = "127.0.0.1"; .port = "8080"; }}
backend F_session_scorer {{ .host = "scorer.edgecompute.app"; .port = "443"; }}
sub vcl_pass {{
{snip}
}}
"""
    test_vcl = """
// @scope: pass
sub test_scorer_cookie_allowlist_strips_customer_session {
  set req.backend = F_session_scorer;
  set req.http.Cookie = "session_id=SUPER_SECRET_USER_SESSION; X-Session-State=valid-scorer-state; other=val";
  set bereq.http.Cookie = "session_id=SUPER_SECRET_USER_SESSION; X-Session-State=valid-scorer-state; other=val";
  testing.call_subroutine("vcl_pass");
  assert.contains(bereq.http.Cookie, "X-Session-State=valid-scorer-state");
  assert.not_contains(bereq.http.Cookie, "SUPER_SECRET_USER_SESSION");
  assert.not_contains(bereq.http.Cookie, "session_id=");
  assert.not_contains(bereq.http.Cookie, "other=");
}

// @scope: pass
sub test_scorer_cookie_unset_when_only_customer_cookies_present {
  set req.backend = F_session_scorer;
  set req.http.Cookie = "session_id=SUPER_SECRET_USER_SESSION; auth=tok123";
  set bereq.http.Cookie = "session_id=SUPER_SECRET_USER_SESSION; auth=tok123";
  testing.call_subroutine("vcl_pass");
  assert.is_notset(bereq.http.Cookie);
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"SEC-14 pass_snippet cookie scoping failed:\n{res.stdout}\n{res.stderr}"

    deliver_cookie_vcl = _generate_deliver_set_cookie_vcl("svc-a")
    assert "Secure" in deliver_cookie_vcl, "rum_cid Set-Cookie missing '; Secure'"

    js = rum_assets.generate_rum_tracker_js("svc'; alert(1);//", "/rum-beacon'; fetch('//evil');//")
    assert "var SERVICE_ID" not in js
    assert "svc'; alert(1);//" not in js
    assert "var BEACON_ENDPOINT = '/rum-beacon'; fetch('//evil');//" not in js
    assert "fetch('//evil');//" in js or "\\u0027" in js or json.dumps("/rum-beacon'; fetch('//evil');//") in js

    backend_payloads: list[dict[str, Any]] = []

    def _fake_fastly(
        method: str,
        path: str,
        *,
        token: str = "",
        json_data: Any = None,
        **kw: Any,
    ) -> Any:
        if method == "POST" and "/backend" in path and isinstance(json_data, dict):
            backend_payloads.append(json_data)
            return json_data
        if method == "POST" and path == "/service":
            return {"id": "svc-portal-1"}
        if "/version" in path and method == "GET":
            return [{"number": 1, "active": False}]
        return {"number": 1, "id": "ok"}

    with patch("backend.provision.fastly_api._fastly_api_request", side_effect=_fake_fastly):
        fastly_api.deploy_remote_frontend(
            "portal-svc",
            "portal.example.com",
            "origin.example.com",
            443,
            True,
            "test-sentinel-tok",
        )
    assert backend_payloads, "Expected backend creation call in deploy_remote_frontend"
    assert backend_payloads[0].get("ssl_check_cert") in {True, 1}


def test_f4_vcl_breakout_and_table_lookup_blocked_across_rum_catalog_provision_and_terraform() -> None:
    """SEC-15: table.lookup, block breakouts, unbalanced parens/quotes, and }V rejected across all VCL validators and HTTP endpoints."""
    bad_conditions = [
        'table.lookup(fos_credentials, "secret_key", "") != ""',
        'table.lookup(cdn_auth, "secret_key", "") != ""',
        'req.http.Host == "a") { set resp.http.X-Leak = "1"; } if (true',
        'req.http.x-fastly-key != ""',
        'req.http.Host == "unterminated',
    ]
    for cond in bad_conditions:
        assert validate_rum_custom_condition(cond) is not None, f"Accepted unsafe condition: {cond!r}"

    bad_expressions = [
        '}V " + table.lookup(fos_credentials, "secret_key", "") + {"',
        'table.lookup(cdn_auth, "secret_key", "")',
        'req.http.User-Agent\rset resp.http.X = "1"',
        'regsub(req.url, "a", "b") )',
        'req.http.X + "unterminated',
    ]
    for expr in bad_expressions:
        errs = validate_custom_field(
            {
                "name": "bad_field",
                "label": "Bad",
                "vcl_log_expression": expr,
                "value_type": "string",
                "collection_stage": "edge",
            },
            set(),
        )
        assert errs, f"validate_custom_field accepted unsafe expression: {expr!r}"

    client = make_client("127.0.0.1")
    exfil_cond = 'table.lookup(fos_credentials, "secret_key", "") != ""'
    assert (
        client.post(
            "/api/services/svc-a/rum/settings",
            json={
                "capture_vitals": True,
                "capture_performance": True,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": exfil_cond,
                "token": "test-sentinel-tok",
            },
        ).status_code
        == 400
    )
    assert client.post(
        "/api/provision/terraform/preview",
        json={
            "service_id": "svc-a",
            "fos_bucket_name": "bucket-a",
            "custom_condition": exfil_cond,
        },
    ).status_code in {400, 422}

    # HTTP route check on POST /api/services/svc-a/logging-settings/update (custom_condition query param)
    for bad_http_cond in (
        exfil_cond,
        'req.http.Host == "a") { set resp.http.X-Leak = "1"; } if (true',
    ):
        r_upd = client.post(
            "/api/services/svc-a/logging-settings/update",
            params={"custom_condition": bad_http_cond},
        )
        assert r_upd.status_code in {400, 422}, (
            f"POST /api/services/svc-a/logging-settings/update accepted unsafe condition {bad_http_cond!r}: "
            f"{r_upd.status_code} {r_upd.text}"
        )

    # Terraform HCL template interpolation escaping (${...} -> $${...}, %{...} -> %%{...})
    tf_escaped = generate_terraform(
        {
            "service_id": "svc-a",
            "logging_service_id": "svc-a",
            "fos_bucket_name": "bucket-${var.secret}-%{if true}pwn%{endif}",
            "cdn_service_name": "CDN ${var.secret} %{if true}pwn%{endif}",
            "cdn_secret": "sec-${var.secret}-%{if true}pwn%{endif}",
            "fos_region": "us-east-1",
        },
        "ak-${var.secret}",
        "sk-%{if true}pwn%{endif}",
    )
    for tf_name in ("fos.tf.json", "cdn_proxy.tf.json"):
        tf_body = tf_escaped[tf_name]
        assert "$${var.secret}" in tf_body, f"{tf_name} did not escape ${{var.secret}}"
        assert "%%{if true}pwn%%{endif}" in tf_body, f"{tf_name} did not escape %{{if true}}"
        assert re.search(r"(?<!\$)\$\{var\.secret\}", tf_body) is None, (
            f"Unescaped ${{var.secret}} interpolation in {tf_name}: {tf_body}"
        )
        assert re.search(r"(?<!%)%\{if", tf_body) is None, f"Unescaped %{{if directive in {tf_name}: {tf_body}"


# ============================================================================
# Area F5 — compute/cloud Capture VCL, Front VCL Rate-Limiting & Shell Auth (9)
# ============================================================================


def test_f5_cloud_templates_and_capture_scrub_x_fos_request_in_full_and_rum_only_modes() -> None:
    """SEC-16: templates.json scrub_headers contains X-FOS-Request and Cloud scrubs it even when logging_enabled == false && rum.enabled == true."""
    templates_json = json.loads((CLOUD_DIR / "vcl" / "capture" / "templates.json").read_text(encoding="utf-8"))
    scrub_list = templates_json.get("scrub_headers") or []
    assert any(str(h).lower() == "x-fos-request" for h in scrub_list), (
        "X-FOS-Request missing from compute/cloud/vcl/capture/templates.json scrub_headers"
    )

    for logging_enabled in (True, False):
        snippets = cloud_desired_snippets(
            {
                "logging_enabled": logging_enabled,
                "rum_enabled": True,
                "fos_region": "us-east-1",
                "cdn_shield": "none",
            }
        )
        main_vcl = build_cloud_capture_vcl_wrapper(snippets)
        test_vcl = """
// @scope: recv
sub test_cloud_scrubs_spoofed_x_fos_request {
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/checkout/cart";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.X-FOS-Request);
  assert.equal(req.backend, F_origin);
}
"""
        res = run_falco_test(main_vcl, test_vcl)
        assert res.returncode == 0, (
            f"SEC-16 scrub failed for logging_enabled={logging_enabled}:\n{res.stdout}\n{res.stderr}"
        )


def test_f5_cloud_rum_sigv4_restricted_to_f_fos_origin_non_shield_and_rum_paths() -> None:
    """SEC-16: Cloud sigv4_sign_vcl signs ONLY F_fos_origin && !req.backend.is_shield && (/js/rum.js || /js/faro-sdk.js)."""
    snippets = cloud_desired_snippets(
        {
            "logging_enabled": False,
            "rum_enabled": True,
            "fos_region": "us-east-1",
            "cdn_shield": "none",
        }
    )
    main_vcl = build_cloud_capture_vcl_wrapper(snippets)
    test_vcl = """
// @scope: miss
sub test_cloud_refuses_customer_origin_backend {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_origin;
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_cloud_refuses_non_rum_path_on_f_fos_origin {
  testing.inject_variable("req.backend.is_shield", false);
  set req.backend = F_fos_origin;
  set req.url = "/processed/request/secret.parquet";
  set bereq.url = "/processed/request/secret.parquet";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_cloud_refuses_shield_hop {
  testing.inject_variable("req.backend.is_shield", true);
  set req.backend = F_fos_origin;
  set req.url = "/js/faro-sdk.js";
  set bereq.url = "/js/faro-sdk.js";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}
"""
    res = run_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"SEC-16 sigv4 guard failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_uses_digest_secure_is_equal_on_x_fastly_key() -> None:
    """SEC-17: compute/cloud/vcl/front/main.vcl compares x-fastly-key in constant time via digest.secure_is_equal."""
    front_src = FRONT_MAIN_VCL.read_text(encoding="utf-8")
    non_comment_src = "\n".join(line for line in front_src.splitlines() if not line.lstrip().startswith(("#", "//")))
    assert (
        re.search(
            r"digest\.secure_is_equal\(\s*(?:var\.cdn_key\s*,\s*req\.http\.x-fastly-key|req\.http\.x-fastly-key\s*,\s*var\.cdn_key)\s*\)",
            non_comment_src,
        )
        is not None
    ), "front/main.vcl must compare var.cdn_key and req.http.x-fastly-key directly via digest.secure_is_equal(...)"
    assert (
        re.search(
            r"(?:var\.cdn_key\s*[!=]=\s*req\.http\.x-fastly-key|req\.http\.x-fastly-key\s*[!=]=\s*var\.cdn_key)",
            non_comment_src,
        )
        is None
    ), "front/main.vcl contains non-constant-time ==/!= comparison between var.cdn_key and req.http.x-fastly-key"

    main_vcl, extra = build_front_vcl_wrapper()
    test_vcl = f"""
// @scope: recv
sub test_secure_is_equal_accepts_valid_and_rejects_wrong_x_fastly_key {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.90", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/processed/request/p.parquet";
  set req.http.x-fastly-key = "{DEFAULT_CDN_KEY}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);

  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/processed/request/p.parquet";
  set req.http.x-fastly-key = "wrong-cdn-key";
  testing.call_subroutine("vcl_recv");
  assert.error(622);
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"front/main.vcl x-fastly-key behavioral test failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_enforces_penaltybox_on_fos_sign_in_and_fos_pq_before_rewrites() -> None:
    """SEC-17: Penaltyboxed IPv4 client is blocked (618 -> 429) on /fos/*, /sign-in, and /fos/pq/* BEFORE rewrites."""
    main_vcl, extra = build_front_vcl_wrapper()
    sess = "valid-session-after-lockout"
    valid_cdn = make_flp_cdn_cookie(session_cookie=sess, exp=int(time.time()) + 240)
    scoped_cdn = make_flp_cdn_cookie(session_cookie=sess, exp=int(time.time()) + 240, scope="logs/")

    deliver_fails = "\n  ".join(
        [
            """testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "198.51.100.91";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
        ]
        * 10
    )
    test_vcl = f"""
// @scope: recv
sub test_wrong_fos_key_emits_622 {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.91", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/processed/request/p.parquet";
  set req.http.x-fastly-key = "wrong-cdn-key";
  testing.call_subroutine("vcl_recv");
  assert.error(622);
}}

// @scope: deliver
sub test_deliver_auth_fails_populate_penaltybox {{
  {deliver_fails}
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "198.51.100.91"));
}}

// @scope: recv
sub test_penaltybox_covers_fos_signin_and_fos_pq {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.91", 15m);

  // 1. /fos/* with VALID x-fastly-key must now error 618
  testing.inject_variable("client.ip", std.str2ip("198.51.100.91", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/processed/request/p.parquet";
  set req.http.x-fastly-key = "{DEFAULT_CDN_KEY}";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  // 2. /sign-in must error 618
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/sign-in";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  // 3. /fos/pq/* with VALID flp_cdn must error 618 BEFORE rewrite
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={valid_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}

// @scope: recv
sub test_penaltybox_covers_non_processed_fos_raw_path {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.91", 15m);
  testing.inject_variable("client.ip", std.str2ip("198.51.100.91", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/{FIXTURE_22_SID}/raw/request/f.zst";
  set req.http.x-fastly-key = "{DEFAULT_CDN_KEY}";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}

// @scope: recv
sub test_penaltybox_covers_non_fos_scoped_processed_path_before_rewrite {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.91", 15m);
  testing.inject_variable("client.ip", std.str2ip("198.51.100.91", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/logs/processed/request/p.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={scoped_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}

// @scope: error
sub test_error_618_emits_429_no_store_and_retry_after {{
  set obj.status = 618;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 429);
  assert.contains(obj.http.Cache-Control, "no-store");
  assert.equal(obj.http.Retry-After, "900");
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, (
        f"SEC-17 penaltybox coverage on /fos, /sign-in, /fos/pq failed:\n{res.stdout}\n{res.stderr}"
    )


def test_f5_cloud_front_vcl_normalizes_ipv6_64_subnet_for_penaltybox_lockout() -> None:
    """SEC-17: Penaltybox normalizes IPv6 client.ip to /64 so host rotation within /64 cannot evade lockout."""
    main_vcl, extra = build_front_vcl_wrapper()
    deliver_calls = "\n  ".join(
        [
            f"""testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "2001:db8:abcd:5678::{i + 1}";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
            for i in range(10)
        ]
    )
    test_vcl = f"""
// @scope: deliver
sub test_cloud_ipv6_64_deliver_normalizes_rotated_hosts {{
  {deliver_calls}
  declare local var.sib_ip IP;
  declare local var.sib_key STRING;
  set var.sib_ip = std.str2ip("2001:db8:abcd:5678:dead:beef:cafe:99", "::1");
  set var.sib_key = "v6:" + addr.extract_bits(var.sib_ip, 96, 32) + ":" + addr.extract_bits(var.sib_ip, 64, 32);
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, var.sib_key));

  set var.sib_ip = std.str2ip("2001:db8:abcd:5679::1", "::1");
  set var.sib_key = "v6:" + addr.extract_bits(var.sib_ip, 96, 32) + ":" + addr.extract_bits(var.sib_ip, 64, 32);
  assert.false(ratelimit.penaltybox_has(auth_fail_pb, var.sib_key));
}}

// @scope: recv
sub test_cloud_ipv6_64_recv_blocks_rotated_host_only {{
  declare local var.atk_ip IP;
  declare local var.atk_key STRING;
  set var.atk_ip = std.str2ip("2001:db8:abcd:5678::1", "::1");
  set var.atk_key = "v6:" + addr.extract_bits(var.atk_ip, 96, 32) + ":" + addr.extract_bits(var.atk_ip, 64, 32);
  ratelimit.penaltybox_add(auth_fail_pb, var.atk_key, 15m);

  // Different host in SAME /64 -> blocked 618
  testing.inject_variable("client.ip", std.str2ip("2001:db8:abcd:5678:dead:beef:cafe:99", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/api/auth/session";
  testing.call_subroutine("vcl_recv");
  assert.error(618);

  // Different /64 subnet -> NOT blocked
  testing.inject_variable("client.ip", std.str2ip("2001:db8:abcd:5679::1", "::1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/api/auth/session";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"SEC-17 IPv6 /64 normalization failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_emits_auth_fail_and_penalizes_621_622_and_624() -> None:
    """SEC-17: 621, 622, and 624 errors set obj.http.x-flp-auth-fail = '1' and feed auth_fail_pb via vcl_deliver."""
    main_vcl, extra = build_front_vcl_wrapper()
    deliver_10 = "\n  ".join(
        [
            """testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "198.51.100.95";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
        ]
        * 10
    )
    for code, expected_http in ((621, 404), (622, 403), (624, 403)):
        test_vcl = f"""
// @scope: error
sub test_code_{code}_sets_auth_fail_marker {{
  set obj.status = {code};
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, {expected_http});
  assert.equal(obj.http.x-flp-auth-fail, "1");
}}

// @scope: deliver
sub test_code_{code}_marker_adds_to_penaltybox {{
  {deliver_10}
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "198.51.100.95"));
}}

// @scope: recv
sub test_code_{code}_penaltybox_blocks_recv {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.95", 15m);
  testing.inject_variable("client.ip", std.str2ip("198.51.100.95", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/api/auth/session";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}
"""
        res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
        assert res.returncode == 0, f"Error {code} did not feed auth_fail_pb:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_front_vcl_tampered_flp_cdn_hmac_triggers_auth_fail_and_penaltybox() -> None:
    """SEC-17: Tampered flp_cdn HMAC signature raises 629 -> x-flp-auth-fail + cookie clear -> penaltybox lockout."""
    main_vcl, extra = build_front_vcl_wrapper()
    sess = "victim-browser-session"
    tampered_cdn = make_flp_cdn_cookie(
        session_cookie=sess,
        cdn_key=DEFAULT_CDN_KEY,
        sid=FIXTURE_22_SID,
        role="admin",
        exp=int(time.time()) + 240,
        tamper_sig=True,
    )
    deliver_10 = "\n  ".join(
        [
            """testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.http.Fastly-Client-IP = "198.51.100.96";
  set resp.http.x-flp-auth-fail = "1";
  testing.call_subroutine("vcl_deliver");"""
        ]
        * 10
    )
    test_vcl = f"""
// @scope: recv
sub test_tampered_flp_cdn_raises_629 {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.96", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={tampered_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(629);
}}

// @scope: error
sub test_error_629_sets_auth_fail_and_clears_cookies {{
  set obj.status = 629;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 403);
  assert.equal(obj.http.x-flp-auth-fail, "1");
  assert.true(obj.http.Set-Cookie ~ "Max-Age=0");
}}

// @scope: deliver
sub test_tampered_flp_cdn_deliver_penalizes_ip {{
  {deliver_10}
  assert.true(ratelimit.penaltybox_has(auth_fail_pb, "198.51.100.96"));
}}

// @scope: recv
sub test_tampered_flp_cdn_recv_enforces_618 {{
  ratelimit.penaltybox_add(auth_fail_pb, "198.51.100.96", 15m);
  testing.inject_variable("client.ip", std.str2ip("198.51.100.96", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/api/auth/session";
  testing.call_subroutine("vcl_recv");
  assert.error(618);
}}
"""
    res = run_falco_test(main_vcl, test_vcl, extra_files=extra)
    assert res.returncode == 0, f"Tampered flp_cdn 629->auth-fail->618 failed:\n{res.stdout}\n{res.stderr}"


def test_f5_cloud_shell_invalid_non_empty_flp_session_clears_cookie_and_sets_auth_fail(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-17: Invalid non-empty flp_session returns 401 with x-flp-auth-fail: 1 and clears stale flp_session."""
    resp = cloud_server.request(
        "GET",
        "/api/services",
        session_cookie="forged-invalid-session-token-value",
    )
    assert resp.status_code == 401
    assert resp.headers.get("x-flp-auth-fail") == "1", (
        f"Missing x-flp-auth-fail: 1 on forged flp_session (headers={dict(resp.headers)})"
    )
    set_cookies = resp.headers.get_list("set-cookie")
    assert any("flp_session=;" in sc or "Max-Age=0" in sc for sc in set_cookies), (
        f"Stale forged flp_session cookie was not cleared on 401: {set_cookies}"
    )


def test_f5_cloud_shell_invalid_non_empty_flp_setup_cookie_sets_auth_fail(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-17: Invalid non-empty flp_setup cookie sets x-flp-auth-fail: 1."""
    resp = cloud_server.request(
        "GET",
        "/api/admin/system-checks",
        setup_cookie="forged-invalid-setup-grant-value",
    )
    assert resp.status_code in {401, 403}
    assert resp.headers.get("x-flp-auth-fail") == "1", (
        f"Missing x-flp-auth-fail: 1 on forged flp_setup (headers={dict(resp.headers)})"
    )


# ============================================================================
# Area F6 — compute/cloud Logout Revocation, /api/views RBAC & RSC Gating (8)
# ============================================================================


def test_f6_cloud_logout_clears_all_three_cookies_and_revokes_session_server_side(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-18: POST /api/auth/logout clears flp_session, flp_cdn, and __Host-flp_cdn, and invalidates session replay."""
    admin = cloud_server.register_admin()
    old_session = admin.session_cookie

    r_logout = cloud_server.request(
        "POST",
        "/api/auth/logout",
        session_cookie=old_session,
        json_body={},
    )
    assert r_logout.status_code == 200
    set_cookies = r_logout.headers.get_list("set-cookie")
    for expected_cookie in ("flp_session=", "flp_cdn=", "__Host-flp_cdn="):
        assert any(sc.startswith(expected_cookie) and "Max-Age=0" in sc for sc in set_cookies), (
            f"Logout did not clear {expected_cookie}: {set_cookies}"
        )

    r_replay = cloud_server.request("GET", "/api/services", session_cookie=old_session)
    assert r_replay.status_code == 401, f"Replayed flp_session after logout was accepted ({r_replay.status_code})!"


def test_f6_cloud_flp_cdn_enforces_short_ttl_window(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-18: flp_cdn cookie issued by Compute uses tightened short TTL (<= 300s, never 3600s)."""
    admin = cloud_server.register_admin()
    cloud_server.provision_fixture_service(admin)

    r_ext = cloud_server.request("GET", "/api/log-extents", session_cookie=admin.session_cookie)
    assert r_ext.status_code == 200

    issued = cloud_issue_cdn_cookie(session_cookie=admin.session_cookie, role="admin")
    set_cookie = issued["set_cookie"]
    assert set_cookie is not None and set_cookie.startswith("flp_cdn=")
    assert "HttpOnly" in set_cookie and "Secure" in set_cookie
    m_age = re.search(r"Max-Age=(\d+)", set_cookie, re.IGNORECASE)
    assert m_age is not None, f"Missing Max-Age on flp_cdn Set-Cookie: {set_cookie}"
    ttl = int(m_age.group(1))
    assert issued["ttl"] == ttl
    assert 180 <= ttl <= 300, f"Expected tightened CDN_COOKIE_TTL in 180..300s, got Max-Age={ttl}"

    # Verify SEC-18 auth_epoch (M7) & flp_session hash binding (M8) in front/main.vcl
    now_ts = int(time.time()) + 240
    sess_owner = admin.session_cookie
    stale_epoch_cdn = make_flp_cdn_cookie(
        session_cookie=sess_owner,
        cdn_key=DEFAULT_CDN_KEY,
        sid=FIXTURE_22_SID,
        role="admin",
        exp=now_ts,
        epoch="1",
    )
    current_epoch_cdn = make_flp_cdn_cookie(
        session_cookie=sess_owner,
        cdn_key=DEFAULT_CDN_KEY,
        sid=FIXTURE_22_SID,
        role="admin",
        exp=now_ts,
        epoch="2",
    )
    epoch2_vcl, epoch2_extra = build_front_vcl_wrapper(extra_front_auth={"auth_epoch": "2"})
    epoch_and_shash_test = f"""
// @scope: recv
sub test_stale_auth_epoch_refused_direct_fos_rewrite {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.140", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session={sess_owner}; flp_cdn={stale_epoch_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.backend, F_compute);
  assert.contains(req.url, "/api/file?key=");
  assert.is_notset(req.http.x-flp-api-file);
}}

// @scope: recv
sub test_mismatched_flp_session_hash_refused_direct_fos_rewrite {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.141", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session=attacker-different-session-token; flp_cdn={current_epoch_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.equal(req.backend, F_compute);
  assert.contains(req.url, "/api/file?key=");
  assert.is_notset(req.http.x-flp-api-file);
}}

// @scope: recv
sub test_matching_epoch_and_session_authorizes_fos_lookup {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.142", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session={sess_owner}; flp_cdn={current_epoch_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/{FIXTURE_22_SID}/processed/request/part-000.parquet");
}}
"""
    res_epoch = run_falco_test(epoch2_vcl, epoch_and_shash_test, extra_files=epoch2_extra)
    assert res_epoch.returncode == 0, (
        f"SEC-18 auth_epoch / session_shash VCL check failed:\n{res_epoch.stdout}\n{res_epoch.stderr}"
    )

    # Backwards-compat rollout window: unset auth_epoch dictionary item defaults to "0" and accepts v1/epoch=0 cookie
    v1_issued = cloud_issue_cdn_cookie(session_cookie=sess_owner, role="admin", scope="", auth_epoch=0)
    m_v1 = re.search(r"flp_cdn=([^;]+)", v1_issued["set_cookie"] or "")
    assert m_v1 is not None
    v1_cookie_val = m_v1.group(1)
    unset_epoch_vcl, unset_epoch_extra = build_front_vcl_wrapper(extra_front_auth=None)
    res_compat = run_falco_test(
        unset_epoch_vcl,
        f"""
// @scope: recv
sub test_unset_auth_epoch_backwards_compat_v1_cookie {{
  testing.inject_variable("client.ip", std.str2ip("198.51.100.143", "127.0.0.1"));
  testing.inject_variable("req.restarts", 0);
  testing.inject_variable("fastly.ff.visits_this_service", 0);
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/part-000.parquet";
  set req.http.Cookie = "flp_session={sess_owner}; flp_cdn={v1_cookie_val}";
  testing.call_subroutine("vcl_recv");
  assert.not_error();
  assert.state(lookup);
  assert.equal(req.url, "/fos/{FIXTURE_22_SID}/processed/request/part-000.parquet");
}}
""",
        extra_files=unset_epoch_extra,
    )
    assert res_compat.returncode == 0, (
        f"SEC-18 unset auth_epoch backwards-compat check failed:\n{res_compat.stdout}\n{res_compat.stderr}"
    )

    # Verify admin role change and user revocation advance auth_epoch and sync flp_front_auth["auth_epoch"]
    epoch_bump = cloud_check_revoke_epoch_bump()
    assert epoch_bump["role_status"] == 200
    assert epoch_bump["revoke_status"] == 200
    assert epoch_bump["epoch_initial"] == 0
    assert epoch_bump["epoch_after_role"] == 1
    assert epoch_bump["dict_after_role"] == "1"
    assert epoch_bump["epoch_after_revoke"] == 2
    assert epoch_bump["dict_after_revoke"] == "2"


def test_f6_cloud_views_post_and_delete_restricted_to_admin_role(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-19: POST /api/views and DELETE /api/views/* return 403 for Analyst and AnalystNoPii."""
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="Analyst")
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPii")

    r_admin_view = cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        json_body={
            "service_id": sid,
            "name": "Admin View",
            "filters_json": json.dumps({"status": {"mode": "include", "values": ["200"]}}),
        },
    )
    assert r_admin_view.status_code == 201
    view_id = r_admin_view.json()["id"]

    for user in (analyst, nopii):
        r_post = cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=user.session_cookie,
            json_body={
                "service_id": sid,
                "name": "Unauthorized View",
                "filters_json": "{}",
            },
        )
        assert r_post.status_code == 403, f"{user.role} POST /api/views returned {r_post.status_code}"

        r_del = cloud_server.request(
            "DELETE",
            f"/api/views/{view_id}",
            session_cookie=user.session_cookie,
        )
        assert r_del.status_code == 403, f"{user.role} DELETE /api/views returned {r_del.status_code}"


def test_f6_cloud_analyst_no_pii_hides_or_redacts_pii_filter_views(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-19: AnalystNoPii never receives raw PII filter values from GET /api/views/{sid}."""
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPii")

    cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        json_body={
            "service_id": sid,
            "name": "Safe POP View",
            "filters_json": json.dumps({"pop": {"mode": "include", "values": ["SJC"]}}),
        },
    )
    cloud_server.request(
        "POST",
        "/api/views",
        session_cookie=admin.session_cookie,
        json_body={
            "service_id": sid,
            "name": "Sensitive IP & Session View",
            "filters_json": json.dumps(
                {
                    "client_ip": {"mode": "include", "values": ["203.0.113.200"]},
                    "edge_sid": {"mode": "include", "values": ["sid-secret-999"]},
                }
            ),
        },
    )

    r_views = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=nopii.session_cookie)
    assert r_views.status_code == 200
    raw_text = r_views.text
    assert "Safe POP View" in raw_text, f"Positive non-PII view missing from AnalystNoPii response: {raw_text}"
    assert "203.0.113.200" not in raw_text, f"AnalystNoPii leaked client_ip in views: {raw_text}"
    assert "sid-secret-999" not in raw_text, f"AnalystNoPii leaked edge_sid in views: {raw_text}"


def test_f6_cloud_blocks_unauthenticated_and_non_admin_nested_rsc_subpaths(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-20: Nested /admin/* and /logs/* RSC payload subpaths (__next.*.txt) require Admin."""
    admin = cloud_server.register_admin()
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="Analyst")
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPii")

    rsc_paths = (
        "/admin/__next.admin.txt",
        "/admin/share/__next.admin.share.txt",
        "/admin/users/__next.page.txt",
        "/logs/__next.logs.txt",
    )
    for path in rsc_paths:
        r_unauth = cloud_server.request("GET", path)
        assert r_unauth.status_code in {302, 303, 307, 401, 403}, (
            f"Unauth GET {path} leaked {r_unauth.status_code}: {r_unauth.text}"
        )
        assert "SECRET_PAYLOAD" not in r_unauth.text

        for user in (analyst, nopii):
            r_non_admin = cloud_server.request("GET", path, session_cookie=user.session_cookie)
            assert r_non_admin.status_code == 403, f"{user.role} GET {path} returned {r_non_admin.status_code}"
            assert "SECRET_PAYLOAD" not in r_non_admin.text


def test_f6_cloud_rejects_vcl_breakout_and_dictionary_exfiltration_in_conditions_and_fields(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-20: Cloud validate_custom_condition and vcl_log_expression block }V, {, }, \\r, table.lookup, secret headers, and unbalanced parens/quotes."""
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)

    for bad_cond in (
        'table.lookup(flp_fos_credentials, "sk", "") != ""',
        'req.http.Host == "a") { set resp.http.X = "1"; } if (true',
        'req.http.Host == "unclosed',
        'req.url ~ "a") )',
    ):
        r_cond = cloud_server.request(
            "PATCH",
            f"/api/admin/services/{sid}",
            session_cookie=admin.session_cookie,
            json_body={
                "generation": cloud_server.service_generation(admin, sid),
                "custom_condition": bad_cond,
            },
        )
        assert r_cond.status_code in {400, 422}, (
            f"Cloud accepted unsafe custom_condition {bad_cond!r}: {r_cond.status_code}"
        )
        assert r_cond.json().get("error") == "invalid_logging_settings", r_cond.text

        r_rum_cond = cloud_server.request(
            "POST",
            f"/api/admin/services/{sid}/reconcile",
            session_cookie=admin.session_cookie,
            json_body={
                "generation": cloud_server.service_generation(admin, sid),
                "rum": {"enabled": True, "custom_condition": bad_cond},
            },
        )
        assert r_rum_cond.status_code in {400, 422}, (
            f"Cloud reconcile accepted unsafe rum.custom_condition {bad_cond!r}: {r_rum_cond.status_code}"
        )

    for bad_expr in (
        '}V " + table.lookup(flp_fos_credentials, "sk", "") + {"',
        'table.lookup(flp_front_auth, "front_key", "")',
        "req.http.x-flp-front-key",
        "req.http.x-flp-worker-key",
        "req.http.x-fastly-key",
        'req.http.User-Agent\rset resp.http.X = "1"',
        'regsub(req.url, "a", "b") )',
        'req.http.X + "unterminated',
    ):
        r_val = cloud_server.request(
            "POST",
            "/api/admin/log-fields/custom/validate",
            session_cookie=admin.session_cookie,
            json_body={
                "field": {
                    "name": "evil_field",
                    "label": "Evil",
                    "vcl_log_expression": bad_expr,
                    "value_type": "string",
                    "duckdb_type": "VARCHAR",
                    "bytes_estimate": 48,
                    "collection_stage": "edge",
                    "enabled": True,
                },
                "existing_names": [],
            },
        )
        assert r_val.status_code in {200, 400, 422}
        if r_val.status_code == 200:
            assert r_val.json()["valid"] is False, f"Cloud validate accepted {bad_expr!r}"


@pytest.mark.parametrize(
    "secret_hdr",
    ("x-fastly-key", "x-flp-front-key", "x-flp-worker-key"),
)
def test_f6_cloud_rejects_secret_headers_in_vcl_conditions_and_rum_conditions(
    cloud_server: CloudTestServer,
    secret_hdr: str,
) -> None:
    """SEC-20: Cloud validate_vcl_condition rejects internal/secret headers on both service PATCH and RUM reconcile."""
    admin = cloud_server.register_admin()
    sid = cloud_server.provision_fixture_service(admin)
    cond = f'req.http.{secret_hdr} != ""'

    r_svc = cloud_server.request(
        "PATCH",
        f"/api/admin/services/{sid}",
        session_cookie=admin.session_cookie,
        json_body={
            "generation": cloud_server.service_generation(admin, sid),
            "custom_condition": cond,
        },
    )
    assert r_svc.status_code in {400, 422}, (
        f"Cloud PATCH accepted secret header condition {cond!r}: {r_svc.status_code} {r_svc.text}"
    )

    r_rum = cloud_server.request(
        "POST",
        f"/api/admin/services/{sid}/reconcile",
        session_cookie=admin.session_cookie,
        json_body={
            "generation": cloud_server.service_generation(admin, sid),
            "rum": {"enabled": True, "custom_condition": cond},
        },
    )
    assert r_rum.status_code in {400, 422}, (
        f"Cloud reconcile accepted secret header rum.custom_condition {cond!r}: {r_rum.status_code} {r_rum.text}"
    )


def test_f6_cloud_rum_tracker_js_escapes_service_id_via_json_encoding() -> None:
    """SEC-20 / SEC-22: compute/cloud logging/rum.rs omits service_id from client JS entirely."""
    payload = "svc'; alert(1); // */"
    js = cloud_rum_tracker_js(payload)
    assert "SERVICE_ID" not in js
    assert payload not in js
    assert json.dumps(payload) not in js


def test_f6_cloud_audit_entries_enforces_internal_admin_check_and_http_403(
    cloud_server: CloudTestServer,
) -> None:
    """SEC-20: GET /api/admin/audit-logs returns 403 for non-Admin and AuthService::audit_entries enforces admin()."""
    admin = cloud_server.register_admin()
    analyst = cloud_server.invite_and_register(admin, role="analyst", display_name="Analyst")
    nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPii")

    for user in (analyst, nopii):
        r = cloud_server.request("GET", "/api/admin/audit-logs", session_cookie=user.session_cookie)
        assert r.status_code == 403

    rbac = cloud_check_audit_entries_rbac()
    assert rbac["analyst_forbidden"] is True
    assert rbac["nopii_forbidden"] is True
    assert rbac["anon_forbidden"] is True
    assert rbac["admin_count"] == 1
