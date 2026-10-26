"""Tests for session_scoring_vcl snippets, session_scoring_setup provisioning, and rotate_aes_key."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from backend.provision.session_scoring_setup import (
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KV_STORE_MANAGE_URL,
    MATRIX_RESOURCE_LINK_NAME,
    PREVIOUS_KEY_HEX,
    SCORING_ENABLED_AT_KEY,
    EntitlementError,
    delete_scoring_service,
    ensure_scoring_service,
    rotate_aes_key,
)
from backend.provision.session_scoring_vcl import (
    DEFAULT_ENFORCE_STATUS_CODE,
    SCORING_BACKEND_API_NAME,
    SCORING_BACKEND_VCL_NAME,
    SCORING_DELIVER_NAME,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
    SCORING_SNIPPET_PRIORITY,
    deliver_snippet,
    enforce_reason_phrase,
    enforce_snippet,
    fetch_snippet,
    generate_scoring_vcl,
    miss_snippet,
    pass_snippet,
    recv_snippet,
    resolve_enforce_status_code,
    scoring_snippet_names,
)

from backend.provision import session_scoring_setup as sss

LOG_SVC = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"
KEYS_STORE_ID = "KEYS_STORE_XYZ"
CURRENT_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{CURRENT_KEY_HEX}"
PREVIOUS_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{PREVIOUS_KEY_HEX}"
ITEM_COLLECTION_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item"


def _hex64(byte: int) -> str:
    return f"{byte:02x}" * 32


# ── 1. Snippet Constants & Recv Snippet ──────────────────────────────────────


def test_snippet_constants_and_naming_templates():
    assert scoring_snippet_names() == [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]
    assert SCORING_SNIPPET_PRIORITY == 100
    assert SCORING_BACKEND_API_NAME == "session_scorer" and SCORING_BACKEND_VCL_NAME == "F_session_scorer"
    assert sss._scoring_service_name("ABC123") == "Session Scoring Service for ABC123"
    d = sss._scoring_domain("MixedCaseSvcId001")
    assert d == "fos-mixedcasesvcid001-session-scorer.edgecompute.app" == d.lower()


def test_recv_snippet_routing_scrub_and_guards():
    vcl = recv_snippet("MyServiceId")
    assert f"set req.backend = {SCORING_BACKEND_VCL_NAME};" in vcl
    assert "fastly.ff.visits_this_service == 0" in vcl
    assert "req.restarts == 0" in vcl and 'req.http.X-Edge-Scoring-Pass != "1"' in vcl
    assert "X-Edge-Shield-Auth" not in vcl
    assert "X-Edge-Scorer-Auth" not in vcl and "X-Edge-Service-Id" not in vcl
    assert "set req.http.X-Edge-Prev-Route" not in vcl and "set req.http.X-Edge-Last-Route" not in vcl

    # Client-edge scrub unsets must appear after the boundary guard
    boundary_idx = vcl.find("req.restarts == 0 && fastly.ff.visits_this_service == 0")
    assert boundary_idx != -1
    for unset in (
        "unset req.http.X-Edge-Scoring-Pass",
        "unset req.http.X-Edge-Score",
        "unset req.http.X-Edge-Sid",
        "unset req.http.X-Edge-Score-Enforce",
        "unset req.http.X-Edge-Score-Reason",
        "unset req.http.X-Edge-Score-Set-Cookie",
        "unset req.http.X-Edge-Prev-Anchor",
        "unset req.http.x-sigsci-skip-inspection-once;",
    ):
        assert vcl.find(unset) > boundary_idx, f"missing or misplaced scrub: {unset!r}"

    # Post-scoring restart re-enables shielding and unsets skip-inspection
    restart_idx = vcl.find("req.restarts == 1 && req.http.x-edge-score")
    assert restart_idx != -1 and "set var.fastly_req_do_shield = true;" in vcl
    assert vcl.count("unset req.http.x-sigsci-skip-inspection-once;") == 2
    assert vcl.find("unset req.http.x-sigsci-skip-inspection-once;", restart_idx) > restart_idx

    # DDoS guard, RTT stamp, NGWAF skip-inspection, and scoring pass marker order
    assert "!fastly.ddos_detected" in vcl
    assert (
        vcl.find('req.http.X-Edge-Scoring-Pass != "1"')
        < vcl.find("!fastly.ddos_detected")
        < vcl.find("std.tolower(req.url) !~")
    )
    assert "set req.http.x-edge-score-t0 = time.elapsed.usec;" in vcl
    assert 'set req.http.X-Edge-Scoring-Pass = "1";' in vcl
    backend_idx = vcl.find(f"set req.backend = {SCORING_BACKEND_VCL_NAME};")
    skip_idx = vcl.find('set req.http.x-sigsci-skip-inspection-once = "true";')
    t0_idx = vcl.find("set req.http.x-edge-score-t0")
    pass_idx = vcl.find("return(pass);")
    assert backend_idx < skip_idx < pass_idx and t0_idx < pass_idx


# ── 2. Pass, Fetch, Deliver, Miss & Enforce Snippets ─────────────────────────


def test_pass_fetch_and_miss_snippets():
    vcl = pass_snippet("MyServiceId", "deadbeef1234567890")
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in vcl
    assert 'set bereq.method = "GET";' in vcl
    assert 'set bereq.http.X-Edge-Service-Id = "MyServiceId";' in vcl
    assert 'set bereq.http.X-Edge-Scorer-Auth = "deadbeef1234567890";' in vcl
    assert "unset bereq.http.X-Edge-Scoring-Pass;" in vcl and "unset bereq.http.x-edge-score;" in vcl

    a, b = pass_snippet("Svc", "secret_a"), pass_snippet("Svc", "secret_b")
    assert a != b and "secret_a" in a and "secret_a" not in b and "secret_b" in b

    fetch_vcl = fetch_snippet()
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in fetch_vcl and "return(deliver);" in fetch_vcl

    miss_vcl = miss_snippet()
    assert "unset bereq.http.x-edge-score;" in miss_vcl and "unset bereq.http.X-Edge-Scoring-Pass;" in miss_vcl


def test_deliver_snippet_subfields_rtt_and_anti_leak():
    vcl = deliver_snippet()
    assert 'if (req.http.X-Edge-Scoring-Pass == "1")' in vcl and "unset req.http.X-Edge-Scoring-Pass;" in vcl
    assert "\n  restart;\n" in vcl and "return(restart)" not in vcl

    # Subfield captures
    for sub, src in (
        ("score", "x-edge-score"),
        ("l1", "x-edge-score-l1"),
        ("l2", "x-edge-score-l2"),
        ("compliance", "X-Edge-Cookie-Compliance"),
        ("reason", "x-edge-score-reason"),
        ("sid", "x-edge-sid"),
        ("enforce", "x-edge-score-enforce"),
        ("exec", "x-edge-score-exec-us"),
        ("matrix", "X-Edge-Matrix-Version"),
        ("set-cookie", "Set-Cookie"),
    ):
        assert f"set req.http.x-edge-score:{sub} = resp.http.{src};" in vcl

    # Matrix capture must happen BEFORE anti-leak unset
    assert vcl.index("set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;") < vcl.index(
        "unset resp.http.X-Edge-Matrix-Version;"
    )

    # Anti-leak unsets
    for header in (
        "x-edge-score",
        "x-edge-score-l1",
        "x-edge-score-l2",
        "x-edge-score-reason",
        "x-edge-sid",
        "x-edge-score-enforce",
        "x-edge-score-exec-us",
        "X-Edge-Cookie-Compliance",
        "X-Edge-Matrix-Version",
    ):
        assert f"unset resp.http.{header};" in vcl

    # Fail-open branch
    assert "} else {" in vcl
    assert 'set req.http.x-edge-score:score = "0";' in vcl
    assert 'set req.http.x-edge-score:compliance = "unknown";' in vcl
    assert 'set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;' in vcl

    # RTT calculation before resp.status branch
    for stmt in (
        "declare local var.rtt INTEGER;",
        "set var.rtt = std.atoi(time.elapsed.usec);",
        "set var.rtt -= std.atoi(req.http.x-edge-score-t0);",
        'set req.http.x-edge-score:rtt = "" + var.rtt;',
    ):
        assert stmt in vcl
    assert vcl.index("set req.http.x-edge-score:rtt") < vcl.index("if (resp.status == 200)")

    # Pass 2 additive Set-Cookie at edge only
    assert 'if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {' in vcl
    assert "add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;" in vcl
    assert "set resp.http.Set-Cookie =" not in vcl and "X-Edge-Shield-Auth" not in vcl


def test_enforce_snippet_guards_and_parameterized_status_codes():
    vcl = enforce_snippet()
    if_idx = vcl.find("if (")
    assert if_idx != -1
    tail = vcl[if_idx:]
    edge_idx = tail.find("fastly.ff.visits_this_service == 0")
    restart_idx = tail.find("req.restarts == 1")
    enforce_idx = tail.find('req.http.x-edge-score:enforce == "1"')
    assert 0 <= edge_idx < restart_idx < enforce_idx
    assert "&&" in tail[edge_idx:restart_idx] and "&&" in tail[restart_idx:enforce_idx]
    assert 'error 429 "Too Many Requests"' in vcl and "X-Edge-Shield-Auth" not in vcl

    assert DEFAULT_ENFORCE_STATUS_CODE == 429
    for raw, expected in [(None, 429), (403, 403), (99, 429), (600, 429), ("not-an-int", 429)]:
        assert resolve_enforce_status_code(raw) == expected
    for code, phrase in [
        (403, "Forbidden"),
        (429, "Too Many Requests"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
        (511, "Network Authentication Required"),
        (599, "Blocked"),
        (444, "Blocked"),
    ]:
        assert enforce_reason_phrase(code) == phrase

    for code, phrase in [
        (403, "Forbidden"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
        (418, "I'm a Teapot"),
        (599, "Blocked"),
        (99, "Too Many Requests"),
        (600, "Too Many Requests"),
    ]:
        effective = 429 if code in (99, 600) else code
        snippet = enforce_snippet(code)
        assert f'error {effective} "{phrase}"' in snippet
        assert "fastly.ff.visits_this_service == 0" in snippet and "req.restarts == 1" in snippet


def test_generate_scoring_vcl_integration_and_injection_guards():
    snippets = generate_scoring_vcl("UniqueServiceXYZ", "deadbeef1234", enforce_status_code=451)
    assert set(snippets.keys()) == set(scoring_snippet_names())
    assert 'error 451 "Unavailable For Legal Reasons"' in snippets[SCORING_ENFORCE_NAME]
    assert (
        'error 429 "Too Many Requests"'
        in generate_scoring_vcl("Svc", "sec", enforce_status_code=None)[SCORING_ENFORCE_NAME]
    )

    # Service ID and secret baked ONLY into pass_snippet
    assert "UniqueServiceXYZ" in snippets[SCORING_PASS_NAME] and "deadbeef1234" in snippets[SCORING_PASS_NAME]
    for other in (SCORING_RECV_NAME, SCORING_FETCH_NAME, SCORING_DELIVER_NAME, SCORING_MISS_NAME, SCORING_ENFORCE_NAME):
        assert "UniqueServiceXYZ" not in snippets[other] and "deadbeef1234" not in snippets[other]

    # Pure determinism & no spoofable Matrix-Age-Days header
    assert generate_scoring_vcl("Svc", "a" * 64) == generate_scoring_vcl("Svc", "a" * 64)
    assert not [n for n, b in snippets.items() if "matrix-age-days" in b.lower()]

    # EC-07 injection guards
    bad_secret = 'abc" || req.http.x-evil == "1'
    for fn in (lambda: pass_snippet("Svc", bad_secret), lambda: generate_scoring_vcl("Svc", bad_secret)):
        with pytest.raises(ValueError, match="request_secret"):
            fn()
    with pytest.raises(ValueError, match="exclude_url_regex"):
        recv_snippet("Svc", exclude_url_regex=r'\.(png)"; bad')
    assert r"\.(png|jpg)" in recv_snippet("Svc", exclude_url_regex=r"^/skip/.*\.(png|jpg)$")


# ── 3. ensure_scoring_service: Create, Entitlements, Rollback & Reuse ────────


def _create_side_effect(*, fail_on=None, fail_exc=None):
    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )
    base = {
        ("GET", "/service"): [],
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
        ("GET", "/service/SCORING_SVC_ID/version"): [],
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if fail_on and (method, path) == fail_on:
            raise fail_exc
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return base.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_creates_full_stack_and_emits_status_callbacks():
    fastly_mock = _create_side_effect()
    status_cb = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        result = ensure_scoring_service(LOG_SVC, TOKEN, status_cb=status_cb)

    assert result["scoring_service_id"] == "SCORING_SVC_ID"
    assert result["scoring_service_name"] == f"Session Scoring Service for {LOG_SVC}"
    assert result["scoring_domain"] == f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app"
    assert result["scoring_keys_store_id"] == "KEYS_STORE_ID"
    assert result["scoring_config_store_id"] == "CFG_STORE_ID"
    assert result["scoring_matrix_store_id"] == "MATRIX_STORE_ID"
    assert len(result["aes_key_hex"]) == 64 and int(result["aes_key_hex"], 16) >= 0

    calls = fastly_mock.call_args_list
    assert call("GET", "/service", token=TOKEN) in calls
    assert next(c for c in calls if c.args[:2] == ("POST", "/service")).args[2] == {
        "name": f"Session Scoring Service for {LOG_SVC}",
        "type": "wasm",
    }
    assert (
        next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/domain")
        .args[2]["name"]
        .endswith("session-scorer.edgecompute.app")
    )
    assert (
        next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/backend").args[2]["name"]
        == "placeholder_origin"
    )

    store_creates = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config")]
    assert len(store_creates) == 2
    key_upload = next(c for c in calls if c.args[1].endswith("/KEYS_STORE_ID/item"))
    assert key_upload.args[2]["item_key"] == CURRENT_KEY_HEX and len(key_upload.args[2]["item_value"]) == 64

    cfg_posts = [c.args[2] for c in calls if c.args[:2] == ("POST", "/resources/stores/config/CFG_STORE_ID/item")]
    assert {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT} in cfg_posts
    anchor_val = next(p["item_value"] for p in cfg_posts if p["item_key"] == SCORING_ENABLED_AT_KEY)
    assert anchor_val.isdigit() and int(anchor_val) > 0

    link_names = {c.args[2]["name"] for c in calls if c.args[1].endswith("/version/1/resource")}
    assert link_names == {KEYS_RESOURCE_LINK_NAME, CONFIG_RESOURCE_LINK_NAME, MATRIX_RESOURCE_LINK_NAME}

    assert status_cb.call_count >= 4
    msgs = " ".join(str(c.args[0]) for c in status_cb.call_args_list).lower()
    assert "scoring service" in msgs and "domain" in msgs and "config stores" in msgs


@pytest.mark.parametrize(
    ("kv_enabled", "fail_on", "fail_exc", "expected_code", "expected_link", "expected_deletes"),
    [
        (False, None, None, "kv_store_not_enabled", KV_STORE_MANAGE_URL, set()),
        (
            True,
            ("POST", "/service"),
            RuntimeError("HTTP 403 Forbidden"),
            "compute_not_enabled",
            COMPUTE_MANAGE_URL,
            set(),
        ),
        (
            True,
            ("POST", "/resources/stores/config"),
            RuntimeError("HTTP 400 Bad Request"),
            "config_store_not_enabled",
            CONFIG_STORE_MANAGE_URL,
            {("DELETE", "/service/SCORING_SVC_ID")},
        ),
        (
            True,
            ("POST", "/service/SCORING_SVC_ID/version/1/resource"),
            RuntimeError("HTTP 500 Internal Server Error"),
            None,
            None,
            {
                ("DELETE", "/service/SCORING_SVC_ID"),
                ("DELETE", "/resources/stores/config/KEYS_STORE_ID"),
                ("DELETE", "/resources/stores/config/CFG_STORE_ID"),
                ("DELETE", "/resources/stores/kv/MATRIX_STORE_ID"),
            },
        ),
    ],
)
def test_ensure_entitlement_mapping_and_rollback(
    kv_enabled, fail_on, fail_exc, expected_code, expected_link, expected_deletes
):
    fastly_mock = _create_side_effect(fail_on=fail_on, fail_exc=fail_exc)
    exc_cls = EntitlementError if expected_code else RuntimeError
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=kv_enabled),
        pytest.raises(exc_cls) as ei,
    ):
        ensure_scoring_service(LOG_SVC, TOKEN)

    if expected_code:
        assert ei.value.code == expected_code and ei.value.link == expected_link
    else:
        assert not isinstance(ei.value, EntitlementError) and "HTTP 5" in str(ei.value)

    deletes = {c.args[:2] for c in fastly_mock.call_args_list if c.args[0] == "DELETE"}
    assert expected_deletes <= deletes
    if not expected_deletes:
        assert ("DELETE", "/service/SCORING_SVC_ID") not in deletes


@pytest.mark.parametrize("anchor_missing", [False, True])
def test_ensure_reuses_existing_service_and_heals_missing_anchor(anchor_missing):
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}
    stored_secret = "stored_secret_" + "z" * 50

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": stored_secret}
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            if anchor_missing:
                raise RuntimeError("404 not found")
            return {"item_key": SCORING_ENABLED_AT_KEY, "item_value": "1700000000"}
        if anchor_missing and (method, path) == ("POST", "/resources/stores/config/EXISTING_CFG/item"):
            return {}
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    assert result["scoring_keys_store_id"] == "EXISTING_KEYS"
    assert result["scoring_config_store_id"] == "EXISTING_CFG"
    assert result["scoring_matrix_store_id"] == "EXISTING_MATRIX"
    assert result["request_secret"] == stored_secret and result["aes_key_hex"] == ""
    if anchor_missing:
        anchor_post = next(
            c
            for c in fastly_mock.call_args_list
            if c.args[:2] == ("POST", "/resources/stores/config/EXISTING_CFG/item")
        )
        assert anchor_post.args[2]["item_key"] == SCORING_ENABLED_AT_KEY and anchor_post.args[2]["item_value"].isdigit()


# ── 4. delete_scoring_service: Teardown Ordering & Error Handling ────────────


def test_delete_runs_full_teardown_in_correct_order():
    versions = [{"number": 1, "active": True}, {"number": 2, "active": False}]
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path))
        return versions if (method, path) == ("GET", "/service/SVC_X/version") else {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect)):
        failed = delete_scoring_service(
            "SVC_X",
            scoring_keys_store_id="KEYS_X",
            scoring_config_store_id="CFG_X",
            scoring_matrix_store_id="MATRIX_X",
            token=TOKEN,
        )

    assert failed == []
    deactivate_idx = calls_in_order.index(("PUT", "/service/SVC_X/version/1/deactivate"))
    svc_delete_idx = calls_in_order.index(("DELETE", "/service/SVC_X"))
    keys_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/KEYS_X"))
    cfg_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/CFG_X"))
    kv_key_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X/keys/matrix"))
    kv_store_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X"))
    assert deactivate_idx < svc_delete_idx < keys_delete_idx and svc_delete_idx < cfg_delete_idx
    assert svc_delete_idx < kv_key_idx < kv_store_idx
    assert ("PUT", "/service/SVC_X/version/2/deactivate") not in calls_in_order


def test_delete_edge_cases_and_store_failure_reporting():
    # Empty service_id is a no-op
    noop_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", noop_mock):
        delete_scoring_service("", token=TOKEN)
    noop_mock.assert_not_called()

    # 404 on version list or store delete is tolerated; 500 on store delete is surfaced
    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/MISSING/version"):
            raise RuntimeError("404 not found")
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if path.startswith("/resources/stores/config/MISSING_STORE"):
            raise RuntimeError("404 not found")
        if method == "DELETE" and path.startswith("/resources/stores/config/STUCK"):
            raise RuntimeError("500 internal error")
        return {}

    msgs: list[str] = []
    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect)):
        delete_scoring_service("MISSING", token=TOKEN)
        delete_scoring_service(
            "SVC", scoring_keys_store_id="MISSING_STORE", scoring_config_store_id="FINE", token=TOKEN
        )
        failed = delete_scoring_service(
            "SVC", scoring_keys_store_id="STUCK", scoring_config_store_id="FINE", token=TOKEN, status_cb=msgs.append
        )

    assert ("scoring_keys", "STUCK") in failed and all(sid != "FINE" for _, sid in failed)
    assert any("STUCK" in m and "manual" in m.lower() for m in msgs)


# ── 5. rotate_aes_key: Cold Start, Steady State, Upsert & Validation ─────────


def test_first_and_second_rotation_and_upsert_fallback():
    # 1. Cold start: PATCH previous_key_hex 404s -> POST upsert fallback
    old_current = _hex64(0xAB)
    calls_1: list = []

    def side_effect_1(method, path, body=None, token=None, **kwargs):
        calls_1.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        if method == "PATCH" and path == PREVIOUS_ITEM_PATH:
            raise RuntimeError("HTTP 404 PATCH /.../previous_key_hex\n    not found")
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_1)):
        res1 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert len(res1["current_key_hex"]) == 64 and int(res1["current_key_hex"], 16) >= 0
    assert res1["previous_key_hex"] == old_current and res1["current_key_hex"] != old_current
    assert "T" in res1["rotated_at"]
    prev_values = [
        (c[2] or {}).get("item_value")
        for c in calls_1
        if (c[1] == PREVIOUS_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == PREVIOUS_KEY_HEX)
    ]
    assert old_current in prev_values

    # 2. Steady state: overwrites previous with old_current, drops old_previous
    old_cur2, old_prev2 = _hex64(0xCD), _hex64(0xEF)
    calls_2: list = []

    def side_effect_2(method, path, body=None, token=None, **kwargs):
        calls_2.append((method, path, body))
        return (
            {"item_key": CURRENT_KEY_HEX, "item_value": old_cur2}
            if (method, path) == ("GET", CURRENT_ITEM_PATH)
            else {}
        )

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_2)):
        res2 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert res2["previous_key_hex"] == old_cur2 and res2["current_key_hex"] not in (old_cur2, old_prev2)
    assert [c[2] for c in calls_2 if c[0] == "PATCH" and c[1] == PREVIOUS_ITEM_PATH] == [{"item_value": old_cur2}]
    assert [c[2] for c in calls_2 if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH] == [
        {"item_value": res2["current_key_hex"]}
    ]
    written_values = [(c[2] or {}).get("item_value") for c in calls_2 if c[0] in ("PATCH", "POST") and c[2]]
    assert old_prev2 not in written_values

    # 3. Missing current_key_hex: GET & PATCH 404 -> POST fallback creates current_key_hex
    calls_3: list = []

    def side_effect_3(method, path, body=None, token=None, **kwargs):
        calls_3.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH) or method == "PATCH":
            raise RuntimeError("HTTP 404 not found")
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_3)):
        res3 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert res3["previous_key_hex"] == ""
    assert any(
        c[0] == "POST"
        and c[1] == ITEM_COLLECTION_PATH
        and c[2] == {"item_key": CURRENT_KEY_HEX, "item_value": res3["current_key_hex"]}
        for c in calls_3
    )
    assert len([c for c in calls_3 if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]) == 1


def test_rotate_validation_auth_errors_and_entropy():
    # Empty store id raises ValueError before any API call
    fastly_mock = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        pytest.raises(ValueError, match="scoring_keys_store_id is required"),
    ):
        rotate_aes_key("", token=TOKEN)
    fastly_mock.assert_not_called()

    # 401 on all calls or on writes propagates as RuntimeError
    for get_ok, tok in [(False, ""), (True, "bad-token")]:

        def _auth_fail(method, path, body=None, token=None, _ok=get_ok, **kwargs):
            if _ok and method == "GET":
                return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x11)}
            raise RuntimeError("HTTP 401 unauthorized\n    bad token")

        with (
            patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_auth_fail)),
            pytest.raises(RuntimeError, match="401"),
        ):
            rotate_aes_key(KEYS_STORE_ID, token=tok)

    # Two successive rotations chain properly and produce distinct keys
    state = {"current": _hex64(0x01)}

    def _stateful(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": state["current"]}
        if method == "PATCH" and path == CURRENT_ITEM_PATH and body:
            state["current"] = body["item_value"]
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_stateful)):
        first = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
        second = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
    assert (
        first["current_key_hex"] != second["current_key_hex"] and second["previous_key_hex"] == first["current_key_hex"]
    )

    # Uses secrets.token_hex(32)
    sentinel = "ff" * 32
    with (
        patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_stateful)),
        patch("secrets.token_hex", return_value=sentinel) as token_hex_mock,
    ):
        res = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
    assert res["current_key_hex"] == sentinel and sss.CURRENT_KEY_HEX == "current_key_hex"
    token_hex_mock.assert_called_with(32)
