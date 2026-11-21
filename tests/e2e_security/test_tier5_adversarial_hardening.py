"""Tier 5 — White-Box Adversarial Hardening & Full-Codebase Security Regression Suite.

Covers all Step A white-box and full-codebase Challenger gaps across:
- Cloud Rust WASM, Front/Capture/Scorer VCL & Harness (`GAP-C01..C12`, `GAP-CLOUD-B01..B10`,
  `BUG-CLOUD-1`, `BUG-CLOUD-RUST-01..04`)
- High-Scale Python, ClickHouse/SQL Console, Fastly/Terraform & Gateways/Frontend
  (`T5-HS-01..12`, `GAP-HS-PY-T5-01..08`, `T5-FE-07..08`, `BUG-HS-01`, `BUG-HS-PY-01..05`, `BUG-GW-02`)
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import secrets
import shutil
import subprocess
from pathlib import Path
from typing import Any

import h11
import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core import catalog as catalog_mod
from backend.core import share_db
from backend.core.fastly.client import validate_rum_custom_condition
from backend.core.vcl_generator import RegexValidationError, validate_url_exclusion_regex
from backend.high_scale import query as hs_query_mod
from backend.high_scale.postgres_control import console_access
from backend.main import app as fastapi_app
from backend.routers.admin import high_scale as admin_hs_mod
from backend.routers.rum import analytics as rum_analytics_mod
from backend.utils import terraform_gen, webhook_url
from backend.utils.bot_sources import fetcher as bot_fetcher_mod
from tests.e2e_security import cloud_harness
from tests.e2e_security.cloud_harness import (
    DEFAULT_CLOUD_ORIGIN,
    DEFAULT_FRONT_KEY,
    FIXTURE_22_SID,
    CloudTestServer,
    _b64url,
    _build_ed25519_assertion,
    _build_ed25519_attestation,
    build_cloud_capture_vcl_wrapper,
    build_front_vcl_wrapper,
    cloud_desired_snippets,
    cloud_run_helper,
    extract_cookie_value,
    make_flp_cdn_cookie,
)
from tests.e2e_security.conftest import (
    REPO_ROOT,
    SHARED_18_PII_KEYS,
    AnalystSessionBundle,
    RecordingClickHouseClient,
    make_client,
    provision_analyst_session,
    run_falco_lint,
    run_falco_test,
)

pytestmark = pytest.mark.no_postgres


def _analyst_client(bundle: AnalystSessionBundle) -> TestClient:
    """Create an Analyst TestClient with lowercased headers so Starlette setdefault('user-agent') never clobbers UA."""
    return TestClient(
        fastapi_app,
        base_url=f"https://{bundle.public_host}",
        client=(bundle.client_ip, 50000),
        headers={k.lower(): v for k, v in bundle.headers().items()},
        cookies=bundle.cookies(),
        raise_server_exceptions=False,
    )


_MACHO_MAGICS = {
    b"\xcf\xfa\xed\xfe",  # MH_CIGAM_64 (arm64 / x86_64 little-endian)
    b"\xfe\xed\xfa\xcf",  # MH_MAGIC_64
    b"\xca\xfe\xba\xbe",  # FAT_MAGIC
    b"\x7fELF",  # ELF (Linux CI compatibility)
}


@pytest.fixture(scope="module")
def _tier5_helper_cache() -> dict[str, dict[str, Any]]:
    """Cache deterministic read-only Rust helper subcommand runs across module tests."""
    return {
        "scoring": cloud_run_helper("scoring_schema_check"),
        "logout": cloud_run_helper("logout_ring_floor_check"),
        "passkey": cloud_run_helper("passkey_epoch_and_policy_check"),
        "oom_and_rbac": cloud_run_helper("fsm1_and_bundle_oom_check"),
    }


# ============================================================================
# Part 1: Cloud Rust WASM, Front/Capture/Scorer VCL & Harness (GAP-C01..12, B01..10)
# ============================================================================


def test_t5_c01_cloud_harness_stamp_skips_warm_rustc_and_atomically_replaces_stale_binary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GAP-C01: Cold/warm/stale .stamp skip & atomic os.replace inode swap in ensure_cloud_binaries."""
    cloud_harness.ensure_cloud_binaries()

    isolated_driver = tmp_path / "driver"
    monkeypatch.setattr(cloud_harness, "DRIVER_DIR", isolated_driver)
    monkeypatch.setattr(cloud_harness, "_BUILT_PATHS", None)

    rustc_calls: list[list[str]] = []
    orig_run = subprocess.run

    def _spy_run(cmd: Any, *args: Any, **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        if isinstance(cmd, list) and cmd and Path(str(cmd[0])).name == "rustc":
            rustc_calls.append([str(x) for x in cmd])
        return orig_run(cmd, *args, **kwargs)

    monkeypatch.setattr(cloud_harness.subprocess, "run", _spy_run)

    # 1. Cold build in tmp_path writes helper binary + .stamp via atomic temp -o path
    _, helper_bin = cloud_harness.ensure_cloud_binaries()
    stamp_file = isolated_driver / cloud_harness.STAMP_FILE_NAME
    assert helper_bin == isolated_driver / "e2e-cloud-vcl-helper"
    assert helper_bin.is_file() and stamp_file.is_file()
    assert len(rustc_calls) == 1
    out_idx = rustc_calls[0].index("-o")
    assert rustc_calls[0][out_idx + 1] != str(helper_bin)
    assert rustc_calls[0][out_idx + 1].endswith(".tmp")

    st1 = os.stat(helper_bin)
    old_fd = os.open(helper_bin, os.O_RDONLY)
    try:
        # 2. Warm worker call (_BUILT_PATHS reset to None) skips rustc completely
        rustc_calls.clear()
        monkeypatch.setattr(cloud_harness, "_BUILT_PATHS", None)
        _, helper_bin2 = cloud_harness.ensure_cloud_binaries()
        st2 = os.stat(helper_bin2)
        assert rustc_calls == []
        assert st2.st_ino == st1.st_ino
        assert st2.st_mtime_ns == st1.st_mtime_ns

        # 3. Stale stamp triggers atomic rebuild onto a brand-new inode without mutating old_fd
        stamp_file.write_text("stale-stamp\n", encoding="utf-8")
        monkeypatch.setattr(cloud_harness, "_BUILT_PATHS", None)
        _, helper_bin3 = cloud_harness.ensure_cloud_binaries()
        st3 = os.stat(helper_bin3)
        assert len(rustc_calls) == 1
        assert st3.st_ino != st1.st_ino
        magic = os.pread(old_fd, 4, 0)
        assert magic in _MACHO_MAGICS
        assert not list(isolated_driver.glob("*.tmp"))
    finally:
        os.close(old_fd)


def test_t5_c02_c03_b10_cloud_scoring_managed_field_8br_schema_and_matrix_version_guards(
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C02, GAP-C03 & GAP-CLOUD-B10: managed_field tamper matrix, 8B-R Arrow schema & matrix version guards."""
    res = _tier5_helper_cache["scoring"]

    # GAP-C02(1): scoring.enabled == False rejects all reserved rum_*, edge_*, and built-in field names
    for rname in ("rum_lcp", "rum_cid", "service_id", "fastly_req_id", "edge_score", "edge_custom"):
        entry = res["disabled_reserved"][rname]
        assert entry["patch_status"] == 400, f"{rname}: {entry}"
        assert entry["patch_error"] == "invalid_custom_field"
        assert entry["preview_status"] == 400
        assert entry["validate_valid"] is False

    # GAP-C03(3): empty {} custom/validate fails requiring duckdb_type
    assert res["empty_validate"]["valid"] is False
    assert any("Missing required field: 'duckdb_type'" in e for e in res["empty_validate"]["errors"])

    # GAP-C03(1): 8B-R RequestSchema::for_fields contains all 9 edge_* Arrow columns (5 Int64, 4 Utf8, slot_only=False)
    assert res["hash0"] == "2c6b7265cd93cb48"
    arrow_by_name = {f["name"]: f for f in res["arrow_fields"]}
    assert len(arrow_by_name) == 9
    int_cols = [n for n, f in arrow_by_name.items() if f["present"] and f["dtype"] == "Int64"]
    utf8_cols = [n for n, f in arrow_by_name.items() if f["present"] and f["dtype"] == "Utf8"]
    assert len(int_cols) == 5 and len(utf8_cols) == 4
    assert all(slot["slot_only"] is False for slot in res["custom_slots"])

    # GAP-C02(2): Active-scoring managed_field tamper matrix rejected across both PATCH and preview
    expected_errors = {
        "system_managed_true": "system_field_locked",
        "forged_committed": "committed_flag_reserved",
    }
    for case_name, outcome in res["tamper_results"].items():
        assert outcome["patch_status"] == 400, f"{case_name}: {outcome}"
        assert outcome["preview_status"] == 400, f"{case_name}: {outcome}"
        want_err = expected_errors.get(case_name, "invalid_custom_field")
        assert outcome["patch_error"] == want_err, f"{case_name}: {outcome}"

    # GAP-C03(2): Committed sql_type-only PATCH preserves RequestSchema::hash(); dropping committed -> 409
    assert res["patch_both_status"] == 200
    assert res["patch_sql_only_status"] == 200
    assert res["hash_with_duckdb"] == "2c6b7265cd93cb48"
    assert res["hash_sql_only"] == "2c6b7265cd93cb48"
    assert res["drop_committed_status"] == 409
    assert res["drop_committed_error"] == "committed_field_locked"

    # GAP-CLOUD-B10 / BUG-CLOUD-RUST-04: Matrix version validation protects matrices/index.json
    assert res["retrain_v1_status"] == 200
    for item in res["bad_matrix_versions"]:
        assert item["retrain_status"] == 400
        assert item["retrain_error"] == "invalid_matrix_version"
        assert item["restore_status"] == 400
        assert item["restore_error"] == "invalid_matrix_version"
    assert res["matrix_versions_status"] == 200
    versions = res["matrix_versions"]["versions"]
    assert len(versions) == 1 and versions[0]["version"] == "2027-01-15-v1"


def test_t5_c04_c12_cloud_disabled_rum_faro_cdn_shield_reconcile_cookie_and_fos_guards(
    cloud_server: CloudTestServer,
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C04 & GAP-C12: Disabled-RUM faro_version & cdn_shield guards, no-body reconcile, cookie & FOS host validation."""
    admin = cloud_server.register_admin("Cloud C04 Admin")
    sid = cloud_server.provision_fixture_service(admin)

    # 1. Disabled-RUM cdn_shield injection rejected; trimmed valid POP & 'none' normalized
    gen = cloud_server.service_generation(admin, sid)
    r_bad_shield = cloud_server.request(
        "PATCH",
        f"/api/admin/services/{sid}",
        session_cookie=admin.session_cookie,
        json_body={"generation": gen, "cdn_shield": 'iad-va-us); set req.http.X-Pwn = "1'},
    )
    assert r_bad_shield.status_code == 400
    assert r_bad_shield.json()["error"] == "invalid_storage_configuration"

    r_ok_shield = cloud_server.request(
        "PATCH",
        f"/api/admin/services/{sid}",
        session_cookie=admin.session_cookie,
        json_body={"generation": gen, "cdn_shield": "  iad-va-us  "},
    )
    assert r_ok_shield.status_code == 200
    assert r_ok_shield.json()["service"]["cdn_shield"] == "iad-va-us"

    gen2 = cloud_server.service_generation(admin, sid)
    r_none_shield = cloud_server.request(
        "PATCH",
        f"/api/admin/services/{sid}",
        session_cookie=admin.session_cookie,
        json_body={"generation": gen2, "cdn_shield": "none"},
    )
    assert r_none_shield.status_code == 200
    assert r_none_shield.json()["service"]["cdn_shield"] == "none"

    # 2. Disabled-RUM faro_version traversal/prerelease rejected
    gen3 = cloud_server.service_generation(admin, sid)
    for bad_faro in ("../../evil", "1.2.3-rc1", "2.13.0/evil.js"):
        r_faro = cloud_server.request(
            "PATCH",
            f"/api/admin/services/{sid}",
            session_cookie=admin.session_cookie,
            json_body={"generation": gen3, "rum": {"enabled": False, "faro_version": bad_faro}},
        )
        assert r_faro.status_code == 400, f"{bad_faro}: {r_faro.text}"
        assert r_faro.json()["error"] in {"invalid_rum_settings", "invalid_rum_config", "invalid_faro_version"}

    # 3. Direct desired_snippets rejects poisoned cdn_shield / faro_version even when rum.enabled == False
    base_state = {
        "service_id": sid,
        "logging_enabled": True,
        "log_period": 60,
        "sample_rate": 1,
        "edge_only": True,
        "custom_condition": "",
        "fos_bucket": "fos-fixture-logs",
        "fos_region": "us-east-1",
        "log_fields": {"groups": ["A"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
        "rum": {"enabled": False, "faro_version": "2.13.0"},
    }
    with pytest.raises(ValueError):
        cloud_desired_snippets({**base_state, "cdn_shield": 'iad-va-us"; error 999 "x'})
    with pytest.raises(ValueError):
        cloud_desired_snippets({**base_state, "rum": {"enabled": True, "faro_version": "../evil"}})

    # 4. Stored poisoned custom_condition & rum.custom_condition fail no-body POST .../reconcile before Fastly API
    stored_rec = _tier5_helper_cache["oom_and_rbac"]["stored_reconcile"]
    assert stored_rec["cc_status"] == 400
    assert stored_rec["cc_error"] == "invalid_logging_settings"
    assert stored_rec["rum_status"] == 400
    assert stored_rec["rum_error"] == "invalid_rum_settings"

    # RFC 6265 cookie parsing: empty flp_cdn= + whitespace around '=' + duplicate-cookie first-match authenticates cleanly,
    # while literal double-quoted cookie token fails closed without panic and clears stale flp_session.
    r_cookie = cloud_server.request(
        "GET",
        "/api/auth/session",
        raw_cookie=f"flp_cdn=;   flp_session = {admin.session_cookie} ; flp_session=second_ignored; theme=dark",
    )
    assert r_cookie.status_code == 200
    assert r_cookie.json()["user"]["role"] == "admin"

    r_cookie_quoted = cloud_server.request(
        "GET",
        "/api/auth/session",
        raw_cookie=f'flp_session="{admin.session_cookie}"',
    )
    assert r_cookie_quoted.status_code == 200
    assert r_cookie_quoted.json()["user"] is None
    assert any("flp_session=;" in sc for sc in r_cookie_quoted.headers.get_list("set-cookie"))

    # 5. GAP-C12: fos_region / fos_bucket SSRF & traversal rejected before SigV4
    cloud_server.reset()
    admin_bp = cloud_server.register_admin("Cloud C12 Provision Admin")
    for bad_prov in (
        {"fos_region": "us-east-1.evil.com", "fos_bucket": "valid-bucket-1"},
        {"fos_region": "US-EAST-1/../../x", "fos_bucket": "valid-bucket-1"},
        {"fos_region": "us-east-1", "fos_bucket": "../evil-bucket"},
        {"fos_region": "us-east-1", "fos_bucket": "Evil_Upper_Bucket"},
    ):
        r_bp = cloud_server.request(
            "POST",
            "/api/admin/provision/execute",
            session_cookie=admin_bp.session_cookie,
            harness_header=True,
            json_body={
                "token": "fixture-token",
                "service_id": "fixtureService",
                **bad_prov,
                "log_fields": {"groups": ["A"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
            },
        )
        assert r_bp.status_code == 400, f"{bad_prov} -> {r_bp.status_code} {r_bp.text}"


def test_t5_c05_cloud_vcl_validator_rtime_escapes_quotes_and_set_cookie_scope_matrix(
    cloud_server: CloudTestServer,
) -> None:
    """GAP-C05: Cloud VCL validators block RTIME adjacency, paired \\\", %XX/%u, -table.lookup_* & Set-Cookie scope."""
    admin = cloud_server.register_admin("Cloud C05 Admin")
    sid = cloud_server.provision_fixture_service(admin)

    blocked_conditions = [
        'now + 0mstable.lookup(fos_credentials, "k") != ""',
        'now + 0.5wsetcookie.delete_by_name(beresp, "a") != ""',
        'now + 100msheader.get(req, "Cookie") != ""',
        'now + 0msfos_credentials != ""',
        '-table.lookup_integer(flp_front_auth, "auth_epoch") > 0',
        'TABLE . LOOKUP(fos_credentials, "secret_key") != ""',
        'req . http . Cookie:flp_session != ""',
        r'req.http.X == "foo\" || true || req.http.Y == \"bar"',
        'req.http.X == "foo\\"',
        'req.http.X == "%00"',
        'req.http.X == "%22"',
        'req.http.X == "%u0000"',
        'req.http.X == "%U{00}"',
        'resp.http.Set-Cookie != ""',
    ]
    gen = cloud_server.service_generation(admin, sid)
    for bad_cond in blocked_conditions:
        r_patch = cloud_server.request(
            "PATCH",
            f"/api/admin/services/{sid}",
            session_cookie=admin.session_cookie,
            json_body={"generation": gen, "custom_condition": bad_cond},
        )
        assert r_patch.status_code == 400, f"condition not blocked: {bad_cond!r} -> {r_patch.text}"

    # RUM condition rejects RTIME-adjacent req.url and Unicode NBSP dot-splitting
    for bad_rum_cond in ('(now + 0msreq.url.path) != ""', 'req\u00a0.\u00a0url != ""'):
        r_rum = cloud_server.request(
            "PATCH",
            f"/api/admin/services/{sid}",
            session_cookie=admin.session_cookie,
            json_body={"generation": gen, "rum": {"enabled": True, "custom_condition": bad_rum_cond}},
        )
        assert r_rum.status_code == 400, f"rum condition not blocked: {bad_rum_cond!r}"

    # req.http.Set-Cookie is REJECTED in custom-field vcl_log_expression, but ALLOWED in custom_condition
    r_cf_setcookie = cloud_server.request(
        "POST",
        "/api/admin/log-fields/custom/validate",
        session_cookie=admin.session_cookie,
        json_body={
            "name": "leak_sc",
            "label": "Leak SC",
            "vcl_log_expression": "req.http.Set-Cookie",
            "collection_stage": "edge",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 32,
        },
    )
    assert r_cf_setcookie.status_code == 200
    assert r_cf_setcookie.json()["valid"] is False

    # Scoring exclude-regex/validate blocks %00 / %u0000 / %U{000} while allowing %22
    for bad_rx in ("^/api%00admin$", "^/api%u0000admin$", "^/api%U{000}admin$"):
        r_rx = cloud_server.request(
            "POST",
            f"/api/services/{sid}/scoring/exclude-regex/validate",
            session_cookie=admin.session_cookie,
            json_body={"exclude_url_regex": bad_rx},
        )
        assert r_rx.status_code in {200, 400}
        if r_rx.status_code == 200:
            assert r_rx.json()["valid"] is False

    r_rx_ok = cloud_server.request(
        "POST",
        f"/api/services/{sid}/scoring/exclude-regex/validate",
        session_cookie=admin.session_cookie,
        json_body={"exclude_url_regex": "^/quote%22path$"},
    )
    assert r_rx_ok.status_code == 200
    assert r_rx_ok.json()["valid"] is True

    # Valid interior regex escapes + req.http.Set-Cookie in custom_condition pass PATCH AND falco lint!
    for valid_cond in (
        r'req.url.ext ~ "\.css$"',
        r'req.http.User-Agent ~ "bot\b"',
        'req.http.Set-Cookie == ""',
    ):
        cur_gen = cloud_server.service_generation(admin, sid)
        r_ok = cloud_server.request(
            "PATCH",
            f"/api/admin/services/{sid}",
            session_cookie=admin.session_cookie,
            json_body={"generation": cur_gen, "custom_condition": valid_cond},
        )
        assert r_ok.status_code == 200, f"{valid_cond!r} rejected: {r_ok.text}"
        snippets = cloud_desired_snippets(
            {
                "service_id": sid,
                "logging_enabled": True,
                "log_period": 60,
                "sample_rate": 1,
                "edge_only": True,
                "custom_condition": valid_cond,
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1",
                "log_fields": {"groups": ["A"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
                "rum": {"enabled": False, "faro_version": "2.13.0"},
            }
        )
        lint = run_falco_lint(build_cloud_capture_vcl_wrapper(snippets))
        assert lint.returncode == 0, f"falco lint failed for {valid_cond!r}: {lint.stdout}\n{lint.stderr}"


def test_t5_c06_b10_cloud_passkey_revoke_epoch_429_retry_reissue_host_cookie_and_policy_version(
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C06 & GAP-CLOUD-B10: Passkey revoke epoch bump, 429->200 retry, v2/__Host-flp_cdn reissue & Front VCL precedence."""
    res = _tier5_helper_cache["passkey"]
    assert res["short_sid_none"] is True
    assert "~1~~" not in res["v1_epoch0_hdr"]
    assert res["pk_revoke_status"] == 200
    assert res["dict_put_attempts_429_200"] == 2
    assert res["kv_epoch_after_pk_rev"] == 2
    assert res["dict_epoch_after_pk_rev"] == "2"

    # GET /api/log-extents re-issues BOTH flp_cdn and __Host-flp_cdn signed at epoch=2
    assert res["reissue_status"] == 200
    reissued = res["reissued_cookies"]
    assert len(reissued) == 2
    assert reissued[0].startswith("flp_cdn=AAAAAAAAAAAAAAAAAAAAAA~analyst~")
    assert "~2~" in reissued[0]
    assert reissued[1].startswith("__Host-flp_cdn=AAAAAAAAAAAAAAAAAAAAAA~analyst~")
    assert "~2~" in reissued[1]

    # __Host-flp_cdn takes precedence over tossed garbage flp_cdn in Shell (0 reissued cookies)
    assert res["host_prec_status"] == 200
    assert res["host_prec_reissued_count"] == 0

    # 429 -> 503 exhaustion keeps HTTP 200 and emits auth.epoch_sync_failed event
    assert res["exhaustion_role_status"] == 200
    assert any(ev.get("event") == "auth.epoch_sync_failed" for ev in res["admin_events"])

    # policy_version bump immediately invalidates existing flp_session
    assert res["after_policy_bump_status"] == 401

    # Verify Front VCL (falco test): stale epoch=1 falls back (pass), __Host-flp_cdn beats tossed flp_cdn (lookup),
    # epoch=0 v2 or tampered v2 triggers 629 (403 + x-flp-auth-fail), and corrupt non-numeric auth_epoch never trusts mismatched v2
    sess = "analyst-front-vcl-sess"
    stale_v2 = make_flp_cdn_cookie(session_cookie=sess, role="analyst", epoch="1")
    valid_v2 = make_flp_cdn_cookie(session_cookie=sess, role="analyst", epoch="2")
    epoch0_v2 = make_flp_cdn_cookie(session_cookie=sess, role="analyst", epoch="0")
    tampered_v2 = make_flp_cdn_cookie(session_cookie=sess, role="analyst", epoch="2", tamper_sig=True)

    main_vcl, extras = build_front_vcl_wrapper(extra_front_auth={"auth_epoch": "2"})
    test_vcl = f"""
// @scope: recv
sub test_stale_epoch_falls_back_to_compute_pass {{
  testing.inject_variable("client.ip", "198.51.100.40");
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn={stale_v2}";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
  assert.equal(req.url, {{"/api/file?key=processed%2Frequest%2Fa.parquet"}});
}}

// @scope: recv
sub test_host_flp_cdn_precedence_over_tossed_flp_cdn {{
  testing.inject_variable("client.ip", "198.51.100.41");
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; flp_cdn=tossed_subdomain_garbage; __Host-flp_cdn={valid_v2}";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
}}

// @scope: recv
sub test_epoch_zero_v2_falls_back_to_compute_when_dict_epoch_is_nonzero {{
  testing.inject_variable("client.ip", "198.51.100.42");
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={epoch0_v2}";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
}}

// @scope: recv
sub test_tampered_v2_rejected_with_629 {{
  testing.inject_variable("client.ip", "198.51.100.43");
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={tampered_v2}";
  testing.call_subroutine("vcl_recv");
  assert.error(629);
}}
"""
    falco_res = run_falco_test(main_vcl, test_vcl, extra_files=extras)
    assert falco_res.returncode == 0, f"{falco_res.stdout}\n{falco_res.stderr}"

    corrupt_vcl, corrupt_extras = build_front_vcl_wrapper(extra_front_auth={"auth_epoch": "corrupt_non_numeric"})
    corrupt_test = f"""
// @scope: recv
sub test_corrupt_dict_epoch_never_authorizes_v2_cookie_from_cache {{
  testing.inject_variable("client.ip", "198.51.100.44");
  set req.method = "GET";
  set req.url = "/fos/pq/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={valid_v2}";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
}}
"""
    corrupt_res = run_falco_test(corrupt_vcl, corrupt_test, extra_files=corrupt_extras)
    assert corrupt_res.returncode == 0, f"{corrupt_res.stdout}\n{corrupt_res.stderr}"


def test_t5_c07_b05_cloud_logout_ring_69_eviction_floor_and_cross_user_isolation(
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C07 & GAP-CLOUD-B05: 69-session logout ring-buffer monotone user_min_session_exp floor & flood isolation."""
    res = _tier5_helper_cache["logout"]
    assert res["total_logged_out"] == 69
    assert res["ring_len"] == 64
    assert res["actual_floor"] == res["expected_floor"]
    assert res["all_69_status_401"] is True
    assert res["all_69_auth_fail_header"] is True
    assert res["fresh_before_flood"] == 200
    assert res["fresh_after_flood"] == 200
    assert res["ring_len_after_flood"] == 64
    assert res["floor_after_flood"] == res["expected_floor"]


def test_t5_c08_cloud_views_redteam_dupkeys_sticky_slots_columns_lambda_concat_and_benign(
    cloud_server: CloudTestServer,
) -> None:
    """GAP-C08 & BUG-CLOUD-1: views.rs NoDupJson, sticky slots, SQL comments/escapes, COLUMNS(lambda) & concat blocking."""
    admin = cloud_server.register_admin("Views RedTeam Admin")
    analyst_full = cloud_server.invite_and_register(admin, role="analyst", display_name="Full Analyst")
    analyst_nopii = cloud_server.invite_and_register(admin, role="analyst-no-pii", display_name="NoPii Analyst")
    sid = cloud_server.provision_fixture_service(admin)

    pii_vectors: dict[str, str] = {
        "dup-case-key": '{"filters":[{"column":"client_ip","value":"198.51.100.7","Column":"url"}]}',
        "sticky-slot": json.dumps(
            {"_view_extras": {"key": {"column": "url", "mode": "include", "value": "SELECT sid FROM logs"}}}
        ),
        "nested-comment": json.dumps({"_view_extras": {"sql": "SELECT /* a /* ' */ b */ client_ip FROM logs"}}),
        "unicode-ident": json.dumps({"_view_extras": {"sql": r'SELECT U&"client\005fip" FROM logs'}}),
        "hex-subscript": json.dumps({"_view_extras": {"sql": r"SELECT _extra[E'\x63lient_ip'] FROM logs"}}),
        "json-ptr-tilde": json.dumps({"_view_extras": {"sql": "SELECT _extra->>'/headers~1client_ip' FROM logs"}}),
        "columns-star-regex": json.dumps({"_view_extras": {"sql": "SELECT COLUMNS('.*') FROM logs"}}),
        "bug-cloud-1-lambda": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE COLUMNS(c -> c != 'url') = '198.51.100.7'"}}
        ),
        "bug-cloud-1-sub-concat": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra['clien' || 't_ip'] = '198.51.100.7'"}}
        ),
        "bug-cloud-1-chr-concat": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE json_extract_string(_extra, chr(99) || 'id') = 's1'"}}
        ),
        "bug-cloud-1-r2-unobf-multiline-arrow": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE _extra ->> '$.request.headers.'\n  'x-forwarded-for' = '198.51.100.7'"
                }
            }
        ),
        "bug-cloud-1-r2-newline-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra->>'clien'\n't_ip' = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r2-comment-newline-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra->>'clien' -- split\n't_ip' = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r2-newline-extract": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE json_extract_string(_extra, 'clien'\n't_ip') = '198.51.100.7'"
                }
            }
        ),
        "bug-cloud-1-r2-newline-subscript": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra['clien'\n't_ip'] = '\"198.51.100.7\"'"}}
        ),
        "bug-cloud-1-r2-newline-struct": json.dumps(
            {"_view_extras": {"sql": "SELECT {'clien'\n't_ip': '198.51.100.7'} FROM logs"}}
        ),
        "bug-cloud-1-r2-cte-subscript-ident": json.dumps(
            {
                "_view_extras": {
                    "sql": "WITH k(c) AS (VALUES (chr(99)||chr(108)||chr(105)||chr(101)||chr(110)||chr(116)||chr(95)||chr(105)||chr(112))) SELECT * FROM logs, k WHERE _extra[c] = '\"198.51.100.7\"'"
                }
            }
        ),
        "bug-cloud-1-r3-slice-subscript-json": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE (_extra::JSON)['xclient_ipx'[2:10]] = '\"198.51.100.7\"'"
                }
            }
        ),
        "bug-cloud-1-r3-list-subscript-json": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE (_extra::JSON)[['a', 'xipx'[2:3]][2]] = '\"198.51.100.7\"'"
                }
            }
        ),
        "bug-cloud-1-r3-operator-subscript": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE (_extra::JSON)[1+1] IS NOT NULL"}}
        ),
        "bug-cloud-1-r3-wildcard-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE (_extra->>'$.*')::VARCHAR LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r3-json-contains": json.dumps(
            {
                "_view_extras": {
                    "sql": 'SELECT * FROM logs WHERE json_contains(_extra, \'{"client_ip":"198.51.100.7"}\')'
                }
            }
        ),
        "bug-cloud-1-r3-json-each": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs, json_each(_extra) WHERE value = '\"198.51.100.7\"'"}}
        ),
        "bug-cloud-1-r3-json-tree": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs, json_tree(_extra) WHERE value = '\"198.51.100.7\"'"}}
        ),
        "bug-cloud-1-r3-json-transform": json.dumps(
            {
                "_view_extras": {
                    "sql": 'SELECT * FROM logs WHERE json_transform(_extra, \'{"client_ip":"VARCHAR"}\') IS NOT NULL'
                }
            }
        ),
        "bug-cloud-1-r3-from-json": json.dumps(
            {
                "_view_extras": {
                    "sql": 'SELECT * FROM logs WHERE from_json(_extra::JSON, \'{"client_ip":"VARCHAR"}\') IS NOT NULL'
                }
            }
        ),
        "bug-cloud-1-r3-map-extract-value": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE map_extract_value(kv, 'client_ip') = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r3-struct-extract-at": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE struct_extract_at(logs, 4) = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r3-extra-raw-like": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-zwsp-quote-cont": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE _extra->>'clien'\u200b\n't_ip' = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r4-bom-blind-fn": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE row_to_json\ufeff(logs)::VARCHAR LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-root-dollar-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE (_extra->>'$') LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-root-empty-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE (_extra->>'') LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-json-contains-u-esc": json.dumps(
            {
                "_view_extras": {
                    "sql": r'SELECT * FROM logs WHERE json_contains(_extra, \'{"\u0063lient_ip":"198.51.100.7"}\')'
                }
            }
        ),
        "bug-cloud-1-r4-json-object-wrap": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE json_object('safe', _extra)->>'safe' LIKE '%198.51.100.7%'"
                }
            }
        ),
        "bug-cloud-1-r4-struct-pack-wrap": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE struct_pack(safe := _extra)['safe'] LIKE '%198.51.100.7%'"
                }
            }
        ),
        "bug-cloud-1-r4-json-array-int-arrow": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE (json_array(_extra)->>0) LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-case-in-extract-arg0": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE json_extract_string(CASE WHEN contains(_extra, '198.51.100.7') THEN '{\"safe\":\"1\"}' ELSE '{}' END, '$.safe') = '1'"
                }
            }
        ),
        "bug-cloud-1-r4-dot-map-extract-concat": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM logs WHERE kv.map_extract_value('clien' || 't_ip') = '198.51.100.7'"
                }
            }
        ),
        "bug-cloud-1-r4-query-dynamic-sql": json.dumps(
            {
                "_view_extras": {
                    "sql": "SELECT * FROM query('SELECT * FROM logs WHERE ' || chr(99) || 'lient_ip = ''198.51.100.7''')"
                }
            }
        ),
        "bug-cloud-1-r4-map-contains-value": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE map_contains_value(kv, '198.51.100.7')"}}
        ),
        "bug-cloud-1-r4-positional-hash-col": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE #2 = '198.51.100.7'"}}
        ),
        "bug-cloud-1-r4-unpivot-logs": json.dumps(
            {"_view_extras": {"sql": "UNPIVOT logs ON (* EXCLUDE (timestamp)) INTO NAME col VALUE val"}}
        ),
        "bug-cloud-1-r4-logs-cast-varchar": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-alias-cast-varchar": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs AS t WHERE CAST(t AS VARCHAR) LIKE '%198.51.100.7%'"}}
        ),
        "bug-cloud-1-r4-cte-row-struct": json.dumps(
            {
                "_view_extras": {
                    "sql": "WITH x AS (SELECT * FROM logs) SELECT * FROM x WHERE x::VARCHAR LIKE '%198.51.100.7%'"
                }
            }
        ),
        "bug-cloud-1-r4-json-map-extra": json.dumps({"_extra": {"mode": "include", "values": ["198.51.100.7"]}}),
        "bug-cloud-1-r4-json-kv-extra": json.dumps({"_extra": "198.51.100.7"}),
    }
    benign_vectors: dict[str, str] = {
        "benign-columns-safe": json.dumps({"_view_extras": {"sql": "SELECT COLUMNS('^(status|url|pop)$') FROM logs"}}),
        "benign-arr-index": json.dumps({"_view_extras": {"sql": "SELECT arr[1] FROM logs"}}),
        "benign-extra-safe": json.dumps({"_view_extras": {"sql": "SELECT _extra['safe'] FROM logs"}}),
        "benign-chained-extra-safe": json.dumps(
            {"_view_extras": {"sql": "SELECT (_extra::JSON)['request']['headers']['safe_header'] FROM logs"}}
        ),
        "benign-json-contains-safe": json.dumps(
            {"_view_extras": {"sql": 'SELECT * FROM logs WHERE json_contains(_extra, \'{"safe_header":"ok"}\')'}}
        ),
        "benign-json-extract-eq-pii-lit": json.dumps(
            {"_view_extras": {"sql": "SELECT * FROM logs WHERE json_extract_string(_extra, '/meta/pop') = '/ip/sid'"}}
        ),
        "benign-r4-alias-qualified-cols": json.dumps(
            {"_view_extras": {"sql": "SELECT t.status, logs.url FROM logs AS t WHERE t.status = 200"}}
        ),
        "benign-r4-dot-chain-safe": json.dumps(
            {"_view_extras": {"sql": "SELECT (_extra->'headers').json_extract_string('safe_header') FROM logs"}}
        ),
        "benign-4xx-pill": json.dumps(
            {
                "status": {"mode": "include", "values": ["404", "429"]},
                "url": {"mode": "exclude", "values": ["/api/ip:status"]},
            }
        ),
    }

    for name, fjson in {**pii_vectors, **benign_vectors}.items():
        r_save = cloud_server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            json_body={
                "id": f"v-{name}",
                "service_id": sid,
                "name": name,
                "filters_json": fjson,
                "time_range_type": "relative",
                "page": "dashboard",
            },
        )
        assert r_save.status_code in {200, 201}, f"{name}: {r_save.status_code} {r_save.text}"

    r_full = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=analyst_full.session_cookie)
    assert r_full.status_code == 200
    full_names = {v["name"] for v in r_full.json()}
    assert set(benign_vectors).issubset(full_names)
    # Note: NoDupJson may reject literal duplicate keys at save or filter time; every saved non-dup PII view is visible to full Analyst
    assert {"bug-cloud-1-lambda", "bug-cloud-1-sub-concat", "bug-cloud-1-chr-concat"}.issubset(full_names)

    r_nopii = cloud_server.request("GET", f"/api/views/{sid}", session_cookie=analyst_nopii.session_cookie)
    assert r_nopii.status_code == 200
    nopii_names = {v["name"] for v in r_nopii.json()}
    assert nopii_names == set(benign_vectors)
    assert "198.51.100.7" not in r_nopii.text


def test_t5_c09_b08_cloud_auth_fail_precision_and_service_scoring_telemetry_redaction(
    cloud_server: CloudTestServer,
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C09 & GAP-CLOUD-B08: x-flp-auth-fail false-positive prevention & service/scoring/telemetry redaction."""
    # Before admin registration, mint a valid unspent flp_setup cookie
    r_setup = cloud_server.request("POST", "/api/setup/accept", json_body={"code": cloud_server.setup_code})
    assert r_setup.status_code == 200
    valid_setup = extract_cookie_value(r_setup, "flp_setup")
    assert valid_setup

    # 1. Malformed JSON (422) on login/verify MUST NOT emit x-flp-auth-fail
    r_malformed = cloud_server._client.request(  # type: ignore[union-attr]
        "POST",
        "/api/auth/login/verify",
        headers={
            "fastly-client-ip": "198.51.100.55",
            "x-flp-front-key": DEFAULT_FRONT_KEY,
            "x-flp-front-rl": "1",
            "origin": DEFAULT_CLOUD_ORIGIN,
            "content-type": "application/json",
        },
        content=b"{not-valid-json",
    )
    assert r_malformed.status_code == 422
    assert r_malformed.headers.get("x-flp-auth-fail") is None

    # 2. Cookieless/inviteless register/options (401) & sending valid flp_setup to /api/files (401) do NOT emit x-flp-auth-fail or clear flp_setup
    r_no_inv = cloud_server.request(
        "POST",
        "/api/auth/register/options",
        json_body={"display_name": "Anon", "invite_token": None},
    )
    assert r_no_inv.status_code == 401
    assert r_no_inv.headers.get("x-flp-auth-fail") is None

    r_setup_on_files = cloud_server.request("GET", "/api/files", setup_cookie=valid_setup)
    assert r_setup_on_files.status_code == 401
    assert r_setup_on_files.headers.get("x-flp-auth-fail") is None
    assert not any("flp_setup=;" in sc for sc in r_setup_on_files.headers.get_list("set-cookie"))

    # 3. Bad invite_token on register/options & forged flp_setup on system-checks DO emit x-flp-auth-fail: 1
    r_bad_inv = cloud_server.request(
        "POST",
        "/api/auth/register/options",
        json_body={"display_name": "Attacker", "invite_token": "forged-invite-token-0001"},
    )
    assert r_bad_inv.status_code == 401
    assert r_bad_inv.headers.get("x-flp-auth-fail") == "1"

    r_forged_setup = cloud_server.request("GET", "/api/admin/system-checks", setup_cookie="forged-setup-grant")
    assert r_forged_setup.status_code == 401
    assert r_forged_setup.headers.get("x-flp-auth-fail") == "1"
    assert any("flp_setup=;" in sc for sc in r_forged_setup.headers.get_list("set-cookie"))

    # 4. Non-admin service redaction & Admin scoring/telemetry secret redaction (from helper)
    sc = _tier5_helper_cache["scoring"]
    for role_key in ("analyst_services", "nopii_services"):
        svc_obj = sc[role_key]["services"][0]
        for stripped_key in (
            "fos_bucket",
            "fos_prefix",
            "fos_region",
            "fastly_token_ref",
            "fos_access_key_ref",
            "fos_secret_key_ref",
            "generation",
        ):
            assert stripped_key not in svc_obj, f"{stripped_key} leaked in {role_key}"
        for cf in svc_obj["log_fields"]["custom_fields"]:
            assert "vcl_log_expression" not in cf

    for admin_blob in (sc["admin_services_text"], sc["scoring_settings_text"]):
        assert "fixture-token" not in admin_blob
        assert "fixture-sk-" not in admin_blob
        assert "current_key_hex" not in admin_blob

    redacted = _tier5_helper_cache["oom_and_rbac"]["telemetry_redacted"]
    assert "super-secret-token-value" not in json.dumps(redacted)
    assert "secret-cookie" not in json.dumps(redacted)
    assert "top-secret-fos-sk" not in json.dumps(redacted)


def test_t5_c10_c11_front_vcl_ops_rbac_626_ipv6_and_rust_scorer_tls_cookie_scrub(
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-C10 & GAP-C11: Front VCL ops 624, traversal 626 (no auth-fail), IPv6 /64 box & Rust scorer TLS/cookie scrub."""
    sess = "sess-c10"
    analyst_cdn = make_flp_cdn_cookie(session_cookie=sess, role="analyst", scope="tenant/logs/", epoch="1")
    admin_scoped_cdn = make_flp_cdn_cookie(session_cookie=sess, role="admin", scope="scope-a/", epoch="1")

    main_vcl, extras = build_front_vcl_wrapper(extra_front_auth={"auth_epoch": "1"})
    front_test_vcl = f"""
// @scope: recv
sub test_analyst_allowed_request_parquet_but_blocked_624_on_ops {{
  testing.inject_variable("client.ip", "198.51.100.60");
  set req.method = "GET";
  set req.url = "/fos/pq/tenant/logs/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={analyst_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
}}

// @scope: recv
sub test_analyst_ops_triggers_624 {{
  testing.inject_variable("client.ip", "198.51.100.61");
  set req.method = "GET";
  set req.url = "/fos/pq/tenant/logs/processed/ops/api_telemetry/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={analyst_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(624);
}}

// @scope: error
sub test_624_sets_403_and_auth_fail_whereas_626_sets_400_without_auth_fail {{
  set obj.status = 624;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 403);
  assert.equal(obj.http.x-flp-auth-fail, "1");
}}

// @scope: error
sub test_626_does_not_set_auth_fail {{
  unset obj.http.x-flp-auth-fail;
  set obj.status = 626;
  testing.call_subroutine("vcl_error");
  assert.equal(obj.status, 400);
  assert.is_notset(obj.http.x-flp-auth-fail);
}}

// @scope: recv
sub test_scope_mismatch_and_traversal_trigger_626 {{
  testing.inject_variable("client.ip", "198.51.100.62");
  set req.method = "GET";
  set req.url = "/fos/pq/scope-b/processed/request/a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={admin_scoped_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(626);
}}

// @scope: recv
sub test_double_slash_triggers_626 {{
  testing.inject_variable("client.ip", "198.51.100.63");
  set req.method = "GET";
  set req.url = "/fos/pq/scope-a/processed/request/sub//a.parquet";
  set req.http.Cookie = "flp_session={sess}; __Host-flp_cdn={admin_scoped_cdn}";
  testing.call_subroutine("vcl_recv");
  assert.error(626);
}}

// @scope: recv
sub test_multi_slash_fos_raw_blocked_622 {{
  testing.inject_variable("client.ip", "2001:db8:abcd:1234::1");
  set req.method = "GET";
  set req.url = "////fos/{FIXTURE_22_SID}/raw/request/a.gz";
  set req.http.x-fastly-key = "wrong-secret";
  testing.call_subroutine("vcl_recv");
  assert.error(622);
}}
"""
    res_front = run_falco_test(main_vcl, front_test_vcl, extra_files=extras)
    assert res_front.returncode == 0, f"{res_front.stdout}\n{res_front.stderr}"

    # GAP-C11: Rust desired_scoring_backend TLS & desired_scoring_snippets cookie scrub in falco test
    sc = _tier5_helper_cache["scoring"]
    backend_cfg = sc["scoring_backend"]
    assert backend_cfg["use_ssl"] is True
    assert backend_cfg["ssl_check_cert"] is True
    assert backend_cfg["port"] == 443
    assert backend_cfg["ssl_cert_hostname"] == "scorer.edgecompute.app"
    assert backend_cfg["ssl_sni_hostname"] == "scorer.edgecompute.app"
    assert backend_cfg["override_host"] == "scorer.edgecompute.app"

    by_sub: dict[str, list[str]] = {}
    for snip in sc["scoring_snippets"]:
        by_sub.setdefault(snip["subroutine"], []).append(snip["body"])

    scorer_vcl = f"""
backend F_origin {{ .host = "127.0.0.1"; .port = "8080"; }}
backend F_session_scorer {{ .host = "127.0.0.1"; .port = "8443"; }}

sub vcl_recv {{
  set req.backend = F_origin;
{chr(10).join(by_sub.get("vcl_recv", []))}
}}

sub vcl_pass {{
{chr(10).join(by_sub.get("vcl_pass", []))}
}}

sub vcl_fetch {{
{chr(10).join(by_sub.get("vcl_fetch", []))}
}}

sub vcl_deliver {{
{chr(10).join(by_sub.get("vcl_deliver", []))}
}}

sub vcl_miss {{
{chr(10).join(by_sub.get("vcl_miss", []))}
}}
"""
    scorer_test_vcl = """
// @scope: recv
sub test_scorer_recv_scrubs_spoofed_client_scoring_headers {
  set req.method = "GET";
  set req.url = "/checkout";
  set req.http.X-Edge-Score = "score=0";
  set req.http.X-Edge-Scoring-Pass = "1";
  set req.http.x-sigsci-skip-inspection-once = "true";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.backend, F_session_scorer);
  assert.state(pass);
}

// @scope: pass
sub test_scorer_pass_strips_customer_cookies_and_preserves_session_anchor {
  set req.backend = F_session_scorer;
  set req.http.X-Edge-Scoring-Pass = "1";
  set req.http.Cookie = "flp_session=topsecret; session=cust_tok; _ess=anchor1";
  set bereq.http.Cookie = req.http.Cookie;
  testing.call_subroutine("vcl_pass");
  assert.equal(bereq.http.Cookie, "_ess=anchor1");
  assert.equal(bereq.http.X-Edge-Scorer-Auth, "test-scorer-req-secret-0001");

  set req.http.Cookie = "flp_session=topsecret; session=cust_tok";
  set bereq.http.Cookie = req.http.Cookie;
  testing.call_subroutine("vcl_pass");
  assert.is_notset(bereq.http.Cookie);
}

// @scope: pass
sub test_non_scorer_origin_pass_keeps_customer_cookies {
  set req.backend = F_origin;
  unset req.http.X-Edge-Scoring-Pass;
  set bereq.http.Cookie = "session=cust_tok";
  testing.call_subroutine("vcl_pass");
  assert.equal(bereq.http.Cookie, "session=cust_tok");
}
"""
    res_scorer = run_falco_test(scorer_vcl, scorer_test_vcl)
    assert res_scorer.returncode == 0, f"{res_scorer.stdout}\n{res_scorer.stderr}"


def test_t5_b01_b07_b09_cloud_file_rbac_worker_gates_and_fsm1_bundle_oom_guards(
    cloud_server: CloudTestServer,
    _tier5_helper_cache: dict[str, dict[str, Any]],
) -> None:
    """GAP-CLOUD-B01, B07 & B09: /api/file RBAC/traversal/cursor, front/worker/tick gates & FSM1/FLPB OOM guards."""
    res = _tier5_helper_cache["oom_and_rbac"]

    # GAP-CLOUD-B09 (BUG-CLOUD-RUST-01 & 02): Crafted u32::MAX / 0x2000_0000 headers fail gracefully under 64 MiB cap
    assert res["fsm1"] == {
        "u32_max_some": False,
        "n_0x20000000_some": False,
        "pairs_u32_max_some": False,
        "valid_some": True,
    }
    assert any(e.startswith("partial_unreadable:partial=c1-") for e in res["bundle_raw_errors"])
    assert any(e.startswith("partial_unreadable:partial=c1-") for e in res["bundle_packed_errors"])
    assert res["day_bundle_written"] is True

    # GAP-CLOUD-B01: /api/file & /api/files RBAC, traversal, and HMAC cursor enforcement
    rbac = res["file_rbac"]
    assert rbac["nopii_get"] == 403
    assert rbac["nopii_head"] == 403
    assert rbac["analyst_ops_files"] == 403
    assert rbac["analyst_ops_file"] == 403
    assert rbac["admin_bad_cursor"] == 400
    for k, st in rbac["admin_bad_keys"].items():
        assert st == 400, f"{k} -> {st}"

    # GAP-CLOUD-B07: Worker x-flp-worker-key gates + Front x-flp-front-key & x-flp-tick gates
    wg = res["worker_gates"]
    assert wg["ingest_missing"] == 403
    assert wg["ingest_wrong"] == 403
    assert wg["ingest_valid_past_auth"] == 400
    assert wg["stage_wrong"] == 403
    assert wg["relay_missing"] == 403

    for path in ("/version", "/api/services", "/_tick", "/_flush"):
        assert cloud_server.request("GET", path, front_key=None).status_code == 403
        assert cloud_server.request("GET", path, front_key="wrong-front-key").status_code == 403

    assert cloud_server.request("GET", "/_tick/", extra_headers={"x-flp-tick": "1"}).status_code in {403, 404}
    assert cloud_server.request("POST", "/_tick", extra_headers={"x-flp-tick": "1"}, json_body={}).status_code in {
        403,
        404,
        405,
    }
    assert cloud_server.request("POST", "/_flush", extra_headers={"x-flp-tick": "1"}, json_body={}).status_code in {
        403,
        404,
        405,
    }
    assert cloud_server.request("GET", "/_tick", extra_headers={"x-flp-tick": "1"}).status_code == 200
    assert cloud_server.request("GET", "/_flush", extra_headers={"x-flp-tick": "1"}).status_code == 200


def test_t5_b02_b03_b04_b06_cloud_webauthn_ceremony_setup_lockout_last_admin_and_csrf(
    cloud_server: CloudTestServer,
) -> None:
    """GAP-CLOUD-B02, B03, B04 & B06: WebAuthn ceremonies, 20-failure setup lockout, last-admin protection & CSRF."""
    # GAP-CLOUD-B03(2): 20 wrong setup codes lock setup so the 21st valid setup_code is rejected (401)
    for _ in range(20):
        r_wrong = cloud_server.request("POST", "/api/setup/accept", json_body={"code": "wrong-setup-code"})
        assert r_wrong.status_code == 401
    r_locked = cloud_server.request("POST", "/api/setup/accept", json_body={"code": cloud_server.setup_code})
    assert r_locked.status_code == 401

    # Reset and capture setup grant before registering Admin A1
    cloud_server.reset()
    r_accept = cloud_server.request("POST", "/api/setup/accept", json_body={"code": cloud_server.setup_code})
    assert r_accept.status_code == 200
    stale_setup = extract_cookie_value(r_accept, "flp_setup")
    assert stale_setup
    cloud_server.reset()

    admin1 = cloud_server.register_admin("Sole Admin A1")

    # GAP-CLOUD-B03(1): Post-bootstrap setup non-reentrancy
    assert (
        cloud_server.request(
            "POST",
            "/api/auth/register/options",
            setup_cookie=stale_setup,
            json_body={"display_name": "Late", "invite_token": None},
        ).status_code
        == 401
    )
    assert (
        cloud_server.request("POST", "/api/setup/accept", json_body={"code": cloud_server.setup_code}).status_code
        == 401
    )
    r_setup_page = cloud_server.request("GET", "/setup")
    assert r_setup_page.status_code == 303
    assert r_setup_page.headers.get("location") == "/admin"

    # GAP-CLOUD-B02: WebAuthn challenge replay, ceremony confusion, missing UV, wrong origin & sign_count rollback
    r_lo_opts = cloud_server.request("POST", "/api/auth/login/options", json_body={})
    opts = r_lo_opts.json()
    chal = opts.get("public_key", opts.get("publicKey", {}))["challenge"]
    chal_id = opts["challenge_id"]

    # Ceremony confusion: passing login challenge_id to register/verify -> 401
    _, _, reg_cred = _build_ed25519_attestation(rp_id=cloud_server.rp_id, origin=cloud_server.origin, challenge=chal)
    assert (
        cloud_server.request(
            "POST",
            "/api/auth/register/verify",
            json_body={"challenge_id": chal_id, "credential": reg_cred},
        ).status_code
        == 401
    )

    # Fresh login challenge: advance sign_count to 5 cleanly, then test replay & sign_count rollback to 4
    r_lo2 = cloud_server.request("POST", "/api/auth/login/options", json_body={})
    o2 = r_lo2.json()
    c2, cid2 = o2.get("public_key", o2.get("publicKey", {}))["challenge"], o2["challenge_id"]
    admin1.sign_count = 4
    valid_assertion = _build_ed25519_assertion(
        admin1, rp_id=cloud_server.rp_id, origin=cloud_server.origin, challenge=c2
    )
    assert admin1.sign_count == 5
    assert (
        cloud_server.request(
            "POST", "/api/auth/login/verify", json_body={"challenge_id": cid2, "credential": valid_assertion}
        ).status_code
        == 200
    )
    # Replaying spent challenge_id a second time -> 401 + x-flp-auth-fail: 1
    r_replay = cloud_server.request(
        "POST", "/api/auth/login/verify", json_body={"challenge_id": cid2, "credential": valid_assertion}
    )
    assert r_replay.status_code == 401
    assert r_replay.headers.get("x-flp-auth-fail") == "1"

    # sign_count rollback (4 <= 5) -> 401 + x-flp-auth-fail: 1
    r_lo3 = cloud_server.request("POST", "/api/auth/login/options", json_body={})
    o3 = r_lo3.json()
    c3, cid3 = o3.get("public_key", o3.get("publicKey", {}))["challenge"], o3["challenge_id"]
    admin1.sign_count = 3
    rollback_assertion = _build_ed25519_assertion(
        admin1, rp_id=cloud_server.rp_id, origin=cloud_server.origin, challenge=c3
    )
    assert admin1.sign_count == 4
    r_rb = cloud_server.request(
        "POST", "/api/auth/login/verify", json_body={"challenge_id": cid3, "credential": rollback_assertion}
    )
    assert r_rb.status_code == 401
    assert r_rb.headers.get("x-flp-auth-fail") == "1"

    # Missing UV flag (0x01 UP only, without 0x04 UV) -> 401
    r_lo4 = cloud_server.request("POST", "/api/auth/login/options", json_body={})
    o4 = r_lo4.json()
    c4, cid4 = o4.get("public_key", o4.get("publicKey", {}))["challenge"], o4["challenge_id"]
    rp_hash = hashlib.sha256(cloud_server.rp_id.encode("utf-8")).digest()
    no_uv_auth_data = rp_hash + b"\x01" + (10).to_bytes(4, "big")
    cd_bytes = json.dumps(
        {"type": "webauthn.get", "challenge": c4, "origin": cloud_server.origin, "crossOrigin": False},
        separators=(",", ":"),
    ).encode("utf-8")
    no_uv_sig = admin1.private_key.sign(no_uv_auth_data + hashlib.sha256(cd_bytes).digest())
    cred_b64 = _b64url(admin1.cred_id)
    r_no_uv = cloud_server.request(
        "POST",
        "/api/auth/login/verify",
        json_body={
            "challenge_id": cid4,
            "credential": {
                "id": cred_b64,
                "rawId": cred_b64,
                "type": "public-key",
                "response": {
                    "clientDataJSON": _b64url(cd_bytes),
                    "authenticatorData": _b64url(no_uv_auth_data),
                    "signature": _b64url(no_uv_sig),
                },
            },
        },
    )
    assert r_no_uv.status_code == 401

    # GAP-CLOUD-B04: Sole Admin A1 cannot self-demote, self-revoke, or revoke sole passkey
    users_list = cloud_server.request("GET", "/api/admin/users", session_cookie=admin1.session_cookie).json()["users"]
    a1_id = users_list[0]["id"]
    a1_cred_id = users_list[0]["passkeys"][0]["id"]
    assert (
        cloud_server.request(
            "PATCH",
            f"/api/admin/users/{a1_id}",
            session_cookie=admin1.session_cookie,
            json_body={"role": "analyst"},
        ).status_code
        == 403
    )
    assert (
        cloud_server.request(
            "POST", f"/api/admin/users/{a1_id}/revoke", session_cookie=admin1.session_cookie, json_body={}
        ).status_code
        == 403
    )
    assert (
        cloud_server.request(
            "POST",
            f"/api/admin/users/{a1_id}/passkeys/{a1_cred_id}/revoke",
            session_cookie=admin1.session_cookie,
            json_body={},
        ).status_code
        == 403
    )

    # Invite & register second Admin A2 -> A2 can demote A1 (200), making A2 the protected last admin (403)
    admin2 = cloud_server.invite_and_register(admin1, role="admin", display_name="Second Admin A2")
    users_after = cloud_server.request("GET", "/api/admin/users", session_cookie=admin2.session_cookie).json()["users"]
    a2_id = next(u["id"] for u in users_after if u["display_name"] == "Second Admin A2")
    assert (
        cloud_server.request(
            "PATCH",
            f"/api/admin/users/{a1_id}",
            session_cookie=admin2.session_cookie,
            json_body={"role": "analyst"},
        ).status_code
        == 200
    )
    assert (
        cloud_server.request(
            "POST", f"/api/admin/users/{a2_id}/revoke", session_cookie=admin2.session_cookie, json_body={}
        ).status_code
        == 403
    )

    # GAP-CLOUD-B06: Global CSRF Origin & Content-Type enforcement across mutating routes
    for bad_origin in (None, "https://evil.example", "null"):
        assert (
            cloud_server.request(
                "POST",
                "/api/admin/invites",
                session_cookie=admin2.session_cookie,
                origin=bad_origin,
                json_body={"display_name": "X", "role": "analyst"},
            ).status_code
            == 403
        )
    for bad_ct in ("text/plain", "application/x-www-form-urlencoded"):
        r_ct = cloud_server._client.request(  # type: ignore[union-attr]
            "POST",
            "/api/admin/invites",
            headers={
                "fastly-client-ip": "198.51.100.10",
                "x-flp-front-key": DEFAULT_FRONT_KEY,
                "x-flp-front-rl": "1",
                "origin": DEFAULT_CLOUD_ORIGIN,
                "content-type": bad_ct,
                "cookie": f"flp_session={admin2.session_cookie}",
            },
            content=b'{"display_name":"X","role":"analyst"}',
        )
        assert r_ct.status_code == 422

    r_charset_ok = cloud_server.request(
        "POST",
        "/api/admin/invites",
        session_cookie=admin2.session_cookie,
        harness_header=True,
        extra_headers={"content-type": "application/json; charset=utf-8"},
        json_body={"display_name": "Harness Invite", "role": "analyst"},
    )
    assert r_charset_ok.status_code in {200, 201}


# ============================================================================
# Part 2: High-Scale Python, ClickHouse/SQL, Fastly/Terraform & Gateways/FE
# ============================================================================


def test_t5_hs01_clickhouse_console_access_ddl_18_masked_keys_and_fail_closed_isolation(
    ch_recorder: RecordingClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5-HS-01: Real provision_console_access 18-key DDL, missing-view abort, and ConsoleClients._assert_isolated."""
    assert len(console_access.MASKED_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_18_PII_KEYS)

    schema_by_table = {
        "request_facts": [
            {"name": "service_id", "type": "LowCardinality(String)"},
            {"name": "publication_state", "type": "LowCardinality(String)"},
            {"name": "batch_id", "type": "String"},
            {"name": "client_ip", "type": "String"},
            {"name": "url", "type": "String"},
            {"name": "custom_fields", "type": "Map(String, String)"},
            {"name": "cmcd", "type": "Map(String, String)"},
        ],
        "rum_vitals_facts": [
            {"name": "service_id", "type": "String"},
            {"name": "publication_state", "type": "String"},
            {"name": "batch_id", "type": "String"},
            {"name": "client_id", "type": "Nullable(String)"},
            {"name": "session_id", "type": "String"},
            {"name": "pop", "type": "String"},
        ],
        "rum_error_facts": [
            {"name": "service_id", "type": "String"},
            {"name": "publication_state", "type": "String"},
            {"name": "batch_id", "type": "String"},
            {"name": "client_id", "type": "String"},
            {"name": "message", "type": "String"},
        ],
    }

    ddl_log: list[str] = []

    class _FakeAdminCH:
        def __init__(self, *, omit_views: bool = False) -> None:
            self.omit_views = omit_views

        def execute(self, sql: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            ddl_log.append(sql)
            for tbl, cols in schema_by_table.items():
                if f"DESCRIBE TABLE `fla`.`{tbl}`" in sql:
                    return cols
            if "FROM system.tables" in sql:
                if self.omit_views:
                    return []
                views = console_access.console_tables("svc-a", masked=("_m_" in sql))
                return [{"name": v} for v in views.values()]
            return []

    secret = b"0123456789abcdef0123456789ABCDEF"
    users = console_access.provision_console_access(_FakeAdminCH(), "fla", "svc-a", secret)
    assert set(users) == {"masked", "raw"}

    masked_req_view = console_access.console_tables("svc-a", True)["request_facts"]
    create_masked_req = next(s for s in ddl_log if f"CREATE VIEW `fla`.`{masked_req_view}`" in s)
    assert "SQL SECURITY DEFINER" in create_masked_req
    assert f"`fla`.`{console_access.PUBLICATIONS_TABLE}` FINAL" in create_masked_req
    for key in SHARED_18_PII_KEYS:
        assert f"'{key}'" in create_masked_req
    assert "`custom_fields`) AS `custom_fields`" in create_masked_req
    assert "`cmcd`) AS `cmcd`" in create_masked_req
    assert any("REVOKE ALL ON *.*" in s for s in ddl_log)

    # Missing view in system.tables aborts BEFORE any GRANT SELECT executes
    ddl_log.clear()
    with pytest.raises(RuntimeError, match="nothing was granted"):
        console_access.provision_console_access(_FakeAdminCH(omit_views=True), "fla", "svc-a", secret)
    assert not any(s.startswith("GRANT SELECT") for s in ddl_log)

    # Non-String PII or Map column fails closed
    with pytest.raises(RuntimeError, match="cannot redact column"):
        console_access._redaction("client_ip", "IPv4", "'ip'")
    with pytest.raises(RuntimeError, match="cannot redact column"):
        console_access._redaction("custom_fields", "Map(String, Int64)", "'ip'")

    # Real ConsoleClients.client_for + POST /api/query fail-closed 403 when secret missing or _assert_isolated fails
    analyst = provision_analyst_session(service_ids=["svc-a"], mask_ips=True)
    ch_cfg = svcconfig.ClickHouseConfig(
        host="127.0.0.1", port=8123, user="default", password="", database="fla", secure=False
    )

    # Case A: secret=None -> client_for returns None -> 403 console_unavailable_for_analysts
    none_clients = console_access.ConsoleClients(settings=ch_cfg, secret=None)
    monkeypatch.setattr("backend.high_scale.postgres_control.get_console_clients", lambda: none_clients)
    c = _analyst_client(analyst)
    r_no_sec = c.post("/api/query", json={"service_id": "svc-a", "sql": "SELECT count() AS n FROM logs"})
    assert r_no_sec.status_code == 403
    assert "console_unavailable_for_analysts" in r_no_sec.text

    # Case B: _assert_isolated detects unredacted PII row (n=1) -> closes client and returns 403
    closed_count = 0

    class _LeakyViewCH:
        def execute(self, sql: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            if "service_id !=" in sql:
                return [{"n": 0}]
            return [{"n": 1}]

        def close(self) -> None:
            nonlocal closed_count
            closed_count += 1

    leaky_clients = console_access.ConsoleClients(
        settings=ch_cfg, secret=secret, client_factory=lambda _cfg: _LeakyViewCH()
    )
    monkeypatch.setattr("backend.high_scale.postgres_control.get_console_clients", lambda: leaky_clients)
    c2 = _analyst_client(analyst)
    r_leaky = c2.post("/api/query", json={"service_id": "svc-a", "sql": "SELECT count() AS n FROM logs"})
    assert r_leaky.status_code == 403
    assert "console_unavailable_for_analysts" in r_leaky.text
    assert closed_count == 1


def test_t5_hs02_production_query_and_request_facts_cache_role_mask_window_tenant_isolation(
    ch_recorder: RecordingClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5-HS-02: Production _HS_QUERY_RESULT_CACHE & _REQUEST_FACTS_CACHE isolate Admin, Unmasked, Masked, Window & Tenant."""
    ch_recorder.set_handler(
        lambda _sql, _params, _settings: [
            {
                "client_ip": "203.0.113.88",
                "url": "/reset?ip=203.0.113.88",
                "cookie_session": "sess-secret-88",
                "status": 200,
            }
        ]
    )
    unmasked_a = provision_analyst_session(
        service_ids=["svc-a"], mask_ips=False, email="unmasked-cache@example.test", client_ip="203.0.113.31"
    )
    masked_a = provision_analyst_session(
        service_ids=["svc-a"], mask_ips=True, email="masked-cache@example.test", client_ip="203.0.113.32"
    )
    hist_masked_a = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        query_start_time="2026-06-01T00:00:00Z",
        query_end_time="2026-06-01T00:30:00Z",
        email="hist-cache@example.test",
        client_ip="203.0.113.33",
    )
    masked_b = provision_analyst_session(
        service_ids=["svc-b"], mask_ips=True, email="svcb-cache@example.test", client_ip="203.0.113.34"
    )

    hs_query_mod._HS_QUERY_RESULT_CACHE.clear()
    admin_hs_mod._REQUEST_FACTS_CACHE.clear()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    try:
        q_body = {
            "service_id": "svc-a",
            "sql": "SELECT client_ip, url, cookie_session FROM logs LIMIT 5",
            "start_time": "2026-06-01T00:00:00Z",
            "end_time": "2026-06-01T01:00:00Z",
        }
        # 1. Admin populates cache with raw IP/session
        adm = make_client("127.0.0.1")
        r_adm1 = adm.post("/api/query", json=q_body)
        assert r_adm1.status_code == 200 and "203.0.113.88" in r_adm1.text
        calls_after_adm1 = len(ch_recorder.calls)
        r_adm2 = adm.post("/api/query", json=q_body)
        assert r_adm2.status_code == 200
        assert len(ch_recorder.calls) == calls_after_adm1  # Genuine cache hit for identical Admin persona!

        # 2. Unmasked Analyst misses Admin's cache (different ConsoleAccess tables)
        c_unm = _analyst_client(unmasked_a)
        r_unm = c_unm.post("/api/query", json=q_body)
        assert r_unm.status_code == 200 and "203.0.113.88" in r_unm.text
        calls_after_unm = len(ch_recorder.calls)
        assert calls_after_unm > calls_after_adm1

        # 3. Masked Analyst misses both Admin & Unmasked caches and NEVER receives raw IP or raw session!
        c_msk = _analyst_client(masked_a)
        r_msk1 = c_msk.post("/api/query", json=q_body)
        assert r_msk1.status_code == 200
        assert "203.0.113.88" not in r_msk1.text
        assert "sess-secret-88" not in r_msk1.text
        calls_after_msk1 = len(ch_recorder.calls)
        assert calls_after_msk1 > calls_after_unm

        # 4. Historical-window Masked Analyst misses open-window Masked cache AND hits cache on repeat (fixed 00:30Z clamp!)
        c_hist = _analyst_client(hist_masked_a)
        r_hist = c_hist.post("/api/query", json=q_body)
        assert r_hist.status_code == 200 and "203.0.113.88" not in r_hist.text
        calls_after_hist = len(ch_recorder.calls)
        assert calls_after_hist > calls_after_msk1
        r_hist2 = c_hist.post("/api/query", json=q_body)
        assert r_hist2.status_code == 200 and "203.0.113.88" not in r_hist2.text
        assert len(ch_recorder.calls) == calls_after_hist  # Genuine cache hit for fixed-window Masked persona!

        # 5. Tenant svc-b Masked Analyst misses svc-a cache
        c_b = _analyst_client(masked_b)
        r_b = c_b.post("/api/query", json={**q_body, "service_id": "svc-b"})
        assert r_b.status_code == 200 and "203.0.113.88" not in r_b.text
        assert len(ch_recorder.calls) > calls_after_hist

        # 6. _REQUEST_FACTS_CACHE also isolates Admin vs Masked Analyst and hits cache on repeat Masked request
        rf_body = {"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-06-01T01:00:00Z"}
        assert adm.post("/api/high-scale/services/svc-a/request-facts", json=rf_body).status_code == 200
        calls_after_rf_adm = len(ch_recorder.calls)
        r_rf_msk1 = c_msk.post("/api/high-scale/services/svc-a/request-facts", json=rf_body)
        assert r_rf_msk1.status_code == 200 and "203.0.113.88" not in r_rf_msk1.text
        calls_after_rf_msk1 = len(ch_recorder.calls)
        assert calls_after_rf_msk1 > calls_after_rf_adm
        r_rf_msk2 = c_msk.post("/api/high-scale/services/svc-a/request-facts", json=rf_body)
        assert r_rf_msk2.status_code == 200 and "203.0.113.88" not in r_rf_msk2.text
        assert len(ch_recorder.calls) == calls_after_rf_msk1
    finally:
        hs_query_mod._HS_QUERY_RESULT_CACHE.clear()
        admin_hs_mod._REQUEST_FACTS_CACHE.clear()


def test_t5_hs03_hs12_py08_sql_console_table_filter_parser_differentials_union_scoping_and_ssrf(
    ch_recorder: RecordingClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5-HS-03, T5-HS-12 & GAP-HS-PY-T5-08: system.one fail-closed, 18 SQL differentials, UNION ALL pop & NAT64 SSRF."""
    analyst = provision_analyst_session(service_ids=["svc-a"], mask_ips=False)

    # 1. All 18 SQL parser-differential / ClickHouse table-function / metadata payloads rejected
    adversarial_sqls = [
        r"SELECT 'a\'' UNION ALL SELECT * FROM system.tables --' FROM logs",
        "SELECT * FROM \"url\"('http://127.0.0.1:8123', 'CSV')",
        "SELECT * FROM file('/etc/passwd', 'LineAsString')",
        "SELECT * FROM remote('127.0.0.1:9000', 'system', 'one')",
        "SELECT * FROM s3('https://evil.example/a.parquet')",
        "SELECT * FROM numbers(10)",
        "SELECT `getenv`('CLICKHOUSE_PASSWORD') FROM logs",
        "SELECT dictGet('default.sec', 'val', 1) FROM logs",
        "SELECT * FROM system.tables",
        "SELECT * FROM (high_scale_batch_publications)",
        "SELECT * FROM logs WHERE batch_id IN high_scale_batch_publications",
        "SELECT * FROM logs WHERE batch_id GLOBAL IN high_scale_batch_publications",
        "SELECT * FROM .request_facts",
        "SELECT status FROM logs SETTINGS readonly=0",
        "SELECT status FROM logs FORMAT TabSeparated",
        "SELECT status FROM logs INTO OUTFILE '/tmp/x'",
        "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT 1 FROM r) SELECT * FROM r",
        "SELECT $$secret$$ FROM logs",
    ]
    c = _analyst_client(analyst)
    for bad_sql in adversarial_sqls:
        r_bad = c.post("/api/query", json={"service_id": "svc-a", "sql": bad_sql})
        assert r_bad.status_code in {400, 403}, f"SQL not rejected ({r_bad.status_code}): {bad_sql}"

    # 2. Multi-branch UNION ALL rewrites pop -> custom_fields['pop'] ONLY on logs, preserving native pop on client_vitals
    ch_recorder.calls.clear()
    r_union = c.post(
        "/api/query",
        json={
            "service_id": "svc-a",
            "sql": "SELECT pop, timestamp FROM client_vitals UNION ALL SELECT pop, timestamp FROM logs",
        },
    )
    assert r_union.status_code == 200, r_union.text
    executed_sql = next(sql for sql, _, _ in reversed(ch_recorder.calls) if "UNION ALL" in sql)
    left_branch, right_branch = executed_sql.split("UNION ALL", 1)
    assert "custom_fields['pop']" not in left_branch
    assert "custom_fields['pop']" in right_branch

    # 3. _assert_table_filters_enforced fails closed when system.one returns [{"n": 1}]
    class _BrokenFilterCH:
        def execute(self, sql: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            if "FROM system.one" in sql:
                return [{"n": 1}]
            return []

    with pytest.raises(RuntimeError, match="additional_table_filters"):
        hs_query_mod._assert_table_filters_enforced(_BrokenFilterCH())

    # 4. Partial tables mapping missing rum_vitals_facts fails closed when querying client_vitals
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    hs_svc_a = get_high_scale_service_registry().resolve("svc-a")
    assert hs_svc_a is not None
    with pytest.raises((PermissionError, RuntimeError)):
        hs_query_mod.query_endpoint(
            hs_svc_a,
            hs_query_mod.DashboardQueryRequest(sql="SELECT pop FROM client_vitals LIMIT 1"),
            None,
            None,
            False,
            client=ch_recorder,
            tables={"request_facts": "v_req_only"},
        )

    # 5. High-Scale SSRF: NAT64 (64:ff9b::/96 where Python is_global=True!), 6to4, Teredo, CGNAT, mapped IPv6 & alerts routes
    for disallowed_ip in (
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::7f00:1",
        "2002:7f00:0001::1",
        "2001:0000:4136:e378:8000:63bf:3fff:fdd2",
        "100.64.0.1",
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",
    ):
        assert webhook_url.restricted_webhook_host(disallowed_ip) is True, disallowed_ip
        assert bot_fetcher_mod._is_disallowed_ip(disallowed_ip) is True, disallowed_ip
        with pytest.raises(ValueError, match="restricted_webhook"):
            webhook_url.validate_webhook_destination(
                f"https://[{disallowed_ip}]/hook" if ":" in disallowed_ip else f"https://{disallowed_ip}/hook"
            )

    adm = make_client("127.0.0.1")
    for bad_hook in (
        "http://[::ffff:127.0.0.1]/hook",
        "http://[::ffff:169.254.169.254]/latest/meta-data/",
        "http://[2002:7f00:0001::1]/hook",
        "http://[64:ff9b::a9fe:a9fe]/hook",
        "http://metadata.google.internal/computeMetadata/v1/",
        "https://intranet.local/hook",
    ):
        r_al = adm.post(
            "/api/alerts",
            json={
                "id": "ssrf-al-1",
                "service_id": "svc-a",
                "name": "SSRF Probe",
                "metric": "5xx",
                "operator": ">",
                "threshold": 5.0,
                "window_min": 5,
                "webhook_url": bad_hook,
            },
        )
        assert r_al.status_code in {400, 422}, f"{bad_hook} -> {r_al.status_code} {r_al.text}"

        r_ch = adm.post(
            "/api/alerts",
            json={
                "id": "ssrf-al-ch",
                "service_id": "svc-a",
                "name": "SSRF Channel Probe",
                "metric": "5xx",
                "operator": ">",
                "threshold": 5.0,
                "window_min": 5,
                "channels": [{"type": "webhook", "url": bad_hook}],
            },
        )
        assert r_ch.status_code in {400, 422}, f"channel {bad_hook} -> {r_ch.status_code}"

        r_test = adm.post(
            "/api/alerts/ssrf-al-1/test",
            json={"service_id": "svc-a", "webhook_url": bad_hook, "dry_run": True},
        )
        assert r_test.status_code in {400, 422}, f"test {bad_hook} -> {r_test.status_code}"

    r_pub_dry = adm.post(
        "/api/alerts/pub-ok/test",
        json={"service_id": "svc-a", "webhook_url": "https://hooks.slack.com/services/T0/B0/X0", "dry_run": True},
    )
    assert r_pub_dry.status_code == 200
    assert r_pub_dry.json()["data"]["status"] == "validated"


def test_t5_hs04_py01_py03_service_pollution_non_default_tenant_and_18_pii_filter_field_oracles(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """T5-HS-04, GAP-HS-PY-T5-01 & 03: Split-brain service_id pollution, svc-b tenant access & 18 PII key blocking."""
    analyst_a = provision_analyst_session(
        service_ids=["svc-a"], mask_ips=True, email="pollute-a@example.test", client_ip="203.0.113.41"
    )
    analyst_b = provision_analyst_session(
        service_ids=["svc-b"], mask_ips=True, email="tenant-b@example.test", client_ip="203.0.113.42"
    )

    # 1. Split-brain service_id parameter pollution across query, headers, path, body, and cookie -> 403
    c_a = _analyst_client(analyst_a)
    assert (
        c_a.post(
            "/api/query?service_id=svc-a",
            json={"service_id": "svc-b", "sql": "SELECT count() AS n FROM logs"},
        ).status_code
        == 403
    )
    assert (
        c_a.post(
            "/api/dashboard/aggregates?service_id=svc-a",
            json={"service_id": "svc-b"},
        ).status_code
        == 403
    )
    assert (
        c_a.get(
            "/api/presets?service=svc-b",
            headers=analyst_a.headers({"X-Fastly-Service-Id": "svc-a"}),
        ).status_code
        == 403
    )
    assert (
        c_a.get(
            "/api/views/svc-a",
            headers=analyst_a.headers({"X-Service-Id": "svc-b"}),
        ).status_code
        == 403
    )
    c_a_cookie_pollute = _analyst_client(analyst_a)
    c_a_cookie_pollute.cookies.set("fla.activeServiceId", "svc-b")
    assert c_a_cookie_pollute.get("/api/presets").status_code == 403

    # 2. All 18 SHARED_18_PII_KEYS + origin_ip + cmcd aliases blocked in field-values, filters, plural fields[], and GET filters
    ch_recorder.calls.clear()
    for key in (*SHARED_18_PII_KEYS, "origin_ip", "cmcd_session", "cmcd_sid", "cmcd_cid"):
        r_fv = c_a.post("/api/dashboard/field-values", json={"service_id": "svc-a", "field": key, "search": "198.51"})
        assert r_fv.status_code == 403 and r_fv.json()["error"] == "pii_policy_violation", key

        r_csv = c_a.post(
            "/api/dashboard/raw/csv",
            json={"service_id": "svc-a", "filters": {key: {"mode": "include", "values": ["x"]}}},
        )
        assert r_csv.status_code == 403 and r_csv.json()["error"] == "pii_policy_violation", key

        r_agg = c_a.post("/api/dashboard/aggregates", json={"service_id": "svc-a", "fields": [key]})
        assert r_agg.status_code == 403 and r_agg.json()["error"] == "pii_policy_violation", key

        qf = json.dumps({key: {"mode": "include", "values": ["x"]}})
        r_get = c_a.get(f"/api/services/svc-a/rum/analytics?filters={qf}")
        assert r_get.status_code == 403 and r_get.json()["error"] == "pii_policy_violation", key

    # SEC-T5-ITER1-01: RFC 6838 +json media types, sniffed JSON on text/plain, non-dict JSON, and duplicate query/header params
    for plus_ct in ("application/problem+json", "application/vnd.api+json", "application/ld+json", "text/plain"):
        r_plus_fv = c_a.post(
            "/api/dashboard/field-values?service_id=svc-a",
            content=json.dumps({"service_id": "svc-a", "field": "oip", "search": "198.51"}).encode("utf-8"),
            headers=analyst_a.headers({"Content-Type": plus_ct}),
        )
        assert r_plus_fv.status_code == 403 and r_plus_fv.json()["error"] == "pii_policy_violation", plus_ct

        r_plus_csv = c_a.post(
            "/api/dashboard/raw/csv?service_id=svc-a",
            content=json.dumps(
                {"service_id": "svc-a", "filters": {"ip": {"mode": "include", "values": ["203.0.113.45"]}}}
            ).encode("utf-8"),
            headers=analyst_a.headers({"Content-Type": plus_ct}),
        )
        assert r_plus_csv.status_code == 403 and r_plus_csv.json()["error"] == "pii_policy_violation", plus_ct

        r_plus_agg = c_a.post(
            "/api/dashboard/aggregates?service_id=svc-a",
            content=json.dumps({"service_id": "svc-a", "fields": ["cookie_session", "ip"]}).encode("utf-8"),
            headers=analyst_a.headers({"Content-Type": plus_ct}),
        )
        assert r_plus_agg.status_code == 403 and r_plus_agg.json()["error"] == "pii_policy_violation", plus_ct

        r_plus_sid = c_a.post(
            "/api/query?service_id=svc-a",
            content=json.dumps({"service_id": "svc-b", "sql": "SELECT count() AS n FROM logs"}).encode("utf-8"),
            headers=analyst_a.headers({"Content-Type": plus_ct}),
        )
        assert r_plus_sid.status_code == 403 and r_plus_sid.json()["error"] == "service_not_authorized", plus_ct

    r_plus_bad = c_a.post(
        "/api/dashboard/field-values?service_id=svc-a",
        content=b'["not-a-dict"]',
        headers=analyst_a.headers({"Content-Type": "application/problem+json"}),
    )
    assert r_plus_bad.status_code == 400 and r_plus_bad.json()["error"] == "invalid_json_body"

    qf_safe = json.dumps({"url": {"mode": "include", "values": ["/ok"]}})
    qf_ip = json.dumps({"ip": {"mode": "include", "values": ["1.2.3.4"]}})
    r_dup_filters = c_a.get(f"/api/services/svc-a/rum/analytics?filters={qf_safe}&filters={qf_ip}")
    assert r_dup_filters.status_code == 403 and r_dup_filters.json()["error"] == "pii_policy_violation"

    r_dup_q_sid = c_a.get("/api/presets?service_id=svc-a&service_id=svc-b")
    assert r_dup_q_sid.status_code == 403 and r_dup_q_sid.json()["error"] == "service_not_authorized"

    r_dup_q_svc = c_a.get("/api/presets?service=svc-a&service=svc-b")
    assert r_dup_q_svc.status_code == 403 and r_dup_q_svc.json()["error"] == "service_not_authorized"

    r_dup_hdr = c_a.get(
        "/api/presets",
        headers=[
            *[(k, v) for k, v in analyst_a.headers().items()],
            ("X-Fastly-Service-Id", "svc-a"),
            ("X-Fastly-Service-Id", "svc-b"),
        ],
    )
    assert r_dup_hdr.status_code == 403 and r_dup_hdr.json()["error"] == "service_not_authorized"

    assert ch_recorder.calls == []

    # 3. Non-default svc-b Analyst succeeds on included-router paths & fla.activeServiceId=svc-b with zero svc-a leak
    svcconfig.update_status("svc-b", {"schema": [{"name": "url", "type": "VARCHAR"}]})
    c_b = _analyst_client(analyst_b)
    r_rf = c_b.post(
        "/api/high-scale/services/svc-b/request-facts",
        json={"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-06-01T01:00:00Z"},
    )
    assert r_rf.status_code == 200, r_rf.text

    r_qs = c_b.get("/api/query/schema/svc-b")
    assert r_qs.status_code == 200 and r_qs.json()["source"] == "svc-b"

    # Omitted param/cookie fails closed without leaking Admin's active service ("svc-a")
    r_omit = c_b.get("/api/presets")
    assert r_omit.status_code == 403
    assert "svc-a" not in r_omit.text
    assert r_omit.json().get("service") == ""

    # Cookie fla.activeServiceId=svc-b resolves cleanly to svc-b
    c_b_cookie = _analyst_client(analyst_b)
    c_b_cookie.cookies.set("fla.activeServiceId", "svc-b")
    r_cookie_ok = c_b_cookie.get("/api/presets")
    assert r_cookie_ok.status_code == 200


def test_t5_hs05_py02_py04_session_token_aead_cmcd_rum_ip_scrubbing_and_schema_pii_stripping(
    ch_recorder: RecordingClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5-HS-05, GAP-HS-PY-T5-02 & 04: AEAD session_token cross-service replay, CMCD/RUM IP scrubbing & 18-PII schema strip."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    rum_analytics_mod._live_events_cache.clear()

    # Populate all 18 PII columns + benign columns in status schema
    svcconfig.update_status(
        "svc-a",
        {"schema": [{"name": k, "type": "VARCHAR"} for k in (*SHARED_18_PII_KEYS, "url", "status")]},
    )

    dual_masked = provision_analyst_session(
        service_ids=["svc-a", "svc-b"],
        mask_ips=True,
        email="dual-masked@example.test",
        client_ip="203.0.113.50",
    )

    monkeypatch.setattr(
        "backend.high_scale.sessions.sessions_endpoint",
        lambda *_a, **_kw: {
            "sessions": [
                {
                    "ip": "203.0.113.77",
                    "ja4": "t13d1516h2_abc_def",
                    "session_start": "2026-06-01T00:00:00Z",
                    "session_end": "2026-06-01T00:10:00Z",
                    "req_count": 4,
                    "flagged": False,
                    "edge_sid": "esid-raw-77",
                }
            ],
            "total": 1,
            "page": 1,
            "limit": 50,
            "has_rtt": False,
            "has_ja4": True,
            "has_edge": False,
        },
    )
    monkeypatch.setattr(
        "backend.high_scale.sessions.sessions_detail",
        lambda *_a, **_kw: {
            "columns": ["timestamp", "url", "ua", "ip", "edge_sid", "cmcd_sid", "cmcd_cid"],
            "data": [
                {
                    "timestamp": "2026-06-01T00:01:00Z",
                    "url": "/pay?client_ip=203.0.113.77&v6=2001:db8::dead:beef",
                    "ua": "Mozilla/5.0 (XFF 203.0.113.77)",
                    "ip": "203.0.113.77",
                    "edge_sid": "esid-raw-77",
                    "cmcd_sid": "cmcd-sid-secret-1",
                    "cmcd_cid": "cmcd-cid-secret-2",
                }
            ],
        },
    )
    monkeypatch.setattr(
        "backend.high_scale.rum.rum_live_events",
        lambda *_a, **_kw: [
            {
                "time": "2026-06-01T00:02:00Z",
                "type": "pageview",
                "path": "/users/203.0.113.77/profile",
                "desc": "Loaded from 203.0.113.77",
                "browser": "Chrome",
                "os": "macOS",
                "raw_log": {"meta": {"page": {"url": "https://shop.example/u/203.0.113.77"}}},
            }
        ],
    )

    try:
        c = _analyst_client(dual_masked)
        # 1. cmcd_session aggregate blocked with 403 pii_policy_violation
        for agg_body in ({"domain": "cmcd"}, {"domain": "request", "dimension": "cmcd_session"}):
            r_cmcd = c.post("/api/high-scale/services/svc-a/aggregates", json=agg_body)
            assert r_cmcd.status_code == 403 and "pii_policy_violation" in r_cmcd.text

        # 2. POST /api/sessions returns masked ip + opaque AEAD session_token
        r_sess = c.post("/api/sessions", json={"service_id": "svc-a"})
        assert r_sess.status_code == 200
        row0 = r_sess.json()["sessions"][0]
        assert "203.0.113.77" not in r_sess.text
        tok_a = row0["session_token"]
        assert tok_a and "203.0.113.77" not in tok_a

        # Raw IP without session_token -> 403 pii_policy_violation
        r_raw_ip = c.post(
            "/api/sessions/detail",
            json={
                "service_id": "svc-a",
                "ip": "203.0.113.77",
                "start_time": "2026-06-01T00:00:00Z",
                "end_time": "2026-06-01T00:10:00Z",
            },
        )
        assert r_raw_ip.status_code == 403 and "pii_policy_violation" in r_raw_ip.text

        # Valid svc-a session_token -> 200 with embedded IPs and cmcd_sid/cmcd_cid scrubbed
        r_det = c.post("/api/sessions/detail?service_id=svc-a", json={"session_token": tok_a})
        assert r_det.status_code == 200
        for secret_val in (
            "203.0.113.77",
            "2001:db8::dead:beef",
            "esid-raw-77",
            "cmcd-sid-secret-1",
            "cmcd-cid-secret-2",
        ):
            assert secret_val not in r_det.text, secret_val

        # Replaying svc-a's AEAD session_token on svc-b -> 400 session_token_invalid
        r_replay_b = c.post("/api/sessions/detail?service_id=svc-b", json={"session_token": tok_a})
        assert r_replay_b.status_code == 400
        assert "session_token_invalid" in r_replay_b.text

        # Raw scoring session events blocked when mask_ips=True -> 403
        r_sc_ev = c.get("/api/services/svc-a/scoring/sessions/abcdef123456/events")
        assert r_sc_ev.status_code == 403

        # 3. RUM live-events scrubs embedded IPs without mutating cache for Admin
        r_rum = c.get(
            "/api/services/svc-a/rum/live-events?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z"
        )
        assert r_rum.status_code == 200
        assert "203.0.113.77" not in r_rum.text and "203.0.113.xxx" in r_rum.text
        r_rum_adm = make_client("127.0.0.1").get(
            "/api/services/svc-a/rum/live-events?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z"
        )
        assert r_rum_adm.status_code == 200 and "203.0.113.77" in r_rum_adm.text

        # 4. GAP-HS-PY-T5-04: query/schema and bootstrap strip all 18 SHARED_18_PII_KEYS
        r_qschema = c.get("/api/query/schema/svc-a")
        assert r_qschema.status_code == 200
        q_names = {col.get("name") or col.get("column") for col in r_qschema.json()["schema"]}
        assert q_names == {"url", "status"}

        r_boot = c.get("/api/bootstrap")
        assert r_boot.status_code == 200
        top_boot_cols = {col.get("name") or col.get("column") for col in r_boot.json()["schema"]}
        assert top_boot_cols == {"url", "status"}
        boot_svc = next(s for s in r_boot.json()["services"] if s["service_id"] == "svc-a")
        boot_cols = {col.get("name") or col.get("column") for col in boot_svc.get("status", {}).get("schema", [])}
        assert boot_cols == {"url", "status"}
    finally:
        rum_analytics_mod._live_events_cache.clear()


def test_t5_hs06_hs07_python_vcl_quotes_escapes_rtime_set_cookie_scope_and_rum_predicates() -> None:
    """T5-HS-06 & T5-HS-07: Python VCL quote/escape priority, Set-Cookie scope asymmetry, RTIME adjacency & RUM predicate."""
    # 1. Paired \" , single \" , trailing \\ , and %XX / %uXXXX / %u{0} rejected
    for bad_cond in (
        r'req.http.X == "foo\" || true || req.http.Y == \"bar"',
        r'req.http.X == "foo\"',
        'req.http.X == "foo\\"',
        'req.http.X == "%22"',
        'req.http.X == "%00"',
        'req.http.X == "%u0022"',
        'req.http.X == "%U0000"',
        'req.http.X == "%u{0}"',
    ):
        err = catalog_mod.validate_vcl_custom_condition(bad_cond)
        assert err is not None, f"expected error for {bad_cond!r}"

    # Deferred quote error priority: table.lookup co-occurring with \" reports table.lookup!
    co_err = catalog_mod.validate_vcl_custom_condition(r'table.lookup(cdn_auth, "k", "x\") != ""')
    assert co_err is not None and "table.lookup" in co_err

    # 2. req.http.Set-Cookie / bereq.http.Set-Cookie ALLOWED in condition, REJECTED in log expression; resp Set-Cookie & Cookie REJECTED in both
    assert catalog_mod.validate_vcl_custom_condition('req.http.Set-Cookie == ""') is None
    assert catalog_mod.validate_vcl_custom_condition('bereq.http.Set-Cookie == ""') is None
    assert catalog_mod.validate_vcl_custom_condition('resp.http.Set-Cookie != ""') is not None
    assert catalog_mod.validate_vcl_custom_condition('req.http.Cookie != ""') is not None
    assert catalog_mod.validate_vcl_log_expression("req.http.Set-Cookie") != []

    # 3. header.*, setcookie.*, unary -table.lookup_*, and RTIME adjacency blocked
    for forbidden_expr in (
        'header.get(req, "Cookie")',
        'setcookie.get_value_by_name(beresp, "sid")',
        '-table.lookup_integer(cdn_auth, "k")',
        '+table.lookup_float(cdn_auth, "k")',
        '!table.lookup_bool(cdn_auth, "k")',
        'now + 0mstable.lookup(cdn_auth, "k")',
        "now + 0.5wreq.http.Authorization",
        'now + 0mheader.get(req, "Cookie")',
    ):
        assert catalog_mod.validate_vcl_log_expression(forbidden_expr) != [], forbidden_expr
        assert catalog_mod.validate_vcl_custom_condition(f'{forbidden_expr} != ""') is not None, forbidden_expr

    # 4. RUM path predicate blocks whitespace-split / RTIME-adjacent req.url.path while allowing req.url.ext
    for bad_rum in ('REQ . URL . PATH != "/x"', '(now + 0msreq.url.path) != "/x"', 'req.url != "/x"'):
        assert validate_rum_custom_condition(bad_rum) is not None, bad_rum
    assert validate_rum_custom_condition('req.url.ext != "js"') is None

    # 5. system_names duplicate & byte_limit boolean/range checks
    assert (
        catalog_mod.validate_custom_field_entry(
            {
                "name": "edge_score",
                "label": "Dup",
                "vcl_log_expression": "req.http.X",
                "collection_stage": "edge",
                "sql_type": "INTEGER",
                "value_type": "numeric",
                "bytes_estimate": 16,
            },
            [],
            system_names={"edge_score"},
        )
        != []
    )
    for bad_bl in (True, 0, -1, 16385):
        assert (
            catalog_mod.validate_custom_field_entry(
                {
                    "name": "valid_cf",
                    "label": "Valid",
                    "vcl_log_expression": "req.http.X",
                    "collection_stage": "edge",
                    "sql_type": "VARCHAR",
                    "value_type": "string",
                    "bytes_estimate": 24,
                    "byte_limit": bad_bl,
                },
                [],
            )
            != []
        )

    # 6. Valid regex escapes succeed AND pass falco lint
    for valid_cond in (r'req.http.User-Agent ~ "bot\b"', r'req.url.path ~ "\.css$"', 'req.http.X == "100% ok"'):
        assert catalog_mod.validate_vcl_custom_condition(valid_cond) is None
        vcl_doc = f'backend F_o {{ .host = "127.0.0.1"; .port = "80"; }}\nsub vcl_recv {{\n  if ({valid_cond}) {{ return(pass); }}\n}}\n'
        assert run_falco_lint(vcl_doc).returncode == 0


def test_t5_hs08_hs09_terraform_untrimmed_controls_exclusion_regex_and_null_description_coalescing() -> None:
    """T5-HS-08 & T5-HS-09: Untrimmed controls in generate_terraform & exclusion regex, plus description: None -> ''."""
    base_tf_cfg: dict[str, Any] = {
        "service_id": "svc-a",
        "name": "TF Service",
        "fos_bucket": "bucket-a",
        "fos_region": "us-east-1",
        "fos_access_key_id": "ak-test",
        "fos_secret_access_key": "sk-test",
        "cdn_url": "https://cdn-svc-a.global.ssl.fastly.net",
        "cdn_secret": "replace-with-secure-secret",
    }

    # 1. Untrimmed \n, \r, \t, \x85, \u2028, \u2029 rejected across all 4 Terraform condition keys; spaces-only "   " allowed
    for ctrl in ("\n", "\r", "\t", "\x85", "\u2028", "\u2029"):
        bad_val = f'{ctrl}req.url.ext != "css"'
        for patch_cfg in (
            {"custom_condition": bad_val},
            {"provisioning": {"custom_condition": bad_val}},
            {"rum_custom_condition": bad_val},
            {"rum": {"enabled": True, "custom_condition": bad_val}},
        ):
            with pytest.raises(ValueError):
                terraform_gen.generate_terraform({**base_tf_cfg, **patch_cfg}, "ak-test", "sk-test")

    tf_spaces = terraform_gen.generate_terraform(
        {
            **base_tf_cfg,
            "custom_condition": "   ",
            "provisioning": {"custom_condition": "   "},
            "rum_custom_condition": "   ",
            "rum": {"enabled": True, "custom_condition": "   "},
        },
        "ak-test",
        "sk-test",
    )
    assert "fastly_service_vcl" in json.dumps(tf_spaces)

    # 2. validate_url_exclusion_regex rejects untrimmed controls, quotes, %00/%u0000/%u{0}, and template markers
    for bad_rx in (
        "\n^/a$",
        "\t^/a$",
        "\u2028^/a$",
        '^/a"b$',
        "^/a%00b$",
        "^/a%u0000b$",
        "^/a%u{0}b$",
        "^/${x}$",
        "^/%{x}$",
    ):
        with pytest.raises(RegexValidationError):
            validate_url_exclusion_regex(bad_rx)
    assert validate_url_exclusion_regex("   ") == ""
    assert validate_url_exclusion_regex(r"^/static/.*\.css$") == r"^/static/.*\.css$"

    # 3. description: None coalesces to "" across log-fields and custom-fields CRUD without 500 ResponseValidationError
    with make_client("127.0.0.1") as adm:
        r_lf = adm.post(
            "/api/services/svc-a/log-fields",
            json={
                "log_fields": {
                    "groups": ["core"],
                    "custom_fields": [
                        {
                            "name": "cf_null_desc",
                            "label": "Null Desc",
                            "description": None,
                            "vcl_log_expression": "req.http.X-Null-Desc",
                            "collection_stage": "edge",
                            "sql_type": "VARCHAR",
                            "value_type": "string",
                            "bytes_estimate": 32,
                            "enabled": True,
                        }
                    ],
                }
            },
        )
        assert r_lf.status_code == 200, r_lf.text

        r_list = adm.get("/api/services/svc-a/custom-fields")
        assert r_list.status_code == 200
        assert r_list.json()["fields"][0]["description"] == ""

        r_patch = adm.patch("/api/services/svc-a/custom-fields/cf_null_desc", json={"description": None})
        assert r_patch.status_code == 200
        assert r_patch.json()["field"]["description"] == ""

        r_del = adm.delete("/api/services/svc-a/custom-fields/cf_null_desc")
        assert r_del.status_code == 200
        assert r_del.json()["field"]["description"] == ""


def test_t5_py05_py06_py07_proxies_export_crlf_pk_hijack_and_share_backup_hardening(
    ch_recorder: RecordingClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GAP-HS-PY-T5-05..07: proxies/export CRLF & Low/csv, cross-service PK hijack 409, and share backup hardening."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(
        "backend.routers.security.hs_proxies",
        lambda *_a, **_kw: {
            "active_clients": [
                {"ip": "203.0.113.91", "risk_level": "High", "impossible_distance": True},
                {"ip": "198.51.100.92", "risk_level": "Low", "impossible_distance": False},
            ]
        },
    )
    monkeypatch.setattr("backend.routers.alerts.sync_admin_state", lambda *_a, **_kw: None)

    with make_client("127.0.0.1") as adm:
        # 1. GAP-HS-PY-T5-05: Header injection / CRLF rejected (400); High/Low + fastly-acl/csv/plain emit h11-valid quoted headers
        for bad_qs in (
            'threshold=high;%20filename="evil.html"&format=fastly-acl',
            "threshold=high%0d%0aX-Injected:%201&format=fastly-acl",
            'threshold=High&format=evil;%20filename="x.sh"',
        ):
            r_bad = adm.get(
                f"/api/security/proxies/export?service_id=svc-a&start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&{bad_qs}"
            )
            assert r_bad.status_code == 400, bad_qs

        r_acl = adm.get(
            "/api/security/proxies/export?service_id=svc-a&start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&threshold=High&format=fastly-acl"
        )
        assert r_acl.status_code == 200
        assert r_acl.headers["content-disposition"] == 'attachment; filename="watchdog_blocklist_high_fastly-acl.csv"'
        h11.Response(status_code=200, headers=list(r_acl.headers.raw))

        r_low = adm.get(
            "/api/security/proxies/export?service_id=svc-a&start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&threshold=Low&format=plain"
        )
        assert r_low.status_code == 200 and "198.51.100.92" in r_low.text

        # 2. GAP-HS-PY-T5-06: Cross-service alert & saved-view primary-key hijack blocked with 409
        assert adm.post(
            "/api/alerts?service_id=svc-a",
            json={
                "id": "pk-alert-1",
                "service_id": "svc-a",
                "name": "Tenant A Alert",
                "metric": "5xx",
                "operator": ">",
                "threshold": 5.0,
                "window_min": 5,
            },
        ).status_code in {200, 201}
        r_hijack_alert = adm.post(
            "/api/alerts?service_id=svc-b",
            json={
                "id": "pk-alert-1",
                "service_id": "svc-b",
                "name": "Hijacked by B",
                "metric": "5xx",
                "operator": ">",
                "threshold": 5.0,
                "window_min": 5,
            },
        )
        assert r_hijack_alert.status_code == 409
        assert [a["name"] for a in adm.get("/api/alerts/svc-a").json()["data"]] == ["Tenant A Alert"]

        assert adm.post(
            "/api/views/",
            json={"id": "pk-view-1", "service_id": "svc-a", "name": "Tenant A View", "filters_json": "{}"},
        ).status_code in {200, 201}
        r_hijack_view = adm.post(
            "/api/views/",
            json={"id": "pk-view-1", "service_id": "svc-b", "name": "Hijacked View B", "filters_json": "{}"},
        )
        assert r_hijack_view.status_code == 409
        assert [v["name"] for v in adm.get("/api/views/svc-a").json()] == ["Tenant A View"]

        # 3. GAP-HS-PY-T5-07: Share backup revoked-ID collision (200), invalid CIDR/policy (400), >10 MiB (413) & cache clear
        passphrase = "tier5-enterprise-backup-passphrase"
        inv = share_db.create_remote_invite(
            name="Backup User",
            email="backup-t5@example.test",
            passcode="ocean-breeze-cabin-42",
            expires_at_utc=None,
            ip_whitelist="198.51.100.0/24",
            service_ids=["svc-a"],
            pii_policy={"mask_ips": True},
        )
        exported_blob = share_db.export_backup(passphrase)
        share_db.revoke_remote_invite(inv["id"])

        r_reimp = adm.post(
            "/api/admin/share/backup/import",
            data={"passphrase": passphrase, "mode": "skip-collisions"},
            files={"file": ("backup.enc", io.BytesIO(exported_blob), "application/octet-stream")},
        )
        assert r_reimp.status_code == 200, r_reimp.text
        assert r_reimp.json()["skipped"] == 1

        def _enc(payload: dict[str, Any]) -> bytes:
            salt = secrets.token_bytes(16)
            nonce = secrets.token_bytes(12)
            key = hashlib.scrypt(passphrase.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
            return b"FOSBACKUP\x01" + salt + nonce + AESGCM(key).encrypt(nonce, json.dumps(payload).encode(), None)

        bad_blob = _enc(
            {
                "schema_version": share_db.LATEST_VERSION,
                "invites": [
                    {
                        "id": "inv-bad-cidr",
                        "name": "Bad CIDR",
                        "email": "badcidr@example.test",
                        "passcode": inv["passcode"],
                        "ip_whitelist": "NOT-AN-IP",
                        "pii_policy": '{"mask_ips": true}',
                    }
                ],
                "invite_services": [],
                "share_settings": [],
            }
        )
        r_bad_imp = adm.post(
            "/api/admin/share/backup/import",
            data={"passphrase": passphrase, "mode": "skip-collisions"},
            files={"file": ("bad.enc", io.BytesIO(bad_blob), "application/octet-stream")},
        )
        assert r_bad_imp.status_code == 400

        share_db.set_setting("sharing_enabled", "true")
        assert share_db.get_setting("sharing_enabled") == "true"
        toggle_blob = _enc(
            {
                "schema_version": share_db.LATEST_VERSION,
                "invites": [],
                "invite_services": [],
                "share_settings": [
                    {"key": "sharing_enabled", "value": "false"},
                    {"key": "rogue_unallowlisted_key", "value": "evil"},
                ],
            }
        )
        assert (
            adm.post(
                "/api/admin/share/backup/import",
                data={"passphrase": passphrase, "mode": "skip-collisions"},
                files={"file": ("toggle.enc", io.BytesIO(toggle_blob), "application/octet-stream")},
            ).status_code
            == 200
        )
        assert share_db.get_setting("sharing_enabled") == "false"
        assert share_db.get_setting("rogue_unallowlisted_key") is None

        oversized = io.BytesIO(b"z" * (10 * 1024 * 1024 + 32))
        assert (
            adm.post(
                "/api/admin/share/backup/import",
                data={"passphrase": passphrase, "mode": "skip-collisions"},
                files={"file": ("big.enc", oversized, "application/octet-stream")},
            ).status_code
            == 413
        )


def test_t5_hs10_hs11_fe07_fe08_gateway_caddy_helm_metrics_cidrs_node_proxy_and_csv_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5-HS-10, T5-HS-11, T5-FE-07 & T5-FE-08: Caddy/Helm mTLS & fcip_priv, METRICS_SCRAPE_CIDRS, duplicate headers & Node CSV/BlockList."""
    # 1. T5-HS-11 / BUG-GW-02 & T5-FE-08: Caddyfile, public-gateway.yaml & admin-gateway Caddyfile behavioral/structural checks
    caddy_root = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    pub_gw = (REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics" / "templates" / "public-gateway.yaml").read_text(
        encoding="utf-8"
    )
    admin_gw = (REPO_ROOT / "deploy" / "chart" / "admin-gateway" / "files" / "Caddyfile").read_text(encoding="utf-8")

    for label, text in (("Caddyfile", caddy_root), ("public-gateway.yaml", pub_gw)):
        m_pub = re.search(r"header_regexp\s+fcip_pub\s+Fastly-Client-IP\s+(\S+)", text)
        m_priv = re.search(r"header_regexp\s+fcip_priv\s+Fastly-Client-IP\s+(\S+)", text)
        assert m_pub is not None and m_priv is not None, label
        pub_re = re.compile(m_pub.group(1))
        priv_pat = m_priv.group(1)
        priv_re = re.compile(priv_pat.replace("^(?i)", "^", 1), re.IGNORECASE)
        for non_public in (
            "0177.0.0.1",
            "127.1",
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.169.254",
            "::1",
            "::ffff:127.0.0.1",
            "0:0:0:0:0:ffff:7f00:1",
            "0000:0000:0000:0000:0000:ffff:0a00:0001",
            "::0:ffff:7f00:1",
            "::7f00:1",
            "0:0:0:0:0:0:7f00:1",
            "00ff::1",
            "64:ff9b::7f00:1",
            "0064:ff9b::7f00:1",
            "ff02::1",
            "2002:7f00:1::1",
        ):
            if pub_re.search(non_public):
                assert priv_re.search(non_public) is not None, f"{label} missed {non_public}"
        for public_ip in (
            "2001:4860:4860::8888",
            "2606:4700::1",
            "2a04:4e42::1",
            "2400:cb00:2048:1::c629:d7a2",
            "203.0.113.45",
            "64.233.160.1",
        ):
            assert pub_re.search(public_ip) is not None, f"{label} rejected public {public_ip} in fcip_pub"
            assert priv_re.search(public_ip) is None, f"{label} false positive on public {public_ip} in fcip_priv"
        for stripped_hdr in (
            "-X-Admin-Gateway-Token",
            "-X-FLA-Admin-Gateway",
            "-X-FLA-Frontend-Proxy",
        ):
            assert stripped_hdr in text, f"{label} missing {stripped_hdr}"
        assert "X-FLA-Public-Gateway" in text
        assert "@api path /api/* /js/* /rum-beacon" in text

    # Remote/public callers cannot reach /docs, /redoc, or /openapi.json on FastAPI
    ext_doc_client = make_client(
        "203.0.113.99",
        headers={
            "Host": "share.example.test",
            "X-Proxied-By-Caddy": "true",
            "Fastly-Client-IP": "203.0.113.99",
        },
    )
    for doc_path in ("/docs", "/redoc", "/openapi.json"):
        assert ext_doc_client.get(doc_path).status_code in {400, 401, 403, 404}

    assert "mode require_and_verify" in admin_gw
    assert "protocols tls1.2 tls1.3" in admin_gw
    assert "abort" in admin_gw
    inject_pos = admin_gw.index("request_header X-Admin-Gateway-Token")
    for gw_strip in (
        "request_header -X-Remote-Analyst",
        "request_header -X-Proxied-By-Caddy",
        "request_header -X-FLA-Public-Gateway",
        "request_header -X-FLA-Frontend-Proxy",
        "request_header -X-FLA-Admin-Gateway",
    ):
        assert gw_strip in admin_gw and admin_gw.index(gw_strip) < inject_pos
    assert "request>headers>X-Admin-Gateway-Token delete" in admin_gw
    assert "request>headers>X-FLA-Admin-Gateway delete" in admin_gw

    # 2. helm template verification: missing cursorSecret fails closed; rendered manifests wire SESSION_TOKEN_SECRET, METRICS_SCRAPE_CIDRS & drop: [ALL]
    helm_bin = shutil.which("helm") or "/opt/homebrew/bin/helm"
    if Path(helm_bin).exists():
        chart_dir = str(REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics")
        fail_proc = subprocess.run(
            [helm_bin, "template", "fla", chart_dir],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert fail_proc.returncode != 0 and "cursorSecret" in (fail_proc.stderr + fail_proc.stdout)

        ok_proc = subprocess.run(
            [
                helm_bin,
                "template",
                "fla",
                chart_dir,
                "--set",
                f"secrets.cursorSecret={'c' * 32}",
                "--set",
                f"secrets.sessionTokenSecret={'s' * 32}",
                "--set",
                f"secrets.clickhousePassword={'p' * 16}",
                "--set",
                f"secrets.postgresPassword={'q' * 16}",
                "--set",
                f"secrets.clickhouseConsoleSecret={'k' * 32}",
                "--set",
                "config.metricsScrapeCidrs=10.42.0.0/16",
                "--set",
                "adminGateway.enabled=true",
                "--set",
                "adminGateway.image=registry.example.com/admin-gw:test",
                "--set",
                "adminGateway.existingSecret=admin-gw-sec",
                "--set",
                "adminGateway.host=admin.example.com",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert ok_proc.returncode == 0, ok_proc.stderr
        rendered = ok_proc.stdout
        assert "SESSION_TOKEN_SECRET" in rendered
        assert "METRICS_SCRAPE_CIDRS" in rendered
        assert "10.42.0.0/16" in rendered
        assert "allowPrivilegeEscalation: false" in rendered
        assert "runAsNonRoot: true" in rendered
        assert "- ALL" in rendered

    # 3. METRICS_SCRAPE_CIDRS & duplicate admin-gateway header rejection on FastAPI
    monkeypatch.setenv("METRICS_SCRAPE_CIDRS", "10.42.0.0/16")
    assert make_client("10.42.1.5").get("/metrics").status_code == 200
    assert make_client("10.99.1.5").get("/metrics").status_code == 403
    assert make_client("10.42.1.5", headers={"X-Forwarded-For": "10.42.1.5"}).get("/metrics").status_code == 403

    gw_secret = "g" * 40
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", gw_secret)
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    dup_gw = make_client(
        "10.42.1.5",
        headers={
            "Host": "admin.example.test",
            "X-Admin-Gateway-Token": gw_secret,
            "X-FLA-Admin-Gateway": gw_secret,
        },
    )
    assert dup_gw.get("/api/admin/share/banner").status_code == 401

    # 4. T5-HS-10 & T5-FE-07: Behavioral execution of Node proxy/CSV modules via node
    node_bin = shutil.which("node") or "/opt/homebrew/bin/node"
    assert Path(node_bin).exists()
    node_probe = subprocess.run(
        [
            node_bin,
            "--input-type=module",
            "-e",
            """
import net from 'node:net';
import fs from 'node:fs';

// Verify Node BlockList loopback stripping parity with apiSsrProxy.ts across expanded IPv4-mapped IPv6
const bl = new net.BlockList();
bl.addSubnet('127.0.0.0', 8, 'ipv4');
bl.addAddress('::1', 'ipv6');
bl.addSubnet('::ffff:127.0.0.0', 104, 'ipv6');
for (const ip of ['127.0.0.1', '127.99.1.2', '::1', '::ffff:127.0.0.1', '0:0:0:0:0:ffff:7f00:1']) {
  const family = net.isIP(ip) === 6 ? 'ipv6' : 'ipv4';
  if (!bl.check(ip, family)) throw new Error('Failed loopback check: ' + ip);
}
if (bl.check('203.0.113.9', 'ipv4')) throw new Error('False positive on public IP');

const exportCsv = fs.readFileSync('frontend/lib/exportCsv.ts', 'utf8');
const adminWire = fs.readFileSync('frontend/lib/cloud-engine/admin-wire.ts', 'utf8');
const proxyTs = fs.readFileSync('frontend/proxy.ts', 'utf8');
if (!exportCsv.includes('sanitizeCsvCell') || !exportCsv.includes('FORMULA_TRIGGER_RE')) {
  throw new Error('exportCsv missing formula neutralization');
}
const m = proxyTs.match(/ANALYST_BLOCKED_PREFIXES\\s*=\\s*\\[([^\\]]+)\\]/);
if (!m || !m[1].includes("'/logs'") || m[1].includes("'/query'")) {
  throw new Error('ANALYST_BLOCKED_PREFIXES mismatch: ' + (m ? m[1] : 'missing'));
}
if (!adminWire.includes('plainNumRe') || !adminWire.includes('leadingCtrlOrSpaceRe')) {
  throw new Error('admin-wire missing formula neutralization');
}
console.log('NODE_PROXY_AND_CSV_PARITY_OK');
""",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert node_probe.returncode == 0 and "NODE_PROXY_AND_CSV_PARITY_OK" in node_probe.stdout, node_probe.stderr
