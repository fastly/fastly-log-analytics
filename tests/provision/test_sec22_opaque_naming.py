"""SEC-22 / M5 regression tests: opaque CSPRNG resource naming, idempotency,
collision retry, legacy compatibility, and external/non-admin leak prevention.
"""

from __future__ import annotations

import argparse
import re
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backend import config as svcconfig
from backend.provision import cli, fos_setup, opaque_names
from backend.provision.declarative import orchestrate as decl_orchestrate
from backend.provision.session_scoring import wasm_deploy
from backend.routers import provision as provision_router
from backend.routers.bootstrap import _project_analyst_bootstrap_service
from backend.routers.rum import beacon_ingest
from backend.routers.services.crud import _trim_for_analyst
from backend.utils import terraform_gen

pytestmark = pytest.mark.no_postgres

_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


# ---------------------------------------------------------------------------
# (a) Shared CSPRNG helper & default generation across surfaces
# ---------------------------------------------------------------------------


def test_opaque_generators_entropy_format_and_independence():
    buckets = {opaque_names.generate_opaque_bucket_name() for _ in range(25)}
    prefixes = {opaque_names.generate_opaque_cdn_prefix() for _ in range(25)}
    scorers = {opaque_names.generate_opaque_scorer_domain() for _ in range(25)}

    assert len(buckets) == 25
    assert len(prefixes) == 25
    assert len(scorers) == 25
    assert buckets.isdisjoint(prefixes)

    for b in buckets:
        assert len(b) <= 63
        assert _DNS_LABEL_RE.match(b), f"invalid S3/DNS label: {b}"
        assert b.startswith("fla-")
        assert len(b) == 28  # "fla-" + 24 hex chars (96 bits)

    for p in prefixes:
        assert len(p) <= 63
        assert _DNS_LABEL_RE.match(p), f"invalid DNS label: {p}"
        assert p.startswith("fla-")

    for s in scorers:
        assert s.endswith(".edgecompute.app")
        label = s.removesuffix(".edgecompute.app")
        assert len(label) <= 63
        assert _DNS_LABEL_RE.match(label), f"invalid scorer DNS label: {s}"


def _wizard_args(**overrides) -> argparse.Namespace:
    defaults = {
        "token": "tok",
        "service_id": "SensitiveSvc987",
        "endpoint_name": None,
        "region": "us-east-1",
        "bucket": None,
        "sample_rate": 100,
        "edge_only": True,
        "period": "1 minute",
        "cdn_name": None,
        "cdn_prefix": None,
        "shield": None,
        "preset": "standard",
        "yes": True,
        "disable_delete_after": False,
        "disable_cron_sync": False,
        "disable_cron_prune": False,
        "non_interactive": True,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_cli_wizard_generates_independent_opaque_bucket_and_cdn_prefix():
    args = _wizard_args(service_id="SensitiveSvc987")
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "AcmeProdStore"}),
    ):
        cfg1 = cli.wizard(args)
        cfg2 = cli.wizard(args)

    for cfg in (cfg1, cfg2):
        bucket = cfg["fos_bucket_name"]
        cdn_url = cfg["cdn_url"]
        cdn_label = cdn_url.removeprefix("https://").removesuffix(".global.ssl.fastly.net")
        assert _DNS_LABEL_RE.match(bucket)
        assert _DNS_LABEL_RE.match(cdn_label)
        assert bucket != cdn_label
        assert "sensitivesvc" not in bucket.lower()
        assert "sensitivesvc" not in cdn_label.lower()
        assert "acmeprod" not in bucket.lower()
        assert "acmeprod" not in cdn_label.lower()
        assert cfg["_system_generated_bucket"] is True
        assert cfg["_system_generated_cdn_domain"] is True

    assert cfg1["fos_bucket_name"] != cfg2["fos_bucket_name"]
    assert cfg1["cdn_url"] != cfg2["cdn_url"]


def test_cli_wizard_preserves_explicit_user_overrides():
    args = _wizard_args(
        service_id="SvcOverride1",
        bucket="custom-user-bucket",
        cdn_prefix="custom-user-cdn",
    )
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "Acme"}),
    ):
        cfg = cli.wizard(args)

    assert cfg["fos_bucket_name"] == "custom-user-bucket"
    assert cfg["cdn_url"] == "https://custom-user-cdn.global.ssl.fastly.net"
    assert cfg["_system_generated_bucket"] is False
    assert cfg["_system_generated_cdn_domain"] is False


def test_provision_validate_defaults_to_independent_opaque_names():
    req = provision_router.ProvisionValidateRequest(
        token="tok",
        service_id="CustomerSvc42",
    )

    def fake_fastly(method, path, token=None, **_kwargs):
        if path == "/tokens/self":
            return {"id": "t1", "name": "admin", "user_id": "u1"}
        return {"name": "CustomerProd", "versions": [{"number": 3, "active": True}]}

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        res = provision_router.provision_validate(req)

    defaults = res["defaults"]
    bucket = defaults["fos_bucket_name"]
    cdn_prefix = defaults["cdn_prefix"]
    assert _DNS_LABEL_RE.match(bucket)
    assert _DNS_LABEL_RE.match(cdn_prefix)
    assert bucket != cdn_prefix
    assert "customersvc" not in bucket.lower()
    assert "customersvc" not in cdn_prefix.lower()
    assert "customerprod" not in bucket.lower()
    assert "customerprod" not in cdn_prefix.lower()


def test_terraform_gen_defaults_to_independent_opaque_names_and_preserves_share_domain():
    cfg_a = {
        "service_id": "SecretSvc999",
        "service_name": "SecretCorp",
        "fos_region": "us-east-1",
        "log_period": 60,
        "share_domain": "analytics.customer.example",
    }
    files_a = terraform_gen.generate_terraform(dict(cfg_a), "ak", "sk")
    files_b = terraform_gen.generate_terraform(dict(cfg_a), "ak", "sk")
    tf_a = "\n".join(files_a.values())
    tf_b = "\n".join(files_b.values())

    assert "fos-secretsvc999-logs" not in tf_a.lower()
    bucket_a = re.search(r'"bucket":\s*"(fla-[0-9a-f]{24})"', tf_a)
    cdn_a = re.search(r'"name":\s*"(fla-[0-9a-f]{24}\.global\.ssl\.fastly\.net)"', tf_a)
    bucket_b = re.search(r'"bucket":\s*"(fla-[0-9a-f]{24})"', tf_b)
    cdn_b = re.search(r'"name":\s*"(fla-[0-9a-f]{24}\.global\.ssl\.fastly\.net)"', tf_b)
    assert bucket_a and cdn_a and bucket_b and cdn_b
    assert bucket_a.group(1) != bucket_b.group(1)
    assert cdn_a.group(1) != cdn_b.group(1)
    assert bucket_a.group(1) not in cdn_a.group(1)
    assert "secretsvc999" not in bucket_a.group(1).lower()
    assert "secretsvc999" not in cdn_a.group(1).lower()

    # Cloud HCL: opaque compute/worker/front domains, user share_domain preserved
    cloud_cfg = {
        "service_id": "SecretSvc999",
        "install_name": "acme-prod",
        "fos_region": "us-east-1",
        "share_domain": "share.acme.example",
    }
    cloud_a = "\n".join(terraform_gen.generate_cloud_terraform(dict(cloud_cfg), "ak", "sk").values())
    cloud_b = "\n".join(terraform_gen.generate_cloud_terraform(dict(cloud_cfg), "ak", "sk").values())
    assert "acme-prod-worker.edgecompute.app" not in cloud_a
    assert "acme-prod-front.global.ssl.fastly.net" not in cloud_a
    assert "acme-prod.edgecompute.app" not in cloud_a
    assert "share.acme.example" in cloud_a
    assert cloud_a != cloud_b


# ---------------------------------------------------------------------------
# (b) Idempotency & crash recovery: reuse persisted random names
# ---------------------------------------------------------------------------


def test_build_desired_state_reuses_persisted_opaque_and_legacy_scorer_domains(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    opaque_dom = "fla-0123456789abcdef01234567.edgecompute.app"
    svcconfig.save_config(
        "SvcA",
        {
            "service_id": "SvcA",
            "fos_bucket_name": "fla-aaaaaaaaaaaaaaaaaaaaaaaa",
            "fos_region": "us-east-1",
            "log_period": 60,
            "scoring": {"enabled": True, "scoring_domain": opaque_dom, "request_secret": "s" * 32},
        },
    )
    state_opaque = decl_orchestrate.build_desired_state("SvcA", "tok")
    assert state_opaque.compute_scorer is not None
    assert state_opaque.compute_scorer.domains == [opaque_dom]

    legacy_dom = "fos-svcb-session-scorer.edgecompute.app"
    svcconfig.save_config(
        "SvcB",
        {
            "service_id": "SvcB",
            "fos_bucket_name": "fos-svcb-logs",
            "fos_region": "us-east-1",
            "log_period": 60,
            "scoring": {"enabled": True, "domain": legacy_dom, "request_secret": "s" * 32},
        },
    )
    state_legacy = decl_orchestrate.build_desired_state("SvcB", "tok")
    assert state_legacy.compute_scorer is not None
    assert state_legacy.compute_scorer.domains == [legacy_dom]


def test_build_desired_state_generates_opaque_domain_when_none_persisted(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svcconfig.save_config(
        "BrandNewSvc",
        {
            "service_id": "BrandNewSvc",
            "fos_bucket_name": "fla-bbbbbbbbbbbbbbbbbbbbbbbb",
            "fos_region": "us-east-1",
            "log_period": 60,
        },
    )
    fake_state = decl_orchestrate._load_or_bootstrap_feature_state("BrandNewSvc", "tok", persist_bootstrap=False)
    object.__setattr__(fake_state.scoring, "enabled", True)
    object.__setattr__(fake_state.scoring, "domain", "")
    with (
        patch(
            "backend.provision.declarative.orchestrate._load_or_bootstrap_feature_state",
            return_value=fake_state,
        ),
        patch("backend.provision.declarative.orchestrate.find_service_by_name", return_value=None),
    ):
        state = decl_orchestrate.build_desired_state("BrandNewSvc", "tok")
    assert state.compute_scorer is not None
    dom = state.compute_scorer.domains[0]
    assert dom.endswith(".edgecompute.app")
    assert "brandnewsvc" not in dom.lower()
    assert dom.startswith("fla-")


# ---------------------------------------------------------------------------
# (c) Collision retries (409 / BucketAlreadyExists / domain taken)
# ---------------------------------------------------------------------------


def test_ensure_fos_bucket_retries_collision_for_system_generated_name():
    from botocore.exceptions import ClientError

    chosen: list[str] = []
    create_calls: list[str] = []

    class FakeS3:
        def head_bucket(self, Bucket: str):
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

        def create_bucket(self, Bucket: str):
            create_calls.append(Bucket)
            if len(create_calls) < 3:
                raise ClientError(
                    {"Error": {"Code": "BucketAlreadyExists", "Message": "already exists"}},
                    "CreateBucket",
                )
            return {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=FakeS3()):
        ok = fos_setup.ensure_fos_bucket(
            "fla-111111111111111111111111",
            "us-east-1",
            "ak",
            "sk",
            system_generated=True,
            on_name_selected=chosen.append,
        )

    assert ok is True
    assert len(create_calls) == 3
    winner = chosen[-1]
    assert winner == create_calls[-1]
    assert winner != "fla-111111111111111111111111"
    assert _DNS_LABEL_RE.match(winner)


def test_ensure_fos_bucket_does_not_randomize_user_supplied_bucket():
    chosen: list[str] = []
    create_called = {"n": 0}

    class FakeS3:
        def head_bucket(self, Bucket: str):
            return {}

        def create_bucket(self, Bucket: str):
            create_called["n"] += 1
            return {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=FakeS3()):
        ok = fos_setup.ensure_fos_bucket(
            "fos-legacy-svc-logs",
            "us-east-1",
            "ak",
            "sk",
            system_generated=False,
            on_name_selected=chosen.append,
        )

    assert ok is True
    assert chosen == ["fos-legacy-svc-logs"]
    assert create_called["n"] == 0


def test_reconcile_cdn_service_state_retries_409_domain_collision(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    sid = "SvcCollision1"
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "fos_bucket_name": "fla-cccccccccccccccccccccccc",
        "fos_region": "us-east-1",
        "fos_access_key": "ak",
        "fos_secret_key": "sk",
        "cdn_secret": "sec",
        "cdn_service_name": "Log Analysis CDN Service",
        "cdn_url": "https://fla-first.global.ssl.fastly.net",
        "_system_generated_cdn_domain": True,
    }
    svcconfig.save_config(sid, dict(cfg))

    domain_attempts: list[str] = []

    def fake_fastly(method, path, data=None, token=None, **_kwargs):
        if method == "POST" and path == "/service":
            return {"id": "cdn-new-1"}
        if method == "POST" and path.endswith("/domain"):
            dom = (data or {}).get("name", "")
            domain_attempts.append(dom)
            if len(domain_attempts) < 3:
                raise RuntimeError("Fastly API error 409: Domain already taken by another service")
            return {"name": dom}
        if method == "PUT" and path.endswith("/activate"):
            return {"number": 2}
        return {"number": 2, "id": "res-1", "status": "ok"}

    with (
        patch("backend.provision.declarative.orchestrate.fastly", side_effect=fake_fastly),
        patch("backend.provision.declarative.fastly_ops.fastly", side_effect=fake_fastly),
        patch("backend.provision.declarative.orchestrate.find_service_by_name", return_value=None),
        patch("backend.provision.declarative.orchestrate._fetch_snippets", return_value=[]),
        patch("backend.provision.declarative.orchestrate._fetch_backends", return_value=[]),
        patch("backend.provision.declarative.orchestrate._fetch_dictionaries", return_value=[]),
        patch(
            "backend.provision.declarative.orchestrate.fastly_integration.fetch_active_version",
            return_value=1,
        ),
    ):
        decl_orchestrate.reconcile_cdn_service_state(sid, "tok")

    assert len(domain_attempts) == 3
    winner = domain_attempts[-1]
    assert winner != "fla-first.global.ssl.fastly.net"
    assert winner.endswith(".global.ssl.fastly.net")
    persisted = svcconfig.load_config(sid)
    assert persisted is not None
    assert persisted["cdn_url"] == f"https://{winner}"


def test_ensure_scoring_service_retries_409_domain_collision_and_persists():
    persisted_domains: list[str] = []
    domain_calls: list[str] = []

    def fake_fastly(method, path, data=None, token=None, **_kwargs):
        if method == "POST" and path == "/service":
            return {"id": "scorer-new-1"}
        if method == "POST" and path.endswith("/domain"):
            dom = (data or {}).get("name", "")
            domain_calls.append(dom)
            if len(domain_calls) < 2:
                raise RuntimeError("Fastly API error 409: Domain already taken")
            return {"name": dom}
        if method == "POST" and path == "/resources/stores/config":
            return {"id": f"store-{(data or {}).get('name', 'x')}"}
        return {}

    with (
        patch("backend.provision.session_scoring.wasm_deploy.fastly", side_effect=fake_fastly),
        patch("backend.provision.session_scoring.wasm_deploy._find_scoring_service", return_value=None),
        patch("backend.provision.session_scoring.wasm_deploy.product_enabled", return_value=True),
        patch(
            "backend.provision.session_scoring.wasm_deploy._ensure_matrix_kv_store",
            return_value={"store_id": "kv-1"},
        ),
        patch("backend.provision.session_scoring.wasm_deploy._ensure_scoring_enabled_at", return_value=None),
    ):
        meta = wasm_deploy.ensure_scoring_service(
            "SvcScorer1",
            "tok",
            preferred_domain="fla-collide.edgecompute.app",
            allow_domain_retry=True,
            on_domain_selected=persisted_domains.append,
        )

    assert len(domain_calls) == 2
    assert meta["scoring_domain"] == domain_calls[-1]
    assert meta["scoring_domain"] != "fla-collide.edgecompute.app"
    assert meta["scoring_domain"].endswith(".edgecompute.app")
    assert persisted_domains[-1] == meta["scoring_domain"]


# ---------------------------------------------------------------------------
# (d) Legacy compatibility: existing fos-<sid>-* scorer & FOS key lookup
# ---------------------------------------------------------------------------


def test_ensure_scoring_service_reuses_legacy_existing_domain():
    existing_svc = {
        "id": "legacy-scorer-id",
        "name": "FLA-SessionScorer-LegacySvc",
        "versions": [{"number": 4, "active": True}],
    }

    def fake_fastly(method, path, data=None, token=None, **_kwargs):
        if method == "GET" and path == "/service/legacy-scorer-id/version/4/domain":
            return [{"name": "fos-legacysvc-session-scorer.edgecompute.app"}]
        if method == "GET" and "/resources/stores/config/" in path and "/item/" in path:
            return {"item_value": "deadbeef" * 8}
        return {}

    with (
        patch("backend.provision.session_scoring.wasm_deploy.fastly", side_effect=fake_fastly),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_scoring_service",
            return_value=existing_svc,
        ),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_config_store",
            side_effect=[{"id": "ks-1"}, {"id": "cs-1"}],
        ),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_kv_store",
            return_value={"store_id": "kv-1"},
        ),
        patch("backend.provision.session_scoring.wasm_deploy._ensure_scoring_enabled_at", return_value=None),
    ):
        meta = wasm_deploy.ensure_scoring_service("LegacySvc", "tok")

    assert meta["scoring_domain"] == "fos-legacysvc-session-scorer.edgecompute.app"


# ---------------------------------------------------------------------------
# (e) External & non-admin leak prevention
# ---------------------------------------------------------------------------


def test_non_admin_service_and_bootstrap_projections_strip_internal_hosts():
    full_service = {
        "service_id": "SvcLeakTest",
        "name": "Prod Site",
        "is_active": True,
        "fos_bucket": "fla-secretbucket1234567890ab",
        "fos_bucket_name": "fla-secretbucket1234567890ab",
        "fos_prefix": "logs/secret",
        "cdn_url": "https://fla-secretcdn1234567890abcd.global.ssl.fastly.net",
        "scorer_domain": "fla-secretscorer1234567890ab.edgecompute.app",
        "scoring": {
            "enabled": True,
            "scoring_domain": "fla-secretscorer1234567890ab.edgecompute.app",
            "scoring_service_id": "scorer-123",
        },
        "cmcd_enabled": True,
        "rum_enabled": True,
        "rum_log_period": 60,
        "status": {
            "schema": "v2",
            "fos_bucket": "fla-secretbucket1234567890ab",
            "cdn_url": "https://fla-secretcdn1234567890abcd.global.ssl.fastly.net",
        },
    }

    trimmed_list = _trim_for_analyst(
        [full_service],
        allowed_ids={"SvcLeakTest"},
    )
    assert len(trimmed_list) == 1
    trimmed_svc = trimmed_list[0]
    assert set(trimmed_svc.keys()) == {"service_id", "name", "is_active"}
    serialized_svc = str(trimmed_list)
    assert "fos_bucket" not in serialized_svc
    assert "cdn_url" not in serialized_svc
    assert "edgecompute.app" not in serialized_svc
    assert "global.ssl.fastly.net" not in serialized_svc

    trimmed_boot = _project_analyst_bootstrap_service(full_service)
    serialized_boot = str(trimmed_boot)
    assert "fla-secretbucket" not in serialized_boot
    assert "fla-secretcdn" not in serialized_boot
    assert "fla-secretscorer" not in serialized_boot
    assert "edgecompute.app" not in serialized_boot
    assert "global.ssl.fastly.net" not in serialized_boot
    assert "fos_prefix" not in serialized_boot


def test_unauthenticated_remote_rum_js_does_not_leak_active_service_id():
    remote_req = SimpleNamespace(
        headers={"x-forwarded-host": "share.customer.example"},
        cookies={},
        state=SimpleNamespace(),
    )
    with (
        patch("backend.utils.remote_access.is_request_remote", return_value=True),
        patch("backend.config.get_active_service_id", return_value="LeakedSvcId999") as mock_active,
    ):
        resolved = beacon_ingest._resolve_rum_js_service_id(remote_req, service_id=None)

    assert resolved is None
    mock_active.assert_not_called()


# ---------------------------------------------------------------------------
# (f) SEC-22 Follow-Up Wave 1 regressions (Items #1, #5, #6, #8, #9)
# ---------------------------------------------------------------------------


def test_rum_tracker_js_omits_service_id_from_public_js_and_beacon_url(monkeypatch):
    from backend.provision.rum_assets import generate_rum_tracker_js

    monkeypatch.setattr(svcconfig, "load_config", lambda _sid: {})
    js = generate_rum_tracker_js("SensitiveCustomerSvcId99")
    assert "SensitiveCustomerSvcId99" not in js
    assert "var SERVICE_ID" not in js
    assert "service_id=" not in js
    assert 'var BEACON_ENDPOINT = "/rum-beacon";' in js
    assert "url: BEACON_ENDPOINT," in js


@pytest.mark.parametrize("versions_val", [None, []])
def test_ensure_scoring_service_queries_existing_domain_when_versions_missing_or_empty(versions_val):
    existing_svc: dict[str, object] = {
        "id": "opaque-scorer-id",
        "name": "Session Scoring Service for SvcMissingVers",
    }
    if versions_val is not None:
        existing_svc["versions"] = versions_val

    seen_paths: list[str] = []

    def fake_fastly(method, path, data=None, token=None, **_kwargs):
        seen_paths.append(f"{method} {path}")
        if method == "GET" and path == "/service/opaque-scorer-id/version":
            return [{"number": 7, "active": True}]
        if method == "GET" and path == "/service/opaque-scorer-id/version/7/domain":
            return [{"name": "fla-0123456789abcdef01234567.edgecompute.app"}]
        if method == "GET" and "/resources/stores/config/" in path and "/item/" in path:
            return {"item_value": "deadbeef" * 8}
        return {}

    with (
        patch("backend.provision.session_scoring.wasm_deploy.fastly", side_effect=fake_fastly),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_scoring_service",
            return_value=existing_svc,
        ),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_config_store",
            side_effect=[{"id": "ks-1"}, {"id": "cs-1"}],
        ),
        patch(
            "backend.provision.session_scoring.wasm_deploy._find_kv_store",
            return_value={"store_id": "kv-1"},
        ),
        patch("backend.provision.session_scoring.wasm_deploy._ensure_scoring_enabled_at", return_value=None),
        patch("backend.provision.session_scoring.wasm_deploy._persisted_scoring_domain", return_value=""),
    ):
        meta = wasm_deploy.ensure_scoring_service("SvcMissingVers", "tok")

    assert meta["scoring_domain"] == "fla-0123456789abcdef01234567.edgecompute.app"
    assert "GET /service/opaque-scorer-id/version" in seen_paths
    assert "GET /service/opaque-scorer-id/version/7/domain" in seen_paths


def test_provision_execute_arms_collision_retry_flags_when_generated_flags_true():
    import asyncio

    from backend.models.common.provision import ProvisionExecuteRequest

    async def _drain(it):
        return [chunk async for chunk in it]

    captured_cfgs: list[dict[str, object]] = []

    def fake_provision(cfg):
        captured_cfgs.append(dict(cfg))
        yield {"type": "log", "message": "ok"}

    req = ProvisionExecuteRequest(
        token="tok",
        service_id="SvcUiWiz1",
        service_name="UI Wizard Svc",
        fos_bucket_name="fla-0123456789abcdef01234567",
        cdn_url="https://fla-fedcba9876543210fedcba98.global.ssl.fastly.net",
        bucket_generated=True,
        cdn_domain_generated=True,
    )
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=None),
        patch("backend.routers.provision._check_domain_available", return_value=(True, "")),
        patch("backend.provision.provision", side_effect=fake_provision),
    ):
        resp = provision_router.provision_execute(req)
        asyncio.run(_drain(resp.body_iterator))

    assert len(captured_cfgs) == 1
    assert captured_cfgs[0].get("_system_generated_bucket") is True
    assert captured_cfgs[0].get("_system_generated_cdn_domain") is True

    # User-supplied values (flags False) must NOT arm system-generated flags
    captured_cfgs.clear()
    req_custom = ProvisionExecuteRequest(
        token="tok",
        service_id="SvcUiWiz2",
        service_name="UI Wizard Svc",
        fos_bucket_name="user-custom-bucket",
        cdn_url="https://user-custom-cdn.global.ssl.fastly.net",
        bucket_generated=False,
        cdn_domain_generated=False,
    )
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=None),
        patch("backend.routers.provision._check_domain_available", return_value=(True, "")),
        patch("backend.provision.provision", side_effect=fake_provision),
    ):
        resp2 = provision_router.provision_execute(req_custom)
        asyncio.run(_drain(resp2.body_iterator))

    assert len(captured_cfgs) == 1
    assert captured_cfgs[0].get("_system_generated_bucket") is None
    assert captured_cfgs[0].get("_system_generated_cdn_domain") is None


def test_deploy_remote_frontend_defaults_opaque_domain_retries_409_and_strips_headers():
    from backend.provision.fastly_api import deploy_remote_frontend

    domain_attempts: list[str] = []
    snippet_payloads: list[dict[str, object]] = []

    def fake_fastly(method, path, body=None, *, token, **_kwargs):
        if method == "POST" and path == "/service":
            return {"id": "rem-svc-1"}
        if method == "POST" and path == "/service/rem-svc-1/version":
            return {"number": 1}
        if method == "POST" and path == "/service/rem-svc-1/version/1/domain":
            dom = (body or {}).get("name", "")
            domain_attempts.append(dom)
            if len(domain_attempts) < 3:
                raise RuntimeError("HTTP 409 POST /domain: Domain already taken")
            return {"name": dom}
        if method == "POST" and path == "/service/rem-svc-1/version/1/snippet":
            snippet_payloads.append(dict(body or {}))
            return {"id": "snip-1"}
        return {"active": True}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name="Remote Portal",
            domain_name="",
            origin_host="origin.example.com",
            origin_port=443,
            use_ssl=True,
            token="tok",
        )

    assert len(domain_attempts) == 3
    winner = domain_attempts[-1]
    assert res["domain_name"] == winner
    assert winner.endswith(".global.ssl.fastly.net")
    label = winner.removesuffix(".global.ssl.fastly.net")
    assert _DNS_LABEL_RE.match(label) and label.startswith("fla-")
    assert len(set(domain_attempts)) == 3

    assert len(snippet_payloads) == 1
    assert snippet_payloads[0]["type"] == "deliver"
    content = str(snippet_payloads[0]["content"])
    for hdr in (
        "resp.http.X-Service-Id",
        "resp.http.X-FOS-Bucket",
        "resp.http.X-FOS-Bucket-Name",
        "resp.http.X-Upstream-Host",
        "resp.http.X-Backend-Host",
        "resp.http.X-Edgecompute-Host",
        "resp.http.Server",
        "resp.http.X-Powered-By",
    ):
        assert f"unset {hdr};" in content


def test_rotate_fos_credentials_uses_opaque_key_description():
    from backend.models.common.services import ServiceCredentialsBody
    from backend.routers.services import crud as services_crud

    created_descs: list[str] = []

    def fake_fastly(method, path, body=None, *, token=None, **_kwargs):
        if method == "POST" and path == "/resources/object-storage/access-keys":
            created_descs.append(str((body or {}).get("description", "")))
            return {"access_key": "AK_NEW", "secret_key": "SK_NEW"}
        return {}

    fake_req = SimpleNamespace(state=SimpleNamespace(is_remote=False))
    body = ServiceCredentialsBody(api_token="admin-tok", access_key="", secret_key="")

    with (
        patch("backend.routers.services.crud.require_service_in_scope", return_value=None),
        patch(
            "backend.routers.services.crud.load_service_config",
            return_value={"fos_region": "us-east-1", "fos_bucket": "fla-0123456789abcdef01234567"},
        ),
        patch("backend.routers.services.crud.mutate_service_config", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        res = services_crud.api_service_update_credentials(fake_req, "SensitiveSvcId777", body)

    assert res["ok"] is True
    assert len(created_descs) == 1
    desc = created_descs[0]
    assert "SensitiveSvcId777" not in desc
    assert re.match(r"^fos-log-analysis-key-[0-9a-f]{24}$", desc), f"unexpected key description: {desc}"
