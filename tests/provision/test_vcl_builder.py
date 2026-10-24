"""Tests for the VclBuilder VCL-assembly primitives (B15 consolidation)."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision import cmcd_orchestrator as orch
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, reconcile_cmcd_custom_fields
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState
from backend.provision.draft_version import draft_version
from backend.provision.log_paths import (
    ANALYTICS_LOG_LAYOUT,
    analytics_log_path,
    minute_list_prefix,
    rum_log_path,
    rum_minute_list_prefix,
)
from backend.provision.sharing_domain import delete_remote_frontend, deploy_remote_frontend
from backend.provision.vcl_builder import VclBuilder


# ── Merged from tests/provision/test_vcl_builder.py ──
class TestLineMode:
    def test_join_lines_static(self) -> None:
        assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"


class TestSectionMode:
    def test_join_sections_static_matches_legacy_idiom(self) -> None:
        sections = ["a", "", "  ", "b"]
        assert VclBuilder.join_sections(sections) == "\n\n".join(s for s in sections if s.strip())


def test_remove_custom_fields_strips_cmcd_fields_only():
    cfg = {
        "log_fields": {
            "custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True),
        }
    }
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names

    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names)
    assert "mine" in names


def test_remove_custom_fields_tolerates_missing_blocks():
    for cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(cfg)  # must not raise


class TestAnalyticsLogPath:
    def test_fixed_root(self):
        assert analytics_log_path() == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestRumLogPath:
    def test_fixed_root(self):
        assert rum_log_path() == "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert minute_list_prefix(dt) == "raw/request/year=2026/month=08/day=27/hour=16/minute=53/"


class TestRumMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert rum_minute_list_prefix(dt) == "raw/rum/year=2026/month=08/day=27/hour=16/minute=53/"


class TestProducersUseCanonicalLayout:
    def test_declarative_generator_matches(self):
        import backend.provision.declarative.generators as gen

        src = inspect.getsource(gen)
        assert "raw/%Y-%m-%d" not in src, "declarative generator must not use the dash layout"
        assert "log_paths" in src, "declarative generator must build paths via log_paths"

    def test_fastly_api_has_no_dash_layout_literal(self):
        import backend.provision.fastly_api as fa

        src = inspect.getsource(fa)
        assert "raw/%Y-%m-%d" not in src, "add_logging_endpoint must use log_paths, not the dash layout"

    def test_services_router_has_no_dash_layout_literal(self):
        import backend.routers.services.core as sc

        src = inspect.getsource(sc)
        assert "raw/%Y-%m-%d" not in src, "logging-settings update must use log_paths, not the dash layout"

    def test_layout_constant_matches_live_bucket_shape(self):
        assert ANALYTICS_LOG_LAYOUT == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def _minimal_rum_state(*, faro_version: str | None) -> FeatureState:
    return FeatureState(
        service_id="svc_test",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=False,
        rum_enabled=True,
        faro_version=faro_version,
    )


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_without_faro():
    state = _minimal_rum_state(faro_version=None)
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_with_faro():
    state = _minimal_rum_state(faro_version="2.9.0")
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv


# ── Merged from tests/provision/test_draft_version.py ──
SID = "svc"


class _Fastly:
    def __init__(self, *, validate_ok=True, rollback_raises=False):
        self.calls: list[tuple[str, str]] = []
        self.validate_ok = validate_ok
        self.rollback_raises = rollback_raises

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        if path.endswith("/clone"):
            return {"number": 9}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad"]}
        if self.rollback_raises and path == f"/service/{SID}/version/5/activate":
            raise RuntimeError("rollback failed")
        return {}


def test_success_call_order_with_comment_and_status_messages():
    fake = _Fastly()
    msgs: list[str] = []
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", status_cb=msgs.append) as new_ver:
        assert new_ver == 9
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("PUT", f"/service/{SID}/version/9"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]
    assert msgs == ["Validating draft version 9...", "Activating version 9..."]


def test_no_comment_skips_the_comment_put():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake):
        pass
    assert ("PUT", f"/service/{SID}/version/9") not in fake.calls


def test_body_failure_rolls_back_to_base_and_never_validates():
    fake = _Fastly()
    with pytest.raises(ValueError, match="mutation boom"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("mutation boom")
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")
    assert not any(p.endswith("/validate") for _, p in fake.calls)


def test_validation_failure_raises_and_rolls_back_without_activating_draft():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake):
            pass
    assert ("PUT", f"/service/{SID}/version/9/activate") not in fake.calls
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_rollback_disabled_makes_no_rollback_call():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake, rollback=False):
            pass
    assert ("PUT", f"/service/{SID}/version/5/activate") not in fake.calls


def test_failed_rollback_does_not_mask_the_original_error():
    fake = _Fastly(rollback_raises=True)
    with pytest.raises(ValueError, match="original"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("original")


def test_comment_before_activate_puts_comment_between_validate_and_activate():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", comment_before_activate=True):
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]


def test_validate_false_skips_validation_entirely():
    fake = _Fastly(validate_ok=False)
    with draft_version(SID, "t", base_version=5, call=fake, validate=False):
        pass
    assert not any(p.endswith("/validate") for _, p in fake.calls)
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/9/activate")


def test_expect_empty_activate_applies_to_the_draft_activation_only():
    seen: list[tuple[str, dict]] = []

    def call(method, path, body=None, **kwargs):
        seen.append((path, kwargs))
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call, expect_empty_activate=True):
        pass
    activate = [kw for p, kw in seen if p.endswith("/version/9/activate")]
    assert activate == [{"token": "t", "expect_empty": True}]


def test_expect_empty_not_sent_by_default():
    seen: list[dict] = []

    def call(method, path, body=None, **kwargs):
        if path.endswith("/activate"):
            seen.append(kwargs)
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call):
        pass
    assert seen == [{"token": "t"}]


def test_custom_status_messages_replace_the_defaults():
    msgs: list[str] = []
    with draft_version(
        SID,
        "t",
        base_version=5,
        call=_Fastly(),
        status_cb=msgs.append,
        validate_msg="V{ver}",
        activate_msg="A{ver}",
    ):
        pass
    assert msgs == ["V9", "A9"]


def test_on_failure_runs_before_the_rollback_call():
    fake = _Fastly()
    seen_at: list[int] = []
    with pytest.raises(ValueError, match="boom"):
        with draft_version(
            SID,
            "t",
            base_version=5,
            call=fake,
            on_failure=lambda exc: seen_at.append(len(fake.calls)),
        ):
            raise ValueError("boom")
    assert seen_at == [1]
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_failed_rollback_is_logged_at_warning(caplog):
    import logging

    fake = _Fastly(rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(ValueError, match="original"):
            with draft_version(SID, "t", base_version=5, call=fake):
                raise ValueError("original")
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


def test_rollback_to_version_logs_and_swallows_runtime_error(caplog):
    import logging

    from backend.provision.draft_version import rollback_to_version

    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        rollback_to_version(SID, "t", 5, call=_Fastly(rollback_raises=True))
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


# ── Merged from tests/provision/test_sharing_domain.py ──
def test_deploy_remote_frontend_success():
    """deploy_remote_frontend issues the correct sequential calls via the shared fastly() client."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    # Sequential parsed-JSON responses returned by the shared client.
    responses = [
        {"id": "service123", "name": service_name},  # Create Service
        {"number": 1, "service_id": "service123"},  # Verify Draft Version
        {"name": domain_name},  # Attach Domain
        {"name": "gce_vm_origin"},  # Attach Backend Origin
        {"number": 1, "active": True},  # Activate Version
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        assert token == "fake-token"
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
        )

    assert res == {
        "service_id": "service123",
        "version": 1,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }
    assert calls == [
        ("POST", "/service", {"name": service_name, "type": "vcl"}),
        ("POST", "/service/service123/version", None),
        ("POST", "/service/service123/version/1/domain", {"name": domain_name}),
        (
            "POST",
            "/service/service123/version/1/backend",
            {
                "name": "gce_vm_origin",
                "address": origin_host,
                "port": origin_port,
                "use_ssl": use_ssl,
                "ssl_check_cert": False,
            },
        ),
        ("PUT", "/service/service123/version/1/activate", None),
    ]


@pytest.mark.parametrize(
    "step_fail,client_err,expected_step",
    [
        (0, "HTTP 400 POST /service\n    Name already taken", "Create Service"),
        (1, "HTTP 400 POST /service/service123/version\n    Failed to create version", "Verify Draft Version"),
        (2, "HTTP 422 POST /service/service123/version/1/domain\n    Invalid domain name", "Attach Domain"),
        (3, "HTTP 400 POST /service/service123/version/1/backend\n    Invalid backend config", "Attach Backend Origin"),
        (4, "HTTP 500 PUT /service/service123/version/1/activate\n    Could not activate", "Activate Version"),
    ],
)
def test_deploy_remote_frontend_failures(step_fail, client_err, expected_step):
    """Any failed Fastly call is re-raised with the human-readable step name preserved."""
    token = "fake-token"

    ok_responses = [
        {"id": "service123"},
        {"number": 1},
        {},
        {},
        {"active": True},
    ]
    state = {"i": 0}

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        idx = state["i"]
        state["i"] += 1
        if idx == step_fail:
            raise RuntimeError(client_err)
        return ok_responses[idx]

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token=token,
            )

    msg = str(exc_info.value)
    assert f"Fastly API error during {expected_step}:" in msg
    # The underlying client error (HTTP code + body) is preserved.
    assert client_err in msg


def test_deploy_remote_frontend_missing_keys_in_json():
    """Error raised when service_id is missing from the create-service response."""
    with patch("backend.provision.sharing_domain.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve service ID" in str(exc_info.value)


def test_deploy_remote_frontend_missing_version_number():
    """Error raised when the draft-version response has no number."""
    responses = [{"id": "service123"}, {"service_id": "service123"}]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve version number" in str(exc_info.value)


def test_deploy_remote_frontend_with_override_host_success():
    """override_host adds override_host; SNI/cert fields only when use_ssl is True."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 443
    use_ssl = True
    token = "fake-token"
    override_host = "my-override.example.com"

    responses = [
        {"id": "service123", "name": service_name},
        {"number": 1},
        {"name": domain_name},
        {"name": "gce_vm_origin"},
        {"active": True},
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
            override_host=override_host,
        )

    assert res["service_id"] == "service123"
    backend_call = next(c for c in calls if c[1] == "/service/service123/version/1/backend")
    backend_payload = backend_call[2]
    assert backend_payload["override_host"] == override_host
    assert backend_payload["ssl_sni_hostname"] == override_host
    assert backend_payload["ssl_cert_hostname"] == override_host


def test_delete_remote_frontend_deactivates_then_deletes():
    """delete_remote_frontend deactivates active versions, then deletes the service."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 2, "active": True}, {"number": 1, "active": False}],  # Get Versions
        {},  # Deactivate Version 2
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("PUT", f"/service/{remote_service_id}/version/2/deactivate"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_no_active_versions():
    """No deactivation call is issued when there are no active versions."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 1, "active": False}],  # Get Versions
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_tolerates_404():
    """A 404 during version lookup/delete is swallowed (service already gone)."""

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        raise RuntimeError("Fastly API error during Get Versions: HTTP 404 GET /service/x/version\n    not found")

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        # Should not raise.
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")


def test_delete_remote_frontend_reraises_non_404():
    """A non-404 error during deletion propagates."""
    responses = [[], RuntimeError("HTTP 500 DELETE /service/remote123\n    boom")]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert "500" in str(exc_info.value)
