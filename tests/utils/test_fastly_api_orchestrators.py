"""Tests for ``backend.provision.fastly_api`` — orchestration functions.

The pure helpers (``load_log_format``, ``validate_log_format``,
``_validate_log_format_regex``, ``generate_capture_vcl``) are covered
in [test_fastly_api.py](tests/utils/test_fastly_api.py). This file
pins the orchestrators that talk to the Fastly API:

  - ``delete_cdn_service`` — deactivate + delete with 404 idempotency
  - ``remove_logging_endpoint`` — no-op short-circuits + clone-flow + rollback
  - ``ensure_logging_endpoint`` — already-exists short-circuit
  - ``ensure_cdn_service`` — name-collision refusal
  - ``redeploy_cdn_vcl`` — clone + upload + validate + activate, rate-limit fallback
  - ``update_logging_endpoint`` — no-changes short-circuit + happy path SSE

Each orchestrator is mocked at the ``fastly()`` HTTP boundary so the
tests don't actually hit Fastly. The goal is to lock the API-call
sequence + error handling — the actual Fastly endpoints are covered
by integration tests in CI.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision import fastly_api

# ── delete_cdn_service ───────────────────────────────────────────────────


def test_delete_cdn_service_deactivates_active_version_before_deleting():
    """Must deactivate the active version FIRST — Fastly refuses to
    delete a service with an active version. Pinned because losing the
    deactivate step would leave orphan services that accumulate
    minimum-charge billing."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}, {"number": 2, "active": True}]
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")

    # GET /service/svc-id/version
    assert ("GET", "/service/svc-id/version") in calls
    # PUT /service/svc-id/version/2/deactivate (the active one)
    assert ("PUT", "/service/svc-id/version/2/deactivate") in calls
    # DELETE /service/svc-id
    assert ("DELETE", "/service/svc-id") in calls


def test_delete_cdn_service_returns_silently_when_404_on_list():
    """A 404 on the version-list call (service already deleted) is
    a no-op success. Pinned because re-running teardown after a
    successful first run shouldn't fail."""
    call_count = [0]

    def fake_fastly(method, path, **kwargs):
        call_count[0] += 1
        if method == "GET" and "/version" in path:
            raise RuntimeError("HTTP 404: service not found")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        # Should not raise
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")

    # The version-list call happened, then we early-returned (no DELETE)
    assert call_count[0] == 1


def test_delete_cdn_service_swallows_404_on_delete_call():
    """A 404 on the DELETE call (service deleted between listing
    and deletion) → silent success. Pinned because the
    list+delete is non-atomic and another teardown process might
    have raced ahead."""

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        if method == "DELETE":
            raise RuntimeError("HTTP 404: not found")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        # Should not raise
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")


def test_delete_cdn_service_reraises_non_404_delete_error():
    """A 5xx or other non-404 from DELETE → propagate. Pinned because
    a 502/503 is transient and the cron should retry — silently
    swallowing would leave the service orphaned."""

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        if method == "DELETE":
            raise RuntimeError("HTTP 502: bad gateway")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError, match="502"):
            fastly_api.delete_cdn_service("svc-id", "MyService", "tok")


def test_delete_cdn_service_emits_status_callback_events():
    """Status callback receives a "Deleting..." message + completion
    message. Pinned because the SSE consumer in the wizard reads
    these status events to update the progress UI."""
    statuses = []

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok", status_cb=statuses.append)

    assert any("Deleting" in s for s in statuses)
    assert any("deleted" in s.lower() for s in statuses)


# ── remove_logging_endpoint ───────────────────────────────────────────────


def test_remove_logging_endpoint_no_op_when_no_active_version():
    """No active version → warn and return (no API mutations). Pinned
    because customers sometimes delete the Fastly service before the
    wizard does its cleanup — losing the no-op would 500 the teardown."""

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # No mutation calls
    mock_fastly.assert_not_called()


def test_remove_logging_endpoint_no_op_when_endpoint_already_absent():
    """If the named endpoint isn't on the active version, we still clone and
    clean up any leftover snippets, backends, dictionaries, or conditions,
    but we do NOT call DELETE on the absent endpoint itself."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["OtherEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # Cloned and validated, but no DELETE call for MyEndpoint
    paths = [p for _, p in calls]
    assert any("clone" in p for p in paths)
    assert not any("logging/s3/MyEndpoint" in p for _, p in calls if _ == "DELETE")
    assert any("validate" in p for p in paths)


def test_remove_logging_endpoint_clones_version_then_deletes_endpoint_and_snippets():
    """Happy path: clone active → delete endpoint → delete snippets
    → validate → activate. Pinned because the clone-then-mutate flow
    is what preserves the audit trail (every change is its own
    Fastly version).

    Snippet removal ENUMERATES the draft and deletes what it owns (it used to
    blind-DELETE a hardcoded name list), so the fake must serve a snippet list.
    """
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": "Fastly Log Analytics - vcl_recv"}, {"name": "Fastly Log Analytics - vcl_deliver"}]
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # Clone, then delete endpoint, then delete snippets, validate, activate
    paths = [p for _, p in calls]
    assert any("clone" in p for p in paths)
    assert any("logging/s3/MyEndpoint" in p for _, p in calls if _ == "DELETE")
    assert any("snippet/" in p for _, p in calls if _ == "DELETE")
    assert any("validate" in p for p in paths)
    assert any("activate" in p for _, p in calls if _ == "PUT")


def test_remove_logging_endpoint_swallows_404_on_snippet_delete():
    """A 404 when deleting a snippet (already removed by a previous
    teardown attempt) → continue, don't fail. Pinned because a half-
    completed teardown that left some snippets behind shouldn't
    block a re-run from completing the rest."""

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "DELETE" and "snippet/" in path:
            raise RuntimeError("HTTP 404: snippet not found")
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        # Should NOT raise despite the 404s on snippet deletes
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")


def test_remove_logging_endpoint_rolls_back_on_validation_failure():
    """If validation of the draft fails, the old active version is
    re-activated. Pinned because losing the rollback would leave the
    customer with a deactivated logging endpoint (no logs flowing)."""
    activate_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "error", "errors": ["bad vcl"]}
        if method == "PUT" and path.endswith("/activate"):
            activate_calls.append(path)
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # The OLD active version (5) was re-activated as rollback
    assert any("/version/5/activate" in p for p in activate_calls)


# ── ensure_cdn_service: input validation ──────────────────────────────────


def test_ensure_cdn_service_missing_logging_service_id():
    """Verify ValueError is raised if logging_service_id is missing from cfg."""
    with pytest.raises(ValueError, match="logging_service_id or service_id must be provided"):
        fastly_api.ensure_cdn_service({}, "AK", "SK", "tok")


# ── redeploy_cdn_vcl ────────────────────────────────────────────────────


def _reconciler_writing_cdn_id(cdn_id: str, *, then_raise: Exception | None = None, version: int = 1) -> MagicMock:
    """Stand-in for ``reconcile_cdn_service_state`` that persists the CDN id the way the real one does."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    def _reconcile(logging_service_id, token, status_cb=None, activate=True):
        svcconfig.mutate_config(logging_service_id, lambda c: c.setdefault("fos_proxy", {}).update(service_id=cdn_id))
        if then_raise is not None:
            raise then_raise
        return ReconciliationResult(service_id=cdn_id, activated_version=version)

    return MagicMock(side_effect=_reconcile)


def test_redeploy_cdn_vcl_success():
    """Verify redeploy_cdn_vcl maps cdn_service_id, writes rate_limiting to disk, and runs reconciler."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    mock_config = {"service_id": "service-id", "cdn_service_id": "cdn-id", "fos_proxy": {}}
    svcconfig.save_config("service-id", dict(mock_config))

    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile,
    ):
        mock_reconcile.return_value = ReconciliationResult(
            service_id="cdn-id", activated_version=12, changes_applied=None
        )

        result = fastly_api.redeploy_cdn_vcl("cdn-id", "tok", rate_limiting=False)

        assert result == 12
        assert svcconfig.load_config("service-id")["fos_proxy"]["rate_limiting_enabled"] is False
        mock_reconcile.assert_called_with(
            logging_service_id="service-id",
            token="tok",
            status_cb=None,
            activate=True,
        )


def test_redeploy_cdn_vcl_raises_when_no_mapping():
    """Verify redeploy_cdn_vcl raises ValueError when cdn_service_id cannot be mapped."""
    with patch("backend.config.list_configs", return_value=[]):
        with pytest.raises(ValueError, match="Could not map cdn_service_id"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


def test_redeploy_cdn_vcl_propagates_reconciler_error():
    """Verify redeploy_cdn_vcl propagates exceptions from the reconciler."""
    mock_config = {"service_id": "service-id", "cdn_service_id": "cdn-id", "fos_proxy": {}}
    svcconfig.save_config("service-id", dict(mock_config))
    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            side_effect=RuntimeError("reconciler failure"),
        ),
    ):
        with pytest.raises(RuntimeError, match="reconciler failure"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


# ── update_logging_endpoint (SSE generator) ──────────────────────────────


def _drain(gen):
    """Consume an SSE generator into (events, exception_or_None)."""
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def test_update_logging_endpoint_short_circuits_when_no_changes():
    """If period, path, sample_rate, edge_only, custom_condition, and
    format all match the current endpoint state, emit a "done"
    event with ``changed=False`` and NO clone. Pinned because
    cloning on every settings-page save would bump version numbers
    unnecessarily.

    Note: the orchestrator always rebuilds the response-condition
    statement with at minimum ``!segmented_caching.is_inner_req``,
    so for the no-change short-circuit to fire, the *current*
    condition statement must already equal that prefix."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc-id", activated_version=10, changes_applied=None),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc-id",
                    "endpoint_name": "MyEndpoint",
                    "sample_rate": 100,
                    "edge_only": False,
                    "log_period": 60,
                    "fos_path": "/raw/%Y-%m-%d/%H/",
                    "update_format": False,
                },
                "tok",
            )
        )

    assert exc is None
    # Final event is done + changed=False
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is False
    assert done["version"] == 10


def _run_update_logging_endpoint(cfg_in, stored_cfg):
    """Drive update_logging_endpoint and return the config it persisted."""
    import json

    from backend import config as svcconfig
    from backend.provision.declarative.reconciler import ReconciliationResult

    svcconfig.save_config("svc-id", dict(stored_cfg))

    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc-id", activated_version=11, changes_applied=["fmt"]),
        ),
    ):
        events, exc = _drain(fastly_api.update_logging_endpoint(cfg_in, "tok"))

    assert exc is None, f"generator raised: {exc}"
    return json.loads(svcconfig.config_path("svc-id").read_text()), events


def test_update_logging_endpoint_preserves_custom_fields_when_incoming_omits_them():
    """REGRESSION: 2026-08-12 production incident — the root cause. This
    orchestrator assigned ``cfg["log_fields"]`` WHOLESALE over the stored
    config with no merge guard (unlike its cli.py and log-fields-set
    siblings). Callers build ``log_fields`` from groups alone, so the assign
    dropped the user's custom_fields AND the system-managed scoring/CMCD
    entries. ``reconcile_vcl_state`` then regenerated the Fastly log format
    from the stripped config, so the CMCD extraction VCL kept running at the
    edge and nothing logged its output — every ``cmcd_*`` column ingested
    empty for a month while the UI still reported CMCD "enabled"."""
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

    stored = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {
            "logging_service_id": "svc-id",
            "endpoint_name": "MyEndpoint",
            "log_period": 60,
            # Built from groups alone — no custom_fields key at all.
            "log_fields": {"schema_version": 2, "preset": "standard", "groups": ["A", "B", "C"]},
        },
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    assert "my_custom" in saved_names, "user custom_field was stripped by the wholesale assign"
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} was stripped by the wholesale assign"
    # The incoming group change must still land.
    assert saved_cfg["log_fields"]["groups"] == ["A", "B", "C"]


def test_update_logging_endpoint_reasserts_cmcd_without_a_state_transition():
    """The reconciler must be keyed on CMCD STATE, not on a transition. A
    reconcile that says nothing about CMCD (no ``cmcd_enabled`` in the
    request) previously left the fields however it found them; with CMCD
    enabled it must re-assert the canonical 14 from code."""
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

    stored = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        # Already-stripped config — the broken state the affected service was in.
        "log_fields": {"schema_version": 2, "groups": ["A", "B"], "field_overrides": {}},
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} not re-asserted by a plain reconcile"


def test_update_logging_endpoint_strips_cmcd_when_disabled():
    """Disable converges too — a reconcile with CMCD off removes stale entries."""
    stored = {
        "logging_enabled": True,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    assert saved_names == {"my_custom"}


def test_update_logging_endpoint_404_on_endpoint_lookup_raises_friendly_error():
    """If the endpoint name doesn't exist on the active version, raise
    a friendly error. Pinned because the FE renders error messages
    in the error toast — losing it would dump the opaque message."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc-id",
                error="Logging endpoint 'GhostEndpoint' not found on service",
            ),
        ),
    ):
        gen = fastly_api.update_logging_endpoint(
            {
                "logging_service_id": "svc-id",
                "endpoint_name": "GhostEndpoint",
                "log_period": 60,
            },
            "tok",
        )
        events, exc = _drain(gen)

    assert isinstance(exc, RuntimeError)
    assert "Declarative reconciliation failed" in str(exc)


def test_update_logging_endpoint_raises_when_no_active_version():
    """No active version → friendly RuntimeError. Pinned because this
    is the "Fastly service is brand-new / inactive" recovery signal
    that the FE keys on to render the "activate first" CTA."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc", error="No active version found for service 'svc'"),
        ),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        events, exc = _drain(gen)

    assert isinstance(exc, RuntimeError)


def test_update_logging_endpoint_refreshes_rate_limiting_flag():
    """A logging-settings update re-probes the account entitlement and persists
    the refreshed flag (so a later Update-CDN deploys correctly), emitting an
    informational status event. It does NOT redeploy the CDN itself."""
    import json

    from backend import config as svcconfig

    svcconfig.save_config("svc", {"provisioning": {"rate_limiting": True}, "log_fields": {"groups": ["A"]}})
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=False),
        # Short-circuit the rest of the generator right after the probe block.
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        events, exc = _drain(gen)

    # Flag refreshed + persisted under provisioning.rate_limiting.
    assert json.loads(svcconfig.config_path("svc").read_text())["provisioning"]["rate_limiting"] is False
    # An informational status event was surfaced...
    assert any(e.get("type") == "status" and "rate limiting" in e.get("message", "").lower() for e in events)
    # ...and the rest of the update still ran (and raised on no active version).
    assert isinstance(exc, RuntimeError)


def test_update_logging_endpoint_skips_flag_refresh_when_no_service_cfg():
    """No on-disk service config (load_config → None) → the probe block is
    skipped entirely (no save_config, no probe call)."""
    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.save_config", side_effect=AssertionError("must not save")),
        patch(
            "backend.provision.fastly_api.account_has_rate_limiting",
            side_effect=AssertionError("must not probe"),
        ),
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        _events, exc = _drain(gen)
    assert isinstance(exc, RuntimeError)  # still raises on no active version


# ── ensure_cdn_service: happy-path orchestration ──────────────────────────


def test_ensure_cdn_service_success():
    """Verify ensure_cdn_service merges configs, writes them to disk, calls reconciler, and detects rate limiting."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
        "cdn_service_name": "MyCDN",
        "cdn_url": "https://mycdn.example.com",
    }
    statuses = []
    mock_reconcile = _reconciler_writing_cdn_id("cdn-new-123")

    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state", mock_reconcile),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True) as mock_detect,
    ):
        result = fastly_api.ensure_cdn_service(
            input_cfg,
            "AK",
            "SK",
            "tok",
            status_cb=statuses.append,
        )

        assert result == {
            "id": "cdn-new-123",
            "name": "MyCDN",
            "rate_limiting": True,
        }
        written_json = svcconfig.load_config("log-svc-123")
        assert written_json["fos_access_key_id"] == "AK"
        assert written_json["fos_secret_access_key"] == "SK"
        assert written_json["cdn_service_name"] == "MyCDN"

        mock_reconcile.assert_called_with(
            logging_service_id="log-svc-123",
            token="tok",
            status_cb=statuses.append,
            activate=True,
        )
        mock_detect.assert_called_with("tok", "log-svc-123")


def test_ensure_cdn_service_rate_limiting_scenarios():
    """Verify ensure_cdn_service returns rate_limiting True, False, or None depending on proactive detection."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
    }

    for detection_val in (True, False, None):
        with (
            patch(
                "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
                _reconciler_writing_cdn_id("cdn-new-123"),
            ),
            patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=detection_val),
        ):
            result = fastly_api.ensure_cdn_service(input_cfg, "AK", "SK", "tok")
            assert result["rate_limiting"] is detection_val


def test_ensure_cdn_service_on_created_callback():
    """Verify on_created is called with the newly created cdn service id."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
    }
    created_ids = []

    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123"),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
    ):
        fastly_api.ensure_cdn_service(input_cfg, "AK", "SK", "tok", on_created=created_ids.append)
        assert created_ids == ["cdn-new-123"]


# ── account_has_rate_limiting: proactive account-entitlement probe ───────


def _vcl_with(pragma_line: str) -> str:
    return f"sub vcl_recv {{\n  {pragma_line}\n}}\n"


def test_account_has_rate_limiting_true_when_pragma_present():
    """The account-level ``ratelimit_opt_in true`` pragma in the generated VCL
    means edge rate limiting is entitled → True."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch(
            "backend.provision.fastly_api.get_generated_vcl",
            return_value=_vcl_with("pragma optional_param ratelimit_opt_in true;"),
        ),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is True


def test_account_has_rate_limiting_false_when_pragma_false():
    """``ratelimit_opt_in false`` must read as NOT entitled — the check matches
    the literal ``true`` value, not a loose ``ratelimit_opt_in`` substring."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch(
            "backend.provision.fastly_api.get_generated_vcl",
            return_value=_vcl_with("pragma optional_param ratelimit_opt_in false;"),
        ),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is False


def test_account_has_rate_limiting_false_when_pragma_absent():
    """Generated VCL read but no pragma at all → False (definitive)."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value="sub vcl_recv {}\n"),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is False


def test_account_has_rate_limiting_none_when_no_active_version():
    """No active version (Compute service mid-deploy / never activated) →
    inconclusive None, not False — so we don't wrongly strip rate limiting."""
    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is None


def test_account_has_rate_limiting_none_when_no_generated_vcl():
    """A wasm/Compute service has an active version but no generated VCL
    (get_generated_vcl → None) → inconclusive None."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=25),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=None),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "wasm-svc") is None


def test_account_has_rate_limiting_skips_falsy_ids_and_returns_none_with_no_ids():
    """Empty/None ids are skipped; with nothing conclusive → None."""
    with patch("backend.provision.fastly_api.get_active_version") as m_ver:
        assert fastly_api.account_has_rate_limiting("tok") is None
        assert fastly_api.account_has_rate_limiting("tok", "", None) is None
        m_ver.assert_not_called()


def test_account_has_rate_limiting_falls_through_to_next_vcl_sibling():
    """First id is a wasm service (no generated VCL); detection falls through to
    the second id, which has the pragma → True. This is why callers pass the CDN
    service AND the logging service."""
    gens = {"wasm-svc": None, "vcl-svc": _vcl_with("pragma optional_param ratelimit_opt_in true;")}
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=lambda sid, ver, tok: gens[sid]),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "wasm-svc", "vcl-svc") is True


def test_account_has_rate_limiting_first_conclusive_wins():
    """The first id that yields parseable generated VCL decides — a definitive
    False short-circuits before probing later ids."""
    seen = []

    def fake_gen(sid, ver, tok):
        seen.append(sid)
        return "sub vcl_recv {}\n"  # no pragma → False, conclusive

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=fake_gen),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "first", "second") is False
    assert seen == ["first"]


def test_account_has_rate_limiting_swallows_unexpected_probe_errors():
    """Detection is best-effort: an unexpected (non-RuntimeError) exception on a
    probe drops to the next id rather than breaking a deploy."""
    with (
        patch("backend.provision.fastly_api.get_active_version", side_effect=ValueError("boom")),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is None


def test_update_logging_endpoint_clones_and_activates_on_changes_detected():
    """When period changes, the route clones the active version,
    updates the period on the new draft, validates, and activates.
    Pinned because the clone-then-mutate flow preserves the audit
    trail via per-change Fastly versions."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True, "log_fields": {}}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc",
                activated_version=11,
                changes_applied={"endpoint_update": {"log_period": 60}},
            ),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "log_period": 60,  # different from current 30
                },
                "tok",
            )
        )

    assert exc is None
    # Final done with changed=True
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is True
    assert done["version"] == 11


def test_update_logging_endpoint_removes_origin_snippets_when_group_l_disabled():
    """When the new log_fields config has NO group L (no `fetch`
    snippet in generate_capture_vcl), the route deletes any pre-
    existing Origin Fetch/Error/Deliver snippets. Pinned because
    losing this would leave orphan snippets that consume Fastly
    VCL budget and continue running stale capture logic."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc",
                activated_version=11,
                changes_applied={
                    "snippets_deleted": [
                        "Fastly Log Analytics Origin Fetch",
                        "Fastly Log Analytics Origin Error",
                        "Fastly Log Analytics Origin Deliver",
                    ]
                },
            ),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "update_format": True,
                },
                "tok",
            )
        )

    assert exc is None
    # Final event shows changes were applied
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is True


def test_update_logging_endpoint_rolls_back_to_old_active_on_exception():
    """If any step after clone raises (validation fail, snippet error),
    re-activate the OLD active version (rollback) + yield error event +
    re-raise. Pinned because losing the rollback would leave the
    customer with no live logging endpoint after a partial update."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc", error="Validation exploded mid-update"),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "log_period": 60,  # forces a change
                },
                "tok",
            )
        )

    # The exception was re-raised
    assert isinstance(exc, RuntimeError)


def test_ensure_cdn_service_reports_created_id_before_later_failure():
    """Verify on_created is called with the cdn_service_id if the reconciler failed AFTER writing it to config."""
    cfg = {
        "logging_service_id": "svc",
    }
    created_ids = []

    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123", then_raise=RuntimeError("Validation failed")),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.ensure_cdn_service(cfg, "ak", "sk", "tok", on_created=created_ids.append)

    assert created_ids == ["cdn-new-123"]


def test_log_sampling_edge_clause_never_gates_on_restarts():
    """The Log Sampling edge gate must NEVER include ``req.restarts == 0``,
    regardless of scoring state. Customer VCL may use restarts for application
    logic (404 fallbacks, auth checks) — gating on restarts == 0 silently drops
    restarted requests from the telemetry stream (finding 007)."""
    from backend.provision.fastly_api import _log_sampling_edge_clause

    for scoring in (False, True):
        clause = _log_sampling_edge_clause(scoring)
        assert "req.restarts" not in clause
        assert clause == "fastly.ff.visits_this_service == 0"


def test_generate_capture_vcl_capture_and_scrubs_are_unified():
    """All edge captures, scrubs, custom fields, and request ID generation
    are consolidated in the main first-pass edge block:
    if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    to minimize VCL execution overhead and simplify structure."""
    from backend.provision.fastly_api import generate_capture_vcl

    cfg = {
        "groups": [],
        "custom_fields": [
            {"name": "cf1", "enabled": True, "collection_stage": "edge", "vcl_log_expression": "req.http.host"}
        ],
    }

    for scoring in (True, False):
        recv = generate_capture_vcl(cfg, scoring_enabled=scoring)["recv"]
        assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in recv
        assert "set req.http.x-fos-edge-data:cf1 = req.http.host;" in recv
        assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in recv
        assert "set req.http.x-fos-edge-data:ip = client.ip;" in recv


def test_load_log_format_keeps_standard_fields_when_only_custom_fields_present():
    """A log_fields carrying custom_fields but no groups/preset must still emit
    the standard fields. Regression: enabling scoring on an empty-log_fields
    service added custom_fields, made the dict truthy, defeated the
    standard-preset fallback in load_log_format, and silently dropped
    url/method/edge (and all other group fields) from the log format."""
    from backend.provision.fastly_api import load_log_format

    fmt = load_log_format(
        {
            "custom_fields": [
                {
                    "name": "edge_score",
                    "enabled": True,
                    "collection_stage": "deliver",
                    "vcl_log_expression": "req.http.x-edge-score:score",
                }
            ]
        }
    )
    # Standard group fields survive alongside the custom field.
    assert "req.url" in fmt
    assert "req.method" in fmt
    assert "visits_this_service" in fmt  # the `edge` field
    assert "edge_score" in fmt

    # Empty / None configs still fall back to the standard fields (unchanged).
    assert "req.url" in load_log_format({})
    assert "req.url" in load_log_format(None)


def test_service_has_scoring_pragmas_requires_both_pragmas():
    """service_has_scoring_pragmas returns True only when both
    fix_unsent_body_drain and no_body_if_bereq_is_get_or_head are true in
    generated VCL."""
    both_vcl = (
        "pragma optional_param fix_unsent_body_drain true;\n"
        "pragma optional_param no_body_if_bereq_is_get_or_head true;\n"
    )
    only_drain_vcl = "pragma optional_param fix_unsent_body_drain true;\n"
    only_no_body_vcl = "pragma optional_param no_body_if_bereq_is_get_or_head true;\n"
    false_vcl = (
        "pragma optional_param fix_unsent_body_drain true;\n"
        "pragma optional_param no_body_if_bereq_is_get_or_head false;\n"
    )

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=both_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is True

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=only_drain_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=only_no_body_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=false_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    assert fastly_api.service_has_scoring_pragmas("", "tok") is None
    assert fastly_api.service_has_scoring_pragmas("svc_1", "") is None

    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is None

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=RuntimeError("boom")),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is None
