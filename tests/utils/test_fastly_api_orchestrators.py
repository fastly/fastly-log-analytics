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

from unittest.mock import patch

import pytest

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
    """If the named endpoint isn't on the active version, return
    without cloning. Pinned because cloning an unchanged version is
    expensive (creates new Fastly version + audit row) and the no-op
    avoids it."""

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["OtherEndpoint"]),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # No clone or delete calls — only the list lookups (which we mocked)
    mock_fastly.assert_not_called()


def test_remove_logging_endpoint_clones_version_then_deletes_endpoint_and_snippets():
    """Happy path: clone active → delete endpoint → delete 6 snippets
    → validate → activate. Pinned because the clone-then-mutate flow
    is what preserves the audit trail (every change is its own
    Fastly version)."""
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


# ── ensure_logging_endpoint: already-exists short-circuit ────────────────


def test_ensure_logging_endpoint_returns_active_ver_when_already_present():
    """If the endpoint is already on the active version, return its
    version number WITHOUT cloning. Pinned because re-running a
    completed provision shouldn't bump Fastly version numbers (which
    audit logs treat as material configuration changes)."""

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        ver = fastly_api.ensure_logging_endpoint(
            {
                "logging_service_id": "svc-id",
                "endpoint_name": "MyEndpoint",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "fos_prefix": "",
                "log_period": 60,
            },
            "AK",
            "SK",
            "tok",
        )

    assert ver == 10
    # No clone or POST calls — pure short-circuit
    mock_fastly.assert_not_called()


def test_ensure_logging_endpoint_raises_when_no_active_version():
    """No active version → RuntimeError (the wizard can't add an
    endpoint to a non-existent version). Pinned because the FE
    distinguishes "no active version" (recoverable — activate the
    service) from other failures."""

    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        with pytest.raises(RuntimeError, match="no active version"):
            fastly_api.ensure_logging_endpoint(
                {
                    "logging_service_id": "svc-id",
                    "endpoint_name": "MyEndpoint",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "b",
                    "log_period": 60,
                },
                "AK",
                "SK",
                "tok",
            )


# ── ensure_cdn_service: name-collision refusal ───────────────────────────


def test_ensure_cdn_service_refuses_when_service_with_same_name_exists():
    """If a Fastly service with the CDN name already exists, raise
    RuntimeError (don't create a duplicate). Pinned because creating
    a duplicate would make ``find_service_by_name`` ambiguous in
    later wizard runs and break the import-existing flow."""

    with (
        patch(
            "backend.provision.fastly_api.find_service_by_name",
            return_value={"id": "existing-svc", "name": "MyCDN"},
        ),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        with pytest.raises(RuntimeError, match="already exists"):
            fastly_api.ensure_cdn_service(
                {
                    "cdn_service_name": "MyCDN",
                    "cdn_url": "https://mycdn.global.ssl.fastly.net",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "b",
                    "cdn_secret": "s",
                },
                "AK",
                "SK",
                "tok",
            )

    # No service-creation calls were made
    mock_fastly.assert_not_called()


# ── redeploy_cdn_vcl ────────────────────────────────────────────────────


def test_redeploy_cdn_vcl_clones_active_uploads_vcl_and_activates():
    """Happy path: get_active → clone → comment → upload main VCL
    → validate → activate. Pinned because dropping the clone step
    would mutate the active version (impossible — Fastly rejects
    PUT to active versions)."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 11}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_vcl", return_value="vcl content"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet") as mock_ensure,
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        result = fastly_api.redeploy_cdn_vcl("cdn-id", "tok")

    assert result == 11
    paths = [p for _, p in calls]
    assert any("clone" in p for p in paths)
    assert any("/vcl/main" in p for _, p in calls if _ == "PUT")
    assert any("/validate" in p for p in paths)
    assert any("/activate" in p for _, p in calls if _ == "PUT")
    # Snippet reconcile runs on every redeploy so live services track _CDN_SNIPPETS.
    # See cdn-no-cache-404 (added 2026-05-19 after the negative-cache outage).
    snippet_names_called = [c.args[0] for c in mock_ensure.call_args_list]
    assert "cdn-no-cache-404" in snippet_names_called


def test_redeploy_cdn_vcl_raises_when_no_active_version():
    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        with pytest.raises(RuntimeError, match="no active version"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


def test_redeploy_cdn_vcl_falls_back_to_no_ratelimit_on_rate_limit_error():
    """If validation fails with a rate-limiting keyword (ratecounter /
    penaltybox / ratelimit), re-upload the VCL without rate limiting
    and re-validate. Pinned because accounts without rate-limiting
    features should still get a working CDN — the fallback is the
    only thing preventing a hard failure on smaller plans."""
    validate_calls = []
    upload_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 11}
        if method == "PUT" and "/vcl/main" in path:
            upload_calls.append(body.get("content", "") if isinstance(body, dict) else "")
            return {}
        if "/validate" in path:
            validate_calls.append(True)
            # First validation fails with rate-limit error; second succeeds
            if len(validate_calls) == 1:
                return {"status": "error", "errors": ["ratecounter not available"]}
            return {"status": "ok"}
        return {}

    # load_vcl is called once with rate_limiting=True, then again with rate_limiting=False
    vcl_versions = {True: "vcl_with_ratelimit", False: "vcl_no_ratelimit"}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch(
            "backend.provision.fastly_api.load_vcl", side_effect=lambda rate_limiting=True: vcl_versions[rate_limiting]
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        result = fastly_api.redeploy_cdn_vcl("cdn-id", "tok")

    # Activation succeeded
    assert result == 11
    # Two uploads (with then without rate limiting)
    assert len(upload_calls) == 2
    assert upload_calls[0] == "vcl_with_ratelimit"
    assert upload_calls[1] == "vcl_no_ratelimit"


def test_redeploy_cdn_vcl_raises_on_non_ratelimit_validation_failure():
    """Validation failures NOT in the rate-limit family → raise.
    Pinned because the fallback is narrowly-scoped — other validation
    errors are real config bugs that need surfacing."""

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 11}
        if "/validate" in path:
            return {"status": "error", "errors": ["syntax error in main.vcl"]}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_vcl", return_value="vcl"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
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

    current_ep = {
        "period": 60,
        "path": "/raw/%Y-%m-%d/%H/",
        "format": "current_format",
        "response_condition": "Log Sampling",
    }
    fake_cond = {"statement": "!segmented_caching.is_inner_req"}
    fake_snippets_resp = [
        {"name": "Fastly Log Analysis Capture", "content": "recv_content"},
        {"name": "Fastly Log Analysis Miss", "content": "miss_content"},
        {"name": "Fastly Log Analysis Pass", "content": "pass_content"},
    ]

    def fake_fastly(method, path, body=None, **kwargs):
        if "/logging/s3/" in path and method == "GET":
            return current_ep
        if path.endswith("/snippet"):
            return fake_snippets_resp
        return {}

    fake_snippets = {
        "recv": "recv_content",
        "miss": "miss_content",
        "pass": "pass_content",
    }

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_log_format", return_value="current_format"),
        patch("backend.provision.fastly_api.list_vcl_snippets", return_value=set()),
        patch("backend.provision.fastly_api.generate_capture_vcl", return_value=fake_snippets),
        patch("backend.provision.fastly_api.find_condition", return_value=fake_cond),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
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


def test_update_logging_endpoint_404_on_endpoint_lookup_raises_friendly_error():
    """If the endpoint name doesn't exist on the active version, raise
    a friendly "Logging endpoint not found" error. Pinned because the
    FE renders this string verbatim in the error toast — losing it
    would dump the opaque "HTTP 404" message."""

    def fake_fastly(method, path, **kwargs):
        if "/logging/s3/" in path:
            raise RuntimeError("HTTP 404: not found")
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
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
    assert "not found" in str(exc).lower()


def test_update_logging_endpoint_raises_when_no_active_version():
    """No active version → friendly RuntimeError. Pinned because this
    is the "Fastly service is brand-new / inactive" recovery signal
    that the FE keys on to render the "activate first" CTA."""

    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        events, exc = _drain(gen)

    assert isinstance(exc, RuntimeError)
    assert "no active version" in str(exc).lower()


def test_update_logging_endpoint_refreshes_rate_limiting_flag():
    """A logging-settings update re-probes the account entitlement and persists
    the refreshed flag (so a later Update-CDN deploys correctly), emitting an
    informational status event. It does NOT redeploy the CDN itself."""
    saved = []
    service_cfg = {"provisioning": {"rate_limiting": True}, "log_fields": {"groups": ["A"]}}
    with (
        patch("backend.config.load_config", return_value=service_cfg),
        patch("backend.config.save_config", side_effect=lambda sid, c: saved.append((sid, c))),
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
    assert saved and saved[0][0] == "svc"
    assert saved[0][1]["provisioning"]["rate_limiting"] is False
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


def test_ensure_cdn_service_emits_status_callbacks_for_each_step():
    """The happy path emits a status callback for each major step
    (creating, adding domain, configuring dicts, etc). Pinned because
    the wizard's progress UI renders these messages — losing them
    would leave the user staring at a blank progress bar."""
    statuses = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "new-svc-id"}
        if "dictionary" in path and method == "POST" and "/item" not in path:
            return {"id": "dict-id"}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="vcl content"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        result = fastly_api.ensure_cdn_service(
            {
                "cdn_service_name": "MyCDN",
                "cdn_url": "https://mycdn.global.ssl.fastly.net",
                "fos_region": "us-east-1",
                "fos_bucket_name": "test-bucket",
                "cdn_secret": "secret-value",
                "logging_service_id": "log-svc",
            },
            "AK",
            "SK",
            "tok",
            status_cb=statuses.append,
        )

    # rate_limiting is None here: the probe hits the fake fastly() which returns
    # {} for the logging service's /version, so detection is inconclusive.
    assert result == {"id": "new-svc-id", "name": "MyCDN", "rate_limiting": None}
    # Each of these phases must emit a status update
    joined = " ".join(statuses)
    assert "Creating CDN service" in joined
    assert "domain" in joined.lower()
    assert "backend" in joined.lower()
    assert "dictionary" in joined.lower()
    assert "VCL" in joined or "vcl" in joined
    assert "Activating" in joined


def test_ensure_cdn_service_writes_all_four_fos_credential_dict_items():
    """The fos_credentials dictionary gets access_key + secret_key +
    bucket + region items. Pinned because losing any of these would
    break the CDN's authenticated FOS reads at runtime — and the
    error wouldn't surface until the first dashboard query."""
    item_writes = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "svc-id"}
        if "dictionary" in path and "/item" in path and method == "POST":
            item_writes.append(body)
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "dict-id"}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="vcl"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_cdn_service(
            {
                "cdn_service_name": "X",
                "cdn_url": "https://x.example",
                "fos_region": "us-east-1",
                "fos_bucket_name": "my-bucket",
                "cdn_secret": "my-secret",
            },
            "ACCESS",
            "SECRET",
            "tok",
        )

    # 4 fos_credentials items + 1 cdn_auth item = 5 total
    keys_written = {w["item_key"]: w["item_value"] for w in item_writes}
    assert keys_written["access_key"] == "ACCESS"
    assert keys_written["secret_key"] == "SECRET"
    assert keys_written["bucket"] == "my-bucket"
    assert keys_written["region"] == "us-east-1"
    assert keys_written["secret"] == "my-secret"  # cdn_auth


def test_ensure_cdn_service_uses_default_shield_pop_when_none_configured():
    """When cfg has no `cdn_shield` key, the backend uses the
    region's default from SHIELD_MAP. Pinned because losing this
    would create CDN services with no shield POP, doubling FOS
    egress on cold reads."""
    backend_payloads = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "svc-id"}
        if path.endswith("/backend") and method == "POST":
            backend_payloads.append(body)
            return {}
        if "dictionary" in path and "/item" in path and method == "POST":
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "d"}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="v"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_cdn_service(
            {
                "cdn_service_name": "X",
                "cdn_url": "https://x.example",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "cdn_secret": "s",
            },
            "AK",
            "SK",
            "tok",
        )

    assert len(backend_payloads) == 1
    # A shield POP should be set (not absent) for us-east-1
    assert "shield" in backend_payloads[0]


def test_ensure_cdn_service_omits_shield_when_explicitly_none():
    """`cdn_shield="none"` (case-insensitive) → backend payload
    OMITS the `shield` key. Pinned because customers in unsupported
    regions explicitly opt out via "none" and forcing a shield
    would 400 the Fastly backend-create call."""
    backend_payloads = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "s"}
        if path.endswith("/backend") and method == "POST":
            backend_payloads.append(body)
            return {}
        if "dictionary" in path and "/item" in path and method == "POST":
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "d"}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="v"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_cdn_service(
            {
                "cdn_service_name": "X",
                "cdn_url": "https://x.example",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "cdn_secret": "s",
                "cdn_shield": "none",
            },
            "AK",
            "SK",
            "tok",
        )

    assert len(backend_payloads) == 1
    assert "shield" not in backend_payloads[0]


def test_ensure_cdn_service_falls_back_to_no_ratelimit_on_validation_failure():
    """If initial validation fails with a rate-limit keyword,
    re-upload VCL without rate limiting + re-validate. Pinned
    because customers on smaller plans depend on this fallback —
    losing it would hard-fail their initial provision."""
    validate_calls = []
    vcl_uploads = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "s"}
        if path.endswith("/vcl") and method == "POST":
            vcl_uploads.append(("POST", body))
            return {}
        if path.endswith("/vcl/main") and method == "PUT":
            vcl_uploads.append(("PUT", body))
            return {}
        if "dictionary" in path and "/item" in path and method == "POST":
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "d"}
        if "/validate" in path:
            validate_calls.append(True)
            # First fails with rate-limit error; second succeeds
            if len(validate_calls) == 1:
                return {"status": "error", "errors": ["penaltybox not available"]}
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch(
            "backend.provision.fastly_api.load_vcl",
            side_effect=lambda rate_limiting=True: f"vcl_rl={rate_limiting}",
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        result = fastly_api.ensure_cdn_service(
            {
                "cdn_service_name": "X",
                "cdn_url": "https://x.example",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "cdn_secret": "s",
            },
            "AK",
            "SK",
            "tok",
        )

    # Service created successfully despite rate-limit fallback
    assert result["id"] == "s"
    # POST then PUT — initial upload with ratelimit, then fallback PUT without
    assert vcl_uploads[0][1]["content"] == "vcl_rl=True"
    assert vcl_uploads[-1][1]["content"] == "vcl_rl=False"


def test_ensure_cdn_service_raises_on_non_ratelimit_validation_failure():
    """Validation failures NOT in the rate-limit family → RuntimeError.
    Pinned because the fallback is narrowly-scoped; other errors are
    real config bugs that must surface."""

    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "s"}
        if "dictionary" in path and "/item" in path and method == "POST":
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "d"}
        if "/validate" in path:
            return {"status": "error", "errors": ["syntax error in main.vcl"]}
        return {}

    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="v"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fastly_api.ensure_cdn_service(
                {
                    "cdn_service_name": "X",
                    "cdn_url": "https://x.example",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "b",
                    "cdn_secret": "s",
                },
                "AK",
                "SK",
                "tok",
            )


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


# ── ensure_cdn_service: proactive rate-limit detection drives the upload ──


def _cdn_cfg() -> dict:
    return {
        "cdn_service_name": "X",
        "cdn_url": "https://x.example",
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "cdn_secret": "s",
        "logging_service_id": "log-svc",
    }


def _cdn_fake_fastly_factory(uploads):
    def fake_fastly(method, path, body=None, **kwargs):
        if path == "/service":
            return {"id": "s"}
        if path.endswith("/vcl") and method == "POST":
            uploads.append(("POST", body))
            return {}
        if path.endswith("/vcl/main") and method == "PUT":
            uploads.append(("PUT", body))
            return {}
        if "dictionary" in path and "/item" in path and method == "POST":
            return {}
        if "dictionary" in path and method == "POST":
            return {"id": "d"}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    return fake_fastly


def test_ensure_cdn_service_deploys_without_ratelimit_when_account_lacks_it():
    """Proactive detection False → upload the no-rate-limit VCL on the FIRST try
    (no reactive validate round-trip) and report rate_limiting=False so the
    orchestrator can persist it."""
    uploads = []
    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=False),
        patch(
            "backend.provision.fastly_api.load_vcl",
            side_effect=lambda rate_limiting=True: f"vcl_rl={rate_limiting}",
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=_cdn_fake_fastly_factory(uploads)),
    ):
        result = fastly_api.ensure_cdn_service(_cdn_cfg(), "AK", "SK", "tok")

    assert result["rate_limiting"] is False
    # Single POST upload, no rate limiting, and NO fallback PUT round-trip.
    assert uploads == [("POST", {"name": "main", "content": "vcl_rl=False", "main": True})]


def test_ensure_cdn_service_deploys_with_ratelimit_when_account_has_it():
    """Proactive detection True → upload the rate-limit VCL; report True."""
    uploads = []
    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True),
        patch(
            "backend.provision.fastly_api.load_vcl",
            side_effect=lambda rate_limiting=True: f"vcl_rl={rate_limiting}",
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=_cdn_fake_fastly_factory(uploads)),
    ):
        result = fastly_api.ensure_cdn_service(_cdn_cfg(), "AK", "SK", "tok")

    assert result["rate_limiting"] is True
    assert uploads == [("POST", {"name": "main", "content": "vcl_rl=True", "main": True})]


def test_ensure_cdn_service_defaults_to_ratelimit_when_detection_inconclusive():
    """Detection None (unknown) → optimistically upload WITH rate limiting and
    report None so the orchestrator leaves the default-True read path intact;
    the reactive validate fallback remains the backstop."""
    uploads = []
    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch(
            "backend.provision.fastly_api.load_vcl",
            side_effect=lambda rate_limiting=True: f"vcl_rl={rate_limiting}",
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=_cdn_fake_fastly_factory(uploads)),
    ):
        result = fastly_api.ensure_cdn_service(_cdn_cfg(), "AK", "SK", "tok")

    assert result["rate_limiting"] is None
    assert uploads == [("POST", {"name": "main", "content": "vcl_rl=True", "main": True})]


# ── ensure_logging_endpoint: clone+VCL+activate orchestration ────────────


def test_ensure_logging_endpoint_clones_and_activates_on_happy_path():
    """When the endpoint doesn't exist on the active version, clone
    a new draft, add the endpoint, ensure VCL snippets, validate,
    activate, return new version. Pinned because the clone-then-
    mutate flow is what creates the audit trail (every change is
    its own Fastly version)."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 11}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.load_log_format", return_value="format"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        new_ver = fastly_api.ensure_logging_endpoint(
            {
                "logging_service_id": "svc",
                "endpoint_name": "MyEndpoint",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "fos_prefix": "",
                "log_period": 60,
            },
            "AK",
            "SK",
            "tok",
        )

    assert new_ver == 11
    # Clone happened
    assert any("clone" in p for _, p in calls)
    # Endpoint POST happened
    assert any("/logging/s3" in p for m, p in calls if m == "POST")
    # Activation happened
    assert any("activate" in p for _, p in calls if _[0] == "P" or _ == "PUT")


def test_ensure_logging_endpoint_rolls_back_on_validation_failure():
    """If draft validation fails, re-activate the OLD version (rollback)
    and re-raise. Pinned because losing this would leave the customer
    with a deactivated logging endpoint after a partial deploy."""
    activate_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 11}
        if "/validate" in path:
            return {"status": "error", "errors": ["bad VCL"]}
        if method == "PUT" and path.endswith("/activate"):
            activate_calls.append(path)
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.load_log_format", return_value="format"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.ensure_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "b",
                    "fos_prefix": "",
                    "log_period": 60,
                },
                "AK",
                "SK",
                "tok",
            )

    # Old version 10 was re-activated as rollback
    assert any("/version/10/activate" in p for p in activate_calls)


def test_update_logging_endpoint_clones_and_activates_on_changes_detected():
    """When period changes, the route clones the active version,
    updates the period on the new draft, validates, and activates.
    Pinned because the clone-then-mutate flow preserves the audit
    trail via per-change Fastly versions."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if "/clone" in path:
            return {"number": 11}
        if "/logging/s3/" in path and method == "GET":
            return {"period": 30, "path": "/raw/", "format": "old", "response_condition": None}
        if path.endswith("/snippet"):
            return [{"name": "x", "content": "y"}]
        # The orchestrator fails closed on an unverifiable validate (cb256a2),
        # so the draft must report status=ok before it activates.
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_log_format", return_value="old"),
        patch("backend.provision.fastly_api.list_vcl_snippets", return_value=set()),
        patch(
            "backend.provision.fastly_api.generate_capture_vcl", return_value={"recv": "r", "miss": "m", "pass": "p"}
        ),
        patch(
            "backend.provision.fastly_api.find_condition", return_value={"statement": "!segmented_caching.is_inner_req"}
        ),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
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
    # Clone happened
    assert any("clone" in p for _, p, _ in calls)
    # Update PUT to the new version's endpoint had period in the payload
    update_calls = [(p, b) for m, p, b in calls if m == "PUT" and "/logging/s3/" in p and isinstance(b, dict)]
    assert any(b and b.get("period") == 60 for _, b in update_calls)
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
    deleted_snippets = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 11}
        if "/logging/s3/" in path and method == "GET":
            return {"period": 60, "path": "/raw/", "format": "x", "response_condition": None}
        if path.endswith("/snippet"):
            return []
        if method == "DELETE" and "/snippet/" in path:
            deleted_snippets.append(path)
            return {}
        # Fail-closed validate gate (cb256a2): draft must report status=ok.
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_log_format", return_value="new-format"),
        # list_vcl_snippets returns origin snippets present
        patch(
            "backend.provision.fastly_api.list_vcl_snippets",
            return_value={"Fastly Log Analysis Origin Fetch", "Fastly Log Analysis Capture"},
        ),
        # generate_capture_vcl returns ONLY base snippets (no "fetch" key)
        patch(
            "backend.provision.fastly_api.generate_capture_vcl",
            return_value={"recv": "r", "miss": "m", "pass": "p"},
        ),
        patch("backend.provision.fastly_api.find_condition", return_value={"statement": ""}),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
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
    # All 3 origin snippets were targeted for deletion
    deleted_names = [path.split("/")[-1] for path in deleted_snippets]
    # URL-encoded form: spaces become %20
    assert any("Origin%20Fetch" in n for n in deleted_names)
    assert any("Origin%20Error" in n for n in deleted_names)
    assert any("Origin%20Deliver" in n for n in deleted_names)


def test_update_logging_endpoint_rolls_back_to_old_active_on_exception():
    """If any step after clone raises (validation fail, snippet error),
    re-activate the OLD active version (rollback) + yield error event +
    re-raise. Pinned because losing the rollback would leave the
    customer with no live logging endpoint after a partial update."""
    activate_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 11}
        if "/logging/s3/" in path and method == "GET":
            return {"period": 30, "path": "/raw/", "format": "old", "response_condition": None}
        if path.endswith("/snippet"):
            return []
        if method == "PUT" and path.endswith("/activate"):
            activate_calls.append(path)
            return {}
        if "/validate" in path:
            raise RuntimeError("Validation exploded mid-update")
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.load_log_format", return_value="old"),
        patch("backend.provision.fastly_api.list_vcl_snippets", return_value=set()),
        patch(
            "backend.provision.fastly_api.generate_capture_vcl",
            return_value={"recv": "r", "miss": "m", "pass": "p"},
        ),
        patch("backend.provision.fastly_api.find_condition", return_value={"statement": ""}),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
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
    # Old version 10 was re-activated as rollback
    assert any("/version/10/activate" in p for p in activate_calls)
    # An error event was yielded before the raise
    assert any(e.get("type") == "error" for e in events)


def test_ensure_logging_endpoint_emits_origin_snippets_when_group_l_enabled():
    """When `log_fields.groups` includes "L" (origin metrics), 3
    additional VCL snippets are emitted: Origin Fetch / Origin Error
    / Origin Deliver. Pinned because these capture the ottfb/ottlb
    timing data — without them, origin charts are blank."""
    snippet_names = []

    def fake_ensure_snippet(name, *args, **kwargs):
        snippet_names.append(name)

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet", side_effect=fake_ensure_snippet),
        patch("backend.provision.fastly_api.load_log_format", return_value="format"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_logging_endpoint(
            {
                "logging_service_id": "svc",
                "endpoint_name": "MyEndpoint",
                "fos_region": "us-east-1",
                "fos_bucket_name": "b",
                "fos_prefix": "",
                "log_period": 60,
                "log_fields": {"groups": ["A", "L"], "field_overrides": {}},
            },
            "AK",
            "SK",
            "tok",
        )

    # All 3 origin-capture snippets registered
    assert "Fastly Log Analysis Origin Fetch" in snippet_names
    assert "Fastly Log Analysis Origin Error" in snippet_names
    assert "Fastly Log Analysis Origin Deliver" in snippet_names


@pytest.mark.parametrize("custom_condition", [None, "", 'req.http.X-Foo == "bar"'])
def test_ensure_logging_endpoint_tolerates_none_custom_condition(custom_condition):
    """``ensure_logging_endpoint`` must treat custom_condition None / "" / a real
    value all as valid. Regression: the /execute API passes
    ``custom_condition=None`` explicitly, so ``cfg.get("custom_condition", "")``
    returned None (the "" default only applies to an ABSENT key) and ``.strip()``
    raised ``'NoneType' object has no attribute 'strip'`` at provisioning Step 7."""
    captured = {}

    def fake_fastly(method, path, body=None, **kwargs):
        if path.endswith("/clone"):
            return {"number": 6}
        if path.endswith("/validate"):
            return {"status": "ok"}
        return {}

    def fake_ensure_condition(name, stmt, ctype, sid, ver, token):
        captured["stmt"] = stmt

    cfg = {
        "logging_service_id": "svc",
        "endpoint_name": "EP",
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "log_period": "1 minute",
        "sample_rate": "100",
        "edge_only": True,
        "custom_condition": custom_condition,  # None must NOT crash
    }
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition", side_effect=fake_ensure_condition),
        patch("backend.provision.fastly_api.load_log_format", return_value="fmt"),
        patch("backend.provision.fastly_api.install_capture_snippets"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        new_ver = fastly_api.ensure_logging_endpoint(cfg, "ak", "sk", "tok")

    assert new_ver == 6
    # The literal string "None" must never leak into the generated VCL condition.
    assert "None" not in captured["stmt"]
    # A real custom_condition is still appended; None / "" contribute nothing.
    if custom_condition:
        assert custom_condition in captured["stmt"]


def test_ensure_cdn_service_reports_created_id_before_later_failure():
    """``ensure_cdn_service`` must hand the new service id to ``on_created`` the
    instant ``POST /service`` succeeds — BEFORE the fallible domain / VCL /
    validation steps. Regression: the orchestrator recorded cdn_service_id only
    AFTER the function returned, so a validation failure mid-way orphaned a CDN
    service the rollback couldn't see, and re-provision then hit 'CDN service
    already exists'."""
    created_ids = []

    def fake_fastly(method, path, body=None, **kwargs):
        if method == "POST" and path == "/service":
            return {"id": "cdn-new-123"}
        if path.endswith("/dictionary"):
            return {"id": "dict-1"}
        return {}

    cfg = {
        "cdn_service_name": "Log Analysis CDN Service for svc",
        "cdn_url": "https://b.global.ssl.fastly.net",
        "fos_region": "us-east-1",
        "cdn_shield": "none",
        "fos_bucket_name": "b",
        "cdn_secret": "s",
        "logging_service_id": "svc",
    }
    with (
        patch("backend.provision.fastly_api.find_service_by_name", return_value=None),
        patch("backend.provision.fastly_api.load_vcl", return_value="vcl"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch(
            "backend.provision.fastly_api._validate_with_ratelimit_fallback",
            return_value={"status": "error", "msg": "boom"},
        ),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fastly_api.ensure_cdn_service(cfg, "ak", "sk", "tok", on_created=created_ids.append)

    # on_created fired with the new id BEFORE the validation step raised, so the
    # orchestrator's state carries cdn_service_id and the rollback can delete it.
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


def test_ensure_logging_endpoint_drops_restart_gate_when_scoring_enabled():
    """End-to-end: with scoring enabled in cfg, ensure_logging_endpoint must
    generate a Log Sampling condition WITHOUT the req.restarts==0 gate so scored
    requests (which log at req.restarts == 1) still match."""
    captured = {}

    def fake_fastly(method, path, body=None, **kwargs):
        if path.endswith("/clone"):
            return {"number": 6}
        if path.endswith("/validate"):
            return {"status": "ok"}
        return {}

    def fake_ensure_condition(name, stmt, ctype, sid, ver, token):
        captured["stmt"] = stmt

    cfg = {
        "logging_service_id": "svc",
        "endpoint_name": "EP",
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "log_period": "1 minute",
        "sample_rate": "100",
        "edge_only": True,
        "custom_condition": "",
        "scoring": {"enabled": True},
    }
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition", side_effect=fake_ensure_condition),
        patch("backend.provision.fastly_api.load_log_format", return_value="fmt"),
        patch("backend.provision.fastly_api.install_capture_snippets"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_logging_endpoint(cfg, "ak", "sk", "tok")

    assert "req.restarts == 0" not in captured["stmt"], captured["stmt"]
    assert "fastly.ff.visits_this_service == 0" in captured["stmt"]


def test_generate_capture_vcl_capture_guard_never_gates_on_restarts():
    """The CAPTURE block must NEVER gate on req.restarts==0 — customer VCL may
    use restarts for application logic (finding 007), and scoring restarts the
    request for the scorer sub-fetch. The SCRUB must STAY first-pass-only so it
    can't re-run post-restart and wipe captured score headers."""
    from backend.provision.fastly_api import generate_capture_vcl

    cfg = {
        "groups": [],
        "custom_fields": [
            {"name": "cf1", "enabled": True, "collection_stage": "edge", "vcl_log_expression": "req.http.host"}
        ],
    }

    for scoring in (True, False):
        recv = generate_capture_vcl(cfg, scoring_enabled=scoring)["recv"]
        lines = recv.splitlines()
        cap_idx = next(i for i, ln in enumerate(lines) if "Capture edge data for logging" in ln)
        capture_guard_line = lines[cap_idx - 1].strip()
        assert capture_guard_line == "if (fastly.ff.visits_this_service == 0) {", (scoring, capture_guard_line)
        # scrub stays first-pass-only
        assert any("if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in ln for ln in lines)


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
