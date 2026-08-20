from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from backend.main import app


def test_lake_info_success():
    """Verify that lake-info returns table details correctly."""
    with (
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
        patch("backend.core.iceberg.get_table_info") as mock_info,
        patch("backend.core.iceberg.get_snapshot_calendar") as mock_calendar,
        patch("urllib.request.urlopen", side_effect=Exception("Fast path miss")),
        patch("backend.core.duckdb._get_fos_client", side_effect=Exception("Fast path miss")),
    ):
        mock_table = MagicMock()
        mock_init.return_value = mock_table

        mock_info.return_value = {
            "min_timestamp": "2026-05-01T00:00:00Z",
            "max_timestamp": "2026-05-02T00:00:00Z",
            "data_files": 10,
            "size_bytes": 1024,
        }

        mock_calendar.return_value = {
            "2026-05-01": {"data_files": 5, "size_bytes": 512},
            "2026-05-02": {"data_files": 5, "size_bytes": 512},
        }

        client = TestClient(app)
        response = client.post(
            "/api/provision/lake-info",
            json={
                "bucket": "test-bucket",
                "region": "us-east-1",
                "access_key": "ak",
                "secret_key": "sk",
                "prefix": "p/",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["table_exists"] is True
        assert data["info"]["data_files"] == 10
        assert data["range"]["start"] == "2026-05-01T00:00:00Z"


def test_lake_info_not_found():
    """Verify that lake-info returns table_exists=False if table doesn't exist."""
    with (
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
        patch("urllib.request.urlopen", side_effect=Exception("Fast path miss")),
        patch("backend.core.duckdb._get_fos_client", side_effect=Exception("Fast path miss")),
    ):
        mock_init.return_value = None

        client = TestClient(app)
        response = client.post(
            "/api/provision/lake-info",
            json={"bucket": "test-bucket", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["table_exists"] is False
        assert "not found" in data["message"]


def test_lake_info_analyst_location():
    """Verify that lake-info uses the provided metadata location (important for analysts)."""
    with (
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
        patch("backend.core.iceberg.get_table_info") as mock_info,
        patch("backend.core.iceberg.get_snapshot_calendar") as mock_calendar,
        patch("urllib.request.urlopen", side_effect=Exception("Fast path miss")),
        patch("backend.core.duckdb._get_fos_client", side_effect=Exception("Fast path miss")),
    ):
        mock_init.return_value = MagicMock()
        mock_info.return_value = {}
        mock_calendar.return_value = {}

        client = TestClient(app)
        loc = "s3://test-bucket/iceberg/metadata/v1.metadata.json"
        client.post(
            "/api/provision/lake-info",
            json={
                "bucket": "test-bucket",
                "region": "us-east-1",
                "access_key": "ak",
                "secret_key": "sk",
                "iceberg_metadata_location": loc,
            },
        )

        # Check that init_iceberg_table was called with the location in source
        args, kwargs = mock_init.call_args
        src = args[0]
        assert src["iceberg_metadata_location"] == loc


_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "access_level": "read_write",
    "fos_bucket": "bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
}


def test_set_ngwaf_workspace_saves_body_field():
    """PATCH /ngwaf-workspace reads ngwaf_workspace_id from the request body, not query params.

    Security: requires a token. Cfg.fastly_api_key is the canonical
    accepted token (constant-time stored-key match), so the test passes
    that as the ``?token=`` query param.
    """
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="test-stored-key")
    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        client = TestClient(app)
        response = client.patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=test-stored-key",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )

    assert response.status_code == 200, response.text[:500]
    mock_validate.assert_called_once_with("test-stored-key", service_id="svc123")
    data = response.json()
    assert data["ngwaf_workspace_id"] == "workspace-abc"
    assert saved.get("ngwaf_workspace_id") == "workspace-abc"


def test_set_ngwaf_workspace_query_param_is_ignored():
    """Query-param-only call (the old broken admin UI) must NOT save the
    workspace value from a query param — body is required.

    Security: token also required; same accept-stored-key shape as
    above.
    """
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="test-stored-key")
    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        client = TestClient(app)
        # Send workspace_id as query param (wrong) with empty body
        response = client.patch(
            "/api/provision/services/svc123/ngwaf-workspace?workspace_id=workspace-abc&token=test-stored-key",
            json={},
        )

    assert response.status_code == 200, response.text[:500]
    mock_validate.assert_called_once_with("test-stored-key", service_id="svc123")
    # Body was empty so ngwaf_workspace_id should be None/cleared, not "workspace-abc"
    assert saved.get("ngwaf_workspace_id") is None


def test_set_ngwaf_workspace_without_token_rejected_401():
    """Security: no token → 401."""
    with (
        patch("backend.config.load_config", return_value=dict(_FAKE_CFG)),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
    ):
        client = TestClient(app)
        response = client.patch(
            "/api/provision/services/svc123/ngwaf-workspace",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "token_required"


def test_set_ngwaf_workspace_with_wrong_token_rejected_401():
    """Security: token doesn't match stored key AND fails /tokens/self
    validation → 401."""
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="legit-stored-key")
    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
        # Fastly /tokens/self also rejects this fake token
        patch("backend.utils.fastly_auth.fastly", side_effect=RuntimeError("HTTP 401")),
    ):
        client = TestClient(app)
        response = client.patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=attacker-supplied-wrong-token",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert response.status_code == 401


def test_set_ngwaf_workspace_with_read_only_stored_token_rejected_401():
    """Security: Finding 016. Even if the caller-supplied token matches
    the stored fastly_api_key, we must always validate it via /tokens/self.
    If that validation reveals it's a read-only token (missing 'global' scope),
    it must be rejected with 401."""
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="stored-read-only-token")

    # Mock /tokens/self return value representing a read-only token
    read_only_token_data = {
        "id": "tok-id",
        "scope": "global:read",  # Read-only scope, not the required "global"
        "services": [],
        "customer_id": "cust-T",
    }

    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", return_value=read_only_token_data),
    ):
        client = TestClient(app)
        response = client.patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=stored-read-only-token",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "insufficient_scope"


def test_ngwaf_workspaces_with_read_only_stored_token_rejected_401():
    """Security: Finding 016. Even if the token matches the stored api key,
    the NGWAF workspace listing route must validate it via /tokens/self,
    blocking read-only tokens with 401."""
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="stored-read-only-token")
    read_only_token_data = {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"}
    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.utils.fastly_auth.fastly", return_value=read_only_token_data),
    ):
        client = TestClient(app)
        response = client.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc123", "token": "stored-read-only-token"},
        )
    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "insufficient_scope"


# ── /api/provision/services ────────────────────────────────────────────────


def test_provision_list_services_returns_provisioned_flag(tmp_path, monkeypatch):
    """List Fastly services with a ``provisioned`` flag set to True
    for any service that's already in CONFIGS_DIR. Pinned because
    the wizard's "pick a service" step shows existing services as
    "already configured" to avoid double-provisioning."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-existing", {"service_id": "svc-existing"})

    fake_services = [
        {"id": "svc-existing", "name": "Already Done", "type": "vcl"},
        {"id": "svc-new", "name": "Fresh", "type": "vcl"},
        {"id": "svc-wasm", "name": "Compute", "type": "wasm"},  # non-VCL → skipped
    ]

    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", return_value=fake_services),
        # Object Storage gate (added with the enablement pre-check) — assume
        # enabled here so this test stays focused on the provisioned-flag logic.
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=True),
    ):
        resp = c.get("/api/provision/services?token=tok")

    assert resp.status_code == 200
    body = resp.json()
    # Only the two VCL services surface
    assert len(body) == 2
    ids = {s["id"] for s in body}
    assert ids == {"svc-existing", "svc-new"}
    # The existing one is flagged
    existing = next(s for s in body if s["id"] == "svc-existing")
    assert existing["provisioned"] is True
    new = next(s for s in body if s["id"] == "svc-new")
    assert new["provisioned"] is False


def test_provision_list_services_400s_on_fastly_api_error():
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 401")),
    ):
        resp = c.get("/api/provision/services?token=badtoken")

    assert resp.status_code == 400


def test_provision_list_services_requires_token():
    with TestClient(app) as c:
        resp = c.get("/api/provision/services")
    assert resp.status_code == 422  # missing required query param


# ── /api/provision/validate ────────────────────────────────────────────────


def test_provision_validate_400s_on_missing_token():
    with TestClient(app) as c:
        resp = c.post("/api/provision/validate", json={"service_id": "svc"})
    assert resp.status_code == 400
    assert "required" in resp.json()["detail"]["error"]


def test_provision_validate_400s_on_missing_service_id():
    with TestClient(app) as c:
        resp = c.post("/api/provision/validate", json={"token": "tok"})
    assert resp.status_code == 400


def test_provision_validate_returns_defaults_with_safe_service_id():
    """The ``defaults`` dict embeds the service_id into bucket-name +
    cdn-service-name slots, sanitised to alphanumeric+hyphens.
    Pinned because losing the regex would allow special chars to
    end up in FOS bucket names (which only accept DNS-safe
    characters)."""

    def _fastly_spy(method, path, **kwargs):
        if "/tokens/self" in path:
            return {"id": "tok-1", "name": "Test Token", "user_id": "u1"}
        if "/service/" in path:
            return {"name": "My Production CDN"}
        return {}

    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=_fastly_spy),
    ):
        resp = c.post(
            "/api/provision/validate",
            json={"token": "tok", "service_id": "svc.with/specials!"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["service_name"] == "My Production CDN"
    # Service id sanitised — no dots, slashes, or exclamations
    assert "/" not in body["defaults"]["fos_bucket_name"]
    assert "." not in body["defaults"]["fos_bucket_name"]
    assert "!" not in body["defaults"]["fos_bucket_name"]
    # The token-info type detection: user_id present → "user"
    assert body["token_info"]["type"] == "user"


def test_provision_validate_classifies_automation_tokens():
    """Token without ``user_id`` → ``type: "automation"``. Pinned
    because the wizard renders a different help link for each type."""

    def _fastly_spy(method, path, **kwargs):
        if "/tokens/self" in path:
            return {"id": "tok-bot", "name": "Bot Token"}  # no user_id
        if "/service/" in path:
            return {"name": "X"}
        return {}

    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=_fastly_spy),
    ):
        resp = c.post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})

    assert resp.status_code == 200
    assert resp.json()["token_info"]["type"] == "automation"


# ── /api/provision/check-domain ────────────────────────────────────────────


def test_check_domain_rejects_invalid_prefix_format():
    """Prefix must be alphanumeric + hyphens, no leading/trailing
    hyphen. Pinned because invalid prefixes would generate
    unreachable Fastly domains (e.g. ``-foo`` is illegal DNS)."""
    with TestClient(app) as c:
        # Leading hyphen
        resp = c.get("/api/provision/check-domain?prefix=-bad")
        assert resp.status_code == 200
        assert resp.json()["available"] is False
        assert "alphanumeric" in resp.json()["reason"].lower()


def test_check_domain_400s_on_empty_prefix():
    with TestClient(app) as c:
        # FastAPI's Query(...) requires the param to be present, but the route
        # itself also has a defensive check. Test the path where the request
        # arrives but prefix=""
        resp = c.get("/api/provision/check-domain?prefix=")

    # Either path → 400 (FastAPI rejects empty required string OR route's check)
    assert resp.status_code in (400, 422)


def test_check_domain_accepts_valid_prefix():
    """Valid alphanumeric+hyphen prefix → check the domain.
    Pinned because the check function does an actual DNS lookup,
    so we mock it."""
    with (
        TestClient(app) as c,
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(True, None),
        ),
    ):
        resp = c.get("/api/provision/check-domain?prefix=valid-prefix-1")

    assert resp.status_code == 200
    assert resp.json()["available"] is True


def test_check_domain_custom_domain_available():
    """Verify check-domain with is_custom=true passes full domain to _check_domain_available."""
    with (
        TestClient(app) as c,
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(True, None),
        ) as mock_check,
    ):
        resp = c.get("/api/provision/check-domain?prefix=my.custom.domain.com&is_custom=true")

    assert resp.status_code == 200
    assert resp.json()["available"] is True
    mock_check.assert_called_once_with("my.custom.domain.com")


def test_check_domain_custom_domain_taken():
    """Verify check-domain with is_custom=true handles taken domain successfully."""
    with (
        TestClient(app) as c,
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(False, "Domain already registered or in use"),
        ) as mock_check,
    ):
        resp = c.get("/api/provision/check-domain?prefix=my.custom.domain.com&is_custom=true")

    assert resp.status_code == 200
    assert resp.json()["available"] is False
    assert resp.json()["reason"] == "Domain already registered or in use"
    mock_check.assert_called_once_with("my.custom.domain.com")


# ── /api/provision/terraform/preview + /export ─────────────────────────────


def test_terraform_preview_returns_hcl_string():
    """Preview generates the HCL the wizard renders in its
    "review" step. Pinned at the structural level (200 + hcl key)."""
    fake_hcl = 'resource "fastly_service_vcl" "x" {}'
    with (
        TestClient(app) as c,
        patch("backend.utils.terraform_gen.generate_terraform", return_value={"main.tf": fake_hcl}),
    ):
        resp = c.post(
            "/api/provision/terraform/preview",
            json={
                "service_id": "svc-1",
                "fos_bucket": "b",
                "cdn_service_name": "CDN",
                "cdn_url": "https://x.global.ssl.fastly.net",
                "fos_region": "us-east-1",
            },
        )

    # Returns a dict of {filename: hcl_string}
    assert resp.status_code == 200
    body = resp.json()
    assert "main.tf" in body


# ── _check_domain_available pure helper ────────────────────────────────────


def test_check_domain_available_returns_false_when_domain_returns_200():
    """A 200 response from the domain means it's actively serving
    traffic — NOT available. Pinned because the wizard surfaces
    this as "already in use" so the user picks another prefix."""
    from backend.routers.provision import _check_domain_available

    fake_resp = MagicMock()
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=fake_resp):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
    assert ok is False
    assert reason is not None
    assert "in use" in reason.lower()


def test_check_domain_available_returns_true_on_fastly_404_with_marker():
    """Fastly's "domain not added to a service" HTTP 404 body has a
    specific marker string; the helper keys on that marker to confirm
    the domain is registered with Fastly but unassigned (i.e. safe
    for the wizard to claim). Pinned because changes to Fastly's
    error page format would break this detection."""
    import urllib.error
    from io import BytesIO

    from backend.routers.provision import _check_domain_available

    body = b"... Please check that this domain has been added to a service. ..."
    err = urllib.error.HTTPError(url="https://x", code=404, msg="not found", hdrs={}, fp=BytesIO(body))
    with patch("urllib.request.urlopen", side_effect=err):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
    assert ok is True
    assert reason is None


def test_check_domain_available_returns_false_on_other_404():
    """A 404 WITHOUT the Fastly marker → domain belongs to someone
    else (different CDN). Pinned because we must NOT claim domains
    that aren't on Fastly."""
    import urllib.error
    from io import BytesIO

    from backend.routers.provision import _check_domain_available

    err = urllib.error.HTTPError(url="https://x", code=404, msg="not found", hdrs={}, fp=BytesIO(b"some other body"))
    with patch("urllib.request.urlopen", side_effect=err):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
    assert ok is False


def test_check_domain_available_treats_dns_error_as_available():
    """DNS / connection error → domain isn't registered anywhere
    (likely available). Pinned because the wizard advances on
    this signal — losing it would block legitimate domain claims."""
    import urllib.error

    from backend.routers.provision import _check_domain_available

    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("dns failed")):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
    assert ok is True
    assert reason is not None
    assert "dns" in reason.lower() or "connection" in reason.lower()


# ── /api/provision/check-fos ───────────────────────────────────────────────


def test_check_fos_returns_ok_when_list_succeeds():
    """Valid creds → ``{ok: true}``. Pinned because the wizard
    advances on ``ok: true`` (no other shape variant)."""
    fake_client = MagicMock()
    fake_client.list_objects_v2.return_value = {"Contents": []}
    with (
        TestClient(app) as c,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
    ):
        resp = c.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_check_fos_maps_access_denied_to_friendly_message():
    """``AccessDenied`` / ``InvalidAccessKeyId`` / ``SignatureDoesNotMatch``
    all map to the same "check your key/secret" message. Pinned
    because the FE renders the message verbatim — losing the
    friendly text would dump opaque AWS error codes."""
    import botocore.exceptions

    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListObjectsV2"
    )
    with (
        TestClient(app) as c,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
    ):
        resp = c.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "access denied" in body["error"].lower()


def test_check_fos_maps_no_such_bucket_to_friendly_message():
    """``NoSuchBucket`` → "Bucket not found" (so the user re-enters
    the bucket name)."""
    import botocore.exceptions

    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "nope"}}, "ListObjectsV2"
    )
    with (
        TestClient(app) as c,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
    ):
        resp = c.post(
            "/api/provision/check-fos",
            json={"bucket": "wrong-bucket", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert "bucket not found" in body["error"].lower()


def test_check_fos_maps_region_mismatch_to_friendly_message():
    """``IllegalLocationConstraintException`` → "Region mismatch"
    (so the user re-selects the region)."""
    import botocore.exceptions

    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "IllegalLocationConstraintException", "Message": "wrong region"}},
        "ListObjectsV2",
    )
    with (
        TestClient(app) as c,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
    ):
        resp = c.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-west-2", "access_key": "ak", "secret_key": "sk"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert "region" in body["error"].lower()


def test_check_fos_maps_endpoint_connection_error_to_friendly_message():
    """``EndpointConnectionError`` in the message → "Connection
    failed. Please verify the FOS Region is correct." Pinned
    because boto3 raises this for typo'd regions (not
    IllegalLocationConstraintException) so this is a separate path."""
    with (
        TestClient(app) as c,
        patch(
            "backend.core.duckdb._get_fos_client",
            side_effect=RuntimeError("boto3 EndpointConnectionError on x.object.fastlystorage.app"),
        ),
    ):
        resp = c.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "not-a-region", "access_key": "ak", "secret_key": "sk"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert "connection failed" in body["error"].lower()


# ── /api/provision/terraform/export ────────────────────────────────────────


def test_terraform_export_returns_zip_with_correct_content_disposition():
    """Export returns a ZIP file with the correct Content-Disposition
    header for browser download. Pinned because the FE keys on the
    filename in the disposition header."""
    fake_files = {
        "main.tf": "resource\n",
        "variables.tf": "variable\n",
        "instructions": "# README",  # special: written as README.md
    }
    with (
        TestClient(app) as c,
        patch("backend.utils.terraform_gen.generate_terraform", return_value=fake_files),
    ):
        resp = c.post("/api/provision/terraform/export", json={"service_id": "svc"})

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert "fastly-log-analysis-terraform.zip" in resp.headers["content-disposition"]


def test_terraform_export_includes_instructions_as_readme_md():
    """The ``instructions`` key gets renamed to ``README.md`` inside
    the zip. Pinned because customers expect a discoverable README,
    not a file named "instructions"."""
    import io
    import zipfile

    fake_files = {"main.tf": "resource\n", "instructions": "# Setup steps\n"}
    with (
        TestClient(app) as c,
        patch("backend.utils.terraform_gen.generate_terraform", return_value=fake_files),
    ):
        resp = c.post("/api/provision/terraform/export", json={"service_id": "svc"})

    assert resp.status_code == 200
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    names = zf.namelist()
    assert "README.md" in names
    assert "main.tf" in names
    # "instructions" gets renamed, not left in the archive
    assert "instructions" not in names


# ── /api/provision/ingest ──────────────────────────────────────────────────


def test_ingest_400s_without_token():
    """Token required. Pinned because the wizard sends the token
    along with the body — missing it means programmatic error."""
    with TestClient(app) as c:
        resp = c.post("/api/provision/ingest", json={"service_id": "svc"})
    assert resp.status_code == 400


def test_ingest_400s_on_bad_log_period():
    """``log_period`` that fails ``parse_period`` (e.g. "1 fortnight")
    → 400. Pinned because Fastly's API rejects bad periods at upload
    time; catching here surfaces the error in the wizard's review
    step instead of mid-deploy."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=ValueError("unknown period: fortnight")),
        # Security: stub token validation (auth gate exercised separately).
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc", "log_period": "1 fortnight"},
        )
    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["error"] == "invalid_log_period"
    assert "fortnight" in body["message"]


def test_ingest_creates_new_fos_key_when_none_provided(tmp_path, monkeypatch):
    """When neither ``fos_access_key`` nor ``find_fos_key`` finds an
    existing one, the route calls ``ensure_fos_access_key`` to create
    a new key. Pinned because dropping the create-key path would
    break the import-existing-service flow."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    new_key = {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "kid"}

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", return_value=new_key) as mock_ensure,
        patch("backend.provision.write_service_config") as mock_write,
        patch("backend.provision._sync_crontab"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc-1", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "fos_region": "us-east-1",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["service_id"] == "svc-1"
    # The created key went into the state passed to write_service_config
    mock_ensure.assert_called_once()
    persisted = mock_write.call_args[0][0]
    assert persisted["fos_access_key_id"] == "AKNEW"
    assert persisted["fos_secret_access_key"] == "SKNEW"


def test_ingest_400s_when_ensure_access_key_raises():
    """If creating the FOS key fails (Fastly API error), surface as
    400 with the underlying error. Pinned because the wizard renders
    the message in the error toast."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", side_effect=RuntimeError("rate limited")),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc-1", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
            },
        )
    assert resp.status_code == 400
    # Fastly API exceptions can leak token / account hints — the router
    # now logs the underlying message server-side and returns only the
    # stable code + a correlation id (see raise_internal).
    body = resp.json()["detail"]
    assert body["error"] == "ensure_access_key_failed"
    assert "error_id" in body


def test_ingest_uses_provided_keys_without_calling_fastly_api(tmp_path, monkeypatch):
    """When fos_access_key + fos_secret_key are provided in the body,
    the route uses them as-is and skips both find/create paths.
    Pinned because importing an analyst service should NOT mint a
    new admin key (which would require admin Fastly creds anyway)."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key") as mock_find,
        patch("backend.provision.ensure_fos_access_key") as mock_ensure,
        patch("backend.provision.write_service_config"),
        patch("backend.provision._sync_crontab"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc-1", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "fos_access_key": "EXISTING_AK",
                "fos_secret_key": "EXISTING_SK",
            },
        )

    assert resp.status_code == 200
    # Neither lookup nor create was attempted
    mock_find.assert_not_called()
    mock_ensure.assert_not_called()


def test_ingest_rerun_preserves_existing_faro_pin_when_body_omits_it(tmp_path, monkeypatch):
    """#1 audit finding: a re-run of /api/provision/ingest (analyst-join,
    wizard re-run, key rotation) rebuilt ``state["rum"]`` from the request
    body alone as ``{enabled, enabled_at}``, with no awareness of a
    ``faro_version`` already pinned on disk. ``write_service_config``'s
    preserved-key merge treats "rum" as present-in-state (the handler
    always sets it when ``rum_enabled`` is true) and does a full overwrite,
    so a re-run silently unpinned an already-RUM-enabled service — handing
    the sync cron an unrequested version change and, via its adopt-default
    self-heal branch, an unrequested Fastly VCL activation.

    Pins: an ingest re-run with ``rum_enabled=True`` and no
    ``rum.faro_version`` in the body must carry the on-disk pin (and its
    persisted upload hashes) forward into ``state["rum"]`` unchanged.
    """
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-1",
        {
            "service_id": "svc-1",
            "rum_enabled": True,
            "rum": {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00Z",
                "faro_version": "9.9.9",
                "faro_content_hash": "existing-hash",
                "faro_fos_etag_md5": "existing-etag",
            },
        },
    )

    captured = {}

    def fake_write(state):
        captured["state"] = state

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch(
            "backend.provision.ensure_fos_access_key",
            return_value={"access_key": "AK", "secret_key": "SK", "id": "kid"},
        ),
        patch("backend.provision.write_service_config", side_effect=fake_write),
        patch("backend.provision._sync_crontab"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc-1", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "rum_enabled": True,
            },
        )

    assert resp.status_code == 200
    assert captured["state"]["rum"]["faro_version"] == "9.9.9"
    assert captured["state"]["rum"]["faro_content_hash"] == "existing-hash"
    assert captured["state"]["rum"]["faro_fos_etag_md5"] == "existing-etag"


def test_ingest_rerun_honors_explicit_faro_version_in_body(tmp_path, monkeypatch):
    """Negative control for the fix above: an explicit ``rum.faro_version``
    in the request body (a deliberate operator re-pin) must still win over
    whatever is stored on disk — the preservation fallback only applies
    when the body carries no pin at all."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-1",
        {
            "service_id": "svc-1",
            "rum_enabled": True,
            "rum": {"enabled": True, "faro_version": "9.9.9"},
        },
    )

    captured = {}

    def fake_write(state):
        captured["state"] = state

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch(
            "backend.provision.ensure_fos_access_key",
            return_value={"access_key": "AK", "secret_key": "SK", "id": "kid"},
        ),
        patch("backend.provision.write_service_config", side_effect=fake_write),
        patch("backend.provision._sync_crontab"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-T"}
                if path == "/tokens/self"
                else {"id": "svc-1", "customer_id": "cust-T"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "rum_enabled": True,
                "rum": {"faro_version": "1.0.0"},
            },
        )

    assert resp.status_code == 200
    assert captured["state"]["rum"]["faro_version"] == "1.0.0"


# ── /api/provision/ngwaf-workspaces ────────────────────────────────────────


def test_ngwaf_workspaces_401s_when_no_token():
    """Security: no ``token=`` query param → 401 (the route now
    REQUIRES the caller to present a token; the silent stored-key
    fallback was the unauth-disclosure vector). Pinned because the FE
    distinguishes 401 (no token) from 400 (bad token) when prompting
    the user."""
    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value=""),
    ):
        resp = c.get("/api/provision/ngwaf-workspaces", params={"service_id": "svc"})
    assert resp.status_code == 401
    assert resp.json()["detail"]["error"] == "token_required"


def test_ngwaf_workspaces_returns_data_shape_workspaces():
    """NGWAF API returns ``{"data": [...]}`` shape. The route
    normalises each workspace to ``{id, name}``. Pinned because
    the FE keys on these two fields.

    Security: caller must pass ``token=tok`` matching the stored
    key (constant-time compare). The mock returns "tok" for the stored
    key so the match succeeds without hitting Fastly's /tokens/self.
    """
    fake_body = b'{"data": [{"id": "ws-1", "name": "Prod"}, {"id": "ws-2", "name": "Staging"}]}'
    fake_resp = MagicMock()
    fake_resp.read.return_value = fake_body
    fake_resp.status = 200
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=fake_resp),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = c.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc", "token": "tok"},
        )
        mock_validate.assert_called_once_with("tok", service_id="svc")

    assert resp.status_code == 200, resp.text[:500]
    body = resp.json()
    workspaces = body["workspaces"]
    assert len(workspaces) == 2
    assert workspaces[0] == {"id": "ws-1", "name": "Prod"}


def test_ngwaf_workspaces_accepts_authorization_header():
    """Verify that `/api/provision/ngwaf-workspaces` correctly accepts and extracts
    the token from the `Authorization: Bearer <token>` header."""
    fake_body = b'{"data": [{"id": "ws-1", "name": "Prod"}]}'
    fake_resp = MagicMock()
    fake_resp.read.return_value = fake_body
    fake_resp.status = 200
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=fake_resp),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = c.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc"},
            headers={"Authorization": "Bearer tok"},
        )
        mock_validate.assert_called_once_with("tok", service_id="svc")

    assert resp.status_code == 200
    body = resp.json()
    assert body["workspaces"][0] == {"id": "ws-1", "name": "Prod"}


def test_ngwaf_workspaces_returns_workspaces_shape():
    """Alternative shape: ``{"workspaces": [...]}`` (older NGWAF
    response). The route handles both via key-presence check.
    Pinned because the key-presence check was a bug fix — naive
    ``.get("data") or .get("workspaces")`` short-circuits on
    legitimate empty lists."""
    fake_body = b'{"workspaces": [{"id": "ws-old", "attributes": {"name": "OldShape"}}]}'
    fake_resp = MagicMock()
    fake_resp.read.return_value = fake_body
    fake_resp.status = 200
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=fake_resp),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = c.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc", "token": "tok"},
        )
        mock_validate.assert_called_once_with("tok", service_id="svc")

    body = resp.json()
    # The route falls back to attributes.name when top-level name is absent
    assert body["workspaces"][0]["name"] == "OldShape"


def test_ngwaf_workspaces_maps_401_to_400_with_permissions_hint():
    """NGWAF returns 401 when the token lacks Edge Security perms.
    The route remaps to 400 with a hint about permissions. Pinned
    because keeping the original 401 would log the user out of the
    admin UI (the FE's auth interceptor treats 401 as session-
    expired)."""
    import urllib.error
    from io import BytesIO

    err = urllib.error.HTTPError(
        url="https://api.fastly.com/ngwaf/v1/workspaces",
        code=401,
        msg="unauthorized",
        hdrs={},
        fp=BytesIO(b"unauthorized"),
    )
    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value="bad-tok"),
        patch("urllib.request.urlopen", side_effect=err),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = c.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc", "token": "bad-tok"},
        )
        mock_validate.assert_called_once_with("bad-tok", service_id="svc")

    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["error"] == "ngwaf_token_invalid"
    assert "permissions" in body["message"].lower()


def test_provision_execute_rejects_invalid_bucket_name_format():
    """Bucket names must match S3-compatible pattern: 3-63 alphanumeric
    + single hyphens, can't start/end with hyphen, can't have
    double hyphens. Pinned because losing this would 400 at Fastly
    upload time with a confusing error instead of in our wizard."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
    ):
        # Underscore is invalid in S3 bucket names
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "invalid_underscore_name",
            },
        )
    assert r.status_code == 400
    body = r.json()["detail"]
    assert body["error"] == "invalid_bucket"
    assert "Invalid bucket name" in body["message"]


def test_provision_execute_rejects_bucket_with_double_hyphens():
    """The regex uses `(?!.*--)` to forbid double hyphens. Pinned
    because some bucket implementations reject double-hyphens at
    create time."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "bad--bucket",
            },
        )
    assert r.status_code == 400


def test_provision_execute_rejects_bucket_starting_with_hyphen():
    """Bucket names can't start with `-`. Pinned because the regex
    requires `[A-Za-z0-9]` as the first char."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "-leading-hyphen",
            },
        )
    assert r.status_code == 400


def test_provision_execute_rejects_fos_prefix_with_sql_metachars():
    """fos_prefix must be alphanumerics + / _ - only. Without this
    guard, a prefix containing `'`, `*`, `?`, or `[]` reaches DuckDB's
    glob() via f-string interpolation and either breaks the SQL literal
    or changes the glob scope. Pinned because losing this validation
    re-opens a SQL-injection-style sink in
    _duckdb_status._delete_ingested_files."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "fos_prefix": "x'; DROP TABLE--",
            },
        )
    assert r.status_code == 400
    assert "prefix" in r.json()["detail"]["error"].lower()


def test_provision_execute_400s_on_bad_log_period():
    """`log_period` that fails parse_period (e.g. "fortnight") → 400.
    Pinned because Fastly's API rejects bad periods at upload time;
    catching here surfaces the error in the wizard's review step
    instead of mid-deploy."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", side_effect=ValueError("unknown: fortnight")),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "log_period": "fortnight",
            },
        )
    assert r.status_code == 400
    body = r.json()["detail"]
    assert body["error"] == "invalid_log_period"
    assert "fortnight" in body["message"]


def test_provision_execute_400s_when_cdn_domain_unavailable():
    """If `cdn_url` is provided AND the domain check returns
    unavailable (not a DNS error), refuse with 400. Pinned because
    losing this would let the wizard try to claim a domain Fastly
    already serves, producing a confusing post-provision error."""
    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(False, "Domain already registered or in use"),
        ),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "cdn_url": "https://taken.global.ssl.fastly.net",
            },
        )
    assert r.status_code == 400
    assert "unavailable" in r.json()["detail"]["error"].lower()


def test_provision_execute_allows_cdn_domain_with_dns_unavailable_reason():
    """When `_check_domain_available` returns False with a "DNS"
    reason (network glitch), the route proceeds rather than failing
    fast. Pinned because losing this would block the wizard on
    every transient DNS hiccup."""

    def fake_provision(cfg, _resume_from_state=False):
        yield {"type": "done", "message": "ok"}

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(False, "DNS resolution failed transiently"),
        ),
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.core.metadata.record_audit"),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "cdn_url": "https://x.global.ssl.fastly.net",
            },
        )

    # 200 + SSE (DNS error doesn't block the proceed)
    assert r.status_code == 200


def test_provision_execute_threads_faro_version_into_rum_config():
    """The wizard's RUM version picker (Task 7) must reach
    cfg["rum"]["faro_version"] so the declarative generator can pin the
    Faro Web SDK bundle for a brand-new service. Pinned because this field
    was silently dropped (Pydantic default extra=ignore) before the
    ProvisionExecuteRequest field existed — an operator's picker choice
    would have had zero effect on the deployed service."""
    captured = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "done", "message": "ok"}

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.core.metadata.record_audit"),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "rum_enabled": True,
                "faro_version": "1.2.3",
            },
        )

    assert r.status_code == 200
    assert captured["cfg"]["rum"]["faro_version"] == "1.2.3"


def test_provision_execute_defaults_faro_version_when_operator_does_not_pin_one():
    """No version chosen (registry outage, or operator skipped the picker)
    must NOT leave cfg["rum"] unpinned (F-2 audit finding): the generated
    tracker JS unconditionally requests /js/faro-sdk.js with no CDN
    fallback, so an unpinned service's VCL would either omit that route
    entirely or route it nowhere resolvable — a permanent 404 for every RUM
    visitor. Provisioning must resolve DEFAULT_FARO_VERSION itself, exactly
    like enable_rum() already does for the enable-after-the-fact path."""
    from backend.core.faro_versions import DEFAULT_FARO_VERSION

    captured = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "done", "message": "ok"}

    with (
        TestClient(app) as c,
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.core.metadata.record_audit"),
    ):
        r = c.post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "rum_enabled": True,
            },
        )

    assert r.status_code == 200
    assert captured["cfg"]["rum"]["faro_version"] == DEFAULT_FARO_VERSION


def test_ngwaf_workspaces_empty_list_with_automation_token_returns_hint():
    """If the workspace list comes back empty AND ``/tokens/self``
    returns a token without ``user_id`` (automation token), include
    a hint that NGWAF requires a personal token. Pinned because
    this is the most common cause of empty NGWAF lists and avoiding
    the hint sends users on a debug wild-goose chase."""
    fake_body = b'{"data": []}'
    fake_resp = MagicMock()
    fake_resp.read.return_value = fake_body
    fake_resp.status = 200
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    fake_token_info = {"id": "tok-id", "name": "automation"}  # no user_id

    with (
        TestClient(app) as c,
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=fake_resp),
        patch("backend.provision.fastly", return_value=fake_token_info),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = c.get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc", "token": "tok"},
        )
        mock_validate.assert_called_once_with("tok", service_id="svc")

    body = resp.json()
    assert body["workspaces"] == []
    assert "automation" in body["error_hint"].lower()


# ── /check-config ────────────────────────────────────────────────────────
#
# Verifies the post-provisioning audit endpoint that powers the admin
# "Health" indicator next to each provisioned service. Each branch maps
# to a distinct user-facing message — the FE displays `details` verbatim,
# so silently breaking one would surface as wrong-looking diagnostics
# without raising.


_CHECK_PARAMS = {"token": "t", "service_id": "log-svc", "cdn_service_id": "cdn-svc", "bucket": "b"}
_REQUIRED_SNIPPETS = [
    "Fastly Log Analysis Capture",
    "Fastly Log Analysis Miss",
    "Fastly Log Analysis Pass",
]
_OPTIONAL_SNIPPETS = [
    "Fastly Log Analysis Origin Fetch",
    "Fastly Log Analysis Origin Error",
    "Fastly Log Analysis Origin Deliver",
]
_REQUIRED_CDN_SNIPPETS = [
    "iceberg-metadata-pointer-ttl",
    "cdn-swr-shield-disable",
    "cdn-race-condition-generation",
    "cdn-no-cache-404",
]


def _check_config_fastly_router(*, endpoints, snippets, dicts, backends, cdn_snippets):
    """Build a side_effect that dispatches `fastly()` calls based on path."""

    def fake_fastly(method, path, token=None, **_):
        if path == "/service/log-svc/version/active":
            return {"number": 7}
        if "/version/7/logging/s3" in path:
            return endpoints
        if "/version/7/snippet" in path:
            return snippets
        if "/version/active/dictionary" in path:
            return dicts
        if "/version/active/backend" in path:
            return backends
        if path == "/service/cdn-svc/version/active":
            return {"number": 3}
        if "/version/3/snippet" in path:
            return cdn_snippets
        raise AssertionError(f"unexpected fastly path: {path}")

    return fake_fastly


def test_check_config_logging_service_no_matching_endpoint():
    """Endpoint list doesn't contain ``bucket_name == bucket`` → details
    explains the missing endpoint, ok=False. Pinned because the FE shows
    this exact string to admins who provisioned via a different bucket."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "other", "bucket_name": "different-bucket"}],
        snippets=[],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    body = resp.json()
    assert body["logging_service"]["ok"] is False
    assert "No S3 logging endpoint" in body["logging_service"]["details"]


def test_check_config_logging_service_missing_required_snippets():
    """Endpoint exists but a required snippet is missing → details lists
    the missing snippet, ok=False. Pinned because admins use this to
    know exactly which VCL snippet to re-add."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": "Fastly Log Analysis Capture"}],  # missing two
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    details = resp.json()["logging_service"]["details"]
    assert "missing CORE snippets" in details
    assert "Fastly Log Analysis Miss" in details
    assert "Fastly Log Analysis Pass" in details


def test_check_config_logging_service_no_response_condition_warns_but_ok():
    """All core snippets present but no `response_condition` → ok=True
    with a "sampling might be disabled" warning. Pinned because losing
    this would either silently pass (admin misses the warning) or
    falsely fail (no condition is a valid setup — just usually
    accidental)."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": None}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    log = resp.json()["logging_service"]
    assert log["ok"] is True
    assert "no response condition" in log["details"].lower()


def test_check_config_logging_service_full_with_optional_origin_snippets():
    """All core + optional Origin snippets present → details mentions
    the optional count. Pinned because the FE Tag "Origin metrics
    enabled" only shows when this exact substring lands."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS + _OPTIONAL_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    log = resp.json()["logging_service"]
    assert log["ok"] is True
    assert "3 origin metric snippets" in log["details"]
    assert "active sampling condition" in log["details"]


def test_check_config_logging_service_exception_captured_in_details():
    """If a fastly() call inside the logging-service block raises, the
    exception message is surfaced via `details` (NOT a 500). Pinned
    because the admin health panel must keep rendering even when one
    Fastly endpoint is flaky."""

    def fake_fastly(method, path, token=None, **_):
        if path == "/service/log-svc/version/active":
            raise RuntimeError("Fastly 503")
        return {}

    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    assert resp.status_code == 200
    assert "Fastly 503" in resp.json()["logging_service"]["details"]


def test_check_config_cdn_service_missing_dictionaries():
    """CDN service missing `fos_credentials` / `cdn_auth` → ok=False
    with the missing dict names enumerated. Pinned because the FE
    surfaces these exact names so admins know which dictionary to
    re-create."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "c"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[],  # missing both
        backends=[],
        cdn_snippets=[],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    cdn = resp.json()["cdn_service"]
    assert cdn["ok"] is False
    details = cdn["details"]
    assert "fos_credentials" in details
    assert "cdn_auth" in details


def test_check_config_cdn_service_no_fos_backend():
    """Dicts present but no backend pointing at *.fastlystorage.app →
    ok=False with a clear message. Pinned because a CDN service can
    have the dicts copied over without the FOS backend — they'd think
    it's "almost done" when it isn't."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "c"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "origin.example.com"}],  # not fastlystorage.app
        cdn_snippets=[],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    cdn = resp.json()["cdn_service"]
    assert cdn["ok"] is False
    assert "fastlystorage.app" in cdn["details"]


def test_check_config_cdn_service_missing_performance_snippets():
    """Backend + dicts OK but the perf snippets are missing → ok=False
    listing them. Pinned because these specific snippet names drive
    the cache-perf charts; missing one silently degrades hit ratio."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "c"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": "iceberg-metadata-pointer-ttl"}],  # missing three
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    cdn = resp.json()["cdn_service"]
    assert cdn["ok"] is False
    assert "cdn-swr-shield-disable" in cdn["details"]
    assert "cdn-race-condition-generation" in cdn["details"]
    assert "cdn-no-cache-404" in cdn["details"]


def test_check_config_cdn_service_fully_configured():
    """Dictionaries + backend + all snippets present → ok=True with
    "fully configured" message."""
    side = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "c"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=side),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    cdn = resp.json()["cdn_service"]
    assert cdn["ok"] is True
    assert "fully configured" in cdn["details"]


def test_cdn_no_cache_404_snippet_strips_caching_on_get_and_head_404s():
    """The cdn-no-cache-404 snippet MUST run in vcl_fetch and disable caching
    on any GET/HEAD 404. Pinned because: PyIceberg's commit lifecycle does
    HEAD-before-PUT on new metadata paths (CAS atomicity); the HEAD
    legitimately 404s; if Fastly caches it, every subsequent commit's
    load_table reads the cached 404 and the cron hangs forever (saw it
    2026-05-19, 5+ failed cycles before fix). Regressing this snippet
    re-introduces that outage."""
    from backend.provision.fastly_api import _CDN_NO_CACHE_404_SNIPPET_NAME, _CDN_SNIPPETS

    by_name = {entry[0]: entry for entry in _CDN_SNIPPETS}
    assert _CDN_NO_CACHE_404_SNIPPET_NAME in by_name
    _name, snippet_type, content, _priority = by_name[_CDN_NO_CACHE_404_SNIPPET_NAME]
    assert snippet_type == "fetch", "must run in vcl_fetch so it can mutate beresp"
    # Both read methods covered.
    assert 'req.method == "GET"' in content
    assert 'req.method == "HEAD"' in content
    assert "beresp.status == 404" in content
    # All four cache knobs killed.
    assert "set beresp.cacheable = false;" in content
    assert "set beresp.ttl = 0s;" in content
    assert "set beresp.stale_while_revalidate = 0s;" in content
    assert "set beresp.stale_if_error = 0s;" in content


def test_check_config_cdn_service_exception_captured_in_details():
    """A fastly() raise inside the CDN block doesn't 500 — the exception
    text lands in `details`. Pinned for the same admin-health-keeps-
    rendering reason as the logging-service variant."""

    def fake_fastly(method, path, token=None, **_):
        if path == "/service/log-svc/version/active":
            return {"number": 7}
        if "/version/7/logging/s3" in path:
            return [{"name": "fos-logs", "bucket_name": "b", "response_condition": "c"}]
        if "/version/7/snippet" in path:
            return [{"name": s} for s in _REQUIRED_SNIPPETS]
        # CDN side: raise
        raise RuntimeError("CDN 503")

    with (
        TestClient(app) as c,
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        resp = c.get("/api/provision/check-config", params=_CHECK_PARAMS)

    assert resp.status_code == 200
    assert "CDN 503" in resp.json()["cdn_service"]["details"]
    # The logging side still ran cleanly
    assert resp.json()["logging_service"]["ok"] is True


# ── /teardown (SSE) ──────────────────────────────────────────────────────


def test_teardown_404s_when_no_service_config_loaded():
    """No `service_id` and no loadable config → 404 with
    "No service config found." Pinned because the FE relies on this
    code path to differentiate "service doesn't exist" from "teardown
    failed mid-stream" (the latter would be a 200 SSE with an error
    event)."""
    with (
        TestClient(app) as c,
        patch("backend.config.load_config", return_value=None),
    ):
        # No service_id provided → state stays None → 404
        resp = c.post("/api/provision/teardown", json={})

    assert resp.status_code == 404
    body = resp.json()
    assert "No service config" in body["detail"]["error"]


def test_teardown_streams_done_event_on_success():
    """Happy path: SSE stream ends with a `{"type": "done"}` event after
    `perform_teardown` yields its events. Pinned because the FE's
    progress modal closes only when it sees the `done` line — losing
    that would leave the modal hung even after a clean teardown."""
    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "name": "Test Svc",
        "provisioning": {},
    }

    with (
        TestClient(app) as c,
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent-config.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler", return_value=MagicMock()),
        patch("backend.core.iceberg.clear_source_caches"),
        patch(
            "backend.provision.perform_teardown",
            return_value=iter([{"type": "status", "message": "removed s3 bucket"}]),
        ),
        patch("backend.core.duckdb.reload_default_source"),
        # Security: stub token validation for the destructive teardown
        # path. The auth gate itself is exercised in test_provision_teardown_auth.py.
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-X"}
                if path == "/tokens/self"
                else {"id": "svc", "customer_id": "cust-X"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": False},
        )
        body = resp.text

    assert resp.status_code == 200
    # The perform_teardown event passed through
    assert "removed s3 bucket" in body
    # The completion sentinel landed
    assert '"type": "done"' in body
    assert "Teardown complete" in body


def test_teardown_streams_error_event_when_perform_teardown_raises():
    """`perform_teardown` raising → SSE stream emits an `{"type":
    "error"}` event (NOT a 500). Pinned because the FE handles the
    error-event branch with a styled toast — a 500 mid-stream would
    instead leave the progress modal stuck without explanation."""
    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "name": "Test Svc",
        "provisioning": {},
    }

    def boom(state, token, opts=None):
        raise RuntimeError("Fastly API rate-limited")
        yield  # makes it a generator (unreachable)

    with (
        TestClient(app) as c,
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler", return_value=MagicMock()),
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.provision.perform_teardown", side_effect=boom),
        patch("backend.core.duckdb.reload_default_source"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-X"}
                if path == "/tokens/self"
                else {"id": "svc", "customer_id": "cust-X"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": False},
        )
        body = resp.text

    assert resp.status_code == 200
    assert '"type": "error"' in body
    assert "Fastly API rate-limited" in body


def test_teardown_announces_cron_update_when_remove_cron_true():
    """`remove_cron=True` → SSE emits the "Cron jobs updated" status
    line after `_sync_crontab` + `scheduler.reload()`. Pinned because
    the FE conditionally renders a cron-progress chip; losing the
    announcement would leave it perpetually pending."""
    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "name": "Test Svc",
        "provisioning": {},
    }

    sync_calls: list[bool] = []
    fake_scheduler = MagicMock()

    with (
        TestClient(app) as c,
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab", side_effect=lambda: sync_calls.append(True)),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.provision.perform_teardown", return_value=iter([])),
        patch("backend.core.duckdb.reload_default_source"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-X"}
                if path == "/tokens/self"
                else {"id": "svc", "customer_id": "cust-X"}
            ),
        ),
    ):
        resp = c.post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": True},
        )
        body = resp.text

    assert sync_calls == [True]
    fake_scheduler.reload.assert_called_once()
    assert "Cron jobs updated" in body


# ── /api/provision/services: Object Storage enablement gate ──────────────────


def _svc_list():
    return [{"id": "svc-1", "name": "My Service", "type": "vcl"}]


def test_list_services_blocks_when_object_storage_not_enabled():
    """A valid token whose account lacks the Object Storage product → 400
    object_storage_not_enabled with an actionable message, surfaced right after
    the token is entered instead of dying with a 403 + rollback at the FOS-key
    step. Object Storage is required, so there is no local fallback."""
    with (
        patch("backend.core.fastly.client.fastly", return_value=_svc_list()),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=False),
        patch("backend.config.list_service_ids", return_value=[]),
    ):
        resp = TestClient(app).get("/api/provision/services", params={"token": "tok"})
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "object_storage_not_enabled"
    assert "Object Storage" in detail["message"]


def test_list_services_returns_list_when_object_storage_enabled():
    with (
        patch("backend.core.fastly.client.fastly", return_value=_svc_list()),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=True),
        patch("backend.config.list_service_ids", return_value=set()),
    ):
        resp = TestClient(app).get("/api/provision/services", params={"token": "tok"})
    assert resp.status_code == 200
    assert resp.json() == [{"id": "svc-1", "name": "My Service", "provisioned": False}]


def test_list_services_invalid_token_not_mislabeled_as_object_storage():
    """A bad token fails the /service list first → generic list_services_failed,
    NOT the object_storage message (the OS probe is never reached)."""
    probe_called = []
    with (
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 401 GET /service")),
        patch(
            "backend.provision.fos_setup.object_storage_enabled",
            side_effect=lambda *a, **k: probe_called.append(True) or False,
        ),
    ):
        resp = TestClient(app).get("/api/provision/services", params={"token": "bad"})
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "list_services_failed"
    assert probe_called == []


def test_object_storage_enabled_true_on_200():
    from backend.provision.fos_setup import object_storage_enabled

    with patch(
        "backend.provision.fos_setup.fastly",
        return_value={"product": {"id": "object_storage"}, "customer": {"id": "c"}},
    ):
        assert object_storage_enabled("tok") is True


def test_object_storage_enabled_false_on_4xx():
    from backend.provision.fos_setup import object_storage_enabled

    with patch(
        "backend.provision.fos_setup.fastly",
        side_effect=RuntimeError("HTTP 404 GET /enabled-products/v1/object_storage\n    not found"),
    ):
        assert object_storage_enabled("tok") is False


def test_object_storage_enabled_inconclusive_on_5xx_or_network():
    """5xx / network → don't block (True); the reactive FOS-key 403 still guards."""
    from backend.provision.fos_setup import object_storage_enabled

    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 503 GET x")):
        assert object_storage_enabled("tok") is True
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("Network error on GET x")):
        assert object_storage_enabled("tok") is True


def test_product_enabled_hits_the_right_endpoint_and_maps_status():
    """product_enabled generalizes the object_storage probe to any product id
    (e.g. kv_store): 200 -> True, 4xx -> False, 5xx -> inconclusive True."""
    from backend.provision.fos_setup import product_enabled

    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "kv_store"}}) as m:
        assert product_enabled("tok", "kv_store") is True
        # Calls the {product_id}-templated enablement endpoint.
        assert m.call_args.args[:2] == ("GET", "/enabled-products/v1/kv_store")
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 404 not found")):
        assert product_enabled("tok", "kv_store") is False
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 502 bad gateway")):
        assert product_enabled("tok", "kv_store") is True


def test_object_storage_enabled_delegates_to_product_enabled():
    """object_storage_enabled is now a thin wrapper over product_enabled."""
    from backend.provision import fos_setup

    with patch("backend.provision.fos_setup.product_enabled", return_value=True) as m:
        assert fos_setup.object_storage_enabled("tok") is True
    assert m.call_args.args == ("tok", "object_storage")


def test_reconcile_takes_credentials_in_body_not_query_string():
    """A Fastly API token in a query string lands in every access log, proxy
    log and Referer along the way. The endpoint must accept service_id/token
    in the request body, and must NOT declare them as query parameters."""
    params = app.openapi()["paths"]["/api/provision/reconcile"]["post"].get("parameters", [])
    assert [p["name"] for p in params if p.get("in") == "query"] == []
    assert "requestBody" in app.openapi()["paths"]["/api/provision/reconcile"]["post"]


def test_reconcile_rejects_missing_credentials():
    """Empty body must 400 rather than starting a reconcile thread with no token."""
    client = TestClient(app)
    with patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile:
        response = client.post("/api/provision/reconcile", json={})
    assert response.status_code == 400
    mock_reconcile.assert_not_called()
