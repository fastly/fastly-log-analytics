"""Tests for backend/routers/provision.py (/api/provision/*) and helper branches."""

from __future__ import annotations

import io
import urllib.error
import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from backend import config
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.main import app
from backend.provision import fos_setup
from backend.provision.fastly_api import _CDN_NO_CACHE_404_SNIPPET_NAME, _CDN_SNIPPETS
from backend.provision.fos_setup import object_storage_enabled, product_enabled
from backend.routers.provision import _check_domain_available
from backend.utils.router_utils import require_json_content_type as _require_json_content_type

_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "access_level": "read_write",
    "fos_bucket": "bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
}


def _client() -> TestClient:
    return TestClient(app)


def _auth_fastly_ok(service_id: str = "svc-1", customer_id: str = "cust-T"):
    return lambda method, path, *, token, **kw: (
        {"id": "tok", "scope": "global", "services": [], "customer_id": customer_id}
        if path == "/tokens/self"
        else {"id": service_id, "customer_id": customer_id}
    )


# ─── /api/provision/lake-info ────────────────────────────────────────────────


def test_lake_info_modes():
    with (
        patch("backend.core.iceberg.ducklake_table_exists", return_value=True) as mock_exists,
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={
                "min_timestamp": "2026-05-01T00:00:00Z",
                "max_timestamp": "2026-05-02T00:00:00Z",
                "data_files": 10,
                "size_bytes": 1024,
            },
        ),
        patch(
            "backend.core.iceberg.get_snapshot_calendar",
            return_value={
                "2026-05-01": {"data_files": 5, "size_bytes": 512},
                "2026-05-02": {"data_files": 5, "size_bytes": 512},
            },
        ),
        patch("urllib.request.urlopen", side_effect=Exception("Fast path miss")),
        patch("backend.core.duckdb._get_fos_client", side_effect=Exception("Fast path miss")),
    ):
        resp = _client().post(
            "/api/provision/lake-info",
            json={
                "bucket": "test-bucket",
                "region": "us-east-1",
                "access_key": "ak",
                "secret_key": "sk",
                "prefix": "p/",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["table_exists"] is True
        assert data["info"]["data_files"] == 10
        assert data["range"]["start"] == "2026-05-01T00:00:00Z"

        # Analyst location forwarded into source
        loc = "s3://test-bucket/iceberg/metadata/v1.metadata.json"
        _client().post(
            "/api/provision/lake-info",
            json={
                "bucket": "test-bucket",
                "region": "us-east-1",
                "access_key": "ak",
                "secret_key": "sk",
                "iceberg_metadata_location": loc,
            },
        )
        assert mock_exists.call_args[0][0]["iceberg_metadata_location"] == loc

        # Table not found
        mock_exists.return_value = False
        resp_nf = _client().post(
            "/api/provision/lake-info",
            json={"bucket": "test-bucket", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
        assert resp_nf.status_code == 200
        assert resp_nf.json()["table_exists"] is False
        assert "not found" in resp_nf.json()["message"]


def test_lake_info_range_and_calendar_match_producer():
    producer = {
        "ok": True,
        "table_exists": True,
        "info": {"min_timestamp": "2026-05-01T00:00:00", "max_timestamp": "2026-05-02T00:00:00", "snapshots": 3},
        "calendar": {
            "2026-05-01": {"data_files": 5, "size_bytes": 512},
            "unknown": {"data_files": 1, "size_bytes": 0},
        },
        "range": {"start": "2026-05-01T00:00:00", "end": None},
    }
    with patch("backend.core.iceberg.lake_info.fetch_lake_info", return_value=producer):
        response = _client().post(
            "/api/provision/lake-info",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk", "prefix": "p/"},
        )
    assert response.status_code == 200
    assert {k: v for k, v in response.json().items() if not k.startswith("_")} == producer


# ─── /api/provision/services/{id}/ngwaf-workspace & /ngwaf-workspaces ────────


def test_set_ngwaf_workspace_saves_body_and_ignores_query_param():
    saved: dict = {}
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="test-stored-key")

    def fake_mutate(sid, fn, **kwargs):
        cfg = dict(cfg_with_key)
        fn(cfg)
        saved.update(cfg)
        return cfg

    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.mutate_config", side_effect=fake_mutate),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=test-stored-key",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
        assert resp.status_code == 200
        mock_validate.assert_called_once_with("test-stored-key", service_id="svc123")
        assert resp.json()["ngwaf_workspace_id"] == "workspace-abc"
        assert saved.get("ngwaf_workspace_id") == "workspace-abc"

        saved.clear()
        resp2 = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?workspace_id=workspace-abc&token=test-stored-key",
            json={},
        )
        assert resp2.status_code == 200
        assert saved.get("ngwaf_workspace_id") is None


@pytest.mark.parametrize(
    ("query", "cfg_key", "fastly_effect", "expected_error"),
    [
        ("", None, None, "token_required"),
        ("?token=attacker-supplied-wrong-token", "legit-stored-key", RuntimeError("HTTP 401"), None),
        (
            "?token=stored-read-only-token",
            "stored-read-only-token",
            {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"},
            "insufficient_scope",
        ),
    ],
)
def test_set_ngwaf_workspace_auth_rejections(query, cfg_key, fastly_effect, expected_error):
    cfg = dict(_FAKE_CFG, **({"fastly_api_key": cfg_key} if cfg_key else {}))
    fastly_kw = (
        {"side_effect": fastly_effect} if isinstance(fastly_effect, Exception) else {"return_value": fastly_effect}
    )
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", **fastly_kw),
    ):
        resp = _client().patch(
            f"/api/provision/services/svc123/ngwaf-workspace{query}",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert resp.status_code == 401
    if expected_error:
        assert resp.json()["detail"]["error"] == expected_error


def test_ngwaf_workspaces_listing_variants():
    # 1. Read-only stored token rejected with 401
    cfg_ro = dict(_FAKE_CFG, fastly_api_key="stored-read-only-token")
    ro_data = {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"}
    with (
        patch("backend.config.load_config", return_value=cfg_ro),
        patch("backend.utils.fastly_auth.fastly", return_value=ro_data),
    ):
        r_ro = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc123", "token": "stored-read-only-token"},
        )
        assert r_ro.status_code == 401
        assert r_ro.json()["detail"]["error"] == "insufficient_scope"

    # 2. Missing token -> 401 token_required
    with patch("backend.config.get_fastly_api_key", return_value=""):
        r_no_tok = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc"})
        assert r_no_tok.status_code == 401
        assert r_no_tok.json()["detail"]["error"] == "token_required"

    # 3. Data shape, Authorization header, and workspaces shape
    def _mock_resp(payload: bytes):
        m = MagicMock()
        m.read.return_value = payload
        m.status = 200
        m.__enter__ = lambda s: s
        m.__exit__ = MagicMock(return_value=False)
        return m

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"data": [{"id": "ws-1", "name": "Prod"}, {"id": "ws-2", "name": "Staging"}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_val,
    ):
        r_data = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_data.status_code == 200
        assert r_data.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}
        mock_val.assert_called_once_with("tok", service_id="svc")

        r_hdr = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc"},
            headers={"Authorization": "Bearer tok"},
        )
        assert r_hdr.status_code == 200
        assert r_hdr.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"workspaces": [{"id": "ws-old", "attributes": {"name": "OldShape"}}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_old = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_old.json()["workspaces"][0]["name"] == "OldShape"

    # 4. Empty list + automation token -> error_hint
    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=_mock_resp(b'{"data": []}')),
        patch("backend.provision.fastly", return_value={"id": "tok-id", "name": "automation"}),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_empty = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_empty.json()["workspaces"] == []
        assert "automation" in r_empty.json()["error_hint"].lower()

    # 5. Upstream 401 -> 400 ngwaf_token_invalid
    err_401 = urllib.error.HTTPError(
        url="https://api.fastly.com/ngwaf/v1/workspaces",
        code=401,
        msg="unauthorized",
        hdrs={},  # type: ignore[arg-type]
        fp=BytesIO(b"unauthorized"),
    )
    with (
        patch("backend.config.get_fastly_api_key", return_value="bad-tok"),
        patch("urllib.request.urlopen", side_effect=err_401),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_401 = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "bad-tok"})
        assert r_401.status_code == 400
        assert r_401.json()["detail"]["error"] == "ngwaf_token_invalid"


# ─── /api/provision/services & /validate ─────────────────────────────────────


def test_provision_list_services_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-existing", {"service_id": "svc-existing"})

    fake_services = [
        {"id": "svc-existing", "name": "Already Done", "type": "vcl"},
        {"id": "svc-new", "name": "Fresh", "type": "vcl"},
        {"id": "svc-wasm", "name": "Compute", "type": "wasm"},
    ]
    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=True),
    ):
        resp = _client().get("/api/provision/services?token=tok")
        assert resp.status_code == 200
        body = resp.json()
        assert {s["id"]: s["provisioned"] for s in body} == {"svc-existing": True, "svc-new": False}

    # Missing token -> 422
    assert _client().get("/api/provision/services").status_code == 422

    # Object storage disabled -> 400 object_storage_not_enabled
    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services[:1]),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=False),
        patch("backend.config.list_service_ids", return_value=[]),
    ):
        r_no_os = _client().get("/api/provision/services", params={"token": "tok"})
        assert r_no_os.status_code == 400
        assert r_no_os.json()["detail"]["error"] == "object_storage_not_enabled"

    # Bad token fails before OS probe
    probe_called: list[bool] = []
    with (
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 401 GET /service")),
        patch(
            "backend.provision.fos_setup.object_storage_enabled",
            side_effect=lambda *a, **k: probe_called.append(True) or False,
        ),
    ):
        r_bad = _client().get("/api/provision/services", params={"token": "bad"})
        assert r_bad.status_code == 400
        assert r_bad.json()["detail"]["error"] == "list_services_failed"
        assert probe_called == []


def test_provision_validate_flows():
    assert _client().post("/api/provision/validate", json={"service_id": "svc"}).status_code == 400
    assert _client().post("/api/provision/validate", json={"token": "tok"}).status_code == 400
    assert _client().post("/api/provision/validate", json={"token": "", "service_id": "svc"}).status_code == 400
    assert _client().post("/api/provision/validate", json={"token": "tok", "service_id": ""}).status_code == 400

    # User token + sanitised defaults
    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: (
            {"id": "tok-1", "name": "Test Token", "user_id": "u1"}
            if "/tokens/self" in p
            else {"name": "My Production CDN"}
        ),
    ):
        resp = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc.with/specials!"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["service_name"] == "My Production CDN"
        assert not any(ch in body["defaults"]["fos_bucket_name"] for ch in ("/", ".", "!"))
        assert body["token_info"]["type"] == "user"

    # Automation token (no user_id)
    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: {"id": "tok-bot", "name": "Bot Token"} if "/tokens/self" in p else {"name": "X"},
    ):
        resp_bot = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})
        assert resp_bot.json()["token_info"]["type"] == "automation"

    # /tokens/self failure swallowed -> empty token_info
    def _fail_tok(m, p, *a, **kw):
        if p == "/tokens/self":
            raise RuntimeError("token endpoint down")
        return {"name": "My Service"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fail_tok):
        resp_swallow = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc-abc"})
        assert resp_swallow.status_code == 200
        assert resp_swallow.json()["token_info"] == {}


# ─── /api/provision/check-domain & _check_domain_available ───────────────────


@pytest.mark.parametrize(
    ("query", "mock_ret", "expected_avail", "expected_call_arg"),
    [
        ("prefix=-bad", None, False, None),
        ("prefix=foo_bar", None, False, None),
        ("prefix=valid-prefix-1", (True, None), True, "valid-prefix-1.global.ssl.fastly.net"),
        ("prefix=my.custom.domain.com&is_custom=true", (True, None), True, "my.custom.domain.com"),
        (
            "prefix=my.custom.domain.com&is_custom=true",
            (False, "Domain already registered or in use"),
            False,
            "my.custom.domain.com",
        ),
    ],
)
def test_check_domain_routes(query, mock_ret, expected_avail, expected_call_arg):
    assert _client().get("/api/provision/check-domain?prefix=").status_code in (400, 422)
    with patch("backend.routers.provision._check_domain_available", return_value=mock_ret) as mock_check:
        resp = _client().get(f"/api/provision/check-domain?{query}")
    assert resp.status_code == 200
    assert resp.json()["available"] is expected_avail
    if expected_call_arg:
        mock_check.assert_called_once_with(expected_call_arg)


def test_check_domain_available_helper_branches():
    fake_resp = MagicMock()
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=fake_resp), patch("backend.utils.telemetry.tracked_call"):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
        assert ok is False and "in use" in (reason or "").lower()

    err_unprov = urllib.error.HTTPError(
        url="https://x",
        code=404,
        msg="not found",
        hdrs={},  # type: ignore[arg-type]
        fp=BytesIO(b"... Please check that this domain has been added to a service. ..."),
    )
    with patch("urllib.request.urlopen", side_effect=err_unprov), patch("backend.utils.telemetry.tracked_call"):
        assert _check_domain_available("test.global.ssl.fastly.net") == (True, None)

    err_other = urllib.error.HTTPError(
        url="https://x",
        code=404,
        msg="not found",
        hdrs={},
        fp=BytesIO(b"some other body"),  # type: ignore[arg-type]
    )
    with patch("urllib.request.urlopen", side_effect=err_other), patch("backend.utils.telemetry.tracked_call"):
        ok_o, reason_o = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_o is False and "already registered" in (reason_o or "")

    with (
        patch("urllib.request.urlopen", side_effect=urllib.error.URLError("dns failed")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_dns, reason_dns = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_dns is True and "DNS/Connection" in (reason_dns or "")

    with (
        patch("urllib.request.urlopen", side_effect=RuntimeError("unexpected")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_ex, reason_ex = _check_domain_available("foo.example")
        assert ok_ex is False and "unexpected" in (reason_ex or "")


# ─── /api/provision/check-fos & /terraform/* ─────────────────────────────────


def test_check_fos_success_and_sample_get():
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = [{"Contents": []}, {"Contents": []}]
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client) as get_client:
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
    assert resp.status_code == 200 and resp.json()["ok"] is True
    get_client.assert_called_once_with("ak", "sk", "us-east-1", bucket_name="b", context="provision:check-fos")
    assert [c.kwargs["Prefix"] for c in fake_client.list_objects_v2.call_args_list] == ["raw/request/", "raw/rum/"]

    fake_client2 = MagicMock()
    fake_client2.list_objects_v2.return_value = {
        "Contents": [{"Key": "raw/request/year=2026/month=09/day=22/hour=13/minute=00/object.gz"}]
    }
    fake_client2.get_object.return_value = {"Body": MagicMock()}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client2):
        resp2 = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )
    assert resp2.status_code == 200 and resp2.json()["ok"] is True
    fake_client2.get_object.assert_called_once_with(
        Bucket="b",
        Key="raw/request/year=2026/month=09/day=22/hour=13/minute=00/object.gz",
        Range="bytes=0-0",
    )


@pytest.mark.parametrize(
    ("exc", "expected_in_msg"),
    [
        (
            botocore.exceptions.ClientError({"Error": {"Code": "AccessDenied", "Message": "raw"}}, "ListObjectsV2"),
            "Access Denied",
        ),
        (
            botocore.exceptions.ClientError(
                {"Error": {"Code": "InvalidAccessKeyId", "Message": "raw"}}, "ListObjectsV2"
            ),
            "Access Denied",
        ),
        (
            botocore.exceptions.ClientError(
                {"Error": {"Code": "SignatureDoesNotMatch", "Message": "raw"}}, "ListObjectsV2"
            ),
            "Access Denied",
        ),
        (
            botocore.exceptions.ClientError({"Error": {"Code": "NoSuchBucket", "Message": "raw"}}, "ListObjectsV2"),
            "Bucket not found",
        ),
        (
            botocore.exceptions.ClientError(
                {"Error": {"Code": "IllegalLocationConstraintException", "Message": "raw"}}, "ListObjectsV2"
            ),
            "Region mismatch",
        ),
        (RuntimeError("EndpointConnectionError: Could not connect to the endpoint URL"), "verify the FOS Region"),
        (RuntimeError("weird unknown error xyz"), "weird unknown error xyz"),
    ],
)
def test_check_fos_error_mappings(exc, expected_in_msg):
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = exc
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert expected_in_msg in body["error"]


def test_terraform_preview_and_export():
    fake_files = {"main.tf": 'resource "fastly_service_vcl" "x" {}\n', "instructions": "# Setup steps\n"}
    with patch("backend.utils.terraform_gen.generate_terraform", return_value=fake_files):
        prev = _client().post(
            "/api/provision/terraform/preview",
            json={
                "service_id": "svc-1",
                "fos_bucket": "b",
                "cdn_service_name": "CDN",
                "cdn_url": "https://x.global.ssl.fastly.net",
                "fos_region": "us-east-1",
            },
        )
        assert prev.status_code == 200 and "main.tf" in prev.json()

        exp = _client().post("/api/provision/terraform/export", json={"service_id": "svc"})
        assert exp.status_code == 200
        assert exp.headers["content-type"] == "application/zip"
        assert "fastly-log-analysis-terraform.zip" in exp.headers["content-disposition"]
        names = zipfile.ZipFile(io.BytesIO(exp.content)).namelist()
        assert "README.md" in names and "main.tf" in names and "instructions" not in names


# ─── /api/provision/ingest ───────────────────────────────────────────────────


def test_ingest_validation_and_key_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    # 1. Missing token -> 400
    assert _client().post("/api/provision/ingest", json={"service_id": "svc"}).status_code == 400

    # 2. Bad log_period -> 400 invalid_log_period
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=ValueError("unknown period: fortnight")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc")),
    ):
        r_period = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc", "log_period": "1 fortnight"},
        )
        assert r_period.status_code == 400
        assert r_period.json()["detail"]["error"] == "invalid_log_period"

    # 3. Creates new FOS key when none provided
    new_key = {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "kid"}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", return_value=new_key) as mock_ensure,
        patch("backend.provision.write_service_config") as mock_write,
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_new = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "fos_region": "us-east-1"},
        )
        assert r_new.status_code == 200
        mock_ensure.assert_called_once()
        persisted = mock_write.call_args[0][0]
        assert persisted["fos_access_key_id"] == "AKNEW" and persisted["fos_secret_access_key"] == "SKNEW"

    # 4. ensure_fos_access_key failure -> 400 ensure_access_key_failed
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", side_effect=RuntimeError("rate limited")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_err = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b"},
        )
        assert r_err.status_code == 400
        assert r_err.json()["detail"]["error"] == "ensure_access_key_failed"

    # 5. Uses provided keys without calling find/create
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key") as mock_find,
        patch("backend.provision.ensure_fos_access_key") as mock_ensure2,
        patch("backend.provision.write_service_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_prov = _client().post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "fos_access_key": "EXISTING_AK",
                "fos_secret_key": "EXISTING_SK",
            },
        )
        assert r_prov.status_code == 200
        mock_find.assert_not_called()
        mock_ensure2.assert_not_called()


def test_ingest_rerun_preserves_faro_pin_and_cdn_secret(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-1",
        {
            "service_id": "svc-1",
            "cdn_secret": "persisted-cdn-secret",
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
    captured: dict = {}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch(
            "backend.provision.ensure_fos_access_key",
            return_value={"access_key": "AK", "secret_key": "SK", "id": "kid"},
        ),
        patch("backend.provision.write_service_config", side_effect=lambda st: captured.update(state=st)),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        resp = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "rum_enabled": True},
        )
        assert resp.status_code == 200
        assert captured["state"]["cdn_secret"] == "persisted-cdn-secret"
        assert captured["state"]["rum"]["faro_version"] == "9.9.9"
        assert captured["state"]["rum"]["faro_content_hash"] == "existing-hash"

        # Explicit faro_version overrides on-disk pin
        resp_override = _client().post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "rum_enabled": True,
                "rum": {"faro_version": "1.0.0"},
            },
        )
        assert resp_override.status_code == 200
        assert captured["state"]["rum"]["faro_version"] == "1.0.0"


# ─── /api/provision/execute ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("payload_extra", "parse_period_effect", "domain_avail", "expected_err_sub"),
    [
        ({"fos_bucket_name": "invalid_underscore_name"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "bad--bucket"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "-leading-hyphen"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "valid-bucket", "fos_prefix": "x'; DROP TABLE--"}, 60, (True, None), "prefix"),
        (
            {"fos_bucket_name": "valid-bucket", "log_period": "fortnight"},
            ValueError("unknown: fortnight"),
            (True, None),
            "invalid_log_period",
        ),
        (
            {"fos_bucket_name": "valid-bucket", "cdn_url": "https://taken.global.ssl.fastly.net"},
            60,
            (False, "Domain already registered or in use"),
            "unavailable",
        ),
    ],
)
def test_provision_execute_validation_rejections(payload_extra, parse_period_effect, domain_avail, expected_err_sub):
    pp_kw = (
        {"side_effect": parse_period_effect}
        if isinstance(parse_period_effect, Exception)
        else {"return_value": parse_period_effect}
    )
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", **pp_kw),
        patch("backend.routers.provision._check_domain_available", return_value=domain_avail),
    ):
        r = _client().post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-1", **payload_extra},
        )
    assert r.status_code == 400
    assert expected_err_sub in r.json()["detail"]["error"].lower()


def test_provision_execute_dns_fallback_and_faro_version_threading():
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "done", "message": "ok"}

    with (
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
        r1 = _client().post(
            "/api/provision/execute",
            json={
                "token": "tok",
                "service_id": "svc-1",
                "fos_bucket_name": "valid-bucket",
                "cdn_url": "https://x.global.ssl.fastly.net",
                "rum_enabled": True,
                "faro_version": "1.2.3",
            },
        )
        assert r1.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == "1.2.3"

        r2 = _client().post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-1", "fos_bucket_name": "valid-bucket", "rum_enabled": True},
        )
        assert r2.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == DEFAULT_FARO_VERSION


# ─── /api/provision/check-config ─────────────────────────────────────────────

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


def test_check_config_logging_and_cdn_permutations():
    by_name = {entry[0]: entry for entry in _CDN_SNIPPETS}
    assert _CDN_NO_CACHE_404_SNIPPET_NAME in by_name
    _name, snippet_type, content, _priority = by_name[_CDN_NO_CACHE_404_SNIPPET_NAME]
    assert snippet_type == "fetch"
    assert 'req.method == "GET"' in content and 'req.method == "HEAD"' in content
    assert "beresp.status == 404" in content and "set beresp.cacheable = false;" in content

    # 1. No matching logging endpoint
    side_no_ep = _check_config_fastly_router(
        endpoints=[{"name": "other", "bucket_name": "different-bucket"}],
        snippets=[],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_no_ep):
        body = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert body["logging_service"]["ok"] is False
        assert "No S3 logging endpoint" in body["logging_service"]["details"]

    # 2. Missing core logging snippets + missing CDN perf snippets
    side_missing_snip = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": "Fastly Log Analysis Capture"}],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": "iceberg-metadata-pointer-ttl"}],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_missing_snip):
        res = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "missing CORE snippets" in res["logging_service"]["details"]
        assert res["cdn_service"]["ok"] is False
        assert "cdn-no-cache-404" in res["cdn_service"]["details"]

    # 3. Full logging + optional origin snippets + full CDN
    side_full = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS + _OPTIONAL_SNIPPETS],
        dicts=[{"name": "fos_credentials"}, {"name": "cdn_auth"}],
        backends=[{"address": "x.fastlystorage.app"}],
        cdn_snippets=[{"name": s} for s in _REQUIRED_CDN_SNIPPETS],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_full):
        res_full = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_full["logging_service"]["ok"] is True
        assert "3 origin metric snippets" in res_full["logging_service"]["details"]
        assert res_full["cdn_service"]["ok"] is True
        assert "fully configured" in res_full["cdn_service"]["details"]

    # 4. No response condition warns + CDN missing dicts/backend
    side_warn = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": None}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[],
        backends=[],
        cdn_snippets=[],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_warn):
        res_warn = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_warn["logging_service"]["ok"] is True
        assert "no response condition" in res_warn["logging_service"]["details"].lower()
        assert res_warn["cdn_service"]["ok"] is False
        assert "fos_credentials" in res_warn["cdn_service"]["details"]

    # 5. Logging & CDN exceptions captured in details
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly 503")):
        res_err = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "Fastly 503" in res_err["logging_service"]["details"]
        assert "Fastly 503" in res_err["cdn_service"]["details"]


# ─── /api/provision/teardown, product helpers, /reconcile, JSON content-type ─


def test_teardown_endpoint_flows():
    with patch("backend.config.load_config", return_value=None):
        resp_404 = _client().post("/api/provision/teardown", json={})
        assert resp_404.status_code == 404
        assert "No service config" in resp_404.json()["detail"]["error"]

    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "name": "Test Svc",
        "provisioning": {},
    }
    sync_calls: list[bool] = []
    fake_scheduler = MagicMock()
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent-config.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab", side_effect=lambda: sync_calls.append(True)),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.core.iceberg.clear_source_caches"),
        patch(
            "backend.provision.perform_teardown",
            return_value=iter([{"type": "status", "message": "removed s3 bucket"}]),
        ),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": True},
        )
        assert resp.status_code == 200
        assert "removed s3 bucket" in resp.text
        assert '"type": "done"' in resp.text
        assert "Cron jobs updated" in resp.text
        assert sync_calls == [True]
        fake_scheduler.reload.assert_called_once()

    def _boom(state, token, opts=None):
        raise RuntimeError("Fastly API rate-limited")
        yield

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler", return_value=MagicMock()),
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.provision.perform_teardown", side_effect=_boom),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp_err = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": False},
        )
        assert resp_err.status_code == 200
        assert '"type": "error"' in resp_err.text
        assert "Fastly API rate-limited" in resp_err.text


def test_product_and_object_storage_helpers():
    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "object_storage"}}):
        assert object_storage_enabled("tok") is True
    with patch(
        "backend.provision.fos_setup.fastly",
        side_effect=RuntimeError("HTTP 404 GET /enabled-products/v1/object_storage"),
    ):
        assert object_storage_enabled("tok") is False
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 503 GET x")):
        assert object_storage_enabled("tok") is True
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("Network error on GET x")):
        assert object_storage_enabled("tok") is True

    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "kv_store"}}) as m:
        assert product_enabled("tok", "kv_store") is True
        assert m.call_args.args[:2] == ("GET", "/enabled-products/v1/kv_store")
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 404 not found")):
        assert product_enabled("tok", "kv_store") is False
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 502 bad gateway")):
        assert product_enabled("tok", "kv_store") is True

    with patch("backend.provision.fos_setup.product_enabled", return_value=True) as m_del:
        assert fos_setup.object_storage_enabled("tok") is True
        assert m_del.call_args.args == ("tok", "object_storage")


def test_reconcile_route_and_json_content_type():
    post_op = app.openapi()["paths"]["/api/provision/reconcile"]["post"]
    assert [p["name"] for p in post_op.get("parameters", []) if p.get("in") == "query"] == []
    assert "requestBody" in post_op

    with patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile:
        resp = _client().post("/api/provision/reconcile", json={})
        assert resp.status_code == 400
        mock_reconcile.assert_not_called()

    for ct in (b"application/json", b"application/json; charset=utf-8"):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"content-type", ct)]}, lambda: None)
        _require_json_content_type(req)

    for bad_hdrs in ([(b"content-type", b"text/plain")], []):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": bad_hdrs}, lambda: None)
        with pytest.raises(HTTPException) as exc:
            _require_json_content_type(req)
        assert exc.value.status_code == 415
