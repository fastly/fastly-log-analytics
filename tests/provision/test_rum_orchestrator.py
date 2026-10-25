"""Tests for RUM asset management and RUM orchestrator v2 provisioning."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.provision import rum_assets
from backend.provision.rum_assets import (
    cleanup_old_faro_versions,
    detect_faro_version_change,
    download_and_upload_faro,
    faro_bundle_intact,
    faro_tracker_ready,
    generate_rum_tracker_js,
    upload_rum_tracker_js,
)
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from tests._helpers.config_fakes import (
    config_lock_held as _lock_held,
)
from tests._helpers.config_fakes import (
    mutate_via_save,
)
from tests._helpers.config_fakes import (
    read_persisted_config as _persisted,
)

FAKE_CFG = {
    "service_id": "svc_test",
    "fos_access_key_id": "AKIAFAKEEXAMPLE",
    "fos_secret_access_key": "fakeSecretKeyExample",
    "fos_bucket": "fake-test-bucket",
    "fos_region": "us-east-1",
}
_FOS_CFG = {
    "fos_bucket": "my-bucket",
    "fos_region": "us-east-1",
    "fos_access_key_id": "test_access_key",
    "fos_secret_access_key": "test_secret_key",
}
SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"
OTHER_BUNDLE = b"!function(e){/* a different, re-released build */}(window);"
_REAL_CLIENT = httpx.Client


def _mock_client_factory(handler):
    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_CLIENT(*args, **kwargs)

    return factory


@pytest.fixture
def mock_fos(monkeypatch):
    def install(handler):
        monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

    return install


@pytest.fixture
def mock_fetch_bundle(monkeypatch):
    def install(bundle: bytes = SAMPLE_BUNDLE, *, raise_if_called: bool = False):
        async def fake_fetch(version: str) -> bytes:
            if raise_if_called:
                raise AssertionError(f"fetch_faro_bundle must not be called for {version!r}")
            return bundle

        monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)

    return install


def _config_pair(monkeypatch, cfg: dict):
    saved: dict = {}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(
        rum_assets.svcconfig,
        "mutate_config",
        mutate_via_save(lambda sid, new_cfg: saved.update(service_id=sid, cfg=new_cfg)),
    )
    return saved


def _seed(cfg: dict) -> None:
    svcconfig.save_config(cfg["service_id"], dict(cfg))


def _disk(service_id: str = "srv_test") -> dict:
    return json.loads(svcconfig.config_path(service_id).read_text())


# ─── Faro Asset Download, Upload, Change Detection & Cleanup ─────────────────


async def test_download_and_upload_faro_flows(monkeypatch, mock_fos, mock_fetch_bundle):
    # 1. Happy path + preserves existing rum keys
    cfg = {**FAKE_CFG, "rum": {"sampling_rate": 0.5, "faro_version": "2.8.0", "faro_content_hash": "stale"}}
    saved = _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    seen: dict = {}
    mock_fos(lambda req: (seen.update(method=req.method, url=str(req.url), body=req.content), httpx.Response(200))[1])

    result = await download_and_upload_faro("svc_test", "2.9.0", "tok")
    expected_hash = hashlib.sha256(SAMPLE_BUNDLE).hexdigest()
    expected_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    assert result["version"] == "2.9.0"
    assert result["path"] == "rum/faro-web-sdk-v2.9.0.iife.js"
    assert result["bytes_uploaded"] == len(SAMPLE_BUNDLE)
    assert result["content_hash"] == expected_hash
    assert result["fos_key"] == "s3://fake-test-bucket/rum/faro-web-sdk-v2.9.0.iife.js"
    assert seen["method"] == "PUT" and seen["body"] == SAMPLE_BUNDLE
    assert saved["cfg"]["rum"]["sampling_rate"] == 0.5
    assert saved["cfg"]["rum"]["faro_version"] == "2.9.0"
    assert saved["cfg"]["rum"]["faro_content_hash"] == expected_hash
    assert saved["cfg"]["rum"]["faro_fos_etag_md5"] == expected_md5

    # 2. Deleted service config is not resurrected
    resurrected: list = []
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: dict(FAKE_CFG))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: None)
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: resurrected.append(a))
    await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert resurrected == []

    # 3. Upload failure raises RuntimeError and does not persist
    saved_fail = _config_pair(monkeypatch, FAKE_CFG)
    mock_fos(lambda req: httpx.Response(500, content=b"upstream error"))
    with pytest.raises(RuntimeError):
        await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert saved_fail == {}


@pytest.mark.parametrize(
    ("concurrent_version", "expected_updated", "expected_stored_version"),
    [
        ("3.0.0", False, "3.0.0"),
        ("2.9.0", True, "2.9.0"),
    ],
)
async def test_download_and_upload_faro_compare_and_set(
    monkeypatch, mock_fos, mock_fetch_bundle, concurrent_version, expected_updated, expected_stored_version
):
    store = {"svc_test": {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_content_hash": "old-hash"}}}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: copy.deepcopy(store.get(sid)))
    monkeypatch.setattr(
        rum_assets.svcconfig,
        "mutate_config",
        mutate_via_save(lambda sid, new_cfg: store.__setitem__(sid, copy.deepcopy(new_cfg))),
    )
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda req: httpx.Response(200))

    store["svc_test"]["rum"]["faro_version"] = concurrent_version
    result = await download_and_upload_faro("svc_test", "2.9.0", "tok", expected_current_version="2.9.0")
    assert result["config_updated"] is expected_updated
    assert store["svc_test"]["rum"]["faro_version"] == expected_stored_version


@pytest.mark.parametrize(
    ("rum_block", "bundle_bytes", "raise_if_called", "expected"),
    [
        (None, SAMPLE_BUNDLE, True, True),
        (
            {"faro_version": "2.8.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            SAMPLE_BUNDLE,
            True,
            True,
        ),
        ({"faro_version": "2.9.0", "faro_content_hash": None}, SAMPLE_BUNDLE, True, True),
        (
            {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            OTHER_BUNDLE,
            False,
            True,
        ),
        (
            {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            SAMPLE_BUNDLE,
            False,
            False,
        ),
    ],
)
async def test_detect_faro_version_change(
    monkeypatch, mock_fetch_bundle, rum_block, bundle_bytes, raise_if_called, expected
):
    cfg = dict(FAKE_CFG)
    if rum_block is not None:
        cfg["rum"] = rum_block
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(bundle_bytes, raise_if_called=raise_if_called)
    assert await detect_faro_version_change("svc_test", "2.9.0") is expected


_LIST_RESPONSE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>fake-test-bucket</Name>
  <Contents><Key>rum/faro-web-sdk-v2.8.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.9.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.10.0.iife.js</Key></Contents>
</ListBucketResult>
"""


@pytest.mark.parametrize(
    ("keep_current", "expected_deleted"),
    [
        (True, ["rum/faro-web-sdk-v2.8.0.iife.js", "rum/faro-web-sdk-v2.9.0.iife.js"]),
        (
            False,
            [
                "rum/faro-web-sdk-v2.10.0.iife.js",
                "rum/faro-web-sdk-v2.8.0.iife.js",
                "rum/faro-web-sdk-v2.9.0.iife.js",
            ],
        ),
    ],
)
async def test_cleanup_old_faro_versions(monkeypatch, mock_fos, keep_current, expected_deleted):
    _config_pair(monkeypatch, {**FAKE_CFG, "rum": {"faro_version": "2.10.0", "faro_content_hash": "irrelevant"}})
    deleted_keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            assert "list-type=2" in str(request.url)
            return httpx.Response(200, content=_LIST_RESPONSE_TEMPLATE.encode())
        if request.method == "DELETE":
            deleted_keys.append(request.url.path.split("/", 2)[-1])
            return httpx.Response(204)
        raise AssertionError(f"unexpected {request.method}")

    mock_fos(handler)
    await cleanup_old_faro_versions("svc_test", keep_current=keep_current)
    assert sorted(deleted_keys) == expected_deleted


async def test_cleanup_old_faro_versions_swallows_error(monkeypatch, mock_fos):
    _config_pair(monkeypatch, {**FAKE_CFG, "rum": {"faro_version": "2.10.0"}})
    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("FOS unreachable", request=req)))
    assert await cleanup_old_faro_versions("svc_test") is None


def test_generate_rum_tracker_js_invariants():
    js = generate_rum_tracker_js("svc_test")
    assert "Faro.ErrorsInstrumentation" in js
    assert "if (Faro.ErrorInstrumentation)" not in js
    assert "new Faro.ErrorInstrumentation()" not in js
    assert "console.warn" in js and "no error instrumentation export found" in js
    assert "script.src = '/js/faro-sdk.js';" in js
    assert "cdn.jsdelivr.net" not in js and "unpkg.com" not in js
    assert not re.search(r"https?://", js)


def test_faro_tracker_readiness_and_upload_gates(monkeypatch, mock_fos, caplog):
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

    # 1. No version pinned
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"enabled": True}})
    assert ready is False and "no Faro version pinned" in reason

    # 2. Bundle missing in FOS (404)
    mock_fos(lambda req: httpx.Response(404))
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"}})
    assert ready is False and "not present/intact in FOS" in reason

    # 3. Intact bundle in FOS (200 + ETag match)
    cfg_ready = {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": etag_md5}}
    mock_fos(lambda req: httpx.Response(200, headers={"ETag": f'"{etag_md5}"'}))
    ready, reason = faro_tracker_ready(cfg_ready)
    assert ready is True and reason == ""
    assert faro_bundle_intact(cfg_ready, "2.9.0") is True

    # 4. Transient HEAD error logs concisely without traceback
    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("connection failed")))
    with caplog.at_level(logging.WARNING):
        assert faro_bundle_intact(cfg_ready, "2.9.0") is False
    rec = next(r for r in caplog.records if "integrity HEAD check failed" in r.getMessage())
    assert rec.exc_info is None and "ConnectError" in rec.getMessage()

    # 5. upload_rum_tracker_js skips when unpinned or bundle missing, publishes when ready
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: {})
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: {**FAKE_CFG, "rum": {"enabled": True}})
    mock_fos(lambda req: (_ for _ in ()).throw(AssertionError(f"unexpected {req.method}")))
    res_unpinned = upload_rum_tracker_js("svc_test", "tok")
    assert res_unpinned["skipped"] is True and res_unpinned["bytes_uploaded"] == 0

    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: dict(cfg_ready))
    mock_fos(lambda req: httpx.Response(404) if req.method == "HEAD" else (_ for _ in ()).throw(AssertionError()))
    assert upload_rum_tracker_js("svc_test", "tok")["skipped"] is True

    put_bodies: dict[str, bytes] = {}

    def _ready_handler(req: httpx.Request) -> httpx.Response:
        if req.method == "HEAD" and "faro-web-sdk-v" in req.url.path:
            return httpx.Response(200, headers={"ETag": f'"{etag_md5}"'})
        if req.method == "HEAD" and req.url.path.endswith("rum-tracker.js"):
            return httpx.Response(404)
        if req.method == "PUT" and req.url.path.endswith("rum-tracker.js"):
            put_bodies["tracker"] = req.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_ready_handler)
    res_pub = upload_rum_tracker_js("svc_test", "tok")
    assert "skipped" not in res_pub and res_pub["bytes_uploaded"] > 0
    assert "/js/faro-sdk.js" in put_bodies["tracker"].decode()


# ─── End-to-End Integration & Self-Heal Cron ─────────────────────────────────


@pytest.fixture
def temp_config_dir(tmp_path, monkeypatch):
    config_dir = Path(tmp_path) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)
    return config_dir


def test_faro_self_hosting_end_to_end_flows(monkeypatch, temp_config_dir, mock_fos, mock_fetch_bundle):
    service_id = "test_service_id"
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(
        json.dumps(
            {
                "service_id": service_id,
                "service_name": "Test Service",
                "rum_enabled": False,
                "last_activated_version": 1,
                **_FOS_CFG,
            }
        )
    )
    chosen_version = "2.8.5"
    mock_fetch_bundle(SAMPLE_BUNDLE)
    uploads: dict[str, bytes] = {}
    etag = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

    def _handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if req.method == "PUT" and "faro-web-sdk-v" in path:
            uploads["bundle"] = req.content
            return httpx.Response(200)
        if req.method == "HEAD" and "faro-web-sdk-v" in path:
            return httpx.Response(200, headers={"ETag": f'"{etag}"'})
        if req.method == "HEAD" and path.endswith("rum-tracker.js"):
            return httpx.Response(404)
        if req.method == "PUT" and path.endswith("rum-tracker.js"):
            uploads["tracker"] = req.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_handler)
    status_messages: list[str] = []
    with patch(
        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
        return_value=MagicMock(activated_version=2, draft_version=None, changes_applied={}),
    ) as mock_reconcile:
        result = enable_rum(service_id, "test_token", status_cb=status_messages.append, faro_version=chosen_version)

    cfg = json.loads(config_path.read_text())
    assert cfg["rum_enabled"] is True and "rum_enabled_at" in cfg
    assert cfg["rum"]["faro_version"] == chosen_version
    assert uploads["bundle"] == SAMPLE_BUNDLE
    tracker_js = uploads["tracker"].decode()
    assert "/js/faro-sdk.js" in tracker_js and not re.search(r"https?://", tracker_js)
    assert mock_reconcile.call_count == 1 and result["activated"] is True
    assert any("Using FOS bucket" in m for m in status_messages)
    assert any("Uploading RUM tracker JS" in m for m in status_messages)
    assert any("Reconciling VCL state" in m for m in status_messages)

    # Upgrade Faro version end-to-end
    mock_fetch_bundle(OTHER_BUNDLE)
    with (
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=3, draft_version=None, changes_applied={}),
        ),
        patch("backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock) as mock_clean,
    ):
        up_res = upgrade_faro_version(service_id, "2.9.0", "test_token")
    assert up_res["previous_version"] == chosen_version and up_res["version"] == "2.9.0"
    assert json.loads(config_path.read_text())["rum"]["faro_version"] == "2.9.0"
    mock_clean.assert_called_once_with(service_id, keep_current=True)

    # Disable RUM end-to-end
    with (
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_del,
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=6, changes_applied={}),
        ),
    ):
        dis_res = disable_rum(service_id, "test_token")
    assert dis_res["deactivated"] is True and dis_res["logging_service_active_version"] == 6
    assert json.loads(config_path.read_text())["rum_enabled"] is False
    mock_del.assert_called_once()


def test_reconcile_cron_self_heals_unpinned_service_without_thrashing(
    monkeypatch, temp_config_dir, mock_fos, mock_fetch_bundle
):
    from backend.provision.declarative.generators import generate_consolidated_snippet
    from backend.provision.declarative.state import FeatureState

    service_id = "self_heal_service"
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(
        json.dumps(
            {
                "service_id": service_id,
                "service_name": "Self Heal Service",
                "rum_enabled": True,
                "log_period": 60,
                "fastly_api_key": "test_token",
                "last_activated_version": 2,
                "rum": {},
                **_FOS_CFG,
            }
        )
    )
    mock_fetch_bundle(SAMPLE_BUNDLE)
    expected_etag = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    counts = {"put": 0, "head": 0}

    def _handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in req.url.path:
            counts["put"] += 1
            return httpx.Response(200)
        if req.method == "HEAD" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in req.url.path:
            counts["head"] += 1
            return httpx.Response(200, headers={"ETag": f'"{expected_etag}"'})
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_handler)
    monkeypatch.setattr(rum_sync_mod.time, "time", lambda: 1_800_000_000.0)
    reconcile_calls: list[tuple] = []
    purge_calls: list[tuple] = []
    monkeypatch.setattr(
        "backend.provision.declarative.reconciler.reconcile_vcl_state",
        lambda sid, tok, *a, **kw: (
            reconcile_calls.append((sid, tok)),
            MagicMock(activated_version=3, draft_version=None),
        )[1],
    )
    monkeypatch.setattr(
        "backend.core.fastly.client.fastly",
        lambda method, path, *, token, expect_empty=False, **kw: purge_calls.append((method, path, token)),
    )

    rum_sync_mod._reconcile_faro_bundle(service_id, None)
    cfg = json.loads(config_path.read_text())
    assert cfg["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert counts == {"put": 1, "head": 1}
    assert reconcile_calls == [(service_id, "test_token")]
    assert purge_calls == [("POST", f"/service/{service_id}/purge/rum-faro-sdk", "test_token")]
    assert '"/js/faro-sdk.js"' in generate_consolidated_snippet(FeatureState.from_config(cfg), "vcl_recv")

    # Second tick: converged, no re-upload or re-reconcile
    rum_sync_mod._reconcile_faro_bundle(service_id, None)
    assert counts["put"] == 1 and reconcile_calls == [(service_id, "test_token")]


# ─── Orchestrator v2 Unit Tests (enable_rum / disable_rum / upgrade / sha) ───


def test_enable_and_disable_rum_early_return_and_missing_config():
    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": True, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res = enable_rum("srv_test", "tok")
        assert res["activated"] is False and res["logging_service_active_version"] == 5
        mock_rec.assert_not_called()

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": False, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res_dis = disable_rum("srv_test", "tok")
        assert res_dis["deactivated"] is False and res_dis["logging_service_active_version"] == 5
        mock_rec.assert_not_called()

    with patch("backend.config.load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            enable_rum("srv_nonexistent", "tok")


@pytest.mark.parametrize("missing", ["fos_bucket", "fos_region", "fos_access_key_id", "fos_secret_access_key"])
def test_enable_rum_missing_single_fos_setting_raises_before_any_save(missing):
    cfg = {
        "service_id": "srv_test",
        "rum_enabled": False,
        "rum": {"faro_version": "1.0.0"},
        "last_activated_version": 1,
        **_FOS_CFG,
    }
    del cfg[missing]
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config") as mock_save,
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock) as mock_faro,
    ):
        with pytest.raises(RuntimeError, match="missing FOS configuration"):
            enable_rum("srv_test", "test_token")
    mock_save.assert_not_called()
    mock_faro.assert_not_called()
    assert cfg["rum_enabled"] is False and cfg["rum"] == {"faro_version": "1.0.0"}


def test_enable_rum_ordering_and_non_blocking_tracker_upload():
    service_id = "srv_test"
    _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})
    call_order: list[str] = []

    async def _upload_faro(*_a, **_kw):
        call_order.append("upload_faro")
        return {"version": "2.9.0", "bytes_uploaded": 5000}

    def _reconcile(*_a, **_kw):
        assert _disk(service_id)["rum"]["faro_version"] == "2.9.0"
        call_order.append("reconcile")
        return MagicMock(activated_version=2, draft_version=None)

    def _upload_js(*_a, **_kw):
        call_order.append("upload_js")
        raise RuntimeError("FOS unreachable after activation")

    with (
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", side_effect=_upload_faro),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=_reconcile),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js", side_effect=_upload_js),
    ):
        result = enable_rum(service_id, "test_token", faro_version="2.9.0")

    assert call_order == ["upload_faro", "reconcile", "upload_js"]
    assert result["activated"] is True and _disk(service_id)["rum_enabled"] is True


def test_enable_and_disable_rum_activate_false_and_non_blocking_delete():
    service_id = "srv_test"
    _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})
    with (
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload_js,
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=3),
        ) as mock_rec,
    ):
        res_en = enable_rum(service_id, "test_token", activate=False)
        mock_rec.assert_called_once_with(service_id, "test_token", dry_run=False, status_cb=None, activate=False)
        mock_upload_js.assert_not_called()
        assert res_en["activated"] is False and res_en["logging_service_active_version"] == 3

    _seed({"service_id": service_id, "rum_enabled": True, "last_activated_version": 5, **_FOS_CFG})
    with (
        patch("backend.provision.rum_assets.delete_rum_tracker_js", side_effect=RuntimeError("FOS unreachable")),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=6),
        ) as mock_rec_dis,
    ):
        res_dis = disable_rum(service_id, "test_token", activate=False)
        mock_rec_dis.assert_called_once_with(service_id, "test_token", dry_run=False, status_cb=None, activate=False)
        assert res_dis["deactivated"] is False and res_dis["logging_service_active_version"] == 6
        assert _disk(service_id)["rum_enabled"] is False


def test_upgrade_faro_version_orchestration():
    # Invalid version rejected before config load
    with patch("backend.config.load_config") as mock_load:
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            enable_rum("srv_test", "tok", faro_version="not-a-version")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            upgrade_faro_version("srv_test", "not-a-version", "tok")
        mock_load.assert_not_called()

    # Refused when RUM not enabled
    with patch("backend.config.load_config", return_value={"service_id": "srv_test", "rum_enabled": False}):
        with pytest.raises(RuntimeError, match="RUM is not enabled"):
            upgrade_faro_version("srv_test", "2.0.0", "tok")

    # Happy path + purge failure non-blocking + never calls upload_rum_tracker_js + activate=False skips cleanup
    base_cfg = {
        "service_id": "srv_test",
        "rum_enabled": True,
        "cdn_service_id": "cdn_test_service",
        "rum": {"faro_version": "1.0.0", "faro_content_hash": "abc123"},
        "last_activated_version": 5,
        **_FOS_CFG,
    }
    call_order: list[str] = []
    with (
        patch("backend.config.load_config", return_value=dict(base_cfg)),
        patch("backend.config.save_config"),
        patch(
            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
            side_effect=AsyncMock(side_effect=lambda *a, **k: (call_order.append("upload"), {"version": "2.0.0"})[1]),
        ),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            side_effect=lambda *a, **k: (
                call_order.append("reconcile"),
                MagicMock(activated_version=7, draft_version=None),
            )[1],
        ),
        patch(
            "backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key", side_effect=RuntimeError("purge down")
        ),
        patch(
            "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
            side_effect=AsyncMock(side_effect=lambda *a, **k: call_order.append("cleanup")),
        ),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_tracker,
    ):
        res = upgrade_faro_version("srv_test", "2.0.0", "tok")
    assert call_order == ["upload", "reconcile", "cleanup"]
    mock_tracker.assert_not_called()
    assert res["previous_version"] == "1.0.0" and res["version"] == "2.0.0" and res["activated"] is True

    # activate=False skips cleanup and sha update
    with (
        patch("backend.config.load_config", return_value=dict(base_cfg)),
        patch("backend.config.save_config") as mock_save,
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=8),
        ),
        patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge,
        patch("backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock) as mock_clean,
    ):
        res_draft = upgrade_faro_version("srv_test", "2.0.0", "tok", activate=False)
    mock_clean.assert_not_called()
    mock_purge.assert_called_once()
    mock_save.assert_not_called()
    assert res_draft["activated"] is False and res_draft["logging_service_active_version"] == 8


def test_rum_vcl_fingerprint_invariants(monkeypatch):
    def _cfg(**overrides):
        return {
            "service_id": "srv_test",
            "fos_region": "us-east-1",
            "cdn_shield": "",
            "rum": {"faro_version": "2.9.0"},
            **overrides,
        }

    assert rum_vcl_fingerprint("srv_test", _cfg(rum={"faro_version": "2.9.0"})) != rum_vcl_fingerprint(
        "srv_test", _cfg(rum={"faro_version": "3.0.0"})
    )
    assert rum_vcl_fingerprint("srv_test", _cfg(rum={})) != rum_vcl_fingerprint(
        "srv_test", _cfg(rum={"faro_version": "2.9.0"})
    )
    c = _cfg()
    assert rum_vcl_fingerprint("srv_test", c) == rum_vcl_fingerprint("srv_test", c)
    assert rum_vcl_fingerprint("srv_test", _cfg(fos_region="us-east-1")) != rum_vcl_fingerprint(
        "srv_test", _cfg(fos_region="uk-east-1")
    )

    disk_cfg = _cfg(rum={"faro_version": "1.2.3"})
    monkeypatch.setattr("backend.config.load_config", lambda sid: disk_cfg)
    assert rum_vcl_fingerprint("srv_test") == rum_vcl_fingerprint("srv_test", disk_cfg)


# ─── Config Merge & Lock Hygiene (test_rum_orchestrator_v2_config_merge.py) ──

SID = "logsvc1"
OLD_VERSION = "1.0.0"
NEW_VERSION = "1.2.3"
_FOS = {"fos_bucket": "bkt", "fos_region": "us-east-1", "fos_access_key_id": "AK", "fos_secret_access_key": "SK"}


def _concurrent_writer(**top_level):
    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("rum", {})["concurrent_rum_key"] = "kept"

    assert not _lock_held(), "config lock held across a Fastly/FOS call"
    svcconfig.mutate_config(SID, _apply)


@pytest.fixture
def disabled_cfg():
    svcconfig.save_config(SID, {"service_id": SID, "keep_me": 1, "rum_enabled": False, **_FOS})
    return svcconfig.load_config(SID)


@pytest.fixture
def enabled_cfg():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "keep_me": 1,
            "rum_enabled": True,
            "rum_vcl_sha": "stale",
            "rum": {"faro_version": OLD_VERSION, "faro_content_hash": "oldhash", "faro_fos_etag_md5": "oldmd5"},
            **_FOS,
        },
    )
    return svcconfig.load_config(SID)


@pytest.fixture
def stubs():
    mod = "backend.provision.rum_orchestrator_v2"
    with (
        patch(f"{mod}.download_and_upload_faro", new_callable=AsyncMock) as upload,
        patch(f"{mod}.reconcile_vcl_state") as reconcile,
        patch(f"{mod}.upload_rum_tracker_js") as tracker,
        patch(f"{mod}.cleanup_old_faro_versions", new_callable=AsyncMock),
        patch(f"{mod}._purge_faro_surrogate_key"),
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as delete_js,
    ):
        reconcile.return_value = MagicMock(activated_version=7, draft_version=None)
        yield MagicMock(upload=upload, reconcile=reconcile, tracker=tracker, delete_js=delete_js)


def test_enable_and_disable_persist_owned_keys_without_holding_lock(disabled_cfg, stubs):
    salt = disabled_cfg["rum"]["cid_salt"]
    held: list[bool] = []

    async def _upload(*_a, **_kw):
        held.append(_lock_held())

    def _reconcile(*_a, **_kw):
        held.append(_lock_held())
        return MagicMock(activated_version=7, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    out = enable_rum(SID, "tok", faro_version=NEW_VERSION)
    assert held == [False, False]
    saved = _persisted()
    assert saved["rum_enabled"] is True and saved["rum_enabled_at"] == out["enabled_at"]
    assert saved["rum"]["faro_version"] == NEW_VERSION and saved["rum"]["cid_salt"] == salt
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved) and saved["keep_me"] == 1

    # Now disable and check lock + owned keys
    held.clear()
    stubs.delete_js.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    stubs.reconcile.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), MagicMock(activated_version=2))[1]
    disable_rum(SID, "tok")
    assert held == [False, False]
    saved_dis = _persisted()
    assert saved_dis["rum_enabled"] is False and "rum_vcl_sha" not in saved_dis and saved_dis["keep_me"] == 1


@pytest.mark.parametrize("fail_stage", ["upload", "reconcile"])
def test_enable_rollback_keeps_concurrent_writes_and_restores_pin(disabled_cfg, stubs, fail_stage):
    salt = disabled_cfg["rum"]["cid_salt"]
    if fail_stage == "upload":

        async def _boom(*_a, **_kw):
            _concurrent_writer(concurrent_key="kept")
            raise RuntimeError("upload failed")

        stubs.upload.side_effect = _boom
    else:

        def _boom(*_a, **_kw):
            _concurrent_writer(concurrent_key="kept")
            raise RuntimeError("reconcile failed")

        stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False and "rum_vcl_sha" not in saved
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept" and saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == salt

    # Also verify pre-existing pin restoration on reconcile failure
    svcconfig.save_config(SID, {"service_id": SID, "rum_enabled": False, "rum": {"faro_version": OLD_VERSION}, **_FOS})
    stubs.upload.side_effect = None
    stubs.reconcile.side_effect = RuntimeError("nope")
    with pytest.raises(RuntimeError):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)
    assert _persisted()["rum"]["faro_version"] == OLD_VERSION


def test_upgrade_and_disable_concurrent_write_preservation(enabled_cfg, stubs):
    # 1. Upgrade reconcile failure restores pin and keeps concurrent writes
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(
            SID,
            lambda c: c["rum"].update(
                faro_version=NEW_VERSION, faro_content_hash="newhash", faro_fos_etag_md5="newmd5"
            ),
        )

    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _boom
    with pytest.raises(RuntimeError, match="reconcile failed"):
        upgrade_faro_version(SID, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum"]["faro_version"] == OLD_VERSION
    assert saved["rum"]["faro_content_hash"] == "oldhash"
    assert saved["rum"]["faro_fos_etag_md5"] == "oldmd5"
    assert saved["rum"]["concurrent_rum_key"] == "kept" and saved["concurrent_key"] == "kept"

    # 2. Upgrade success refreshes fingerprint and keeps concurrent writes
    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        _concurrent_writer(concurrent_key="kept2"),
        MagicMock(activated_version=9, draft_version=None),
    )[1]
    upgrade_faro_version(SID, NEW_VERSION, "tok")
    saved_ok = _persisted()
    assert saved_ok["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved_ok) and saved_ok["rum_vcl_sha"] != "stale"
    assert saved_ok["concurrent_key"] == "kept2"

    # 3. Disable rollback restores enabled and keeps concurrent writes
    stubs.reconcile.side_effect = _boom
    with pytest.raises(RuntimeError, match="reconcile failed"):
        disable_rum(SID, "tok")
    saved_dis = _persisted()
    assert saved_dis["rum_enabled"] is True
    assert saved_dis["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved_dis)
    assert saved_dis["concurrent_key"] == "kept"
