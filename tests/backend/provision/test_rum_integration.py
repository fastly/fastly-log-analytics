"""Integration tests for full RUM enable/disable flow.

These tests verify the complete end-to-end flow:
- enable_rum: config write → bucket provision → JS upload → VCL reconciliation
- disable_rum: config write → JS deletion → VCL reconciliation

Real file I/O is tested; FOS operations are mocked.
"""

import datetime as _dt
import hashlib
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.provision import rum_assets
from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum, upgrade_faro_version


@pytest.fixture
def temp_config_dir(tmp_path, monkeypatch):
    """Create a temporary config directory and redirect backend.config to it."""

    from backend import config as svcconfig

    config_dir = Path(tmp_path) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)

    return config_dir


@pytest.fixture
def sample_service_config():
    """Return a minimal service config with FOS credentials."""
    return {
        "service_id": "test_service_id",
        "service_name": "Test Service",
        "rum_enabled": False,
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "test_access_key",
        "fos_secret_access_key": "test_secret_key",
        "last_activated_version": 1,
    }


@pytest.fixture
def initialized_config(temp_config_dir, sample_service_config):
    """Write sample config to disk and return the service_id."""
    service_id = sample_service_config["service_id"]
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(json.dumps(sample_service_config, indent=2))

    return service_id, config_path


class TestRUMIntegration:
    """End-to-end integration tests for RUM enable/disable flow."""

    def test_enable_rum_full_flow(self, temp_config_dir, initialized_config):
        """Test full enable_rum flow: config write → bucket → upload → reconcile.

        Verifies:
        - result["activated"] is True
        - result["logging_service_active_version"] == 2 (post-activate)
        - "enabled_at" in result
        - Bucket was called once
        - Upload was called once
        - Reconciliation was called once
        - All three stages fired status callbacks
        """
        service_id, config_path = initialized_config
        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
            with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                with patch(
                    "backend.core.faro_versions.fetch_available_faro_versions",
                    new_callable=AsyncMock,
                    return_value=[DEFAULT_FARO_VERSION],
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                        return_value={"version": DEFAULT_FARO_VERSION},
                    ) as mock_download_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            # Setup mocks
                            mock_bucket.return_value = None  # ensure_fos_bucket returns None
                            mock_upload.return_value = {
                                "path": "rum/rum-tracker.js",
                                "bytes_uploaded": 1000,
                                "fos_key": "s3://test-bucket/rum/rum-tracker.js",
                            }
                            mock_reconcile.return_value = MagicMock(
                                activated_version=2,
                                activated_version_set=True,
                                changes_applied={},
                            )

                            # Call enable_rum
                            result = enable_rum(service_id, token, status_cb=mock_status_cb)

                        # Verify config was written with rum_enabled=True and timestamp
                        config_content = config_path.read_text()
                        config = json.loads(config_content)
                        assert config["rum_enabled"] is True
                        assert "rum_enabled_at" in config
                        assert config["service_id"] == service_id
                        # No CDN fallback exists, so enabling RUM without an
                        # explicit faro_version must still pin and upload a
                        # self-hosted bundle rather than leaving it unpinned.
                        assert config["rum"]["faro_version"] == DEFAULT_FARO_VERSION

                        # Verify bucket provisioning was bypassed since bucket was already configured
                        assert mock_bucket.call_count == 0

                        # Verify JS upload was called once
                        assert mock_upload.call_count == 1
                        upload_call = mock_upload.call_args
                        assert upload_call[0][0] == service_id
                        assert upload_call[0][1] == token

                        # Verify the Faro Web SDK bundle was uploaded with the default version
                        mock_download_faro.assert_called_once_with(
                            service_id, DEFAULT_FARO_VERSION, token, status_cb=mock_status_cb
                        )

                        # Verify reconciliation was called once
                        assert mock_reconcile.call_count == 1

                        # Verify result
                        assert result["activated"] is True
                        assert result["logging_service_active_version"] == 2
                        assert "enabled_at" in result

                        # Verify status callbacks fired for all stages
                        assert len(status_messages) >= 3  # At least: bucket, upload, reconcile
                        assert any("Using FOS bucket" in msg for msg in status_messages)
                        assert any("Uploading RUM tracker JS" in msg for msg in status_messages)
                        assert any("Reconciling VCL state" in msg for msg in status_messages)

    def test_disable_rum_full_flow(self, temp_config_dir):
        """Test full disable_rum flow: config write → delete JS → reconcile.

        Verifies:
        - result["deactivated"] is True
        - result["logging_service_active_version"] == 6
        - Delete was called once
        - Reconciliation was called once
        - Status callbacks fired for deletion and reconciliation stages
        """
        # Create config with rum_enabled=True
        config = {
            "service_id": "test_service_id",
            "service_name": "Test Service",
            "rum_enabled": True,
            "rum_enabled_at": _dt.datetime.now(_dt.UTC).isoformat(),
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }
        service_id = config["service_id"]
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(json.dumps(config, indent=2))

        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                # Setup mocks
                mock_delete.return_value = None
                mock_reconcile.return_value = MagicMock(
                    activated_version=6,
                    changes_applied={},
                )

                # Call disable_rum
                result = disable_rum(service_id, token, status_cb=mock_status_cb)

                # Verify config was written with rum_enabled=False
                config_content = config_path.read_text()
                loaded_config = json.loads(config_content)
                assert loaded_config["rum_enabled"] is False

                # Verify JS deletion was called once
                assert mock_delete.call_count == 1
                delete_call = mock_delete.call_args
                assert delete_call[0][0] == service_id
                assert delete_call[0][1] == token

                # Verify reconciliation was called once
                assert mock_reconcile.call_count == 1

                # Verify result
                assert result["deactivated"] is True
                assert result["logging_service_active_version"] == 6

                # Verify status callbacks fired for all stages
                assert len(status_messages) >= 2  # At least: delete, reconcile
                assert any("Deleting RUM tracker JS" in msg for msg in status_messages)
                assert any("Reconciling VCL state" in msg for msg in status_messages)


# ── Task 10: full self-hosting chain, external I/O mocked only at the ─────
# ── transport boundary (never at download_and_upload_faro/rum_assets ──────
# ── themselves), so a regression in the FOS key path, the pinned-version ──
# ── persistence, or the tracker JS's external hostname would fail these. ──

# Shape-accurate stand-in for a real Faro IIFE bundle — same fixture data
# used by tests/backend/provision/test_rum_assets.py.
_SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"
_OTHER_BUNDLE = b"!function(e){/* a different, upgraded build */}(window);"

_REAL_HTTPX_CLIENT = httpx.Client


def _install_mock_transport(monkeypatch, handler) -> None:
    """Patch the shared ``httpx`` module's ``Client`` so every caller in the
    chain (rum_assets' FOS PUT/HEAD, rum_sync's FOS HEAD) is intercepted by
    ``handler`` — no bytes ever leave the process."""

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_HTTPX_CLIENT(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", factory)


def _install_mock_fetch_bundle(monkeypatch, bundle: bytes) -> None:
    """Stub the one real network call in the chain that isn't FOS: the
    npm/unpkg download inside ``backend.provision.rum_assets``."""

    async def fake_fetch(version: str) -> bytes:
        return bundle

    monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)


class TestFaroSelfHostingEndToEnd:
    """Task 10 integration coverage: provision with a chosen Faro version,
    upgrade to a different version, and the reconcile cron's self-heal —
    exercised through the real ``download_and_upload_faro``/
    ``upload_rum_tracker_js`` code paths with only FOS/npm/unpkg transport
    mocked, never the functions themselves.
    """

    def test_enable_rum_with_chosen_version_uploads_bundle_and_pins_config(
        self, monkeypatch, temp_config_dir, initialized_config
    ):
        """Provisioning with an explicit (non-default) Faro version must:
        - land the bundle in FOS at rum/faro-web-sdk-v{version}.iife.js
        - persist that exact version to cfg["rum"]["faro_version"]
        - upload a tracker JS that references the relative /js/faro-sdk.js
        """
        service_id, config_path = initialized_config
        chosen_version = "2.8.5"  # deliberately not DEFAULT_FARO_VERSION
        token = "test_token"

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)

        uploads: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and path.endswith(f"faro-web-sdk-v{chosen_version}.iife.js"):
                uploads["bundle"] = request.content
                return httpx.Response(200)
            if request.method == "HEAD" and path.endswith(f"faro-web-sdk-v{chosen_version}.iife.js"):
                # upload_rum_tracker_js's faro_tracker_ready gate HEADs the
                # bundle it just uploaded to confirm the route it references
                # is genuinely backed by real content in FOS.
                etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
                return httpx.Response(200, headers={"ETag": f'"{etag}"'})
            if request.method == "HEAD" and path.endswith("rum-tracker.js"):
                # Never uploaded before — force upload_rum_tracker_js past
                # its up-to-date short-circuit.
                return httpx.Response(404)
            if request.method == "PUT" and path.endswith("rum-tracker.js"):
                uploads["tracker"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(
                activated_version=2,
                draft_version=None,
                changes_applied={},
            )

            result = enable_rum(service_id, token, faro_version=chosen_version)

        # 1. Config persists the *chosen* version, not the default.
        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == chosen_version
        assert config["rum_enabled"] is True

        # 2. The bundle actually landed at the version-specific FOS key.
        assert uploads["bundle"] == _SAMPLE_BUNDLE

        # 3. The tracker JS references the relative, first-party path...
        tracker_js = uploads["tracker"].decode()
        assert "/js/faro-sdk.js" in tracker_js
        # ...and contains no external hostname anywhere (the critical
        # invariant — this must be impossible to silently regress).
        assert not re.search(r"https?://", tracker_js)
        assert "cdn.jsdelivr.net" not in tracker_js

        # 4. VCL was reconciled exactly once, and the result reports success.
        assert mock_reconcile.call_count == 1
        assert result["activated"] is True

    def test_generated_tracker_js_has_no_external_hostname_end_to_end(self, monkeypatch, temp_config_dir):
        """Dedicated regression test for the user's core requirement ("no
        external dependencies, everything served from the main service").
        Runs the real enable_rum → upload_rum_tracker_js chain (not just
        generate_rum_tracker_js in isolation) and asserts on the exact bytes
        that would be written to FOS and served to browsers.
        """
        service_id = "no_cdn_service"
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "No CDN Service",
                    "rum_enabled": False,
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "last_activated_version": 1,
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)

        captured_tracker_js: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and "faro-web-sdk-v" in path:
                return httpx.Response(200)
            if request.method == "HEAD" and "faro-web-sdk-v" in path:
                # upload_rum_tracker_js's faro_tracker_ready gate HEADs the
                # bundle it just uploaded to confirm the route it references
                # is genuinely backed by real content in FOS.
                etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
                return httpx.Response(200, headers={"ETag": f'"{etag}"'})
            if request.method == "HEAD" and path.endswith("rum-tracker.js"):
                return httpx.Response(404)
            if request.method == "PUT" and path.endswith("rum-tracker.js"):
                captured_tracker_js["body"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None, changes_applied={})
            enable_rum(service_id, "test_token")

        tracker_js = captured_tracker_js["body"].decode()
        assert tracker_js  # sanity: something was actually captured
        assert not re.search(r"https?://", tracker_js), "tracker JS must contain no absolute URL at all"
        assert "cdn.jsdelivr.net" not in tracker_js

    def test_upgrade_faro_version_uploads_new_bundle_and_reconciles(self, monkeypatch, temp_config_dir):
        """upgrade_faro_version to a different pinned version must upload the
        new bundle to its own FOS key, persist the new version to config,
        and invoke VCL reconcile — without disturbing the tracker JS (which
        never changes across a version bump)."""
        service_id = "upgrade_service"
        old_version = "2.8.5"
        new_version = "2.9.0"
        old_content_hash = hashlib.sha256(_SAMPLE_BUNDLE).hexdigest()
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "Upgrade Service",
                    "rum_enabled": True,
                    "rum_enabled_at": _dt.datetime.now(_dt.UTC).isoformat(),
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "last_activated_version": 2,
                    "rum": {
                        "faro_version": old_version,
                        "faro_content_hash": old_content_hash,
                    },
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _OTHER_BUNDLE)

        uploads: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and path.endswith(f"faro-web-sdk-v{new_version}.iife.js"):
                uploads["new_bundle"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(activated_version=3, draft_version=None, changes_applied={})
            # Cleanup of stale bundle versions is a best-effort, orthogonal
            # concern covered by tests/backend/provision/test_rum_assets.py;
            # stub it here so this test stays focused on the upload+config+
            # reconcile chain this task cares about.
            with patch(
                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock
            ) as mock_cleanup:
                result = upgrade_faro_version(service_id, new_version, "test_token")

        # The new bundle landed at the new version's own FOS key.
        assert uploads["new_bundle"] == _OTHER_BUNDLE

        # Config now reflects the new version (and the old one is gone).
        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == new_version
        assert config["rum"]["faro_content_hash"] == hashlib.sha256(_OTHER_BUNDLE).hexdigest()

        # VCL reconcile ran exactly once, and old-version cleanup was invoked.
        assert mock_reconcile.call_count == 1
        mock_cleanup.assert_called_once_with(service_id, keep_current=True)

        assert result["previous_version"] == old_version
        assert result["version"] == new_version
        assert result["activated"] is True

    def test_reconcile_cron_self_heals_unpinned_service_without_thrashing(self, monkeypatch, temp_config_dir):
        """A RUM-enabled service with no pinned Faro version at all (the
        pre-Task-2 state) must, on the first cron tick, adopt
        DEFAULT_FARO_VERSION, upload a bundle, persist the pin, AND
        reconcile the deployed VCL (F-4) — then on the very next tick do
        nothing further (no re-download, no re-upload, no re-reconcile)
        because it has converged.

        Also the "post-self-heal cron state" half of the missing invariant
        (VCL/FOS audit finding): once faro_version is pinned, the generated
        VCL for this service's config must actually contain a
        /js/faro-sdk.js route — combined with the FOS PUT assertion below
        (the object exists), this is the full invariant. Before the F-4 fix,
        the self-heal uploaded the bundle but never reconciled, so a
        service in this exact state would have a pinned version with NO
        route pointing at it."""
        service_id = "self_heal_service"
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "Self Heal Service",
                    "rum_enabled": True,
                    "log_period": 60,
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "fastly_api_key": "test_token",
                    "last_activated_version": 2,
                    "rum": {},
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)
        expected_etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

        put_count = 0
        head_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal put_count, head_count
            path = request.url.path
            if request.method == "PUT" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in path:
                put_count += 1
                return httpx.Response(200)
            if request.method == "HEAD" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in path:
                head_count += 1
                return httpx.Response(200, headers={"ETag": f'"{expected_etag}"'})
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)
        # Freeze time so the throttled upstream-drift check runs exactly
        # once (last_check starts at 0) and stays inside the window on the
        # second tick, keeping the "no thrashing" assertion deterministic.
        frozen_time = 1_800_000_000.0
        monkeypatch.setattr(rum_sync_mod.time, "time", lambda: frozen_time)

        # The self-heal's one-time VCL reconcile (F-4) would otherwise clone/
        # validate/activate a real Fastly service version — mock it, and
        # record calls so "one-time, not per-tick" is actually verified.
        reconcile_calls: list[tuple] = []

        def fake_reconcile(service_id_arg, token_arg, *args, **kwargs):
            reconcile_calls.append((service_id_arg, token_arg))
            return MagicMock(activated_version=3, draft_version=None)

        monkeypatch.setattr("backend.provision.declarative.reconciler.reconcile_vcl_state", fake_reconcile)
        # The surrogate-key purge after adoption would otherwise hit the
        # real Fastly API (this config has a real-shaped fastly_api_key).
        purge_calls: list[tuple] = []
        monkeypatch.setattr(
            "backend.core.fastly.client.fastly",
            lambda method, path, *, token, expect_empty=False, **kw: purge_calls.append((method, path, token)),
        )

        # Tick 1: self-heal adoption.
        rum_sync_mod._reconcile_faro_bundle(service_id, None)

        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == DEFAULT_FARO_VERSION
        assert put_count == 1
        assert head_count == 1
        assert reconcile_calls == [(service_id, "test_token")]
        assert purge_calls == [("POST", f"/service/{service_id}/purge/rum-faro-sdk", "test_token")]

        # Missing-invariant check: faro_version is now pinned, so the VCL
        # generated from this exact on-disk config must contain the
        # /js/faro-sdk.js route — combined with put_count == 1 above (the
        # FOS object exists), this is the full "pinned => route AND object"
        # invariant on the post-self-heal cron state.
        from backend.provision.declarative.generators import generate_consolidated_snippet
        from backend.provision.declarative.state import FeatureState

        state = FeatureState.from_config(config)
        recv_vcl = generate_consolidated_snippet(state, "vcl_recv")
        assert '"/js/faro-sdk.js"' in recv_vcl

        # Tick 2: already converged — no further FOS PUT, re-adoption, or
        # re-reconcile.
        rum_sync_mod._reconcile_faro_bundle(service_id, None)

        assert put_count == 1, "self-heal must not thrash on the next tick"
        assert reconcile_calls == [(service_id, "test_token")], "reconcile must not re-run once converged"
        config_after = json.loads(config_path.read_text())
        assert config_after["rum"]["faro_version"] == DEFAULT_FARO_VERSION
