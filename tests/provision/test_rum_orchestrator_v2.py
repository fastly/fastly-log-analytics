"""Tests for RUM orchestration using declarative reconciliation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum, rum_vcl_fingerprint, upgrade_faro_version


def _seed(cfg: dict) -> None:
    svcconfig.save_config(cfg["service_id"], dict(cfg))


def _disk(service_id: str = "srv_test") -> dict:
    return json.loads(svcconfig.config_path(service_id).read_text())


_FOS_CFG = {
    "fos_bucket": "my-bucket",
    "fos_region": "us-east-1",
    "fos_access_key_id": "test_access_key",
    "fos_secret_access_key": "test_secret_key",
}


class TestEnableRumReusesConfiguredBucket:
    """enable_rum() reuses the FOS bucket already in config; it never provisions one.

    The imperative ensure_fos_bucket step was replaced by declarative
    reconciliation in c65c9da7: the bucket comes from request logging (or a
    prior RUM enable), and a missing bucket/credential is a hard error before
    any state is saved.
    """

    def test_enable_rum_uses_configured_bucket_without_provisioning(self):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}
        status_messages: list[str] = []

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ) as mock_faro:
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

                                result = enable_rum(service_id, "test_token", status_cb=status_messages.append)

        mock_bucket.assert_not_called()
        mock_faro.assert_called_once()
        mock_reconcile.assert_called_once()
        mock_upload.assert_called_once()
        assert result["logging_service_active_version"] == 2
        assert result["activated"] is True
        assert any("Using FOS bucket 'my-bucket'" in m for m in status_messages)

    def test_enable_rum_already_enabled_returns_early(self):
        """Verify early return if RUM already enabled."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "rum_enabled_at": "2026-08-05T10:00:00+00:00",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        mock_load.return_value = cfg

                        # Call
                        result = enable_rum(service_id, "test_token")

                        # Verify bucket/upload/reconcile were NOT called
                        mock_bucket.assert_not_called()
                        mock_upload.assert_not_called()
                        mock_reconcile.assert_not_called()

                        # Verify result
                        assert result["activated"] is False
                        assert result["logging_service_active_version"] == 5


class TestEnableRumRollbackOnBucketFailure:
    """enable_rum() fails closed before saving anything when FOS settings are absent."""

    @pytest.mark.parametrize("missing", ["fos_bucket", "fos_region", "fos_access_key_id", "fos_secret_access_key"])
    def test_enable_rum_missing_single_fos_setting_raises_before_any_save(self, missing):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}
        del cfg[missing]

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                    ) as mock_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            with pytest.raises(RuntimeError, match="missing FOS configuration"):
                                enable_rum(service_id, "test_token")

        mock_save.assert_not_called()
        mock_faro.assert_not_called()
        mock_upload.assert_not_called()
        mock_reconcile.assert_not_called()
        assert cfg["rum_enabled"] is False

    def test_enable_rum_rollback_on_faro_bundle_upload_failure(self):
        """Verify config is rolled back if the Faro bundle upload fails.

        The bundle upload runs BEFORE reconcile and is still a blocking
        step (the VCL the reconciler is about to activate must not route to
        a bundle that was never uploaded), so a failure here must still
        roll back and never reach reconcile.
        """
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload,
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Faro bundle upload failed"),
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            mock_reconcile.return_value = MagicMock(activated_version=2)

            with pytest.raises(RuntimeError, match="Faro bundle upload failed"):
                enable_rum(service_id, "test_token")

            # Verify reconcile was NOT called, and the tracker JS was never
            # uploaded (it only runs after a successful reconcile)
            mock_reconcile.assert_not_called()
            mock_upload.assert_not_called()

        saved = _disk(service_id)
        assert saved["rum_enabled"] is False
        assert "rum_vcl_sha" not in saved

    def test_enable_rum_tracker_upload_failure_after_reconcile_is_non_blocking(self):
        """Verify a tracker-JS upload failure AFTER a successful reconcile does
        NOT raise and does NOT roll back config.

        This is the ordering fix's core safety property: by the time the
        tracker upload runs, the Fastly side has already activated
        successfully and is live. Rolling back local config at that point
        would desync it from reality (Fastly thinks RUM is enabled; config
        would claim it isn't) for a problem that self-heals on the next
        reconcile/cron tick.
        """
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload,
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.9.0"},
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            mock_upload.side_effect = RuntimeError("FOS unreachable")
            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

            # Must NOT raise.
            result = enable_rum(service_id, "test_token")

            mock_reconcile.assert_called_once()
            mock_upload.assert_called_once()

        # No rollback: config still says enabled.
        assert _disk(service_id)["rum_enabled"] is True
        assert result["activated"] is True
        assert result["logging_service_active_version"] == 2

    def test_enable_rum_no_config_raises_error(self):
        """Verify RuntimeError is raised if service config not found."""
        service_id = "srv_nonexistent"

        with patch("backend.config.load_config") as mock_load:
            mock_load.return_value = None

            with pytest.raises(RuntimeError, match="No config found"):
                enable_rum(service_id, "test_token")


class TestEnableRumUploadArgs:
    """Test that enable_rum() calls upload_rum_tracker_js with correct arguments."""

    def test_enable_rum_upload_called_with_correct_args(self):
        """Verify upload_rum_tracker_js is called with service_id, token, and status_cb."""
        service_id = "srv_test"
        token = "fastly_test_token_12345"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_load.return_value = cfg
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2)

                                # Call
                                enable_rum(service_id, token, status_cb=mock_status_cb)

                                # Verify upload was called with correct arguments
                                mock_upload.assert_called_once_with(service_id, token, status_cb=mock_status_cb)

    def test_enable_rum_upload_called_without_status_cb(self):
        """Verify upload_rum_tracker_js is called with status_cb=None when no callback provided."""
        service_id = "srv_test"
        token = "fastly_test_token_12345"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_load.return_value = cfg
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2)

                                # Call without status_cb
                                enable_rum(service_id, token)

                                # Verify upload was called with status_cb=None
                                mock_upload.assert_called_once_with(service_id, token, status_cb=None)


class TestEnableRumStatusCallback:
    """status_cb is threaded through every enable step for UI updates."""

    def test_enable_rum_passes_status_cb_to_every_step(self):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}

        def status_cb(msg):
            return None

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                    ) as mock_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

                            result = enable_rum(service_id, "test_token", status_cb=status_cb)

        assert mock_faro.call_args.kwargs["status_cb"] is status_cb
        assert mock_reconcile.call_args.kwargs["status_cb"] is status_cb
        assert mock_upload.call_args.kwargs["status_cb"] is status_cb
        assert result["activated"] is True


class TestDisableRumDeletesJs:
    """Test that disable_rum() calls JS deletion before reconciliation."""

    def test_disable_rum_deletes_js(self):
        """Verify JS deletion is called after config update but before reconciliation."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config") as mock_write:
                with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        # Setup
                        mock_load.return_value = cfg
                        mock_delete.return_value = None
                        mock_reconcile.return_value = MagicMock(activated_version=6)

                        # Call
                        result = disable_rum(service_id, "test_token", status_cb=None)

                        # Verify delete was called with correct args
                        mock_delete.assert_called_once_with(service_id, "test_token", status_cb=None)

                        # Verify reconcile was called after delete
                        assert mock_reconcile.called

                        # Verify result
                        assert result["logging_service_active_version"] == 6
                        assert result["deactivated"] is True

    def test_disable_rum_already_disabled_returns_early(self):
        """Verify early return if RUM already disabled (no JS deletion)."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                    mock_load.return_value = cfg

                    # Call
                    result = disable_rum(service_id, "test_token")

                    # Verify delete/reconcile were NOT called
                    mock_delete.assert_not_called()
                    mock_reconcile.assert_not_called()

                    # Verify result
                    assert result["deactivated"] is False
                    assert result["logging_service_active_version"] == 5

    def test_disable_rum_with_status_cb(self):
        """Verify status_cb is passed through for UI updates."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        mock_load.return_value = cfg
                        mock_delete.return_value = None
                        mock_reconcile.return_value = MagicMock(activated_version=6)

                        # Call
                        disable_rum(service_id, "test_token", status_cb=mock_status_cb)

                        # Verify status_cb was called with delete status
                        mock_delete.assert_called_once()
                        call_args = mock_delete.call_args
                        assert call_args.kwargs["status_cb"] == mock_status_cb


class TestDisableRumContinuesOnDeleteFailure:
    """Test that disable_rum() continues reconciliation if JS deletion fails (non-blocking)."""

    def test_disable_rum_continues_on_delete_failure(self):
        """Verify that if JS deletion fails, VCL reconciliation still proceeds."""
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": True, "last_activated_version": 5, **_FOS_CFG})

        with (
            patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete,
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            # Setup: delete fails but reconcile still succeeds
            mock_delete.side_effect = RuntimeError("FOS bucket unreachable")
            mock_reconcile.return_value = MagicMock(activated_version=6)

            # Call and expect NO exception (non-blocking)
            result = disable_rum(service_id, "test_token")

            # Verify delete was called (and failed)
            mock_delete.assert_called_once()

            # Verify reconcile was STILL called (non-blocking)
            mock_reconcile.assert_called_once()

            # Verify result is valid (didn't rollback)
            assert result["logging_service_active_version"] == 6
            assert result["deactivated"] is True

        # Config was NOT rolled back (non-blocking): it stays disabled.
        assert _disk(service_id)["rum_enabled"] is False


class TestRumOrchestratorBypassActivation:
    """Test that enable_rum() and disable_rum() respect the activate=False parameter."""

    def test_enable_rum_bypasses_activation(self):
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                # Reconciler returned a draft version but no activated version
                                mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=3)

                                result = enable_rum(service_id, "test_token", activate=False)

                                mock_reconcile.assert_called_once_with(
                                    service_id, "test_token", dry_run=False, status_cb=None, activate=False
                                )
                                assert result["logging_service_active_version"] == 3
                                assert result["activated"] is False

    def test_disable_rum_bypasses_activation(self):
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_assets.delete_rum_tracker_js"):
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        # Reconciler returned a draft version but no activated version
                        mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=6)

                        result = disable_rum(service_id, "test_token", activate=False)

                        mock_reconcile.assert_called_once_with(
                            service_id, "test_token", dry_run=False, status_cb=None, activate=False
                        )
                        assert result["logging_service_active_version"] == 6
                        assert result["deactivated"] is False


class TestEnableRumWithFaroVersion:
    """Test enable_rum()'s optional faro_version parameter."""

    def _cfg(self, service_id="srv_test", **overrides):
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }
        cfg.update(overrides)
        return cfg

    def test_enable_rum_faro_version_uploads_bundle_before_reconcile_and_tracker_after(self):
        """Pin the ordering, not just the outcome: the Faro bundle must upload
        BEFORE reconcile (the VCL about to be activated routes to it), and the
        tracker JS must upload AFTER reconcile (the tracker's dependency route
        only exists once that reconcile has activated) — never the reverse,
        which is the exact ordering hazard this fix closes."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)
        call_order = []

        def mock_upload(*args, **kwargs):
            call_order.append("upload_js")
            return {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}

        async def mock_download_upload(*args, **kwargs):
            call_order.append("upload_faro")
            return {"version": "2.9.0", "bytes_uploaded": 5000}

        def mock_reconcile(*args, **kwargs):
            call_order.append("reconcile")
            return MagicMock(activated_version=2, draft_version=None)

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js", side_effect=mock_upload):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        side_effect=mock_download_upload,
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile
                        ):
                            result = enable_rum(service_id, "test_token", faro_version="2.9.0")

        assert call_order == ["upload_faro", "reconcile", "upload_js"]
        assert result["activated"] is True

    def test_enable_rum_faro_version_persisted_before_reconcile(self):
        """Verify cfg["rum"]["faro_version"] is on disk by the time reconcile_vcl_state
        runs, since the generator reads it from the saved config."""
        service_id = "srv_test"
        _seed(self._cfg(service_id))

        def mock_reconcile(*args, **kwargs):
            # At the moment reconcile runs, the pinned version must already be persisted.
            assert _disk(service_id)["rum"]["faro_version"] == "2.9.0"
            return MagicMock(activated_version=2, draft_version=None)

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.9.0"},
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile),
        ):
            enable_rum(service_id, "test_token", faro_version="2.9.0")

    def test_enable_rum_invalid_faro_version_rejected_before_any_work(self):
        """Verify a malformed faro_version is rejected before config is even loaded."""
        with patch("backend.config.load_config") as mock_load:
            with pytest.raises(ValueError, match="faro_version must be a plain"):
                enable_rum("srv_test", "test_token", faro_version="not-a-version")
            mock_load.assert_not_called()

    def test_enable_rum_faro_upload_failure_restores_previous_faro_version(self):
        """Verify config is rolled back to the pre-attempt faro_version (and
        rum_enabled=False) if the Faro bundle upload fails, without erasing a
        key another writer added while the upload was in flight."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, rum={"faro_version": "1.0.0"}))

        async def _fail_after_concurrent_write(*_a, **_kw):
            svcconfig.mutate_config(service_id, lambda c: c.update(unrelated_marker="present-after-reload"))
            raise RuntimeError("unpkg unreachable")

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                side_effect=_fail_after_concurrent_write,
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            with pytest.raises(RuntimeError, match="unpkg unreachable"):
                enable_rum(service_id, "test_token", faro_version="2.0.0")
            mock_reconcile.assert_not_called()

        last_saved = _disk(service_id)
        assert last_saved["rum_enabled"] is False
        assert last_saved["rum"]["faro_version"] == "1.0.0"
        assert last_saved["unrelated_marker"] == "present-after-reload"

    def test_enable_rum_faro_reconcile_failure_restores_previous_faro_version(self):
        """Verify config is rolled back to the pre-attempt faro_version if
        reconcile_vcl_state fails after the bundle was already uploaded,
        keeping a concurrent writer's key."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, rum={"faro_version": "1.0.0"}))

        def _fail_after_concurrent_write(*_a, **_kw):
            svcconfig.mutate_config(service_id, lambda c: c.update(unrelated_marker="present-after-reload"))
            raise RuntimeError("VCL validation failed")

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.0.0"},
            ),
            patch(
                "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                side_effect=_fail_after_concurrent_write,
            ),
        ):
            with pytest.raises(RuntimeError, match="VCL validation failed"):
                enable_rum(service_id, "test_token", faro_version="2.0.0")

        last_saved = _disk(service_id)
        assert last_saved["rum_enabled"] is False
        assert last_saved["rum"]["faro_version"] == "1.0.0"
        assert last_saved["unrelated_marker"] == "present-after-reload"

    def test_enable_rum_missing_fos_config_raises_before_any_save(self):
        """Verify the FOS bucket/credentials check runs BEFORE any config
        mutation is persisted (task-6 review Important finding) — cfg["rum"]
        (and rum_enabled) must be left exactly as it was on disk when FOS
        isn't configured yet, not just "an exception propagated"."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "rum": {"faro_version": "1.0.0"},
            "last_activated_version": 1,
            # Deliberately missing fos_bucket / fos_region / fos_access_key_id /
            # fos_secret_access_key.
        }
        original_rum_block = dict(cfg["rum"])

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro"
                    ) as mock_download_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            with pytest.raises(RuntimeError, match="missing FOS configuration"):
                                enable_rum(service_id, "test_token", faro_version="2.0.0")

        # Nothing was ever persisted, so cfg on "disk" (our mock) is provably
        # unchanged — not just that the exception propagated.
        mock_save.assert_not_called()
        mock_upload.assert_not_called()
        mock_download_upload.assert_not_called()
        mock_reconcile.assert_not_called()
        assert cfg["rum"] == original_rum_block
        assert cfg["rum_enabled"] is False

    def test_enable_rum_without_faro_version_defaults_to_default_faro_version(self):
        """Omitting faro_version must NOT leave RUM enabled-but-unpinned:
        the tracker JS unconditionally loads /js/faro-sdk.js with no CDN
        fallback, so enabling RUM must always pin and upload a bundle."""
        service_id = "srv_test"
        _seed(self._cfg(service_id))
        pinned_at_reconcile = []

        def mock_reconcile(*args, **kwargs):
            pinned_at_reconcile.append(_disk(service_id)["rum"]["faro_version"])
            return MagicMock(activated_version=2, draft_version=None)

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.core.faro_versions.fetch_available_faro_versions",
                new_callable=AsyncMock,
                return_value=[DEFAULT_FARO_VERSION],
            ),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": DEFAULT_FARO_VERSION},
            ) as mock_download_upload,
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile),
        ):
            enable_rum(service_id, "test_token")

        mock_download_upload.assert_called_once_with(service_id, DEFAULT_FARO_VERSION, "test_token", status_cb=None)
        assert pinned_at_reconcile == [DEFAULT_FARO_VERSION]


class TestUpgradeFaroVersion:
    """Test upgrade_faro_version() orchestration."""

    def _cfg(self, service_id="srv_test", faro_version="1.0.0", **overrides):
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "cdn_service_id": "cdn_test_service",
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "rum": {"faro_version": faro_version, "faro_content_hash": "abc123"},
            "last_activated_version": 5,
        }
        cfg.update(overrides)
        return cfg

    def test_upgrade_faro_version_happy_path(self):
        """Verify the full sequence: upload -> reconcile -> purge -> cleanup,
        and that the result reports both the old and new version."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")
        call_order = []

        async def mock_download_upload(*args, **kwargs):
            call_order.append("upload")
            return {"version": "2.0.0"}

        def mock_reconcile(*args, **kwargs):
            call_order.append("reconcile")
            return MagicMock(activated_version=7, draft_version=None)

        async def mock_cleanup(*args, **kwargs):
            call_order.append("cleanup")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    side_effect=mock_download_upload,
                ):
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge:
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                side_effect=mock_cleanup,
                            ):
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token")

        assert call_order == ["upload", "reconcile", "cleanup"]
        mock_purge.assert_called_once()
        assert result["previous_version"] == "1.0.0"
        assert result["version"] == "2.0.0"
        assert result["logging_service_active_version"] == 7
        assert result["activated"] is True

    def test_upgrade_faro_version_invalid_version_rejected_before_any_work(self):
        """Verify a malformed version is rejected before config is even loaded."""
        with patch("backend.config.load_config") as mock_load:
            with pytest.raises(ValueError, match="faro_version must be a plain"):
                upgrade_faro_version("srv_test", "not-a-version", "test_token")
            mock_load.assert_not_called()

    def test_upgrade_faro_version_refused_when_rum_not_enabled(self):
        """Verify upgrade is refused with a clear error if RUM isn't enabled."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)
        cfg["rum_enabled"] = False

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro") as mock_download_upload:
                with pytest.raises(RuntimeError, match="RUM is not enabled"):
                    upgrade_faro_version(service_id, "2.0.0", "test_token")
                mock_download_upload.assert_not_called()

    def test_upgrade_faro_version_upload_failure_reraises_without_reconcile(self):
        """Verify an upload failure re-raises and never reaches reconcile."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                side_effect=RuntimeError("unpkg unreachable"),
            ):
                with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                    with pytest.raises(RuntimeError, match="unpkg unreachable"):
                        upgrade_faro_version(service_id, "2.0.0", "test_token")
                    mock_reconcile.assert_not_called()

    def test_upgrade_faro_version_reconcile_failure_restores_previous_version(self):
        """Verify config is rolled back to the previous faro_version (and its
        content hash) if reconcile fails after the new bundle was uploaded."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, faro_version="1.0.0"))

        async def _persist_new_pin(*_a, **_kw):
            # download_and_upload_faro persists the new pin itself on success.
            def _apply(c):
                c["rum"].update(faro_version="2.0.0", faro_content_hash="new-hash")

            svcconfig.mutate_config(service_id, _apply)
            return {"version": "2.0.0"}

        with (
            patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", side_effect=_persist_new_pin),
            patch(
                "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                side_effect=RuntimeError("VCL validation failed"),
            ),
        ):
            with pytest.raises(RuntimeError, match="VCL validation failed"):
                upgrade_faro_version(service_id, "2.0.0", "test_token")

        saved_cfg = _disk(service_id)
        assert saved_cfg["rum"]["faro_version"] == "1.0.0"
        assert saved_cfg["rum"]["faro_content_hash"] == "abc123"

    def test_upgrade_faro_version_purge_failure_does_not_fail_upgrade(self):
        """Verify a surrogate-key purge failure is a warning, not an upgrade failure."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=7, draft_version=None),
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key",
                            side_effect=RuntimeError("purge API unreachable"),
                        ):
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ):
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token")

        assert result["version"] == "2.0.0"
        assert result["activated"] is True

    def test_upgrade_faro_version_never_uploads_tracker_js(self):
        """Verify upgrade_faro_version never calls upload_rum_tracker_js.

        The tracker body is a static wrapper that always loads the constant
        path /js/faro-sdk.js regardless of pinned version (see
        generate_rum_tracker_js's docstring) — a version bump has nothing
        new to publish there. This pins that claim as a regression test
        rather than leaving it as an unverified assumption.
        """
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=7, draft_version=None),
                    ):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key"):
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ):
                                with patch(
                                    "backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"
                                ) as mock_upload_tracker:
                                    upgrade_faro_version(service_id, "2.0.0", "test_token")

        mock_upload_tracker.assert_not_called()

    def test_upgrade_faro_version_activate_false_skips_cleanup_and_sha_update(self):
        """F-6 audit finding: with activate=False, the draft is compiled and
        validated but never went live — the OLD bundle is still what the
        live VCL serves. cleanup_old_faro_versions(keep_current=True) reads
        the NEW (not-yet-live) pin from config and would delete the OLD,
        still-live bundle if run here; the stored rum_vcl_sha must likewise
        stay pointed at the deployed (old) VCL, not the compiled-but-inactive
        draft."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=None, draft_version=8),
                    ):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge:
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ) as mock_cleanup:
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token", activate=False)

        mock_cleanup.assert_not_called()
        # Purge is unconditional (best-effort, harmless either way) — only
        # cleanup and the rum_vcl_sha update are activation-gated.
        mock_purge.assert_called_once()
        assert result["activated"] is False
        assert result["logging_service_active_version"] == 8
        # save_config must not have been called with a payload carrying a
        # freshly-recomputed rum_vcl_sha (the activation-gated write this
        # fix adds) — every save_config call in this flow before that point
        # is download_and_upload_faro's own persistence, which this test
        # mocks out entirely, so ANY save_config call here would have to be
        # the (incorrectly, for activate=False) added sha update.
        mock_save.assert_not_called()


class TestEnableRumSkipsTrackerWhenNotActivated:
    """Test that enable_rum() does not publish the tracker when the VCL
    route it depends on never actually went live."""

    def test_enable_rum_activate_false_skips_tracker_upload(self):
        """Verify upload_rum_tracker_js is NOT called when activate=False.

        A draft that was compiled and validated but never activated does
        not serve /js/faro-sdk.js — publishing the tracker in that state
        would reproduce the exact ordering hazard this fix closes.
        """
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=3)

                                result = enable_rum(service_id, "test_token", activate=False)

        mock_upload.assert_not_called()
        assert result["activated"] is False


class TestRumVclFingerprint:
    """F-4 audit finding: rum_vcl_fingerprint must actually reflect a pinned
    faro_version, since that's the whole point of using it for /rum/status's
    vcl_drift — before this fix it called generate_rum_vcl() alone, whose
    Phase 1 output never varies with faro_version at all, so a service
    whose deployed VCL was generated with faro_version=None (never
    reconciled after a version was pinned) would report no drift."""

    def _cfg(self, **overrides):
        cfg = {
            "service_id": "srv_test",
            "fos_region": "us-east-1",
            "cdn_shield": "",
            "rum": {"faro_version": "2.9.0"},
        }
        cfg.update(overrides)
        return cfg

    def test_different_faro_versions_produce_different_fingerprints(self):
        """The whole point of the fix: two services differing ONLY in
        pinned faro_version must get different fingerprints."""
        cfg_a = self._cfg(rum={"faro_version": "2.9.0"})
        cfg_b = self._cfg(rum={"faro_version": "3.0.0"})

        assert rum_vcl_fingerprint("srv_test", cfg_a) != rum_vcl_fingerprint("srv_test", cfg_b)

    def test_unpinned_and_pinned_produce_different_fingerprints(self):
        """A service with faro_version=None (VCL missing the /js/faro-sdk.js
        route entirely) must not collide with one that has a version
        pinned — this is the exact drift case F-4 was about."""
        unpinned = self._cfg(rum={})
        pinned = self._cfg(rum={"faro_version": "2.9.0"})

        assert rum_vcl_fingerprint("srv_test", unpinned) != rum_vcl_fingerprint("srv_test", pinned)

    def test_same_faro_version_is_deterministic(self):
        """Same inputs, same output — this is a cache-comparison fingerprint,
        it must not vary run to run."""
        cfg = self._cfg()
        assert rum_vcl_fingerprint("srv_test", cfg) == rum_vcl_fingerprint("srv_test", cfg)

    def test_reads_faro_version_from_disk_when_cfg_omitted(self, monkeypatch):
        """Callers that don't pass cfg (e.g. /rum/status) must get a
        fingerprint reflecting the service's current on-disk config."""
        disk_cfg = self._cfg(rum={"faro_version": "1.2.3"})
        monkeypatch.setattr("backend.config.load_config", lambda service_id: disk_cfg)

        from_disk = rum_vcl_fingerprint("srv_test")
        explicit = rum_vcl_fingerprint("srv_test", disk_cfg)

        assert from_disk == explicit

    def test_cdn_shield_falls_back_to_shield_map_by_region(self):
        """No explicit cdn_shield — shield selection must derive from
        fos_region (mirroring _generate_rum_section_vcl's own fallback), not
        silently default to a fixed shield regardless of region."""
        cfg_iad = self._cfg(fos_region="us-east-1", cdn_shield="")
        cfg_lon = self._cfg(fos_region="uk-east-1", cdn_shield="")

        assert rum_vcl_fingerprint("srv_test", cfg_iad) != rum_vcl_fingerprint("srv_test", cfg_lon)
