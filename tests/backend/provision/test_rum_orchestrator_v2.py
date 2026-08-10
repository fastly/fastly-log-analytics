"""Tests for RUM orchestration using declarative reconciliation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum, upgrade_faro_version


class TestEnableRumProvisionsBucket:
    """Test that enable_rum() calls bucket provisioning as a blocking step."""

    @pytest.mark.skip(reason="Obsolete after declarative bucket provisioning bypass in c65c9da7")
    def test_enable_rum_provisions_bucket(self):
        """Verify bucket provisioning is called before JS upload."""
        # Mock config
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

        # Mock external calls
        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            # Setup
                            mock_load.return_value = cfg
                            mock_bucket.return_value = True  # Success
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2, activated_version_set=True)

                            # Call
                            result = enable_rum(service_id, "test_token", status_cb=None)

                            # Verify bucket provisioning was called with correct args
                            mock_bucket.assert_called_once_with(
                                name="my-bucket",
                                region="us-east-1",
                                access_key="test_access_key",
                                secret_key="test_secret_key",
                                status_cb=None,
                                service_id=service_id,
                            )

                            # Verify upload_rum_tracker_js was called after bucket
                            assert mock_upload.called
                            assert mock_reconcile.called

                            # Verify result
                            assert result["logging_service_active_version"] == 2
                            assert result["activated"] is True

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

    @pytest.mark.skip(reason="Obsolete after declarative bucket provisioning bypass in c65c9da7")
    def test_enable_rum_with_status_cb(self):
        """Verify status_cb is passed through for UI updates."""
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

        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            mock_load.return_value = cfg
                            mock_bucket.return_value = True
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2)

                            # Call
                            result = enable_rum(service_id, "test_token", status_cb=mock_status_cb)

                            # Verify status_cb was called with bucket status
                            mock_bucket.assert_called_once()
                            call_args = mock_bucket.call_args
                            assert call_args.kwargs["status_cb"] == mock_status_cb

                            # Verify result is valid
                            assert result["activated"] is True


class TestEnableRumRollbackOnBucketFailure:
    """Test that enable_rum() rolls back config if bucket provisioning fails."""

    @pytest.mark.skip(reason="Obsolete after declarative bucket provisioning bypass in c65c9da7")
    def test_enable_rum_rollback_on_bucket_failure(self):
        """Verify config is rolled back if bucket provisioning fails."""
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

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            # Setup
                            mock_load.return_value = cfg.copy()
                            mock_bucket.side_effect = RuntimeError("Bucket creation failed")
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2)

                            # Call and expect exception
                            with pytest.raises(RuntimeError, match="Bucket creation failed"):
                                enable_rum(service_id, "test_token")

                            # Verify config was saved twice:
                            # 1. First save: setting rum_enabled=True
                            # 2. Second save: rolling back to rum_enabled=False
                            assert mock_save.call_count == 2

                            # Verify second save (rollback) set rum_enabled=False
                            second_save_call = mock_save.call_args_list[1]
                            saved_cfg = second_save_call[0][1]
                            assert saved_cfg["rum_enabled"] is False

                            # Verify upload/reconcile were NOT called
                            mock_upload.assert_not_called()
                            mock_reconcile.assert_not_called()

    def test_enable_rum_rollback_on_upload_failure(self):
        """Verify config is rolled back if JS upload fails."""
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

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            # Setup
                            mock_load.return_value = cfg.copy()
                            mock_bucket.return_value = True
                            mock_upload.side_effect = RuntimeError("Upload failed")
                            mock_reconcile.return_value = MagicMock(activated_version=2)

                            # Call and expect exception
                            with pytest.raises(RuntimeError, match="Upload failed"):
                                enable_rum(service_id, "test_token")

                            # Verify config was rolled back
                            assert mock_save.call_count == 2
                            second_save_call = mock_save.call_args_list[1]
                            saved_cfg = second_save_call[0][1]
                            assert saved_cfg["rum_enabled"] is False

                            # Verify reconcile was NOT called
                            mock_reconcile.assert_not_called()

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
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            mock_load.return_value = cfg
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2)

                            # Call without status_cb
                            enable_rum(service_id, token)

                            # Verify upload was called with status_cb=None
                            mock_upload.assert_called_once_with(service_id, token, status_cb=None)


class TestEnableRumCallOrder:
    """Test that enable_rum() calls steps in the correct order."""

    @pytest.mark.skip(reason="Obsolete after declarative bucket provisioning bypass in c65c9da7")
    def test_enable_rum_bucket_provisioned_before_upload(self):
        """Verify bucket provisioning happens BEFORE JS upload."""
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

        call_order = []

        def mock_bucket(*args, **kwargs):
            call_order.append("bucket")
            return True

        def mock_upload(*args, **kwargs):
            call_order.append("upload")
            return {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}

        def mock_reconcile(*args, **kwargs):
            call_order.append("reconcile")
            return MagicMock(activated_version=2)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket", side_effect=mock_bucket):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js", side_effect=mock_upload):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile
                        ):
                            mock_load.return_value = cfg

                            # Call
                            enable_rum(service_id, "test_token")

                            # Verify call order: bucket -> upload -> reconcile
                            assert call_order == ["bucket", "upload", "reconcile"]


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
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        # Setup: delete fails but reconcile still succeeds
                        mock_load.return_value = cfg
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

                        # Verify config was NOT rolled back (non-blocking)
                        # config save count: 1 for disabling rum_enabled
                        assert mock_save.call_count == 1


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

    def test_enable_rum_faro_version_uploads_bundle_before_reconcile(self):
        """Verify a pinned faro_version uploads the bundle (in order: JS tracker,
        then Faro bundle, then reconcile) rather than being silently dropped."""
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

        assert call_order == ["upload_js", "upload_faro", "reconcile"]
        assert result["activated"] is True

    def test_enable_rum_faro_version_persisted_before_reconcile(self):
        """Verify cfg["rum"]["faro_version"] is on disk by the time reconcile_vcl_state
        runs, since the generator reads it from the saved config."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)
        saved_snapshots = []

        def mock_save(_service_id, saved_cfg):
            saved_snapshots.append(dict(saved_cfg))

        def mock_reconcile(*args, **kwargs):
            # At the moment reconcile runs, the most recent save must already
            # have persisted the pinned version.
            assert saved_snapshots[-1]["rum"]["faro_version"] == "2.9.0"
            return MagicMock(activated_version=2, draft_version=None)

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config", side_effect=mock_save):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                        return_value={"version": "2.9.0"},
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile
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
        rum_enabled=False) if the Faro bundle upload fails.

        Uses two DISTINCT config objects for the initial load vs. the
        rollback's reload (rather than one shared object returned twice) so
        this actually exercises "reload from disk before restoring" instead
        of silently passing via object-identity aliasing — a real disk read
        never returns the same object twice."""
        service_id = "srv_test"
        cfg_initial = self._cfg(service_id, rum={"faro_version": "1.0.0"})
        # Distinct object representing what's actually on disk by the time
        # the rollback reloads it (mirrors upgrade_faro_version's own reload).
        cfg_on_disk_at_rollback = dict(cfg_initial)
        cfg_on_disk_at_rollback["unrelated_marker"] = "present-after-reload"

        with patch("backend.config.load_config", side_effect=[cfg_initial, cfg_on_disk_at_rollback]):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                        side_effect=RuntimeError("unpkg unreachable"),
                    ):
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            with pytest.raises(RuntimeError, match="unpkg unreachable"):
                                enable_rum(service_id, "test_token", faro_version="2.0.0")
                            mock_reconcile.assert_not_called()

        last_saved = mock_save.call_args_list[-1][0][1]
        assert last_saved["rum_enabled"] is False
        assert last_saved["rum"]["faro_version"] == "1.0.0"
        # Only present if the rollback actually reloaded from disk.
        assert last_saved["unrelated_marker"] == "present-after-reload"

    def test_enable_rum_faro_reconcile_failure_restores_previous_faro_version(self):
        """Verify config is rolled back to the pre-attempt faro_version if
        reconcile_vcl_state fails after the bundle was already uploaded.

        Same two-distinct-objects setup as the upload-failure test above, to
        prove the rollback reloads fresh disk state rather than reusing the
        stale in-memory cfg (Minor 1 from the task-6 review)."""
        service_id = "srv_test"
        cfg_initial = self._cfg(service_id, rum={"faro_version": "1.0.0"})
        cfg_on_disk_at_rollback = dict(cfg_initial)
        cfg_on_disk_at_rollback["unrelated_marker"] = "present-after-reload"

        with patch("backend.config.load_config", side_effect=[cfg_initial, cfg_on_disk_at_rollback]):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                        return_value={"version": "2.0.0"},
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                            side_effect=RuntimeError("VCL validation failed"),
                        ):
                            with pytest.raises(RuntimeError, match="VCL validation failed"):
                                enable_rum(service_id, "test_token", faro_version="2.0.0")

        last_saved = mock_save.call_args_list[-1][0][1]
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

    def test_enable_rum_without_faro_version_unaffected(self):
        """Verify omitting faro_version never touches download_and_upload_faro."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro"
                    ) as mock_download_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)
                            enable_rum(service_id, "test_token")

        mock_download_upload.assert_not_called()


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
        cfg_before = self._cfg(service_id, faro_version="1.0.0")
        # Simulate download_and_upload_faro having already persisted the new
        # version by the time the post-failure reload happens.
        cfg_after_upload = self._cfg(service_id, faro_version="2.0.0")
        cfg_after_upload["rum"]["faro_content_hash"] = "new-hash"

        with patch("backend.config.load_config", side_effect=[cfg_before, cfg_after_upload]):
            with patch("backend.config.save_config") as mock_save:
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        side_effect=RuntimeError("VCL validation failed"),
                    ):
                        with pytest.raises(RuntimeError, match="VCL validation failed"):
                            upgrade_faro_version(service_id, "2.0.0", "test_token")

        saved_cfg = mock_save.call_args_list[-1][0][1]
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
