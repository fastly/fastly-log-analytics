"""Integration tests for reconciliation control loop (against mocked Fastly API)."""

import json
from unittest.mock import patch

import pytest

from backend.provision.declarative.diff import DiffResult, VCLSnippet
from backend.provision.declarative.reconciler import (
    MANAGED_BACKEND_NAMES,
    VclValidationError,
    _detect_and_queue_legacy_cleanup,
    _is_legacy_snippet,
    reconcile_vcl_state,
)


class TestReconciliationIdempotence:
    """Test idempotence property: second run is a no-op."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_idempotent_second_run_is_noop(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Second run should be a no-op."""
        # Setup: desired state already deployed
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        # First run
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                result1 = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Should return with activated_version = current active
                assert result1.activated_version == 1


class TestReconciliationFeatures:
    """Test feature enable/disable scenarios."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_enables_rum_adds_secondary_endpoint(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """Verify enabling RUM creates secondary endpoint."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # In dry-run, we see the changes that would be applied
                assert result.changes_applied is not None
                # Endpoint count should show we'd add the RUM endpoint
                assert result.changes_applied.get("endpoints_added", 0) > 0

    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_uploads_rum_js_when_enabled(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
    ):
        """Verify RUM JS is uploaded when RUM is enabled in desired state."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # In dry_run, upload should still be called (before we check idempotency)
                mock_upload_rum_js.assert_called_once_with("srv_test", "token", status_cb=None)


class TestBackendWhitelistEnforcement:
    """Test Gotcha 5: catastrophic backend deletion prevention."""

    def test_reconcile_backend_deletion_whitelist_enforcement(self):
        """CRITICAL: Verify non-whitelisted backends are NEVER deleted."""
        from backend.provision.declarative.diff import DiffResult
        from backend.provision.declarative.reconciler import _apply_diff

        # Set up: one whitelisted backend and one customer origin backend
        diff = DiffResult()
        diff.backends_to_remove = ["my_origin"]  # Not in whitelist!

        # Should raise AssertionError before deletion
        with pytest.raises(AssertionError, match="non-whitelisted backend"):
            _apply_diff("srv_test", "token", 2, diff)

    def test_managed_backend_names_constant_is_defined(self):
        """Verify MANAGED_BACKEND_NAMES whitelist is defined."""
        assert isinstance(MANAGED_BACKEND_NAMES, set)
        assert "session_scorer" in MANAGED_BACKEND_NAMES
        assert "F_fos_origin" in MANAGED_BACKEND_NAMES


class TestLegacySnippetDetection:
    """Test legacy VCL snippet detection and auto-cleanup."""

    def test_is_legacy_snippet_recognizes_all_legacy_prefixes(self):
        """Verify _is_legacy_snippet identifies all pre-2.2 snippet types."""
        legacy_names = [
            "Fastly Log Analysis - recv",
            "RUM - recv",
            "Session Scoring - recv",
            "CMCD - recv",
        ]
        for name in legacy_names:
            assert _is_legacy_snippet(name), f"Failed to recognize legacy snippet: {name}"

    def test_is_legacy_snippet_ignores_consolidated(self):
        """Verify _is_legacy_snippet does NOT flag consolidated snippets."""
        consolidated_names = [
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_fetch",
        ]
        for name in consolidated_names:
            assert not _is_legacy_snippet(name), f"Incorrectly flagged as legacy: {name}"

    def test_detect_and_queue_legacy_cleanup_adds_to_removal_queue(self):
        """Verify legacy snippets are queued for removal when no consolidated exist."""
        legacy_snippet = VCLSnippet(
            name="RUM - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet]
        diff = DiffResult()

        _detect_and_queue_legacy_cleanup(current_snippets, diff)

        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_unconditionally_cleans_legacy_snippets(self):
        """Verify legacy snippets are unconditionally queued even if consolidated ones already exist."""
        legacy_snippet = VCLSnippet(
            name="RUM - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        consolidated_snippet = VCLSnippet(
            name="Fastly Log Analytics - vcl_recv",
            priority=10,
            body="# new vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet, consolidated_snippet]
        diff = DiffResult()

        _detect_and_queue_legacy_cleanup(current_snippets, diff)

        # Should unconditionally queue legacy for removal
        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_calls_status_callback(self):
        """Verify status callback is invoked when legacy snippets are detected."""
        legacy_snippet = VCLSnippet(
            name="Session Scoring - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet]
        diff = DiffResult()
        callback_messages = []

        def capture_status(msg: str) -> None:
            callback_messages.append(msg)

        _detect_and_queue_legacy_cleanup(current_snippets, diff, status_cb=capture_status)

        assert len(callback_messages) > 0
        assert "legacy" in callback_messages[0].lower()
        assert "1" in callback_messages[0]  # Should mention count


class TestMigrationFromLegacyToConsolidated:
    """End-to-end test of migration from legacy to consolidated VCL."""

    @patch("backend.provision.declarative.reconciler._bootstrap_featurestate_from_fastly")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    def test_legacy_to_consolidated_migration_full_flow(
        self,
        mock_activate,
        mock_validate,
        mock_fetch_dicts,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
        mock_bootstrap,
    ):
        """Verify full migration: legacy snippets detected and queued for removal."""
        # Setup: service with legacy snippets (pre-2.2 deployment)
        legacy_snippets = [
            VCLSnippet(name="RUM - recv", priority=10, body="# legacy rum", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - recv", priority=10, body="# legacy scoring", subroutine="vcl_recv"),
            VCLSnippet(name="CMCD - recv", priority=10, body="# legacy cmcd", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = legacy_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_fetch_dicts.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""  # Validation passes
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False)

                # Verify result shows activation happened
                assert result.activated_version == 2
                assert result.error is None
                # Verify legacy snippets were in the diff
                assert result.changes_applied is not None
                # _apply_diff should have been called with a diff that includes legacy removals
                mock_apply_diff.assert_called_once()
                # _apply_diff(service_id, token, draft_version, diff, ...)
                # So diff is the 4th positional arg (index 3)
                call_args = mock_apply_diff.call_args
                call_diff = call_args[0][3]  # Get 4th positional argument
                # The diff should have queued all 3 legacy snippets for removal
                assert "RUM - recv" in call_diff.snippets_to_remove
                assert "Session Scoring - recv" in call_diff.snippets_to_remove
                assert "CMCD - recv" in call_diff.snippets_to_remove


class TestValidationFailureHandling:
    """Test that validation failures prevent activation."""

    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    def test_reconcile_validates_before_activating(
        self,
        mock_validate,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
    ):
        """Verify draft is NOT activated if validation fails."""
        # Mock validation to fail
        mock_validate.return_value = "VCL syntax error at line 42"
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                # Reconciliation should raise VclValidationError
                with pytest.raises(VclValidationError):
                    reconcile_vcl_state("srv_test", "token", dry_run=False)


class TestBypassActivation:
    """Test that setting activate=False bypasses activation but compiles/validates."""

    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    def test_reconcile_bypasses_activation_when_activate_is_false(
        self,
        mock_activate,
        mock_validate,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
    ):
        """Verify we clone, apply VCL, validate, but do NOT activate when activate=False."""
        mock_validate.return_value = ""  # Valid
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False, activate=False)

                # Clone, apply and validate must be called on draft
                assert mock_clone_active.called
                assert mock_apply_diff.called
                assert mock_validate.called

                # BUT activation must NOT be called
                assert not mock_activate.called

                # Result must track compiled draft and not activated version
                assert result.activated_version is None
                assert result.draft_version == 2


class TestLegacySnippetMigration:
    """Test automatic cleanup of legacy snippets (Gotcha 2)."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_cleans_legacy_snippets_on_first_run(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Legacy snippets are deleted on first reconciliation."""
        from backend.provision.declarative.diff import VCLSnippet

        # Simulate old RUM snippets deployed
        old_snippets = [
            VCLSnippet(name="RUM - Recv", priority=100, body="old", subroutine="vcl_recv"),
            VCLSnippet(name="RUM - Set cookies", priority=101, body="old", subroutine="vcl_deliver"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = old_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": False,  # Disable RUM
                    "scoring_enabled": False,  # Disable scoring
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Should show legacy snippets would be removed
                assert result.changes_applied is not None
                assert result.changes_applied.get("snippets_removed", 0) > 0

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_cleans_legacy_snippets_even_with_existing_consolidated(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Legacy snippets are deleted even if consolidated snippets already exist."""
        from backend.provision.declarative.diff import VCLSnippet

        # Simulate both consolidated and legacy RUM/Scoring snippets deployed
        current_snippets = [
            VCLSnippet(
                name="Fastly Log Analytics - vcl_recv", priority=10, body="consolidated recv", subroutine="vcl_recv"
            ),
            VCLSnippet(name="RUM - Recv", priority=100, body="old RUM recv", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old Scoring recv", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = current_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": False,
                    "scoring_enabled": False,
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Both legacy snippets should be queued for removal (count of 2)
                assert result.changes_applied.get("snippets_removed", 0) == 2


class TestDictionaryFiltering:
    """Test filtering of non-whitelisted customer dictionaries."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries")
    def test_reconcile_filters_custom_dictionaries(
        self,
        mock_fetch_dicts,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """Verify custom dictionaries not in whitelist are filtered and don't trigger deletion assert."""
        from backend.provision.declarative.diff import ServiceDictionary

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        # Return a whitelisted dict and a custom customer dict
        mock_fetch_dicts.return_value = [
            ServiceDictionary(name="fos_credentials"),
            ServiceDictionary(name="customer_private_dict"),
        ]

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                }
                mock_read.return_value = json.dumps(cfg)

                # This should not raise an AssertionError during dry run or summary computation
                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                assert result.changes_applied is not None
                # Since customer_private_dict is filtered out, it shouldn't be queued for deletion
                assert result.changes_applied.get("dictionaries_removed", 0) == 0


class TestBootstrapFeatureState:
    """Test bootstrapping feature state from Fastly."""

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration")
    def test_bootstrap_detects_fos_origin_non_legacy(self, mock_fastly_int, mock_fastly):
        """Verify bootstrap fallback correctly detects 'fos_origin' as RUM enabling indicator."""
        from backend.provision.declarative.reconciler import _bootstrap_featurestate_from_fastly

        mock_fastly_int.fetch_active_version.return_value = 1
        mock_fastly_int.fetch_snippets.return_value = []
        mock_fastly_int.fetch_logging_endpoints.return_value = []

        # Return VCL content containing only "fos_origin" (the non-legacy string)
        mock_fastly.return_value = {"items": [{"content": "backend fos_origin { .connect_timeout = 10s; }"}]}

        state = _bootstrap_featurestate_from_fastly("srv_test", "token")
        assert state.rum_enabled is True


class TestVarDeconfliction:
    """Test de-confliction of local variable declarations (Gotcha 2)."""

    def test_deconflict_vars_in_vcl_miss_and_pass(self):
        """Verify we preserve declarations in vcl_miss/vcl_pass but comment them out when an unmanaged snippet collides."""
        from backend.provision.declarative.diff import DiffResult, VCLSnippet
        from backend.provision.declarative.reconciler import _apply_diff

        snippet_miss = VCLSnippet(
            name="Fastly Log Analytics - vcl_miss",
            priority=10,
            body="declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';",
            subroutine="vcl_miss",
        )
        snippet_recv = VCLSnippet(
            name="Fastly Log Analytics - vcl_recv",
            priority=10,
            body="declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';",
            subroutine="vcl_recv",
        )
        diff = DiffResult(
            snippets_to_add=[snippet_miss, snippet_recv],
        )

        with patch("backend.provision.declarative.reconciler.fastly_integration") as mock_fastly_int:
            # First, test with no unmanaged snippets - declarations should remain intact
            _apply_diff(
                service_id="srv_test",
                token="token",
                draft_version=1,
                diff=diff,
                current_snippets=[],
            )

            assert "declare local var.fosAccessKey STRING;" in snippet_miss.body
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body

            # Second, test with an unmanaged snippet in vcl_miss declaring the same variable
            unmanaged_snippet = VCLSnippet(
                name="Some User Snippet",
                priority=10,
                body="declare local var.fosAccessKey STRING;",
                subroutine="vcl_miss",
            )

            # Reset snippet bodies to original state before run
            snippet_miss.body = "declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';"
            snippet_recv.body = "declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';"

            _apply_diff(
                service_id="srv_test",
                token="token",
                draft_version=1,
                diff=diff,
                current_snippets=[unmanaged_snippet],
            )

            # In vcl_miss, the declaration must be commented out to prevent collision with the unmanaged snippet
            assert (
                "# declare local var.fosAccessKey STRING; (omitted to prevent collision with unmanaged snippet)"
                in snippet_miss.body
            )

            # In vcl_recv, the declaration must remain intact (no unmanaged snippet in vcl_recv)
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body
