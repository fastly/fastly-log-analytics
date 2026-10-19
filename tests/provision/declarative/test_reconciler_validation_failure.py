"""A draft that fails validation surfaces VclValidationError; no vacuous active-version guard may mask it."""

from __future__ import annotations

import itertools
import json
from unittest.mock import patch

import pytest

from backend.provision.declarative.diff import DiffResult, VCLSnippet
from backend.provision.declarative.reconciler import VclValidationError, reconcile_vcl_state


@patch("backend.provision.declarative.reconciler._activate_draft")
@patch("backend.provision.declarative.reconciler._validate_draft")
@patch("backend.provision.declarative.reconciler._clone_active_version")
@patch("backend.provision.declarative.reconciler._apply_diff")
@patch("backend.provision.declarative.reconciler.compute_diff")
@patch("backend.provision.declarative.reconciler._fetch_active_version")
@patch("backend.provision.declarative.reconciler._fetch_snippets")
@patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
@patch("backend.provision.declarative.reconciler._fetch_backends")
def test_validation_failure_raises_vcl_validation_error_even_if_active_version_moves(
    mock_backends,
    mock_endpoints,
    mock_snippets,
    mock_active,
    mock_diff,
    mock_apply,
    mock_clone,
    mock_validate,
    mock_activate,
):
    counter = itertools.count(1)
    mock_active.side_effect = lambda *a, **k: next(counter)  # every fetch returns a different version
    mock_snippets.return_value = []
    mock_endpoints.return_value = []
    mock_backends.return_value = []
    mock_clone.return_value = 99
    mock_validate.return_value = "boom: bad VCL"
    mock_diff.return_value = DiffResult(
        snippets_to_add=[VCLSnippet(name="x", priority=10, body="# c", subroutine="recv")]
    )

    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text") as mock_read:
        mock_read.return_value = json.dumps({"service_id": "srv_test", "log_period": 60, "sample_rate": 100})
        with pytest.raises(VclValidationError):
            reconcile_vcl_state("srv_test", "token", dry_run=False)
    mock_activate.assert_not_called()
