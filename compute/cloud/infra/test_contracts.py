"""Thin wrapper re-exporting recorded Fastly API contract tests from `tests.core.test_vcl_semantics`."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.core.test_vcl_semantics import (  # noqa: E402
    RECORDED_FASTLY_API_FIXTURES as FIXTURES,
)
from tests.core.test_vcl_semantics import (
    test_live_created_install_get_contracts_have_not_drifted,
    test_live_recorded_get_contracts_have_not_drifted,
    test_recorded_backend_readbacks_have_matching_tls_and_only_tick_is_shielded,
    test_recorded_created_services_have_correct_isolated_store_links,
    test_recorded_front_clock_and_write_only_dictionary_configuration,
    test_redaction_removes_credentials_and_identity_before_recording,
)

__all__ = [
    "FIXTURES",
    "test_live_created_install_get_contracts_have_not_drifted",
    "test_live_recorded_get_contracts_have_not_drifted",
    "test_recorded_backend_readbacks_have_matching_tls_and_only_tick_is_shielded",
    "test_recorded_created_services_have_correct_isolated_store_links",
    "test_recorded_front_clock_and_write_only_dictionary_configuration",
    "test_redaction_removes_credentials_and_identity_before_recording",
]
