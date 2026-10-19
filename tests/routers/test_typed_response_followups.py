"""Tightened response models keep the producers' wire payloads byte-for-byte.

Each case feeds a REAL producer's output (or a dict shaped exactly like one)
through the model with ``exclude_unset`` + by-alias dumping — the same
serialization the routes apply — and compares serialized JSON, so an int that
turns into a float or a key that turns into ``null`` fails.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError


def _wire(model_cls: type, payload: dict[str, Any]) -> str:
    return model_cls.model_validate(payload).model_dump_json(by_alias=True, exclude_unset=True)


def _assert_same_bytes(model_cls: type, payload: dict[str, Any]) -> None:
    assert json.loads(_wire(model_cls, payload)) == jsonable_encoder(payload)
    assert _wire(model_cls, payload) == json.dumps(jsonable_encoder(payload), separators=(",", ":"), ensure_ascii=False)


# ── scoring ──────────────────────────────────────────────────────────────────


def test_l2_enforce_state_matches_both_real_producers():
    from backend.models.session_scoring import ScoringL2EnforceState
    from backend.routers import session_scoring_admin as admin

    _assert_same_bytes(ScoringL2EnforceState, admin._l2_enforce_unavailable())
    _assert_same_bytes(
        ScoringL2EnforceState,
        admin._build_l2_enforce_block(
            scoring_enabled_at_raw="1700000000",
            l2_enforce_raw="1",
            l2_enabled_at_raw="1700086400",
            now_epoch=1700200000,
        ),
    )
    _assert_same_bytes(
        ScoringL2EnforceState,
        admin._build_l2_enforce_block(
            scoring_enabled_at_raw=None, l2_enforce_raw=None, l2_enabled_at_raw=None, now_epoch=1700200000
        ),
    )


def test_scoring_state_models_match_route_producers(monkeypatch):
    from backend import config as svcconfig
    from backend.models.session_scoring import (
        ScoringEnforceStatusCodeState,
        ScoringExcludeRegexState,
        ScoringThresholdState,
    )
    from backend.routers import session_scoring_admin as admin

    cfg = {
        "service_id": "svc",
        "scoring": {
            "enabled": True,
            "operator_threshold": 80,
            "operator_threshold_set_at": "2026-01-01T00:00:00+00:00",
            "exclude_url_regex": "\\.(png|css)$",
            "enforce_status_code": 403,
        },
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)

    _assert_same_bytes(ScoringThresholdState, admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringExcludeRegexState, admin.scoring_exclude_regex_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, admin.scoring_enforce_status_code_get("svc"))

    cfg["scoring"] = {"enabled": True}
    _assert_same_bytes(ScoringThresholdState, admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, admin.scoring_enforce_status_code_get("svc"))


def test_scoring_enforce_threshold_and_rows_roundtrip():
    from backend.models.session_scoring import (
        ScoringComplianceRow,
        ScoringDistributionRow,
        ScoringEnforceThresholdResponse,
        ScoringTopFlaggedRow,
    )

    _assert_same_bytes(
        ScoringEnforceThresholdResponse, {"threshold": None, "enforced": False, "key": "enforce_threshold"}
    )
    _assert_same_bytes(ScoringEnforceThresholdResponse, {"threshold": 70, "enforced": True, "key": "enforce_threshold"})
    _assert_same_bytes(ScoringDistributionRow, {"hour": "2026-01-01T00:00:00", "bucket": "0-25", "count": 4})
    _assert_same_bytes(ScoringComplianceRow, {"hour": "2026-01-01T00:00:00", "compliance": "ok", "count": 4})
    _assert_same_bytes(
        ScoringTopFlaggedRow,
        {"timestamp": "2026-01-01T00:00:00", "edge_sid": None, "edge_score": 90, "url": "/a"},
    )
    with pytest.raises(ValidationError):
        ScoringTopFlaggedRow.model_validate({"edge_sid": "x", "edge_score": 1})


# ── log-fields catalog ───────────────────────────────────────────────────────


def test_log_fields_catalog_model_matches_real_producer():
    from backend.models.log_fields_catalog import LogFieldsCatalogResponse
    from backend.routers.bootstrap import _compute_log_fields_catalog

    payload = _compute_log_fields_catalog(None)
    assert payload["groups"] and payload["fields"] and payload["insights"] and payload["presets"]
    _assert_same_bytes(LogFieldsCatalogResponse, payload)


def test_log_fields_catalog_custom_entries_roundtrip():
    from backend.core import log_fields as lf
    from backend.models.log_fields_catalog import LogFieldCatalogEntry

    entries = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_field", "label": "My Field", "enabled": True}]}
    )
    assert entries
    for entry in entries:
        # Custom entries build their keys in a different order than built-ins; the
        # model's declared order (unchanged by typing) is shared, so compare values.
        assert json.loads(_wire(LogFieldCatalogEntry, entry)) == jsonable_encoder(entry)


# ── bootstrap ────────────────────────────────────────────────────────────────


def test_bootstrap_pop_geo_and_debug_state_roundtrip():
    from backend.models.common import BootstrapDebugState, BootstrapPopGeo

    _assert_same_bytes(BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"})
    _assert_same_bytes(BootstrapPopGeo, {"city": "", "region": "", "country": ""})
    _assert_same_bytes(
        BootstrapDebugState,
        {"debug_responses_enabled": False, "query_debug_visibility": "disabled", "api_call_debug_visibility": "admins"},
    )


# ── provision wizard ─────────────────────────────────────────────────────────


def test_provision_models_roundtrip():
    from backend.models.provision import (
        NgwafWorkspacesResponse,
        ProvisionCheckConfigResponse,
        ProvisionLakeInfoResponse,
        ProvisionValidateResponse,
    )

    _assert_same_bytes(
        ProvisionCheckConfigResponse,
        {"logging_service": {"ok": True, "details": "ok"}, "cdn_service": {"ok": False, "details": ""}},
    )
    _assert_same_bytes(
        NgwafWorkspacesResponse,
        {"workspaces": [{"id": "w", "name": "n"}], "error_hint": "h", "_debug_raw": "GET /x\n\n--- Response (200) ---"},
    )
    _assert_same_bytes(
        ProvisionValidateResponse,
        {"service_name": "s", "token_info": {"id": "1", "name": "t", "user_id": None, "type": "automation"}},
    )
    _assert_same_bytes(
        ProvisionLakeInfoResponse,
        {
            "ok": True,
            "table_exists": True,
            "info": {"table_name": "logs", "snapshots": 2, "data_files": 7, "size_bytes": 10, "region": None},
            "calendar": {"2026-01-01": {"data_files": 7, "size_bytes": 0}},
            "range": {"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
        },
    )
    with pytest.raises(ValidationError):
        ProvisionCheckConfigResponse.model_validate({"logging_service": {"ok": True, "details": "x"}})


# ── security top bots ────────────────────────────────────────────────────────


def test_security_top_bots_rows_roundtrip():
    from backend.models.security import SecurityBotRow

    _assert_same_bytes(SecurityBotRow, {"id": "gptbot", "name": "Gptbot", "category": "ai", "request_count": 12})
    _assert_same_bytes(SecurityBotRow, {"name": "Googlebot", "category": "search", "request_count": 5})
    _assert_same_bytes(
        SecurityBotRow,
        {
            "id": "b",
            "name": "b",
            "category": "unknown",
            "request_count": 3,
            "verified_count": 3,
            "impersonator_count": 0,
            "unverified_count": 0,
            "verification_coverage": 1.0,
            "pending_count": 0,
        },
    )
