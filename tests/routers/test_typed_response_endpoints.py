from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError

from tests.conftest import MOCK_SERVICE_ID

# ── From test_typed_response_followups.py ──

"""Tightened response models keep the producers' wire payloads byte-for-byte.

Each case feeds a REAL producer's output (or a dict shaped exactly like one)
through the model with ``exclude_unset`` + by-alias dumping — the same
serialization the routes apply — and compares serialized JSON, so an int that
turns into a float or a key that turns into ``null`` fails.
"""


def _wire(model_cls: type, payload: dict[str, Any]) -> str:
    return model_cls.model_validate(payload).model_dump_json(by_alias=True, exclude_unset=True)


def _assert_same_bytes(model_cls: type, payload: dict[str, Any]) -> None:
    assert json.loads(_wire(model_cls, payload)) == jsonable_encoder(payload)
    assert _wire(model_cls, payload) == json.dumps(jsonable_encoder(payload), separators=(",", ":"), ensure_ascii=False)


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


def test_bootstrap_pop_geo_and_debug_state_roundtrip():
    from backend.models.common import BootstrapDebugState, BootstrapPopGeo

    _assert_same_bytes(BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"})
    _assert_same_bytes(BootstrapPopGeo, {"city": "", "region": "", "country": ""})
    _assert_same_bytes(
        BootstrapDebugState,
        {"debug_responses_enabled": False, "query_debug_visibility": "disabled", "api_call_debug_visibility": "admins"},
    )


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


# ── From test_typed_response_query.py ──


def _result() -> dict:
    return {
        "columns": ["ts", "tz_ts", "amount", "n"],
        "data": [
            {
                "ts": dt.datetime(2026, 1, 2, 3, 4, 5),
                "tz_ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
                "amount": Decimal("12.50"),
                "n": 7,
            }
        ],
        "row_count": 1,
        "total_rows": 1,
        "truncated": False,
        "elapsed_ms": 3,
        "explain_plan": "plan",
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


def test_query_rows_encode_like_jsonable_encoder(client):
    result = _result()
    expected_rows = json.loads(json.dumps(jsonable_encoder(result["data"])))
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == expected_rows
    assert body["data"][0]["tz_ts"].endswith("+00:00")
    assert body["data"][0]["amount"] == 12.5
    assert {k: body[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")} == {
        k: result[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")
    }


def test_query_omits_explain_plan_when_producer_did_not_set_it(client):
    result = _result()
    del result["explain_plan"]
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    assert "explain_plan" not in resp.json()


# ── From test_typed_response_rum_scoring.py ──


@pytest.mark.parametrize(
    "model_name,payload",
    [
        ("RumStatusResponse", {"enabled": False, "enabled_at": None}),
        (
            "RumStatusResponse",
            {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00+00:00",
                "deployed_vcl_sha": "a",
                "current_vcl_sha": "b",
                "vcl_drift": True,
                "capture_vitals": True,
                "capture_performance": False,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": "",
            },
        ),
        ("RumSettingsUpdateResponse", {"ok": True, "message": "m", "skipped": False}),
        (
            "RumBeaconHealthResponse",
            {
                "enabled": True,
                "beacon_fire_rate": 3,
                "recent_beacons": 3,
                "last_beacon_time": None,
                "setup_complete": True,
                "message": "x",
                "_debug_queries": [{"sql": "s"}],
                "_debug_sqlite": [],
                "_debug_calls": [],
                "_is_cached": False,
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": True,
                "beacon_count": 0,
                "pageview_count": 0,
                "interaction_count": 0,
                "error_count": 0,
                "message": "Waiting",
                "vitals": {
                    "lcp": {"p75": None, "distribution": None},
                    "cls": {"p75": None, "distribution": None},
                    "inp": {"p75": None, "distribution": None},
                    "fid": {"p75": None, "fcp": None, "ttfb": None},
                },
                "worst_pages": [],
                "errors": [],
                "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                "environments": {"browsers": {}, "os": {}, "devices": {}},
                "_debug_queries": [],
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": 12,
                "pageview_count": 10,
                "interaction_count": 1,
                "error_count": 1,
                "vitals": {
                    "lcp": {"p75": 2.5, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
                    "cls": {"p75": 0.1, "distribution": {"good": 90, "needs_improvement": 10, "poor": 0}},
                    "inp": {"p75": 180, "distribution": {"good": 70, "needs_improvement": 20, "poor": 10}},
                    "fid": {"p75": 3.0, "fcp": 1.2, "ttfb": 0.4},
                },
                "worst_pages": [
                    {"path": "/a", "views": 4, "avg_load_time": 1, "lcp_p75": None, "cls_p75": 0.05, "error_rate": 0.0}
                ],
                "errors": [{"message": "boom", "file": "a.js", "line": 3, "col": 4, "count": 2}],
                "trends": {
                    "timestamps": ["2026-01-01T00:00:00+00:00"],
                    "lcp": [None],
                    "cls": [0.1],
                    "error_rate": [0.0],
                    "pageviews": [10],
                    "interactions": [1],
                    "errors": [1],
                },
                "environments": {"browsers": {"Chrome": 9}, "os": {"macOS": 9}, "devices": {"desktop": 9}},
            },
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    from backend.models import rum

    model = getattr(rum, model_name)
    validated = model.model_validate(payload)
    assert validated.model_dump(mode="json") == payload
    # Same bytes, not just equal values: an int must not come back as 5.0.
    assert validated.model_dump_json() == json.dumps(payload, separators=(",", ":"))


def test_scoring_status_route_disabled_shape(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body(resp) == {"enabled": False}


def test_scoring_status_and_config_match_producer(client, monkeypatch):
    from backend import config as svcconfig
    from backend.routers import session_scoring as ss

    cfg = {
        "service_id": MOCK_SERVICE_ID,
        "scoring": {"enabled": True, "request_secret": "s3cret", "custom_key": [1, 2], "deployed_vcl_sha": "v"},
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    expected = ss.scoring_status(MOCK_SERVICE_ID)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body(resp) == expected
    assert "request_secret" not in resp.json()

    composite = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/config")
    assert composite.status_code == 200
    assert _body(composite)["status"] == expected


def test_scoring_label_delete_model_roundtrip():
    from backend.models.session_scoring import ScoringLabelDeleteResponse

    payload = {"status": "success", "id": "abc"}
    assert ScoringLabelDeleteResponse.model_validate(payload).model_dump(mode="json") == payload


def test_scoring_status_declares_config_identity_keys():
    from backend.models.session_scoring import ScoringStatusResponse

    payload = {
        "enabled": True,
        "scoring_service_id": "svc",
        "scoring_service_name": "n",
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    declared = set(ScoringStatusResponse.model_fields)
    assert set(payload) <= declared
    assert ScoringStatusResponse.model_validate(payload).model_dump(mode="json", exclude_unset=True) == payload


def test_scoring_labels_list_matches_producer(client, monkeypatch):
    from backend.scoring import labels as _labels

    rows = [
        {
            "id": "a1",
            "service_id": MOCK_SERVICE_ID,
            "sid": "s1",
            "label": lbl,
            "notes": "n",
            "flagged_by": "admin",
            "sample_ip": "",
            "sample_ua": "",
            "sample_url": "",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }
        for lbl in ("good", "bad", "neutral")
    ]
    counts = {"good": 1, "bad": 1, "neutral": 1}
    monkeypatch.setattr(_labels, "list_labels", lambda sid, limit=500: rows)
    monkeypatch.setattr(_labels, "counts_by_label", lambda sid: counts)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert resp.status_code == 200
    assert _body(resp) == {"labels": rows, "counts": counts}
    assert list(resp.json()["counts"]) == ["good", "bad", "neutral"]


def test_scoring_label_row_rejects_unknown_label():
    from pydantic import ValidationError

    from backend.models.session_scoring import ScoringLabelRow

    assert ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    with pytest.raises(ValidationError):
        ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "maybe"})


def test_scoring_label_literal_matches_allowed_labels():
    import typing

    from backend.models.session_scoring import ScoringLabelRow
    from backend.scoring.labels import ALLOWED_LABELS

    ann = ScoringLabelRow.model_fields["label"].annotation
    assert set(typing.get_args(ann)) == set(ALLOWED_LABELS)


# ── From test_typed_response_status_reads.py ──


def test_debug_state_matches_producer(client):
    from backend.models.common import _debug_responses_enabled

    resp = client.get("/api/debug/state")
    assert resp.status_code == 200
    assert _body(resp) == {"debug_responses_enabled": _debug_responses_enabled()}


def test_schema_response_serializes_schema_alias():
    from backend.models.status_reads import SchemaResponse

    payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert SchemaResponse.model_validate(payload).model_dump(mode="json", by_alias=True) == payload


def test_audit_logs_entries_keep_the_producer_row_shape():
    from backend.models.status_reads import AuditLogsResponse

    # ``metadata.get_audit_logs`` stringifies timestamps and json-decodes ``details``.
    entry = {
        "id": 7,
        "timestamp": "2026-01-01 00:00:00+00:00",
        "event_type": "provision",
        "details": {"bucket": "b", "log_fields": {"groups": ["A"]}},
        "actor": "ui",
        "source": "local",
    }
    payload = {"total": 1, "page": 1, "per_page": 50, "entries": [entry], "_debug_calls": []}
    dumped = AuditLogsResponse.model_validate(payload).model_dump(mode="json")
    assert dumped["entries"] == [entry]
    assert list(dumped["entries"][0]) == list(entry)
    assert dumped["_debug_calls"] == []


def test_cmcd_status_disabled_and_enabled_shapes(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    off = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert off.status_code == 200
    assert _body(off) == {"enabled": False}

    cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    on = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert _body(on) == {
        "enabled": True,
        "mode": "headers",
        "version": 1,
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }


# ── From test_typed_response_wire_compat.py ──


def _roundtrip(obj):
    return json.loads(json.dumps(obj))


def _body(resp):
    """Response JSON minus the telemetry-middleware ``_debug_*``/``_section_timings`` envelope."""
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def test_log_fields_catalog_route_matches_producer(client):
    from backend.routers.bootstrap import _compute_log_fields_catalog

    resp = client.get("/api/log-fields/catalog")
    assert resp.status_code == 200
    assert _body(resp) == _roundtrip(_compute_log_fields_catalog(None))


def test_log_fields_catalog_model_keeps_custom_entry_shape():
    from backend.core import log_fields as lf
    from backend.models.log_fields_catalog import LogFieldsCatalogResponse

    custom = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_custom", "label": "My", "description": "d"}]}
    )
    payload = {"groups": [], "fields": custom, "insights": [], "presets": {}}
    dumped = LogFieldsCatalogResponse.model_validate(payload).model_dump(mode="json")
    assert dumped == _roundtrip(payload)
    assert "typical_bytes" not in dumped["fields"][0]


def test_bootstrap_response_embeds_catalog_unchanged():
    from backend.models.common import BootstrapResponse
    from backend.routers.bootstrap import _compute_log_fields_catalog

    catalog = _compute_log_fields_catalog(None)
    resp = BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=catalog)
    assert resp.model_dump(mode="json", by_alias=True)["log_fields_catalog"] == _roundtrip(catalog)
