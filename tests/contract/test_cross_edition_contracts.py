"""Cross-edition OpenAPI 3.1 and shared golden-vector contract tests (High-Scale Edition)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.core.share_db import mask_ip
from backend.high_scale.cmcd import cmcd_aggregates
from backend.high_scale.ingest_controller.service import _ASSET_TYPE_CASE, assets_aggregates
from backend.high_scale.insights.sql_queries import COLUMNS as INSIGHT_COLUMNS
from backend.high_scale.network import network_bundle, network_health, network_quality
from backend.high_scale.performance import value_summary
from backend.high_scale.rum import rum_analytics
from backend.main import app
from backend.models.admin.operations import MetadataStorageResponse, UsageComputeResponse
from backend.models.analytics import (
    AssetsRequest,
    CmcdRequest,
    NetworkBundleRequest,
    NetworkHealthRequest,
    NetworkQualityRequest,
    RumStatusResponse,
    ScoringPerReasonResponse,
    SecurityAggregatesResponse,
    ValueSummaryRequest,
)
from backend.repositories.insights.shaping import shape_cache_collapse_detail
from backend.repositories.utils.response_shaping.domains import compute_summary_from_dims
from backend.routers.session_scoring.scoring_analytics import _normalize_cookie_compliance

pytestmark = pytest.mark.no_postgres

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "frontend" / "openapi.json"
VECTORS_PATH = REPO_ROOT / "tests" / "contract" / "fixtures" / "shared_parity_vectors.json"


def _load_openapi() -> dict[str, Any]:
    return json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))


def _load_vectors() -> dict[str, Any]:
    return json.loads(VECTORS_PATH.read_text(encoding="utf-8"))


def _resolve_ref(spec: dict[str, Any], ref: str) -> dict[str, Any]:
    assert ref.startswith("#/components/schemas/"), f"Unsupported $ref: {ref}"
    name = ref.removeprefix("#/components/schemas/")
    schemas = spec.get("components", {}).get("schemas", {})
    assert name in schemas, f"Missing OpenAPI schema {name}"
    return schemas[name]


def _validate_schema(
    spec: dict[str, Any],
    value: Any,
    schema: dict[str, Any],
    path: str = "$",
    depth: int = 0,
) -> list[str]:
    if depth > 25:
        return []
    if "$ref" in schema:
        return _validate_schema(spec, value, _resolve_ref(spec, schema["$ref"]), path, depth + 1)
    if "allOf" in schema:
        errs: list[str] = []
        for sub in schema["allOf"]:
            errs.extend(_validate_schema(spec, value, sub, path, depth + 1))
        return errs
    if "anyOf" in schema:
        for branch in schema["anyOf"]:
            if not _validate_schema(spec, value, branch, path, depth + 1):
                return []
        return [f"{path}: value {value!r} failed anyOf"]
    if "oneOf" in schema:
        for branch in schema["oneOf"]:
            if not _validate_schema(spec, value, branch, path, depth + 1):
                return []
        return [f"{path}: value {value!r} failed oneOf"]
    if "const" in schema and value != schema["const"]:
        return [f"{path}: expected const {schema['const']!r}, got {value!r}"]
    if "enum" in schema and value not in schema["enum"]:
        return [f"{path}: expected enum {schema['enum']!r}, got {value!r}"]

    if "type" in schema:
        allowed = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if value is None:
            actual = "null"
        elif isinstance(value, bool):
            actual = "boolean"
        elif isinstance(value, int):
            actual = "integer"
        elif isinstance(value, float):
            actual = "number"
        elif isinstance(value, str):
            actual = "string"
        elif isinstance(value, list):
            actual = "array"
        elif isinstance(value, dict):
            actual = "object"
        else:
            actual = type(value).__name__
        if not any(t == actual or (t == "number" and actual == "integer") for t in allowed):
            return [f"{path}: expected type {allowed!r}, got {actual} ({value!r})"]

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            return [f"{path}: {value} < minimum {schema['minimum']}"]
        if "maximum" in schema and value > schema["maximum"]:
            return [f"{path}: {value} > maximum {schema['maximum']}"]

    if isinstance(value, list) and "items" in schema:
        errs = []
        for idx, item in enumerate(value[:10]):
            errs.extend(_validate_schema(spec, item, schema["items"], f"{path}[{idx}]", depth + 1))
        return errs

    if isinstance(value, dict):
        errs = []
        for req_key in schema.get("required", []):
            if req_key not in value:
                errs.append(f"{path}: missing required key {req_key!r}")
        props = schema.get("properties", {})
        add_props = schema.get("additionalProperties", True)
        for key, val in value.items():
            if key in props:
                errs.extend(_validate_schema(spec, val, props[key], f"{path}.{key}", depth + 1))
            elif add_props is False:
                errs.append(f"{path}.{key}: unexpected additional property")
            elif isinstance(add_props, dict):
                errs.extend(_validate_schema(spec, val, add_props, f"{path}.{key}", depth + 1))
        return errs

    return []


def _schema_for(spec: dict[str, Any], method: str, path: str) -> dict[str, Any]:
    op = spec["paths"][path][method.lower()]
    return op["responses"]["200"]["content"]["application/json"]["schema"]


def _empty_service(client: Any, service_id: str = "svc-contract") -> SimpleNamespace:
    wm = SimpleNamespace(coverage_start=None, coverage_end=None, last_visible_event_id=None, truncated=False)
    return SimpleNamespace(
        service_id=service_id,
        client=client,
        watermark=lambda: wm,
        watermark_for=lambda domain="request": wm,
    )


def _hs_classify_asset_url(url: str) -> str:
    """Apply High-Scale's `path_lower` normalization + `_ASSET_TYPE_CASE` rules."""
    path_lower = url.split("?", 1)[0].split("#", 1)[0].lower()
    if path_lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico")):
        return "Images"
    if path_lower.endswith(
        (
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            ".txt",
            ".csv",
            ".zip",
            ".tar",
            ".gz",
            ".tgz",
            ".rar",
            ".7z",
        )
    ):
        return "Documents"
    if path_lower.endswith((".js", ".mjs", ".css")):
        return "JavaScript/CSS"
    if path_lower.endswith((".woff", ".woff2", ".ttf", ".otf", ".eot")):
        return "Fonts"
    if path_lower.endswith((".m3u8", ".ts", ".mp4", ".m4s", ".webm")):
        return "Video"
    return "API/Dynamic"


def _hs_filter_waf_tags(raw_tags: str) -> list[str]:
    """Mirror ClickHouse `COLUMNS['signal']` tag normalization and informational exclusion."""
    excluded_exact = {
        "BOT-ANALYSIS",
        "DATACENTER",
        "SITE-FLAGGED-IP",
        "TOR-EXIT",
        "CORPORATE-PROXY",
        "RESIDENTIAL-PROXY",
        "PUBLIC-PROXY",
        "VPN",
    }
    kept: list[str] = []
    for raw in raw_tags.split(","):
        s = raw.strip()
        up = s.upper()
        if (
            not s
            or up in excluded_exact
            or up.startswith("VERIFIED-BOT")
            or up.startswith("SUSPECTED-")
            or up.startswith("SITE.")
        ):
            continue
        kept.append(s)
    return kept


def test_live_app_openapi_matches_snapshot_for_contract_routes() -> None:
    live = app.openapi()
    snap = _load_openapi()
    for path in (
        "/api/assets/aggregates",
        "/api/value/summary",
        "/api/network-health",
        "/api/network-quality",
        "/api/network/bundle",
        "/api/cmcd/aggregates",
        "/api/security/aggregates",
        "/api/insights/cache-collapse-detail",
        "/api/services/{service_id}/rum/analytics",
        "/api/services/{service_id}/rum/status",
        "/api/services/{service_id}/scoring/evaluation/per-reason",
        "/api/admin/metadata-storage",
        "/api/usage/compute",
        "/api/provision/ngwaf-workspaces",
    ):
        assert path in live["paths"]
        assert path in snap["paths"]


def test_offline_fastapi_routes_conform_to_openapi_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "list_service_ids", lambda: ["svc-contract"])
    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid=None: {
            "service_id": "svc-contract",
            "name": "Contract Service",
            "deployment_mode": "high_scale",
            "log_fields": {"active_fields": ["timestamp", "status", "url", "ip", "cache"]},
        },
    )
    spec = _load_openapi()
    client = TestClient(app)
    for path in (
        "/api/health",
        "/api/presets",
        "/api/insight-availability",
        "/api/log-fields/catalog",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, f"GET {path} -> {resp.status_code}: {resp.text}"
        schema = _schema_for(spec, "GET", path)
        errors = _validate_schema(spec, resp.json(), schema, f"GET {path}")
        assert errors == []


def test_high_scale_empty_and_populated_payloads_match_openapi_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "list_service_ids", lambda: ["svc-contract"])
    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid=None: {
            "service_id": "svc-contract",
            "name": "Contract Service",
            "deployment_mode": "high_scale",
            "log_fields": {"active_fields": ["timestamp", "status", "url", "ip", "cache"]},
        },
    )
    spec = _load_openapi()

    class _EmptyClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            if "rum_analytics_counts" in sql:
                return [{"beacons": 0, "pageviews": 0, "interactions": 0, "errors": 0, "sampled": 0}]
            if "rum_analytics_any_data" in sql:
                return [{"has_data": 0}]
            return []

    svc = _empty_service(_EmptyClient())
    start_s, end_s = "2026-11-05T11:30:00Z", "2026-11-05T13:30:00Z"

    # Empty state contracts
    assets_empty = assets_aggregates(svc, AssetsRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s)  # type: ignore[arg-type]
    assets_payload = assets_empty.model_dump() if hasattr(assets_empty, "model_dump") else assets_empty
    assert _validate_schema(spec, assets_payload, _schema_for(spec, "POST", "/api/assets/aggregates")) == []

    value_empty = value_summary(
        svc, ValueSummaryRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s
    )  # type: ignore[arg-type]
    value_payload = value_empty.model_dump() if hasattr(value_empty, "model_dump") else value_empty
    assert _validate_schema(spec, value_payload, _schema_for(spec, "POST", "/api/value/summary")) == []

    net_health_empty = network_health(
        svc, NetworkHealthRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s
    )  # type: ignore[arg-type]
    assert (
        _validate_schema(
            spec,
            net_health_empty.model_dump() if hasattr(net_health_empty, "model_dump") else net_health_empty,
            _schema_for(spec, "POST", "/api/network-health"),
        )
        == []
    )

    net_quality_empty = network_quality(
        svc, NetworkQualityRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s
    )  # type: ignore[arg-type]
    assert (
        _validate_schema(
            spec,
            net_quality_empty.model_dump() if hasattr(net_quality_empty, "model_dump") else net_quality_empty,
            _schema_for(spec, "POST", "/api/network-quality"),
        )
        == []
    )

    net_bundle_empty = network_bundle(
        svc, NetworkBundleRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s
    )  # type: ignore[arg-type]
    assert (
        _validate_schema(
            spec,
            net_bundle_empty.model_dump() if hasattr(net_bundle_empty, "model_dump") else net_bundle_empty,
            _schema_for(spec, "POST", "/api/network/bundle"),
        )
        == []
    )

    cmcd_empty = cmcd_aggregates(
        svc, CmcdRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s, None, False
    )  # type: ignore[arg-type]
    assert (
        _validate_schema(
            spec,
            cmcd_empty.model_dump() if hasattr(cmcd_empty, "model_dump") else cmcd_empty,
            _schema_for(spec, "POST", "/api/cmcd/aggregates"),
        )
        == []
    )

    rum_empty = rum_analytics(svc, start_s, end_s)  # type: ignore[arg-type]
    assert (
        _validate_schema(
            spec,
            rum_empty.model_dump() if hasattr(rum_empty, "model_dump") else rum_empty,
            _schema_for(spec, "GET", "/api/services/{service_id}/rum/analytics"),
        )
        == []
    )

    # Model-level parity fields checked by FE and Cloud adapters
    sec_default = SecurityAggregatesResponse(rdns_verification_available=True).model_dump()
    assert sec_default["rdns_verification_available"] is True
    assert _validate_schema(spec, sec_default, _schema_for(spec, "POST", "/api/security/aggregates")) == []

    rum_status = RumStatusResponse(enabled=False, supports_storage_purge=True).model_dump()
    assert rum_status["supports_storage_purge"] is True
    assert _validate_schema(spec, rum_status, _schema_for(spec, "GET", "/api/services/{service_id}/rum/status")) == []

    per_reason = ScoringPerReasonResponse(
        buckets=[],
        n_good=0,
        n_bad=0,
        has_min_samples_overall=False,
    ).model_dump()
    assert per_reason["buckets"] == []
    assert (
        _validate_schema(
            spec,
            per_reason,
            _schema_for(spec, "GET", "/api/services/{service_id}/scoring/evaluation/per-reason"),
        )
        == []
    )

    meta_storage = MetadataStorageResponse(
        tables={},
        db_bytes=0,
        db_path="clickhouse://metadata",
        retention={},
        supported=False,
        reason="unavailable",
    ).model_dump()
    assert _validate_schema(spec, meta_storage, _schema_for(spec, "GET", "/api/admin/metadata-storage")) == []

    usage_compute = UsageComputeResponse(source="not_applicable", reason="high_scale").model_dump()
    assert _validate_schema(spec, usage_compute, _schema_for(spec, "GET", "/api/usage/compute")) == []


def test_cmcd_and_rum_timestamp_formatting_contracts() -> None:
    spec = _load_openapi()

    class _CmcdPopulatedClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            if "cmcd:sessions_ts" in sql:
                return [{"bucket": "2026-11-05 12:00:00", "requests": 5, "sessions": 2, "new_sessions": 1}]
            return []

    svc = _empty_service(_CmcdPopulatedClient())
    start_s, end_s = "2026-11-05T11:30:00Z", "2026-11-05T12:30:00Z"
    cmcd = cmcd_aggregates(
        svc, CmcdRequest(filters={}, start_time=start_s, end_time=end_s), start_s, end_s, None, False
    )  # type: ignore[arg-type]
    cmcd_payload = cmcd.model_dump() if hasattr(cmcd, "model_dump") else cmcd
    assert _validate_schema(spec, cmcd_payload, _schema_for(spec, "POST", "/api/cmcd/aggregates")) == []
    for pt in cmcd_payload.get("sessions_ts", []):
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", pt["timestamp"]), pt


def test_shared_parity_golden_vectors() -> None:
    vectors = _load_vectors()

    # 1. PII IP masking parity
    for vec in vectors["pii_ip_masking"]:
        assert mask_ip(vec["input"]) == vec["expected"]

    # 2. 6-bucket asset classification + query/hash stripping parity
    for vec in vectors["asset_classification"]:
        assert _hs_classify_asset_url(vec["url"]) == vec["expected_asset_type"]

    # Ensure High-Scale SQL asset case contains every canonical bucket name
    for bucket in ("Images", "Documents", "JavaScript/CSS", "Fonts", "Video", "API/Dynamic"):
        assert f"'{bucket}'" in _ASSET_TYPE_CASE
    dims = [
        {"asset_type": b, "cache_status": "HIT", "content_encoding": "br", "requests": 2, "total_bytes": 100}
        for b in ("Images", "Documents", "JavaScript/CSS", "Fonts", "Video", "API/Dynamic")
    ]
    shaped_assets = compute_summary_from_dims(dims)
    assert len(shaped_assets["asset_type_breakdown"]) == 6

    # 3. WAF signal informational exclusion parity
    waf_vec = vectors["waf_signal_filtering"]
    signal_sql = INSIGHT_COLUMNS["signal"]
    for prefix in waf_vec["excluded_prefixes"]:
        assert prefix in signal_sql
    for exact in waf_vec["excluded_exact"]:
        assert exact in signal_sql
    assert _hs_filter_waf_tags(waf_vec["raw_tags"]) == waf_vec["expected_attack_signals"]

    # 4. Cookie compliance 6-char VCL normalization parity
    for vec in vectors["cookie_compliance_normalization"]:
        assert _normalize_cookie_compliance(vec["input"]) == vec["expected"]

    # 5. Cache-collapse detail calculation & IP masking parity
    cc = vectors["cache_collapse_detail"]
    rates_obj = cc["rates"]
    rates = (
        rates_obj["w_hits"],
        rates_obj["w_miss"],
        rates_obj["w_pass"],
        rates_obj["w_total"],
        rates_obj["b_hits"],
        rates_obj["b_miss"],
        rates_obj["b_pass"],
        rates_obj["b_total"],
    )
    exp = cc["expected"]
    masked = shape_cache_collapse_detail(
        cc["url"],
        rates,
        cc["timeline_rows"],
        cc["miss_rows"],
        mask_ips=True,
        include_has_data=True,
    )
    assert masked["window_hit_rate"] == exp["window_hit_rate"]
    assert masked["baseline_hit_rate"] == exp["baseline_hit_rate"]
    assert masked["breakdown"] == exp["breakdown"]
    assert masked["has_data"] is exp["has_data"]
    assert masked["timeline"][0]["expected_hits"] == exp["timeline_expected_hits"]
    assert masked["timeline"][0]["hit_rate"] == pytest.approx(exp["timeline_hit_rate"], rel=1e-6)
    assert masked["recent_misses"][0]["ip"] == exp["masked_ip"]

    unmasked = shape_cache_collapse_detail(
        cc["url"],
        rates,
        cc["timeline_rows"],
        cc["miss_rows"],
        mask_ips=False,
        include_has_data=True,
    )
    assert unmasked["recent_misses"][0]["ip"] == exp["unmasked_ip"]
