"""Consolidated tests for test_main."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

# ── Shared Helpers ───────────────────────────────────────────────────────────

# ── Merged from tests/test_openapi_and_contracts.py ──

"""Consolidated tests for test_openapi_and_contracts."""


import os

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNAPSHOT_PATH = os.path.join(REPO_ROOT, "frontend", "openapi.json")


def test_openapi_snapshot_matches_live_app():
    """``frontend/openapi.json`` must match what the running app emits.

    When this fails, regenerate the snapshot locally:

        cd frontend && npm run gen:types

    Then commit ``frontend/openapi.json`` and ``types/api.generated.ts``.

    Failing here is a *signal*, not a flake — every backend change to a
    router, request/response model, or status-code annotation needs the
    frontend types regenerated so the openapi-fetch call sites stay
    type-correct against the real wire format.
    """
    from backend.main import app

    live = app.openapi()

    if not os.path.exists(SNAPSHOT_PATH):
        pytest.fail(f"Snapshot missing: {SNAPSHOT_PATH}\nRun: cd frontend && npm run gen:types")

    with open(SNAPSHOT_PATH) as f:
        snapshot = json.load(f)

    if live == snapshot:
        return

    # Helpful diff: name the top-level keys that drifted so the reader
    # can find the offending router without ploughing through 16k lines.
    live_paths = set(live.get("paths", {}).keys())
    snap_paths = set(snapshot.get("paths", {}).keys())
    added = sorted(live_paths - snap_paths)
    removed = sorted(snap_paths - live_paths)

    live_schemas = set(live.get("components", {}).get("schemas", {}).keys())
    snap_schemas = set(snapshot.get("components", {}).get("schemas", {}).keys())
    added_schemas = sorted(live_schemas - snap_schemas)
    removed_schemas = sorted(snap_schemas - live_schemas)

    # Find paths whose definitions changed (same key, different value)
    shared_paths = live_paths & snap_paths
    changed = sorted(p for p in shared_paths if live["paths"][p] != snapshot["paths"][p])

    msg = ["OpenAPI snapshot is stale. Regenerate with:\n"]
    msg.append("    cd frontend && npm run gen:types\n")
    msg.append("Then commit frontend/openapi.json and frontend/types/api.generated.ts.\n")
    if added:
        msg.append(f"\nRoutes added (in live, not in snapshot): {added}")
    if removed:
        msg.append(f"\nRoutes removed (in snapshot, not in live): {removed}")
    if changed:
        # Cap the list to avoid wall-of-text — first 10 is plenty to
        # point the reader at the offending router.
        shown = changed[:10]
        rest = len(changed) - len(shown)
        msg.append(f"\nRoutes whose schema drifted: {shown}")
        if rest > 0:
            msg.append(f" (+{rest} more)")
    if added_schemas:
        msg.append(f"\nModels added: {added_schemas}")
    if removed_schemas:
        msg.append(f"\nModels removed: {removed_schemas}")
    if not (added or removed or changed or added_schemas or removed_schemas):
        # Top-level diff (version bump, info change, etc.)
        live_keys = set(live.keys())
        snap_keys = set(snapshot.keys())
        msg.append(f"\nTop-level diff: live={sorted(live_keys)} snapshot={sorted(snap_keys)}")
        if "info" in live and "info" in snapshot and live["info"] != snapshot["info"]:
            msg.append(f"\ninfo block changed: live={live['info']} snapshot={snapshot['info']}")

    pytest.fail("\n".join(msg))


# ── Merged from test_openapi_typed_responses.py ──────────────────────────────────────────────


# Empty: every JSON route has a response_model. Never add an entry — type the route instead.
UNTYPED_JSON: frozenset[str] = frozenset()

NON_JSON_EXEMPT = frozenset(
    {
        "GET /api/admin/quarantine/download/{item_id}",
        "GET /api/services/{service_id}/control-room/error-stream",
        "GET /api/admin/events/stream",
        "GET /api/admin/usage-log/export",
        "GET /api/cron-runs/{run_id}/stream",
        "GET /api/download",
        "GET /api/download-all",
        "GET /api/download-folder",
        "GET /api/log-extents/stream",
        "GET /api/security/proxies/export",
        "GET /api/services/{service_id}/realtime-stream",
        "GET /api/share/oauth/authorize",
        "GET /api/share/oauth/callback",
        "GET /js/faro-sdk.js",
        "GET /js/rum.js",
        "POST /api/admin/metadata-cleanup",
        "POST /api/admin/metadata-cleanup/{service_id}",
        "POST /api/admin/reset-logs",
        "POST /api/admin/reset-rum",
        "POST /api/admin/share/backup/export",
        "POST /api/dashboard/raw/csv",
        "POST /api/provision/execute",
        "POST /api/provision/reconcile",
        "POST /api/provision/teardown",
        "POST /api/services/{service_id}/cmcd/disable",
        "POST /api/services/{service_id}/cmcd/enable",
        "POST /api/services/{service_id}/cron-settings",
        "POST /api/services/{service_id}/logging-settings/update",
        "POST /api/services/{service_id}/ngwaf-sync",
        "POST /api/services/{service_id}/rum/disable",
        "POST /api/services/{service_id}/rum/enable",
        "POST /api/services/{service_id}/rum/upgrade",
        "POST /api/services/{service_id}/scoring/disable",
        "POST /api/services/{service_id}/scoring/enable",
    }
)

UNTYPED_JSON_CEILING = 0
_METHODS = ("get", "post", "put", "patch", "delete")


def _is_untyped(schema: dict | None) -> bool:
    if not schema:
        return True
    if "$ref" in schema or "anyOf" in schema or "oneOf" in schema or "allOf" in schema:
        return False
    if schema.get("type") == "array":
        return False
    if schema.get("properties"):
        return False
    if schema.get("type") in (None, "object"):
        return schema.get("additionalProperties") in (None, True, {})
    return False


@pytest.fixture(scope="module")
def untyped_ops() -> set[str]:
    from backend.main import app

    ops: set[str] = set()
    for path, item in app.openapi()["paths"].items():
        for method in _METHODS:
            op = item.get(method)
            if not op:
                continue
            for code, resp in op.get("responses", {}).items():
                json_body = (resp.get("content") or {}).get("application/json")
                if code.startswith("2") and json_body is not None and _is_untyped(json_body.get("schema")):
                    ops.add(f"{method.upper()} {path}")
    return ops


def test_no_new_untyped_json_operations(untyped_ops):
    new = untyped_ops - UNTYPED_JSON - NON_JSON_EXEMPT
    assert not new, f"Add a response_model (extra='allow' + response_model_exclude_unset=True) to: {sorted(new)}"


def test_allowlists_have_no_stale_entries(untyped_ops):
    stale = (UNTYPED_JSON | NON_JSON_EXEMPT) - untyped_ops
    assert not stale, f"Now typed or removed; delete from the allowlists: {sorted(stale)}"


def test_untyped_json_backlog_only_shrinks():
    assert len(UNTYPED_JSON) <= UNTYPED_JSON_CEILING
    assert not UNTYPED_JSON & NON_JSON_EXEMPT


def test_export_downloads_declare_their_body_in_openapi():
    """File-download routes must say what they return so the client never sees ``unknown``."""
    from backend.main import app

    paths = app.openapi()["paths"]

    tf = paths["/api/provision/terraform/export"]["post"]["responses"]["200"]["content"]
    assert tf == {"application/zip": {"schema": {"type": "string", "format": "binary"}}}

    cf = paths["/api/services/{service_id}/custom-fields/export"]["get"]["responses"]["200"]["content"]
    assert cf == {"application/json": {"schema": {"$ref": "#/components/schemas/CustomFieldsExportFile"}}}


def test_custom_fields_export_file_matches_import_body():
    from backend.models.common import CustomFieldsExportFile, CustomFieldsImportBody

    assert (
        CustomFieldsExportFile.model_json_schema()["properties"]
        == CustomFieldsImportBody.model_json_schema()["properties"]
    )


# ── Merged from test_error_envelope_contract.py ──────────────────────────────────────────────

from backend.main import app

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")
# The middleware/handler-produced codes that DEFAULT_ERROR_RESPONSES
# documents. 422 is deliberately excluded here — it's covered by its own
# shape check (A); this set is the "did the router get the canonical
# mapping at all" signal (B).
_CANONICAL_CODES = {"400", "401", "403", "404", "429", "500", "502", "503"}

# Operations that intentionally keep FastAPI's auto-generated
# ``HTTPValidationError`` 422 instead of the canonical ``ErrorEnvelope``.
# ``GET /api/health`` is live-fuzzed by tests/test_schemathesis_smoke.py and
# has typed query params, so its documented 422 must match the REAL
# request-validation body. There is no app-wide RequestValidationError
# reshaper, so the only accurate documentation for its 422 is
# HTTPValidationError (reshaping the body would be an out-of-scope
# wire-format change). The carve-out asserts that exact shape below so it
# can't silently mask some other wrong model.
_ACCURATE_VALIDATION_422 = {"GET    /api/health"}


def _operations():
    """Yield ``(label, operation_dict)`` for every documented HTTP op."""
    paths = app.openapi()["paths"]
    for path, item in sorted(paths.items()):
        for method, op in item.items():
            if method in _HTTP_METHODS:
                yield f"{method.upper():6} {path}", op


def _ref_of(op: dict, code: str) -> str | None:
    """The response-schema ``$ref`` for ``code``, or None if absent."""
    try:
        return op["responses"][code]["content"]["application/json"]["schema"]["$ref"]
    except (KeyError, TypeError):
        return None


def test_documented_422_is_error_envelope():
    """(A) Any operation documenting a 422 must reference ErrorEnvelope,
    except the explicitly-carved-out auto-validation endpoints."""
    offenders = [
        (label, ref.split("/")[-1])
        for label, op in _operations()
        if label not in _ACCURATE_VALIDATION_422
        and (ref := _ref_of(op, "422")) is not None
        and not ref.endswith("ErrorEnvelope")
    ]
    assert not offenders, (
        "These operations document 422 with a non-canonical model "
        "(add responses=DEFAULT_ERROR_RESPONSES to the router):\n"
        + "\n".join(f"  {label} -> {model}" for label, model in offenders)
    )


def test_carved_out_422_is_the_accurate_auto_validation_shape():
    """The carve-out is tight: each exempted op must document 422 as
    FastAPI's HTTPValidationError (the real request-validation body), not
    some other accidental model — otherwise the exemption would mask a
    genuine drift."""
    ops = dict(_operations())
    for label in _ACCURATE_VALIDATION_422:
        assert label in ops, f"carve-out names a non-existent operation: {label}"
        ref = _ref_of(ops[label], "422")
        assert ref is not None and ref.endswith("HTTPValidationError"), (
            f"{label} is carved out of the ErrorEnvelope-422 rule but its 422 "
            f"is {ref!r}, not HTTPValidationError — remove the carve-out or fix the shape."
        )


def test_every_operation_documents_a_canonical_error_code():
    """(B) Every operation documents at least one canonical error code."""
    missing = [label for label, op in _operations() if not (_CANONICAL_CODES & set(op.get("responses", {}).keys()))]
    assert not missing, (
        "These operations document none of the canonical error codes "
        f"{sorted(_CANONICAL_CODES)} (missing DEFAULT_ERROR_RESPONSES):\n"
        + "\n".join(f"  {label}" for label in missing)
    )


# ── Merged from test_response_contract.py ──────────────────────────────────────────────


import pytest
from pydantic import AliasChoices

from tests._helpers.duckdb_fixtures import make_seeded_logs_con

# Telemetry / debug keys that every repo merges in via `**runner.telemetry()`.
# These are intentionally stripped from the FE-visible payload because the
# FE has its own request tracing. Listing them here documents the
# stripping intent so a new telemetry key surfacing in a different repo
# fails the test.
_ALLOWED_DROPPED_KEYS = {
    "debug_queries",
    "debug_calls",
    "_debug_queries",
    "_debug_calls",
    "_is_cached",
    # The session-scoring reads run through the `_cached` wrapper, which also
    # bakes a per-handler `_section_timings` entry into the envelope (the
    # dashboard repos emit it under the same name). It's telemetry, not data.
    "_section_timings",
    # `insights.get_insights` computes a window-total row count that the FE
    # doesn't currently render. Keeping it as repo-internal telemetry rather
    # than declaring it on InsightsResponse keeps the FE TS surface minimal.
    # Move to the model if/when the FE actually needs it.
    "window_total_requests",
}

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


@pytest.fixture
def seeded_con():
    """Seeded DuckDB used across every contract test."""
    con = make_seeded_logs_con(_TABLE_NAME, num_rows=20)
    yield con
    con.close()


def _check_no_dropped_keys(repo_result: dict, response_model_cls, label: str):
    """Assert every key in ``repo_result`` is either a declared field on
    the response model OR in the allowlist of intentionally-stripped
    telemetry keys."""
    declared_fields = set(response_model_cls.model_fields.keys())
    for field in response_model_cls.model_fields.values():
        alias = field.validation_alias
        if isinstance(alias, str):
            declared_fields.add(alias)
        elif isinstance(alias, AliasChoices):
            declared_fields.update(choice for choice in alias.choices if isinstance(choice, str))
    repo_keys = set(repo_result.keys())
    dropped = repo_keys - declared_fields - _ALLOWED_DROPPED_KEYS
    assert not dropped, (
        f"{label}: repo returned keys not declared on {response_model_cls.__name__} "
        f"and not in the allowlist: {sorted(dropped)}\n"
        f"Either add them to the model (FE wants them) or add them to "
        f"_ALLOWED_DROPPED_KEYS (intentionally stripped)."
    )


# ── dashboard.get_aggregates ↔ AggregatesResponse ────────────────────────


def test_cache_observation_alias_is_declared_and_preserved():
    from backend.models.common import BaseResponse

    payload = {"_response_cache": {"state": "miss", "ttl_seconds": 15, "prewarmer": False}}
    _check_no_dropped_keys(payload, BaseResponse, "cache observation")
    assert BaseResponse(**payload).model_dump(by_alias=True)["_response_cache"] == payload["_response_cache"]
    with pytest.raises(AssertionError, match="undeclared_key"):
        _check_no_dropped_keys({"undeclared_key": True}, BaseResponse, "invalid observation")


def test_dashboard_aggregates_contract(seeded_con):
    """`get_aggregates` return keys must match `AggregatesResponse` —
    minus the explicit telemetry allowlist. Pinned because losing this
    would let a repo refactor silently drop a field the FE renders
    (the regression mode of the `window_total_requests` bug)."""
    from backend.models.analytics import AggregatesResponse
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    _dashboard_cache.clear()
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    _check_no_dropped_keys(result, AggregatesResponse, "dashboard.get_aggregates")


# ── insights.get_insights ↔ InsightsResponse ─────────────────────────────


def test_insights_get_insights_contract(seeded_con):
    """`get_insights` return keys must match `InsightsResponse`.

    Regression-pinned for the original drop: ``window_total_requests``
    was in the repo's return dict but not declared on
    ``InsightsResponse``, so Pydantic silently stripped it and the FE
    saw `undefined`. The fix was to add it to the model — this test
    keeps it from regressing."""
    from backend.models.analytics import InsightsResponse
    from backend.repositories.insights import _insights_cache, get_insights

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(seeded_con, _src, window_hours=1, baseline_hours=1)
    _check_no_dropped_keys(result, InsightsResponse, "insights.get_insights")


# ── origin.get_summary ↔ OriginSummaryResponse ───────────────────────────


def test_origin_get_summary_contract(seeded_con):
    """`get_summary` return keys must match `OriginSummaryResponse`."""
    from backend.models.analytics import OriginSummaryResponse
    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginSummaryResponse, "origin.get_summary")


# ── origin.get_timeseries ↔ OriginTimeseriesResponse ─────────────────────


def test_origin_get_timeseries_contract(seeded_con):
    """`get_timeseries` return keys must match `OriginTimeseriesResponse`."""
    from backend.models.analytics import OriginTimeseriesResponse
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginTimeseriesResponse, "origin.get_timeseries")


# ── performance.get_performance_aggregates ↔ PerformanceAggregatesResponse


def test_performance_aggregates_contract(seeded_con):
    """`get_performance_aggregates` return keys must match
    `PerformanceAggregatesResponse`."""
    from backend.models.analytics import PerformanceAggregatesResponse
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, PerformanceAggregatesResponse, "performance.get_performance_aggregates")


# ── security.get_security_aggregates ↔ SecurityAggregatesResponse ────────


def test_security_aggregates_contract(seeded_con):
    """`get_security_aggregates` return keys must match
    `SecurityAggregatesResponse`."""
    from backend.models.analytics import SecurityAggregatesResponse
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, SecurityAggregatesResponse, "security.get_security_aggregates")


# ── network.get_health ↔ NetworkHealthResponse ───────────────────────────


def test_network_health_contract(seeded_con):
    """`get_health` return keys must match `NetworkHealthResponse`."""
    from backend.models.analytics import NetworkHealthResponse
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, NetworkHealthResponse, "network.get_health")


# ── Session-scoring analyst-safe reads ↔ scoring response models ──────────
#
# Unlike the repo contracts above (which take a `con`), the scoring reads are
# router handlers that pull rows via `_query_logs` and route through `_cached`,
# so we patch `_query_logs`/`_table_columns`/labels and call the handler
# directly with a stub admin request (no analyst session → maximal key set).
# The handlers' `extra="allow"` + `response_model_exclude_unset` mean nothing
# is stripped at runtime; this guard keeps each DECLARED model a superset of
# the produced shape so the generated TS types stay complete and a future key
# can't slip past the FE contract.

import types
from unittest.mock import patch as _patch


def _admin_request():
    """Stub Request on the admin (loopback) path — no analyst session."""
    return types.SimpleNamespace(state=types.SimpleNamespace(analyst_session=None))


def _check_scoring_keys(produced: dict, model_cls, label: str):
    _check_no_dropped_keys(produced, model_cls, label)


def test_scoring_top_flagged_contract():
    from backend.models.analytics import ScoringTopFlaggedResponse, ScoringTopFlaggedRow
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "timestamp": "2026-06-01T10:00:00+00:00",
            "edge_sid": "a",
            "edge_score": 90,
            "edge_score_l1": 40,
            "edge_score_l2": 50,
            "edge_cookie_compliance": "ok",
            "edge_score_reason": "rare-transition",
            "ip": "1.1.1.1",
            "ua": "UA",
            "url": "/p",
            "status": 200,
            "country": "US",
        }
    ]
    with _patch("backend.repositories.sessions.query_logs", return_value=rows):
        result = ss.scoring_top_flagged(request=_admin_request(), service_id="csvc", since_hours=24, limit=50)
    _check_scoring_keys(result, ScoringTopFlaggedResponse, "scoring.top_flagged")
    _check_no_dropped_keys(result["rows"][0], ScoringTopFlaggedRow, "scoring.top_flagged[row]")


def test_scoring_score_distribution_contract():
    from backend.models.analytics import ScoringDistributionRow, ScoringScoreDistributionResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "bucket": "75-100", "count": 5}]
    with _patch("backend.repositories.sessions.query_logs", return_value=rows):
        result = ss.scoring_score_distribution(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringScoreDistributionResponse, "scoring.score_distribution")
    _check_no_dropped_keys(result["rows"][0], ScoringDistributionRow, "scoring.score_distribution[row]")


def test_scoring_compliance_breakdown_contract():
    from backend.models.analytics import ScoringComplianceBreakdownResponse, ScoringComplianceRow
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "compliance": "ok", "count": 200}]
    with _patch("backend.repositories.sessions.query_logs", return_value=rows):
        result = ss.scoring_compliance_breakdown(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringComplianceBreakdownResponse, "scoring.compliance_breakdown")
    _check_no_dropped_keys(result["rows"][0], ScoringComplianceRow, "scoring.compliance_breakdown[row]")


def test_scoring_latency_timeseries_contract():
    """DICT-01: the latency rows carry the column-dependent percentile fields
    only on re-provisioned services — the maximal row must be a subset of
    ScoringLatencyRow so the FE useScorerTimeseries types stay complete."""
    from backend.models.analytics import ScoringLatencyRow, ScoringLatencyTimeseriesResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "hour": "2026-06-17T10:00:00+00:00",
            "scored_count": 500,
            "fail_open_count": 30,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 45000,
            "rtt_p99_us": 95000,
            "exec_p50_us": 540,
            "exec_p95_us": 900,
        }
    ]
    cols = {"edge_score", "edge_score_reason", "edge_score_rtt_us", "edge_score_exec_us"}
    with (
        _patch("backend.repositories.sessions.query_logs", return_value=rows),
        _patch.object(ss, "_table_columns", lambda sid: cols),
    ):
        result = ss.scoring_latency_timeseries(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringLatencyTimeseriesResponse, "scoring.latency_timeseries")
    _check_no_dropped_keys(result["rows"][0], ScoringLatencyRow, "scoring.latency_timeseries[row]")


def test_scoring_health_contract():
    from backend.models.analytics import (
        ScoringHealthResponse,
        ScoringLatencySnapshot,
        ScoringMatrixStaleness,
        ScoringReasonCount,
    )
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 800,
            "distinct_sids": 50,
            "avg_score": 12.5,
            "p50_score": 5,
            "p95_score": 60,
            "max_score": 100,
            "scorer_errors": 40,
            "top_reasons": [{"reason": "cookie-missing", "count": 10}],
            "fail_open_breakdown": [{"reason": "compute-unavailable-503", "count": 2}],
            "l2_evaluated": 200,
            "l2_high_count": 10,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 42000,
            "rtt_p99_us": 91000,
            "rtt_max_us": 100000,
            "exec_p50_us": 540,
            "exec_p95_us": 880,
        }
    ]
    cols = {
        "edge_score",
        "edge_score_l2",
        "edge_score_reason",
        "edge_cookie_compliance",
        "edge_sid",
        "edge_score_rtt_us",
        "edge_score_exec_us",
    }
    with (
        _patch("backend.repositories.sessions.query_logs", return_value=agg),
        _patch.object(ss, "_table_columns", lambda sid: cols),
        _patch("backend.routers.session_scoring.l2_enforce_readiness_block", lambda sid: {"enabled": False}),
    ):
        result = ss.scoring_health(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringHealthResponse, "scoring.health")
    _check_no_dropped_keys(result["latency"], ScoringLatencySnapshot, "scoring.health.latency")
    _check_no_dropped_keys(result["matrix_staleness"], ScoringMatrixStaleness, "scoring.health.matrix_staleness")
    _check_no_dropped_keys(result["top_reasons"][0], ScoringReasonCount, "scoring.health.top_reasons[item]")


def test_scoring_evaluation_contract():
    """Exercise the scored branch (the maximal key set: auc / passed /
    threshold / default_min_auc / n_reconstructed / n_labels_total)."""
    from backend.models.analytics import ScoringEvaluationResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    result_obj = types.SimpleNamespace(n_good=5, n_bad=5, auc=0.9, passed=True, pass_threshold=0.7)
    with (
        _patch("backend.scoring.counts_by_label", return_value={"good": 5, "bad": 5, "neutral": 1}),
        _patch("backend.scoring.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_load_matrix", lambda sid: {"version": "v9", "transitions": {}}),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: [({"max_edge_score": 90}, "bad")]),
        _patch("backend.scoring.evaluate_from_persisted_scores", return_value=result_obj),
        _patch("backend.config.load_config", return_value={"scoring": {"matrix_version": "v8"}}),
    ):
        result = ss.scoring_evaluation(service_id="csvc")
    _check_scoring_keys(result, ScoringEvaluationResponse, "scoring.evaluation")


def test_scoring_curves_contract():
    """Exercise the scored branch (roc / pr / auc / average_precision)."""
    from backend.models.analytics import ScoringCurvePoint, ScoringCurvesResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    sessions = [({"max_edge_score": 90}, "bad"), ({"max_edge_score": 5}, "good")]
    with (
        _patch("backend.scoring.counts_by_label", return_value={"good": 5, "bad": 5}),
        _patch("backend.scoring.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: sessions),
    ):
        result = ss.scoring_curves(service_id="csvc")
    _check_scoring_keys(result, ScoringCurvesResponse, "scoring.curves")
    if result.get("roc"):
        _check_no_dropped_keys(result["roc"][0], ScoringCurvePoint, "scoring.curves.roc[point]")
    if result.get("pr"):
        _check_no_dropped_keys(result["pr"][0], ScoringCurvePoint, "scoring.curves.pr[point]")


def test_scoring_threshold_preview_contract():
    from backend.models.analytics import ScoringThresholdBucket, ScoringThresholdPreviewResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total": 100,
            "flagged_total": 20,
            "flagged_good": 1,
            "flagged_bad": 5,
            "passed_good": 10,
            "passed_bad": 2,
        }
    ]
    with (
        _patch("backend.repositories.sessions.query_logs", return_value=agg),
        _patch("backend.scoring.list_labels", return_value=[{"sid": "s1", "label": "bad"}]),
        _patch("backend.scoring.counts_by_label", return_value={"good": 5, "bad": 5}),
    ):
        result = ss.scoring_threshold_preview(request=_admin_request(), service_id="csvc", threshold=75, since_hours=24)
    _check_scoring_keys(result, ScoringThresholdPreviewResponse, "scoring.threshold_preview")
    _check_no_dropped_keys(result["flagged"], ScoringThresholdBucket, "scoring.threshold_preview.flagged")
    _check_no_dropped_keys(result["passed"], ScoringThresholdBucket, "scoring.threshold_preview.passed")


def test_scoring_analytics_composite_contract():
    """Admin path includes all five analyst-safe sub-keys plus the two
    admin-only evaluation blocks — all must be declared on the composite."""
    import backend.routers.session_scoring as ss_admin
    from backend.models.analytics import ScoringAnalyticsResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    with (
        _patch.object(ss, "scoring_top_flagged", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_score_distribution", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False}),
        _patch.object(ss, "scoring_health", lambda **kw: {"since_hours": 24}),
        _patch.object(ss, "scoring_evaluation", lambda **kw: {"has_min_samples": False}),
        _patch.object(ss_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []}),
    ):
        result = ss.scoring_analytics_composite(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringAnalyticsResponse, "scoring.analytics")


# ── Merged from test_schemathesis_smoke.py ──────────────────────────────────────────────

import hypothesis
import pytest
import schemathesis
from schemathesis import checks

# Hypothesis settings profile: schemathesis fuzzes each endpoint with
# ~100 generated cases by default → ~5 min wall-clock for 3 endpoints.
# Cap at 20 cases per endpoint so the full test runs in <60s and fits
# under the pytest-timeout default. Bump locally with --hypothesis-profile=full
# when chasing a regression.
hypothesis.settings.register_profile(
    "schemathesis_smoke",
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=list(hypothesis.HealthCheck),
)
hypothesis.settings.load_profile("schemathesis_smoke")


# Zero-side-effect read endpoints safe to fuzz without DB / network setup.
# Session-gated endpoints (share/tos, share/heartbeat) will 401 without a
# cookie — schemathesis still validates the 401 conforms to the documented
# ErrorEnvelope schema, which is the load-bearing contract.
ALLOWED_PATHS = frozenset(
    {
        "/api/health",
        "/api/bootstrap",
        "/api/schema",
        "/api/share/auth-config",
        "/api/share/tos",
        "/api/share/heartbeat",
        "/api/admin/app-config/query-monitor",
        "/api/log-fields/catalog",
    }
)

_PATH_REGEX = "|".join(p.replace("/", r"\/") for p in ALLOWED_PATHS)

# Load + filter the schema at module import. Schemathesis' @schema.parametrize
# decorator generates one pytest case per (op, body shape) combination so the
# test surface scales with ALLOWED_PATHS.
schema = schemathesis.openapi.from_asgi("/openapi.json", app)
schema = schema.include(path_regex=rf"^({_PATH_REGEX})$")


@pytest.mark.slow
@pytest.mark.timeout(120)  # schemathesis can take ~60-90s even with the smoke profile
@schema.parametrize()
def test_safe_read_endpoints_conform(case):
    """For every generated (path × header × query × body) case:

    - ``not_a_server_error`` — no input combination produces a 5xx.
    - ``status_code_conformance`` — the returned status code is in the
      route's documented ``responses=`` mapping (router-wide default
      now covers 400/401/403/404/422/429/500/502/503).
    - ``response_schema_conformance`` — the response body validates
      against the documented schema (``ErrorEnvelope`` for 4xx/5xx,
      the per-route ``response_model`` for 2xx).
    """
    response = case.call()
    case.validate_response(
        response,
        checks=(
            checks.not_a_server_error,
            checks.status_code_conformance,
            checks.response_schema_conformance,
        ),
    )


# ── Merged from test_changelog_breaking_parity.py ──────────────────────────────────────────────

import os
import re

from backend.main import app

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_BASELINE_PATH = os.path.join(_REPO_ROOT, "tests", "contract", "openapi_baseline.json")
_CHANGELOG_PATH = os.path.join(_REPO_ROOT, "CHANGELOG.md")


def _normalize_params(text: str) -> str:
    """Collapse ``{anything}`` path params to ``{}`` so a path matches the
    CHANGELOG regardless of which param name each side spells (the spec uses
    ``{service_id}``; prose often writes ``{id}``)."""
    return re.sub(r"\{[^}]+\}", "{}", text)


def _changelog_breaking_section() -> str:
    """Return all ``### Breaking`` sections across the changelog."""
    with open(_CHANGELOG_PATH) as f:
        text = f.read()
    headings = list(re.finditer(r"^### Breaking\s*$", text, flags=re.MULTILINE))
    sections = []
    for heading in headings:
        section_start = heading.end()
        next_version = re.search(r"^## ", text[section_start:], flags=re.MULTILINE)
        section_end = section_start + next_version.start() if next_version else len(text)
        sections.append(text[section_start:section_end])
    return "\n".join(sections)


def test_removed_paths_are_noted_in_changelog():
    with open(_BASELINE_PATH) as f:
        baseline = json.load(f)
    live = app.openapi()

    baseline_paths = set(baseline.get("paths", {}))
    live_paths = set(live.get("paths", {}))
    removed = sorted(baseline_paths - live_paths)

    breaking = _normalize_params(_changelog_breaking_section())
    missing = [p for p in removed if _normalize_params(p) not in breaking]

    assert not missing, (
        "These paths were removed from the OpenAPI surface but are not noted in "
        f"the CHANGELOG ### Breaking section: {missing}\n"
        "Add a Breaking bullet naming each path, then regenerate the baseline "
        "(jq '{paths: (.paths | with_entries(.value = {}))}' frontend/openapi.json > tests/contract/openapi_baseline.json) "
        "as a deliberate release step."
    )
