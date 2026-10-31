"""Session scoring router package — combines analytics, admin, and VCL submodules."""
# ruff: noqa: F401, F403, E402, I001

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import threading
import time as _time
import types as _types
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query, Request

from backend.core import metadata as metadata_db
from backend.core.request_context import (
    RequestContext,
    ServiceId,
    SessionTokenError,
    build_request_context,
    open_session_token,
    require_admin,
    seal_session_token,
)
from backend.core.storage import execute_with_stale_view_retry
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.models.analytics import SessionDetailRequest, SessionDetailResponse, SessionsRequest, SessionsResponse
from backend.models.analytics import (
    ScoringAnalyticsResponse,
    ScoringAuditListResponse,
    ScoringComplianceBreakdownResponse,
    ScoringConfigCompositeResponse,
    ScoringCurvesResponse,
    ScoringDashboardResponse,
    ScoringEnforceStatusCodeBody,
    ScoringEnforceStatusCodePutResponse,
    ScoringEnforceStatusCodeState,
    ScoringEnforceThresholdPutResponse,
    ScoringEnforceThresholdResponse,
    ScoringEvaluationResponse,
    ScoringExcludeRegexBody,
    ScoringExcludeRegexPutResponse,
    ScoringExcludeRegexState,
    ScoringExcludeRegexValidateResponse,
    ScoringHealthResponse,
    ScoringL2EnforceBody,
    ScoringL2EnforcePutResponse,
    ScoringL2EnforceState,
    ScoringLabelCreate,
    ScoringLabelDeleteResponse,
    ScoringLabelRow,
    ScoringLabelUpdate,
    ScoringLabelsListResponse,
    ScoringLatencyTimeseriesResponse,
    ScoringMatrixRestoreResponse,
    ScoringMatrixVersionsResponse,
    ScoringPerReasonResponse,
    ScoringRetrainResponse,
    ScoringRotateKeyResponse,
    ScoringScoreDistributionResponse,
    ScoringSessionEventsResponse,
    ScoringStatusResponse,
    ScoringThresholdBody,
    ScoringThresholdPreviewResponse,
    ScoringThresholdState,
    ScoringTokenBody,
    ScoringTopFlaggedResponse,
)
from backend.provision.session_scoring_orchestrator import SCORING_ENABLED_AT_KEY as _SCORING_ENABLED_AT_KEY
from backend.repositories import sessions as _scoring_repo
from backend.repositories import sessions as repo
from backend.utils.auth import mask_ips_for, require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import clamp_or_400, get_analyst_time_bounds
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    not_found,
    query_errors,
    raise_internal,
    resolve_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response, stream_iac_reconcile
from backend.utils.telemetry import process_context_scope

from . import scoring_vcl, scoring_analytics, scoring_admin
from .scoring_admin import (
    _ENFORCE_THRESHOLD_CACHE_TTL,
    _ENFORCE_THRESHOLD_KEY,
    _L2_ENABLED_AT_KEY,
    _L2_ENFORCE_ENABLED_KEY,
    _L2_RAMP_DAYS,
    _L2_READINESS_DAYS,
    _build_l2_enforce_block,
    _build_scoring_config_bundle,
    _enforce_threshold_cache,
    _l2_enforce_cache,
    _l2_enforce_unavailable,
    _l2_get_item,
    _l2_parse_epoch,
    _push_matrix_to_kv_if_configured,
    _read_l2_enforce_state,
    _require_scoring_config_store,
    _upsert_config_store_item,
    _validate_threshold_0_100,
    l2_enforce_readiness_block,
    scoring_audit_list,
    scoring_config_composite,
    scoring_enforce_threshold_get,
    scoring_enforce_threshold_put,
    scoring_l2_enforce_get,
    scoring_l2_enforce_put,
    scoring_matrix_versions_list,
    scoring_matrix_versions_restore,
    scoring_retrain,
    scoring_rotate_key,
    scoring_threshold_get,
    scoring_threshold_put,
)
from .scoring_analytics import (
    _ANALYTICS_TTL_SEC,
    _LABEL_PII_FIELDS,
    _MIN_LABELS_PER_CLASS,
    _RECONSTRUCT_CACHE_TTL,
    _analytics_cache,
    _analytics_cache_lock,
    _bust_analytics_cache,
    _cached,
    _cached_scoring_rows,
    _check_min_samples,
    _dispatch_sessions_query,
    _enforce_scoring_service_scope,
    _fetch_session_events,
    _finalize_cached,
    _finalize_label_write,
    _inflight,
    _is_analyst_request,
    _isoformat_ts,
    _label_counts,
    _labels_fingerprint,
    _load_matrix,
    _prepare_scoring_query,
    _project_label_for_analyst,
    _read_matrix_file,
    _reconstruct_labeled_sessions,
    _reconstruct_labeled_sessions_cache,
    _scoring_source,
    _scoring_time_window,
    _seal_session_rows,
    _strip_pii_fields_for_analyst,
    _table_columns,
    _table_columns_cache,
    _tenant_matrix_path,
    scoring_analytics_composite,
    scoring_compliance_breakdown,
    scoring_curves,
    scoring_dashboard,
    scoring_evaluation,
    scoring_evaluation_per_reason,
    scoring_health,
    scoring_labels_create,
    scoring_labels_delete,
    scoring_labels_list,
    scoring_labels_update,
    scoring_latency_timeseries,
    scoring_score_distribution,
    scoring_session_events,
    scoring_threshold_preview,
    scoring_top_flagged,
    sessions_detail,
    sessions_endpoint,
    sessions_router,
)
from .scoring_vcl import (
    _SECRET_KEYS,
    _apply_vcl_snippet_update,
    _cached_fastly_probe,
    _load_scoring_cfg,
    _record_scoring_audit,
    _record_scoring_toggle_audit,
    _require_confirm,
    _require_fastly_token,
    _require_scoring_enabled_and_token,
    _safe_scoring_dict,
    _scoring_pragma_cache,
    _scoring_svc_version_cache,
    _validate_service_exclude_regex,
    scoring_disable,
    scoring_enable,
    scoring_enforce_status_code_get,
    scoring_enforce_status_code_put,
    scoring_exclude_regex_get,
    scoring_exclude_regex_put,
    scoring_exclude_regex_validate,
    scoring_status,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/services", tags=["session-scoring"], responses=DEFAULT_ERROR_RESPONSES)
router.include_router(scoring_vcl.router)
router.include_router(scoring_analytics.router)
router.include_router(scoring_admin.router)

_SCORING_ROUTE_ORDER = {
    name: idx
    for idx, name in enumerate(
        (
            "scoring_enable",
            "scoring_disable",
            "scoring_analytics_composite",
            "scoring_config_composite",
            "scoring_status",
            "scoring_labels_list",
            "scoring_labels_create",
            "scoring_labels_update",
            "scoring_labels_delete",
            "scoring_top_flagged",
            "scoring_score_distribution",
            "scoring_compliance_breakdown",
            "scoring_latency_timeseries",
            "scoring_health",
            "scoring_evaluation",
            "scoring_curves",
            "scoring_threshold_preview",
            "scoring_retrain",
            "scoring_session_events",
            "scoring_enforce_threshold_get",
            "scoring_enforce_threshold_put",
            "scoring_l2_enforce_get",
            "scoring_l2_enforce_put",
            "scoring_exclude_regex_get",
            "scoring_exclude_regex_put",
            "scoring_exclude_regex_validate",
            "scoring_enforce_status_code_get",
            "scoring_enforce_status_code_put",
            "scoring_matrix_versions_list",
            "scoring_matrix_versions_restore",
            "scoring_rotate_key",
            "scoring_audit_list",
            "scoring_threshold_get",
            "scoring_threshold_put",
            "scoring_evaluation_per_reason",
            "scoring_dashboard",
        )
    )
}
router.routes.sort(key=lambda r: _SCORING_ROUTE_ORDER.get(getattr(getattr(r, "endpoint", None), "__name__", ""), 999))

for _reg_name, _reg_cache in (
    ("session_scoring._analytics_cache", _analytics_cache),
    ("session_scoring._inflight", _inflight),
    ("session_scoring._reconstruct_labeled_sessions_cache", _reconstruct_labeled_sessions_cache),
    ("session_scoring._table_columns_cache", _table_columns_cache),
    ("session_scoring._scoring_pragma_cache", _scoring_pragma_cache),
    ("session_scoring._scoring_svc_version_cache", _scoring_svc_version_cache),
    ("session_scoring_admin._enforce_threshold_cache", _enforce_threshold_cache),
    ("session_scoring_admin._l2_enforce_cache", _l2_enforce_cache),
):
    _CacheRegistry.register(_reg_name, _reg_cache)

_SUBMODULES = (scoring_analytics, scoring_admin, scoring_vcl)


class _SessionScoringShimModule(_types.ModuleType):
    """Forwards attribute writes/deletes on backend.routers.session_scoring to all 3 submodules."""

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        for _mod in _SUBMODULES:
            _types.ModuleType.__setattr__(_mod, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        for _mod in _SUBMODULES:
            if hasattr(_mod, name):
                try:
                    _types.ModuleType.__delattr__(_mod, name)
                except AttributeError:
                    pass


_self_mod = sys.modules.get(__name__)
if _self_mod is not None and not isinstance(_self_mod, _SessionScoringShimModule):
    _self_mod.__class__ = _SessionScoringShimModule
