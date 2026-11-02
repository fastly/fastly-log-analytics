from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from backend.models.common import (
    BaseResponse,
    Limit500,
    PaginationMixin,
    PassthroughModel,
    RangeKeyedRequest,
    ScalarCell,
    WireSafeModel,
)


class ScoringThresholdBody(BaseModel):
    threshold: int | None = None


class ScoringL2EnforceBody(BaseModel):
    enabled: bool = False


class ScoringExcludeRegexBody(BaseModel):
    regex: str = ""


class ScoringEnforceStatusCodeBody(BaseModel):
    status_code: int | None = None


class ScoringTokenBody(BaseModel):
    token: str = ""


class ScoringLabelCreate(BaseModel):
    sid: str = ""
    label: str = ""
    notes: str = ""
    flagged_by: str = "admin"
    sample_ip: str = ""
    sample_ua: str = ""
    sample_url: str = ""


class ScoringLabelUpdate(BaseModel):
    label: str | None = None
    notes: str | None = None


class _ScoringEventFieldsBase(PassthroughModel):
    edge_cookie_compliance: str | None = None
    edge_score_reason: str | None = None
    ip: str | None = None
    ua: str | None = None
    url: str | None = None
    status: int | None = None


class ScoringTopFlaggedRow(PassthroughModel):
    timestamp: str
    edge_sid: str | None
    edge_score: int
    edge_score_l1: int | None = None
    edge_score_l2: int | None = None
    edge_score_reason: str | None = None
    edge_cookie_compliance: str | None = None
    ip: str | None = None
    country: str | None = None
    ua: str | None = None
    url: str | None = None
    status: int | None = None


class _ScoringSinceHoursBase(PassthroughModel):
    since_hours: int | None = None


class ScoringTopFlaggedResponse(_ScoringSinceHoursBase):
    rows: list[ScoringTopFlaggedRow] = []


class ScoringDistributionRow(PassthroughModel):
    hour: str
    bucket: str
    count: int


class ScoringScoreDistributionResponse(_ScoringSinceHoursBase):
    rows: list[ScoringDistributionRow] = []


class ScoringComplianceRow(PassthroughModel):
    hour: str
    compliance: str
    count: int


class ScoringComplianceBreakdownResponse(_ScoringSinceHoursBase):
    rows: list[ScoringComplianceRow] = []


class _ScoringLatencyMetricsBase(PassthroughModel):
    rtt_p50_us: int | None = None
    rtt_p95_us: int | None = None
    rtt_p99_us: int | None = None
    exec_p50_us: int | None = None
    exec_p95_us: int | None = None


class ScoringLatencyRow(_ScoringLatencyMetricsBase):
    hour: str | None = None
    scored_count: int | None = None
    fail_open_count: int | None = None
    total_count: int | None = None


class ScoringLatencyTimeseriesResponse(_ScoringSinceHoursBase):
    rows: list[ScoringLatencyRow] = []
    has_latency: bool = False
    granularity: str | None = None


class ScoringReasonCount(PassthroughModel):
    reason: str | None = None
    count: int | None = None


class ScoringLatencySnapshot(_ScoringLatencyMetricsBase):
    available: bool = False
    rtt_max_us: int | None = None


class ScoringMatrixStaleness(PassthroughModel):
    l2_evaluated: int | None = None
    l2_high_count: int | None = None
    l2_high_pct: float | None = None
    is_stale: bool | None = None
    threshold_pct: float | None = None


class ScoringL2EnforceState(PassthroughModel):
    available: bool
    enabled: bool
    l2_enabled_at: int | None
    days_since_optin: float | None
    ramp_progress: float
    fully_ramped: bool
    warmup_days_remaining: float | None
    scoring_enabled_at: int | None
    deployment_age_days: float | None
    ready: bool
    ramp_days: float
    readiness_days: float


class _ScoringGoodBadBase(PassthroughModel):
    n_good: int | None = None
    n_bad: int | None = None


class _ScoringClassSamplesBase(_ScoringGoodBadBase):
    min_per_class: int | None = None
    has_min_samples: bool | None = None


class _ScoringAucBase(_ScoringGoodBadBase):
    auc: float | None = None
    passed: bool | None = None
    threshold: float | None = None


class _ScoringEvalMetricsBase(_ScoringClassSamplesBase, _ScoringAucBase):
    pass


class ScoringPerReasonBucket(_ScoringEvalMetricsBase):
    reason: str | None = None


class ScoringPerReasonResponse(_ScoringGoodBadBase):
    buckets: list[ScoringPerReasonBucket] = []
    min_per_class: int | None = None
    known_reasons: list[str] | None = None
    has_min_samples_overall: bool | None = None


class ScoringHealthResponse(_ScoringSinceHoursBase):
    total_edge_rows: int | None = None
    scored_rows: int | None = None
    fire_rate_pct: float | None = None
    distinct_sids: int | None = None
    avg_score: float | None = None
    p50_score: float | None = None
    p95_score: float | None = None
    max_score: int | None = None
    scorer_errors: int | None = None
    fail_open_rate_pct: float | None = None
    top_reasons: list[ScoringReasonCount] = []
    fail_open_breakdown: list[ScoringReasonCount] = []
    latency: ScoringLatencySnapshot | None = None
    matrix_staleness: ScoringMatrixStaleness | None = None
    l2_enforce: ScoringL2EnforceState | None = None


class ScoringEvaluationResponse(_ScoringEvalMetricsBase):
    n_neutral: int | None = None
    matrix_version: str | None = None
    error: str | None = None
    n_reconstructed: int | None = None
    n_labels_total: int | None = None
    default_min_auc: float | None = None


class ScoringCurvePoint(PassthroughModel):
    threshold: int | None = None
    fpr: float | None = None
    tpr: float | None = None
    precision: float | None = None
    recall: float | None = None


class ScoringCurvesResponse(_ScoringClassSamplesBase):
    note: str | None = None
    n_labels_total: int | None = None
    auc: float | None = None
    average_precision: float | None = None
    roc: list[ScoringCurvePoint] | None = None
    pr: list[ScoringCurvePoint] | None = None


class ScoringThresholdBucket(PassthroughModel):
    total: int | None = None
    good: int | None = None
    bad: int | None = None
    unlabeled: int | None = None


class ScoringThresholdPreviewResponse(_ScoringSinceHoursBase):
    threshold: int | None = None
    total_scored_sessions: int | None = None
    flagged: ScoringThresholdBucket | None = None
    passed: ScoringThresholdBucket | None = None
    precision: float | None = None
    recall: float | None = None


class _ScoringCorePanelsBase(PassthroughModel):
    top_flagged: ScoringTopFlaggedResponse | None = None
    score_distribution: ScoringScoreDistributionResponse | None = None
    compliance_breakdown: ScoringComplianceBreakdownResponse | None = None
    health: ScoringHealthResponse | None = None
    evaluation: ScoringEvaluationResponse | None = None
    evaluation_per_reason: ScoringPerReasonResponse | None = None


class ScoringAnalyticsResponse(_ScoringCorePanelsBase):
    latency_timeseries: ScoringLatencyTimeseriesResponse | None = None


class ScoringLabelRow(WireSafeModel):
    id: str
    service_id: str | None = None
    sid: str
    label: Literal["good", "bad", "neutral"]
    notes: str | None = None
    flagged_by: str | None = None
    sample_ip: str | None = None
    sample_ua: str | None = None
    sample_url: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ScoringLabelCounts(WireSafeModel):
    good: int
    bad: int
    neutral: int


class ScoringLabelsListResponse(WireSafeModel):
    labels: list[ScoringLabelRow]
    counts: ScoringLabelCounts


class ScoringRetrainRejected(PassthroughModel):
    too_few_events: int | None = None
    too_fast: int | None = None
    kept: int | None = None
    routes_seen: int | None = None


class ScoringAucAgainstLabels(_ScoringAucBase):
    pass


class ScoringRetrainResponse(PassthroughModel):
    ok: bool | None = None
    matrix_version: str | None = None
    since_days: int | None = None
    sessions_trained_on: int | None = None
    transitions: int | None = None
    vocab_size: int | None = None
    rejected: ScoringRetrainRejected | None = None
    auc_against_labels: ScoringAucAgainstLabels | None = None
    default_min_auc: float | None = None
    local_matrix_saved: bool | None = None
    fos_matrix_published: bool | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringSessionEvent(_ScoringEventFieldsBase):
    ts: str | None = None
    edge_score: int | None = None


class ScoringSessionEventsResponse(PassthroughModel):
    sid: str | None = None
    since_days: int | None = None
    event_count: int | None = None
    events: list[ScoringSessionEvent] = []


class ScoringEnforceThresholdResponse(PassthroughModel):
    threshold: int | None
    enforced: bool
    key: str


class _ScoringOkMessageBase(PassthroughModel):
    ok: bool | None = None
    message: str | None = None


class ScoringEnforceThresholdPutResponse(_ScoringOkMessageBase):
    threshold: int | None = None
    enforced: bool | None = None


class ScoringL2EnforcePutResponse(_ScoringOkMessageBase):
    enabled: bool | None = None
    l2_enabled_at: int | None = None


class ScoringExcludeRegexState(PassthroughModel):
    current: str
    is_default: bool
    default: str
    effective: str


class _ScoringVclPutResponseBase(_ScoringOkMessageBase):
    is_default: bool | None = None
    logging_service_active_version: int | None = None


class ScoringExcludeRegexPutResponse(_ScoringVclPutResponseBase):
    effective_regex: str | None = None
    lint_warnings: list[str] | None = None


class ScoringExcludeRegexValidateResponse(PassthroughModel):
    ok: bool | None = None
    lint_warnings: list[str] | None = None
    error: str | None = None
    reason: str | None = None


class ScoringEnforceStatusCodeState(PassthroughModel):
    current: int | None
    default: int
    effective: int
    min: int
    max: int
    is_default: bool


class ScoringEnforceStatusCodePutResponse(_ScoringVclPutResponseBase):
    effective_status_code: int | None = None


class ScoringMatrixVersion(PassthroughModel):
    version: str | None = None
    key: str | None = None
    size_bytes: int | None = None
    last_modified: str | None = None


class ScoringMatrixVersionsResponse(PassthroughModel):
    versions: list[ScoringMatrixVersion] = []
    current_version: str | None = None


class ScoringMatrixRestoreResponse(PassthroughModel):
    ok: bool | None = None
    restored_version: str | None = None
    restored_at: str | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringRotateKeyResponse(_ScoringOkMessageBase):
    rotated_at: str | None = None
    previous_key_grace: bool | None = None


class ScoringAuditRow(PassthroughModel):
    id: int | None = None
    timestamp: str | None = None
    action: str | None = None
    actor: str | None = None
    details: Any = None


class ScoringAuditListResponse(PassthroughModel):
    audit: list[ScoringAuditRow] = []
    limit: int | None = None


class ScoringThresholdState(PassthroughModel):
    threshold: int | None
    set_at: str | None
    enforced: bool


class ScoringStatusResponse(PassthroughModel):
    enabled: bool | None = None
    scoring_service_id: str | None = None
    scoring_service_name: str | None = None
    scoring_domain: str | None = None
    matrix_version: str | None = None
    enabled_at: str | None = None
    scoring_active_version: int | None = None
    scoring_activated_at: str | None = None
    scorer_drift: bool | None = None
    drift_detail: str | None = None
    drift_known: bool | None = None
    preflight_pragmas_ready: bool | None = None


class _ScoringConfigBase(PassthroughModel):
    status: ScoringStatusResponse | None = None
    exclude_regex: ScoringExcludeRegexState | None = None
    enforce_status_code: ScoringEnforceStatusCodeState | None = None


class ScoringDashboardResponse(_ScoringSinceHoursBase, _ScoringCorePanelsBase, _ScoringConfigBase):
    threshold: int | None = None
    curves: ScoringCurvesResponse | None = None
    threshold_preview: ScoringThresholdPreviewResponse | None = None
    config_threshold: ScoringThresholdState | None = None


class ScoringConfigCompositeResponse(_ScoringConfigBase):
    threshold: ScoringThresholdState | None = None


class ScoringLabelDeleteResponse(PassthroughModel):
    status: str | None = None
    id: str | None = None


class SessionsRequest(RangeKeyedRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_by: str = "session_start"
    flagged_only: bool = False
    streaming_only: bool = False
    min_reqs_flag: int | None = None
    min_4xx_pct_flag: float | None = None


class Session(BaseModel):
    ip: str
    ua: str | None = None
    ja4: str | None = None
    country: str | None = None
    asn: int | None = None
    session_start: str
    session_end: str
    req_count: int
    edge_count: int | None = None
    shield_count: int | None = None
    unique_urls: int | None = None
    reqs_4xx: int | None = None
    reqs_5xx: int | None = None
    total_bytes: int | None = None
    median_rtt_ms: float | None = None
    edge_sid: str | None = None
    is_streaming: bool | None = None
    flagged: bool
    session_token: str | None = None


class SessionsResponse(BaseResponse):
    sessions: list[Session]
    asn_names: dict[int, str] = {}
    total: int
    page: int
    limit: int
    has_rtt: bool
    has_ja4: bool
    has_edge: bool
    has_edge_sid: bool = False
    has_cmcd: bool = False
    min_reqs_flag: int | None = None
    min_4xx_pct_flag: float | None = None


class SessionDetailRequest(BaseModel):
    session_token: str | None = None
    ip: str | None = None
    ja4: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class SessionDetailResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, ScalarCell]]
