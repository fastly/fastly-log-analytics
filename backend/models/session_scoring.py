"""Body + response models for the session-scoring router.

Pydantic body shapes for the PUT/POST endpoints in
``backend/routers/session_scoring_admin.py`` and response shapes for the
analyst-safe ``/scoring/*`` GET reads in
``backend/routers/session_scoring.py``. Range / business validation stays
in the handlers — these models pin field TYPES so the OpenAPI surface
carries a real schema instead of an opaque ``dict``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class ScoringThresholdBody(BaseModel):
    """Body for ``PUT /scoring/threshold`` and ``PUT /scoring/enforce-threshold``.

    ``threshold`` is the 0-100 risk score. ``None`` clears the operator's
    chosen value (preview-only) or disables live enforcement, depending on
    the endpoint."""

    threshold: int | None = None


class ScoringL2EnforceBody(BaseModel):
    """Body for ``PUT /scoring/l2-enforce``.

    ``enabled`` is the operator's explicit opt-in: ``True`` makes edge Layer-2
    join the *enforced* combined score (fading in over a few days from the moment
    of consent); ``False`` keeps L2 observe-only. Default ``False`` so an empty
    body is a no-op-toward-off rather than an accidental enable."""

    enabled: bool = False


class ScoringExcludeRegexBody(BaseModel):
    """Body for ``PUT /scoring/exclude-regex`` and ``POST /scoring/exclude-regex/validate``.

    Empty string resets to the bundled default regex."""

    regex: str = ""


class ScoringEnforceStatusCodeBody(BaseModel):
    """Body for ``PUT /scoring/enforce-status-code``.

    HTTP code returned by the enforce snippet when the scorer flags a
    request. ``None`` resets to the default 429."""

    status_code: int | None = None


class ScoringTokenBody(BaseModel):
    """Body for ``POST /scoring/enable`` and ``POST /scoring/disable``.

    ``token`` falls back to the per-service ``fastly_api_key`` when
    empty. Optional on the wire — callers can omit the body entirely
    when the server has a stored token."""

    token: str = ""


class ScoringLabelCreate(BaseModel):
    """Body for ``POST /scoring/labels``.

    ``sid`` and ``label`` are validated downstream by ``save_label``
    (raises ValueError → ``invalid_label`` 400) so we keep this model
    permissive on field content."""

    sid: str = ""
    label: str = ""
    notes: str = ""
    flagged_by: str = "admin"
    sample_ip: str = ""
    sample_ua: str = ""
    sample_url: str = ""


class ScoringLabelUpdate(BaseModel):
    """Body for ``PATCH /scoring/labels/{label_id}``.

    Both fields are optional. ``label`` content is validated
    downstream by ``update_label``."""

    label: str | None = None
    notes: str | None = None


# ── Response models for the analyst-safe /scoring/* GET reads ────────────────
#
# These endpoints build plain dicts and route them through the in-process
# ``_cached`` wrapper in backend/routers/session_scoring.py, which bakes in the
# UNDERSCORE-form telemetry envelope (``_is_cached``, ``_section_timings``, and
# ``_debug_queries`` / ``_debug_calls`` when DEBUG_RESPONSES is on). That's why
# every model below sets ``extra="allow"``: it lets those underscore keys — and
# any future producer key — pass through the ``response_model`` verbatim instead
# of being silently stripped. ``_section_timings`` (analyst-visible) and the
# admin debug panel both depend on the envelope surviving, so stripping it would
# be a wire-format regression. Declaring the data fields gives the generated TS
# client real types so the SessionScoring components can drop their
# ``as unknown as`` casts.
#
# Two correctness rules from the audit guide are encoded here:
#   * column-dependent values (the scorer-latency percentiles, present only on
#     services re-provisioned after 2026-06-17) are ``| None`` so serialization
#     never 500s on a service that hasn't grown the columns yet;
#   * ``ua`` / ``url`` stay on the top-flagged row (analyst triage invariant).
#
# Timestamp/``hour`` columns are typed ``str``: the handlers ``.isoformat()``
# them before returning so the wire bytes stay byte-identical to the pre-typing
# ``jsonable_encoder`` output. Without that, attaching any response_model would
# re-serialize tz-aware datetimes through Pydantic and flip the UTC suffix from
# ``+00:00`` to ``Z`` on UTC-configured hosts — a silent wire change.


class _ScoringRead(BaseModel):
    """Base for the scoring read responses — passes the telemetry envelope
    (and any undeclared key) through instead of stripping it."""

    model_config = ConfigDict(extra="allow")


class ScoringTopFlaggedRow(_ScoringRead):
    timestamp: str | None = None
    edge_sid: str | None = None
    edge_score: int | None = None
    edge_score_l1: int | None = None
    edge_score_l2: int | None = None
    edge_cookie_compliance: str | None = None
    edge_score_reason: str | None = None
    ip: str | None = None
    ua: str | None = None
    url: str | None = None
    status: int | None = None
    country: str | None = None


class ScoringTopFlaggedResponse(_ScoringRead):
    rows: list[ScoringTopFlaggedRow] = []
    since_hours: int | None = None


class ScoringDistributionRow(_ScoringRead):
    hour: str | None = None
    bucket: str | None = None
    count: int | None = None


class ScoringScoreDistributionResponse(_ScoringRead):
    rows: list[ScoringDistributionRow] = []
    since_hours: int | None = None


class ScoringComplianceRow(_ScoringRead):
    hour: str | None = None
    compliance: str | None = None
    count: int | None = None


class ScoringComplianceBreakdownResponse(_ScoringRead):
    rows: list[ScoringComplianceRow] = []
    since_hours: int | None = None


class ScoringLatencyRow(_ScoringRead):
    hour: str | None = None
    scored_count: int | None = None
    fail_open_count: int | None = None
    total_count: int | None = None
    # Present only when the edge_score_rtt_us / edge_score_exec_us columns exist.
    rtt_p50_us: int | None = None
    rtt_p95_us: int | None = None
    rtt_p99_us: int | None = None
    exec_p50_us: int | None = None
    exec_p95_us: int | None = None


class ScoringLatencyTimeseriesResponse(_ScoringRead):
    rows: list[ScoringLatencyRow] = []
    since_hours: int | None = None
    has_latency: bool = False
    granularity: str | None = None


class ScoringReasonCount(_ScoringRead):
    reason: str | None = None
    count: int | None = None


class ScoringLatencySnapshot(_ScoringRead):
    available: bool = False
    rtt_p50_us: int | None = None
    rtt_p95_us: int | None = None
    rtt_p99_us: int | None = None
    rtt_max_us: int | None = None
    exec_p50_us: int | None = None
    exec_p95_us: int | None = None


class ScoringMatrixStaleness(_ScoringRead):
    l2_evaluated: int | None = None
    l2_high_count: int | None = None
    l2_high_pct: float | None = None
    is_stale: bool | None = None
    threshold_pct: float | None = None


class ScoringHealthResponse(_ScoringRead):
    since_hours: int | None = None
    total_edge_rows: int | None = None
    scored_rows: int | None = None
    fire_rate_pct: float | None = None
    distinct_sids: int | None = None
    avg_score: float | None = None
    p50_score: float | None = None
    p95_score: float | None = None
    max_score: int | None = None
    scorer_errors: int | None = None
    # SRE-15: fail-opens as a fraction of routed edge traffic. The raw
    # ``scorer_errors`` count scales with request volume (per
    # scorer-instance-per-request-coldstart), so a count tone misfires under
    # load; the rate is the traffic-normalized spike signal the UI tones on.
    fail_open_rate_pct: float | None = None
    top_reasons: list[ScoringReasonCount] = []
    fail_open_breakdown: list[ScoringReasonCount] = []
    latency: ScoringLatencySnapshot | None = None
    matrix_staleness: ScoringMatrixStaleness | None = None
    # Admin-only opt-in/readiness block (None for analysts). Left as a free
    # dict — the consuming card is admin-only and intentionally untyped here.
    l2_enforce: dict | None = None


class ScoringEvaluationResponse(_ScoringRead):
    has_min_samples: bool | None = None
    min_per_class: int | None = None
    n_good: int | None = None
    n_bad: int | None = None
    n_neutral: int | None = None
    matrix_version: str | None = None
    # Only present on the below-min-samples / missing-matrix / scored branches.
    error: str | None = None
    n_reconstructed: int | None = None
    n_labels_total: int | None = None
    auc: float | None = None
    passed: bool | None = None
    threshold: float | None = None
    default_min_auc: float | None = None


class ScoringCurvePoint(_ScoringRead):
    threshold: int | None = None
    # ROC points carry fpr/tpr; PR points carry precision/recall.
    fpr: float | None = None
    tpr: float | None = None
    precision: float | None = None
    recall: float | None = None


class ScoringCurvesResponse(_ScoringRead):
    has_min_samples: bool | None = None
    min_per_class: int | None = None
    n_good: int | None = None
    n_bad: int | None = None
    note: str | None = None
    n_labels_total: int | None = None
    auc: float | None = None
    average_precision: float | None = None
    roc: list[ScoringCurvePoint] | None = None
    pr: list[ScoringCurvePoint] | None = None


class ScoringThresholdBucket(_ScoringRead):
    # ``flagged`` carries total+good+bad+unlabeled; ``passed`` omits total.
    total: int | None = None
    good: int | None = None
    bad: int | None = None
    unlabeled: int | None = None


class ScoringThresholdPreviewResponse(_ScoringRead):
    threshold: int | None = None
    since_hours: int | None = None
    total_scored_sessions: int | None = None
    flagged: ScoringThresholdBucket | None = None
    passed: ScoringThresholdBucket | None = None
    precision: float | None = None
    recall: float | None = None


class ScoringAnalyticsResponse(_ScoringRead):
    """Composite of the analyst-safe sub-reads (plus the two admin-only
    evaluation blocks, present only on the admin path)."""

    top_flagged: ScoringTopFlaggedResponse | None = None
    score_distribution: ScoringScoreDistributionResponse | None = None
    compliance_breakdown: ScoringComplianceBreakdownResponse | None = None
    latency_timeseries: ScoringLatencyTimeseriesResponse | None = None
    health: ScoringHealthResponse | None = None
    # Admin-only (omitted entirely on the analyst path). Left as free dicts —
    # the standalone /scoring/evaluation is typed via ScoringEvaluationResponse,
    # but evaluation/per-reason is admin-only and intentionally untyped.
    evaluation: dict | None = None
    evaluation_per_reason: dict | None = None


class ScoringLabelRow(_ScoringRead):
    # Fields ordered to match labels._row_to_dict so the serialized key order
    # is byte-identical. All Optional: the analyst path projects out the PII /
    # attribution fields (notes/flagged_by/sample_*), and response_model_exclude_unset
    # then drops them from the wire so the analyst shape is unchanged.
    # created_at/updated_at are plain SQLite text strings (datetime('now')), so
    # no .isoformat() coercion is needed — they pass through verbatim. id is a
    # UUID string (not an autoincrement int).
    id: str | None = None
    service_id: str | None = None
    sid: str | None = None
    label: str | None = None
    notes: str | None = None
    flagged_by: str | None = None
    sample_ip: str | None = None
    sample_ua: str | None = None
    sample_url: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ScoringLabelsListResponse(_ScoringRead):
    labels: list[ScoringLabelRow] = []
    counts: dict[str, int] = {}


# ── Response models for the admin endpoints (session_scoring_admin.py) ───────
#
# Same wire-safety contract as the analyst-safe reads above: every model
# extends ``_ScoringRead`` (extra="allow") so the ``_cached`` telemetry
# envelope and any future producer key pass through verbatim, every route
# applies ``response_model_exclude_unset=True`` so per-branch shapes (e.g.
# the validate endpoint's success-vs-failure keys) don't sprout ``null``s,
# and timestamp fields are ``str`` because the producers ``.isoformat()``
# them (state_sync matrix history, rotate_aes_key, SQLite datetime('now')).
# Field lists are derived from the PRODUCERS, not the frontend consumers.


class ScoringRetrainRejected(_ScoringRead):
    too_few_events: int | None = None
    too_fast: int | None = None
    kept: int | None = None
    routes_seen: int | None = None


class ScoringAucAgainstLabels(_ScoringRead):
    auc: float | None = None
    passed: bool | None = None
    threshold: float | None = None
    n_good: int | None = None
    n_bad: int | None = None


class ScoringRetrainResponse(_ScoringRead):
    ok: bool | None = None
    matrix_version: str | None = None
    since_days: int | None = None
    sessions_trained_on: int | None = None
    transitions: int | None = None
    vocab_size: int | None = None
    rejected: ScoringRetrainRejected | None = None
    # Always present; ``null`` until both label classes reach min-per-class.
    auc_against_labels: ScoringAucAgainstLabels | None = None
    default_min_auc: float | None = None
    local_matrix_saved: bool | None = None
    fos_matrix_published: bool | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringSessionEvent(_ScoringRead):
    # Shape from repositories/session_scoring.fetch_session_events. ``ip``
    # stays declared — the analyst masking middleware's key-name pass runs
    # after serialization; ``ua``/``url`` stay visible (analyst triage
    # invariant).
    ts: str | None = None
    url: str | None = None
    status: int | None = None
    ip: str | None = None
    ua: str | None = None
    edge_score: int | None = None
    edge_cookie_compliance: str | None = None
    edge_score_reason: str | None = None


class ScoringSessionEventsResponse(_ScoringRead):
    sid: str | None = None
    since_days: int | None = None
    event_count: int | None = None
    events: list[ScoringSessionEvent] = []


class ScoringEnforceThresholdResponse(_ScoringRead):
    threshold: int | None = None
    enforced: bool | None = None
    key: str | None = None


class ScoringEnforceThresholdPutResponse(_ScoringRead):
    ok: bool | None = None
    threshold: int | None = None
    enforced: bool | None = None
    message: str | None = None


class ScoringL2EnforceState(_ScoringRead):
    """Shape from _build_l2_enforce_block / _l2_enforce_unavailable —
    also nested as ``l2_enforce`` in ScoringHealthResponse (left ``dict``
    there for wire-compat; this standalone GET carries the real schema)."""

    available: bool | None = None
    enabled: bool | None = None
    l2_enabled_at: int | None = None
    days_since_optin: float | None = None
    ramp_progress: float | None = None
    fully_ramped: bool | None = None
    warmup_days_remaining: float | None = None
    scoring_enabled_at: int | None = None
    deployment_age_days: float | None = None
    ready: bool | None = None
    ramp_days: float | None = None
    readiness_days: float | None = None


class ScoringL2EnforcePutResponse(_ScoringRead):
    ok: bool | None = None
    enabled: bool | None = None
    l2_enabled_at: int | None = None
    message: str | None = None


class ScoringExcludeRegexState(_ScoringRead):
    current: str | None = None
    is_default: bool | None = None
    default: str | None = None
    effective: str | None = None


class ScoringExcludeRegexPutResponse(_ScoringRead):
    ok: bool | None = None
    # ``**result`` spread from update_recv_exclusion_regex.
    effective_regex: str | None = None
    is_default: bool | None = None
    logging_service_active_version: int | None = None
    lint_warnings: list[str] | None = None
    message: str | None = None


class ScoringExcludeRegexValidateResponse(_ScoringRead):
    # Success branch: {ok, lint_warnings}; failure: {ok, error, reason}.
    # exclude_unset keeps each branch's key set unchanged.
    ok: bool | None = None
    lint_warnings: list[str] | None = None
    error: str | None = None
    reason: str | None = None


class ScoringEnforceStatusCodeState(_ScoringRead):
    current: int | None = None
    default: int | None = None
    effective: int | None = None
    min: int | None = None
    max: int | None = None
    is_default: bool | None = None


class ScoringEnforceStatusCodePutResponse(_ScoringRead):
    ok: bool | None = None
    # ``**result`` spread from update_enforce_status_code.
    effective_status_code: int | None = None
    is_default: bool | None = None
    logging_service_active_version: int | None = None
    message: str | None = None


class ScoringMatrixVersion(_ScoringRead):
    version: str | None = None
    key: str | None = None
    size_bytes: int | None = None
    last_modified: str | None = None


class ScoringMatrixVersionsResponse(_ScoringRead):
    versions: list[ScoringMatrixVersion] = []
    current_version: str | None = None


class ScoringMatrixRestoreResponse(_ScoringRead):
    ok: bool | None = None
    restored_version: str | None = None
    restored_at: str | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringRotateKeyResponse(_ScoringRead):
    ok: bool | None = None
    rotated_at: str | None = None
    previous_key_grace: bool | None = None
    message: str | None = None


class ScoringAuditRow(_ScoringRead):
    # SELECT id, timestamp, action, actor, details FROM scoring_audit.
    # ``details`` is JSON-parsed to a dict, but stays the raw string when
    # parsing fails — hence Any, not dict.
    id: int | None = None
    timestamp: str | None = None
    action: str | None = None
    actor: str | None = None
    details: Any = None


class ScoringAuditListResponse(_ScoringRead):
    audit: list[ScoringAuditRow] = []
    limit: int | None = None


class ScoringThresholdState(_ScoringRead):
    """GET and PUT /scoring/threshold both return this exact shape."""

    threshold: int | None = None
    set_at: str | None = None
    enforced: bool | None = None


class ScoringPerReasonBucket(_ScoringRead):
    reason: str | None = None
    n_good: int | None = None
    n_bad: int | None = None
    min_per_class: int | None = None
    has_min_samples: bool | None = None
    # Present only when has_min_samples (evaluate_per_reason adds them
    # conditionally); exclude_unset keeps the gated branch byte-stable.
    auc: float | None = None
    passed: bool | None = None
    threshold: float | None = None


class ScoringPerReasonResponse(_ScoringRead):
    buckets: list[ScoringPerReasonBucket] = []
    min_per_class: int | None = None
    known_reasons: list[str] | None = None
    has_min_samples_overall: bool | None = None
    n_good: int | None = None
    n_bad: int | None = None


class ScoringDashboardResponse(_ScoringRead):
    """Composite of the per-endpoint producers — each sub-block is the
    byte-identical dict its standalone endpoint returns, so each nests the
    same model that endpoint declares. ``status`` stays a free dict: it's
    the secret-filtered cfg.scoring block, whose keys vary by provisioning
    history."""

    since_hours: int | None = None
    threshold: int | None = None
    status: dict | None = None
    evaluation: ScoringEvaluationResponse | None = None
    evaluation_per_reason: ScoringPerReasonResponse | None = None
    health: ScoringHealthResponse | None = None
    top_flagged: ScoringTopFlaggedResponse | None = None
    score_distribution: ScoringScoreDistributionResponse | None = None
    compliance_breakdown: ScoringComplianceBreakdownResponse | None = None
    curves: ScoringCurvesResponse | None = None
    threshold_preview: ScoringThresholdPreviewResponse | None = None
    config_threshold: ScoringThresholdState | None = None
    exclude_regex: ScoringExcludeRegexState | None = None
    enforce_status_code: ScoringEnforceStatusCodeState | None = None
