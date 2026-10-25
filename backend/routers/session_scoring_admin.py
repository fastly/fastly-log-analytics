"""Session-scoring admin + training endpoints (v2.0 file-size carve).

Carved out of ``backend/routers/session_scoring.py`` (the 2442-line monolith)
so each half stays under the 1500-line tech-debt threshold. The router
instance + shared helpers continue to live in ``session_scoring.py``; this
module just registers its routes on the same router by importing it.

Endpoints here (all admin-write or training-action shaped):
- POST /api/services/{id}/scoring/retrain
- GET  /api/services/{id}/scoring/sessions/{sid}/events
- GET/PUT /api/services/{id}/scoring/enforce-threshold
- GET/PUT/POST /api/services/{id}/scoring/exclude-regex(/validate)
- GET/PUT /api/services/{id}/scoring/enforce-status-code
- GET /api/services/{id}/scoring/matrix-versions
- POST /api/services/{id}/scoring/matrix-versions/{version}/restore
- POST /api/services/{id}/scoring/rotate-key
- GET /api/services/{id}/scoring/audit
- GET/PUT /api/services/{id}/scoring/threshold
- GET /api/services/{id}/scoring/evaluation/per-reason
- GET /api/services/{id}/scoring/dashboard  (composite)

Cross-module symbol contract: ``session_scoring.py`` registers this
module's routes by importing it for its side effects (the bottom-of-
file ``from backend.routers import session_scoring_admin``). Reorder
or skip that import and the admin routes vanish — pin via the
``test_session_scoring_admin_routes_register`` test.
"""

from __future__ import annotations

import os

from fastapi import Depends, Header, HTTPException, Path, Query, Request

from backend.deps import ServiceId, require_admin
from backend.models.session_scoring import (
    ScoringAuditListResponse,
    ScoringDashboardResponse,
    ScoringEnforceStatusCodeBody,
    ScoringEnforceStatusCodePutResponse,
    ScoringEnforceStatusCodeState,
    ScoringEnforceThresholdPutResponse,
    ScoringEnforceThresholdResponse,
    ScoringExcludeRegexBody,
    ScoringExcludeRegexPutResponse,
    ScoringExcludeRegexState,
    ScoringExcludeRegexValidateResponse,
    ScoringL2EnforceBody,
    ScoringL2EnforcePutResponse,
    ScoringL2EnforceState,
    ScoringMatrixRestoreResponse,
    ScoringMatrixVersionsResponse,
    ScoringPerReasonResponse,
    ScoringRetrainResponse,
    ScoringRotateKeyResponse,
    ScoringSessionEventsResponse,
    ScoringThresholdBody,
    ScoringThresholdState,
)
from backend.provision.session_scoring_setup import (
    SCORING_ENABLED_AT_KEY as _SCORING_ENABLED_AT_KEY,
)

_L2_ENABLED_AT_KEY = "l2_enabled_at"
_L2_ENFORCE_ENABLED_KEY = "l2_enforce_enabled"

# Pull the shared router + helpers from the main session_scoring module.
# Importing the module (not the names) avoids a circular-import trap:
# session_scoring's own bottom-of-file import of this module runs after
# its top-level definitions, so by that point router/helpers are bound.
#
# The ``# type: ignore[has-type]`` markers below sidestep a mypy
# limitation: under the circular import (this file ↔ session_scoring),
# mypy can't resolve the right-hand side type when it analyses this
# module first. The ignores are scoped per-line so any genuine error
# (typo, removed export) still surfaces as a separate diagnostic.
from backend.routers import session_scoring as _ss
from backend.utils.router_utils import load_service_config, query_errors, raise_internal, resolve_fastly_token

router = _ss.router  # type: ignore[has-type]
logger = _ss.logger  # type: ignore[has-type]
_bust_analytics_cache = _ss._bust_analytics_cache  # type: ignore[has-type]
_cached = _ss._cached  # type: ignore[has-type]
_fetch_session_events = _ss._scoring_repo.fetch_session_events  # type: ignore[has-type]
_scoring_time_window = _ss._scoring_time_window  # type: ignore[has-type]
_reconstruct_labeled_sessions = _ss._reconstruct_labeled_sessions  # type: ignore[has-type]
_MIN_LABELS_PER_CLASS = _ss._MIN_LABELS_PER_CLASS  # type: ignore[has-type]
_label_counts = _ss._label_counts  # type: ignore[has-type]
_table_columns = _ss._table_columns  # type: ignore[has-type]
_tenant_matrix_path = _ss._tenant_matrix_path  # type: ignore[has-type]

# Composite-endpoint dependencies — the /scoring/dashboard composite at the
# bottom of this file calls back into the analytics endpoints that live in
# the main module. Pull them by name so the composite can dispatch without
# re-routing through HTTP.
scoring_evaluation = _ss.scoring_evaluation  # type: ignore[has-type]
scoring_health = _ss.scoring_health  # type: ignore[has-type]
scoring_top_flagged = _ss.scoring_top_flagged  # type: ignore[has-type]
scoring_score_distribution = _ss.scoring_score_distribution  # type: ignore[has-type]
scoring_compliance_breakdown = _ss.scoring_compliance_breakdown  # type: ignore[has-type]
scoring_curves = _ss.scoring_curves  # type: ignore[has-type]
scoring_threshold_preview = _ss.scoring_threshold_preview  # type: ignore[has-type]
scoring_status = _ss.scoring_status  # type: ignore[has-type]

# ── module-private constants ──────────────────────────────────────────────────

_ENFORCE_THRESHOLD_KEY = "enforce_threshold"

# Process-local TTL cache for the scoring-config ConfigStore reads. The
# ``/scoring/enforce-threshold`` GET fires on every /admin/session-scoring
# mount and costs ~200-460 ms per call (Fastly ConfigStore round-trip) per
# the perf audit. 30 s TTL keeps repeated panel-refreshes / tab-toggles
# cheap without making the operator wait long after their own PUT — and
# the PUT counterpart busts the cache anyway so write-then-read is instant.
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_ENFORCE_THRESHOLD_CACHE_TTL = 300.0
_enforce_threshold_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)

# ── L2 enforcement opt-in (mirrors the Rust scorer's scoring_config gate) ─────
#
# L2's contribution to the *enforced* combined score is gated by an explicit
# operator opt-in (l2_enforce_enabled) and fades in over _L2_RAMP_DAYS from the
# l2_enabled_at anchor — NOT from deployment age. The 7-day deployment age is now
# only an advisory readiness gauge. These mirror the Rust constants in
# compute/scorer/src/scorer.rs (L2_RAMP_DAYS) + the day-7 readiness band.
_L2_RAMP_DAYS = 3.0
_L2_READINESS_DAYS = 7.0
# Process-local TTL cache for the L2-enforce ConfigStore reads (same pattern +
# TTL as the enforce-threshold cache). Shared by the GET endpoint AND the
# scoring_health readiness block, so a cold admin mount makes at most one Fastly
# round-trip per TTL regardless of which path warms it; the PUT busts it for an
# accurate read-after-write.
_l2_enforce_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)


def _l2_parse_epoch(raw: str | None) -> int | None:
    """Parse a UNIX-epoch-seconds ConfigStore value; None when absent/non-numeric."""
    if not raw:
        return None
    s = str(raw).strip()
    return int(s) if s.isdigit() else None


def _l2_enforce_unavailable() -> dict:
    """The L2-enforce/readiness block when it can't be read (scoring not enabled,
    no token, or an unreachable ConfigStore). All-off / not-ready so the UI
    degrades to a safe inert state instead of erroring."""
    return {
        "available": False,
        "enabled": False,
        "l2_enabled_at": None,
        "days_since_optin": None,
        "ramp_progress": 0.0,
        "fully_ramped": False,
        "warmup_days_remaining": None,
        "scoring_enabled_at": None,
        "deployment_age_days": None,
        "ready": False,
        "ramp_days": _L2_RAMP_DAYS,
        "readiness_days": _L2_READINESS_DAYS,
    }


def _build_l2_enforce_block(
    *,
    scoring_enabled_at_raw: str | None,
    l2_enforce_raw: str | None,
    l2_enabled_at_raw: str | None,
    now_epoch: int,
) -> dict:
    """Pure builder for the L2-enforce/readiness block from the three raw
    ConfigStore values + a fixed ``now`` (epoch seconds). Mirrors the Rust
    scorer's gate: ``enabled`` iff ``l2_enforce_enabled`` trims to "1"; the
    fade-in age comes from ``l2_enabled_at`` (the opt-in anchor), NOT deployment
    age; ``scoring_enabled_at`` drives only the advisory readiness gauge. Kept
    side-effect-free so the ramp math is unit-testable directly."""
    enabled = (l2_enforce_raw or "").strip() == "1"
    scoring_enabled_at = _l2_parse_epoch(scoring_enabled_at_raw)
    l2_enabled_at = _l2_parse_epoch(l2_enabled_at_raw)

    # Advisory deployment-age readiness gauge (never an actuator).
    deployment_age_days: float | None = None
    ready = False
    if scoring_enabled_at is not None:
        deployment_age_days = max(0.0, (now_epoch - scoring_enabled_at) / 86_400.0)
        ready = deployment_age_days >= _L2_READINESS_DAYS

    # Opt-in fade-in — only meaningful once the anchor exists. A future anchor
    # (clock skew) clamps to 0.0, mirroring the scorer's load_l2_days_since_optin.
    days_since_optin: float | None = None
    ramp_progress = 0.0
    fully_ramped = False
    warmup_days_remaining: float | None = None
    if l2_enabled_at is not None:
        days = 0.0 if now_epoch <= l2_enabled_at else (now_epoch - l2_enabled_at) / 86_400.0
        days_since_optin = days
        ramp_progress = max(0.0, min(1.0, days / _L2_RAMP_DAYS))
        fully_ramped = days >= _L2_RAMP_DAYS
        warmup_days_remaining = max(0.0, _L2_RAMP_DAYS - days)

    return {
        "available": True,
        "enabled": enabled,
        "l2_enabled_at": l2_enabled_at,
        "days_since_optin": round(days_since_optin, 3) if days_since_optin is not None else None,
        "ramp_progress": round(ramp_progress, 4),
        "fully_ramped": fully_ramped,
        "warmup_days_remaining": round(warmup_days_remaining, 3) if warmup_days_remaining is not None else None,
        "scoring_enabled_at": scoring_enabled_at,
        "deployment_age_days": round(deployment_age_days, 3) if deployment_age_days is not None else None,
        "ready": ready,
        "ramp_days": _L2_RAMP_DAYS,
        "readiness_days": _L2_READINESS_DAYS,
    }


def _l2_get_item(config_store_id: str, key: str, token: str) -> str:
    """GET one scoring_config item value; "" when the key is absent (404).
    Re-raises any other RuntimeError so a real Fastly outage surfaces (the GET
    endpoint maps it to 502; the best-effort readiness path swallows it)."""
    from backend.core.fastly.client import fastly

    try:
        item = fastly("GET", f"/resources/stores/config/{config_store_id}/item/{key}", token=token)
        return (item or {}).get("item_value", "") or ""
    except RuntimeError as exc:
        if "404" in str(exc):
            return ""
        raise


def _read_l2_enforce_state(service_id: str, config_store_id: str, token: str) -> dict:
    """Cached read+build of the L2-enforce/readiness block (mirrors the
    enforce-threshold GET cache). Assumes ``config_store_id`` + ``token`` are
    already resolved. Caches only successful reads under ``(service_id, store)``."""
    import datetime as _dt

    cache_key = (service_id, config_store_id)
    cached = _l2_enforce_cache.get(cache_key)
    if cached is not None:
        return cached
    block = _build_l2_enforce_block(
        scoring_enabled_at_raw=_l2_get_item(config_store_id, _SCORING_ENABLED_AT_KEY, token),
        l2_enforce_raw=_l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, token),
        l2_enabled_at_raw=_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, token),
        now_epoch=int(_dt.datetime.now(_dt.UTC).timestamp()),
    )
    _l2_enforce_cache[cache_key] = block
    return block


def _load_scoring_cfg(service_id: str) -> tuple[dict, dict]:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    return cfg, (cfg.get("scoring") or {})


def _require_confirm(confirm: bool, detail_msg: str) -> None:
    if not confirm:
        raise HTTPException(status_code=400, detail={"error": detail_msg})


def _push_matrix_to_kv_if_configured(service_id: str, cfg: dict | None, matrix_dict: dict | None = None) -> bool:
    matrix_store_id = ((cfg or {}).get("scoring") or {}).get("scoring_matrix_store_id", "")
    resolved_token = resolve_fastly_token(service_id)
    if not matrix_store_id or not resolved_token:
        if not matrix_store_id and matrix_dict is not None:
            logger.info("retrain: no scoring_matrix store id in config — KV push skipped (re-enable to provision it)")
        return False
    try:
        if matrix_dict is not None:
            from backend.core.fastly.client import fastly_raw
            from backend.provision.session_scoring_setup import MATRIX_KEY_NAME
            from backend.scoring.matrix import serialize_kv

            fastly_raw(
                "PUT",
                f"/resources/stores/kv/{matrix_store_id}/keys/{MATRIX_KEY_NAME}",
                serialize_kv(matrix_dict),
                content_type="application/octet-stream",
                token=resolved_token,
            )
        else:
            from backend.provision.session_scoring_orchestrator import _write_matrix_to_kv

            _write_matrix_to_kv(matrix_store_id, service_id, resolved_token)
        return True
    except Exception as exc:
        logger.warning(f"Could not write matrix to scoring_matrix KV: {exc}")
        return False


def l2_enforce_readiness_block(service_id: str) -> dict:
    """Best-effort L2-enforce/readiness block for the scoring_health composite.
    Degrades to the unavailable block on any missing-config / no-token / Fastly
    error so it can never break the (DuckDB-driven) health response. Called from
    ``scoring_health`` for admin requests only."""
    _cfg, scoring = _load_scoring_cfg(service_id)
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        return _l2_enforce_unavailable()
    token = resolve_fastly_token(service_id)
    if not token:
        return _l2_enforce_unavailable()
    try:
        return _read_l2_enforce_state(service_id, config_store_id, token)
    except Exception:  # noqa: BLE001 — advisory block must never break health
        return _l2_enforce_unavailable()


@router.post(
    "/{service_id}/scoring/retrain",
    response_model=ScoringRetrainResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_retrain(
    service_id: ServiceId,
    since_days: int = Query(default=7, ge=1, le=90, description="Window of DuckDB traffic to train on"),
    version: str | None = Query(default=None, description="Override matrix version label; defaults to today's date"),
) -> dict:
    """Build a fresh transition matrix from the last N days of DuckDB
    traffic, save it to ``compute/scorer/matrix.json``, publish to FOS,
    and evaluate AUC against the operator's accumulated labels.

    Synchronous — for a 7-day window with ~10k sessions the whole pipeline
    runs in <30s. The endpoint returns the new matrix metadata + AUC so
    the UI can show "matrix moved from 0.62 → 0.91 after retrain". The new
    matrix is pushed straight into the scoring_matrix KV Store, so the LIVE
    edge scorer picks it up as Compute instances recycle — no Wasm rebuild
    (the matrix is read from KV at runtime, not embedded).

    Pipeline:
      1. extract_traces from DuckDB → in-memory sessions
      2. build_matrix → TransitionMatrix
      3. evaluate AUC against labels (if >=3 each class)
      4. Save matrix.json to disk + publish to FOS
      4b. Write the matrix to the scoring_matrix KV Store (live scorer)
      5. Bust the /scoring/evaluation cache
    """
    import datetime as _dt

    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.scoring import fixtures as _fixtures
    from backend.scoring import labels as _labels
    from backend.scoring import matrix as _matrix
    from backend.scoring.evaluate import DEFAULT_MIN_AUC
    from backend.scoring.evaluate import evaluate as _evaluate

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    cfg = svcconfig.load_config(service_id) or {}
    matrix_version = version or _matrix.default_version()
    start = _dt.datetime.now(_dt.UTC) - _dt.timedelta(days=int(since_days))

    # 1. Extract sessions from DuckDB. The extract function expects a
    # live connection; reuse the same read-only path the analytics
    # endpoints use so we never block ingest writers.
    con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
    try:
        sessions_iter = _fixtures.extract_traces(con, service_id=service_id, start=start)
        # 2. Build matrix in one streaming pass.
        tmatrix, stats = _matrix.build_matrix(
            (s.to_jsonl_dict() for s in sessions_iter),
        )
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("scoring_retrain: con.close failed (ignored)", exc_info=True)

    matrix_dict = tmatrix.to_json_dict(version=matrix_version)

    # 3. Evaluate against accumulated labels if we have enough of each.
    auc_result = None
    label_rows = _labels.list_labels(service_id)
    n_good, n_bad, _ = _label_counts(service_id)
    if n_good >= _MIN_LABELS_PER_CLASS and n_bad >= _MIN_LABELS_PER_CLASS:
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        if labeled_sessions:
            er = _evaluate(matrix_dict, labeled_sessions)
            auc_result = {
                "auc": round(float(er.auc), 4),
                "passed": bool(er.passed),
                "threshold": float(er.pass_threshold),
                "n_good": er.n_good,
                "n_bad": er.n_bad,
            }

    # 4. Save matrix.json + publish to FOS. Local save is best-effort —
    # if the backend container can't write to compute/scorer/ (read-only
    # image mount), we still succeed by relying on FOS as the durable
    # store. _load_matrix() will pull from FOS next call.
    try:
        tenant_matrix_path = _tenant_matrix_path(service_id)
        tenant_matrix_path.parent.mkdir(parents=True, exist_ok=True)
        with tenant_matrix_path.open("w") as f:
            import json as _json

            _json.dump(matrix_dict, f)
        local_saved = True
    except Exception as exc:
        local_saved = False
        logger.warning(f"Could not write matrix.json locally: {exc}")

    fos_published = False
    try:
        from backend.state_sync import publish_matrix_to_fos

        publish_matrix_to_fos(service_id, matrix_dict)
        fos_published = True
    except Exception as exc:
        logger.warning(f"Could not publish matrix to FOS: {exc}")

    # 4b. Push the matrix into the scoring_matrix KV Store so the LIVE scorer
    #     picks it up — no Wasm rebuild. Writes the in-memory matrix directly
    #     (not the best-effort local file, which may have failed on a
    #     read-only mount). Best-effort: needs a Fastly token + a provisioned
    #     matrix store. On failure the matrix is still in FOS and a subsequent
    #     enable re-seeds KV. Takes effect as Compute instances recycle.
    kv_written = _push_matrix_to_kv_if_configured(service_id, cfg, matrix_dict)

    # 5. Bust analytics caches so the next StatusPanel hit sees the new AUC.
    _bust_analytics_cache(service_id)

    # Operator audit: every retrain is attributable + reviewable.
    from backend.core import metadata as metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_retrained",
        details={
            "matrix_version": matrix_version,
            "since_days": since_days,
            "sessions_trained_on": tmatrix.session_count,
            "auc_against_labels": auc_result,
            "fos_published": fos_published,
            "kv_written": kv_written,
        },
    )

    return {
        "ok": True,
        "matrix_version": matrix_version,
        "since_days": since_days,
        "sessions_trained_on": tmatrix.session_count,
        "transitions": tmatrix.transition_count,
        "vocab_size": len(tmatrix.vocab),
        "rejected": {
            "too_few_events": stats.sessions_dropped_short,
            "too_fast": stats.sessions_dropped_fast,
            "kept": stats.sessions_kept,
            "routes_seen": stats.routes_seen,
        },
        "auc_against_labels": auc_result,
        "default_min_auc": float(DEFAULT_MIN_AUC),
        "local_matrix_saved": local_saved,
        "fos_matrix_published": fos_published,
        "matrix_kv_written": kv_written,
        "deploy_hint": (
            "Live: matrix pushed to the scoring_matrix KV Store — the edge scorer "
            "picks it up as Compute instances recycle (typically minutes), no redeploy. "
            "The /scoring/evaluation endpoint reflects it immediately."
            if kv_written
            else (
                "Matrix saved to FOS but NOT pushed to the live scorer (no KV store / "
                "Fastly token available). Re-enable scoring for this service to "
                "provision the matrix KV store and seed it. The /scoring/evaluation "
                "endpoint reflects the new matrix immediately regardless."
            )
        ),
    }


# ── Session details (sid → page sequence) ────────────────────────────────────


@router.get(
    "/{service_id}/scoring/sessions/{sid}/events",
    response_model=ScoringSessionEventsResponse,
    response_model_exclude_unset=True,
)
def scoring_session_events(
    request: Request,
    service_id: ServiceId,
    sid: str = Path(..., description="Edge session id (12-hex chars)"),
    since_days: int = Query(default=30, ge=1, le=90),
) -> dict:
    """Return the event timeline for a single session — the URLs the
    session hit, in order, with per-request status/score/compliance/reason
    so the UI can render a 'view this labeled session' popover.

    The data is the same shape ``evaluate()`` consumes for AUC; this
    endpoint just exposes it through a public route keyed on the sid the
    operator clicked. Cap is 500 events per sid (any realistic browsing
    session well under that; the cap is a runaway-loop safety bound).

    Analyst-reachable, so the lookback is clamped to the invite window
    (since_days expressed as hours). The per-event ``ip`` is masked for
    analysts by the response middleware's key-name pass; ``ua`` / ``url`` stay
    intact (analysts triage on them). Admin requests keep the full window.
    """
    # Analyst → pass the clamped absolute predicate; admin (disc is None) keeps
    # the repo's own relative since_days DAY window untouched.
    ts_pred, disc = _scoring_time_window(request, int(since_days) * 24)
    grouped = _fetch_session_events(
        service_id, [sid], since_days=since_days, ts_predicate=(ts_pred if disc is not None else None)
    )
    events = grouped.get(sid, [])
    return {
        "sid": sid,
        "since_days": since_days,
        "event_count": len(events),
        "events": events,
    }


def _require_scoring_config_store(service_id: str, token: str = "") -> tuple[dict, str, str]:
    """Resolve ``(cfg, config_store_id, resolved_token)`` or raise HTTP 400."""
    cfg, scoring = _load_scoring_cfg(service_id)
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    return cfg, config_store_id, resolved_token


def _upsert_config_store_item(api_token: str, store_id: str, key: str, value: str) -> None:
    """Upsert a ConfigStore item via PATCH, falling back to POST on first write."""
    from backend.core.fastly.client import fastly

    try:
        fastly(
            "PATCH",
            f"/resources/stores/config/{store_id}/item/{key}",
            {"item_value": value},
            token=api_token,
        )
    except Exception:
        fastly(
            "POST",
            f"/resources/stores/config/{store_id}/item",
            {"item_key": key, "item_value": value},
            token=api_token,
        )


@router.get(
    "/{service_id}/scoring/enforce-threshold",
    response_model=ScoringEnforceThresholdResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_threshold_get(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Read the live enforce_threshold value from the scoring_config
    Compute ConfigStore. None = no enforcement.

    The Rust scorer reads this on every request — when set AND the
    request's score >= threshold, it emits X-Edge-Score-Enforce: 1,
    which the SCORING_ENFORCE_NAME VCL snippet turns into a 429.
    """
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    cache_key = (service_id, config_store_id)
    cached = _enforce_threshold_cache.get(cache_key)
    if cached is not None:
        return {**cached, "_is_cached": True}

    try:
        raw = _l2_get_item(config_store_id, _ENFORCE_THRESHOLD_KEY, resolved_token)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="enforce_threshold_read_failed", status=502)
    threshold: int | None = int(raw) if raw and raw.isdigit() else None

    result = {
        "threshold": threshold,
        "enforced": threshold is not None,
        "key": _ENFORCE_THRESHOLD_KEY,
    }
    _enforce_threshold_cache[cache_key] = result
    return result


@router.put(
    "/{service_id}/scoring/enforce-threshold",
    response_model=ScoringEnforceThresholdPutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_threshold_put(
    body: ScoringThresholdBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the enforcement change"),
) -> dict:
    """Write the live enforce_threshold to the scoring_config ConfigStore.
    Pass ``{"threshold": null}`` to clear (disable enforcement).

    Effective at the edge within seconds (next Compute invocation
    re-reads the ConfigStore). Audited to scoring_audit so the operator
    can review when enforcement was flipped on/off.

    Gated by ``?confirm=true`` (matches the matrix-restore pattern) so
    an accidental click can't silently flip enforcement at the edge."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change enforcement. This affects live edge blocking.")

    from backend.core import metadata as metadata_db

    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    threshold = body.threshold
    if threshold is not None and not 0 <= threshold <= 100:
        raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    # Upsert: PATCH the item, falling back to POST if it doesn't exist
    # yet (first time enforcement is set for this service).
    value = str(threshold) if threshold is not None else ""
    try:
        _upsert_config_store_item(resolved_token, config_store_id, _ENFORCE_THRESHOLD_KEY, value)
    except Exception as e:
        raise_internal(logger, e, code="scoring_enforce_threshold_put_failed")

    # Drop the cached GET response so the operator's read-after-write is
    # accurate instead of returning the up-to-30s-old snapshot.
    _enforce_threshold_cache.pop((service_id, config_store_id), None)

    metadata_db.record_scoring_audit(
        service_id,
        "threshold_enforce_disabled" if threshold is None else "threshold_enforced",
        details={"threshold": threshold},
    )

    return {
        "ok": True,
        "threshold": threshold,
        "enforced": threshold is not None,
        "message": (
            "Enforcement disabled — scorer will stop setting X-Edge-Score-Enforce on responses."
            if threshold is None
            else f"Enforcement live at threshold {threshold}. Scorer will set X-Edge-Score-Enforce=1 "
            "when score >= threshold; the Enforce VCL snippet 429s those requests."
        ),
    }


# ── L2 enforcement opt-in (operator decides when L2 joins the enforced score) ─


@router.get(
    "/{service_id}/scoring/l2-enforce",
    response_model=ScoringL2EnforceState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_l2_enforce_get(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Read the live L2-enforcement opt-in + fade-in state from the scoring_config
    ConfigStore, plus the advisory deployment-age readiness gauge.

    L2's sub-score is ALWAYS computed + logged at the edge; this endpoint reports
    only whether (and how strongly) it contributes to the *enforced* combined
    score. ``enabled`` is the operator's explicit opt-in; ``ramp_progress`` /
    ``days_since_optin`` / ``warmup_days_remaining`` track the fade-in from the
    opt-in anchor; ``ready`` / ``deployment_age_days`` are the advisory "enough
    observed L2 data yet?" gauge (deploy age ≥ 7 days), never an actuator.

    Read+cache pattern mirrors /scoring/enforce-threshold (300s TTL, busted by the
    PUT). 502 on a non-404 ConfigStore error rather than masking it as off."""
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    try:
        return _read_l2_enforce_state(service_id, config_store_id, resolved_token)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="l2_enforce_read_failed", status=502)


@router.put(
    "/{service_id}/scoring/l2-enforce",
    response_model=ScoringL2EnforcePutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_l2_enforce_put(
    body: ScoringL2EnforceBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the L2-enforcement change"),
) -> dict:
    """Opt edge Layer-2 in/out of the *enforced* combined score by writing the
    ``l2_enforce_enabled`` flag to the scoring_config ConfigStore.

    Enabling stamps a fresh ``l2_enabled_at`` anchor on the off→on transition (or
    when none exists yet) so L2 fades in over ~3 days from the moment of consent;
    re-confirming an already-on service does NOT reset an in-progress fade-in.
    Disabling writes "0" and leaves the anchor as-is, so a later off→on re-enable
    overwrites it and restarts the fade-in. L2's sub-score keeps being computed +
    logged regardless — only its contribution to the enforced score is gated.

    Effective at the edge within seconds (the scorer re-reads the ConfigStore each
    request). Audited. Gated by ``?confirm=true`` — the same kill switch as
    enforce-threshold — because it changes live edge blocking."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change L2 enforcement. This affects live edge blocking.")

    import datetime as _dt

    from backend.core import metadata as metadata_db

    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    enabled = bool(body.enabled)

    try:
        l2_enabled_at: int | None
        if enabled:
            # Stamp a fresh fade-in anchor on off→on (or when none exists yet),
            # but preserve an in-progress ramp when re-confirming an already-on
            # service. Read both current values first.
            currently_on = _l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, resolved_token).strip() == "1"
            existing_anchor = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
            _upsert_config_store_item(resolved_token, config_store_id, _L2_ENFORCE_ENABLED_KEY, "1")
            if (not currently_on) or (existing_anchor is None):
                l2_enabled_at = int(_dt.datetime.now(_dt.UTC).timestamp())
                _upsert_config_store_item(resolved_token, config_store_id, _L2_ENABLED_AT_KEY, str(l2_enabled_at))
            else:
                l2_enabled_at = existing_anchor
        else:
            _upsert_config_store_item(resolved_token, config_store_id, _L2_ENFORCE_ENABLED_KEY, "0")
            # Leave l2_enabled_at as-is; a later off→on re-enable overwrites it.
            l2_enabled_at = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
    except Exception as e:
        raise_internal(logger, e, code="scoring_l2_enforce_put_failed")

    # Read-after-write accuracy: drop the cached GET block + bust the analytics
    # cache (scoring_health's readiness block reuses the same numbers).
    _l2_enforce_cache.pop((service_id, config_store_id), None)
    _bust_analytics_cache(service_id)

    metadata_db.record_scoring_audit(
        service_id,
        "l2_enforce_enabled" if enabled else "l2_enforce_disabled",
        details={"enabled": enabled, "l2_enabled_at": l2_enabled_at},
    )

    return {
        "ok": True,
        "enabled": enabled,
        "l2_enabled_at": l2_enabled_at,
        "message": (
            f"L2 now contributes to the enforced score, fading in over {int(_L2_RAMP_DAYS)} days "
            "from opt-in. The edge scorer picks this up within seconds."
            if enabled
            else "L2 enforcement disabled — L2 returns to observe-only (sub-score still logged, "
            "no contribution to the enforced score)."
        ),
    }


# ── Recv exclusion regex (URLs that bypass the scorer) ─────────────────────


@router.get(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_get(service_id: ServiceId) -> dict:
    """Return the operator-configured URL-exclusion regex for the recv snippet.

    URLs that match this regex are NOT routed to the Compute scorer
    (saves cost on static assets / health checks / etc.). The default
    matches common static-asset file extensions; the operator can
    override it via the PUT endpoint below.

    Response shape:
      {
        "current":      str,    # the stored value (literal default after
                                # first enable_scoring; or operator override)
        "is_default":   bool,   # true when current is empty OR equals the
                                # built-in default literal
        "default":      str,    # the built-in default regex
        "effective":    str,    # what's actually interpolated into VCL
      }
    """
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        resolve_exclude_url_regex,
    )

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("exclude_url_regex") or ""
    effective = resolve_exclude_url_regex(current or None)
    return {
        "current": current,
        # Empty cfg (legacy services from before enable_scoring populated
        # the default) AND services whose stored value happens to equal
        # the bundled default both count as "default" for UI purposes —
        # the admin shouldn't see "custom override" when nothing's actually
        # been customised.
        "is_default": (not current) or current == DEFAULT_ASSET_EXT_REGEX,
        "default": DEFAULT_ASSET_EXT_REGEX,
        "effective": effective,
    }


def _require_scoring_enabled_and_token(service_id: str, token: str = "") -> tuple[dict, dict, str]:
    cfg, scoring = _load_scoring_cfg(service_id)
    if not scoring.get("enabled"):
        raise HTTPException(status_code=400, detail={"error": "Session scoring is not enabled for this service"})
    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})
    return cfg, scoring, resolved_token


def _validate_service_exclude_regex(service_id: str, raw: str):
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import validate_recv_exclusion_regex_with_lint

    return validate_recv_exclusion_regex_with_lint(
        raw,
        build_full_snippet=lambda cleaned_regex: recv_snippet(service_id, exclude_url_regex=cleaned_regex or None),
        require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "1") != "0",
    )


@router.put(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexPutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_put(
    body: ScoringExcludeRegexBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the change"),
) -> dict:
    """Update the URL-exclusion regex for the scoring recv snippet.

    Validation pipeline (must pass all four to land):
      1. Input policy (length cap, no quote / control chars, valid regex).
      2. Falco static analysis on the assembled recv-snippet body.
      3. Fastly's VCL ``validate`` endpoint on the cloned version.
      4. ``activate_version`` (Fastly's compiler runs again).

    Re-deploys ONLY the recv snippet — Compute service, Wasm, log
    format, and the other 5 scoring snippets stay untouched. Takes
    ~5-10s end-to-end.

    Pass ``{"regex": ""}`` to reset to the built-in default. Body shape:
        { "regex": str }

    Gated by ``?confirm=true`` because a typo here can disable scoring
    entirely (regex matches everything) or DoS Compute (regex matches
    nothing → every request scored). The confirm flag matches the
    enforce-threshold + matrix-restore precedent.
    """
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )

    from backend.core import metadata as metadata_db
    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex
    from backend.utils.vcl_validator import RegexValidationError

    _cfg, _scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    try:
        cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": exc.message, "reason": exc.reason},
        )

    # Layers 3 + 4: clone → swap → validate → activate via the
    # orchestrator helper.
    try:
        result = update_recv_exclusion_regex(service_id, resolved_token, new_regex=cleaned)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="exclude_regex_update_failed", status=502)

    metadata_db.record_scoring_audit(
        service_id,
        "scoring_exclude_regex_changed",
        details={
            "is_default": result["is_default"],
            "effective_regex": result["effective_regex"][:200],
            "logging_service_active_version": result["logging_service_active_version"],
            "lint_warnings": lint.warnings[:5],
        },
    )

    return {
        "ok": True,
        **result,
        "lint_warnings": lint.warnings,
        "message": (
            "Reset to default URL exclusion regex."
            if result["is_default"]
            else "Custom URL exclusion regex applied. Effective at the edge after Fastly version activation."
        ),
    }


# ── Dry-run validator for the exclude-regex (no persistence, no VCL) ──────


@router.post(
    "/{service_id}/scoring/exclude-regex/validate",
    response_model=ScoringExcludeRegexValidateResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_validate(
    body: ScoringExcludeRegexBody,
    service_id: ServiceId,
) -> dict:
    """Run the 2-layer pre-publish validator on a candidate regex WITHOUT
    persisting it or touching Fastly.

    Drives the admin UI's on-blur lint check: the operator types a regex,
    tabs out of the textarea, and gets immediate feedback on whether the
    value would pass input policy (length / quote / control-char / Python
    re.compile) AND falco's static analysis on the assembled snippet,
    BEFORE they commit to a publish flow.

    Response shape:
      Success:  {"ok": true,  "lint_warnings": [...]}
      Failure:  {"ok": false, "error": "...", "reason": "..."}

    The third layer (Fastly's own VCL compiler during version activate)
    only runs on real publish — we don't burn a clone/activate round-trip
    for a preview. False-positives between falco and Fastly's compiler are
    rare; the publish flow still catches them.
    """
    from backend.utils.vcl_validator import RegexValidationError

    try:
        _cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        return {
            "ok": False,
            "error": exc.message,
            "reason": exc.reason,
        }

    return {
        "ok": True,
        "lint_warnings": lint.warnings,
    }


# ── Enforce response status code (default 429, operator-overridable) ──────


@router.get(
    "/{service_id}/scoring/enforce-status-code",
    response_model=ScoringEnforceStatusCodeState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_status_code_get(service_id: ServiceId) -> dict:
    """Return the operator-configured HTTP status code that the enforce
    snippet returns when the scorer flags a request.

    Defaults to 429 (Too Many Requests). Operators can pick any 4xx/5xx
    code via the PUT endpoint below.

    Response shape:
      {
        "current":     int,    # operator's override, or null when default
        "default":     int,    # built-in default (429)
        "effective":   int,    # what's actually baked into the VCL
        "min":         int,    # min allowed value (400)
        "max":         int,    # max allowed value (599)
        "is_default":  bool,
      }
    """
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
        DEFAULT_ENFORCE_STATUS_CODE,
        resolve_enforce_status_code,
    )

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("enforce_status_code")
    effective = resolve_enforce_status_code(current)
    return {
        "current": current,
        "default": DEFAULT_ENFORCE_STATUS_CODE,
        "effective": effective,
        "min": _ENFORCE_STATUS_CODE_MIN,
        "max": _ENFORCE_STATUS_CODE_MAX,
        "is_default": effective == DEFAULT_ENFORCE_STATUS_CODE,
    }


@router.put(
    "/{service_id}/scoring/enforce-status-code",
    response_model=ScoringEnforceStatusCodePutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_status_code_put(
    body: ScoringEnforceStatusCodeBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the change"),
) -> dict:
    """Update the HTTP status code returned by the enforce snippet.

    Body shape: ``{"status_code": int | null}``. Pass ``null`` (or omit)
    to reset to the default 429.

    Validation:
      - Must be int in 400-599 (4xx/5xx HTTP error range).
      - Anything else → 400 with explanation.

    Re-deploys ONLY the enforce snippet — Compute service, Wasm, log
    format, and the other 5 scoring snippets stay untouched. Takes
    ~5-10s end-to-end.

    Gated by ``?confirm=true`` because the change affects live edge
    response codes seen by real users — same precedent as
    enforce-threshold and exclude-regex.
    """
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )

    from backend.core import metadata as metadata_db
    from backend.provision.session_scoring_orchestrator import update_enforce_status_code
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
    )

    new_code = body.status_code
    if new_code is not None and not (_ENFORCE_STATUS_CODE_MIN <= new_code <= _ENFORCE_STATUS_CODE_MAX):
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"status_code must be in {_ENFORCE_STATUS_CODE_MIN}-{_ENFORCE_STATUS_CODE_MAX} (HTTP 4xx/5xx)"
            },
        )

    _cfg, _scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    try:
        result = update_enforce_status_code(service_id, resolved_token, new_status_code=new_code)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="enforce_status_code_update_failed", status=502)

    metadata_db.record_scoring_audit(
        service_id,
        "scoring_enforce_status_code_changed",
        details={
            "is_default": result["is_default"],
            "effective_status_code": result["effective_status_code"],
            "logging_service_active_version": result["logging_service_active_version"],
        },
    )

    return {
        "ok": True,
        **result,
        "message": (
            "Reset to default enforce status code (429)."
            if result["is_default"]
            else f"Enforce status code → {result['effective_status_code']}. Effective at the edge after Fastly version activation."
        ),
    }


# ── Matrix version history + rollback ──────────────────────────────────────


@router.get(
    "/{service_id}/scoring/matrix-versions",
    response_model=ScoringMatrixVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_list(service_id: ServiceId) -> dict:
    """List historical scoring matrices archived in FOS.

    publish_matrix_to_fos snapshots the prior current matrix to
    ``iceberg/meta/scoring_matrix_history/{version}.json`` before
    overwriting, so the operator can roll back to any prior trained
    matrix. Returns most-recent first."""
    from backend.state_sync import list_scoring_matrix_versions

    _cfg, scoring = _load_scoring_cfg(service_id)
    current_version = scoring.get("matrix_version")
    return {
        "versions": list_scoring_matrix_versions(service_id),
        "current_version": current_version,
    }


@router.post(
    "/{service_id}/scoring/matrix-versions/{version}/restore",
    response_model=ScoringMatrixRestoreResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_restore(
    service_id: ServiceId,
    version: str = Path(
        ...,
        description="Matrix version string to restore",
        pattern=r"^[A-Za-z0-9._-]+$",
        max_length=64,
    ),
    confirm: bool = Query(default=False, description="Set true to actually perform the restore"),
) -> dict:
    """Restore a historical matrix to the current scoring_matrix.json
    key in FOS. Also deletes the local matrix.json so the next
    /scoring/evaluation call sees the FOS-restored matrix, and pushes the
    restored matrix into the scoring_matrix KV Store so the LIVE edge scorer
    picks it up (as Compute instances recycle) — no redeploy. The
    /scoring/evaluation AUC reflects the restored matrix immediately.

    Gated by ``?confirm=true`` so an accidental click can't silently
    rewind the live AUC numbers."""
    _require_confirm(confirm, "Pass ?confirm=true to actually restore. This will replace the current matrix.")

    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.state_sync import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Matrix version {version!r} not found in FOS history"},
        )

    # Drop the tenant-scoped local matrix (matrix_<sid>.json) so
    # _load_matrix falls through to the FOS-restored version instead of
    # shadowing it. (_load_matrix's resolution order consults only the
    # tenant path — the pre-audit-005 shared matrix.json is never read,
    # and prod was verified clean of it on 2026-07-06.)
    tenant_matrix_path = _tenant_matrix_path(service_id)
    try:
        if tenant_matrix_path.exists():
            tenant_matrix_path.unlink()
    except Exception as exc:
        logger.warning(f"Could not remove local {tenant_matrix_path.name} after restore: {exc}")

    # Update cfg.scoring.matrix_version so /scoring/status reflects the rollback.
    cfg = svcconfig.mutate_config(
        service_id, lambda c: c.setdefault("scoring", {}).__setitem__("matrix_version", version)
    )

    # Push the FOS-restored matrix into the scoring_matrix KV Store so the live
    # scorer rolls back too. _write_matrix_to_kv resolves via the FOS chain
    # (the local copy was just deleted). Best-effort — needs a token + store.
    kv_written = _push_matrix_to_kv_if_configured(service_id, cfg)

    _bust_analytics_cache(service_id)

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_restored",
        details={"restored_version": version, "restored_at": result["restored_at"], "kv_written": kv_written},
    )

    return {
        "ok": True,
        "restored_version": version,
        "restored_at": result["restored_at"],
        "matrix_kv_written": kv_written,
        "deploy_hint": (
            "Live: restored matrix pushed to the scoring_matrix KV Store — the edge "
            "scorer rolls back as Compute instances recycle (minutes), no redeploy."
            if kv_written
            else (
                "Backend AUC + evaluation reflect the restored matrix. It was NOT pushed "
                "to the live scorer (no KV store / token); re-enable scoring to seed KV."
            )
        ),
    }


# ── AES key rotation ────────────────────────────────────────────────────────


@router.post(
    "/{service_id}/scoring/rotate-key",
    response_model=ScoringRotateKeyResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
@query_errors(500, code="scoring_rotate_key_failed", logger_obj=logger)
def scoring_rotate_key(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Rotate the AES-GCM cookie-state encryption key.

    Moves the current key to ``previous_key_hex`` (grace window for
    in-flight cookies still using the old key) and writes a fresh
    32-byte key as the new ``current_key_hex``. The Rust scorer's
    cookie codec already tries previous as a fallback so existing
    sessions keep decoding for one rotation cycle.

    Returns rotation metadata — the new key itself is NOT returned in
    the response (only stored in the Fastly ConfigStore + audit log).
    """
    from backend.core import metadata as metadata_db
    from backend.provision.session_scoring_setup import rotate_aes_key

    load_service_config(service_id)
    _cfg, scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    scoring_keys_store_id = scoring.get("scoring_keys_store_id")
    if not scoring_keys_store_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Service has no scoring_keys_store_id (was scoring enabled before key rotation was supported?)"
            },
        )

    result = rotate_aes_key(scoring_keys_store_id, token=resolved_token)

    # Record the rotation in the audit log (without the key value).
    metadata_db.record_scoring_audit(
        service_id,
        "key_rotated",
        details={
            "rotated_at": result["rotated_at"],
            "previous_key_grace": bool(result.get("previous_key_hex")),
        },
    )

    # Don't echo the key itself.
    return {
        "ok": True,
        "rotated_at": result["rotated_at"],
        "previous_key_grace": bool(result.get("previous_key_hex")),
        "message": (
            "AES key rotated. Cookies signed with the previous key keep "
            "decoding via the previous_key_hex grace slot — clear that "
            "slot by rotating again after the idle-expire window (~hours)."
        ),
    }


# ── Operator audit log ──────────────────────────────────────────────────────


@router.get(
    "/{service_id}/scoring/audit",
    response_model=ScoringAuditListResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_audit_list(
    service_id: ServiceId,
    limit: int = Query(default=100, ge=1, le=1000),
    since: str | None = Query(default=None, description="ISO timestamp lower bound (inclusive)"),
) -> dict:
    """List recent operator actions on this service's scoring config.

    Tracks: scoring_enabled, scoring_disabled, threshold_committed,
    threshold_cleared, threshold_enforced, threshold_enforce_disabled,
    matrix_retrained, matrix_restored, key_rotated. Each row has
    timestamp, action, actor, details (JSON). Used for compliance review
    + "who broke prod last Tuesday?" triage.

    ``since`` (optional ISO timestamp) filters to rows at or after that
    instant — handy for the admin UI to poll for new events without
    re-rendering the entire history."""
    from backend.core import metadata as metadata_db

    # 404 when the service itself isn't known — mirrors /scoring/status so
    # the UI gets a consistent shape across the audit + status pair.
    load_service_config(service_id)

    rows = metadata_db.list_scoring_audit(service_id, limit=limit, since=since)
    return {"audit": rows, "limit": limit}


# ── Operator's chosen threshold (persisted, not enforced) ───────────────────


@router.get(
    "/{service_id}/scoring/threshold",
    response_model=ScoringThresholdState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_threshold_get(service_id: ServiceId) -> dict:
    """Return the operator's chosen score threshold.

    NOT enforced — the live scorer doesn't read this. It's a persisted
    operator preference so the threshold slider can remember the
    'committed' value across sessions, and the StatusPanel can show
    'committed threshold: X' as a stable reference. Actual enforcement
    requires a Rust scorer change + Wasm redeploy and is deferred to
    a future release once the operator is confident in the value.
    """
    _cfg, scoring = _load_scoring_cfg(service_id)
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,  # See docstring — preview-only
    }


@router.put(
    "/{service_id}/scoring/threshold",
    response_model=ScoringThresholdState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_threshold_put(
    body: ScoringThresholdBody,
    service_id: ServiceId,
) -> dict:
    """Persist the operator's chosen threshold (0-100) into the per-service
    config. Pass ``{"threshold": null}`` to clear. Always returns the
    new state. Does NOT push to Compute — preview-only."""
    import datetime as _dt

    from backend import config as svcconfig

    threshold = body.threshold
    if threshold is not None and not 0 <= threshold <= 100:
        raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    cfg = load_service_config(service_id)
    prior_threshold = cfg.get("scoring", {}).get("operator_threshold")
    set_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")

    def _apply_threshold(c: dict) -> None:
        scoring = c.setdefault("scoring", {})
        if threshold is None:
            scoring.pop("operator_threshold", None)
            scoring.pop("operator_threshold_set_at", None)
        else:
            scoring["operator_threshold"] = threshold
            scoring["operator_threshold_set_at"] = set_at

    # Operator audit trail — every threshold change is attributable.
    from backend.core import metadata as metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "threshold_committed" if threshold is not None else "threshold_cleared",
        details={"prior_threshold": prior_threshold, "new_threshold": threshold},
    )
    updated_cfg = svcconfig.mutate_config(service_id, _apply_threshold)
    if not isinstance(updated_cfg, dict):
        _apply_threshold(cfg)
        updated_cfg = cfg
    scoring = updated_cfg.get("scoring", {})

    _bust_analytics_cache(service_id)  # so /scoring/status reflects it next fetch
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
    }


# ── Per-reason AUC breakdown ────────────────────────────────────────────────


@router.get(
    "/{service_id}/scoring/evaluation/per-reason",
    response_model=ScoringPerReasonResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_evaluation_per_reason(
    service_id: ServiceId,
) -> dict:
    """AUC broken down by L1/L2 rule (cookie-missing, impossibly-fast,
    robotic-consistency, rare-transition, low-transition-prob).

    Same min-samples gate as /scoring/evaluation but applied per-bucket
    (so a reason with <3 labels in either class shows a 'need more
    labels with reason=X' CTA instead of a noisy AUC). The headline
    /scoring/evaluation gives the combined AUC; this answers 'which
    rule contributed most to AUC' once enough per-reason labels exist.
    """
    from backend.scoring import labels as _labels
    from backend.scoring.evaluate import evaluate_per_reason

    n_good, n_bad, n_neutral = _label_counts(service_id)

    cache_key = ("scoring-evaluation-per-reason", service_id, n_good, n_bad, n_neutral)

    def _produce() -> dict:
        if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
            # No point bucketing — the headline AUC isn't even computable.
            return {
                "has_min_samples_overall": False,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
                "buckets": [],
            }
        label_rows = _labels.list_labels(service_id)
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        result = evaluate_per_reason(labeled_sessions, min_per_class=_MIN_LABELS_PER_CLASS)
        result["has_min_samples_overall"] = True
        result["n_good"] = n_good
        result["n_bad"] = n_bad
        return result

    return _cached(cache_key, _produce)


# ── Composite dashboard endpoint ────────────────────────────────────────────
#
# Single round-trip variant of the 8 endpoints the session-scoring admin page
# mounts (status, evaluation, health, top-flagged, score-distribution,
# compliance-breakdown, curves, threshold-preview). Opens ONE read-only
# DuckDB connection, builds ONE filtered temp table, runs each aggregation
# against it.
#
# Wire-compat: this is purely additive — the 8 existing endpoints stay
# mounted with their current cache-key contracts and TTL behavior. The
# frontend can opt in by calling /scoring/dashboard instead of the 8
# individual queries, or keep fanning out for now.


@router.get(
    "/{service_id}/scoring/dashboard",
    response_model=ScoringDashboardResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_dashboard(
    request: Request,
    service_id: ServiceId,
    since_hours: int = Query(default=24, ge=1, le=168),
    threshold: int = Query(default=75, ge=0, le=100, description="Preview cutoff for threshold-preview block"),
) -> dict:
    """One-shot dashboard payload. Returns:

    ```
    {
        since_hours, threshold,
        status: {...},                                  # /scoring/status
        evaluation: {...},                              # /scoring/evaluation
        health: {...},                                  # /scoring/health
        top_flagged: {rows: [...], since_hours},        # /scoring/top-flagged
        score_distribution: {rows: [...]},              # /scoring/score-distribution
        compliance_breakdown: {rows: [...]},            # /scoring/compliance-breakdown
        curves: {...},                                  # /scoring/curves
        threshold_preview: {...},                       # /scoring/threshold-preview
    }
    ```

    Each sub-object is byte-identical to the corresponding individual
    endpoint's response — the frontend can swap to
    ``dashboard.top_flagged`` without changing card-level contracts.

    Cache key includes ``since_hours``, ``threshold``, and the per-class
    label counts so label mutations + slider drags invalidate naturally.
    """
    from backend import config as svcconfig

    n_good, n_bad, n_neutral = _label_counts(service_id)

    cache_key = (
        "scoring-dashboard",
        service_id,
        since_hours,
        threshold,
        n_good,
        n_bad,
        n_neutral,
    )

    def _produce() -> dict:
        # --- /scoring/status (no DuckDB) ---
        cfg = svcconfig.load_config(service_id)
        status_block: dict = scoring_status(service_id=service_id) if cfg else {"enabled": False}

        # Build the dashboard in a single payload by delegating to the
        # existing per-endpoint producers. Each handles its own _query_logs
        # call — meaning 6 DuckDB connections instead of 1 (the audit's
        # ideal). The win this iteration captures is the in-flight collapse:
        # one composite request → one cache key → one set of fetches that
        # serializes through the per-key lock instead of 8 frontend
        # requests racing through the proxy + react-query.
        #
        # The shared-temp-table optimization stays available for a future
        # PR — wiring it requires refactoring each per-endpoint producer
        # to accept an open connection + table name, which touches 5
        # endpoints worth of test surface. Punting that to v1.2.0 keeps
        # this change additive + zero-risk.
        import contextvars
        from concurrent.futures import ThreadPoolExecutor

        # Pre-warm _table_columns once so the 5 sub-handlers below share the
        # cached schema + view binding instead of opening 5 concurrent
        # DESCRIBE connections on cold cache.
        _table_columns(service_id)

        tasks = {
            "evaluation": lambda: scoring_evaluation(service_id=service_id),
            "evaluation_per_reason": lambda: scoring_evaluation_per_reason(service_id=service_id),
            "health": lambda: scoring_health(request=request, service_id=service_id, since_hours=since_hours),
            "top_flagged": lambda: scoring_top_flagged(
                request=request, service_id=service_id, since_hours=since_hours, limit=50
            ),
            "score_distribution": lambda: scoring_score_distribution(
                request=request, service_id=service_id, since_hours=since_hours
            ),
            "compliance_breakdown": lambda: scoring_compliance_breakdown(
                request=request, service_id=service_id, since_hours=since_hours
            ),
            "curves": lambda: scoring_curves(service_id=service_id),
            "threshold_preview": lambda: scoring_threshold_preview(
                request=request, service_id=service_id, threshold=threshold, since_hours=since_hours
            ),
        }
        results: dict = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = {k: pool.submit(contextvars.copy_context().run, fn) for k, fn in tasks.items()}
            for k, fut in futs.items():
                results[k] = fut.result()

        return {
            "since_hours": since_hours,
            "threshold": threshold,
            "status": status_block,
            "evaluation": results["evaluation"],
            "evaluation_per_reason": results["evaluation_per_reason"],
            "health": results["health"],
            "top_flagged": results["top_flagged"],
            "score_distribution": results["score_distribution"],
            "compliance_breakdown": results["compliance_breakdown"],
            "curves": results["curves"],
            "threshold_preview": results["threshold_preview"],
            # `config_threshold` is intentionally distinct from the
            # top-level `threshold` (which is the per-request slider
            # cutoff fed into threshold_preview). The config block
            # mirrors /scoring/config's shape so the frontend can read
            # the persisted operator preferences off the same payload.
            "config_threshold": scoring_threshold_get(service_id),
            "exclude_regex": scoring_exclude_regex_get(service_id),
            "enforce_status_code": scoring_enforce_status_code_get(service_id),
        }

    return _cached(cache_key, _produce)


# R-1: register the enforce-threshold TTL cache so the autouse
# fixture in tests/conftest.py drains it via CacheRegistry.clear_all()
# instead of hand-clearing.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("session_scoring_admin._enforce_threshold_cache", _enforce_threshold_cache)
_CacheRegistry.register("session_scoring_admin._l2_enforce_cache", _l2_enforce_cache)
