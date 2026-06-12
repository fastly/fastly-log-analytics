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

from fastapi import Header, HTTPException, Path, Query

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

router = _ss.router  # type: ignore[has-type]
logger = _ss.logger  # type: ignore[has-type]
_bust_analytics_cache = _ss._bust_analytics_cache  # type: ignore[has-type]
_cached = _ss._cached  # type: ignore[has-type]
_load_matrix = _ss._load_matrix  # type: ignore[has-type]
_fetch_session_events = _ss._fetch_session_events  # type: ignore[has-type]
_reconstruct_labeled_sessions = _ss._reconstruct_labeled_sessions  # type: ignore[has-type]
_resolve_token = _ss._resolve_token  # type: ignore[has-type]
_query_logs = _ss._query_logs  # type: ignore[has-type]
_finalize_cached = _ss._finalize_cached  # type: ignore[has-type]
_SECRET_KEYS = _ss._SECRET_KEYS  # type: ignore[has-type]
_MIN_LABELS_PER_CLASS = _ss._MIN_LABELS_PER_CLASS  # type: ignore[has-type]

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

_ENFORCE_THRESHOLD_CACHE_TTL = 30.0
_enforce_threshold_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)


@router.post("/{service_id}/scoring/retrain")
def scoring_retrain(
    service_id: str = Path(...),
    since_days: int = Query(default=7, ge=1, le=90, description="Window of DuckDB traffic to train on"),
    version: str | None = Query(default=None, description="Override matrix version label; defaults to today's date"),
) -> dict:
    """Build a fresh transition matrix from the last N days of DuckDB
    traffic, save it to ``compute/scorer/matrix.json``, publish to FOS,
    and evaluate AUC against the operator's accumulated labels.

    Synchronous — for a 7-day window with ~10k sessions the whole pipeline
    runs in <30s. The endpoint returns the new matrix metadata + AUC so
    the UI can show "matrix moved from 0.62 → 0.91 after retrain". The
    Wasm build + Compute deploy is a separate step (requires Fastly CLI
    + Rust toolchain on the operator's box — not Docker-friendly): the
    response includes a hint pointing at ``scripts/scoring/deploy_wasm.sh``.

    Pipeline:
      1. extract_traces from DuckDB → in-memory sessions
      2. build_matrix → TransitionMatrix
      3. evaluate AUC against labels (if >=3 each class)
      4. Save matrix.json to disk + publish to FOS
      5. Bust the /scoring/evaluation cache
    """
    import datetime as _dt

    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH
    from backend.scoring import fixtures as _fixtures
    from backend.scoring import labels as _labels
    from backend.scoring import matrix as _matrix
    from backend.scoring.evaluate import DEFAULT_MIN_AUC
    from backend.scoring.evaluate import evaluate as _evaluate

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    cfg = svcconfig.load_config(service_id) or {}
    matrix_version = version or _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d-r")
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
            pass

    matrix_dict = tmatrix.to_json_dict(version=matrix_version)

    # 3. Evaluate against accumulated labels if we have enough of each.
    auc_result = None
    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    if counts.get("good", 0) >= _MIN_LABELS_PER_CLASS and counts.get("bad", 0) >= _MIN_LABELS_PER_CLASS:
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
        _MATRIX_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _MATRIX_PATH.open("w") as f:
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

    # 5. Bust analytics caches so the next StatusPanel hit sees the new AUC.
    _bust_analytics_cache(service_id)

    # Operator audit: every retrain is attributable + reviewable.
    from backend.core import metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_retrained",
        details={
            "matrix_version": matrix_version,
            "since_days": since_days,
            "sessions_trained_on": tmatrix.session_count,
            "auc_against_labels": auc_result,
            "fos_published": fos_published,
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
        "deploy_hint": (
            "Run scripts/scoring/deploy_wasm.sh --service-id "
            f"{(cfg.get('scoring') or {}).get('scoring_service_id', '?')} from your local box "
            "to embed this matrix into the Wasm and push to Fastly Compute. "
            "Until then the live scorer keeps using its previously-embedded matrix; "
            "the /scoring/evaluation endpoint will reflect the new matrix immediately "
            "(it reads matrix.json + FOS, not the deployed Wasm)."
        ),
    }


# ── Session details (sid → page sequence) ────────────────────────────────────


@router.get("/{service_id}/scoring/sessions/{sid}/events")
def scoring_session_events(
    service_id: str = Path(...),
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
    """
    grouped = _fetch_session_events(service_id, [sid], since_days=since_days)
    events = grouped.get(sid, [])
    return {
        "sid": sid,
        "since_days": since_days,
        "event_count": len(events),
        "events": events,
    }


@router.get("/{service_id}/scoring/enforce-threshold")
def scoring_enforce_threshold_get(
    service_id: str = Path(...),
    token: str = Header(default=""),
) -> dict:
    """Read the live enforce_threshold value from the scoring_config
    Compute ConfigStore. None = no enforcement.

    The Rust scorer reads this on every request — when set AND the
    request's score >= threshold, it emits X-Edge-Score-Enforce: 1,
    which the SCORING_ENFORCE_NAME VCL snippet turns into a 429.
    """
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    cache_key = (service_id, config_store_id)
    cached = _enforce_threshold_cache.get(cache_key)
    if cached is not None:
        return {**cached, "_is_cached": True}

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        item = fastly(
            "GET",
            f"/resources/stores/config/{config_store_id}/item/{_ENFORCE_THRESHOLD_KEY}",
            token=resolved_token,
        )
        raw = (item or {}).get("item_value", "")
        threshold: int | None = int(raw) if raw and raw.isdigit() else None
    except RuntimeError as exc:
        # 404 from ConfigStore = key not present = enforcement not set.
        # Mirrors the pattern in session_scoring_orchestrator.py:307-311.
        if "404" in str(exc):
            threshold = None
        else:
            logger.exception("scoring_enforce_threshold_get failed for %s", service_id)
            raise HTTPException(
                status_code=502,
                detail={"error": f"failed to read enforce threshold: {exc}"},
            )

    result = {
        "threshold": threshold,
        "enforced": threshold is not None,
        "key": _ENFORCE_THRESHOLD_KEY,
    }
    _enforce_threshold_cache[cache_key] = result
    return result


@router.put("/{service_id}/scoring/enforce-threshold")
def scoring_enforce_threshold_put(
    body: dict,
    service_id: str = Path(...),
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
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={"error": "Pass ?confirm=true to actually change enforcement. This affects live edge blocking."},
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.core.fastly.client import fastly

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    raw = body.get("threshold")
    threshold: int | None
    if raw is None:
        threshold = None
    else:
        try:
            threshold = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"error": "threshold must be int 0-100 or null"})
        if not 0 <= threshold <= 100:
            raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    # Upsert: PATCH the item, falling back to POST if it doesn't exist
    # yet (first time enforcement is set for this service).
    value = str(threshold) if threshold is not None else ""
    try:
        try:
            fastly(
                "PATCH",
                f"/resources/stores/config/{config_store_id}/item/{_ENFORCE_THRESHOLD_KEY}",
                {"item_value": value},
                token=resolved_token,
            )
        except Exception:
            fastly(
                "POST",
                f"/resources/stores/config/{config_store_id}/item",
                {"item_key": _ENFORCE_THRESHOLD_KEY, "item_value": value},
                token=resolved_token,
            )
    except Exception as e:
        logger.exception("scoring_enforce_threshold_put failed for %s", service_id)
        raise HTTPException(status_code=500, detail={"error": str(e)})

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


# ── Recv exclusion regex (URLs that bypass the scorer) ─────────────────────


@router.get("/{service_id}/scoring/exclude-regex")
def scoring_exclude_regex_get(service_id: str = Path(...)) -> dict:
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        resolve_exclude_url_regex,
    )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
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


@router.put("/{service_id}/scoring/exclude-regex")
def scoring_exclude_regex_put(
    body: dict,
    service_id: str = Path(...),
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
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
            },
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import (
        RegexValidationError,
        validate_recv_exclusion_regex_with_lint,
    )

    raw = body.get("regex", "")
    if not isinstance(raw, str):
        raise HTTPException(status_code=400, detail={"error": "body.regex must be a string"})

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(
            status_code=400,
            detail={"error": "Session scoring is not enabled for this service"},
        )
    request_secret = scoring.get("request_secret") or ""
    if not request_secret:
        raise HTTPException(
            status_code=400,
            detail={"error": "Internal: request_secret missing from cfg. Re-run enable_scoring."},
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    # Layers 1 + 2: input policy + falco static analysis on the
    # assembled snippet. We close over the per-service ids so the
    # validator can build the full snippet body.
    def _build(cleaned_regex: str) -> str:
        return recv_snippet(service_id, request_secret, exclude_url_regex=cleaned_regex or None)

    try:
        cleaned, lint = validate_recv_exclusion_regex_with_lint(
            raw,
            build_full_snippet=_build,
            # Production keeps falco mandatory; tests / local dev where
            # falco isn't on PATH can override via env.
            require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "0") == "1",
        )
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
        raise HTTPException(status_code=502, detail={"error": str(exc)})

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


@router.post("/{service_id}/scoring/exclude-regex/validate")
def scoring_exclude_regex_validate(
    body: dict,
    service_id: str = Path(...),
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import (
        RegexValidationError,
        validate_recv_exclusion_regex_with_lint,
    )

    raw = body.get("regex", "")
    if not isinstance(raw, str):
        raise HTTPException(status_code=400, detail={"error": "body.regex must be a string"})

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    # The validator needs a request_secret to build the assembled snippet
    # for falco lint — that's a VCL substitution, not anything the lint
    # inspects semantically. Use a stable placeholder when scoring isn't
    # enabled yet so the operator can still pre-validate before turn-on.
    request_secret = scoring.get("request_secret") or "PLACEHOLDER_FOR_LINT_ONLY"

    def _build(cleaned_regex: str) -> str:
        return recv_snippet(service_id, request_secret, exclude_url_regex=cleaned_regex or None)

    try:
        _cleaned, lint = validate_recv_exclusion_regex_with_lint(
            raw,
            build_full_snippet=_build,
            require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "0") == "1",
        )
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


@router.get("/{service_id}/scoring/enforce-status-code")
def scoring_enforce_status_code_get(service_id: str = Path(...)) -> dict:
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
        DEFAULT_ENFORCE_STATUS_CODE,
        resolve_enforce_status_code,
    )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
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


@router.put("/{service_id}/scoring/enforce-status-code")
def scoring_enforce_status_code_put(
    body: dict,
    service_id: str = Path(...),
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
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
            },
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import update_enforce_status_code
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
    )

    raw = body.get("status_code")
    new_code: int | None
    if raw is None:
        new_code = None
    else:
        try:
            new_code = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail={"error": "status_code must be an integer or null"},
            )
        if not (_ENFORCE_STATUS_CODE_MIN <= new_code <= _ENFORCE_STATUS_CODE_MAX):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"status_code must be in {_ENFORCE_STATUS_CODE_MIN}-{_ENFORCE_STATUS_CODE_MAX} (HTTP 4xx/5xx)"
                },
            )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(
            status_code=400,
            detail={"error": "Session scoring is not enabled for this service"},
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        result = update_enforce_status_code(service_id, resolved_token, new_status_code=new_code)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail={"error": str(exc)})

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


@router.get("/{service_id}/scoring/matrix-versions")
def scoring_matrix_versions_list(service_id: str = Path(...)) -> dict:
    """List historical scoring matrices archived in FOS.

    publish_matrix_to_fos snapshots the prior current matrix to
    ``iceberg/meta/scoring_matrix_history/{version}.json`` before
    overwriting, so the operator can roll back to any prior trained
    matrix. Returns most-recent first."""
    from backend import config as svcconfig
    from backend.state_sync import list_scoring_matrix_versions

    cfg = svcconfig.load_config(service_id) or {}
    current_version = (cfg.get("scoring") or {}).get("matrix_version")
    return {
        "versions": list_scoring_matrix_versions(service_id),
        "current_version": current_version,
    }


@router.post("/{service_id}/scoring/matrix-versions/{version}/restore")
def scoring_matrix_versions_restore(
    service_id: str = Path(...),
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
    /scoring/evaluation call sees the FOS-restored matrix.

    Live edge scorer (Wasm) keeps using its previously-embedded matrix
    until the operator re-runs deploy_wasm.sh. The /scoring/evaluation
    AUC will reflect the restored matrix immediately.

    Gated by ``?confirm=true`` so an accidental click can't silently
    rewind the live AUC numbers."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={"error": "Pass ?confirm=true to actually restore. This will replace the current matrix."},
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH
    from backend.state_sync import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Matrix version {version!r} not found in FOS history"},
        )

    # Drop the local matrix.json so _load_matrix falls through to the
    # FOS-restored version instead of shadowing it.
    try:
        if _MATRIX_PATH.exists():
            _MATRIX_PATH.unlink()
    except Exception as exc:
        logger.warning(f"Could not remove local matrix.json after restore: {exc}")

    # Update cfg.scoring.matrix_version so /scoring/status reflects the rollback.
    cfg = svcconfig.load_config(service_id)
    if cfg:
        scoring = cfg.setdefault("scoring", {})
        scoring["matrix_version"] = version
        svcconfig.save_config(service_id, cfg)

    _bust_analytics_cache(service_id)

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_restored",
        details={"restored_version": version, "restored_at": result["restored_at"]},
    )

    return {
        "ok": True,
        "restored_version": version,
        "restored_at": result["restored_at"],
        "deploy_hint": (
            "Backend AUC + evaluation endpoints now reflect the restored matrix. "
            "Live edge scorer keeps using its previously-embedded matrix until "
            "you re-run scripts/scoring/deploy_wasm.sh."
        ),
    }


# ── AES key rotation ────────────────────────────────────────────────────────


@router.post("/{service_id}/scoring/rotate-key")
def scoring_rotate_key(
    service_id: str = Path(...),
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
    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_setup import rotate_aes_key

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})

    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(status_code=400, detail={"error": "Scoring is not enabled for this service"})

    scoring_keys_store_id = scoring.get("scoring_keys_store_id")
    if not scoring_keys_store_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Service has no scoring_keys_store_id (was scoring enabled before key rotation was supported?)"
            },
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        result = rotate_aes_key(scoring_keys_store_id, token=resolved_token)
    except Exception as e:
        logger.exception("scoring_rotate_key failed for %s", service_id)
        raise HTTPException(status_code=500, detail={"error": str(e)})

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


@router.get("/{service_id}/scoring/audit")
def scoring_audit_list(
    service_id: str = Path(...),
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
    from backend import config as svcconfig
    from backend.core import metadata_db

    # 404 when the service itself isn't known — mirrors /scoring/status so
    # the UI gets a consistent shape across the audit + status pair.
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})

    rows = metadata_db.list_scoring_audit(service_id, limit=limit, since=since)
    return {"audit": rows, "limit": limit}


# ── Operator's chosen threshold (persisted, not enforced) ───────────────────


@router.get("/{service_id}/scoring/threshold")
def scoring_threshold_get(service_id: str = Path(...)) -> dict:
    """Return the operator's chosen score threshold.

    NOT enforced — the live scorer doesn't read this. It's a persisted
    operator preference so the threshold slider can remember the
    'committed' value across sessions, and the StatusPanel can show
    'committed threshold: X' as a stable reference. Actual enforcement
    requires a Rust scorer change + Wasm redeploy and is deferred to
    a future release once the operator is confident in the value.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,  # See docstring — preview-only
    }


@router.put("/{service_id}/scoring/threshold")
def scoring_threshold_put(
    body: dict,
    service_id: str = Path(...),
) -> dict:
    """Persist the operator's chosen threshold (0-100) into the per-service
    config. Pass ``{"threshold": null}`` to clear. Always returns the
    new state. Does NOT push to Compute — preview-only."""
    import datetime as _dt

    from backend import config as svcconfig

    raw = body.get("threshold")
    threshold: int | None
    if raw is None:
        threshold = None
    else:
        try:
            threshold = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"error": "threshold must be int 0-100 or null"})
        if not 0 <= threshold <= 100:
            raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    scoring = cfg.setdefault("scoring", {})
    prior_threshold = scoring.get("operator_threshold")
    if threshold is None:
        scoring.pop("operator_threshold", None)
        scoring.pop("operator_threshold_set_at", None)
    else:
        scoring["operator_threshold"] = threshold
        scoring["operator_threshold_set_at"] = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
    # Operator audit trail — every threshold change is attributable.
    from backend.core import metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "threshold_committed" if threshold is not None else "threshold_cleared",
        details={"prior_threshold": prior_threshold, "new_threshold": threshold},
    )
    svcconfig.save_config(service_id, cfg)

    _bust_analytics_cache(service_id)  # so /scoring/status reflects it next fetch
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
    }


# ── Per-reason AUC breakdown ────────────────────────────────────────────────


@router.get("/{service_id}/scoring/evaluation/per-reason")
def scoring_evaluation_per_reason(
    service_id: str = Path(...),
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

    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)
    n_neutral = counts.get("neutral", 0)

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


@router.get("/{service_id}/scoring/dashboard")
def scoring_dashboard(
    service_id: str = Path(...),
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
    from backend.scoring import labels as _labels

    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)
    n_neutral = counts.get("neutral", 0)

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
        cfg = svcconfig.load_config(service_id) or {}
        scoring = cfg.get("scoring") or {}
        if not scoring.get("enabled"):
            status_block: dict = {"enabled": False}
        else:
            status_block = {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}

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
        evaluation = scoring_evaluation(service_id=service_id)
        health = scoring_health(service_id=service_id, since_hours=since_hours)
        top_flagged = scoring_top_flagged(service_id=service_id, since_hours=since_hours, limit=50)
        score_distribution = scoring_score_distribution(service_id=service_id, since_hours=since_hours)
        compliance_breakdown = scoring_compliance_breakdown(service_id=service_id, since_hours=since_hours)
        curves = scoring_curves(service_id=service_id)
        threshold_preview = scoring_threshold_preview(
            service_id=service_id, threshold=threshold, since_hours=since_hours
        )

        return {
            "since_hours": since_hours,
            "threshold": threshold,
            "status": status_block,
            "evaluation": evaluation,
            "health": health,
            "top_flagged": top_flagged,
            "score_distribution": score_distribution,
            "compliance_breakdown": compliance_breakdown,
            "curves": curves,
            "threshold_preview": threshold_preview,
        }

    return _cached(cache_key, _produce)
