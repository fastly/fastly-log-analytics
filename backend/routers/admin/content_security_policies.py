"""Streaming → Content Security policies and assignments (admin-only, per service).

A policy is defined ahead of time and enforces nothing until an assignment points at it:
the service's one default assignment, or one content ID for a begin/end window. Edits are
committed first and pushed to the blocker second (``publish`` in every reply says whether the
blocker accepted them), so a failed push leaves the change saved and "not yet live" rather than lost.
``/admin/*`` is blocked for analysts by ``remote_access`` and every route also needs ``require_admin``:
rules hold subscriber IDs and IP addresses.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import NoReturn

from fastapi import Depends, HTTPException, Query

from backend import config as svcconfig
from backend.core import content_security_publish as publisher
from backend.core.duckdb import enrich_asn_labels
from backend.core.metadata import content_security_policies as db
from backend.core.metadata.state import record_audit
from backend.deps import get_source, require_admin
from backend.models.content_security_policies import (
    AssignmentBody,
    AssignmentResult,
    AssignmentsResponse,
    ContentSecurityPolicy,
    ContentSecuritySyncStatusResponse,
    EffectivePolicyResponse,
    LiveRuleBody,
    LiveRuleResult,
    PoliciesResponse,
    PolicyBody,
    PolicyResult,
    PublishResult,
    RegexPreviewResponse,
    RevisionsResponse,
    RuleBody,
)
from backend.utils.cs_policy import (
    ISO_COUNTRY_CODES,
    PolicyValidationError,
    assignment_status,
    regex_matches,
    validate_country_regex,
)
from backend.utils.router_utils import make_error

from ._router import router

logger = logging.getLogger(__name__)

_ACTOR = "admin"
_BASE = "/admin/content-security"


def _service_id(source: dict) -> str:
    return source.get("service_id") or source.get("name", "")


def _require_read_write_source(source: dict) -> None:
    if source.get("access_level", "read_write") != "read_write":
        raise HTTPException(status_code=403, detail=make_error("admin_only", "This operation requires admin access."))


def _fail(exc: Exception) -> NoReturn:
    """Map a policy-layer error to its HTTP response."""
    if isinstance(exc, db.PolicyNotFoundError):
        raise HTTPException(status_code=404, detail=make_error("not_found", "Not found")) from exc
    if isinstance(exc, db.PolicyConflictError):
        raise HTTPException(status_code=409, detail=make_error(exc.code, exc.message)) from exc
    if isinstance(exc, PolicyValidationError):
        raise HTTPException(status_code=422, detail=make_error(exc.code, exc.message)) from exc
    raise exc


def _configured() -> bool:
    return bool(svcconfig.load_content_security_config().get("blocklist_url"))


def _body_rules(body: PolicyBody) -> list[dict]:
    rules = [
        {"category": category, "value": r["value"], "action": r["action"], "kbps": r["kbps"]}
        for category, items in body.rules.model_dump().items()
        for r in items
    ]
    for category, values in body.individual.model_dump().items():
        rules.extend({"category": category, "value": v, "action": "block", "kbps": 0} for v in values)
    return rules


def _uses(service_id: str) -> tuple[dict[str, list[dict]], list[dict]]:
    now = datetime.now(UTC)
    assignments = db.list_assignments(service_id)
    by_policy: dict[str, list[dict]] = {}
    for a in assignments:
        by_policy.setdefault(a["policy_id"], []).append(
            {
                "assignment_id": a["id"],
                "scope_type": a["scope_type"],
                "content_id": a["content_id"],
                "mode": a["mode"],
                "status": assignment_status(a["starts_at"], a["ends_at"], now),
            }
        )
    return by_policy, assignments


def _label_asn_rules(service_id: str, docs: list[dict]) -> None:
    """Add the owner name to every ASN rule (``label``), in place. Best effort: a failed lookup leaves no label."""
    rows = [r for d in docs for r in d["rules"].get("asn", [])]
    if not rows:
        return
    try:
        enrich_asn_labels(rows, service_id)
    except Exception:
        logger.warning("ASN name lookup failed", exc_info=True)


def _policy_out(
    service_id: str, doc: dict, uses: dict[str, list[dict]] | None = None, *, labelled: bool = False
) -> dict:
    if uses is None:
        uses = _uses(service_id)[0]
    if not labelled:
        _label_asn_rules(service_id, [doc])
    return {**doc, "used_by": uses.get(doc["id"], [])}


def _assignment_out(service_id: str, a: dict, names: dict[str, str] | None = None) -> dict:
    if names is None:
        names = {p["id"]: p["name"] for p in db.list_policies(service_id)}
    return {
        **a,
        "policy_name": names.get(a["policy_id"]),
        "status": assignment_status(a["starts_at"], a["ends_at"], datetime.now(UTC)),
    }


def _publish(service_id: str) -> dict:
    return publisher.publish_service(service_id)


def _audit(service_id: str, event: str, details: dict, publish: dict) -> None:
    record_audit(service_id, event, {**details, "publish": publish["status"], "rev": publish["rev"]}, actor=_ACTOR)


# ── Policies ─────────────────────────────────────────────────────────────────


@router.get(f"{_BASE}/policies", response_model=PoliciesResponse, response_model_exclude_unset=True)
def list_policies(source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    uses = _uses(sid)[0]
    docs = db.list_policies(sid)
    _label_asn_rules(sid, docs)
    return {"configured": _configured(), "policies": [_policy_out(sid, p, uses, labelled=True) for p in docs]}


@router.post(f"{_BASE}/policies", response_model=PolicyResult, response_model_exclude_unset=True, status_code=201)
def create_policy(body: PolicyBody, source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        doc = db.create_policy(sid, body.name, body.country_regex, _body_rules(body), _ACTOR)
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(sid, "content_security_policy_created", {"policy_id": doc["id"], "name": doc["name"]}, publish)
    return {"policy": _policy_out(sid, doc), "publish": publish}


@router.get(f"{_BASE}/policies/{{policy_id}}", response_model=ContentSecurityPolicy, response_model_exclude_unset=True)
def get_policy(policy_id: str, source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    doc = db.get_policy(sid, policy_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=make_error("not_found", "Policy not found"))
    return _policy_out(sid, doc)


@router.put(f"{_BASE}/policies/{{policy_id}}", response_model=PolicyResult, response_model_exclude_unset=True)
def replace_policy(
    policy_id: str, body: PolicyBody, source: dict = Depends(get_source), _admin: None = Depends(require_admin)
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        doc = db.replace_policy(sid, policy_id, body.name, body.country_regex, _body_rules(body), _ACTOR)
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_policy_updated",
        {"policy_id": policy_id, "name": doc["name"], "policy_rev": doc["rev"]},
        publish,
    )
    return {"policy": _policy_out(sid, doc), "publish": publish}


@router.delete(f"{_BASE}/policies/{{policy_id}}", response_model=PolicyResult, response_model_exclude_unset=True)
def delete_policy(policy_id: str, source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        doc = db.delete_policy(sid, policy_id, _ACTOR)
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(sid, "content_security_policy_deleted", {"policy_id": policy_id, "name": doc["name"]}, publish)
    return {"policy": {**doc, "used_by": []}, "publish": publish}


@router.post(f"{_BASE}/policies/{{policy_id}}/rules", response_model=PolicyResult, response_model_exclude_unset=True)
def upsert_rule(
    policy_id: str, body: RuleBody, source: dict = Depends(get_source), _admin: None = Depends(require_admin)
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        doc = db.upsert_rule(sid, policy_id, body.model_dump(), _ACTOR)
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_rule_set",
        {
            "policy_id": policy_id,
            "category": body.category,
            "value": body.value,
            "action": body.action,
            "kbps": body.kbps,
        },
        publish,
    )
    return {"policy": _policy_out(sid, doc), "publish": publish}


@router.delete(f"{_BASE}/policies/{{policy_id}}/rules", response_model=PolicyResult, response_model_exclude_unset=True)
def remove_rule(
    policy_id: str,
    category: str = Query(...),
    value: str = Query(..., min_length=1, max_length=2048),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        doc = db.remove_rule(sid, policy_id, category, value, _ACTOR)
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid, "content_security_rule_removed", {"policy_id": policy_id, "category": category, "value": value}, publish
    )
    return {"policy": _policy_out(sid, doc), "publish": publish}


@router.get(
    f"{_BASE}/policies/{{policy_id}}/revisions", response_model=RevisionsResponse, response_model_exclude_unset=True
)
def policy_revisions(policy_id: str, source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        revisions = db.list_revisions(sid, policy_id)
    except db.PolicyNotFoundError as exc:
        _fail(exc)
    return {
        "revisions": [
            {
                "rev": r["rev"],
                "changed_by": r["changed_by"],
                "changed_at": r["changed_at"],
                "policy": {**r["doc"], "used_by": []},
            }
            for r in revisions
        ]
    }


# ── Assignments ──────────────────────────────────────────────────────────────


@router.get(f"{_BASE}/assignments", response_model=AssignmentsResponse, response_model_exclude_unset=True)
def list_assignments(source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    names = {p["id"]: p["name"] for p in db.list_policies(sid)}
    return {
        "configured": _configured(),
        "assignments": [_assignment_out(sid, a, names) for a in db.list_assignments(sid)],
    }


@router.post(
    f"{_BASE}/assignments", response_model=AssignmentResult, response_model_exclude_unset=True, status_code=201
)
def create_assignment(
    body: AssignmentBody, source: dict = Depends(get_source), _admin: None = Depends(require_admin)
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        a = db.create_assignment(
            sid, body.policy_id, body.scope_type, body.content_id, body.mode, body.starts_at, body.ends_at, _ACTOR
        )
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_assignment_created",
        {k: a[k] for k in ("id", "policy_id", "scope_type", "content_id", "mode", "starts_at", "ends_at")},
        publish,
    )
    return {"assignment": _assignment_out(sid, a), "publish": publish}


@router.put(
    f"{_BASE}/assignments/{{assignment_id}}", response_model=AssignmentResult, response_model_exclude_unset=True
)
def update_assignment(
    assignment_id: str,
    body: AssignmentBody,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        a = db.update_assignment(
            sid,
            assignment_id,
            body.policy_id,
            body.scope_type,
            body.content_id,
            body.mode,
            body.starts_at,
            body.ends_at,
        )
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_assignment_updated",
        {k: a[k] for k in ("id", "policy_id", "scope_type", "content_id", "mode", "starts_at", "ends_at")},
        publish,
    )
    return {"assignment": _assignment_out(sid, a), "publish": publish}


@router.delete(
    f"{_BASE}/assignments/{{assignment_id}}", response_model=AssignmentResult, response_model_exclude_unset=True
)
def delete_assignment(
    assignment_id: str, source: dict = Depends(get_source), _admin: None = Depends(require_admin)
) -> dict:
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        a = db.delete_assignment(sid, assignment_id)
    except db.PolicyNotFoundError as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_assignment_deleted",
        {"id": assignment_id, "scope_type": a["scope_type"], "content_id": a["content_id"]},
        publish,
    )
    return {"assignment": _assignment_out(sid, a), "publish": publish}


# ── Effective policy and live edits ──────────────────────────────────────────


@router.get(f"{_BASE}/effective", response_model=EffectivePolicyResponse, response_model_exclude_unset=True)
def effective_policy(
    content_id: str | None = Query(None, max_length=512),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    """The policy in effect now for ``content_id`` (its own assignment, else the default)."""
    _require_read_write_source(source)
    sid = _service_id(source)
    a = db.effective_assignment(sid, content_id or None)
    out: dict = {
        "content_id": content_id or None,
        "assignment": None,
        "policy": None,
        "shared_with": 0,
        "configured": _configured(),
    }
    if a is None:
        return out
    doc = db.get_policy(sid, a["policy_id"])
    if doc is None:
        return out
    uses = _uses(sid)[0]
    out["assignment"] = _assignment_out(sid, a, {doc["id"]: doc["name"]})
    out["policy"] = _policy_out(sid, doc, uses)
    out["shared_with"] = max(len(uses.get(doc["id"], [])) - 1, 0)
    return out


@router.post(f"{_BASE}/live-rule", response_model=LiveRuleResult, response_model_exclude_unset=True)
def live_rule(body: LiveRuleBody, source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    """Add a rule to the policy in effect for the content being viewed (creates a Default if none)."""
    _require_read_write_source(source)
    sid = _service_id(source)
    try:
        result = publisher.apply_live_rule(
            sid,
            body.content_id,
            {"category": body.category, "value": body.value, "action": body.action, "kbps": body.kbps},
            _ACTOR,
        )
    except (PolicyValidationError, db.PolicyNotFoundError) as exc:
        _fail(exc)
    publish = _publish(sid)
    _audit(
        sid,
        "content_security_live_rule",
        {
            "policy_id": result["policy"]["id"],
            "assignment_id": result["assignment"]["id"],
            "created_default": result["created_default"],
            "category": body.category,
            "value": body.value,
            "action": body.action,
            "kbps": body.kbps,
        },
        publish,
    )
    return {
        "policy": _policy_out(sid, result["policy"]),
        "assignment": _assignment_out(sid, result["assignment"]),
        "created_default": result["created_default"],
        "publish": publish,
    }


# ── Publishing and helpers ───────────────────────────────────────────────────


@router.post(f"{_BASE}/publish", response_model=PublishResult, response_model_exclude_unset=True)
def publish_now(source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    """Push the service's current policies and assignments to the blocker (retry / Sync now)."""
    _require_read_write_source(source)
    return _publish(_service_id(source))


@router.get(f"{_BASE}/sync-status", response_model=ContentSecuritySyncStatusResponse, response_model_exclude_unset=True)
def sync_status(source: dict = Depends(get_source), _admin: None = Depends(require_admin)) -> dict:
    _require_read_write_source(source)
    return publisher.sync_status(_service_id(source))


@router.get(f"{_BASE}/country-regex/preview", response_model=RegexPreviewResponse, response_model_exclude_unset=True)
def country_regex_preview(pattern: str = Query("", max_length=200), _admin: None = Depends(require_admin)) -> dict:
    """Which countries a mass-block regex would block, for the editor's live preview."""
    try:
        valid = validate_country_regex(pattern)
    except PolicyValidationError as exc:
        return {"valid": False, "error": exc.message, "matches": []}
    return {"valid": True, "error": None, "matches": regex_matches(valid, list(ISO_COUNTRY_CODES)) if valid else []}
