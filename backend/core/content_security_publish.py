"""Content Security policies: compile a service's state, push it to the blocker, migrate old blocks.

The blocker (``PUT {base}/v2/services/{service_id}/policy-set``) takes the whole service's
active state in one idempotent call, so a retry or a reconcile can never leave it half applied.
A change is committed first and published second: if the push fails, ``published_rev`` stays
behind ``rev`` and the UI shows the change as "not yet live" until a retry or reconcile lands.
See ``local-docs/content-security-policies-contract.md``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from backend import config as svcconfig
from backend.core.metadata import content_security_policies as db
from backend.core.metadata.state import record_audit
from backend.utils.blocklist_client import BlocklistCallError, call_policy_set
from backend.utils.cs_policy import (
    INDIVIDUAL_CATEGORIES,
    RULE_CATEGORIES,
    assignment_status,
)

logger = logging.getLogger(__name__)

DEFAULT_POLICY_NAME = "Default"


def compile_policy(doc: dict) -> dict:
    """A policy as the blocker takes it (``kbps`` only on pace rules)."""
    rules: dict[str, list[dict]] = {}
    for category in RULE_CATEGORIES:
        out = []
        for r in doc["rules"].get(category, []):
            item = {"value": r["value"], "action": r["action"]}
            if r["action"] == "pace":
                item["kbps"] = r["kbps"]
            out.append(item)
        rules[category] = out
    return {
        "id": doc["id"],
        "name": doc["name"],
        "country_regex": doc["country_regex"],
        "rules": rules,
        "individual": {c: list(doc["individual"].get(c, [])) for c in INDIVIDUAL_CATEGORIES},
    }


def compile_policy_set(service_id: str, now: datetime | None = None) -> tuple[dict, int]:
    """The service's publishable state and the ``rev`` it corresponds to.

    Ended assignments are left out (the edge would never pick them), and only policies an
    included assignment points at are sent.
    """
    now = now or datetime.now(UTC)
    rev = db.get_publish_state(service_id)["rev"]
    assignments = [
        a for a in db.list_assignments(service_id) if assignment_status(a["starts_at"], a["ends_at"], now) != "ended"
    ]
    policies = {}
    for a in assignments:
        if a["policy_id"] not in policies:
            doc = db.get_policy(service_id, a["policy_id"])
            if doc is not None:
                policies[a["policy_id"]] = compile_policy(doc)
    body = {
        "rev": rev,
        "assignments": [
            {
                "id": a["id"],
                "scope": a["scope_type"],
                "content_id": a["content_id"],
                "policy_id": a["policy_id"],
                "mode": a["mode"],
                "starts_at": a["starts_at"],
                "ends_at": a["ends_at"],
            }
            for a in assignments
            if a["policy_id"] in policies
        ],
        "policies": policies,
    }
    return body, rev


def publish_service(service_id: str) -> dict:
    """Push the service's current state. Never raises: returns ``{status, rev, error}``.

    ``status`` is ``published``, ``not_configured`` (no blocker URL set) or ``failed``.
    """
    cfg = svcconfig.load_content_security_config()
    url = cfg.get("blocklist_url") or ""
    body, rev = compile_policy_set(service_id)
    if not url:
        return {"status": "not_configured", "rev": rev, "error": None}
    try:
        call_policy_set(url, service_id, "PUT", body, cfg.get("blocklist_auth_header") or None)
    except BlocklistCallError as exc:
        db.mark_publish_failed(service_id, str(exc))
        record_audit(service_id, "content_security_publish_failed", {"rev": rev, "error": str(exc)}, actor="admin")
        return {"status": "failed", "rev": rev, "error": str(exc)}
    db.mark_published(service_id, rev)
    return {"status": "published", "rev": rev, "error": None}


def sync_status(service_id: str) -> dict:
    """Where the blocker stands relative to the saved state."""
    state = db.get_publish_state(service_id)
    configured = bool(svcconfig.load_content_security_config().get("blocklist_url"))
    return {
        "configured": configured,
        "rev": state["rev"],
        "published_rev": state["published_rev"],
        "in_sync": configured and state["published_rev"] >= state["rev"],
        "last_ok_at": state["last_ok_at"],
        "last_error": state["last_error"],
    }


# ── Live edits ───────────────────────────────────────────────────────────────


def apply_live_rule(service_id: str, content_id: str | None, rule: dict, actor: str) -> dict:
    """Add a rule to the policy in effect for ``content_id`` (D4: create the default if none).

    Returns ``{policy, assignment, created_default}``; the caller publishes. Raises
    :class:`PolicyConflictError` when a default assignment exists but is not in effect (its
    window has not started or has ended), since editing its policy would change nothing.
    """
    assignment = db.effective_assignment(service_id, content_id)
    created = False
    if assignment is None:
        if any(a["scope_type"] == "default" for a in db.list_assignments(service_id)):
            raise db.PolicyConflictError(
                "default_not_active",
                "The default assignment is not in effect right now. Adjust its dates, or assign a policy to this content.",
            )
        policy = next((p for p in db.list_policies(service_id) if p["name"] == DEFAULT_POLICY_NAME), None)
        if policy is None:
            policy = db.create_policy(service_id, DEFAULT_POLICY_NAME, None, [], actor)
        assignment = db.create_assignment(service_id, policy["id"], "default", None, "ENFORCE", None, None, actor)
        created = True
    policy = db.upsert_rule(service_id, assignment["policy_id"], rule, actor)
    return {"policy": policy, "assignment": assignment, "created_default": created}
