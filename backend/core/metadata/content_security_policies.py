"""Streaming → Content Security policies, rules and assignments (Postgres).

A **policy** (per service) holds rules: block / pace / exempt per value for six categories,
an optional country mass-block regex, and individual subscriber / IP blocks. It enforces
nothing until an **assignment** points at it: the service's one *default* assignment, or
one content ID for a begin/end window. Every change bumps the service ``rev`` in
``content_security_publish_state`` so the publisher can tell whether the blocker is behind.

Every mutation takes a per-service advisory lock for its transaction, so the
"one default" and "no overlapping windows per content ID" checks cannot race.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from backend.core.metadata.base import get_con
from backend.utils.cs_policy import (
    INDIVIDUAL_CATEGORIES,
    MAX_ASSIGNMENTS_PER_SERVICE,
    MAX_INDIVIDUAL_PER_SERVICE,
    MAX_POLICIES_PER_SERVICE,
    MAX_VALUE_LENGTH,
    RULE_CATEGORIES,
    PolicyValidationError,
    assignment_status,
    normalize_rule,
    normalize_rules,
    normalize_window,
    pick_assignment,
    validate_country_regex,
    windows_overlap,
)
from backend.utils.date_utils import iso_z_now


class PolicyNotFoundError(LookupError):
    """No such policy / assignment in this service."""


class PolicyConflictError(PolicyValidationError):
    """The change collides with existing state (HTTP 409)."""


def _lock(con, service_id: str) -> None:
    """Open a transaction and serialize policy writes for one service until it ends.

    The metadata pool runs ``autocommit=True`` and its ``commit()`` / ``rollback()`` are
    no-ops, so a multi-statement write opens its transaction with an explicit ``BEGIN`` and
    ends it with :func:`_commit` / :func:`_rollback` (the same shape ``cron_log`` uses).
    """
    con.execute("BEGIN")
    con.execute("SELECT pg_advisory_xact_lock(hashtext(?))", (service_id,))


def _commit(con) -> None:
    con.execute("COMMIT")


def _rollback(con) -> None:
    con.execute("ROLLBACK")


def _bump_rev(con, service_id: str) -> int:
    con.execute(
        "INSERT INTO content_security_publish_state (service_id, rev) VALUES (?, 1) "
        "ON CONFLICT (service_id) DO UPDATE SET rev = content_security_publish_state.rev + 1",
        (service_id,),
    )
    row = con.execute("SELECT rev FROM content_security_publish_state WHERE service_id = ?", (service_id,)).fetchone()
    return int(row["rev"])


# ── Policies ─────────────────────────────────────────────────────────────────

_POLICY_COLUMNS = "id, service_id, name, country_regex, rev, created_at, created_by, updated_at, updated_by"


def _rules_for(con, policy_id: str) -> list[dict]:
    rows = con.execute(
        "SELECT category, value, kind, kbps FROM content_security_policy_rules WHERE policy_id = ?", (policy_id,)
    ).fetchall()
    return [{"category": r["category"], "value": r["value"], "action": r["kind"], "kbps": int(r["kbps"])} for r in rows]


def _policy_doc(row, rules: list[dict]) -> dict:
    by_cat: dict[str, list[dict]] = {c: [] for c in RULE_CATEGORIES}
    individual: dict[str, list[str]] = {c: [] for c in INDIVIDUAL_CATEGORIES}
    for r in sorted(rules, key=lambda r: (r["category"], r["value"])):
        if r["category"] in individual:
            individual[r["category"]].append(r["value"])
        else:
            by_cat[r["category"]].append({"value": r["value"], "action": r["action"], "kbps": r["kbps"]})
    return {
        "id": row["id"],
        "service_id": row["service_id"],
        "name": row["name"],
        "country_regex": row["country_regex"],
        "rev": int(row["rev"]),
        "rules": by_cat,
        "individual": individual,
        "created_at": str(row["created_at"]),
        "created_by": row["created_by"],
        "updated_at": str(row["updated_at"]),
        "updated_by": row["updated_by"],
    }


def _load_policy(con, service_id: str, policy_id: str) -> dict | None:
    row = con.execute(
        f"SELECT {_POLICY_COLUMNS} FROM content_security_policies "
        "WHERE service_id = ? AND id = ? AND deleted_at IS NULL",
        (service_id, policy_id),
    ).fetchone()
    return _policy_doc(row, _rules_for(con, policy_id)) if row else None


def get_policy(service_id: str, policy_id: str) -> dict | None:
    return _load_policy(get_con(service_id), service_id, policy_id)


def list_policies(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        f"SELECT {_POLICY_COLUMNS} FROM content_security_policies WHERE service_id = ? AND deleted_at IS NULL "
        "ORDER BY name, id",
        (service_id,),
    ).fetchall()
    return [_policy_doc(r, _rules_for(con, r["id"])) for r in rows]


def _snapshot(con, doc: dict, actor: str) -> None:
    con.execute(
        "INSERT INTO content_security_policy_revisions (policy_id, rev, doc, changed_by, changed_at) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (policy_id, rev) DO NOTHING",
        (doc["id"], doc["rev"], json.dumps(doc, separators=(",", ":")), actor, iso_z_now()),
    )


def _individual_total(con, service_id: str) -> int:
    row = con.execute(
        "SELECT COUNT(*) AS n FROM content_security_policy_rules r "
        "JOIN content_security_policies p ON p.id = r.policy_id "
        "WHERE p.service_id = ? AND p.deleted_at IS NULL AND r.category IN ('subscriber_id', 'ip')",
        (service_id,),
    ).fetchone()
    return int(row["n"])


def _check_individual_cap(con, service_id: str) -> None:
    if _individual_total(con, service_id) > MAX_INDIVIDUAL_PER_SERVICE:
        _rollback(con)
        raise PolicyValidationError(
            "too_many_individual",
            f"At most {MAX_INDIVIDUAL_PER_SERVICE} individual subscriber / IP blocks per service",
        )


def _clean_name(name: str) -> str:
    n = name.strip()
    if not n or len(n) > 120:
        raise PolicyValidationError("invalid_name", "Name must be 1 to 120 characters")
    return n


def _name_taken(con, service_id: str, name: str, exclude_id: str | None) -> bool:
    row = con.execute(
        "SELECT id FROM content_security_policies WHERE service_id = ? AND name = ? AND deleted_at IS NULL AND id <> ?",
        (service_id, name, exclude_id or ""),
    ).fetchone()
    return row is not None


def _insert_rules(con, policy_id: str, rules: list[dict]) -> None:
    for r in rules:
        con.execute(
            "INSERT INTO content_security_policy_rules (policy_id, category, value, kind, kbps) VALUES (?, ?, ?, ?, ?)",
            (policy_id, r["category"], r["value"], r["action"], r["kbps"]),
        )


def create_policy(service_id: str, name: str, country_regex: str | None, rules: list[dict], actor: str) -> dict:
    """Create a policy. ``rules`` are raw ``{category, value, action, kbps?}`` dicts (validated here)."""
    name = _clean_name(name)
    regex = validate_country_regex(country_regex)
    norm = normalize_rules(rules)
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        count = con.execute(
            "SELECT COUNT(*) AS n FROM content_security_policies WHERE service_id = ? AND deleted_at IS NULL",
            (service_id,),
        ).fetchone()
        if int(count["n"]) >= MAX_POLICIES_PER_SERVICE:
            raise PolicyConflictError("too_many_policies", f"At most {MAX_POLICIES_PER_SERVICE} policies per service")
        if _name_taken(con, service_id, name, None):
            raise PolicyConflictError("name_taken", f"A policy named {name!r} already exists")
        policy_id = str(uuid.uuid4())
        now = iso_z_now()
        con.execute(
            "INSERT INTO content_security_policies "
            "(id, service_id, name, country_regex, rev, created_at, created_by, updated_at, updated_by) "
            "VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)",
            (policy_id, service_id, name, regex, now, actor, now, actor),
        )
        _insert_rules(con, policy_id, norm)
        _check_individual_cap(con, service_id)
        _bump_rev(con, service_id)
        doc = _load_policy(con, service_id, policy_id)
        assert doc is not None
        _snapshot(con, doc, actor)
        _commit(con)
        return doc
    except Exception:
        _rollback(con)
        raise


def _touch_policy(con, service_id: str, policy_id: str, actor: str, **fields) -> dict:
    """Bump the policy rev (and set ``fields``), bump the service rev, snapshot, return the doc."""
    sets = ", ".join(f"{k} = ?" for k in fields)
    con.execute(
        f"UPDATE content_security_policies SET rev = rev + 1, updated_at = ?, updated_by = ?"
        f"{', ' + sets if sets else ''} WHERE service_id = ? AND id = ?",
        (iso_z_now(), actor, *fields.values(), service_id, policy_id),
    )
    _check_individual_cap(con, service_id)
    _bump_rev(con, service_id)
    doc = _load_policy(con, service_id, policy_id)
    assert doc is not None
    _snapshot(con, doc, actor)
    _commit(con)
    return doc


def replace_policy(
    service_id: str, policy_id: str, name: str, country_regex: str | None, rules: list[dict], actor: str
) -> dict:
    """Replace a policy's name, regex and whole rule set (the editor's Save)."""
    name = _clean_name(name)
    regex = validate_country_regex(country_regex)
    norm = normalize_rules(rules)
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        if _load_policy(con, service_id, policy_id) is None:
            raise PolicyNotFoundError(policy_id)
        if _name_taken(con, service_id, name, policy_id):
            raise PolicyConflictError("name_taken", f"A policy named {name!r} already exists")
        con.execute("DELETE FROM content_security_policy_rules WHERE policy_id = ?", (policy_id,))
        _insert_rules(con, policy_id, norm)
        return _touch_policy(con, service_id, policy_id, actor, name=name, country_regex=regex)
    except Exception:
        _rollback(con)
        raise


def upsert_rule(service_id: str, policy_id: str, rule: dict, actor: str) -> dict:
    """Add or change one rule (the live-block path). A value has one action: re-adding replaces it."""
    norm = normalize_rule(rule["category"], rule["value"], rule["action"], rule.get("kbps"))
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        if _load_policy(con, service_id, policy_id) is None:
            raise PolicyNotFoundError(policy_id)
        con.execute(
            "DELETE FROM content_security_policy_rules WHERE policy_id = ? AND category = ? AND value = ?",
            (policy_id, norm["category"], norm["value"]),
        )
        if norm["category"] in RULE_CATEGORIES:
            per_cat = con.execute(
                "SELECT COUNT(*) AS n FROM content_security_policy_rules WHERE policy_id = ? AND category = ?",
                (policy_id, norm["category"]),
            ).fetchone()
            if int(per_cat["n"]) >= 500:
                raise PolicyValidationError("too_many_rules", f"At most 500 {norm['category']} rules per policy")
        _insert_rules(con, policy_id, [norm])
        return _touch_policy(con, service_id, policy_id, actor)
    except Exception:
        _rollback(con)
        raise


def remove_rule(service_id: str, policy_id: str, category: str, value: str, actor: str) -> dict:
    from backend.utils.cs_policy import normalize_value

    norm_value = normalize_value(category, value)
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        if _load_policy(con, service_id, policy_id) is None:
            raise PolicyNotFoundError(policy_id)
        gone = con.execute(
            "DELETE FROM content_security_policy_rules WHERE policy_id = ? AND category = ? AND value = ?",
            (policy_id, category, norm_value),
        )
        if gone.rowcount == 0:
            raise PolicyNotFoundError(f"{category} {norm_value}")
        return _touch_policy(con, service_id, policy_id, actor)
    except Exception:
        _rollback(con)
        raise


def delete_policy(service_id: str, policy_id: str, actor: str) -> dict:
    """Soft-delete a policy. Refused while any assignment still points at it."""
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        doc = _load_policy(con, service_id, policy_id)
        if doc is None:
            raise PolicyNotFoundError(policy_id)
        used = con.execute(
            "SELECT COUNT(*) AS n FROM content_security_assignments "
            "WHERE service_id = ? AND policy_id = ? AND deleted_at IS NULL",
            (service_id, policy_id),
        ).fetchone()
        if int(used["n"]):
            raise PolicyConflictError("policy_in_use", "Remove the policy's assignments first")
        con.execute(
            "UPDATE content_security_policies SET deleted_at = ?, updated_at = ?, updated_by = ? "
            "WHERE service_id = ? AND id = ?",
            (iso_z_now(), iso_z_now(), actor, service_id, policy_id),
        )
        _bump_rev(con, service_id)
        _commit(con)
        return doc
    except Exception:
        _rollback(con)
        raise


def list_revisions(service_id: str, policy_id: str, limit: int = 50) -> list[dict]:
    con = get_con(service_id)
    if _load_policy(con, service_id, policy_id) is None:
        raise PolicyNotFoundError(policy_id)
    rows = con.execute(
        "SELECT rev, doc, changed_by, changed_at FROM content_security_policy_revisions "
        "WHERE policy_id = ? ORDER BY rev DESC LIMIT ?",
        (policy_id, limit),
    ).fetchall()
    return [
        {
            "rev": int(r["rev"]),
            "changed_by": r["changed_by"],
            "changed_at": str(r["changed_at"]),
            "doc": json.loads(r["doc"]),
        }
        for r in rows
    ]


# ── Assignments ──────────────────────────────────────────────────────────────

_ASSIGNMENT_COLUMNS = (
    "id, service_id, policy_id, scope_type, content_id, mode, starts_at, ends_at, created_at, created_by"
)


def _assignment_row(r) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "policy_id": r["policy_id"],
        "scope_type": r["scope_type"],
        "content_id": r["content_id"],
        "mode": r["mode"],
        "starts_at": r["starts_at"],
        "ends_at": r["ends_at"],
        "created_at": str(r["created_at"]),
        "created_by": r["created_by"],
    }


def list_assignments(service_id: str) -> list[dict]:
    """Non-deleted assignments, default first then by content ID and start."""
    con = get_con(service_id)
    rows = con.execute(
        f"SELECT {_ASSIGNMENT_COLUMNS} FROM content_security_assignments WHERE service_id = ? AND deleted_at IS NULL "
        "ORDER BY CASE scope_type WHEN 'default' THEN 0 ELSE 1 END, content_id, starts_at, id",
        (service_id,),
    ).fetchall()
    return [_assignment_row(r) for r in rows]


def get_assignment(service_id: str, assignment_id: str) -> dict | None:
    row = (
        get_con(service_id)
        .execute(
            f"SELECT {_ASSIGNMENT_COLUMNS} FROM content_security_assignments "
            "WHERE service_id = ? AND id = ? AND deleted_at IS NULL",
            (service_id, assignment_id),
        )
        .fetchone()
    )
    return _assignment_row(row) if row else None


def _validate_assignment(
    con,
    service_id: str,
    exclude_id: str | None,
    policy_id: str,
    scope_type: str,
    content_id: str | None,
    mode: str,
    starts_at: str | None,
    ends_at: str | None,
) -> tuple[str | None, str | None, str | None]:
    if scope_type not in ("default", "content_id"):
        raise PolicyValidationError("invalid_scope", "Scope must be default or content_id")
    if mode not in ("WARN", "ENFORCE"):
        raise PolicyValidationError("invalid_mode", "Mode must be WARN or ENFORCE")
    if scope_type == "default":
        if content_id:
            raise PolicyValidationError("invalid_scope", "The default assignment has no content ID")
        content_id = None
    else:
        content_id = (content_id or "").strip()
        if not content_id or len(content_id) > MAX_VALUE_LENGTH:
            raise PolicyValidationError("invalid_content_id", f"Content ID must be 1 to {MAX_VALUE_LENGTH} characters")
    if _load_policy(con, service_id, policy_id) is None:
        raise PolicyNotFoundError(policy_id)
    start, end = normalize_window(starts_at, ends_at)
    existing = [a for a in _list_in(con, service_id) if a["id"] != exclude_id]
    if len(existing) >= MAX_ASSIGNMENTS_PER_SERVICE and exclude_id is None:
        raise PolicyConflictError(
            "too_many_assignments", f"At most {MAX_ASSIGNMENTS_PER_SERVICE} assignments per service"
        )
    if scope_type == "default":
        if any(a["scope_type"] == "default" for a in existing):
            raise PolicyConflictError("default_exists", "This service already has a default assignment")
    else:
        for a in existing:
            if (
                a["scope_type"] == "content_id"
                and a["content_id"] == content_id
                and windows_overlap((a["starts_at"], a["ends_at"]), (start, end))
            ):
                raise PolicyConflictError(
                    "assignment_overlap",
                    f"{content_id} already has an assignment that overlaps this window",
                )
    return content_id, start, end


def _list_in(con, service_id: str) -> list[dict]:
    rows = con.execute(
        f"SELECT {_ASSIGNMENT_COLUMNS} FROM content_security_assignments WHERE service_id = ? AND deleted_at IS NULL",
        (service_id,),
    ).fetchall()
    return [_assignment_row(r) for r in rows]


def create_assignment(
    service_id: str,
    policy_id: str,
    scope_type: str,
    content_id: str | None,
    mode: str,
    starts_at: str | None,
    ends_at: str | None,
    actor: str,
) -> dict:
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        content_id, start, end = _validate_assignment(
            con, service_id, None, policy_id, scope_type, content_id, mode, starts_at, ends_at
        )
        assignment_id = str(uuid.uuid4())
        con.execute(
            "INSERT INTO content_security_assignments "
            "(id, service_id, policy_id, scope_type, content_id, mode, starts_at, ends_at, created_at, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (assignment_id, service_id, policy_id, scope_type, content_id, mode, start, end, iso_z_now(), actor),
        )
        _bump_rev(con, service_id)
        _commit(con)
    except Exception:
        _rollback(con)
        raise
    result = get_assignment(service_id, assignment_id)
    assert result is not None
    return result


def update_assignment(
    service_id: str,
    assignment_id: str,
    policy_id: str,
    scope_type: str,
    content_id: str | None,
    mode: str,
    starts_at: str | None,
    ends_at: str | None,
) -> dict:
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        if get_assignment(service_id, assignment_id) is None:
            raise PolicyNotFoundError(assignment_id)
        content_id, start, end = _validate_assignment(
            con, service_id, assignment_id, policy_id, scope_type, content_id, mode, starts_at, ends_at
        )
        con.execute(
            "UPDATE content_security_assignments SET policy_id = ?, scope_type = ?, content_id = ?, mode = ?, "
            "starts_at = ?, ends_at = ? WHERE service_id = ? AND id = ?",
            (policy_id, scope_type, content_id, mode, start, end, service_id, assignment_id),
        )
        _bump_rev(con, service_id)
        _commit(con)
    except Exception:
        _rollback(con)
        raise
    result = get_assignment(service_id, assignment_id)
    assert result is not None
    return result


def delete_assignment(service_id: str, assignment_id: str) -> dict:
    con = get_con(service_id)
    _lock(con, service_id)
    try:
        existing = get_assignment(service_id, assignment_id)
        if existing is None:
            raise PolicyNotFoundError(assignment_id)
        con.execute(
            "UPDATE content_security_assignments SET deleted_at = ? WHERE service_id = ? AND id = ?",
            (iso_z_now(), service_id, assignment_id),
        )
        _bump_rev(con, service_id)
        _commit(con)
        return existing
    except Exception:
        _rollback(con)
        raise


def effective_assignment(service_id: str, content_id: str | None, now: datetime | None = None) -> dict | None:
    """The assignment in effect for ``content_id`` right now (D2), or ``None``."""
    return pick_assignment(list_assignments(service_id), content_id, now or datetime.now(UTC))


def assignment_with_status(a: dict, now: datetime | None = None) -> dict:
    return {**a, "status": assignment_status(a["starts_at"], a["ends_at"], now or datetime.now(UTC))}


# ── Publish state ────────────────────────────────────────────────────────────


def get_publish_state(service_id: str) -> dict:
    row = (
        get_con(service_id)
        .execute(
            "SELECT rev, published_rev, last_ok_at, last_error "
            "FROM content_security_publish_state WHERE service_id = ?",
            (service_id,),
        )
        .fetchone()
    )
    if row is None:
        return {"rev": 0, "published_rev": 0, "last_ok_at": None, "last_error": None}
    return {
        "rev": int(row["rev"]),
        "published_rev": int(row["published_rev"]),
        "last_ok_at": row["last_ok_at"],
        "last_error": row["last_error"],
    }


def mark_published(service_id: str, rev: int) -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO content_security_publish_state (service_id, rev, published_rev, last_ok_at, last_error) "
        "VALUES (?, ?, ?, ?, NULL) ON CONFLICT (service_id) DO UPDATE SET "
        "published_rev = GREATEST(content_security_publish_state.published_rev, EXCLUDED.published_rev), "
        "last_ok_at = EXCLUDED.last_ok_at, last_error = NULL",
        (service_id, rev, rev, iso_z_now()),
    )
    con.commit()


def mark_publish_failed(service_id: str, error: str) -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO content_security_publish_state (service_id, last_error) VALUES (?, ?) "
        "ON CONFLICT (service_id) DO UPDATE SET last_error = EXCLUDED.last_error",
        (service_id, error[:500]),
    )
    con.commit()
