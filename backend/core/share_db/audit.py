"""Audit log writes + filtered reads for the global share DB.

Append-only by design: ``purge_old_audit_logs`` is the only deletion path
and it's gated on a retention window (default 90 days) driven by the
``share_audit_retention_days`` setting.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, timedelta

from backend.core.share_db.connection import get_global_share_con
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def log_share_audit_event(
    *,
    event_type: str,
    email: str | None,
    ip_address: str,
    details: str,
    con: sqlite3.Connection | None = None,
) -> None:
    con = con or get_global_share_con()
    con.execute(
        """INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details)
           VALUES (?, ?, ?, ?, ?)""",
        (iso_z_now(), event_type, email, ip_address or "0.0.0.0", details),
    )
    con.commit()


def get_share_audit_logs(
    limit: int = 200,
    *,
    event_type: str | None = None,
    email_substr: str | None = None,
    since: str | None = None,
    until: str | None = None,
    con: sqlite3.Connection | None = None,
) -> list[dict]:
    """Return audit log rows ordered newest-first.

    Optional filters compose with AND. ``since`` / ``until`` are ISO-Z strings
    compared lexicographically (the column is stored as ``iso_z_now()`` text,
    which is monotonic enough for prefix/range comparison without parsing).
    """
    con = con or get_global_share_con()
    clauses: list[str] = []
    params: list = []
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if email_substr:
        clauses.append("email LIKE ?")
        params.append(f"%{email_substr}%")
    if since:
        clauses.append("timestamp >= ?")
        params.append(since)
    if until:
        clauses.append("timestamp <= ?")
        params.append(until)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM remote_share_audit_logs{where} ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    rows = con.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


# Audit event types that mean "an analyst successfully signed in" — the two
# login paths (passcode + OIDC/OAuth). Kept as one tuple so the last-login
# derivation and any future login-analytics stay in sync.
_LOGIN_SUCCESS_EVENTS = ("LOGIN_SUCCESS", "LOGIN_SUCCESS_OAUTH")


def get_last_login_by_email(*, con: sqlite3.Connection | None = None) -> dict[str, str]:
    """Per-analyst most-recent successful-login timestamp, keyed by lowercased email.

    Derived from successful-login audit events (``LOGIN_SUCCESS`` +
    ``LOGIN_SUCCESS_OAUTH``) rather than a dedicated column, so it works
    retroactively and needs no schema migration. Bounded by the audit
    retention window (default 90 days) — which is exactly the "who is
    actually using the app lately" horizon an admin cares about.

    Returns ``{email_lc: last_login_at_iso_z}``. One GROUP BY over the audit
    table (indexed by timestamp; the table is small). ``timestamp`` is stored as
    monotonic ``iso_z_now()`` text, so ``MAX`` gives the most recent login
    without parsing.
    """
    con = con or get_global_share_con()
    placeholders = ",".join("?" * len(_LOGIN_SUCCESS_EVENTS))
    rows = con.execute(
        f"""SELECT lower(email) AS email_lc, MAX(timestamp) AS last_login_at
              FROM remote_share_audit_logs
             WHERE event_type IN ({placeholders}) AND email IS NOT NULL
             GROUP BY lower(email)""",
        _LOGIN_SUCCESS_EVENTS,
    ).fetchall()
    return {r["email_lc"]: r["last_login_at"] for r in rows if r["email_lc"]}


def purge_old_audit_logs(retention_days: int = 90, *, con: sqlite3.Connection | None = None) -> int:
    """Delete audit rows older than the retention window. Returns row count."""
    con = con or get_global_share_con()
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(retention_days)))
    cur = con.execute("DELETE FROM remote_share_audit_logs WHERE timestamp < ?", (cutoff,))
    con.commit()
    return cur.rowcount or 0


def purge_stale_share_records(
    max_idle_session_days: int = 30,
    *,
    con: sqlite3.Connection | None = None,
) -> dict[str, int]:
    """Purge expired claim tokens, expired invites, and stale sessions.

    Also executes PRAGMA wal_checkpoint(TRUNCATE).
    """
    con = con or get_global_share_con()
    now_iso = iso_z_now()
    session_cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(max_idle_session_days)))

    # 1. Purge expired claim tokens
    cur = con.execute("DELETE FROM remote_invite_claim_tokens WHERE expires_at < ?", (now_iso,))
    deleted_tokens = cur.rowcount or 0

    # 2. Purge expired invites (cascades to invite_services, remote_sessions, and claim tokens via FK)
    cur = con.execute("DELETE FROM remote_invites WHERE expires_at IS NOT NULL AND expires_at < ?", (now_iso,))
    deleted_invites = cur.rowcount or 0

    # 3. Purge stale sessions (> max_idle_session_days idle)
    cur = con.execute("DELETE FROM remote_sessions WHERE last_active_time < ?", (session_cutoff,))
    deleted_sessions = cur.rowcount or 0

    con.commit()

    # 4. Checkpoint WAL
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as e:
        logger.warning("[share_db] wal_checkpoint failed: %s", e)

    return {
        "deleted_claim_tokens": deleted_tokens,
        "deleted_expired_invites": deleted_invites,
        "deleted_stale_sessions": deleted_sessions,
    }


def purge_all_share_records(
    audit_retention_days: int = 90,
    max_idle_session_days: int = 30,
    *,
    con: sqlite3.Connection | None = None,
) -> dict[str, int]:
    """Purge old audit logs, expired invites, stale sessions, and expired tokens."""
    con = con or get_global_share_con()
    deleted_audit = purge_old_audit_logs(retention_days=audit_retention_days, con=con)
    stale_res = purge_stale_share_records(max_idle_session_days=max_idle_session_days, con=con)
    return {
        "deleted_audit_logs": deleted_audit,
        "deleted_expired_invites": stale_res["deleted_expired_invites"],
        "deleted_stale_sessions": stale_res["deleted_stale_sessions"],
        "deleted_claim_tokens": stale_res["deleted_claim_tokens"],
        "audit_retention_days": audit_retention_days,
        "max_idle_session_days": max_idle_session_days,
    }

