"""Global remote-share store.

Holds remote-analyst invitations, service scopes, audit logs, share settings,
persisted analyst sessions, one-time claim tokens, and TOS versions in the
PostgreSQL database.

Every public symbol is exposed on ``backend.core.share_db``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.metadata.pg_connection import (
    close_all_pg_connections,
    get_pg_thread_connection,
)
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


# ── Connection layer ────────────────────────────────────────────────────────


def get_global_share_con() -> Any:
    """Return a thread-local connection to the global share DB (PostgreSQL)."""
    return get_pg_thread_connection()


def close_all_connections() -> None:
    """Close every open connection in the pool."""
    close_all_pg_connections()


# ── Schema definitions ──────────────────────────────────────────────────────

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS remote_invites (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        passcode TEXT NOT NULL,
        expires_at TEXT,
        ip_whitelist TEXT,
        pii_policy TEXT NOT NULL DEFAULT '{"mask_ips": false}',
        query_window_hours INTEGER,
        query_start_time TEXT,
        query_end_time TEXT,
        created_at TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0,
        tos_accepted_at TEXT,
        tos_version TEXT,
        allow_concurrent_sessions INTEGER NOT NULL DEFAULT 0,
        auth_method TEXT NOT NULL DEFAULT 'passcode',
        oauth_provider TEXT,
        oauth_subject TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_invites_email ON remote_invites(email)",
    """CREATE TABLE IF NOT EXISTS invite_services (
        invite_id TEXT NOT NULL,
        service_id TEXT NOT NULL,
        PRIMARY KEY (invite_id, service_id),
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    "CREATE INDEX IF NOT EXISTS idx_invite_services_invite_id ON invite_services(invite_id)",
    """CREATE TABLE IF NOT EXISTS remote_share_audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL,
        email TEXT,
        ip_address TEXT NOT NULL,
        details TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_share_audit_logs_timestamp ON remote_share_audit_logs(timestamp)",
    """CREATE TABLE IF NOT EXISTS share_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS remote_sessions (
        session_id TEXT PRIMARY KEY,
        invite_id TEXT NOT NULL,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        ip_address TEXT NOT NULL,
        user_agent TEXT NOT NULL,
        fingerprint_signature TEXT NOT NULL,
        pii_policy TEXT NOT NULL,
        query_window_hours INTEGER,
        query_start_time TEXT,
        query_end_time TEXT,
        login_time TEXT NOT NULL,
        last_active_time TEXT NOT NULL,
        last_activity TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    """CREATE TABLE IF NOT EXISTS remote_invite_claim_tokens (
        token TEXT PRIMARY KEY,
        invite_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        claimed_at TEXT,
        claimed_from_ip TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    """CREATE TABLE IF NOT EXISTS share_tos_versions (
        version TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        published_at TEXT NOT NULL
    )""",
]

LATEST_VERSION = 4


# ── Terms-of-service version reads/writes ───────────────────────────────────


def get_latest_tos(*, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute(
        "SELECT version, text, published_at FROM share_tos_versions ORDER BY published_at DESC, version DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def publish_tos_version(version: str, text: str, *, con: Any = None) -> None:
    """Insert a new TOS row. Idempotent on (version): re-publishing the
    same version is a no-op so callers can run this from migrations or
    admin paths without guarding for duplicates."""
    con = con or get_global_share_con()
    row = con.execute("SELECT 1 FROM share_tos_versions WHERE version=?", (version,)).fetchone()
    if row is not None:
        return
    con.execute(
        "INSERT INTO share_tos_versions(version, text, published_at) VALUES(?, ?, ?)",
        (version, text, iso_z_now()),
    )
    con.commit()


# ── share_settings KV accessors ─────────────────────────────────────────────

MAX_CONCURRENT_ANALYST_SESSIONS_KEY = "max_concurrent_analyst_sessions"
SHARE_AUDIT_RETENTION_DAYS_KEY = "share_audit_retention_days"


def get_setting(key: str, default: str | None = None, *, con: Any = None) -> str | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT value FROM share_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        "INSERT INTO share_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    con.commit()


def get_max_concurrent_sessions(*, con: Any = None) -> int:
    raw = get_setting(MAX_CONCURRENT_ANALYST_SESSIONS_KEY, "10", con=con)
    try:
        return max(1, int(raw or "10"))
    except (TypeError, ValueError):
        return 10


# ── Persisted analyst session CRUD ──────────────────────────────────────────


def upsert_session(session: dict, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        """INSERT INTO remote_sessions(
            session_id, invite_id, name, email, ip_address, user_agent,
            fingerprint_signature, pii_policy, query_window_hours,
            query_start_time, query_end_time, login_time, last_active_time, last_activity)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(session_id) DO UPDATE SET
            ip_address=excluded.ip_address,
            user_agent=excluded.user_agent,
            last_active_time=excluded.last_active_time,
            last_activity=excluded.last_activity""",
        (
            session["session_id"],
            session["invite_id"],
            session["name"],
            session["email"],
            session["ip_address"],
            session["user_agent"],
            session["fingerprint_signature"],
            json.dumps(session.get("pii_policy") or {}, separators=(",", ":")),
            session.get("query_window_hours"),
            session.get("query_start_time"),
            session.get("query_end_time"),
            session["login_time"],
            session["last_active_time"],
            session.get("last_activity"),
        ),
    )
    con.commit()


def delete_session(session_id: str, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    con.execute("DELETE FROM remote_sessions WHERE session_id=?", (session_id,))
    con.commit()


def get_session(session_id: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_sessions WHERE session_id=?", (session_id,)).fetchone()
    if row is None:
        return None
    rec = dict(row)
    rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
    return rec


def get_all_sessions(*, con: Any = None) -> list[dict]:
    con = con or get_global_share_con()
    rows = con.execute("SELECT * FROM remote_sessions").fetchall()
    out: list[dict] = []
    for r in rows:
        rec = dict(r)
        rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
        out.append(rec)
    return out


# ── Audit log writes + filtered reads ───────────────────────────────────────


def log_share_audit_event(
    *,
    event_type: str,
    email: str | None,
    ip_address: str,
    details: str,
    con: Any = None,
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
    con: Any = None,
) -> list[dict]:
    """Return audit log rows ordered newest-first."""
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


_LOGIN_SUCCESS_EVENTS = ("LOGIN_SUCCESS", "LOGIN_SUCCESS_OAUTH")


def get_last_login_by_email(*, con: Any = None) -> dict[str, str]:
    """Per-analyst most-recent successful-login timestamp, keyed by lowercased email."""
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


def purge_old_audit_logs(retention_days: int = 90, *, con: Any = None) -> int:
    """Delete audit rows older than the retention window. Returns row count."""
    con = con or get_global_share_con()
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(retention_days)))
    cur = con.execute("DELETE FROM remote_share_audit_logs WHERE timestamp < ?", (cutoff,))
    con.commit()
    return cur.rowcount or 0


def purge_stale_share_records(
    max_idle_session_days: int = 30,
    *,
    con: Any = None,
) -> dict[str, int]:
    """Purge expired claim tokens, expired invites, and stale sessions."""
    con = con or get_global_share_con()
    now_iso = iso_z_now()
    session_cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(max_idle_session_days)))

    cur = con.execute("DELETE FROM remote_invite_claim_tokens WHERE expires_at < ?", (now_iso,))
    deleted_tokens = cur.rowcount or 0

    cur = con.execute("DELETE FROM remote_invites WHERE expires_at IS NOT NULL AND expires_at < ?", (now_iso,))
    deleted_invites = cur.rowcount or 0

    cur = con.execute("DELETE FROM remote_sessions WHERE last_active_time < ?", (session_cutoff,))
    deleted_sessions = cur.rowcount or 0

    con.commit()

    return {
        "deleted_claim_tokens": deleted_tokens,
        "deleted_expired_invites": deleted_invites,
        "deleted_stale_sessions": deleted_sessions,
    }


def purge_all_share_records(
    audit_retention_days: int = 90,
    max_idle_session_days: int = 30,
    *,
    con: Any = None,
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


# ── Invites, Passcode & Validation re-exports ───────────────────────────────

from backend.core.share_db import invites as _invites_mod  # noqa: E402
from backend.core.share_db.invites import (  # noqa: E402
    _ARGON2_HASHER,
    _BREACHED_TOP_LIST,
    _HASHER,
    WeakPasscodeError,
    _equalize_passcode_timing,
    bind_invite_oauth_subject,
    claim_token,
    create_claim_token,
    create_remote_invite,
    delete_remote_invite,
    export_backup,
    gdpr_erase,
    generate_wordphrase,
    get_remote_invite,
    get_remote_invite_by_email_passcode,
    get_remote_invite_oauth,
    get_remote_invite_services,
    get_remote_invites,
    hash_passcode,
    import_backup,
    mark_tos_accepted,
    needs_rehash,
    reactivate_remote_invite,
    revoke_remote_invite,
    set_invite_concurrent_sessions,
    update_remote_invite_passcode,
    update_remote_invite_pii,
    update_remote_invite_services,
    validate_passcode_strength,
    verify_passcode,
)
from backend.core.share_db.validation import (  # noqa: E402
    InvalidEmailError,
    InvalidNameError,
    InvalidPiiPolicyError,
    apply_pii_policy,
    ip_in_whitelist,
    mask_ip,
    parse_ip_whitelist,
    validate_email,
    validate_name,
    validate_pii_policy,
)

# Back-compat submodule aliases for callers referencing folded submodules.
_self = sys.modules[__name__]
for _sub in ("connection", "schema", "tos", "settings", "sessions", "audit"):
    setattr(_self, _sub, _self)
    sys.modules[f"{__name__}.{_sub}"] = _self
setattr(_self, "passcode", _invites_mod)
sys.modules[f"{__name__}.passcode"] = _invites_mod

__all__ = [
    # Date helpers (legacy re-export).
    "iso_z",
    "iso_z_now",
    # Connection layer.
    "get_global_share_con",
    "close_all_connections",
    # Schema + migrations.
    "_SCHEMA",
    "LATEST_VERSION",
    # Passcode.
    "_HASHER",
    "_ARGON2_HASHER",
    "_BREACHED_TOP_LIST",
    "hash_passcode",
    "verify_passcode",
    "needs_rehash",
    "validate_passcode_strength",
    "WeakPasscodeError",
    "generate_wordphrase",
    "_equalize_passcode_timing",
    # Validation.
    "validate_name",
    "validate_email",
    "validate_pii_policy",
    "parse_ip_whitelist",
    "ip_in_whitelist",
    "InvalidNameError",
    "InvalidEmailError",
    "InvalidPiiPolicyError",
    "mask_ip",
    "apply_pii_policy",
    # Invites + claim tokens + backup + GDPR.
    "create_remote_invite",
    "get_remote_invite",
    "get_remote_invite_services",
    "get_remote_invites",
    "get_remote_invite_by_email_passcode",
    "get_remote_invite_oauth",
    "bind_invite_oauth_subject",
    "update_remote_invite_services",
    "update_remote_invite_passcode",
    "update_remote_invite_pii",
    "set_invite_concurrent_sessions",
    "revoke_remote_invite",
    "reactivate_remote_invite",
    "delete_remote_invite",
    "mark_tos_accepted",
    "create_claim_token",
    "claim_token",
    "export_backup",
    "import_backup",
    "gdpr_erase",
    # Sessions.
    "upsert_session",
    "delete_session",
    "get_session",
    "get_all_sessions",
    # Audit.
    "log_share_audit_event",
    "get_share_audit_logs",
    "get_last_login_by_email",
    "purge_old_audit_logs",
    "purge_all_share_records",
    "purge_stale_share_records",
    # TOS.
    "get_latest_tos",
    "publish_tos_version",
    # Settings.
    "get_setting",
    "set_setting",
    "get_max_concurrent_sessions",
    "MAX_CONCURRENT_ANALYST_SESSIONS_KEY",
    "SHARE_AUDIT_RETENTION_DAYS_KEY",
]
