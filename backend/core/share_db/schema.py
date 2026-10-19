"""Share DB schema + forward-only migrations framework.

A private ``MIGRATIONS`` dict (key = integer version, value = callable) is
applied via ``apply_pending(con)`` on first open. Uses ``PRAGMA
user_version`` on this file (the per-service framework's user_version
lives in the per-service files, so namespaces never collide).

The ``_init_db`` entry point creates the latest schema snapshot from
``_SCHEMA`` then runs ``apply_pending`` — both idempotent so re-running on
an already-initialized DB is a no-op.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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


def _has_column(con: Any, table: str, col: str) -> bool:
    try:
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
        return col in cols
    except Exception:
        return False


def _migration_002_seed_initial_tos(con: Any) -> None:
    """Seed the initial TOS text used by the acknowledgment gate."""
    row = con.execute("SELECT 1 FROM share_tos_versions WHERE version=?", ("v1",)).fetchone()
    if row is None:
        from backend.utils.date_utils import iso_z_now

        con.execute(
            "INSERT INTO share_tos_versions(version, text, published_at) VALUES(?, ?, ?)",
            (
                "v1",
                (
                    "I acknowledge that I am viewing third-party operational log data, "
                    "that my access is logged, and that I will not retain, redistribute, "
                    "or use this data outside the scope of my engagement."
                ),
                iso_z_now(),
            ),
        )


def _migration_003_add_allow_concurrent_sessions(con: Any) -> None:
    """Add ``remote_invites.allow_concurrent_sessions``."""
    if _has_column(con, "remote_invites", "allow_concurrent_sessions"):
        return
    con.execute("ALTER TABLE remote_invites ADD COLUMN allow_concurrent_sessions INTEGER NOT NULL DEFAULT 0")


def _migration_004_add_oauth_columns(con: Any) -> None:
    """Add the OAuth/OIDC invite columns."""
    if not _has_column(con, "remote_invites", "auth_method"):
        con.execute("ALTER TABLE remote_invites ADD COLUMN auth_method TEXT NOT NULL DEFAULT 'passcode'")
    if not _has_column(con, "remote_invites", "oauth_provider"):
        con.execute("ALTER TABLE remote_invites ADD COLUMN oauth_provider TEXT")
    if not _has_column(con, "remote_invites", "oauth_subject"):
        con.execute("ALTER TABLE remote_invites ADD COLUMN oauth_subject TEXT")


MIGRATIONS: dict[int, Any] = {
    2: _migration_002_seed_initial_tos,
    3: _migration_003_add_allow_concurrent_sessions,
    4: _migration_004_add_oauth_columns,
}
LATEST_VERSION = 4


def get_current_version(con: Any = None) -> int:
    """Return latest schema version (Postgres standard schema is always at latest)."""
    return LATEST_VERSION


def apply_pending(con: Any = None) -> int:
    """No-op under Postgres — schema is managed by pg_schema.py."""
    return 0


def _init_db(con: Any = None) -> None:
    """No-op under Postgres — schema is managed by pg_schema.py."""
    pass
