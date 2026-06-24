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
import sqlite3
from collections.abc import Callable

from backend.utils.date_utils import iso_z_now

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
        tos_version TEXT
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


def _migration_002_seed_initial_tos(con: sqlite3.Connection) -> None:
    """Seed the initial TOS text used by the acknowledgment gate."""
    row = con.execute("SELECT 1 FROM share_tos_versions WHERE version=?", ("v1",)).fetchone()
    if row is None:
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


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    2: _migration_002_seed_initial_tos,
}

LATEST_VERSION = max(MIGRATIONS) if MIGRATIONS else 0


# Single-source: get_current_version is identical across all SQLite
# migration runners — import from sqlite_migrations rather than duplicate.
from backend.core.sqlite_migrations import get_current_version  # noqa: E402,F401


def apply_pending(con: sqlite3.Connection) -> int:
    """Apply every share-DB migration past ``user_version``.

    Delegates to :func:`backend.core.sqlite_migrations.run_pending_migrations`
    — same forward-only framework the per-service metadata DBs use, just
    with this module's ``MIGRATIONS`` registry and the ``share_db`` log
    prefix so messages stay distinguishable in the log stream.
    """
    from backend.core.sqlite_migrations import run_pending_migrations

    return run_pending_migrations(con, MIGRATIONS, log_prefix="share_db")


def _init_db(con: sqlite3.Connection) -> None:
    """Create schema from the latest snapshot, then apply migrations forward.

    Idempotent: ``CREATE ... IF NOT EXISTS`` on every statement plus
    ``apply_pending`` which is itself idempotent.
    """
    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
    apply_pending(con)

    # If the connection was rebuilt by ``get_safe_share_db_connection`` after
    # quarantining a corrupt file, write a single recovery audit row.
    # Local import to break the schema <-> audit/connection cycle.
    from backend.core.share_db.audit import log_share_audit_event
    from backend.core.share_db.connection import _recovery_marker

    corrupt_from = _recovery_marker.pop(id(con), None)
    if corrupt_from:
        log_share_audit_event(
            event_type="SHARE_DB_RECOVERED",
            email=None,
            ip_address="127.0.0.1",
            details=f"previous file quarantined to {corrupt_from}",
            con=con,
        )
