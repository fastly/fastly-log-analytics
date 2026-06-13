"""``share_settings`` KV accessors used by the admin + scheduler paths.

Today's known keys: ``max_concurrent_analyst_sessions`` (seeded by
migration 001), ``share_audit_retention_days`` (read by the audit-log
purge cron), and ``passcode_default_algo`` (set by migration 003 to
``argon2id``).
"""

from __future__ import annotations

import sqlite3

from backend.core.share_db.connection import get_global_share_con

# Known share_settings keys. Constants instead of magic strings so callers
# (admin payloads, scheduler crons, migrations) all reference the same name.
MAX_CONCURRENT_ANALYST_SESSIONS_KEY = "max_concurrent_analyst_sessions"
SHARE_AUDIT_RETENTION_DAYS_KEY = "share_audit_retention_days"
PASSCODE_DEFAULT_ALGO_KEY = "passcode_default_algo"


def get_setting(key: str, default: str | None = None, *, con: sqlite3.Connection | None = None) -> str | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT value FROM share_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        "INSERT INTO share_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    con.commit()


def get_max_concurrent_sessions(*, con: sqlite3.Connection | None = None) -> int:
    raw = get_setting(MAX_CONCURRENT_ANALYST_SESSIONS_KEY, "10", con=con)
    try:
        return max(1, int(raw or "10"))
    except (TypeError, ValueError):
        return 10
