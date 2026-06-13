"""Terms-of-service version reads/writes for the share-flow acknowledgment gate."""

from __future__ import annotations

import sqlite3

from backend.core.share_db.connection import get_global_share_con
from backend.utils.date_utils import iso_z_now


def get_latest_tos(*, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    # rowid DESC breaks ties for rows published in the same second
    # (iso_z_now() is second-resolution).
    row = con.execute(
        "SELECT version, text, published_at FROM share_tos_versions ORDER BY published_at DESC, rowid DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def publish_tos_version(version: str, text: str, *, con: sqlite3.Connection | None = None) -> None:
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
