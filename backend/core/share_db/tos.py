"""Terms-of-service version reads for the share-flow acknowledgment gate."""

from __future__ import annotations

import sqlite3

from backend.core.share_db.connection import get_global_share_con


def get_latest_tos(*, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute(
        "SELECT version, text, published_at FROM share_tos_versions ORDER BY published_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None
