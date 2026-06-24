"""Persisted analyst session CRUD for the share flow.

Rows live in ``remote_sessions`` and are rehydrated by
``backend.utils.tunnel.manager.TunnelManager.start`` on app startup. The
``pii_policy`` column is JSON-serialised here so callers see a dict in/out.
"""

from __future__ import annotations

import json
import sqlite3

from backend.core.share_db.connection import get_global_share_con


def upsert_session(session: dict, *, con: sqlite3.Connection | None = None) -> None:
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


def delete_session(session_id: str, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    con.execute("DELETE FROM remote_sessions WHERE session_id=?", (session_id,))
    con.commit()


def get_session(session_id: str, *, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_sessions WHERE session_id=?", (session_id,)).fetchone()
    if row is None:
        return None
    rec = dict(row)
    rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
    return rec


def get_all_sessions(*, con: sqlite3.Connection | None = None) -> list[dict]:
    con = con or get_global_share_con()
    rows = con.execute("SELECT * FROM remote_sessions").fetchall()
    out: list[dict] = []
    for r in rows:
        rec = dict(r)
        rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
        out.append(rec)
    return out
