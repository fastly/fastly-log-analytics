"""Audit log + applied-data-migration tracking against the metadata SQLite store.

The audit log is mirrored across hosts via state_sync (export/import); the
applied-data-migration table is local-only because each host runs its own
migration sweep on boot.
"""

from __future__ import annotations

import json
import logging

from backend.core.metadata.base import get_con

logger = logging.getLogger(__name__)

# ── audit_logs ────────────────────────────────────────────────────────────────


def record_audit(service_id: str, event_type: str, details: dict, actor: str = "ui") -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO audit_logs (source_name, event_type, details, actor) VALUES (?, ?, ?, ?)",
        (service_id, event_type, json.dumps(details), actor),
    )
    con.commit()


def list_audit(service_id: str, limit: int = 200, since: str | None = None) -> list[dict]:
    """List audit log entries for a service, most recent first."""
    con = get_con(service_id)
    if since:
        rows = con.execute(
            "SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
            "WHERE source_name = ? AND timestamp >= ? ORDER BY timestamp DESC LIMIT ?",
            (service_id, since, limit),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
            "WHERE source_name = ? ORDER BY timestamp DESC LIMIT ?",
            (service_id, limit),
        ).fetchall()
    return [
        {
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "source_name": r["source_name"],
            "event_type": r["event_type"],
            "details": r["details"],
            "actor": r["actor"],
        }
        for r in rows
    ]


def get_audit_logs(
    service_id: str,
    *,
    event_type: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "timestamp",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated audit log query with optional event_type filter."""
    con = get_con(service_id)
    where = ["source_name = ?"]
    params: list = [service_id]
    if event_type and event_type != "all":
        where.append("event_type = ?")
        params.append(event_type)
    where_sql = "WHERE " + " AND ".join(where)

    total = int(con.execute(f"SELECT count(*) FROM audit_logs {where_sql}", params).fetchone()[0])

    valid_sort_cols = {"timestamp", "event_type", "actor"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "timestamp"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, timestamp, event_type, details, actor
            FROM audit_logs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [
        {
            "id": r["id"],
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "event_type": r["event_type"],
            "details": json.loads(r["details"] or "{}"),
            "actor": r["actor"],
            "source": "local",
        }
        for r in rows
    ]
    return total, entries


def export_audit(service_id: str, limit: int = 200) -> list[dict]:
    """Used by state_sync.export_admin_state — same as list_audit but with a stable column shape."""
    return list_audit(service_id, limit=limit)


def replace_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Replace all audit logs for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM audit_logs WHERE source_name = ?", (service_id,))
    if rows:
        con.executemany(
            "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r.get("timestamp"),
                    r.get("source_name"),
                    r.get("event_type"),
                    r.get("details"),
                    r.get("actor"),
                )
                for r in rows
            ],
        )
    con.commit()


def merge_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Insert audit log entries from remote without deleting local ones.

    Used by state_sync.import_admin_state on read_only analyst hosts to
    preserve local audit entries created by the analyst's own actions
    (which the wholesale ``replace_audit_for_service`` would have wiped on
    every cron tick).

    Dedup key: (timestamp, source_name, event_type, actor) — a row with
    those four fields equal to an existing row is considered the same
    event and skipped. ``timestamp`` has second precision so collisions
    between distinct events are improbable, and even if they happen the
    audit log tolerates the missed insert.
    """
    if not rows:
        return
    con = get_con(service_id)
    for r in rows:
        existing = con.execute(
            "SELECT 1 FROM audit_logs WHERE source_name = ? AND timestamp = ? AND event_type = ? AND actor = ? LIMIT 1",
            (r.get("source_name"), r.get("timestamp"), r.get("event_type"), r.get("actor")),
        ).fetchone()
        if existing:
            continue
        con.execute(
            "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)",
            (r.get("timestamp"), r.get("source_name"), r.get("event_type"), r.get("details"), r.get("actor")),
        )
    con.commit()
