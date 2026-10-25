"""Audit log + applied-data-migration tracking against the metadata Postgres store.

The audit log is mirrored across hosts via state_sync (export/import); the
applied-data-migration table is local-only because each host runs its own
migration sweep on boot.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.metadata.base import get_con, get_con_readonly
from backend.utils.date_utils import iso_z, iso_z_now

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
    per_page = max(1, min(per_page, 100))
    offset = max(0, page - 1) * per_page

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
    src_names = list({r.get("source_name") for r in rows if r.get("source_name") is not None})
    has_null_src = any(r.get("source_name") is None for r in rows)
    if src_names and not has_null_src:
        placeholders = ", ".join("?" for _ in src_names)
        existing_rows = con.execute(
            f"SELECT timestamp, source_name, event_type, actor FROM audit_logs WHERE source_name IN ({placeholders})",
            src_names,
        ).fetchall()
    else:
        existing_rows = con.execute(
            "SELECT timestamp, source_name, event_type, actor FROM audit_logs",
        ).fetchall()
    seen: set[tuple] = {
        (
            str(er["timestamp"]) if er["timestamp"] is not None else None,
            er["source_name"],
            er["event_type"],
            er["actor"],
        )
        for er in existing_rows
    }
    to_insert: list[tuple] = []
    for r in rows:
        ts = r.get("timestamp")
        ts_str = str(ts) if ts is not None else None
        src_name = r.get("source_name")
        ev_type = r.get("event_type")
        actor = r.get("actor")
        key = (ts_str, src_name, ev_type, actor)
        if key in seen:
            continue
        seen.add(key)
        to_insert.append((ts, src_name, ev_type, r.get("details"), actor))
    if to_insert:
        con.executemany(
            "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)",
            to_insert,
        )
    con.commit()


# ── asn_names cache ──────────────────────────────────────────────────────────


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    import contextlib

    fresh_cutoff = iso_z(datetime.now(UTC) - timedelta(days=max_age_days))
    placeholders = ",".join("?" * len(asns))
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            f"SELECT asn, name FROM asn_names WHERE asn IN ({placeholders}) AND fetched_at >= ?",
            list(asns) + [fresh_cutoff],
        ).fetchall()
    return {int(r["asn"]): r["name"] for r in rows}


def upsert_asn_names(service_id: str, mapping: dict[int, str]) -> None:
    if not mapping:
        return
    con = get_con(service_id)
    now = iso_z_now()
    con.executemany(
        "INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(asn) DO UPDATE SET name = excluded.name, fetched_at = excluded.fetched_at",
        [(int(asn), name, now) for asn, name in mapping.items()],
    )
    con.commit()


def asn_ints_for_search(service_id: str, name_ilike: str) -> list[int]:
    """Return ASN integers whose cached name matches the given LIKE pattern.

    Used by the dashboard ASN search to pre-fetch matching ASNs and inline them
    into a DuckDB IN clause (avoids cross-engine JOINs).
    """
    import contextlib

    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
            (name_ilike,),
        ).fetchall()
    return [int(r["asn"]) for r in rows]


# ── Saved-dashboard-view CRUD ────────────────────────────────────────────────


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "service_id": r["service_id"],
            "name": r["name"],
            "filters_json": r["filters_json"],
            "time_range_type": r["time_range_type"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "page": r["page"],
            "created_at": str(r["created_at"]) if r["created_at"] is not None else "",
        }
        for r in rows
    ]


def save_view(service_id: str, view) -> dict:
    import uuid

    con = get_con(service_id)
    view_id = view.id or str(uuid.uuid4())
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            view_id,
            service_id,
            view.name,
            view.filters_json,
            view.time_range_type,
            view.start_time,
            view.end_time,
            view.page,
        ),
    )
    con.commit()
    return {"id": view_id, "status": "success"}


def delete_view(service_id: str, view_id: str) -> dict:
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE id = ? AND service_id = ?", (view_id, service_id))
    con.commit()
    return {"status": "success"}


def _view_row_tuple(v: dict) -> tuple:
    return (
        v.get("id"),
        v.get("service_id"),
        v.get("name"),
        v.get("filters_json"),
        v.get("time_range_type"),
        v.get("start_time"),
        v.get("end_time"),
        v.get("page"),
        v.get("created_at"),
    )


def replace_views_for_service(service_id: str, views: list[dict]) -> None:
    """Replace all saved views for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE service_id = ?", (service_id,))
    if views:
        con.executemany(
            "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [_view_row_tuple(v) for v in views],
        )
    con.commit()


def upsert_views_for_service(service_id: str, views: list[dict]) -> None:
    """Upsert saved views by id WITHOUT deleting local-only rows.

    Used by state_sync.import_admin_state on read_only analyst hosts so
    locally-created views (which the analyst created on their own pod) are
    preserved through every metadata_sync cron tick. Without this, the
    cron's wholesale DELETE+INSERT silently wiped any analyst-side view
    that hadn't been mirrored back to FOS — and ``export_admin_state``
    refuses to push from read_only hosts, so the loss was permanent.
    """
    if not views:
        return
    con = get_con(service_id)
    con.executemany(
        "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "name=excluded.name, filters_json=excluded.filters_json, "
        "time_range_type=excluded.time_range_type, start_time=excluded.start_time, "
        "end_time=excluded.end_time, page=excluded.page, created_at=excluded.created_at",
        [_view_row_tuple(v) for v in views],
    )
    con.commit()


# ── Alert-rule CRUD ──────────────────────────────────────────────────────────

_ALERT_COLUMNS = (
    "id, service_id, name, category, metric, evaluation_type, operator, threshold, "
    "window_min, comparison_period_min, status_codes, webhook_url, enabled, "
    "last_triggered_at, created_at, evaluation_scope, channels_json, zscore_threshold, baseline_period_days"
)


def _row_to_alert(r: Any) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "name": r["name"],
        "category": r["category"],
        "metric": r["metric"],
        "evaluation_type": r["evaluation_type"],
        "operator": r["operator"],
        "threshold": r["threshold"],
        "window_min": r["window_min"],
        "comparison_period_min": r["comparison_period_min"],
        "status_codes": json.loads(r["status_codes"]) if r["status_codes"] else None,
        "webhook_url": r["webhook_url"],
        "enabled": bool(r["enabled"]),
        "last_triggered_at": r["last_triggered_at"],
        "created_at": r["created_at"],
        "evaluation_scope": r["evaluation_scope"] or "all",
        "channels": json.loads(r["channels_json"]) if r["channels_json"] else [],
        "zscore_threshold": r["zscore_threshold"],
        "baseline_period_days": r["baseline_period_days"],
    }


def list_alerts(service_id: str, filter_service_id: str | None = None) -> list[dict]:
    """Return all alerts, optionally filtered by service_id."""
    con = get_con(service_id)
    where = "WHERE service_id = ? " if filter_service_id else ""
    params: list = [filter_service_id] if filter_service_id else []
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts {where}ORDER BY created_at DESC",
        params,
    ).fetchall()

    return [_row_to_alert(r) for r in rows]


def list_alerts_cross_service(service_ids: list[str], limit: int = 500) -> list[dict]:
    """Return the globally-newest ``limit`` alerts across all given services,
    sorted by ``created_at DESC``.
    """
    if not service_ids:
        return []

    con = get_con(service_ids[0])
    placeholders = ", ".join("?" for _ in service_ids)
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts WHERE service_id IN ({placeholders}) ORDER BY created_at DESC LIMIT ?",
        [*service_ids, limit],
    ).fetchall()
    return [_row_to_alert(r) for r in rows]


def count_alerts(service_id: str) -> int:
    """Return total number of alerts (enabled + disabled) for a service.

    Used by the scheduler to gate the alerts evaluation cron: when zero, the
    cron is not registered at all so we don't waste a tick per ``log_period``
    producing "skipped — no alerts configured" entries in cron_runs.
    """
    con = get_con(service_id)
    row = con.execute("SELECT count(*) AS n FROM alerts WHERE service_id = ?", (service_id,)).fetchone()
    return int(row["n"]) if row else 0


def save_alert(service_id: str, alert) -> dict:
    """Insert or update an alert. Returns {id, status}."""
    import uuid

    con = get_con(service_id)
    alert_id = alert.id or str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO alerts (id, service_id, name, category, metric, evaluation_type,
            operator, threshold, window_min, comparison_period_min, status_codes,
            webhook_url, enabled, evaluation_scope, channels_json, zscore_threshold, baseline_period_days)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            service_id = excluded.service_id,
            name = excluded.name,
            category = excluded.category,
            metric = excluded.metric,
            evaluation_type = excluded.evaluation_type,
            operator = excluded.operator,
            threshold = excluded.threshold,
            window_min = excluded.window_min,
            comparison_period_min = excluded.comparison_period_min,
            status_codes = excluded.status_codes,
            webhook_url = excluded.webhook_url,
            enabled = excluded.enabled,
            evaluation_scope = excluded.evaluation_scope,
            channels_json = excluded.channels_json,
            zscore_threshold = excluded.zscore_threshold,
            baseline_period_days = excluded.baseline_period_days
        """,
        (
            alert_id,
            service_id,
            alert.name,
            alert.category,
            alert.metric,
            alert.evaluation_type,
            alert.operator,
            alert.threshold,
            alert.window_min,
            alert.comparison_period_min,
            json.dumps(alert.status_codes) if alert.status_codes else None,
            alert.webhook_url,
            1 if alert.enabled else 0,
            alert.evaluation_scope,
            json.dumps([c if isinstance(c, dict) else c.model_dump() for c in alert.channels])
            if alert.channels
            else "[]",
            alert.zscore_threshold,
            alert.baseline_period_days,
        ),
    )
    con.commit()
    return {"id": alert_id, "status": "success"}


def toggle_alert(service_id: str, alert_id: str, enabled: bool) -> dict:
    con = get_con(service_id)
    cur = con.execute(
        "SELECT service_id FROM alerts WHERE id = ? AND service_id = ?",
        (alert_id, service_id),
    )
    row = cur.fetchone()
    con.execute(
        "UPDATE alerts SET enabled = ? WHERE id = ? AND service_id = ?",
        (1 if enabled else 0, alert_id, service_id),
    )
    con.commit()
    return {"id": alert_id, "status": "success", "service_id": row["service_id"] if row else None}


def delete_alert(service_id: str, alert_id: str) -> dict:
    con = get_con(service_id)
    cur = con.execute("SELECT service_id FROM alerts WHERE id = ? AND service_id = ?", (alert_id, service_id))
    row = cur.fetchone()
    con.execute("DELETE FROM alerts WHERE id = ? AND service_id = ?", (alert_id, service_id))
    con.commit()
    return {"status": "success", "service_id": row["service_id"] if row else None}


def update_alert_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    con = get_con(service_id)
    if triggered_ts:
        con.execute(
            "UPDATE alerts SET last_triggered_at = ? WHERE id = ? AND service_id = ?",
            (triggered_ts, alert_id, service_id),
        )
    else:
        con.execute(
            "UPDATE alerts SET last_triggered_at = datetime('now') WHERE id = ? AND service_id = ?",
            (alert_id, service_id),
        )
    con.commit()
