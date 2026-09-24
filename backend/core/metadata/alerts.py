"""Alert-rule CRUD against ``alerts`` table in PostgreSQL metadata store."""

from __future__ import annotations

import json
from typing import Any

from backend.core.metadata.base import get_con

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
        "SELECT service_id FROM alerts WHERE id = ?",
        (alert_id,),
    )
    row = cur.fetchone()
    con.execute(
        "UPDATE alerts SET enabled = ? WHERE id = ?",
        (1 if enabled else 0, alert_id),
    )
    con.commit()
    return {"id": alert_id, "status": "success", "service_id": row["service_id"] if row else None}


def delete_alert(service_id: str, alert_id: str) -> dict:
    con = get_con(service_id)
    cur = con.execute("SELECT service_id FROM alerts WHERE id = ?", (alert_id,))
    row = cur.fetchone()
    con.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    con.commit()
    return {"status": "success", "service_id": row["service_id"] if row else None}


def update_alert_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    con = get_con(service_id)
    if triggered_ts:
        con.execute(
            "UPDATE alerts SET last_triggered_at = ? WHERE id = ?",
            (triggered_ts, alert_id),
        )
    else:
        con.execute(
            "UPDATE alerts SET last_triggered_at = datetime('now') WHERE id = ?",
            (alert_id,),
        )
    con.commit()
