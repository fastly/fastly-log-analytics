"""Postgres alert, saved view, and cron run log wrappers."""

from __future__ import annotations

import sys as _sys
from typing import Any

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.models.common import Alert, SavedView

evaluate_alert: Any = None


def get_alerts(service_id: str | None = None, *, limit: int = 500) -> list[dict]:
    """Return alerts for a service or globally-newest across all services."""
    if service_id:
        return metadata_db.list_alerts(service_id, filter_service_id=service_id)
    service_ids: list[str] = [sid for cfg in svcconfig.list_configs() if (sid := cfg.get("service_id"))]
    return metadata_db.list_alerts_cross_service(service_ids, limit=limit)


def save_alert(alert: Alert) -> dict:
    return metadata_db.save_alert(alert.service_id, alert)


def get_alert_by_id(alert_id: str, service_id: str) -> dict | None:
    """Return the alert row whose id matches ``alert_id`` in the given service (or None)."""
    for a in metadata_db.list_alerts(service_id, filter_service_id=service_id):
        if a.get("id") == alert_id:
            return a
    return None


def toggle_alert(alert_id: str, enabled: bool, service_id: str) -> dict:
    """Toggle an alert. ``service_id`` is required."""
    return metadata_db.toggle_alert(service_id, alert_id, enabled)


def delete_alert(alert_id: str, service_id: str) -> dict:
    """Delete an alert. ``service_id`` is required."""
    return metadata_db.delete_alert(service_id, alert_id)


def update_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    metadata_db.update_alert_last_triggered(service_id, alert_id, triggered_ts)


def get_cron_logs(
    service_id: str,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
    *,
    skip_total: bool = False,
) -> tuple[int, list[dict]]:
    return metadata_db.get_cron_runs(
        service_id,
        task=task,
        status=status,
        page=page,
        per_page=per_page,
        sort_col=sort_col,
        sort_dir=sort_dir,
        since_id=since_id,
        with_total=since_id is None and not skip_total,
    )


def delete_cron_log(service_id: str, log_id: int) -> None:
    metadata_db.delete_cron_run(service_id, log_id)


def purge_cron_logs(service_id: str, task: str | None = None, days: int | None = None) -> None:
    metadata_db.purge_cron_runs(service_id, task=task, days=days)


def get_views(service_id: str) -> list[dict]:
    return metadata_db.list_views(service_id)


def save_view(view: SavedView) -> dict:
    return metadata_db.save_view(view.service_id, view)


def get_view_by_id(view_id: str, service_id: str) -> dict | None:
    """Return the saved-view row whose id matches ``view_id`` in the given service (or None)."""
    for v in metadata_db.list_views(service_id):
        if v.get("id") == view_id:
            out = dict(v)
            out.setdefault("service_id", service_id)
            return out
    return None


def delete_view(view_id: str, service_id: str) -> dict:
    """Delete the saved-view row in the given service (or report not_found)."""
    res = metadata_db.delete_view(service_id, view_id)
    res["service_id"] = service_id
    return res


_sys.modules.setdefault("backend.repositories.usage", _sys.modules[__name__])
_sys.modules.setdefault("backend.repositories.alerts", _sys.modules[__name__])
_repos_pkg = _sys.modules.get("backend.repositories")
if _repos_pkg is not None:
    _repos_pkg.usage = _sys.modules[__name__]  # type: ignore[attr-defined]
    _repos_pkg.alerts = _sys.modules[__name__]  # type: ignore[attr-defined]
