"""Repository for saved views.

Storage lives in per-service SQLite via ``backend.core.metadata_db``.
"""

from __future__ import annotations

from backend import config as svcconfig
from backend.core import metadata_db
from backend.models.views import SavedView


def get_views(service_id: str) -> list[dict]:
    return metadata_db.list_views(service_id)


def save_view(view: SavedView) -> dict:
    return metadata_db.save_view(view.service_id, view)


def _find_view_service(view_id: str) -> str | None:
    """Scan all per-service metadata DBs to find which service owns this view."""
    for cfg in svcconfig.list_configs():
        sid = cfg.get("service_id")
        if not sid:
            continue
        for v in metadata_db.list_views(sid):
            if v["id"] == view_id:
                return sid
    return None


def get_view_by_id(view_id: str) -> dict | None:
    """Return the saved-view row whose id matches ``view_id`` (or None).

    Security mirror of ``alerts.get_alert_by_id`` — the router-level
    cross-tenant scope gate calls this before delete_view so an
    unauthorized analyst gets 403 without the row being deleted.
    """
    for cfg in svcconfig.list_configs():
        sid = cfg.get("service_id")
        if not sid:
            continue
        for v in metadata_db.list_views(sid):
            if v.get("id") == view_id:
                # Stamp the owning service_id onto the result so the
                # caller's scope check can compare without re-scanning.
                out = dict(v)
                out.setdefault("service_id", sid)
                return out
    return None


def delete_view(view_id: str, service_id_hint: str | None = None) -> dict:
    sid = service_id_hint or _find_view_service(view_id)
    if not sid:
        return {"status": "not_found", "service_id": None}
    res = metadata_db.delete_view(sid, view_id)
    res["service_id"] = sid
    return res
