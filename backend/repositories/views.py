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


def delete_view(view_id: str, service_id_hint: str | None = None) -> dict:
    sid = service_id_hint or _find_view_service(view_id)
    if not sid:
        return {"status": "not_found", "service_id": None}
    res = metadata_db.delete_view(sid, view_id)
    res["service_id"] = sid
    return res
