"""Repository for saved views.

Storage lives in per-service SQLite via ``backend.core.metadata_db``.
"""

from __future__ import annotations

from backend.core import metadata_db
from backend.models.views import SavedView


def get_views(service_id: str) -> list[dict]:
    return metadata_db.list_views(service_id)


def save_view(view: SavedView) -> dict:
    return metadata_db.save_view(view.service_id, view)


def get_view_by_id(view_id: str, service_id: str) -> dict | None:
    """Return the saved-view row whose id matches ``view_id`` in the
    given service (or None).

    Security mirror of ``alerts.get_alert_by_id`` — the router-level
    cross-tenant scope gate calls this before delete_view so an
    unauthorized analyst gets 403 without the row being deleted.

    ``service_id`` is required (audit finding 018). The pre-fix variant
    scanned every per-service metadata DB to locate the owning service,
    which turned a lightweight "fetch unknown id" request into an O(N)
    workload across the whole tenant set — trivially exploited as a
    resource-exhaustion vector.
    """
    for v in metadata_db.list_views(service_id):
        if v.get("id") == view_id:
            # Stamp the owning service_id onto the result so the
            # caller's scope check can compare without re-scanning.
            out = dict(v)
            out.setdefault("service_id", service_id)
            return out
    return None


def delete_view(view_id: str, service_id: str) -> dict:
    """Delete the saved-view row in the given service (or report
    not_found). ``service_id`` is required — see audit finding 018."""
    res = metadata_db.delete_view(service_id, view_id)
    res["service_id"] = service_id
    return res
