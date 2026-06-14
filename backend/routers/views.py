"""Saved views router."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.deps import get_service_id
from backend.models.views import SavedView
from backend.repositories import views as repo
from backend.routers._state_sync import sync_admin_state

router = APIRouter(prefix="/api/views", tags=["views"])


def _analyst_allowed_services(request: Request) -> set[str] | None:
    """Security: return the analyst's allowed service set, or None
    for admin. Mirrors the same helper in alerts.py — the cross-tenant
    risk is identical for saved views."""
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None
    return set(analyst_session.service_ids or [])


@router.get("/{service_id}")
def list_views(service_id: str, request: Request):
    """Security: analyst can only list views for services in their
    scope. Without this gate an analyst scoped to ``svc-A`` could enumerate
    saved views for ``svc-B`` by typing /api/views/svc-B in their browser."""
    allowed = _analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": service_id},
        )
    return repo.get_views(service_id)


@router.post("/")
def create_view(view: SavedView, request: Request):
    """Security: analyst can only create views for services in
    their scope. Middleware already blocks POST on /api/views for
    analysts; this is defense-in-depth."""
    allowed = _analyst_allowed_services(request)
    if allowed is not None and view.service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": view.service_id},
        )
    res = repo.save_view(view)
    sync_admin_state(view.service_id)
    return res


@router.delete("/{view_id}")
def delete_view(view_id: str, request: Request, service_id: str | None = Depends(get_service_id)):
    # Security: service_id is required (audit finding 018). The pre-fix
    # variant fell through to an O(N) scan across every tenant DB when
    # service_id was absent, which an authenticated user could trivially
    # exploit for resource exhaustion. Reject early with a 400.
    if not service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    # Security: pre-flight scope check, mirrors alerts.delete_alert.
    allowed = _analyst_allowed_services(request)
    if allowed is not None:
        existing = repo.get_view_by_id(view_id, service_id)
        if existing and existing.get("service_id") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": existing.get("service_id")},
            )
    res = repo.delete_view(view_id, service_id)
    sync_admin_state(res.get("service_id"))
    return res
