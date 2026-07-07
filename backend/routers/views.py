"""Saved views router."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from backend.deps import get_service_id
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.views import SavedView, SavedViewRecord, ViewSaveResponse
from backend.repositories import views as repo
from backend.routers._state_sync import sync_admin_state
from backend.utils.auth import require_service_in_scope

router = APIRouter(prefix="/api/views", tags=["views"], responses=DEFAULT_ERROR_RESPONSES)


@router.get(
    "/{service_id}",
    response_model=list[SavedViewRecord],
    response_model_exclude_unset=True,
)
def list_views(
    service_id: str,
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max saved views to return."),
):
    """Security: analyst can only list views for services in their
    scope. Without this gate an analyst scoped to ``svc-A`` could enumerate
    saved views for ``svc-B`` by typing /api/views/svc-B in their browser.

    Hard ``limit`` cap (default 500, max 2000) caps the payload as a
    tenant accumulates saved views.
    """
    require_service_in_scope(request, service_id)
    return repo.get_views(service_id)[:limit]


@router.post("/", status_code=201, response_model=ViewSaveResponse)
def create_view(view: SavedView, request: Request):
    """Security: analyst can only create views for services in
    their scope. Middleware already blocks POST on /api/views for
    analysts; this is defense-in-depth.

    Returns 201 Created — resource POST convention. Body still carries
    the saved view so callers don't need a follow-up GET for the id."""
    require_service_in_scope(request, view.service_id)
    res = repo.save_view(view)
    sync_admin_state(view.service_id)
    return res


# response_model intentionally omitted: 204 No Content — empty body.
@router.delete("/{view_id}", status_code=204)
def delete_view(view_id: str, request: Request, service_id: str | None = Depends(get_service_id)):
    # Security: service_id is required (audit finding 018). The pre-fix
    # variant fell through to an O(N) scan across every tenant DB when
    # service_id was absent, which an authenticated user could trivially
    # exploit for resource exhaustion. Reject early with a 400.
    if not service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    # Security: pre-flight scope check, mirrors alerts.delete_alert. The
    # membership gate is shared; the record-ownership check (does the
    # addressed view actually belong to an in-scope service?) stays inline.
    allowed = require_service_in_scope(request, service_id)
    if allowed is not None:
        existing = repo.get_view_by_id(view_id, service_id)
        if existing and existing.get("service_id") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": existing.get("service_id")},
            )
    res = repo.delete_view(view_id, service_id)
    sync_admin_state(res.get("service_id") or service_id)
    return Response(status_code=204)
