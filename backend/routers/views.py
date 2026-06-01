"""Saved views router."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.deps import get_service_id
from backend.models.views import SavedView
from backend.repositories import views as repo
from backend.utils.router_utils import sync_admin_state

router = APIRouter(prefix="/api/views", tags=["views"])


@router.get("/{service_id}")
def list_views(service_id: str):
    return repo.get_views(service_id)


@router.post("/")
def create_view(view: SavedView):
    res = repo.save_view(view)
    sync_admin_state(view.service_id)
    return res


@router.delete("/{view_id}")
def delete_view(view_id: str, service_id: str | None = Depends(get_service_id)):
    res = repo.delete_view(view_id, service_id_hint=service_id)
    sync_admin_state(res.get("service_id"))
    return res
