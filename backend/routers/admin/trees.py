"""Raw + Iceberg tree endpoints (file-tree browsing for the admin UI)."""

from __future__ import annotations

from fastapi import Depends, Query

from backend.deps import get_source
from backend.models.admin import TreeResponse

from ._router import router


@router.get("/admin/raw-tree", response_model=TreeResponse)
def raw_tree_endpoint(
    source: dict = Depends(get_source),
    prefix: str = Query(default=""),
):
    from backend.core.duckdb import get_raw_tree_node

    result = get_raw_tree_node(source, prefix, root="raw")
    return TreeResponse.with_telemetry(nodes=result.get("children", []))


@router.get("/admin/iceberg-tree", response_model=TreeResponse)
def iceberg_tree_endpoint(
    source: dict = Depends(get_source),
    prefix: str = Query(default=""),
):
    from backend.core.duckdb import get_raw_tree_node

    result = get_raw_tree_node(source, prefix, root="iceberg")
    return TreeResponse.with_telemetry(nodes=result.get("children", []))
