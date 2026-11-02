"""Service DuckDB/DuckLake storage schema and type-lock helpers."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException

from backend.core import duckdb as _db
from backend.core import storage as _storage

router = APIRouter()


def _committed_duckdb_field_types(src: dict) -> dict[str, str]:
    """Fallback schema lookup via the service's DuckDB/DuckLake schema."""
    db_path = _db.db_path_for_source(src)
    if not os.path.exists(db_path):
        return {}
    con = _db.get_connection(source=src, read_only=True, max_wait=2.0, skip_view_update=True)
    try:
        return {
            str(col.get("name") or col.get("column")): str(col.get("type"))
            for col in _db.get_schema(con, src, stats=False)
            if (col.get("name") or col.get("column")) and col.get("type")
        }
    finally:
        con.close()


def _check_storage_type_lock(
    service_id: str, field_name: str, new_duckdb_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the committed table."""
    src = _db.get_source_for_service(service_id)
    if not src or src.get("storage_mode", "cloud") == "local":
        return
    try:
        duckdb_to_arrow = getattr(_storage, "_DUCKDB_TO_ARROW", {})
        existing_duckdb_type = _committed_duckdb_field_types(src).get(field_name)
        if existing_duckdb_type and new_duckdb_type:
            current_mapped = duckdb_to_arrow.get(existing_duckdb_type.upper(), existing_duckdb_type.upper())
            target_mapped = duckdb_to_arrow.get(new_duckdb_type.upper(), new_duckdb_type.upper())
            if current_mapped != target_mapped:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "errors": [
                            f"Cannot change data type for '{field_name}' (currently {current_mapped}) after it has been created in the database. Please create a new field instead."
                        ]
                    },
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "errors": [
                    f"Could not verify the committed type for '{field_name}' "
                    f"({type(e).__name__}); the change was not applied. Please retry."
                ]
            },
        ) from e


def _locked_storage_field_names(src: dict | None, *, error_lead: str, error_tail: str) -> set[str]:
    """Return committed DuckLake/DuckDB-schema field names for ``src``; fail closed (503) on error."""
    if not src or src.get("storage_mode", "cloud") == "local":
        return set()
    try:
        return set(_committed_duckdb_field_types(src).keys())
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"errors": [f"{error_lead} ({type(e).__name__}); {error_tail}"]},
        ) from e
