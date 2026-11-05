"""Service storage schema and type-lock helpers."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.core import fos_client as _db

router = APIRouter()

_TYPE_ALIASES: dict[str, str] = {
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "BIGINT": "INT64",
    "INTEGER": "INT32",
    "INT": "INT32",
    "SMALLINT": "INT16",
    "TINYINT": "INT8",
    "DOUBLE": "FLOAT64",
    "FLOAT": "FLOAT32",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
}


def _committed_field_types(src: dict) -> dict[str, str]:
    """Return committed custom field types (empty in ClickHouse JSON custom fields mode)."""
    del src
    return {}


def _check_storage_type_lock(
    service_id: str, field_name: str, new_sql_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the committed table."""
    del new_value_type
    src = _db.get_source_for_service(service_id)
    if not src or src.get("storage_mode", "cloud") == "local":
        return
    try:
        existing_sql_type = _committed_field_types(src).get(field_name)
        if existing_sql_type and new_sql_type:
            current_mapped = _TYPE_ALIASES.get(existing_sql_type.upper(), existing_sql_type.upper())
            target_mapped = _TYPE_ALIASES.get(new_sql_type.upper(), new_sql_type.upper())
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
    """Return committed schema field names for ``src``; fail closed (503) on error."""
    if not src or src.get("storage_mode", "cloud") == "local":
        return set()
    try:
        return set(_committed_field_types(src).keys())
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"errors": [f"{error_lead} ({type(e).__name__}); {error_tail}"]},
        ) from e
