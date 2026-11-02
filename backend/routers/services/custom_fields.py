"""Custom log fields CRUD, VCL lint validation, export, and import endpoints."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from backend.core import duckdb as _db
from backend.core import log_fields as lf_module
from backend.core.log_fields import _filter_user_custom_fields, _is_system_field
from backend.models.common import (
    CustomFieldCreate,
    CustomFieldResponse,
    CustomFieldsExportFile,
    CustomFieldsImportBody,
    CustomFieldsImportResponse,
    CustomFieldsListResponse,
    CustomFieldUpdate,
    VclLintRequest,
    VclLintResponse,
)
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import load_service_config, mutate_service_config

from .storage import _check_storage_type_lock, _locked_storage_field_names
from .vcl import _persist_log_fields, _validate_log_format_version

router = APIRouter()


def _persist_custom_fields_change(
    service_id: str, cfg: dict, lf: dict, new_custom: list, candidate_lf: dict, now: str, *, full: bool = True
) -> None:
    new_lf = _persist_log_fields(lf, new_custom, candidate_lf, now, full=full)
    cfg["log_fields"] = new_lf
    mutate_service_config(service_id, lambda c: c.__setitem__("log_fields", new_lf))


def _load_modifiable_custom_fields(
    request: Request, service_id: str, field_name: str, action_verb: str
) -> tuple[dict, dict, list]:
    require_service_in_scope(request, service_id)
    if _is_system_field(field_name):
        raise HTTPException(
            status_code=422,
            detail={"errors": [f"Field '{field_name}' is system-managed and cannot be {action_verb}."]},
        )
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    return cfg, lf, lf.get("custom_fields", [])


def _split_custom_field_warnings(items: list[str], *, strip_prefix: bool = False) -> tuple[list[str], list[str]]:
    hard_errors: list[str] = []
    warnings: list[str] = []
    for item in items:
        if item.startswith("WARN"):
            warnings.append(item.removeprefix("WARN: ") if strip_prefix else item)
        else:
            hard_errors.append(item)
    return hard_errors, warnings


def _validate_and_save_custom_field(
    service_id: str, cfg: dict, lf: dict, field_dict: dict, other_names: list[str], new_custom: list[dict], now: str
) -> dict:
    hard_errors, warnings = _split_custom_field_warnings(lf_module.validate_custom_field(field_dict, other_names))
    if hard_errors:
        raise HTTPException(status_code=422, detail={"errors": hard_errors})
    candidate_lf = {**lf, "custom_fields": new_custom}
    _validate_log_format_version(candidate_lf)
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, candidate_lf, now)
    return {"ok": True, "field": field_dict, "warnings": warnings}


@router.get("/services/{service_id}/custom-fields", response_model=CustomFieldsListResponse)
def api_list_custom_fields(request: Request, service_id: str) -> CustomFieldsListResponse:
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    return CustomFieldsListResponse(fields=_filter_user_custom_fields(lf.get("custom_fields", [])))  # type: ignore


@router.post("/services/{service_id}/custom-fields", response_model=CustomFieldResponse)
def api_create_custom_field(request: Request, service_id: str, body: CustomFieldCreate):
    field_dict = body.model_dump()
    cfg, lf, existing = _load_modifiable_custom_fields(
        request, service_id, field_dict.get("name", ""), "created manually"
    )
    _check_storage_type_lock(
        service_id, field_dict["name"], field_dict.get("duckdb_type"), field_dict.get("value_type")
    )
    now = datetime.now(UTC).isoformat()
    field_dict["created_at"] = field_dict["updated_at"] = now
    return _validate_and_save_custom_field(
        service_id, cfg, lf, field_dict, [cf["name"] for cf in existing], existing + [field_dict], now
    )


@router.patch("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_update_custom_field(request: Request, service_id: str, field_name: str, body: CustomFieldUpdate):
    cfg, lf, existing = _load_modifiable_custom_fields(request, service_id, field_name, "edited")
    idx = next((i for i, cf in enumerate(existing) if cf["name"] == field_name), None)
    if idx is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    updates = body.model_dump(exclude_unset=True)
    old_field = existing[idx]
    changing_type = ("duckdb_type" in updates and updates["duckdb_type"] != old_field.get("duckdb_type")) or (
        "value_type" in updates and updates["value_type"] != old_field.get("value_type")
    )
    if changing_type:
        src = _db.get_source_for_service(service_id)
        schema_fields = _locked_storage_field_names(
            src,
            error_lead=f"Could not verify the committed schema for '{field_name}'",
            error_tail="the change was not applied. Please retry.",
        )
        if field_name in schema_fields:
            raise HTTPException(
                status_code=422,
                detail={
                    "errors": [
                        "Cannot change 'duckdb_type' or 'value_type' after the field has been created in the database. Please create a new field instead."
                    ]
                },
            )
    now = datetime.now(UTC).isoformat()
    updated = {**old_field, **{k: v for k, v in updates.items() if v is not None}, "updated_at": now}
    other_names = [cf["name"] for cf in existing if cf["name"] != field_name]
    new_custom = existing[:idx] + [updated] + existing[idx + 1 :]
    return _validate_and_save_custom_field(service_id, cfg, lf, updated, other_names, new_custom, now)


@router.delete("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_delete_custom_field(request: Request, service_id: str, field_name: str):
    cfg, lf, existing = _load_modifiable_custom_fields(request, service_id, field_name, "deleted")
    field = next((cf for cf in existing if cf["name"] == field_name), None)
    if field is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    new_custom = [cf for cf in existing if cf["name"] != field_name]
    now = datetime.now(UTC).isoformat()
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, {**lf, "custom_fields": new_custom}, now)
    return {"ok": True, "field": field}


@router.post("/services/{service_id}/custom-fields/validate-vcl", response_model=VclLintResponse)
def api_validate_custom_vcl(request: Request, service_id: str, body: VclLintRequest):
    from backend import provision

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    candidate = {
        "name": "lint_check",
        "label": "Lint Check",
        "vcl_log_expression": body.vcl_log_expression,
        "collection_stage": body.collection_stage,
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        "enabled": True,
    }
    errors, warnings = _split_custom_field_warnings(lf_module.validate_custom_field(candidate, []), strip_prefix=True)
    base_lf = body.log_fields_config if body.log_fields_config else lf
    candidate_lf = {**base_lf, "custom_fields": lf.get("custom_fields", []) + [candidate]}
    fmt_hard_errors, fmt_warnings = _split_custom_field_warnings(provision.validate_log_format(candidate_lf))
    errors.extend(fmt_hard_errors)
    warnings.extend(fmt_warnings)
    fmt = provision.load_log_format(candidate_lf) if not errors else None
    return VclLintResponse(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        format_length=len(fmt) if fmt else None,
        format_length_limit=provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX,
    )


@router.get(
    "/services/{service_id}/custom-fields/export",
    response_class=StreamingResponse,
    responses={
        200: {
            "model": CustomFieldsExportFile,
            "description": "JSON file download (Content-Disposition: attachment) holding the user-defined custom fields.",
        }
    },
)
def api_export_custom_fields(request: Request, service_id: str):
    require_service_in_scope(request, service_id)
    lf = lf_module.get_lf_config(load_service_config(service_id))
    user_fields = _filter_user_custom_fields(lf.get("custom_fields", []))
    return StreamingResponse(
        iter([json.dumps({"custom_fields": user_fields})]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=custom_fields_{service_id}.json"},
    )


@router.post(
    "/services/{service_id}/custom-fields/import",
    response_model=CustomFieldsImportResponse,
    response_model_exclude_unset=True,
)
def api_import_custom_fields(request: Request, service_id: str, body: CustomFieldsImportBody):
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    fields_to_import = body.custom_fields
    lf = lf_module.get_lf_config(cfg)
    existing_map = {cf["name"]: cf for cf in lf.get("custom_fields", [])}
    locked_field_names = _locked_storage_field_names(
        _db.get_source_for_service(service_id),
        error_lead="Could not verify committed fields for import",
        error_tail="no changes were applied. Please retry.",
    )
    new_custom_map = {**existing_map}
    now = datetime.now(UTC).isoformat()
    type_lock_errors: list[str] = []
    validation_errors: list[str] = []
    for field_dict in fields_to_import:
        if "name" not in field_dict:
            continue
        fname = field_dict["name"]
        if _is_system_field(fname):
            validation_errors.append(f"{fname}: cannot import system-managed field")
            continue
        existing_field = existing_map.get(fname, {})
        if fname in locked_field_names:
            for tk in ("duckdb_type", "value_type"):
                if field_dict.get(tk) and field_dict[tk] != existing_field.get(tk):
                    type_lock_errors.append(
                        f"Cannot change '{tk}' of '{fname}': field is already committed to the database."
                    )
                    break
            else:
                pass
            if any(fname in err for err in type_lock_errors):
                continue
        other_names = [n for n in new_custom_map if n != fname]
        for err in lf_module.validate_custom_field(field_dict, other_names):
            if not err.startswith("WARN:"):
                validation_errors.append(f"{fname}: {err}")
        field_dict.pop("created_at", None)
        field_dict.pop("updated_at", None)
        field_dict["created_at"] = existing_field.get("created_at", now)
        field_dict["updated_at"] = now
        new_custom_map[fname] = field_dict
    if type_lock_errors:
        raise HTTPException(status_code=422, detail={"errors": type_lock_errors})
    if validation_errors:
        raise HTTPException(status_code=422, detail={"errors": validation_errors})
    new_custom = list(new_custom_map.values())
    candidate_lf = {**lf, "custom_fields": new_custom}
    _validate_log_format_version(candidate_lf)
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, candidate_lf, now, full=False)
    return {"ok": True, "imported_count": len(fields_to_import)}
