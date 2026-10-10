"""Streaming → Content Security blocklist: global settings + per-service blocks.

Admin-only (``/api/admin/*`` is blocked for analysts in ``remote_access``, and
every route also depends on ``require_admin``). Blocking calls the operator's
HTTP function — ``POST`` ``{"conditions": [{category, value}, ...]}`` blocks
requests matching ALL conditions, ``DELETE`` with the same body reverts — and
records the result in ``content_security_blocklist`` plus the service's audit
log. A row is written only after the function returns 2xx,
so the active list never claims a block the function rejected.
"""

from __future__ import annotations

import logging
from ipaddress import ip_address
from urllib.parse import urlsplit

import psycopg
from fastapi import Depends, HTTPException

from backend import config as svcconfig
from backend.core.metadata import content_security_blocks as blocks_db
from backend.core.metadata.state import record_audit
from backend.deps import get_source, require_admin
from backend.models.cmcd import (
    ContentSecurityBlock,
    ContentSecurityBlockCondition,
    ContentSecurityBlockRequest,
    ContentSecurityBlocksResponse,
    ContentSecuritySettings,
    ContentSecuritySettingsUpdate,
)
from backend.utils.blocklist_client import (
    BlocklistCallError,
    call_blocklist,
    parse_auth_header,
    validate_blocklist_url,
)
from backend.utils.router_utils import make_error

from ._router import router

logger = logging.getLogger(__name__)

_ACTOR = "admin"


def _service_id_from_source(source: dict) -> str:
    return source.get("service_id") or source.get("name", "")


def _require_read_write_source(source: dict) -> None:
    if source.get("access_level", "read_write") != "read_write":
        raise HTTPException(status_code=403, detail=make_error("admin_only", "This operation requires admin access."))


def _public_settings(cfg: dict) -> dict:
    """Settings as the browser sees them: the header's name only, never its value."""
    header = cfg.get("blocklist_auth_header") or ""
    return {
        "blocklist_url": cfg.get("blocklist_url") or "",
        # Saved headers are normalized "Name: value"; partition (not parse) so a
        # hand-edited file can't 500 the settings page.
        "auth_header_name": header.partition(":")[0].strip() or None if header else None,
    }


def _origin(url: str) -> tuple[str, str, int | None]:
    parts = urlsplit(url)
    return parts.scheme, (parts.hostname or "").lower(), parts.port


def _blocklist_target_or_409() -> tuple[str, str | None]:
    """The configured (url, auth header), or 409 when blocking is off."""
    cfg = svcconfig.load_content_security_config()
    url = cfg.get("blocklist_url") or ""
    if not url:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "blocklist_not_configured",
                "Set the blocklist URL in Admin → Overall Settings → Content Security first.",
            ),
        )
    return url, cfg.get("blocklist_auth_header") or None


@router.get("/admin/content-security/settings", response_model=ContentSecuritySettings)
def get_content_security_settings(_admin: None = Depends(require_admin)) -> dict:
    return _public_settings(svcconfig.load_content_security_config())


@router.put("/admin/content-security/settings", response_model=ContentSecuritySettings)
def update_content_security_settings(
    body: ContentSecuritySettingsUpdate,
    _admin: None = Depends(require_admin),
) -> dict:
    try:
        url = validate_blocklist_url(body.blocklist_url)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=make_error("invalid_url", str(exc))) from exc

    current = svcconfig.load_content_security_config()
    header = current.get("blocklist_auth_header") or ""
    if body.auth_header is not None:
        header = body.auth_header.strip()
        if header:
            try:
                name, value = parse_auth_header(header)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=make_error("invalid_header", str(exc))) from exc
            header = f"{name}: {value}"
    elif header and url and current.get("blocklist_url") and _origin(url) != _origin(current["blocklist_url"]):
        # A saved credential must not silently follow the URL to another server.
        raise HTTPException(
            status_code=422,
            detail=make_error(
                "auth_header_reentry_required",
                "The URL now points to a different server. Re-enter the authorization header or remove it.",
            ),
        )

    if not url:
        # Turning blocking off drops the credential too, so a later URL can't inherit it.
        header = ""

    cfg = {**current, "blocklist_url": url, "blocklist_auth_header": header}
    svcconfig.save_content_security_config(cfg)
    # Never log the URL or header — both may carry an operator token.
    logger.info(
        "content security blocklist URL %s, auth header %s",
        "set" if url else "cleared",
        "set" if header else "none",
    )
    return _public_settings(cfg)


@router.get("/admin/content-security/blocks", response_model=ContentSecurityBlocksResponse)
def list_content_security_blocks(
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    return {
        "configured": bool(svcconfig.load_content_security_config().get("blocklist_url")),
        "blocks": blocks_db.list_active_blocks(_service_id_from_source(source)),
    }


def _normalize_conditions(conditions: list[ContentSecurityBlockCondition]) -> list[dict]:
    """Validate per-category values and return the canonical (category-sorted) list."""
    seen: set[str] = set()
    out: list[dict] = []
    for c in conditions:
        if c.category in seen:
            raise HTTPException(
                status_code=422,
                detail=make_error("duplicate_category", f"Only one condition per category ({c.category})"),
            )
        seen.add(c.category)
        value = c.value
        if c.category == "ip":
            try:
                value = str(ip_address(value))
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=make_error("invalid_ip", "Not a valid IP address")) from exc
        out.append({"category": c.category, "value": value})
    return blocks_db.canonical_conditions(out)


def _describe(conditions: list[dict]) -> str:
    return " AND ".join(f"{c['category']}={c['value']}" for c in conditions)


@router.post("/admin/content-security/blocks", response_model=ContentSecurityBlock, status_code=201)
def create_content_security_block(
    body: ContentSecurityBlockRequest,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    conditions = _normalize_conditions(body.conditions)
    already = make_error("already_blocked", f"{_describe(conditions)} is already blocked")
    if blocks_db.find_active_block(service_id, conditions):
        raise HTTPException(status_code=409, detail=already)
    url, auth_header = _blocklist_target_or_409()

    try:
        status = call_blocklist(url, "POST", conditions, auth_header)
    except BlocklistCallError as exc:
        record_audit(
            service_id,
            "content_security_block_failed",
            {"conditions": conditions, "error": str(exc)},
            actor=_ACTOR,
        )
        raise HTTPException(status_code=502, detail=make_error("blocklist_call_failed", str(exc))) from exc

    try:
        block = blocks_db.insert_block(service_id, conditions, _ACTOR)
    except psycopg.errors.UniqueViolation as exc:
        # A concurrent request recorded the same block between our check and insert.
        raise HTTPException(status_code=409, detail=already) from exc
    record_audit(
        service_id,
        "content_security_block",
        {"block_id": block["id"], "conditions": conditions, "status": status},
        actor=_ACTOR,
    )
    return block


@router.delete("/admin/content-security/blocks/{block_id}", response_model=ContentSecurityBlock)
def delete_content_security_block(
    block_id: str,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    block = blocks_db.get_block(service_id, block_id)
    if block is None:
        raise HTTPException(status_code=404, detail=make_error("not_found", "Block not found"))
    if block["unblocked_at"] is not None:
        return block
    url, auth_header = _blocklist_target_or_409()
    conditions = block["conditions"]

    try:
        status = call_blocklist(url, "DELETE", conditions, auth_header)
    except BlocklistCallError as exc:
        record_audit(
            service_id,
            "content_security_unblock_failed",
            {"block_id": block_id, "conditions": conditions, "error": str(exc)},
            actor=_ACTOR,
        )
        raise HTTPException(status_code=502, detail=make_error("blocklist_call_failed", str(exc))) from exc

    updated = blocks_db.mark_unblocked(service_id, block_id, _ACTOR)
    record_audit(
        service_id,
        "content_security_unblock",
        {"block_id": block_id, "conditions": conditions, "status": status},
        actor=_ACTOR,
    )
    assert updated is not None
    return updated
