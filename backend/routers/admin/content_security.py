"""Streaming → Content Security blocker settings (global).

Admin-only (``/api/admin/*`` is blocked for analysts in ``remote_access``, and
every route also depends on ``require_admin``). Blocking calls the operator's
HTTP function — ``POST`` ``{"kbps": N, "conditions": [{category, value}, ...]}``
blocks (``kbps`` 0) or throttles (``kbps`` > 0) requests matching ALL conditions, ``DELETE`` with the same body reverts — and
records the result in ``content_security_blocklist`` plus the service's audit
log. A row is written only after the function returns 2xx,
so the active list never claims a block the function rejected.
"""

from __future__ import annotations

import logging
from urllib.parse import urlsplit

from fastapi import Depends, HTTPException

from backend import config as svcconfig
from backend.deps import require_admin
from backend.models.cmcd import (
    ContentSecuritySettings,
    ContentSecuritySettingsUpdate,
)
from backend.utils.blocklist_client import (
    parse_auth_header,
    validate_blocklist_url,
)
from backend.utils.router_utils import make_error

from ._router import router

logger = logging.getLogger(__name__)

_ACTOR = "admin"


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
