from __future__ import annotations

import hmac
import os

from starlette.requests import Request

ADMIN_GATEWAY_HEADER = "X-Admin-Gateway-Token"


def admin_gateway_required() -> bool:
    return os.getenv("ADMIN_GATEWAY_REQUIRED", "") == "1"


def validate_admin_gateway_config() -> None:
    if os.getenv("ADMIN_GATEWAY_REQUIRED", "") not in ("", "0", "1"):
        raise ValueError("ADMIN_GATEWAY_REQUIRED must be 0 or 1")
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    if (admin_gateway_required() or secret) and len(secret) < 32:
        raise ValueError("ADMIN_GATEWAY_SECRET must contain at least 32 characters for admin gateway access")


def authenticated_admin_gateway(request: Request) -> bool:
    if getattr(request.state, "admin_gateway_authenticated", False):
        return True
    if request.headers.get("x-proxied-by-caddy"):
        return False
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    values = request.headers.getlist(ADMIN_GATEWAY_HEADER)
    if len(secret) < 32 or len(values) != 1:
        return False
    return hmac.compare_digest(values[0].encode("utf-8"), secret.encode("utf-8"))
