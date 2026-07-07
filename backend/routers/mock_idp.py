"""In-process mock OIDC provider — E2E/dev ONLY, gated on ``OAUTH_MOCK_IDP=1``.

Lets the Playwright OAuth login journey run entirely against 127.0.0.1 (no real
IdP, no network): the backend's own OIDC client fetches discovery/JWKS/token
from these routes over loopback, and the browser is auto-approved at /authorize.
It signs id_tokens with a process-lifetime RSA key whose public half it serves
as the JWKS, so the REAL joserfc verification path is exercised.

NEVER mounted unless ``OAUTH_MOCK_IDP=1`` — so it is absent from the OpenAPI
schema in normal builds (no snapshot drift) and can never appear in production.
The auto-approved identity is controllable per-test via the ``mock_idp_email`` /
``mock_idp_email_verified`` cookies (default a fixed e2e address).
"""

from __future__ import annotations

import base64
import json
import os
import time

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from joserfc import jwt
from joserfc.jwk import RSAKey

router = APIRouter(prefix="/mock-idp", tags=["mock-idp"])

_KID = "mock-idp-key"
_key: RSAKey | None = None


def mock_idp_enabled() -> bool:
    return os.getenv("OAUTH_MOCK_IDP") == "1"


def _signing_key() -> RSAKey:
    global _key
    if _key is None:
        _key = RSAKey.generate_key(2048, {"kid": _KID, "use": "sig", "alg": "RS256"})
    return _key


def _issuer() -> str:
    return os.getenv("OAUTH_MOCK_IDP_ISSUER", "http://127.0.0.1:18004/mock-idp")


def _b64url(obj: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


def _unb64url(s: str) -> dict:
    pad = "=" * (-len(s) % 4)
    return json.loads(base64.urlsafe_b64decode((s + pad).encode()))


@router.get("/.well-known/openid-configuration")
def discovery():
    b = _issuer()
    return {
        "issuer": b,
        "authorization_endpoint": f"{b}/authorize",
        "token_endpoint": f"{b}/token",
        "jwks_uri": f"{b}/jwks",
        "id_token_signing_alg_values_supported": ["RS256"],
    }


@router.get("/jwks")
def jwks():
    return {"keys": [_signing_key().as_dict(private=False)]}


@router.get("/authorize")
def authorize(request: Request, redirect_uri: str, state: str, nonce: str):
    """Auto-approve: bounce straight back to redirect_uri with an authorization
    code that carries the (test-controlled) identity + the request nonce."""
    email = request.cookies.get("mock_idp_email") or os.getenv("OAUTH_MOCK_IDP_EMAIL", "e2e-analyst@example.com")
    email_verified = (request.cookies.get("mock_idp_email_verified") or "true").lower() != "false"
    code = _b64url({"email": email, "nonce": nonce, "sub": f"mock-sub-{email}", "ev": email_verified})
    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(f"{redirect_uri}{sep}code={code}&state={state}", status_code=302)


@router.post("/token")
async def token(request: Request):
    form = await request.form()
    try:
        data = _unb64url(str(form.get("code") or ""))
    except Exception:
        from fastapi.responses import JSONResponse

        return JSONResponse({"error": "invalid_grant"}, status_code=400)
    now = int(time.time())
    claims = {
        "iss": _issuer(),
        "aud": os.getenv("OAUTH_GOOGLE_CLIENT_ID", "mock-client-id"),
        "sub": data["sub"],
        "email": data["email"],
        "email_verified": bool(data.get("ev", True)),
        "nonce": data["nonce"],
        "iat": now,
        "exp": now + 300,
    }
    id_token = jwt.encode({"alg": "RS256", "kid": _KID}, claims, _signing_key())
    return {"access_token": "mock-access-token", "token_type": "Bearer", "id_token": id_token}
