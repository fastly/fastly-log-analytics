"""Shared fixtures for the live RBAC regression suite.

The probes in ``test_live_rbac_probes.py`` exercise a deployed instance
(dev or staging) through real HTTP — they are NOT unit tests. They run
only when the required env vars are present so the default
``uv run pytest`` invocation skips them silently:

  * ``FLA_PROBE_BASE_URL``   — e.g. ``https://fastly-log-analytics.global.ssl.fastly.net``
  * ``FLA_PROBE_EMAIL``      — analyst email registered on the target
  * ``FLA_PROBE_PASSCODE``   — passcode for the share-login flow

A pre-release CI job (or a manual operator run before a deploy) sets the
vars and runs ``pytest tests/security/test_live_rbac_probes.py -v``. The
suite pins the five P0 fixes from the 2026-06-15 audit:

  * Fix 1 (R-6):    bare-name ``debug_queries`` / ``debug_calls`` stripped
  * Fix 2 (R-4):    ``/scoring/labels`` projects out PII fields
  * Fix 3 (R-3):    ``/scoring/analytics`` composite omits admin-only keys
  * Fix 4 (R-1):    time-bounds enforcement returns 400 on empty window
  * Fix 5 (F-8..F-10): ``SHOW TABLES`` + foreign-table SELECT both 403

Each fix also has unit-test coverage in tests/utils/ and tests/routers/;
this suite is the END-TO-END gate that catches regressions a unit
mock-out would miss (e.g. the middleware ordering changes and the strip
helper stops receiving the response).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest

_BASE_URL_ENV = "FLA_PROBE_BASE_URL"
_EMAIL_ENV = "FLA_PROBE_EMAIL"
_PASSCODE_ENV = "FLA_PROBE_PASSCODE"

# Keep the User-Agent stable across the test session — the backend's
# session fingerprint check invalidates a session whose UA differs from
# the login UA, so a probe that logs in with one UA and then queries
# with another would 401 mid-suite.
_STABLE_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _env(name: str) -> str | None:
    v = os.getenv(name)
    return v if v else None


@pytest.fixture(scope="session")
def probe_base_url() -> str:
    url = _env(_BASE_URL_ENV)
    if not url:
        pytest.skip(
            f"{_BASE_URL_ENV} not set; skipping live RBAC probes. "
            "Set FLA_PROBE_BASE_URL / FLA_PROBE_EMAIL / FLA_PROBE_PASSCODE to run."
        )
    return url.rstrip("/")


@pytest.fixture(scope="session")
def probe_creds() -> tuple[str, str]:
    email = _env(_EMAIL_ENV)
    passcode = _env(_PASSCODE_ENV)
    if not email or not passcode:
        pytest.skip(f"{_EMAIL_ENV} / {_PASSCODE_ENV} not set; skipping live RBAC probes.")
    return email, passcode


@pytest.fixture(scope="session")
def probe_session(probe_base_url: str, probe_creds: tuple[str, str]) -> dict[str, Any]:
    """Log in once per session, return the session cookie + active service_id.

    Returns a dict shaped: ``{"cookie": "...", "service_id": "...", "base_url": "..."}``.
    Used by the per-test probe helpers to build authenticated requests.
    """
    email, passcode = probe_creds
    req = urllib.request.Request(
        f"{probe_base_url}/api/share/login",
        method="POST",
        data=json.dumps({"email": email, "passcode": passcode}).encode(),
        headers={
            "Content-Type": "application/json",
            "Origin": probe_base_url,
            "Referer": f"{probe_base_url}/share-login",
            "X-Remote-Analyst": "1",
            "User-Agent": _STABLE_UA,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            cookies = resp.headers.get_all("Set-Cookie") or []
            body = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        pytest.skip(
            f"share-login probe failed ({exc.code}); cannot run live RBAC probes. "
            "Verify FLA_PROBE_EMAIL / FLA_PROBE_PASSCODE against {probe_base_url}/api/share/login."
        )
    sid_cookie = next(
        (c.split(";")[0].split("=", 1)[1] for c in cookies if c.startswith("analyst_session_id=")),
        None,
    )
    if not sid_cookie:
        pytest.skip("share-login returned no analyst_session_id cookie; cannot run probes.")
    service_ids = body.get("service_ids") or []
    if not service_ids:
        pytest.skip("share-login returned no service_ids; analyst has no service access.")
    return {
        "cookie": f"analyst_session_id={sid_cookie}",
        "service_id": service_ids[0],
        "base_url": probe_base_url,
    }


def analyst_request(
    session: dict[str, Any],
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 60.0,
) -> tuple[int, bytes]:
    """Make an authenticated analyst request. Returns (status, body bytes).

    Captures HTTPError as the (status, body) shape too so probes can
    assert on 4xx without try/except in every test.
    """
    headers = {
        "Cookie": session["cookie"],
        "Origin": session["base_url"],
        "Referer": f"{session['base_url']}/dashboard",
        "User-Agent": _STABLE_UA,
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        session["base_url"] + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read() if hasattr(exc, "read") else b""
        return exc.code, raw
