"""Tripwire: every real route must be consciously classified for analyst reach.

Why this exists (2026-08 RBAC audit): the analyst blocklist in
``backend.utils.remote_access`` is hand-curated. A new route is analyst-
reachable by default unless someone remembers to add it to a blocked-prefix /
blocked-subpath / suffix-gate entry. That is exactly how
``/api/services/{id}/rum/status``, ``/rum/enable``, and ``/rum/disable``
shipped without gating in the first place — and ``tests/remote_access/
test_middleware.py`` could not have caught it because that suite builds its
test app from HAND-COPIED STUB ROUTES, so a real route nobody wrote a stub
for is invisible to it.

This test closes that gap by walking the REAL app's route table
(``backend.main.app.openapi()["paths"]``, the convention this repo already
uses for whole-surface OpenAPI checks — see
``tests/test_error_envelope_contract.py``) and forcing every single
``(method, path)`` into one of exactly three buckets:

  1. Blocked — ``_is_blocked_path()`` returns True for the (parameter-
     substituted) path. No inventory entry needed; the blocklist function
     itself is already covered by ``tests/routers/test_rbac_audit_fixes.py``.
  2. ``_READ_ALLOWLIST`` — a GET/HEAD route an analyst may reach, grouped by
     rationale below. This is the reviewable "what can an analyst see"
     surface.
  3. ``_WRITE_VERB_GATE_ROUTES`` — a POST route an analyst may reach ONLY
     because it isn't blocked AND its path starts with an entry in
     ``_ANALYST_ALLOWED_WRITE_PREFIXES`` (the read-shaped-POST verb gate in
     remote_access.py). Listed separately from the read allowlist — and
     cross-checked against the actual prefix set below — because a route
     landing here is NOT individually reviewed; it rides on a blanket prefix
     allowance. Treating that as equivalent to a deliberate per-route
     classification would hide exactly the kind of drift this test exists to
     catch, so ``test_write_gate_routes_actually_rely_on_the_verb_gate``
     makes the reliance explicit and asserts nothing in this bucket is
     independently blocklisted.

A route that lands in NEITHER "blocked" NOR either inventory reds this
suite — that is the entire point. See
``test_unclassified_route_fails_the_tripwire`` for a live demonstration
using a route injected straight into the real app's router (proving the
mechanism, not a copy of it).

Populated from CURRENT behavior so this passes on today's tree — it is a
tripwire for future drift, not a re-litigation of existing classifications.
Four routes below are marked FLAGGED FOR TRIAGE: the audit found them
analyst-reachable and unclassified, but nobody has confirmed whether that's
correct, so they are listed as-is (not endorsed as reviewed-safe) purely so
the suite reflects reality today.
"""

from __future__ import annotations

import re

import pytest

from backend.main import app
from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES, _UNAUTH_ANALYST_PATHS, _is_blocked_path

pytestmark = pytest.mark.security_regression

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")

# Plausible, non-identifying placeholder values for every path parameter name
# used across the route table. No real service/session/invite IDs — this is
# a public repo (see infra-leak-sweep).
_PATH_PARAM_SUBSTITUTIONS = {
    "service_id": "svc1",
    "alert_id": "alert1",
    "invite_id": "invite1",
    "session_id": "sess1",
    "quarantine_id": "quar1",
    "source_id": "src1",
    "qid": "q1",
    "log_id": "1",
    "run_id": "run1",
    "field_name": "field1",
    "label_id": "label1",
    "version": "v1",
    "sid": "sid1",
    "view_id": "view1",
    "tab": "overview",
    "token": "tok1",
}

_PARAM_RE = re.compile(r"\{([^}]+)\}")


def _substitute(path: str) -> str:
    """Replace every ``{param}`` segment with a plausible placeholder.

    Raises if a route introduces a path-parameter name this file doesn't
    know about yet — fail loudly rather than silently leaving a brace in
    the path (which would never match any blocklist regex and could mask
    a real gap).
    """

    def _rep(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in _PATH_PARAM_SUBSTITUTIONS:
            raise AssertionError(
                f"route path parameter {name!r} (from {path!r}) has no placeholder in "
                "_PATH_PARAM_SUBSTITUTIONS — add one so this tripwire can classify the route"
            )
        return _PATH_PARAM_SUBSTITUTIONS[name]

    return _PARAM_RE.sub(_rep, path)


def _real_routes() -> list[tuple[str, str]]:
    """Every ``(METHOD, path_template)`` from the real app's OpenAPI surface.

    Sorted so the test is deterministic regardless of dict/route
    registration order.
    """
    paths = app.openapi()["paths"]
    out: list[tuple[str, str]] = []
    for path in paths:
        for method in _HTTP_METHODS:
            if method in paths[path]:
                out.append((method.upper(), path))
    return sorted(out)


# ── Bucket 2: analyst-reachable GET/HEAD reads ──────────────────────────────
#
# Each group names WHY the class of routes is analyst-appropriate. Keep this
# reviewable — new entries should read as a deliberate decision, not a bare
# string appended to make the test pass.

_READ_ALLOWLIST: set[tuple[str, str]] = {
    # Pre-session / unauthenticated share flow. Mirrors _UNAUTH_ANALYST_PATHS
    # and the /api/share/oauth/* prefix exemption in the middleware — reached
    # before any analyst session cookie exists.
    ("GET", "/api/health"),
    ("GET", "/api/bootstrap"),
    ("GET", "/api/share/auth-config"),
    ("GET", "/api/share/heartbeat"),
    ("GET", "/api/share/tos"),
    ("GET", "/api/share/oauth/authorize"),
    ("GET", "/api/share/oauth/callback"),
    # Public-facing dynamic JS assets and beacon telemetry endpoints.
    # Safe to load anonymously (no credentials required).
    ("GET", "/js/rum.js"),
    ("GET", "/js/faro-sdk.js"),
    ("GET", "/rum-beacon"),
    ("POST", "/rum-beacon"),
    # Reference / catalog data: no per-service secrets, needed to drive the
    # analyst UI's filter and field pickers.
    ("GET", "/api/log-fields/catalog"),
    ("GET", "/api/presets"),
    ("GET", "/api/schema"),
    ("GET", "/api/services"),
    ("GET", "/api/insight-availability"),
    ("GET", "/api/views/{service_id}"),
    # Header-badge data + its live-push channel — analyst-safe siblings of
    # the admin-only /api/sync-status / /api/sync-status/stream (see the
    # _ANALYST_SSE_ALLOWLIST comment on /api/log-extents/stream).
    ("GET", "/api/log-extents"),
    ("GET", "/api/log-extents/stream"),
    # Session-scoring analyst-safe reads: the suffix gate
    # (_ANALYST_BLOCKED_SCORING_SUFFIXES) already keeps /config, /status,
    # /audit, /threshold, /exclude-regex, /enforce-*, /matrix-versions,
    # /dashboard, and /evaluation/per-reason admin-only. Everything below is
    # the deliberately-open complement (see the module docstring on that
    # suffix gate in remote_access.py).
    ("GET", "/api/services/{service_id}/scoring/analytics"),
    ("GET", "/api/services/{service_id}/scoring/compliance-breakdown"),
    ("GET", "/api/services/{service_id}/scoring/curves"),
    ("GET", "/api/services/{service_id}/scoring/evaluation"),
    ("GET", "/api/services/{service_id}/scoring/health"),
    ("GET", "/api/services/{service_id}/scoring/labels"),
    ("GET", "/api/services/{service_id}/scoring/latency-timeseries"),
    ("GET", "/api/services/{service_id}/scoring/score-distribution"),
    ("GET", "/api/services/{service_id}/scoring/sessions/{sid}/events"),
    ("GET", "/api/services/{service_id}/scoring/threshold-preview"),
    ("GET", "/api/services/{service_id}/scoring/top-flagged"),
    # RUM analyst-safe reads: the /rum/ suffix gate
    # (_ANALYST_BLOCKED_RUM_SUFFIXES) blocks /enable, /disable, /versions,
    # /upgrade. /rum/status is intentionally open (route projects down to
    # {enabled, enabled_at} for analysts) and the beacon-telemetry reads
    # below carry no operator-config disclosure — see the module comment
    # above _ANALYST_BLOCKED_RUM_SUFFIXES.
    ("GET", "/api/services/{service_id}/rum/status"),
    ("GET", "/api/services/{service_id}/rum/beacon-health"),
    ("GET", "/api/services/{service_id}/rum/analytics"),
    ("GET", "/api/services/{service_id}/rum/live-events"),
    # Control Room's live metrics push channel (S-1 audit decision):
    # aggregate rps/error-rate/cache-ratio, no PII, no infra details.
    ("GET", "/api/services/{service_id}/realtime-stream"),
    # Security proxies export: read-only CSV export of security proxies.
    ("GET", "/api/security/proxies/export"),
    # Network pop-health & security threat-intel (read-only analytical endpoints)
    ("GET", "/api/network/pop-health"),
    ("GET", "/api/security/threat-intel"),
    # ── FLAGGED FOR TRIAGE ───────────────────────────────────────────────
    # The 2026-08 RBAC audit found these four analyst-reachable with NO
    # explicit classification anywhere (not in a blocked-prefix/subpath/
    # suffix entry, not previously reviewed as safe). They are listed here
    # ONLY so this tripwire reflects today's actual behavior — this is NOT
    # an endorsement that they should stay open. See the follow-up report
    # for the triage recommendation on each.
    ("GET", "/api/services/{service_id}/cmcd/status"),
    ("GET", "/api/services/{service_id}/control-room/wizard/state"),
    ("GET", "/api/services/{service_id}/control-room/{tab}"),
    ("GET", "/api/services/{service_id}/realtime-seed"),
}

# ── Bucket 3: mutating routes that reach the handler ONLY via the blunt
# read-only-verb gate (POST + a matching _ANALYST_ALLOWED_WRITE_PREFIXES
# prefix), not because anyone reviewed the individual route. Kept separate
# from _READ_ALLOWLIST on purpose — see the module docstring and
# test_write_gate_routes_actually_rely_on_the_verb_gate below, which proves
# the reliance rather than asserting it by fiat.
_WRITE_VERB_GATE_ROUTES: set[tuple[str, str]] = {
    ("POST", "/api/share/acknowledge"),
    ("POST", "/api/share/claim/{token}"),
    ("POST", "/api/share/login"),
    ("POST", "/api/share/logout"),
    ("POST", "/api/assets/aggregates"),
    ("POST", "/api/dashboard/aggregates"),
    ("POST", "/api/dashboard/bundle"),
    ("POST", "/api/dashboard/field-values"),
    ("POST", "/api/dashboard/raw/csv"),
    ("POST", "/api/security/aggregates"),
    ("POST", "/api/security/top-bots"),
    ("POST", "/api/security/proxies"),
    ("POST", "/api/origin/aggregates"),
    ("POST", "/api/origin/ip-health"),
    ("POST", "/api/origin/path-breakdown"),
    ("POST", "/api/origin/pop-latency"),
    ("POST", "/api/origin/shielding-analysis"),
    ("POST", "/api/origin/slow-urls"),
    ("POST", "/api/origin/status-codes"),
    ("POST", "/api/origin/summary"),
    ("POST", "/api/origin/timeseries"),
    ("POST", "/api/performance/aggregates"),
    ("POST", "/api/insights"),
    ("POST", "/api/insights/cache-collapse-detail"),
    ("POST", "/api/network-health"),
    ("POST", "/api/network-quality"),
    ("POST", "/api/query"),
    ("POST", "/api/sessions"),
    ("POST", "/api/sessions/detail"),
    ("POST", "/api/cmcd/aggregates"),
    ("POST", "/api/value/summary"),
    ("POST", "/api/web-vitals"),
    ("POST", "/api/ux-events"),
}

_ALL_INVENTORIED = _READ_ALLOWLIST | _WRITE_VERB_GATE_ROUTES


def _is_unauth_path(path: str) -> bool:
    return path in _UNAUTH_ANALYST_PATHS or path.startswith("/api/share/claim/") or path.startswith("/api/share/oauth/")


def _verb_gate_blocks(method: str, substituted_path: str) -> bool:
    """Mirror the read-only-verb gate in ``RemoteAccessMiddleware.dispatch``
    (backend/utils/remote_access.py): PUT/PATCH/DELETE are unconditionally
    refused for analysts, and POST is refused unless the path is unauthenticated
    or starts with an ``_ANALYST_ALLOWED_WRITE_PREFIXES`` entry. Kept in lockstep with that
    logic on purpose — this is what makes ``_WRITE_VERB_GATE_ROUTES`` mean
    "reachable ONLY via this gate" rather than an assumption.
    """
    if method in ("PUT", "PATCH", "DELETE"):
        return True
    if method == "POST":
        if _is_unauth_path(substituted_path):
            return False
        return not any(substituted_path.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES)
    return False


def test_every_real_route_is_blocked_or_inventoried():
    """The tripwire. A route that is neither blocked nor in an inventory
    means someone shipped a new analyst-reachable endpoint without
    consciously classifying it — exactly the RUM/status gap this test
    exists to prevent from recurring silently."""
    uninventoried: list[str] = []
    for method, path in _real_routes():
        substituted = _substitute(path)
        if _is_blocked_path(substituted):
            continue
        if _verb_gate_blocks(method, substituted):
            continue
        if (method, path) in _ALL_INVENTORIED:
            continue
        uninventoried.append(f"{method} {path}")

    assert not uninventoried, (
        "Route(s) reachable by an authenticated analyst with NO explicit classification:\n  "
        + "\n  ".join(uninventoried)
        + "\n\nEither add a blocklist entry in backend/utils/remote_access.py "
        "(_ANALYST_BLOCKED_PREFIXES / _ANALYST_BLOCKED_SUBPATHS / a suffix gate / "
        "_ANALYST_BLOCKED_SUBPATH_REGEX) if the route should be admin-only, "
        "or add it to _READ_ALLOWLIST / _WRITE_VERB_GATE_ROUTES in this file "
        "with a comment explaining why an analyst may reach it."
    )


def test_inventory_has_no_stale_entries():
    """Catches the opposite drift: an inventory entry for a route that no
    longer exists, or one that got blocked out from under the inventory
    (which would make the entry misleadingly imply "reviewed and open" for
    a route that's actually closed). Keeps the inventory an accurate map of
    current behavior rather than an ever-growing junk drawer."""
    real = set(_real_routes())
    stale = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if (m, p) not in real]
    assert not stale, f"Inventory entries for routes that no longer exist in the app: {stale}"

    now_blocked = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if _is_blocked_path(_substitute(p))]
    assert not now_blocked, (
        f"Inventory entries for routes that are now blocked by _is_blocked_path: {now_blocked}. "
        "Remove them from _READ_ALLOWLIST/_WRITE_VERB_GATE_ROUTES — they're covered by the "
        "blocklist now, the inventory entry is stale."
    )


def test_write_gate_routes_actually_rely_on_the_verb_gate():
    """Makes explicit what _WRITE_VERB_GATE_ROUTES membership actually means:
    every entry is a POST route, is NOT individually blocklisted, and is
    reachable ONLY because its path starts with an
    _ANALYST_ALLOWED_WRITE_PREFIXES entry. If a future change moves one of
    these onto an explicit per-route review, move it to _READ_ALLOWLIST
    (renaming aside, that bucket means "reviewed"); if the write-prefix set
    ever stops covering one of these paths the route becomes blocked and
    belongs in neither bucket — test_inventory_has_no_stale_entries catches
    that flip.
    """
    for method, path in _WRITE_VERB_GATE_ROUTES:
        assert method == "POST", f"{method} {path} is in _WRITE_VERB_GATE_ROUTES but isn't a POST route"
        substituted = _substitute(path)
        assert not _is_blocked_path(substituted), (
            f"{method} {path} is individually blocklisted — it doesn't need the verb gate, "
            "this entry is redundant/misclassified"
        )
        assert any(substituted.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES), (
            f"{method} {path} is not covered by any _ANALYST_ALLOWED_WRITE_PREFIXES entry — "
            "it shouldn't be reachable at all, so it doesn't belong in _WRITE_VERB_GATE_ROUTES"
        )


def test_unclassified_route_fails_the_tripwire():
    """Proves the tripwire actually fires. Injects a throwaway route into
    the REAL app's router (the same object test_every_real_route_is_blocked_or_inventoried
    walks) that is neither blocked nor inventoried, re-runs the same
    classification logic inline, and asserts it is reported as
    unclassified. The route is removed in a ``finally`` so it can't leak
    into any other test in the session.

    This is the "prove the test can fail" requirement — a route that is
    analyst-reachable and unclassified must show up, or this tripwire is
    decorative.
    """
    dummy_path = "/api/__tripwire_dummy_unclassified_route__"

    async def _dummy_handler():
        return {"ok": True}

    # Must be a proper APIRoute (via add_api_route), not a bare
    # starlette.routing.Route — FastAPI's OpenAPI generator only inspects
    # APIRoute instances, so a plain Route would silently vanish from
    # app.openapi()["paths"] and this proof-of-mechanism would be testing
    # nothing.
    app.router.add_api_route(dummy_path, _dummy_handler, methods=["GET"])
    # FastAPI caches the generated OpenAPI schema on first access (including
    # from any earlier test in this session) — invalidate it so the schema
    # we inspect actually reflects the just-added dummy route.
    app.openapi_schema = None
    try:
        paths = app.openapi()["paths"]
        assert dummy_path in paths, "test setup bug: dummy route didn't register in the OpenAPI surface"

        found_uninventoried = False
        for method in _HTTP_METHODS:
            if method not in paths[dummy_path]:
                continue
            if _is_blocked_path(dummy_path):
                continue
            if _verb_gate_blocks(method.upper(), dummy_path):
                continue
            if (method.upper(), dummy_path) in _ALL_INVENTORIED:
                continue
            found_uninventoried = True

        assert found_uninventoried, (
            "Injected an unclassified dummy route but the tripwire logic did not flag it — "
            "the tripwire itself is broken"
        )
    finally:
        app.router.routes[:] = [r for r in app.router.routes if getattr(r, "path", None) != dummy_path]
        # FastAPI/Starlette cache the generated OpenAPI schema on first
        # access; clear it so later tests in the same process (or a rerun
        # of this test) see a route table without the dummy entry.
        app.openapi_schema = None
