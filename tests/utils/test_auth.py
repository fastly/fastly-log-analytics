"""Contract tests for backend.utils.auth helpers.

``analyst_allowed_services`` was extracted in commit 793fd6d from seven
inlined sites; it now fans out across alerts/views/scoring routers.
A regression in its three branches — admin-None, analyst-set, or
empty/None service_ids — silently re-opens the cross-tenant gate.
Pin the contract here so the integration suite isn't the only thing
catching it.
"""

from __future__ import annotations

from types import SimpleNamespace

from backend.utils.auth import analyst_allowed_services


def _req(analyst_session: object | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def test_analyst_allowed_services_returns_none_for_admin():
    """Admin sessions don't carry an analyst_session; the helper signals
    "no scope filter" via None so caller code can branch on
    ``allowed is not None``."""
    assert analyst_allowed_services(_req(None)) is None


def test_analyst_allowed_services_returns_set_for_scoped_analyst():
    """An analyst scoped to two services gets a set of those two ids —
    callers use it to filter list/get endpoints."""
    sess = SimpleNamespace(service_ids=["svc-A", "svc-B"])
    assert analyst_allowed_services(_req(sess)) == {"svc-A", "svc-B"}


def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_none():
    """An analyst session without service_ids (defensive null) gets an
    empty set — NOT None. Returning None would collapse to the admin
    branch and grant unrestricted access; an empty set correctly
    blocks every service."""
    sess = SimpleNamespace(service_ids=None)
    assert analyst_allowed_services(_req(sess)) == set()


def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_empty():
    """Same defense as the None case for the [] variant."""
    sess = SimpleNamespace(service_ids=[])
    assert analyst_allowed_services(_req(sess)) == set()
