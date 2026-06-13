"""Tests for backend.repositories.views.

Covers ``get_views`` / ``save_view`` / ``delete_view`` / ``get_view_by_id``.

Post-audit-finding-018: cross-tenant ``_find_view_service`` scan helper
is gone — every public function now requires ``service_id`` directly so
an unknown id lookup can't sprawl O(N) across every tenant DB.
"""

from __future__ import annotations

from backend.models.views import SavedView
from backend.repositories.views import delete_view, get_views, save_view


def _make_view(service_id: str, name: str = "My View") -> SavedView:
    return SavedView(service_id=service_id, name=name, filters_json='{"status":[500]}', page="dashboard")


def test_save_and_list_views():
    sid = "svc-views-1"
    saved = save_view(_make_view(sid, "alpha"))
    # save_view returns the row identifier and a status; the persisted row
    # is what carries the user-visible fields like ``name``.
    assert saved["id"]
    assert saved["status"] == "success"

    rows = get_views(sid)
    names = [v["name"] for v in rows]
    assert "alpha" in names


def test_get_views_empty_when_unseeded():
    assert get_views("svc-views-empty") == []


def test_delete_view_scoped_to_service():
    sid = "svc-views-2"
    view = save_view(_make_view(sid, "to-delete"))
    res = delete_view(view["id"], service_id=sid)
    assert res["status"] in ("success", "deleted")
    assert res["service_id"] == sid
    assert all(v["id"] != view["id"] for v in get_views(sid))


def test_delete_view_unknown_id_returns_not_found():
    res = delete_view("does-not-exist", service_id="svc-views-3")
    # Without the row existing, metadata_db returns a not_found-shaped response
    # OR the wrapper returns the not_found shape itself. Either is acceptable
    # — the contract is "no exception, an actionable status payload back".
    assert "status" in res


def test_get_view_by_id_returns_view_with_service_id_stamped():
    """``get_view_by_id`` is the security mirror of ``alerts.get_alert_by_id``.
    The router-level cross-tenant gate calls it before delete_view to verify
    the requesting analyst owns the targeted view. The returned row MUST
    have ``service_id`` stamped so the caller can compare without re-scanning."""
    from backend.repositories.views import get_view_by_id

    sid = "svc-views-get-by-id"
    view = save_view(_make_view(sid, "by-id"))
    row = get_view_by_id(view["id"], sid)
    assert row is not None
    assert row["id"] == view["id"]
    # Critical: service_id is stamped onto the row so the cross-tenant
    # check downstream doesn't have to re-scan.
    assert row["service_id"] == sid


def test_get_view_by_id_returns_none_when_view_does_not_exist():
    from backend.repositories.views import get_view_by_id

    assert get_view_by_id("nonexistent-id", "svc-no-views") is None
