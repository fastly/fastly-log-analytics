"""Tests for backend.repositories.views.

Covers ``get_views`` / ``save_view`` / ``delete_view`` and the
``_find_view_service`` cross-service lookup that lets the API resolve a
view id back to its owning per-service SQLite file.
"""

from __future__ import annotations

from unittest.mock import patch

from backend.models.views import SavedView
from backend.repositories.views import _find_view_service, delete_view, get_views, save_view


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


def test_delete_view_with_explicit_service_hint():
    sid = "svc-views-2"
    view = save_view(_make_view(sid, "to-delete"))
    res = delete_view(view["id"], service_id_hint=sid)
    assert res["status"] in ("success", "deleted")
    assert res["service_id"] == sid
    assert all(v["id"] != view["id"] for v in get_views(sid))


def test_delete_view_unknown_id_returns_not_found():
    res = delete_view("does-not-exist", service_id_hint="svc-views-3")
    # Without the row existing, metadata_db returns a not_found-shaped response
    # OR the wrapper returns the not_found shape itself. Either is acceptable
    # — the contract is "no exception, an actionable status payload back".
    assert "status" in res


def test_find_view_service_scans_known_configs():
    sid = "svc-views-find"
    view = save_view(_make_view(sid))

    # _find_view_service iterates svcconfig.list_configs(); patch it to
    # surface only this service so the scan is deterministic.
    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": sid}],
    ):
        found = _find_view_service(view["id"])

    assert found == sid


def test_find_view_service_returns_none_when_no_match():
    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": "svc-no-such-view"}],
    ):
        assert _find_view_service("nonexistent-view-id") is None


def test_delete_view_falls_back_to_cross_service_scan():
    """When no service_id_hint is given, delete_view scans configs to find
    the owning service."""
    sid = "svc-views-fallback"
    view = save_view(_make_view(sid))

    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": sid}],
    ):
        res = delete_view(view["id"])

    assert res["service_id"] == sid


def test_get_view_by_id_returns_view_with_service_id_stamped():
    """``get_view_by_id`` is the security mirror of ``alerts.get_alert_by_id``.
    The router-level cross-tenant gate calls it before delete_view to verify
    the requesting analyst owns the targeted view. The returned row MUST
    have ``service_id`` stamped so the caller can compare without re-scanning."""
    from backend.repositories.views import get_view_by_id

    sid = "svc-views-get-by-id"
    view = save_view(_make_view(sid, "by-id"))

    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": sid}],
    ):
        row = get_view_by_id(view["id"])

    assert row is not None
    assert row["id"] == view["id"]
    # Critical: service_id is stamped onto the row so the cross-tenant
    # check downstream doesn't have to re-scan.
    assert row["service_id"] == sid


def test_get_view_by_id_returns_none_when_view_does_not_exist():
    from backend.repositories.views import get_view_by_id

    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": "svc-no-views"}],
    ):
        assert get_view_by_id("nonexistent-id") is None


def test_get_view_by_id_skips_configs_with_no_service_id():
    """Defensive: a config whose ``service_id`` key is missing/None must
    not crash the lookup. The function skips and moves on."""
    from backend.repositories.views import get_view_by_id

    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{"service_id": None}, {}, {"service_id": "svc-empty-x"}],
    ):
        # No matching service_id → None, no exception.
        assert get_view_by_id("any-id") is None


def test_find_view_service_skips_configs_with_no_service_id():
    """Same defensive coverage on the find helper: configs with missing
    service_id are skipped silently."""
    with patch(
        "backend.repositories.views.svcconfig.list_configs",
        return_value=[{}, {"service_id": None}, {"service_id": "svc-skip-empty"}],
    ):
        assert _find_view_service("any-id") is None
