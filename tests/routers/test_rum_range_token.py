"""range_token/anchor resolution on GET /rum/analytics (additive; legacy start/end unchanged)."""

from __future__ import annotations

import asyncio
import typing
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.core.request_context import RequestContext
from backend.main import app
from backend.routers import rum as rum_router
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import TimeBounds, get_analyst_time_bounds
from backend.utils.time_window import resolve_window
from tests.routers import test_rum_analytics as _base

client = TestClient(app)
setup_temp_rum_db = _base.setup_temp_rum_db
SERVICE_ID = "svc-rum-token"
ANCHOR = "2026-08-05T12:00:30Z"
EMPTY = {"no_data": True, "vitals_rows": []}


@pytest.fixture(autouse=True)
def _skip_view_update():
    with patch("backend.core.iceberg.view.update_iceberg_view"):
        yield


@pytest.fixture
def captured(setup_temp_rum_db):
    calls: list[dict] = []

    def fake(con, **kw):
        calls.append(kw)
        return EMPTY

    with patch("backend.routers.rum.rum_repo.fetch_rum_analytics", fake):
        yield calls


def _get(**params):
    r = client.get(f"/api/services/{SERVICE_ID}/rum/analytics", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def test_token_resolves_same_window_as_dashboard_helper(captured) -> None:
    _get(range_token="24h", anchor=ANCHOR)
    exp_start, exp_end = resolve_window("24h", ANCHOR)
    assert (captured[0]["start_time"], captured[0]["end_time"]) == (exp_start, exp_end)
    assert exp_end == "2026-08-05T12:00:00Z"


def test_token_ignores_supplied_absolute_bounds(captured) -> None:
    _get(range_token="1h", anchor=ANCHOR, start_time="2000-01-01T00:00:00Z", end_time="2000-01-02T00:00:00Z")
    assert (captured[0]["start_time"], captured[0]["end_time"]) == resolve_window("1h", ANCHOR)


def test_absent_token_uses_supplied_bounds_unchanged(captured) -> None:
    _get(start_time="2026-08-05T00:00:00Z", end_time="2026-08-05T23:59:59Z", anchor=ANCHOR)
    assert (captured[0]["start_time"], captured[0]["end_time"]) == ("2026-08-05T00:00:00Z", "2026-08-05T23:59:59Z")


def test_invalid_token_falls_back_to_supplied_bounds(captured) -> None:
    _get(range_token="bogus", anchor=ANCHOR, start_time="2026-08-05T00:00:00Z", end_time="2026-08-05T01:00:00Z")
    assert (captured[0]["start_time"], captured[0]["end_time"]) == ("2026-08-05T00:00:00Z", "2026-08-05T01:00:00Z")


def _ctx(source: dict, session: object | None, bounds: TimeBounds) -> RequestContext:
    from tests.conftest import make_test_request_context

    return make_test_request_context(
        service_id=SERVICE_ID,
        source=source,
        session=session,
        read_only=True,
        time_bounds=bounds,
    )


@pytest.mark.security_regression
def test_analyst_token_window_is_clamped_to_invite(setup_temp_rum_db, captured) -> None:
    """A 30d token must never widen an analyst past their 1h invite window."""
    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    req = SimpleNamespace(state=SimpleNamespace(analyst_session=session))
    bounds = get_analyst_time_bounds(typing.cast(typing.Any, req))
    source = {
        "name": "test_service",
        "service_id": "test_service",
        "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    def run(ctx: RequestContext, request: object) -> None:
        asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, request),
                start_time=None,
                end_time=None,
                filters=None,
                ctx=ctx,
                range_token="30d",
                anchor=None,
            )
        )

    run(_ctx(source, session, bounds), req)
    run(_ctx(source, None, TimeBounds(None, None)), SimpleNamespace(state=SimpleNamespace(analyst_session=None)))

    def span_h(c: dict) -> float:
        s, e = parse_iso_utc(c["start_time"]), parse_iso_utc(c["end_time"])
        assert s is not None and e is not None
        return (e - s).total_seconds() / 3600

    assert span_h(captured[0]) <= 1.01, "analyst token window not clamped to invite"
    assert span_h(captured[1]) > 24 * 29, "admin negative control should get the full 30d window"
