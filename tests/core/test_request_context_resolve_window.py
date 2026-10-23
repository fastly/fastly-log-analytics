"""``RequestContext.resolve_window``: resolve the keyed token first, clamp to the invite AFTER."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from backend import config as svcconfig
from backend.core.request_context import RequestContext
from backend.utils.remote_access import TimeBounds
from backend.utils.time_window import resolve_window
from tests.conftest import make_test_request_context


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _ctx(time_bounds: TimeBounds | None, analyst: object | None) -> RequestContext:
    return make_test_request_context(
        service_id="svc",
        source={"name": "svc", "service_id": "svc"},
        session=analyst,
        time_bounds=time_bounds,
    )


def _req(**kw):
    base = {"range_token": None, "anchor": None, "start_time": None, "end_time": None}
    base.update(kw)
    return SimpleNamespace(**base)


def test_keyed_token_ignores_frontend_bounds(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_status", lambda name: {})
    anchor = "2026-03-10T12:00:00Z"
    expected = resolve_window("24h", anchor)
    got = _ctx(None, None).resolve_window(
        _req(range_token="24h", anchor=anchor, start_time="2000-01-01T00:00:00Z", end_time="2000-01-02T00:00:00Z")
    )
    assert tuple(_dt(x) for x in got) == tuple(_dt(x) for x in expected)


def test_keyed_token_is_clamped_after_resolve_to_invite_window(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_status", lambda name: {})
    anchor_dt = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    invite_start = anchor_dt - timedelta(hours=2)
    ctx = _ctx(TimeBounds(start=invite_start, end=anchor_dt), SimpleNamespace(session_id="s"))
    start, end = ctx.resolve_window(_req(range_token="30d", anchor="2026-03-10T12:00:00Z"))
    assert _dt(start) >= invite_start
    assert _dt(end) <= anchor_dt


def test_unknown_token_falls_back_to_clamped_absolute_bounds():
    ctx = _ctx(None, None)
    assert ctx.resolve_window(_req(range_token="bogus", start_time=None, end_time=None)) == (None, None)
    start, end = ctx.resolve_window(
        _req(range_token=None, start_time="2026-03-10T00:00:00Z", end_time="2026-03-10T06:00:00Z")
    )
    assert datetime.fromisoformat(start).hour == 0 and datetime.fromisoformat(end).hour == 6
