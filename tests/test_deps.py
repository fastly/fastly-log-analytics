"""Tests for ``backend.deps`` — FastAPI dependency injection helpers.

Every route handler in the app depends on at least one of these. Their
behaviour is mostly about *resolution priority* (which header wins,
when does config-fallback fire) and *error mapping* (which exceptions
become which HTTP status codes). Both are easy to get wrong in a
refactor and silent to break, so each branch is pinned here.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend import deps
from backend.core.duckdb import DBBusyError


def _call_get_service_id(service=None, sid=None, x_fastly_service_id=None, x_service_id=None) -> str | None:
    """Call get_service_id directly without going through FastAPI's DI.

    The signature uses ``Query(default=None)`` / ``Header(default=None)``
    as parameter defaults — those are ``Query``/``Header`` instances,
    not None — so calling the function bare would fail the truthiness
    check. This helper bypasses by always passing all four explicitly.
    """
    return deps.get_service_id(
        service=service,
        sid=sid,
        x_fastly_service_id=x_fastly_service_id,
        x_service_id=x_service_id,
    )


# ── get_service_id: resolution priority ──────────────────────────────────────


def test_get_service_id_returns_explicit_id_when_config_loads():
    """When the provided id resolves to a real config, return it as-is."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "abc"}):
        assert _call_get_service_id(sid="abc") == "abc"


def test_get_service_id_query_param_service_takes_priority():
    """``?service=`` is checked first — used by the frontend's URL state
    machine. Other params are tried in order: sid, x-fastly, x-service."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "from-query"}):
        out = _call_get_service_id(
            service="from-query",
            sid="from-sid",
            x_fastly_service_id="from-fastly-header",
            x_service_id="from-svc-header",
        )
    assert out == "from-query"


def test_get_service_id_falls_back_to_x_fastly_header():
    """When no query params present, the x-fastly-service-id header wins."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "hdr"}):
        out = _call_get_service_id(x_fastly_service_id="hdr")
    assert out == "hdr"


def test_get_service_id_x_service_id_header_used_as_last_resort():
    """x-service-id is the fallback when nothing else is set — kept for
    backwards-compat with older clients that don't send the Fastly-
    namespaced header."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "old"}):
        out = _call_get_service_id(x_service_id="old")
    assert out == "old"


def test_get_service_id_resolves_cdn_service_id_to_logging_service_id():
    """A CDN service has its own Fastly service ID; when the frontend
    sends that, deps must look it up via ``cdn_service_id`` and return
    the *logging* service ID the rest of the app uses as the key."""
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value={"cdn-svc-1": "log-svc-1"}),
    ):
        assert _call_get_service_id(sid="cdn-svc-1") == "log-svc-1"


def test_get_service_id_passes_through_unknown_id_when_no_cdn_match():
    """Unknown id with no cdn match → return it anyway (don't 404 at the
    dep layer). Downstream ``get_source`` is what turns "unknown" into
    a 400 — keeping the resolution stage permissive lets routes that
    don't need a source still serve requests."""
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value={}),
    ):
        assert _call_get_service_id(sid="ghost") == "ghost"


def test_get_service_id_falls_back_to_active_when_nothing_provided():
    """No params/headers + active service set → return the active id."""
    with patch("backend.deps.svcconfig.get_active_service_id", return_value="active-svc"):
        assert _call_get_service_id() == "active-svc"


def test_get_service_id_returns_none_when_no_active_and_nothing_provided():
    """Bare install with no service configured → None. Routes that
    can handle None (like ``/api/presets``) return empty; routes that
    require it (via ``get_source``) raise 400."""
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=None):
        assert _call_get_service_id() is None


# ── get_source: source lookup + 400 on miss ──────────────────────────────────


def test_get_source_returns_source_dict_when_found():
    fake_src = {"name": "logs_svc", "service_id": "svc"}
    with patch("backend.deps.db.get_source_for_service", return_value=fake_src):
        assert deps.get_source(service_id="svc") is fake_src


def test_get_source_raises_400_when_no_service_id():
    """No active service → 400 with ``no_service: true`` in the detail.
    The frontend keys on ``no_service`` to render the "configure first"
    onboarding state instead of a generic error banner."""
    with pytest.raises(HTTPException) as exc:
        deps.get_source(service_id=None)
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


def test_get_source_raises_400_when_lookup_returns_none():
    """Service id present but no source found (stale frontend URL after
    a teardown) → 400, same error shape as no-service-at-all."""
    with patch("backend.deps.db.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            deps.get_source(service_id="ghost")
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── _ConnectionHolder: enter / DBBusyError / exit ────────────────────────────
#
# The pool path (``backend.core.duckdb_pool``) keeps connections alive across
# requests and ultimately calls ``backend.core.duckdb.get_connection`` rather
# than ``backend.deps.get_connection`` — so a mock at the deps-level reference
# doesn't intercept. Tests below that pin the always-fresh open/close
# lifecycle disable the pool via ``DUCKDB_CONNECTION_POOL=0``; this preserves
# the legacy contract assertions while keeping pool-specific behaviour in
# its own targeted tests further down.


@pytest.fixture
def disable_pool(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    yield


def test_connection_holder_opens_and_closes(disable_pool):
    """Happy path: enter opens a connection via ``get_connection``,
    exit closes it cleanly."""
    fake_con = MagicMock()
    src = {"name": "x"}

    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder(src, read_only=True)
        con = holder.__enter__()
        assert con is fake_con
        # The kwargs the holder passes are part of the contract with get_connection
        mock_get.assert_called_once_with(source=src, max_wait=10, skip_view_update=False, read_only=True)
        holder.__exit__(None, None, None)
        fake_con.close.assert_called_once()


def test_connection_holder_skip_view_update_forwarded():
    """``skip_view_update=True`` (used by metadata routes) must propagate
    to ``get_connection``; otherwise admin routes would block on
    Iceberg manifest reads."""
    with patch("backend.deps.get_connection") as mock_get:
        holder = deps._ConnectionHolder({"name": "x"}, skip_view_update=True)
        holder.__enter__()
        assert mock_get.call_args.kwargs["skip_view_update"] is True


def test_connection_holder_db_busy_maps_to_503(disable_pool):
    """``DBBusyError`` from get_connection → 503 with ``busy: true``.
    The 503 (rather than 500/400) is what makes the frontend's React
    Query layer keep its cached data instead of clearing the UI."""
    with patch("backend.deps.get_connection", side_effect=DBBusyError("locked")):
        holder = deps._ConnectionHolder({"name": "x"})
        with pytest.raises(HTTPException) as exc:
            holder.__enter__()
    assert exc.value.status_code == 503
    assert exc.value.detail["busy"] is True


def test_connection_holder_exit_tolerates_close_failure(disable_pool):
    """If ``close()`` raises (locked DB, double-close), the dependency
    must still tear down cleanly — otherwise the request fails AFTER
    the response was already sent."""
    fake_con = MagicMock()
    fake_con.close.side_effect = RuntimeError("already closed")

    with patch("backend.deps.get_connection", return_value=fake_con):
        holder = deps._ConnectionHolder({"name": "x"})
        holder.__enter__()
        holder.__exit__(None, None, None)  # must not raise


def test_connection_holder_exit_with_no_open_connection_is_noop():
    """If __enter__ never ran (or already exited), __exit__ should
    be a no-op rather than NoneType.close()."""
    holder = deps._ConnectionHolder({"name": "x"})
    # Never entered — con is None
    holder.__exit__(None, None, None)  # must not raise


# ── get_con: generator-style dependency ──────────────────────────────────────


def test_get_con_yields_connection_and_closes_after(disable_pool):
    """``get_con`` is a generator dep — FastAPI calls .send(None), gets
    the connection, then calls .close() after the response. The con
    must be closed at that point."""
    fake_con = MagicMock()

    with patch("backend.deps.get_connection", return_value=fake_con):
        gen = deps.get_con(source={"name": "x"})
        con = next(gen)
        assert con is fake_con
        # Drain the generator → triggers __exit__
        with pytest.raises(StopIteration):
            next(gen)
        fake_con.close.assert_called_once()


def test_get_meta_con_symbol_removed():
    """v2.0 cut: ``get_meta_con`` was deleted. The pool fingerprint check
    in ``duckdb_pool.checkout_connection`` skips ``update_iceberg_view``
    when the (view-cache identity, buffer mtime) tuple is unchanged, so
    the dedicated skip-view-update dep that bootstrap routes used is
    no longer needed. Pin removal so a future refactor doesn't quietly
    re-introduce it."""
    assert not hasattr(deps, "get_meta_con"), (
        "get_meta_con was removed at the v2.0 cut. Routes that used it should use "
        "get_con instead; the pool fingerprint check makes the skip-view "
        "optimization unnecessary."
    )


def test_get_con_default_is_read_only(disable_pool):
    """Dashboard queries default to read_only=True — otherwise long-running
    cron writes would block them. Pinning this default so a future
    refactor doesn't silently flip it."""
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        gen = deps.get_con(source={"name": "x"})
        next(gen)
        assert mock_get.call_args.kwargs["read_only"] is True


# ── _ConnectionHolder: pool path ─────────────────────────────────────────────


def test_connection_holder_pool_path_checks_out_and_returns():
    """When the pool is enabled (default), read_only requests route through
    ``duckdb_pool.checkout_connection`` instead of calling get_connection
    directly. The holder must still expose the connection on ``self.con``
    and let the pool reclaim it on exit (not call ``close`` on the
    connection itself — that would defeat reuse)."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()

    # Build a context manager stub the holder's __enter__ will drive.
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm) as mock_checkout:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        con = holder.__enter__()
        assert con is fake_con
        mock_checkout.assert_called_once()
        # Exit must forward to the pool context manager (which knows whether
        # to return-vs-discard); the holder itself MUST NOT call con.close.
        holder.__exit__(None, None, None)
        cm.__exit__.assert_called_once()
        fake_con.close.assert_not_called()


def test_connection_holder_pool_path_skipped_when_skip_view_update():
    """``skip_view_update`` paths can't pool — they need a fresh, un-bound
    connection. Verify the holder falls through to the legacy code path
    for that case even when the pool is enabled."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()
    with (
        patch.object(duckdb_pool, "checkout_connection") as mock_checkout,
        patch("backend.deps.get_connection", return_value=fake_con) as mock_get,
    ):
        holder = deps._ConnectionHolder({"name": "x"}, skip_view_update=True)
        holder.__enter__()
        mock_checkout.assert_not_called()
        mock_get.assert_called_once()


def test_connection_holder_pool_path_discards_on_error():
    """If the request raised mid-query, the pool must mark the connection
    errored on exit so a poisoned connection doesn't get reused. We rely
    on passing the original exc_type to the pool's __exit__."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm):
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder.__enter__()
        # Simulate request error
        holder.__exit__(RuntimeError, RuntimeError("boom"), None)
        # Pool CM sees the exc_type — its release() uses that to discard
        called_exc_type = cm.__exit__.call_args[0][0]
        assert called_exc_type is RuntimeError


# ── AnalyticsDeps: removed at v2.0 cut ───────────────────────────────────────


def test_analytics_deps_symbol_removed():
    """v2.0 cut Phase 8: the bundled ``AnalyticsDeps`` (get_source + get_con)
    was replaced by :class:`backend.core.request_context.RequestContext`
    via ``Depends(build_request_context)``. The new dep enforces analyst
    tenancy structurally (the old bundle skipped it because
    ``require_service_access`` was never wired as a sibling dep on any
    route). Pin removal so a refactor doesn't quietly re-introduce it."""
    assert not hasattr(deps, "AnalyticsDeps"), (
        "AnalyticsDeps was removed at v2.0 cut. Routes use "
        "RequestContext via Depends(build_request_context); access "
        "ctx.source / ctx.con / ctx.service_id."
    )
