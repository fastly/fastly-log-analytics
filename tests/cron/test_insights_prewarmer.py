"""Tests for :mod:`backend.cron.jobs.insights_prewarmer`.

The prewarmer is a ``@cron_task``-wrapped function that periodically
calls ``get_insights`` for the default selection (window=1h,
baseline=168h) so the first user after a cold start lands on a warm
cache hit instead of paying the ~3.5 s cold-path cost.

Tests stub out the duckdb helpers, ``get_insights``, and (where
relevant) ``start_cron_run`` to exercise every branch of the
prewarmer body. We unwrap the cron decorator via ``__wrapped__`` so
we exercise the inner function directly without spinning up the
``cron_task`` ThreadPoolExecutor watchdog.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import insights_prewarmer


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    """Make ``get_source_for_service`` return a stable dict."""
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt"}
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: src,
    )
    return src


@pytest.fixture
def stub_cron_run(monkeypatch) -> dict[str, MagicMock]:
    """Mock the cron-run + connection lifecycle so the inner body runs.

    Also stubs the analyst-side dependencies to a deterministic *inactive*
    default (sharing off, no invites) so the admin-only path is exercised
    unless a test explicitly opts into analyst shapes.
    """
    start = MagicMock(return_value=99)
    log = MagicMock()
    con = MagicMock()
    get_conn = MagicMock(return_value=con)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.core.duckdb.get_connection", get_conn)
    monkeypatch.setattr("backend.cron.scheduler._display_label", lambda src, sid: src.get("name", sid))
    mgr = MagicMock()
    mgr.is_sharing_active.return_value = False
    monkeypatch.setattr("backend.utils.tunnel.get_tunnel_manager", lambda: mgr)
    invites = MagicMock(return_value=[])
    monkeypatch.setattr("backend.core.share_db.get_remote_invites", invites)
    return {"start": start, "log": log, "con": con, "get_conn": get_conn, "mgr": mgr, "invites": invites}


# ── source / start-cron-run gating ───────────────────────────────────────────


def test_returns_when_source_missing(monkeypatch):
    """No source = no work. Must not raise, must not call get_insights."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("missing-svc")

    get_insights_mock.assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, stub_source, caplog):
    """``start_cron_run`` raises RuntimeError when another instance of this
    cron is in-flight. The prewarmer should log a skip and return without
    calling get_insights."""
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=RuntimeError("already running")),
    )
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_not_called()
    log_cron.assert_not_called()
    assert any("skipping" in r.message for r in caplog.records)


# ── happy paths: admin warm + analyst shapes ─────────────────────────────────


def test_success_admin_only_logs_and_records(monkeypatch, stub_source, stub_cron_run):
    """Sharing inactive → only the admin/unclamped default is warmed, with
    force_refresh so every tick rewrites the entry (resets the TTL)."""
    get_insights_mock = MagicMock(return_value={"results": [{"foo": 1}]})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    # Called once (admin) with window/baseline pinned to the dashboard defaults
    # and force_refresh=True; no clamp (admin/unclamped key).
    get_insights_mock.assert_called_once()
    args, kwargs = get_insights_mock.call_args
    assert args[0] is stub_cron_run["con"]
    assert args[1] is stub_source
    assert kwargs["window_hours"] == 1.0
    assert kwargs["baseline_hours"] == 168.0
    assert kwargs["force_refresh"] is True
    assert kwargs.get("clamp_cache_key") is None

    # log_cron_run records success + the admin-only summary.
    stub_cron_run["log"].assert_called_once()
    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "success"
    assert "admin + 0 analyst" in log_kwargs["summary"]
    assert log_kwargs["run_id"] == 99

    # Connection closed in finally.
    stub_cron_run["con"].close.assert_called_once()


def test_warms_analyst_shapes_when_sharing_active(monkeypatch, stub_source, stub_cron_run):
    """Sharing active + two distinct active invite shapes → admin warm plus one
    warm per shape, each carrying its stable clamp_cache_key + mask_ips +
    force_refresh."""
    stub_cron_run["mgr"].is_sharing_active.return_value = True
    stub_cron_run["invites"].return_value = [
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-1"],
            "pii_policy": {"mask_ips": True},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 24,
        },
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-1"],
            "pii_policy": {"mask_ips": False},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 168,
        },
    ]
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    # 1 admin + 2 analyst shapes.
    assert get_insights_mock.call_count == 3
    analyst_calls = [c for c in get_insights_mock.call_args_list if c.kwargs.get("clamp_cache_key") is not None]
    assert len(analyst_calls) == 2
    for c in analyst_calls:
        assert c.kwargs["force_refresh"] is True
        assert c.kwargs["clamp_start"] is not None and c.kwargs["clamp_end"] is not None
    # The two shapes warm distinct stable keys + the masking split is preserved.
    keys = {c.kwargs["clamp_cache_key"] for c in analyst_calls}
    assert keys == {"||24", "||168"}
    masks = {c.kwargs["mask_ips"] for c in analyst_calls}
    assert masks == {True, False}

    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "success"
    assert "admin + 2 analyst" in log_kwargs["summary"]


def test_kill_switch_skips_analyst_shapes(monkeypatch, stub_source, stub_cron_run):
    """INSIGHTS_PREWARM_ANALYST=0 warms admin only even with sharing active."""
    monkeypatch.setenv("INSIGHTS_PREWARM_ANALYST", "0")
    stub_cron_run["mgr"].is_sharing_active.return_value = True
    stub_cron_run["invites"].return_value = [
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-1"],
            "pii_policy": {"mask_ips": True},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 24,
        },
    ]
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_called_once()  # admin only
    assert "admin + 0 analyst" in stub_cron_run["log"].call_args.kwargs["summary"]


def test_success_logs_info_line(monkeypatch, stub_source, stub_cron_run, caplog):
    """The success path emits a structured INFO line on the scheduler logger."""
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(return_value={"_is_cached": False}),
    )

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert any("prewarmed in" in r.message for r in caplog.records)


def test_display_label_falls_back_to_service_id(monkeypatch, stub_source, stub_cron_run, caplog):
    """When _display_label returns the service_id itself, no '(svc-id)' suffix."""
    # The prewarmer binds _display_label at module import time, so we
    # patch the prewarmer's own reference rather than the scheduler
    # module. ``_display_label`` is the post-rename name (was
    # ``_display_name``); tests that patch by
    # name must use the current symbol.
    monkeypatch.setattr(
        "backend.cron.jobs.insights_prewarmer._display_label",
        lambda src, sid: sid,
    )
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(return_value={}),
    )

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    # Display string is just 'svc-1' — no parenthesised duplicate.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("svc-1" in m and "(svc-1)" not in m for m in msgs)


# ── error paths ──────────────────────────────────────────────────────────────


def test_get_insights_raises_records_error_no_rethrow(monkeypatch, stub_source, stub_cron_run, caplog):
    """If get_insights blows up, the prewarmer must:
    - not rethrow,
    - record an 'error' cron run with the exception message,
    - emit a WARNING log,
    - still close the connection."""
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(side_effect=RuntimeError("duckdb exploded")),
    )

    with caplog.at_level(logging.WARNING, logger="backend.scheduler"):
        # Must not raise.
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "error"
    assert "duckdb exploded" in log_kwargs["summary"]
    assert log_kwargs["error_message"] == "duckdb exploded"
    assert log_kwargs["run_id"] == 99
    # Warning line emitted.
    assert any("duckdb exploded" in r.message for r in caplog.records)
    # Connection still closed in finally.
    stub_cron_run["con"].close.assert_called_once()


def test_get_connection_raises_records_error(monkeypatch, stub_source, stub_cron_run):
    """get_connection failure also funnels through the error branch.
    Since ``con`` was never assigned, the finally block skips close()."""
    monkeypatch.setattr(
        "backend.core.duckdb.get_connection",
        MagicMock(side_effect=RuntimeError("pool empty")),
    )
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_not_called()
    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "error"
    assert "pool empty" in log_kwargs["error_message"]


def test_connection_close_failure_swallowed(monkeypatch, stub_source, stub_cron_run):
    """A blow-up while closing the connection is suppressed so the cron
    exits cleanly."""
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(return_value={"_is_cached": False}),
    )
    stub_cron_run["con"].close.side_effect = RuntimeError("close failed")

    # Must not raise.
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    stub_cron_run["con"].close.assert_called_once()
    # Success still recorded — the close error doesn't escape.
    log_args, _ = stub_cron_run["log"].call_args
    assert log_args[3] == "success"


# ── _active_analyst_shapes: filtering / dedup / cap ──────────────────────────


def _inv(**over) -> dict:
    base = {
        "revoked": 0,
        "expires_at": None,
        "service_ids": ["svc-1"],
        "pii_policy": {"mask_ips": False},
        "query_start_time": None,
        "query_end_time": None,
        "query_window_hours": None,
    }
    base.update(over)
    return base


def test_active_analyst_shapes_filters_and_dedups(monkeypatch):
    from datetime import UTC, datetime, timedelta

    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    invites = [
        _inv(query_window_hours=24),  # keep
        _inv(query_window_hours=24),  # duplicate → dedup
        _inv(query_window_hours=24, pii_policy={"mask_ips": True}),  # distinct mask
        _inv(revoked=1, query_window_hours=24),  # revoked → drop
        _inv(expires_at=past, query_window_hours=24),  # expired → drop
        _inv(service_ids=["svc-2"], query_window_hours=24),  # other service → drop
    ]
    monkeypatch.setattr("backend.core.share_db.get_remote_invites", lambda: invites)

    shapes = insights_prewarmer._active_analyst_shapes("svc-1")
    assert set(shapes) == {(None, None, 24, False), (None, None, 24, True)}


def test_active_analyst_shapes_caps(monkeypatch, caplog):
    invites = [_inv(query_window_hours=h) for h in range(1, insights_prewarmer._MAX_ANALYST_SHAPES + 5)]
    monkeypatch.setattr("backend.core.share_db.get_remote_invites", lambda: invites)

    with caplog.at_level(logging.WARNING, logger="backend.scheduler"):
        shapes = insights_prewarmer._active_analyst_shapes("svc-1")

    assert len(shapes) == insights_prewarmer._MAX_ANALYST_SHAPES
    assert any("warming first" in r.message for r in caplog.records)
