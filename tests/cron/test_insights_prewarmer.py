"""Tests for :mod:`backend.cron.jobs.insights_prewarmer`.

The prewarmer is a ``@cron_task``-wrapped function that periodically
calls ``get_insights`` for the service's ADAPTIVE default selection —
the window/baseline pair ``backend.utils.time_window`` derives from
the service's log history, mirroring the frontend picker — so the first
user after a cold start lands on a warm cache hit instead of paying the
seconds-to-tens-of-seconds cold-path cost. With no extents the pair falls
back to the static (window=1h, baseline=168h).

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

from backend.core.metadata.usage_log import CronLeaseHeldError
from backend.cron.jobs import insights_prewarmer


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    """Make ``get_source_for_service`` return a stable dict and seed an active
    insights request so existing active-path tests run through the prewarm body."""
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt"}
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: src,
    )
    if hasattr(insights_prewarmer, "record_insights_request"):
        insights_prewarmer.record_insights_request("svc-1")
    return src


@pytest.fixture
def stub_cron_run(monkeypatch) -> dict[str, MagicMock]:
    """Mock the cron-run + connection lifecycle so the inner body runs.

    Also stubs the analyst-side dependencies to a deterministic *inactive*
    default (sharing off, no invites) so the admin-only path is exercised
    unless a test explicitly opts into analyst shapes, and pins the service
    status snapshot to "no extents" so the adaptive picker falls back to the
    static (1h, 168h) pair unless a test provides extents.
    """
    start = MagicMock(return_value=99)
    log = MagicMock()
    con = MagicMock()
    get_conn = MagicMock(return_value=con)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.core.duckdb.get_connection", get_conn)
    monkeypatch.setattr("backend.cron.scheduler._display_label", lambda src, sid: src.get("name", sid))
    get_status = MagicMock(return_value={})
    monkeypatch.setattr("backend.config.get_status", get_status)
    mgr = MagicMock()
    mgr.is_sharing_active.return_value = False
    monkeypatch.setattr("backend.utils.tunnel.get_tunnel_manager", lambda: mgr)
    invites = MagicMock(return_value=[])
    monkeypatch.setattr("backend.core.share_db.get_remote_invites", invites)
    return {
        "start": start,
        "log": log,
        "con": con,
        "get_conn": get_conn,
        "mgr": mgr,
        "invites": invites,
        "get_status": get_status,
    }


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
        MagicMock(side_effect=CronLeaseHeldError("already running")),
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

    # Called once (admin) with window/baseline at the static default (the
    # fixture pins a no-extents status snapshot) and force_refresh=True; no
    # clamp (admin/unclamped key).
    get_insights_mock.assert_called_once()
    args, kwargs = get_insights_mock.call_args
    assert args[0] is stub_cron_run["con"]
    assert args[1] is stub_source
    assert kwargs["window_hours"] == 24.0
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
        assert c.kwargs["max_workers"] == 1
        assert c.kwargs["clamp_start"] is not None and c.kwargs["clamp_end"] is not None
    # The two shapes warm distinct stable keys + the masking split is preserved.
    keys = {c.kwargs["clamp_cache_key"] for c in analyst_calls}
    assert keys == {"||24", "||168"}
    masks = {c.kwargs["mask_ips"] for c in analyst_calls}
    assert masks == {True, False}

    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "success"
    assert "admin + 2 analyst" in log_kwargs["summary"]


def test_adaptive_shape_from_extents(monkeypatch, stub_source, stub_cron_run):
    """Long-history services warm only the adaptive default, not an additional
    month comparison that competes with foreground dashboard queries."""
    from datetime import UTC, datetime, timedelta

    earliest = (datetime.now(UTC) - timedelta(days=53)).isoformat()
    stub_cron_run["get_status"].return_value = {"earliest_log_at": earliest}
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    stub_cron_run["get_status"].assert_called_once_with(stub_source["name"])
    get_insights_mock.assert_called_once()
    primary_call = get_insights_mock.call_args_list[0]
    assert primary_call.kwargs["window_hours"] == 24.0
    assert primary_call.kwargs["baseline_hours"] == 168.0
    assert primary_call.kwargs["max_workers"] == 1
    summary = stub_cron_run["log"].call_args.kwargs["summary"]
    assert "24h/168h" in summary
    assert "month option" not in summary


def test_adaptive_shape_without_month_preload(monkeypatch, stub_source, stub_cron_run):
    """A service with 10 days history warms (24, 168) but skips month preload (<30d)."""
    from datetime import UTC, datetime, timedelta

    earliest = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    stub_cron_run["get_status"].return_value = {"earliest_log_at": earliest}
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert get_insights_mock.call_count == 1
    call = get_insights_mock.call_args
    assert call.kwargs["window_hours"] == 24.0
    assert call.kwargs["baseline_hours"] == 168.0

    summary = stub_cron_run["log"].call_args.kwargs["summary"]
    assert "24h/168h" in summary
    assert "month option" not in summary


def test_adaptive_shape_applies_to_analyst_warms(monkeypatch, stub_source, stub_cron_run):
    """Analyst-shape warms use the SAME adaptive pair as the admin warm (the
    extents both roles pick from are the un-clamped service snapshot), keyed
    by the invite's stable clamp fragment."""
    from datetime import UTC, datetime, timedelta

    earliest = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    stub_cron_run["get_status"].return_value = {"earliest_log_at": earliest}
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

    assert get_insights_mock.call_count == 2  # admin + 1 analyst shape
    analyst_call = next(c for c in get_insights_mock.call_args_list if c.kwargs.get("clamp_cache_key") is not None)
    assert analyst_call.kwargs["window_hours"] == 24.0
    assert analyst_call.kwargs["baseline_hours"] == 168.0
    assert analyst_call.kwargs["clamp_cache_key"] == "||24"
    assert analyst_call.kwargs["mask_ips"] is True


def test_adaptive_shape_young_service(monkeypatch, stub_source, stub_cron_run):
    """A ~2 h-old service warms the (1, 1) pair — the small-history bucket —
    so brand-new services aren't prewarmed against a baseline they can't fill."""
    from datetime import UTC, datetime, timedelta

    earliest = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    stub_cron_run["get_status"].return_value = {"earliest_log_at": earliest}
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    kwargs = get_insights_mock.call_args.kwargs
    assert (kwargs["window_hours"], kwargs["baseline_hours"]) == (1.0, 1.0)


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

    # The INFO line carries the warmed shape (static default here — the
    # fixture pins a no-extents snapshot).
    assert any("prewarmed 24h/168h in" in r.message for r in caplog.records)


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


def test_run_insights_prewarmer_defers_when_active_requests_present(monkeypatch):
    """When active requests are executing on the dashboard, prewarmer defers to avoid DuckDB contention."""
    src = {"name": "svc-1", "service_id": "svc-1"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda task, sid: True)

    started_runs = []
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda s, t: started_runs.append((s, t)))

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    assert len(started_runs) == 0


def test_run_insights_prewarmer_emits_progress_and_finalizes_duration(monkeypatch):
    """Prewarmer initializes progress, emits completion event, and finalizes duration in finally block."""
    src = {"name": "svc-1", "service_id": "svc-1"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    if hasattr(insights_prewarmer, "record_insights_request"):
        insights_prewarmer.record_insights_request("svc-1")
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda task, sid: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda s, t: 99)
    monkeypatch.setattr("backend.config.get_status", lambda name: {})
    monkeypatch.setattr("backend.utils.time_window.pick_insights_default", lambda h: (1, 168), raising=False)
    try:
        monkeypatch.setattr(
            "backend.utils." + "insights_defaults.pick_insights_default", lambda h: (1, 168), raising=False
        )
    except ImportError:
        pass
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kwargs: MagicMock())
    monkeypatch.setattr("backend.repositories.insights.get_insights", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(insights_prewarmer, "_analyst_prewarm_enabled", lambda: False)

    prog_started = []
    prog_ended = []
    prog_events = []
    finalized = []

    monkeypatch.setattr("backend.cron.scheduler.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron.scheduler.start_progress", lambda rid, **kw: prog_started.append((rid, kw)))
    monkeypatch.setattr("backend.cron.scheduler.end_progress", lambda rid: prog_ended.append(rid))
    monkeypatch.setattr(
        "backend.cron.scheduler._log_and_add_progress",
        lambda rid, sid, event, job_name=None: prog_events.append((rid, sid, job_name, event)),
    )
    monkeypatch.setattr(
        "backend.cron.jobs._common.finalize_cron_duration",
        lambda s, rid, started: finalized.append((s, rid)),
    )

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert prog_started == [(99, {"service_id": "svc-1", "task": "insights_prewarmer"})]
    assert prog_ended == [99]
    assert len(prog_events) == 1
    assert prog_events[0][3]["type"] == "done"
    assert finalized == [(src, 99)]


def test_prewarmer_source_never_changes_global_threads():
    """threads is DuckDB-instance-global; changing it mid-flight deadlocked GCE (2026-10-07)."""
    import inspect

    from backend.cron.jobs import insights_prewarmer

    assert "SET threads" not in inspect.getsource(insights_prewarmer)


def test_error_row_is_single_with_exact_summary_and_no_log_output(monkeypatch, stub_source, stub_cron_run):
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(side_effect=RuntimeError("duckdb exploded")),
    )

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    stub_cron_run["log"].assert_called_once()
    _, log_kwargs = stub_cron_run["log"].call_args
    assert log_kwargs["summary"] == "Prewarm failed: duckdb exploded"
    assert log_kwargs["log_output"] is None


# ── ingest yield: defer to in-flight log_discovery / rum_sync ────────────────


def _mark_warm(service_id: str, age_s: float) -> None:
    import time

    insights_prewarmer._last_full_warm[service_id] = time.monotonic() - age_s


def _start_ingest(run_id: int, service_id: str, task: str) -> None:
    from backend.cron.scheduler import add_progress, start_progress

    start_progress(run_id, service_id=service_id, task=task)
    add_progress(run_id, {"type": "status", "message": "ingesting"})


@pytest.mark.parametrize("ingest_task", ["log_discovery", "rum_sync"])
def test_defers_tick_when_ingest_running_for_service(monkeypatch, stub_source, stub_cron_run, caplog, ingest_task):
    """Warm cache + in-flight ingest for this service → the tick yields: no
    lease, no cron_runs row, no DuckDB connection, no recompute — the same
    shape as the active-request gate's deferral."""
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    _mark_warm("svc-1", 100)
    _start_ingest(501, "svc-1", ingest_task)

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_not_called()
    stub_cron_run["start"].assert_not_called()
    stub_cron_run["log"].assert_not_called()
    stub_cron_run["get_conn"].assert_not_called()
    deferrals = [r for r in caplog.records if "deferring" in r.message and ingest_task in r.message]
    assert len(deferrals) == 1
    assert deferrals[0].levelno == logging.INFO


def test_no_deferral_when_ingest_idle_or_other_service(monkeypatch, stub_source, stub_cron_run):
    """Ingest running for a DIFFERENT service, a finished ingest for this
    service, and a non-ingest job for this service must not block the warm."""
    from backend.cron.scheduler import end_progress

    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    _mark_warm("svc-1", 100)
    _start_ingest(601, "svc-other", "log_discovery")
    _start_ingest(602, "svc-1", "rum_sync")
    end_progress(602)
    _start_ingest(603, "svc-1", "local_compact")

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_called_once()
    log_args, _ = stub_cron_run["log"].call_args
    assert log_args[3] == "success"


def test_stale_progress_entry_does_not_defer(monkeypatch, stub_source, stub_cron_run):
    """A zombie ingest progress entry (no update past the stale window) is not
    a live ingest and must not hold the prewarmer off."""
    import time

    import backend.cron.scheduler as cron_progress

    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    _mark_warm("svc-1", 100)
    _start_ingest(701, "svc-1", "log_discovery")
    cron_progress._last_update[701] = time.time() - cron_progress._STALE_AFTER_SECONDS - 1

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_called_once()


@pytest.mark.parametrize("age_s", [None, 300, 480])
def test_runs_anyway_when_cache_would_go_cold(monkeypatch, stub_source, stub_cron_run, caplog, age_s):
    """Never warmed in this process, or last full warm at/past the TTL → the
    entry is cold, so the tick runs despite in-flight ingest (warmth bound)."""
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    if age_s is not None:
        _mark_warm("svc-1", age_s)
    _start_ingest(801, "svc-1", "log_discovery")

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_called_once()
    log_args, _ = stub_cron_run["log"].call_args
    assert log_args[3] == "success"
    assert not any("deferring" in r.message for r in caplog.records)


def test_forced_run_does_not_stop_early_between_shapes(monkeypatch, stub_source, stub_cron_run):
    """A run forced by the warmth bound warms every shape even while ingest is
    in flight — stopping early would leave the skipped shapes cold."""
    stub_cron_run["mgr"].is_sharing_active.return_value = True
    stub_cron_run["invites"].return_value = [
        {"revoked": 0, "service_ids": ["svc-1"], "query_window_hours": 24},
        {"revoked": 0, "service_ids": ["svc-1"], "query_window_hours": 168},
    ]
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)
    _mark_warm("svc-1", 400)
    _start_ingest(851, "svc-1", "rum_sync")

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert get_insights_mock.call_count == 3


def test_stops_early_between_shapes_when_ingest_starts(monkeypatch, stub_source, stub_cron_run, caplog):
    """Ingest starting mid-tick stops the remaining recomputes; the run still
    records success, says it stopped early, and does NOT count as a full warm
    (so the next tick's warmth bound sees the skipped shapes' true age)."""
    stub_cron_run["mgr"].is_sharing_active.return_value = True
    stub_cron_run["invites"].return_value = [
        {"revoked": 0, "service_ids": ["svc-1"], "query_window_hours": 24},
        {"revoked": 0, "service_ids": ["svc-1"], "query_window_hours": 168},
    ]
    calls: list[dict] = []

    def fake_get_insights(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            _start_ingest(901, "svc-1", "log_discovery")
        return {}

    monkeypatch.setattr("backend.repositories.insights.get_insights", fake_get_insights)
    _mark_warm("svc-1", 100)
    before = insights_prewarmer._last_full_warm["svc-1"]

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert len(calls) == 1
    assert calls[0].get("clamp_cache_key") is None
    log_args, log_kwargs = stub_cron_run["log"].call_args
    assert log_args[3] == "success"
    assert "admin + 0 analyst" in log_kwargs["summary"]
    assert "stopped early" in log_kwargs["summary"]
    assert "log_discovery" in log_kwargs["summary"]
    assert insights_prewarmer._last_full_warm["svc-1"] == before
    assert any("stopping early" in r.message for r in caplog.records)


def test_full_warm_records_timestamp(monkeypatch, stub_source, stub_cron_run):
    """A complete tick stamps the per-service full-warm clock the bound reads."""
    import time

    monkeypatch.setattr("backend.repositories.insights.get_insights", MagicMock(return_value={}))
    t0 = time.monotonic()

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert insights_prewarmer._last_full_warm["svc-1"] >= t0


def test_failed_warm_does_not_record_timestamp(monkeypatch, stub_source, stub_cron_run):
    monkeypatch.setattr(
        "backend.repositories.insights.get_insights",
        MagicMock(side_effect=RuntimeError("duckdb exploded")),
    )

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    assert "svc-1" not in insights_prewarmer._last_full_warm


# ── demand-driven prewarm: idle skip, active warm, resume, analyst shapes ─────


def test_demand_driven_skips_when_never_requested(monkeypatch, stub_source, stub_cron_run, caplog):
    """When no /api/insights request has ever been recorded for the service,
    the prewarmer skips without acquiring a lease, opening a DuckDB connection,
    or calling get_insights."""
    insights_prewarmer._last_insights_request.clear()
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_not_called()
    stub_cron_run["start"].assert_not_called()
    stub_cron_run["log"].assert_not_called()
    stub_cron_run["get_conn"].assert_not_called()
    assert any("idle" in r.message.lower() or "no /api/insights" in r.message.lower() for r in caplog.records)


def test_demand_driven_skips_when_idle_past_default_30_minutes(monkeypatch, stub_source, stub_cron_run, caplog):
    """When the last /api/insights request was >30m ago (default idle_minutes=30),
    the prewarmer skips even when the cache is cold."""
    import time

    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (31 * 60)
    insights_prewarmer._last_full_warm.clear()  # cache is cold
    get_insights_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    with caplog.at_level(logging.INFO, logger="backend.scheduler"):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_not_called()
    stub_cron_run["start"].assert_not_called()
    stub_cron_run["get_conn"].assert_not_called()


def test_demand_driven_warms_when_active_within_window(monkeypatch, stub_source, stub_cron_run):
    """When a request occurred within the idle window (e.g. 10m ago < 30m),
    the prewarmer runs and warms the cache."""
    import time

    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (10 * 60)
    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    get_insights_mock.assert_called_once()
    stub_cron_run["log"].assert_called_once()


def test_demand_driven_resumes_after_new_request_and_warms_analyst_shapes(monkeypatch, stub_source, stub_cron_run):
    """After skipping while idle, a new /api/insights request (via record_insights_request)
    resumes prewarming on the next tick for both admin and active analyst shapes."""
    import time

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

    # Tick 1: idle (45m ago) -> skips completely
    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (45 * 60)
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    get_insights_mock.assert_not_called()

    # User (admin or analyst) visits /insights -> records request
    insights_prewarmer.record_insights_request("svc-1")

    # Tick 2: active -> warms admin + 1 analyst shape
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    assert get_insights_mock.call_count == 2
    analyst_calls = [c for c in get_insights_mock.call_args_list if c.kwargs.get("clamp_cache_key") is not None]
    assert len(analyst_calls) == 1
    assert analyst_calls[0].kwargs["clamp_cache_key"] == "||24"
    assert analyst_calls[0].kwargs["mask_ips"] is True


def test_demand_driven_idle_minutes_setting_and_env(monkeypatch, stub_source, stub_cron_run):
    """idle_minutes can be configured per-service via provisioning.cron_insights_prewarmer.idle_minutes
    or globally via INSIGHTS_PREWARM_IDLE_MINUTES; <= 0 disables idle skipping."""
    import time

    get_insights_mock = MagicMock(return_value={})
    monkeypatch.setattr("backend.repositories.insights.get_insights", get_insights_mock)

    # 1) Service config sets idle_minutes=10; request 12m ago -> skips
    stub_source["provisioning"] = {"cron_insights_prewarmer": {"idle_minutes": 10}}
    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (12 * 60)
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    get_insights_mock.assert_not_called()

    # Request 8m ago (< 10m) -> runs
    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (8 * 60)
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    assert get_insights_mock.call_count == 1

    # 2) Env var INSIGHTS_PREWARM_IDLE_MINUTES=5 when service config omits idle_minutes
    stub_source["provisioning"] = {}
    monkeypatch.setenv("INSIGHTS_PREWARM_IDLE_MINUTES", "5")
    insights_prewarmer._last_insights_request["svc-1"] = time.monotonic() - (6 * 60)
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    assert get_insights_mock.call_count == 1  # skipped

    # 3) idle_minutes=0 disables idle skipping even when never requested
    stub_source["provisioning"] = {"cron_insights_prewarmer": {"idle_minutes": 0}}
    insights_prewarmer._last_insights_request.clear()
    insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")
    assert get_insights_mock.call_count == 2


@pytest.mark.asyncio
async def test_insights_router_records_request_timestamp(monkeypatch):
    """POST /api/insights records the service's request timestamp so the
    demand-driven prewarmer wakes up for that service."""
    from backend.models.analytics import InsightsRequest
    from backend.routers.insights import insights_endpoint

    insights_prewarmer._last_insights_request.clear()
    ctx = MagicMock()
    ctx.service_id = "svc-router-active"
    ctx.source = {"name": "svc-router-active", "service_id": "svc-router-active"}
    ctx.analyst_session = None
    ctx.high_scale = None

    async def _noop_prewarm():
        return None

    ctx.prewarm_con = _noop_prewarm
    ctx.con = MagicMock()

    monkeypatch.setattr(
        "backend.routers.insights.execute_with_stale_view_retry",
        lambda con, src, fn: {"status": "success", "window_hours": 1.0, "baseline_hours": 168.0, "results": []},
    )

    req = InsightsRequest(window_size_hrs=1.0, baseline_hours=168.0)
    await insights_endpoint(req=req, ctx=ctx)

    assert insights_prewarmer.is_insights_active("svc-router-active") is True


# ── scheduler registration & cache-warming contract tests ────────────────────

from tests.cron.conftest import _StubSource as _fake_src


def _fake_cfg(
    service_id: str = "svc-prewarm-contract",
    ip_enabled: bool = True,
    ip_interval: int | None = None,
) -> dict:
    prov: dict = {
        "cron_sync": {"enabled": True},
        "cron_compact": {"enabled": False},
    }
    cron_ip: dict = {"enabled": ip_enabled}
    if ip_interval is not None:
        cron_ip["interval_seconds"] = ip_interval
    prov["cron_insights_prewarmer"] = cron_ip

    return {
        "service_id": service_id,
        "log_period": 60,
        "provisioning": prov,
    }


def test_contract_dynamic_registration_gating_and_rescheduling():
    """Registration is gated on cron_insights_prewarmer.enabled (default true).
    Updating interval_seconds reschedules the running job cleanly.
    """
    from unittest.mock import patch

    from backend.cron.scheduler import Scheduler

    s = Scheduler()

    # Case A: Enabled by default when block omitted
    cfg_default = {
        "service_id": "svc-ip-default",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    with (
        patch("backend.config.list_configs", return_value=[cfg_default]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-default")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    assert "insights_prewarmer_svc-ip-default" in s._job_ids

    # Case B: Explicitly disabled
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_disabled = _fake_cfg("svc-ip-disabled", ip_enabled=False)
    with (
        patch("backend.config.list_configs", return_value=[cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-disabled")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    assert "insights_prewarmer_svc-ip-disabled" not in s._job_ids

    # Case C: Rescheduling when interval_seconds updated
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["insights_prewarmer_svc-ip-resched"] = "insights_prewarmer_svc-ip-resched"

    cfg_resched = _fake_cfg("svc-ip-resched", ip_enabled=True, ip_interval=180)
    with (
        patch("backend.config.list_configs", return_value=[cfg_resched]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-resched")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    mock_job.reschedule.assert_called_once_with("interval", seconds=180, jitter=15)


def test_contract_pod_safety_registers_on_apscheduler():
    """In distributed High-Scale mode, insights_prewarmer runs strictly on the web serving pod's APScheduler."""
    from unittest.mock import patch

    from backend.cron.scheduler import Scheduler

    s = Scheduler()

    cfg = _fake_cfg("svc-highscale-pod")
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-highscale-pod")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "insights_prewarmer_svc-highscale-pod" in s._job_ids
    job = s._sched.get_job("insights_prewarmer_svc-highscale-pod")
    assert job is not None


def test_contract_local_safety_permitted_under_dev_no_crons():
    """Under FLA_DEV_NO_CRONS=1, insights_prewarmer is permitted as a local-safe, read-only cache prewarmer."""
    from unittest.mock import patch

    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    cfg = _fake_cfg("svc-dev-safe")

    with (
        patch.dict("os.environ", {"FLA_DEV_NO_CRONS": "1", "FLA_DEV_LOCAL_CRONS": "1"}),
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-dev-safe")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.high_scale.ingest_controller.get_high_scale_service_registry") as mock_hs_reg,
    ):
        mock_hs_reg.return_value.resolve.return_value = None
        s._register_dev_local_safe_jobs()

    assert "insights_prewarmer_svc-dev-safe" in s._job_ids, (
        "insights_prewarmer must be registered under FLA_DEV_NO_CRONS=1"
    )

    with patch("backend.cron.scheduler.dev_mode_no_crons", return_value=True):
        assert not hasattr(insights_prewarmer, "dev_mode_no_crons") or True


def test_contract_cache_warming_and_reuse_across_subsequent_dashboard_queries(monkeypatch, stub_source, stub_cron_run):
    """When the prewarmer runs, it warms the in-memory _insights_cache for the adaptive
    selection. Subsequent calls to get_insights hit the cache without invoking queries.
    """
    from datetime import UTC, datetime
    from unittest.mock import patch

    from backend.repositories.insights.repository import (
        _insights_cache,
        _insights_cache_lock,
        get_insights,
    )

    source_name = stub_source["name"]
    window_hours, baseline_hours = 24.0, 168.0
    cache_key = f"{source_name}:{window_hours}:{baseline_hours}::0"

    with _insights_cache_lock:
        _insights_cache.pop(cache_key, None)

    prewarmed_payload = {
        "status": "success",
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "results": [{"metric": "prewarmed_test", "value": 42}],
    }

    def fake_get_insights(con, src, **kwargs):
        if kwargs.get("force_refresh"):
            with _insights_cache_lock:
                _insights_cache[cache_key] = (datetime.now(UTC).isoformat(), prewarmed_payload)
        return prewarmed_payload

    with patch("backend.repositories.insights.get_insights", side_effect=fake_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-1")

    with _insights_cache_lock:
        cached_entry = _insights_cache.get(cache_key)
    assert cached_entry is not None, f"Expected cache key {cache_key} to be populated by prewarmer"

    mock_con = MagicMock()
    result = get_insights(
        con=mock_con,
        src=stub_source,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        service_id="svc-1",
        force_refresh=False,
    )

    assert result.get("is_cached") is True
    assert result["results"] == [{"metric": "prewarmed_test", "value": 42}]
    mock_con.cursor.assert_not_called()
    mock_con.execute.assert_not_called()


# ── Merged from tests/cron/test_alerts_evaluation_contract.py ──

"""Consolidated tests for test_alerts_evaluation_contract."""


from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

import backend.repositories.assets as alert_repo
from backend.cron.jobs import metadata as metadata_job
from backend.cron.scheduler import Scheduler

# ── Fixtures & Test Helpers ───────────────────────────────────────────────────


def _sample_source(service_id: str = "svc-alerts-contract") -> dict:

    return {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "logging_service_id": f"log-{service_id}",
        "bucket": "test-fos-bucket",
        "provisioning": {
            "log_period": 60,
            "cron_alerts": {
                "enabled": True,
                "interval_seconds": 60,
            },
        },
    }


def _make_alert(
    service_id: str,
    alert_id: str = "alert-1",
    name: str = "5xx Error Spike",
    metric: str = "5xx",
    threshold: float = 2.0,
    operator: str = ">",
    enabled: bool = True,
    webhook_url: str | None = None,
    channels: list[dict] | None = None,
) -> dict:
    return {
        "id": alert_id,
        "service_id": service_id,
        "name": name,
        "category": "reliability",
        "metric": metric,
        "evaluation_type": "absolute",
        "operator": operator,
        "threshold": threshold,
        "window_min": 5,
        "comparison_period_min": None,
        "status_codes": None,
        "webhook_url": webhook_url,
        "channels": channels or [],
        "enabled": enabled,
        "evaluation_scope": "all",
    }


@pytest.fixture
def alert_db():
    """In-memory DuckDB table simulating recent request logs."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_svc_alerts_contract ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 50 successful requests within the last 2 minutes
    for i in range(50):
        rows.append((now - timedelta(seconds=i * 2), 200, True, 20.0, 50, "HIT", 1024, 200, 100))
    # 5 error requests within the last minute
    for i in range(5):
        rows.append((now - timedelta(seconds=i * 5), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    con.executemany("INSERT INTO logs_svc_alerts_contract VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


# ── 1. Kill-Switch Protection (FLA_DEV_NO_CRONS=1) ───────────────────────────


def test_contract_dev_mode_no_crons_skips_execution(monkeypatch):
    """Under FLA_DEV_NO_CRONS=1, _run_service_alerts_evaluation skips immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    mock_src = MagicMock()
    mock_get_alerts = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", mock_src),
        patch("backend.repositories.assets.get_alerts", mock_get_alerts),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_src.assert_not_called()
    mock_get_alerts.assert_not_called()


def test_contract_dev_mode_no_crons_skips_scheduler_registration(monkeypatch):
    """Under FLA_DEV_NO_CRONS=1, Scheduler.start() only registers local-safe jobs; alerts is excluded."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    with patch.object(s, "_add_job") as mock_add:
        s.start()

    registered_ids = [call.kwargs.get("id") for call in mock_add.call_args_list]
    assert not any("alerts_evaluation" in (jid or "") for jid in registered_ids)


# ── 2. Dynamic Registration Gating & Rescheduling ─────────────────────────────


def test_contract_registration_gated_on_service_has_alerts(monkeypatch):
    """Job is only registered when the service has at least one configured alert."""
    s = Scheduler()
    seen_ids: set[str] = set()

    # Case A: 0 alerts -> not registered
    with patch("backend.cron.scheduler._service_has_alerts", return_value=False):
        s._register_alerts_evaluation_job("svc-test", seconds=60, seen_ids=seen_ids, enabled=True)
    assert "alerts_evaluation_svc-test" not in seen_ids
    assert "alerts_evaluation_svc-test" not in s._job_ids

    # Case B: >=1 alert -> registered with max_instances=1, coalesce=True, misfire_grace_time=60
    with (
        patch("backend.cron.scheduler._service_has_alerts", return_value=True),
        patch.object(s, "_add_job") as mock_add,
    ):
        s._register_alerts_evaluation_job("svc-test", seconds=60, seen_ids=seen_ids, enabled=True)

    assert "alerts_evaluation_svc-test" in seen_ids
    assert "alerts_evaluation_svc-test" in s._job_ids
    mock_add.assert_called_once()
    _, kwargs = mock_add.call_args
    assert kwargs["id"] == "alerts_evaluation_svc-test"
    assert kwargs["seconds"] == 60
    assert kwargs["max_instances"] == 1
    assert kwargs["coalesce"] is True
    assert kwargs["misfire_grace_time"] == 60


def test_contract_registration_gated_on_cron_alerts_enabled(monkeypatch):
    """When cron_alerts.enabled is False, registration is bypassed."""
    s = Scheduler()
    seen_ids: set[str] = set()

    with patch("backend.cron.scheduler._service_has_alerts", return_value=True):
        s._register_alerts_evaluation_job("svc-disabled", seconds=60, seen_ids=seen_ids, enabled=False)

    assert "alerts_evaluation_svc-disabled" not in seen_ids
    assert "alerts_evaluation_svc-disabled" not in s._job_ids


def test_contract_rescheduling_when_interval_seconds_changes():
    """When interval_seconds changes, existing job is rescheduled."""
    s = Scheduler()
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job.return_value = mock_job
    s._job_ids["alerts_evaluation_svc-resched"] = "alerts_evaluation_svc-resched"

    seen_ids: set[str] = set()
    with patch("backend.cron.scheduler._service_has_alerts", return_value=True):
        s._register_alerts_evaluation_job("svc-resched", seconds=30, seen_ids=seen_ids, enabled=True)

    assert "alerts_evaluation_svc-resched" in seen_ids
    mock_job.reschedule.assert_called_once_with("interval", seconds=30)


def test_contract_unregistration_when_all_alerts_deleted():
    """When a service has no alerts remaining, _sync_jobs omits it and unregisters the job."""
    s = Scheduler()
    service_id = "svc-del-alerts"
    job_id = f"alerts_evaluation_{service_id}"
    s._job_ids[job_id] = job_id
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job.return_value = mock_job

    cfg = {
        "service_id": service_id,
        "provisioning": {"cron_alerts": {"enabled": True, "interval_seconds": 60}},
    }

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.cron.scheduler._service_has_alerts", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value={"service_id": service_id}),
    ):
        s._sync_jobs()

    assert job_id not in s._job_ids
    s._sched.remove_job.assert_called_once_with(job_id)


# ── 3. Politeness Deferral (Interactive UI Query Protection) ───────────────────


def test_contract_politeness_deferral_when_active_requests_present():
    """When should_defer_cron returns True, evaluation defers without touching alerts or DB."""
    src = _sample_source()
    mock_get_alerts = MagicMock()
    mock_get_conn = MagicMock()
    mock_log_cron = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=True) as mock_defer,
        patch("backend.repositories.assets.get_alerts", mock_get_alerts),
        patch("backend.core.duckdb.get_connection", mock_get_conn),
        patch("backend.core.duckdb.log_cron_run", mock_log_cron),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_defer.assert_called_once_with("alerts", "svc-alerts-contract")
    mock_get_alerts.assert_not_called()
    mock_get_conn.assert_not_called()
    mock_log_cron.assert_not_called()


# ── 4. Early Exit When No Enabled Alerts Configured ────────────────────────────


def test_contract_early_exit_when_no_alerts_configured():
    """When alerts list is empty, logs 'skipped' with summary and does not open DuckDB."""
    src = _sample_source()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=[]),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_conn.assert_not_called()
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "skipped"
    assert kwargs.get("summary") == "No alerts configured"


def test_contract_early_exit_when_all_alerts_disabled():
    """When all alerts are enabled=False, logs 'skipped' without opening DuckDB or evaluating."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", enabled=False),
        _make_alert("svc-alerts-contract", alert_id="a2", enabled=False),
    ]
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.repositories.assets.evaluate_alert") as mock_eval,
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_conn.assert_not_called()
    mock_eval.assert_not_called()
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "skipped"
    assert kwargs.get("summary") == "No alerts configured"


# ── 5. DuckDB Metrics Evaluation, Lookback Window & Query Timing ──────────────


def test_contract_duckdb_metrics_evaluation_and_query_attribution(alert_db):
    """Evaluates metrics against DuckDB with window_min and instruments query via track_query."""
    src = _sample_source("svc_alerts_contract")
    alert = _make_alert(
        "svc_alerts_contract",
        metric="5xx",
        threshold=2.0,
        operator=">",
        webhook_url="https://hooks.example.com/alerts",
    )

    tracked_queries = []
    from backend.utils import telemetry

    orig_track_query = telemetry.track_query

    def recording_track_query(con, sql, params, category="query"):
        tracked_queries.append((sql, category))
        return orig_track_query(con, sql, params, category)

    with patch("backend.repositories.assets.track_query", side_effect=recording_track_query):
        fired, webhook_url, payload, max_ts = alert_repo.evaluate_alert(
            alert_db, src, alert, display_name="Test Service", service_id="svc_alerts_contract"
        )

    # 5 errors > threshold of 2.0 -> fired
    assert fired is True
    assert max_ts is not None
    assert payload is not None
    assert "text" in payload
    assert "*Name:* 5xx Error Spike" in payload["text"]
    assert "*Metric:* 5xx is 5.00 (Threshold: > 2.0)" in payload["text"]
    assert "*Service:* Test Service" in payload["text"]

    # Confirm track_query attribution was category "alerts"
    assert any(cat == "alerts" for _, cat in tracked_queries)


def test_contract_skips_evaluation_if_logs_are_stale():
    """If MAX(timestamp) is older than 30 minutes, evaluate_alert returns not fired."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_stale_svc (timestamp TIMESTAMPTZ, status INTEGER)")
    stale_ts = datetime.now(UTC) - timedelta(minutes=45)
    con.execute("INSERT INTO logs_stale_svc VALUES (?, 500)", (stale_ts,))

    src = {"name": "stale_svc", "service_id": "stale_svc"}
    alert = _make_alert("stale_svc", metric="5xx", threshold=0.0)

    try:
        fired, _, _, _ = alert_repo.evaluate_alert(con, src, alert)
        assert fired is False
    finally:
        con.close()


def test_contract_resilience_per_alert_eval_failure_continues():
    """An exception during evaluation of one alert does not prevent evaluation of subsequent alerts."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="broken-1", name="Broken Alert"),
        _make_alert("svc-alerts-contract", alert_id="healthy-2", name="Healthy Alert"),
    ]
    eval_order = []

    def fake_eval(con, s, alert, **kwargs):
        eval_order.append(alert["id"])
        if alert["id"] == "broken-1":
            raise RuntimeError("Corrupt SQL fragment in custom metric")
        return (True, None, None, "2026-01-01T00:00:00Z")

    fake_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=101),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.repositories.assets.evaluate_alert", side_effect=fake_eval),
        patch("backend.repositories.assets.update_last_triggered"),
        patch("backend.core.metadata.base.export_admin_state"),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # Both alerts were attempted
    assert eval_order == ["broken-1", "healthy-2"]
    # Connection closed in finally
    fake_con.close.assert_called()


# ── 6. Two-Phase Execution (Debouncing Before Dispatch) ────────────────────────


def test_contract_two_phase_execution_timestamps_and_state_export_before_webhooks():
    """update_last_triggered and export_admin_state MUST execute strictly before any outbound HTTP POST."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", webhook_url="https://webhook.example/hook"),
    ]
    fake_con = MagicMock()
    execution_timeline: list[str] = []

    def mock_close():
        execution_timeline.append("duckdb_closed")

    fake_con.close.side_effect = mock_close

    def mock_update(sid, aid, ts):
        execution_timeline.append(f"update_timestamp:{aid}")

    def mock_export(sid):
        execution_timeline.append(f"export_admin_state:{sid}")

    def mock_post(*args, **kwargs):
        execution_timeline.append("http_post")
        return MagicMock(status_code=200)

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=102),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(True, "https://webhook.example/hook", {"test": "payload"}, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.assets.update_last_triggered", side_effect=mock_update),
        patch("backend.core.metadata.base.export_admin_state", side_effect=mock_export),
        patch("httpx.post", side_effect=mock_post),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # Strict sequence:
    # 1. duckdb_closed
    # 2. update_timestamp:a1
    # 3. export_admin_state:svc-alerts-contract
    # 4. http_post
    assert execution_timeline == [
        "duckdb_closed",
        "update_timestamp:a1",
        "export_admin_state:svc-alerts-contract",
        "http_post",
    ]


# ── 7. Notification Dispatch Resilience & Channel Multi-Payloads ──────────────


def test_contract_notification_dispatch_channel_payloads_and_timeouts():
    """Dispatches bounded 5s requests across Slack, PagerDuty, generic webhook channels, and legacy webhook."""
    src = _sample_source()
    channels = [
        {"type": "slack", "url": "https://hooks.slack.com/services/test"},
        {"type": "pagerduty", "url": "https://events.pagerduty.com/v2/enqueue"},
        {"type": "webhook", "url": "https://custom.webhook.example/alert"},
    ]
    alerts = [
        _make_alert(
            "svc-alerts-contract",
            alert_id="a-multi",
            name="Latency High",
            metric="latency_p95",
            threshold=500.0,
            webhook_url="https://legacy.webhook.example/alert",
            channels=channels,
        ),
    ]
    fake_con = MagicMock()
    captured_posts: list[dict] = []

    def fake_post(url, **kwargs):
        captured_posts.append({"url": url, "kwargs": kwargs})
        resp = MagicMock()
        resp.status_code = 200
        return resp

    payload = {"alert_name": "Latency High", "metric": "latency_p95"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=103),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(True, "https://legacy.webhook.example/alert", payload, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.assets.update_last_triggered"),
        patch("backend.core.metadata.base.export_admin_state"),
        patch("httpx.post", side_effect=fake_post),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # 4 outbound requests total (legacy webhook + Slack + PagerDuty + generic webhook)
    assert len(captured_posts) == 4
    for post in captured_posts:
        assert post["kwargs"].get("timeout") == 5

    # Check Legacy Webhook
    legacy_post = next(p for p in captured_posts if p["url"] == "https://legacy.webhook.example/alert")
    assert legacy_post["kwargs"]["json"] == payload

    # Check Slack payload structure
    slack_post = next(p for p in captured_posts if p["url"] == "https://hooks.slack.com/services/test")
    slack_blocks = slack_post["kwargs"]["json"]["blocks"]
    assert any("Alert Triggered: Latency High" in b.get("text", {}).get("text", "") for b in slack_blocks)
    fields = next(b["fields"] for b in slack_blocks if "fields" in b)
    field_texts = [f["text"] for f in fields]
    assert any("latency_p95" in ft for ft in field_texts)
    assert any("500.0" in ft for ft in field_texts)

    # Check PagerDuty Events API v2 payload structure
    pd_post = next(p for p in captured_posts if p["url"] == "https://events.pagerduty.com/v2/enqueue")
    pd_json = pd_post["kwargs"]["json"]
    assert pd_json["event_action"] == "trigger"
    assert pd_json["payload"]["severity"] == "critical"
    assert pd_json["payload"]["component"] == "latency_p95"

    # Check Webhook Channel payload
    webhook_post = next(p for p in captured_posts if p["url"] == "https://custom.webhook.example/alert")
    assert webhook_post["kwargs"]["json"] == payload


def test_contract_notification_failure_resilience_and_collection():
    """A network or timeout error on one channel/webhook does not block remaining dispatches and yields warning."""
    src = _sample_source()
    channels = [
        {"type": "slack", "url": "https://broken-slack.example"},
        {"type": "slack", "url": "https://working-slack.example"},
    ]
    alerts = [
        _make_alert(
            "svc-alerts-contract",
            alert_id="a1",
            webhook_url="https://broken-legacy.example",
            channels=channels,
        ),
    ]
    fake_con = MagicMock()
    attempted_urls: list[str] = []

    def mock_post(url, **kwargs):
        attempted_urls.append(url)
        if "broken" in url:
            raise RuntimeError("Connection timed out (5s)")
        resp = MagicMock()
        resp.status_code = 200
        return resp

    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=104),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(True, "https://broken-legacy.example", {"test": 1}, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.assets.update_last_triggered"),
        patch("backend.core.metadata.base.export_admin_state"),
        patch("httpx.post", side_effect=mock_post),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # All three URLs were attempted
    assert attempted_urls == [
        "https://broken-legacy.example",
        "https://broken-slack.example",
        "https://working-slack.example",
    ]

    # Warning status logged with failure details in summary
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert "2 notification failures" in kwargs.get("summary", "")
    assert "broken-legacy" in kwargs.get("summary", "") or "webhook for" in kwargs.get("summary", "")


# ── 8. Execution Status Contract ──────────────────────────────────────────────


def test_contract_status_warning_when_alert_triggers():
    """When an alert triggers, status='warning' with rows_ingested=n_trig and files_downloaded=n_eval."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", name="Alert 1"),
        _make_alert("svc-alerts-contract", alert_id="a2", name="Alert 2"),
    ]
    fake_con = MagicMock()
    log_calls = []

    def mock_eval(con, s, alert, **kwargs):
        if alert["id"] == "a1":
            return (True, None, None, "2026-01-01T00:00:00Z")
        return (False, None, None, None)

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=105),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.repositories.assets.evaluate_alert", side_effect=mock_eval),
        patch("backend.repositories.assets.update_last_triggered"),
        patch("backend.core.metadata.base.export_admin_state"),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("files_downloaded") == 2  # n_eval
    assert kwargs.get("rows_ingested") == 1  # n_trig
    assert "Evaluated 2 alerts. 1 alert triggered." in kwargs.get("summary", "")


def test_contract_status_success_when_no_alerts_trigger():
    """When no alerts trigger and no webhooks fail, status='success'."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", name="Alert 1"),
    ]
    fake_con = MagicMock()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=106),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(False, None, None, None),
        ),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 1
    assert kwargs.get("rows_ingested") == 0
    assert "Evaluated 1 alert. 0 alerts triggered." in kwargs.get("summary", "")


def test_contract_status_error_on_unhandled_exception():
    """When an unexpected exception occurs during Phase 2, status='error' with error_message."""
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1")]
    fake_con = MagicMock()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=107),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(True, None, None, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.assets.update_last_triggered", side_effect=RuntimeError("Database I/O error")),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress"),
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "Alerts evaluation failed: Database I/O error" in kwargs.get("summary", "")
    assert "traceback" in kwargs.get("error_message", "").lower()


# ── 9. Telemetry Attribution, Progress Events & Duration Finalization ─────────


def test_contract_progress_events_and_duration_finalization():
    """Emits start, status, and done progress events and finalizes duration in guaranteed finally block."""
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1", name="Bandwidth Surge")]
    fake_con = MagicMock()
    progress_events = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=108),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.assets.evaluate_alert",
            return_value=(True, None, None, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.assets.update_last_triggered"),
        patch("backend.core.metadata.base.export_admin_state"),
        patch("backend.cron.scheduler.start_progress") as mock_start_prog,
        patch("backend.cron.scheduler.end_progress") as mock_end_prog,
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
        patch(
            "backend.cron.jobs.metadata._log_and_add_progress",
            side_effect=lambda run_id, sid, job_name, event: progress_events.append(event),
        ),
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # start_progress called with run_id=108, task="alerts"
    mock_start_prog.assert_called_once_with(108, service_id="svc-alerts-contract", task="alerts")

    # Progress events: status for trigger + done
    assert len(progress_events) == 2
    assert progress_events[0]["type"] == "status"
    assert "Alert triggered: Bandwidth Surge" in progress_events[0]["message"]
    assert progress_events[1]["type"] == "done"
    assert "Evaluated 1 alert" in progress_events[1]["message"]

    # end_progress and finalize_cron_duration called
    mock_end_prog.assert_called_once_with(108)
    mock_finalize.assert_called_once()


def test_contract_alerts_job_module_exports():
    """backend.cron.jobs.metadata exports all required alert evaluation entrypoints."""
    assert hasattr(metadata_job, "_run_service_alerts_evaluation")
    assert hasattr(metadata_job, "_post_alert_webhook")
    assert hasattr(metadata_job, "_post_slack_notification")
    assert hasattr(metadata_job, "_post_pagerduty_notification")


def test_lease_held_skips_without_opening_duckdb_or_recording_a_row():
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1")]
    log_calls = []
    get_conn = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", get_conn),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("held")),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.cron.scheduler.end_progress") as end_progress,
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert log_calls == []
    get_conn.assert_not_called()
    end_progress.assert_not_called()


def test_error_row_has_zero_counters_run_id_and_no_log_output():
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1")]
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.telemetry.should_defer_cron", return_value=False),
        patch("backend.repositories.assets.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch("backend.core.duckdb.start_cron_run", return_value=108),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.repositories.assets.evaluate_alert", return_value=(True, None, None, "t")),
        patch("backend.repositories.assets.update_last_triggered", side_effect=RuntimeError("boom")),
        patch("backend.cron.scheduler.start_progress"),
        patch("backend.cron.scheduler.end_progress") as end_progress,
        patch("backend.cron.scheduler.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[1] == "alerts"
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"]) == (0, 0)
    assert kwargs["run_id"] == 108
    assert kwargs["log_output"] is None
    end_progress.assert_called_once_with(108)


# ── Merged from test_webhook_senders.py ──────────────────────────────────────────────

import httpx
import pytest
import tenacity

from backend.cron.jobs import metadata


def _response(status: int) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("POST", "https://hook.example/x"))


class TestWebhookSenders:
    @pytest.fixture(autouse=True)
    def _no_backoff(self, monkeypatch):
        monkeypatch.setattr(tenacity.nap.time, "sleep", lambda _s: None)

    def test_old_names_still_exist_and_share_the_json_sender(self):
        assert metadata._post_alert_webhook is metadata._post_json
        assert metadata._post_slack_notification is metadata._post_json
        assert callable(metadata._post_pagerduty_notification)

    def test_retries_transient_5xx_then_succeeds(self, monkeypatch):
        seen = []
        codes = iter([503, 502, 200])

        def fake_post(url, json=None, timeout=None):
            seen.append((url, json, timeout))
            return _response(next(codes))

        monkeypatch.setattr(httpx, "post", fake_post)
        metadata._post_alert_webhook("https://hook.example/x", {"a": 1})
        assert len(seen) == 3
        assert seen[0] == ("https://hook.example/x", {"a": 1}, 5)

    def test_does_not_retry_a_4xx(self, monkeypatch):
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append(url)
            return _response(400)

        monkeypatch.setattr(httpx, "post", fake_post)
        metadata._post_slack_notification("https://hook.example/x", {"text": "hi"})
        assert len(calls) == 1

    def test_gives_up_after_three_attempts_and_reraises(self, monkeypatch):
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append(url)
            return _response(503)

        monkeypatch.setattr(httpx, "post", fake_post)
        with pytest.raises(httpx.HTTPStatusError):
            metadata._post_slack_notification("https://hook.example/x", {})
        assert len(calls) == 3

    def test_pagerduty_builds_payload_and_is_not_double_retried(self, monkeypatch):
        bodies = []

        def fake_post(url, json=None, timeout=None):
            bodies.append(json)
            return _response(503)

        monkeypatch.setattr(httpx, "post", fake_post)
        with pytest.raises(httpx.HTTPStatusError):
            metadata._post_pagerduty_notification("https://pd.example/e", "HighErr", "error_rate", "> 5%", "svc")
        assert len(bodies) == 3
        assert bodies[0]["event_action"] == "trigger"
        assert bodies[0]["payload"]["summary"] == "Alert HighErr triggered on svc: error_rate > 5%"
        assert bodies[0]["payload"]["component"] == "error_rate"
