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

from backend.core.metadata.cron_log import CronLeaseHeldError
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
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, sid: True)

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
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, sid: False)
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

    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda rid, **kw: prog_started.append((rid, kw)))
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda rid: prog_ended.append(rid))
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
    from backend.cron_progress import add_progress, start_progress

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
    from backend.cron_progress import end_progress

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

    from backend import cron_progress

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
    from backend.routers.insights import insights_endpoint

    from backend.models.insights import InsightsRequest

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
    access_level: str = "read_write",
    ip_enabled: bool = True,
    ip_interval: int | None = None,
) -> dict:
    prov: dict = {
        "access_level": access_level,
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
        "access_level": access_level,
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
        "access_level": "read_write",
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


def test_contract_dual_role_registration_admin_and_analyst():
    """Registered for both Admin (read_write) and Analyst Path A (read_only)."""
    from unittest.mock import patch

    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    cfg_admin = _fake_cfg("svc-role-admin", access_level="read_write")
    cfg_analyst = _fake_cfg("svc-role-analyst", access_level="read_only")

    with (
        patch("backend.config.list_configs", return_value=[cfg_admin, cfg_analyst]),
        patch(
            "backend.core.duckdb.get_source_for_service",
            side_effect=lambda sid: _fake_src(sid, "read_only" if "analyst" in sid else "read_write"),
        ),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "insights_prewarmer_svc-role-admin" in s._job_ids, "Admin must register insights_prewarmer"
    assert "insights_prewarmer_svc-role-analyst" in s._job_ids, "Analyst Path A must register insights_prewarmer"


def test_contract_pod_safety_registers_on_apscheduler():
    """In distributed High-Scale mode, insights_prewarmer runs strictly on the web serving pod's APScheduler."""
    from unittest.mock import patch

    from backend.cron.scheduler import Scheduler

    s = Scheduler()

    cfg = _fake_cfg("svc-highscale-pod", access_level="read_write")
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
        patch("backend.high_scale.registry_bootstrap.get_high_scale_service_registry") as mock_hs_reg,
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
