"""Tests for :mod:`backend.cron.jobs.sync`.

Pins the orchestration shape of ``_run_service_cron`` (per-tick FOS
ingest) and ``_run_full_sweep`` (daily catch-net LIST). Both jobs are
heavy wrappers around ``backend.core.ingest.ingest`` — these tests stub
the ingest generator and assert the surrounding event/log/skip/error
plumbing.

Late imports inside the function bodies (``from backend.core.duckdb
import refresh_config_status``) are patched at the source module
(``backend.core.duckdb.refresh_config_status``) — that path resolves the
name on each call, which is what the late ``from … import …`` statement
actually performs. Module-level imports — ``_log_and_add_progress``,
``_check_disk_space``, ``_extract_log_text`` from
``backend.cron.scheduler`` — must be patched on
``backend.cron.jobs.sync`` itself because the name was bound there at
import time and the function body never re-resolves it.

The ``_usage_log_phase`` inner helper defined inside
``_run_service_cron`` cannot be intercepted at module level (it is a
local closure). Its body's source-module imports
(``backfill_fastly_edge_writes``, ``reconcile_fastly_stats``,
``run_usage_log_cleanup``) ARE patchable on their source modules and we
stub them out everywhere ``_run_service_cron`` is exercised so the
phase becomes a no-op that doesn't leak DB I/O.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

# ── shared fixtures ─────────────────────────────────────────────────────────


def _fake_src(service_id: str = "svc-1", access_level: str = "read_write") -> dict:
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "fos-test-bkt",
        "access_level": access_level,
    }


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    """Stub the cron_progress lifecycle + module-level shim helpers.

    The progress helpers (``start_progress`` / ``end_progress`` /
    ``cleanup_progress_and_reap``) are imported lazily inside
    ``_run_service_cron``; patch their source module so the late
    ``from backend.cron_progress import …`` resolves to the mocks.

    The display/event/log helpers are bound at module level on
    ``backend.cron.jobs.sync`` (via ``from backend.cron.scheduler
    import …``); patch them where the name lives, not where it
    originated.
    """
    # Ensure the module is imported so monkeypatch.setattr's dotted
    # path resolver can find it as an attribute of ``backend.cron.jobs``.
    from backend.cron.jobs import sync as sync_mod  # noqa: F401

    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    log_event = MagicMock()

    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)

    monkeypatch.setattr(sync_mod, "_log_and_add_progress", log_event)
    monkeypatch.setattr(sync_mod, "_display_label", lambda src, sid: src.get("name", sid))
    monkeypatch.setattr(sync_mod, "_extract_log_text", lambda rid: "")
    # Default to "disk OK" so happy-path tests don't trip on the disk gate.
    monkeypatch.setattr(
        sync_mod,
        "_check_disk_space",
        lambda cache_dir, sid, name: (True, ""),
    )
    monkeypatch.setattr(sync_mod, "_claim_heavy_refresh", lambda sid: False)

    return {
        "start_progress": start_progress,
        "end_progress": end_progress,
        "cleanup": cleanup,
        "log_event": log_event,
    }


@pytest.fixture
def stub_usage_log_phase(monkeypatch) -> None:
    """No-op the helpers called inside the inner ``_usage_log_phase``.

    Patched on their source modules because the closure re-imports them
    on every invocation."""
    monkeypatch.setattr("backend.core.duckdb.backfill_fastly_edge_writes", MagicMock(return_value=0))
    monkeypatch.setattr("backend.core.duckdb.reconcile_fastly_stats", MagicMock(return_value=0))
    monkeypatch.setattr("backend.utils.usage_logger.run_usage_log_cleanup", MagicMock())


@pytest.fixture
def stub_post_ingest(monkeypatch) -> None:
    """Stub the post-ingest helpers that are late-imported by the body.

    Targets the source modules — that is where late ``from … import …``
    looks up the names on each call."""
    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", MagicMock())
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    # Late ``from backend.cron.jobs._common import …`` — patch on source
    # module so the import resolves to the mock.
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    # Dashboard cache invalidation is best-effort and wrapped in a bare
    # except — stub the symbols so the invalidate path runs cleanly.
    monkeypatch.setattr("backend.repositories.dashboard._dashboard_cache", {}, raising=False)
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", MagicMock(), raising=False)


@pytest.fixture
def stub_load_config(monkeypatch):
    """``svcconfig.load_config`` returns a minimal read-write config by
    default. Tests override per-case via the returned mock."""
    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    load = MagicMock(return_value=cfg)
    save = MagicMock()
    monkeypatch.setattr("backend.config.load_config", load)
    monkeypatch.setattr("backend.config.save_config", save)
    return {"load": load, "save": save, "cfg": cfg}


def _make_ingest_events(events: list[dict]) -> Any:
    """Yield the canned events, mimicking the ingest generator."""

    def _gen(*args, **kwargs):
        yield from events

    return _gen


# ── _run_service_cron ────────────────────────────────────────────────────────


def test_returns_when_should_defer_cron_true(monkeypatch, stub_load_config):
    """Active-request gate fires → ingest never called."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: True)
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)

    sync_mod._run_service_cron.__wrapped__("svc-1")

    ingest_mock.assert_not_called()
    # Config load never even runs — gate fires before.
    stub_load_config["load"].assert_not_called()
    get_src.assert_not_called()


def test_skips_when_config_missing(monkeypatch):
    """``svcconfig.load_config`` → None → warn + return without
    touching source-lookup."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)

    sync_mod._run_service_cron.__wrapped__("ghost-svc")

    get_src.assert_not_called()


def test_skips_when_source_missing(monkeypatch, stub_load_config):
    """Config present but source builder returns None → early return."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=None))
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)

    sync_mod._run_service_cron.__wrapped__("svc-1")

    start_cron.assert_not_called()


def test_read_only_skipped_without_force(monkeypatch, stub_load_config):
    """read_only source without ``force=True`` → early return before
    ``start_cron_run``."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src(access_level="read_only")),
    )
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)

    sync_mod._run_service_cron.__wrapped__("svc-1", force=False)

    start_cron.assert_not_called()


def test_read_only_runs_with_force(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """``force=True`` lets a read-only source proceed past the gate
    and actually invoke ``ingest``."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src(access_level="read_only")),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    # Simulate a no-data ingest (one done event, new_files=0).
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0}]),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1", force=True)

    # Ingest path ran: progress lifecycle + log_cron_run success.
    stub_progress["start_progress"].assert_called_once()
    stub_progress["end_progress"].assert_called_once()
    # The success "no new files" branch fires log_cron_run with status="success".
    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls, "expected at least one log_cron_run(..., 'success')"


def test_sync_no_new_files_surfaces_reclaimed_count(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """The reconcile can reclaim strands on an otherwise-idle tick (new_files=0).
    The no-new-files branch must persist files_deleted_fos + surface the reclaim
    in the summary — previously it hard-coded 'No new log files found' and 0."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {
                    "type": "done",
                    "new_files": 0,
                    "rows_inserted": 0,
                    "deleted_files": 3,
                    "message": "No new files; reclaimed 3 raw file(s) left by an interrupted prior run.",
                }
            ]
        ),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1", force=True)

    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls, "expected a success log_cron_run"
    kwargs = success_calls[-1].kwargs
    assert kwargs.get("files_deleted_fos") == 3, "reclaimed strands must be recorded on idle ticks"
    assert "reclaim" in kwargs.get("summary", "").lower()


def test_manual_sync_all_clears_time_range(
    monkeypatch,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """``is_manual=True`` (run_id supplied) with no start_time triggers
    ``del prov['time_range']`` + ``save_config``."""
    from backend.cron.jobs import sync as sync_mod

    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {
            "cron_sync": {"enabled": True},
            "time_range": {"start": "2025-01-01", "end": "2025-01-31"},
        },
    }
    saved: list[dict] = []
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=cfg))
    monkeypatch.setattr(
        "backend.config.save_config",
        MagicMock(side_effect=lambda sid, c: saved.append(dict(c.get("provisioning", {})))),
    )
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0}]),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1", run_id=42)

    # save_config was called with time_range removed from provisioning.
    assert saved, "expected save_config to fire on manual sync-all"
    assert "time_range" not in saved[-1], f"time_range still present: {saved[-1]}"


def test_disk_space_failure_logs_error_and_returns(
    monkeypatch,
    stub_load_config,
    stub_progress,
):
    """Disk pre-check fails → ``log_cron_run`` records error + ingest
    never invoked."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    # Override stub_progress's "disk ok" default.
    from backend.cron.jobs import sync as _sync_mod

    monkeypatch.setattr(
        _sync_mod,
        "_check_disk_space",
        lambda cache_dir, sid, name: (False, "Free space below floor"),
    )
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)

    sync_mod._run_service_cron.__wrapped__("svc-1")

    ingest_mock.assert_not_called()
    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs.get("error_message") == "Free space below floor"
    assert "Sync aborted" in kwargs.get("summary", "")


def test_ingest_error_event_logs_cron_with_processed_files(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """An ``error`` event after some file_done events → log_cron_run
    records 'error' with processed files + rows counts."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {"type": "file_done", "current": 5, "total_inserted": 1234, "total_corrupt": 2},
                {"type": "error", "message": "S3 transient failure"},
            ]
        ),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1")

    error_calls = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert error_calls, "expected an error log_cron_run"
    args, kwargs = error_calls[0]
    assert kwargs.get("error_message") == "S3 transient failure"
    assert kwargs.get("files_downloaded") == 5
    assert kwargs.get("rows_ingested") == 1234
    assert kwargs.get("corrupt_rows") == 2
    assert "5 files" in kwargs.get("summary", "")


def test_ingest_done_event_records_summary_and_recompute_rollups(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """``done`` event with new_files > 0 + touched_hours → success
    summary + ``recompute_touched_hours`` invoked."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    recompute = MagicMock()
    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {
                    "type": "done",
                    "new_files": 7,
                    "rows_inserted": 2000,
                    "corrupt_rows": 0,
                    "deleted_files": 7,
                    "touched_hours": ["2026-06-15-09", "2026-06-15-10"],
                }
            ]
        ),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1")

    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls, "expected log_cron_run success"
    _args, kwargs = success_calls[0]
    summary = kwargs.get("summary", "")
    assert "7 files" in summary
    assert "2000 rows" in summary
    # Rollup recompute invoked with the touched-hours set.
    recompute.assert_called_once()
    _a, _b, hours_set = recompute.call_args.args
    assert hours_set == {"2026-06-15-09", "2026-06-15-10"}


def test_ingest_exception_records_crashed(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """A raised exception inside the ingest loop → log_cron_run with
    'error' + 'Ingestion crashed' summary, end_progress still fires."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)

    def _boom(*args, **kwargs):
        yield {"type": "file_done", "current": 2, "total_inserted": 100, "total_corrupt": 0}
        raise RuntimeError("DuckDB connection refused")

    monkeypatch.setattr("backend.core.ingest.ingest", _boom)

    sync_mod._run_service_cron.__wrapped__("svc-1")

    error_calls = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert error_calls
    _args, kwargs = error_calls[0]
    assert "Ingestion crashed" in kwargs.get("summary", "")
    assert "DuckDB connection refused" in kwargs.get("error_message", "")
    # end_progress still ran (finally block).
    stub_progress["end_progress"].assert_called()


def test_ingest_without_terminal_done_event_finalizes_row(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """Regression for the 2026-06-19 ingestion stall.

    If ``ingest()`` ends WITHOUT a terminal ``done`` event (and without an
    ``error`` event or exception), the for/else leaves ``done_event`` empty
    and NO ``log_cron_run`` fires — the cron_runs row would leak as
    ``running`` and the ``start_cron_run`` guard would then skip every
    subsequent sync tick. The ``finally`` backstop must finalize the row so
    that can't happen.
    """
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    finalize = MagicMock(return_value=True)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)
    # ingest yields only progress events — NO terminal 'done', NO 'error'.
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "file_done", "current": 3, "total_inserted": 50, "total_corrupt": 0}]),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1")

    # No success/error status was logged (this is exactly the leak path) ...
    assert not [c for c in log_cron.call_args_list if c.args[3] in ("success", "error")], (
        "no terminal log_cron_run is expected on the no-done path — that's the leak the backstop covers"
    )
    # ... so the finally backstop must have finalized the row instead.
    finalize.assert_called_once()
    args, kwargs = finalize.call_args
    assert args[1] == "sync"
    assert args[2] == 42
    stub_progress["end_progress"].assert_called_once()


def test_successful_done_does_not_double_finalize(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """The backstop is idempotent: on the happy path the success branch
    logs the terminal status and finalize_cron_run_if_running is still
    called (in the finally) but is a no-op against an already-terminal row.
    We assert it's invoked with the run_id so the wiring is present; the
    no-op semantics are covered in test_metadata_db_reap."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    finalize = MagicMock(return_value=False)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0}]),
    )

    sync_mod._run_service_cron.__wrapped__("svc-1")

    finalize.assert_called_once()
    assert finalize.call_args.args[2] == 42


# ── _run_full_sweep ─────────────────────────────────────────────────────────


def test_full_sweep_returns_silently_when_source_missing(monkeypatch, stub_load_config):
    """No source → return without opening a cron run or starting
    progress."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=None))
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)

    sync_mod._run_full_sweep.__wrapped__("ghost-svc")

    start_cron.assert_not_called()


def test_full_sweep_skipped_when_start_cron_run_raises(monkeypatch, stub_load_config):
    """``start_cron_run`` raising RuntimeError → log + return without
    starting progress."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=RuntimeError("already running")),
    )
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)
    start_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    ingest_mock.assert_not_called()
    start_progress.assert_not_called()


def test_full_sweep_success_logs_new_files_count(
    monkeypatch,
    stub_load_config,
    stub_progress,
):
    """A clean ``done`` event with new_files > 0 → log_cron_run
    success with "Backfilled N late-arriving file(s)" summary."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=99))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {"type": "file_done", "current": 4, "total_inserted": 500, "total_corrupt": 0},
                {
                    "type": "done",
                    "new_files": 4,
                    "rows_inserted": 500,
                    "corrupt_rows": 0,
                },
            ]
        ),
    )

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "success"
    summary = kwargs.get("summary", "")
    assert "Backfilled 4" in summary
    assert "500" in summary
    assert kwargs.get("files_downloaded") == 4
    assert kwargs.get("rows_ingested") == 500


def test_full_sweep_surfaces_reclaimed_strand_count(
    monkeypatch,
    stub_load_config,
    stub_progress,
):
    """full_sweep is the whole-bucket backstop for the stranded-delete reconcile,
    so when its done event carries deleted_files (strands reclaimed) it must
    persist files_deleted_fos + note the reclaim in the summary — even with no
    new late-arriving files."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=99))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0, "deleted_files": 5}]),
    )

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    log_cron.assert_called_once()
    _, kwargs = log_cron.call_args
    assert kwargs.get("files_deleted_fos") == 5, "reclaimed strands must be recorded"
    assert "reclaim" in kwargs.get("summary", "").lower()


def test_full_sweep_error_event_logs_with_processed_counts(
    monkeypatch,
    stub_load_config,
    stub_progress,
):
    """An ``error`` event in the stream → log_cron_run records 'error'
    with processed counts, then early-returns out of the body."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=99))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {"type": "file_done", "current": 3, "total_inserted": 200, "total_corrupt": 1},
                {"type": "error", "message": "rate-limited"},
            ]
        ),
    )

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs.get("error_message") == "rate-limited"
    assert kwargs.get("files_downloaded") == 3
    assert kwargs.get("rows_ingested") == 200
    assert kwargs.get("corrupt_rows") == 1
    assert "Full-sweep failed" in kwargs.get("summary", "")


# ── Cron → ingest → metadata_db cascade (real ingest, moto S3) ────────────


def test_run_service_cron_full_cascade_real_ingest_against_moto_s3(
    s3_mock, fos_source, monkeypatch, tmp_path, stub_progress, stub_usage_log_phase
):
    """End-to-end integration: ``_run_service_cron`` calls the REAL
    ``backend.core.ingest.ingest`` against a moto-backed FOS bucket
    seeded with two gzip log files, and we assert the full writer-side
    cascade actually fired.

    The other tests in this file all stub ``backend.core.ingest.ingest``
    with a canned generator, which leaves the orchestration seam between
    ``ingest → metadata_db.insert_ingested_files → start/log_cron_run``
    unverified. A regression that silently dropped the metadata write or
    skipped the cron_runs transition still passed all of them.

    This test pins three contracts in one cascade:

      1. Files actually land in ``metadata_db.ingested_files`` — closes
         the dedup / sync-status gap.
      2. The ``cron_runs`` row transitions ``started → success`` — the
         status the admin UI shows.
      3. Zero leftover rows in ``ingest_in_flight`` — closes the
         orphan-buffer crash-recovery gap.
    """
    import gzip
    import io
    import json
    import os
    from datetime import UTC, datetime, timedelta

    from backend.core import iceberg as ice
    from backend.core import metadata as metadata_db
    from backend.cron.jobs import sync as sync_mod

    # ── Sandbox cache + warehouse onto the tmpfs ─────────────────────────
    cache_path = str(tmp_path / "cache")
    warehouse_path = str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    # Sandbox the DuckDB file path. Without this the test inherits the
    # module-level DUCKDB_PATH that resolves at import time from
    # _build_default_source() — i.e., whatever local-disk service config
    # the dev machine has provisioned. On a developer box with a running
    # uvicorn (port 18002), that file is already held writer-locked by
    # the dev backend; the cron's refresh_view_and_warm_pool then spins
    # in the get_connection retry loop with
    # 'IO Error: Could not set lock on file ... Conflicting lock is held'
    # until the 300 s deadline trips. Same risk on CI if any other
    # process/test has the default service's file open — pin the path
    # under tmp_path so the test owns its own DuckDB file.
    monkeypatch.setattr("backend.core.duckdb.DUCKDB_PATH", str(tmp_path / "test.duckdb"))

    # ingest.py captured `_get_fos_client` at module import; s3_mock
    # patches the duckdb-module symbol, but ingest holds its own
    # reference. Production wraps boto3 to accept ``caller_hint`` on
    # ``get_paginator``; moto's plain client doesn't, so wrap.
    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _CallerHintShim(s3_mock))

    # Clear iceberg module-level caches so a prior test's snapshot/view
    # state can't leak in (the autouse fixture clears these too).
    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # Sync-cron-specific fixtures: defer-gate off, disk-space OK already
    # stubbed by stub_progress; no remaining usage-log helpers needed.
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)

    # Real ``_run_service_cron`` calls ``svcconfig.load_config`` to read
    # cron_sync.enabled + time_range; the lambda above returns a stub.
    # ``get_source_for_service`` must return our moto-bound source so
    # ingest can list the bucket. ``start_cron_run`` writes the
    # ``running`` row into metadata_db.cron_runs (we let it run — the
    # whole point is to verify the row transitions).
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: fos_source)
    # log_cron_run + refresh_config_status are real — we want them to
    # actually update metadata_db.cron_runs from running → success.

    # ── Seed moto with two gzipped JSON log files ────────────────────────
    base = datetime.now(UTC) - timedelta(hours=2)
    rows_per_file = 4
    seeded = [
        ("raw/2026-05-21/10/2026-05-21T10-00-00.svc.gz", base),
        ("raw/2026-05-21/10/2026-05-21T10-05-00.svc.gz", base + timedelta(minutes=5)),
    ]
    for key, ts in seeded:
        rows = [
            {
                "timestamp": (ts + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
                "ip": f"10.0.0.{i}",
                "status": 200,
                "url": f"/c/{i}",
                "method": "GET",
            }
            for i in range(rows_per_file)
        ]
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    # Bootstrap the iceberg table BEFORE the cron runs so commit has
    # somewhere to append. The provision wizard does this in production.
    ice.init_iceberg_table(fos_source)

    svc_id = fos_source["service_id"]
    svc_name = fos_source["name"]

    # ── Drive the real cron — NO ingest stub ─────────────────────────────
    sync_mod._run_service_cron.__wrapped__(svc_id, force=True)

    # ── Contract 1: every ingested file landed in metadata_db ────────────
    ingested = metadata_db.list_ingested_files(svc_name)
    ingested_names = {row["file_name"] for row in ingested}
    for key, _ in seeded:
        assert any(name.endswith(key) for name in ingested_names), (
            f"metadata_db.ingested_files missing entry ending in {key!r}; "
            f"got {sorted(ingested_names)}. A refactor that skips the "
            "insert_ingested_files call between ingest and the cron's "
            "log_cron_run would silently break dedup."
        )

    # ── Contract 2: cron_runs transitioned started → completed ───────────
    _total, runs = metadata_db.get_cron_runs(svc_name, task="sync", per_page=10)
    assert runs, "cron_runs has no rows — start_cron_run never fired"
    latest = runs[0]
    assert latest["status"] == "success", (
        f"latest sync cron_runs.status = {latest['status']!r}; "
        "expected 'success'. Either log_cron_run never ran, or ingest "
        "surfaced an error event we should investigate."
    )
    assert latest.get("files_downloaded") == len(seeded)
    assert latest.get("rows_ingested") == rows_per_file * len(seeded)

    # ── Contract 3: no orphan in_flight rows ─────────────────────────────
    in_flight = metadata_db.list_in_flight(svc_name)
    assert in_flight == [], (
        f"ingest_in_flight non-empty after cron: {in_flight}. "
        "clear_in_flight must run after insert_ingested_files; leftover "
        "rows here would cause double-ingest on next restart."
    )
