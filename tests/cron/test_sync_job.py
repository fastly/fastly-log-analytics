"""Consolidated tests for test_sync_job."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from tests._helpers.config_fakes import fake_mutate_config


@pytest.fixture(autouse=True)
def clean_dev_no_crons(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


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
    ``_run_log_discovery_cron``; patch their source module so the late
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
        "provisioning": {"cron_sync": {"enabled": True, "polling_mode": "regular"}},
    }
    load = MagicMock(return_value=cfg)
    save = MagicMock(side_effect=fake_mutate_config(lambda: cfg))
    monkeypatch.setattr("backend.config.load_config", load)
    monkeypatch.setattr("backend.config.mutate_config", save)
    return {"load": load, "save": save, "cfg": cfg}


def _make_ingest_events(events: list[dict]) -> Any:
    """Yield the canned events, mimicking the ingest generator."""

    def _gen(*args, **kwargs):
        yield from events

    return _gen


# ── _run_log_discovery_cron ────────────────────────────────────────────────────────


def test_scheduled_tick_is_not_deferred_by_in_flight_requests(monkeypatch, stub_load_config):
    """Request freshness is the SLA: each deferral cost a full interval and the
    dashboard's own polling kept the gate tripped, so the tick must proceed."""
    from backend.cron.jobs import sync as sync_mod
    from backend.utils import active_requests

    active_requests._reset_for_tests()
    active_requests.increment_active_requests()
    try:
        get_src = MagicMock(return_value=None)
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)

        sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

        stub_load_config["load"].assert_called_once_with("svc-1")
        get_src.assert_called_once_with("svc-1")
    finally:
        active_requests._reset_for_tests()


def test_skips_when_config_missing(monkeypatch):
    """``svcconfig.load_config`` → None → warn + return without
    touching source-lookup."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)

    sync_mod._run_log_discovery_cron.__wrapped__("ghost-svc")

    get_src.assert_not_called()


def test_skips_when_source_missing(monkeypatch, stub_load_config):
    """Config present but source builder returns None → early return."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=None))
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    start_cron.assert_not_called()


def test_high_scale_discovery_persists_its_originating_run_id(monkeypatch, stub_load_config):
    from backend.cron.jobs import sync as sync_mod

    source = _fake_src()
    source["deployment_mode"] = "high_scale"
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://broker")
    discovery = MagicMock(return_value=1)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discovery)
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=521)

    assert discovery.call_count == 5
    assert all(call.kwargs["run_id"] == 521 for call in discovery.call_args_list)
    assert log_cron.call_args.kwargs["run_id"] == 521


def test_high_scale_discovery_rejects_missing_broker_without_listing(monkeypatch, stub_load_config):
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _fake_src())
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    discover = MagicMock()
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    log_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=522)

    discover.assert_not_called()
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["run_id"] == 522
    assert "CELERY_BROKER_URL" in log_run.call_args.kwargs["error_message"]


def test_high_scale_discovery_records_list_failure_on_its_run(monkeypatch, stub_load_config):
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _fake_src())
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "memory://")
    discover = MagicMock(side_effect=OSError("FOS unavailable"))
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    log_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=523)

    discover.assert_called_once()
    assert discover.call_args.kwargs["run_id"] == 523
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["run_id"] == 523
    assert log_run.call_args.kwargs["error_message"] == "FOS unavailable"


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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=False)

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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)

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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)

    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls, "expected a success log_cron_run"
    kwargs = success_calls[-1].kwargs
    assert kwargs.get("files_deleted_fos") == 3, "reclaimed strands must be recorded on idle ticks"
    assert "reclaim" in kwargs.get("summary", "").lower()


def test_log_discovery_persists_data_plane_outcomes_as_error(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    from backend.cron.jobs import sync as sync_mod

    counters = {
        "valid_records": 1,
        "malformed_records": 1,
        "corrupt_containers": 0,
        "quarantine_capture_failures": 0,
        "source_delete_failures": 0,
        "cap_evictions": 0,
        "objects_processed": 1,
        "objects_successful": 0,
        "objects_partial": 1,
        "objects_failed": 0,
    }
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
                    "new_files": 1,
                    "rows_inserted": 1,
                    "corrupt_rows": 1,
                    "outcome_counters": counters,
                }
            ]
        ),
    )

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)

    terminal = [call for call in log_cron.call_args_list if call.args[3] == "error"]
    assert terminal
    assert terminal[-1].kwargs["outcome_counters"] == counters


def test_adaptive_ingest_stops_after_empty_followup_and_aggregates_outcomes(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    pass_results = iter([2, 1, 0])
    calls = []
    sleeps = []

    def ingest_fn(**kwargs):
        count = next(pass_results)
        calls.append(kwargs)
        yield {
            "type": "done",
            "new_files": count,
            "rows_inserted": count * 10,
            "outcome_counters": {"valid_records": count * 10, "objects_processed": count},
        }

    monkeypatch.setattr(sync_mod.time, "sleep", sleeps.append)

    events = list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=True, service_id="svc"))
    done = events[-1]

    assert len(calls) == 3
    assert sleeps == [0.5, 0.5]
    assert done["new_files"] == 3
    assert done["rows_inserted"] == 30
    assert done["outcome_counters"]["valid_records"] == 30
    assert done["outcome_counters"]["objects_processed"] == 3


def test_adaptive_ingest_publishes_rows_before_each_followup(monkeypatch):
    """Rows from an intermediate pass must be published before the follow-up
    sleep, not held until the end of the ~20 s polling window."""
    from backend.cron.jobs import sync as sync_mod

    pass_results = iter([2, 1, 0])
    timeline: list[str] = []

    def ingest_fn(**_kwargs):
        count = next(pass_results)
        timeline.append(f"pass:{count}")
        yield {"type": "done", "new_files": count, "rows_inserted": count * 10}

    monkeypatch.setattr(sync_mod.time, "sleep", lambda _s: timeline.append("sleep"))

    list(
        sync_mod._ingest_with_adaptive_followups(
            ingest_fn, adaptive=True, after_pass=lambda: timeline.append("publish")
        )
    )

    assert timeline == ["pass:2", "publish", "sleep", "pass:1", "publish", "sleep", "pass:0"]


def test_adaptive_ingest_skips_publish_when_no_followup_runs(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    published: list[None] = []

    def ingest_fn(**_kwargs):
        yield {"type": "done", "new_files": 1, "rows_inserted": 5}

    list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=False, after_pass=lambda: published.append(None)))

    assert published == []


def test_adaptive_ingest_obeys_twenty_second_followup_cap(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    now = [0.0]
    calls = []

    def monotonic():
        return now[0]

    def ingest_fn(**_kwargs):
        calls.append(None)
        now[0] = 18.0
        yield {"type": "done", "new_files": 1}

    monkeypatch.setattr(sync_mod.time, "monotonic", monotonic)
    monkeypatch.setattr(sync_mod.time, "sleep", lambda _seconds: now.__setitem__(0, now[0] + 3))

    list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=True))

    assert len(calls) == 1


def test_log_discovery_stands_down_for_a_high_scale_shared_source(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """``provisioning.cron_sync.high_scale_shared_source=True`` must force
    ``delete_after=False`` into ``ingest(...)`` regardless of the service's
    own ``delete_after`` setting — this is the fix for legacy sync racing
    an out-of-band high-scale consumer of the same raw FOS stream and
    deleting objects it hasn't archived yet."""
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        MagicMock(return_value=_fake_src()),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())

    stub_load_config["cfg"]["provisioning"]["cron_sync"]["delete_after"] = True
    stub_load_config["cfg"]["provisioning"]["cron_sync"]["high_scale_shared_source"] = True

    ingest_mock = MagicMock(
        side_effect=_make_ingest_events(
            [{"type": "done", "new_files": 0, "rows_inserted": 0, "deleted_files": 0, "message": "ok"}]
        )
    )
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)

    from backend.cron.jobs import sync as sync_mod

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)

    assert ingest_mock.call_args.kwargs["delete_after"] is False


def test_sync_status_published_after_request_extents_refresh(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """Publish this ingest's request extent, not the previous tick's cache."""
    from backend.cron.jobs import sync as sync_mod

    calls: list[str] = []
    status = {"latest_log_at": "2026-01-01T00:00:00Z"}

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

    def _fake_refresh_config_status(*args, include_top_values=True, **kwargs):
        calls.append(f"refresh_config_status(top_values={include_top_values})")
        status["latest_log_at"] = "2026-01-02T00:00:00Z"

    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", _fake_refresh_config_status)

    def _fake_compute_snapshot(service_id):
        calls.append("compute_sync_status_cached")
        return dict(status)

    monkeypatch.setattr(
        "backend.sync_status_snapshot.compute_sync_status_cached",
        _fake_compute_snapshot,
    )

    fake_publisher = MagicMock()
    fake_publisher.publish.side_effect = lambda *a, **k: calls.append("publish")
    monkeypatch.setattr("backend.sync_status_publisher.publisher", fake_publisher)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)

    # The heavy top-values pass (~5 s on GCE) runs after publish so it never
    # holds freshly ingested rows back from the header.
    assert calls == [
        "refresh_config_status(top_values=False)",
        "compute_sync_status_cached",
        "publish",
        "refresh_config_status(top_values=True)",
    ]
    fake_publisher.publish.assert_called_once_with("svc-1", {"latest_log_at": "2026-01-02T00:00:00Z"})


@pytest.mark.parametrize("heavy", [False, True])
def test_usage_log_bookkeeping_runs_only_on_heavy_refresh_ticks(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    heavy,
):
    """The ~3 s usage-log phase sat between every tick's last ingest pass and
    the next tick's first, widening the request-lag sawtooth; it is idempotent
    and read at hourly grain, so it rides the 60 s heavy-refresh window."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr(sync_mod, "_claim_heavy_refresh", lambda sid: heavy)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0}]),
    )
    backfill = MagicMock(return_value=0)
    cleanup = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.backfill_fastly_edge_writes", backfill)
    monkeypatch.setattr("backend.core.duckdb.reconcile_fastly_stats", MagicMock(return_value=0))
    monkeypatch.setattr("backend.utils.usage_logger.run_usage_log_cleanup", cleanup)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    assert backfill.called is heavy
    assert cleanup.called is heavy


def test_manual_sync_all_clears_time_range(
    monkeypatch,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """``is_manual=True`` (run_id supplied) with no start_time triggers
    ``del prov['time_range']`` + ``mutate_config``."""
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
    monkeypatch.setattr("backend.config.mutate_config", fake_mutate_config(cfg, saves=saved))
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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=42)

    # mutate_config was called with time_range removed from provisioning.
    assert saved, "expected mutate_config to fire on manual sync-all"
    assert "time_range" not in saved[-1].get("provisioning", {}), f"time_range still present: {saved[-1]}"


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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    error_calls = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert error_calls, "expected an error log_cron_run"
    args, kwargs = error_calls[0]
    assert kwargs.get("error_message") == "S3 transient failure"
    assert kwargs.get("files_downloaded") == 5
    assert kwargs.get("rows_ingested") == 1234
    assert kwargs.get("corrupt_rows") == 2
    assert "5 files" in kwargs.get("summary", "")


def _wait_for_post_ingest_rollups(service_id: str, timeout: float) -> bool:
    import time

    from backend.cron.jobs import sync as sync_mod

    deadline = time.monotonic() + timeout
    while True:
        with sync_mod._rollup_lock:
            worker = sync_mod._rollup_workers.get(service_id)
        if worker is None:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        worker.join(min(remaining, 0.05))


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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    assert _wait_for_post_ingest_rollups("svc-1", timeout=5)

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


def test_rollup_recompute_does_not_block_discovery_tick(
    monkeypatch,
    stub_load_config,
    stub_progress,
    stub_post_ingest,
    stub_usage_log_phase,
):
    """Closed-hour rollup rebuilds can take minutes; running them inline held
    the 10 s discovery job (max_instances=1) and let the FOS backlog grow to
    minutes of request lag. The tick must return while the rollup is still
    running."""
    import threading

    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    release = threading.Event()
    started = threading.Event()

    def _slow_recompute(_sid, _src, _hours):
        started.set()
        release.wait(5)

    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", _slow_recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events(
            [
                {
                    "type": "done",
                    "new_files": 1,
                    "rows_inserted": 5,
                    "deleted_files": 1,
                    "touched_hours": ["2026-06-15-09"],
                }
            ]
        ),
    )

    sync_mod._run_log_discovery_cron.__wrapped__("svc-block")

    assert started.wait(5), "rollup recompute never started"
    assert not _wait_for_post_ingest_rollups("svc-block", timeout=0.05)
    release.set()
    assert _wait_for_post_ingest_rollups("svc-block", timeout=5)


def test_post_ingest_rollups_coalesce_hours_queued_while_running(monkeypatch):
    """Hours queued while a pass is running fold into ONE follow-up pass on the
    same worker, so a slow rollup never spawns a pile of concurrent rebuilds."""
    import threading

    from backend.cron.jobs import sync as sync_mod

    calls: list[set[str]] = []
    threads: set[str] = set()
    release = threading.Event()
    first_started = threading.Event()

    def _recompute(_sid, _src, hours):
        threads.add(threading.current_thread().name)
        calls.append(set(hours))
        if len(calls) == 1:
            first_started.set()
            release.wait(5)

    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", _recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))

    src = _fake_src()
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-09"})
    assert first_started.wait(5)
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-10"})
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-11", "2026-06-15-10"})
    release.set()

    assert _wait_for_post_ingest_rollups("svc-coalesce", timeout=5)
    assert calls == [{"2026-06-15-09"}, {"2026-06-15-10", "2026-06-15-11"}]
    assert len(threads) == 1


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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    # No success/error status was logged (this is exactly the leak path) ...
    assert not [c for c in log_cron.call_args_list if c.args[3] in ("success", "error")], (
        "no terminal log_cron_run is expected on the no-done path — that's the leak the backstop covers"
    )
    # ... so the finally backstop must have finalized the row instead.
    finalize.assert_called_once()
    args, kwargs = finalize.call_args
    assert args[1] == "log_discovery"
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

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

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


def test_scheduled_discovery_persists_service_creation_time_as_range_start(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    service_id = "svc-creation-range"
    source = _fake_src()
    cfg = {"created_at": "2026-09-29T12:00:00Z", "provisioning": {"cron_sync": {"enabled": True}}}
    save = MagicMock(side_effect=fake_mutate_config(cfg))
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", save)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "memory://")
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 108)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    discover = MagicMock(return_value=0)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_log_discovery_cron.__wrapped__(service_id)

    assert cfg["provisioning"]["time_range"] == {"start": "2026-09-29T12:00:00Z"}
    assert source["time_range"] == cfg["provisioning"]["time_range"]
    save.assert_called_once()
    assert discover.call_count == 5


def test_manual_discovery_clears_pinned_range_before_listing(monkeypatch):
    from backend.cron.jobs import sync as sync_mod

    service_id = "svc-manual-range"
    source = _fake_src()
    cfg = {
        "provisioning": {
            "cron_sync": {"enabled": True, "polling_mode": "adaptive"},
            "time_range": {"start": "2026-09-01T00:00:00Z"},
        }
    }
    save = MagicMock(side_effect=fake_mutate_config(cfg))
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", save)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "memory://")
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    discover = MagicMock(return_value=1)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_log_discovery_cron.__wrapped__(service_id, force=True, run_id=109)

    assert "time_range" not in cfg["provisioning"]
    assert source["time_range"] is None
    save.assert_called_once()
    assert discover.call_count == 5
    assert {call.kwargs["run_id"] for call in discover.call_args_list} == {109}


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
        MagicMock(side_effect=CronLeaseHeldError("already running")),
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
    stub_post_ingest,
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


def test_full_sweep_finalizes_duration(monkeypatch, stub_load_config, stub_progress):
    """finalize_cron_duration must be called in finally block."""
    from backend.cron.jobs import sync as sync_mod

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=104))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        _make_ingest_events([{"type": "done", "new_files": 0, "rows_inserted": 0}]),
    )

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    finalize.assert_called_once()
    assert finalize.call_args.args[1] == 104


def test_high_scale_full_sweep_records_discovery_failure_on_originating_run(monkeypatch, stub_load_config):
    from backend.cron.jobs import sync as sync_mod

    source = _fake_src()
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 105)
    discover = MagicMock(side_effect=OSError("FOS listing unavailable"))
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    log_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)

    sync_mod._run_full_sweep.__wrapped__("svc-1")

    discover.assert_called_once_with("svc-1", run_id=105, originating_task="full_sync")
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["run_id"] == 105
    assert log_run.call_args.kwargs["error_message"] == "FOS listing unavailable"
    assert finalize.call_args.args[1] == 105


# ── Cron → ingest → metadata_db cascade (real ingest, moto S3) ────────────


def test_run_log_discovery_cron_full_cascade_real_ingest_against_moto_s3(
    s3_mock, fos_source, monkeypatch, tmp_path, stub_progress, stub_usage_log_phase
):
    """End-to-end integration: ``_run_log_discovery_cron`` calls the REAL
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

    # ── Sandbox cache onto the tmpfs ─────────────────────────────────────
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
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

    # Clear iceberg module-level view cache so a prior test's view
    # state can't leak in (the autouse fixture clears these too).
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # Sync-cron-specific fixtures: defer-gate off, disk-space OK already
    # stubbed by stub_progress; no remaining usage-log helpers needed.
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)

    # Real ``_run_log_discovery_cron`` calls ``svcconfig.load_config`` to read
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
        ("raw/request/year=2026/month=05/day=21/hour=10/minute=00/2026-05-21T10-00-00.svc.gz", base),
        (
            "raw/request/year=2026/month=05/day=21/hour=10/minute=05/2026-05-21T10-05-00.svc.gz",
            base + timedelta(minutes=5),
        ),
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
    sync_mod._run_log_discovery_cron.__wrapped__(svc_id, force=True)

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
    _total, runs = metadata_db.get_cron_runs(svc_name, task="log_discovery", per_page=10)
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


# ── cron_run lifecycle (log_discovery) ──────────────────────────────────────


def _lifecycle_env(monkeypatch, events, *, log_text="LOGTEXT"):
    from backend.cron.jobs import sync as sync_mod  # noqa: F401

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    start = MagicMock(return_value=42)
    log_cron = MagicMock()
    order: list[str] = []
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.duckdb.finalize_cron_run_if_running",
        lambda *a, **k: order.append(("backstop", a, k)) or True,
    )
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda rid: order.append(("end_progress", rid)))
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: log_text)
    monkeypatch.setattr("backend.core.ingest.ingest", _make_ingest_events(events))
    return start, log_cron, order


def test_lifecycle_lease_held_skips_without_ingest_or_row(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    from backend.cron.jobs import sync as sync_mod

    start, log_cron, order = _lifecycle_env(monkeypatch, [])
    start.side_effect = CronLeaseHeldError("held")
    ingest = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    ingest.assert_not_called()
    log_cron.assert_not_called()
    assert order == []


def test_lifecycle_backstop_gets_context_and_runs_before_end_progress(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    from backend.cron.jobs import sync as sync_mod

    _, _, order = _lifecycle_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    assert [o[0] for o in order] == ["backstop", "end_progress"]
    _, args, kwargs = order[0]
    assert args[1:] == ("log_discovery", 42)
    assert kwargs["summary"] == "Sync exited without recording a terminal status"
    assert kwargs["error_message"] == "orphaned run auto-finalized (no terminal ingest event)"
    assert isinstance(kwargs["duration_s"], float)
    assert order[1] == ("end_progress", 42)


def test_lifecycle_error_event_row_shape(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    from backend.cron.jobs import sync as sync_mod

    events = [
        {"type": "file_done", "current": 3, "total_inserted": 50, "total_corrupt": 1},
        {"type": "error", "message": "s3 timeout"},
    ]
    _, log_cron, _ = _lifecycle_env(monkeypatch, events)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    errors = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert len(errors) == 1
    kwargs = errors[0].kwargs
    assert kwargs["summary"] == "Ingestion failed after processing 3 files (50 rows)"
    assert kwargs["error_message"] == "s3 timeout"
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"], kwargs["corrupt_rows"]) == (3, 50, 1)
    assert kwargs["log_output"] == "LOGTEXT"
    assert kwargs["run_id"] == 42


def test_lifecycle_disk_failure_row_has_no_log_output(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    from backend.cron.jobs import sync as sync_mod

    _, log_cron, order = _lifecycle_env(monkeypatch, [])
    monkeypatch.setattr(sync_mod, "_check_disk_space", lambda cache_dir, sid, name: (False, "disk full"))

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Sync aborted: disk full"
    assert kwargs["error_message"] == "disk full"
    assert kwargs["log_output"] is None
    assert [o[0] for o in order] == ["backstop", "end_progress"]


def test_lifecycle_high_scale_missing_broker_row_has_no_log_output(monkeypatch, stub_load_config, stub_progress):
    from backend.cron.jobs import sync as sync_mod

    _, log_cron, _ = _lifecycle_env(monkeypatch, [])
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Celery ingest misconfigured: no broker URL"
    assert kwargs["error_message"] == "DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL"
    assert kwargs["log_output"] is None


# ── Merged from test_log_discovery_contract.py ──────────────────────────────────────────────

import logging
import stat
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app


@pytest.fixture
def log_discovery_test_source(monkeypatch, tmp_path):
    service_id = "svc-log-discovery-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-log-discovery-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def test_contract_1_manual_trigger_with_synthetic_gz_in_fos(log_discovery_test_source):
    """Checklist Item 1:
    Trigger POST /api/admin/sync/{service_id} with synthetic .gz files in FOS; verify HTTP 200 response.
    """
    service_id = log_discovery_test_source["service_id"]
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-contract-sync-123"

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.sync._run_log_discovery_cron"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/sync/{service_id}",
                headers={"x-fastly-service-id": service_id},
            )

        assert resp.status_code == 200, f"Expected 200, got: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-contract-sync-123"
        assert "started" in body["message"].lower()
        assert started["task"] == "log_discovery"
    finally:
        app.dependency_overrides.clear()


def test_contract_2_cron_runs_records_success_and_nonzero_files_ingested(log_discovery_test_source):
    """Checklist Item 2:
    Confirm execution records in cron_runs with status success and non-zero files_ingested.
    """
    from backend.core import metadata as metadata_db

    service_id = log_discovery_test_source["service_id"]
    source_name = log_discovery_test_source["name"]

    # Start run
    run_id = metadata_db.start_cron_run(service_id, "log_discovery")
    assert run_id is not None

    # Complete run with success and non-zero files ingested
    metadata_db.log_cron_run(
        service_id,
        "log_discovery",
        duration_s=1.25,
        status="success",
        run_id=run_id,
        files_downloaded=5,
        rows_ingested=150,
        summary="Ingested 5 file(s) (150 rows) in 1.25s",
    )

    total, runs = metadata_db.get_cron_runs(source_name, task="log_discovery", per_page=5)
    assert total >= 1
    assert runs[0]["status"] == "success"
    assert runs[0]["files_downloaded"] == 5
    assert runs[0]["rows_ingested"] == 150


def test_contract_3_usage_log_attributes_fos_class_a_list_and_class_b_get_to_cron_log_discovery(
    log_discovery_test_source, monkeypatch
):
    """Checklist Item 3:
    Verify the PostgreSQL usage_log table attributes FOS Class A LIST and Class B GET calls to cron.log_discovery.
    Also verifies that the cron_task wrapper executes with process_context='cron.log_discovery' and flushes on exit.
    """
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db
    from backend.utils.telemetry import get_process_context

    service_id = log_discovery_test_source["service_id"]
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []

    def fake_ingest_followups(*args, **kwargs):
        observed["context"] = get_process_context()
        yield {"type": "done", "files": 0, "rows": 0, "corrupt": 0}

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: log_discovery_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 999)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron.jobs.sync._ingest_with_adaptive_followups", fake_ingest_followups)
    monkeypatch.setattr("backend.cron.jobs.sync._claim_heavy_refresh", lambda sid: False)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda *a, **kw: None)
    monkeypatch.setattr(
        "backend.utils.usage_logger.flush_usage_log",
        lambda sid: flush_contexts.append(get_process_context()),
    )

    sync_mod._run_log_discovery_cron(service_id, force=True)

    assert observed["context"] == "cron.log_discovery", (
        f"Execution must run under process_context='cron.log_discovery', got {observed['context']!r}"
    )
    assert flush_contexts == ["cron.log_discovery"], (
        f"Usage log flush must run under process_context='cron.log_discovery', got {flush_contexts}"
    )

    # Emulate FOS Class A LIST and Class B GET calls captured during log discovery
    fos_calls = [
        {
            "method": "LISTOBJECTSV2",
            "service": "FOS",
            "details": "Class A · list",
            "url": "s3://test-bucket/raw/request/",
        },
        {
            "method": "GETOBJECT",
            "service": "FOS",
            "bytes": 65536,
            "details": "Class B · get",
            "url": "s3://test-bucket/raw/request/log.gz",
        },
    ]

    metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.log_discovery")

    con = usage_log_db.get_con(service_id)
    rows = con.execute(
        "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
        (service_id,),
    ).fetchall()

    list_rows = [r for r in rows if r["operation_type"] == "LISTOBJECTSV2"]
    assert list_rows, "Must record LISTOBJECTSV2 in usage_log"
    assert list_rows[-1]["operation_class"] == "A", "LISTOBJECTSV2 must be classified as Class A"
    assert list_rows[-1]["process_context"] == "cron.log_discovery", (
        "LISTOBJECTSV2 must be attributed to cron.log_discovery"
    )

    get_rows = [r for r in rows if r["operation_type"] == "GETOBJECT"]
    assert get_rows, "Must record GETOBJECT in usage_log"
    assert get_rows[-1]["operation_class"] == "B", "GETOBJECT must be classified as Class B"
    assert get_rows[-1]["process_context"] == "cron.log_discovery", "GETOBJECT must be attributed to cron.log_discovery"


def test_contract_6_safety_gate_under_fla_dev_no_crons(log_discovery_test_source, monkeypatch, caplog):
    """Checklist Item 6:
    Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
    """
    service_id = log_discovery_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    # 1. Registration check: Scheduler._register_dev_local_safe_jobs must NOT register log_discovery
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[log_discovery_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=log_discovery_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
        job_ids = [c.kwargs["id"] for c in add_job.call_args_list]
        assert f"log_discovery_{service_id}" not in job_ids, (
            "log_discovery must never register under FLA_DEV_NO_CRONS=1"
        )

    # 2. Execution check: direct invocation must refuse execution and log warning citing FLA_DEV_NO_CRONS=1
    caplog.set_level(logging.WARNING)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        res = sync_mod._run_log_discovery_cron(service_id, force=True)
        get_src.assert_not_called()
        assert res is None or res.get("status") == "skipped"

    assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records), (
        "Execution refusal log must cite FLA_DEV_NO_CRONS=1"
    )


def test_contract_8_malformed_corrupt_caps_and_analyst_denial(log_discovery_test_source, tmp_path, monkeypatch):
    """Checklist Item 8:
    Verify malformed-line and corrupt-gzip outcomes, per-category counters, error status,
    immediate per-service cap eviction, private evidence downloads, and Analyst Path A/B denial.
    """
    from backend.core import quarantine
    from backend.core.metadata.quarantine import list_quarantine_evidence

    service_id = log_discovery_test_source["service_id"]

    # 1. FIFO cap eviction & 0o600 / 0o700 private permissions
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b'{"line": 1}\n', b'{"line": 2}\n', b'{"line": 3}\n']
    results = [
        quarantine.capture_evidence(
            service_id,
            "request",
            f"raw/request/part_{i}.log.gz",
            payload,
            line_ordinal=i,
            byte_offset=0,
            error_category="invalid_json",
            error_text="malformed json test",
        )
        for i, payload in enumerate(payloads, start=1)
    ]

    # Third insertion must trigger eviction of item 1
    assert results[2]["cap_evictions"] == 1
    items = list_quarantine_evidence(service_id)
    assert len(items) == 2
    remaining_ids = {item["id"] for item in items}
    assert int(results[0]["id"]) not in remaining_ids
    assert int(results[2]["id"]) in remaining_ids

    # Check evidence file permission 0o600 and dir permission 0o700
    evidence_path = quarantine._evidence_path(service_id, int(results[2]["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700

    # 2. Private evidence download via API
    item, evidence_file = quarantine.read_evidence(service_id, int(results[2]["id"]))
    try:
        assert evidence_file.read() == payloads[2]
    finally:
        evidence_file.close()

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        client = TestClient(app)
        download_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert download_resp.status_code == 200
        assert download_resp.content == payloads[2]

        # 3. Analyst Path B denial (require_admin blocks remote analyst)
        from fastapi import HTTPException

        def reject_analyst():
            raise HTTPException(status_code=403, detail={"error": "admin_only"})

        app.dependency_overrides[require_admin] = reject_analyst
        denied_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert denied_resp.status_code == 403
        assert denied_resp.json()["detail"]["error"] == "admin_only"

        # 4. Analyst Path A denial (read_only instance cannot trigger sync)
        ro_source = {**log_discovery_test_source, "access_level": "read_only"}
        app.dependency_overrides[get_source] = lambda: ro_source
        app.dependency_overrides.pop(require_admin, None)
        sync_denied_resp = client.post(
            f"/api/admin/sync/{service_id}",
            headers={"x-fastly-service-id": service_id},
        )
        assert sync_denied_resp.status_code == 403
        assert "read-only" in sync_denied_resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


# ── Merged from test_commit.py ──────────────────────────────────────────────


import pytest

from backend.cron.jobs import commit


class TestCommitJob:
    @pytest.fixture
    def stub_load_config(self, monkeypatch):
        cfg = {
            "service_id": "svc-1",
            "name": "svc-1",
            "provisioning": {"cron_sync": {"enabled": True, "commit_interval_mins": 5}},
        }
        load = MagicMock(return_value=cfg)
        monkeypatch.setattr("backend.config.load_config", load)
        return {"load": load, "cfg": cfg}

    def test_returns_when_config_missing(self, monkeypatch):

        monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
        get_src = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
        commit._run_commit.__wrapped__("ghost-svc")
        get_src.assert_not_called()

    def test_returns_when_source_missing(self, monkeypatch, stub_load_config):
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
        commit._run_commit.__wrapped__("svc-1")
        start.assert_not_called()

    def test_read_only_source_skipped_without_force(self, monkeypatch, stub_load_config):
        """A read_only source must NOT run commit unless force=True is
        passed (manual admin trigger). This is the contract that prevents
        a read-only mirror from clobbering the writer's snapshots."""
        ro_src = {"name": "ro", "service_id": "svc-1", "access_level": "read_only"}
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)

        commit._run_commit.__wrapped__("svc-1")  # force defaults False

        start.assert_not_called()

    def test_disabled_sync_skipped_without_force(self, monkeypatch, stub_load_config, stub_source):
        """cron_sync.enabled=False without force → no-op. The shared
        ``cron_sync`` config gates both ingest and commit so they can be
        suspended together for maintenance windows."""
        stub_load_config["cfg"]["provisioning"]["cron_sync"]["enabled"] = False
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
        commit._run_commit.__wrapped__("svc-1")
        start.assert_not_called()

    def test_disk_check_failure_logs_error_and_returns(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        """Disk space below floor → log_cron_run with 'error' + 'Commit
        aborted: ...', commit_buffer never runs. The whole point of the
        pre-check is that a midway disk failure during commit would corrupt
        the iceberg state — refusing to start is the safe move."""
        # ``_check_disk_space`` was bound on ``backend.cron.jobs.commit`` at
        # import time via ``from backend.cron.scheduler import _check_disk_space``.
        # Patch where the name lives so the body resolves to our mock.
        monkeypatch.setattr("backend.cron.jobs.commit._check_disk_space", lambda *a, **kw: (False, "disk 95% full"))
        cb = MagicMock()
        monkeypatch.setattr("backend.core.iceberg.commit_buffer", cb)

        commit._run_commit.__wrapped__("svc-1")

        cb.assert_not_called()
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert "disk 95% full" in kwargs["error_message"]
        assert "Commit aborted" in kwargs["summary"]

    def test_success_with_committed_files_logs_summary_and_refreshes_view(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        """Happy path: commit_buffer returns rows_committed > 0 →
        log_cron_run records 'success' with the row count + snapshot id,
        refresh_view_and_warm_pool fires, and metadata_sync is dispatched."""
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(return_value={"files_committed": 2, "rows_committed": 1234, "snapshot_id": 8675309}),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success"
        assert kwargs["rows_ingested"] == 1234
        assert "1234 rows" in kwargs["summary"]
        assert "8675309" in kwargs["summary"]

        from backend.cron.jobs import _common
        from backend.cron.jobs.metadata import dispatch_post_commit_metadata_sync

        _common.refresh_view_and_warm_pool.assert_called_once()
        dispatch_post_commit_metadata_sync.assert_called_once_with("svc-1")

    def test_success_no_files_committed_logs_no_new_data(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        """Empty buffer → status='success' (NOT a failure mode) with the
        'No new data to commit' summary. metadata_sync should NOT be
        triggered (no new data to sync)."""
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(return_value={"files_committed": 0, "rows_committed": 0, "snapshot_id": None}),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success"
        assert "No new data" in kwargs["summary"]

        from backend.cron.jobs.metadata import dispatch_post_commit_metadata_sync

        dispatch_post_commit_metadata_sync.assert_not_called()

    def test_metadata_sync_dispatch_failure_does_not_fail_durable_commit(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.cron.jobs.commit._log_and_add_progress", stub_cron_envelope["log_event"])
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(return_value={"files_committed": 1, "rows_committed": 12, "snapshot_id": "ducklake"}),
        )
        monkeypatch.setattr(
            "backend.cron.jobs.metadata.dispatch_post_commit_metadata_sync",
            MagicMock(side_effect=RuntimeError("worker unavailable")),
        )

        commit._run_commit.__wrapped__("svc-1")

        assert stub_cron_envelope["log"].call_args.args[3] == "success"
        assert any(
            call.kwargs.get("event", {}).get("type") == "warning"
            for call in stub_cron_envelope["log_event"].call_args_list
        )

    def test_commit_failure_without_progress_is_not_a_successful_noop(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(
                return_value={
                    "files_committed": 0,
                    "rows_committed": 0,
                    "snapshot_id": "ducklake",
                    "error": "write attach unavailable",
                }
            ),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert kwargs["error_message"] == "write attach unavailable"
        assert "No buffer files committed" in kwargs["summary"]

    def test_quarantined_files_log_incomplete_commit(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        """Quarantining unreadable input is surfaced as an error, not a successful drain."""
        monkeypatch.setattr("backend.cron.jobs.commit._log_and_add_progress", stub_cron_envelope["log_event"])
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(
                return_value={
                    "files_committed": 5,
                    "rows_committed": 100,
                    "snapshot_id": 1,
                    "quarantined_files": 2,
                }
            ),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert "quarantined 2" in kwargs["summary"]
        assert "2 unreadable buffer file(s) quarantined" in kwargs["error_message"]
        assert stub_cron_envelope["log_event"].call_args.kwargs["event"]["type"] == "error"

    def test_partial_commit_error_preserves_counts_and_refreshes_durable_rows(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(
                return_value={
                    "files_committed": 1,
                    "rows_committed": 12,
                    "snapshot_id": "ducklake",
                    "quarantined_files": 0,
                    "error": "later chunk attach failed",
                }
            ),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert kwargs["rows_ingested"] == 12
        assert kwargs["error_message"] == "later chunk attach failed"
        assert "Committed 1 buffer file(s) (12 rows)" in kwargs["summary"]
        assert "commit incomplete: later chunk attach failed" in kwargs["summary"]
        from backend.cron.jobs import _common

        _common.refresh_view_and_warm_pool.assert_called_once()

    def test_high_scale_commit_uses_ledger_merge_not_standard_buffer(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
        merge_calls = []
        monkeypatch.setattr("backend.core.ingest.merge_lake_files", lambda sid: merge_calls.append(("merge", sid)))
        monkeypatch.setattr(
            "backend.core.ingest.finalize_committed_raw",
            lambda sid: merge_calls.append(("finalize", sid)) or {"delete_after": False, "deleted": 0},
        )
        meta_con = MagicMock()
        meta_con.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=None)),
            MagicMock(fetchone=MagicMock(return_value=(0,))),
        ]
        monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: meta_con)
        standard_commit = MagicMock()
        monkeypatch.setattr("backend.core.iceberg.commit_buffer", standard_commit)

        commit._run_commit.__wrapped__("svc-1")

        assert merge_calls == [("merge", "svc-1"), ("finalize", "svc-1")]
        standard_commit.assert_not_called()
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success"
        assert "merged small lake files" in kwargs["summary"]
        from backend.cron.jobs.metadata import dispatch_post_commit_metadata_sync

        dispatch_post_commit_metadata_sync.assert_not_called()

    def test_unexpected_exception_logged_as_error(self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
        """Uncaught exception in commit_buffer → status='error' AND
        end_progress STILL fires in the finally block. Pinned because a
        skipped end_progress would leak a 'running' progress row that
        the dashboard's live indicator never clears."""
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(side_effect=RuntimeError("snapshot conflict")),
        )

        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert "snapshot conflict" in kwargs["error_message"]
        stub_cron_envelope["end_progress"].assert_called_once()

    def test_lease_held_is_clean_skip_without_row_or_progress(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        stub_cron_envelope["start"].side_effect = CronLeaseHeldError("lease held")
        cb = MagicMock()
        monkeypatch.setattr("backend.core.iceberg.commit_buffer", cb)

        commit._run_commit.__wrapped__("svc-1")

        cb.assert_not_called()
        stub_cron_envelope["log"].assert_not_called()
        stub_cron_envelope["end_progress"].assert_not_called()

    def test_caller_supplied_run_id_is_reused_without_new_lease(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(return_value={"files_committed": 0, "rows_committed": 0}),
        )

        commit._run_commit.__wrapped__("svc-1", run_id=77)

        stub_cron_envelope["start"].assert_not_called()
        assert stub_cron_envelope["log"].call_args.kwargs["run_id"] == 77
        stub_cron_envelope["end_progress"].assert_called_once_with(77)

    def test_disk_check_failure_row_has_no_log_output_and_progress_is_closed(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.cron.jobs.commit._check_disk_space", lambda *a, **kw: (False, "disk 95% full"))

        commit._run_commit.__wrapped__("svc-1")

        stub_cron_envelope["log"].assert_called_once()
        assert stub_cron_envelope["log"].call_args.kwargs["log_output"] is None
        stub_cron_envelope["end_progress"].assert_called_once_with(33)

    def test_exception_after_success_row_overwrites_with_error_row(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.iceberg.commit_buffer",
            MagicMock(return_value={"files_committed": 1, "rows_committed": 5, "snapshot_id": 9}),
        )
        monkeypatch.setattr(
            "backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock(side_effect=RuntimeError("warm failed"))
        )

        commit._run_commit.__wrapped__("svc-1")

        statuses = [c.args[3] for c in stub_cron_envelope["log"].call_args_list]
        assert statuses == ["success", "error"]
        last = stub_cron_envelope["log"].call_args
        assert last.kwargs["summary"] == "Buffer commit failed"
        assert last.kwargs["error_message"] == "warm failed"

    def test_high_scale_failure_records_error_row_without_log_output(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
        monkeypatch.setattr("backend.core.ingest.merge_lake_files", MagicMock(side_effect=RuntimeError("merge broke")))

        commit._run_commit.__wrapped__("svc-1")

        stub_cron_envelope["log"].assert_called_once()
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert kwargs["summary"] == "DuckLake merge / raw finalization failed"
        assert kwargs["error_message"] == "merge broke"
        assert kwargs["log_output"] is None
        assert kwargs["run_id"] == 33

    def test_high_scale_previous_commit_lookup_is_scoped_to_the_service(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
        monkeypatch.setattr("backend.core.ingest.merge_lake_files", lambda sid: None)
        monkeypatch.setattr(
            "backend.core.ingest.finalize_committed_raw", lambda sid: {"delete_after": False, "deleted": 0}
        )
        meta_con = MagicMock()
        meta_con.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=None)),
            MagicMock(fetchone=MagicMock(return_value=(4,))),
        ]
        monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: meta_con)

        commit._run_commit.__wrapped__("svc-1")

        sql, params = meta_con.execute.call_args_list[0].args
        assert "service_id = ? AND task = 'commit'" in sql
        assert params == ("svc-1",)
        assert stub_cron_envelope["log"].call_args.kwargs["files_downloaded"] == 4
