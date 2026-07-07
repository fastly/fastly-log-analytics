"""FLA_DEV_NO_CRONS kill-switch coverage.

The kill switch is the single env-var the dev workflow relies on to
prevent local-dev backends from ingesting against the same FOS bucket
prod is reading. Four enforcement points:

  1. ``Scheduler.start()``       — skip ALL job registration except the
                                   local-only allowlist (local_compact,
                                   rollup_compact); never call _sync_jobs
  2. ``Scheduler.reload()``      — refuse to re-arm jobs after start bailed
  3. ``_run_service_cron``       — refuse manual /admin/ingest-logs force=True
  4. ``_run_full_sweep``         — refuse gap_heal's direct invocation
  5. ``_run_gap_heal``           — refuse the gap heal itself

If ANY of these regress, dev backends can silently start ingesting and
race the prod cron. These tests pin the contract.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

# ── The env-var helper ──────────────────────────────────────────────────────


def test_dev_mode_no_crons_returns_false_when_unset(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "True"])
def test_dev_mode_no_crons_truthy_values(monkeypatch, value):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", value)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "", "off"])
def test_dev_mode_no_crons_falsy_values(monkeypatch, value):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", value)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is False


# ── Scheduler.start() ──────────────────────────────────────────────────────


def test_scheduler_start_skips_full_registration_but_runs_local_allowlist(monkeypatch):
    """Most important contract test: start() must NOT call _sync_jobs (the
    full registration that arms FOS-writing / ingest / outbound jobs) when
    the kill switch is set — it only runs the local-only allowlist via
    _register_dev_local_safe_jobs. If _sync_jobs regressed back in, every
    cron job (sync/commit/optimize/expire/...) would tick on dev."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    with (
        patch.object(sched, "_sync_jobs") as sync_jobs,
        patch.object(sched, "_register_dev_local_safe_jobs") as dev_jobs,
        patch.object(sched._sched, "start") as ap_start,
    ):
        sched.start()
        sync_jobs.assert_not_called()  # full registration must NEVER run on dev
        dev_jobs.assert_called_once()  # only the local-safe allowlist runs
        # allowlist was mocked → no job IDs → APScheduler stays stopped
        ap_start.assert_not_called()


def test_dev_local_allowlist_registers_only_local_compaction(monkeypatch):
    """_register_dev_local_safe_jobs arms local_compact + rollup_compact +
    rollup_heal and NOTHING else — no sync/commit/optimize/expire/ngwaf/etc.
    All three only rewrite the local parquet cache (never FOS), which is the
    allowlist's admission bar."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-x",
        "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
    }
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-x"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert sorted(sched._job_ids) == ["local_compact_svc-x", "rollup_compact_svc-x", "rollup_heal_svc-x"]
    added = sorted(c.kwargs["id"] for c in add_job.call_args_list)
    assert added == ["local_compact_svc-x", "rollup_compact_svc-x", "rollup_heal_svc-x"]


def test_dev_local_allowlist_skips_rollup_compact_for_read_only(monkeypatch):
    """read_only services own no rollup data → only local_compact arms."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    cfg = {"service_id": "svc-ro", "provisioning": {"access_level": "read_only"}}
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ro"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert sorted(sched._job_ids) == ["local_compact_svc-ro"]
    added = [c.kwargs["id"] for c in add_job.call_args_list]
    assert added == ["local_compact_svc-ro"]


def test_scheduler_reload_is_a_noop_when_kill_switch_on(monkeypatch):
    """reload() is called by service-config saves. If it re-registers jobs
    after start() bailed, an admin clicking 'Save' in the cron settings
    modal would sneak crons back in. This pins that it can't happen."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    with patch.object(sched, "_sync_jobs") as sync_jobs:
        sched.reload()
        sync_jobs.assert_not_called()


def test_scheduler_start_runs_normally_when_kill_switch_off(monkeypatch):
    """The kill switch defaults to off — prod behavior is unchanged."""
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    # Stub the heavy parts so the test stays fast; just verify the
    # gating logic doesn't short-circuit them.
    with patch.object(sched, "_sync_jobs") as sync_jobs, patch.object(sched._sched, "start") as ap_start:
        with patch("backend.config.list_configs", return_value=[]):
            sched.start()
        sync_jobs.assert_called_once()
        ap_start.assert_called_once()


# ── Ingest-class job entry points ───────────────────────────────────────────


def test_run_service_cron_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_service_cron is the path that manual /admin/ingest-logs hits
    with force=True. force=True intentionally bypasses
    provisioning.cron_sync.enabled. The env var beats both."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_service_cron

    # If the gate works, _run_service_cron returns immediately and
    # ingest() is never imported. Patch get_source_for_service to a
    # MagicMock; if the gate fails, the function would call it and the
    # MagicMock would record the call.
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_service_cron("svc-test", force=True)
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


def test_run_full_sweep_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_full_sweep is what gap_heal invokes directly (NOT through a
    config check). This is the leading-suspect bypass-candidate the
    dev-mode investigation identified for the 23:21:11 surprise sync."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_full_sweep

    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_full_sweep("svc-test")
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


def test_run_gap_heal_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_gap_heal sees local dev as having a 100% ingest gap against
    prod's FOS bucket (because dev never ingested) — without the gate it
    would happily trigger a full sweep over the prod data."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_gap_heal

    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_gap_heal("svc-test")
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)
