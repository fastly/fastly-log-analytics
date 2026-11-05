"""Consolidated tests for High-Scale log discovery, full sweep, gap heal, and ledger sweep cron jobs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend.core.metadata.usage_log import CronLeaseHeldError
from backend.cron.jobs.rum_ledger import _run_ledger_sweep, sweep_ledger_once
from backend.cron.jobs.sync import (
    _GAP_HEAL_LAST_TRIGGER,
    _gap_heal_severity,
    _is_batch_due,
    _last_successful_gap_heal_trigger,
    _run_full_sweep,
    _run_gap_heal,
    _run_log_discovery_cron,
    discover_prefix,
)
from backend.cron.scheduler import Scheduler

SRC = {
    "name": "svc-1",
    "service_id": "svc-1",
    "service_name": "Svc One",
    "storage_mode": "cloud",
    "bucket": "test-bucket",
    "prefix": "logs/",
}


def _cfg(sync_enabled: bool = True, **extra) -> dict:
    return {
        "service_id": "svc-1",
        "log_period": 10,
        "provisioning": {
            "cron_sync": {"enabled": sync_enabled, "polling_mode": "fixed"},
            **extra,
        },
    }


@pytest.fixture
def stub_sync_env(monkeypatch, stub_source, stub_cron_envelope):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.config.load_config", lambda sid: _cfg(True))
    monkeypatch.setattr("backend.cron.jobs.sync.invalidate_service_cache", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.sync.backfill_fastly_edge_writes", MagicMock(return_value=0))
    monkeypatch.setattr("backend.cron.jobs.sync.reconcile_fastly_stats", MagicMock(return_value=0))
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *_a, **_kw: False)
    monkeypatch.setattr("backend.utils.telemetry.run_usage_log_cleanup", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.publish_sync_status_safe", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    return stub_cron_envelope


# ── discover_prefix & _is_batch_due ──────────────────────────────────────────


def test_discover_prefix_delegates_to_high_scale_prefix(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "backend.cron.jobs.sync._discover_high_scale_prefix",
        lambda sid, *, domain, prefix_subpath=None: calls.append((sid, domain, prefix_subpath)) or 5,
    )
    assert discover_prefix("svc-1", prefix_subpath="raw/request/2026-04-12T10:00") == 5
    assert calls == [("svc-1", "request", "raw/request/2026-04-12T10:00")]


def test_is_batch_due_checks_latest_log_age(monkeypatch):
    now = datetime.now(UTC)
    recent_iso = (now - timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    stale_iso = (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

    monkeypatch.setattr("backend.config.get_status", lambda sid: {"request": {"latest_log_at": recent_iso}})
    assert _is_batch_due("svc-1", {"log_period": 10}) is True

    monkeypatch.setattr("backend.config.get_status", lambda sid: {"request": {"latest_log_at": stale_iso}})
    assert _is_batch_due("svc-1", {"log_period": 10}) is False

    monkeypatch.setattr("backend.config.get_status", lambda sid: None)
    assert _is_batch_due("svc-1", {"log_period": 10}) is False


# ── _run_log_discovery_cron ──────────────────────────────────────────────────


def test_log_discovery_refuses_when_dev_no_crons(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    load_cfg = MagicMock()
    monkeypatch.setattr("backend.config.load_config", load_cfg)
    _run_log_discovery_cron.__wrapped__("svc-1")
    load_cfg.assert_not_called()


def test_log_discovery_skips_when_config_or_source_missing(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.config.load_config", lambda sid: None)
    get_src = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync.get_source_for_service", get_src)
    _run_log_discovery_cron.__wrapped__("svc-1")
    get_src.assert_not_called()

    monkeypatch.setattr("backend.config.load_config", lambda sid: _cfg(True))
    monkeypatch.setattr("backend.cron.jobs.sync.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.fos_client.start_cron_run", start)
    _run_log_discovery_cron.__wrapped__("svc-1")
    start.assert_not_called()


def test_log_discovery_skips_when_lease_held(monkeypatch, stub_sync_env):
    stub_sync_env["start"].side_effect = CronLeaseHeldError("already running")
    disc = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", disc)
    _run_log_discovery_cron.__wrapped__("svc-1")
    disc.assert_not_called()
    stub_sync_env["log"].assert_not_called()


def test_log_discovery_succeeds_without_celery_broker_url(monkeypatch, stub_sync_env):
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", lambda sid, **kw: 1)
    _run_log_discovery_cron.__wrapped__("svc-1")
    stub_sync_env["log"].assert_called_once()
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 5
    assert "Discovered 5 new file(s)" in kwargs["summary"]


def test_log_discovery_discovers_files_and_refreshes_status(monkeypatch, stub_sync_env):
    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", lambda sid, **kw: 2)
    monkeypatch.setattr("backend.cron.jobs.sync._claim_heavy_refresh", lambda sid: True)
    invalidate = MagicMock()
    publish = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync.invalidate_service_cache", invalidate)
    monkeypatch.setattr("backend.cron.jobs._common.publish_sync_status_safe", publish)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize)

    _run_log_discovery_cron.__wrapped__("svc-1", force=True, run_id=7)

    stub_sync_env["log"].assert_called_once()
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 10
    assert "Discovered 10 new file(s)" in kwargs["summary"]
    invalidate.assert_called_once_with("fos-test-svc")
    publish.assert_called_once_with("svc-1", "scheduler")
    finalize.assert_called_once()


def test_log_discovery_records_error_when_discover_prefix_raises(monkeypatch, stub_sync_env):
    monkeypatch.setattr(
        "backend.cron.jobs.sync.discover_prefix",
        MagicMock(side_effect=RuntimeError("FOS 503")),
    )
    _run_log_discovery_cron.__wrapped__("svc-1")
    stub_sync_env["log"].assert_called_once()
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Log discovery failed"
    assert "FOS 503" in kwargs["error_message"]


# ── _run_full_sweep ──────────────────────────────────────────────────────────


def test_full_sweep_refuses_when_dev_no_crons(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    get_src = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync.get_source_for_service", get_src)
    _run_full_sweep.__wrapped__("svc-1")
    get_src.assert_not_called()


def test_full_sweep_defers_unless_forced(monkeypatch, stub_sync_env):
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *_a, **_kw: True)
    disc = MagicMock(return_value=4)
    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", disc)

    _run_full_sweep.__wrapped__("svc-1", force=False)
    disc.assert_not_called()

    _run_full_sweep.__wrapped__("svc-1", force=True)
    disc.assert_called_once_with("svc-1", run_id=11, originating_task="full_sync")
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 4


def test_full_sweep_logs_error_on_exception(monkeypatch, stub_sync_env):
    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", MagicMock(side_effect=RuntimeError("S3 timeout")))
    _run_full_sweep.__wrapped__("svc-1", force=True)
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Full-prefix ledger sweep failed"
    assert "S3 timeout" in kwargs["error_message"]


# ── _run_gap_heal ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "gap_pct,lost_lines,expected_band",
    [
        (0.85, 100, "critical"),
        (0.10, 600_000, "critical"),
        (0.55, 100, "severe"),
        (0.15, 500, "elevated"),
        (0.02, 50, "mild"),
    ],
)
def test_gap_heal_severity_classification(gap_pct, lost_lines, expected_band):
    band = _gap_heal_severity(gap_pct, lost_lines)
    assert band.name == expected_band


def test_gap_heal_skips_when_fastly_stats_unavailable(monkeypatch, stub_sync_env):
    def _raise_stats_err(*_a, **_kw):
        raise HTTPException(status_code=502, detail={"error": "fastly_stats_failed"})

    monkeypatch.setattr("backend.routers.admin.compute_log_accounting", _raise_stats_err)
    _run_gap_heal.__wrapped__("svc-1")
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert "Fastly stats unavailable" in kwargs["summary"]


def test_gap_heal_succeeds_when_no_sustained_loss(monkeypatch, stub_sync_env):
    monkeypatch.setattr("backend.routers.admin.compute_log_accounting", lambda *_a, **_kw: {"sustained_loss": None})
    sweep = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync._run_full_sweep", sweep)
    _run_gap_heal.__wrapped__("svc-1")
    sweep.assert_not_called()
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["summary"] == "No sustained loss detected"


def test_gap_heal_triggers_full_sweep_and_throttles_subsequent_runs(monkeypatch, stub_sync_env):
    _GAP_HEAL_LAST_TRIGGER.clear()
    sustained = SimpleNamespace(
        n_buckets=3,
        started_at="2026-04-12T08:00:00Z",
        max_gap_pct=0.25,
        total_lost_lines=25_000,
    )
    monkeypatch.setattr(
        "backend.routers.admin.compute_log_accounting",
        lambda *_a, **_kw: {"sustained_loss": sustained},
    )
    sweep = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.sync._run_full_sweep", sweep)

    _run_gap_heal.__wrapped__("svc-1")
    sweep.assert_called_once()
    assert _last_successful_gap_heal_trigger("svc-1") is not None
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "warning"
    assert "triggering full_sweep" in kwargs["summary"]

    # Second run within throttle window should warn without triggering _run_full_sweep
    sweep.reset_mock()
    stub_sync_env["log"].reset_mock()
    _run_gap_heal.__wrapped__("svc-1")
    sweep.assert_not_called()
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "warning"
    assert "throttled" in kwargs["summary"]


# ── _run_ledger_sweep & sweep_ledger_once ────────────────────────────────────


def test_sweep_ledger_once_aggregates_request_domain(monkeypatch, stub_source):
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.cron.jobs.rum_ledger._discover_high_scale_prefix", lambda sid, **kw: 7)
    fake_ctrl = MagicMock()
    fake_ctrl.expired_claims.return_value = ["c1"]
    fake_ctrl.operational_snapshot.return_value = {
        "source_objects": [
            {"domain": "request", "status": "source_missing", "count": 2},
            {"domain": "rum_vitals", "status": "source_missing", "count": 5},
        ]
    }
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.PostgresControlPlane", lambda: fake_ctrl)
    summary = sweep_ledger_once("svc-1")
    assert summary == {
        "reclaimed": 1,
        "redispatched": 1,
        "discovered": 7,
        "dead_letter": 2,
        "broker_ok": True,
    }


def test_run_ledger_sweep_records_summary(monkeypatch, stub_sync_env):
    monkeypatch.setattr(
        "backend.cron.jobs.rum_ledger.sweep_ledger_once",
        lambda sid, **kw: {"reclaimed": 2, "redispatched": 2, "discovered": 5, "dead_letter": 0, "broker_ok": True},
    )
    _run_ledger_sweep.__wrapped__("svc-1")
    args, kwargs = stub_sync_env["log"].call_args
    assert args[3] == "success"
    assert "reclaimed=2 redispatched=2 discovered=5" in kwargs["summary"]
    assert kwargs["files_downloaded"] == 5


# ── Scheduler registration of ingest-class jobs ─────────────────────────────


def test_scheduler_sync_jobs_registers_high_scale_ingest_jobs(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    cfg = {
        "service_id": "svc-hs",
        "log_period": 30,
        "rum": {"enabled": True},
        "provisioning": {},
    }
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.fos_client.get_source_for_service", lambda sid: SRC)
    monkeypatch.setattr("backend.core.fos_client.is_configured", lambda src: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda sid: None)
    monkeypatch.setattr("backend.cron.scheduler.runner._service_has_alerts", lambda sid: False)

    s = Scheduler()
    s._sched = MagicMock()
    s._sync_jobs()

    for expected_id in (
        "log_discovery_svc-hs",
        "ledger_sweep_svc-hs",
        "rum_discovery_svc-hs",
        "ledger_rum_sweep_svc-hs",
        "full_sync_svc-hs",
        "gap_heal_svc-hs",
        "insights_prewarmer_svc-hs",
        "metadata_cleanup_svc-hs",
        "bot_data_refresh",
        "rdns_enrichment",
        "share_audit_purge",
    ):
        assert expected_id in s._job_ids, f"Expected {expected_id} to be registered"
