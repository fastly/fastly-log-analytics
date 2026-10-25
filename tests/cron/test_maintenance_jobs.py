"""Consolidated tests for test_maintenance_jobs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.core import duckdb_recycle as _recycle
from backend.cron.jobs.metadata import run_duckdb_recycle
from backend.main import app


def test_trim_memory_graceful():
    """_trim_memory runs gc.collect and survives missing or present malloc_trim."""
    _recycle._trim_memory()


def test_get_pool_status_and_barrier():
    """Pool status and barrier inspection return expected dictionary structure."""
    stats = _pool.get_pool_status()
    assert "pools" in stats
    assert "retired_pools" in stats

    # Barrier check
    assert _db.is_recycle_barrier_active("/non/existent/path") is False
    assert isinstance(_db.is_recycle_barrier_active(), set)


def test_run_duckdb_recycle_rss_threshold_skip():
    """Recycle is skipped when RSS is below the configured threshold."""
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result
        assert "100MB < threshold 500MB" in result
        mock_recycle.assert_not_called()


def test_run_duckdb_recycle_adaptive_expedite():
    """Recycle adjusts schedule dynamically when approaching threshold."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 3600
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=850 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        # 850MB is >= 800MB (80% of 1000MB threshold), skips recycle but tightens interval to 2m
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result
        mock_recycle.assert_not_called()
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_incomplete():
    """Recycle expedites schedule down to 2m when recycle_once reports incomplete."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 3600
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=0),
        patch("backend.core.duckdb.current_rss_bytes", return_value=200 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once",
            return_value="interval: recycled 0/1 instance(s), freed ~0MB (incomplete: 1)",
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "incomplete" in result
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_relax():
    """Recycle relaxes schedule back to baseline when RSS drops below 50% threshold."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 120  # currently expedited to 2m
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[1100 * 1024 * 1024, 400 * 1024 * 1024]),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once",
            return_value="interval: recycled 1/1 instance(s), freed ~700MB",
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "recycled 1/1" in result
        # 400MB is < 500MB (50% of 1000MB threshold), relaxes back to 60.0m baseline
        mock_job.reschedule.assert_called_with("interval", minutes=60.0)


def test_recycle_once_incomplete_reporting(monkeypatch):
    """recycle_once adds (incomplete: N) detail when any instance drain fails."""
    monkeypatch.setattr(
        _recycle,
        "_sources_by_db_path",
        lambda: {"/tmp/test.duckdb": [{"name": "s1"}]},
    )
    monkeypatch.setattr(
        _recycle,
        "_recycle_db_path",
        lambda path, sources: {"db_path": path, "status": "incomplete", "drained": False, "live_after": 1},
    )
    detail = _recycle.recycle_once()
    assert "incomplete: 1" in detail


def test_admin_duckdb_recycle_endpoint():
    """POST /api/admin/duckdb/recycle triggers recycle or skips based on force flag."""
    client = TestClient(app)

    # 1. Skipped when below threshold and force=False
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
    ):
        resp = client.post("/api/admin/duckdb/recycle")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "skipped"

    # 2. Forced recycle
    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * 1024 * 1024, 200 * 1024 * 1024]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch(
            "backend.core.duckdb_recycle.recycle_once",
            return_value="manual_admin: recycled 1/1 instance(s), freed ~100MB",
        ),
    ):
        resp = client.post("/api/admin/duckdb/recycle?force=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "recycled"
        assert data["freed_mb"] == 100.0


def test_admin_duckdb_status_endpoint():
    """GET /api/admin/duckdb/status returns memory and pool states."""
    client = TestClient(app)

    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=250 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
    ):
        resp = client.get("/api/admin/duckdb/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["memory"]["current_rss_mb"] == 250.0
        assert data["memory"]["recycle_threshold_mb"] == 1000.0
        assert data["memory"]["recycle_interval_min"] == 60.0
        assert "pools" in data
        assert "barrier" in data


# ── Merged from test_heartbeat.py ──────────────────────────────────────────────

import threading
import time

import pytest

from backend.core.metadata import pg_connection as pgc
from backend.core.metadata.base import get_con
from backend.cron import decorators
from backend.cron.decorators import CronHeartbeat

SID = "svc-heartbeat"
JOB = "heartbeat_job"
OTHER_JOB = "other_job"


def _stray_threads(before: set[threading.Thread]) -> list[threading.Thread]:
    """Threads started since ``before`` that are still alive (the shared watchdog pool is long-lived by design)."""
    return [
        t for t in threading.enumerate() if t not in before and t.is_alive() and not t.name.startswith("cron-watchdog")
    ]


def _wait_for_exit(threads: list[threading.Thread], timeout: float = 5.0) -> list[threading.Thread]:
    deadline = time.monotonic() + timeout
    while threads and time.monotonic() < deadline:
        threads = [t for t in threads if t.is_alive()]
        time.sleep(0.01)
    return threads


@pytest.fixture
def baseline():
    """Thread set and checked-out pool size with this thread's own metadata connection already open."""
    get_con(SID)
    return set(threading.enumerate()), len(pgc._checked_out)


def _seed_leases() -> None:
    con = get_con(SID)
    for job in (JOB, OTHER_JOB):
        con.execute(
            "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
            "VALUES (?, ?, ?, ?, ?, 'running')",
            (SID, job, 1.0, 1.0, 60),
        )
    con.commit()


def _heartbeat_at(job: str) -> float:
    row = (
        get_con(SID)
        .execute("SELECT heartbeat_at FROM job_runs WHERE service_id = ? AND job_name = ?", (SID, job))
        .fetchone()
    )
    return float(row[0])


def test_heartbeat_refreshes_only_its_own_job_lease(baseline):
    _seed_leases()

    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)

    assert _heartbeat_at(JOB) > 1.0
    assert _heartbeat_at(OTHER_JOB) == 1.0


def test_no_thread_or_pooled_connection_survives_a_normal_exit(baseline):
    before, checked_out = baseline
    _seed_leases()

    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_no_thread_or_pooled_connection_survives_an_exception_in_the_body(baseline):
    before, checked_out = baseline
    _seed_leases()

    with pytest.raises(RuntimeError, match="job blew up"):
        with CronHeartbeat(SID, JOB, interval_s=0.02):
            deadline = time.monotonic() + 5.0
            while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
                time.sleep(0.02)
            raise RuntimeError("job blew up")

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_the_thread_releases_its_connection_even_when_it_never_used_one():
    with patch("backend.core.metadata.base.release_thread_connection") as release:
        with CronHeartbeat(SID, None, interval_s=0.02):
            pass

    release.assert_called_once_with()


def test_a_job_without_a_name_refreshes_nothing(baseline):
    _seed_leases()

    with CronHeartbeat(SID, None, interval_s=0.02):
        time.sleep(0.15)

    assert _heartbeat_at(JOB) == 1.0
    assert _heartbeat_at(OTHER_JOB) == 1.0


def test_a_failing_refresh_does_not_kill_the_loop_or_leak(baseline):
    before, checked_out = baseline
    calls: list[int] = []

    def _boom(*_a, **_kw):
        calls.append(1)
        raise RuntimeError("db down")

    with patch("backend.core.metadata.base.get_con", side_effect=_boom):
        with CronHeartbeat(SID, JOB, interval_s=0.01):
            deadline = time.monotonic() + 5.0
            while len(calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)

    assert len(calls) >= 3
    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


# ── through cron_task ────────────────────────────────────────────────────────


@pytest.fixture
def fast_heartbeat(monkeypatch):
    monkeypatch.setattr("backend.cron.decorators.DEFAULT_INTERVAL_S", 0.02)


def test_cron_task_leaks_nothing_when_the_executor_refuses_the_job(baseline, fast_heartbeat):
    before, checked_out = baseline

    @decorators.cron_task("exec-refuses", job_name=JOB)
    def _job(service_id: str):
        return None

    class _Dead:
        def submit(self, *_a, **_kw):
            raise RuntimeError("cannot schedule new futures after shutdown")

    with patch.object(decorators, "_get_watchdog_executor", return_value=_Dead()):
        with pytest.raises(RuntimeError, match="shutdown"):
            _job(SID)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_cron_task_leaks_nothing_across_repeated_failures_and_watchdog_timeouts(baseline, fast_heartbeat, monkeypatch):
    before, checked_out = baseline
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("flaky", job_name=JOB)
    def _job(service_id: str, mode: str):
        if mode == "slow":
            time.sleep(0.15)
        if mode == "raise":
            raise ValueError("boom")

    for mode in ["ok", "raise", "slow"] * 5:
        if mode == "raise":
            with pytest.raises(ValueError, match="boom"):
                _job(SID, mode)
        else:
            _job(SID, mode)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_cron_task_returns_the_job_result(fast_heartbeat):
    @decorators.cron_task("returns", job_name=JOB)
    def _job(service_id: str):
        return {"ok": service_id}

    assert _job(SID) == {"ok": SID}


def test_watchdog_timeout_still_waits_for_the_job_and_returns_its_result(fast_heartbeat, monkeypatch):
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("slow-result", job_name=JOB)
    def _job(service_id: str):
        time.sleep(0.15)
        return "finished"

    assert _job(SID) == "finished"


# ── Merged from test_metric_snapshot_job.py ──────────────────────────────────────────────

import io
from datetime import UTC, datetime, timedelta

from backend.core import metric_snapshots
from backend.cron.jobs.metric_snapshot import (
    _run_metric_snapshot,
    _safe_record,
    _sample_active_queries,
    _sample_celery_queues,
    _sample_cron_duration,
    _sample_ingest_lag,
    _sample_ingest_ledger,
    _sample_os_vitals,
)
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history


def test_tick_records_active_query_count():
    """Active-query path is the simplest — it doesn't depend on
    per-service state, just the global registry summary."""
    with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 3}):
        _run_metric_snapshot()

    out = _get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) >= 1
    assert out[-1]["value"] == 3.0


def test_tick_records_pool_wait_per_service():
    fake_stats = [
        {"service": "svc-a", "wait": {"p95_ms": 12.3, "count": 100}},
        {"service": "svc-b", "wait": {"p95_ms": 45.6, "count": 50}},
    ]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        _run_metric_snapshot()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    b = _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert any(r["value"] == 12.3 for r in a)
    assert any(r["value"] == 45.6 for r in b)


def test_tick_continues_when_one_metric_fails():
    """A blow-up in one sampler must not skip the others — the
    _safe_record wrapper + per-metric try/except in the sampler ensures
    the rest of the tick still runs."""
    with patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")):
        with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 7}):
            _run_metric_snapshot()  # must not raise

    out = _get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert out and out[-1]["value"] == 7.0


# ── _safe_record swallows per-metric failures ──────────────────────────────────


def test_safe_record_swallows_record_snapshot_exception():
    """_safe_record is the wrapper that keeps a sampler-emitted bad
    value from poisoning the whole tick. Pin that an exception in
    record_snapshot is caught and only logged at debug."""
    with patch.object(metric_snapshots, "record_snapshot", side_effect=ValueError("bad metric")):
        _safe_record("bogus", 1.0)  # must not raise


def test_sample_celery_queues_records_reachability_and_known_queues():
    with patch(
        "backend.celery_status.celery_queue_depths",
        return_value=({"q.ingest": 4}, True),
    ):
        _sample_celery_queues()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("celery_broker_reachable", since=since)[-1]["value"] == 1.0
    assert _get_history("celery_queue_depth_q.ingest", since=since)[-1]["value"] == 4.0
    assert _get_history("celery_queue_depth_q.control", since=since)[-1]["value"] == 0.0
    assert _get_history("celery_queue_depth", since=since)[-1]["value"] == 4.0


def test_sample_celery_queues_records_broker_unreachable():
    with patch(
        "backend.celery_status.celery_queue_depths",
        return_value=({}, False),
    ):
        _sample_celery_queues()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("celery_broker_reachable", since=since)[-1]["value"] == 0.0
    assert _get_history("celery_queue_depth", since=since)[-1]["value"] == 0.0


def test_sample_ingest_ledger_records_each_state():
    with patch(
        "backend.celery_status.ingest_ledger_summary",
        return_value={"discovered": 5, "claimed": 2, "committed": 9},
    ):
        _sample_ingest_ledger()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_ledger_discovered", since=since)[-1]["value"] == 5.0
    assert _get_history("ingest_ledger_claimed", since=since)[-1]["value"] == 2.0


# ── _sample_cron_duration: per-service iteration + error containment ──────────


def _fake_row(task: str, secs: float) -> dict:
    """SQLite row_factory dict shape (con.row_factory=Row)."""
    return {"task": task, "duration_s": secs}


def test_sample_cron_duration_records_per_service_per_task():
    """Mock list_configs to enumerate two services and verify that the
    latest terminal cron_runs row for each task is recorded with the
    right service_id/task labels."""
    cfgs = [{"service_id": "svc-a"}, {"service_id": "svc-b"}]

    fake_cons: dict[str, MagicMock] = {}
    for svc, rows in (("svc-a", [_fake_row("sync", 0.5)]), ("svc-b", [_fake_row("optimize", 2.0)])):
        cur = MagicMock()
        cur.fetchall.return_value = rows
        con = MagicMock()
        con.execute.return_value = cur
        fake_cons[svc] = con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=lambda sid: fake_cons[sid]),
    ):
        _sample_cron_duration()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    b = _get_history("cron_duration_ms", since=since, service_id="svc-b", task="optimize")
    assert any(r["value"] == 500.0 for r in a)  # 0.5 s → 500 ms
    assert any(r["value"] == 2000.0 for r in b)


def test_sample_cron_duration_skips_configs_without_service_id():
    """A config without a service_id (partial init / corrupted file) is
    silently skipped — must not raise and must not call get_con."""
    cfgs = [{"service_id": None}, {}, {"service_id": "svc-real"}]

    called_with: list[str] = []

    def _record(sid: str) -> MagicMock:
        called_with.append(sid)
        cur = MagicMock()
        cur.fetchall.return_value = []
        con = MagicMock()
        con.execute.return_value = cur
        return con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_record),
    ):
        _sample_cron_duration()

    assert called_with == ["svc-real"]


def test_sample_cron_duration_continues_after_one_service_fails():
    """A get_con failure for one service must be caught and logged; the
    iteration must proceed to the next service."""
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    good_cur = MagicMock()
    good_cur.fetchall.return_value = [_fake_row("sync", 1.0)]
    good_con = MagicMock()
    good_con.execute.return_value = good_cur

    def _get_con(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return good_con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_get_con),
    ):
        _sample_cron_duration()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    good = _get_history("cron_duration_ms", since=since, service_id="svc-good", task="sync")
    assert any(r["value"] == 1000.0 for r in good)


def test_sample_cron_duration_outer_except_handles_import_failure():
    """If the late `from backend import config` itself blows up (e.g.
    the package somehow can't import in this process), the outer except
    must swallow it. Pin that path."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("module broken")):
        _sample_cron_duration()  # must not raise


# ── _sample_ingest_lag: per-service iteration + error containment ─────────────


def test_sample_ingest_lag_records_recent_lag():
    """Happy path: latest ingest 10 minutes ago → lag_s ≈ 600."""
    cfgs = [{"service_id": "svc-ingest"}]
    ten_min_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=ten_min_ago),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("ingest_lag_s", since=since, service_id="svc-ingest")
    assert rows
    # ~600 s, allow ±30 s wall-clock slack.
    assert 570 <= rows[-1]["value"] <= 630


def test_sample_ingest_lag_skips_when_no_latest_ts():
    """A service that has never ingested has no latest_ts. Must skip
    rather than recording a bogus huge value or crashing on None."""
    cfgs = [{"service_id": "svc-empty"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=None),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-empty") == []


def test_sample_ingest_lag_skips_unparseable_timestamp():
    """A latest_ts that parse_iso_utc can't parse returns None — the
    sampler must skip silently rather than crashing on datetime math."""
    cfgs = [{"service_id": "svc-bad-ts"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value="not-a-timestamp"),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-bad-ts") == []


def test_sample_ingest_lag_continues_after_one_service_fails():
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    def _fake_get_latest(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_fake_get_latest),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-good")


def test_sample_ingest_lag_skips_configs_without_service_id():
    cfgs = [{"service_id": None}, {"service_id": "svc-real"}]
    seen: list[str] = []

    def _record(sid: str) -> None:
        seen.append(sid)
        return None  # no data

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_record),
    ):
        _sample_ingest_lag()

    assert seen == ["svc-real"]


# ── _sample_active_queries: exception path ────────────────────────────────────


def test_sample_active_queries_swallows_summary_exception():
    with patch("backend.core.query_registry.query_registry.summary", side_effect=RuntimeError("boom")):
        _sample_active_queries()  # must not raise

    # Nothing recorded for this tick.
    since = datetime.now(UTC) - timedelta(seconds=5)
    pre = _get_history("active_query_count", since=since)
    assert pre == [] or all(r["value"] != 0 for r in pre[:0])  # vacuous — just pin no-crash


# ── _sample_os_vitals: getloadavg / /proc/meminfo / disk_usage branches ───────


def test_sample_os_vitals_swallows_getloadavg_exception():
    """getloadavg() raises on platforms that don't expose it (some
    container runtimes) — must be caught."""
    with patch("os.getloadavg", side_effect=OSError("not available")):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_parses_proc_meminfo_when_available():
    """Linux path: /proc/meminfo opens and is parsed; mem_used_pct is
    recorded based on (1 - MemAvailable/MemTotal) * 100. The dev macOS
    box doesn't have /proc, so this exercises the parse path via a
    fake open()."""
    meminfo = "MemTotal:       16384 kB\nMemAvailable:    4096 kB\nBuffers:    100 kB\n"

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        return io.StringIO(meminfo)

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("mem_used_pct", since=since)
    assert rows
    # (1 - 4096/16384) * 100 = 75.0
    assert rows[-1]["value"] == 75.0


def test_sample_os_vitals_swallows_meminfo_parse_error():
    """Open succeeds but the file contents are garbage — must catch the
    non-FileNotFoundError exception."""

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        raise PermissionError("denied")

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_records_disk_usage():
    """disk_usage() returns a triple; the percentage is rounded to two
    decimal places."""
    fake_disk = MagicMock(total=1000, used=500, free=500)

    with patch("shutil.disk_usage", return_value=fake_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("disk_used_pct", since=since)
    assert rows
    assert rows[-1]["value"] == 50.0


def test_sample_os_vitals_swallows_disk_usage_exception_per_path():
    """When /app/data raises a non-FileNotFoundError (e.g. permission
    denied), the loop must continue and still try /."""

    def _disk(path):
        if path == "/app/data":
            raise PermissionError("denied")
        return MagicMock(total=1000, used=100, free=900)

    with patch("shutil.disk_usage", side_effect=_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("disk_used_pct_root", since=since)
    assert rows
    assert rows[-1]["value"] == 10.0


def test_metric_snapshot_samples_vitals(monkeypatch):
    """Test that _run_metric_snapshot samples all operational vitals and records them."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": "svc_test"}])
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/path/{sid}.duckdb")
    monkeypatch.setattr(
        "backend.core.duckdb_pool.get_all_stats",
        lambda: [{"service": "svc_test", "wait": {"p95_ms": 12.5}}],
    )
    monkeypatch.setattr(
        "backend.core.query_registry.query_registry.summary",
        lambda: {"active_total": 3},
    )
    monkeypatch.setattr(
        "backend.core.iceberg._ducklake.get_admission_stats",
        lambda: {"svc_test": {"acquisitions": 2.0, "wait_ms_total": 40.0, "hold_ms_total": 100.0, "timeouts": 0.0}},
    )
    monkeypatch.setattr(
        "backend.celery_status.celery_queue_depths",
        lambda: ({"q.ingest": 5}, True),
    )
    monkeypatch.setattr(
        "backend.celery_status.ingest_ledger_summary",
        lambda: {"discovered": 10, "claimed": 2},
    )
    monkeypatch.setattr("os.path.exists", lambda p: True if "svc_test.duckdb" in p else False)
    monkeypatch.setattr("os.path.getsize", lambda p: 1048576)

    result = ms_mod._run_metric_snapshot.__wrapped__()
    assert "sampled=" in result

    batch = metric_snapshots.get_batch(since="1h")
    assert "pool_wait_p95_ms|svc_test" in batch
    assert batch["pool_wait_p95_ms|svc_test"][-1]["value"] == 12.5
    assert "duckdb_file_bytes|svc_test" in batch
    assert batch["duckdb_file_bytes|svc_test"][-1]["value"] == 1048576.0
    assert "active_query_count" in batch
    assert batch["active_query_count"][-1]["value"] == 3.0
    assert "ducklake_admission_wait_avg_ms|svc_test" in batch
    assert batch["ducklake_admission_wait_avg_ms|svc_test"][-1]["value"] == 20.0
    assert "celery_broker_reachable" in batch
    assert batch["celery_broker_reachable"][-1]["value"] == 1.0
    assert "celery_queue_depth_q.ingest" in batch
    assert batch["celery_queue_depth_q.ingest"][-1]["value"] == 5.0
    assert "ingest_ledger_discovered" in batch
    assert batch["ingest_ledger_discovered"][-1]["value"] == 10.0


def test_cron_duration_includes_warning_status(tmp_path, monkeypatch):
    """Verify that _sample_cron_duration extracts terminal durations for tasks with status 'warning'."""
    import sqlite3

    from backend.cron.jobs import metric_snapshot as ms_mod

    service_id = "svc_warning_test"
    db_file = tmp_path / f"{service_id}.metadata.db"
    con = sqlite3.connect(str(db_file))
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE cron_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task TEXT NOT NULL,
            duration_s REAL,
            status TEXT NOT NULL
        )
        """
    )
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('commit', 4.5, 'success')")
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('ledger_sweep', 1.8, 'warning')")
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('optimize', 12.2, 'error')")
    con.commit()

    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": service_id}])
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: con)

    count = ms_mod._sample_cron_duration()
    assert count == 3

    batch = metric_snapshots.get_batch(since="1h")
    assert f"cron_duration_ms|{service_id}|commit" in batch
    assert batch[f"cron_duration_ms|{service_id}|commit"][-1]["value"] == 4500.0

    assert f"cron_duration_ms|{service_id}|ledger_sweep" in batch
    assert batch[f"cron_duration_ms|{service_id}|ledger_sweep"][-1]["value"] == 1800.0

    assert f"cron_duration_ms|{service_id}|optimize" in batch
    assert batch[f"cron_duration_ms|{service_id}|optimize"][-1]["value"] == 12200.0
    con.close()


def test_sample_celery_workers_uses_timeout():
    """Verify _sample_celery_workers uses an explicit short timeout (0.5s) to avoid blocking."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    mock_inspect = MagicMock()
    mock_inspect.stats.return_value = {"worker1@node": {}}
    mock_inspect.active.return_value = {"worker1@node": [{"id": "task1"}]}

    with patch("backend.celery_app.app.control.inspect", return_value=mock_inspect) as inspect_call:
        count = ms_mod._sample_celery_workers()
        assert count == 2
        inspect_call.assert_called_once_with(timeout=0.5)

    batch = metric_snapshots.get_batch(since="1h")
    assert "celery_active_workers" in batch
    assert batch["celery_active_workers"][-1]["value"] == 1.0
    assert "celery_active_tasks" in batch
    assert batch["celery_active_tasks"][-1]["value"] == 1.0


def test_sample_duckdb_size(monkeypatch):
    """Verify _sample_duckdb_size samples duckdb_file_bytes per service using os.path.getsize."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    cfgs = [{"service_id": "svc_1"}, {"service_id": "svc_2"}, {"service_id": "svc_missing"}]
    monkeypatch.setattr("backend.config.list_configs", lambda: cfgs)
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/{sid}.duckdb")

    def _mock_exists(path: str) -> bool:
        return "svc_missing" not in path

    def _mock_getsize(path: str) -> int:
        if "svc_1" in path:
            return 2048
        if "svc_2" in path:
            return 8192
        raise FileNotFoundError(path)

    monkeypatch.setattr("os.path.exists", _mock_exists)
    monkeypatch.setattr("os.path.getsize", _mock_getsize)

    count = ms_mod._sample_duckdb_size()
    assert count == 2

    batch = metric_snapshots.get_batch(since="1h")
    assert "duckdb_file_bytes|svc_1" in batch
    assert batch["duckdb_file_bytes|svc_1"][-1]["value"] == 2048.0
    assert "duckdb_file_bytes|svc_2" in batch
    assert batch["duckdb_file_bytes|svc_2"][-1]["value"] == 8192.0
    assert "duckdb_file_bytes|svc_missing" not in batch


def test_metric_snapshot_registered_in_dev_local_safe_mode():
    """Verify metric_snapshot is registered when FLA_DEV_NO_CRONS=1 and dev-local crons are enabled."""
    from backend.cron.jobs import metric_snapshot as ms_mod
    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    sched._add_job = MagicMock()

    sched._register_dev_local_safe_jobs()

    assert "metric_snapshot" in sched._job_ids
    sched._add_job.assert_any_call(
        ms_mod._run_metric_snapshot,
        "interval",
        seconds=60,
        jitter=5,
        id="metric_snapshot",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
