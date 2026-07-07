"""SRE / observability remediation — invariant tests.

Pins the behaviour added by the SRE pre-release pass (see
``pending-docs/pre-release-findings.md`` § SRE / Observability Engineer):
the correlation chain (SRE-01/02), cron/global-job attribution (SRE-09),
deep-health coverage of the ingestion-frozen modes (SRE-04/05), the
health-snapshot additions (SRE-03/06/11/13/20), the DuckDB-pool reject +
warm counters (SRE-12/16), and the stale ring-buffer constant (SRE-18).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# ── SRE-01 / SRE-02: the access log carries a correlation id + latency ──────


def test_admin_access_log_carries_rid_and_duration(client):
    """The [admin] access line must include rid=<id> and a (Nms) latency so an
    operator can find the slow request in `docker logs` AND join it to the
    slow_queries rows it spawned. Attaches a handler directly to the named
    logger so the assertion is robust to structlog's stdlib bridge."""
    captured: list[str] = []
    h = logging.Handler()
    h.emit = lambda record: captured.append(record.getMessage())  # type: ignore[method-assign]
    lg = logging.getLogger("backend.access.admin")
    lg.addHandler(h)
    old = lg.level
    lg.setLevel(logging.INFO)
    try:
        r = client.get("/api/health")
    finally:
        lg.removeHandler(h)
        lg.setLevel(old)

    assert r.status_code == 200
    assert any("rid=" in m and "ms)" in m for m in captured), captured
    # The minted id must be non-empty (not the "-" fallback) — SRE-01.
    assert not any("rid=- " in m for m in captured), captured


def test_new_request_id_is_nonempty_hex():
    from backend.utils.remote_access import _new_request_id

    rid = _new_request_id()
    assert isinstance(rid, str) and len(rid) == 16
    int(rid, 16)  # parses as hex
    assert _new_request_id() != rid  # not a constant


# ── SRE-09: @global_job bodies run inside the cron attribution scope ─────────


def test_global_job_runs_in_cron_attribution_scope(monkeypatch):
    import backend.utils.system_jobs as sj
    from backend.core.query_attribution import current_attribution
    from backend.cron.decorators import global_job

    monkeypatch.setattr(sj, "record_job_run", lambda *a, **k: None)
    seen: dict = {}

    @global_job("share_audit_purge", color="35", tag="purge", label="Purge")
    def _job():
        attr = current_attribution.get()
        seen["kind"] = attr.kind if attr else None
        seen["job"] = attr.cron_job if attr else None
        return "ok"

    _job()
    assert seen["kind"] == "cron"
    assert seen["job"] == "share_audit_purge"
    # Scope is popped on exit — no leak into the next APScheduler job.
    assert current_attribution.get() is None


# ── SRE-12 / SRE-16: pool reject + warm counters in stats() ──────────────────


def test_pool_stats_exposes_reject_and_warm_fields():
    from backend.core.duckdb_pool import _Pool

    p = _Pool("svc-sre12", max_size=2)
    s = p.stats()
    assert s["saturated_rejects_total"] == 0
    assert s["drain_rejects_total"] == 0
    assert "last_warmed_at" in s and s["last_warmed_at"] is None


def test_pool_saturation_increments_reject_counter():
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    p = _Pool("svc-sat", max_size=1)
    # Pretend the one slot is checked out and nothing idle → next acquire with
    # a zero deadline hits the saturation branch immediately (no DuckDB build).
    p._in_use = 1
    with pytest.raises(_PoolBusy):
        p.acquire({"name": "svc-sat"}, max_wait=0)
    assert p.stats()["saturated_rejects_total"] == 1
    assert p.stats()["drain_rejects_total"] == 0


def test_pool_drain_increments_drain_reject_counter():
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    p = _Pool("svc-drain", max_size=2)
    p.begin_drain()
    with pytest.raises(_PoolBusy):
        p.acquire({"name": "svc-drain"}, max_wait=0)
    assert p.stats()["drain_rejects_total"] == 1
    assert p.stats()["saturated_rejects_total"] == 0


# ── SRE-18: the history cap is 400 (the value the UI copy now states) ─────────


def test_history_cap_is_400():
    from backend.core.query_registry import _HISTORY_CAP, QueryRegistry

    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05: deep /api/health sees the ingestion-frozen modes ────────


def _deep_health(cron_rows: list[tuple], *, ingest_minutes_ago: int = 1):
    """Drive health_check(deep=True) against an in-memory metadata.db seeded
    with one recent ingest + the given cron_runs rows. Returns (status_code,
    parsed_body).

    Each cron_rows tuple is (task, status, mins_ago, err) or, for SRE-22
    adaptive-staleness tests, (task, status, mins_ago, err, ingest_count) —
    the 5th element is optional (defaults to 0, i.e. an "empty tick" that
    adaptive_stale_minutes' gap calculation ignores)."""
    from backend.main import health_check

    now = datetime.now(UTC)
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, "
        "status TEXT, started_at TEXT, error_message TEXT, "
        "files_downloaded INTEGER DEFAULT 0, rows_ingested INTEGER DEFAULT 0)"
    )
    con.execute(
        "INSERT INTO ingested_files VALUES (?, ?)",
        ("svc", (now - timedelta(minutes=ingest_minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")),
    )
    for row in cron_rows:
        task, status, mins_ago, err = row[:4]
        ingest_count = row[4] if len(row) > 4 else 0
        con.execute(
            "INSERT INTO cron_runs (task, status, started_at, error_message, rows_ingested) VALUES (?, ?, ?, ?, ?)",
            (task, status, (now - timedelta(minutes=mins_ago)).strftime("%Y-%m-%d %H:%M:%S"), err, ingest_count),
        )
    con.commit()

    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=False),
    ):
        result = health_check(fake_request, deep=True)

    if isinstance(result, dict):
        return 200, result
    return result.status_code, json.loads(result.body)


def test_deep_health_healthy_when_sync_recent_and_terminal():
    # Recent ingest + sync success + commit success + no running row → ok.
    code, body = _deep_health([("sync", "success", 2, None), ("commit", "success", 3, None)])
    assert code == 200
    assert body["status"] == "ok"


def test_deep_health_degrades_on_stuck_running_sync():
    # SRE-04: a sync row stuck 'running' past the threshold — invisible to the
    # success-only filter — must degrade rather than report the older success.
    code, body = _deep_health([("sync", "success", 120, None), ("sync", "running", 20, None)])
    assert code == 503
    svc = body["services"][0]
    assert svc["status"] == "degraded"
    assert "stuck running" in svc["reason"]


def test_deep_health_not_fooled_by_briefly_running_sync():
    # A sync that just started (under the threshold) is healthy in-progress,
    # NOT a stall — must stay ok.
    code, body = _deep_health([("sync", "success", 30, None), ("sync", "running", 2, None)])
    assert code == 200
    assert body["status"] == "ok"


def test_deep_health_degrades_on_commit_error():
    # SRE-05: a commit cron erroring every run (buffer growing, nothing
    # reaching Iceberg) is task='sync'-invisible — must degrade.
    code, body = _deep_health([("sync", "success", 2, None), ("commit", "error", 1, "boom")])
    assert code == 503
    svc = body["services"][0]
    assert svc["status"] == "degraded"
    assert "commit cron errored" in svc["reason"]


def test_deep_health_degrades_on_metadata_sync_error():
    # SRE-05: a read_only analyst service's only ingest path is metadata_sync.
    code, body = _deep_health([("metadata_sync", "error", 1, "fos down")])
    assert code == 503
    assert body["services"][0]["status"] == "degraded"
    assert "metadata_sync cron errored" in body["services"][0]["reason"]


# ── SRE-22: adaptive per-service staleness widening ──────────────────────────


def _steady_cadence_rows(n: int, gap_minutes: int, *, start_after_minutes: int) -> list[tuple]:
    """n non-empty successful sync runs, gap_minutes apart, the most recent
    ending start_after_minutes ago — the historical cadence the naive-stale
    ingest (at ingest_minutes_ago, seeded separately) needs to be judged
    against."""
    return [("sync", "success", start_after_minutes + i * gap_minutes, None, 5) for i in range(n)]


def test_deep_health_widens_staleness_for_historically_quiet_service():
    # This service has always had ~40min gaps between real ingests — an
    # organic quiet period this long is normal for it. The naive 30-min
    # default would flag it degraded at 35min stale; the widened threshold
    # (2 * 40 = 80min) must not.
    history = _steady_cadence_rows(12, gap_minutes=40, start_after_minutes=35)
    code, body = _deep_health(history, ingest_minutes_ago=35)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == "ok"
    assert svc["stale_minutes_used"] == 80


def test_deep_health_still_degrades_on_a_real_outage():
    # Same historical cadence as above, but now it's been quiet for 200
    # minutes — well past even the widened (80min) threshold. Must still
    # degrade; adaptive widening isn't a way to silence a genuine outage.
    history = _steady_cadence_rows(12, gap_minutes=40, start_after_minutes=200)
    code, body = _deep_health(history, ingest_minutes_ago=200)
    assert code == 503
    svc = body["services"][0]
    assert svc["status"] == "degraded"
    assert svc["stale_minutes_used"] == 80
    assert "no ingest since" in svc["reason"]


# ── SRE-03 / 06 / 11 / 13 / 20: health-snapshot additions ────────────────────


def test_health_snapshot_has_new_observability_fields(client):
    body = client.get("/api/admin/health-snapshot").json()
    for key in ("scheduler_last_tick_age_s", "recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    # SRE-20: default test env (no STRUCTLOG_FORMAT / OTEL_EXPORTER) → console/none.
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


def test_health_snapshot_scheduler_tick_age_surfaced(client):
    with patch("backend.core.metric_snapshots.last_snapshot_age_s", return_value=12.5):
        body = client.get("/api/admin/health-snapshot").json()
    assert body["scheduler_last_tick_age_s"] == 12.5


def test_health_snapshot_recent_cron_failures_aggregates_errors(client):
    latest = {
        "sync": {"status": "success", "started_at": "2026-06-20T10:00:00Z", "error_message": None},
        "commit": {"status": "error", "started_at": "2026-06-20T10:05:00Z", "error_message": "boom"},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc-a"]),
        patch("backend.core.metadata.cron_log.latest_cron_per_task", return_value=latest),
    ):
        body = client.get("/api/admin/health-snapshot").json()

    fails = body["recent_cron_failures"]
    assert len(fails) == 1
    assert fails[0]["service_id"] == "svc-a"
    assert fails[0]["task"] == "commit"
    assert fails[0]["status"] == "error"


def test_health_snapshot_config_backup_reads_marker(client, tmp_path, monkeypatch):
    marker = tmp_path / "last_config_backup.json"
    at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    marker.write_text(json.dumps({"at": at, "remote": "gs://example/configs.tar.gz"}))
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(marker))

    body = client.get("/api/admin/health-snapshot").json()
    cb = body["config_backup"]
    assert cb is not None
    assert cb["last_backup_at"] == at
    assert cb["age_s"] is not None and cb["age_s"] >= 0
    assert cb["source"] == "gs://example/configs.tar.gz"


def test_health_snapshot_config_backup_absent_when_no_marker(client, tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(tmp_path / "does-not-exist.json"))
    body = client.get("/api/admin/health-snapshot").json()
    assert body["config_backup"] is None


def test_health_snapshot_fos_probe_opt_in(client):
    # Default: no FOS probe (off → real network round-trip avoided).
    body = client.get("/api/admin/health-snapshot").json()
    assert body.get("fos") is None

    # probe_fos=1 populates per-service reachability.
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-a", "name": "svc-a"}]),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value={"reachable": True, "error": None}),
    ):
        body = client.get("/api/admin/health-snapshot?probe_fos=1").json()
    assert body["fos"]["svc-a"]["reachable"] is True
