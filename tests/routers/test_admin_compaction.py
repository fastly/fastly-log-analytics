"""Tests for the compaction + metadata-retention/storage/cleanup
endpoints in ``backend.routers.admin.compaction``. These all had zero
direct coverage pre-v2.0.

Strategy: ``client`` fixture wires up the FastAPI TestClient with a
mocked source; each endpoint's downstream collaborator (iceberg /
local_compaction / metadata_db / config) is patched to a stub that
returns the shape the endpoint expects to forward.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

# ── POST /api/admin/optimize-now ────────────────────────────────────────────


def test_optimize_now_forwards_to_iceberg_optimize_table(client):
    """POST /admin/optimize-now triggers Iceberg compaction; the
    optimize_table result dict flows back as JSON (middleware may add
    _debug_* fields — we assert the payload is a superset)."""
    fake_result = {"files_rewritten": 4, "files_added": 1, "bytes_rewritten": 12345}
    with patch("backend.core.iceberg.optimize_table", return_value=fake_result) as m:
        resp = client.post("/api/admin/optimize-now")

    assert resp.status_code == 200
    body = resp.json()
    assert body["files_rewritten"] == 4
    assert body["files_added"] == 1
    assert body["bytes_rewritten"] == 12345
    # Caller-passed min_files_per_partition defaults to None.
    _, kwargs = m.call_args
    assert kwargs.get("min_files_per_partition") is None


def test_optimize_now_passes_min_files_when_specified(client):
    with patch("backend.core.iceberg.optimize_table", return_value={"files_rewritten": 0}) as m:
        resp = client.post("/api/admin/optimize-now?min_files=1")

    assert resp.status_code == 200
    _, kwargs = m.call_args
    assert kwargs.get("min_files_per_partition") == 1


# ── POST /api/admin/local-compact-now ───────────────────────────────────────


def test_local_compact_now_forwards_default_min_files_three(client):
    """Default min_files=3 (normal cron behaviour) is forwarded
    explicitly so the test pins the surface."""
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 2}) as m:
        resp = client.post("/api/admin/local-compact-now")

    assert resp.status_code == 200
    assert resp.json()["rewritten"] == 2
    _, kwargs = m.call_args
    assert kwargs["min_files_per_partition"] == 3
    assert kwargs["dry_run"] is False


def test_local_compact_now_min_files_zero_is_allowed(client):
    """ge=0 — min_files=0 forces every partition through dedup
    (one-shot historical cleanup). 422 would break the FE button."""
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 0}) as m:
        resp = client.post("/api/admin/local-compact-now?min_files=0&dry_run=true")

    assert resp.status_code == 200
    _, kwargs = m.call_args
    assert kwargs["min_files_per_partition"] == 0
    assert kwargs["dry_run"] is True


def test_local_compact_now_rejects_negative_min_files(client):
    """ge=0 must reject negative values with 422 (otherwise the SQL
    deeper down would mishandle it)."""
    resp = client.post("/api/admin/local-compact-now?min_files=-1")
    assert resp.status_code == 422


# ── GET /api/admin/compaction-stats ─────────────────────────────────────────


def test_compaction_stats_returns_local_compaction_snapshot(client):
    # Field is "partitions" (count of leaf partitions); both the producer
    # backend.core.local_compaction.compaction_stats and the typed
    # CompactionStatsResponse model agree on this name. The pre-typed
    # version of the test used "partitions_total" which Pydantic silently
    # dropped (unknown key) so the assertion stayed broken.
    fake = {"partitions": 100, "partitions_above_3": 5, "avg_files_per_partition": 1.8}
    with patch("backend.core.local_compaction.compaction_stats", return_value=fake):
        resp = client.get("/api/admin/compaction-stats")

    assert resp.status_code == 200
    body = resp.json()
    assert body["partitions"] == 100
    assert body["partitions_above_3"] == 5
    assert body["avg_files_per_partition"] == 1.8


# ── PATCH /api/admin/metadata-retention ────────────────────────────────────


def test_metadata_retention_patch_writes_config_and_audit(client):
    """Body values flow into svcconfig.save_config; an audit row is
    written; the resolved retention (defaults merged with cfg) comes
    back so the UI can confirm what was saved."""
    saved: dict = {}
    audit: list[dict] = []

    def _save(sid, cfg):
        saved["sid"] = sid
        saved["cfg"] = cfg

    def _record(**kwargs):
        audit.append(kwargs)

    with (
        patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.config.save_config", side_effect=_save),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=_record),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention",
            json={"usage_log_days": 14, "ingested_files_days": 30, "cron_runs_days": 7},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["retention"]["usage_log_days"] == 14
    assert body["retention"]["ingested_files_days"] == 30
    assert body["retention"]["cron_runs_days"] == 7

    assert saved["cfg"]["metadata_retention"] == {
        "usage_log_days": 14,
        "ingested_files_days": 30,
        "cron_runs_days": 7,
    }
    assert len(audit) == 1
    assert audit[0]["event_type"] == "metadata_retention_update"


def test_metadata_retention_patch_404_when_service_missing(client):
    with patch("backend.config.load_config", return_value=None):
        resp = client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "Service not found"


def test_metadata_retention_clamps_negative_to_zero(client):
    """Negative / non-numeric retention values are clamped to 0
    (disables cleanup for that table). Pinned so a -1 in the JSON body
    doesn't end up as a negative day-count in cleanup_metadata."""
    saved: dict = {}

    with (
        patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved.update({"cfg": cfg})),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention",
            json={"usage_log_days": -5, "cron_runs_days": "not-a-number"},
        )

    assert resp.status_code == 200
    assert saved["cfg"]["metadata_retention"]["usage_log_days"] == 0
    assert saved["cfg"]["metadata_retention"]["cron_runs_days"] == 0


def test_metadata_retention_force_disables_ingested_files_days_when_dedup_inactive(client):
    """If the service's dedup gate (delete_after=False) means the
    ingested_files table is load-bearing, the writer-side override
    forces ingested_files_days back to 0 — preventing operator
    misconfiguration."""
    saved: dict = {}
    with (
        patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved.update({"cfg": cfg})),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"ingested_files_days": 30})

    assert resp.status_code == 200
    assert saved["cfg"]["metadata_retention"]["ingested_files_days"] == 0


def test_metadata_retention_audit_failure_does_not_break_patch(client):
    """audit failure is swallowed — the save already happened, returning
    500 here would mislead the FE into thinking the write failed."""
    with (
        patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.config.save_config"),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=RuntimeError("audit-boom")),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7})

    assert resp.status_code == 200


# ── GET /api/admin/metadata-storage ────────────────────────────────────────


def test_metadata_storage_returns_stats_plus_retention_and_lock(client):
    fake_stats = {"by_table": {"usage_log": {"rows": 1000, "bytes": 50000}}}
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value=fake_stats),
        patch(
            "backend.config.load_config",
            return_value={
                "name": "svc",
                "service_id": "svc",
                "metadata_retention": {"usage_log_days": 14},
            },
        ),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
    ):
        resp = client.get("/api/admin/metadata-storage")

    assert resp.status_code == 200
    body = resp.json()
    assert body["by_table"]["usage_log"]["rows"] == 1000
    assert body["retention"]["usage_log_days"] == 14
    assert body["ingested_files_locked"] is False  # dedup ACTIVE → not locked


def test_metadata_storage_locks_when_dedup_inactive(client):
    """When delete_after=False (dedup INactive), the FE renders a
    tooltip explaining why the input is disabled — it relies on the
    ``ingested_files_locked: true`` field flipping correctly."""
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value={}),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
    ):
        resp = client.get("/api/admin/metadata-storage")

    assert resp.status_code == 200
    assert resp.json()["ingested_files_locked"] is True


# ── POST /api/admin/metadata-cleanup (SSE) ────────────────────────────────


def _collect_sse_events(body: bytes) -> list[dict]:
    """Parse the SSE stream into a list of JSON event payloads."""
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if not line.startswith("data: "):
            continue
        try:
            events.append(json.loads(line[len("data: ") :]))
        except json.JSONDecodeError:
            continue
    return events


def test_metadata_cleanup_streams_done_event_on_success(client):
    """Happy path: cleanup_metadata returns its result dict; the SSE
    stream ends with a {"type": "done", ...} event."""
    fake_result = {"deleted": {"usage_log": 100, "cron_runs": 10}, "vacuumed": True}

    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {"usage_log_days": 7}}),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.core.metadata.cleanup_metadata", return_value=fake_result),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = _collect_sse_events(resp.content)
    assert events, "expected at least one SSE event"
    # Last event is "done" with the cleanup_metadata result attached.
    done = events[-1]
    assert done["type"] == "done"
    assert done["result"]["vacuumed"] is True
    assert done["result"]["deleted"]["usage_log"] == 100
    assert "Trimmed 110 rows" in done["message"]


def test_metadata_cleanup_streams_done_with_zero_rows_summary(client):
    """When no rows match retention, the summary text says so —
    the FE shows that string directly to the operator."""
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.core.metadata.cleanup_metadata",
            return_value={"deleted": {}, "vacuumed": False},
        ),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    done = events[-1]
    assert done["type"] == "done"
    assert done["message"] == "No rows older than retention windows."


def test_metadata_cleanup_handles_start_cron_run_failure(client):
    """Finding 014: ``start_cron_run`` was called OUTSIDE the worker's
    try/except, so a metadata-DB write failure during cron-id allocation
    would kill the worker thread without pushing the sentinel ``None`` —
    leaving the SSE generator blocked forever on ``events.get()`` and
    leaking a thread from the FastAPI worker pool. The fix initialises
    ``run_id = None`` and moves ``start_cron_run`` inside the try so the
    finally block closes the queue and ``log_cron_run`` is invoked even
    when ``start_cron_run`` itself blows up."""
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("simulated SQLite locked")),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.core.metadata.cleanup_metadata") as mock_cleanup,
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    # Must end with an error event, not hang waiting for done.
    assert any(e["type"] == "error" for e in events), (
        f"start_cron_run failure must surface an error SSE event; events={events}"
    )
    assert not any(e.get("type") == "done" for e in events)
    err = next(e for e in events if e["type"] == "error")
    assert "simulated SQLite locked" in err["message"]

    # cleanup_metadata must NEVER have been invoked — start_cron_run failed
    # before the cleanup body runs.
    mock_cleanup.assert_not_called()

    # log_cron_run must still fire (closing the run as error) with run_id=None
    # since start_cron_run never returned an id.
    assert mock_log.called, "log_cron_run must be invoked even when start_cron_run fails"
    _args, kwargs = mock_log.call_args
    assert kwargs.get("run_id") is None


def test_metadata_cleanup_streams_error_event_when_cleanup_raises(client):
    """cleanup_metadata blowing up → SSE emits an error event AND the
    cron_runs row is closed with status=error so the schedule grid
    shows the failure."""
    log_calls: list = []

    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.core.metadata.cleanup_metadata", side_effect=RuntimeError("simulated DB lock")),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    # One error event, no done event.
    assert any(e["type"] == "error" for e in events)
    assert not any(e.get("type") == "done" for e in events)
    err = next(e for e in events if e["type"] == "error")
    assert "simulated DB lock" in err["message"]

    # cron_runs got an "error" terminal row keyed to the start_cron_run id.
    assert log_calls, "log_cron_run must be invoked even on error"
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 99
    assert args[3] == "error"  # status positional


def test_metadata_cleanup_sets_no_buffering_headers(client):
    """Reverse-proxy / browser buffering would defeat streaming UX —
    pin the headers the SSE sender depends on."""
    with (
        patch("backend.config.load_config", return_value={}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.core.metadata.cleanup_metadata",
            return_value={"deleted": {}, "vacuumed": False},
        ),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    # sse-starlette sets `no-store`; both prevent intermediate caching.
    cache_control = resp.headers.get("cache-control", "")
    assert "no-store" in cache_control or "no-cache" in cache_control
    assert resp.headers.get("x-accel-buffering") == "no"
    assert resp.headers.get("connection") == "keep-alive"


def test_backfill_bundle_rollups_success(client):
    """POST /admin/backfill-bundle-rollups invokes all rollup backfillers and closed-day compactors."""
    mocks = [
        "backfill_slow_urls_bundles",
        "backfill_origin_summary_bundles",
        "backfill_origin_dims_bundles",
        "backfill_origin_latency_ts_bundles",
        "backfill_network_rtt_bundles",
        "backfill_network_speed_bundles",
        "backfill_verified_bots_ts_bundles",
        "backfill_perf_latency_bundles",
        "backfill_security_dims_bundles",
        "backfill_perf_dims_bundles",
        "backfill_wellknown_bots_rollup",
        "backfill_ngwaf_bots_bundles",
        "backfill_overview_bundles",
        "backfill_network_summary_bundles",
        "compact_origin_summary_closed_days_to_daily",
        "compact_origin_dims_closed_days_to_daily",
        "compact_origin_latency_ts_closed_days_to_daily",
        "compact_network_rtt_closed_days_to_daily",
        "compact_network_speed_closed_days_to_daily",
        "compact_verified_bots_ts_closed_days_to_daily",
        "compact_perf_latency_closed_days_to_daily",
        "compact_security_dims_closed_days_to_daily",
        "compact_perf_dims_closed_days_to_daily",
        "compact_ngwaf_bots_closed_days_to_daily",
        "compact_overview_closed_days_to_daily",
    ]

    with patch.multiple("backend.core.rollups", **{name: MagicMock(return_value=1) for name in mocks}):
        resp = client.post("/api/admin/backfill-bundle-rollups")
        assert resp.status_code == 200
        body = resp.json()
        assert body["slow_urls"] == 1
        assert body["origin_summary"] == 1
        assert body["origin_summary_days"] == 1
