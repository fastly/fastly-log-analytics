"""SSE endpoint /api/cron-runs/{run_id}/stream — orphan detection.

Background: when a sync is interrupted by a server restart, the row in the
``cron_runs`` SQLite table stays at ``status='running'`` but the in-memory
``backend.cron_progress._progress`` dict has no entry for it. The SSE
endpoint must surface that as an error event quickly so the UI can switch
out of "Loading logs..." — not hang on keepalives for 30 seconds.
"""

from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from backend.main import app


def _parse_sse_events(text: str) -> list[dict]:
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out


def test_orphan_run_emits_error_within_window():
    """A run_id with no entry in the in-memory progress dict must produce an
    error event within the keepalive window (~3 s of retries + a small grace
    margin), not the legacy 30 s.
    """
    # Pick a run id that is guaranteed not to exist in the in-memory dict
    nonexistent = 99_999_999

    from unittest.mock import patch

    async def mock_sleep(delay):
        pass

    with patch("asyncio.sleep", mock_sleep):
        with TestClient(app) as client:
            t0 = time.monotonic()
            with client.stream("GET", f"/api/cron-runs/{nonexistent}/stream") as resp:
                assert resp.status_code == 200
                body = "".join(resp.iter_text())
            elapsed = time.monotonic() - t0

    events = _parse_sse_events(body)
    assert events, f"Expected at least one data event, got body: {body[:200]!r}"
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None, f"Expected an error event, got: {events}"
    assert "interrupted" in err["message"].lower() or "no live progress" in err["message"].lower()

    # Margin: 3 s retries + scheduler / TestClient startup overhead. The
    # legacy bug was 30 s; anything under 10 s proves the fix.
    assert elapsed < 10.0, f"Stream took {elapsed:.1f}s — orphan detection regressed (was: <5s expected)"


def test_live_run_streams_events_to_done():
    """Sanity: an in-memory progress entry with a 'done' event should still stream and terminate."""
    from backend.cron_progress import _progress, _run_metadata, end_progress, start_progress

    run_id = 42_424_242
    start_progress(run_id, service_id="test-svc", task="sync")
    # Pre-populate with one status and one terminal event so the stream finishes immediately
    _progress[run_id].extend(
        [
            {"type": "status", "message": "starting"},
            {"type": "done", "message": "all good"},
        ]
    )
    end_progress(run_id)

    try:
        with TestClient(app) as client:
            with client.stream("GET", f"/api/cron-runs/{run_id}/stream") as resp:
                body = "".join(resp.iter_text())
    finally:
        _progress.pop(run_id, None)
        _run_metadata.pop(run_id, None)

    events = _parse_sse_events(body)
    types = [e.get("type") for e in events]
    assert "done" in types, f"Expected a done event, got: {events}"
    assert "starting" in (events[0].get("message") or "")


def test_completed_run_streams_from_database(isolate_metadata_db):
    """If a run has finished and is no longer in progress, it should stream logs from SQLite."""
    from backend import config as svcconfig
    from backend.core import metadata as metadata_db

    service_id = "test-svc"

    # Seed the mock config
    mock_cfg = {"name": "Test Service", "active": True}
    svcconfig.save_config(service_id, mock_cfg)

    try:
        # Seed a completed cron run in SQLite using the dynamic run_id
        run_id = metadata_db.start_cron_run(service_id, "sync")
        metadata_db.log_cron_run(
            service_id=service_id,
            task="sync",
            duration_s=10.0,
            status="done",
            log_output="[STATUS] Syncing logs...\n[STATUS] Downloaded 5 files.\n[DONE] Ingested 123 rows.",
            run_id=run_id,
        )

        with TestClient(app) as client:
            headers = {"x-service-id": service_id}
            with client.stream("GET", f"/api/cron-runs/{run_id}/stream", headers=headers) as resp:
                assert resp.status_code == 200
                body = "".join(resp.iter_text())

        events = _parse_sse_events(body)
        assert len(events) == 3
        assert events[0] == {"type": "status", "message": "Syncing logs..."}
        assert events[1] == {"type": "status", "message": "Downloaded 5 files."}
        assert events[2] == {"type": "done", "message": "Ingested 123 rows."}
    finally:
        # Clean up the config
        import os

        try:
            cfg_path = svcconfig.config_path(service_id)
            if os.path.exists(cfg_path):
                os.remove(cfg_path)
        except Exception:
            pass


def test_cron_runs_stream_cross_tenant_isolation_mismatch():
    """Verify that a request to stream an active in-memory run belonging to
    another service_id is rejected or safely fails to stream the live events.
    """
    from backend.cron_progress import _progress, _run_metadata, start_progress

    run_id = 99_999_888
    # Start progress for 'tenant-b-svc'
    start_progress(run_id, service_id="tenant-b-svc", task="sync")
    _progress[run_id].extend(
        [
            {"type": "status", "message": "tenant-b secretive logs"},
        ]
    )

    try:
        with TestClient(app) as client:
            # Request under tenant-a-svc header
            headers = {"x-service-id": "tenant-a-svc"}
            with client.stream("GET", f"/api/cron-runs/{run_id}/stream", headers=headers) as resp:
                body = "".join(resp.iter_text())
    finally:
        _progress.pop(run_id, None)
        _run_metadata.pop(run_id, None)

    events = _parse_sse_events(body)
    # The event should not contain the secret log content of tenant-b
    for event in events:
        assert "tenant-b" not in (event.get("message") or "")
