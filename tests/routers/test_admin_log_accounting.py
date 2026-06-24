"""Tests for ``GET /api/admin/log-accounting``.

The endpoint reconciles Fastly's authoritative `/stats/service/{id}` log-line
counts against locally-ingested `sum(row_count) FROM ingested_files` over the
same window, surfacing any gap as a per-bucket signal.

Why these tests matter: a silent regression in the bucket-alignment math
(timezone, SUBSTR width, or outer-join key shape) would make the panel
*always* show a gap or *always* show zero — both invisible to the user
unless they happen to spot-check Fastly's invoice. Each test pins one
slice of the alignment contract.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.core import metadata as metadata_db


@pytest.fixture(autouse=True)
def _clear_log_accounting_ttl_caches():
    """Clear the module-level Fastly + DuckDB count caches between tests.

    ``compute_log_accounting`` memoises both fetches by
    ``(service, from_ts, to_ts, by)`` to silence repeated polls in prod.
    Tests in this file share the same ``hours=4 by=hour`` window so the
    second test to run would otherwise receive the FIRST test's mocked
    payload from the TTL cache. Clear on both setUp and tearDown so the
    leak can't bleed into unrelated test modules either.
    """
    from backend.routers.admin import log_accounting as _la

    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()
    yield
    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()


@pytest.fixture
def log_accounting_source():
    return {
        "name": "test_service",
        "service_id": "test-service-id",
        "logging_service_id": "test-logging-svc-id",
    }


@pytest.fixture
def log_accounting_client(log_accounting_source, in_memory_duckdb):
    """Mirror the shared `client` fixture but with a source that has
    `logging_service_id` so the endpoint reaches the Fastly call path."""
    from fastapi.testclient import TestClient

    from backend.deps import get_con, get_source
    from backend.main import app

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: log_accounting_source
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _fake_urlopen(payload: dict):
    """Return a context-manager stand-in matching urlopen's interface."""

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(payload).encode()

    return lambda req, timeout=30: _Resp()


def _seed_ingested(svc_id: str, rows: list[tuple[str, int]]):
    """Insert (ingested_at_iso, row_count) rows into ingested_files.

    We bypass ``insert_ingested_files`` so we can pin ingested_at — the helper
    uses ``DEFAULT (datetime('now'))`` which would defeat bucket alignment tests.
    """
    con = metadata_db.get_con(svc_id)
    for i, (ts_iso, rc) in enumerate(rows):
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"raw/file-{i}.gz", svc_id, ts_iso, rc, 1000),
        )
    con.commit()


def test_log_accounting_aligns_buckets(log_accounting_client, log_accounting_source):
    """Fastly emits 1000/1000/1000 across three consecutive hourly buckets;
    we ingest 998/1000/995. The per-bucket gap math must produce exactly
    2/0/5 with matching gap_pct. If timezone or SUBSTR width drift, this
    is the test that catches it."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=3)
    b1 = now - timedelta(hours=2)
    b2 = now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 998),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 1000),
            (b2.strftime("%Y-%m-%dT%H:%M:%S"), 995),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "log": 1000},
            {"start_time": int(b1.timestamp()), "log": 1000},
            {"start_time": int(b2.timestamp()), "log": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    gaps = {b["ts"]: b["gap"] for b in body["buckets"]}
    assert gaps[f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"] == 2
    assert gaps[f"{b1.strftime('%Y-%m-%dT%H')}:00:00Z"] == 0
    assert gaps[f"{b2.strftime('%Y-%m-%dT%H')}:00:00Z"] == 5
    assert body["totals"]["fastly_logs"] == 3000
    assert body["totals"]["our_rows"] == 2993
    assert body["totals"]["gap"] == 7
    assert body["fastly_field_used"] == "log"


def test_log_accounting_handles_missing_fastly_field(log_accounting_client, log_accounting_source):
    """If Fastly's response carries none of our candidate log-count fields,
    the endpoint must not crash — it should treat each bucket as 0 and
    return ``fastly_field_used=None`` so the frontend can flag it."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=1)
    _seed_ingested(svc_id, [(b0.strftime("%Y-%m-%dT%H:%M:%S"), 500)])
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 999, "edge_requests": 888},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=2&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    assert body["fastly_field_used"] is None
    # Our 500 ingested rows still surface even when Fastly side is 0.
    by_ts = {b["ts"]: b for b in body["buckets"]}
    target_ts = f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"
    assert by_ts[target_ts]["fastly_logs"] == 0
    assert by_ts[target_ts]["our_rows"] == 500


def test_log_accounting_zero_log_field_not_flagged_missing(log_accounting_client, log_accounting_source, caplog):
    """A quiet hour legitimately reports ``log: 0``. The endpoint must treat
    that as the log-count field present with value 0 — NOT as a missing field.

    Regression: the old truthiness check (``if v:``) treated a zero count as
    "field absent", logging a bogus "no log-count field" warning whose own
    keys list visibly contained ``log`` and returning ``fastly_field_used=None``
    for any all-zero window (exactly what a brand-new service shows)."""
    import logging as _logging

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=1)
    # ``log`` present but 0, plus unrelated keys — a genuinely empty hour.
    fastly_payload = {"data": [{"start_time": int(b0.timestamp()), "log": 0, "requests": 5}]}
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
        caplog.at_level(_logging.WARNING, logger="admin.log_accounting"),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=2&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    # Field IS present (value 0) — detected, not flagged as missing.
    assert body["fastly_field_used"] == "log"
    by_ts = {b["ts"]: b for b in body["buckets"]}
    target_ts = f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"
    assert by_ts[target_ts]["fastly_logs"] == 0
    # No bogus "no log-count field" warning for a quiet hour.
    assert not any("no log-count field" in r.message for r in caplog.records)


def test_log_accounting_outer_join_handles_orphan_buckets(log_accounting_client, log_accounting_source):
    """One bucket only in Fastly (cron hasn't caught up), one only locally
    (e.g. backfill predating the Fastly query window). Outer-join must
    surface BOTH buckets with the missing side as 0; an inner join would
    quietly drop them and mask the gap."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    fastly_only = now - timedelta(hours=1)
    local_only = now - timedelta(hours=2)
    _seed_ingested(svc_id, [(local_only.strftime("%Y-%m-%dT%H:%M:%S"), 300)])
    fastly_payload = {"data": [{"start_time": int(fastly_only.timestamp()), "log": 700}]}
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    by_ts = {b["ts"]: b for b in body["buckets"]}
    fastly_ts = f"{fastly_only.strftime('%Y-%m-%dT%H')}:00:00Z"
    local_ts = f"{local_only.strftime('%Y-%m-%dT%H')}:00:00Z"
    assert by_ts[fastly_ts]["fastly_logs"] == 700
    assert by_ts[fastly_ts]["our_rows"] == 0
    assert by_ts[local_ts]["fastly_logs"] == 0
    assert by_ts[local_ts]["our_rows"] == 300


def test_log_accounting_flags_sustained_loss_over_threshold(log_accounting_client, log_accounting_source):
    """Three completed hourly buckets each show a 10% one-sided gap (Fastly
    emitted 1000, we ingested 900). The sustained-loss detector must flag
    the run: started_at=earliest, n_buckets=3, max_gap_pct=0.1, total_lost=300.
    Pinned because the alert is the loss-vs-drift distinction — a regression
    that suppressed it would silently hide real log loss."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=3)
    b1 = now - timedelta(hours=2)
    b2 = now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b2.strftime("%Y-%m-%dT%H:%M:%S"), 900),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "log": 1000},
            {"start_time": int(b1.timestamp()), "log": 1000},
            {"start_time": int(b2.timestamp()), "log": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    assert body["sustained_loss"] is not None
    sl = body["sustained_loss"]
    assert sl["started_at"] == f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"
    assert sl["n_buckets"] == 3
    assert abs(sl["max_gap_pct"] - 0.1) < 1e-6
    assert sl["total_lost_lines"] == 300


def test_log_accounting_no_alert_on_normal_drift(log_accounting_client, log_accounting_source):
    """Bucket-edge drift ±2% bidirectional must NOT trip the alert — that
    would fire constantly under healthy operation and train the operator
    to ignore it."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=3)
    b1 = now - timedelta(hours=2)
    b2 = now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 1020),  # +2%
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 980),  # -2%
            (b2.strftime("%Y-%m-%dT%H:%M:%S"), 1010),  # +1%
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "log": 1000},
            {"start_time": int(b1.timestamp()), "log": 1000},
            {"start_time": int(b2.timestamp()), "log": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    assert resp.json()["sustained_loss"] is None


def test_log_accounting_in_flight_bucket_does_not_trigger_alert(log_accounting_client, log_accounting_source):
    """The in-flight (current) bucket commonly shows a one-sided gap because
    Fastly Stats lags our ingest by a few minutes. A single in-flight bucket
    with 30% gap must not trip the alert — the detector requires ≥2
    consecutive *completed* buckets."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    # No prior completed buckets; only the in-flight bucket has data.
    fastly_payload = {
        "data": [{"start_time": int(now.timestamp()), "log": 1000}],
    }
    _seed_ingested(svc_id, [(now.strftime("%Y-%m-%dT%H:%M:%S"), 700)])  # 30% gap
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    assert resp.json()["sustained_loss"] is None


def test_log_accounting_catchup_caught_up(log_accounting_client, log_accounting_source):
    """When the most-recent ingest is within 300s of now, status must be
    ``caught_up``. This is the steady-state signal the operator looks for to
    confirm cron is healthy without scanning the bucket-by-bucket gap chart."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(microsecond=0)
    recent = now - timedelta(seconds=60)
    _seed_ingested(svc_id, [(recent.strftime("%Y-%m-%dT%H:%M:%S"), 1000)])
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen({"data": []})),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    catchup = resp.json()["catchup"]
    assert catchup["status"] == "caught_up"
    assert catchup["lag_seconds"] is not None and catchup["lag_seconds"] <= 300
    assert catchup["latest_ingest_ts"] is not None


def test_log_accounting_catchup_stalled(log_accounting_client, log_accounting_source):
    """When the latest ingest is >1h old, status must be ``stalled`` — this is
    the page-worthy threshold (cron is wedged or the source is dead)."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(microsecond=0)
    old = now - timedelta(hours=2)
    _seed_ingested(svc_id, [(old.strftime("%Y-%m-%dT%H:%M:%S"), 1000)])
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen({"data": []})),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    catchup = resp.json()["catchup"]
    assert catchup["status"] == "stalled"
    assert catchup["lag_seconds"] >= 3600


def test_log_accounting_catchup_no_data(log_accounting_client, log_accounting_source):
    """Empty ``ingested_files`` (fresh install, never-synced source) returns
    ``no_data`` rather than ``stalled`` — distinguishes "never started" from
    "started and broken"."""
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen({"data": []})),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    catchup = resp.json()["catchup"]
    assert catchup["status"] == "no_data"
    assert catchup["lag_seconds"] is None
    assert catchup["latest_ingest_ts"] is None


def test_log_accounting_handles_no_logging_service_id(in_memory_duckdb):
    """Source without ``logging_service_id`` returns 400 — the panel needs
    a clear error to surface "configure your Fastly service" UX rather
    than a silent 500."""
    from fastapi.testclient import TestClient

    from backend.deps import get_con, get_source
    from backend.main import app

    src_no_log = {"name": "test_service", "service_id": "test-service-id"}
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: src_no_log
    try:
        with patch("backend.config.get_fastly_logging_service_id", return_value=""):
            with TestClient(app) as c:
                resp = c.get("/api/admin/log-accounting?hours=4&by=hour")
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 400
    assert "logging_service_id" in resp.json()["detail"]["error"]
