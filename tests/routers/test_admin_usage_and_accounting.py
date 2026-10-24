"""Tests for ``GET /api/admin/log-accounting``.

The endpoint reconciles Fastly's authoritative `/stats/service/{id}` ``requests``
counts against locally-ingested `sum(row_count) FROM ingested_files` over the
same window, surfacing any gap as a per-bucket signal. ``requests`` (not the
``log`` counter, which counts ``vcl_log`` re-executions) is the loss denominator
because it sits 1:1 with our ingested rows on every service.

Why these tests matter: a silent regression in the bucket-alignment math
(timezone, SUBSTR width, or outer-join key shape) would make the panel
*always* show a gap or *always* show zero — both invisible to the user
unless they happen to spot-check Fastly's invoice. Each test pins one
slice of the alignment contract.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.core import metadata as metadata_db
from backend.core.metadata import get_con
from backend.deps import get_source
from backend.main import app
from backend.routers import usage
from backend.routers.admin import router as admin_router
from backend.routers.usage import _extract_fos_ops, _fastly_api
from backend.utils.remote_access import _is_blocked_path
from tests.conftest import MOCK_SERVICE_ID


# ── Merged from tests/routers/test_admin_log_accounting.py ──
@pytest.fixture
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

    ``ingested_at`` is stored space-separated in production (SQLite's
    ``datetime('now')`` default — see ``backend/core/ingest.py``), never
    with a ``T``. Normalize here so callers can keep passing readable
    ISO-``T`` strings while the stored value matches what the real writer
    produces — the accounting SQL compares ``ingested_at`` as a plain
    string against space-separated bounds and won't match a ``T``.
    """
    con = metadata_db.get_con(svc_id)
    for i, (ts_iso, rc) in enumerate(rows):
        ts = ts_iso.replace("T", " ")
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"raw/file-{i}.gz", svc_id, ts, rc, 1000),
        )
    con.commit()


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200, resp.text
    # The service is chosen via the x-service-id header on an otherwise-identical
    # URL, so the cached response MUST Vary on it — else the browser serves one
    # service's body for another within the 30s window (the bug that made the
    # gap card not change on service switch).
    assert "x-service-id" in resp.headers.get("vary", "").lower()
    body = resp.json()
    gaps = {b["ts"]: b["gap"] for b in body["buckets"]}
    assert gaps[f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"] == 2
    assert gaps[f"{b1.strftime('%Y-%m-%dT%H')}:00:00Z"] == 0
    assert gaps[f"{b2.strftime('%Y-%m-%dT%H')}:00:00Z"] == 5
    assert body["totals"]["fastly_requests"] == 3000
    assert body["totals"]["our_rows"] == 2993
    assert body["totals"]["gap"] == 7


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_zero_requests_bucket(log_accounting_client, log_accounting_source):
    """A quiet hour legitimately reports ``requests: 0``. The endpoint must
    surface it as a zero-request bucket without crashing, and our ingested rows
    for that bucket still appear (yielding a negative gap — in-flight noise, not
    a loss signal)."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=1)
    _seed_ingested(svc_id, [(b0.strftime("%Y-%m-%dT%H:%M:%S"), 500)])
    fastly_payload = {"data": [{"start_time": int(b0.timestamp()), "requests": 0}]}
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=2&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    by_ts = {b["ts"]: b for b in body["buckets"]}
    target_ts = f"{b0.strftime('%Y-%m-%dT%H')}:00:00Z"
    assert by_ts[target_ts]["fastly_requests"] == 0
    assert by_ts[target_ts]["our_rows"] == 500


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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
    fastly_payload = {"data": [{"start_time": int(fastly_only.timestamp()), "requests": 700}]}
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
    assert by_ts[fastly_ts]["fastly_requests"] == 700
    assert by_ts[fastly_ts]["our_rows"] == 0
    assert by_ts[local_ts]["fastly_requests"] == 0
    assert by_ts[local_ts]["our_rows"] == 300


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
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


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_multi_endpoint_still_flags_loss(log_accounting_client, log_accounting_source):
    """The gap is measured against Fastly ``requests``, which is endpoint-
    independent — so a real sustained loss must STILL be flagged regardless of
    how many logging endpoints the service has. Regression guard against the
    retired ``log``-based band-aid that suppressed ``sustained_loss`` on any
    multi-endpoint service (which would have hidden real loss + starved the
    gap-heal cron)."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=3)
    b1 = now - timedelta(hours=2)
    b2 = now - timedelta(hours=1)
    # 10% one-sided gap on requests across three completed buckets.
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
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Loss is flagged; no endpoint-count suppression exists anymore.
    assert body["sustained_loss"] is not None
    assert body["sustained_loss"]["n_buckets"] == 3
    assert body["totals"]["gap"] == 300
    # The retired fields are gone from the response.
    assert "gap_comparable" not in body
    assert "logging_endpoints" not in body


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    assert resp.json()["sustained_loss"] is None


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_in_flight_bucket_does_not_trigger_alert(log_accounting_client, log_accounting_source):
    """The in-flight (current) bucket commonly shows a one-sided gap because
    Fastly Stats lags our ingest by a few minutes. A single in-flight bucket
    with 30% gap must not trip the alert — the detector requires ≥2
    consecutive *completed* buckets."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    # No prior completed buckets; only the in-flight bucket has data.
    fastly_payload = {
        "data": [{"start_time": int(now.timestamp()), "requests": 1000}],
    }
    _seed_ingested(svc_id, [(now.strftime("%Y-%m-%dT%H:%M:%S"), 700)])  # 30% gap
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    assert resp.json()["sustained_loss"] is None


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
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


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_clamps_to_created_at(log_accounting_client, log_accounting_source):
    """If the service was created/provisioned less than 24h ago, the start
    time of the log-accounting window is clamped to the service's created_at time
    and we only compare from that hour forward."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    created_at = now - timedelta(hours=2)
    # Put created_at 5 minutes past the hour start
    created_at_str = (created_at + timedelta(minutes=5)).isoformat()

    b0 = now - timedelta(hours=2)
    b1 = now - timedelta(hours=1)

    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 100),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 200),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 100},
            {"start_time": int(b1.timestamp()), "requests": 200},
        ]
    }

    config_mock = {
        "service_id": svc_id,
        "name": svc_id,
        "created_at": created_at_str,
    }

    with (
        patch("backend.config.load_config", return_value=config_mock),
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=24&by=hour")

    assert resp.status_code == 200, resp.text
    body = resp.json()

    # The query window must be clamped to the created_at hour start
    assert body["from_ts"] == b0.strftime("%Y-%m-%dT%H:%M:%SZ")
    # We should have exactly 2 buckets representing b0 and b1
    assert len(body["buckets"]) == 2


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_edge_only(log_accounting_client, log_accounting_source):
    """If the service has edge-only logging enabled, log-accounting should use
    'edge_requests' as the denominator instead of 'requests' from the Fastly stats response."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=1)

    # Ingested rows is 200
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 200),
        ],
    )

    # Fastly payload contains requests=300 (which has shield hops) and edge_requests=200
    fastly_payload = {
        "data": [
            {
                "start_time": int(b0.timestamp()),
                "requests": 300,
                "edge_requests": 200,
            },
        ]
    }

    config_mock = {
        "service_id": svc_id,
        "name": svc_id,
        "provisioning": {
            "edge_only": True,
        },
    }

    with (
        patch("backend.config.load_config", return_value=config_mock),
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=1&by=hour")

    assert resp.status_code == 200, resp.text
    body = resp.json()

    # The gap should be 0% because edge_requests (200) matches our_rows (200)
    assert len(body["buckets"]) == 1
    bucket = body["buckets"][0]
    assert bucket["fastly_requests"] == 200
    assert bucket["our_rows"] == 200
    assert bucket["gap_pct"] == 0.0


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.usefixtures("_clear_log_accounting_ttl_caches")
def test_log_accounting_not_edge_only(log_accounting_client, log_accounting_source):
    """If edge-only logging is NOT enabled, log-accounting should use standard 'requests'."""
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0 = now - timedelta(hours=1)

    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 200),
        ],
    )

    fastly_payload = {
        "data": [
            {
                "start_time": int(b0.timestamp()),
                "requests": 300,
                "edge_requests": 200,
            },
        ]
    }

    config_mock = {
        "service_id": svc_id,
        "name": svc_id,
        "provisioning": {
            "edge_only": False,
        },
    }

    with (
        patch("backend.config.load_config", return_value=config_mock),
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=1&by=hour")

    assert resp.status_code == 200, resp.text
    body = resp.json()

    # The gap should be non-zero because we used requests=300 as denominator vs our_rows=200
    assert len(body["buckets"]) == 1
    bucket = body["buckets"][0]
    assert bucket["fastly_requests"] == 300
    assert bucket["our_rows"] == 200
    assert abs(bucket["gap_pct"] - 0.3333) < 0.01


# ── Merged from tests/routers/test_admin_usage_log.py ──
_DEFAULT_UL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.01,
    "cdn_egress_rate_per_gb": 0.12,
    "storage_rate_per_gb_month": 0.02,
    "min_billed_days": 30,
    "billing_zone": "MBZ200",
}

_TEST_SOURCE = {"name": "svc-usage-test", "service_id": "svc-usage-test"}


@patch("backend.config.load_usage_logging_config")
def test_get_usage_logging_settings(mock_load):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.get("/api/admin/usage-logging")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 30
    assert body["class_a_rate_per_1k"] == pytest.approx(0.005)
    assert body["billing_zone"] == "MBZ200"


@patch("backend.config.load_usage_logging_config")
@patch("backend.config.save_usage_logging_config")
def test_patch_usage_logging_partial_update(mock_save, mock_load):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.patch(
            "/api/admin/usage-logging",
            json={"enabled": False, "retention_days": 60, "class_b_rate_per_10k": 0.025},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is False
    assert body["retention_days"] == 60
    assert body["class_b_rate_per_10k"] == pytest.approx(0.025)
    assert body["class_a_rate_per_1k"] == pytest.approx(0.005)
    mock_save.assert_called_once()


@pytest.mark.parametrize(
    "field",
    [
        "retention_days",
        "class_a_rate_per_1k",
        "class_b_rate_per_10k",
        "cdn_egress_rate_per_gb",
        "storage_rate_per_gb_month",
        "min_billed_days",
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1, -0.05])
@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_patch_usage_logging_rejects_non_positive_numbers(mock_load, field: str, bad_value: float):
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={field: bad_value})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["field"] == field
    assert "positive number" in detail["error"]


@pytest.mark.parametrize(
    ("zone", "expected_rate"),
    [
        ("MBZ100", 0.08),
        ("MBZ200", 0.12),
        ("Global", 0.18),
    ],
)
@patch("backend.config.load_usage_logging_config")
@patch("backend.config.save_usage_logging_config")
def test_patch_usage_logging_billing_zone_auto_sets_cdn_rate(mock_save, mock_load, zone: str, expected_rate: float):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={"billing_zone": zone})
    assert r.status_code == 200
    body = r.json()
    assert body["billing_zone"] == zone
    assert body["cdn_egress_rate_per_gb"] == pytest.approx(expected_rate)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_patch_usage_logging_rejects_invalid_billing_zone(mock_load):
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["field"] == "billing_zone"
    assert "Invalid billing_zone" in detail["error"]


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_usage_log_postgres_roundtrip_and_filters(mock_ul_cfg):
    svc = "svc-pg-usage-log"
    metadata_db.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "PutObject",
                        "path": "s3://bucket/a.parquet",
                        "bytes": 2048,
                        "time_ms": 12.5,
                        "caller": "commit_files",
                        "status": "200",
                    }
                ],
                process_context="cron:commit:svc",
            )
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T11:30:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "GetObject",
                        "path": "s3://bucket/raw.gz",
                        "bytes": 4096,
                        "time_ms": 8.0,
                        "caller": "download_batch",
                        "status": "200",
                    }
                ],
                process_context="cron:sync:svc",
            )
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T12:05:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "CDN",
                        "method": "edge_egress",
                        "path": "https://cdn.example.com/item",
                        "bytes": 1073741824,
                        "time_ms": 3.2,
                        "caller": "fastly_stats",
                        "status": "200",
                    }
                ],
                process_context="fastly:stats",
            )

        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as client:
                # 1. Unfiltered window (uses usage_log_hourly_summary for interior hours + boundary scan)
                r = client.get(
                    "/api/admin/usage-log",
                    params={"start": "2026-06-01T09:00:00Z", "end": "2026-06-01T14:00:00Z"},
                )
                assert r.status_code == 200
                body = r.json()
                assert body["service_id"] == svc
                assert body["total"] == 3
                agg = body["aggregate"]
                assert agg["total_class_a"] == 1
                assert agg["total_class_b"] == 1
                assert agg["total_cdn_downloads"] == 1
                assert agg["total_cdn_bytes"] == 1073741824
                assert agg["total_fos_bytes"] == 2048 + 4096
                assert agg["class_a_breakdown"] == {"PutObject": 1}
                assert agg["class_b_breakdown"] == {"GetObject": 1}
                assert agg["estimated_cost_cdn"] == pytest.approx(0.12)

                # 2. Filter by usage_type="FOS-B"
                r_b = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "usage_type": "FOS-B",
                    },
                )
                assert r_b.status_code == 200
                body_b = r_b.json()
                assert body_b["total"] == 1
                assert body_b["entries"][0]["operation_class"] == "B"
                assert body_b["entries"][0]["count"] == 1
                # Per-row cost at $0.01/10k = 0.000001
                assert body_b["entries"][0]["estimated_cost"] == pytest.approx(0.000001)

                # 3. Filter by process_context="cron:commit"
                r_proc = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "process_context": "cron:commit",
                    },
                )
                assert r_proc.status_code == 200
                body_proc = r_proc.json()
                assert body_proc["total"] == 1
                assert body_proc["entries"][0]["operation_type"] == "PutObject"

                # 4. Filter by operation_type="edge_egress"
                r_op = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "operation_type": "edge_egress",
                    },
                )
                assert r_op.status_code == 200
                body_op = r_op.json()
                assert body_op["total"] == 1
                assert body_op["entries"][0]["operation_class"] == "CDN"
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


def test_usage_log_export_csv_and_formula_escaping():
    svc = "svc-pg-usage-export"
    metadata_db.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "PutObject",
                        "path": "=cmd|' /C calc'!A0",
                        "bytes": 512,
                        "time_ms": 5.0,
                        "caller": "+sum(1,2)",
                        "status": "-200",
                    }
                ],
                process_context="@at_context",
            )
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as client:
                r = client.get(
                    "/api/admin/usage-log/export",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
            assert r.status_code == 200
            assert "text/csv" in r.headers["content-type"]
            assert "filename=usage_log.csv" in r.headers["content-disposition"]

            rows = list(csv.reader(io.StringIO(r.text)))
            assert len(rows) == 2
            assert rows[0] == [
                "timestamp",
                "service_id",
                "operation_class",
                "operation_type",
                "url",
                "bytes",
                "duration_ms",
                "function_name",
                "process_context",
                "status",
                "count",
            ]
            data_row = rows[1]
            assert data_row[1] == svc
            assert data_row[2] == "A"
            # Formula-injection prefixes (=, +, @, -) must be escaped with leading "'"
            assert data_row[4].startswith("'=")
            assert data_row[7].startswith("'+")
            assert data_row[8].startswith("'@")
            assert data_row[9].startswith("'-")
            assert data_row[10] == "1"
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_delete_usage_log_clears_service_entries(mock_ul_cfg):
    svc = "svc-pg-usage-delete"
    metadata_db.clear_usage_log(svc)
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
        metadata_db.log_usage_calls(
            svc,
            [
                {
                    "service": "FOS",
                    "method": "PutObject",
                    "path": "s3://bucket/a.parquet",
                    "bytes": 100,
                    "time_ms": 1.0,
                    "caller": "fn",
                    "status": "200",
                }
            ],
            process_context="ctx",
        )
    app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
    try:
        with TestClient(app) as client:
            del_res = client.delete("/api/admin/usage-log")
            assert del_res.status_code == 200
            assert del_res.json()["ok"] is True

            get_res = client.get(
                "/api/admin/usage-log",
                params={"start": "2026-06-01T00:00:00Z", "end": "2026-06-02T00:00:00Z"},
            )
            assert get_res.status_code == 200
            assert get_res.json()["total"] == 0
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.utils.system_jobs.get_system_job_status")
@patch("backend.cron.scheduler.get_scheduler")
def test_get_system_jobs_endpoint(mock_sched, mock_status):
    mock_status.return_value = {
        "bot_data_refresh": {
            "last_run_at": "2026-06-01T10:00:00Z",
            "status": "ok",
            "duration_s": 1.25,
            "detail": "3 sources updated",
        }
    }
    fake_job = SimpleNamespace(next_run_time=datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC))
    sched_obj = MagicMock()
    sched_obj.get_job.side_effect = lambda jid: fake_job if jid == "bot_data_refresh" else None
    mock_sched.return_value = sched_obj

    with TestClient(app) as client:
        r = client.get("/api/admin/system-jobs")

    assert r.status_code == 200
    jobs = {j["id"]: j for j in r.json()["jobs"]}
    assert set(jobs.keys()) == {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}
    assert jobs["bot_data_refresh"]["status"] == "ok"
    assert jobs["bot_data_refresh"]["next_run_at"] == "2026-06-02T03:00:00Z"
    assert jobs["rdns_enrichment"]["next_run_at"] is None


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/admin/usage-logging"),
        ("PATCH", "/api/admin/usage-logging"),
        ("GET", "/api/admin/usage-log"),
        ("GET", "/api/admin/usage-log/export"),
        ("DELETE", "/api/admin/usage-log"),
        ("GET", "/api/admin/system-jobs"),
    ],
)
def test_usage_log_endpoints_reject_analyst_path_b(method: str, path: str):
    # 1. Middleware path gate blocks all /api/admin/* paths
    assert _is_blocked_path(path) is True

    # 2. Route-level Depends(require_admin) also rejects Analyst Path B with 403 admin_only
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def _stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-usage",
            service_ids=["svc-usage-test"],
            email="analyst@example.com",
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)
    analyst_app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

    with TestClient(analyst_app) as client:
        if method == "PATCH":
            r = client.patch(path, json={"enabled": True})
        elif method == "DELETE":
            r = client.delete(path)
        else:
            r = client.get(path)

    assert r.status_code == 403
    detail = r.json().get("detail", r.json())
    assert detail["error"] == "admin_only"


# ── Merged from tests/routers/test_usage_log.py ──
_DEFAULT_UL_CFG_usage_log = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.01,
    "cdn_egress_rate_per_gb": 0.12,
}

_TEST_SOURCE_usage_log = {"name": "test_service", "service_id": "svc1"}


class TestUsageLogRouter:
    @patch("backend.config.load_usage_logging_config")
    def test_get_usage_logging_settings(self, mock_load):
        mock_load.return_value = _DEFAULT_UL_CFG_usage_log
        with TestClient(app) as c:
            r = c.get("/api/admin/usage-logging")
        assert r.status_code == 200
        assert r.json()["enabled"] is True

    @patch("backend.config.load_usage_logging_config")
    @patch("backend.config.save_usage_logging_config")
    def test_update_usage_logging_settings(self, mock_save, mock_load):
        mock_load.return_value = dict(_DEFAULT_UL_CFG_usage_log)
        with TestClient(app) as c:
            r = c.patch("/api/admin/usage-logging", json={"enabled": False, "retention_days": 60})
        assert r.status_code == 200
        assert r.json()["enabled"] is False
        assert r.json()["retention_days"] == 60
        assert mock_save.called

    @patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG_usage_log))
    @patch("backend.core.metadata.get_usage_logs")
    def test_usage_log_empty_table(self, mock_get_logs, mock_ul_cfg):
        mock_get_logs.return_value = (
            [],
            0,
            {
                "total_class_a": 0,
                "total_class_b": 0,
                "total_cdn_downloads": 0,
                "total_cdn_bytes": 0,
                "total_fos_bytes": 0,
                "class_a_breakdown": {},
                "class_b_breakdown": {},
            },
        )
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE_usage_log
        with TestClient(app) as c:
            r = c.get(
                "/api/admin/usage-log",
                params={"start": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"},
            )
        app.dependency_overrides.pop(get_source, None)

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 0
        assert body["aggregate"]["total_class_a"] == 0

    @patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG_usage_log))
    @patch("backend.core.metadata.get_usage_logs")
    def test_usage_log_aggregates_correctly(self, mock_get_logs, mock_ul_cfg):
        mock_get_logs.return_value = (
            [
                {
                    "id": 1,
                    "timestamp": "2024-06-01 12:00:00",
                    "service_id": "svc1",
                    "operation_class": "A",
                    "operation_type": "PutObject",
                    "url": "/file.gz",
                    "bytes": 0,
                    "duration_ms": 10,
                    "function_name": "test",
                    "process_context": "test",
                    "status": "OK",
                }
            ]
            * 4,
            4,
            {
                "total_class_a": 2,
                "total_class_b": 1,
                "total_cdn_downloads": 1,
                "total_cdn_bytes": 2147483648,
                "total_fos_bytes": 1073741824,
                "class_a_breakdown": {"PutObject": 2},
                "class_b_breakdown": {"GetObject": 1},
            },
        )
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE_usage_log
        with TestClient(app) as c:
            r = c.get(
                "/api/admin/usage-log",
                params={"start": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"},
            )
        app.dependency_overrides.pop(get_source, None)

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 4
        agg = body["aggregate"]
        assert agg["total_class_a"] == 2
        assert agg["total_class_b"] == 1

        # Class A: 2 ops @ $0.005/1k = 0.00001
        assert abs(agg["estimated_cost_class_a"] - 0.00001) < 1e-10
        # Class B: 1 op @ $0.01/10k = 0.000001
        assert abs(agg["estimated_cost_class_b"] - 0.000001) < 1e-10
        # CDN: 2GB @ $0.12/GB = 0.24
        assert abs(agg["estimated_cost_cdn"] - 0.24) < 1e-6

    @patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG_usage_log))
    @patch("backend.core.metadata.get_usage_logs")
    def test_usage_log_usage_type_filter_cdn_only(self, mock_get_logs, mock_ul_cfg):
        mock_get_logs.return_value = (
            [
                {
                    "id": 1,
                    "timestamp": "2024-06-01 12:01:00",
                    "service_id": "svc1",
                    "operation_class": "CDN",
                    "operation_type": "download",
                    "url": "/b.parquet",
                    "bytes": 1024,
                    "duration_ms": 10,
                    "function_name": "test",
                    "process_context": "test",
                    "status": "OK",
                }
            ],
            1,
            {
                "total_class_a": 0,
                "total_class_b": 0,
                "total_cdn_downloads": 1,
                "total_cdn_bytes": 1024,
                "total_fos_bytes": 0,
                "class_a_breakdown": {},
                "class_b_breakdown": {},
            },
        )
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE_usage_log
        with TestClient(app) as c:
            r = c.get(
                "/api/admin/usage-log",
                params={
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                    "usage_type": "CDN",
                },
            )
        app.dependency_overrides.pop(get_source, None)

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["entries"][0]["operation_class"] == "CDN"

    @patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG_usage_log))
    @patch("backend.core.metadata.get_usage_logs")
    def test_usage_log_process_context_filter(self, mock_get_logs, mock_ul_cfg):
        mock_get_logs.return_value = (
            [
                {
                    "id": 1,
                    "timestamp": "2024-06-01 12:00:00",
                    "service_id": "svc1",
                    "operation_class": "A",
                    "operation_type": "PutObject",
                    "url": "/a.gz",
                    "bytes": 0,
                    "duration_ms": 10,
                    "function_name": "test",
                    "process_context": "cron:sync:svc1",
                    "status": "OK",
                }
            ],
            1,
            {
                "total_class_a": 1,
                "total_class_b": 0,
                "total_cdn_downloads": 0,
                "total_cdn_bytes": 0,
                "total_fos_bytes": 0,
                "class_a_breakdown": {"PutObject": 1},
                "class_b_breakdown": {},
            },
        )
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE_usage_log
        with TestClient(app) as c:
            r = c.get(
                "/api/admin/usage-log",
                params={
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                    "process_context": "cron:sync",
                },
            )
        app.dependency_overrides.pop(get_source, None)

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert "cron:sync" in body["entries"][0]["process_context"]

    @patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG_usage_log))
    @patch("backend.core.metadata.get_usage_logs")
    def test_usage_log_operation_type_filter(self, mock_get_logs, mock_ul_cfg):
        mock_get_logs.return_value = (
            [
                {
                    "id": 1,
                    "timestamp": "2024-06-01 12:00:00",
                    "service_id": "svc1",
                    "operation_class": "A",
                    "operation_type": "PutObject",
                    "url": "/a.gz",
                    "bytes": 0,
                    "duration_ms": 10,
                    "function_name": "test",
                    "process_context": "test",
                    "status": "OK",
                }
            ],
            1,
            {
                "total_class_a": 1,
                "total_class_b": 0,
                "total_cdn_downloads": 0,
                "total_cdn_bytes": 0,
                "total_fos_bytes": 0,
                "class_a_breakdown": {"PutObject": 1},
                "class_b_breakdown": {},
            },
        )
        mock_get_logs.return_value = mock_get_logs.return_value  # keep it
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE_usage_log
        with TestClient(app) as c:
            r = c.get(
                "/api/admin/usage-log",
                params={
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                    "operation_type": "PutObject",
                },
            )
        app.dependency_overrides.pop(get_source, None)

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["entries"][0]["operation_type"] == "PutObject"


# ── Merged from tests/routers/test_usage_router.py ──
def test_extract_fos_ops_reads_flattened_keys():
    """The aggregate endpoint usually returns flattened keys —
    ``object_storage_class_a_operations_count`` etc. — so the helper
    reads those first."""
    record = {
        "object_storage_class_a_operations_count": 1234,
        "object_storage_class_b_operations_count": 5678,
        "other_metric": 9999,
    }
    a, b = _extract_fos_ops(record)
    assert a == 1234
    assert b == 5678


def test_extract_fos_ops_falls_back_to_nested_shape():
    """If the flattened keys are absent, fall back to the nested
    ``object_storage`` dict. Pinned because Fastly returns both
    shapes across endpoints and dropping the fallback would silently
    zero the customer's billed-ops chart."""
    record = {
        "object_storage": {
            "class_a_operations_count": 100,
            "class_b_operations_count": 200,
        }
    }
    a, b = _extract_fos_ops(record)
    assert a == 100
    assert b == 200


def test_extract_fos_ops_returns_zero_when_no_data():
    """No matching keys → (0, 0). Pinned because the caller sums
    these across days; returning None would crash the sum."""
    a, b = _extract_fos_ops({"other": 42})
    assert a == 0
    assert b == 0


def test_extract_fos_ops_handles_none_values_as_zero():
    """Null values from the API → 0 (not int(None)). Pinned because
    Fastly returns null for buckets with no activity."""
    record = {
        "object_storage_class_a_operations_count": None,
        "object_storage_class_b_operations_count": None,
    }
    a, b = _extract_fos_ops(record)
    assert (a, b) == (0, 0)


def test_extract_fos_ops_prefers_flat_over_nested_when_both_present():
    """When both shapes co-exist (legacy + new API together), the
    flattened form wins. Pinned to lock in the precedence — flipping
    it would double-count for the brief overlap period."""
    record = {
        "object_storage_class_a_operations_count": 999,
        "object_storage_class_b_operations_count": 999,
        "object_storage": {"class_a_operations_count": 1, "class_b_operations_count": 1},
    }
    a, b = _extract_fos_ops(record)
    assert (a, b) == (999, 999)


def test_fastly_api_constructs_correct_url_with_auth_header():
    """URL = ``https://api.fastly.com{path}`` and ``Fastly-Key`` header.
    Pinned because losing the Bearer-style auth would 401 every call."""
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"data": []}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
        result = _fastly_api("/services/svc1/details", "test-token")

    assert result == {"data": []}
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://api.fastly.com/services/svc1/details"
    assert req.get_header("Fastly-key") == "test-token"


def test_prefill_returns_global_rates(client, tmp_path, monkeypatch):
    """The prefill endpoint hydrates the cost estimator with the
    currently-configured per-class rates. Pinned because the
    frontend keys on these exact field names to populate inputs."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"},
    )
    config.save_usage_logging_config(
        {
            "enabled": True,
            "retention_days": 30,
            "class_a_rate_per_1k": 0.123,
            "class_b_rate_per_10k": 0.456,
            "cdn_egress_rate_per_gb": 0.789,
            "storage_rate_per_gb_month": 0.021,
        }
    )

    resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    body = resp.json()
    assert body["class_a_rate_per_1k"] == 0.123
    assert body["class_b_rate_per_10k"] == 0.456
    assert body["cdn_egress_rate_per_gb"] == 0.789
    assert body["storage_rate_per_gb_month"] == 0.021


def test_prefill_returns_defaults_when_no_usage_config_saved(client, tmp_path, monkeypatch):
    """A fresh install has no usage_logging config → defaults
    (0.005 / 0.01 / 0.12 / 0.02) surface. Pinned to lock the
    default values the cost estimator falls back to."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"},
    )

    resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    body = resp.json()
    assert body["class_a_rate_per_1k"] == 0.005
    assert body["class_b_rate_per_10k"] == 0.01


def test_current_storage_400s_when_bucket_not_configured(client):
    """Source without ``bucket`` → 400 with a friendly error. Pinned
    because the admin UI keys on this 400 to render the "configure
    FOS" call-to-action."""
    fake_src = {"name": "svc", "service_id": "svc"}  # no bucket

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: fake_src
    try:
        resp = client.get("/api/usage/current-storage")
    finally:
        # leave the fixture-provided override in place — TestClient resets
        pass

    assert resp.status_code == 400
    assert "bucket" in resp.json()["detail"]["error"].lower()


def _complete_src(**overrides) -> dict:
    """Source with all the fields the usage router reads."""
    base = {
        "name": "test_service",
        "service_id": MOCK_SERVICE_ID,
        "logging_service_id": MOCK_SERVICE_ID,
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
    }
    base.update(overrides)
    return base


def test_operations_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """``by`` values outside {hour, day} clamp to ``hour`` (NOT
    rejected). Pinned because the frontend defaults to "minute" for
    other endpoints and we don't want to 500 if the user accidentally
    sends "minute" here.

    Regression: this also exercises the date-format fix where
    ``parse_date_window`` returns ISO-T-Z strings and the route
    must NOT call ``strptime("%Y-%m-%d %H:%M:%S")`` on them.
    """
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    captured: dict = {}

    def _capture_fastly(path, api_key):
        captured["path"] = path
        return {"data": []}

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=_capture_fastly):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"by": "minute"},
        )

    assert resp.status_code == 200
    # The clamped granularity must end up in the API path
    assert "by=hour" in captured["path"]
    # Date-format regression: the from/to timestamps must be valid Unix
    # seconds (i.e. parse_window_str_to_dt successfully parsed the
    # ISO-T-Z value parse_date_window produced).
    assert "from=" in captured["path"] and "to=" in captured["path"]


def test_current_storage_delete_after_branch_uses_iceberg_as_live(client):
    """``delete_after=True`` → raw files counted as "deleted" and
    Iceberg files as "live". Pinned because flipping this would
    double-count the customer's bill.

    Regression: this also exercises the date-format fix in
    parse_window_str_to_dt and the None-cfg defensive guard."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Size": 5000}, {"Size": 3000}]},
    ]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        # get_storage_stats is patched below so the real connection is never
        # used; without this stub the route would try to open the default
        # ``logs.duckdb`` relative path read-only and crash with IO Error.
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 8000, "data_files": 2},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 10, "total_bytes": 12345, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["live_bytes"] == 8000  # iceberg only
    assert body["deleted_bytes"] == 12345  # raw → deleted bucket


def test_current_storage_no_delete_branch_sums_raw_and_iceberg_as_live(client):
    """``delete_after=False`` → all bytes are "live" with zero "deleted"."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Size": 1000}]},
    ]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": False, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 1000, "data_files": 1},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 5, "total_bytes": 4000, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["live_bytes"] == 5000  # iceberg (1000) + raw (4000)
    assert body["deleted_bytes"] == 0


def test_current_storage_handles_missing_config_gracefully(client):
    """REGRESSION: when ``load_config(src['name'])`` returns None
    (config missing / mid-teardown), the route must NOT crash with
    AttributeError on ``cfg.get(...)``. Pinned because this is the
    defensive None-guard added in the same commit as the date-format
    fix."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.config.load_config", return_value=None),  # config missing
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 0, "data_files": 0},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 0, "total_bytes": 0, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Must not 500
    assert resp.status_code == 200


def test_current_storage_emits_gb_hours_for_billing_card(client):
    """The 30-day minimum billing window is baked into ``total_billed_gb_hours``.
    Pinned because the frontend's cost card multiplies this by the
    per-GB-month rate and divides by 720 — a refactor that changed
    the basis would silently 30x or /30 the bill estimate."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={
                # 1 GB = 1024**3 bytes
                "total_files": 1,
                "total_bytes": 1024**3,
                "debug_queries": [],
            },
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # 1 GB * (30 * 24 hours) = 720 GB-hours
    assert body["total_billed_gb_hours"] == 720.0


def test_log_activity_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """Mirror of /operations — invalid ``by`` clamps to ``hour``."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    resp = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02", "by": "fortnight"},
    )

    assert resp.status_code == 200
    assert resp.json()["granularity"] == "hour"


def test_log_activity_accepts_supported_granularities(client, tmp_path, monkeypatch):
    """Supported values are {second, minute, hour, day} — all pass
    through unchanged. Pinned because the FE dropdown lists these
    four; adding/removing would diverge from the API contract."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    for by in ("second", "minute", "hour", "day"):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": by},
        )
        assert resp.status_code == 200
        assert resp.json()["granularity"] == by


def test_operations_403_when_no_fastly_api_key(client, tmp_path, monkeypatch):
    """No ``fastly_api_key`` on the service config → 403 with friendly
    error. Pinned because the FE renders the "configure API key" CTA
    keyed on this exact 403."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # No fastly_api_key saved
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    app.dependency_overrides[get_source] = lambda: _complete_src()
    resp = client.get(
        "/api/usage/operations",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02"},
    )

    assert resp.status_code == 403
    assert "api key" in resp.json()["detail"]["error"].lower()


def test_operations_accumulates_class_a_and_class_b_per_day(client, tmp_path, monkeypatch):
    """End-to-end accumulation: two records on the same day are summed,
    a different day is its own bucket. Pinned because the billing card
    keys on the per-day shape sort-by-date."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    # Two records on 2026-01-05, one on 2026-01-06
    fake_payload = {
        "data": [
            {
                "start_time": 1767657600,  # 2026-01-05 00:00 UTC
                "object_storage_class_a_operations_count": 100,
                "object_storage_class_b_operations_count": 50,
            },
            {
                "start_time": 1767661200,  # 2026-01-05 01:00 UTC
                "object_storage_class_a_operations_count": 200,
                "object_storage_class_b_operations_count": 25,
            },
            {
                "start_time": 1767744000,  # 2026-01-06 00:00 UTC
                "object_storage_class_a_operations_count": 7,
                "object_storage_class_b_operations_count": 8,
            },
        ]
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", return_value=fake_payload):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "day"},
        )

    assert resp.status_code == 200
    body = resp.json()
    # Totals reflect all three records summed
    assert body["total_class_a"] == 307
    assert body["total_class_b"] == 83
    # Two records on the same day are merged into one bucket, the
    # third record gets its own bucket — total 2 distinct day buckets
    assert len(body["data"]) == 2
    # And the bucket with the same-day pair sums to 300/75
    day_buckets = sorted(body["data"], key=lambda d: d["date"])
    assert day_buckets[0]["class_a"] == 300
    assert day_buckets[0]["class_b"] == 75
    assert day_buckets[1]["class_a"] == 7
    assert day_buckets[1]["class_b"] == 8


def test_operations_maps_http_error_to_502(client, tmp_path, monkeypatch):
    """Fastly Stats upstream failure → 502 with a generic error code +
    correlation id. The upstream status and body are NOT echoed to the
    client (they can contain internal hostnames / token fragments per
    the v2.0 raise_internal sweep); operators triage via the server log
    keyed on ``error_id``. Pinned because the FE distinguishes 502
    (transient upstream) from 4xx (config issue) when retrying.
    """
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    err = RuntimeError("HTTP 503 GET /stats/aggregate\n    service unavailable")

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=err):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502
    body = resp.json()["detail"]
    assert body["error"] == "fastly_stats_aggregate_failed"
    assert "error_id" in body
    # Critical: upstream status/body MUST NOT leak through the wire.
    assert "503" not in body["error"]
    assert "service unavailable" not in body["error"]


def test_operations_maps_generic_exception_to_502(client, tmp_path, monkeypatch):
    """Non-HTTPError exceptions (connection errors, timeouts) also
    map to 502 — same retry semantics on the FE."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("connection refused")):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502


def test_bandwidth_403_when_no_fastly_api_key(client, tmp_path, monkeypatch):
    """Mirror of operations: no API key → 403."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})  # no api_key

    app.dependency_overrides[get_source] = lambda: _complete_src()
    resp = client.get(
        "/api/usage/bandwidth",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 403


def test_bandwidth_skips_fastly_call_when_no_cdn_service_id(client, tmp_path, monkeypatch):
    """When ``cdn_service_id`` is empty, the route returns an empty
    data array WITHOUT calling Fastly. Pinned because making the call
    against an empty service ID would 404 the Stats API and surface
    as a 502 to the user — confusing for unprovisioned services."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="")
    with patch("backend.routers.usage._fastly_api") as mock_api:
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 200
    assert resp.json()["data"] == []
    assert resp.json()["total_bytes"] == 0
    # No Fastly call was made
    mock_api.assert_not_called()


def test_bandwidth_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """``by`` values outside {hour, minute, day} clamp to ``hour``."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    captured = {}

    def _capture(path, api_key):
        captured["path"] = path
        return {"data": []}

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", side_effect=_capture):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "fortnight"},
        )

    assert resp.status_code == 200
    assert resp.json()["granularity"] == "hour"
    assert "by=hour" in captured["path"]


def test_bandwidth_aggregates_payload_into_points(client, tmp_path, monkeypatch):
    """End-to-end: bandwidth records aggregate by timestamp and the
    total_bytes sum lands in the response. Pinned because the cost
    card multiplies total_bytes by the per-GB egress rate."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    payload = {
        "data": [
            {"start_time": 1767657600, "bandwidth": 1000, "requests": 5},
            {"start_time": 1767657600, "bandwidth": 500, "requests": 2},  # same ts → merged
            {"start_time": 1767661200, "bandwidth": 300, "requests": 1},
        ]
    }

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", return_value=payload):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "hour"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_bytes"] == 1800  # 1000 + 500 + 300
    # Two distinct time buckets after merging the duplicate timestamp
    assert len(body["data"]) == 2


def test_bandwidth_maps_fastly_exception_to_502(client, tmp_path, monkeypatch):
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("timeout")):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502


def test_log_activity_enriches_with_api_request_counts(client, tmp_path, monkeypatch):
    """When ``fastly_api_key`` + ``logging_service_id`` exist AND
    ``by`` ∈ {minute, hour, day}, the route enriches the repo-returned
    rows with ``api_requests`` from /stats/service. Pinned because
    the "logs generated vs processed" panel keys on this enrichment."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    # Use a time bucket that lines up with the Fastly payload's
    # start_time → the route uses datetime.fromtimestamp(ts, UTC) to
    # derive the time key, so compute it the same way for the repo row.
    from datetime import UTC, datetime

    api_ts = 1767657600
    time_key = datetime.fromtimestamp(api_ts, tz=UTC).strftime("%Y-%m-%dT%H:00")

    fake_repo_data = {
        "data": [{"time": time_key, "row_count": 100, "bytes": 1000}],
        "granularity": "hour",
        "total_rows": 100,
        "total_bytes": 1000,
        "total_api_requests": 0,
    }
    fake_api_payload = {"data": [{"start_time": api_ts, "requests": 200}]}

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api", return_value=fake_api_payload),
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200
    body = resp.json()
    # The repo row got annotated with api_requests
    by_time = {p["time"]: p for p in body["data"]}
    assert by_time[time_key]["api_requests"] == 200
    assert body["total_api_requests"] == 200


def test_log_activity_swallows_fastly_enrichment_failure(client, tmp_path, monkeypatch):
    """When the Fastly API enrichment fails (network error, 5xx),
    the route still returns the repo data unchanged. Pinned because
    raising here would convert a partial-data success into a 500 —
    customers want SOME data over none."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    fake_repo_data = {
        "data": [{"time": "2026-01-05T00:00", "row_count": 100, "bytes": 1000}],
        "granularity": "hour",
        "total_rows": 100,
        "total_bytes": 1000,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("upstream down")),
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200


def test_log_activity_skips_fastly_enrichment_for_by_second(client, tmp_path, monkeypatch):
    """``by=second`` skips the API enrichment entirely (the Stats API
    only supports minute+ granularity). Pinned because making the call
    with by=second would 422 from Fastly and surface confusingly here."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    fake_repo_data = {
        "data": [],
        "granularity": "second",
        "total_rows": 0,
        "total_bytes": 0,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api") as mock_api,
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "second"},
        )

    assert resp.status_code == 200
    mock_api.assert_not_called()


def test_log_activity_skips_enrichment_when_no_api_key(client, tmp_path, monkeypatch):
    """Without an API key, no enrichment — and no 500. Pinned because
    a freshly-provisioned service without the API key saved should
    still render log-activity data from the repo."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # No fastly_api_key
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    fake_repo_data = {
        "data": [{"time": "2026-01-05T00:00", "row_count": 50, "bytes": 500}],
        "granularity": "hour",
        "total_rows": 50,
        "total_bytes": 500,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api") as mock_api,
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200
    mock_api.assert_not_called()


def test_prefill_uses_cached_status_for_avg_file_size_and_data_days(client, tmp_path, monkeypatch):
    """When `cached_status` has `avg_log_size_kb` + earliest/latest
    timestamps, the prefill response surfaces them as
    `avg_log_file_size_kb` + `data_days`. Pinned because the cost
    estimator's daily-bytes formula multiplies avg_kb × requests/day
    × data_days — losing this enrichment would render zeros."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"})

    fake_status = {
        "avg_log_size_kb": 42.5,
        "earliest_log_at": "2026-05-01T00:00:00Z",
        "latest_log_at": "2026-05-10T00:00:00Z",
    }

    with patch("backend.config.get_status", return_value=fake_status):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["avg_log_file_size_kb"] == 42.5
    # 10 days inclusive (May 1 → May 10)
    assert body["data_days"] >= 9


def test_prefill_handles_cached_status_parse_failure_gracefully(client, tmp_path, monkeypatch):
    """If parsing earliest/latest timestamps raises, return whatever
    we have without crashing. Pinned because cached_status can have
    malformed timestamps from older versions — the prefill must not
    500 because of that."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"})

    fake_status = {"earliest_log_at": "garbage", "latest_log_at": "also garbage"}
    with patch("backend.config.get_status", return_value=fake_status):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Must not 500
    assert resp.status_code == 200


def test_prefill_propagates_provisioning_settings_into_response(client, tmp_path, monkeypatch):
    """`prov.sample_rate`, `prov.edge_only`, `prov.cron_sync.delete_after`,
    `prov.cron_compact.enabled`, `cfg.log_retention_days` all flow
    into the prefill response. Pinned because the cost estimator
    pre-populates its inputs from these exact field names — a
    rename would render the wizard's "save settings" inputs empty.

    Note: the prefill route calls ``svcconfig.load_config(source["name"])``
    so we patch load_config directly to avoid the source.name vs
    saved-key key mismatch (source.name = ``test_service`` per fixture,
    while the wizard saves keyed by service_id)."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
        "log_retention_days": 60,
        "log_period": 120,
        "provisioning": {
            "sample_rate": 50,
            "edge_only": False,
            "cron_sync": {"delete_after": False, "commit_interval_mins": 10},
            "cron_compact": {"enabled": False},
        },
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["sample_rate"] == 50
    assert body["edge_only"] is False
    assert body["compaction_enabled"] is False
    assert body["delete_after"] is False
    assert body["commit_interval_mins"] == 10
    assert body["log_retention_days"] == 60
    assert body["log_period_seconds"] == 120


def test_prefill_fetches_requests_per_day_from_fastly_stats_when_api_key_set(client, tmp_path, monkeypatch):
    """When `fastly_api_key` is configured AND a stats svc_id exists,
    prefill calls /stats/service to compute requests_per_day +
    edge_requests_per_day averages. Pinned because the cost estimator
    multiplies these by the per-class operation rate."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # 3 days of data, 1000 requests/day each (today filtered out as incomplete)
    fake_payload = {
        "data": [
            {"start_time": 1767225600, "requests": 1000, "edge_requests": 800},  # 2026-01-01
            {"start_time": 1767312000, "requests": 1500, "edge_requests": 1200},  # 2026-01-02
        ]
    }

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
    }

    def fake_complete_src(**overrides):
        return {**_complete_src(), "cdn_service_id": "cdn-svc-id", **overrides}

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: fake_complete_src()

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch("backend.routers.usage._fastly_api", return_value=fake_payload),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # average of 1000 + 1500 = 1250 (today is filtered out, leaving complete days)
    assert body["requests_per_day"] == 1250
    assert body["edge_requests_per_day"] == 1000  # (800 + 1200) / 2


def test_prefill_extracts_sample_rate_from_log_sampling_condition_vcl(client, tmp_path, monkeypatch):
    """When the active version has a "Log Sampling" response_condition,
    prefill parses `randombool(N, …)` from the VCL statement and
    surfaces N as `sample_rate`. Pinned because the FE renders the
    sample-rate slider position from this value — losing the parse
    would render 100% even when actually sampling at 25%."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    from backend.deps import get_source
    from backend.main import app

    # Both cdn_service_id (used for stats svc_id) and logging_service_id
    # required for the VCL-parsing branch to fire
    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-svc-id")

    fake_cfg = {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"}
    fake_endpoint = {
        "period": 60,
        "response_condition": "Log Sampling",
    }
    fake_cond = {"statement": "randombool(25, 100) && req.restarts == 0"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=42),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
        patch("backend.core.fastly.service.find_condition", return_value=fake_cond),
        patch("backend.routers.usage._fastly_api", return_value={"data": []}),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    body = resp.json()
    assert body["sample_rate"] == 25
    assert body["edge_only"] is True  # req.restarts == 0 → edge-only
    # log_period_seconds comes from endpoint's "period"
    assert body["log_period_seconds"] == 60


def test_prefill_swallows_fastly_api_exception_returns_partial_response(client, tmp_path, monkeypatch):
    """If the Fastly Stats API call raises (network failure), prefill
    still returns the static config-derived fields rather than 500.
    Pinned because losing this would block dashboard from rendering
    on every transient Fastly hiccup."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-id")

    fake_cfg = {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("upstream down")),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Returns 200 with static defaults rather than 500
    assert resp.status_code == 200
    body = resp.json()
    # Static defaults still present
    assert "sample_rate" in body
    assert "class_a_rate_per_1k" in body


def test_prefill_computes_estimated_bytes_per_line_from_log_fields(client, tmp_path, monkeypatch):
    """`estimated_bytes_per_line` is computed from the saved
    log_fields config. Pinned because the cost estimator uses this
    × requests/day × bytes/$ as the "log storage" line item."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
        "log_fields": {"groups": ["A", "B"], "field_overrides": {}},
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    body = resp.json()
    assert body["estimated_bytes_per_line"] is not None
    # Non-empty config should produce a positive byte count
    assert body["estimated_bytes_per_line"] > 0


def test_prefill_calculates_rum_beacons_per_day(client, tmp_path, monkeypatch):
    """Verify that `/api/usage/prefill` calculates `rum_beacons_per_day`
    based on client_vitals and client_errors ingested files in SQLite."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # Set up specific clean get_source override
    source_name = "test_rum_svc"
    app.dependency_overrides[get_source] = lambda: _complete_src(name=source_name)

    # Initialize metadata DB with ingested_files table
    from backend.core.metadata import get_con

    db = get_con(source_name)

    # Insert mock RUM file records
    db.execute(
        f"""
        INSERT INTO ingested_files (source_name, table_name, file_name, file_date, row_count, file_size_bytes, ingested_at)
        VALUES
        ('{source_name}', 'client_vitals', 'vit1.parquet', '2026-08-10', 1000, 50000, '2026-08-10 12:00:00'),
        ('{source_name}', 'client_errors', 'err1.parquet', '2026-08-10', 500, 25000, '2026-08-10 12:05:00'),
        ('{source_name}', 'client_vitals', 'vit2.parquet', '2026-08-11', 2000, 100000, '2026-08-11 12:00:00')
        """
    )
    db.commit()

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": source_name,
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # 2026-08-10 has 1000 + 500 = 1500 beacons
    # 2026-08-11 has 2000 beacons
    # Average should be (1500 + 2000) / 2 = 1750 beacons
    assert body["rum_beacons_per_day"] == 1750


def test_prefill_returns_billing_zone_and_default(client, tmp_path, monkeypatch):
    """Verify that /api/usage/prefill returns the configured billing_zone or default MBZ200."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"},
    )

    # 1. Default when not configured is MBZ200
    resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    assert resp.json()["billing_zone"] == "MBZ200"

    # 2. Configured billing_zone MBZ100
    config.save_usage_logging_config({"billing_zone": "MBZ100"})
    resp2 = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp2.status_code == 200
    assert resp2.json()["billing_zone"] == "MBZ100"


def test_admin_usage_logging_patch_billing_zone(client, tmp_path, monkeypatch):
    """Verify PATCH /api/admin/usage-logging validates billing_zone and syncs cdn_egress_rate_per_gb."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # 1. Invalid zone raises 400
    resp = client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"})
    assert resp.status_code == 400
    assert "Invalid billing_zone" in resp.json()["detail"]["error"]

    # 2. Valid zone MBZ100 auto-syncs cdn_egress_rate_per_gb to 0.08
    resp_mbz100 = client.patch("/api/admin/usage-logging", json={"billing_zone": "MBZ100"})
    assert resp_mbz100.status_code == 200
    data = resp_mbz100.json()
    assert data["billing_zone"] == "MBZ100"
    assert data["cdn_egress_rate_per_gb"] == 0.08

    # 3. Valid zone Global auto-syncs cdn_egress_rate_per_gb to 0.18
    resp_global = client.patch("/api/admin/usage-logging", json={"billing_zone": "Global"})
    assert resp_global.status_code == 200
    data = resp_global.json()
    assert data["billing_zone"] == "Global"
    assert data["cdn_egress_rate_per_gb"] == 0.18

    # 4. Explicit override preserved
    resp_custom = client.patch(
        "/api/admin/usage-logging",
        json={"billing_zone": "MBZ200", "cdn_egress_rate_per_gb": 0.15},
    )
    assert resp_custom.status_code == 200
    data = resp_custom.json()
    assert data["billing_zone"] == "MBZ200"
    assert data["cdn_egress_rate_per_gb"] == 0.15


_ = json

_ = pytest


# ── Merged from tests/routers/test_usage_summary.py ──
@pytest.fixture
def test_client():
    return TestClient(app)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_success(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """The endpoint returns 200 when storage stats and FOS listing both
    succeed. Migrated from inline boto3-MagicMock paginator stubs to the
    shared moto-backed s3_mock fixture — the LIST fallback now flows
    through real S3 semantics rather than a hand-rolled paginator stub.

    ``get_connection`` is still mocked: the route opens a DuckDB handle
    only to pass to the already-mocked ``get_storage_stats``, so the real
    DB connection contributes nothing to what we're verifying — and under
    pytest-xdist the on-disk per-service DuckDB file can be held by a
    peer worker, surfacing as a DBBusyError 500 we'd otherwise chase.
    """
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    mock_get_storage_stats.return_value = {"total_files": 10, "total_bytes": 1024, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    # Seed one iceberg object so the FOS LIST fallback finds something.
    # get_table_info (tried first) has no real iceberg metadata to read
    # and will swallow its own exception, falling through to the LIST.
    source = {**fos_source, "prefix": "test-prefix"}
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/iceberg/data/x.parquet",
        Body=b"x" * 1024,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_splits_out_rum_bytes(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """RUM beacon objects live under ``raw/rum/`` inside the same bucket as
    regular logs. The endpoint must report their size separately
    (``rum_bytes``) and subtract it from the regular-log total
    (``regular_log_bytes``) rather than double-counting it into both."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 5000
    mock_get_storage_stats.return_value = {"total_files": 3, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "prefix": "test-prefix"}
    rum_object_size = 777
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/raw/rum/2026-08-05T00-00-00.log.gz",
        Body=b"r" * rum_object_size,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == rum_object_size
        assert body["regular_log_bytes"] == total_bytes - rum_object_size
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_rum_scan_failure_defaults_to_zero(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """A broken FOS listing for the RUM prefix must not 500 the whole
    cost panel — it should fail open to rum_bytes=0 (regular_log_bytes
    falls back to the full total, same as before this field existed)."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 2048
    mock_get_storage_stats.return_value = {"total_files": 1, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "bucket": "bucket-that-does-not-exist"}

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == 0
        assert body["regular_log_bytes"] == total_bytes
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_api_key_falls_back_to_placeholder_in_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == "mock-api-key"


def test_api_key_stays_empty_outside_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == ""


def test_api_key_prefers_the_configured_key(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "real-key")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == "real-key"


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute(
        "DELETE FROM ingested_files WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
        (service_id,),
    )
    db.commit()


def _seed(service_id: str, received_at: str, count: int) -> None:
    db = get_con(service_id)
    date_str = received_at.split("T")[0]
    db.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name, file_date) VALUES (?, ?, ?, ?, ?, ?)",
        (f"s3://bucket/{received_at}.parquet", service_id, count, 100, "client_vitals", date_str),
    )
    db.commit()


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_computes_beacon_counts_and_cost(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_basic"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.01}

    _seed(service_id, "2026-08-05T10:00:00Z", 3)
    _seed(service_id, "2026-08-06T10:00:00Z", 7)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200, r.text
        body = r.json()

        assert body["total_beacons"] == 10
        assert body["total_estimated_class_a"] == 10
        assert body["class_a_rate_per_1k"] == 0.01
        # 10 ops / 1000 * 0.01 = 0.0001
        assert body["total_estimated_cost_usd"] == pytest.approx(0.0001, abs=1e-8)

        by_date = {p["date"]: p for p in body["data"]}
        assert by_date["2026-08-05"]["beacon_count"] == 3
        assert by_date["2026-08-06"]["beacon_count"] == 7
        assert by_date["2026-08-06"]["estimated_cost_usd"] == pytest.approx((7 / 1000.0) * 0.01, abs=1e-8)
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_no_beacons_returns_zeroed_response(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_empty"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["total_estimated_cost_usd"] == 0.0
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_rum_breakdown_no_resolvable_service_returns_not_found_note(test_client):
    source = {"name": None, "service_id": None}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["note"] == "Service not found."
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_clamps_sub_hourly_granularity_to_daily(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_clamp"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}
    _seed(service_id, "2026-08-05T10:00:00Z", 1)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-06T00:00:00Z", "by": "minute"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "adjusted to daily granularity" in body["note"]
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


def test_rum_breakdown_db_query_failure_falls_back_to_zero_counts(test_client, monkeypatch):
    service_id = "test_usage_rum_breakdown_db_failure"

    def _raise(_sid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.core.metadata.get_con", _raise)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
    finally:
        app.dependency_overrides.pop(get_source, None)
