"""Tests for rdns_enrichment cron job and rDNS admin endpoints.

Covers:
- _run_rdns_enrichment success, warning on DNS resolution failures, and dev_mode_no_crons skip.
- enrich_batch dynamic queue-depth batch sizing (low, medium, high backlog).
- POST /api/admin/rdns/enrich and GET /api/admin/rdns/stats admin endpoints.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.cron.jobs.metadata import _run_rdns_enrichment
from backend.main import app
from backend.utils import rdns_cache


def test_run_rdns_enrichment_records_success_with_counts():
    """Successful enrichment records resolved, errors, and discovered counts with status='success'."""
    fake_summary = {"resolved": 20, "errors": 0, "discovered": 10, "batch_limit": 200}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "success"
    summary = args[3]
    assert "resolved=20" in summary
    assert "errors=0" in summary
    assert "discovered=10" in summary


def test_run_rdns_enrichment_records_warning_on_dns_failures():
    """When resolution has errors and zero successful resolves, status is recorded as 'warning'."""
    fake_summary = {"resolved": 0, "errors": 8, "discovered": 0, "batch_limit": 200}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "warning"
    summary = args[3]
    assert "DNS lookup failures detected" in summary


def test_run_rdns_enrichment_skips_when_dev_mode_active():
    """When FLA_DEV_NO_CRONS=1 is set, enrichment skips DuckDB view queries and DNS resolution."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.rdns_cache.enrich_batch") as mock_enrich,
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_enrich.assert_not_called()
    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "skipped"
    assert "dev_mode_no_crons" in args[3]


@pytest.mark.parametrize(
    ("pending_count", "expected_batch_limit"),
    [
        (0, 200),     # empty backlog defaults to standard 200 limit
        (50, 100),    # small backlog uses baseline minimum 100
        (150, 150),   # moderate small backlog scales to exact count
        (500, 500),   # medium backlog scales up to 500
        (2500, 1000), # large backlog caps at safety ceiling 1000
    ],
)
def test_enrich_batch_dynamic_batch_sizing(pending_count, expected_batch_limit):
    """Dynamic queue depth scaling selects appropriate batch limits."""
    with (
        patch("backend.utils.rdns_cache._get_pending_count", return_value=pending_count),
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]),
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        res = rdns_cache.enrich_batch()
        assert res["batch_limit"] == expected_batch_limit


def test_enrich_batch_honors_explicit_limit_override():
    """When explicit limit is supplied, dynamic queue scaling is bypassed."""
    with (
        patch("backend.utils.rdns_cache._get_pending_count") as mock_count,
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]) as mock_select,
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        res = rdns_cache.enrich_batch(limit=42)
        mock_count.assert_not_called()
        assert res["batch_limit"] == 42
        assert mock_select.call_args[1]["limit"] == 42


def test_admin_rdns_enrich_endpoint():
    """POST /api/admin/rdns/enrich runs manual enrichment batch and returns summary."""
    client = TestClient(app)
    fake_summary = {"resolved": 5, "errors": 0, "discovered": 2, "batch_limit": 200}

    with patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)


def test_admin_rdns_stats_endpoint():
    """GET /api/admin/rdns/stats returns cache statistics."""
    client = TestClient(app)
    fake_stats = {"total": 1200, "pending": 45, "last_enrichment_at": "2026-09-21T00:00:00Z"}

    with patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats):
        resp = client.get("/api/admin/rdns/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["stats"] == fake_stats
