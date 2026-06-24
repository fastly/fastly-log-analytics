"""Tests for backend.cron.jobs.duckdb_recycle — the @global_job scheduler entry.

The recycle *mechanism* is covered in tests/core/test_duckdb_recycle.py; this
file covers the thin cron-layer wrapper (RSS-threshold gate + global_job
plumbing) that lives in the cron layer so core stays free of a backend.cron
import.
"""

from unittest.mock import patch

from backend.cron.jobs.duckdb_recycle import run_duckdb_recycle


def test_run_duckdb_recycle_skips_below_rss_threshold(monkeypatch):
    """The scheduler entry skips the recycle when RSS is below the threshold."""
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "999999999")
    # __wrapped__ unwraps the global_job decorator to call the inner fn directly.
    detail = run_duckdb_recycle.__wrapped__()
    assert detail.startswith("skipped:")


def test_run_duckdb_recycle_runs_when_threshold_disabled(monkeypatch):
    """With no RSS threshold (0 = always run), the entry delegates to
    recycle_once rather than short-circuiting."""
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "0")
    with patch(
        "backend.cron.jobs.duckdb_recycle.recycle_once", return_value="interval: recycled 0/0 instance(s), freed ~0MB"
    ) as mock_recycle:
        detail = run_duckdb_recycle.__wrapped__()
    mock_recycle.assert_called_once_with(reason="interval")
    assert "recycled" in detail
