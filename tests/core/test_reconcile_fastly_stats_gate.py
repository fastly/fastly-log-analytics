"""Regression tests for reconcile_fastly_stats hourly gating.

Fastly's /stats/aggregate snaps to hour boundaries, so reconciling more than
once per hour is pure waste: the upstream numbers don't change, and the
per-class SUBSTR scan over `usage_log` for the 26h window is ~700ms on a
populated DB. The gate at the top of `reconcile_fastly_stats` skips the
Fastly API call entirely when we already reconciled within the last hour.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest


@pytest.fixture
def fresh_service():
    """Empty usage_log + ingested_files for a unique service id."""
    from backend.core import metadata as metadata_db

    service_id = "recon-svc-1"
    # Touch ingested_files so the DB is initialised cleanly.
    metadata_db.insert_ingested_files(
        service_id,
        [("s3://bkt/raw/2026-05-22/12/2026-05-22T12:00:00.000-x.log.gz", 1, 1)],
    )
    yield service_id


def _seed_reconciliation_row(service_id: str, ts_iso: str) -> None:
    # usage_log lives in its own per-service SQLite (v2.0 cutover);
    # ``get_latest_reconciliation_ts`` reads from there too.
    from backend.core.metadata import usage_log_db as _usage_log_db

    con = _usage_log_db.get_con(service_id)
    con.execute(
        """
        INSERT INTO usage_log
        (timestamp, service_id, operation_class, operation_type, url, status,
         duration_ms, function_name, process_context, count)
        VALUES (?, ?, 'A', 'RECONCILE_A', 'fastly://stats/aggregate/test', 'OK',
                0.0, 'fastly.reconciliation', 'fastly:reconciliation', 1)
        """,
        (ts_iso, service_id),
    )
    con.commit()


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_skipped_when_recent_reconciliation_exists(_key, _enabled, fresh_service):
    """A reconciliation row written 30 minutes ago should short-circuit the call
    before any HTTP request to Fastly is made."""
    from backend.core import duckdb as _db

    recent = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(fresh_service, recent)

    src = {"name": fresh_service, "logging_service_id": "logsvc"}
    with patch("urllib.request.urlopen") as urlopen:
        result = _db.reconcile_fastly_stats(src)
        assert result == 0
        assert not urlopen.called


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_runs_when_last_reconciliation_is_old(_key, _enabled, fresh_service):
    """A reconciliation row written >1h ago should NOT block the next call —
    we proceed to the Fastly API."""
    from backend.core import duckdb as _db

    old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(fresh_service, old)

    src = {"name": fresh_service, "logging_service_id": "logsvc"}
    with patch("urllib.request.urlopen") as urlopen:
        # urlopen raises so we don't have to mock the full response shape — we
        # only care that the gate let us past.
        urlopen.side_effect = RuntimeError("called!")
        _db.reconcile_fastly_stats(src)
        assert urlopen.called


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_runs_when_no_prior_reconciliation(_key, _enabled, fresh_service):
    """First-ever reconcile call (no prior 'fastly.reconciliation' rows) must
    proceed past the gate."""
    from backend.core import duckdb as _db

    src = {"name": fresh_service, "logging_service_id": "logsvc"}
    with patch("urllib.request.urlopen") as urlopen:
        urlopen.side_effect = RuntimeError("called!")
        _db.reconcile_fastly_stats(src)
        assert urlopen.called
