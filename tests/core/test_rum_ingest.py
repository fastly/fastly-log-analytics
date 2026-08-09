from __future__ import annotations

import gzip
import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from backend.core.rum_ingest import ingest_rum_logs


@pytest.fixture
def mock_metadata_db(tmp_path):
    """Create a temporary SQLite metadata DB for testing."""
    db_path = tmp_path / "test_service.metadata.db"
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    # Initialize required tables matching actual schemas
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_beacons (
            id INTEGER PRIMARY KEY,
            service_id TEXT NOT NULL,
            received_at TEXT NOT NULL DEFAULT (datetime('now')),
            beacon_data TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            file_name TEXT,
            source_name TEXT,
            ingested_at TEXT DEFAULT (datetime('now')),
            row_count INTEGER,
            file_size_bytes INTEGER,
            error_count INTEGER DEFAULT 0,
            file_date DATE,
            PRIMARY KEY (file_name, source_name)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingested_files_summary (
            source_name TEXT PRIMARY KEY,
            file_count INTEGER NOT NULL DEFAULT 0,
            total_rows INTEGER NOT NULL DEFAULT 0,
            total_bytes INTEGER NOT NULL DEFAULT 0,
            count_with_bytes INTEGER NOT NULL DEFAULT 0,
            latest_file_name TEXT,
            last_ingested TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cron_runs (
            id INTEGER PRIMARY KEY,
            service_id TEXT,
            job_name TEXT,
            duration_seconds REAL,
            status TEXT,
            rows_ingested INTEGER,
            error_message TEXT,
            started_at TEXT
        )
    """)
    con.commit()
    yield con
    con.close()


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
def test_ingest_rum_logs_empty_bucket(
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
):
    """Test RUM ingest behavior when the bucket contains no new RUM logs."""
    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
        "prefix": "test-prefix",
    }

    # Mock S3 list_objects_v2 paginator returning empty contents
    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{}]
    mock_s3.get_paginator.return_value = mock_paginator

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    assert events == [("started", 123), ("done", 0)]
    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"  # Status is success (no logs found is not an error)
    assert mock_log.call_args[1]["rows_ingested"] == 0
    mock_finalize.assert_called_once_with(service_id, "rum_sync", 123)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
def test_ingest_rum_logs_success(
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
):
    """Test successful ingestion, decompression, mapping, and recording of RUM log files."""
    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
    }

    # Generate sample RUM log line matching standard Fastly fields
    raw_log = {
        "rum_cid": "sess_123",
        "fastly_req_id": "req_vital_0",
        "rum_metric_name": "LCP",
        "rum_metric_value": 1800.0,
        "rum_metric_rating": "good",
        "rum_pathname": "/about",
        "timestamp": "2026-08-07T03:07:47+00:00",
    }
    gzipped_content = gzip.compress(json.dumps(raw_log).encode("utf-8"))

    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/rum/rum_log_0.json.gz", "Size": len(gzipped_content)},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    # Mock s3 get_object response
    mock_response = {"Body": MagicMock()}
    mock_response["Body"].read.return_value = gzipped_content
    mock_s3.get_object.return_value = mock_response

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    assert events == [
        ("started", 123),
        ("file_done", "raw/rum/rum_log_0.json.gz", 1),
        ("done", 1),
    ]

    # Verify rows in database
    beacons = mock_metadata_db.execute("SELECT * FROM rum_beacons").fetchall()
    assert len(beacons) == 1
    assert beacons[0]["service_id"] == service_id
    assert beacons[0]["received_at"] == "2026-08-07T03:07:47+00:00"

    parsed_beacon = json.loads(beacons[0]["beacon_data"])
    assert parsed_beacon["pathname"] == "/about"
    assert parsed_beacon["path"] == "/about"
    assert parsed_beacon["name"] == "LCP"
    assert parsed_beacon["value"] == 1800.0
    assert parsed_beacon["rating"] == "good"
    assert parsed_beacon["cid"] == "sess_123"

    # Verify that file key was successfully registered to ingested_files
    ingested = mock_metadata_db.execute("SELECT * FROM ingested_files").fetchall()
    assert len(ingested) == 1
    assert ingested[0]["file_name"] == "raw/rum/rum_log_0.json.gz"
    assert ingested[0]["row_count"] == 1
