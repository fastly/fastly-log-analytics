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

    con.execute("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            file_name TEXT,
            source_name TEXT,
            ingested_at TEXT DEFAULT (datetime('now')),
            row_count INTEGER,
            file_size_bytes INTEGER,
            error_count INTEGER DEFAULT 0,
            file_date DATE,
            table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (file_name, source_name, table_name)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingest_in_flight (
            buffer_filename TEXT NOT NULL,
            source_name TEXT NOT NULL,
            files_json TEXT NOT NULL,
            started_at TEXT DEFAULT (datetime('now')),
            table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (buffer_filename, table_name)
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
@patch("backend.core.iceberg.write_to_buffer")
def test_ingest_rum_logs_success(
    mock_write_buffer,
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

    # Generate sample RUM log line matching standard Fastly fields (fallback query param format)
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
                {"Key": "raw_rum/rum_log_0.json.gz", "Size": len(gzipped_content)},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    # Mock s3 get_object response
    import io

    mock_response = {"Body": io.BytesIO(gzipped_content)}
    mock_s3.get_object.return_value = mock_response

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    assert events == [
        ("started", 123),
        ("file_done", "rum_log_0.json.gz", 1),
        ("done", 1),
    ]

    # Verify write_to_buffer was called with table_name="client_vitals"
    mock_write_buffer.assert_called_once()
    assert mock_write_buffer.call_args[1]["table_name"] == "client_vitals"

    # Verify that file key was successfully registered to ingested_files for both tables
    ingested = mock_metadata_db.execute("SELECT * FROM ingested_files").fetchall()
    assert len(ingested) == 2
    table_names = {row["table_name"] for row in ingested}
    assert table_names == {"client_vitals", "client_errors"}

    vitals_row = [r for r in ingested if r["table_name"] == "client_vitals"][0]
    errors_row = [r for r in ingested if r["table_name"] == "client_errors"][0]
    assert vitals_row["row_count"] == 1
    assert errors_row["row_count"] == 0


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
@patch("backend.core.iceberg.write_to_buffer")
def test_ingest_rum_logs_faro_dual_fan_out(
    mock_write_buffer,
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
):
    """Test dual-event fan-out under Faro format (vitals & exception in the same beacon payload)."""
    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
    }

    # Generate Faro format log with both vitals and exceptions
    faro_payload = {
        "measurements": [
            {
                "type": "web-vitals",
                "values": {"FID": 45.0},
                "meta": {"rating": "good"},
            }
        ],
        "exceptions": [
            {
                "value": "Uncaught ReferenceError: x is not defined",
                "stacktrace": {"frames": [{"filename": "bundle.js", "lineno": 100, "colno": 2}]},
            }
        ],
    }
    raw_log = {
        "rum_body": json.dumps(faro_payload),
        "fastly_req_id": "req_faro_dual",
        "rum_cid": "faro_sess",
        "timestamp": "2026-08-07T03:07:47+00:00",
    }
    gzipped_content = gzip.compress(json.dumps(raw_log).encode("utf-8"))

    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_rum/rum_log_1.json.gz", "Size": len(gzipped_content)},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    # Mock s3 get_object response
    import io

    mock_response = {"Body": io.BytesIO(gzipped_content)}
    mock_s3.get_object.return_value = mock_response

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    # Single beacon line contained both a vital and an error -> fanned out into 2 rows total
    assert events == [
        ("started", 123),
        ("file_done", "rum_log_1.json.gz", 2),
        ("done", 2),
    ]

    # Verify write_to_buffer was called twice (once for vitals, once for errors)
    assert mock_write_buffer.call_count == 2
    written_tables = {call.kwargs["table_name"] for call in mock_write_buffer.call_args_list}
    assert written_tables == {"client_vitals", "client_errors"}

    # Verify that file key was successfully registered to ingested_files for both tables
    ingested = mock_metadata_db.execute("SELECT * FROM ingested_files").fetchall()
    assert len(ingested) == 2
    vitals_row = [r for r in ingested if r["table_name"] == "client_vitals"][0]
    errors_row = [r for r in ingested if r["table_name"] == "client_errors"][0]
    assert vitals_row["row_count"] == 1
    assert errors_row["row_count"] == 1
