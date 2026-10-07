from __future__ import annotations

import gzip
import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from backend.core import quarantine
from backend.core.metadata.quarantine import list_quarantine_evidence
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


@pytest.mark.parametrize(
    "source",
    [None, {"name": "test_service_missing_bucket", "service_id": "test_service_missing_bucket", "bucket": ""}],
)
def test_ingest_rum_logs_skips_when_source_or_bucket_is_missing(source):
    service_id = "test_service_missing_bucket"
    with (
        patch("backend.core.rum_ingest.get_source_for_service", return_value=source),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123),
        patch("backend.core.rum_ingest.log_cron_run") as log_run,
        patch("backend.core.rum_ingest._get_fos_client") as get_fos,
    ):
        events = list(ingest_rum_logs(service_id))

    assert events == [("started", 123), ("done", 0)]
    log_run.assert_called_once()
    assert log_run.call_args.args[3] == "success"
    assert log_run.call_args.kwargs["files_downloaded"] == 0
    assert log_run.call_args.kwargs["rows_ingested"] == 0
    get_fos.assert_not_called()


def test_ingest_rum_logs_records_discovery_failure_and_finalizes_run():
    service_id = "test_service_list_failure"
    source = {"name": service_id, "service_id": service_id, "bucket": "test-bucket"}
    with (
        patch("backend.core.rum_ingest.get_source_for_service", return_value=source),
        patch("backend.core.rum_ingest._get_fos_client", return_value=MagicMock()),
        patch("backend.core.ingest._recover_in_flight"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest.list_fos_files", side_effect=RuntimeError("listing unavailable")),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123),
        patch("backend.core.rum_ingest.log_cron_run") as log_run,
        patch("backend.core.rum_ingest.finalize_cron_run_if_running") as finalize,
    ):
        events = list(ingest_rum_logs(service_id))

    assert events == [
        ("started", 123),
        ("error", "sync", "listing unavailable"),
    ]
    log_run.assert_called_once()
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["files_downloaded"] == 0
    assert log_run.call_args.kwargs["error_message"] == "listing unavailable"
    finalize.assert_called_once_with(service_id, "rum_sync", 123)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
@patch("backend.core.iceberg.write_to_buffer")
@pytest.mark.parametrize("capture_failure", [False, True])
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
    capture_failure,
):
    """Valid rows survive malformed neighbors, which are retained as exact-byte evidence."""
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
    malformed_line = b'{"timestamp":\n'
    gzipped_content = gzip.compress(json.dumps(raw_log).encode("utf-8") + b"\n" + malformed_line)

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
    import io

    mock_response = {"Body": io.BytesIO(gzipped_content)}
    mock_s3.get_object.return_value = mock_response

    # Execute ingest generator
    if capture_failure:
        with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("evidence unavailable")):
            events = list(ingest_rum_logs(service_id))
        capture = None
    else:
        with patch("backend.core.quarantine.capture_evidence", return_value={"id": 1, "cap_evictions": 0}) as capture:
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
    expected_outcomes = {
        "valid_records": 1,
        "malformed_records": 1,
        "corrupt_containers": 0,
        "quarantine_capture_failures": int(capture_failure),
        "source_delete_failures": 0,
        "cap_evictions": 0,
        "objects_processed": 1,
        "objects_successful": 0,
        "objects_partial": int(not capture_failure),
        "objects_failed": int(capture_failure),
    }
    assert mock_log.call_args.kwargs["outcome_counters"] == expected_outcomes
    assert mock_log.call_args.args[3] == "error"
    if capture_failure:
        assert capture is None
    else:
        capture.assert_called_once()
        assert capture.call_args.args[3] == malformed_line


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
@patch("backend.core.iceberg.write_to_buffer")
def test_ingest_rum_logs_preserves_query_metrics_and_exception_context(
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
    import io

    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {"name": service_id, "service_id": service_id, "bucket": "test-bucket"}

    rows = [
        {
            "timestamp": "2026-08-07T03:07:47+00:00",
            "url": "https://example.com/rum?rum_metric_name=LCP&rum_metric_value=1234.5"
            "&rum_metric_rating=good&cid=client-1&rum_pathname=%2Fcheckout",
            "fastly_req_id": "request-vital",
        },
        {
            "timestamp": "2026-08-07T03:07:48+00:00",
            "referer": "https://example.com/deep/path?ignored=1",
            "rum_error_message": "Uncaught ReferenceError",
            "rum_error_file": "checkout.js",
            "rum_error_line": "7",
            "fastly_req_id": "request-error",
        },
    ]
    payload = gzip.compress(b"\n".join(json.dumps(row).encode() for row in rows) + b"\n")
    fos = mock_get_fos.return_value
    fos.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "raw/rum/query-and-error.json.gz", "Size": len(payload)}]}
    ]
    fos.get_object.return_value = {"Body": io.BytesIO(payload)}

    events = list(ingest_rum_logs(service_id))

    assert events == [
        ("started", 123),
        ("file_done", "query-and-error.json.gz", 2),
        ("done", 2),
    ]
    written = {call.kwargs["table_name"]: call.args[1].to_pylist() for call in mock_write_buffer.call_args_list}
    assert len(written["client_vitals"]) == len(written["client_errors"]) == 1
    vital = written["client_vitals"][0]
    assert (vital["metric_name"], vital["metric_value"], vital["metric_rating"]) == ("LCP", 1234.5, "good")
    assert (vital["cid"], vital["pathname"], vital["req_id"]) == ("client-1", "/checkout", "request-vital")
    error = written["client_errors"][0]
    assert (error["error_message"], error["error_file"], error["error_line"]) == (
        "Uncaught ReferenceError",
        "checkout.js",
        7,
    )
    assert (error["pathname"], error["req_id"]) == ("/deep/path", "request-error")
    assert mock_log.call_args.args[3] == "success"
    assert mock_log.call_args.kwargs["outcome_counters"]["valid_records"] == 2
    mock_finalize.assert_called_once_with(service_id, "rum_sync", 123)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
def test_ingest_rum_logs_retains_whole_corrupt_container_and_fails_its_run(
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
    monkeypatch,
    tmp_path,
):
    import io

    service_id = "rum-corrupt-gzip"
    broken_gzip = b"\x1f\x8bnot a valid gzip stream"
    monkeypatch.setattr("backend.config.SERVICES_DATA_DIR", tmp_path)
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 124
    mock_get_source.return_value = {"name": service_id, "service_id": service_id, "bucket": "test-bucket"}
    fos = mock_get_fos.return_value
    fos.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "raw/rum/corrupt.json.gz", "Size": len(broken_gzip)}]}
    ]
    fos.get_object.return_value = {"Body": io.BytesIO(broken_gzip)}

    events = list(ingest_rum_logs(service_id))

    assert events[0] == ("started", 124)
    assert events[1][:2] == ("error", "s3://test-bucket/raw/rum/corrupt.json.gz")
    assert events[-1] == ("done", 0)
    assert mock_log.call_args.args[3] == "error"
    assert mock_log.call_args.kwargs["outcome_counters"]["corrupt_containers"] == 1
    assert mock_log.call_args.kwargs["outcome_counters"]["objects_failed"] == 1
    evidence = list_quarantine_evidence(service_id)
    assert len(evidence) == 1
    item, evidence_file = quarantine.read_evidence(service_id, evidence[0]["id"])
    with evidence_file:
        assert evidence_file.read() == broken_gzip
    assert item["original_key"] == "raw/rum/corrupt.json.gz"
    mock_finalize.assert_called_once_with(service_id, "rum_sync", 124)


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
                {"Key": "raw/rum/rum_log_1.json.gz", "Size": len(gzipped_content)},
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


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
def test_ingest_rum_logs_list_fos_files_error_marks_run_error(
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
):
    """Gap 4: A LIST error from list_fos_files marks the RUM run error, not success with 0 files."""
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

    def fake_list_gen(**kwargs):
        yield {"type": "error", "message": "FOS list failed: Access Denied"}
        return {
            "new_files": [],
            "file_sizes": {},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.core.ingest.list_fos_files", side_effect=fake_list_gen):
        events = list(ingest_rum_logs(service_id))

    # Must yield error event, not done 0
    assert any(evt[0] == "error" for evt in events), f"Expected error event, got {events}"

    # log_cron_run must be called with status='error'
    assert mock_log.called
    call_args = mock_log.call_args[0]
    call_kwargs = mock_log.call_args[1]
    status = call_args[3] if len(call_args) > 3 else call_kwargs.get("status")
    assert status == "error", f"Expected cron run status 'error', got {status}"


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
def test_ingest_rum_logs_incremental_idle_tick_bounds_list_calls(
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
):
    """Gap 1: Incremental discovery on an idle tick issues <= 5 LIST calls
    against a large mocked raw/rum/ prefix."""
    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
    }

    # Pre-populate ingested_files with existing v3 RUM files
    mock_metadata_db.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name) "
        "VALUES (?, ?, 10, 100, 'client_vitals')",
        ("s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/file1.json.gz", "test_service"),
    )
    mock_metadata_db.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name) "
        "VALUES (?, ?, 10, 100, 'client_errors')",
        ("s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/file1.json.gz", "test_service"),
    )
    mock_metadata_db.commit()

    list_calls = []
    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3
    mock_paginator = MagicMock()

    def fake_paginate(**kwargs):
        list_calls.append(kwargs)
        # If full raw/rum/ prefix is scanned, simulate a large bucket returning 10 pages
        if kwargs.get("Prefix") == "raw/rum/":
            return [
                {
                    "Contents": [
                        {
                            "Key": f"raw/rum/year=2026/month=10/day=07/hour=10/minute={i:02d}/old_{j}.json.gz",
                            "Size": 100,
                        }
                        for j in range(100)
                    ]
                }
                for i in range(10)
            ]
        # Minute prefix returns empty (idle tick)
        return [{"Contents": []}]

    mock_paginator.paginate.side_effect = fake_paginate
    mock_s3.get_paginator.return_value = mock_paginator

    events = list(ingest_rum_logs(service_id))
    assert events == [("started", 123), ("done", 0)]

    # The idle tick must issue <= 5 LIST calls (one per minute prefix),
    # never a full scan of raw/rum/
    assert len(list_calls) <= 5, f"Expected <= 5 LIST calls on idle tick, got {len(list_calls)}: {list_calls}"
    assert all("minute=" in call.get("Prefix", "") for call in list_calls), (
        f"All calls must be minute-scoped, got {list_calls}"
    )


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
@patch("backend.core.iceberg.write_to_buffer")
def test_ingest_rum_logs_time_budget_allows_first_chunk_trap_41(
    mock_write_buffer,
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
    monkeypatch,
):
    """Gap 3 & Trap #41: ingest_rum_logs respects max_seconds budget,
    but ALWAYS allows the first chunk to execute even if the budget is expired."""
    import io
    import time

    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
    }

    # Generate 100 fake files (2 chunks of 50)
    fake_files = [
        f"s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/f_{i}.json.gz" for i in range(100)
    ]
    valid_line = json.dumps({"timestamp": "2026-10-07T14:50:00Z", "rum_metric_name": "LCP", "rum_metric_value": "120"})
    gz_content = gzip.compress(valid_line.encode("utf-8"))

    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(gz_content)}

    def fake_list_gen(**kwargs):
        if False:
            yield {}
        return {
            "new_files": fake_files,
            "file_sizes": {f: len(gz_content) for f in fake_files},
            "skipped_already": 0,
            "stranded_already": [],
        }

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)

    # Time manipulation: start at 1000.0, and after chunk 0 completes, advance time to 2000.0 (past max_seconds=20)
    current_time = 1000.0

    def fake_time():
        nonlocal current_time
        return current_time

    monkeypatch.setattr(time, "time", fake_time)

    # Wrap _download_chunk_to_local to advance time after first chunk
    from backend.core import ingest as ingest_mod

    orig_download = ingest_mod._download_chunk_to_local

    download_count = 0

    def counting_download(s3_client, chunk, tmpdir):
        nonlocal current_time, download_count
        download_count += 1
        res = orig_download(s3_client, chunk, tmpdir)
        current_time += 100.0  # Expire the 20s budget
        return res

    monkeypatch.setattr(ingest_mod, "_download_chunk_to_local", counting_download)

    events = list(ingest_rum_logs(service_id, max_seconds=20))

    # Exactly 1 chunk must have downloaded and processed (50 files), not 2 chunks (100 files)
    assert download_count == 1, f"Expected exactly 1 chunk due to max_seconds bound, got {download_count}"


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.core.rum_ingest._get_fos_client")
@patch("backend.core.metadata.get_con")
@patch("backend.core.metadata.ingest_log.get_con")
@patch("backend.core.rum_ingest.start_cron_run")
@patch("backend.core.rum_ingest.log_cron_run")
@patch("backend.core.rum_ingest.finalize_cron_run_if_running")
@patch("backend.core.iceberg.write_to_buffer")
def test_ingest_rum_logs_deletes_raw_objects_inline_and_excludes_failures(
    mock_write_buffer,
    mock_finalize,
    mock_log,
    mock_start,
    mock_get_con_ingest,
    mock_get_con_metadata,
    mock_get_fos,
    mock_get_source,
    mock_metadata_db,
    monkeypatch,
):
    """Gap 2: ingest_rum_logs deletes raw objects inline after buffer write + insert_ingested_files,
    excluding unreadable files and files whose quarantine capture failed."""
    import io

    service_id = "test_service"
    mock_get_con_ingest.return_value = mock_metadata_db
    mock_get_con_metadata.return_value = mock_metadata_db
    mock_start.return_value = 123
    mock_get_source.return_value = {
        "name": "test_service",
        "service_id": service_id,
        "bucket": "test-bucket",
    }

    file_good = "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/good.json.gz"
    file_bad = "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/bad.json.gz"
    good_line = (
        json.dumps({"timestamp": "2026-10-07T14:50:00Z", "rum_metric_name": "LCP", "rum_metric_value": "120"}).encode()
        + b"\n"
    )
    bad_line = b'{"timestamp":\n'  # malformed JSON

    content_good = gzip.compress(good_line)
    content_bad = gzip.compress(bad_line)

    deleted_batches = []

    def fake_delete(fos_client, bucket, keys):
        deleted_batches.append(list(keys))
        return len(keys), set()

    monkeypatch.setattr("backend.core.ingest._delete_objects_robust_with_failures", fake_delete)

    def fake_list_gen(**kwargs):
        if False:
            yield {}
        return {
            "new_files": [file_good, file_bad],
            "file_sizes": {file_good: len(content_good), file_bad: len(content_bad)},
            "skipped_already": 0,
            "stranded_already": [],
        }

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)

    mock_s3 = MagicMock()
    mock_get_fos.return_value = mock_s3

    def fake_get_object(**kwargs):
        key = kwargs.get("Key")
        if "good.json.gz" in key:
            return {"Body": io.BytesIO(content_good)}
        return {"Body": io.BytesIO(content_bad)}

    mock_s3.get_object.side_effect = fake_get_object

    # Simulate quarantine capture failure for bad.json.gz
    with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("quarantine unavailable")):
        events = list(ingest_rum_logs(service_id))

    # The good file's raw object MUST have been deleted inline
    all_deleted_keys = [k for batch in deleted_batches for k in batch]
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/good.json.gz" in all_deleted_keys
    # The bad file (quarantine capture failure) MUST be excluded from deletion
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/bad.json.gz" not in all_deleted_keys
