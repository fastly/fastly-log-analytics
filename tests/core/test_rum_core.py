"""RUM ledger pipeline (celery-mode) — the RUM counterpart of
tests/core/test_step4_sweeper.py's regular-log ledger coverage.

Mirrors that file's rigor: real DuckDB connections with a real file-backed
DuckLake attach (tmp_path, not :memory:) and a real SQLite ledger (via
get_con), mocking only genuinely external things (the FOS/S3 client,
celery's .delay()).
"""

from __future__ import annotations

import gzip
import io
import json
import re
import shutil
import sqlite3
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core import quarantine
from backend.core.fastly.rum_provisioning import (
    RUM_ASSET_FETCH_NAME,
    RUM_DELIVER_SET_COOKIE_NAME,
    RUM_FARO_FETCH_NAME,
    RUM_RECV_NAME,
    RUM_SIGV4_SIGN_NAME,
    generate_rum_asset_fetch_vcl,
    generate_rum_vcl,
    validate_rum_custom_condition,
)
from backend.core.ingest import (
    LEDGER_RECLAIM_AFTER_S,
    _parse_rum_beacon_file,
    _parse_rum_line,
    _quarantine_rum_corrupt_lines,
    convert_batch_rum_objects,
    discover_rum_prefix,
    sweep_rum_ledger_once,
)
from backend.core.metadata.base import get_con
from backend.core.metadata.quarantine import list_quarantine_evidence
from backend.core.quarantine import read_evidence
from backend.core.rum_ingest import ingest_rum_logs
from backend.utils.vcl_validator import lint_vcl
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    attacher,
    clear_ledger,
    download_stub,
    lake_reader,
    ledger_row,
    seed_discovered,
)

_attacher = attacher
_clear_ledger = clear_ledger
_download_stub = download_stub
_lake_reader = lake_reader


# ── Merged from tests/core/test_rum_ingest.py ──
@pytest.fixture
def mock_metadata_db(tmp_path):
    """Create a temporary SQLite metadata DB for testing."""
    con = sqlite3.connect(str(tmp_path / "test_service.metadata.db"))
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            file_name TEXT, source_name TEXT, ingested_at TEXT DEFAULT (datetime('now')),
            row_count INTEGER, file_size_bytes INTEGER, error_count INTEGER DEFAULT 0,
            file_date DATE, table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (file_name, source_name, table_name)
        );
        CREATE TABLE IF NOT EXISTS ingest_in_flight (
            buffer_filename TEXT NOT NULL, source_name TEXT NOT NULL, files_json TEXT NOT NULL,
            started_at TEXT DEFAULT (datetime('now')), table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (buffer_filename, table_name)
        );
        CREATE TABLE IF NOT EXISTS ingested_files_summary (
            source_name TEXT PRIMARY KEY, file_count INTEGER NOT NULL DEFAULT 0,
            total_rows INTEGER NOT NULL DEFAULT 0, total_bytes INTEGER NOT NULL DEFAULT 0,
            count_with_bytes INTEGER NOT NULL DEFAULT 0, latest_file_name TEXT, last_ingested TEXT
        );
        CREATE TABLE IF NOT EXISTS cron_runs (
            id INTEGER PRIMARY KEY, service_id TEXT, job_name TEXT, duration_seconds REAL,
            status TEXT, rows_ingested INTEGER, error_message TEXT, started_at TEXT
        );
    """)
    yield con
    con.close()


@pytest.fixture
def rum_ingest_env(mock_metadata_db):
    """Standard patch harness for ingest_rum_logs tests."""
    with (
        patch("backend.core.rum_ingest.get_source_for_service") as mock_get_source,
        patch("backend.core.rum_ingest._get_fos_client") as mock_get_fos,
        patch("backend.core.metadata.get_con", return_value=mock_metadata_db),
        patch("backend.core.metadata.ingest_log.get_con", return_value=mock_metadata_db),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123) as mock_start,
        patch("backend.core.rum_ingest.log_cron_run") as mock_log,
        patch("backend.core.rum_ingest.finalize_cron_run_if_running") as mock_finalize,
        patch("backend.core.iceberg.write_to_buffer") as mock_write_buffer,
    ):
        mock_s3 = MagicMock()
        mock_get_fos.return_value = mock_s3
        mock_get_source.return_value = {
            "name": "test_service",
            "service_id": "test_service",
            "bucket": "test-bucket",
        }

        def stage_single_object(key: str, content: bytes) -> None:
            mock_s3.get_paginator.return_value.paginate.return_value = [
                {"Contents": [{"Key": key, "Size": len(content)}]}
            ]
            mock_s3.get_object.return_value = {"Body": io.BytesIO(content)}

        yield SimpleNamespace(
            db=mock_metadata_db,
            s3=mock_s3,
            get_source=mock_get_source,
            get_fos=mock_get_fos,
            start=mock_start,
            log=mock_log,
            finalize=mock_finalize,
            write_buffer=mock_write_buffer,
            stage_single_object=stage_single_object,
        )


def test_ingest_rum_logs_empty_bucket(rum_ingest_env):
    """Test RUM ingest behavior when the bucket contains no new RUM logs."""
    service_id = "test_service"
    rum_ingest_env.get_source.return_value["prefix"] = "test-prefix"
    rum_ingest_env.s3.get_paginator.return_value.paginate.return_value = [{}]

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    assert events == [("started", 123), ("done", 0)]
    rum_ingest_env.log.assert_called_once()
    assert rum_ingest_env.log.call_args[0][3] == "success"  # Status is success (no logs found is not an error)
    assert rum_ingest_env.log.call_args[1]["rows_ingested"] == 0
    rum_ingest_env.finalize.assert_called_once_with(service_id, "rum_sync", 123)


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


@pytest.mark.parametrize("capture_failure", [False, True])
def test_ingest_rum_logs_success(rum_ingest_env, capture_failure):
    """Valid rows survive malformed neighbors, which are retained as exact-byte evidence."""
    service_id = "test_service"

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
    rum_ingest_env.stage_single_object("raw/rum/rum_log_0.json.gz", gzipped_content)

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
    rum_ingest_env.write_buffer.assert_called_once()
    assert rum_ingest_env.write_buffer.call_args[1]["table_name"] == "client_vitals"

    # Verify that file key was successfully registered to ingested_files for both tables
    ingested = rum_ingest_env.db.execute("SELECT * FROM ingested_files").fetchall()
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
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"] == expected_outcomes
    assert rum_ingest_env.log.call_args.args[3] == "error"
    if capture_failure:
        assert capture is None
    else:
        capture.assert_called_once()
        assert capture.call_args.args[3] == malformed_line


def test_ingest_rum_logs_preserves_query_metrics_and_exception_context(rum_ingest_env):
    service_id = "test_service"
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
    rum_ingest_env.stage_single_object("raw/rum/query-and-error.json.gz", payload)

    events = list(ingest_rum_logs(service_id))

    assert events == [
        ("started", 123),
        ("file_done", "query-and-error.json.gz", 2),
        ("done", 2),
    ]
    written = {
        call.kwargs["table_name"]: call.args[1].to_pylist() for call in rum_ingest_env.write_buffer.call_args_list
    }
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
    assert rum_ingest_env.log.call_args.args[3] == "success"
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"]["valid_records"] == 2
    rum_ingest_env.finalize.assert_called_once_with(service_id, "rum_sync", 123)


def test_ingest_rum_logs_retains_whole_corrupt_container_and_fails_its_run(
    rum_ingest_env,
    monkeypatch,
    tmp_path,
):
    service_id = "rum-corrupt-gzip"
    broken_gzip = b"\x1f\x8bnot a valid gzip stream"
    monkeypatch.setattr("backend.config.SERVICES_DATA_DIR", tmp_path)
    rum_ingest_env.start.return_value = 124
    rum_ingest_env.get_source.return_value = {"name": service_id, "service_id": service_id, "bucket": "test-bucket"}
    rum_ingest_env.stage_single_object("raw/rum/corrupt.json.gz", broken_gzip)

    events = list(ingest_rum_logs(service_id))

    assert events[0] == ("started", 124)
    assert events[1][:2] == ("error", "s3://test-bucket/raw/rum/corrupt.json.gz")
    assert events[-1] == ("done", 0)
    assert rum_ingest_env.log.call_args.args[3] == "error"
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"]["corrupt_containers"] == 1
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"]["objects_failed"] == 1
    evidence = list_quarantine_evidence(service_id)
    assert len(evidence) == 1
    item, evidence_file = quarantine.read_evidence(service_id, evidence[0]["id"])
    with evidence_file:
        assert evidence_file.read() == broken_gzip
    assert item["original_key"] == "raw/rum/corrupt.json.gz"
    rum_ingest_env.finalize.assert_called_once_with(service_id, "rum_sync", 124)


def test_ingest_rum_logs_faro_dual_fan_out(rum_ingest_env):
    """Test dual-event fan-out under Faro format (vitals & exception in the same beacon payload)."""
    service_id = "test_service"

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
    rum_ingest_env.stage_single_object("raw/rum/rum_log_1.json.gz", gzipped_content)

    # Execute ingest generator
    events = list(ingest_rum_logs(service_id))

    # Single beacon line contained both a vital and an error -> fanned out into 2 rows total
    assert events == [
        ("started", 123),
        ("file_done", "rum_log_1.json.gz", 2),
        ("done", 2),
    ]

    # Verify write_to_buffer was called twice (once for vitals, once for errors)
    assert rum_ingest_env.write_buffer.call_count == 2
    written_tables = {call.kwargs["table_name"] for call in rum_ingest_env.write_buffer.call_args_list}
    assert written_tables == {"client_vitals", "client_errors"}

    # Verify that file key was successfully registered to ingested_files for both tables
    ingested = rum_ingest_env.db.execute("SELECT * FROM ingested_files").fetchall()
    assert len(ingested) == 2
    vitals_row = [r for r in ingested if r["table_name"] == "client_vitals"][0]
    errors_row = [r for r in ingested if r["table_name"] == "client_errors"][0]
    assert vitals_row["row_count"] == 1
    assert errors_row["row_count"] == 1


def test_ingest_rum_logs_list_fos_files_error_marks_run_error(rum_ingest_env):
    """Gap 4: A LIST error from list_fos_files marks the RUM run error, not success with 0 files."""
    service_id = "test_service"
    rum_ingest_env.get_source.return_value["prefix"] = "test-prefix"

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
    assert rum_ingest_env.log.called
    call_args = rum_ingest_env.log.call_args[0]
    call_kwargs = rum_ingest_env.log.call_args[1]
    status = call_args[3] if len(call_args) > 3 else call_kwargs.get("status")
    assert status == "error", f"Expected cron run status 'error', got {status}"


def test_ingest_rum_logs_incremental_idle_tick_bounds_list_calls(rum_ingest_env):
    """Gap 1: Incremental discovery on an idle tick issues <= 5 LIST calls
    against a large mocked raw/rum/ prefix."""
    service_id = "test_service"

    # Pre-populate ingested_files with existing v3 RUM files
    for table_name in ("client_vitals", "client_errors"):
        rum_ingest_env.db.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name) "
            "VALUES (?, ?, 10, 100, ?)",
            (
                "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/file1.json.gz",
                "test_service",
                table_name,
            ),
        )
    rum_ingest_env.db.commit()

    list_calls = []

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

    rum_ingest_env.s3.get_paginator.return_value.paginate.side_effect = fake_paginate

    events = list(ingest_rum_logs(service_id))
    assert events == [("started", 123), ("done", 0)]

    # The idle tick must issue <= 5 LIST calls (one per minute prefix),
    # never a full scan of raw/rum/
    assert len(list_calls) <= 5, f"Expected <= 5 LIST calls on idle tick, got {len(list_calls)}: {list_calls}"
    assert all("minute=" in call.get("Prefix", "") for call in list_calls), (
        f"All calls must be minute-scoped, got {list_calls}"
    )


def test_ingest_rum_logs_time_budget_allows_first_chunk_trap_41(rum_ingest_env, monkeypatch):
    """Gap 3 & Trap #41: ingest_rum_logs respects max_seconds budget,
    but ALWAYS allows the first chunk to execute even if the budget is expired."""
    service_id = "test_service"

    # Generate 100 fake files (2 chunks of 50)
    fake_files = [
        f"s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/f_{i}.json.gz" for i in range(100)
    ]
    valid_line = json.dumps({"timestamp": "2026-10-07T14:50:00Z", "rum_metric_name": "LCP", "rum_metric_value": "120"})
    gz_content = gzip.compress(valid_line.encode("utf-8"))
    rum_ingest_env.s3.get_object.return_value = {"Body": io.BytesIO(gz_content)}

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

    list(ingest_rum_logs(service_id, max_seconds=20))

    # Exactly 1 chunk must have downloaded and processed (50 files), not 2 chunks (100 files)
    assert download_count == 1, f"Expected exactly 1 chunk due to max_seconds bound, got {download_count}"


def test_ingest_rum_logs_deletes_raw_objects_inline_and_excludes_failures(rum_ingest_env, monkeypatch):
    """Gap 2: ingest_rum_logs deletes raw objects inline after buffer write + insert_ingested_files,
    excluding unreadable files and files whose quarantine capture failed."""
    service_id = "test_service"

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

    def fake_get_object(**kwargs):
        key = kwargs.get("Key")
        if "good.json.gz" in key:
            return {"Body": io.BytesIO(content_good)}
        return {"Body": io.BytesIO(content_bad)}

    rum_ingest_env.s3.get_object.side_effect = fake_get_object

    # Simulate quarantine capture failure for bad.json.gz
    with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("quarantine unavailable")):
        list(ingest_rum_logs(service_id))

    # The good file's raw object MUST have been deleted inline
    all_deleted_keys = [k for batch in deleted_batches for k in batch]
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/good.json.gz" in all_deleted_keys
    # The bad file (quarantine capture failure) MUST be excluded from deletion
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/bad.json.gz" not in all_deleted_keys


# ── Merged from tests/core/test_rum_ledger.py ──
def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def test_discover_rum_prefix_defaults_to_rum_raw_and_inserts_ledger_rows():
    """discover_rum_prefix must LIST raw/rum/ (never plain raw/request/) and dispatch
    convert_batch_rum_files for every newly discovered file."""
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"

    con, cur = _clear_ledger(service_id)

    captured_kwargs = {}

    def mock_list_fos_files(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 999},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                with patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_convert_rum_delay:
                    discovered = discover_rum_prefix(service_id, run_id=813)
                    mock_convert_rum_delay.assert_called_once_with(service_id, [object_key])

    assert discovered == 1
    assert captured_kwargs["prefix_subpath"] == "raw/rum/"

    row = cur.execute(
        "SELECT status, size_bytes, originating_task, originating_run_id FROM ingest_ledger "
        "WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["size_bytes"] == 999
    assert row["originating_task"] == "rum_discovery"
    assert row["originating_run_id"] == 813


def test_discover_rum_prefix_honors_explicit_minute_subpath():
    """The celery-mode discovery job dispatches discover_rum_prefix once per
    minute-scoped subpath (rum_minute_list_prefix) — confirm it's threaded
    through to list_fos_files unchanged."""
    service_id = "test-celery-rum-svc"
    _clear_ledger(service_id)

    captured_kwargs = {}

    def mock_list_fos_files(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    minute_prefix = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/"
    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                discover_rum_prefix(service_id, prefix_subpath=minute_prefix)

    assert captured_kwargs["prefix_subpath"] == minute_prefix


def test_rum_ledger_sweep_persists_its_originating_run():
    service_id = "test-celery-rum-sweep-origin"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=06/beacons.json.gz"
    con, _ = _clear_ledger(service_id)

    def list_one_file(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 777},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=list_one_file):
                with patch("backend.core.ingest.convert_batch_rum_files.delay"):
                    result = sweep_rum_ledger_once(service_id, run_id=91)

    assert result["discovered"] == 1
    row = con.execute(
        "SELECT originating_task, originating_run_id FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["originating_task"] == "ledger_rum_sweep"
    assert row["originating_run_id"] == 91


def test_parse_rum_beacon_file_splits_vitals_and_errors_and_quarantines_bad_line(tmp_path):
    """One beacon file: a flat rum_* vitals line, a flat rum_error_* line,
    and a truncated/unparseable line. The bad line must be reported back to
    the caller for quarantine, not silently dropped."""
    service_id = "svc-parse"
    vitals_line = json.dumps(
        {
            "timestamp": "2026-08-27T10:05:00Z",
            "rum_metric_name": "LCP",
            "rum_metric_value": "1234.5",
            "rum_metric_rating": "good",
            "browser": "Chrome",
            "pop": "SEA",
        }
    )
    error_line = json.dumps(
        {
            "timestamp": "2026-08-27T10:05:01Z",
            "rum_error_message": "TypeError: boom",
            "rum_error_file": "app.js",
            "rum_error_line": "42",
        }
    )
    bad_line = '{"timestamp": "2026-08-27T10:05:02Z", "rum_metric_name": "CLS"'  # truncated

    raw_file = tmp_path / "beacons.json"
    with gzip.open(raw_file, "wt", encoding="utf-8") as f:
        f.write(vitals_line + "\n" + error_line + "\n" + bad_line + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(str(raw_file), service_id)

    assert len(vitals_rows) == 1
    assert vitals_rows[0]["metric_name"] == "LCP"
    assert vitals_rows[0]["metric_value"] == 1234.5
    assert vitals_rows[0]["pop"] == "SEA"

    assert len(errors_rows) == 1
    assert errors_rows[0]["error_message"] == "TypeError: boom"
    assert errors_rows[0]["error_line"] == 42

    assert len(corrupt_lines) == 1
    assert corrupt_lines[0][0] == bad_line
    assert corrupt_lines[0][1] == "invalid_json"


def test_parse_rum_beacon_file_skips_rows_for_other_services():
    """A multi-tenant bucket line tagged for a different service is skipped
    silently — not treated as corruption."""
    service_id = "svc-mine"
    other_line = json.dumps({"timestamp": "2026-08-27T10:00:00Z", "service_id": "svc-other", "rum_metric_name": "LCP"})
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(other_line + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, service_id)
    assert vitals_rows == []
    assert errors_rows == []
    assert corrupt_lines == []


def _write_gz_beacon(path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _run_convert_rum(service_id, object_key, local_file, tmp_path):
    """Shared patch scaffold for convert_batch_rum_objects using DuckLakeLedgerEnv."""
    s3_path = f"s3://test-bucket/{object_key}"
    with DuckLakeLedgerEnv(
        service_id,
        tmp_path,
        download_stub({s3_path: str(local_file)}),
        bucket="test-bucket",
        table_name=None,
    ) as attach:
        summary = convert_batch_rum_objects(service_id, [object_key], "test-worker")
    return summary, attach


def test_convert_rum_object_splits_into_both_tables_and_commits(tmp_path):
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"
    _clear_ledger(service_id)
    con = get_con(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps(
        {"timestamp": "2026-08-27T10:05:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1000"}
    )
    error_line = json.dumps({"timestamp": "2026-08-27T10:05:01Z", "rum_error_message": "boom"})
    raw_file = tmp_path / "beacons.json"
    _write_gz_beacon(raw_file, [vitals_line, error_line])

    summary, attach = _run_convert_rum(service_id, object_key, raw_file, tmp_path)

    assert summary["committed"] == 1

    check_con = lake_reader(attach)
    try:
        vitals_count = check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0]
        errors_count = check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0]
        assert vitals_count == 1
        assert errors_count == 1

        src_file = check_con.execute("SELECT _source_file FROM lake.client_vitals").fetchone()[0]
        assert src_file == f"s3://test-bucket/{object_key}"
    finally:
        check_con.close()

    row = con.execute(
        "SELECT status, committed_at FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "committed"
    assert row["committed_at"] is not None


def test_convert_rum_object_idempotent_under_redelivery(tmp_path):
    """Redelivering the same file (crash-after-commit, sweeper reclaim, or
    a plain retry) must not duplicate rows in either table."""
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=06/redeliver.json.gz"
    con, _ = _clear_ledger(service_id)

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:06:00Z", "rum_metric_name": "CLS", "rum_metric_value": "0.1"})
    error_line = json.dumps({"timestamp": "2026-08-27T10:06:01Z", "rum_error_message": "kaboom"})
    raw_file = tmp_path / "redeliver.json"
    _write_gz_beacon(raw_file, [vitals_line, error_line])

    attach = None
    for _ in range(2):
        con.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) "
            "VALUES (?, ?, 'discovered', ?) ON CONFLICT (service_id, object_key) DO UPDATE SET status='discovered'",
            (service_id, object_key, time.time()),
        )
        con.commit()
        summary, attach = _run_convert_rum(service_id, object_key, raw_file, tmp_path)
        assert summary["committed"] == 1

    check_con = lake_reader(attach)
    try:
        assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
        assert check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1
    finally:
        check_con.close()


def test_convert_rum_object_quarantines_malformed_line_and_still_commits_valid_rows(tmp_path):
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=07/mixed.json.gz"
    con, _ = _clear_ledger(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:07:00Z", "rum_metric_name": "FID", "rum_metric_value": "5"})
    bad_line = '{"timestamp": "2026-08-27T10:07:01Z", "rum_metric_name": "broken"'  # truncated
    raw_file = tmp_path / "mixed.json"
    _write_gz_beacon(raw_file, [vitals_line, bad_line])

    fos_mock = MagicMock()
    s3_path = f"s3://test-bucket/{object_key}"

    with (
        DuckLakeLedgerEnv(
            service_id,
            tmp_path,
            download_stub({s3_path: str(raw_file)}),
            bucket="test-bucket",
            fos=fos_mock,
            table_name=None,
        ) as attach,
        patch(
            "backend.core.quarantine.capture_evidence",
            return_value={
                "id": 1,
                "cap_evictions": 0,
                "quarantine_capture_failures": 0,
            },
        ) as capture,
    ):
        summary = convert_batch_rum_objects(service_id, [object_key], "test-worker")

    assert summary["committed"] == 1

    check_con = lake_reader(attach)
    try:
        assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    finally:
        check_con.close()

    fos_mock.put_object.assert_not_called()
    capture.assert_called_once()
    assert capture.call_args.args == (
        service_id,
        "rum",
        object_key,
        (bad_line + "\n").encode(),
    )
    assert capture.call_args.kwargs["line_ordinal"] == 2
    assert capture.call_args.kwargs["byte_offset"] == len((vitals_line + "\n").encode())


def test_convert_rum_object_partial_write_failure_does_not_mark_committed(tmp_path):
    """If the errors-table write fails after vitals succeeded, the ledger
    row must NOT be committed — a partial dual-table write is not a
    success. A retry converges once the failure is fixed.

    Triggers the failure organically (an ``error_line`` value that overflows
    CLIENT_ERRORS_ARROW_SCHEMA's int32 column, raising pyarrow.ArrowInvalid
    inside ``pa.Table.from_pylist``) rather than mocking pyarrow — its
    ``Table`` type is a Cython extension class and its methods can't be
    monkeypatched via ``unittest.mock``."""
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=08/partial.json.gz"
    con, _ = _clear_ledger(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:08:00Z", "rum_metric_name": "TTFB", "rum_metric_value": "50"})
    broken_error_line = json.dumps(
        {"timestamp": "2026-08-27T10:08:01Z", "rum_error_message": "boom", "rum_error_line": "99999999999999"}
    )
    fixed_error_line = json.dumps(
        {"timestamp": "2026-08-27T10:08:01Z", "rum_error_message": "boom", "rum_error_line": "42"}
    )
    raw_file = tmp_path / "partial.json"
    _write_gz_beacon(raw_file, [vitals_line, broken_error_line])

    summary, attach = _run_convert_rum(service_id, object_key, raw_file, tmp_path)

    assert summary["failed"] == 1

    row = con.execute(
        "SELECT status, committed_at, attempts FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    check_con = lake_reader(attach)
    try:
        assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    finally:
        check_con.close()

    # Retry (the bad input is now "fixed") converges: vitals row isn't
    # duplicated, and errors now lands too.
    _write_gz_beacon(raw_file, [vitals_line, fixed_error_line])
    con.execute(
        "UPDATE ingest_ledger SET status='discovered' WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    con.commit()
    summary2, attach = _run_convert_rum(service_id, object_key, raw_file, tmp_path)
    assert summary2["committed"] == 1

    check_con = lake_reader(attach)
    try:
        assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
        assert check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1
    finally:
        check_con.close()


def test_sweep_rum_ledger_reclaims_stale_claim_and_redispatches_convert_rum():
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=09/stuck.json.gz"
    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, object_key, stale_claim, stale_claim - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_convert_rum_delay,
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_batch_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)
                    mock_convert_rum_delay.assert_called_once_with(service_id, [object_key])
                    mock_convert_batch_delay.assert_not_called()

    assert summary["reclaimed"] == 1
    row = cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["claimed_by"] is None


def test_sweep_rum_ledger_does_not_touch_regular_log_rows():
    """Scoping check: a stale regular-log row for the same service must be
    left alone by the RUM sweep (and vice versa is exercised by the
    existing regular sweep_ledger_once tests)."""
    service_id = "test-celery-rum-svc"
    con, cur = _clear_ledger(service_id)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    rum_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    regular_key = "raw/request/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'w1', ?, ?)",
        (service_id, rum_key, stale, stale - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'w1', ?, ?)",
        (service_id, regular_key, stale, stale - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_convert_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)
                    mock_convert_rum_delay.assert_called_once_with(service_id, [rum_key])

    assert summary["reclaimed"] == 1
    regular_row = cur.execute(
        "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, regular_key)
    ).fetchone()
    assert regular_row["status"] == "claimed", "the regular-log row must be untouched by the RUM sweep"


def test_sweep_rum_ledger_skips_redispatch_when_queue_holds_backlog():
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/queued.gz"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time() - 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 50}, True)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_convert_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)

    mock_convert_rum_delay.assert_not_called()
    assert summary["redispatched"] == 0


def test_sweep_rum_ledger_waits_for_retry_due_time():
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/retry-later.gz"
    con, cur = _clear_ledger(service_id)
    now = time.time()
    cur.execute(
        "INSERT INTO ingest_ledger "
        "(service_id, object_key, status, discovered_at, next_attempt_at) "
        "VALUES (?, ?, 'discovered', ?, ?)",
        (service_id, object_key, now - 3600, now + 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"bucket": "test-bucket"}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)

    assert summary["redispatched"] == 0
    mock_delay.assert_not_called()


# ── Merged from tests/core/test_rum_ledger_edges.py ──
SERVICE_ID = "test-rum-ledger-edges"

BUCKET = "test-bucket"


def _seed_discovered(con, object_key: str) -> None:
    seed_discovered(con, con.cursor(), SERVICE_ID, [object_key])


def _ledger_row(con, object_key: str):
    return ledger_row(con, SERVICE_ID, object_key)


def _write_gz(path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


@contextmanager
def _rum_env(tmp_path, download, fos=None, attach=None):
    """Patch scaffold for convert_batch_rum_objects with a real DuckLake attach."""
    with DuckLakeLedgerEnv(
        SERVICE_ID,
        tmp_path,
        download,
        bucket=BUCKET,
        fos=fos,
        attach=attach,
        table_name=None,
    ) as real_attach:
        yield real_attach


def _table_exists(con, name: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND table_name=? LIMIT 1", (name,)
        ).fetchone()
    )


VITALS_LINE = json.dumps({"timestamp": "2026-08-27T11:00:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1500"})

ERROR_LINE = json.dumps({"timestamp": "2026-08-27T11:00:01Z", "rum_error_message": "boom", "rum_error_line": "9"})


def test_convert_rum_object_returns_not_claimed_when_no_discovered_row_exists(tmp_path):
    """A redelivered message for an already-committed (or absent) key must
    no-op. Re-converting a committed key would re-insert rows for a raw file
    that finalize_committed_raw may already have deleted."""
    object_key = "raw/rum/2026/08/27/11/00/gone.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) "
        "VALUES (?, ?, 'committed', ?, ?)",
        (SERVICE_ID, object_key, time.time(), time.time()),
    )
    con.commit()

    raw = tmp_path / "gone.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach:
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["not_claimed"] == 1
        assert summary["committed"] == 0

    # Untouched: still committed, no attempt burned, and nothing written.
    row = _ledger_row(con, object_key)
    assert row["status"] == "committed"
    assert row["attempts"] == 0
    reader = _lake_reader(attach)
    assert not _table_exists(reader, "client_vitals")
    reader.close()


def test_convert_rum_object_uses_ducklake_detach_in_finally(tmp_path):
    """Trap #35: convert_batch_rum_objects must call _ducklake_detach in finally,
    never raw duckdb_con.execute('DETACH lake')."""
    object_key = "raw/rum/2026/08/27/11/02/detach_test.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    raw = tmp_path / "detach_test.json"
    _write_gz(raw, [VITALS_LINE])
    with (
        _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})),
        patch("backend.core.iceberg._ducklake._ducklake_detach") as mock_detach,
    ):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["committed"] == 1
        assert mock_detach.called, "convert_batch_rum_objects must call _ducklake_detach in finally"


def test_convert_rum_object_dead_letters_a_key_whose_object_is_gone(tmp_path):
    """A ledger row for an object no longer in FOS (already ingested and
    deleted, or expired) is terminal. Retrying it would burn
    LEDGER_MAX_ATTEMPTS GETs per dead key on every sweep."""
    object_key = "raw/rum/2026/08/27/11/03/vanished.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) when calling the HeadObject operation: Not Found")
    with _rum_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["dead_letter"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "dead_letter"
    # dead_letter is a decision, not a failure: no attempt is burned.
    assert row["attempts"] == 0
    assert "missing from FOS" in row["last_error"]
    fos.head_object.assert_called_once_with(Bucket=BUCKET, Key=object_key)


def test_convert_rum_object_treats_download_failure_on_a_live_object_as_transient(tmp_path):
    """The object still HEADs fine, so the download blip is transient: the
    row must stay retryable rather than be dead-lettered (which would
    permanently drop a file that is still sitting in the bucket)."""
    object_key = "raw/rum/2026/08/27/11/04/blip.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 123}
    with _rum_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "transient" in row["last_error"]


def test_convert_rum_object_commits_valid_rows_even_when_quarantine_upload_fails(tmp_path):
    """The valid rows are already parsed and about to be durably committed
    when quarantine runs. A FOS failure while uploading the sidecar must not
    fail the convert — otherwise one unwritable errors/ prefix blocks ingest
    for every file that contains a single malformed beacon."""
    object_key = "raw/rum/2026/08/27/11/05/quarantine-fails.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    raw = tmp_path / "qfail.json"
    _write_gz(raw, [VITALS_LINE, '{"timestamp": "2026-08-27T11:05:00Z", "rum_metric_name": "trunc'])

    fos = MagicMock()
    fos.put_object.side_effect = Exception("AccessDenied writing errors/ prefix")
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), fos=fos) as attach:
        with patch(
            "backend.core.quarantine.capture_evidence",
            return_value={"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1},
        ):
            summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    reader.close()
    counters = json.loads(
        con.execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        ).fetchone()[0]
    )
    assert counters["quarantine_capture_failures"] == 1
    assert counters["objects_failed"] == 1
    fos.put_object.assert_not_called()


def test_convert_rum_object_commits_when_ingested_files_bookkeeping_fails(tmp_path):
    """``ingested_files`` is a reader convenience; the ledger is
    authoritative. A bookkeeping failure must not roll back a committed
    convert."""
    object_key = "raw/rum/2026/08/27/11/06/bookkeeping.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    raw = tmp_path / "bk.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE])

    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    assert reader.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1
    reader.close()


def test_convert_rum_object_backfills_source_file_column_on_a_v2_created_table(tmp_path):
    """v2's RUM writer (``ingest_rum_logs``) creates ``client_vitals``
    WITHOUT ``_source_file``. This path shares the table, and its
    DELETE-by-``_source_file`` idempotency depends on that column existing —
    so it must be added rather than the insert failing."""
    object_key = "raw/rum/2026/08/27/11/07/widen.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    # Pre-create the table the way v2 would: full RUM schema, no _source_file.
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute(
        "CREATE TABLE lake.client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, "
        "pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, "
        "city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE)"
    )
    setup.execute(
        "INSERT INTO lake.client_vitals (timestamp, metric_name, metric_value) "
        "VALUES ('2026-08-01T00:00:00Z', 'v2-legacy-row', 1.0)"
    )
    setup.close()

    raw = tmp_path / "widen.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), attach=attach):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["committed"] == 1

    reader = _lake_reader(attach)
    rows = reader.execute("SELECT metric_name, _source_file FROM lake.client_vitals ORDER BY metric_name").fetchall()
    reader.close()
    # The legacy row survives with a NULL _source_file; the new row is
    # attributed and therefore idempotently re-deletable on redelivery.
    assert rows == [
        ("LCP", f"s3://{BUCKET}/{object_key}"),
        ("v2-legacy-row", None),
    ]


def test_convert_rum_object_rolls_back_and_stays_retryable_on_a_schema_drifted_table(tmp_path):
    """If the shared lake table has drifted so the INSERT cannot land, the
    transaction must roll back and the ledger row must stay retryable —
    never a half-written table plus a committed row."""
    object_key = "raw/rum/2026/08/27/11/08/drift.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    # A narrower table than the arrow schema: INSERT ... BY NAME cannot map
    # metric_value/pathname/... and raises inside the transaction.
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, _source_file VARCHAR)")
    setup.close()

    raw = tmp_path / "drift.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE])
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), attach=attach):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 0
    # The errors table is written second — it must not exist at all, since
    # the vitals write aborted the convert first.
    assert not _table_exists(reader, "client_errors")
    reader.close()


def test_convert_batch_rum_objects_commits_rows_from_each_claimed_object(tmp_path):
    object_keys = [
        "raw/rum/2026/08/27/11/00/batch-a.json.gz",
        "raw/rum/2026/08/27/11/01/batch-b.json.gz",
    ]
    con, _ = clear_ledger(SERVICE_ID, quarantine=True)
    local_files = {}
    for index, object_key in enumerate(object_keys):
        _seed_discovered(con, object_key)
        local_file = tmp_path / f"batch-{index}.json.gz"
        _write_gz(
            local_file,
            [
                json.dumps(
                    {
                        "timestamp": f"2026-08-27T11:0{index}:00Z",
                        "rum_metric_name": "LCP",
                        "rum_metric_value": str(1000 + index),
                    }
                ),
                json.dumps(
                    {
                        "timestamp": f"2026-08-27T11:0{index}:01Z",
                        "rum_error_message": f"error-{index}",
                    }
                ),
            ],
        )
        if index == 0:
            with gzip.open(local_file, "ab") as raw:
                raw.write(b'{"invalid":\x00}\n')
        local_files[f"s3://{BUCKET}/{object_key}"] = str(local_file)

    with _rum_env(tmp_path, _download_stub(local_files)) as attach:
        summary = convert_batch_rum_objects(SERVICE_ID, object_keys, "worker-batch-rum")

    assert summary == {
        "requested": 2,
        "claimed": 2,
        "not_claimed": 0,
        "committed": 2,
        "dead_letter": 0,
        "failed": 0,
    }
    evidence_rows = list_quarantine_evidence(SERVICE_ID)
    assert len(evidence_rows) == 1
    assert evidence_rows[0]["original_key"] == object_keys[0]
    _, evidence = read_evidence(SERVICE_ID, int(evidence_rows[0]["id"]))
    try:
        assert evidence.read() == b'{"invalid":\x00}\n'
    finally:
        evidence.close()
    object_outcome = con.execute(
        "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (SERVICE_ID, object_keys[0]),
    ).fetchone()
    assert json.loads(object_outcome["outcome_counters"])["malformed_records"] == 1
    reader = _lake_reader(attach)
    try:
        assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 2
        assert reader.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 2
        assert reader.execute("SELECT count(DISTINCT _source_file) FROM lake.client_vitals").fetchone()[0] == 2
    finally:
        reader.close()
    rows = con.execute(
        "SELECT object_key, status FROM ingest_ledger WHERE service_id=? ORDER BY object_key",
        (SERVICE_ID,),
    ).fetchall()
    assert [(row["object_key"], row["status"]) for row in rows] == [(key, "committed") for key in object_keys]

    con.execute(
        "UPDATE ingest_ledger SET status='discovered', claimed_by=NULL, claimed_at=NULL WHERE service_id=?",
        (SERVICE_ID,),
    )
    con.commit()
    with _rum_env(tmp_path, _download_stub(local_files)):
        assert convert_batch_rum_objects(SERVICE_ID, object_keys, "worker-rum-redelivery")["committed"] == 2
    reader = _lake_reader(attach)
    try:
        assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 2
        assert reader.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 2
    finally:
        reader.close()


def test_convert_batch_rum_objects_noops_for_empty_or_unclaimed_batches():
    con, _ = clear_ledger(SERVICE_ID)
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        assert convert_batch_rum_objects(SERVICE_ID, [], "worker-empty") == {
            "requested": 0,
            "claimed": 0,
            "not_claimed": 0,
            "committed": 0,
            "dead_letter": 0,
            "failed": 0,
        }
        assert convert_batch_rum_objects(SERVICE_ID, ["raw/rum/unknown.json.gz"], "worker-unknown") == {
            "requested": 1,
            "claimed": 0,
            "not_claimed": 1,
            "committed": 0,
            "dead_letter": 0,
            "failed": 0,
        }
    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 0


def test_convert_batch_rum_objects_keeps_claim_retryable_without_source():
    object_key = "raw/rum/2026/08/27/11/02/no-source.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "worker-no-source")

    assert summary["failed"] == 1
    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "no source registered" in row["last_error"]
    assert row["committed_at"] is None


def test_convert_batch_rum_objects_separates_deleted_and_transient_downloads(tmp_path):
    object_keys = [
        "raw/rum/2026/08/27/11/03/already-deleted.json.gz",
        "raw/rum/2026/08/27/11/04/still-live.json.gz",
    ]
    con, _ = clear_ledger(SERVICE_ID)
    for object_key in object_keys:
        _seed_discovered(con, object_key)

    fos = MagicMock()

    def head_object(*, Bucket, Key):
        if Key == object_keys[0]:
            raise RuntimeError("404 Not Found")
        raise RuntimeError("503 temporary storage error")

    fos.head_object.side_effect = head_object
    with _rum_env(
        tmp_path,
        _download_stub({}),
        fos=fos,
        attach=lambda *_args, **_kwargs: True,
    ):
        summary = convert_batch_rum_objects(SERVICE_ID, object_keys, "worker-download-failures")

    assert summary["dead_letter"] == 1
    assert summary["failed"] == 1
    assert _ledger_row(con, object_keys[0])["status"] == "dead_letter"
    transient = _ledger_row(con, object_keys[1])
    assert transient["status"] == "discovered"
    assert transient["attempts"] == 1
    assert "download failed" in transient["last_error"]


def test_convert_batch_rum_objects_keeps_batch_retryable_when_attach_fails(tmp_path):
    object_key = "raw/rum/2026/08/27/11/05/attach-failed.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    raw = tmp_path / "attach-failed.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}),
        attach=lambda *_args, **_kwargs: False,
    ):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "worker-attach-failure")

    assert summary["failed"] == 1
    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "attach failed" in row["last_error"]


def test_convert_batch_rum_objects_commits_valid_rows_when_evidence_capture_raises(tmp_path):
    object_key = "raw/rum/2026/08/27/11/06/capture-failed.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    local_file = tmp_path / "capture-failed.json.gz"
    _write_gz(
        local_file,
        [
            json.dumps(
                {
                    "timestamp": "2026-08-27T11:06:00Z",
                    "rum_metric_name": "LCP",
                    "rum_metric_value": "1200",
                }
            ),
            '{"invalid":',
        ],
    )
    s3_path = f"s3://{BUCKET}/{object_key}"

    with _rum_env(tmp_path, _download_stub({s3_path: str(local_file)})):
        with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("evidence disk full")):
            summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "worker-capture-failure")

    assert summary["committed"] == 1
    row = con.execute(
        "SELECT status, outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (SERVICE_ID, object_key),
    ).fetchone()
    counters = json.loads(row["outcome_counters"])
    assert row["status"] == "committed"
    assert counters["valid_records"] == 1
    assert counters["malformed_records"] == 1
    assert counters["quarantine_capture_failures"] == 1
    assert counters["objects_failed"] == 1


def test_quarantine_rum_corrupt_lines_captures_exact_bytes_locally(tmp_path):
    """Each malformed RUM line is stored as its own exact-byte evidence item."""
    fos = MagicMock()
    src = {"service_id": SERVICE_ID, "name": "prod-source", "bucket": BUCKET, "prefix": "/tenant-a/"}
    object_key = "tenant-a/raw/rum/2026/08/27/11/09/mixed.json.gz"
    bad = [b"{bad one\n", b'{"a":1}\n']
    local_file = tmp_path / "mixed.json"
    with gzip.open(local_file, "wb") as raw:
        raw.write(b"".join(bad))
    corrupt = [("{bad one", "invalid_json"), ('{"a":1}', "parse_error: nope")]

    with (
        patch(
            "backend.core.quarantine.capture_evidence",
            return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
        ) as capture,
    ):
        result = _quarantine_rum_corrupt_lines(fos, src, object_key, corrupt, valid_count=7, local_file=str(local_file))

    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 7,
        "malformed_records": 2,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }
    assert capture.call_count == 2
    assert capture.call_args_list[0].args == (SERVICE_ID, "rum", object_key, bad[0])
    assert capture.call_args_list[0].kwargs["line_ordinal"] == 1
    assert capture.call_args_list[0].kwargs["byte_offset"] == 0
    assert capture.call_args_list[1].args == (SERVICE_ID, "rum", object_key, bad[1])
    assert capture.call_args_list[1].kwargs["error_category"] == "parse_error"


def test_quarantine_rum_corrupt_lines_rejects_a_key_outside_the_rum_raw_prefix(tmp_path):
    fos = MagicMock()
    src = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}
    local_file = tmp_path / "one.json"
    with gzip.open(local_file, "wb") as raw:
        raw.write(b"x\n")

    result = _quarantine_rum_corrupt_lines(
        fos,
        src,
        "raw/2026/08/27/regular-log.json.gz",
        [("x", "invalid_json")],
        1,
        local_file=str(local_file),
    )

    fos.put_object.assert_not_called()
    assert result["quarantine_capture_failures"] == 1


def _null_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return None


def test_discover_rum_prefix_is_a_noop_without_config_or_source():
    """A worker that races ahead of config/source registration must insert
    nothing rather than dispatch converts for keys it cannot resolve."""
    con, _ = clear_ledger(SERVICE_ID)

    with patch("backend.config.load_config", return_value=None):
        with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
            assert discover_rum_prefix(SERVICE_ID) == 0
            mock_list.assert_not_called()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
                assert discover_rum_prefix(SERVICE_ID) == 0
                mock_list.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 0


def test_discover_rum_prefix_returns_zero_when_the_list_yields_no_result():
    """A LIST that produces no result payload at all (aborted generator) is
    not an empty bucket — it must not be reported as a successful scan."""
    clear_ledger(SERVICE_ID)
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_null_list_fos):
                with patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_delay:
                    assert discover_rum_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()


def test_discover_rum_prefix_does_not_redispatch_an_already_known_key():
    """ON CONFLICT DO NOTHING means a re-LIST of the same minute must
    dispatch nothing — otherwise every sweep re-enqueues the whole
    lookback window."""
    object_key = "raw/rum/2026/08/27/11/10/known.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    def _list_one(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        path = f"s3://{BUCKET}/{object_key}"
        return {"new_files": [path], "file_sizes": {path: 10}, "skipped_already": 0, "stranded_already": []}

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_list_one):
                with patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_delay:
                    assert discover_rum_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 1


def test_sweep_rum_ledger_fails_closed_when_the_broker_check_raises():
    """Without a trustworthy queue depth, the sweeper must not amplify a
    Valkey outage into duplicate RUM messages."""
    con, cur = clear_ledger(SERVICE_ID)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    reclaim_key = "raw/rum/2026/08/27/11/11/reclaim.json.gz"
    stuck_key = "raw/rum/2026/08/27/11/11/stuck.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) "
        "VALUES (?, ?, 'claimed', 'dead-worker', ?, ?)",
        (SERVICE_ID, reclaim_key, stale, stale - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (SERVICE_ID, stuck_key, stale - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch(
                        "backend.celery_status.celery_queue_depths",
                        side_effect=Exception("broker unreachable"),
                    ),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_batch_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary["reclaimed"] == 1
    assert summary["redispatched"] == 0
    assert summary["discovered"] == 0
    assert summary["broker_ok"] is False
    mock_rum_delay.assert_not_called()
    mock_batch_delay.assert_not_called()
    assert _ledger_row(con, reclaim_key)["status"] == "discovered"


def test_sweep_rum_ledger_scopes_to_the_source_prefix():
    """With a source prefix configured, the RUM keyspace is
    ``<prefix>/raw/rum/`` — an unprefixed key belongs to a different source
    layout and must not be swept by this service's RUM sweep."""
    con, cur = clear_ledger(SERVICE_ID)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    prefixed = "tenant-a/raw/rum/2026/08/27/11/12/mine.json.gz"
    unprefixed = "raw/rum/2026/08/27/11/12/not-mine.json.gz"
    for key in (prefixed, unprefixed):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) "
            "VALUES (?, ?, 'claimed', 'dead-worker', ?, ?)",
            (SERVICE_ID, key, stale, stale - 30),
        )
    con.commit()

    src = {"name": "t", "bucket": BUCKET, "prefix": "/tenant-a/"}
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=src):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary["reclaimed"] == 1
    mock_rum_delay.assert_called_once_with(SERVICE_ID, [prefixed])
    assert _ledger_row(con, unprefixed)["status"] == "claimed"


@pytest.mark.parametrize("status", ["committed", "quarantined", "dead_letter"])
def test_sweep_rum_ledger_leaves_terminal_rows_alone(status):
    """Only ``claimed``/``discovered`` rows are sweepable. Re-dispatching a
    committed row would re-convert a raw file that may already be deleted;
    re-dispatching a quarantined one would defeat the attempt cap."""
    con, cur = clear_ledger(SERVICE_ID)
    key = f"raw/rum/2026/08/27/11/13/{status}.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_at, discovered_at) VALUES (?, ?, ?, ?, ?)",
        (SERVICE_ID, key, status, time.time() - 999999, time.time() - 999999),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary["reclaimed"] == 0
    assert summary["redispatched"] == 0
    assert summary["discovered"] == 0
    mock_rum_delay.assert_not_called()
    assert _ledger_row(con, key)["status"] == status


# ── Merged from tests/core/test_rum_parse_variants.py ──
_SERVICE_ID_rum_parse_variants = "svc-parse-variants"


def _faro_payload() -> dict:
    return {
        "meta": {
            "browser": {"name": "Firefox", "mobile": True},
            "os": {"name": "Android"},
        },
        "page": {"url": "https://shop.example.com/checkout/cart?step=2"},
        "measurements": [
            {
                "type": "web-vitals",
                "context": {"rating": "needs-improvement"},
                "values": {"lcp": 2600.5, "delta": 12.0},
            }
        ],
        "exceptions": [
            {
                "value": "TypeError: undefined is not a function",
                "stacktrace": {"frames": [{"filename": "bundle.js", "lineno": 918, "colno": 27}]},
            }
        ],
    }


def test_parse_rum_line_extracts_vitals_and_exceptions_from_faro_body():
    """A Faro ``rum_body`` must fan out into one row per extracted metric —
    the web-vital into vitals, the exception into errors with its stack
    frame's file/line/col — not collapse into a single 'unknown' row."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps(_faro_payload()),
        "rum_cid": "cid-abc",
        "fastly_req_id": "req-123",
        "geo_city": "Portland",
        "geo_country_code": "US",
        "server_pop": "PDX",
        "tls_version": "TLSv1.3",
        "time_to_first_byte": "88.5",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    # 'delta' is excluded by the extractor; only the real vital survives.
    assert [r["metric_name"] for r in vitals_rows] == ["lcp"]
    vital = vitals_rows[0]
    assert vital["metric_value"] == 2600.5
    assert vital["metric_rating"] == "needs-improvement"
    assert vital["pathname"] == "/checkout/cart"
    assert vital["browser"] == "Firefox"
    assert vital["os"] == "Android"
    assert vital["device"] == "Mobile"  # meta.browser.mobile
    assert vital["cid"] == "cid-abc"
    assert vital["req_id"] == "req-123"
    # Geo/TLS/TTFB come off the log line, not the Faro payload.
    assert vital["city"] == "Portland"
    assert vital["country"] == "US"
    assert vital["pop"] == "PDX"
    assert vital["tls"] == "TLSv1.3"
    assert vital["ttfb"] == 88.5

    assert len(errors_rows) == 1
    err = errors_rows[0]
    assert err["error_message"] == "TypeError: undefined is not a function"
    assert err["error_file"] == "bundle.js"
    assert err["error_line"] == 918
    assert err["error_col"] == 27
    assert err["pathname"] == "/checkout/cart"
    assert err["device"] == "Mobile"


def test_parse_rum_line_unwraps_double_encoded_faro_body():
    """Some beacon transports deliver ``rum_body`` double-JSON-encoded (a
    JSON string whose content is itself JSON). The parser unwraps one extra
    layer — without it the payload reads as a str, extraction is skipped,
    and the line silently degrades to a single 'unknown' vitals row."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps(json.dumps(_faro_payload())),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert [r["metric_name"] for r in vitals_rows] == ["lcp"]
    assert len(errors_rows) == 1
    assert errors_rows[0]["error_file"] == "bundle.js"


def test_parse_rum_line_falls_back_to_flat_fields_when_faro_body_is_garbage():
    """An unparseable ``rum_body`` must not lose the line: the flat ``rum_*``
    fields on the same record are still authoritative."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": "{not json at all",
        "rum_metric_name": "CLS",
        "rum_metric_value": "0.08",
        "rum_metric_rating": "good",
        "rum_pathname": "/pricing",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert errors_rows == []
    assert len(vitals_rows) == 1
    assert vitals_rows[0]["metric_name"] == "CLS"
    assert vitals_rows[0]["metric_value"] == 0.08
    assert vitals_rows[0]["metric_rating"] == "good"
    assert vitals_rows[0]["pathname"] == "/pricing"


def test_parse_rum_line_faro_payload_with_no_metrics_yields_pageview():
    """An empty-but-valid Faro payload still represents a real page view —
    the extractor's fallback row must reach the vitals table rather than the
    line being dropped."""
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": json.dumps({"meta": {}, "page": {"url": "https://example.com/blog/post-1"}}),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert errors_rows == []
    assert [r["metric_name"] for r in vitals_rows] == ["pageview"]
    assert vitals_rows[0]["pathname"] == "/blog/post-1"
    assert vitals_rows[0]["metric_value"] == 1.0
    # No mobile hint in meta -> Desktop, and the Chrome/macOS defaults apply.
    assert vitals_rows[0]["device"] == "Desktop"
    assert vitals_rows[0]["browser"] == "Chrome"
    assert vitals_rows[0]["os"] == "macOS"


def test_parse_rum_line_recovers_metric_from_beacon_url_query_string():
    """The pixel-GET beacon carries its values ONLY in the request URL's
    query string. If they aren't lifted out, every such line lands as
    metric_name='unknown', metric_value=0.0 — a silently empty vitals
    table."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "url": (
            "/rum-beacon?rum_metric_name=INP&rum_metric_value=145.25"
            "&rum_metric_rating=good&cid=cid-from-query&rum_pathname=/search"
        ),
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert errors_rows == []
    assert len(vitals_rows) == 1
    row = vitals_rows[0]
    assert row["metric_name"] == "INP"
    assert row["metric_value"] == 145.25
    assert row["metric_rating"] == "good"
    assert row["cid"] == "cid-from-query"
    assert row["pathname"] == "/search"


def test_parse_rum_line_prefers_explicit_fields_over_query_string():
    """Query-string recovery is a fallback, never an override: an explicit
    top-level ``rum_*`` field wins over the same key in the URL."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "1200",
        "rum_metric_rating": "good",
        "rum_cid": "cid-explicit",
        "rum_pathname": "/explicit",
        "url": (
            "/rum-beacon?rum_metric_name=INP&rum_metric_value=999"
            "&rum_metric_rating=poor&cid=cid-query&rum_pathname=/from-query"
        ),
    }

    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    row = vitals_rows[0]
    assert row["metric_name"] == "LCP"
    assert row["metric_value"] == 1200
    assert row["metric_rating"] == "good"
    assert row["cid"] == "cid-explicit"
    assert row["pathname"] == "/explicit"


def test_parse_rum_line_uses_rum_raw_query_when_url_absent():
    """``rum_raw_query`` is the VCL-side alias for the beacon URL — it must
    be consulted with the same query-string recovery as ``url``."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_raw_query": "/b?rum_metric_name=TTFB&rum_metric_value=60.5",
    }

    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert vitals_rows[0]["metric_name"] == "TTFB"
    assert vitals_rows[0]["metric_value"] == 60.5


def test_parse_rum_line_keeps_non_numeric_metric_value_from_breaking_the_row():
    """A garbage ``rum_metric_value`` must degrade to 0.0 (a writable
    float64) rather than raise and lose the whole line — but the metric name
    it was reported under is preserved so the bad beacon is still visible."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "not-a-number",
        "rum_pathname": "/broken",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert errors_rows == []
    assert vitals_rows[0]["metric_name"] == "LCP"
    assert vitals_rows[0]["metric_value"] == 0.0


def test_parse_rum_line_falls_back_to_referer_path_for_pathname():
    """With no pathname anywhere in the beacon, the referring page's path is
    the only attribution available — dropping to '/' would pile every
    unattributed beacon onto the root path in the pathname breakdowns."""
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "FCP",
        "rum_metric_value": "700",
        "referer": "https://shop.example.com/collections/shoes?page=3",
    }

    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert vitals_rows[0]["pathname"] == "/collections/shoes"


def test_parse_rum_line_defaults_pathname_to_root_and_collapses_double_slash():
    log_data = {
        "timestamp": "2026-08-27T10:01:00Z",
        "rum_metric_name": "FCP",
        "rum_metric_value": "700",
    }
    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)
    assert vitals_rows[0]["pathname"] == "/"

    log_data["rum_pathname"] = "//collections//shoes"
    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)
    assert vitals_rows[0]["pathname"] == "/collections/shoes"


def test_parse_rum_line_routes_flat_error_beacon_to_errors_only():
    """A flat ``rum_error_*`` beacon is an error, not a vital: it must
    produce exactly one errors row and no vitals row (a duplicate vitals row
    would double-count the beacon in every metric panel)."""
    log_data = {
        "timestamp": "2026-08-27T10:02:00Z",
        "rum_error_message": "ReferenceError: gtag is not defined",
        "rum_error_file": "analytics.js",
        "rum_error_line": "17",
        "rum_error_col": "5",
        "rum_pathname": "/account",
    }

    vitals_rows, errors_rows = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)

    assert vitals_rows == []
    assert len(errors_rows) == 1
    err = errors_rows[0]
    assert err["error_message"] == "ReferenceError: gtag is not defined"
    assert err["error_file"] == "analytics.js"
    assert err["error_line"] == 17
    assert err["error_col"] == 5
    assert err["pathname"] == "/account"


def test_parse_rum_line_returns_none_for_another_services_beacon():
    """Multi-tenant bucket routing: a line tagged for a different service is
    neither ours nor corrupt — the caller must be able to tell the
    difference, so the parser returns None rather than empty lists."""
    assert (
        _parse_rum_line(
            {"rum_service_id": "some-other-service", "rum_metric_name": "LCP"}, _SERVICE_ID_rum_parse_variants
        )
        is None
    )
    # Our own service_id (either key spelling) parses normally.
    assert (
        _parse_rum_line(
            {
                "service_id": _SERVICE_ID_rum_parse_variants,
                "timestamp": "2026-08-27T10:00:00Z",
                "rum_metric_name": "LCP",
            },
            _SERVICE_ID_rum_parse_variants,
        )
        is not None
    )


def test_parse_rum_line_raises_on_missing_or_unparseable_timestamp():
    """Gap 8: A line with a missing or unparseable timestamp must fail so that
    it is quarantined with a categorized reason rather than silently dropping or inventing timestamps."""
    with pytest.raises(ValueError, match="missing_timestamp"):
        _parse_rum_line({"rum_metric_name": "LCP", "rum_metric_value": "1"}, _SERVICE_ID_rum_parse_variants)

    with pytest.raises(ValueError, match="invalid_timestamp"):
        _parse_rum_line(
            {"timestamp": "not-a-timestamp", "rum_metric_name": "LCP", "rum_metric_value": "1"},
            _SERVICE_ID_rum_parse_variants,
        )


def test_parse_rum_beacon_file_quarantines_missing_or_unparseable_timestamp(tmp_path):
    """Gap 8: Every line with missing/unparseable timestamp goes into corrupt_lines."""
    raw_file = tmp_path / "bad_ts.json.gz"
    lines = [
        json.dumps({"timestamp": "2026-08-27T10:04:00Z", "rum_metric_name": "CLS", "rum_metric_value": "0.2"}),
        json.dumps({"rum_metric_name": "LCP", "rum_metric_value": "1.0"}),  # missing timestamp
        json.dumps({"timestamp": "bad-iso", "rum_metric_name": "INP", "rum_metric_value": "150"}),  # invalid timestamp
    ]
    with gzip.open(raw_file, "wt", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    vitals, errors, corrupt = _parse_rum_beacon_file(str(raw_file), _SERVICE_ID_rum_parse_variants)
    assert len(vitals) == 1
    assert len(corrupt) == 2
    reasons = [reason for _, reason in corrupt]
    assert any("missing_timestamp" in r for r in reasons)
    assert any("invalid_timestamp" in r for r in reasons)


def test_parse_rum_beacon_file_ignores_blank_lines_without_quarantining_them():
    """Beacon files routinely end with a trailing newline. Counting that as
    corruption would quarantine (and alarm on) every single file."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    good = json.dumps({"timestamp": "2026-08-27T10:04:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1"})
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write("\n" + good + "\n   \n\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, _SERVICE_ID_rum_parse_variants)

    assert len(vitals_rows) == 1
    assert errors_rows == []
    assert corrupt_lines == []


def test_parse_rum_beacon_file_quarantines_valid_json_that_is_not_a_beacon_object():
    """A line can be valid JSON yet not a beacon object (a bare array or
    scalar from a truncated/garbled write). The parser raises on it, and the
    file-level reader must capture that as a quarantined line with the
    reason attached — not crash the whole file, which would strand every
    good beacon in it."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    good = json.dumps({"timestamp": "2026-08-27T10:04:00Z", "rum_metric_name": "CLS", "rum_metric_value": "0.2"})
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write("[1, 2, 3]\n" + good + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, _SERVICE_ID_rum_parse_variants)

    # The good beacon still lands.
    assert [r["metric_name"] for r in vitals_rows] == ["CLS"]
    assert errors_rows == []
    assert len(corrupt_lines) == 1
    line, reason = corrupt_lines[0]
    assert line == "[1, 2, 3]"
    assert reason.startswith("parse_error: ")
    assert len(reason) <= 200


def test_parse_rum_line_normalizes_naive_timestamp_to_utc():
    """A naive ISO timestamp is assumed UTC (that is what the beacon emits);
    leaving it naive would break the timestamp('us', tz='UTC') arrow column."""
    log_data = {"timestamp": "2026-08-27T10:03:00", "rum_metric_name": "LCP", "rum_metric_value": "1"}
    vitals_rows, _ = _parse_rum_line(log_data, _SERVICE_ID_rum_parse_variants)
    ts = vitals_rows[0]["timestamp"]
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0
    assert (ts.year, ts.month, ts.day, ts.hour, ts.minute) == (2026, 8, 27, 10, 3)


# ── Merged from tests/core/test_rum_provisioning.py ──
FALCO_INSTALLED = shutil.which("falco") is not None


def test_generate_rum_vcl_unaffected_by_missing_faro_version():
    """Callers that don't pass faro_version (every caller today) must see
    identical output to explicitly passing faro_version=None."""
    with_default = generate_rum_vcl("srv_test")
    with_explicit_none = generate_rum_vcl("srv_test", faro_version=None)
    assert with_default == with_explicit_none
    assert set(with_default.keys()) == {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME}


def test_generate_rum_asset_fetch_vcl_byte_identical_without_faro_version():
    """The two Phase-3 snippets must be untouched when faro_version is not
    supplied — this is what the orchestrator and declarative generators
    call today, and their VCL must not silently change shape."""
    no_faro = generate_rum_asset_fetch_vcl("iad-va-us")
    explicit_none = generate_rum_asset_fetch_vcl("iad-va-us", faro_version=None)

    assert no_faro == explicit_none
    assert set(no_faro.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME}
    assert RUM_FARO_FETCH_NAME not in no_faro

    # No Faro fingerprints anywhere in the unmodified snippets.
    combined = "\n".join(no_faro.values())
    assert "faro" not in combined.lower()
    assert "/js/faro-sdk.js" not in combined


def test_generate_rum_vcl_faro_version_none_does_not_touch_phase1_snippets():
    """faro_version=None must leave the Phase 1 recv/deliver snippets
    completely untouched — no Faro strings leak in even by accident."""
    snippets = generate_rum_vcl("srv_test", faro_version=None)
    combined = "\n".join(snippets.values())
    assert "faro" not in combined.lower()


def test_faro_version_adds_route_rewrite_and_surrogate_key():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")

    assert set(snippets.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME, RUM_FARO_FETCH_NAME}

    recv = snippets[RUM_ASSET_FETCH_NAME]
    # Both paths ride ONE route — they only ever differed in the FOS object
    # they resolve to, and that rewrite lives in the SigV4 snippet below.
    assert 'if ((req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js") && req.method == "GET")' in recv
    assert recv.count("return(lookup);") == 1

    sigv4 = snippets[RUM_SIGV4_SIGN_NAME]
    assert 'bereq.url.path == "/js/faro-sdk.js"' in sigv4
    assert 'set bereq.url = "/rum/faro-web-sdk-v2.9.0.iife.js";' in sigv4
    # Existing rum.js rewrite must still be present.
    assert 'set bereq.url = "/rum/rum-tracker.js";' in sigv4

    fetch = snippets[RUM_FARO_FETCH_NAME]
    assert 'set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";' in fetch
    assert "set beresp.ttl = 604800s;" in fetch


def test_faro_fetch_keeps_faro_key_and_adds_shared_rum_key():
    """The bundle carries two surrogate keys and must keep both: 'rum-faro-sdk'
    is what the FOS-sync cron and upgrade_faro_version purge on their own
    (Faro-only invalidation), while 'rum-js' is the shared tag across BOTH
    hosted scripts that reconciliation purges in one call. Dropping either
    silently strands one of those purge paths."""
    fetch = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_FARO_FETCH_NAME]

    match = re.search(r'Surrogate-Key = "([^"]+)"', fetch)
    assert match, "expected a Surrogate-Key header"
    assert set(match.group(1).split()) == {"rum-faro-sdk", "rum-js"}


def test_faro_fetch_caching_gates_on_200_status():
    """F-3 audit finding: without a beresp.status check, a FOS 403/404
    (mid-upload, or a bucket-policy blip) would be cached at the edge for
    7 days AND handed to browsers tagged immutable — unpurgeable. The
    caching/Surrogate-Key/Cache-Control block must be nested inside an
    explicit 200 check, with a non-caching branch for everything else."""
    fetch = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_FARO_FETCH_NAME]

    assert "if (beresp.status == 200)" in fetch
    # The caching directives must be inside the 200 branch, not top-level.
    status_check_idx = fetch.index("if (beresp.status == 200)")
    surrogate_key_idx = fetch.index('set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";')
    assert status_check_idx < surrogate_key_idx
    # Non-200 branch must explicitly refuse to cache.
    assert "set beresp.cacheable = false;" in fetch


def test_faro_fetch_caching_decouples_edge_and_browser_ttl():
    """F-3 audit finding: /js/faro-sdk.js is a stable path serving mutable
    (per-upgrade) content, so a long browser max-age + immutable would mean
    an upgrade can never invalidate already-issued browser copies — a purge
    only clears the edge cache. The edge TTL must stay long (purged
    explicitly on upload/upgrade via Surrogate-Key), while the
    browser-visible Cache-Control gets a short max-age and drops
    'immutable' entirely."""
    fetch = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_FARO_FETCH_NAME]

    assert "immutable" not in fetch
    assert 'set beresp.http.Surrogate-Control = "max-age=604800";' in fetch
    assert "set beresp.ttl = 604800s;" in fetch

    # Extract the Cache-Control value and assert its max-age is much shorter
    # than the edge TTL — the exact number isn't load-bearing, "much less
    # than 604800" is.
    match = re.search(r'Cache-Control = "([^"]+)"', fetch)
    assert match, "expected a browser Cache-Control header"
    cache_control = match.group(1)
    max_age_match = re.search(r"max-age=(\d+)", cache_control)
    assert max_age_match, f"no max-age found in Cache-Control: {cache_control!r}"
    assert int(max_age_match.group(1)) < 604800


def test_faro_version_present_in_asset_fetch_but_generate_rum_vcl_still_clean():
    """Phase 1 (generate_rum_vcl) never emits Faro content, even when a
    valid faro_version is supplied — Faro routing lives exclusively in
    generate_rum_asset_fetch_vcl."""
    snippets = generate_rum_vcl("srv_test", faro_version="2.9.0")
    assert set(snippets.keys()) == {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME}
    combined = "\n".join(snippets.values())
    assert "faro" not in combined.lower()


def test_faro_asset_fetch_respects_shield_pop_selection():
    """Both asset paths share one backend-selection expression, so the
    shield_pop threading cannot diverge between them."""
    snippets = generate_rum_asset_fetch_vcl("frankfurt-de", faro_version="1.0.0")
    recv = snippets[RUM_ASSET_FETCH_NAME]
    assert recv.count("fastly.try_select_shield(ssl_shield_frankfurt_de, F_fos_origin)") == 1
    assert '"/js/faro-sdk.js"' in recv


def test_faro_asset_fetch_shield_none():
    snippets = generate_rum_asset_fetch_vcl("none", faro_version="1.0.0")
    recv = snippets[RUM_ASSET_FETCH_NAME]
    assert recv.count("set req.backend = F_fos_origin;") == 1
    assert '"/js/faro-sdk.js"' in recv


@pytest.mark.parametrize(
    "bad_version",
    [
        '1.2."3',  # embedded quote — breaks the VCL string literal
        "1.2.3\n",  # newline
        "../../etc/passwd",  # path traversal
        "1.2.3 ",  # trailing whitespace
        " 1.2.3",  # leading whitespace
        "1.2.3; set req.http.x = 1",  # attempted VCL injection
        "1.2",  # not a full X.Y.Z
        "1.2.3.4",  # too many components
        "1.2.x",  # non-numeric component
        "",  # empty
        "1.2.3-beta",  # pre-release suffix
        "1.2\\.3",  # backslash
        "1.2.3\x00",  # null byte
        "\x00",  # bare null byte
        "１.２.３",  # fullwidth digits (U+FF10-FF19) — \d matches these too
        "١.٢.٣",  # Arabic-Indic digits (U+0660-0669) — \d matches these too
        "2.9.٠",  # mixed ASCII + non-ASCII digit in one component
    ],
)
def test_generate_rum_asset_fetch_vcl_rejects_malicious_version(bad_version):
    with pytest.raises(ValueError):
        generate_rum_asset_fetch_vcl("iad-va-us", faro_version=bad_version)


@pytest.mark.parametrize(
    "bad_version",
    [
        '1.2."3',
        "1.2.3\n",
        "../../etc/passwd",
        "1.2\\.3",
        "1.2.3\x00",
        "１.２.３",
        "١.٢.٣",
    ],
)
def test_generate_rum_vcl_rejects_malicious_version(bad_version):
    """generate_rum_vcl validates too, even though it never emits Faro
    content — a caller that threads a bad version through this function
    must fail loudly rather than have it silently discarded."""
    with pytest.raises(ValueError):
        generate_rum_vcl("srv_test", faro_version=bad_version)


@pytest.mark.parametrize(
    "unicode_digit_version",
    [
        "１.２.３",  # fullwidth (U+FF10-FF19)
        "١.٢.٣",  # Arabic-Indic (U+0660-0669)
        "๑.๒.๓",  # Thai digits (U+0E50-0E59)
        "2.9.٠",  # mixed ASCII + non-ASCII digit
    ],
)
def test_assert_faro_version_safe_rejects_unicode_digits_directly(unicode_digit_version):
    """Regression: Python's \\d in re (without re.ASCII) matches any Unicode
    decimal-digit codepoint (category Nd), not just 0-9. A version string
    built entirely from non-ASCII digits must still be rejected — verified
    directly against the validator rather than only through the public
    generator functions."""
    from backend.core.fastly.rum_provisioning import _assert_faro_version_safe

    with pytest.raises(ValueError):
        _assert_faro_version_safe(unicode_digit_version)


def test_rejected_version_never_reaches_generated_vcl():
    """Defense in depth: confirm the rejection happens BEFORE any VCL
    string is built — a malicious value must never even transiently exist
    inside a returned snippet body."""
    malicious = '2.9.0"; unset bereq.http.Authorization; #'
    with pytest.raises(ValueError):
        generate_rum_asset_fetch_vcl("iad-va-us", faro_version=malicious)


def _wrap_for_falco(body: str, subroutine: str) -> str:
    """Assemble a syntactically-complete VCL file for falco lint.

    RUM asset-fetch VCL references the FOS backend, a per-region shield
    backend, and the ``fos_credentials`` edge dictionary (a Fastly `table`)
    — none of which the shared ``lint_vcl`` wrapper declares (it only knows
    about the session-scoring backends). Declare stand-ins here and lint the
    fully-assembled file with ``wrap_subroutine=None`` so we still go
    through the shared falco subprocess/parsing plumbing.
    """
    stage = subroutine.removeprefix("vcl_")
    return f"""backend F_fos_origin {{
  .host = "example.com";
  .port = "443";
  .ssl = true;
}}

backend ssl_shield_iad_va_us {{
  .host = "example.com";
  .port = "443";
  .ssl = true;
}}

table fos_credentials {{
  "access_key": "x",
}}

sub {subroutine} {{
  #FASTLY {stage}
{body}
}}
"""


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_asset_fetch_recv_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_ASSET_FETCH_NAME], "vcl_recv")
    result = lint_vcl(full, snippet_name="faro_asset_fetch_recv", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_sigv4_rewrite_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_SIGV4_SIGN_NAME], "vcl_miss")
    result = lint_vcl(full, snippet_name="faro_sigv4_miss", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_fetch_caching_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_FARO_FETCH_NAME], "vcl_fetch")
    result = lint_vcl(full, snippet_name="faro_fetch_cache", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_unchanged_asset_fetch_snippets_without_faro():
    """The pre-existing (no faro_version) snippets must still lint clean —
    regression guard for the byte-identity requirement."""
    snippets = generate_rum_asset_fetch_vcl("iad-va-us")
    recv_full = _wrap_for_falco(snippets[RUM_ASSET_FETCH_NAME], "vcl_recv")
    miss_full = _wrap_for_falco(snippets[RUM_SIGV4_SIGN_NAME], "vcl_miss")
    recv_result = lint_vcl(recv_full, snippet_name="asset_fetch_recv", wrap_subroutine=None)
    miss_result = lint_vcl(miss_full, snippet_name="asset_fetch_miss", wrap_subroutine=None)
    assert recv_result.ok, f"falco errors: {recv_result.errors}"
    assert miss_result.ok, f"falco errors: {miss_result.errors}"


def test_falco_required_when_missing_binary_env_set(monkeypatch):
    """Mirrors the FALCO_REQUIRED contract used in test_vcl_semantics.py:
    if a CI environment declares falco mandatory but it isn't on PATH, the
    lint helper must not silently report success."""
    monkeypatch.setattr("backend.utils.vcl_validator._falco_binary", lambda: None)
    result = lint_vcl("if (true) {}", wrap_subroutine=None)
    assert result.skipped is True
    assert result.ok is True  # lint_vcl itself is fail-open on missing binary


@pytest.mark.parametrize("cond", ["", "   ", None])
def test_empty_condition_is_allowed(cond):
    assert validate_rum_custom_condition(cond) is None


@pytest.mark.parametrize(
    "cond",
    [
        'req.url.path !~ "^/api/"',  # the exact value the affected service ran
        'req.url.path ~ "^/api/"',  # the kill-switch inverse
        'req.url ~ "^/api/"',
        'client.ip != "203.0.113.5" && req.url.path !~ "^/api/"',
    ],
)
def test_path_predicates_are_rejected(cond):
    err = validate_rum_custom_condition(cond)
    assert err is not None
    assert "req.url" in err
    # The message must tell the operator where page filtering actually belongs,
    # otherwise they'll just try another unworkable variant.
    assert "tracker" in err


def test_single_quotes_rejected():
    """VCL string literals are double-quoted; the old placeholder used singles."""
    err = validate_rum_custom_condition("client.ip != '192.168.1.1'")
    assert err is not None
    assert "double quotes" in err


@pytest.mark.parametrize(
    "cond",
    [
        'req.http.User-Agent !~ "(bot|crawler)"',
        'client.geo.country_code != "US"',
        'req.http.Host == "example.com"',
    ],
)
def test_workable_conditions_pass(cond):
    assert validate_rum_custom_condition(cond) is None
