"""Unit tests for usage_logger helpers and telemetry process context."""

from __future__ import annotations

from unittest.mock import patch

# ---------------------------------------------------------------------------
# telemetry — process context helpers


def test_process_context_set_and_get():
    from backend.utils.telemetry import _set_process_context_for_tests, get_process_context

    _set_process_context_for_tests("cron:sync:svc1")
    assert get_process_context() == "cron:sync:svc1"


def test_process_context_default_is_none():
    import threading

    result = {}

    def _read():
        # A new thread copies the parent context; the parent hasn't set the var
        # in this test, but we check via the ContextVar default value
        from backend.utils.telemetry import _PROCESS_CONTEXT

        result["ctx"] = _PROCESS_CONTEXT.get(None)

    t = threading.Thread(target=_read)
    t.start()
    t.join()
    # The default for _PROCESS_CONTEXT is None, confirmed independently
    assert result["ctx"] is None


def test_start_call_tracking_resets_calls():
    from backend.utils.telemetry import get_tracked_calls, record_call, start_call_tracking

    start_call_tracking()
    record_call("PutObject", "/file.gz", 10.0, service="FOS", details="Class A")
    assert len(get_tracked_calls()) == 1

    start_call_tracking()
    assert get_tracked_calls() == []


def test_record_call_stores_bytes():
    from backend.utils.telemetry import get_tracked_calls, record_call, start_call_tracking

    start_call_tracking()
    record_call("GetObject", "/data.parquet", 5.0, service="FOS", bytes_count=1024)
    calls = get_tracked_calls()
    assert len(calls) == 1
    assert calls[0]["bytes"] == 1024


# ---------------------------------------------------------------------------
# log_usage_calls — classification and insertion (now per-service SQLite)


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_inserts_fos_a(mock_enabled):
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "FOS",
            "method": "PutObject",
            "path": "/prefix/file.gz",
            "time_ms": 12.0,
            "status": "OK",
            "details": "Class A",
            "caller": "upload_file",
            "bytes": None,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls, process_context="cron:sync:svc1")

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    row = con.execute("SELECT operation_class, operation_type, service_id FROM usage_log").fetchone()
    assert row is not None
    assert row["operation_class"] == "A"
    assert row["operation_type"] == "PutObject"
    assert row["service_id"] == "svc1"


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_raw_http_put_post_as_class_a(mock_enabled):
    """Telemetry proxy emits raw HTTP verbs (PUT/POST/COPY) via request.method.

    The classifier must recognise those as Class A so PutObject, UploadPart,
    DeleteObjects batch, etc. routed through the proxy aren't misbilled as B.
    """
    from backend.core.duckdb import log_usage_calls

    calls = [
        {"service": "FOS", "method": "PUT", "path": "/k", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "POST", "path": "/?delete", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "COPY", "path": "/dst", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "GET", "path": "/k", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "DELETE", "path": "/k", "time_ms": 1.0, "status": "OK"},
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    rows = con.execute("SELECT operation_type, operation_class FROM usage_log ORDER BY rowid").fetchall()
    assert [(r["operation_type"], r["operation_class"]) for r in rows] == [
        ("PUT", "A"),
        ("POST", "A"),
        ("COPY", "A"),
        ("GET", "B"),
        ("DELETE", "B"),
    ]


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_cdn(mock_enabled):
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "CDN",
            "method": "download",
            "path": "/data.parquet",
            "time_ms": 50.0,
            "status": "OK",
            "details": None,
            "caller": None,
            "bytes": 2048,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    row = con.execute("SELECT operation_class, bytes FROM usage_log").fetchone()
    assert row is not None
    assert row["operation_class"] == "CDN"
    assert row["bytes"] == 2048


@patch("backend.config.is_usage_logging_enabled", return_value=False)
def test_log_usage_calls_skips_when_disabled(mock_enabled):
    """When usage logging is disabled, no DB interaction should occur."""
    from backend.core.duckdb import log_usage_calls

    calls = [{"service": "FOS", "method": "PutObject", "path": "/x", "time_ms": 1.0, "status": "OK"}]
    # Should exit silently without raising
    log_usage_calls({"name": "svc1"}, calls)


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_non_fos_as_class_b(mock_enabled):
    """Non-FOS, non-CDN calls (e.g. Fastly API) default to operation_class 'B'."""
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "Fastly API",
            "method": "GET",
            "path": "/api/services",
            "time_ms": 8.0,
            "status": "OK",
            "details": None,
            "caller": None,
            "bytes": None,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    rows = con.execute("SELECT operation_class FROM usage_log").fetchall()
    # Default classification for unknown services is Class B (per metadata_db)
    assert len(rows) == 1
    assert rows[0]["operation_class"] == "B"


# ---------------------------------------------------------------------------
# purge_usage_log — retention enforcement


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7})
def test_purge_usage_log_deletes_old_rows(mock_cfg):
    from backend.core.duckdb import purge_usage_log
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        [
            ("2020-01-01T00:00:00", "svc1", "A", "OK"),  # very old — should be purged
            ("2099-01-01T00:00:00", "svc1", "A", "OK"),  # future — should survive
        ],
    )
    con.commit()

    purge_usage_log({"name": "svc1"})

    rows = con.execute("SELECT timestamp FROM usage_log").fetchall()
    assert len(rows) == 1
    assert "2099" in str(rows[0]["timestamp"])


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 0})
def test_purge_usage_log_skips_when_retention_zero(mock_cfg):
    from backend.core.duckdb import purge_usage_log
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    con.execute(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        ("2020-01-01T00:00:00", "svc1", "A", "OK"),
    )
    con.commit()

    purge_usage_log({"name": "svc1"})

    count = con.execute("SELECT count(*) FROM usage_log").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# flush_usage_log / run_usage_log_cleanup — the wrapper functions in
# backend.utils.usage_logger that the scheduler calls each cron tick.
# Both are deliberately broad-try/except — verify the happy + error paths
# while protecting against any of them escaping as an exception.


def test_flush_usage_log_skips_when_logging_disabled():
    """Disabled globally → flush is a no-op (no DB writes, no exceptions)."""
    from backend.utils.usage_logger import flush_usage_log

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-1")  # must not raise

    mock_log.assert_not_called()


def test_flush_usage_log_skips_when_no_tracked_calls():
    """If the per-request context has no recorded calls, flush returns
    early without bothering log_usage_calls."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()  # reset to empty

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-2")

    mock_log.assert_not_called()


def test_flush_usage_log_skips_when_service_config_missing():
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry.record_call("PutObject", "/k", 1.0, service="FOS")

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-missing")

    mock_log.assert_not_called()


def test_flush_usage_log_happy_path_forwards_calls_and_context():
    """All preconditions met → log_usage_calls is invoked with the source
    dict, the tracked calls list, and the process_context kwarg."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry._set_process_context_for_tests("cron:sync:svc-flush-3")
    telemetry.record_call("PutObject", "/x.gz", 12.0, service="FOS", bytes_count=42)

    fake_cfg = {"service_id": "svc-flush-3", "fos_bucket": "b", "fos_region": "us-east-1", "name": "X"}
    fake_source = {"name": "svc-flush-3", "bucket": "b", "region": "us-east-1"}

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_to_source", return_value=fake_source),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-3")

    assert mock_log.call_count == 1
    args, kwargs = mock_log.call_args
    assert args[0] == fake_source
    assert len(args[1]) == 1
    assert args[1][0]["method"] == "PutObject"
    assert kwargs.get("process_context") == "cron:sync:svc-flush-3"


def test_flush_usage_log_swallows_inner_exception():
    """Even if log_usage_calls raises, flush must NOT propagate — the
    scheduler must keep running its cron jobs."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry.record_call("PutObject", "/k", 1.0, service="FOS")

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.log_usage_calls", side_effect=RuntimeError("db down")),
    ):
        # Must not raise
        flush_usage_log("svc-flush-error")


def test_run_usage_log_cleanup_skips_when_service_config_missing():
    from backend.utils.usage_logger import run_usage_log_cleanup

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.purge_usage_log") as mock_purge,
    ):
        run_usage_log_cleanup("svc-cleanup-missing")

    mock_purge.assert_not_called()


def test_run_usage_log_cleanup_happy_path_delegates_to_purge():
    from backend.utils.usage_logger import run_usage_log_cleanup

    fake_cfg = {"service_id": "svc-cleanup-1", "fos_bucket": "b"}
    fake_source = {"name": "svc-cleanup-1"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_to_source", return_value=fake_source),
        patch("backend.core.duckdb.purge_usage_log") as mock_purge,
    ):
        run_usage_log_cleanup("svc-cleanup-1")

    mock_purge.assert_called_once_with(fake_source)


def test_run_usage_log_cleanup_swallows_inner_exception():
    from backend.utils.usage_logger import run_usage_log_cleanup

    with (
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.purge_usage_log", side_effect=RuntimeError("boom")),
    ):
        # Must not raise — the scheduler relies on this contract.
        run_usage_log_cleanup("svc-cleanup-error")
