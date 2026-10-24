"""Ledger sweeper (celery-mode crash net) behavior.

Fixture timestamps are epoch floats because that is what the PRODUCER
writes — convert stamps ``claimed_at = time.time()`` and discovery stamps
``discovered_at = time.time()`` (see backend/core/ingest.py).
"""

import json as _json
import time
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.ingest import (
    LEDGER_MAX_ATTEMPTS,
    LEDGER_RECLAIM_AFTER_S,
    LEDGER_RETRY_BASE_S,
    _ledger_record_failure,
    _quarantine_convert_corrupt_lines,
    convert_batch_objects,
    sweep_ledger_once,
)
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    lake_reader,
)
from tests._helpers.ledger import (
    clear_ledger as _clear_ledger,
)


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def test_kill_worker_mid_convert_heal_test(monkeypatch):
    """A claim older than the reclaim window is reset to 'discovered' and
    re-dispatched — the crash-recovery path for a dead worker."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

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
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["reclaimed"] == 1
    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["claimed_by"] is None


def test_sweeper_redispatches_stuck_discovered_rows(monkeypatch):
    """A row stuck in 'discovered' (its convert message was lost to a broker
    restart / crash-after-insert) is re-dispatched."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/07/lost_dispatch.json.gz"

    con, cur = _clear_ledger(service_id)
    old = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, old),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["redispatched"] == 1


def test_sweeper_excludes_rum_keys_from_reclaim_and_redispatch(monkeypatch):
    """A stale rum/raw/ row must be left alone by sweep_ledger_once — that's
    sweep_rum_ledger_once's job."""
    service_id = "test-celery-svc"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"

    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    old_discovered = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, rum_key, stale_claim, stale_claim - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, log_key, old_discovered),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    dispatched_keys = [k for call in mock_convert_delay.call_args_list for k in call.args[1]]
    assert log_key in dispatched_keys
    assert rum_key not in dispatched_keys
    assert summary["reclaimed"] == 0
    assert summary["redispatched"] == 1

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, rum_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "dead-worker-1"


def test_sweeper_discovers_unseen_files(monkeypatch):
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/06/missed_logs.json.gz"

    con, cur = _clear_ledger(service_id)

    def mock_list_fos_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 1234},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                with patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay:
                    summary = sweep_ledger_once(service_id, run_id=79)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["discovered"] == 1
    cur.execute(
        "SELECT status, size_bytes, discovered_at, originating_task, originating_run_id "
        "FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["size_bytes"] == 1234
    assert row["discovered_at"] is not None
    assert row["originating_task"] == "ledger_sweep"
    assert row["originating_run_id"] == 79


def test_convert_failure_requeues_then_quarantines(tmp_path):
    """convert_batch_objects records failures on the ledger row: attempts increment,
    the row requeues as 'discovered', and after LEDGER_MAX_ATTEMPTS it is
    quarantined instead of retrying forever."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/08/poison.json.gz"

    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    raw_file = tmp_path / "poison.json"
    raw_file.write_text('{"timestamp": "2026-08-27T10:08:00Z"}\n')
    s3_map = {f"s3://test-bucket/{object_key}": str(raw_file)}

    def _boom(*args, **kwargs):
        raise RuntimeError("poison file")

    statuses = []
    with (
        DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local=s3_map),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=_boom),
    ):
        for _ in range(LEDGER_MAX_ATTEMPTS):
            convert_batch_objects(service_id, [object_key], "test-worker")
            row = con.execute(
                "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key=?",
                (service_id, object_key),
            ).fetchone()
            statuses.append(row["status"])

    assert statuses[:-1] == ["discovered"] * (LEDGER_MAX_ATTEMPTS - 1)
    assert statuses[-1] == "quarantined"

    row = con.execute(
        "SELECT status, attempts, last_error FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "quarantined"
    assert row["attempts"] == LEDGER_MAX_ATTEMPTS
    assert "poison file" in row["last_error"]


def test_failure_persists_exponential_retry_schedule():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, lease_generation) "
        "VALUES (?, 'raw/retry.gz', 'claimed', 1)",
        (service_id,),
    )
    con.commit()

    before = time.time()
    with patch("backend.core.ingest.random.uniform", return_value=0):
        status = _ledger_record_failure(con, service_id, "raw/retry.gz", "temporary outage", 1)
    after = time.time()

    assert status == "discovered"
    row = con.execute(
        "SELECT attempts, error_kind, next_attempt_at FROM ingest_ledger "
        "WHERE service_id=? AND object_key='raw/retry.gz'",
        (service_id,),
    ).fetchone()
    assert row["attempts"] == 1
    assert row["error_kind"] == "transient"
    assert before + LEDGER_RETRY_BASE_S <= row["next_attempt_at"] <= after + LEDGER_RETRY_BASE_S


def test_sweeper_only_redispatches_retry_rows_when_due():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    now = time.time()
    cur.execute(
        "INSERT INTO ingest_ledger "
        "(service_id, object_key, status, discovered_at, next_attempt_at) "
        "VALUES (?, 'raw/not-due.gz', 'discovered', ?, ?)",
        (service_id, now - 3600, now + 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"bucket": "test-bucket"}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    assert summary["redispatched"] == 0
    mock_delay.assert_not_called()

    con.execute(
        "UPDATE ingest_ledger SET next_attempt_at=? WHERE service_id=? AND object_key='raw/not-due.gz'",
        (time.time() - 1, service_id),
    )
    con.commit()
    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"bucket": "test-bucket"}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    assert summary["redispatched"] == 1
    mock_delay.assert_called_once()


def test_lease_generation_fences_stale_failure():
    """A reclaimed worker cannot fail a newer regular/RUM claim."""
    con, cur = _clear_ledger("test-celery-svc")
    for object_key in ("raw/request/log.gz", "raw/rum/beacon.gz"):
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, status, claimed_by, claimed_at, lease_generation, discovered_at) "
            "VALUES (?, ?, 'claimed', 'new-worker', ?, 2, ?)",
            ("test-celery-svc", object_key, time.time(), time.time()),
        )
    con.commit()

    for object_key in ("raw/request/log.gz", "raw/rum/beacon.gz"):
        assert _ledger_record_failure(con, "test-celery-svc", object_key, "stale error", 1) == "stale"

    rows = con.execute(
        "SELECT object_key, status, claimed_by, lease_generation, attempts "
        "FROM ingest_ledger WHERE service_id=? ORDER BY object_key",
        ("test-celery-svc",),
    ).fetchall()
    assert [(r["status"], r["claimed_by"], r["lease_generation"], r["attempts"]) for r in rows] == [
        ("claimed", "new-worker", 2, 0),
        ("claimed", "new-worker", 2, 0),
    ]


def _read_expr_for(raw_file, columns_sql="{'timestamp': 'VARCHAR', 'url': 'VARCHAR'}") -> str:
    from backend.utils.sql_validator import escape_sql_literal

    safe_local = escape_sql_literal(str(raw_file))
    return (
        f"read_json_auto('{safe_local}', format='newline_delimited', "
        f"records='auto', columns={columns_sql}, ignore_errors=true)"
    )


def test_quarantine_convert_corrupt_lines_captures_local_evidence(tmp_path):
    """A malformed JSON line: ignore_errors=true does NOT drop it, it
    inserts an all-NULL row — that NULL-timestamp row is the actual corruption
    signal this function must detect and quarantine."""
    raw_file = tmp_path / "raw.json"
    valid_line_1 = _json.dumps({"timestamp": "2026-01-01T00:00:00Z", "url": "/a"})
    valid_line_2 = _json.dumps({"timestamp": "2026-01-01T00:00:01Z", "url": "/b"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:02Z", "url": "/c"'  # truncated JSON
    raw_file.write_text(f"{valid_line_1}\n{valid_line_2}\n{corrupt_line}\n")

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)
    assert con.execute(f"SELECT count(*) FROM {read_expr}").fetchone()[0] == 3
    assert con.execute(f"SELECT count(*) FROM {read_expr} WHERE timestamp IS NULL").fetchone()[0] == 1

    fos = MagicMock()
    src = {"service_id": "svc-quarantine", "name": "svc-quarantine", "bucket": "test-bucket", "prefix": ""}

    with patch(
        "backend.core.quarantine.capture_evidence",
        return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
    ) as capture:
        result = _quarantine_convert_corrupt_lines(
            con, fos, src, read_expr, str(raw_file), "raw/request/year=2026/file.gz"
        )

    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 2,
        "malformed_records": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }
    assert capture.call_args.args[0:3] == (
        "svc-quarantine",
        "request",
        "raw/request/year=2026/file.gz",
    )
    assert capture.call_args.args[3] == (corrupt_line + "\n").encode()


def test_quarantine_convert_corrupt_lines_noop_when_nothing_dropped(tmp_path):
    """The common case (no corrupt lines) performs no evidence writes."""
    raw_file = tmp_path / "raw.json"
    raw_file.write_text('{"timestamp": "2026-01-01T00:00:00Z", "url": "/a"}\n')

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)

    fos = MagicMock()
    src = {"service_id": "svc-clean", "name": "svc-clean", "bucket": "test-bucket", "prefix": ""}

    result = _quarantine_convert_corrupt_lines(con, fos, src, read_expr, str(raw_file), "raw/request/file.gz")
    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 1,
        "malformed_records": 0,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }


def test_convert_batch_excludes_null_timestamp_rows_from_lake(tmp_path):
    """End-to-end guard: a corrupt line must never land in the queryable lake
    table, even though ignore_errors=true alone would have put a NULL row there."""
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    object_key = "raw/2026/08/27/10/09/mixed.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    raw_file = tmp_path / "mixed.json"
    valid_line = _json.dumps({"timestamp": "2026-01-01T00:00:00Z"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:01Z"'  # truncated
    raw_file.write_text(f"{valid_line}\n{corrupt_line}\n")
    s3_map = {f"s3://test-bucket/{object_key}": str(raw_file)}

    with DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local=s3_map) as env:
        summary = convert_batch_objects(service_id, [object_key], "test-worker")

    assert summary["committed"] == 1
    check_con = lake_reader(env, read_only=True)
    try:
        rows = check_con.execute("SELECT count(*) FROM lake.logs").fetchone()[0]
        null_rows = check_con.execute("SELECT count(*) FROM lake.logs WHERE timestamp IS NULL").fetchone()[0]
        assert rows == 1, "only the valid row should have been written to the lake table"
        assert null_rows == 0, "a corrupt row must never land in the queryable lake table"
    finally:
        check_con.close()

    from backend.core.metadata.ingest_log import get_ingested_files_status_summary

    ingested = get_ingested_files_status_summary(service_id)
    assert ingested.get("total_rows") == 1, "ingested_files must record only the valid row, not the corrupt one"


def test_sweep_dead_letter_count_excludes_rum_objects():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    now = time.time()
    for key, status in (
        ("raw/request/2026/08/27/10/a.gz", "dead_letter"),
        ("raw/rum/2026/08/27/10/b.gz", "dead_letter"),
        ("raw/rum/2026/08/27/10/c.gz", "quarantined"),
    ):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?)",
            (service_id, key, status, now),
        )
    con.commit()

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
    ):
        summary = sweep_ledger_once(service_id)

    assert summary["dead_letter"] == 1
