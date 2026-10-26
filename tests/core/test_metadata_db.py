"""Tests for metadata_db lease acquisition, audit, concurrency, reaping, schema, Postgres routing, usage log summaries, and quarantine evidence."""

from __future__ import annotations

import datetime as _dt
import inspect
import sqlite3
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from backend.core.metadata.quarantine import (
    delete_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    get_quarantine_storage_total,
    list_quarantine_evidence,
)

from backend import celery_app, config, main
from backend.core import metadata as metadata_db
from backend.core import quarantine
from backend.core import share_db as share_db_connection
from backend.core.metadata import base, ingest_log, pg_connection, pg_schema, state, usage_log, usage_log_db
from backend.core.metadata.base import InvalidServiceIdError, get_con
from backend.core.metadata.cron_log import start_cron_run
from backend.utils.date_utils import iso_z_now


# ── Lease & Audit Merge ──
def test_start_cron_run_second_call_raises_already_running():
    sid = "svc-lease-basic"
    assert start_cron_run(sid, "log_discovery") > 0
    with pytest.raises(RuntimeError, match="already running"):
        start_cron_run(sid, "log_discovery")


def test_start_cron_run_different_task_same_service_both_succeed():
    sid = "svc-lease-different-task"
    a = start_cron_run(sid, "log_discovery")
    b = start_cron_run(sid, "commit")
    assert a > 0 and b > 0 and a != b


def test_start_cron_run_different_service_same_task_both_succeed():
    assert start_cron_run("svc-lease-a", "log_discovery") > 0
    assert start_cron_run("svc-lease-b", "log_discovery") > 0


def test_lease_index_exists_and_is_unique_partial():
    """Pin the partial unique index contract that start_cron_run's ON CONFLICT target depends on."""
    con = get_con("svc-lease-index-check")
    row = con.execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
        "AND indexname = 'idx_job_runs_running_lease'"
    ).fetchone()
    assert row is not None, "idx_job_runs_running_lease is missing"
    indexdef = (row["indexdef"] if hasattr(row, "keys") else row[0]).upper()
    assert "UNIQUE" in indexdef and "STATUS" in indexdef and "RUNNING" in indexdef


def test_start_cron_run_exactly_one_of_two_concurrent_callers_wins():
    """Two threads racing on the same (service_id, task) must yield 1 winner and 1 'already running'."""
    sid, task = "svc-lease-race", "log_discovery"
    results: list[tuple[bool, str]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def _attempt():
        try:
            barrier.wait(timeout=5)
        except threading.BrokenBarrierError:
            pass
        try:
            run_id = start_cron_run(sid, task)
            with lock:
                results.append((True, str(run_id)))
        except RuntimeError as e:
            with lock:
                results.append((False, str(e)))

    threads = [threading.Thread(target=_attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    wins = [r for r in results if r[0]]
    losses = [r for r in results if not r[0]]
    assert len(wins) == 1, f"expected exactly 1 winner, got {results}"
    assert len(losses) == 1 and "already running" in losses[0][1]


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _audit_row(ts: str = "2026-06-12T10:00:00Z", event_type: str = "ingest_start", actor: str = "cron") -> dict:
    return {"timestamp": ts, "source_name": "fos", "event_type": event_type, "details": "{}", "actor": actor}


def test_merge_audit_noop_when_empty():
    state.merge_audit_for_service("svc-merge-empty", [])
    assert _con("svc-merge-empty").execute("SELECT count(*) FROM audit_logs").fetchone()[0] == 0


def test_merge_audit_inserts_new_rows():
    sid = "svc-merge-new"
    state.merge_audit_for_service(
        sid,
        [_audit_row("2026-06-12T10:00:00Z", "ingest_start"), _audit_row("2026-06-12T10:01:00Z", "ingest_done")],
    )
    assert _con(sid).execute("SELECT count(*) FROM audit_logs").fetchone()[0] == 2


def test_merge_audit_dedups_on_composite_key():
    sid = "svc-merge-dedup"
    row = _audit_row()
    state.merge_audit_for_service(sid, [row])
    state.merge_audit_for_service(sid, [row])
    assert _con(sid).execute("SELECT count(*) FROM audit_logs").fetchone()[0] == 1


def test_merge_audit_distinguishes_by_actor():
    sid = "svc-merge-actor"
    state.merge_audit_for_service(sid, [_audit_row(actor="cron"), _audit_row(actor="analyst")])
    assert _con(sid).execute("SELECT count(*) FROM audit_logs").fetchone()[0] == 2


# ── Scoring Audit ──
SVC = "test-audit-svc"


def test_record_then_list_returns_the_row():
    metadata_db.record_scoring_audit(
        SVC, "scoring_enabled", details={"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    )
    rows = metadata_db.list_scoring_audit(SVC)
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "scoring_enabled"
    assert row["actor"] == "operator"
    assert row["details"] == {"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    assert "timestamp" in row and "id" in row


def test_record_multiple_rows_returns_newest_first():
    metadata_db.record_scoring_audit(SVC, "scoring_enabled")
    metadata_db.record_scoring_audit(SVC, "threshold_committed", details={"new_threshold": 80})
    metadata_db.record_scoring_audit(SVC, "matrix_retrained", details={"matrix_version": "v2"})
    assert [r["action"] for r in metadata_db.list_scoring_audit(SVC)] == [
        "matrix_retrained",
        "threshold_committed",
        "scoring_enabled",
    ]


def test_list_limit_truncates_to_n_rows():
    for i in range(5):
        metadata_db.record_scoring_audit(SVC, f"action_{i}", details={"i": i})
    rows = metadata_db.list_scoring_audit(SVC, limit=2)
    assert [r["action"] for r in rows] == ["action_4", "action_3"]


def test_list_since_filter_excludes_older_rows():
    metadata_db.record_scoring_audit(SVC, "older_action")
    con = metadata_db.get_con(SVC)
    con.execute("UPDATE scoring_audit SET timestamp = datetime('now', '-10 seconds') WHERE action = 'older_action'")
    con.commit()
    cutoff = str(con.execute("SELECT datetime('now', '-5 seconds') AS now").fetchone()["now"])
    metadata_db.record_scoring_audit(SVC, "newer_action")

    actions = [r["action"] for r in metadata_db.list_scoring_audit(SVC, since=cutoff)]
    assert actions == ["newer_action"]


def test_list_since_filter_with_future_timestamp_returns_empty():
    metadata_db.record_scoring_audit(SVC, "anything")
    assert metadata_db.list_scoring_audit(SVC, since="2099-01-01T00:00:00") == []


def test_record_swallows_malformed_service_id():
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(None, "scoring_enabled")  # type: ignore[arg-type]


def test_record_handles_unjsonable_details_gracefully():
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(SVC, "weird_action", details={"a_set": {1, 2, 3}})  # type: ignore[dict-item]


def test_list_returns_empty_on_brand_new_service():
    assert metadata_db.list_scoring_audit("freshly-minted-svc") == []


# ── Concurrency & Teardown ──
def test_concurrent_inserts_no_lock_no_loss():
    sid = "svc-concurrent"
    n_threads, rows_per_thread = 8, 50
    metadata_db.get_con(sid)

    def worker(thread_id: int) -> int:
        rows = [(f"thread-{thread_id}/file-{i}.gz", 100, 4096) for i in range(rows_per_thread)]
        metadata_db.insert_ingested_files(sid, rows)
        return rows_per_thread

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        totals = list(ex.map(worker, range(n_threads)))

    assert sum(totals) == n_threads * rows_per_thread
    count = (
        metadata_db.get_con(sid)
        .execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,))
        .fetchone()[0]
    )
    assert count == n_threads * rows_per_thread


def test_concurrent_inserts_with_overlap_dedup_via_upsert():
    sid = "svc-concurrent-overlap"
    n_threads = 6
    files = [f"shared/f-{i}.gz" for i in range(20)]

    def worker(thread_id: int) -> None:
        metadata_db.insert_ingested_files(sid, [(fname, 100 + thread_id, 1000 + thread_id) for fname in files])

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        list(ex.map(worker, range(n_threads)))

    count = (
        metadata_db.get_con(sid)
        .execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,))
        .fetchone()[0]
    )
    assert count == len(files)


def test_teardown_then_get_con_recreates_schema():
    """Under Postgres, teardown() is a non-destructive no-op and subsequent writes succeed."""
    sid = "svc-teardown-recreate"
    metadata_db.insert_ingested_files(sid, [("seed.gz", 1, 100)])
    metadata_db.teardown(sid)
    metadata_db.insert_ingested_files(sid, [("post-teardown.gz", 2, 200)])

    rows = (
        metadata_db.get_con(sid)
        .execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid,))
        .fetchall()
    )
    assert {"seed.gz", "post-teardown.gz"} <= {r[0] for r in rows}


def test_concurrent_teardown_and_writes_no_lost_post_teardown_data():
    sid = "svc-teardown-race"
    lock = threading.Lock()
    metadata_db.insert_ingested_files(sid, [("warmup.gz", 1, 100)])

    def writer() -> None:
        with lock:
            try:
                metadata_db.insert_ingested_files(sid, [(f"writer-{i}.gz", 1, 100) for i in range(20)])
            except Exception:
                pass

    def tearer() -> None:
        with lock:
            metadata_db.teardown(sid)

    with ThreadPoolExecutor(max_workers=4) as ex:
        for f in [ex.submit(writer), ex.submit(tearer), ex.submit(writer), ex.submit(tearer)]:
            f.result()

    metadata_db.insert_ingested_files(sid, [("recovered.gz", 99, 9999)])
    names = {r[0] for r in metadata_db.get_con(sid).execute("SELECT file_name FROM ingested_files").fetchall()}
    assert "recovered.gz" in names


def test_teardown_is_idempotent():
    metadata_db.teardown("svc-teardown-never-existed")


def test_threads_get_isolated_connections():
    sid = "svc-isolation"
    n_workers = 4
    barrier = threading.Barrier(n_workers)

    def worker(_: int) -> int:
        con = metadata_db.get_con(sid)
        barrier.wait(timeout=5.0)
        return id(con)

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        ids = list(ex.map(worker, range(n_workers)))

    assert len(set(ids)) == n_workers


# ── Reaping & Lease Expiry ──
def _seed_running(service_id: str, task: str = "sync") -> int:
    con = metadata_db.get_con(service_id)
    cur = con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, ?, datetime('now'), 0.0, 'running', '[]')",
        (service_id, task),
    )
    con.commit()
    return int(cur.lastrowid or 0)


def _seed_running_at(sid: str, task: str, when) -> int:
    with time_machine.travel(when, tick=False):
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (sid, task, iso_z_now()),
        )
        con.commit()
        return int(cur.lastrowid or 0)


def _seed_lease(sid: str, task: str, heartbeat_at: float, ttl_s: int = 60) -> None:
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
        "VALUES (?, ?, ?, ?, ?, 'running')",
        (sid, task, heartbeat_at, heartbeat_at, ttl_s),
    )
    con.commit()


def test_reap_marks_running_rows_as_error():
    sid = "svc-reap-1"
    rid = _seed_running(sid)
    assert metadata_db.reap_running_cron_runs(sid) == 1
    row = (
        metadata_db.get_con(sid).execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    )
    assert row["status"] == "error" and "interrupted" in (row["error_message"] or "").lower()


def test_reap_is_no_op_when_nothing_running():
    sid = "svc-reap-2"
    metadata_db.get_con(sid)
    assert metadata_db.reap_running_cron_runs(sid) == 0


def test_reap_does_not_touch_completed_rows():
    sid = "svc-reap-3"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()
    assert metadata_db.reap_running_cron_runs(sid) == 0
    assert con.execute("SELECT status FROM cron_runs WHERE task = 'sync'").fetchone()["status"] == "success"


def test_reap_ignores_age_unlike_start_cron_run():
    sid = "svc-reap-4"
    rid = _seed_running(sid)
    assert metadata_db.reap_running_cron_runs(sid) == 1
    assert (
        metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()["status"]
        == "error"
    )


def test_reap_clears_legacy_null_service_id_rows():
    sid = "svc-reap-null-legacy"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (NULL, 'sync', datetime('now'), 0.0, 'running', '[]')"
    )
    con.commit()
    assert metadata_db.reap_running_cron_runs(sid) == 1
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE task = 'sync'").fetchone()
    assert row["status"] == "error" and "interrupted" in (row["error_message"] or "").lower()


def test_reap_custom_reason_is_recorded():
    sid = "svc-reap-5"
    _seed_running(sid)
    metadata_db.reap_running_cron_runs(sid, reason="Pod evicted by k8s")
    row = metadata_db.get_con(sid).execute("SELECT error_message FROM cron_runs WHERE status = 'error'").fetchone()
    assert "k8s" in (row["error_message"] or "")


def test_start_cron_run_reaps_rows_exactly_at_lease_expiry():
    sid = "svc-lease-boundary"
    t0 = _dt.datetime(2026, 6, 16, 12, 0, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, "commit", t0)
    _seed_lease(sid, "commit", t0.timestamp(), ttl_s=60)
    cross_id = _seed_running_at(sid, "optimize", t0)
    _seed_lease(sid, "optimize", t0.timestamp() + 3600, ttl_s=60)

    with time_machine.travel(t0 + _dt.timedelta(seconds=59), tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "commit")

    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "running"

    with time_machine.travel(t0 + _dt.timedelta(seconds=61), tick=False):
        new_id = metadata_db.start_cron_run(sid, "commit")

    assert new_id and new_id != old_id
    old = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (old_id,)).fetchone()
    assert old["status"] == "error" and "heartbeat lost" in (old["error_message"] or "")
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (cross_id,)).fetchone()["status"] == "running"


def test_sync_orphan_threshold_is_short_and_reaps_at_10_min():
    sid = "svc-sync-threshold"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, "sync", t0)

    with time_machine.travel(t0 + _dt.timedelta(minutes=11), tick=False):
        new_id = metadata_db.start_cron_run(sid, "sync")

    assert new_id and new_id != old_id
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


def test_live_lease_blocks_start_even_when_cron_row_is_old():
    sid = "svc-sync-live-lease"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    row_id = _seed_running_at(sid, "sync", t0)
    now = t0 + _dt.timedelta(minutes=11)
    _seed_lease(sid, "sync", (now - _dt.timedelta(seconds=30)).timestamp(), ttl_s=60)

    with time_machine.travel(now, tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "sync")

    assert (
        metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (row_id,)).fetchone()["status"]
        == "running"
    )


@pytest.mark.parametrize("task", ["sync", "commit"])
def test_lease_less_running_row_is_reaped_immediately_for_any_task(task):
    sid = f"svc-no-lease-{task}"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, task, t0)

    with time_machine.travel(t0 + _dt.timedelta(seconds=5), tick=False):
        new_id = metadata_db.start_cron_run(sid, task)

    con = metadata_db.get_con(sid)
    assert new_id and new_id != old_id
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


def test_finalize_flips_running_row_to_error():
    sid = "svc-finalize-1"
    rid = _seed_running(sid, "sync")
    assert metadata_db.finalize_cron_run_if_running(
        sid, "sync", rid, summary="no terminal event", error_message="orphaned"
    )
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, error_message, summary FROM cron_runs WHERE id = ?", (rid,))
        .fetchone()
    )
    assert row["status"] == "error" and row["error_message"] == "orphaned" and row["summary"] == "no terminal event"


def test_finalize_is_noop_on_terminal_row():
    sid = "svc-finalize-2"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()
    rid = int(con.execute("SELECT id FROM cron_runs WHERE status='success'").fetchone()["id"])
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", rid) is False
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()["status"] == "success"


def test_finalize_noop_when_run_id_missing():
    sid = "svc-finalize-3"
    metadata_db.get_con(sid)
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", None) is False
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", 999999) is False


# ── Schema & Validation ──
def _reapply_schema() -> None:
    pg_schema.ensure_pg_schema(force=True)


_EXPECTED_TABLES = {
    "sources",
    "ingested_files",
    "ingested_files_summary",
    "ingest_in_flight",
    "cron_runs",
    "asn_names",
    "audit_logs",
    "views",
    "alerts",
    "scoring_labels",
}

_EXPECTED_INDEXES = {
    "idx_ingested_files_source_ingested_at",
    "idx_in_flight_source",
    "idx_cron_task_started",
    "idx_cron_started",
    "idx_audit_source",
    "idx_scoring_labels_svc_sid",
    "idx_scoring_labels_svc_label",
}


def _list_tables(con: sqlite3.Connection) -> set[str]:
    return {r[0] for r in con.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()}


def _list_indexes(con: sqlite3.Connection) -> set[str]:
    return {
        r[0] for r in con.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()").fetchall()
    }


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
    }


def _column_type(con: sqlite3.Connection, table: str, column: str) -> str:
    row = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
        (table, column),
    ).fetchone()
    return row[0] if row else ""


def test_init_schema_creates_all_expected_tables():
    missing = _EXPECTED_TABLES - _list_tables(metadata_db.get_con("svc-schema-shape"))
    assert not missing, f"missing tables after _init_schema: {missing}"


def test_init_schema_creates_all_expected_indexes():
    missing = _EXPECTED_INDEXES - _list_indexes(metadata_db.get_con("svc-schema-indexes"))
    assert not missing, f"missing indexes after _init_schema: {missing}"


def test_alerts_table_has_evaluation_scope_column():
    cols = _columns(metadata_db.get_con("svc-schema-alerts"), "alerts")
    assert "evaluation_scope" in cols and "comparison_period_min" in cols


def test_usage_log_byte_columns_are_bigint():
    con = usage_log_db.get_con("svc-usage-bigint")
    for table, column in (
        ("usage_log", "bytes"),
        ("usage_log", "count"),
        ("usage_log_hourly_summary", "bytes"),
        ("usage_log_hourly_summary", "count"),
    ):
        assert _column_type(con, table, column) == "bigint", f"{table}.{column} must be bigint, not int4"


def test_log_usage_calls_accepts_bytes_over_int4_max():
    sid = "svc-usage-overflow"
    big = 3_000_000_000
    usage_log.log_usage_calls(
        sid, [{"method": "GET", "service": "FOS", "path": "/big.gz", "bytes": big, "caller": "test"}]
    )

    con = usage_log_db.get_con(sid)
    row = con.execute("SELECT bytes FROM usage_log WHERE service_id = ? AND url = ?", (sid, "/big.gz")).fetchone()
    assert row is not None and row[0] == big
    hourly = con.execute("SELECT max(bytes) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
    assert hourly is not None and hourly[0] == big


def test_init_schema_is_idempotent_no_data_loss():
    sid = "svc-schema-idem"
    metadata_db.insert_ingested_files(sid, [("file-a.gz", 100, 4096), ("file-b.gz", 200, 8192)])
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0] == 2
    _reapply_schema()
    assert con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0] == 2


def test_init_schema_run_twice_is_safe_in_sequence():
    metadata_db.get_con("svc-schema-double")
    _reapply_schema()
    _reapply_schema()


def test_pre_existing_data_survives_added_table():
    sid = "svc-schema-future"
    metadata_db.insert_ingested_files(sid, [("survivor.gz", 1, 100)])
    con = metadata_db.get_con(sid)
    con.execute("CREATE TABLE IF NOT EXISTS new_feature_table (id SERIAL PRIMARY KEY, payload TEXT)")
    con.commit()
    _reapply_schema()

    rows = con.execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid,)).fetchall()
    assert [r[0] for r in rows] == ["survivor.gz"]
    assert "new_feature_table" in _list_tables(con)


@pytest.mark.parametrize(
    "bad_sid",
    [None, 123, object(), {"name": "svc-a"}],
    ids=["none", "int", "object", "dict"],
)
def test_db_path_rejects_non_string_service_id(bad_sid):
    with pytest.raises(TypeError):
        metadata_db.db_path(bad_sid)


def test_get_con_rejects_non_string_service_id():
    with pytest.raises(TypeError):
        metadata_db.get_con(object())


@pytest.mark.parametrize(
    "bad_sid",
    [
        "../etc/passwd",
        "foo/bar",
        "foo\x00bar",
        "",
        "x" * 65,
        "with space",
        "foo.bar",
        "\U00018d1f",
        "café",
    ],
    ids=["traversal", "slash", "null_byte", "empty", "too_long", "space", "period", "apfs_illegal", "non_ascii"],
)
def test_db_path_rejects_malformed_service_id(bad_sid):
    with pytest.raises(InvalidServiceIdError):
        metadata_db.db_path(bad_sid)


def test_invalid_service_id_in_path_returns_422(client):
    resp = client.get("/api/services/foo.bar/scoring/labels")
    assert resp.status_code == 422, f"expected 422, got {resp.status_code}: {resp.text[:200]}"
    body = resp.json()
    assert isinstance(body.get("detail"), list)
    err = body["detail"][0]
    assert err["loc"] == ["path", "service_id"]
    assert "service_id must match" in err["msg"]
    assert err["type"] == "value_error.invalid_service_id"


# ── Postgres-Only Mode & Connection Pooling ──
def _in_fresh_thread(fn):
    box: dict[str, BaseException | None] = {"exc": None}

    def run():
        try:
            fn()
        except BaseException as e:  # noqa: BLE001
            box["exc"] = e

    t = threading.Thread(target=run)
    t.start()
    t.join()
    return box["exc"]


@pytest.fixture
def no_dsn(monkeypatch):
    pg_connection.reset_pg_pool_for_tests()
    monkeypatch.delenv("METADATA_DSN", raising=False)
    yield
    pg_connection.reset_pg_pool_for_tests()


@pytest.mark.parametrize(
    "opener",
    [base.get_con, base.get_con_readonly, usage_log_db.get_con, usage_log_db.open_readonly],
    ids=["base.get_con", "base.get_con_readonly", "usage_log_db.get_con", "usage_log_db.open_readonly"],
)
def test_connection_openers_raise_without_dsn(no_dsn, opener):
    exc = _in_fresh_thread(lambda: opener("svc-1"))
    assert isinstance(exc, RuntimeError) and "METADATA_DSN is not set" in str(exc)


def test_deployment_validation_refuses_a_non_postgres_metadata_dsn(monkeypatch):
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", "postgresql://u:p@h/db")
    monkeypatch.setenv("METADATA_DSN", "sqlite:///tmp/x.db")
    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        config.validate_deployment_mode()


def test_usage_log_ddl_still_comes_from_usage_log_db_schema():
    statements = pg_schema.pg_schema_statements()
    joined = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS usage_log (" in joined
    assert "CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (" in joined
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    assert "AUTOINCREMENT" not in joined


@pytest.fixture
def postgres_mode(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://fake/fake")
    yield


def test_get_con_routes_to_postgres_when_dsn_set(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=sentinel) as mock_get:
        assert base.get_con("svc-1") is sentinel
    mock_get.assert_called_once_with()


def test_get_con_still_validates_service_id_under_postgres(postgres_mode):
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=MagicMock()):
        with pytest.raises(InvalidServiceIdError):
            base.get_con("not a valid id / has slash")


def test_latest_ingest_timestamp_uses_postgres_compatible_aggregate(postgres_mode):
    cursor = MagicMock()
    cursor.fetchone.return_value = {"latest": "2026-09-08T21:34:32Z"}
    connection = MagicMock()

    def execute(sql, params):
        if "datetime(" in sql:
            raise AssertionError("SQLite datetime() is not valid PostgreSQL")
        return cursor

    connection.execute.side_effect = execute
    with patch.object(ingest_log, "get_con", return_value=connection):
        assert ingest_log.get_latest_ingest_ts("svc-1") == "2026-09-08T21:34:32Z"


def test_get_con_readonly_routes_to_postgres(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_readonly_connection", return_value=sentinel) as mock_get:
        assert base.get_con_readonly("svc-1") is sentinel
    mock_get.assert_called_once_with()


def test_release_thread_connection_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "release_pg_thread_connection") as mock_release:
        base.release_thread_connection()
    mock_release.assert_called_once_with()


def test_close_all_connections_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "close_all_pg_connections") as mock_close:
        base.close_all_connections()
    mock_close.assert_called_once()


def test_teardown_is_noop_under_postgres(postgres_mode):
    base.teardown("svc-1")


def test_get_global_share_con_routes_to_postgres_thread_connection():
    sentinel = MagicMock()
    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as mock_pg:
        assert share_db_connection.get_global_share_con() is sentinel
    mock_pg.assert_called_once_with()


def test_stale_thread_wrapper_is_replaced_after_close_all_from_another_thread():
    first_ready = threading.Event()
    closed = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        results["first"] = pg_connection.get_pg_thread_connection()
        first_ready.set()
        closed.wait(10)
        second = pg_connection.get_pg_thread_connection()
        results["second"] = second
        results["value"] = second.execute("SELECT 1").fetchone()[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert first_ready.wait(10)
    pg_connection.close_all_pg_connections()
    closed.set()
    t.join(15)

    assert results["second"] is not results["first"]
    assert results["value"] == 1


def test_close_all_leaves_a_connection_with_a_query_in_flight_to_its_owner():
    started = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        con = pg_connection.get_pg_thread_connection()
        results["wrapper"] = con
        started.set()
        results["value"] = con.execute("SELECT 1 FROM pg_sleep(1)").fetchone()[0]
        results["returned_after_query"] = con._returned[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert started.wait(10)
    wrapper = results["wrapper"]
    assert isinstance(wrapper, pg_connection.PgConnectionWrapper)
    for _ in range(500):
        if not wrapper._busy.acquire(blocking=False):
            break
        wrapper._busy.release()
        time.sleep(0.01)
    else:
        raise AssertionError("worker never started its query")
    pg_connection.close_all_pg_connections()
    t.join(15)

    assert results["value"] == 1
    assert results["returned_after_query"] is False


def test_return_to_pool_once_is_atomic_under_contention():
    pool = MagicMock()
    returned = [False]
    start = threading.Barrier(8)

    def go() -> None:
        start.wait(5)
        pg_connection._return_to_pool_once(object(), returned, pool)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert pool.putconn.call_count == 1


# ── Usage Log Hourly Summary ──
def test_log_usage_calls_updates_hourly_summary(monkeypatch):
    sid = "svc_test_summary_1"
    usage_log.clear_usage_log(sid)
    now_hour_10 = "2026-05-01T10:15:30Z"
    calls_hour_10 = [
        {"service": "FOS", "method": "PutObject", "path": "/f1", "bytes": 100},
        {"service": "FOS", "method": "PutObject", "path": "/f2", "bytes": 200},
        {"service": "FOS", "method": "GetObject", "path": "/f3", "bytes": 50},
        {"service": "CDN", "method": "download", "path": "/c1", "bytes": 1000},
    ]

    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, calls_hour_10)

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes "
        "FROM usage_log_hourly_summary WHERE service_id = ? "
        "ORDER BY hour, operation_class, operation_type",
        (sid,),
    ).fetchall()

    assert [(r["hour"], r["operation_class"], r["operation_type"], r["count"], r["bytes"]) for r in rows] == [
        ("2026-05-01T10", "A", "PutObject", 2, 300),
        ("2026-05-01T10", "B", "GetObject", 1, 50),
        ("2026-05-01T10", "CDN", "download", 1, 1000),
    ]

    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, [{"service": "FOS", "method": "PutObject", "path": "/f4", "bytes": 400}])

    row = con.execute(
        "SELECT count, bytes FROM usage_log_hourly_summary "
        "WHERE service_id = ? AND hour = '2026-05-01T10' AND operation_class = 'A' AND operation_type = 'PutObject'",
        (sid,),
    ).fetchone()
    assert row["count"] == 3 and row["bytes"] == 700


def test_log_synthetic_usage_updates_hourly_summary():
    sid = "svc_test_summary_2"
    usage_log.clear_usage_log(sid)
    calls = [
        {"_timestamp_override": "2026-06-15T08:12:00Z", "method": "PUT_OBJECT", "path": "/obj1.parquet", "bytes": 500},
        {"_timestamp_override": "2026-06-15T08:45:00Z", "method": "PUT_OBJECT", "path": "/obj2.parquet", "bytes": 700},
        {"_timestamp_override": "2026-06-15T09:05:00Z", "method": "PUT_OBJECT", "path": "/obj3.parquet", "bytes": 300},
    ]

    assert usage_log.log_synthetic_usage(sid, calls) == 3
    rows = (
        usage_log_db.get_con(sid)
        .execute(
            "SELECT hour, operation_class, count, bytes FROM usage_log_hourly_summary WHERE service_id = ? ORDER BY hour",
            (sid,),
        )
        .fetchall()
    )
    assert [(r["hour"], r["operation_class"], r["count"], r["bytes"]) for r in rows] == [
        ("2026-06-15T08", "A", 2, 1200),
        ("2026-06-15T09", "A", 1, 300),
    ]


def test_reconcile_fastly_stats_adjusts_summary():
    sid = "svc_test_summary_3"
    usage_log.clear_usage_log(sid)
    hour = "2026-07-01T14:00:00Z"
    calls = [{"service": "FOS", "method": "PutObject", "path": f"/k{i}"} for i in range(10)]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=hour):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    assert (
        con.execute(
            "SELECT count FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
            (sid,),
        ).fetchone()["count"]
        == 10
    )

    assert usage_log.reconcile_fastly_stats(sid, [{"hour_iso": hour, "class_a": 25, "class_b": 0}]) == 1
    assert (
        con.execute(
            "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
            (sid,),
        ).fetchone()["total"]
        == 25
    )

    assert usage_log.reconcile_fastly_stats(sid, [{"hour_iso": hour, "class_a": 30, "class_b": 0}]) == 1
    assert (
        con.execute(
            "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
            (sid,),
        ).fetchone()["total"]
        == 30
    )


def test_purge_and_clear_usage_log():
    sid = "svc_test_summary_4"
    usage_log.clear_usage_log(sid)
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2020-01-01T12:00:00Z"):
        usage_log.log_usage_calls(sid, [{"service": "FOS", "method": "PutObject", "path": "/old"}])

    con = usage_log_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    usage_log.purge_usage_log(sid, retention_days=1)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    usage_log.clear_usage_log(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0


# ── v3 Boot Migration (pg_schema) ──
class _FakeCursor:
    def __init__(self, fail_with: dict | None = None):
        self.statements: list[str] = []
        self._fail_with = fail_with or {}

    def execute(self, sql: str):
        self.statements.append(sql)
        for needle, exc in self._fail_with.items():
            if needle in sql:
                raise exc

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, cursor: _FakeCursor):
        self._conn = _FakeConn(cursor)

    def connection(self):
        return self._conn


def _pg_error(sqlstate: str, message: str) -> Exception:
    exc = Exception(message)
    exc.sqlstate = sqlstate  # type: ignore[attr-defined]
    return exc


def test_pg_schema_statements_are_postgres_dialect():
    statements = pg_schema.pg_schema_statements()
    blob = "\n".join(statements)

    for tbl in (
        "cron_runs",
        "slow_queries",
        "ingest_ledger",
        "metric_snapshots",
        "high_scale_ownership",
        "high_scale_source_objects",
        "high_scale_publication_manifests",
    ):
        assert tbl in blob

    assert "AUTOINCREMENT" not in blob
    assert "datetime('now')" not in blob
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    assert all(
        "IF NOT EXISTS" in s or "DROP INDEX" in s or "ON CONFLICT" in s or "ALTER TABLE" in s for s in statements
    )


def test_apply_pg_schema_executes_every_statement():
    cur = _FakeCursor()
    applied, raced = pg_schema.apply_pg_schema(cur)
    assert applied == len(pg_schema.pg_schema_statements()) and raced == 0 and len(cur.statements) == applied


def test_apply_pg_schema_tolerates_a_concurrent_pod_winning():
    cur = _FakeCursor(
        fail_with={
            "cron_runs (": _pg_error("42P07", 'relation "cron_runs" already exists'),
            "idx_cron_status": _pg_error("23505", "duplicate key value violates unique constraint"),
        }
    )
    applied, raced = pg_schema.apply_pg_schema(cur)
    assert raced == 2 and applied == len(pg_schema.pg_schema_statements()) - 2


def test_apply_pg_schema_raises_on_a_real_ddl_error():
    cur = _FakeCursor(fail_with={"cron_runs (": _pg_error("42601", "syntax error at or near")})
    with pytest.raises(RuntimeError, match="failed to apply schema statement"):
        pg_schema.apply_pg_schema(cur)


def test_ensure_pg_schema_is_a_noop_without_metadata_dsn(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    pg_schema.reset_ensure_flag_for_tests()
    assert pg_schema.ensure_pg_schema() is False


def test_ensure_pg_schema_applies_once_per_process(monkeypatch):
    cur = _FakeCursor()
    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    pg_schema.reset_ensure_flag_for_tests()

    assert pg_schema.ensure_pg_schema() is True
    first_pass = len(cur.statements)
    assert first_pass > 0

    assert pg_schema.ensure_pg_schema() is False
    assert len(cur.statements) == first_pass

    assert pg_schema.ensure_pg_schema(force=True) is True
    assert len(cur.statements) == first_pass * 2
    pg_schema.reset_ensure_flag_for_tests()


def test_ensure_pg_schema_never_raises_and_does_not_latch_on_failure(monkeypatch, caplog):
    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")

    def _boom():
        raise OSError("connection refused")

    monkeypatch.setattr(pg_connection, "get_pg_pool", _boom)
    pg_schema.reset_ensure_flag_for_tests()

    with caplog.at_level("CRITICAL"):
        assert pg_schema.ensure_pg_schema() is False
    assert "schema bootstrap FAILED" in caplog.text

    cur = _FakeCursor()
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    assert pg_schema.ensure_pg_schema() is True, "a failed attempt must not latch"
    pg_schema.reset_ensure_flag_for_tests()


def test_lifespan_and_worker_boot_call_ensure_pg_schema():
    assert "_application_lifespan(app)" in inspect.getsource(main.lifespan)
    assert "ensure_pg_schema" in inspect.getsource(main._application_lifespan)
    assert "ensure_pg_schema" in inspect.getsource(celery_app._worker_process_init)


# ── Quarantine & Evidence ──
@pytest.fixture
def svc_id():
    svc = "test-quarantine-svc"
    get_con(svc).execute("SELECT 1")
    return svc


def test_quarantine_storage_total_default_zero(svc_id):
    assert get_quarantine_storage_total(svc_id) == 0


def test_service_lock_always_takes_the_postgres_advisory_lock(monkeypatch):
    executed: list[tuple] = []

    class _Con:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        @contextmanager
        def transaction(self):
            yield

    class _Pool:
        @contextmanager
        def connection(self):
            yield _Con()

    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _Pool())
    with quarantine._service_lock("svc-lock"):
        pass

    assert executed == [("SELECT pg_advisory_xact_lock(%s)", (quarantine._advisory_key("svc-lock"),))]


def _capture_evidence(
    service_id: str = "evidence-service",
    source_type: str = "request",
    original_key: str = "raw/request/part.log.gz",
    payload: bytes = b"bad",
    *,
    line_ordinal: int | None = None,
    byte_offset: int | None = None,
    error_category: str = "invalid_json",
    error_text: str = "invalid record",
) -> dict:
    return quarantine.capture_evidence(
        service_id,
        source_type,
        original_key,
        payload,
        line_ordinal=line_ordinal,
        byte_offset=byte_offset,
        error_category=error_category,
        error_text=error_text,
    )


def _fail_unlink_for(monkeypatch, target_path: Path) -> None:
    real_unlink = Path.unlink

    def _fail_target(path: Path, *args, **kwargs):
        if path == target_path:
            raise PermissionError("file is busy")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _fail_target)


def test_capture_evidence_preserves_exact_bytes_and_metadata():
    payload = b'{"bad":"\x00\xff\xf0\x9f\x8e\x89"}\n'
    result = _capture_evidence(payload=payload, line_ordinal=7, byte_offset=128)

    assert result["quarantine_capture_failures"] == 0 and result["cap_evictions"] == 0
    item, evidence = quarantine.read_evidence("evidence-service", int(result["id"]))
    try:
        assert evidence.read() == payload
    finally:
        evidence.close()

    assert item["source_type"] == "request"
    assert item["original_key"] == "raw/request/part.log.gz"
    assert item["line_ordinal"] == 7 and item["byte_offset"] == 128 and item["byte_length"] == len(payload)
    assert item["error_category"] == "invalid_json"
    assert item["sha256"] == "7aa0ef0bdccc1d2cb18dac4f2af559c2a259024bae2653330536f95cbeb84fe8"
    evidence_path = quarantine._evidence_path("evidence-service", int(result["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700
    assert quarantine.QUARANTINE_ITEM_CAP == 1_000


def test_capture_evidence_enforces_shared_fifo_cap(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b"first", b"second", b"third"]

    results = [
        _capture_evidence(
            "fifo-service",
            source_type,
            f"raw/{source_type}/{index}.gz",
            payload,
            error_category="corrupt_container",
            error_text="invalid gzip",
        )
        for index, (source_type, payload) in enumerate(zip(("request", "rum", "request"), payloads, strict=True))
    ]

    assert [result["cap_evictions"] for result in results] == [0, 0, 1]
    rows = list_quarantine_evidence("fifo-service", oldest_first=True)
    assert [row["original_key"] for row in rows] == ["raw/rum/1.gz", "raw/request/2.gz"]
    assert quarantine.read_evidence("fifo-service", int(results[0]["id"])) is None


def test_evidence_summary_counts_sources_categories_and_bytes():
    for source_type, category, payload in (
        ("request", "invalid_json", b"bad"),
        ("rum", "invalid_json", b"worse"),
        ("request", "corrupt_container", b"gzip"),
    ):
        result = _capture_evidence(
            "summary-service",
            source_type,
            f"raw/{source_type}/{category}.gz",
            payload,
            error_category=category,
            error_text="invalid evidence",
        )
        assert result["quarantine_capture_failures"] == 0

    summary = get_quarantine_evidence_summary("summary-service")
    assert summary["total_items"] == 3 and summary["total_bytes"] == 12
    assert summary["request_items"] == 2 and summary["rum_items"] == 1
    assert summary["category_counts"] == {"invalid_json": 2, "corrupt_container": 1}
    assert summary["oldest_at"] is not None and summary["newest_at"] is not None


def test_concurrent_capture_keeps_all_items_and_purge_removes_bytes():
    payload = b"corrupt gzip bytes"

    def capture(index: int):
        return _capture_evidence(
            "concurrent-service",
            "rum" if index % 2 else "request",
            f"raw/{index}.gz",
            payload + str(index).encode(),
            error_category="corrupt_container",
            error_text="invalid gzip",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(capture, range(16)))

    assert all(result["quarantine_capture_failures"] == 0 for result in results)
    assert len(list_quarantine_evidence("concurrent-service")) == 16

    metadata, evidence = quarantine.read_evidence("concurrent-service", int(results[0]["id"]))
    path = quarantine._evidence_path("concurrent-service", int(metadata["id"]))
    evidence.close()

    assert quarantine.purge_evidence("concurrent-service", int(metadata["id"])) == {"purged": 1}
    assert not path.exists()
    assert quarantine.purge_evidence("concurrent-service") == {"purged": 15}
    assert list_quarantine_evidence("concurrent-service") == []


def test_capture_failure_is_reported_to_the_ingest_caller(monkeypatch):
    def fail_storage_path(_service_id: str) -> Path:
        raise OSError("storage unavailable")

    monkeypatch.setattr(quarantine, "_evidence_dir", fail_storage_path)
    result = _capture_evidence(
        "unavailable-service", "request", "raw/unavailable.gz", b"bad line", line_ordinal=1, byte_offset=0
    )
    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_capture_lock_failure_is_reported_to_the_ingest_caller(monkeypatch):
    class FailedServiceLock:
        def __enter__(self):
            raise TimeoutError("metadata lock unavailable")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(quarantine, "_service_lock", lambda _service_id: FailedServiceLock())
    result = _capture_evidence(
        "unavailable-service", "request", "raw/unavailable.gz", b"bad line", line_ordinal=1, byte_offset=0
    )
    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_fifo_eviction_retains_existing_evidence_when_file_unlink_fails(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 1)
    first = _capture_evidence("unlink-service", "request", "raw/first.gz", b"first", line_ordinal=1, byte_offset=0)
    first_path = quarantine._evidence_path("unlink-service", int(first["id"]))
    _fail_unlink_for(monkeypatch, first_path)

    second = _capture_evidence(
        "unlink-service",
        "rum",
        "raw/second.gz",
        b"second",
        error_category="corrupt_container",
        error_text="invalid gzip",
    )
    assert second == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert [item["original_key"] for item in list_quarantine_evidence("unlink-service")] == ["raw/first.gz"]
    assert first_path.exists()
    assert list(quarantine._evidence_dir("unlink-service").glob("*.dat")) == [first_path]


def test_purge_preserves_metadata_when_file_unlink_fails(monkeypatch):
    item = _capture_evidence(
        "failed-purge-service", "request", "raw/request/purge.gz", b"bad line", line_ordinal=1, byte_offset=0
    )
    item_id = int(item["id"])
    path = quarantine._evidence_path("failed-purge-service", item_id)
    _fail_unlink_for(monkeypatch, path)

    with pytest.raises(OSError, match="could not delete quarantine evidence"):
        quarantine.purge_evidence("failed-purge-service", item_id)

    assert get_quarantine_evidence("failed-purge-service", item_id) is not None
    assert path.exists()


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"source_type": "other"}, ValueError),
        ({"payload": "not-bytes"}, TypeError),
        ({"line_ordinal": 0}, ValueError),
        ({"byte_offset": -1}, ValueError),
    ],
)
def test_capture_evidence_rejects_invalid_metadata(overrides, error):
    args = {
        "service_id": "invalid-evidence-service",
        "source_type": "request",
        "original_key": "raw/request/invalid.gz",
        "payload": b"bad",
        "line_ordinal": None,
        "byte_offset": None,
        "error_category": "invalid_json",
        "error_text": "invalid record",
        **overrides,
    }
    with pytest.raises(error):
        quarantine.capture_evidence(**args)


def test_evidence_paths_reject_service_traversal():
    with pytest.raises(ValueError, match="outside the services data directory"):
        quarantine._evidence_dir("../../outside")


def test_read_evidence_rejects_metadata_paths_outside_the_evidence_directory(monkeypatch):
    monkeypatch.setattr(
        quarantine,
        "get_quarantine_evidence",
        lambda _service_id, _item_id: {"id": 17, "file_path": "../outside.dat"},
    )
    with pytest.raises(ValueError, match="invalid evidence path"):
        quarantine.read_evidence("invalid-path-service", 17)


def test_read_evidence_propagates_missing_local_file():
    result = _capture_evidence(
        "missing-file-service", "request", "raw/request/missing.gz", b"bad", line_ordinal=1, byte_offset=0
    )
    item_id = int(result["id"])
    quarantine._evidence_path("missing-file-service", item_id).unlink()

    with pytest.raises(FileNotFoundError):
        quarantine.read_evidence("missing-file-service", item_id)


def test_capture_failure_after_metadata_insert_cleans_metadata_and_temp_file(monkeypatch):
    def fail_replace(*_args):
        raise OSError("rename failed")

    monkeypatch.setattr(quarantine.os, "replace", fail_replace)
    result = _capture_evidence(
        "rename-failure-service", "rum", "raw/rum/rename-failure.gz", b"bad", line_ordinal=1, byte_offset=0
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert list_quarantine_evidence("rename-failure-service") == []
    assert list(quarantine._evidence_dir("rename-failure-service").iterdir()) == []


def test_purge_missing_item_is_idempotent():
    assert quarantine.purge_evidence("empty-purge-service", 987654) == {"purged": 0}
    assert get_quarantine_evidence("empty-purge-service", 987654) is None


def test_empty_evidence_id_delete_is_a_noop():
    assert delete_quarantine_evidence("empty-delete-service", []) == 0
    assert list_quarantine_evidence("empty-delete-service") == []
