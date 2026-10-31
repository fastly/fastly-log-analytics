"""Tests for metadata_db lease acquisition, audit, concurrency, reaping, schema, Postgres routing, usage log summaries, and quarantine evidence."""

from __future__ import annotations

import datetime as _dt
import inspect
import json
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

import backend.core.metadata as metadata_db
import backend.core.metadata.base as pg_connection
import backend.core.metadata.base as pg_schema
import backend.core.metadata.ingest_log as quarantine
import backend.core.metadata.usage_log as usage_log_db
from backend import celery_app, config, main
from backend.core import share_db as share_db_connection
from backend.core.metadata import base, ingest_log, usage_log
from backend.core.metadata.base import InvalidServiceIdError, get_con
from backend.core.metadata.ingest_log import (
    delete_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    get_quarantine_storage_total,
    list_quarantine_evidence,
)
from backend.core.metadata.usage_log import start_cron_run
from backend.utils.date_utils import iso_z_now


def test_start_cron_run_leases():
    sid = "svc-lease-basic"
    assert start_cron_run(sid, "log_discovery") > 0
    with pytest.raises(RuntimeError, match="already running"):
        start_cron_run(sid, "log_discovery")

    sid2 = "svc-lease-different-task"
    a, b = start_cron_run(sid2, "log_discovery"), start_cron_run(sid2, "commit")
    assert a > 0 and b > 0 and a != b
    assert start_cron_run("svc-lease-a", "log_discovery") > 0
    assert start_cron_run("svc-lease-b", "log_discovery") > 0


def test_lease_index_exists_and_is_unique_partial():
    row = (
        get_con("svc-lease-index-check")
        .execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'idx_job_runs_running_lease'"
        )
        .fetchone()
    )
    assert row is not None, "idx_job_runs_running_lease is missing"
    indexdef = (row["indexdef"] if hasattr(row, "keys") else row[0]).upper()
    assert "UNIQUE" in indexdef and "STATUS" in indexdef and "RUNNING" in indexdef


def test_fresh_schema_has_expected_columns_indexes_and_bigint_usage_counters():
    con = get_con("svc-schema-check")

    def _cols(table: str) -> dict[str, str]:
        rows = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
        return {
            (r["column_name"] if hasattr(r, "keys") else r[0]): (r["data_type"] if hasattr(r, "keys") else r[1])
            for r in rows
        }

    assert {
        "lease_generation",
        "published_at",
        "error_kind",
        "next_attempt_at",
        "originating_task",
        "originating_run_id",
        "outcome_counters",
        "raw_delete_claim_token",
        "raw_delete_claimed_at",
    } <= set(_cols("ingest_ledger"))
    assert "reason_counts" in _cols("quarantined_files")
    assert "outcome_counters" in _cols("cron_runs")
    for table in ("usage_log", "usage_log_hourly_summary"):
        cols = _cols(table)
        assert cols["bytes"] == "bigint" and cols["count"] == "bigint"

    idx = {
        (r["indexname"] if hasattr(r, "keys") else r[0])
        for r in con.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()").fetchall()
    }
    assert {
        "idx_ingest_ledger_claimed_scan",
        "idx_ingest_ledger_commit_scan",
        "idx_ingest_ledger_raw_delete_scan",
        "idx_ingest_ledger_retry_scan",
    } <= idx


def test_start_cron_run_exactly_one_of_two_concurrent_callers_wins():
    sid, task = "svc-lease-race", "log_discovery"
    results: list[tuple[bool, str]] = []
    lock, barrier = threading.Lock(), threading.Barrier(2)

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

    wins, losses = [r for r in results if r[0]], [r for r in results if not r[0]]
    assert len(wins) == 1 and len(losses) == 1 and "already running" in losses[0][1]


def _audit_row(ts: str = "2026-06-12T10:00:00Z", event_type: str = "ingest_start", actor: str = "cron") -> dict:
    return {"timestamp": ts, "source_name": "fos", "event_type": event_type, "details": "{}", "actor": actor}


SVC = "test-audit-svc"


def test_scoring_audit_lifecycle_and_filters():
    metadata_db.record_scoring_audit(
        SVC, "scoring_enabled", details={"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    )
    rows = metadata_db.list_scoring_audit(SVC)
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "scoring_enabled" and row["actor"] == "operator"
    assert row["details"] == {"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    assert "timestamp" in row and "id" in row

    metadata_db.record_scoring_audit(SVC, "threshold_committed", details={"new_threshold": 80})
    metadata_db.record_scoring_audit(SVC, "matrix_retrained", details={"matrix_version": "v2"})
    assert [r["action"] for r in metadata_db.list_scoring_audit(SVC)] == [
        "matrix_retrained",
        "threshold_committed",
        "scoring_enabled",
    ]
    assert [r["action"] for r in metadata_db.list_scoring_audit(SVC, limit=2)] == [
        "matrix_retrained",
        "threshold_committed",
    ]

    svc2 = "test-audit-svc-since"
    metadata_db.record_scoring_audit(svc2, "older_action")
    con = metadata_db.get_con(svc2)
    con.execute("UPDATE scoring_audit SET timestamp = datetime('now', '-10 seconds') WHERE action = 'older_action'")
    con.commit()
    cutoff = str(con.execute("SELECT datetime('now', '-5 seconds') AS now").fetchone()["now"])
    metadata_db.record_scoring_audit(svc2, "newer_action")
    assert [r["action"] for r in metadata_db.list_scoring_audit(svc2, since=cutoff)] == ["newer_action"]
    assert metadata_db.list_scoring_audit(svc2, since="2099-01-01T00:00:00") == []


def test_scoring_audit_validation_and_empty_service():
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(None, "scoring_enabled")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(SVC, "weird_action", details={"a_set": {1, 2, 3}})  # type: ignore[dict-item]
    assert metadata_db.list_scoring_audit("freshly-minted-svc") == []


def test_concurrent_inserts_and_dedup():
    sid = "svc-concurrent"
    n_threads, rows_per_thread = 8, 50
    metadata_db.get_con(sid)

    def worker(thread_id: int) -> int:
        metadata_db.insert_ingested_files(
            sid, [(f"thread-{thread_id}/file-{i}.gz", 100, 4096) for i in range(rows_per_thread)]
        )
        return rows_per_thread

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        totals = list(ex.map(worker, range(n_threads)))
    assert sum(totals) == n_threads * rows_per_thread
    assert (
        metadata_db.get_con(sid)
        .execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,))
        .fetchone()[0]
        == n_threads * rows_per_thread
    )

    sid_ov = "svc-concurrent-overlap"
    files = [f"shared/f-{i}.gz" for i in range(20)]
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(
            ex.map(
                lambda tid: metadata_db.insert_ingested_files(sid_ov, [(f, 100 + tid, 1000 + tid) for f in files]),
                range(6),
            )
        )
    assert metadata_db.get_con(sid_ov).execute(
        "SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid_ov,)
    ).fetchone()[0] == len(files)


def test_teardown_and_concurrent_teardown():
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

    sid_race = "svc-teardown-race"
    lock = threading.Lock()
    metadata_db.insert_ingested_files(sid_race, [("warmup.gz", 1, 100)])

    def writer() -> None:
        with lock:
            try:
                metadata_db.insert_ingested_files(sid_race, [(f"writer-{i}.gz", 1, 100) for i in range(20)])
            except Exception:
                pass

    def tearer() -> None:
        with lock:
            metadata_db.teardown(sid_race)

    with ThreadPoolExecutor(max_workers=4) as ex:
        for f in [ex.submit(writer), ex.submit(tearer), ex.submit(writer), ex.submit(tearer)]:
            f.result()

    metadata_db.insert_ingested_files(sid_race, [("recovered.gz", 99, 9999)])
    assert "recovered.gz" in {
        r[0] for r in metadata_db.get_con(sid_race).execute("SELECT file_name FROM ingested_files").fetchall()
    }
    metadata_db.teardown("svc-teardown-never-existed")


def test_threads_get_isolated_connections():
    sid = "svc-isolation"
    barrier = threading.Barrier(4)

    def worker(_: int) -> int:
        con = metadata_db.get_con(sid)
        barrier.wait(timeout=5.0)
        return id(con)

    with ThreadPoolExecutor(max_workers=4) as ex:
        ids = list(ex.map(worker, range(4)))
    assert len(set(ids)) == 4


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
        "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) VALUES (?, ?, ?, ?, ?, 'running')",
        (sid, task, heartbeat_at, heartbeat_at, ttl_s),
    )
    con.commit()


def test_reap_running_cron_runs_variants():
    sid = "svc-reap-1"
    rid = _seed_running(sid)
    assert metadata_db.reap_running_cron_runs(sid) == 1
    con = metadata_db.get_con(sid)
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    assert row["status"] == "error" and "interrupted" in (row["error_message"] or "").lower()

    assert metadata_db.reap_running_cron_runs("svc-reap-2") == 0

    con.execute("DELETE FROM cron_runs")
    sid3 = "svc-reap-3"
    con3 = metadata_db.get_con(sid3)
    con3.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con3.commit()
    assert metadata_db.reap_running_cron_runs(sid3) == 0
    assert con3.execute("SELECT status FROM cron_runs WHERE task = 'sync'").fetchone()["status"] == "success"

    con.execute("DELETE FROM cron_runs")
    sid5 = "svc-reap-5"
    _seed_running(sid5)
    metadata_db.reap_running_cron_runs(sid5, reason="Pod evicted by k8s")
    row5 = metadata_db.get_con(sid5).execute("SELECT error_message FROM cron_runs WHERE status = 'error'").fetchone()
    assert "k8s" in (row5["error_message"] or "")


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


def test_sync_orphan_threshold_and_live_lease():
    sid = "svc-sync-threshold"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, "sync", t0)
    with time_machine.travel(t0 + _dt.timedelta(minutes=11), tick=False):
        new_id = metadata_db.start_cron_run(sid, "sync")
    con = metadata_db.get_con(sid)
    assert new_id and new_id != old_id
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"

    sid2 = "svc-sync-live-lease"
    row_id = _seed_running_at(sid2, "sync", t0)
    now = t0 + _dt.timedelta(minutes=11)
    _seed_lease(sid2, "sync", (now - _dt.timedelta(seconds=30)).timestamp(), ttl_s=60)
    with time_machine.travel(now, tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid2, "sync")
    assert (
        metadata_db.get_con(sid2).execute("SELECT status FROM cron_runs WHERE id = ?", (row_id,)).fetchone()["status"]
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


def test_finalize_cron_run_if_running():
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

    sid2 = "svc-finalize-2"
    con2 = metadata_db.get_con(sid2)
    con2.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con2.commit()
    rid2 = int(con2.execute("SELECT id FROM cron_runs WHERE status='success'").fetchone()["id"])
    assert metadata_db.finalize_cron_run_if_running(sid2, "sync", rid2) is False
    assert con2.execute("SELECT status FROM cron_runs WHERE id = ?", (rid2,)).fetchone()["status"] == "success"
    assert metadata_db.finalize_cron_run_if_running(sid2, "sync", None) is False
    assert metadata_db.finalize_cron_run_if_running(sid2, "sync", 999999) is False


def _reapply_schema() -> None:
    pg_schema.ensure_pg_schema(force=True)


_EXPECTED_TABLES = set(
    "sources ingested_files ingested_files_summary ingest_in_flight cron_runs asn_names audit_logs views alerts scoring_labels".split()
)
_EXPECTED_INDEXES = set(
    "idx_ingested_files_source_ingested_at idx_in_flight_source idx_cron_task_started idx_cron_started "
    "idx_audit_source idx_scoring_labels_svc_sid idx_scoring_labels_svc_label".split()
)


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
        "SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
        (table, column),
    ).fetchone()
    return row[0] if row else ""


def test_init_schema_tables_indexes_and_columns():
    con = metadata_db.get_con("svc-schema-shape")
    assert not (_EXPECTED_TABLES - _list_tables(con))
    assert not (_EXPECTED_INDEXES - _list_indexes(con))
    cols = _columns(con, "alerts")
    assert "evaluation_scope" in cols and "comparison_period_min" in cols

    ucon = usage_log_db.get_con("svc-usage-bigint")
    for table in ("usage_log", "usage_log_hourly_summary"):
        for col in ("bytes", "count"):
            assert _column_type(ucon, table, col) == "bigint"


def test_log_usage_calls_accepts_bytes_over_int4_max():
    sid, big = "svc-usage-overflow", 3_000_000_000
    usage_log.log_usage_calls(
        sid, [{"method": "GET", "service": "FOS", "path": "/big.gz", "bytes": big, "caller": "test"}]
    )
    con = usage_log_db.get_con(sid)
    row = con.execute("SELECT bytes FROM usage_log WHERE service_id = ? AND url = ?", (sid, "/big.gz")).fetchone()
    assert row is not None and row[0] == big
    hourly = con.execute("SELECT max(bytes) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
    assert hourly is not None and hourly[0] == big


def test_init_schema_idempotency_and_survival():
    sid = "svc-schema-idem"
    metadata_db.insert_ingested_files(sid, [("file-a.gz", 100, 4096), ("file-b.gz", 200, 8192)])
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0] == 2
    _reapply_schema()
    _reapply_schema()
    assert con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0] == 2

    sid2 = "svc-schema-future"
    metadata_db.insert_ingested_files(sid2, [("survivor.gz", 1, 100)])
    con.execute("CREATE TABLE IF NOT EXISTS new_feature_table (id SERIAL PRIMARY KEY, payload TEXT)")
    con.commit()
    _reapply_schema()
    rows = con.execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid2,)).fetchall()
    assert [r[0] for r in rows] == ["survivor.gz"] and "new_feature_table" in _list_tables(con)


@pytest.mark.parametrize("bad_sid", [None, 123, object(), {"name": "svc-a"}], ids=["none", "int", "object", "dict"])
def test_validate_service_id_rejects_non_string_service_id(bad_sid):
    with pytest.raises(TypeError):
        metadata_db.get_con(bad_sid)


@pytest.mark.parametrize(
    "bad_sid",
    ["../etc/passwd", "foo/bar", "foo\x00bar", "", "x" * 65, "with space", "foo.bar", "\U00018d1f", "café"],
    ids="traversal slash null_byte empty too_long space period apfs_illegal non_ascii".split(),
)
def test_validate_service_id_rejects_malformed_service_id(bad_sid):
    with pytest.raises(InvalidServiceIdError):
        metadata_db.get_con(bad_sid)


def test_invalid_service_id_in_path_returns_422(client):
    resp = client.get("/api/services/foo.bar/scoring/labels")
    assert resp.status_code == 422
    err = resp.json()["detail"][0]
    assert err["loc"] == ["path", "service_id"] and "service_id must match" in err["msg"]
    assert err["type"] == "value_error.invalid_service_id"


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
    "opener", [base.get_con, base.get_con_readonly, usage_log_db.get_con, usage_log_db.open_readonly]
)
def test_connection_openers_raise_without_dsn(no_dsn, opener):
    exc = _in_fresh_thread(lambda: opener("svc-1"))
    assert isinstance(exc, RuntimeError) and "METADATA_DSN is not set" in str(exc)


def test_deployment_validation_and_usage_log_ddl(monkeypatch):
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", "postgresql://u:p@h/db")
    monkeypatch.setenv("METADATA_DSN", "sqlite:///tmp/x.db")
    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        config.validate_deployment_mode()

    statements = pg_schema.pg_schema_statements()
    joined = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS usage_log (" in joined
    assert "CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (" in joined
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements) and "AUTOINCREMENT" not in joined


def test_postgres_routing_and_aggregates(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://fake/fake")
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=sentinel) as m_get:
        assert base.get_con("svc-1") is sentinel
        with pytest.raises(InvalidServiceIdError):
            base.get_con("not a valid id / has slash")
    m_get.assert_called_once_with()

    with patch.object(base.pg_connection, "get_pg_readonly_connection", return_value=sentinel) as m_ro:
        assert base.get_con_readonly("svc-1") is sentinel
    m_ro.assert_called_once_with()

    with patch.object(base.pg_connection, "release_pg_thread_connection") as m_rel:
        base.release_thread_connection()
    m_rel.assert_called_once_with()

    with patch.object(base.pg_connection, "close_all_pg_connections") as m_close:
        base.close_all_connections()
    m_close.assert_called_once()

    base.teardown("svc-1")

    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as m_share:
        assert share_db_connection.get_global_share_con() is sentinel
    m_share.assert_called_once_with()

    cursor = MagicMock()
    cursor.fetchone.return_value = {"latest": "2026-09-08T21:34:32Z"}
    connection = MagicMock()
    connection.execute.side_effect = lambda sql, params: (
        (_ for _ in ()).throw(AssertionError("SQLite datetime()")) if "datetime(" in sql else cursor
    )
    with patch.object(ingest_log, "get_con", return_value=connection):
        assert ingest_log.get_latest_ingest_ts("svc-1") == "2026-09-08T21:34:32Z"


def test_stale_thread_wrapper_is_replaced_after_close_all_from_another_thread():
    first_ready, closed = threading.Event(), threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        results["first"] = pg_connection.get_pg_thread_connection()
        first_ready.set()
        closed.wait(10)
        second = pg_connection.get_pg_thread_connection()
        results["second"], results["value"] = second, second.execute("SELECT 1").fetchone()[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert first_ready.wait(10)
    pg_connection.close_all_pg_connections()
    closed.set()
    t.join(15)
    assert results["second"] is not results["first"] and results["value"] == 1


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
    assert results["value"] == 1 and results["returned_after_query"] is False


def test_return_to_pool_once_is_atomic_under_contention():
    pool, returned, start = MagicMock(), [False], threading.Barrier(8)
    threads = [
        threading.Thread(target=lambda: (start.wait(5), pg_connection._return_to_pool_once(object(), returned, pool)))
        for _ in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert pool.putconn.call_count == 1


def test_log_usage_calls_and_synthetic_updates_hourly_summary():
    sid = "svc_test_summary_1"
    usage_log.clear_usage_log(sid)
    now_h10 = "2026-05-01T10:15:30Z"
    calls = [
        {"service": "FOS", "method": "PutObject", "path": "/f1", "bytes": 100},
        {"service": "FOS", "method": "PutObject", "path": "/f2", "bytes": 200},
        {"service": "FOS", "method": "GetObject", "path": "/f3", "bytes": 50},
        {"service": "CDN", "method": "download", "path": "/c1", "bytes": 1000},
    ]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_h10):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes FROM usage_log_hourly_summary WHERE service_id = ? ORDER BY hour, operation_class, operation_type",
        (sid,),
    ).fetchall()
    assert [(r["hour"], r["operation_class"], r["operation_type"], r["count"], r["bytes"]) for r in rows] == [
        ("2026-05-01T10", "A", "PutObject", 2, 300),
        ("2026-05-01T10", "B", "GetObject", 1, 50),
        ("2026-05-01T10", "CDN", "download", 1, 1000),
    ]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_h10):
        usage_log.log_usage_calls(sid, [{"service": "FOS", "method": "PutObject", "path": "/f4", "bytes": 400}])
    row = con.execute(
        "SELECT count, bytes FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-05-01T10' AND operation_class = 'A' AND operation_type = 'PutObject'",
        (sid,),
    ).fetchone()
    assert row["count"] == 3 and row["bytes"] == 700

    sid2 = "svc_test_summary_2"
    usage_log.clear_usage_log(sid2)
    syn = [
        {"_timestamp_override": "2026-06-15T08:12:00Z", "method": "PUT_OBJECT", "path": "/obj1.parquet", "bytes": 500},
        {"_timestamp_override": "2026-06-15T08:45:00Z", "method": "PUT_OBJECT", "path": "/obj2.parquet", "bytes": 700},
        {"_timestamp_override": "2026-06-15T09:05:00Z", "method": "PUT_OBJECT", "path": "/obj3.parquet", "bytes": 300},
    ]
    assert usage_log.log_synthetic_usage(sid2, syn) == 3
    rows2 = (
        usage_log_db.get_con(sid2)
        .execute(
            "SELECT hour, operation_class, count, bytes FROM usage_log_hourly_summary WHERE service_id = ? ORDER BY hour",
            (sid2,),
        )
        .fetchall()
    )
    assert [(r["hour"], r["operation_class"], r["count"], r["bytes"]) for r in rows2] == [
        ("2026-06-15T08", "A", 2, 1200),
        ("2026-06-15T09", "A", 1, 300),
    ]


def test_reconcile_purge_and_clear_usage_log():
    sid = "svc_test_summary_3"
    usage_log.clear_usage_log(sid)
    hour = "2026-07-01T14:00:00Z"
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=hour):
        usage_log.log_usage_calls(sid, [{"service": "FOS", "method": "PutObject", "path": f"/k{i}"} for i in range(10)])

    con = usage_log_db.get_con(sid)
    assert (
        con.execute(
            "SELECT count FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'", (sid,)
        ).fetchone()["count"]
        == 10
    )
    for target in (25, 30):
        assert usage_log.reconcile_fastly_stats(sid, [{"hour_iso": hour, "class_a": target, "class_b": 0}]) == 1
        assert (
            con.execute(
                "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
                (sid,),
            ).fetchone()["total"]
            == target
        )

    sid4 = "svc_test_summary_4"
    usage_log.clear_usage_log(sid4)
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2020-01-01T12:00:00Z"):
        usage_log.log_usage_calls(sid4, [{"service": "FOS", "method": "PutObject", "path": "/old"}])
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid4,)).fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid4,)).fetchone()[0] == 1
    usage_log.purge_usage_log(sid4, retention_days=1)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid4,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid4,)).fetchone()[0] == 1
    usage_log.clear_usage_log(sid4)
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid4,)).fetchone()[0] == 0


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


class _FakePool:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    @contextmanager
    def connection(self):
        yield MagicMock(cursor=lambda: self._cursor)


def _pg_error(sqlstate: str, message: str) -> Exception:
    exc = Exception(message)
    exc.sqlstate = sqlstate  # type: ignore[attr-defined]
    return exc


def test_pg_schema_dialect_and_apply():
    statements = pg_schema.pg_schema_statements()
    blob = "\n".join(statements)
    for tbl in "cron_runs slow_queries ingest_ledger metric_snapshots high_scale_ownership high_scale_source_objects high_scale_publication_manifests".split():
        assert tbl in blob
    assert "AUTOINCREMENT" not in blob and "datetime('now')" not in blob
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    assert all(
        "IF NOT EXISTS" in s or "DROP INDEX" in s or "ON CONFLICT" in s or "ALTER TABLE" in s for s in statements
    )

    cur = _FakeCursor()
    applied, raced = pg_schema.apply_pg_schema(cur)
    assert applied == len(statements) and raced == 0 and len(cur.statements) == applied

    cur_race = _FakeCursor(
        fail_with={
            "cron_runs (": _pg_error("42P07", 'relation "cron_runs" already exists'),
            "idx_cron_status": _pg_error("23505", "duplicate key value violates unique constraint"),
        }
    )
    applied2, raced2 = pg_schema.apply_pg_schema(cur_race)
    assert raced2 == 2 and applied2 == len(statements) - 2

    with pytest.raises(RuntimeError, match="failed to apply schema statement"):
        pg_schema.apply_pg_schema(_FakeCursor(fail_with={"cron_runs (": _pg_error("42601", "syntax error at or near")}))


def test_ensure_pg_schema_lifecycle_and_boot_wiring(monkeypatch, caplog):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    pg_schema.reset_ensure_flag_for_tests()
    assert pg_schema.ensure_pg_schema() is False

    cur = _FakeCursor()
    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    pg_schema.reset_ensure_flag_for_tests()
    assert pg_schema.ensure_pg_schema() is True
    first_pass = len(cur.statements)
    assert first_pass > 0 and pg_schema.ensure_pg_schema() is False and len(cur.statements) == first_pass
    assert pg_schema.ensure_pg_schema(force=True) is True and len(cur.statements) == first_pass * 2

    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: (_ for _ in ()).throw(OSError("connection refused")))
    pg_schema.reset_ensure_flag_for_tests()
    with caplog.at_level("CRITICAL"):
        assert pg_schema.ensure_pg_schema() is False
    assert "schema bootstrap FAILED" in caplog.text

    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(_FakeCursor()))
    assert pg_schema.ensure_pg_schema() is True
    pg_schema.reset_ensure_flag_for_tests()

    assert "_application_lifespan(app)" in inspect.getsource(main.lifespan)
    assert "ensure_pg_schema" in inspect.getsource(main._application_lifespan)
    assert "ensure_pg_schema" in inspect.getsource(celery_app._worker_process_init)


def test_quarantine_defaults_and_advisory_lock(monkeypatch):
    svc = "test-quarantine-svc"
    get_con(svc).execute("SELECT 1")
    assert get_quarantine_storage_total(svc) == 0

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
    monkeypatch.setattr(
        Path,
        "unlink",
        lambda path, *a, **kw: (
            (_ for _ in ()).throw(PermissionError("file is busy"))
            if path == target_path
            else real_unlink(path, *a, **kw)
        ),
    )


def test_capture_evidence_preserves_exact_bytes_and_metadata():
    payload = b'{"bad":"\x00\xff\xf0\x9f\x8e\x89"}\n'
    result = _capture_evidence(payload=payload, line_ordinal=7, byte_offset=128)
    assert result["quarantine_capture_failures"] == 0 and result["cap_evictions"] == 0
    item, evidence = quarantine.read_evidence("evidence-service", int(result["id"]))
    try:
        assert evidence.read() == payload
    finally:
        evidence.close()

    assert item["source_type"] == "request" and item["original_key"] == "raw/request/part.log.gz"
    assert item["line_ordinal"] == 7 and item["byte_offset"] == 128 and item["byte_length"] == len(payload)
    assert item["error_category"] == "invalid_json"
    assert item["sha256"] == "7aa0ef0bdccc1d2cb18dac4f2af559c2a259024bae2653330536f95cbeb84fe8"
    evidence_path = quarantine._evidence_path("evidence-service", int(result["id"]))
    assert (
        stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
        and stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700
    )
    assert quarantine.QUARANTINE_ITEM_CAP == 1_000


def test_capture_evidence_enforces_shared_fifo_cap(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    results = [
        _capture_evidence(
            "fifo-service", st, f"raw/{st}/{i}.gz", p, error_category="corrupt_container", error_text="invalid gzip"
        )
        for i, (st, p) in enumerate(zip(("request", "rum", "request"), [b"first", b"second", b"third"], strict=True))
    ]
    assert [r["cap_evictions"] for r in results] == [0, 0, 1]
    assert [r["original_key"] for r in list_quarantine_evidence("fifo-service", oldest_first=True)] == [
        "raw/rum/1.gz",
        "raw/request/2.gz",
    ]
    assert quarantine.read_evidence("fifo-service", int(results[0]["id"])) is None


def test_evidence_summary_counts_sources_categories_and_bytes():
    for st, cat, p in (
        ("request", "invalid_json", b"bad"),
        ("rum", "invalid_json", b"worse"),
        ("request", "corrupt_container", b"gzip"),
    ):
        res = _capture_evidence(
            "summary-service", st, f"raw/{st}/{cat}.gz", p, error_category=cat, error_text="invalid evidence"
        )
        assert res["quarantine_capture_failures"] == 0
    summary = get_quarantine_evidence_summary("summary-service")
    assert summary["total_items"] == 3 and summary["total_bytes"] == 12
    assert summary["request_items"] == 2 and summary["rum_items"] == 1
    assert summary["category_counts"] == {"invalid_json": 2, "corrupt_container": 1}
    assert summary["oldest_at"] is not None and summary["newest_at"] is not None


def test_concurrent_capture_keeps_all_items_and_purge_removes_bytes():
    def capture(i: int):
        return _capture_evidence(
            "concurrent-service",
            "rum" if i % 2 else "request",
            f"raw/{i}.gz",
            b"corrupt gzip bytes" + str(i).encode(),
            error_category="corrupt_container",
            error_text="invalid gzip",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(capture, range(16)))
    assert all(r["quarantine_capture_failures"] == 0 for r in results)
    assert len(list_quarantine_evidence("concurrent-service")) == 16

    metadata, evidence = quarantine.read_evidence("concurrent-service", int(results[0]["id"]))
    path = quarantine._evidence_path("concurrent-service", int(metadata["id"]))
    evidence.close()
    assert quarantine.purge_evidence("concurrent-service", int(metadata["id"])) == {"purged": 1} and not path.exists()
    assert quarantine.purge_evidence("concurrent-service") == {"purged": 15}
    assert list_quarantine_evidence("concurrent-service") == []


def test_capture_failures_and_unlink_failures(monkeypatch):
    with patch.object(quarantine, "_evidence_dir", side_effect=OSError("storage unavailable")):
        assert _capture_evidence(
            "unavailable-service", "request", "raw/unavailable.gz", b"bad line", line_ordinal=1, byte_offset=0
        ) == {
            "id": 0,
            "cap_evictions": 0,
            "quarantine_capture_failures": 1,
        }

    class _FailLock:
        def __enter__(self):
            raise TimeoutError("metadata lock unavailable")

        def __exit__(self, *_a):
            return False

    with patch.object(quarantine, "_service_lock", lambda _s: _FailLock()):
        assert _capture_evidence(
            "unavailable-service", "request", "raw/unavailable.gz", b"bad line", line_ordinal=1, byte_offset=0
        ) == {
            "id": 0,
            "cap_evictions": 0,
            "quarantine_capture_failures": 1,
        }

    with patch.object(quarantine, "QUARANTINE_ITEM_CAP", 1):
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
        assert first_path.exists() and list(quarantine._evidence_dir("unlink-service").glob("*.dat")) == [first_path]

    item = _capture_evidence(
        "failed-purge-service", "request", "raw/request/purge.gz", b"bad line", line_ordinal=1, byte_offset=0
    )
    item_id = int(item["id"])
    path = quarantine._evidence_path("failed-purge-service", item_id)
    _fail_unlink_for(monkeypatch, path)
    with pytest.raises(OSError, match="could not delete quarantine evidence"):
        quarantine.purge_evidence("failed-purge-service", item_id)
    assert get_quarantine_evidence("failed-purge-service", item_id) is not None and path.exists()


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


def test_evidence_path_validation_and_idempotent_deletes(monkeypatch):
    with pytest.raises(ValueError, match="outside the services data directory"):
        quarantine._evidence_dir("../../outside")

    with patch.object(quarantine, "get_quarantine_evidence", lambda _s, _i: {"id": 17, "file_path": "../outside.dat"}):
        with pytest.raises(ValueError, match="invalid evidence path"):
            quarantine.read_evidence("invalid-path-service", 17)

    result = _capture_evidence(
        "missing-file-service", "request", "raw/request/missing.gz", b"bad", line_ordinal=1, byte_offset=0
    )
    item_id = int(result["id"])
    quarantine._evidence_path("missing-file-service", item_id).unlink()
    with pytest.raises(FileNotFoundError):
        quarantine.read_evidence("missing-file-service", item_id)

    with patch.object(quarantine.os, "replace", side_effect=OSError("rename failed")):
        res_ren = _capture_evidence(
            "rename-failure-service", "rum", "raw/rum/rename-failure.gz", b"bad", line_ordinal=1, byte_offset=0
        )
    assert res_ren == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert list_quarantine_evidence("rename-failure-service") == []
    assert list(quarantine._evidence_dir("rename-failure-service").iterdir()) == []

    assert quarantine.purge_evidence("empty-purge-service", 987654) == {"purged": 0}
    assert get_quarantine_evidence("empty-purge-service", 987654) is None
    assert delete_quarantine_evidence("empty-delete-service", []) == 0
    assert list_quarantine_evidence("empty-delete-service") == []


# ── Merged from tests/utils/test_state_sync.py ──


def _make_s3_mock():
    s3 = MagicMock()
    s3.put_object = MagicMock()
    return s3


def test_export_does_not_include_alerts():
    """export_admin_state must not include _alerts in the uploaded JSON."""
    source = {
        "name": "svc1",
        "bucket": "bkt",
        "prefix": "",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
    }

    s3_mock = _make_s3_mock()
    cfg_mock = {"log_fields": {"schema_version": 2, "custom_fields": []}}

    captured_body: dict = {}

    def capture_put(**kwargs):
        captured_body.update(json.loads(kwargs["Body"].decode("utf-8")))

    s3_mock.put_object.side_effect = capture_put

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=source),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3_mock),
        patch("backend.core.metadata.base.svcconfig.load_config", return_value=cfg_mock),
        # The metadata_db autouse fixture isolates per-service SQLite to tmp_path,
        # so list_views / export_audit all hit a fresh empty file.
    ):
        from backend.core.metadata.base import export_admin_state

        export_admin_state("svc1")

    assert "_alerts" not in captured_body, "_alerts should not be exported to admin_state.json"
    assert "_views" in captured_body
    assert "_audit_logs" in captured_body
    assert "custom_fields" in captured_body


# ── get_admin_state_key: key construction ────────────────────────────────────


def test_admin_state_key_with_empty_prefix():
    """Bare prefix → ``meta/admin_state.json`` (no leading slash).
    Pinned because a leading slash breaks S3 listing semantics."""
    from backend.core.metadata.base import get_admin_state_key

    key = get_admin_state_key({"prefix": ""})
    assert key == "meta/admin_state.json"


def test_admin_state_key_with_prefix_strips_slashes():
    """``/customer1/`` → ``customer1/meta/...`` (slashes stripped
    from both ends). Prevents double-slash keys when admins paste a
    prefix with trailing slashes."""
    from backend.core.metadata.base import get_admin_state_key

    key = get_admin_state_key({"prefix": "/customer1/"})
    assert key == "customer1/meta/admin_state.json"


# ── export_admin_state: early-return branches ────────────────────────────────


def _src(**overrides) -> dict:
    base = {
        "name": "svc1",
        "bucket": "bkt",
        "prefix": "",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
    }
    base.update(overrides)
    return base


def test_export_skips_when_source_not_found():
    """Unknown service id → no-op. The frontend may call sync for a
    service that was just torn down; we must not raise."""
    s3 = MagicMock()
    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=None),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import export_admin_state

        export_admin_state("ghost-svc")

    s3.put_object.assert_not_called()


def test_export_swallows_unexpected_exceptions():
    """Any error during export (S3 down, bad config) must be caught and
    logged — export is best-effort, called from request handlers that
    shouldn't 500 because the state mirror failed."""
    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", side_effect=RuntimeError("S3 broken")),
    ):
        from backend.core.metadata.base import export_admin_state

        # Must not raise
        export_admin_state("svc1")


def test_export_omits_custom_fields_when_config_missing():
    """If ``load_config`` returns None (service was torn down between
    the source lookup and config load), the export still uploads —
    just without the custom_fields key."""
    s3 = MagicMock()
    captured: dict = {}

    def capture_put(**kwargs):
        captured.update(json.loads(kwargs["Body"].decode("utf-8")))

    s3.put_object.side_effect = capture_put

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
        patch("backend.core.metadata.base.svcconfig.load_config", return_value=None),
    ):
        from backend.core.metadata.base import export_admin_state

        export_admin_state("svc1")

    s3.put_object.assert_called_once()
    assert "custom_fields" not in captured
    assert "_views" in captured  # but the other state IS exported


# ── _cdn_get: URL construction + telemetry ───────────────────────────────────


def test_cdn_get_builds_url_with_secret_query_param():
    src = _src(cdn_url="https://cdn-test.fastly.net/", cdn_secret="topsecret")

    fake_resp = MagicMock()
    fake_resp.read.return_value = b"payload"
    fake_resp.headers = {}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm) as mock_open,
        patch("backend.utils.telemetry.record_cdn_call"),
    ):
        from backend.core.metadata.base import _cdn_get

        body = _cdn_get(src, "meta/admin_state.json")

    assert body == b"payload"
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://cdn-test.fastly.net/meta/admin_state.json?key=topsecret"


def test_cdn_get_records_telemetry_with_byte_count():
    """Every CDN hit must record a `record_cdn_call` entry so the usage
    page attributes the bytes — drift here under-reports egress."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    payload = b"x" * 4096

    fake_resp = MagicMock()
    fake_resp.read.return_value = payload
    fake_resp.headers = {}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm),
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
    ):
        from backend.core.metadata.base import _cdn_get

        _cdn_get(src, "some/key.json")

    mock_record.assert_called_once()
    assert mock_record.call_args.kwargs["bytes_count"] == 4096
    assert mock_record.call_args.kwargs["caller"] == "state_sync._cdn_get"


def test_get_scoring_matrix_key_construction():
    """scoring_matrix.json lives next to admin_state.json under meta/."""
    from backend.core.metadata.base import get_scoring_matrix_key

    assert get_scoring_matrix_key({"prefix": ""}) == "meta/scoring_matrix.json"
    assert get_scoring_matrix_key({"prefix": "/c1/"}) == "c1/meta/scoring_matrix.json"


def test_publish_matrix_uploads_to_meta():
    s3 = MagicMock()
    captured: dict = {}

    def capture_put(**kwargs):
        captured["key"] = kwargs["Key"]
        captured["body"] = json.loads(kwargs["Body"].decode("utf-8"))

    s3.put_object.side_effect = capture_put

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import publish_matrix_to_fos

        publish_matrix_to_fos("svc1", {"version": "2026-06-03-a", "counts": {"/": {"/login": 5}}})

    assert captured["key"] == "meta/scoring_matrix.json"
    assert captured["body"]["version"] == "2026-06-03-a"


def test_fetch_matrix_returns_none_on_no_such_key():
    """First-time call: matrix hasn't been published yet → NoSuchKey
    → return None (not raise) so _load_matrix's fallback chain can move
    on to the default-empty bundled matrix."""
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey
    s3.get_object.side_effect = _NoSuchKey("missing")

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import fetch_matrix_from_fos

        assert fetch_matrix_from_fos("svc1") is None


# ── Matrix history: key construction, listing, restore, snapshot-before-overwrite


def test_get_scoring_matrix_history_key_with_empty_prefix():
    """Historical matrices live next to scoring_matrix.json under a
    scoring_matrix_history/ subdir keyed by version string."""
    from backend.core.metadata.base import get_scoring_matrix_history_key

    key = get_scoring_matrix_history_key({"prefix": ""}, "2026-06-03-a")
    assert key == "meta/scoring_matrix_history/2026-06-03-a.json"


def test_get_scoring_matrix_history_key_strips_prefix_slashes():
    """Mirrors get_admin_state_key/get_scoring_matrix_key: slashes are
    stripped from both ends of the customer prefix to prevent double-slash
    keys when an admin pastes ``/customer1/`` in the source config."""
    from backend.core.metadata.base import get_scoring_matrix_history_key

    key = get_scoring_matrix_history_key({"prefix": "/customer1/"}, "v7")
    assert key == "customer1/meta/scoring_matrix_history/v7.json"


def test_list_scoring_matrix_versions_returns_empty_when_source_missing():
    """Same shape as the other matrix helpers: unknown service id → []
    (not None, not raise) so the routes layer can render an empty table."""
    with patch("backend.core.metadata.base.get_source_for_service", return_value=None):
        from backend.core.metadata.base import list_scoring_matrix_versions

        assert list_scoring_matrix_versions("ghost-svc") == []


def test_list_scoring_matrix_versions_sorts_descending_by_last_modified():
    """The matrix-history UI shows newest-first so the operator's most
    recent rollback target is at the top — this pins the sort direction
    so a future ``sort(... reverse=False)`` typo can't silently invert it.

    The paginator is mocked with two pages, deliberately yielding entries
    out-of-order across the two pages so the final sort (not page order)
    is what's exercised.
    """
    import datetime as _dt

    s3 = MagicMock()

    def _obj(version: str, ts_iso: str, size: int = 1024) -> dict:
        return {
            "Key": f"meta/scoring_matrix_history/{version}.json",
            "Size": size,
            "LastModified": _dt.datetime.fromisoformat(ts_iso),
        }

    page1 = {
        "Contents": [
            _obj("v1", "2026-06-01T10:00:00+00:00"),
            _obj("v3", "2026-06-03T10:00:00+00:00"),
        ]
    }
    page2 = {
        "Contents": [
            _obj("v2", "2026-06-02T10:00:00+00:00"),
            _obj("v4", "2026-06-04T10:00:00+00:00"),
        ]
    }
    paginator = MagicMock()
    paginator.paginate.return_value = iter([page1, page2])
    s3.get_paginator.return_value = paginator

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import list_scoring_matrix_versions

        rows = list_scoring_matrix_versions("svc1")

    # The paginator must be invoked against the bucket + the history prefix.
    paginator.paginate.assert_called_once_with(Bucket="bkt", Prefix="meta/scoring_matrix_history/")
    assert [r["version"] for r in rows] == ["v4", "v3", "v2", "v1"], (
        "list_scoring_matrix_versions must sort by last_modified DESC"
    )
    # Spot-check the shape of one row so a future refactor that drops a field
    # (e.g. size_bytes) is caught here too.
    assert rows[0]["key"] == "meta/scoring_matrix_history/v4.json"
    assert rows[0]["size_bytes"] == 1024
    assert rows[0]["last_modified"] == "2026-06-04T10:00:00+00:00"


def test_list_scoring_matrix_versions_swallows_s3_errors():
    """Best-effort: any S3 error (auth, network, missing bucket) → []
    rather than bubbling up, because the matrix-history panel renders
    inline on the admin scoring page and shouldn't 500 it."""
    s3 = MagicMock()
    s3.get_paginator.side_effect = RuntimeError("S3 down")

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import list_scoring_matrix_versions

        assert list_scoring_matrix_versions("svc1") == []


def test_restore_scoring_matrix_version_returns_none_when_version_missing():
    """Operator picked a version that no longer exists (manually deleted,
    or stale UI). head_object raises → return None so the route layer
    can 404 cleanly instead of attempting a copy_object that would
    create an empty 'restored' matrix."""
    s3 = MagicMock()
    s3.head_object.side_effect = Exception("NoSuchKey")

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import restore_scoring_matrix_version

        assert restore_scoring_matrix_version("svc1", "missing-version") is None
    s3.copy_object.assert_not_called()


def test_restore_scoring_matrix_version_happy_path():
    """Version exists → copy_object lands on the current scoring_matrix.json
    key and we return {"version": ..., "restored_at": ...}."""
    s3 = MagicMock()
    # head_object returns OK (no exception) → version exists.
    s3.head_object.return_value = {"ContentLength": 2048}

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None
    assert result["version"] == "v7"
    assert "restored_at" in result

    # The last copy_object call must target the current matrix key, with
    # the historical version as the CopySource.
    final_copy = s3.copy_object.call_args_list[-1]
    assert final_copy.kwargs["Key"] == "meta/scoring_matrix.json"
    assert final_copy.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "meta/scoring_matrix_history/v7.json",
    }


def test_publish_matrix_snapshots_prior_before_overwriting():
    """SNAPSHOT-BEFORE-OVERWRITE: when publish_matrix_to_fos is called and
    a prior live matrix exists, the prior matrix bytes must be copied to
    the scoring_matrix_history/{prior_version}.json key BEFORE the new
    matrix is written to scoring_matrix.json. If the order were reversed,
    a fresh retrain would clobber the prior matrix and the history slot
    would receive the NEW bytes — making rollback impossible."""
    s3 = MagicMock()

    prior_matrix = {"version": "old-v1", "counts": {"/": {"/login": 3}}}
    prior_bytes = json.dumps(prior_matrix).encode("utf-8")
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=prior_bytes))}

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import publish_matrix_to_fos

        publish_matrix_to_fos("svc1", {"version": "new-v2", "counts": {}})

    # Two put_object calls in order: (1) history snapshot, (2) new live matrix.
    put_calls = s3.put_object.call_args_list
    assert len(put_calls) == 2, f"expected 2 put_object calls (history + current), got {len(put_calls)}"

    # First put writes the PRIOR bytes to the history slot, keyed by the
    # prior matrix's version string.
    history_call = put_calls[0]
    assert history_call.kwargs["Key"] == "meta/scoring_matrix_history/old-v1.json"
    assert history_call.kwargs["Body"] == prior_bytes

    # Second put writes the NEW matrix to the live key.
    current_call = put_calls[1]
    assert current_call.kwargs["Key"] == "meta/scoring_matrix.json"
    assert json.loads(current_call.kwargs["Body"].decode("utf-8"))["version"] == "new-v2"


def test_restore_scoring_matrix_version_snapshots_live_before_restore():
    """SNAPSHOT-BEFORE-OVERWRITE (restore variant): if the operator picks
    the wrong historical version, the only way to recover is if the
    pre-restore live matrix was archived first. This pins that
    restore_scoring_matrix_version calls copy_object TWICE in order:
    (1) current → pre-restore-{epoch_ms} snapshot, then
    (2) historical → current.

    If the order were reversed, the pre-restore snapshot would capture
    the JUST-RESTORED matrix (i.e. the historical version itself),
    silently losing the prior-live matrix forever.
    """
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 1024}  # historical version exists

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
        # Pin time.time() so the snapshot key is deterministic and the
        # ordering assertion below can reference an exact key string.
        patch("backend.core.metadata.base.time.time", return_value=1717420800.0),
    ):
        from backend.core.metadata.base import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None and result["version"] == "v7"

    copy_calls = s3.copy_object.call_args_list
    assert len(copy_calls) == 2, f"expected 2 copy_object calls (snapshot + restore), got {len(copy_calls)}"

    # FIRST copy_object: snapshot current matrix to pre-restore-{epoch_ms}.
    snapshot_call = copy_calls[0]
    expected_snapshot_key = f"meta/scoring_matrix_history/pre-restore-{int(1717420800.0 * 1000)}.json"
    assert snapshot_call.kwargs["Key"] == expected_snapshot_key, (
        "first copy_object must write the pre-restore snapshot, not the restore itself"
    )
    assert snapshot_call.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "meta/scoring_matrix.json",
    }

    # SECOND copy_object: historical version → current key.
    restore_call = copy_calls[1]
    assert restore_call.kwargs["Key"] == "meta/scoring_matrix.json"
    assert restore_call.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "meta/scoring_matrix_history/v7.json",
    }


def test_restore_scoring_matrix_version_proceeds_when_snapshot_fails():
    """The pre-restore snapshot is best-effort: if it fails (e.g. no
    prior live matrix exists yet, or a transient S3 error), the restore
    itself must still proceed. The operator's active intent always wins
    over the safety net — otherwise a first-ever restore would always
    fail because there's nothing to snapshot."""
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 1024}

    # First copy_object (snapshot) raises; second (actual restore) succeeds.
    s3.copy_object.side_effect = [
        Exception("NoSuchKey: nothing to snapshot"),
        None,
    ]

    with (
        patch("backend.core.metadata.base.get_source_for_service", return_value=_src()),
        patch("backend.core.metadata.base._get_fos_client", return_value=s3),
    ):
        from backend.core.metadata.base import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None, "restore must succeed even when snapshot step fails"
    assert result["version"] == "v7"
    assert len(s3.copy_object.call_args_list) == 2


def test_cdn_get_blocks_invalid_redirects():
    """Verify that SafeRedirectHandler allows redirects to safe cdn URLs
    but blocks any redirects to forbidden URLs with URLError."""
    import urllib.request
    from unittest.mock import MagicMock, patch

    import pytest

    src = {"bucket": "test", "cdn_url": "https://cdn-test.fastly.net"}
    from backend.core.metadata.base import _cdn_get

    captured_handler = None

    def fake_build_opener(*handlers):
        nonlocal captured_handler
        for h in handlers:
            if h.__class__.__name__ == "SafeRedirectHandler" or (
                isinstance(h, type) and h.__name__ == "SafeRedirectHandler"
            ):
                captured_handler = h
        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value.read.return_value = b"{}"
        mock_opener.open.return_value.__enter__.return_value.headers = {}
        return mock_opener

    # Ensure hasattr(urlopen, "assert_called") is False during this call so the opener code path is taken
    with (
        patch("urllib.request.build_opener", side_effect=fake_build_opener),
        patch("urllib.request.urlopen", new=lambda *a, **kw: None),
        patch("backend.utils.telemetry.record_cdn_call"),
    ):
        _cdn_get(src, "some/key.json")

    assert captured_handler is not None
    handler_inst = captured_handler() if isinstance(captured_handler, type) else captured_handler

    # 1. Test redirect to safe URL
    req_mock = MagicMock()
    # It delegates to super().redirect_request, so let's mock the super class call or verify it doesn't raise
    with patch("urllib.request.HTTPRedirectHandler.redirect_request") as mock_super_redirect:
        handler_inst.redirect_request(req_mock, None, 302, "Found", {}, "https://cdn-another.fastly.net/safe")
        mock_super_redirect.assert_called_once_with(
            req_mock, None, 302, "Found", {}, "https://cdn-another.fastly.net/safe"
        )

    # 2. Test redirect to unsafe URL (e.g. localhost, cloud metadata, or anything not ending in .fastly.net/.fastlystorage.app)
    with pytest.raises(urllib.error.URLError) as excinfo:
        handler_inst.redirect_request(req_mock, None, 302, "Found", {}, "http://169.254.169.254/")
    assert "Redirected to an invalid URL" in str(excinfo.value)


def test_cloud_copy_deletions_bill_for_thirty_days_and_old_rows_are_pruned():
    from datetime import UTC, datetime, timedelta

    from backend.core.metadata.base import get_con
    from backend.core.metadata.usage_log import (
        clear_usage_log,
        cloud_copy_deleted_bytes_in_billing_window,
        record_cloud_copy_deletion,
    )
    from backend.utils.date_utils import iso_z

    svc = "svc-cloud-copy-deletions"
    clear_usage_log(svc)
    con = get_con(svc)
    old = iso_z(datetime.now(UTC) - timedelta(days=40))
    inside = iso_z(datetime.now(UTC) - timedelta(days=10))
    con.execute(
        "INSERT INTO cloud_copy_deletions (service_id, deleted_at, bytes, objects) VALUES (?, ?, 700, 7), (?, ?, 300, 3)",
        (svc, old, svc, inside),
    )
    con.commit()
    # 40-day-old deletions are past the FOS minimum and no longer bill; recent ones still do.
    assert cloud_copy_deleted_bytes_in_billing_window(svc) == 300
    record_cloud_copy_deletion(svc, 1000, 4)
    record_cloud_copy_deletion(svc, 0, 0)
    assert cloud_copy_deleted_bytes_in_billing_window(svc) == 1300
    assert con.execute("SELECT count(*) FROM cloud_copy_deletions WHERE service_id = ?", (svc,)).fetchone()[0] == 2, (
        "rows past the billing window are pruned on insert"
    )
    clear_usage_log(svc)
    assert cloud_copy_deleted_bytes_in_billing_window(svc) == 0


def test_get_pg_pool_passes_open_true_explicitly():
    import backend.core.metadata.pg_pool as pg_pool_mod

    orig_pool = pg_pool_mod._pool
    captured_kwargs: dict = {}

    class _FakePool:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

        def close(self, timeout=1.0):
            pass

    try:
        setattr(pg_pool_mod, "_pool", None)
        with patch.object(pg_pool_mod, "ConnectionPool", _FakePool):
            pool = pg_pool_mod.get_pg_pool()
            assert isinstance(pool, _FakePool)
            assert captured_kwargs.get("open") is True
    finally:
        setattr(pg_pool_mod, "_pool", orig_pool)


def test_job_runs_terminal_pruning_preserves_active_leases():
    from backend.core.metadata.cron_runs import purge_cron_runs

    sid = "svc-job-runs-prune"
    con = get_con(sid)
    now = time.time()
    con.execute("DELETE FROM job_runs WHERE service_id = ?", (sid,))
    con.execute(
        "INSERT INTO job_runs (service_id, job_name, status, started_at, heartbeat_at, lease_ttl_s) "
        "VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
        (
            sid,
            "old_done",
            "completed",
            now - 10 * 86400,
            now - 10 * 86400,
            60,
            sid,
            "recent_fail",
            "failed",
            now - 3600,
            now - 3600,
            60,
            sid,
            "active_lease",
            "running",
            now - 30,
            now,
            60,
        ),
    )

    run_id = start_cron_run(sid, "new_lease")
    assert run_id > 0

    rows = con.execute("SELECT job_name, status FROM job_runs WHERE service_id = ?", (sid,)).fetchall()
    by_task = {r["job_name"]: r["status"] for r in rows}
    assert "old_done" not in by_task
    assert by_task["recent_fail"] == "failed"
    assert by_task["active_lease"] == "running"
    assert by_task["new_lease"] == "running"

    purge_cron_runs(sid, days=0)
    rows_after = con.execute("SELECT job_name, status FROM job_runs WHERE service_id = ?", (sid,)).fetchall()
    by_task_after = {r["job_name"]: r["status"] for r in rows_after}
    assert "recent_fail" not in by_task_after
    assert by_task_after["active_lease"] == "running"
    assert by_task_after["new_lease"] == "running"


def test_fos_usage_atomic_rollback_on_summary_failure():
    from backend.core.metadata import fos_usage

    sid = "svc-fos-usage-atomic"
    con = get_con(sid)
    con.execute("DELETE FROM usage_log WHERE service_id = ?", (sid,))
    con.execute("DELETE FROM usage_log_hourly_summary WHERE service_id = ?", (sid,))

    with patch.object(fos_usage, "_upsert_hourly_summary", side_effect=RuntimeError("summary boom")):
        fos_usage.log_usage_calls(sid, [{"method": "ListObjectsV2", "bytes": 123}])

    raw_cnt = con.execute("SELECT COUNT(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0]
    assert raw_cnt == 0


def test_ingest_log_atomic_rollback_and_rum_summary_isolation():
    sid = "svc-ingest-atomic-rum"
    con = get_con(sid)
    con.execute("DELETE FROM ingested_files WHERE source_name = ?", (sid,))
    con.execute("DELETE FROM ingested_files_summary WHERE source_name = ?", (sid,))

    orig_execute = con.execute

    def _fail_on_summary_insert(sql: str, params=()):
        if "INSERT INTO ingested_files_summary AS summary" in sql:
            raise RuntimeError("simulated summary upsert failure")
        return orig_execute(sql, params)

    with (
        patch("backend.core.metadata.ingest_log.get_con", return_value=con),
        patch.object(con, "execute", side_effect=_fail_on_summary_insert),
        pytest.raises(RuntimeError, match="simulated summary upsert failure"),
    ):
        ingest_log.insert_ingested_files(sid, [("s3://b/2026-01-01T00:00:00.000-a.gz", 10, 100)], table_name="logs")

    # Transaction rolled back: ingested_files must not retain the orphaned row
    cnt = orig_execute("SELECT COUNT(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert cnt == 0

    # Retry succeeds and records full delta in summary
    ingest_log.insert_ingested_files(sid, [("s3://b/2026-01-01T00:00:00.000-a.gz", 10, 100)], table_name="logs")
    # RUM writes must not pollute the request-log summary
    ingest_log.insert_ingested_files(sid, [("s3://b/2026-01-02T00:00:00.000-rum.gz", 999, 5000)], table_name="rum")

    summary = ingest_log.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 1
    assert summary["total_rows"] == 10
    assert summary["total_bytes"] == 100
    assert summary["latest_file_name"] == "s3://b/2026-01-01T00:00:00.000-a.gz"

    recomputed = ingest_log.recompute_ingested_files_summary(con, sid)
    assert recomputed["file_count"] == 1
    assert recomputed["total_rows"] == 10
    assert recomputed["total_bytes"] == 100
    assert recomputed["latest_file_name"] == "s3://b/2026-01-01T00:00:00.000-a.gz"
