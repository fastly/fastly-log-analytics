"""CRUD-layer tests for ``backend.core.metadata`` (views, audit_logs, ingested_files, cron_runs, asn_names, sources, usage_log, pg_connection, reconciliation)."""

from __future__ import annotations

import contextlib
import gc
import json
import logging
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from backend.utils.date_utils import iso_z

from backend import config as svcconfig
from backend.core import duckdb as _db
from backend.core import metadata as metadata_db
from backend.core import query_registry
from backend.core.metadata import pg_connection as pgc
from backend.core.metadata import reconciliation, usage_log_db
from backend.core.metadata.cron_log import (
    _decode_outcome_counters,
    _normalize_outcome_counters,
    record_ingest_object_outcome,
)
from backend.core.metadata.usage_log import _usage_class_predicate
from backend.core.rum_ingest import ingest_rum_logs


@pytest.fixture
def sid() -> str:
    return "test-svc-md"


@pytest.fixture()
def _reset_pg_state():
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()
    yield
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()


# ── Shared test helpers ─────────────────────────────────────────────────────


def _seed_view_directly(sid: str, view_id: str = "old", name: str = "Old View") -> None:
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, page) VALUES (?, ?, ?, ?, ?)",
        (view_id, sid, name, "{}", "dashboard"),
    )
    con.commit()


def _insert_raw_ingested_file(
    sid: str,
    file_name: str,
    *,
    row_count: int = 1,
    file_size_bytes: int = 1,
    ingested_at: str | None = None,
    null_file_date: bool = False,
) -> None:
    con = metadata_db.get_con(sid)
    if null_file_date:
        sql = "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) VALUES (?, ?, ?, ?, NULL)"
        params: tuple = (file_name, sid, row_count, file_size_bytes)
    elif ingested_at is not None:
        sql = "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) VALUES (?, ?, ?, ?, ?)"
        params = (file_name, sid, ingested_at, row_count, file_size_bytes)
    else:
        sql = "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)"
        params = (file_name, sid, row_count, file_size_bytes)
    con.execute(sql, params)
    con.commit()


def _seed_ledger_row(
    sid: str, object_key: str, originating_run_id: int, originating_task: str = "log_discovery"
) -> None:
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, originating_task, originating_run_id) VALUES (?, ?, 'discovered', ?, ?)",
        (sid, object_key, originating_task, originating_run_id),
    )
    con.commit()


def _make_outcome_counters(**overrides: int) -> dict[str, int]:
    keys = (
        "valid_records",
        "malformed_records",
        "corrupt_containers",
        "quarantine_capture_failures",
        "source_delete_failures",
        "cap_evictions",
        "objects_processed",
        "objects_successful",
        "objects_partial",
        "objects_failed",
    )
    return {**dict.fromkeys(keys, 0), **overrides}


@contextlib.contextmanager
def _exploding_executemany(con):
    real_executemany = con.executemany

    def _explode(*_a, **_kw):
        raise RuntimeError("simulated insert failure")

    con.executemany = _explode  # type: ignore[method-assign]
    try:
        yield
    finally:
        con.executemany = real_executemany  # type: ignore[method-assign]


# ── replace_views_for_service & audit_logs ──────────────────────────────────


def test_replace_views_for_service(sid):
    _seed_view_directly(sid, view_id="old", name="Old View")
    view = {
        "id": "new1",
        "service_id": sid,
        "name": "New A",
        "filters_json": "{}",
        "time_range_type": "rolling",
        "start_time": None,
        "end_time": None,
        "page": "dashboard",
        "created_at": "2026-01-01T00:00:00Z",
    }
    metadata_db.replace_views_for_service(sid, [view])
    names = {v["name"] for v in metadata_db.list_views(sid)}
    assert "Old View" not in names and "New A" in names

    metadata_db.replace_views_for_service(sid, [])
    assert metadata_db.list_views(sid) == []


def test_audit_logs_crud_export_and_replace(sid):
    metadata_db.record_audit(sid, "alert_created", {"alert_id": "a1", "name": "5xx"}, actor="dmichael")
    entries = metadata_db.list_audit(sid)
    assert len(entries) == 1
    assert (entries[0]["event_type"], entries[0]["actor"]) == ("alert_created", "dmichael")
    assert json.loads(entries[0]["details"]) == {"alert_id": "a1", "name": "5xx"}
    assert metadata_db.list_audit(sid, since="2099-01-01T00:00:00Z") == []
    assert len(metadata_db.list_audit(sid, since="2000-01-01T00:00:00Z")) == 1
    assert metadata_db.list_audit(sid) == metadata_db.export_audit(sid)

    for i in range(5):
        metadata_db.record_audit(sid, f"event_{i}", {}, actor="x")
    assert len(metadata_db.list_audit(sid, limit=2)) == 2

    entry = {
        "timestamp": "2026-01-01T00:00:00Z",
        "source_name": sid,
        "event_type": "evt_new",
        "details": json.dumps({"k": "v"}),
        "actor": "admin",
    }
    metadata_db.replace_audit_for_service(sid, [entry])
    assert {e["event_type"] for e in metadata_db.list_audit(sid)} == {"evt_new"}


# ── ingested_files & dedup cache ────────────────────────────────────────────


def test_ingested_files_insert_upsert_and_ordering(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000), ("b.gz", 200, 8000)])
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz", "b.gz"}

    metadata_db.insert_ingested_files(sid, [("a.gz", 999, 99999)])
    by_name = {f["file_name"]: f for f in metadata_db.list_ingested_files(sid)}
    assert (by_name["a.gz"]["row_count"], by_name["a.gz"]["file_size_bytes"]) == (999, 99999)

    sid2 = f"{sid}-order"
    _insert_raw_ingested_file(sid2, "first.gz", row_count=1, file_size_bytes=100, ingested_at="2026-01-01T00:00:00Z")
    _insert_raw_ingested_file(sid2, "second.gz", row_count=2, file_size_bytes=200, ingested_at="2026-01-02T00:00:00Z")
    assert [f["file_name"] for f in metadata_db.list_ingested_files(sid2)] == ["second.gz", "first.gz"]


def test_get_reclaimable_strand_filenames(sid):
    metadata_db.insert_ingested_files(
        sid, [("durable.gz", 100, 5000), ("marker.gz", 0, 1200), ("legacy.gz", 100, 8000)]
    )
    con = metadata_db.get_con(sid)
    con.execute(
        "UPDATE ingested_files SET ingested_at = ? WHERE file_name = ? AND source_name = ?",
        ("2020-01-01 00:00:00", "legacy.gz", sid),
    )
    con.commit()

    cands = {"durable.gz", "marker.gz", "legacy.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2000-01-01 00:00:00") == {
        "durable.gz",
        "legacy.gz",
    }
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2025-01-01 00:00:00") == {"durable.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, {"durable.gz", "marker.gz"}, "2000-01-01 00:00:00") == {
        "durable.gz"
    }
    assert metadata_db.get_reclaimable_strand_filenames(sid, set(), "2000-01-01 00:00:00") == set()


def test_get_node_count_avg_variants(sid):
    assert metadata_db.get_node_count_avg(sid) is None
    _insert_raw_ingested_file(sid, "short-T.gz", null_file_date=True)
    _insert_raw_ingested_file(sid, "lowercase-only.gz", null_file_date=True)
    assert metadata_db.get_node_count_avg(sid) is None

    sid_a = f"{sid}-a"
    metadata_db.insert_ingested_files(
        sid_a,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-aaa.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-bbb.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:30.000-ccc.log.gz", 1, 1),
        ],
    )
    assert metadata_db.get_node_count_avg(sid_a) == 1.5

    sid_b = f"{sid}-b"
    metadata_db.insert_ingested_files(
        sid_b,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-a.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 1, 1),
            *[(f"s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-{ch}.log.gz", 1, 1) for ch in ("c", "d", "e", "f")],
        ],
    )
    for minute, suffix in (("32", "x"), ("33", "y")):
        _insert_raw_ingested_file(
            sid_b,
            f"s3://b/raw/2026-05-15/23/2026-05-15T23:{minute}:00.000-legacy-{suffix}.log.gz",
            null_file_date=True,
        )
    assert metadata_db.get_node_count_avg(sid_b) == 2.0


def test_get_log_accounting_counts_arms(sid):
    sid_iso = f"{sid}-iso"
    _insert_raw_ingested_file(
        sid_iso,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz",
        row_count=100,
        ingested_at="2026-05-15T23:00:05",
    )
    _insert_raw_ingested_file(
        sid_iso,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz",
        row_count=250,
        ingested_at="2026-05-15T23:30:05",
    )
    assert metadata_db.get_log_accounting_counts(
        sid_iso, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (350, 2)}

    sid_fd = f"{sid}-fd"
    metadata_db.insert_ingested_files(
        sid_fd,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz", 100, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 250, 1),
        ],
    )
    fd_rows = (
        metadata_db.get_con(sid_fd)
        .execute("SELECT file_name, file_date FROM ingested_files WHERE source_name = ?", (sid_fd,))
        .fetchall()
    )
    assert all(str(r["file_date"]) == "2026-05-15" for r in fd_rows)
    assert metadata_db.get_log_accounting_counts(
        sid_fd, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (350, 2)}

    sid_fb = f"{sid}-fb"
    _insert_raw_ingested_file(sid_fb, "raw/legacy.gz", row_count=42, ingested_at="2026-05-15T23:00:00")
    assert metadata_db.get_log_accounting_counts(
        sid_fb, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (42, 1)}


def test_ingested_filenames_dedup_cache_lifecycle(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100), ("b.gz", 2, 200)])
    metadata_db._clear_ingested_filenames_cache(sid)
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}

    con = metadata_db.get_con(sid)
    con.execute("DELETE FROM ingested_files WHERE source_name = ?", (sid,))
    con.commit()
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}

    metadata_db.insert_ingested_files(sid, [("c.gz", 2, 200)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz", "c.gz"}

    snapshot = metadata_db.get_ingested_filenames(sid, limit=200)
    snapshot.add("mutated.gz")
    assert "mutated.gz" not in metadata_db.get_ingested_filenames(sid, limit=200)

    metadata_db._clear_ingested_filenames_cache(sid)
    metadata_db.insert_ingested_files(sid, [("d.gz", 1, 100)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"c.gz", "d.gz"}

    _insert_raw_ingested_file(sid, "sideloaded.gz", row_count=1, file_size_bytes=100)
    assert metadata_db.get_ingested_filenames(sid) == {"c.gz", "d.gz", "sideloaded.gz"}
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"c.gz", "d.gz", "sideloaded.gz"}

    assert (sid, "logs") in metadata_db._ingested_filenames_cache
    metadata_db.teardown(sid)
    assert (sid, "logs") not in metadata_db._ingested_filenames_cache


# ── cron_runs: start + log + busy + summary ────────────────────────────────


def test_cron_run_start_busy_and_orphan_reap(sid):
    assert metadata_db.cron_busy(sid) is False
    run_id = metadata_db.start_cron_run(sid, "sync")
    assert isinstance(run_id, int) and run_id > 0 and metadata_db.cron_busy(sid) is True
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(sid, "sync")

    assert metadata_db.start_cron_run("cronseam-b", "sync") > 0

    metadata_db.log_cron_run(
        sid, "sync", duration_s=12.5, status="success", files_downloaded=10, rows_ingested=1000, run_id=run_id
    )
    assert metadata_db.cron_busy(sid) is False
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, duration_s, rows_ingested FROM cron_runs WHERE id = ?", (run_id,))
        .fetchone()
    )
    assert (row["status"], row["duration_s"], row["rows_ingested"]) == ("success", 12.5, 1000)

    old_ts = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, parquet_keys) VALUES (?, ?, 'running', '[]')",
        ("sync", old_ts),
    )
    con.commit()
    assert isinstance(metadata_db.start_cron_run(sid, "sync"), int)

    metadata_db.log_cron_run(sid, "manual", duration_s=1.0, status="success", files_downloaded=1)
    rows = con.execute("SELECT task, status FROM cron_runs WHERE task = 'manual'").fetchall()
    assert len(rows) == 1 and rows[0]["status"] == "success"


def test_log_cron_run_persists_outcome_counters_as_json(sid):
    run_id = metadata_db.start_cron_run(sid, "log_discovery")
    counters = _make_outcome_counters(
        valid_records=3,
        malformed_records=1,
        cap_evictions=2,
        objects_processed=2,
        objects_successful=1,
        objects_partial=1,
    )
    metadata_db.log_cron_run(
        sid,
        "log_discovery",
        duration_s=1.0,
        status="error",
        files_downloaded=2,
        rows_ingested=3,
        corrupt_rows=1,
        outcome_counters=counters,
        run_id=run_id,
    )
    con = metadata_db.get_con(sid)
    columns = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'cron_runs'"
    ).fetchall()
    assert "outcome_counters" in {row[0] for row in columns}
    row = con.execute(
        "SELECT status, files_downloaded, rows_ingested, corrupt_rows, outcome_counters FROM cron_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row[0:4] == ("error", 2, 3, 1) and json.loads(row[4]) == counters


def test_record_ingest_object_outcome_and_validation(sid):
    run_id = metadata_db.start_cron_run(sid, "log_discovery")
    _seed_ledger_row(sid, "raw/request/object.log.gz", run_id)
    _seed_ledger_row(sid, "raw/request/other.log.gz", run_id)

    initial = _make_outcome_counters(valid_records=4, objects_processed=1, objects_successful=1)
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", initial)
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", initial)
    other = {**initial, "valid_records": 6}
    assert record_ingest_object_outcome(sid, "raw/request/other.log.gz", other)

    revised = {**initial, "malformed_records": 2, "objects_successful": 0, "objects_partial": 1}
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", revised)
    expected_totals = {key: revised[key] + other[key] for key in revised}
    metadata_db.log_cron_run(sid, "log_discovery", duration_s=1.0, status="success", run_id=run_id)

    con = metadata_db.get_con(sid)
    row = con.execute(
        "SELECT status, rows_ingested, corrupt_rows, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ?",
        (run_id, sid),
    ).fetchone()
    assert (row["status"], row["rows_ingested"], row["corrupt_rows"]) == ("error", 10, 2)
    assert json.loads(row["outcome_counters"]) == expected_totals
    ledger_row = con.execute(
        "SELECT outcome_counters FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
        (sid, "raw/request/object.log.gz"),
    ).fetchone()
    assert json.loads(ledger_row[0]) == revised

    assert record_ingest_object_outcome("unknown-svc", "raw/request/missing.log.gz", {}) is False

    _seed_ledger_row(sid, "raw/request/missing-run.log.gz", 987654)
    with pytest.raises(RuntimeError, match="originating cron run 987654 is missing"):
        record_ingest_object_outcome(sid, "raw/request/missing-run.log.gz", {"valid_records": 1})

    rum_run_id = metadata_db.start_cron_run(sid, "rum_discovery")
    _seed_ledger_row(sid, "raw/request/wrong-task.log.gz", rum_run_id, originating_task="log_discovery")
    with pytest.raises(RuntimeError, match="origin task does not match"):
        record_ingest_object_outcome(sid, "raw/request/wrong-task.log.gz", {"valid_records": 1})

    counters = _normalize_outcome_counters({"valid_records": 2})
    assert (counters["valid_records"], counters["malformed_records"], len(counters)) == (2, 0, 10)


@pytest.mark.parametrize(
    "counters",
    [{"unexpected": 1}, {"valid_records": -1}, {"valid_records": True}, {"valid_records": 1.5}],
)
def test_normalize_outcome_counters_rejects_unknown_and_invalid_values(counters):
    with pytest.raises(ValueError):
        _normalize_outcome_counters(counters)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("{broken", "invalid outcome_counters JSON"),
        ("[]", "must be a JSON object"),
        ('{"valid_records": -1}', "invalid outcome_counters values"),
    ],
)
def test_decode_outcome_counters_rejects_malformed_persisted_state(raw, message):
    with pytest.raises(RuntimeError, match=message):
        _decode_outcome_counters(raw)


def test_latest_cron_per_task_ranking_and_exclusions(sid):
    r1 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=1.0, status="success", run_id=r1)
    r2 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=2.0, status="error", run_id=r2, error_message="boom")

    r3 = metadata_db.start_cron_run(sid, "optimize")
    metadata_db.log_cron_run(sid, "optimize", duration_s=3.0, status="success", run_id=r3)
    metadata_db.start_cron_run(sid, "optimize")

    out = metadata_db.latest_cron_per_task(sid)
    assert set(out.keys()) == {"sync", "optimize"}
    assert (out["sync"]["status"], out["sync"]["error_message"]) == ("error", "boom")
    assert (out["optimize"]["status"], out["optimize"]["duration_s"]) == ("success", 3.0)

    sentinel = "Process interrupted by server restart"
    ra1 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(sid, "insights_prewarmer", duration_s=1.0, status="success", run_id=ra1)
    ra2 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(
        sid, "insights_prewarmer", duration_s=0.0, status="error", run_id=ra2, error_message=sentinel
    )
    assert (
        metadata_db.latest_cron_per_task(sid, exclude_error_messages=(sentinel,))["insights_prewarmer"]["status"]
        == "success"
    )
    assert metadata_db.latest_cron_per_task(sid)["insights_prewarmer"]["error_message"] == sentinel


# ── adaptive_stale_minutes, asn_names, sources ──────────────────────────────


def _seed_sync_run(sid: str, minutes_ago: float, *, status: str = "success", rows_ingested: int = 1) -> None:
    con = metadata_db.get_con(sid)
    ts = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, rows_ingested) VALUES (?, ?, ?, ?)",
        ("sync", ts, status, rows_ingested),
    )
    con.commit()


def test_adaptive_stale_minutes_behaviors():
    con = metadata_db.get_con("stale-short")
    for i in range(3):
        _seed_sync_run("stale-short", minutes_ago=i * 40)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-wide", minutes_ago=i * 40)
        _seed_sync_run("stale-wide", minutes_ago=i * 40 + 20, rows_ingested=0)
    _seed_sync_run("stale-wide", minutes_ago=5, status="error", rows_ingested=5)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 80

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-narrow", minutes_ago=i * 5)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-cap", minutes_ago=i * 10)
    _seed_sync_run("stale-cap", minutes_ago=15 * 10 + 2 * 24 * 60)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30, max_minutes=1440) == 1440

    class _BoomConnection:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such column: files_downloaded")

    assert metadata_db.adaptive_stale_minutes(_BoomConnection(), default_minutes=30) == 30


def test_asn_names_and_sources_registry(sid):
    assert metadata_db.lookup_asn_names(sid, []) == {}
    metadata_db.upsert_asn_names(sid, {})
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON", 14618: "AMAZON-AES"})
    assert metadata_db.lookup_asn_names(sid, [7922, 16509, 99999]) == {7922: "COMCAST", 16509: "AMAZON"}

    metadata_db.upsert_asn_names(sid, {7922: "New Name"})
    assert metadata_db.lookup_asn_names(sid, [7922])[7922] == "New Name"
    assert set(metadata_db.asn_ints_for_search(sid, "%AMAZON%")) == {16509, 14618}
    assert 16509 in metadata_db.asn_ints_for_search(sid, "%amazon%")

    con = metadata_db.get_con(sid)
    old = (datetime.now(UTC) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute("INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?)", (1, "OLD", old))
    con.commit()
    assert metadata_db.lookup_asn_names(sid, [1], max_age_days=30) == {}

    metadata_db.register_source(sid, "svc1", '{"a": 1}', "logs_svc1")
    metadata_db.register_source(sid, "svc1", '{"a": 2}', "logs_svc1")
    out = con.execute("SELECT config FROM sources WHERE name = ?", ("svc1",)).fetchone()
    assert json.loads(out["config"])["a"] == 1


# ── usage_log telemetry & reconciliation ────────────────────────────────────


def _seed_usage_log_row(
    sid: str,
    timestamp: str,
    operation_class: str = "A",
    operation_type: str = "PUT_OBJECT",
    process_context: str = "cron:sync",
    bytes_count: int = 1024,
) -> None:
    con = usage_log_db.get_con(sid)
    con.execute(
        "INSERT INTO usage_log "
        "(service_id, timestamp, operation_class, operation_type, url, bytes, "
        "duration_ms, function_name, process_context, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            sid,
            timestamp,
            operation_class,
            operation_type,
            "https://x.example/k",
            bytes_count,
            5.0,
            "ingest",
            process_context,
            "OK",
        ],
    )
    con.execute(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, 1, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + 1, "
        "bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "last_updated = EXCLUDED.last_updated",
        [sid, timestamp[:13], operation_class, operation_type, bytes_count, timestamp],
    )
    con.commit()


def _query_jan_usage(sid: str, **kwargs):
    return metadata_db.get_usage_logs(
        service_id=sid, start="2026-01-01T00:00:00Z", end="2026-01-31T23:59:59Z", **kwargs
    )


def test_usage_class_predicate_and_log_usage_calls(sid, caplog):
    for usage_type, want in {
        "": ("", []),
        "CDN": ("operation_class = 'CDN'", []),
        "FOS-A": ("operation_class = 'A'", []),
        "FOS-B": ("operation_class = 'B'", []),
        "FOS": ("operation_class IN ('A', 'B')", []),
        "custom-x": ("operation_class = ?", ["custom-x"]),
    }.items():
        assert _usage_class_predicate(usage_type) == want

    metadata_db.log_usage_calls(sid, [])
    assert usage_log_db.get_con(sid).execute("SELECT count(*) FROM usage_log").fetchone()[0] == 0

    metadata_db.log_usage_calls(
        sid,
        [
            {"method": "PUT_OBJECT", "service": "FOS"},
            {"method": "GET_OBJECT", "service": "FOS"},
            {"method": "LIST_OBJECTS_V2", "service": "FOS"},
            {"method": "GET", "service": "CDN"},
        ],
    )
    classes = [
        r["operation_class"]
        for r in usage_log_db.get_con(sid).execute("SELECT operation_class FROM usage_log ORDER BY id").fetchall()
    ]
    assert classes == ["A", "B", "A", "CDN"]

    metadata_db.clear_usage_log(sid)
    cases = [
        ("/file.parquet", "HIT · duckdb httpfs", 1000),
        ("/file2.parquet", "MISS, HIT · duckdb httpfs", 1000),
        ("/file3.parquet", "MISS, MISS · duckdb httpfs", 2000),
        ("/file4.parquet", "MISS, PASS · duckdb httpfs", 2000),
        ("/file5.parquet", "PASS, PASS · duckdb httpfs", 2000),
        ("/file6.parquet", "HIT, MISS · duckdb httpfs", 2000),
    ]
    for path, details, _ in cases:
        metadata_db.log_usage_calls(
            sid,
            [
                {
                    "method": "GET",
                    "service": "CDN",
                    "path": path,
                    "status": "200",
                    "time_ms": 10.0,
                    "bytes": 1000,
                    "details": details,
                }
            ],
        )
    logs, _, _ = metadata_db.get_usage_logs(sid, "2000-01-01", "2099-01-01")
    logs.sort(key=lambda x: x["url"])
    assert [(row["url"], row["bytes"]) for row in logs] == [(p, exp) for p, _, exp in cases]

    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        metadata_db.log_usage_calls(
            sid, [{"function_name": "ingest", "operation_type": "PUT_OBJECT", "url": "/x", "status": "OK"}]
        )
    assert any("Failed to log usage calls" in r.message for r in caplog.records)


def test_get_usage_logs_one_pass_aggregates_and_filters(sid):
    rows, total, agg = _query_jan_usage(sid)
    assert (
        rows == [] and total == 0 and (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_bytes"]) == (0, 0, 0)
    )

    con = usage_log_db.get_con(sid)
    con.executemany(
        "INSERT INTO usage_log "
        "(timestamp, service_id, operation_class, operation_type, url, status, "
        "duration_ms, function_name, process_context, bytes, count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("2026-05-25T10:00:00Z", sid, "A", "PUT_OBJECT", "u1", "OK", 1.0, "fn1", None, 100, 2),
            ("2026-05-25T10:00:01Z", sid, "A", "PUT_OBJECT", "u2", "OK", 1.0, "fn1", None, 200, 3),
            ("2026-05-25T10:00:02Z", sid, "A", "POST", "u3", "OK", 1.0, "fn1", None, 50, 1),
            ("2026-05-25T10:00:03Z", sid, "B", "GET_OBJECT", "u4", "OK", 1.0, "fn1", None, 500, 5),
            ("2026-05-25T10:00:04Z", sid, "CDN", "GET", "u5", "OK", 1.0, "fn1", None, 1000, 7),
            ("2026-05-25T10:00:05Z", sid, "CDN", "GET", "u6", "OK", 1.0, "fn1", None, 2000, 4),
        ],
    )
    con.executemany(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, "
        "bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "last_updated = EXCLUDED.last_updated",
        [
            (sid, "2026-05-25T10", "A", "PUT_OBJECT", 5, 300, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "A", "POST", 1, 50, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "B", "GET_OBJECT", 5, 500, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "CDN", "GET", 11, 3000, "2026-05-25T10:00:00Z"),
        ],
    )
    con.commit()

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T00:00:00Z", "2026-05-25T23:59:59Z")
    assert total == 22
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (6, 5, 11)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (3000, 850)
    assert agg["class_a_breakdown"] == {"PUT_OBJECT": 5, "POST": 1}
    assert agg["class_b_breakdown"] == {"GET_OBJECT": 5}


def test_get_usage_logs_query_variants(sid):
    for ts in ("2026-01-01T00:00:00Z", "2026-01-15T12:00:00Z", "2025-12-31T00:00:00Z", "2026-02-01T00:00:00Z"):
        _seed_usage_log_row(sid, ts)
    _seed_usage_log_row("other-svc", "2026-01-10T00:00:00Z")
    rows, total, _ = _query_jan_usage(sid)
    assert total == 2 and len(rows) == 2

    sid_f = f"{sid}-filters"
    _seed_usage_log_row(sid_f, "2026-01-10T00:00:00Z", operation_class="A", operation_type="OLDEST", bytes_count=100)
    _seed_usage_log_row(
        sid_f, "2026-01-10T00:01:00Z", operation_class="B", process_context="api:dashboard", bytes_count=500
    )
    _seed_usage_log_row(sid_f, "2026-01-15T00:00:00Z", operation_class="CDN", operation_type="NEWEST", bytes_count=1000)

    r_cdn, t_cdn, _ = _query_jan_usage(sid_f, usage_type="CDN")
    assert t_cdn == 1 and r_cdn[0]["operation_class"] == "CDN"
    r_fos, t_fos, _ = _query_jan_usage(sid_f, usage_type="FOS")
    assert t_fos == 2 and {r["operation_class"] for r in r_fos} == {"A", "B"}
    r_fosa, t_fosa, _ = _query_jan_usage(sid_f, usage_type="FOS-A")
    assert t_fosa == 1 and r_fosa[0]["operation_class"] == "A"
    r_ctx, t_ctx, _ = _query_jan_usage(sid_f, process_context="api")
    assert t_ctx == 1 and "api" in r_ctx[0]["process_context"]
    r_op, t_op, _ = _query_jan_usage(sid_f, operation_type="OLD")
    assert t_op == 1 and r_op[0]["operation_type"] == "OLDEST"

    r_all, _, agg = _query_jan_usage(sid_f)
    assert [r["operation_type"] for r in r_all] == ["NEWEST", "PUT_OBJECT", "OLDEST"]
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (1, 1, 1)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (1000, 600)

    r_page, t_page, _ = _query_jan_usage(sid_f, page=2, page_size=1)
    assert t_page == 3 and len(r_page) == 1

    _, _, agg_custom = _query_jan_usage(sid_f, usage_type="CUSTOM")
    assert (agg_custom["total_class_a"], agg_custom["total_class_b"]) == (0, 0)

    sid_sub = f"{sid}-subhour"
    for ts in ("2026-05-25T10:05:00Z", "2026-05-25T10:15:00Z", "2026-05-25T11:05:00Z"):
        _seed_usage_log_row(sid_sub, ts, operation_class="A")
    _, total_sub, agg_sub = metadata_db.get_usage_logs(sid_sub, "2026-05-25T10:00:00Z", "2026-05-25T10:30:00Z")
    assert total_sub == 2 and agg_sub["total_class_a"] == 2


def test_log_synthetic_usage_and_reconcile_fastly_stats(sid, caplog):
    assert metadata_db.log_synthetic_usage(sid, []) == 0
    assert metadata_db.log_synthetic_usage(sid, [{"method": "PUT"}, {"caller": "x"}]) == 0
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/b"}]) == 2
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}]) == 1
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}]) == 0

    urls = sorted(
        r["url"]
        for r in usage_log_db.get_con(sid)
        .execute("SELECT url FROM usage_log WHERE function_name = 'fastly.edge' ORDER BY url")
        .fetchall()
    )
    assert urls == ["/a", "/b", "/c"]

    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        assert metadata_db.log_synthetic_usage(sid, [{"path": "/x"}, {"path": "/y"}]) == 0
    assert any("Synthetic usage log failed" in r.message for r in caplog.records)

    assert metadata_db.reconcile_fastly_stats(sid, []) == 0
    assert (
        metadata_db.reconcile_fastly_stats(
            sid, [{"class_a": 10, "class_b": 5}, {"hour_iso": "not-a-date", "class_a": 1}]
        )
        == 0
    )

    for i in range(5):
        _seed_usage_log_row(sid, f"2026-05-22T13:0{i}:00Z", operation_class="A")
    assert (
        metadata_db.reconcile_fastly_stats(sid, [{"hour_iso": "2026-05-22T13:00:00Z", "class_a": 10, "class_b": 3}])
        == 2
    )
    rows = con.execute(
        "SELECT operation_class, count FROM usage_log WHERE function_name = 'fastly.reconciliation' ORDER BY operation_class"
    ).fetchall()
    assert {r["operation_class"]: r["count"] for r in rows} == {"A": 5, "B": 3}

    for i in range(2):
        _seed_usage_log_row(sid, f"2026-05-22T14:0{i}:00Z", operation_class="A")
    record = [{"hour_iso": "2026-05-22T14:00:00Z", "class_a": 10, "class_b": 0}]
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1
    r14 = con.execute(
        "SELECT count FROM usage_log WHERE function_name = 'fastly.reconciliation' AND operation_class = 'A' AND timestamp = ?",
        ("2026-05-22T14:00:00Z",),
    ).fetchall()
    assert len(r14) == 1 and r14[0]["count"] == 8

    for i in range(7):
        _seed_usage_log_row(sid, f"2026-05-22T15:0{i}:00Z", operation_class="A")
    assert (
        metadata_db.reconcile_fastly_stats(sid, [{"hour_iso": "2026-05-22T15:00:00Z", "class_a": 7, "class_b": 0}]) == 0
    )


# ── Postgres dialect shim & connection lifecycle ────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_sql_rewriting_helpers():
    assert (
        pgc._replace_placeholders_outside_literals("SELECT * FROM t WHERE a = ? AND b = 'literal ? mark' AND c = ?")
        == "SELECT * FROM t WHERE a = %s AND b = 'literal ? mark' AND c = %s"
    )
    assert pgc._replace_placeholders_outside_literals("SELECT 1") == "SELECT 1"

    for table in ("sources", "rdns"):
        assert (
            pgc._rewrite_insert_or(f"INSERT OR IGNORE INTO {table} (a, b) VALUES (?, ?)")
            == f"INSERT INTO {table} (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING"
        )

    qf_out = pgc._rewrite_insert_or("INSERT OR REPLACE INTO quarantined_files (service_id, file_name) VALUES (?, ?)")
    assert qf_out.startswith(
        "INSERT INTO quarantined_files (service_id, file_name) VALUES (?, ?) ON CONFLICT(service_id, file_name) DO UPDATE SET"
    )
    assert "source_name=EXCLUDED.source_name" in qf_out and "corrupt_rows=EXCLUDED.corrupt_rows" in qf_out

    v_out = pgc._rewrite_insert_or("INSERT OR REPLACE INTO views (id, service_id, name) VALUES (?, ?, ?)")
    assert "ON CONFLICT(id) DO UPDATE SET" in v_out and "service_id=EXCLUDED.service_id" in v_out

    with pytest.raises(NotImplementedError, match="brand_new_table"):
        pgc._rewrite_insert_or("INSERT OR IGNORE INTO brand_new_table (x) VALUES (?)")

    assert (
        pgc._rewrite_sql("INSERT INTO cron_runs (service_id, task) VALUES (?, ?)")
        == "INSERT INTO cron_runs (service_id, task) VALUES (%s, %s)"
    )
    assert "current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql("SELECT datetime('now')")
    assert "current_timestamp AT TIME ZONE 'UTC' + INTERVAL '-15 minutes'" in pgc._rewrite_sql(
        "SELECT datetime('now', '-15 minutes')"
    )
    assert "to_char(current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql(
        "SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
    )
    instr_out = pgc._rewrite_sql("SELECT instr(a, b), substr(a, 1, 2) FROM t")
    assert "strpos(a, b)" in instr_out and "substring(a, 1, 2)" in instr_out

    assert "EXCLUDED.a" in pgc._rewrite_sql("INSERT INTO t VALUES (?) ON CONFLICT(id) DO UPDATE SET a = excluded.a")
    assert pgc._rewrite_sql("SELECT 'excluded.something'") == "SELECT 'excluded.something'"

    sql1, id1 = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s)")
    assert sql1.endswith("RETURNING id") and id1 == "id"
    sql2, id2 = pgc._maybe_add_returning("INSERT INTO ingested_files (a) VALUES (%s)")
    assert sql2 == "INSERT INTO ingested_files (a) VALUES (%s)" and id2 is None
    sql3, id3 = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s) RETURNING id")
    assert sql3.count("RETURNING") == 1 and id3 is None


class _FakeCursor:
    def __init__(self, fetchone_result=None):
        self.executed: list[tuple[str, tuple]] = []
        self._fetchone_result = fetchone_result
        self.rowcount = 1

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))

    def executemany(self, sql, seq):
        self.executed.append((sql, tuple(seq)))

    def fetchone(self):
        return self._fetchone_result

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, fetchone_result=None):
        self._cursor = _FakeCursor(fetchone_result)

    def cursor(self):
        return self._cursor


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_connection_wrapper_execution_and_monitoring(monkeypatch):
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    wrapper._service_id = "svc-1"
    seen: dict = {}
    orig_register = query_registry.query_registry.register

    def _spy_register(db_type, sql, **kwargs):
        seen["db_type"] = db_type
        seen["service_id"] = kwargs.get("service_id")
        return orig_register(db_type, sql, **kwargs)

    with patch.object(query_registry.query_registry, "register", side_effect=_spy_register):
        wrapper.execute("SELECT * FROM t WHERE a = ?", (1,))
    assert conn._cursor.executed[0] == ("SELECT * FROM t WHERE a = %s", (1,))
    assert seen == {"db_type": "Postgres", "service_id": "svc-1"}

    wrapper.commit()
    wrapper.rollback()

    c_cron = _FakeConn(fetchone_result={"id": 42})
    w_cron = pgc.PgConnectionWrapper(c_cron, pool=MagicMock())
    cur = w_cron.execute("INSERT INTO cron_runs (task) VALUES (?)", ("log_discovery",))
    assert cur.lastrowid == 42 and "RETURNING id" in c_cron._cursor.executed[0][0]
    assert wrapper.execute("INSERT INTO ingested_files (a) VALUES (?)", (1,)).lastrowid is None

    class _RaisingCursor(_FakeCursor):
        def execute(self, sql, params=()):
            raise RuntimeError("boom")

    c_err = _FakeConn()
    c_err._cursor = _RaisingCursor()
    w_err = pgc.PgConnectionWrapper(c_err, pool=MagicMock())
    with patch.object(query_registry.query_registry, "deregister") as mock_deregister:
        with pytest.raises(RuntimeError, match="boom"):
            w_err.execute("SELECT 1")
    assert mock_deregister.call_count == 1 and isinstance(mock_deregister.call_args[1].get("error"), RuntimeError)

    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)
    w_noclose = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=False, pool=pool)
    w_noclose.close()
    pool.putconn.assert_not_called()
    w_close = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
    w_close.close()
    pool.putconn.assert_called_once_with(conn)


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_thread_and_readonly_connection_lifecycle(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    first = pgc.get_pg_thread_connection()
    assert first is pgc.get_pg_thread_connection() and pool.getconn.call_count == 1

    pgc.close_all_pg_connections()
    assert pool.putconn.call_count == 1
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2

    pgc.release_pg_thread_connection()
    assert pool.putconn.call_count == 2
    pgc.release_pg_thread_connection()
    assert pool.putconn.call_count == 2

    a = pgc.get_pg_readonly_connection()
    b = pgc.get_pg_readonly_connection()
    assert a is not b
    a.close()
    b.close()


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_thread_isolation_and_weakset_gc(monkeypatch):
    pool = MagicMock()
    conns = [_FakeConn(), _FakeConn(), _FakeConn()]
    pool.getconn.side_effect = conns
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()
    other_thread_wrapper = {}
    t = threading.Thread(target=lambda: other_thread_wrapper.setdefault("w", pgc.get_pg_thread_connection()))
    t.start()
    t.join()

    pgc.PgConnectionWrapper(_FakeConn())._finalizer()
    pgc.release_pg_thread_connection()
    owned_returns = [call.args[0] for call in pool.putconn.call_args_list if call.args[0] in conns]
    assert owned_returns == [conns[0]] and other_thread_wrapper["w"]._returned == [False]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 3

    pgc.close_all_pg_connections()
    other_thread_wrapper.clear()
    pool.getconn.side_effect = lambda: _FakeConn()

    conn_gc = _FakeConn()
    w_gc = pgc.PgConnectionWrapper(conn_gc, pool=pool)
    del w_gc
    gc.collect()
    assert len([c for c in pool.putconn.call_args_list if c.args[0] is conn_gc]) == 1

    conn_closed = _FakeConn()
    w_closed = pgc.PgConnectionWrapper(conn_closed, return_to_pool_on_close=True, pool=pool)
    w_closed.close()
    del w_closed
    gc.collect()
    assert len([c for c in pool.putconn.call_args_list if c.args[0] is conn_closed]) == 1

    pgc.close_all_pg_connections()
    t2 = threading.Thread(target=pgc.get_pg_thread_connection)
    t2.start()
    t2.join()
    gc.collect()
    assert len(pgc._checked_out) == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_is_postgres_and_compat_row(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    assert pgc.is_postgres() is False
    monkeypatch.setenv("METADATA_DSN", "postgresql://x/y")
    assert pgc.is_postgres() is True

    row = pgc._CompatRow(["count", "name"], [7, "svc-a"])
    assert (row[0], row[1], row[0:2]) == (7, "svc-a", (7, "svc-a"))
    assert (row["count"], row["name"], row.get("count"), row.get("absent", "dflt")) == (7, "svc-a", 7, "dflt")
    assert dict(row) == {"count": 7, "name": "svc-a"} and list(row.keys()) == ["count", "name"]
    with pytest.raises(KeyError):
        _ = row["nope"]
    with pytest.raises(IndexError):
        _ = row[9]

    cur = MagicMock(description=None)
    assert pgc._compat_row_factory(cur)((1, 2)) == (1, 2)
    cur.description = [MagicMock(), MagicMock()]
    cur.description[0].name, cur.description[1].name = "a", "b"
    made = pgc._compat_row_factory(cur)((10, 20))
    assert made[0] == 10 and made["b"] == 20


# ── Metadata reconciliation (storage stats + cleanup) ──────────────────────


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = usage_log_db.get_con(service_id)
    now_ts = iso_z(datetime.now(UTC) - timedelta(days=days_ago))
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) "
        "VALUES (?, ?, 'A', 'PUT_OBJECT', 0, 1)",
        [(now_ts, service_id) for _ in range(rows)],
    )
    con.execute(
        """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
           VALUES (?, ?, 'A', 'PUT_OBJECT', ?, 0, ?)
           ON CONFLICT (service_id, hour, operation_class, operation_type)
           DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, last_updated = EXCLUDED.last_updated""",
        (service_id, now_ts[:13], rows, now_ts),
    )
    con.commit()


def _seed_ingested_file(service_id: str, rows: int, days_ago: int = 0, source_name: str = "fos") -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{source_name}-{days_ago}d-{i}.gz", source_name) for i in range(rows)],
    )
    con.commit()


def _seed_cron_run(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        f"VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_and_schema_tables():
    stats = reconciliation.get_metadata_storage_stats("svc-stats-fresh")
    for table in ("usage_log", "ingested_files", "cron_runs", "saved_views", "audit_log", "in_flight_buffers"):
        assert stats["tables"][table]["rows"] == 0
    assert stats["db_bytes"] is None and stats["db_path"] is None

    sid = "svc-stats-seeded"
    _seed_usage_log(sid, 7)
    _seed_ingested_file(sid, 3)
    _seed_cron_run(sid, 2)
    s2 = reconciliation.get_metadata_storage_stats(sid, force=True)
    assert (
        s2["tables"]["usage_log"]["rows"],
        s2["tables"]["ingested_files"]["rows"],
        s2["tables"]["cron_runs"]["rows"],
    ) == (7, 3, 2)
    assert s2["tables"]["cron_runs"]["bytes"] > 0 and s2["db_bytes"] is None

    existing = {
        r[0]
        for r in _con(sid).execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    }
    for sql_table, _ in reconciliation._STATS_TABLES:
        if sql_table != "usage_log":
            assert sql_table in existing


@pytest.mark.usefixtures("_reset_pg_state")
def test_is_ingested_files_dedup_active(monkeypatch):
    assert reconciliation.is_ingested_files_dedup_active("svc-no-cfg") is True
    for cfg, expected in (
        ({"provisioning": {"cron_sync": {"delete_after": True}}}, True),
        ({"provisioning": {"cron_sync": {"delete_after": False}}}, False),
        ({"provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}}}, False),
    ):
        monkeypatch.setattr(svcconfig, "load_config", lambda _s, c=cfg: c)
        assert reconciliation.is_ingested_files_dedup_active("svc-cfg") is expected


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_metadata_retention_and_events(monkeypatch):
    assert reconciliation._retention_timestamp_expr("ingested_at") == "CAST(ingested_at AS TIMESTAMPTZ)"

    sid = "svc-cleanup-aged"
    _seed_usage_log(sid, 5, days_ago=10)
    _seed_usage_log(sid, 3, days_ago=0)
    res = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})
    assert (res["deleted"]["usage_log"], res["after"]["usage_log"], res["vacuumed"]) == (5, 3, False)

    for bad_ret in (0, -5, "not-a-number"):
        sid_b = f"svc-cleanup-{bad_ret}"
        _seed_usage_log(sid_b, 4, days_ago=400)
        r_b = reconciliation.cleanup_metadata(sid_b, retention={"usage_log_days": bad_ret})
        assert (r_b["deleted"]["usage_log"], r_b["after"]["usage_log"]) == (0, 4)

    sid_def = "svc-cleanup-default"
    _seed_cron_run(sid_def, 4, days_ago=400)
    assert reconciliation.cleanup_metadata(sid_def)["deleted"]["cron_runs"] == 4

    sid_ev = "svc-cleanup-events"
    _seed_usage_log(sid_ev, 2, days_ago=10)
    events: list[dict] = []
    reconciliation.cleanup_metadata(
        sid_ev,
        retention={"usage_log_days": 1, "ingested_files_days": 1, "cron_runs_days": 1},
        on_event=events.append,
    )
    assert any(e["type"] == "status" for e in events) and any(e["type"] == "progress" for e in events)
    last_p = [e for e in events if e["type"] == "progress"][-1]
    assert last_p["current"] == last_p["total"]

    sid_bad_cb = "svc-cleanup-bad-cb"
    _seed_usage_log(sid_bad_cb, 3, days_ago=10)
    assert (
        reconciliation.cleanup_metadata(
            sid_bad_cb,
            retention={"usage_log_days": 1},
            on_event=lambda _e: (_ for _ in ()).throw(RuntimeError("cb fail")),
        )["deleted"]["usage_log"]
        == 3
    )

    assert (
        reconciliation.cleanup_metadata("svc-no-rollups", retention={"usage_log_days": 1, "rollups_days": 0})[
            "rollups_deleted"
        ]
        == 0
    )
    monkeypatch.setattr(_db, "get_source_for_service", lambda _s: None)
    assert (
        reconciliation.cleanup_metadata("svc-no-src", retention={"usage_log_days": 1, "rollups_days": 7})[
            "rollups_deleted"
        ]
        == 0
    )

    sid_fo = "svc-cleanup-forced-off"
    _seed_ingested_file(sid_fo, 5, days_ago=400)
    monkeypatch.setattr(svcconfig, "load_config", lambda _s: {"provisioning": {"cron_sync": {"delete_after": False}}})
    ev_fo: list[dict] = []
    r_fo = reconciliation.cleanup_metadata(sid_fo, retention={"ingested_files_days": 30}, on_event=ev_fo.append)
    assert (r_fo["deleted"]["ingested_files"], r_fo["after"]["ingested_files"]) == (0, 5)
    assert any(e["type"] == "status" and "dedup gate" in e.get("message", "") for e in ev_fo)


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_rum_ingested_files_and_no_reingest(monkeypatch):
    sid = "svc-cleanup-rum-dedup"
    _seed_ingested_file(sid, 3, days_ago=400)
    con = _con(sid)
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-400 days'), 1, 100)",
        [(f"raw/rum/old-{t}-{i}.gz", t) for t in ("client_vitals", "client_errors") for i in range(2)],
    )
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-10 days'), 1, 100)",
        [(f"raw/rum/recent-{t}.gz", t) for t in ("client_vitals", "client_errors")],
    )
    con.commit()
    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})
    assert (res["deleted"]["ingested_files"], res["after"]["ingested_files"]) == (7, 2)

    con.execute("DELETE FROM ingested_files")
    sid2 = "svc-cleanup-no-reingest"
    con2 = _con(sid2)
    old_file_key = "s3://test-bucket/raw/rum/year=2025/month=08/day=01/hour=12/minute=00/old.json.gz"
    con2.execute(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'test_service', 'client_vitals', datetime('now', '-400 days'), 1, 100)",
        (old_file_key,),
    )
    con2.commit()
    reconciliation.cleanup_metadata(sid2, retention={"ingested_files_days": 1})
    assert con2.execute("SELECT COUNT(*) FROM ingested_files WHERE file_name = ?", (old_file_key,)).fetchone()[0] == 0

    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr(
        "backend.core.rum_ingest.get_source_for_service",
        lambda s: {"name": sid2, "bucket": "test-bucket", "service_id": sid2},
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda s: con2)
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda s, t: 1)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", lambda *a, **kw: None)

    assert list(ingest_rum_logs(sid2)) == [("started", 1), ("done", 0)]
    assert con2.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0] == 0


def _poison_summary(service_id: str, *, file_count: int, total_rows: int, total_bytes: int) -> None:
    con = _con(service_id)
    con.execute(
        "INSERT INTO ingested_files_summary "
        "(source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested) "
        "VALUES (?, ?, ?, ?, ?, 'raw/poison.gz', datetime('now')) "
        "ON CONFLICT(source_name) DO UPDATE SET "
        "file_count=excluded.file_count, total_rows=excluded.total_rows, "
        "total_bytes=excluded.total_bytes, count_with_bytes=excluded.count_with_bytes",
        (service_id, file_count, total_rows, total_bytes, file_count),
    )
    con.commit()


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_ingested_files_summary_recompute():
    sid = "svc-rollup-recompute"
    _seed_ingested_file(sid, 5, days_ago=10, source_name=sid)
    _seed_ingested_file(sid, 3, days_ago=0, source_name=sid)
    _poison_summary(sid, file_count=999_999, total_rows=999_999, total_bytes=99_999_900)

    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert (res["deleted"]["ingested_files"], res["after"]["ingested_files"]) == (5, 3)
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert (summary["file_count"], summary["total_rows"], summary["total_bytes"]) == (3, 3, 300)

    sid2 = "svc-rollup-noop"
    _seed_ingested_file(sid2, 3, days_ago=0, source_name=sid2)
    _poison_summary(sid2, file_count=42, total_rows=4242, total_bytes=4200)
    assert reconciliation.cleanup_metadata(sid2, retention={"ingested_files_days": 7})["deleted"]["ingested_files"] == 0
    s2 = metadata_db.get_ingested_files_status_summary(sid2)
    assert (s2["file_count"], s2["total_rows"]) == (42, 4242)
