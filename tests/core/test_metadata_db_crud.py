"""CRUD-layer tests for ``backend.core.metadata``.

The schema migration tests live in ``test_metadata_db_schema.py``; the
concurrency tests in ``test_metadata_db_concurrency.py``; the orphan
reaping in ``test_metadata_db_reap.py``. This file covers the
domain-table read/write helpers that don't fit elsewhere: views,
audit logs, ingested files, cron run lifecycle, asn name cache,
sources registry, and usage_log telemetry.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from backend.core import metadata as metadata_db
from backend.core.metadata import usage_log_db
from backend.core.metadata.cron_log import (
    _decode_outcome_counters,
    _normalize_outcome_counters,
    record_ingest_object_outcome,
)
from backend.core.metadata.usage_log import _usage_class_predicate


@pytest.fixture
def sid() -> str:
    return "test-svc-md"


# ── Shared test helpers ─────────────────────────────────────────────────────


def _seed_view_directly(sid: str, view_id: str = "old", name: str = "Old View") -> None:
    """Insert a view via raw SQL to seed state without constructing a Pydantic model."""
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
    """Insert an ingested_files row via raw SQL to control ingested_at / file_date."""
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


# ── replace_views_for_service ───────────────────────────────────────────────


def test_replace_views_for_service_clears_existing_and_inserts_new(sid):
    """state_sync.import_admin_state overwrites analyst saved-views with the admin's."""
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


def test_replace_views_for_service_with_empty_list_clears_all(sid):
    """Empty list → DELETE only, no INSERT."""
    _seed_view_directly(sid, view_id="v1", name="x")
    metadata_db.replace_views_for_service(sid, [])
    assert metadata_db.list_views(sid) == []


# ── audit_logs ──────────────────────────────────────────────────────────────


def test_record_audit_persists_event_with_details_json(sid):
    """``details`` is stored as a JSON string for ``list_audit``."""
    metadata_db.record_audit(sid, "alert_created", {"alert_id": "a1", "name": "5xx"}, actor="dmichael")
    entries = metadata_db.list_audit(sid)
    assert len(entries) == 1
    assert (entries[0]["event_type"], entries[0]["actor"]) == ("alert_created", "dmichael")
    assert json.loads(entries[0]["details"]) == {"alert_id": "a1", "name": "5xx"}


def test_list_audit_filters_by_since_timestamp(sid):
    """``since`` constrains the result to entries newer than the given timestamp."""
    metadata_db.record_audit(sid, "old_event", {}, actor="x")
    assert metadata_db.list_audit(sid, since="2099-01-01T00:00:00Z") == []
    assert len(metadata_db.list_audit(sid, since="2000-01-01T00:00:00Z")) == 1


def test_list_audit_respects_limit(sid):
    for i in range(5):
        metadata_db.record_audit(sid, f"event_{i}", {}, actor="x")
    assert len(metadata_db.list_audit(sid, limit=2)) == 2


def test_export_audit_matches_list_audit_shape(sid):
    """state_sync.export uses this — shape-drift between list/export breaks cross-pod sync."""
    metadata_db.record_audit(sid, "evt", {"k": "v"}, actor="a")
    assert metadata_db.list_audit(sid) == metadata_db.export_audit(sid)


def test_replace_audit_for_service_clears_and_inserts(sid):
    metadata_db.record_audit(sid, "evt_old", {}, actor="x")
    entry = {
        "timestamp": "2026-01-01T00:00:00Z",
        "source_name": sid,
        "event_type": "evt_new",
        "details": json.dumps({"k": "v"}),
        "actor": "admin",
    }
    metadata_db.replace_audit_for_service(sid, [entry])
    assert {e["event_type"] for e in metadata_db.list_audit(sid)} == {"evt_new"}


# ── ingested_files ──────────────────────────────────────────────────────────


def test_insert_and_get_ingested_filenames_round_trip(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000), ("b.gz", 200, 8000)])
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz", "b.gz"}


def test_get_reclaimable_strand_filenames_requires_durable_and_post_epoch(sid):
    """Reconcile only deletes strands with row_count>0 ingested at/after the durability epoch."""
    metadata_db.insert_ingested_files(
        sid,
        [("durable.gz", 100, 5000), ("marker.gz", 0, 1200), ("legacy.gz", 100, 8000)],
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


def test_get_reclaimable_strand_filenames_restricts_to_candidates(sid):
    """Only asked-for file_names are classified, and an empty candidate set short-circuits."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 9, 100), ("b.gz", 9, 100), ("marker.gz", 0, 100)])
    past = "2000-01-01 00:00:00"
    assert metadata_db.get_reclaimable_strand_filenames(sid, {"a.gz", "marker.gz"}, past) == {"a.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, set(), past) == set()


def test_insert_ingested_files_upserts_on_conflict(sid):
    """Re-inserting an existing (file_name, source_name) pair updates counts."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000)])
    metadata_db.insert_ingested_files(sid, [("a.gz", 999, 99999)])

    files = metadata_db.list_ingested_files(sid)
    assert len(files) == 1
    assert (files[0]["row_count"], files[0]["file_size_bytes"]) == (999, 99999)


def test_list_ingested_files_returns_newest_first(sid):
    """Sort order is ``ORDER BY ingested_at DESC``."""
    _insert_raw_ingested_file(sid, "first.gz", row_count=1, file_size_bytes=100, ingested_at="2026-01-01T00:00:00Z")
    _insert_raw_ingested_file(sid, "second.gz", row_count=2, file_size_bytes=200, ingested_at="2026-01-02T00:00:00Z")
    assert [f["file_name"] for f in metadata_db.list_ingested_files(sid)] == ["second.gz", "first.gz"]


def test_get_node_count_avg_groups_by_basename_timestamp(sid):
    """avg(files-per-flush) groups on the 19-char timestamp prefix of the basename."""
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-aaa.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-bbb.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:30.000-ccc.log.gz", 1, 1),
        ],
    )
    assert metadata_db.get_node_count_avg(sid) == 1.5


def test_get_node_count_avg_returns_none_when_empty(sid):
    assert metadata_db.get_node_count_avg(sid) is None


def test_get_node_count_avg_combines_canonical_and_legacy_basenames(sid):
    """Fast/slow split produces the same average across canonical and legacy rows."""
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-a.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 1, 1),
            *[(f"s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-{ch}.log.gz", 1, 1) for ch in ("c", "d", "e", "f")],
        ],
    )
    for minute, suffix in (("32", "x"), ("33", "y")):
        _insert_raw_ingested_file(
            sid, f"s3://b/raw/2026-05-15/23/2026-05-15T23:{minute}:00.000-legacy-{suffix}.log.gz", null_file_date=True
        )
    assert metadata_db.get_node_count_avg(sid) == 2.0


def test_get_node_count_avg_slow_arm_skips_non_canonical_basenames(sid):
    """Slow arm gates on ``instr(file_name, 'T') >= 11`` so junk rows don't crash."""
    _insert_raw_ingested_file(sid, "short-T.gz", null_file_date=True)
    _insert_raw_ingested_file(sid, "lowercase-only.gz", null_file_date=True)
    assert metadata_db.get_node_count_avg(sid) is None


def test_get_log_accounting_counts_groups_by_filename_when_iso_prefix_present(sid):
    """ISO-prefixed basenames bucket by emission time pulled from the path."""
    _insert_raw_ingested_file(
        sid,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz",
        row_count=100,
        ingested_at="2026-05-15T23:00:05",
    )
    _insert_raw_ingested_file(
        sid,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz",
        row_count=250,
        ingested_at="2026-05-15T23:30:05",
    )
    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (350, 2)}


def test_get_log_accounting_counts_uses_file_date_fast_arm_when_populated(sid):
    """When file_date is populated via insert_ingested_files, the fast UNION arm runs."""
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz", 100, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 250, 1),
        ],
    )
    fd_rows = (
        metadata_db.get_con(sid)
        .execute("SELECT file_name, file_date FROM ingested_files WHERE source_name = ?", (sid,))
        .fetchall()
    )
    assert all(str(r["file_date"]) == "2026-05-15" for r in fd_rows)
    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (350, 2)}


def test_get_log_accounting_counts_falls_back_to_ingested_at_for_non_iso_filenames(sid):
    """When the basename has no ISO prefix, the bucket falls back to ingested_at."""
    _insert_raw_ingested_file(sid, "raw/legacy.gz", row_count=42, ingested_at="2026-05-15T23:00:00")
    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (42, 1)}


# ── ingested_filenames dedup cache ──────────────────────────────────────────


def test_dedup_cache_populates_on_first_bounded_read(sid):
    """First bounded call hits DB; second call returns cached set without touching DB."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100), ("b.gz", 2, 200)])
    metadata_db._clear_ingested_filenames_cache(sid)
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}

    con = metadata_db.get_con(sid)
    con.execute("DELETE FROM ingested_files WHERE source_name = ?", (sid,))
    con.commit()
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}


def test_insert_ingested_files_extends_dedup_cache(sid):
    """After the cache is warm, ``insert_ingested_files`` adds the new filenames."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)
    metadata_db.insert_ingested_files(sid, [("b.gz", 2, 200)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}


def test_insert_skipping_cache_when_unpopulated(sid):
    """``insert_ingested_files`` does not prematurely seed an empty cache."""
    metadata_db.insert_ingested_files(sid, [("legacy-1.gz", 1, 100), ("legacy-2.gz", 1, 100)])
    metadata_db._clear_ingested_filenames_cache(sid)
    metadata_db.insert_ingested_files(sid, [("new.gz", 1, 100)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"legacy-1.gz", "legacy-2.gz", "new.gz"}


def test_unbounded_read_invalidates_cache(sid):
    """Unbounded reads invalidate the cache so the next bounded read sees DB changes."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)
    _insert_raw_ingested_file(sid, "sideloaded.gz", row_count=1, file_size_bytes=100)

    assert metadata_db.get_ingested_filenames(sid) == {"a.gz", "sideloaded.gz"}
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "sideloaded.gz"}


def test_teardown_invalidates_cache(sid):
    """``teardown`` drops the service entry from the dedup cache."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)
    assert (sid, "logs") in metadata_db._ingested_filenames_cache
    metadata_db.teardown(sid)
    assert (sid, "logs") not in metadata_db._ingested_filenames_cache


def test_dedup_cache_returns_a_copy(sid):
    """Callers receive a copy so mutations do not corrupt the cached set."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    snapshot = metadata_db.get_ingested_filenames(sid, limit=200)
    snapshot.add("mutated.gz")
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz"}


# ── cron_runs: start + log + busy + summary ────────────────────────────────


def test_start_cron_run_returns_a_run_id(sid):
    run_id = metadata_db.start_cron_run(sid, "sync")
    assert isinstance(run_id, int) and run_id > 0


def test_start_cron_run_raises_when_task_already_running(sid):
    metadata_db.start_cron_run(sid, "sync")
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(sid, "sync")


def test_start_cron_run_gate_is_per_service_not_global(sid):
    """crash SEAM-01: a 'running' sync on service A must NOT block a sync on service B."""
    svc_a, svc_b = "cronseam-a", "cronseam-b"
    assert metadata_db.start_cron_run(svc_a, "sync") > 0
    assert metadata_db.start_cron_run(svc_b, "sync") > 0
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(svc_a, "sync")


def test_start_cron_run_reaps_orphans_before_busy_check(sid):
    """An old 'running' row past the orphan threshold is flipped to 'error' before busy check."""
    con = metadata_db.get_con(sid)
    old_ts = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, parquet_keys) VALUES (?, ?, 'running', '[]')",
        ("sync", old_ts),
    )
    con.commit()
    assert isinstance(metadata_db.start_cron_run(sid, "sync"), int)


def test_log_cron_run_updates_existing_when_run_id_provided(sid):
    run_id = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(
        sid, "sync", duration_s=12.5, status="success", files_downloaded=10, rows_ingested=1000, run_id=run_id
    )
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, duration_s, rows_ingested FROM cron_runs WHERE id = ?", (run_id,))
        .fetchone()
    )
    assert (row["status"], row["duration_s"], row["rows_ingested"]) == ("success", 12.5, 1000)


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


def test_record_ingest_object_outcome_replaces_its_origin_run_contribution(sid):
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


def test_record_ingest_object_outcome_returns_false_for_unknown_object(sid):
    assert record_ingest_object_outcome(sid, "raw/request/missing.log.gz", {}) is False
    assert (
        metadata_db.get_con(sid).execute("SELECT count(*) FROM cron_runs WHERE service_id = ?", (sid,)).fetchone()[0]
        == 0
    )


def test_record_ingest_object_outcome_rejects_missing_origin_run(sid):
    _seed_ledger_row(sid, "raw/request/missing-run.log.gz", 987654)
    with pytest.raises(RuntimeError, match="originating cron run 987654 is missing"):
        record_ingest_object_outcome(sid, "raw/request/missing-run.log.gz", {"valid_records": 1})

    row = (
        metadata_db.get_con(sid)
        .execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
            (sid, "raw/request/missing-run.log.gz"),
        )
        .fetchone()
    )
    assert row["outcome_counters"] == "{}"


def test_record_ingest_object_outcome_rejects_mismatched_origin_task(sid):
    run_id = metadata_db.start_cron_run(sid, "rum_discovery")
    _seed_ledger_row(sid, "raw/request/wrong-task.log.gz", run_id, originating_task="log_discovery")
    with pytest.raises(RuntimeError, match="origin task does not match"):
        record_ingest_object_outcome(sid, "raw/request/wrong-task.log.gz", {"valid_records": 1})

    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, sid))
        .fetchone()
    )
    assert row["status"] == "running" and row["outcome_counters"] in (None, "{}")


def test_normalize_outcome_counters_defaults_omitted_values_to_zero():
    counters = _normalize_outcome_counters({"valid_records": 2})
    assert (counters["valid_records"], counters["malformed_records"], counters["objects_failed"]) == (2, 0, 0)
    assert len(counters) == 10


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


def test_log_cron_run_inserts_when_no_run_id(sid):
    """Path used by code that didn't go through start_cron_run (retries, manual ingest)."""
    metadata_db.log_cron_run(sid, "manual", duration_s=1.0, status="success", files_downloaded=1)
    rows = metadata_db.get_con(sid).execute("SELECT task, status FROM cron_runs WHERE task = 'manual'").fetchall()
    assert len(rows) == 1 and rows[0]["status"] == "success"


def test_cron_busy_returns_true_when_within_threshold(sid):
    metadata_db.start_cron_run(sid, "sync")
    assert metadata_db.cron_busy(sid) is True


def test_cron_busy_returns_false_after_log_completes_run(sid):
    run_id = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=1.0, status="success", run_id=run_id)
    assert metadata_db.cron_busy(sid) is False


def test_cron_busy_returns_false_when_no_runs(sid):
    assert metadata_db.cron_busy(sid) is False


def test_latest_cron_per_task_picks_latest_non_running_per_task(sid):
    """Returns one row per task — the latest non-running one."""
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


def test_latest_cron_per_task_excludes_boot_reap_sentinel_before_ranking(sid):
    """``exclude_error_messages`` drops boot-reap rows before per-task ranking."""
    sentinel = "Process interrupted by server restart"

    ra1 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(sid, "insights_prewarmer", duration_s=1.0, status="success", run_id=ra1)
    ra2 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(
        sid, "insights_prewarmer", duration_s=0.0, status="error", run_id=ra2, error_message=sentinel
    )

    rb1 = metadata_db.start_cron_run(sid, "optimize")
    metadata_db.log_cron_run(sid, "optimize", duration_s=2.0, status="error", run_id=rb1, error_message="real boom")

    out = metadata_db.latest_cron_per_task(sid, exclude_error_messages=(sentinel,))
    assert out["insights_prewarmer"]["status"] == "success"
    assert (out["optimize"]["status"], out["optimize"]["error_message"]) == ("error", "real boom")

    out_default = metadata_db.latest_cron_per_task(sid)
    assert (out_default["insights_prewarmer"]["status"], out_default["insights_prewarmer"]["error_message"]) == (
        "error",
        sentinel,
    )


def test_get_usage_logs_aggregates_and_breaks_down_in_one_pass(sid):
    """Aggregate totals and per-type breakdown come from a single GROUP BY scan."""
    con = usage_log_db.get_con(sid)
    rows = [
        ("2026-05-25T10:00:00Z", sid, "A", "PUT_OBJECT", "u1", "OK", 1.0, "fn1", None, 100, 2),
        ("2026-05-25T10:00:01Z", sid, "A", "PUT_OBJECT", "u2", "OK", 1.0, "fn1", None, 200, 3),
        ("2026-05-25T10:00:02Z", sid, "A", "POST", "u3", "OK", 1.0, "fn1", None, 50, 1),
        ("2026-05-25T10:00:03Z", sid, "B", "GET_OBJECT", "u4", "OK", 1.0, "fn1", None, 500, 5),
        ("2026-05-25T10:00:04Z", sid, "CDN", "GET", "u5", "OK", 1.0, "fn1", None, 1000, 7),
        ("2026-05-25T10:00:05Z", sid, "CDN", "GET", "u6", "OK", 1.0, "fn1", None, 2000, 4),
    ]
    con.executemany(
        "INSERT INTO usage_log "
        "(timestamp, service_id, operation_class, operation_type, url, status, "
        "duration_ms, function_name, process_context, bytes, count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    summary_rows = [
        (sid, "2026-05-25T10", "A", "PUT_OBJECT", 5, 300, "2026-05-25T10:00:00Z"),
        (sid, "2026-05-25T10", "A", "POST", 1, 50, "2026-05-25T10:00:00Z"),
        (sid, "2026-05-25T10", "B", "GET_OBJECT", 5, 500, "2026-05-25T10:00:00Z"),
        (sid, "2026-05-25T10", "CDN", "GET", 11, 3000, "2026-05-25T10:00:00Z"),
    ]
    con.executemany(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, "
        "bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "last_updated = EXCLUDED.last_updated",
        summary_rows,
    )
    con.commit()

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T00:00:00Z", "2026-05-25T23:59:59Z")
    assert total == 22
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (6, 5, 11)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (3000, 850)
    assert agg["class_a_breakdown"] == {"PUT_OBJECT": 5, "POST": 1}
    assert agg["class_b_breakdown"] == {"GET_OBJECT": 5}


def test_usage_class_predicate_matches_across_callers():
    """``_usage_class_predicate`` is shared by ``get_usage_logs`` and ``iter_usage_logs_chunks``."""
    expected = {
        "": ("", []),
        "CDN": ("operation_class = 'CDN'", []),
        "FOS-A": ("operation_class = 'A'", []),
        "FOS-B": ("operation_class = 'B'", []),
        "FOS": ("operation_class IN ('A', 'B')", []),
        "custom-x": ("operation_class = ?", ["custom-x"]),
    }
    for usage_type, want in expected.items():
        assert _usage_class_predicate(usage_type) == want


# ── adaptive_stale_minutes (SRE-22) ─────────────────────────────────────────


def _seed_sync_run(sid: str, minutes_ago: float, *, status: str = "success", rows_ingested: int = 1) -> None:
    """Insert a terminal `sync` cron_runs row directly for percentile tests."""
    con = metadata_db.get_con(sid)
    ts = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, rows_ingested) VALUES (?, ?, ?, ?)",
        ("sync", ts, status, rows_ingested),
    )
    con.commit()


def test_adaptive_stale_minutes_falls_back_with_too_little_history(sid):
    for i in range(3):
        _seed_sync_run(sid, minutes_ago=i * 40)
    assert metadata_db.adaptive_stale_minutes(metadata_db.get_con(sid), default_minutes=30) == 30


def test_adaptive_stale_minutes_widens_to_2x_p95_gap_for_a_steady_cadence(sid):
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 40)
    assert metadata_db.adaptive_stale_minutes(metadata_db.get_con(sid), default_minutes=30) == 80


def test_adaptive_stale_minutes_never_narrows_below_default(sid):
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 5)
    assert metadata_db.adaptive_stale_minutes(metadata_db.get_con(sid), default_minutes=30) == 30


def test_adaptive_stale_minutes_caps_at_max_minutes_on_a_huge_outlier(sid):
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 10)
    _seed_sync_run(sid, minutes_ago=15 * 10 + 2 * 24 * 60)
    assert metadata_db.adaptive_stale_minutes(metadata_db.get_con(sid), default_minutes=30, max_minutes=1440) == 1440


def test_adaptive_stale_minutes_ignores_empty_and_errored_runs(sid):
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 40)
        _seed_sync_run(sid, minutes_ago=i * 40 + 20, rows_ingested=0)
    _seed_sync_run(sid, minutes_ago=5, status="error", rows_ingested=5)
    assert metadata_db.adaptive_stale_minutes(metadata_db.get_con(sid), default_minutes=30) == 80


def test_adaptive_stale_minutes_falls_back_on_query_error():
    class _BoomConnection:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such column: files_downloaded")

    assert metadata_db.adaptive_stale_minutes(_BoomConnection(), default_minutes=30) == 30


# ── asn_names cache ─────────────────────────────────────────────────────────


def test_lookup_asn_names_returns_only_fresh_entries(sid):
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON"})
    out = metadata_db.lookup_asn_names(sid, [7922, 16509, 99999])
    assert out == {7922: "COMCAST", 16509: "AMAZON"}


def test_lookup_asn_names_excludes_stale_entries(sid):
    con = metadata_db.get_con(sid)
    old = (datetime.now(UTC) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute("INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?)", (1, "OLD", old))
    con.commit()
    assert metadata_db.lookup_asn_names(sid, [1], max_age_days=30) == {}


def test_lookup_asn_names_returns_empty_on_empty_input(sid):
    assert metadata_db.lookup_asn_names(sid, []) == {}


def test_upsert_asn_names_upserts_on_conflict(sid):
    metadata_db.upsert_asn_names(sid, {7922: "Old Name"})
    metadata_db.upsert_asn_names(sid, {7922: "New Name"})
    assert metadata_db.lookup_asn_names(sid, [7922])[7922] == "New Name"


def test_upsert_asn_names_noops_on_empty_dict(sid):
    metadata_db.upsert_asn_names(sid, {})


def test_asn_ints_for_search_returns_matching_asns(sid):
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON", 14618: "AMAZON-AES"})
    assert set(metadata_db.asn_ints_for_search(sid, "%AMAZON%")) == {16509, 14618}


def test_asn_ints_for_search_is_case_insensitive(sid):
    metadata_db.upsert_asn_names(sid, {16509: "AMAZON"})
    assert 16509 in metadata_db.asn_ints_for_search(sid, "%amazon%")


# ── sources registry ────────────────────────────────────────────────────────


def test_register_source_is_idempotent(sid):
    """``INSERT OR IGNORE`` — the second register call is a no-op."""
    metadata_db.register_source(sid, "svc1", '{"a": 1}', "logs_svc1")
    metadata_db.register_source(sid, "svc1", '{"a": 2}', "logs_svc1")
    out = metadata_db.get_con(sid).execute("SELECT config FROM sources WHERE name = ?", ("svc1",)).fetchone()
    assert json.loads(out["config"])["a"] == 1


# ── usage_log telemetry ────────────────────────────────────────────────────


def test_log_usage_calls_classifies_fos_class_a_correctly(sid):
    """FOS PUT_OBJECT / POST_OBJECT / COPY_OBJECT / LIST_OBJECTS_V2 are Class A; others are Class B."""
    metadata_db.log_usage_calls(
        sid,
        [
            {"method": "PUT_OBJECT", "service": "FOS"},
            {"method": "GET_OBJECT", "service": "FOS"},
            {"method": "LIST_OBJECTS_V2", "service": "FOS"},
        ],
    )
    classes = [
        r["operation_class"]
        for r in usage_log_db.get_con(sid).execute("SELECT operation_class FROM usage_log ORDER BY id").fetchall()
    ]
    assert classes == ["A", "B", "A"]


def test_log_usage_calls_classifies_cdn_separately(sid):
    metadata_db.log_usage_calls(sid, [{"method": "GET", "service": "CDN"}])
    row = usage_log_db.get_con(sid).execute("SELECT operation_class FROM usage_log").fetchone()
    assert row["operation_class"] == "CDN"


def test_log_usage_calls_noops_on_empty_list(sid):
    metadata_db.log_usage_calls(sid, [])
    assert usage_log_db.get_con(sid).execute("SELECT count(*) FROM usage_log").fetchone()[0] == 0


# ── get_usage_logs (paginated query + aggregates) ───────────────────────


def _seed_usage_log_row(
    sid: str,
    timestamp: str,
    operation_class: str = "A",
    operation_type: str = "PUT_OBJECT",
    process_context: str = "cron:sync",
    bytes_count: int = 1024,
) -> None:
    """Insert a usage_log row directly via SQL for testing query helpers."""
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


def test_get_usage_logs_filters_by_service_id_and_time_range(sid):
    for ts in ("2026-01-01T00:00:00Z", "2026-01-15T12:00:00Z", "2025-12-31T00:00:00Z", "2026-02-01T00:00:00Z"):
        _seed_usage_log_row(sid, ts)
    _seed_usage_log_row("other-svc", "2026-01-10T00:00:00Z")

    rows, total, _ = _query_jan_usage(sid)
    assert total == 2 and len(rows) == 2


def test_get_usage_logs_usage_type_filter_cdn(sid):
    for idx, cls in enumerate(("A", "B", "CDN")):
        _seed_usage_log_row(sid, f"2026-01-10T00:0{idx}:00Z", operation_class=cls)

    rows, total, _ = _query_jan_usage(sid, usage_type="CDN")
    assert total == 1 and rows[0]["operation_class"] == "CDN"


def test_get_usage_logs_usage_type_filter_fos_includes_both_a_and_b(sid):
    for idx, cls in enumerate(("A", "B", "CDN")):
        _seed_usage_log_row(sid, f"2026-01-10T00:0{idx}:00Z", operation_class=cls)

    rows, total, _ = _query_jan_usage(sid, usage_type="FOS")
    assert total == 2 and {r["operation_class"] for r in rows} == {"A", "B"}


def test_get_usage_logs_specific_fos_a_filter(sid):
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", operation_class="B")

    rows, total, _ = _query_jan_usage(sid, usage_type="FOS-A")
    assert total == 1 and rows[0]["operation_class"] == "A"


def test_get_usage_logs_process_context_filter_uses_ilike(sid):
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", process_context="cron:sync")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", process_context="api:dashboard")

    rows, total, _ = _query_jan_usage(sid, process_context="cron")
    assert total == 1 and "cron" in rows[0]["process_context"]


def test_get_usage_logs_paginates_with_page_and_page_size(sid):
    for i in range(5):
        _seed_usage_log_row(sid, f"2026-01-{10 + i:02d}T00:00:00Z")

    rows, total, _ = _query_jan_usage(sid, page=2, page_size=2)
    assert total == 5 and len(rows) == 2


def test_get_usage_logs_returns_class_aggregates(sid):
    for idx, (cls, b) in enumerate((("A", 100), ("A", 200), ("B", 500), ("CDN", 1000))):
        _seed_usage_log_row(sid, f"2026-01-10T00:0{idx}:00Z", operation_class=cls, bytes_count=b)

    _, _, agg = _query_jan_usage(sid)
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (2, 1, 1)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (1000, 800)


def test_get_usage_logs_orders_by_timestamp_desc(sid):
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_type="OLDEST")
    _seed_usage_log_row(sid, "2026-01-15T00:00:00Z", operation_type="NEWEST")

    rows, _, _ = _query_jan_usage(sid)
    assert [r["operation_type"] for r in rows] == ["NEWEST", "OLDEST"]


def test_get_usage_logs_empty_result_returns_zero_aggregates(sid):
    rows, total, agg = _query_jan_usage(sid)
    assert rows == [] and total == 0
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_bytes"]) == (0, 0, 0)


def test_log_usage_calls_doubles_cdn_egress_on_shield_miss(sid):
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


# ── usage_log coverage expansion (sub-86% gap fill) ─────────────────────────


def test_log_usage_calls_swallows_executemany_exception(sid, caplog):
    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        metadata_db.log_usage_calls(
            sid, [{"function_name": "ingest", "operation_type": "PUT_OBJECT", "url": "/x", "status": "OK"}]
        )
    assert any("Failed to log usage calls" in r.message for r in caplog.records)


def test_log_synthetic_usage_empty_calls_returns_zero(sid):
    assert metadata_db.log_synthetic_usage(sid, []) == 0


def test_log_synthetic_usage_returns_zero_when_no_paths(sid):
    assert metadata_db.log_synthetic_usage(sid, [{"method": "PUT"}, {"caller": "x"}]) == 0


def test_log_synthetic_usage_dedupes_against_existing_urls(sid):
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/b"}]) == 2
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}]) == 1

    urls = sorted(
        r["url"]
        for r in usage_log_db.get_con(sid)
        .execute("SELECT url FROM usage_log WHERE function_name = 'fastly.edge' ORDER BY url")
        .fetchall()
    )
    assert urls == ["/a", "/b", "/c"]


def test_log_synthetic_usage_skips_when_all_paths_already_present(sid):
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/dup1"}, {"path": "/dup2"}]) == 2
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/dup1"}, {"path": "/dup2"}]) == 0


def test_log_synthetic_usage_swallows_insert_exception(sid, caplog):
    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        out = metadata_db.log_synthetic_usage(sid, [{"path": "/x"}, {"path": "/y"}])
    assert out == 0
    assert any("Synthetic usage log failed" in r.message for r in caplog.records)


# ── reconcile_fastly_stats ──────────────────────────────────────────────────


def test_reconcile_fastly_stats_returns_zero_on_empty_records(sid):
    assert metadata_db.reconcile_fastly_stats(sid, []) == 0


def test_reconcile_fastly_stats_skips_records_without_hour_iso(sid):
    out = metadata_db.reconcile_fastly_stats(
        sid, [{"class_a": 10, "class_b": 5}, {"hour_iso": "not-a-date", "class_a": 1}]
    )
    assert out == 0


def test_reconcile_fastly_stats_writes_gap_rows_for_underreporting(sid):
    for i in range(5):
        _seed_usage_log_row(sid, f"2026-05-22T13:0{i}:00Z", operation_class="A")

    written = metadata_db.reconcile_fastly_stats(
        sid, [{"hour_iso": "2026-05-22T13:00:00Z", "class_a": 10, "class_b": 3}]
    )
    assert written == 2

    rows = (
        usage_log_db.get_con(sid)
        .execute(
            "SELECT operation_class, count FROM usage_log "
            "WHERE function_name = 'fastly.reconciliation' ORDER BY operation_class"
        )
        .fetchall()
    )
    assert {r["operation_class"]: r["count"] for r in rows} == {"A": 5, "B": 3}


def test_reconcile_fastly_stats_does_not_double_count_prior_reconciliation_rows(sid):
    for i in range(2):
        _seed_usage_log_row(sid, f"2026-05-22T14:0{i}:00Z", operation_class="A")

    record = [{"hour_iso": "2026-05-22T14:00:00Z", "class_a": 10, "class_b": 0}]
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1

    rows = (
        usage_log_db.get_con(sid)
        .execute("SELECT count FROM usage_log WHERE function_name = 'fastly.reconciliation' AND operation_class = 'A'")
        .fetchall()
    )
    assert len(rows) == 1 and rows[0]["count"] == 8


def test_reconcile_fastly_stats_writes_nothing_when_local_matches_fastly(sid):
    for i in range(7):
        _seed_usage_log_row(sid, f"2026-05-22T15:0{i}:00Z", operation_class="A")

    written = metadata_db.reconcile_fastly_stats(
        sid, [{"hour_iso": "2026-05-22T15:00:00Z", "class_a": 7, "class_b": 0}]
    )
    assert written == 0


# ── _query_usage_log_aggregate_rollup branches via get_usage_logs ───────────


def test_get_usage_logs_sub_hour_range_uses_direct_scan(sid):
    for ts in ("2026-05-25T10:05:00Z", "2026-05-25T10:15:00Z", "2026-05-25T11:05:00Z"):
        _seed_usage_log_row(sid, ts, operation_class="A")

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T10:00:00Z", "2026-05-25T10:30:00Z")
    assert total == 2 and agg["total_class_a"] == 2


def test_get_usage_logs_custom_usage_type_filters_by_class(sid):
    _seed_usage_log_row(sid, "2026-05-26T10:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-05-26T10:01:00Z", operation_class="CUSTOM")

    _entries, _total, agg = metadata_db.get_usage_logs(
        sid, "2026-05-26T00:00:00Z", "2026-05-26T23:59:59Z", usage_type="CUSTOM"
    )
    assert (agg["total_class_a"], agg["total_class_b"]) == (0, 0)


def test_get_usage_logs_operation_type_filter_uses_like(sid):
    for idx, op in enumerate(("PUT_OBJECT", "GET_OBJECT", "POST")):
        _seed_usage_log_row(sid, f"2026-05-27T10:0{idx}:00Z", operation_type=op)

    entries, total, _agg = metadata_db.get_usage_logs(
        sid, "2026-05-27T00:00:00Z", "2026-05-27T23:59:59Z", operation_type="PUT"
    )
    assert total == 1 and entries[0]["operation_type"] == "PUT_OBJECT"


def test_get_usage_logs_process_context_filter_uses_like(sid):
    for idx, ctx in enumerate(("cron:sync", "cron:commit", "request:handler")):
        _seed_usage_log_row(sid, f"2026-05-28T10:0{idx}:00Z", process_context=ctx)

    entries, total, _agg = metadata_db.get_usage_logs(
        sid, "2026-05-28T00:00:00Z", "2026-05-28T23:59:59Z", process_context="cron"
    )
    assert total == 2 and all("cron" in e["process_context"] for e in entries)


# ── Merged from tests/core/test_pg_connection.py ──

"""Postgres metadata backend: dialect shim + connection lifecycle.

No live Postgres needed — these pin the SQL rewriting and the
get/put-conn lifecycle against a fake pool/cursor. The one thing they
deliberately do NOT cover is whether the rewritten SQL actually executes
against a real Postgres server; that's the manual verification step in
the migration runbook.
"""


from unittest.mock import MagicMock, patch

import pytest
from backend.utils.date_utils import iso_z

from backend.core.metadata import pg_connection as pgc
from backend.core.metadata import reconciliation
from backend.core.metadata import usage_log_db as _usage_log_db

# ── _rewrite_sql / helpers ────────────────────────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_placeholder_replace_skips_string_literals():
    sql = "SELECT * FROM t WHERE a = ? AND b = 'literal ? mark' AND c = ?"
    out = pgc._replace_placeholders_outside_literals(sql)
    assert out == "SELECT * FROM t WHERE a = %s AND b = 'literal ? mark' AND c = %s"


@pytest.mark.usefixtures("_reset_pg_state")
def test_placeholder_replace_no_placeholders():
    sql = "SELECT 1"
    assert pgc._replace_placeholders_outside_literals(sql) == "SELECT 1"


@pytest.mark.usefixtures("_reset_pg_state")
@pytest.mark.parametrize(
    "table,cols_hint",
    [
        ("sources", None),
        ("rdns", None),
    ],
)
def test_insert_or_ignore_known_tables_become_on_conflict_do_nothing(table, cols_hint):
    sql = f"INSERT OR IGNORE INTO {table} (a, b) VALUES (?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert out == f"INSERT INTO {table} (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING"


@pytest.mark.usefixtures("_reset_pg_state")
def test_insert_or_replace_quarantined_files_becomes_on_conflict_update():
    """The conflict target must be the table's real UNIQUE — (service_id,
    file_name). This previously asserted ``ON CONFLICT(id)``, pinning a bug:
    ``insert_quarantined_file`` never supplies ``id`` (it is autoincrement),
    so the clause could never fire and a re-quarantine of the same file
    INSERTed a duplicate under Postgres while SQLite's OR REPLACE correctly
    replaced it. ``file_name`` is no longer in the update list because it is
    now part of the conflict key.
    """
    sql = "INSERT OR REPLACE INTO quarantined_files (service_id, file_name) VALUES (?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert out.startswith(
        "INSERT INTO quarantined_files (service_id, file_name) VALUES (?, ?) "
        "ON CONFLICT(service_id, file_name) DO UPDATE SET"
    )
    assert "source_name=EXCLUDED.source_name" in out
    assert "corrupt_rows=EXCLUDED.corrupt_rows" in out


@pytest.mark.usefixtures("_reset_pg_state")
def test_insert_or_replace_views_becomes_on_conflict_update():
    sql = "INSERT OR REPLACE INTO views (id, service_id, name) VALUES (?, ?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert "ON CONFLICT(id) DO UPDATE SET" in out
    assert "service_id=EXCLUDED.service_id" in out


@pytest.mark.usefixtures("_reset_pg_state")
def test_insert_or_unmapped_table_raises_loudly():
    """A new SQLite writer using INSERT OR * must be taught to this module —
    silently dropping the semantics would let a unique-violation crash (or
    worse, a silent duplicate) reach production instead of failing at
    review/test time."""
    with pytest.raises(NotImplementedError, match="brand_new_table"):
        pgc._rewrite_insert_or("INSERT OR IGNORE INTO brand_new_table (x) VALUES (?)")


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_passes_through_plain_insert():
    sql = "INSERT INTO cron_runs (service_id, task) VALUES (?, ?)"
    assert pgc._rewrite_sql(sql) == "INSERT INTO cron_runs (service_id, task) VALUES (%s, %s)"


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_datetime_now():
    assert "current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql("SELECT datetime('now')")


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_datetime_now_with_modifier():
    out = pgc._rewrite_sql("SELECT datetime('now', '-15 minutes')")
    assert "current_timestamp AT TIME ZONE 'UTC' + INTERVAL '-15 minutes'" in out


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_strftime_iso():
    out = pgc._rewrite_sql("SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
    assert "to_char(current_timestamp AT TIME ZONE 'UTC'" in out


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_instr_and_substr():
    out = pgc._rewrite_sql("SELECT instr(a, b), substr(a, 1, 2) FROM t")
    assert "strpos(a, b)" in out
    assert "substring(a, 1, 2)" in out


@pytest.mark.usefixtures("_reset_pg_state")
def test_rewrite_sql_excluded_lowercase_to_upper_only_with_on_conflict():
    # Only rewritten inside an actual ON CONFLICT clause (already-Postgres
    # SQL passed straight through must not get corrupted by a bare
    # substring match on unrelated text containing "excluded.").
    sql = "INSERT INTO t VALUES (?) ON CONFLICT(id) DO UPDATE SET a = excluded.a"
    assert "EXCLUDED.a" in pgc._rewrite_sql(sql)
    # No ON CONFLICT clause present — must stay untouched, not uppercased.
    assert pgc._rewrite_sql("SELECT 'excluded.something'") == "SELECT 'excluded.something'"


@pytest.mark.usefixtures("_reset_pg_state")
def test_maybe_add_returning_cron_runs():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s)")
    assert sql.endswith("RETURNING id")
    assert id_col == "id"


@pytest.mark.usefixtures("_reset_pg_state")
def test_maybe_add_returning_other_table_untouched():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO ingested_files (a) VALUES (%s)")
    assert sql == "INSERT INTO ingested_files (a) VALUES (%s)"
    assert id_col is None


@pytest.mark.usefixtures("_reset_pg_state")
def test_maybe_add_returning_idempotent_if_already_present():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s) RETURNING id")
    assert sql.count("RETURNING") == 1
    assert id_col is None  # already has it — caller's own RETURNING wins, no double-fetch


# ── PgConnectionWrapper / PgCursorWrapper against a fake psycopg conn ──────
# Direct wrappers own a fake pool: exception tracebacks can delay their GC
# until another test has patched the global pool resolver.


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
def test_wrapper_execute_rewrites_and_runs():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    wrapper.execute("SELECT * FROM t WHERE a = ?", (1,))
    sql, params = conn._cursor.executed[0]
    assert sql == "SELECT * FROM t WHERE a = %s"
    assert params == (1,)


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_execute_registers_with_live_query_monitor():
    """Postgres queries must appear in the same Live Query Monitor
    (query_registry) SQLite queries already feed — this was a documented
    blind spot: PgConnectionWrapper bypassed InstrumentedConnection
    entirely, so a slow Postgres metadata query was invisible to
    /admin/queries and never persisted to slow_queries."""
    from backend.core import query_registry

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
        wrapper.execute("SELECT 1")

    assert seen["db_type"] == "Postgres"
    assert seen["service_id"] == "svc-1"


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_execute_deregisters_on_error():
    from backend.core import query_registry

    class _RaisingCursor(_FakeCursor):
        def execute(self, sql, params=()):
            raise RuntimeError("boom")

    conn = _FakeConn()
    conn._cursor = _RaisingCursor()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())

    with patch.object(query_registry.query_registry, "deregister") as mock_deregister:
        with pytest.raises(RuntimeError, match="boom"):
            wrapper.execute("SELECT 1")

    assert mock_deregister.call_count == 1
    _qid, kwargs = mock_deregister.call_args
    assert isinstance(kwargs.get("error"), RuntimeError)


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_execute_cron_runs_returns_lastrowid():
    conn = _FakeConn(fetchone_result={"id": 42})
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    cur = wrapper.execute("INSERT INTO cron_runs (task) VALUES (?)", ("log_discovery",))
    assert cur.lastrowid == 42
    sql, _ = conn._cursor.executed[0]
    assert "RETURNING id" in sql


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_execute_non_cron_runs_no_lastrowid():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    cur = wrapper.execute("INSERT INTO ingested_files (a) VALUES (?)", (1,))
    assert cur.lastrowid is None


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_commit_rollback_are_noops_under_autocommit():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    wrapper.commit()  # must not raise
    wrapper.rollback()  # must not raise


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_close_returns_to_pool_only_when_flagged(monkeypatch):
    conn = _FakeConn()
    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    long_lived = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=False, pool=pool)
    long_lived.close()
    pool.putconn.assert_not_called()

    checked_out = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
    checked_out.close()
    pool.putconn.assert_called_once_with(conn)


# ── Thread-local long-lived connection lifecycle ───────────────────────────


@pytest.fixture()
def _reset_pg_state():
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()
    yield
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()


@pytest.mark.usefixtures("_reset_pg_state")
def test_get_pg_thread_connection_reuses_within_thread(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    first = pgc.get_pg_thread_connection()
    second = pgc.get_pg_thread_connection()
    assert first is second
    pool.getconn.assert_called_once()


@pytest.mark.usefixtures("_reset_pg_state")
def test_close_all_pg_connections_returns_and_clears(monkeypatch):
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.get_pg_thread_connection()
    assert wrapper is not None
    pgc.close_all_pg_connections()
    pool.putconn.assert_called_once_with(fake)

    # A subsequent get must check out a NEW connection, not reuse the
    # returned one — proves the thread-local cache was actually cleared.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2


@pytest.mark.usefixtures("_reset_pg_state")
def test_release_pg_thread_connection_returns_and_clears(monkeypatch):
    """The heartbeat-thread leak fix: a thread that will never be reused
    must be able to hand its connection back WITHOUT disturbing any other
    thread's connection (unlike close_all_pg_connections, which is
    test-teardown-only and drains every thread)."""
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()
    pgc.release_pg_thread_connection()
    pool.putconn.assert_called_once_with(fake)

    # A subsequent get_pg_thread_connection on the SAME thread must check
    # out a fresh connection, not reuse the one just returned.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2


@pytest.mark.usefixtures("_reset_pg_state")
def test_release_pg_thread_connection_noop_when_no_connection(monkeypatch):
    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)
    pgc.release_pg_thread_connection()  # must not raise
    pool.putconn.assert_not_called()


@pytest.mark.usefixtures("_reset_pg_state")
def test_release_pg_thread_connection_only_affects_calling_thread(monkeypatch):
    """Two threads each hold their own connection; releasing thread A's
    must not touch thread B's — the actual bug this fixes was a per-tick
    heartbeat thread returning ALL threads' connections (or none)."""
    import threading

    pool = MagicMock()
    conns = [_FakeConn(), _FakeConn()]
    pool.getconn.side_effect = conns
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()  # this test's own thread: conns[0]

    other_thread_wrapper = {}

    def other_thread():
        other_thread_wrapper["w"] = pgc.get_pg_thread_connection()

    t = threading.Thread(target=other_thread)
    t.start()
    t.join()

    # An unrelated wrapper can finalize while get_pg_pool is patched.
    unrelated = pgc.PgConnectionWrapper(_FakeConn())
    unrelated._finalizer()

    pgc.release_pg_thread_connection()  # only this thread's (conns[0])
    owned_returns = [call.args[0] for call in pool.putconn.call_args_list if call.args[0] in conns]
    assert owned_returns == [conns[0]]
    assert other_thread_wrapper["w"]._returned == [False]

    # This thread's own connection is gone; a re-fetch checks out fresh.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 3


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_finalizer_returns_connection_when_garbage_collected(monkeypatch):
    """The GC safety net: a wrapper that is dropped WITHOUT any explicit
    release/close must still return its connection to the pool. This is
    what actually protects against the leak in cases release_thread_
    connection() can't reach — a sync HTTP route handler running on an
    anyio threadpool worker that gets pruned (anyio kills idle workers
    after 10s on its own schedule; no request-scoped cleanup can
    reliably catch that, since a request's dependencies/endpoint aren't
    guaranteed to share one worker thread)."""
    import gc

    pool = MagicMock()
    conn = _FakeConn()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn, pool=pool)
    assert conn not in [c.args[0] for c in pool.putconn.call_args_list]

    del wrapper
    gc.collect()

    # A stray finalizer from an unrelated, already-torn-down test can also
    # fire during this gc.collect() (module-global _checked_out state is
    # shared across tests in this file) and call the SAME patched
    # get_pg_pool() with ITS OWN leftover connection — assert our specific
    # conn was returned exactly once, not that it's the pool's only call.
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1, f"expected exactly one putconn(conn) call, got {pool.putconn.call_args_list}"


@pytest.mark.usefixtures("_reset_pg_state")
def test_wrapper_finalizer_is_noop_after_explicit_close(monkeypatch):
    """An explicitly-closed (return_to_pool_on_close) wrapper must not
    ALSO return its connection a second time when later garbage
    collected — that would let two callers hold the same live connection
    concurrently."""
    import gc

    pool = MagicMock()
    conn = _FakeConn()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
    wrapper.close()
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1

    del wrapper
    gc.collect()

    # Still just the one call for OUR conn — the finalizer must not fire a
    # second putconn() for the same connection after an explicit close().
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1, f"expected exactly one putconn(conn) call, got {pool.putconn.call_args_list}"


@pytest.mark.usefixtures("_reset_pg_state")
def test_checked_out_drops_dead_threads_wrapper_without_explicit_release(monkeypatch):
    """Live incident: _checked_out was a plain list, so it held a permanent
    STRONG reference to every wrapper ever created — including threads that
    died naturally without ever calling release_pg_thread_connection() (the
    dominant case: anyio's HTTP threadpool prunes idle workers on its own
    schedule, and nothing in application code observes that). That
    permanent reference meant the wrapper's GC finalizer could never fire,
    no matter how thoroughly it was implemented — a real, measured case had
    27 wrappers in _checked_out while only 3 of their owning threads still
    existed. _checked_out must be a WeakSet so a dead thread's wrapper can
    actually be collected (and its connection returned) the moment nothing
    else references it, with no explicit release call required."""
    import gc
    import threading

    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    def dies_without_releasing():
        pgc.get_pg_thread_connection()  # never calls release_pg_thread_connection()

    t = threading.Thread(target=dies_without_releasing)
    t.start()
    t.join()

    # CPython's refcounting drops the wrapper the instant the thread's
    # threading.local() slot is cleared on thread exit — no reference
    # cycle is involved here, so this doesn't even need a gc.collect() to
    # observe; the call below is defensive insurance, not load-bearing.
    gc.collect()

    assert len(pgc._checked_out) == 0, (
        "a WeakSet must drop the dead thread's wrapper once nothing else references it — "
        "if this is 1, _checked_out is (again) holding a strong reference and the leak is back"
    )


@pytest.mark.usefixtures("_reset_pg_state")
def test_get_pg_readonly_connection_is_fresh_each_call(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    a = pgc.get_pg_readonly_connection()
    b = pgc.get_pg_readonly_connection()
    assert a is not b
    assert pool.getconn.call_count == 2


@pytest.mark.usefixtures("_reset_pg_state")
def test_get_pg_readonly_connection_close_returns_to_pool(monkeypatch):
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    con = pgc.get_pg_readonly_connection()
    con.close()
    pool.putconn.assert_called_once_with(fake)


@pytest.mark.usefixtures("_reset_pg_state")
def test_is_postgres_toggles_on_env(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    assert pgc.is_postgres() is False
    monkeypatch.setenv("METADATA_DSN", "postgresql://x/y")
    assert pgc.is_postgres() is True


@pytest.mark.usefixtures("_reset_pg_state")
def test_compat_row_supports_positional_and_named_access():
    """The seam exists so SQLite-shaped metadata SQL runs unchanged against
    Postgres. psycopg's plain ``dict_row`` broke half of that: ~10 modules
    do ``.fetchone()[0]`` / ``row[0]``, and every one raised ``KeyError: 0``
    under Postgres while passing on SQLite (where the suite runs). It
    surfaced live as the log_ingest cron failing every tick from
    ``cron/jobs/commit.py``'s ``.fetchone()[0]``, pinning the service to
    "degraded". ``sqlite3.Row`` allows both styles; so must this.
    """
    row = pgc._CompatRow(["count", "name"], [7, "svc-a"])

    # Positional — the access style that was broken.
    assert row[0] == 7
    assert row[1] == "svc-a"
    assert row[0:2] == (7, "svc-a")

    # Named — must keep working.
    assert row["count"] == 7
    assert row["name"] == "svc-a"
    assert row.get("count") == 7
    assert row.get("absent", "dflt") == "dflt"

    # Still a real mapping for callers that treat it as one.
    assert dict(row) == {"count": 7, "name": "svc-a"}
    assert list(row.keys()) == ["count", "name"]

    with pytest.raises(KeyError):
        row["nope"]
    with pytest.raises(IndexError):
        row[9]


@pytest.mark.usefixtures("_reset_pg_state")
def test_compat_row_factory_handles_no_description():
    """A statement with no result set (INSERT/DDL) has ``description is
    None``; the factory must not blow up building keys from it."""
    from unittest.mock import MagicMock

    cur = MagicMock()
    cur.description = None
    make = pgc._compat_row_factory(cur)
    assert make((1, 2)) == (1, 2)

    cur.description = [MagicMock(name="d1"), MagicMock(name="d2")]
    cur.description[0].name = "a"
    cur.description[1].name = "b"
    made = pgc._compat_row_factory(cur)((10, 20))
    assert made[0] == 10 and made["b"] == 20


# ── Metadata reconciliation (storage stats + cleanup) ──────────────────────


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Insert ``rows`` usage_log entries dated ``days_ago`` in the past.

    ``usage_log`` lives in its own per-service database; seed
    via :func:`backend.core.metadata.usage_log_db.get_con` so the row
    counts/cleanup paths see them.
    """
    con = _usage_log_db.get_con(service_id)
    now_dt = datetime.now(UTC) - timedelta(days=days_ago)
    now_ts = iso_z(now_dt)
    hour = now_ts[:13]
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
        (service_id, hour, rows, now_ts),
    )
    con.commit()


def _seed_ingested_file(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, 'fos', datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{days_ago}d-{i}.gz",) for i in range(rows)],
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


# ── get_metadata_storage_stats ────────────────────────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_returns_zero_rows_on_fresh_db():
    sid = "svc-stats-fresh"
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert "tables" in stats
    # All expected tables are present in the schema (initialised by get_con).
    for table in ("usage_log", "ingested_files", "cron_runs"):
        assert table in stats["tables"]
        assert stats["tables"][table]["rows"] == 0
    assert stats["db_bytes"] is None
    assert stats["db_path"] is None


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_counts_seeded_rows():
    sid = "svc-stats-seeded"
    _seed_usage_log(sid, 7)
    _seed_ingested_file(sid, 3)
    _seed_cron_run(sid, 2)
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert stats["tables"]["usage_log"]["rows"] == 7
    assert stats["tables"]["ingested_files"]["rows"] == 3
    assert stats["tables"]["cron_runs"]["rows"] == 2


@pytest.mark.usefixtures("_reset_pg_state")
def test_stats_tables_sql_names_all_exist_in_schema():
    """Regression guard for the silent-drop bug.

    Every ``sql_table`` in ``_STATS_TABLES`` must be a real table in the
    metadata schema. Four entries previously named non-existent tables
    (``saved_views``/``audit_log``/``in_flight_buffers``/
    ``locally_compacted_files``); ``get_metadata_storage_stats`` swallowed
    the resulting ``OperationalError`` and silently dropped those four rows
    from the admin storage panel. Pin every name against ``sqlite_master``
    so a future rename can't regress the same way. ``usage_log`` lives in a
    separate per-service file and is exempt.
    """
    sid = "svc-stats-schema"
    con = _con(sid)  # initialises base schema + pending migrations
    existing = {
        r[0] for r in con.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    }
    for sql_table, _out_key in reconciliation._STATS_TABLES:
        if sql_table == "usage_log":
            continue
        assert sql_table in existing, f"_STATS_TABLES names missing table: {sql_table}"


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_reports_reference_tables_under_ui_keys():
    """The reference tables surface under their UI-facing keys. Each table
    exists on a fresh DB, so each key must be present with rows >= 0 — not absent."""
    sid = "svc-stats-refs"
    stats = reconciliation.get_metadata_storage_stats(sid)
    for ui_key in ("saved_views", "audit_log", "in_flight_buffers"):
        assert ui_key in stats["tables"], f"reference table dropped from stats: {ui_key}"
        assert stats["tables"][ui_key]["rows"] == 0


# ── is_ingested_files_dedup_active ───────────────────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_dedup_active_default_when_no_config():
    # Service has no config file → defaults to "safe to trim" (True).
    assert reconciliation.is_ingested_files_dedup_active("svc-no-cfg") is True


@pytest.mark.usefixtures("_reset_pg_state")
def test_dedup_active_when_delete_after_true(monkeypatch):
    sid = "svc-delete-after-true"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": True}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    assert reconciliation.is_ingested_files_dedup_active(sid) is True


@pytest.mark.usefixtures("_reset_pg_state")
def test_dedup_active_returns_false_when_delete_after_false(monkeypatch):
    sid = "svc-delete-after-false"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": False}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    # Returns False — i.e. ``ingested_files`` is the dedup gate, must not
    # be trimmed.
    assert reconciliation.is_ingested_files_dedup_active(sid) is False


@pytest.mark.usefixtures("_reset_pg_state")
def test_dedup_active_returns_false_for_high_scale_shared_source_even_with_delete_after_true(monkeypatch):
    """A high-scale shared source forces raw deletion off regardless of
    ``delete_after`` — since raw files persist, ``ingested_files`` remains
    the only thing stopping full_sync from re-ingesting everything."""
    sid = "svc-high-scale-shared"

    def _fake_load_config(s: str):
        return {
            "provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}},
        }

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    assert reconciliation.is_ingested_files_dedup_active(sid) is False


# ── cleanup_metadata ─────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_deletes_aged_usage_log_rows():
    sid = "svc-cleanup-aged"
    _seed_usage_log(sid, 5, days_ago=10)
    _seed_usage_log(sid, 3, days_ago=0)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})

    assert result["deleted"]["usage_log"] == 5
    assert result["after"]["usage_log"] == 3
    # Vacuum is a SQLite-file concept skipped entirely under Postgres (see
    # test_cleanup_skips_vacuum_under_postgres) — Postgres reclaims space via
    # its own autovacuum daemon, outside application control.
    assert result["vacuumed"] is False
    assert result["duration_s"] >= 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_zero_retention_disables_table():
    sid = "svc-cleanup-zero"
    _seed_usage_log(sid, 5, days_ago=10)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 0})

    # Retention=0 → skip deletion for this table, nothing trimmed.
    assert result["deleted"]["usage_log"] == 0
    assert result["after"]["usage_log"] == 5
    assert result["vacuumed"] is False  # No deletes → no VACUUM


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_uses_default_retention_when_key_missing():
    sid = "svc-cleanup-default"
    # No retention passed → defaults apply. DEFAULT_METADATA_RETENTION
    # picks safe positive values, so rows older than that get trimmed.
    _seed_cron_run(sid, 4, days_ago=400)  # well past any reasonable default
    result = reconciliation.cleanup_metadata(sid)
    assert result["deleted"]["cron_runs"] == 4


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_emits_progress_events():
    sid = "svc-cleanup-events"
    _seed_usage_log(sid, 2, days_ago=10)
    events: list[dict] = []

    def _on_event(e: dict) -> None:
        events.append(e)

    reconciliation.cleanup_metadata(
        sid,
        retention={"usage_log_days": 1, "ingested_files_days": 1, "cron_runs_days": 1},
        on_event=_on_event,
    )

    # At least one status event AND at least one progress event fired.
    assert any(e["type"] == "status" for e in events)
    assert any(e["type"] == "progress" for e in events)
    # Final progress event hits the total step count.
    last_progress = [e for e in events if e["type"] == "progress"][-1]
    assert last_progress["current"] == last_progress["total"]


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_force_disables_ingested_files_when_delete_after_false(monkeypatch):
    sid = "svc-cleanup-forced-off"
    _seed_ingested_file(sid, 5, days_ago=400)

    from backend import config as svcconfig

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda s: {"provisioning": {"cron_sync": {"delete_after": False}}},
    )

    events: list[dict] = []
    result = reconciliation.cleanup_metadata(
        sid,
        retention={"ingested_files_days": 30},  # Caller wants trim
        on_event=events.append,
    )

    # Forced override: nothing deleted from ingested_files.
    assert result["deleted"]["ingested_files"] == 0
    assert result["after"]["ingested_files"] == 5
    # The override surfaces as a status event so the operator sees why.
    override_msgs = [e for e in events if e["type"] == "status" and "dedup gate" in e.get("message", "")]
    assert override_msgs, "expected status event explaining the override"


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_trims_rum_ingested_files_past_raw_retention_window():
    """RUM raw objects are deleted inline as they go (gap 2), and any age-based
    backstop deletes within log_retention_days (default 90d). Metadata cleanup
    trims RUM ingested_files older than that window so ingested_files does not
    grow without limit."""
    sid = "svc-cleanup-rum-dedup"
    _seed_ingested_file(sid, 3, days_ago=400)
    con = _con(sid)
    # Seed 4 RUM files 400 days ago (past the 91-day raw retention window)
    # and 2 RUM files 10 days ago (within the 91-day window)
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

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})

    # 3 request files (400d old, cutoff 1d) + 4 RUM files (400d old, cutoff 91d) = 7 deleted
    assert result["deleted"]["ingested_files"] == 7
    # 2 recent RUM files (10d old, within 91d window) survive
    assert result["after"]["ingested_files"] == 2
    remaining = [
        r["file_name"]
        for r in _con(sid).execute("SELECT file_name FROM ingested_files WHERE source_name = 'fos'").fetchall()
    ]
    assert all("recent-" in fn for fn in remaining)


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_rum_trimming_causes_no_reingest(monkeypatch):
    """Gap 6 & Gap 2: Proving that trimming aged RUM ingested_files rows does not
    cause re-ingest because raw objects were deleted inline and discovery is incremental."""
    from unittest.mock import MagicMock

    from backend.core.rum_ingest import ingest_rum_logs

    sid = "svc-cleanup-no-reingest"
    con = _con(sid)
    # 1. File was previously ingested 400 days ago
    old_file_key = "s3://test-bucket/raw/rum/year=2025/month=08/day=01/hour=12/minute=00/old.json.gz"
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'test_service', 'client_vitals', datetime('now', '-400 days'), 1, 100)",
        (old_file_key,),
    )
    con.commit()

    # 2. Cleanup runs and trims the 400d old record
    reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})
    assert con.execute("SELECT COUNT(*) FROM ingested_files WHERE file_name = ?", (old_file_key,)).fetchone()[0] == 0

    # 3. Next rum_sync runs against FOS:
    # Per the Gap 2 contract, old.json.gz was deleted upon ingest, so FOS has no such file.
    # Furthermore, incremental minute-prefix listing only scans recent minutes.
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = mock_paginator

    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr(
        "backend.core.rum_ingest.get_source_for_service",
        lambda s: {"name": sid, "bucket": "test-bucket", "service_id": sid},
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda s: con)
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda s, t: 1)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", lambda *a, **kw: None)

    events = list(ingest_rum_logs(sid))
    # Must report 0 rows ingested — NO re-ingest!
    assert events == [("started", 1), ("done", 0)]
    assert con.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0] == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_on_event_callback_failure_does_not_abort():
    sid = "svc-cleanup-bad-callback"
    _seed_usage_log(sid, 3, days_ago=10)

    def _bad_callback(e: dict) -> None:
        raise RuntimeError("callback fail")

    # Must not raise — the implementation swallows callback errors so a
    # buggy SSE consumer can't break the cleanup itself.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1}, on_event=_bad_callback)
    assert result["deleted"]["usage_log"] == 3


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_negative_retention_is_treated_as_disabled():
    sid = "svc-cleanup-neg"
    _seed_usage_log(sid, 5, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": -5})
    # Negative → skipped (same as 0).
    assert result["deleted"]["usage_log"] == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_non_int_retention_falls_back_to_disabled():
    sid = "svc-cleanup-bad-type"
    _seed_usage_log(sid, 3, days_ago=400)
    # Garbage value → coerce-fails → 0 days → skip.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": "not-a-number"})
    assert result["deleted"]["usage_log"] == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_postgres_retention_casts_text_timestamps():
    assert reconciliation._retention_timestamp_expr("ingested_at") == "CAST(ingested_at AS TIMESTAMPTZ)"
    assert reconciliation._retention_timestamp_expr("started_at") == "CAST(started_at AS TIMESTAMPTZ)"


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_rollups_skipped_when_rollups_days_zero():
    sid = "svc-cleanup-no-rollups"
    _seed_usage_log(sid, 1, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 0})
    assert result["rollups_deleted"] == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_skips_vacuum_under_postgres():
    """auto_vacuum/incremental_vacuum/freelist_count are SQLite-file
    concepts with no Postgres equivalent (Postgres reclaims space via its
    own autovacuum daemon). Under the (now sole) Postgres metadata backend
    the DELETE trim must still run, but the vacuum branch must be skipped
    rather than executing PRAGMA statements a Postgres connection can't run.
    """
    sid = "svc-cleanup-pg-skip"
    _seed_usage_log(sid, 5, days_ago=10)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})

    assert result["deleted"]["usage_log"] == 5
    assert result["vacuumed"] is False


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_rollups_skipped_when_source_missing(monkeypatch):
    sid = "svc-cleanup-no-src"
    _seed_usage_log(sid, 1, days_ago=400)

    from backend.core import duckdb as _db

    monkeypatch.setattr(_db, "get_source_for_service", lambda s: None)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 7})
    assert result["rollups_deleted"] == 0


# ── ingested_files_summary drift fix ─────────────────────────────────────────


def _seed_ingested_file_for(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Like ``_seed_ingested_file`` but stamps ``source_name=service_id``."""
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{service_id}-{days_ago}d-{i}.gz", service_id) for i in range(rows)],
    )
    con.commit()


def _poison_summary(service_id: str, *, file_count: int, total_rows: int, total_bytes: int) -> None:
    """Force ``ingested_files_summary`` to a value that does NOT match the
    table — simulates the cumulative-ever drift the retention DELETE used to
    cause (it never decremented the incrementally-maintained rollup)."""
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
def test_cleanup_recomputes_ingested_files_summary_after_trim():
    """The retention DELETE on ingested_files must leave the summary rollup
    consistent with the surviving rows, not the pre-trim (drifted) totals."""
    sid = "svc-rollup-recompute"
    _seed_ingested_file_for(sid, 5, days_ago=10)  # aged out → trimmed
    _seed_ingested_file_for(sid, 3, days_ago=0)  # recent → survive
    # Drifted rollup far larger than the table (the bug we're fixing).
    _poison_summary(sid, file_count=999_999, total_rows=999_999, total_bytes=99_999_900)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})

    assert result["deleted"]["ingested_files"] == 5
    assert result["after"]["ingested_files"] == 3

    # Rollup recomputed to the surviving rows (row_count=1, file_size=100 each).
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 3
    assert summary["total_rows"] == 3
    assert summary["total_bytes"] == 300


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_skips_summary_recompute_when_nothing_trimmed():
    """The recompute is gated on an actual deletion: when no ingested_files
    rows are trimmed (all recent / delete_after=false), the rollup is left
    untouched so delete_after=false services don't pay a daily full rescan."""
    sid = "svc-rollup-noop"
    _seed_ingested_file_for(sid, 3, days_ago=0)  # all recent → none trimmed at 7d
    _poison_summary(sid, file_count=42, total_rows=4242, total_bytes=4200)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert result["deleted"]["ingested_files"] == 0

    # Untouched — no deletion means no recompute.
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 42
    assert summary["total_rows"] == 4242


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_report_postgres_relation_sizes_for_every_table():
    sid = "svc-stats-bytes"
    _seed_usage_log(sid, 2)
    _seed_cron_run(sid, 1)
    stats = reconciliation.get_metadata_storage_stats(sid, force=True)
    for table in ("usage_log", "cron_runs", "ingested_files"):
        size = stats["tables"][table]["bytes"]
        assert isinstance(size, int) and size >= 0, f"{table} bytes should be a pg_total_relation_size int"
    assert stats["tables"]["cron_runs"]["bytes"] > 0
    assert stats["db_bytes"] is None
