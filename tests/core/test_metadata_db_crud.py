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
