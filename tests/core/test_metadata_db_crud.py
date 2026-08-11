"""CRUD-layer tests for ``backend.core.metadata``.

The schema migration tests live in ``test_metadata_db_schema.py``; the
concurrency tests in ``test_metadata_db_concurrency.py``; the orphan
reaping in ``test_metadata_db_reap.py``. This file covers the
domain-table read/write helpers that don't fit elsewhere: views,
audit logs, ingested files, cron run lifecycle, asn name cache,
sources registry, and usage_log telemetry.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from backend.core import metadata as metadata_db
from backend.core.metadata import usage_log_db


@pytest.fixture
def sid() -> str:
    return "test-svc-md"


# ── replace_views_for_service ───────────────────────────────────────────────


def _seed_view_directly(sid: str, view_id: str = "old", name: str = "Old View") -> None:
    """Insert a view via raw SQL — bypasses ``save_view``'s Pydantic-model
    signature so the tests don't have to construct a real model just to
    seed state."""
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, page) VALUES (?, ?, ?, ?, ?)",
        (view_id, sid, name, "{}", "dashboard"),
    )
    con.commit()


def test_replace_views_for_service_clears_existing_and_inserts_new(sid):
    """state_sync.import_admin_state calls this to overwrite the
    analyst pod's saved-views with the admin's. Pinned because
    losing the DELETE step would leave stale views forever."""
    _seed_view_directly(sid, view_id="old", name="Old View")

    metadata_db.replace_views_for_service(
        sid,
        [
            {
                "id": "new1",
                "service_id": sid,
                "name": "New A",
                "filters_json": "{}",
                "time_range_type": "rolling",
                "start_time": None,
                "end_time": None,
                "page": "dashboard",
                "created_at": "2026-01-01T00:00:00Z",
            },
        ],
    )

    views = metadata_db.list_views(sid)
    names = {v["name"] for v in views}
    assert "Old View" not in names  # replaced
    assert "New A" in names


def test_replace_views_for_service_with_empty_list_clears_all(sid):
    """Empty list → DELETE only, no INSERT. Pinned because the
    DELETE-then-loop pattern can silently no-op the DELETE if the
    list is mishandled."""
    _seed_view_directly(sid, view_id="v1", name="x")
    metadata_db.replace_views_for_service(sid, [])
    assert metadata_db.list_views(sid) == []


# ── audit_logs ──────────────────────────────────────────────────────────────


def test_record_audit_persists_event_with_details_json(sid):
    """``details`` is stored as JSON string (the reader does NOT decode
    it for ``list_audit``; only ``list_audit_paginated`` does).
    Pinned because conflating these two readers would crash the
    audit panel."""
    metadata_db.record_audit(sid, "alert_created", {"alert_id": "a1", "name": "5xx"}, actor="dmichael")
    entries = metadata_db.list_audit(sid)

    assert len(entries) == 1
    assert entries[0]["event_type"] == "alert_created"
    assert entries[0]["actor"] == "dmichael"
    # list_audit returns raw JSON string; round-trip via json.loads
    assert json.loads(entries[0]["details"]) == {"alert_id": "a1", "name": "5xx"}


def test_list_audit_filters_by_since_timestamp(sid):
    """``since`` constrains the result to entries newer than the
    given timestamp. Pinned because the admin audit-log paging keys
    on this for "load more"."""
    metadata_db.record_audit(sid, "old_event", {}, actor="x")
    # Audit timestamps default to ``datetime('now')`` (server time).
    # Use a far-future ``since`` to verify the filter actually
    # filters; a recent ``since`` would still match.
    future = "2099-01-01T00:00:00Z"
    entries = metadata_db.list_audit(sid, since=future)
    assert entries == []

    # Past since → returns the event
    entries = metadata_db.list_audit(sid, since="2000-01-01T00:00:00Z")
    assert len(entries) == 1


def test_list_audit_respects_limit(sid):
    for i in range(5):
        metadata_db.record_audit(sid, f"event_{i}", {}, actor="x")
    entries = metadata_db.list_audit(sid, limit=2)
    assert len(entries) == 2


def test_export_audit_matches_list_audit_shape(sid):
    """state_sync.export uses this — pinned because shape-drift
    between list/export would silently break the cross-pod sync."""
    metadata_db.record_audit(sid, "evt", {"k": "v"}, actor="a")
    list_out = metadata_db.list_audit(sid)
    export_out = metadata_db.export_audit(sid)
    assert list_out == export_out


def test_replace_audit_for_service_clears_and_inserts(sid):
    metadata_db.record_audit(sid, "evt_old", {}, actor="x")

    metadata_db.replace_audit_for_service(
        sid,
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source_name": sid,
                "event_type": "evt_new",
                "details": json.dumps({"k": "v"}),
                "actor": "admin",
            }
        ],
    )

    entries = metadata_db.list_audit(sid)
    types = {e["event_type"] for e in entries}
    assert types == {"evt_new"}


# ── ingested_files ──────────────────────────────────────────────────────────


def test_insert_and_get_ingested_filenames_round_trip(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000), ("b.gz", 200, 8000)])
    names = metadata_db.get_ingested_filenames(sid)
    assert names == {"a.gz", "b.gz"}


def test_get_reclaimable_strand_filenames_requires_durable_and_post_epoch(sid):
    """The reconcile may only delete a strand it can POSITIVELY prove safe: durable
    data (row_count>0) AND ingested at/after the durability epoch. Markers
    (row_count==0) and pre-epoch rows (untrustworthy row_count) are excluded."""
    metadata_db.insert_ingested_files(
        sid,
        [("durable.gz", 100, 5000), ("marker.gz", 0, 1200), ("legacy.gz", 100, 8000)],
    )
    # Backdate legacy.gz so it falls BEFORE the epoch; the others keep datetime('now').
    con = metadata_db.get_con(sid)
    con.execute(
        "UPDATE ingested_files SET ingested_at = ? WHERE file_name = ? AND source_name = ?",
        ("2020-01-01 00:00:00", "legacy.gz", sid),
    )
    con.commit()

    cands = {"durable.gz", "marker.gz", "legacy.gz"}
    # A far-past epoch admits durable + legacy (both row_count>0), never the marker.
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2000-01-01 00:00:00") == {
        "durable.gz",
        "legacy.gz",
    }
    # An epoch between legacy (2020) and "now" excludes the pre-epoch legacy row AND
    # the zero-row marker, leaving only the durable, post-epoch row.
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2025-01-01 00:00:00") == {"durable.gz"}


def test_get_reclaimable_strand_filenames_restricts_to_candidates(sid):
    """The ``candidates`` filter keeps the query cheap — only the asked-for
    file_names are classified, and an empty candidate set short-circuits."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 9, 100), ("b.gz", 9, 100), ("marker.gz", 0, 100)])
    past = "2000-01-01 00:00:00"
    assert metadata_db.get_reclaimable_strand_filenames(sid, {"a.gz", "marker.gz"}, past) == {"a.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, set(), past) == set()


def test_insert_ingested_files_upserts_on_conflict(sid):
    """Re-inserting an existing (file_name, source_name) pair
    UPDATEs the row_count + file_size_bytes — pinned because the
    cron may re-process a file with updated counts."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000)])
    metadata_db.insert_ingested_files(sid, [("a.gz", 999, 99999)])

    files = metadata_db.list_ingested_files(sid)
    assert len(files) == 1  # not duplicated
    assert files[0]["row_count"] == 999
    assert files[0]["file_size_bytes"] == 99999


def test_list_ingested_files_returns_newest_first(sid):
    """Sort order is ``ORDER BY ingested_at DESC``. SQLite's
    ``datetime('now')`` has 1-second resolution, so we set the
    timestamps explicitly to verify the ORDER BY without races."""
    con = metadata_db.get_con(sid)
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("first.gz", sid, 1, 100, "2026-01-01T00:00:00Z"),
            ("second.gz", sid, 2, 200, "2026-01-02T00:00:00Z"),
        ],
    )
    con.commit()

    files = metadata_db.list_ingested_files(sid)
    # Newest first → second.gz comes before first.gz
    assert files[0]["file_name"] == "second.gz"
    assert files[1]["file_name"] == "first.gz"


def test_list_ingested_files_for_status_returns_tuples(sid):
    """The status-cache variant returns tuples (no dict overhead)
    because the cron is hot. Pinned because the type is part of
    the contract — callers do tuple unpacking."""
    metadata_db.insert_ingested_files(sid, [("x.gz", 5, 500)])
    rows = metadata_db.list_ingested_files_for_status(sid)
    assert len(rows) == 1
    fn, _ingested_at, rc, sz = rows[0]
    assert fn == "x.gz"
    assert rc == 5
    assert sz == 500


def test_get_node_count_avg_groups_by_basename_timestamp(sid):
    """avg(files-per-flush) groups on the 19-char timestamp prefix of the
    basename. Pinned because the SQL push-down derives the group key via
    `substr(file_name, instr(file_name, 'T') - 10, 19)` and any drift in the
    file_name format (or a stray uppercase T before the timestamp) would
    silently change the average."""
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-aaa.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-bbb.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:30.000-ccc.log.gz", 1, 1),
        ],
    )
    # two groups: 23:30:00 (count=2) and 23:30:30 (count=1) → avg = 1.5
    assert metadata_db.get_node_count_avg(sid) == 1.5


def test_get_node_count_avg_returns_none_when_empty(sid):
    assert metadata_db.get_node_count_avg(sid) is None


def test_get_node_count_avg_combines_canonical_and_legacy_basenames(sid):
    """Fast/slow split must produce the SAME average as the pre-split
    single-arm query — fast arm aggregates canonical-basename rows
    (file_date IS NOT NULL, walked via idx_ingested_files_source_date),
    slow arm aggregates legacy/test rows (file_date IS NULL).

    Pinned because a refactor that drops the slow arm would silently
    omit test fixtures + ad-hoc backfills from the average; a refactor
    that drops the fast arm would re-introduce the full-table scan."""
    # Canonical-basename rows (insert_ingested_files runs _parse_file_date
    # which populates file_date for these). Two distinct emission
    # buckets: 23:30:00 has 2 files, 23:31:00 has 4 files. Mean-of-counts
    # for the canonical group alone would be 3.0.
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-a.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-c.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-d.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-e.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-f.log.gz", 1, 1),
        ],
    )
    # Legacy / test fixture rows: insert with file_date=NULL directly so
    # they take the slow arm. Bucket 23:32:00 with 1 file, 23:33:00 with
    # 1 file. Mean-of-counts for these alone would be 1.0.
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES (?, ?, ?, ?, NULL)",
        ("s3://b/raw/2026-05-15/23/2026-05-15T23:32:00.000-legacy-x.log.gz", sid, 1, 1),
    )
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES (?, ?, ?, ?, NULL)",
        ("s3://b/raw/2026-05-15/23/2026-05-15T23:33:00.000-legacy-y.log.gz", sid, 1, 1),
    )
    con.commit()

    # All four buckets contribute: (2, 4, 1, 1) → avg = 2.0. If the slow
    # arm were dropped, average would be (2 + 4) / 2 = 3.0. If the fast
    # arm were dropped, it'd be (1 + 1) / 2 = 1.0.
    assert metadata_db.get_node_count_avg(sid) == 2.0


def test_get_node_count_avg_slow_arm_skips_non_canonical_basenames(sid):
    """The slow arm gates on ``instr(file_name, 'T') >= 11`` so junk
    rows (no parseable T-timestamp) can't crash the substr and don't
    contribute a NULL group key. Pinned because dropping the instr()
    guard on the slow arm would let basenames without a T silently
    produce GROUP BY NULL rows — averaged in as their own bucket of 0."""
    con = metadata_db.get_con(sid)
    # Rows where instr(file_name, 'T') < 11 — must NOT contribute.
    # 'short-T.gz' has T at pos 7; 'lowercase-only.gz' has no uppercase
    # T at all (instr returns 0). Both fail the `instr(...) >= 11` guard.
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES (?, ?, ?, ?, NULL)",
        ("short-T.gz", sid, 1, 1),
    )
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES (?, ?, ?, ?, NULL)",
        ("lowercase-only.gz", sid, 1, 1),
    )
    con.commit()

    # No canonical rows + only junk slow-arm rows → no contributing
    # group keys → avg is None (not 0 or some pathological value).
    assert metadata_db.get_node_count_avg(sid) is None


def test_get_log_accounting_counts_groups_by_filename_when_iso_prefix_present(sid):
    """ISO-prefixed basenames bucket by emission time pulled from the path;
    rows + file counts aggregate per bucket. Pinned because the SQL CASE
    branch that slices the bucket key out of file_name is what aligns our
    series with Fastly Stats — if it drifts, every log-accounting render
    shows phantom gaps."""
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        ("s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz", sid, "2026-05-15T23:00:05", 100, 1),
    )
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", sid, "2026-05-15T23:30:05", 250, 1),
    )
    con.commit()
    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (350, 2)}


def test_get_log_accounting_counts_uses_file_date_fast_arm_when_populated(sid):
    """When file_date is populated (i.e. ingested via insert_ingested_files,
    which auto-parses the basename), the fast UNION arm groups by the substr
    of file_name AND filters by file_date >= start_date AND file_date <=
    end_date — which uses the (source_name, file_date) composite index
    instead of the unindexed datetime(ingested_at) scan. Result must equal
    the slow-arm baseline (verified by test_groups_by_filename above) so
    callers don't see a semantic shift after the split."""
    metadata_db.insert_ingested_files(
        sid,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz", 100, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 250, 1),
        ],
    )
    # Sanity: insert_ingested_files must populate file_date on the new rows
    # — without it the fast arm would skip and the slow arm would shoulder
    # the work, defeating the point of the rewrite.
    con = metadata_db.get_con(sid)
    fd_rows = con.execute(
        "SELECT file_name, file_date FROM ingested_files WHERE source_name = ?",
        (sid,),
    ).fetchall()
    assert all(r["file_date"] == "2026-05-15" for r in fd_rows), (
        f"insert_ingested_files should populate file_date; got {[dict(r) for r in fd_rows]}"
    )

    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (350, 2)}


def test_get_log_accounting_counts_falls_back_to_ingested_at_for_non_iso_filenames(sid):
    """When the basename has no ISO prefix (legacy/test files), the bucket
    falls back to ingested_at — matches the pre-pushdown Python branch."""
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        ("raw/legacy.gz", sid, "2026-05-15T23:00:00", 42, 1),
    )
    con.commit()
    counts = metadata_db.get_log_accounting_counts(
        sid, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    )
    assert counts == {"2026-05-15T23": (42, 1)}


# ── ingested_filenames dedup cache ──────────────────────────────────────────


def test_dedup_cache_populates_on_first_bounded_read(sid):
    """First bounded call SHOULD hit SQLite, second call SHOULD return the
    cached set without touching the DB. Pinned because this is the per-tick
    sync hot path — a regression silently re-introduces the ~640 ms fetchall
    on busy services."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100), ("b.gz", 2, 200)])
    metadata_db._clear_ingested_filenames_cache(sid)

    # First read populates the cache from SQLite.
    first = metadata_db.get_ingested_filenames(sid, limit=200)
    assert first == {"a.gz", "b.gz"}

    # Surgical state corruption: delete the rows directly and assert the
    # second call returns the cached set, NOT the empty DB.
    con = metadata_db.get_con(sid)
    con.execute("DELETE FROM ingested_files WHERE source_name = ?", (sid,))
    con.commit()
    second = metadata_db.get_ingested_filenames(sid, limit=200)
    assert second == {"a.gz", "b.gz"}


def test_insert_ingested_files_extends_dedup_cache(sid):
    """After the cache is warm, ``insert_ingested_files`` MUST add the new
    filenames so the next dedup check sees them without another SQL fetch.
    Otherwise the cache would silently let the cron re-process a just-ingested
    file until process restart."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)  # warm the cache

    metadata_db.insert_ingested_files(sid, [("b.gz", 2, 200)])
    # Second bounded read should see "b.gz" without hitting SQLite — but
    # asserting the set contents is sufficient since the DB DOES contain
    # both rows now; the contract is "cache stays consistent".
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}


def test_insert_skipping_cache_when_unpopulated(sid):
    """``insert_ingested_files`` MUST NOT prematurely seed an empty cache —
    otherwise the first bounded read on a process that hasn't cached anything
    yet would see only the latest batch (a few rows) instead of the full
    most-recent-N set. The DB is the source of truth on cache miss."""
    metadata_db.insert_ingested_files(sid, [("legacy-1.gz", 1, 100), ("legacy-2.gz", 1, 100)])
    metadata_db._clear_ingested_filenames_cache(sid)

    # Now insert another file BEFORE any bounded read populates the cache.
    metadata_db.insert_ingested_files(sid, [("new.gz", 1, 100)])

    # First bounded read should fetch ALL three from the DB (cache miss).
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {
        "legacy-1.gz",
        "legacy-2.gz",
        "new.gz",
    }


def test_unbounded_read_invalidates_cache(sid):
    """Unbounded reads (admin teardown, repair tools) MUST invalidate the
    cache so the next bounded read picks up any out-of-band DB changes the
    admin tool may have made (e.g., bulk delete)."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)  # warm cache

    # Sneak a row in via raw SQL and force unbounded read.
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)",
        ("sideloaded.gz", sid, 1, 100),
    )
    con.commit()

    # Unbounded read hits the DB (sees both) and invalidates the cache.
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz", "sideloaded.gz"}

    # Next bounded read re-populates from DB and now reflects the sideload.
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "sideloaded.gz"}


def test_teardown_invalidates_cache(sid):
    """``teardown`` removes the service DB; the cache MUST also drop so a
    later re-provisioned service with the same ID doesn't see phantom
    filenames from the previous incarnation."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    metadata_db.get_ingested_filenames(sid, limit=200)
    assert (sid, "logs") in metadata_db._ingested_filenames_cache

    metadata_db.teardown(sid)
    assert (sid, "logs") not in metadata_db._ingested_filenames_cache


def test_dedup_cache_returns_a_copy(sid):
    """Callers MUST receive a copy — the ingest pipeline mutates ``already``
    by adding newly discovered filenames; mutating the cached set directly
    would silently corrupt the cache for every other caller."""
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100)])
    snapshot = metadata_db.get_ingested_filenames(sid, limit=200)
    snapshot.add("mutated.gz")
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz"}


# ── cron_runs: start + log + busy + summary ────────────────────────────────


def test_start_cron_run_returns_a_run_id(sid):
    run_id = metadata_db.start_cron_run(sid, "sync")
    assert isinstance(run_id, int) and run_id > 0


def test_start_cron_run_raises_when_task_already_running(sid):
    """Two concurrent ``start_cron_run(svc, 'sync')`` calls must
    not both succeed — the second raises RuntimeError. Pinned
    because a duplicate run would write the same parquet keys
    twice and double-count rows."""
    metadata_db.start_cron_run(sid, "sync")
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(sid, "sync")


def test_start_cron_run_gate_is_per_service_not_global(sid):
    """crash SEAM-01: a 'running' sync on service A must NOT block a sync on
    service B. The busy check (``cron_log.py``) has no ``service_id`` predicate
    — independence rests ENTIRELY on ``get_con(service_id)`` routing each
    service to its own SQLite file (``db_path``). This pins that contract: with
    A running, B starts cleanly, while a second A correctly raises. Mutation
    that breaks it: point ``get_con`` at one shared file → B sees A's running
    row and raises (the scheduler would then serialise all services' syncs)."""
    svc_a, svc_b = "cronseam-a", "cronseam-b"
    # A starts and stays running.
    run_a = metadata_db.start_cron_run(svc_a, "sync")
    assert isinstance(run_a, int) and run_a > 0

    # B must start independently — different per-service DB file, so A's
    # running row is invisible to B's busy check.
    run_b = metadata_db.start_cron_run(svc_b, "sync")
    assert isinstance(run_b, int) and run_b > 0

    # And the within-service gate still holds for A (proves B's success wasn't
    # because the gate is simply off).
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(svc_a, "sync")


def test_start_cron_run_reaps_orphans_before_busy_check(sid):
    """If an OLD 'running' row (past the orphan threshold) exists,
    it's flipped to 'error' BEFORE the busy check, so the new
    start succeeds. Pinned because the reaping is what unblocks
    cron after a server crash mid-run."""
    # Insert an orphan directly via SQL — bypass start_cron_run's reaping
    con = metadata_db.get_con(sid)
    old_ts = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, parquet_keys) VALUES (?, ?, 'running', '[]')",
        ("sync", old_ts),
    )
    con.commit()

    # Now start a new run — should reap the orphan and succeed
    new_id = metadata_db.start_cron_run(sid, "sync")
    assert isinstance(new_id, int)


def test_log_cron_run_updates_existing_when_run_id_provided(sid):
    run_id = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(
        sid,
        "sync",
        duration_s=12.5,
        status="success",
        files_downloaded=10,
        rows_ingested=1000,
        run_id=run_id,
    )

    con = metadata_db.get_con(sid)
    row = con.execute(
        "SELECT status, duration_s, rows_ingested FROM cron_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["status"] == "success"
    assert row["duration_s"] == 12.5
    assert row["rows_ingested"] == 1000


def test_log_cron_run_inserts_when_no_run_id(sid):
    """Path used by code that didn't go through start_cron_run
    (retries, manual ingest). Pinned because losing this branch
    would lose the run's metadata entirely."""
    metadata_db.log_cron_run(sid, "manual", duration_s=1.0, status="success", files_downloaded=1)

    con = metadata_db.get_con(sid)
    rows = con.execute("SELECT task, status FROM cron_runs WHERE task = 'manual'").fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "success"


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
    """The rewrite from `id IN (SELECT max(id) GROUP BY task)` to the
    distinct-tasks + per-task index seek must still return one row per task —
    the *latest non-running* one. Pinned because the new form is a CTE with a
    correlated subquery, and a regression here would silently downgrade the
    sync-status panel."""
    r1 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=1.0, status="success", run_id=r1)
    r2 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=2.0, status="error", run_id=r2, error_message="boom")

    r3 = metadata_db.start_cron_run(sid, "optimize")
    metadata_db.log_cron_run(sid, "optimize", duration_s=3.0, status="success", run_id=r3)
    # A still-running optimize must NOT be picked
    metadata_db.start_cron_run(sid, "optimize")

    out = metadata_db.latest_cron_per_task(sid)
    assert set(out.keys()) == {"sync", "optimize"}
    assert out["sync"]["status"] == "error"
    assert out["sync"]["error_message"] == "boom"
    assert out["optimize"]["status"] == "success"
    assert out["optimize"]["duration_s"] == 3.0


def test_get_usage_logs_aggregates_and_breaks_down_in_one_pass(sid):
    """The aggregate totals and per-type breakdown are derived from a single
    GROUP BY (operation_class, operation_type) scan. Pinned because the prior
    two-query form is what the cost panel + Usage Log page contract was built
    against, and any drift in the totals or breakdown shape would silently
    break both."""
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
        """INSERT INTO usage_log
           (timestamp, service_id, operation_class, operation_type, url, status,
            duration_ms, function_name, process_context, bytes, count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    con.commit()

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T00:00:00Z", "2026-05-25T23:59:59Z")
    # ``total`` is the sum of the ``count`` column across matched rows
    # (2+3+1+5+7+4 = 22) — derived from the same grouped aggregate the
    # agg.* fields are built from, so the page query doesn't need a
    # separate COUNT(*). See usage_log.py:586-592 for the perf rationale.
    assert total == 22
    assert agg["total_class_a"] == 6  # 2+3+1
    assert agg["total_class_b"] == 5
    assert agg["total_cdn_downloads"] == 11  # 7+4
    assert agg["total_cdn_bytes"] == 3000
    assert agg["total_fos_bytes"] == 850  # 100+200+50+500
    assert agg["class_a_breakdown"] == {"PUT_OBJECT": 5, "POST": 1}
    assert agg["class_b_breakdown"] == {"GET_OBJECT": 5}


def test_usage_class_predicate_matches_across_callers():
    """``_usage_class_predicate`` is the single source of truth shared by
    ``get_usage_logs`` and ``iter_usage_logs_chunks`` (and the rollup
    aggregate). Pinned so the operation_class WHERE fragment stays identical
    for every usage_type — a drift here would silently re-class CDN/FOS rows
    differently between the page query and the CSV export."""
    from backend.core.metadata.usage_log import _usage_class_predicate

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


def test_cron_summary_for_tasks_returns_latest_per_task(sid):
    """Returns the most recent run's summary for each requested task.
    Pinned because the admin dashboard's "last sync" card keys on
    this exact shape."""
    rid = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=5.0, status="success", summary="OK", run_id=rid)

    out = metadata_db.cron_summary_for_tasks(sid, tasks=("sync", "commit"))
    assert "sync" in out
    assert out["sync"]["status"] == "success"
    assert out["sync"]["summary"] == "OK"
    # commit hasn't run — absent from the dict
    assert "commit" not in out


# ── adaptive_stale_minutes (SRE-22) ─────────────────────────────────────────


def _seed_sync_run(sid: str, minutes_ago: float, *, status: str = "success", rows_ingested: int = 1) -> None:
    """Insert a terminal `sync` cron_runs row directly — bypasses
    start_cron_run/log_cron_run's running-gate so many synthetic history
    rows can be seeded quickly for percentile tests."""
    con = metadata_db.get_con(sid)
    ts = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, rows_ingested) VALUES (?, ?, ?, ?)",
        ("sync", ts, status, rows_ingested),
    )
    con.commit()


def test_adaptive_stale_minutes_falls_back_with_too_little_history(sid):
    # Only 3 non-empty runs — below min_samples=10 (needs >=11 points for 10
    # gaps) — must return default_minutes unchanged.
    for i in range(3):
        _seed_sync_run(sid, minutes_ago=i * 40)
    con = metadata_db.get_con(sid)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30


def test_adaptive_stale_minutes_widens_to_2x_p95_gap_for_a_steady_cadence(sid):
    # 15 non-empty runs spaced exactly 40 minutes apart (ending at "now") →
    # every gap is 40min, so p95(gaps) == 40 and the widened threshold should
    # land at 2 * 40 = 80, well above the 30-minute default.
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 40)
    con = metadata_db.get_con(sid)
    got = metadata_db.adaptive_stale_minutes(con, default_minutes=30)
    assert got == 80


def test_adaptive_stale_minutes_never_narrows_below_default(sid):
    # A very tight historical cadence (5min gaps) must not pull the
    # threshold BELOW the caller's default_minutes.
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 5)
    con = metadata_db.get_con(sid)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30


def test_adaptive_stale_minutes_caps_at_max_minutes_on_a_huge_outlier(sid):
    # One enormous historical gap (2 days) among otherwise-tight runs must
    # not blow the threshold past max_minutes (default 1440 = 24h).
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 10)
    _seed_sync_run(sid, minutes_ago=15 * 10 + 2 * 24 * 60)
    con = metadata_db.get_con(sid)
    got = metadata_db.adaptive_stale_minutes(con, default_minutes=30, max_minutes=1440)
    assert got == 1440


def test_adaptive_stale_minutes_ignores_empty_and_errored_runs(sid):
    # Empty ("no new files") and errored runs must not count as ingest
    # events — only status='success' AND (files_downloaded>0 OR
    # rows_ingested>0) rows contribute to the gap calculation.
    for i in range(15):
        _seed_sync_run(sid, minutes_ago=i * 40)  # real ingests, 40min apart
        _seed_sync_run(sid, minutes_ago=i * 40 + 20, rows_ingested=0)  # empty tick in between
    _seed_sync_run(sid, minutes_ago=5, status="error", rows_ingested=5)  # errored — excluded regardless of count
    con = metadata_db.get_con(sid)
    got = metadata_db.adaptive_stale_minutes(con, default_minutes=30)
    assert got == 80  # unaffected by the empty/errored noise


def test_adaptive_stale_minutes_falls_back_on_query_error():
    # Any exception (e.g. a missing/older schema without files_downloaded)
    # must degrade to default_minutes, never propagate — this is what keeps
    # the existing minimal-schema health-check test fixtures passing
    # unmodified.
    class _BoomConnection:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such column: files_downloaded")

    assert metadata_db.adaptive_stale_minutes(_BoomConnection(), default_minutes=30) == 30


# ── asn_names cache ─────────────────────────────────────────────────────────


def test_lookup_asn_names_returns_only_fresh_entries(sid):
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON"})
    out = metadata_db.lookup_asn_names(sid, [7922, 16509, 99999])
    assert out[7922] == "COMCAST"
    assert out[16509] == "AMAZON"
    # Unknown ASN absent (not None)
    assert 99999 not in out


def test_lookup_asn_names_excludes_stale_entries(sid):
    """Entries older than ``max_age_days`` are excluded. Pinned
    because the cache re-fetch logic re-resolves stale names."""
    # Manually insert a stale entry
    con = metadata_db.get_con(sid)
    old = (datetime.now(UTC) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute("INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?)", (1, "OLD", old))
    con.commit()

    out = metadata_db.lookup_asn_names(sid, [1], max_age_days=30)
    assert out == {}  # excluded as stale


def test_lookup_asn_names_returns_empty_on_empty_input(sid):
    """Short-circuit on empty list — no SQL call. Pinned because
    building an IN () with zero placeholders is a SQL syntax error."""
    assert metadata_db.lookup_asn_names(sid, []) == {}


def test_upsert_asn_names_upserts_on_conflict(sid):
    metadata_db.upsert_asn_names(sid, {7922: "Old Name"})
    metadata_db.upsert_asn_names(sid, {7922: "New Name"})

    out = metadata_db.lookup_asn_names(sid, [7922])
    assert out[7922] == "New Name"


def test_upsert_asn_names_noops_on_empty_dict(sid):
    """Empty mapping → no SQL call. Same syntax-error reason."""
    metadata_db.upsert_asn_names(sid, {})  # must not raise


def test_asn_ints_for_search_returns_matching_asns(sid):
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON", 14618: "AMAZON-AES"})
    asns = metadata_db.asn_ints_for_search(sid, "%AMAZON%")
    assert set(asns) == {16509, 14618}


def test_asn_ints_for_search_is_case_insensitive(sid):
    """The query uses ``COLLATE NOCASE``. Pinned because users
    type "amazon" but the cache stores "AMAZON"."""
    metadata_db.upsert_asn_names(sid, {16509: "AMAZON"})
    assert 16509 in metadata_db.asn_ints_for_search(sid, "%amazon%")


# ── sources registry ────────────────────────────────────────────────────────


def test_register_source_is_idempotent(sid):
    """``INSERT OR IGNORE`` — the second register call is a no-op.
    Pinned because re-provisioning would otherwise hit a primary-key
    conflict."""
    metadata_db.register_source(sid, "svc1", '{"a": 1}', "logs_svc1")
    metadata_db.register_source(sid, "svc1", '{"a": 2}', "logs_svc1")  # must not raise

    out = metadata_db.get_source_by_name(sid, "svc1")
    # First registration's config wins — pin the IDEMPOTENT semantics
    assert json.loads(out["config"])["a"] == 1


def test_get_source_by_name_returns_none_for_unknown(sid):
    assert metadata_db.get_source_by_name(sid, "ghost") is None


def test_get_source_by_name_returns_full_dict_when_found(sid):
    metadata_db.register_source(sid, "svc-a", '{"k": "v"}', "logs_svc_a")
    out = metadata_db.get_source_by_name(sid, "svc-a")
    assert out == {"name": "svc-a", "config": '{"k": "v"}', "table_name": "logs_svc_a"}


# ── usage_log telemetry ────────────────────────────────────────────────────


def test_log_usage_calls_classifies_fos_class_a_correctly(sid):
    """FOS PUT_OBJECT / POST_OBJECT / COPY_OBJECT / LIST_OBJECTS_V2
    are Class A; everything else FOS is Class B. Pinned because the
    cost estimator multiplies by the per-class rate."""
    metadata_db.log_usage_calls(
        sid,
        [
            {"method": "PUT_OBJECT", "service": "FOS"},
            {"method": "GET_OBJECT", "service": "FOS"},
            {"method": "LIST_OBJECTS_V2", "service": "FOS"},
        ],
    )

    # usage_log lives in its own SQLite file post-2026-06-12 — see
    # backend/core/metadata/usage_log_db.py for rationale. Read against
    # that db, not metadata.db.
    con = usage_log_db.get_con(sid)
    classes = [
        r["operation_class"] for r in con.execute("SELECT operation_class FROM usage_log ORDER BY id").fetchall()
    ]
    assert classes == ["A", "B", "A"]


def test_log_usage_calls_classifies_cdn_separately(sid):
    metadata_db.log_usage_calls(sid, [{"method": "GET", "service": "CDN"}])
    con = usage_log_db.get_con(sid)
    row = con.execute("SELECT operation_class FROM usage_log").fetchone()
    assert row["operation_class"] == "CDN"


def test_log_usage_calls_noops_on_empty_list(sid):
    metadata_db.log_usage_calls(sid, [])  # must not raise
    con = usage_log_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM usage_log").fetchone()[0] == 0


# ── get_usage_logs (paginated query + aggregates) ───────────────────────


def _seed_usage_log_row(
    sid: str,
    timestamp: str,
    operation_class: str = "A",
    operation_type: str = "PUT_OBJECT",
    process_context: str = "cron:sync",
    bytes_count: int = 1024,
):
    """Insert a usage_log row directly via SQL for testing query helpers."""
    # usage_log now lives in its own per-service SQLite file (separated
    # from metadata.db on 2026-06-12) — insert against that db so the
    # public-API readers (metadata_db.get_usage_logs etc.) find the rows.
    con = usage_log_db.get_con(sid)
    con.execute(
        """INSERT INTO usage_log
            (service_id, timestamp, operation_class, operation_type, url, bytes,
             duration_ms, function_name, process_context, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
    # ``get_usage_logs`` now opens a separate ``mode=ro`` connection per
    # call (decoupled from the cron writer), so the writer's uncommitted
    # transaction is invisible to the reader. Commit here so the assertions
    # see the seeded rows.
    con.commit()


def test_get_usage_logs_filters_by_service_id_and_time_range(sid):
    """Only rows in the time range for the right service are returned.
    Pinned because the FE's usage page paginates these — losing the
    range filter would dump all of history into the first page."""
    _seed_usage_log_row(sid, "2026-01-01T00:00:00Z")  # in range
    _seed_usage_log_row(sid, "2026-01-15T12:00:00Z")  # in range
    _seed_usage_log_row(sid, "2025-12-31T00:00:00Z")  # out of range (before)
    _seed_usage_log_row(sid, "2026-02-01T00:00:00Z")  # out of range (after)
    # Different service — must not leak
    _seed_usage_log_row("other-svc", "2026-01-10T00:00:00Z")

    rows, total, agg = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
    )
    assert total == 2
    assert len(rows) == 2


def test_get_usage_logs_usage_type_filter_cdn(sid):
    """`usage_type=CDN` → only operation_class='CDN' rows. Pinned
    because the FE's tab switch keys on this filter — losing it
    would show FOS rows under the CDN tab."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", operation_class="B")
    _seed_usage_log_row(sid, "2026-01-10T00:02:00Z", operation_class="CDN")

    rows, total, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
        usage_type="CDN",
    )
    assert total == 1
    assert rows[0]["operation_class"] == "CDN"


def test_get_usage_logs_usage_type_filter_fos_includes_both_a_and_b(sid):
    """`usage_type=FOS` → operation_class IN ('A', 'B'). Pinned
    because the FOS tab aggregates both classes."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", operation_class="B")
    _seed_usage_log_row(sid, "2026-01-10T00:02:00Z", operation_class="CDN")

    rows, total, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
        usage_type="FOS",
    )
    assert total == 2
    classes = {r["operation_class"] for r in rows}
    assert classes == {"A", "B"}


def test_get_usage_logs_specific_fos_a_filter(sid):
    """`usage_type=FOS-A` → only A class. Pinned to distinguish from
    FOS-B and the combined FOS filter."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", operation_class="B")

    rows, total, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
        usage_type="FOS-A",
    )
    assert total == 1
    assert rows[0]["operation_class"] == "A"


def test_get_usage_logs_process_context_filter_uses_ilike(sid):
    """`process_context` is a substring filter (LIKE %x%). Pinned
    because admins type partial values into the filter input."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", process_context="cron:sync")
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", process_context="api:dashboard")

    rows, total, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
        process_context="cron",
    )
    assert total == 1
    assert "cron" in rows[0]["process_context"]


def test_get_usage_logs_paginates_with_page_and_page_size(sid):
    """`page=2 page_size=2` skips first 2 and returns next 2. Pinned
    because losing the pagination math would either show duplicate
    rows or skip entire pages."""
    for i in range(5):
        _seed_usage_log_row(sid, f"2026-01-{10 + i:02d}T00:00:00Z")

    rows, total, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
        page=2,
        page_size=2,
    )
    assert total == 5
    assert len(rows) == 2


def test_get_usage_logs_returns_class_aggregates(sid):
    """Aggregate dict contains total_class_a / total_class_b /
    total_cdn_downloads / total_cdn_bytes / total_fos_bytes +
    per-class breakdowns. Pinned because the cost-card on the FE
    keys on these field names."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_class="A", bytes_count=100)
    _seed_usage_log_row(sid, "2026-01-10T00:01:00Z", operation_class="A", bytes_count=200)
    _seed_usage_log_row(sid, "2026-01-10T00:02:00Z", operation_class="B", bytes_count=500)
    _seed_usage_log_row(sid, "2026-01-10T00:03:00Z", operation_class="CDN", bytes_count=1000)

    _, _, agg = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
    )

    assert agg["total_class_a"] == 2
    assert agg["total_class_b"] == 1
    assert agg["total_cdn_downloads"] == 1
    assert agg["total_cdn_bytes"] == 1000
    # FOS bytes = A + B
    assert agg["total_fos_bytes"] == 100 + 200 + 500


def test_get_usage_logs_orders_by_timestamp_desc(sid):
    """Default order is timestamp DESC (newest first). Pinned because
    the FE's log table assumes newest-on-top rendering — flipping
    would surprise admins."""
    _seed_usage_log_row(sid, "2026-01-10T00:00:00Z", operation_type="OLDEST")
    _seed_usage_log_row(sid, "2026-01-15T00:00:00Z", operation_type="NEWEST")

    rows, _, _ = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
    )
    assert rows[0]["operation_type"] == "NEWEST"
    assert rows[1]["operation_type"] == "OLDEST"


def test_get_usage_logs_empty_result_returns_zero_aggregates(sid):
    """No matching rows → empty list + total=0 + agg with all zeros.
    Pinned because the cost card renders 0 (not None / blank) when
    there's no telemetry — losing this would crash format strings."""
    rows, total, agg = metadata_db.get_usage_logs(
        service_id=sid,
        start="2026-01-01T00:00:00Z",
        end="2026-01-31T23:59:59Z",
    )
    assert rows == []
    assert total == 0
    assert agg["total_class_a"] == 0
    assert agg["total_class_b"] == 0
    assert agg["total_cdn_bytes"] == 0


def test_log_usage_calls_doubles_cdn_egress_on_shield_miss(sid):
    metadata_db.clear_usage_log(sid)

    # 1. Edge HIT (single egress)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "HIT · duckdb httpfs",
            }
        ],
    )

    # 2. Shield Miss, Edge Hit (single egress - Edge didn't fetch from shield on this request)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file2.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "MISS, HIT · duckdb httpfs",
            }
        ],
    )

    # 3. Full MISS (double egress)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file3.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "MISS, MISS · duckdb httpfs",
            }
        ],
    )

    # 4. MISS, PASS (double egress)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file4.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "MISS, PASS · duckdb httpfs",
            }
        ],
    )

    # 5. PASS, PASS (double egress)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file5.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "PASS, PASS · duckdb httpfs",
            }
        ],
    )

    # 6. HIT, MISS (double egress - Shield hit, but Edge missed so it pulled from Shield)
    metadata_db.log_usage_calls(
        sid,
        [
            {
                "method": "GET",
                "service": "CDN",
                "path": "/file6.parquet",
                "status": "200",
                "time_ms": 10.0,
                "bytes": 1000,
                "details": "HIT, MISS · duckdb httpfs",
            }
        ],
    )

    logs, _, _ = metadata_db.get_usage_logs(sid, "2000-01-01", "2099-01-01")

    assert len(logs) == 6

    # Sort by path so we can check bytes
    logs.sort(key=lambda x: x["url"])

    assert logs[0]["url"] == "/file.parquet"
    assert logs[0]["bytes"] == 1000

    assert logs[1]["url"] == "/file2.parquet"
    assert logs[1]["bytes"] == 1000

    assert logs[2]["url"] == "/file3.parquet"
    assert logs[2]["bytes"] == 2000

    assert logs[3]["url"] == "/file4.parquet"
    assert logs[3]["bytes"] == 2000

    assert logs[4]["url"] == "/file5.parquet"
    assert logs[4]["bytes"] == 2000

    assert logs[5]["url"] == "/file6.parquet"
    assert logs[5]["bytes"] == 2000


# ── usage_log coverage expansion (sub-86% gap fill) ─────────────────────────


def test_log_usage_calls_swallows_executemany_exception(sid, caplog):
    """log_usage_calls catches and logs a SQLite insert exception without
    re-raising. Pinned because the cron writer can't crash the request
    that triggered the call — usage telemetry is best-effort, not a
    correctness gate."""
    import logging as _logging

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con(sid)
    real_executemany = con.executemany

    def _explode(*_a, **_kw):
        raise RuntimeError("simulated commit failure")

    con.executemany = _explode  # type: ignore[method-assign]
    try:
        with caplog.at_level(_logging.ERROR, logger="backend.core.metadata.usage_log"):
            metadata_db.log_usage_calls(
                sid,
                [{"function_name": "ingest", "operation_type": "PUT_OBJECT", "url": "/x", "status": "OK"}],
            )
    finally:
        con.executemany = real_executemany  # type: ignore[method-assign]

    assert any("Failed to log usage calls" in r.message for r in caplog.records)


def test_log_synthetic_usage_empty_calls_returns_zero(sid):
    """Empty input → immediate 0 return without touching the connection."""
    assert metadata_db.log_synthetic_usage(sid, []) == 0


def test_log_synthetic_usage_returns_zero_when_no_paths(sid):
    """Calls without a ``path`` key are filtered out; if NONE have a path,
    log_synthetic_usage returns 0 before any SELECT or INSERT runs."""
    assert metadata_db.log_synthetic_usage(sid, [{"method": "PUT"}, {"caller": "x"}]) == 0


def test_log_synthetic_usage_dedupes_against_existing_urls(sid):
    """A second call with overlapping URLs only inserts the new ones."""
    from backend.core.metadata import usage_log_db

    n1 = metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/b"}])
    assert n1 == 2
    n2 = metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}])
    assert n2 == 1, "should have inserted only /c (dedup against /a)"

    con = usage_log_db.get_con(sid)
    urls = sorted(
        r["url"]
        for r in con.execute("SELECT url FROM usage_log WHERE function_name = 'fastly.edge' ORDER BY url").fetchall()
    )
    assert urls == ["/a", "/b", "/c"]


def test_log_synthetic_usage_skips_when_all_paths_already_present(sid):
    """If every path in the input is already in the table, new_rows is
    empty and the function short-circuits to 0 BEFORE attempting any
    INSERT."""
    n1 = metadata_db.log_synthetic_usage(sid, [{"path": "/dup1"}, {"path": "/dup2"}])
    assert n1 == 2
    n2 = metadata_db.log_synthetic_usage(sid, [{"path": "/dup1"}, {"path": "/dup2"}])
    assert n2 == 0


def test_log_synthetic_usage_swallows_insert_exception(sid, caplog):
    """When executemany fails on the synthetic-row INSERT, the function
    logs and returns 0 rather than propagating."""
    import logging as _logging

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con(sid)
    real_executemany = con.executemany

    def _explode(*_a, **_kw):
        raise RuntimeError("simulated insert failure")

    con.executemany = _explode  # type: ignore[method-assign]
    try:
        with caplog.at_level(_logging.ERROR, logger="backend.core.metadata.usage_log"):
            out = metadata_db.log_synthetic_usage(sid, [{"path": "/x"}, {"path": "/y"}])
    finally:
        con.executemany = real_executemany  # type: ignore[method-assign]

    assert out == 0
    assert any("Synthetic usage log failed" in r.message for r in caplog.records)


# ── reconcile_fastly_stats ──────────────────────────────────────────────────


def test_reconcile_fastly_stats_returns_zero_on_empty_records(sid):
    assert metadata_db.reconcile_fastly_stats(sid, []) == 0


def test_reconcile_fastly_stats_skips_records_without_hour_iso(sid):
    """Records missing hour_iso or with invalid hour_iso must be skipped
    without raising. If ALL records are malformed, return 0."""
    out = metadata_db.reconcile_fastly_stats(
        sid,
        [{"class_a": 10, "class_b": 5}, {"hour_iso": "not-a-date", "class_a": 1}],
    )
    assert out == 0


def test_reconcile_fastly_stats_writes_gap_rows_for_underreporting(sid):
    """When Fastly reports more Class-A/B ops than usage_log holds,
    reconciliation rows fill the gap."""
    from backend.core.metadata import usage_log_db

    for i in range(5):
        _seed_usage_log_row(sid, f"2026-05-22T13:0{i}:00Z", operation_class="A")

    written = metadata_db.reconcile_fastly_stats(
        sid,
        [{"hour_iso": "2026-05-22T13:00:00Z", "class_a": 10, "class_b": 3}],
    )
    assert written == 2

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT operation_class, count FROM usage_log "
        "WHERE function_name = 'fastly.reconciliation' ORDER BY operation_class"
    ).fetchall()
    classes = {r["operation_class"]: r["count"] for r in rows}
    assert classes == {"A": 5, "B": 3}


def test_reconcile_fastly_stats_does_not_double_count_prior_reconciliation_rows(sid):
    """Reconciliation rows are excluded from the local_sum math AND prior
    rows in the same window are deleted before new ones are written. A
    second call with the same Fastly counts must produce the SAME gap,
    not double it."""
    from backend.core.metadata import usage_log_db

    for i in range(2):
        _seed_usage_log_row(sid, f"2026-05-22T14:0{i}:00Z", operation_class="A")

    n1 = metadata_db.reconcile_fastly_stats(
        sid,
        [{"hour_iso": "2026-05-22T14:00:00Z", "class_a": 10, "class_b": 0}],
    )
    n2 = metadata_db.reconcile_fastly_stats(
        sid,
        [{"hour_iso": "2026-05-22T14:00:00Z", "class_a": 10, "class_b": 0}],
    )
    assert n1 == 1 and n2 == 1

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT count FROM usage_log WHERE function_name = 'fastly.reconciliation' AND operation_class = 'A'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 8


def test_reconcile_fastly_stats_writes_nothing_when_local_matches_fastly(sid):
    """If local_sum equals fastly_count, gap is 0 and no row is written."""
    for i in range(7):
        _seed_usage_log_row(sid, f"2026-05-22T15:0{i}:00Z", operation_class="A")

    written = metadata_db.reconcile_fastly_stats(
        sid,
        [{"hour_iso": "2026-05-22T15:00:00Z", "class_a": 7, "class_b": 0}],
    )
    assert written == 0


# ── _query_usage_log_aggregate_rollup branches via get_usage_logs ───────────


def test_get_usage_logs_sub_hour_range_uses_direct_scan(sid):
    """When start_hour == end_hour, the rollup helper takes the single-
    raw-scan path. The aggregate result still matches the seeded rows."""
    _seed_usage_log_row(sid, "2026-05-25T10:05:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-05-25T10:15:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-05-25T11:05:00Z", operation_class="A")  # different hour, excluded

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T10:00:00Z", "2026-05-25T10:30:00Z")
    assert total == 2
    assert agg["total_class_a"] == 2


def test_get_usage_logs_custom_usage_type_filters_by_class(sid):
    """A usage_type outside the known enum (CDN/FOS/FOS-A/FOS-B) is
    applied as a direct ``operation_class = ?`` filter — defensive
    forward-compat branch."""
    _seed_usage_log_row(sid, "2026-05-26T10:00:00Z", operation_class="A")
    _seed_usage_log_row(sid, "2026-05-26T10:01:00Z", operation_class="CUSTOM")

    _entries, _total, agg = metadata_db.get_usage_logs(
        sid, "2026-05-26T00:00:00Z", "2026-05-26T23:59:59Z", usage_type="CUSTOM"
    )
    assert agg["total_class_a"] == 0
    assert agg["total_class_b"] == 0


def test_get_usage_logs_operation_type_filter_uses_like(sid):
    """operation_type filter applies as ``LIKE %X%`` so substring matches
    work (e.g., 'PUT' matches 'PUT_OBJECT')."""
    _seed_usage_log_row(sid, "2026-05-27T10:00:00Z", operation_type="PUT_OBJECT")
    _seed_usage_log_row(sid, "2026-05-27T10:01:00Z", operation_type="GET_OBJECT")
    _seed_usage_log_row(sid, "2026-05-27T10:02:00Z", operation_type="POST")

    entries, total, _agg = metadata_db.get_usage_logs(
        sid, "2026-05-27T00:00:00Z", "2026-05-27T23:59:59Z", operation_type="PUT"
    )
    assert total == 1
    assert entries[0]["operation_type"] == "PUT_OBJECT"


def test_get_usage_logs_process_context_filter_uses_like(sid):
    """process_context filter is also LIKE-based — matches admin
    Usage Log page's 'filter by process' text input."""
    _seed_usage_log_row(sid, "2026-05-28T10:00:00Z", process_context="cron:sync")
    _seed_usage_log_row(sid, "2026-05-28T10:01:00Z", process_context="cron:commit")
    _seed_usage_log_row(sid, "2026-05-28T10:02:00Z", process_context="request:handler")

    entries, total, _agg = metadata_db.get_usage_logs(
        sid, "2026-05-28T00:00:00Z", "2026-05-28T23:59:59Z", process_context="cron"
    )
    assert total == 2
    assert all("cron" in e["process_context"] for e in entries)
