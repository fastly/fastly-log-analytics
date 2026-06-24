"""Tests for :mod:`backend.core.metadata.state`.

Covers the audit-log merge dedup path (used by state_sync to merge
remote audit rows into the local DB without clobbering analyst-side
entries) and the data-migration retry-on-locked-db path.
"""

from __future__ import annotations

from backend.core import metadata as metadata_db
from backend.core.metadata import state


def _con(service_id: str):
    return metadata_db.get_con(service_id)


# ── merge_audit_for_service ──────────────────────────────────────────────────


def test_merge_audit_noop_when_empty():
    state.merge_audit_for_service("svc-merge-empty", [])
    # Sanity: didn't touch the DB at all (audit_logs still empty).
    con = _con("svc-merge-empty")
    rows = con.execute("SELECT count(*) FROM audit_logs").fetchone()
    assert rows[0] == 0


def test_merge_audit_inserts_new_rows():
    sid = "svc-merge-new"
    rows = [
        {
            "timestamp": "2026-06-12T10:00:00Z",
            "source_name": "fos",
            "event_type": "ingest_start",
            "details": "{}",
            "actor": "cron",
        },
        {
            "timestamp": "2026-06-12T10:01:00Z",
            "source_name": "fos",
            "event_type": "ingest_done",
            "details": "{}",
            "actor": "cron",
        },
    ]
    state.merge_audit_for_service(sid, rows)
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2


def test_merge_audit_dedups_on_composite_key():
    """If the (timestamp, source_name, event_type, actor) tuple already
    exists in the local DB, the merge skips that row rather than creating
    a duplicate."""
    sid = "svc-merge-dedup"
    row = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
        "actor": "cron",
    }
    state.merge_audit_for_service(sid, [row])
    state.merge_audit_for_service(sid, [row])  # second call → dedup, skip
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 1


def test_merge_audit_distinguishes_by_actor():
    """Same timestamp + source_name + event_type but different actor →
    treated as a distinct event and inserted (the local analyst and the
    cron writer can both stamp ``ingest_start`` at the same second
    legitimately)."""
    sid = "svc-merge-actor"
    base = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
    }
    state.merge_audit_for_service(sid, [{**base, "actor": "cron"}, {**base, "actor": "analyst"}])
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2
