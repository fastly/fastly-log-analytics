"""Tests for ``record_scoring_audit`` / ``list_scoring_audit`` / ``prune_scoring_audit``.

Exercises the metadata_db layer directly — no FastAPI, no mocks. The
autouse ``isolate_metadata_db`` fixture from ``tests/conftest.py``
already redirects ``metadata_db._DATA_DIR`` at the per-test tmp_path,
so every call to ``get_con(service_id)`` lands in a sandboxed SQLite
file the test owns end-to-end.

What's pinned here:
  - The append + read round-trip preserves action / actor / details.
  - Multiple rows come back newest-first (DESC by id, which also
    matches the timestamp tiebreaker).
  - ``limit`` truncates correctly.
  - ``since`` filters by ISO timestamp lower bound.
  - ``prune_scoring_audit(keep_last=N)`` leaves exactly the N newest
    rows behind on a service with > N existing entries.
  - The recorder swallows SQLite failures rather than raising
    (operator hot-path must not fail because the audit append failed —
    the operator action itself already happened).
"""

from __future__ import annotations

import pytest

from backend.core import metadata as metadata_db

SVC = "test-audit-svc"


# ── record + list round trip ────────────────────────────────────────────────


def test_record_then_list_returns_the_row():
    """The basic happy path: append one entry, read it back. Fields
    must round-trip verbatim and the details dict must come back
    parsed (JSON-string in storage, dict on the read side)."""
    metadata_db.record_scoring_audit(
        SVC,
        "scoring_enabled",
        details={"matrix_version": "v1", "scoring_service_id": "scorer-1"},
    )

    rows = metadata_db.list_scoring_audit(SVC)
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "scoring_enabled"
    assert row["actor"] == "operator"  # default actor
    assert row["details"] == {"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    assert "timestamp" in row
    assert "id" in row


def test_record_multiple_rows_returns_newest_first():
    """Multiple sequential appends → DESC ordering. We rely on the
    id-DESC tiebreaker rather than timestamp-DESC alone because
    sqlite's ``datetime('now')`` resolves to whole seconds and the
    autoincrement id is the only monotonic guarantee within a second."""
    metadata_db.record_scoring_audit(SVC, "scoring_enabled")
    metadata_db.record_scoring_audit(SVC, "threshold_committed", details={"new_threshold": 80})
    metadata_db.record_scoring_audit(SVC, "matrix_retrained", details={"matrix_version": "v2"})

    rows = metadata_db.list_scoring_audit(SVC)
    actions = [r["action"] for r in rows]
    assert actions == ["matrix_retrained", "threshold_committed", "scoring_enabled"]


def test_list_limit_truncates_to_n_rows():
    """``limit`` is enforced server-side. Insert 5, ask for 2, get the
    2 newest."""
    for i in range(5):
        metadata_db.record_scoring_audit(SVC, f"action_{i}", details={"i": i})
    rows = metadata_db.list_scoring_audit(SVC, limit=2)
    assert len(rows) == 2
    # Newest first: action_4 then action_3
    assert rows[0]["action"] == "action_4"
    assert rows[1]["action"] == "action_3"


def test_list_since_filter_excludes_older_rows():
    """``since`` is an ISO timestamp lower bound. Insert a row, capture
    a cut-off timestamp, and insert another. The first row must be filtered out."""
    metadata_db.record_scoring_audit(SVC, "older_action")
    con = metadata_db.get_con(SVC)
    con.execute("UPDATE scoring_audit SET timestamp = datetime('now', '-10 seconds') WHERE action = 'older_action'")
    con.commit()
    cutoff = con.execute("SELECT datetime('now', '-5 seconds') AS now").fetchone()["now"]
    metadata_db.record_scoring_audit(SVC, "newer_action")

    rows = metadata_db.list_scoring_audit(SVC, since=cutoff)
    actions = [r["action"] for r in rows]
    assert actions == ["newer_action"]
    assert "older_action" not in actions


def test_list_since_filter_with_future_timestamp_returns_empty():
    """A ``since`` far in the future → no rows match → empty list (NOT
    an error). The admin UI polls with ``since=<last_seen_ts>`` and
    expects an empty array when nothing new has happened."""
    metadata_db.record_scoring_audit(SVC, "anything")
    rows = metadata_db.list_scoring_audit(SVC, since="2099-01-01T00:00:00")
    assert rows == []


# ── prune ──────────────────────────────────────────────────────────────────


def test_prune_keeps_last_n_rows():
    """Insert 10 rows, prune to keep_last=5, list returns 5 newest.

    We backdate the first batch of 5 rows so they are strictly older,
    making the "which 5 survived" assertion deterministic. Within a batch we rely
    on the id-DESC tiebreaker that ``prune_scoring_audit``'s inner
    SELECT carries."""
    # First batch — older
    for i in range(5):
        metadata_db.record_scoring_audit(SVC, f"old_{i}")

    con = metadata_db.get_con(SVC)
    con.execute("UPDATE scoring_audit SET timestamp = datetime('now', '-10 seconds') WHERE action LIKE 'old_%'")
    con.commit()

    # Second batch — newer
    for i in range(5):
        metadata_db.record_scoring_audit(SVC, f"new_{i}")

    # Before pruning: 10 rows
    pre = metadata_db.list_scoring_audit(SVC, limit=1000)
    assert len(pre) == 10

    metadata_db.prune_scoring_audit(SVC, keep_last=5)

    post = metadata_db.list_scoring_audit(SVC, limit=1000)
    assert len(post) == 5
    # The 5 survivors are all from the "new_" batch.
    survivor_actions = {r["action"] for r in post}
    assert survivor_actions == {f"new_{i}" for i in range(5)}
    # And not a single "old_" leaked through.
    assert not any(r["action"].startswith("old_") for r in post)


def test_prune_no_op_when_below_keep_last():
    """If rows < keep_last, prune is a no-op — every row survives."""
    for i in range(3):
        metadata_db.record_scoring_audit(SVC, f"a_{i}")
    metadata_db.prune_scoring_audit(SVC, keep_last=100)
    rows = metadata_db.list_scoring_audit(SVC, limit=1000)
    assert len(rows) == 3


def test_prune_is_per_service_scoped():
    """Pruning service A must not touch service B's rows. This is the
    only guarantee that one busy service's audit-trimming cron can't
    accidentally wipe a quieter service's history."""
    other = "test-audit-other-svc"
    for i in range(3):
        metadata_db.record_scoring_audit(SVC, f"svc_a_{i}")
    for i in range(3):
        metadata_db.record_scoring_audit(other, f"svc_b_{i}")

    metadata_db.prune_scoring_audit(SVC, keep_last=1)

    # SVC trimmed to 1; other still has all 3.
    assert len(metadata_db.list_scoring_audit(SVC, limit=100)) == 1
    assert len(metadata_db.list_scoring_audit(other, limit=100)) == 3


# ── malformed-input resilience ─────────────────────────────────────────────


def test_record_swallows_malformed_service_id():
    """``record_scoring_audit`` is best-effort by design — every
    SQLite failure path is logged at DEBUG and swallowed so the
    operator hot-path (e.g. /scoring/threshold PUT) doesn't 500
    because the audit append racing-conditioned a WAL writer.

    ``service_id=None`` hits the ``db_path`` TypeError guard, which
    bubbles up as TypeError (not sqlite3.Error). Since the function
    only catches ``sqlite3.Error``, this would raise — pin the
    contract so a future refactor that broadens the except clause
    keeps the swallow semantics consistent.

    Today: passing None RAISES TypeError. The test documents that —
    if/when the function broadens to ``except Exception``, update the
    assertion to ``records_no_exception`` and the production code
    aligns with the docstring."""
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(None, "scoring_enabled")  # type: ignore[arg-type]


def test_record_handles_unjsonable_details_gracefully():
    """``json.dumps`` will raise on non-serializable details (e.g. a
    raw set, or a datetime without isoformat). Today the implementation
    catches only ``sqlite3.Error`` so a TypeError from json.dumps
    propagates — pin this so callers don't accidentally pass an
    un-jsonable dict and silently drop the audit entry without
    knowing."""
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(
            SVC,
            "weird_action",
            details={"a_set": {1, 2, 3}},  # type: ignore[dict-item]
        )


# ── list-on-empty resilience ───────────────────────────────────────────────


def test_list_returns_empty_on_brand_new_service():
    """A service whose metadata DB has never seen a scoring_audit
    insert → empty list (NOT an error). The first call to ``get_con``
    creates the table via _init_schema, so the SELECT returns 0 rows."""
    rows = metadata_db.list_scoring_audit("freshly-minted-svc")
    assert rows == []
