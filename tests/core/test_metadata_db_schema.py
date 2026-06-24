"""Schema-shape and migration-safety tests for backend.core.metadata_db.

Covers:
- All declared tables and indexes are present after ``_init_schema``.
- ``_init_schema`` is idempotent — running it on an already-initialised
  file is a no-op (no errors, no data loss).
- The ``IF NOT EXISTS`` guards mean future schema additions just append
  to ``_SCHEMA`` and run on next ``get_con``. This test verifies that
  pattern: re-init after a row is seeded must preserve the row.

These tests don't enforce a specific schema-version bump strategy because
the codebase doesn't have one yet — when one is added, this file is the
right place to tighten the migration assertions.
"""

from __future__ import annotations

import sqlite3

import pytest

from backend.core import metadata as metadata_db

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
    "local_compacted_files",
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
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def _list_indexes(con: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


# ── Initial schema shape ──────────────────────────────────────────────────────


def test_init_schema_creates_all_expected_tables():
    sid = "svc-schema-shape"
    con = metadata_db.get_con(sid)
    found = _list_tables(con)
    missing = _EXPECTED_TABLES - found
    assert not missing, f"missing tables after _init_schema: {missing}"


def test_init_schema_creates_all_expected_indexes():
    sid = "svc-schema-indexes"
    con = metadata_db.get_con(sid)
    found = _list_indexes(con)
    missing = _EXPECTED_INDEXES - found
    assert not missing, f"missing indexes after _init_schema: {missing}"


def test_alerts_table_has_evaluation_scope_column():
    """Regression: alerts.evaluation_scope was added later. Make sure new
    services get the full current shape."""
    sid = "svc-schema-alerts"
    con = metadata_db.get_con(sid)
    cols = _columns(con, "alerts")
    assert "evaluation_scope" in cols
    assert "comparison_period_min" in cols  # also late-added


# The legacy usage_log table in metadata.db was deleted alongside its
# DDL + triggers. Per-service usage rows live in the dedicated
# usage_log SQLite (backend.core.metadata.usage_log_db); its shape is
# pinned by tests/routers/test_usage_log.py.


# ── Idempotency: re-running init must not lose data ──────────────────────────


def test_init_schema_is_idempotent_no_data_loss():
    """Re-applying ``_init_schema`` to an already-populated file must
    preserve every row. This is the load-bearing property that lets future
    schema additions just append to ``_SCHEMA``.
    """
    sid = "svc-schema-idem"
    metadata_db.insert_ingested_files(sid, [("file-a.gz", 100, 4096), ("file-b.gz", 200, 8192)])

    con = metadata_db.get_con(sid)
    before = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert before == 2

    # Re-apply schema. With ``IF NOT EXISTS`` everywhere, this should be a no-op.
    metadata_db._init_schema(con)

    after = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert after == 2, f"data lost after re-init: was 2 rows, now {after}"


def test_init_schema_run_twice_is_safe_in_sequence():
    """The autouse ``isolate_metadata_db`` fixture clears the
    ``_initialized`` set between tests, so cold opens may legitimately
    re-init. Verify back-to-back calls don't raise.
    """
    sid = "svc-schema-double"
    metadata_db._init_schema(metadata_db.get_con(sid))
    metadata_db._init_schema(metadata_db.get_con(sid))  # must not raise


# ── Forward-compat: a future schema addition pattern ──────────────────────────


def test_pre_existing_data_survives_added_table():
    """Simulate a future migration: a new ``CREATE TABLE IF NOT EXISTS``
    statement is added to ``_SCHEMA``. Existing data in other tables
    must survive when ``_init_schema`` re-runs on the next process boot.
    """
    sid = "svc-schema-future"
    metadata_db.insert_ingested_files(sid, [("survivor.gz", 1, 100)])

    # Apply a hypothetical future migration (extra table)
    con = metadata_db.get_con(sid)
    con.execute("CREATE TABLE IF NOT EXISTS new_feature_table (id INTEGER PRIMARY KEY, payload TEXT)")
    con.commit()

    # Re-apply the standard schema — survivor row must remain
    metadata_db._init_schema(con)

    rows = con.execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid,)).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "survivor.gz"

    # And the new_feature_table is still there too
    tables = _list_tables(con)
    assert "new_feature_table" in tables


# ── Boundary guard: service_id must be a string ────────────────────────────────


@pytest.mark.parametrize(
    "bad_sid",
    [
        None,
        123,
        object(),
        {"name": "svc-a"},
    ],
    ids=["none", "int", "object", "dict"],
)
def test_db_path_rejects_non_string_service_id(bad_sid):
    """``db_path`` builds the SQLite filename via ``f"{service_id}.metadata.db"``.
    A non-string argument would silently produce a junk path containing the
    object's repr (e.g. ``<...0x...>.metadata.db``), leaking files on disk
    and corrupting per-service routing. The boundary must fail loud.
    """
    with pytest.raises(TypeError):
        metadata_db.db_path(bad_sid)


def test_get_con_rejects_non_string_service_id():
    """Same boundary as ``db_path`` — ``get_con`` builds the path via
    ``db_path``, and a non-string would create the file regardless. Pin
    the rejection at the public entry point as well.
    """
    with pytest.raises(TypeError):
        metadata_db.get_con(object())


@pytest.mark.parametrize(
    "bad_sid",
    [
        "../etc/passwd",  # path traversal via segment
        "foo/bar",  # embedded separator
        "foo\x00bar",  # null byte (truncates fopen on POSIX)
        "",  # empty produces ".metadata.db" (hidden junk file)
        "x" * 65,  # over 64-char cap
        "with space",  # whitespace
        "foo.bar",  # periods (not in Fastly's documented format)
        "\U00018d1f",  # plane-1 codepoint APFS rejects with Errno 92
        "café",  # any non-ASCII Unicode
    ],
    ids=["traversal", "slash", "null_byte", "empty", "too_long", "space", "period", "apfs_illegal", "non_ascii"],
)
def test_db_path_rejects_malformed_service_id(bad_sid):
    """The pattern guard rejects any string that could traverse the data
    directory or hit ``OSError(Errno 92): Illegal byte sequence`` on APFS /
    strict Linux. Pinned because losing this regresses the FastAPI 422
    contract — schemathesis fuzzing surfaced the path with %F0%98%B4%9F
    producing an opaque sqlite3.OperationalError 500.
    """
    from backend.core.metadata.base import InvalidServiceIdError

    with pytest.raises(InvalidServiceIdError):
        metadata_db.db_path(bad_sid)


def test_invalid_service_id_in_path_returns_422(client):
    """A malformed ``service_id`` in a path parameter must surface as 422
    (validation error) rather than 500 (sqlite OperationalError). The
    backend exception handler in main.py converts InvalidServiceIdError
    to a body matching FastAPI's ``HTTPValidationError`` schema so the
    response stays OpenAPI-conformant (schemathesis verified).
    """
    # Use a route that takes service_id as a Path parameter and reaches
    # the metadata_db layer. /scoring/labels exercises this surface.
    resp = client.get("/api/services/foo.bar/scoring/labels")
    assert resp.status_code == 422, f"expected 422, got {resp.status_code}: {resp.text[:200]}"
    body = resp.json()
    assert isinstance(body.get("detail"), list), "detail must be a list per HTTPValidationError"
    err = body["detail"][0]
    assert err["loc"] == ["path", "service_id"]
    assert "service_id must match" in err["msg"]
    assert err["type"] == "value_error.invalid_service_id"
