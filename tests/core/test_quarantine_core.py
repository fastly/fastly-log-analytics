"""Tests for the quarantine metadata core layer."""

from __future__ import annotations

import pytest

from backend.core.metadata.quarantine import (
    _parse_json_col,
    delete_quarantined_rows,
    get_con,
    get_expired_quarantined_files,
    get_quarantine_storage_total,
    get_quarantine_summary,
    get_quarantined_file_by_id,
    insert_quarantined_file,
    list_quarantined_files,
)


@pytest.fixture
def svc_id():
    svc = "test-quarantine-svc"
    get_con(svc).execute("SELECT 1")  # trigger migrations
    return svc


def test_quarantine_flow(svc_id):
    # 1. Insert a quarantined file
    insert_quarantined_file(
        service_id=svc_id,
        file_name="batch_1.parquet",
        source_name=svc_id,
        fos_key="raw/batch_1.parquet",
        error_key="errors/batch_1.parquet",
        meta_key="metadata/batch_1.parquet",
        valid_rows=100,
        corrupt_rows=10,
        file_size_bytes=1024,
        corrupt_samples=["sample1", "sample2"],
        reason_counts={"invalid_ip": 10},
        error_size_bytes=2048,
    )

    # 2. Get quarantined file by ID
    files = list_quarantined_files(svc_id)
    assert len(files) == 1
    file_id = files[0]["id"]

    file_by_id = get_quarantined_file_by_id(svc_id, file_id)
    assert file_by_id is not None
    assert file_by_id["file_name"] == "batch_1.parquet"
    assert file_by_id["valid_rows"] == 100
    assert file_by_id["corrupt_rows"] == 10
    assert file_by_id["corrupt_samples"] == ["sample1", "sample2"]
    assert file_by_id["reason_counts"] == {"invalid_ip": 10}

    # 3. Get quarantine summary
    summary = get_quarantine_summary(svc_id)
    assert summary["total_files"] == 1
    assert summary["total_corrupt_rows"] == 10
    assert summary["oldest_at"] is not None

    # 4. Storage total
    assert get_quarantine_storage_total(svc_id) == 2048

    # 5. Get expired files (using a mock date or passing retention of 0 for future but let's test 14 retention days by default empty)
    expired = get_expired_quarantined_files(svc_id, retention_days=14)
    assert len(expired) == 0

    # Let's manually backdate the quarantined_at timestamp to test expiration
    con = get_con(svc_id)
    con.execute("UPDATE quarantined_files SET quarantined_at = datetime('now', '-20 days')")
    con.commit()

    expired = get_expired_quarantined_files(svc_id, retention_days=14)
    assert len(expired) == 1
    assert expired[0]["id"] == file_id

    # 6. Delete quarantined rows
    deleted_count = delete_quarantined_rows(svc_id, [file_id])
    assert deleted_count == 1

    # Verify deleted
    assert len(list_quarantined_files(svc_id)) == 0
    assert get_quarantined_file_by_id(svc_id, file_id) is None


def test_delete_quarantined_rows_empty(svc_id):
    assert delete_quarantined_rows(svc_id, []) == 0


def test_parse_json_col_edge_cases():
    assert _parse_json_col(None) == {}
    assert _parse_json_col("", default=[]) == []
    assert _parse_json_col("{invalid json}", default={"error": True}) == {"error": True}
    assert _parse_json_col('["a", "b"]') == ["a", "b"]


def test_readers_scope_by_service_id_not_source_name(svc_id):
    """Every reader must scope on service_id, not source_name.

    All five readers used to filter ``WHERE source_name = ?`` while binding
    the *service_id*, which only worked because the two coincide today
    (``src["name"]`` is the service id). A source named differently from its
    service made its quarantined files invisible to the admin Quarantine
    view AND to the retention sweeper, while they kept occupying FOS. This
    pins the fix by using a source_name that deliberately does NOT match.
    """
    for fn in ("mismatch-a.gz", "mismatch-b.gz"):
        insert_quarantined_file(
            service_id=svc_id,
            file_name=fn,
            source_name="a-differently-named-source",
            fos_key=f"raw/{fn}",
            error_key=f"errors/{fn}.bad.jsonl",
            meta_key=f"errors/{fn}.meta.json",
            valid_rows=10,
            corrupt_rows=2,
            file_size_bytes=999,
            error_size_bytes=21,
        )

    listed = {r["file_name"] for r in list_quarantined_files(svc_id)}
    assert {"mismatch-a.gz", "mismatch-b.gz"} <= listed, "differently-named source is invisible to the admin view"

    summary = get_quarantine_summary(svc_id)
    assert summary["total_files"] >= 2
    assert summary["total_corrupt_rows"] >= 4

    # The retention sweeper is the one whose silent failure leaves bytes in
    # FOS forever, so it gets its own assertion. Backdate the rows rather
    # than passing a negative retention: the helper interpolates into
    # ``datetime('now', '-N days')``, and a negative N yields the invalid
    # modifier ``'--1 days'``, which SQLite resolves to NULL — the query
    # would return nothing regardless of the scoping fix.
    con = get_con(svc_id)
    con.execute(
        "UPDATE quarantined_files SET quarantined_at = '2020-01-01 00:00:00' "
        "WHERE service_id = ? AND file_name LIKE 'mismatch-%'",
        (svc_id,),
    )
    con.commit()
    expired = get_expired_quarantined_files(svc_id, retention_days=1)
    assert {"errors/mismatch-a.gz.bad.jsonl", "errors/mismatch-b.gz.bad.jsonl"} <= {r["error_key"] for r in expired}

    assert get_quarantine_storage_total(svc_id) >= 42

    ids = [r["id"] for r in list_quarantined_files(svc_id) if r["file_name"].startswith("mismatch-")]
    assert delete_quarantined_rows(svc_id, ids) == len(ids)


def test_requarantine_replaces_instead_of_duplicating(svc_id):
    """``INSERT OR REPLACE`` dedupes on the table's UNIQUE, so it must be
    ``(service_id, file_name)``. The Postgres ``ON CONFLICT`` target was
    ``id`` — which the insert never supplies (it is autoincrement) — so the
    conflict clause could never fire and a re-quarantine INSERTed a
    duplicate under Postgres while SQLite correctly replaced.
    """
    args = dict(
        service_id=svc_id,
        file_name="repeat.gz",
        source_name="src-x",
        fos_key="raw/repeat.gz",
        error_key="errors/repeat.bad.jsonl",
        meta_key="errors/repeat.meta.json",
        file_size_bytes=100,
        error_size_bytes=10,
    )
    insert_quarantined_file(valid_rows=5, corrupt_rows=1, **args)
    insert_quarantined_file(valid_rows=9, corrupt_rows=4, **args)

    rows = [r for r in list_quarantined_files(svc_id) if r["file_name"] == "repeat.gz"]
    assert len(rows) == 1, "re-quarantining the same file must replace, not duplicate"
    assert rows[0]["corrupt_rows"] == 4, "the replacement must carry the newer counts"

    delete_quarantined_rows(svc_id, [rows[0]["id"]])
