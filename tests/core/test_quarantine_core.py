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
        source_name="source_1",
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
