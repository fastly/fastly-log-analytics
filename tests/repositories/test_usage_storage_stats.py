from unittest.mock import MagicMock


def test_get_storage_stats_returns_filtered_files_and_bytes():
    """Verify that get_storage_stats correctly filters files by date and sums their sizes."""
    import backend.core.metadata_db as metadata_db
    from backend.repositories.usage import get_storage_stats

    metadata_db.get_con("test_svc").execute("DELETE FROM ingested_files")

    # 2 files in range, 1 out of range
    metadata_db.get_con("test_svc").execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) VALUES "
        "('file1.gz', 'test_svc', 100, 1024, '2024-05-10T10:00:00Z'),"
        "('file2.gz', 'test_svc', 200, 2048, '2024-05-15T12:00:00Z'),"
        "('file3.gz', 'test_svc', 300, 4096, '2024-05-20T14:00:00Z')"
    )

    src = {"name": "test_svc"}
    mock_con = MagicMock()

    stats = get_storage_stats(mock_con, src, "2024-05-10T00:00:00Z", "2024-05-16T00:00:00Z")

    assert stats["total_files"] == 2
    assert stats["total_bytes"] == 3072


def test_get_storage_stats_safe_from_sql_injection():
    """The previous DuckDB implementation interpolated src['name'] directly into the SQL string.
    The new metadata_db implementation uses parameterized queries under the hood, protecting
    against names containing quotes or other special characters."""
    import backend.core.metadata_db as metadata_db
    from backend.repositories.usage import get_storage_stats

    evil_name = "test' OR 1=1; DROP TABLE ingested_files; --"
    metadata_db.get_con(evil_name).execute("DELETE FROM ingested_files")

    metadata_db.get_con(evil_name).execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) VALUES "
        "(?, ?, 100, 1024, '2024-05-10T10:00:00Z')",
        ("file1.gz", evil_name),
    )

    src = {"name": evil_name}
    mock_con = MagicMock()

    # This would have raised a DuckDB syntax error in the old implementation
    stats = get_storage_stats(mock_con, src, "2024-05-01T00:00:00Z", "2024-05-20T00:00:00Z")

    assert stats["total_files"] == 1
    assert stats["total_bytes"] == 1024
