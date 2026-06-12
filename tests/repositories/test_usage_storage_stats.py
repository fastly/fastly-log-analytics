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
    """A malformed service_id with quote-injection characters must be rejected
    at the data-layer chokepoint BEFORE it can reach the SQL layer.

    The original version of this test exercised the parameterized-query path
    by passing the evil name through ``get_storage_stats`` and asserting no
    SQL errors. That scenario is now structurally impossible: commit
    ``acf81f0`` added an ``_SERVICE_ID_RE = ^[A-Za-z0-9_-]{1,64}$`` validator
    inside :func:`backend.core.metadata.base.get_con`, so the evil name
    raises :class:`InvalidServiceIdError` on the very first call. The
    parameterization is still in place (it's defense in depth), but the
    validator catches the attack at a higher layer.

    This test now pins the validator's behavior instead — if a future
    refactor weakens the regex (e.g. allows quotes), this test trips first.
    """
    import pytest

    import backend.core.metadata_db as metadata_db
    from backend.core.metadata.base import InvalidServiceIdError

    evil_name = "test' OR 1=1; DROP TABLE ingested_files; --"
    with pytest.raises(InvalidServiceIdError):
        metadata_db.get_con(evil_name)
