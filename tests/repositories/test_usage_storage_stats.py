from unittest.mock import MagicMock, patch


def test_get_storage_stats_returns_filtered_files_and_bytes():
    """Verify that get_storage_stats correctly filters files by date and sums their sizes."""
    import backend.core.metadata as metadata_db
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

    import backend.core.metadata as metadata_db
    from backend.core.metadata.base import InvalidServiceIdError

    evil_name = "test' OR 1=1; DROP TABLE ingested_files; --"
    with pytest.raises(InvalidServiceIdError):
        metadata_db.get_con(evil_name)


# ── get_edge_ratio: DuckDB schema branches ─────────────────────────────────────


def _schema(*cols: str) -> list[dict]:
    return [{"name": c} for c in cols]


def test_get_edge_ratio_returns_none_when_edge_column_missing():
    """Some Fastly schemas don't include the boolean ``edge`` column
    (older format versions, custom log formats). The cost panel must
    quietly return None instead of erroring so the row just hides."""
    from backend.repositories.usage import get_edge_ratio

    src = {"name": "svc_no_edge"}
    con = MagicMock()
    with patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "status")):
        ratio, debug = get_edge_ratio(con, src)
    assert ratio is None
    # Early-return path: no SQL queries should be issued.
    assert debug == []


def test_get_edge_ratio_returns_none_when_query_yields_no_result():
    """``execute_with_retry`` returns None when the underlying DuckDB
    op was swallowed (lock/timeout/etc.). The repository must propagate
    the None rather than crashing."""
    from backend.repositories.usage import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=None),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


def test_get_edge_ratio_returns_rounded_value_for_valid_row():
    """Happy path: the SQL returns a percentage, the repository rounds
    to one decimal place (matches the precision the UI displays)."""
    from backend.repositories.usage import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = [42.6789]
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio == 42.7


def test_get_edge_ratio_returns_none_for_empty_table():
    """Table exists with the ``edge`` column but has no rows — the
    percentage SQL returns a single row with NULL. Must surface as None
    so the UI can render the empty state instead of "0.0%"."""
    from backend.repositories.usage import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = [None]
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


def test_get_edge_ratio_returns_none_when_no_row_at_all():
    """Defensive: ``fetchone`` returning None (rather than [None]) must
    also short-circuit to None instead of indexing into nothing."""
    from backend.repositories.usage import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = None
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


# ── get_log_activity: bucketing + service_id fallback ──────────────────────────


def test_get_log_activity_uses_service_name_and_bucketing():
    """get_log_activity reads the per-service SQLite ``ingested_files``
    rows, buckets by the requested width, and tacks on the empty debug
    keys that downstream telemetry middleware expects."""
    import backend.core.metadata as metadata_db
    from backend.repositories.usage import get_log_activity

    metadata_db.get_con("act_svc").execute("DELETE FROM ingested_files")
    metadata_db.get_con("act_svc").execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at, file_date) VALUES "
        "('f1.gz', 'act_svc', 100, 1024, '2024-05-10T10:00:00Z', '2024-05-10'),"
        "('f2.gz', 'act_svc', 200, 2048, '2024-05-10T11:00:00Z', '2024-05-10'),"
        "('f3.gz', 'act_svc', 300, 4096, '2024-05-11T12:00:00Z', '2024-05-11')"
    )

    out = get_log_activity(
        {"name": "act_svc"},
        "2024-05-10T00:00:00Z",
        "2024-05-12T00:00:00Z",
        "day",
    )

    # Telemetry keys are always present so the response shape is uniform.
    assert out["_debug_queries"] == []
    assert out["_debug_calls"] == []
    # And the bucketing actually ran (something day-shaped came back).
    assert any(k in out for k in ("buckets", "rows", "series", "data"))


def test_get_log_activity_falls_back_to_service_id_when_name_missing():
    """Some callers pass {service_id: ...} without a ``name`` key (e.g.
    the analyst-side share-token flow that doesn't have a SQL-safe table
    name handy). The repo must fall back to service_id rather than
    looking up the empty string and silently returning nothing."""
    import backend.core.metadata as metadata_db
    from backend.repositories.usage import get_log_activity

    metadata_db.get_con("id_only_svc").execute("DELETE FROM ingested_files")

    with patch.object(metadata_db, "get_log_activity", return_value={"buckets": []}) as mock_get:
        get_log_activity(
            {"service_id": "id_only_svc"},
            "2024-05-10T00:00:00Z",
            "2024-05-12T00:00:00Z",
            "hour",
        )

    # The first positional arg to metadata_db.get_log_activity is the
    # service id — pin that the fallback handed off service_id, not "".
    assert mock_get.call_args.args[0] == "id_only_svc"
    assert mock_get.call_args.args[3] == "hour"
