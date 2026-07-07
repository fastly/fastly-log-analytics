import os
from unittest.mock import MagicMock, patch

import pyarrow as pa

from backend.core import iceberg


def test_align_to_schema():
    # Create a table with a subset of fields and one extra field
    data = {
        "timestamp": [1714819200],  # USMALLINT/UTINYINT/UINTEGER/UBIGINT widening is handled by DuckDB usually,
        # but here we test the Arrow alignment.
        "ip": ["1.1.1.1"],
        "extra_field": ["hidden"],
    }
    table = pa.table(data)

    aligned = iceberg._align_to_schema(table)

    # Check that it has all fields from get_arrow_schema()
    arrow_schema = iceberg.get_arrow_schema()
    assert len(aligned.schema) == len(arrow_schema)
    assert "timestamp" in aligned.column_names
    assert "ip" in aligned.column_names
    assert "extra_field" not in aligned.column_names

    # Check that missing fields are null
    assert "status" in aligned.column_names
    assert aligned.column("status").null_count == len(aligned)


def test_table_identifier():
    source = {"bucket": "my-bucket", "prefix": "logs/"}
    assert iceberg._table_identifier(source) == ("default", "logs")


def test_warehouse_uri():
    source = {"bucket": "my-bucket", "prefix": "logs/"}
    assert iceberg._warehouse_uri(source) == "s3://my-bucket/logs/iceberg"

    source_no_prefix = {"bucket": "my-bucket", "prefix": ""}
    assert iceberg._warehouse_uri(source_no_prefix) == "s3://my-bucket/iceberg"


@patch("backend.core.iceberg._get_catalog")
def test_init_iceberg_table_exists(mock_get_catalog):
    mock_catalog = MagicMock()
    mock_get_catalog.return_value = mock_catalog

    source = {"name": "test-svc", "bucket": "b", "prefix": "p"}

    # Mock table already exists
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_table.schema.return_value.fields = iceberg.get_iceberg_schema().fields

    result = iceberg.init_iceberg_table(source)

    assert result == mock_table
    mock_catalog.load_table.assert_called_once_with(("default", "logs"))
    mock_catalog.create_table.assert_not_called()


def test_buffer_dir():
    source = {"name": "test-svc"}
    # backend.core.duckdb._cache_dir usually returns cache/{svc}
    with patch("backend.core.duckdb._cache_dir", return_value="cache/test-svc"):
        assert iceberg._buffer_dir(source) == "cache/test-svc/buffer"


@patch("backend.core.iceberg._table_identifier")
@patch("backend.core.duckdb._get_fos_client")
@patch("backend.core.iceberg.os.makedirs")
@patch("backend.core.iceberg._refresh_local_catalog_metadata")
@patch("backend.core.iceberg._get_catalog")
def test_sync_data_analyst_time_range_filter(
    mock_get_catalog, mock_refresh, mock_makedirs, mock_get_fos_client, mock_table_identifier
):
    """Test that time range parameters correctly construct PyIceberg scan filters using datetime."""
    # Mocking standard inputs
    source = {"name": "test-svc", "bucket": "b", "prefix": "p", "access_level": "read_only"}
    mock_catalog = MagicMock()
    mock_get_catalog.return_value = mock_catalog
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_table_identifier.return_value = ("default", "logs")

    mock_scan = MagicMock()
    mock_table.scan.return_value = mock_scan

    # Let filter return the mock_scan again so it chains
    mock_scan.filter.return_value = mock_scan

    # Test with string values like the frontend passes
    res = iceberg.sync_data(source, start_time="2026-05-01T00:00:00Z", end_time="2026-05-02T00:00:00Z")

    # We expect .filter() to have been called twice, with properly formatted ISO strings representing datetimes
    assert mock_scan.filter.call_count == 2

    # We need to inspect the expression arguments to ensure they are the correctly formatted strings, not naive strings.
    # First call is GreaterThanOrEqual
    args_1, kwargs_1 = mock_scan.filter.call_args_list[0]
    expr_1 = args_1[0]
    assert expr_1.term.name == "timestamp"
    # Should end in +00:00 explicitly from timezone formatting
    assert expr_1.literal.value == "2026-05-01T00:00:00+00:00"

    # Second call is LessThanOrEqual
    args_2, kwargs_2 = mock_scan.filter.call_args_list[1]
    expr_2 = args_2[0]
    assert expr_2.term.name == "timestamp"
    assert expr_2.literal.value == "2026-05-02T00:00:00+00:00"


@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.iceberg._refresh_local_catalog_metadata")
def test_sync_data_invalid_time_range(mock_refresh, mock_get_catalog):
    """Test that an invalid time range (start > end) returns an empty result gracefully."""
    source = {"name": "test-svc", "bucket": "b", "prefix": "p", "access_level": "read_only"}

    # Test with start_time > end_time
    res = iceberg.sync_data(source, start_time="2026-05-03T00:00:00Z", end_time="2026-05-02T00:00:00Z")

    assert res == {"files_downloaded": 0, "rows_downloaded": 0, "message": "Invalid time range: start after end."}


@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_downloads_missing_files(
    mock_cache_dir, mock_get_catalog, s3_mock, fos_source, tmp_path, monkeypatch
):
    """End-to-End: real moto S3 + mocked catalog. Verifies the missing file is
    actually downloaded into the cache directory.

    Migrated from the original ad-hoc boto3 mocking idiom (commit: Milestone A,
    task 0.1). The PyIceberg catalog stays mocked — only the S3 transport is
    real-ish.
    """
    # The proxy can't reach moto's in-process transport patch; the s3_mock
    # fixture monkeypatches _get_fos_client to return the moto client directly,
    # bypassing the proxy-routed factory for this test.
    # Use the shared fos_source fixture (bucket="test-bucket"), and rewrite
    # prefix so that the s3:// URIs we put on the mocked scan match.
    source = {**fos_source, "prefix": "logs"}

    # Reset the boto3 client cache so _get_fos_client builds a fresh client
    # bound to the moto session under this fixture.
    from backend.core import duckdb as _db

    monkeypatch.setattr(_db, "_fos_client_cache", {})

    # Seed the moto bucket with the "missing" parquet file. The "existing"
    # file is only on the local disk side, never on S3 (matches original test
    # semantics — we just don't want sync_data to try to download it).
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="logs/data/partition=1/missing.parquet",
        Body=b"dummy parquet content",
    )

    # Map the cache_dir to our temporary test path
    mock_cache_dir.return_value = str(tmp_path)

    # Mock the Iceberg catalog/table (PyIceberg setup is out of scope here)
    mock_catalog = MagicMock()
    mock_get_catalog.return_value = mock_catalog
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    mock_scan = MagicMock()
    mock_table.scan.return_value = mock_scan
    mock_scan.filter.return_value = mock_scan

    mock_file_missing = MagicMock()
    mock_file_missing.file.file_path = "s3://test-bucket/logs/data/partition=1/missing.parquet"
    mock_file_missing.file.record_count = 100

    mock_file_existing = MagicMock()
    mock_file_existing.file.file_path = "s3://test-bucket/logs/data/partition=2/existing.parquet"
    mock_file_existing.file.record_count = 200

    mock_scan.plan_files.return_value = [mock_file_missing, mock_file_existing]

    # Pre-create the "existing" file locally so sync_data skips it
    existing_local_path = tmp_path / "data" / "partition=2" / "existing.parquet"
    existing_local_path.parent.mkdir(parents=True, exist_ok=True)
    existing_local_path.touch()

    with patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True):
        res = iceberg.sync_data(source)

    # Should have downloaded exactly the one missing file
    assert res["files_downloaded"] == 1
    assert res["rows_downloaded"] == 100

    # The file should now exist locally with the bytes moto served
    missing_local_path = tmp_path / "data" / "partition=1" / "missing.parquet"
    assert missing_local_path.exists()
    assert missing_local_path.read_bytes() == b"dummy parquet content"


def test_get_iceberg_schema_no_custom_fields():
    """Returns base schema when no custom fields are configured."""
    base_schema = iceberg.get_iceberg_schema()
    assert iceberg.get_iceberg_schema(None) == base_schema
    assert iceberg.get_iceberg_schema({}) == base_schema
    assert iceberg.get_iceberg_schema({"custom_fields": []}) == base_schema


def test_get_iceberg_schema_varchar_custom_field():
    """VARCHAR custom fields are added after the base fields."""
    config = {"custom_fields": [{"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True}]}
    schema = iceberg.get_iceberg_schema(config)
    field_names = [f.name for f in schema.fields]
    assert "my_field" in field_names
    assert field_names.index("my_field") > field_names.index("timestamp")
    from pyiceberg.types import StringType

    field = schema.find_field("my_field")
    assert isinstance(field.field_type, StringType)


def test_get_iceberg_schema_numeric_custom_fields():
    """INTEGER, BIGINT, and DOUBLE custom fields map to correct Iceberg types (not STRING)."""
    from pyiceberg.types import DoubleType, IntegerType, LongType

    config = {
        "custom_fields": [
            {"name": "cf_int", "duckdb_type": "INTEGER", "enabled": True},
            {"name": "cf_bigint", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "cf_double", "duckdb_type": "DOUBLE", "enabled": True},
        ]
    }
    schema = iceberg.get_iceberg_schema(config)
    assert isinstance(schema.find_field("cf_int").field_type, IntegerType)
    assert isinstance(schema.find_field("cf_bigint").field_type, LongType)
    assert isinstance(schema.find_field("cf_double").field_type, DoubleType)


def test_get_iceberg_schema_disabled_field_excluded():
    """Disabled custom fields are not added to the schema."""
    config = {
        "custom_fields": [
            {"name": "active_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "inactive_field", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    schema = iceberg.get_iceberg_schema(config)
    field_names = {f.name for f in schema.fields}
    assert "active_field" in field_names
    assert "inactive_field" not in field_names


def test_get_iceberg_schema_sort_order():
    """Custom fields are sorted alphabetically for deterministic field ID assignment."""
    config = {
        "custom_fields": [
            {"name": "zzz_last", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "aaa_first", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    }
    schema = iceberg.get_iceberg_schema(config)
    field_names = [f.name for f in schema.fields]
    assert field_names.index("aaa_first") < field_names.index("zzz_last")


def test_get_iceberg_schema_field_ids_stable():
    """Field IDs for base fields are unchanged when custom fields are present."""
    config = {"custom_fields": [{"name": "extra", "duckdb_type": "VARCHAR", "enabled": True}]}
    base = iceberg.get_iceberg_schema()
    dynamic = iceberg.get_iceberg_schema(config)
    for base_field in base.fields:
        dyn_field = dynamic.find_field(base_field.name)
        assert dyn_field.field_id == base_field.field_id


def test_field_order_covers_ingest_storage_fields():
    """Drift guard: every storage-backed catalog field has a slot in _FIELD_ORDER.

    Regression for the Track-C bug where resp_header_content_encoding /
    oconnect_ms / cookie_session were added to LOG_FIELD_CATALOG but never
    appended to the hand-maintained _FIELD_ORDER positional list, so
    get_iceberg_schema silently omitted them, the DuckDB `logs` view had no such
    columns, ingest dropped them, and the field-gated insights stayed dark
    forever — with a green CI. This asserts the two lists can never diverge
    again in either direction.

    `get_catalog_field_ids(None)` IS the exact predicate ingest uses to select
    columns it will store (group not in METRICS/VIRTUAL/INTERNAL and vcl set),
    so keying the guard off it keeps the check in lock-step with the ingest
    pipeline rather than a hand-copied field list.
    """
    from backend.core._log_fields_data import LOG_FIELD_CATALOG
    from backend.core.iceberg import _core
    from backend.core.ingest import get_catalog_field_ids

    storage_ids = set(get_catalog_field_ids())  # None config → base storage fields only
    order_names = set(_core._FIELD_ORDER)

    # (1) Every field ingest stores must have a positional slot — else its
    # column never materializes in the schema / arrow view / parquet.
    missing = storage_ids - order_names
    assert not missing, (
        f"Storage-backed catalog fields missing from _FIELD_ORDER (won't materialize): {sorted(missing)}"
    )

    # (2) Every _FIELD_ORDER name must be either a storage field, a reserved
    # slot, or an INTERNAL catalog field (e.g. _source_file). Anything else is a
    # typo or a name that will never carry data.
    internal_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("group") == "INTERNAL"}
    allowed_non_storage = _core._RESERVED_FIELD_SLOTS | internal_ids
    extra = order_names - storage_ids - allowed_non_storage
    assert not extra, (
        f"_FIELD_ORDER has names that are neither storage fields, reserved slots, nor INTERNAL: {sorted(extra)}"
    )

    # (3) End-to-end: the emitted Arrow schema (what the DuckDB `logs` view is
    # built from) actually contains every storage field.
    schema_names = iceberg.get_schema_field_names()
    unmaterialized = storage_ids - schema_names
    assert not unmaterialized, f"Storage fields absent from the emitted Arrow/DuckDB schema: {sorted(unmaterialized)}"


def _reset_pointer_cache():
    with iceberg._pointer_cache_lock:
        iceberg._pointer_cache.clear()


def test_read_metadata_pointer_s3_fallback(s3_mock, fos_source):
    """When the pointer file is missing, fall back to listing the metadata
    dir and pick the alphabetically latest .metadata.json.

    Migrated from MagicMock(boto3) → real moto S3: NoSuchKey now flows
    through the actual botocore exception path instead of a stubbed
    ``Exception("Not found")``, so we exercise the real fallback trigger.
    """
    _reset_pointer_cache()
    # Process-unique prefix → a process-unique _pointer_cache key. The cache
    # is a module-global dict shared across the whole worker; a daemon
    # _write_table_summary_async thread leaked from another test (it scans
    # metadata under the common "test-bucket"/"logs"/("default","logs") key
    # long after that test ends) can otherwise repopulate this key between the
    # reset above and the read below, so the cache returns a stale value
    # instead of exercising the path under test. That cross-test race only
    # surfaces under the CPU contention of a parallel `make ci`, never a lone
    # `pytest`. A unique prefix makes the key uncollidable.
    prefix = "ptrcache-fallback"
    source = {**fos_source, "prefix": prefix}
    identifier = ("default", "logs")

    # Seed two metadata files; deliberately do NOT seed the pointer file
    # so the GetObject path raises NoSuchKey and the listing fallback runs.
    for key in (
        f"{prefix}/iceberg/default/logs/metadata/00000-old.metadata.json",
        f"{prefix}/iceberg/default/logs/metadata/00001-latest.metadata.json",
    ):
        s3_mock.put_object(Bucket="test-bucket", Key=key, Body=b"{}")

    loc = iceberg._read_metadata_pointer(source, identifier)
    assert loc == f"s3://test-bucket/{prefix}/iceberg/default/logs/metadata/00001-latest.metadata.json"


def test_read_metadata_pointer_caches_within_ttl(s3_mock, fos_source):
    """Pre-fix telemetry on 2026-05-20 showed cron_compact calling
    _read_metadata_pointer 4× within the same second (init_table, sync_data,
    get_table_info, get_snapshot_calendar), each costing ~200ms via the
    CDN. The in-process TTL cache must collapse those redundant calls to a
    single wire fetch — that's the entire point of the cache."""
    _reset_pointer_cache()
    # Unique prefix → uncollidable cache key; see the note in
    # test_read_metadata_pointer_s3_fallback for why the shared key flakes
    # under parallel `make ci` load.
    prefix = "ptrcache-ttlhit"
    source = {**fos_source, "prefix": prefix}
    identifier = ("default", "logs")
    pointer_value = f"s3://test-bucket/{prefix}/iceberg/default/logs/metadata/v1.metadata.json".encode()
    s3_mock.put_object(
        Bucket="test-bucket",
        Key=f"{prefix}/iceberg/default/logs/metadata_location.txt",
        Body=pointer_value,
    )

    with patch.object(s3_mock, "get_object", wraps=s3_mock.get_object) as spy:
        for _ in range(4):
            loc = iceberg._read_metadata_pointer(source, identifier)
            assert loc == pointer_value.decode()
        assert spy.call_count == 1, f"Expected 1 FOS call (rest served from cache), got {spy.call_count}"


def test_read_metadata_pointer_cache_expires_after_ttl(s3_mock, fos_source, monkeypatch):
    """Even without explicit invalidation, the cache must expire after
    _POINTER_CACHE_TTL_SEC so a long-running process eventually picks up
    pointer updates committed by other processes (Admin from a peer
    backend, manual ops via CDN purge). Tested with TTL=0 to avoid sleep."""
    _reset_pointer_cache()
    monkeypatch.setattr(iceberg, "_POINTER_CACHE_TTL_SEC", 0.0)
    # Unique prefix → uncollidable cache key (see test_read_metadata_pointer_s3_fallback).
    prefix = "ptrcache-ttlexpire"
    source = {**fos_source, "prefix": prefix}
    identifier = ("default", "logs")
    s3_mock.put_object(
        Bucket="test-bucket",
        Key=f"{prefix}/iceberg/default/logs/metadata_location.txt",
        Body=f"s3://test-bucket/{prefix}/iceberg/default/logs/metadata/v1.metadata.json".encode(),
    )

    with patch.object(s3_mock, "get_object", wraps=s3_mock.get_object) as spy:
        iceberg._read_metadata_pointer(source, identifier)
        iceberg._read_metadata_pointer(source, identifier)
        assert spy.call_count == 2, "TTL=0 must defeat the cache entirely"


def test_write_metadata_pointer_invalidates_cache(s3_mock, fos_source):
    """Same-process write must bust the cache so the next reader sees the
    fresh value instead of returning the stale pre-commit pointer. The
    cron_compact workflow reads the pointer before commit, writes the new
    pointer at commit time, then reads again as part of the post-commit
    refresh — without invalidation the post-commit read would return the
    stale pre-commit value for up to _POINTER_CACHE_TTL_SEC."""
    _reset_pointer_cache()
    # Unique prefix → uncollidable cache key (see test_read_metadata_pointer_s3_fallback).
    prefix = "ptrcache-invalidate"
    source = {**fos_source, "prefix": prefix}
    identifier = ("default", "logs")
    pointer_key = f"{prefix}/iceberg/default/logs/metadata_location.txt"
    s3_mock.put_object(
        Bucket="test-bucket",
        Key=pointer_key,
        Body=f"s3://test-bucket/{prefix}/iceberg/default/logs/metadata/v1.metadata.json".encode(),
    )

    # _write_metadata_pointer fires _write_table_summary_async in a daemon
    # thread; this test only cares about pointer write + cache invalidation,
    # not the async summary, so stub it to avoid spawning a thread that
    # outlives the test and scans metadata on the shared moto client.
    with (
        patch("backend.core.iceberg._core._write_table_summary_async"),
        patch.object(s3_mock, "get_object", wraps=s3_mock.get_object) as spy,
    ):
        loc1 = iceberg._read_metadata_pointer(source, identifier)
        assert loc1.endswith("v1.metadata.json")
        assert spy.call_count == 1

        # Write a new pointer — invalidates cache. _write_metadata_pointer
        # writes to S3 itself via the same moto client, so the next read
        # naturally sees v2 from real storage.
        iceberg._write_metadata_pointer(
            source, f"s3://test-bucket/{prefix}/iceberg/default/logs/metadata/v2.metadata.json"
        )

        loc2 = iceberg._read_metadata_pointer(source, identifier)
        assert loc2.endswith("v2.metadata.json")
        assert spy.call_count == 2, "Write must invalidate; otherwise reader would return stale v1 until TTL elapses"


# ── post-sync cache update: rate-limit retry ────────────────────────────────


def _make_rate_limited_catalog(fail_times: int, then_succeed: bool = True):
    """Build a fake catalog whose `load_table` raises a Fastly rate-limit
    error `fail_times` times in a row, then optionally returns a usable
    table mock. Models FOS's '[Errno 16] Reduce your request rate' that
    surfaces during heavy sync windows."""
    call_count = {"n": 0}
    mock_catalog = MagicMock()

    def _load_table(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] <= fail_times:
            raise OSError("[Errno 16] Reduce your request rate")
        if not then_succeed:
            raise OSError("[Errno 16] Reduce your request rate")
        # Successful path: minimal table with one plan_files entry
        mock_table = MagicMock()
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 1
        mock_table.current_snapshot.return_value = mock_snap
        mock_table.metadata_location = "s3://b/meta.json"
        mock_table.location.return_value = "s3://b/iceberg"
        mock_scan = MagicMock()
        mock_table.scan.return_value = mock_scan
        mock_file = MagicMock()
        mock_file.file.file_path = "s3://b/iceberg/data/x.parquet"
        mock_scan.plan_files.return_value = [mock_file]
        return mock_table

    mock_catalog.load_table.side_effect = _load_table
    return mock_catalog, call_count


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_post_sync_cache_update_retries_on_fos_rate_limit(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path, caplog
):
    """The post-download cache-update step retries when FOS returns
    '[Errno 16] Reduce your request rate'. Pinned because failing this
    step silently leaves _view_cache stale, which surfaces as
    "No files found that match the pattern" warnings on every subsequent
    sync-status poll until the next read happens to land outside the
    lock window. Backoff: 0.5s, 1s — three attempts total."""
    import time as _time

    source = {**fos_source, "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    # sync_data calls _get_catalog once for the initial fetch, then once
    # per retry inside the cache-update loop. First call returns a happy-
    # path catalog; every subsequent call returns the rate-limited one.
    fetch_table = MagicMock()
    fetch_scan = MagicMock()
    fetch_scan.filter.return_value = fetch_scan
    fetch_scan.plan_files.return_value = []  # nothing to download
    fetch_table.scan.return_value = fetch_scan
    fetch_catalog = MagicMock(load_table=MagicMock(return_value=fetch_table))

    rate_limited_catalog, call_count = _make_rate_limited_catalog(fail_times=2, then_succeed=True)

    catalog_call_count = {"n": 0}

    def _catalog_router(*_args, **_kwargs):
        catalog_call_count["n"] += 1
        return fetch_catalog if catalog_call_count["n"] == 1 else rate_limited_catalog

    mock_get_catalog.side_effect = _catalog_router

    # Patch sleep so retries don't actually delay the test
    with patch.object(_time, "sleep"):
        iceberg.sync_data(source)

    # The retry happened: post-sync catalog used 3 load_table calls (2 fail + 1 succeed)
    assert call_count["n"] == 3, f"expected 3 load_table calls (retry chain), got {call_count['n']}"
    # No 'Failed to update cache' warning should fire on success
    assert not any("Failed to update cache after sync" in r.message for r in caplog.records)


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_post_sync_cache_update_warns_after_exhausted_retries(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path, caplog
):
    """When rate-limit persists across all retries, the warning fires
    (and is the right warning, not a silent swallow). Pinned because a
    persistent throttle means we cannot self-heal — the user needs the
    log signal to investigate."""
    import logging
    import time as _time

    source = {**fos_source, "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    fetch_table = MagicMock()
    fetch_scan = MagicMock()
    fetch_scan.filter.return_value = fetch_scan
    fetch_scan.plan_files.return_value = []
    fetch_table.scan.return_value = fetch_scan
    fetch_catalog = MagicMock(load_table=MagicMock(return_value=fetch_table))

    rate_limited_catalog, call_count = _make_rate_limited_catalog(fail_times=99, then_succeed=False)

    catalog_call_count = {"n": 0}

    def _catalog_router(*_args, **_kwargs):
        catalog_call_count["n"] += 1
        return fetch_catalog if catalog_call_count["n"] == 1 else rate_limited_catalog

    mock_get_catalog.side_effect = _catalog_router

    with caplog.at_level(logging.WARNING, logger="backend.core.iceberg"), patch.object(_time, "sleep"):
        iceberg.sync_data(source)

    # All 3 retries attempted
    assert call_count["n"] == 3
    # Warning surfaced after retries exhausted
    assert any(
        "Failed to update cache after sync" in r.message and "Reduce your request rate" in r.message
        for r in caplog.records
    ), f"expected warning containing rate-limit detail, got: {[r.message for r in caplog.records]}"


# ── sync_data fast path: preserve previously-downloaded local files ─────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_fast_path_preserves_already_downloaded_local_files(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """Pin the 2026-05-21 data-loss bug.

    Once every cached snapshot entry has been converted from an s3:// URI
    to a local path by ``_reconcile_snapshot_cache_after_sync``, the fast
    path used to skip them entirely when building ``cloud_files``. That
    left ``active_paths`` empty, and the orphan-cleanup loop at the end of
    ``sync_data`` then walked the cache dir and ``os.remove``'d every
    parquet on disk — leaving only the next freshly-committed file to
    survive. Verified live: snapshot 01242 referenced 1379 data files,
    but only the latest 1 file actually existed on disk (1378 missing).

    This test seeds the steady-state cache: 3 previously-downloaded
    parquets on disk, all listed in the snapshot cache as local paths
    (no s3:// entries), with the cached metadata_location matching what
    the catalog reports. It then asserts every file survives the sync."""
    from backend.core import iceberg as _ice

    source = {**fos_source, "prefix": "logs", "name": "preserve-local-svc"}
    mock_cache_dir.return_value = str(tmp_path)

    data_dir = tmp_path / "data"
    files = [
        data_dir / "timestamp_hour=2026-05-15-23" / "00000-0-old.parquet",
        data_dir / "timestamp_hour=2026-05-16-00" / "00000-0-mid.parquet",
        data_dir / "timestamp_hour=2026-05-21-15" / "00000-0-new.parquet",
    ]
    for f in files:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"parquet-body")
    local_paths = [str(f.resolve()) for f in files]

    metadata_loc = "s3://test-bucket/logs/iceberg/default/logs/metadata/00099.metadata.json"
    iceberg_loc = "s3://test-bucket/logs/iceberg/default/logs"
    _ice._snapshot_files_cache[source["name"]] = (metadata_loc, 12345, iceberg_loc, list(local_paths))

    mock_table = MagicMock()
    mock_table.metadata_location = metadata_loc
    mock_table.location.return_value = iceberg_loc
    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table.scan.return_value = mock_scan
    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = catalog

    try:
        res = _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)

    survivors = [str(f.resolve()) for f in files if f.exists()]
    assert survivors == local_paths, (
        f"sync_data deleted previously-downloaded files! "
        f"expected all {len(local_paths)} to survive, only {len(survivors)} did. "
        f"missing: {set(local_paths) - set(survivors)}. "
        "Regression of the 2026-05-21 data-loss bug — see the comment in "
        "sync_data's fast-path block that wires local-path cache entries "
        "into cloud_files so active_paths includes them."
    )
    assert res.get("files_downloaded", 0) == 0, (
        f"no files should have been downloaded — already-local entries must skip the download phase. "
        f"got files_downloaded={res.get('files_downloaded')}"
    )


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_falls_back_to_slow_path_when_local_cache_entry_missing(
    mock_cache_dir, mock_get_catalog, _mock_refresh, s3_mock, fos_source, tmp_path
):
    """Pin the fast-path recovery behaviour for the 2026-05-21 data-loss bug.

    If the snapshot cache lists a local path whose file is missing on disk
    (e.g. because the buggy orphan-cleanup loop already deleted it before
    the fix was deployed), the fast path cannot satisfy the download — it
    has no s3:// URI to fetch from, only a local path. It must fall through
    to the slow plan_files() path so the file's actual file_path is
    rediscovered and queued for download.

    Without this guard, the fast path would populate cloud_files with the
    local path as a fake URI; _download_file would then send a garbage
    s3-key derived from the local path to FOS and the recovery would
    silently fail. This test pins the fallback by:
      1. seeding the cache with two local paths, only one of which exists
      2. having the catalog's plan_files() report both as fresh s3:// URIs
      3. asserting plan_files was consulted (slow path) and the missing
         file became part of cloud_files via the s3:// URI scan
    """
    from backend.core import iceberg as _ice

    source = {**fos_source, "prefix": "logs", "name": "recover-missing-svc"}
    mock_cache_dir.return_value = str(tmp_path)
    data_dir = tmp_path / "data"

    on_disk = data_dir / "timestamp_hour=2026-05-21-15" / "00000-0-present.parquet"
    on_disk.parent.mkdir(parents=True, exist_ok=True)
    on_disk.write_bytes(b"parquet-body")
    present_local = str(on_disk.resolve())

    missing_local = str((data_dir / "timestamp_hour=2026-05-15-23" / "00000-0-deleted.parquet").resolve())
    assert not os.path.exists(missing_local), "missing fixture should not exist on disk"

    metadata_loc = "s3://test-bucket/logs/iceberg/default/logs/metadata/00099.metadata.json"
    iceberg_loc = "s3://test-bucket/logs/iceberg/default/logs"
    _ice._snapshot_files_cache[source["name"]] = (
        metadata_loc,
        12345,
        iceberg_loc,
        [present_local, missing_local],
    )

    present_uri = f"{iceberg_loc}/data/timestamp_hour=2026-05-21-15/00000-0-present.parquet"
    missing_uri = f"{iceberg_loc}/data/timestamp_hour=2026-05-15-23/00000-0-deleted.parquet"

    # Seed the moto bucket so the slow path's download phase can succeed —
    # this test cares about the fact that plan_files() was consulted (slow
    # path taken), but having the download work end-to-end avoids needing
    # to inspect intermediate state via spies.
    for uri in (present_uri, missing_uri):
        key = uri.replace("s3://test-bucket/", "")
        s3_mock.put_object(Bucket="test-bucket", Key=key, Body=b"parquet-body")

    plan_calls = {"n": 0}

    def _plan_files():
        plan_calls["n"] += 1
        files = []
        for uri in (present_uri, missing_uri):
            scan_file = MagicMock()
            scan_file.file.file_path = uri
            scan_file.file.record_count = 1
            files.append(scan_file)
        return files

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.side_effect = _plan_files
    mock_table = MagicMock()
    mock_table.metadata_location = metadata_loc
    mock_table.location.return_value = iceberg_loc
    mock_table.scan.return_value = mock_scan
    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = catalog

    try:
        _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)

    assert plan_calls["n"] >= 1, (
        "fast path silently accepted a missing local-cache entry — slow plan_files() path "
        "must run so the s3:// URI is rediscovered and the file can be redownloaded. "
        "Otherwise _download_file would send the local path as a fake s3 key to FOS."
    )
    assert os.path.exists(missing_local), (
        f"slow path ran but did not actually download the missing file to {missing_local}. "
        "Recovery from the 2026-05-21 data-loss bug requires the fallback to fetch via s3:// URI."
    )


# ── sync_data orphan cleanup: skip local-compaction output dirs ─────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_orphan_cleanup_preserves_local_compaction_dirs(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """Pin the orphan-cleanup-vs-local-compaction bug.

    Local-compaction writes merged rollups into ``<cache>/data/daily/`` and
    ``<cache>/data/weekly/``. Those files are LOCAL-ONLY — they are not part
    of the iceberg snapshot, so they never appear in ``active_paths``. The
    pre-fix orphan-cleanup loop walked the whole cache_dir and deleted any
    .parquet not in ``active_paths``, so on every sync the compaction
    outputs disappeared — which in production dropped the view from 1.65M
    rows down to ~302K within minutes of a restart. The fix limits the
    orphan walk to ``timestamp_hour=*`` partition dirs only.
    """
    from backend.core import iceberg as _ice

    source = {**fos_source, "prefix": "logs", "name": "preserve-compaction-svc"}
    mock_cache_dir.return_value = str(tmp_path)

    data_dir = tmp_path / "data"

    # Active iceberg-pointed partition file (should survive)
    active = data_dir / "timestamp_hour=2026-05-21-15" / "00000-0-active.parquet"
    # Orphaned iceberg-pointed partition file (should be deleted)
    orphan = data_dir / "timestamp_hour=2026-05-15-23" / "00000-0-orphan.parquet"
    # Local-compaction outputs (should survive — NOT in active_paths)
    daily = data_dir / "daily" / "daily_2026-05-15_abc123.parquet"
    weekly = data_dir / "weekly" / "weekly_2026-05-04_def456.parquet"
    # Hourly-tier compaction output — written INSIDE a timestamp_hour= dir.
    # The 2026-06-01 production loss was triggered by orphan-cleanup deleting
    # these `compacted_*` files, after which the registry blocked re-download
    # of the source files (silent ~31k missing rows in the view).
    hourly_merged = data_dir / "timestamp_hour=2026-05-21-15" / "compacted_abc123def456.parquet"
    for f in (active, orphan, daily, weekly, hourly_merged):
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"parquet-body")

    metadata_loc = "s3://test-bucket/logs/iceberg/default/logs/metadata/00099.metadata.json"
    iceberg_loc = "s3://test-bucket/logs/iceberg/default/logs"
    active_uri = f"{iceberg_loc}/data/timestamp_hour=2026-05-21-15/00000-0-active.parquet"

    # Seed snapshot cache with only the active file as local — fast path.
    _ice._snapshot_files_cache[source["name"]] = (
        metadata_loc,
        12345,
        iceberg_loc,
        [str(active.resolve())],
    )

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table = MagicMock()
    mock_table.metadata_location = metadata_loc
    mock_table.location.return_value = iceberg_loc
    mock_table.scan.return_value = mock_scan
    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = catalog

    try:
        _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)
        _ = active_uri  # silence unused

    assert daily.exists(), (
        "sync_data orphan-cleanup deleted a local-compaction daily rollup! "
        "The walk must skip data/daily/ and data/weekly/ since those files "
        "are local-only outputs (not part of the iceberg snapshot)."
    )
    assert weekly.exists(), (
        "sync_data orphan-cleanup deleted a local-compaction weekly rollup. "
        "Same regression as daily/ — must skip non-partition subdirs."
    )
    assert active.exists(), "active iceberg-pointed file should survive"
    assert hourly_merged.exists(), (
        "sync_data orphan-cleanup deleted an hourly-tier compacted_*.parquet "
        "file inside a timestamp_hour=* dir. These are local-only rollups "
        "(compaction writes them via uuid4 hash); deleting them silently "
        "drops the rows from the view because the registry then blocks the "
        "iceberg source files from being re-downloaded."
    )
    assert not orphan.exists(), (
        "orphaned timestamp_hour file should still be deleted — the fix only "
        "narrows WHICH dirs are walked and which file patterns are skipped, "
        "it does not disable cleanup."
    )


def test_sync_data_prevents_path_traversal(fos_source, tmp_path):
    """Verify that sync_data prevents path traversal if data file URIs contain traversal sequences."""
    from backend.core import iceberg as _ice

    source = {**fos_source, "name": "traversal-test"}

    # Let's mock a scan containing a file with traversal path
    malicious_uri = "s3://test-bucket/logs/iceberg/data/../../../../tmp/evil.parquet"
    mock_file = MagicMock()
    mock_file.file_path = malicious_uri
    mock_file.record_count = 100

    mock_plan_file = MagicMock()
    mock_plan_file.file = mock_file

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_plan_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    catalog = MagicMock()
    catalog.load_table.return_value = mock_table

    fake_s3 = MagicMock()

    # Mock the catalog and files list
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.iceberg._get_catalog", return_value=catalog),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        result = _ice.sync_data(source)

    # Since the file is skipped, result should have 0 files downloaded and files_skipped as 0
    assert result.get("files_downloaded") == 0
    assert result.get("files_skipped") == 0
    assert not fake_s3.download_file.called


# ── _update_iceberg_view_locked: do not downgrade non-empty view ─────────────


def test_update_iceberg_view_locked_does_not_downgrade_existing_non_empty_view(monkeypatch):
    """When catalog load returns nothing (transient FOS failure) AND the
    buffer is empty AND no local files are resolved, the function used to
    unconditionally CREATE the "WHERE false" empty view — which then stuck
    in _view_cache until a writer cron rebuilt successfully. After the fix,
    if a non-empty view was previously cached, we skip the downgrade and
    let the existing view keep working until the next successful rebuild.

    Pinned because the production "Total Logs: 0" symptom (1.27M rows in
    metadata but 0 from view) was caused by this downgrade path firing
    during a lock-contended sync window."""
    from backend.core import iceberg as _ice

    # Seed a prior non-empty view in the cache (a previous successful rebuild)
    source_key = "no-downgrade-svc"
    prior_sql = "SELECT * FROM read_parquet('cache/no-downgrade-svc/data/**/*.parquet')"
    _ice._view_cache[source_key] = ("old-metadata-loc", frozenset(), tuple(), prior_sql, 1.0, False)

    # Make sure no snapshot cache (forces catalog fetch path)
    _ice._snapshot_files_cache.pop(source_key, None)

    # Stub the catalog to "fail" by returning None for table — simulates
    # transient FOS rate limit / network blip
    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = RuntimeError("transient FOS failure")

    source = {
        "name": source_key,
        "bucket": "b",
        "prefix": "p",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }

    # Stub everything that would otherwise hit the network or filesystem
    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])  # empty buffer
    monkeypatch.setattr(_ice, "_read_metadata_pointer", lambda src, ident: None)
    monkeypatch.setattr(_ice, "_load_persistent_cache", lambda src: None)
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)

    # Patch the inner sqlite check (catalog DB lookup) to no-op
    monkeypatch.setattr("os.path.exists", lambda p: False)

    fake_con = MagicMock()
    # Don't let con.execute("CREATE OR REPLACE VIEW ...") run — assert it wasn't called
    fake_con.execute = MagicMock()

    _ice._update_iceberg_view_locked(fake_con, source)

    # The empty-view CREATE must NOT have been executed
    create_view_calls = [
        c for c in fake_con.execute.call_args_list if c.args and "CREATE OR REPLACE VIEW" in str(c.args[0])
    ]
    assert not create_view_calls, (
        f"empty-view downgrade fired despite cached non-empty view; calls: {create_view_calls}"
    )

    # The cached view must still be the original (not wiped to empty)
    assert _ice._view_cache.get(source_key, (None,) * 6)[3] == prior_sql

    # Cleanup
    _ice._view_cache.pop(source_key, None)


def test_fast_path_view_rebind_works_on_readonly_connection(tmp_path, monkeypatch):
    """End-to-end reproduction of the "No data available" dashboard bug.

    Production scenario: a writer's update_iceberg_view created (and
    cached) the right view SQL. Later the persistent view in the DuckDB
    file got downgraded to the WHERE-false empty fallback — but the
    cache still holds the correct SQL. When a dashboard RO connection
    opens and update_iceberg_view runs its fast-path branch, it tries
    to re-execute the cached `CREATE OR REPLACE VIEW` statement. On RO
    that statement fails (Cannot execute statement of type CREATE on
    database … in read-only mode). The catch swallows it. The RO
    session then queries the still-empty persistent view → 0 rows →
    "No data available" in the UI.

    The fix: in the fast path, detect RO and rewrite the cached
    statement to `CREATE OR REPLACE TEMP VIEW` before executing. The
    TEMP view shadows the empty persistent view within this session,
    so dashboard SELECTs return real data.

    This test sets up the EXACT pathological state and asserts the
    dashboard query returns the seeded rows."""
    import duckdb as _duckdb

    from backend.core import iceberg as _ice

    # Writer creates seed data AND a persistent EMPTY view (the
    # symptomatic state — the dashboard sees this view).
    db_path = str(tmp_path / "fast_path.duckdb")
    w = _duckdb.connect(db_path)
    w.execute("CREATE TABLE seed_data(x INT, y VARCHAR)")
    w.execute("INSERT INTO seed_data VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    # The bug-state persistent view: EMPTY fallback
    w.execute("CREATE OR REPLACE VIEW logs_fast_path_svc AS SELECT NULL::INT AS x, NULL::VARCHAR AS y WHERE false")
    w.close()

    # Cache holds the GOOD SQL (writer cached it earlier, before something
    # downgraded the persistent view). The cache key includes the schema
    # field set — we have to compute it the same way the function does
    # so the fast-path comparison matches.
    source_key = "fast-path-svc"
    cached_view_sql = "CREATE OR REPLACE VIEW logs_fast_path_svc AS SELECT * FROM seed_data"
    metadata_loc = "fake-md-loc"
    buf_set = frozenset()
    schema_fields = tuple(sorted({f.name for f in _ice.get_arrow_schema(None)}))
    # 7th tuple slot is the source_variant_fp (finding 002, added 2026-06-15):
    # ``(access_level, (tr.start, tr.end), cron_enabled)``. The test source
    # below has no time_range / access_level and cron is enabled by default
    # so the fingerprint is the empty default.
    variant_fp = ("", (), True)
    _ice._view_cache[source_key] = (metadata_loc, buf_set, schema_fields, cached_view_sql, 1.0, False, variant_fp)

    ro = _duckdb.connect(db_path, read_only=True)
    try:
        # Sanity: persistent view starts empty (the bug state)
        assert ro.execute("SELECT COUNT(*) FROM logs_fast_path_svc").fetchone()[0] == 0

        source = {
            "name": source_key,
            "bucket": "b",
            "prefix": "p",
            "endpoint": "ep",
            "access_key_id": "k",
            "secret_access_key": "s",
            "region": "us-east-1",
        }

        # Stub the metadata_loc lookup so fast-path matches
        import sqlite3 as _sqlite3

        class _FakeCatRow:
            def execute(self, *_a, **_k):
                class _R:
                    def fetchone(self_inner):
                        return (metadata_loc,)

                return _R()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

        monkeypatch.setattr(_sqlite3, "connect", lambda *a, **k: _FakeCatRow())
        monkeypatch.setattr(
            "backend.core.iceberg.os.path.exists",
            lambda p: "iceberg_catalog.db" in str(p),
        )
        monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
        monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)

        _ice._update_iceberg_view_locked(ro, source)

        # CRITICAL: dashboard SELECT must now return real data, not the
        # empty persistent view's 0 rows. TEMP view should shadow.
        rows = ro.execute("SELECT COUNT(*) FROM logs_fast_path_svc").fetchone()
        assert rows[0] == 3, (
            f"fast-path failed to re-bind view on RO — dashboard would still see "
            f"the empty persistent view. Got: {rows[0]}, expected 3"
        )
    finally:
        ro.close()
        _ice._view_cache.pop(source_key, None)


def test_duckdb_databases_readonly_detection_against_real_connection(tmp_path):
    """The RO detection inside _update_iceberg_view_locked uses
    `SELECT readonly FROM duckdb_databases()`. The previous detection
    (`PRAGMA database_list` checking row[2] == 'read-only') was wrong
    because row[2] is the FILE PATH, not a readonly flag — so the
    TEMP VIEW branch never fired and every RO dashboard query produced
    "ERROR Failed to create view … Cannot execute statement of type
    CREATE on database … attached in read-only mode!" — leaving the
    dashboard with "No data available" despite ingested rows.

    This pins the exact query the code uses against a real DuckDB
    connection so a future schema change to `duckdb_databases()`
    surfaces in the test rather than in prod stderr."""
    import duckdb as _duckdb

    db_path = str(tmp_path / "ro_detect.duckdb")
    w = _duckdb.connect(db_path)
    w.execute("CREATE TABLE t(x INT)")
    w.close()

    # Writer connection should report readonly=False
    rw = _duckdb.connect(db_path)
    rw_row = rw.execute(
        "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
    ).fetchone()
    rw.close()
    assert rw_row is not None and rw_row[0] is False

    # Read-only connection should report readonly=True
    ro = _duckdb.connect(db_path, read_only=True)
    ro_row = ro.execute(
        "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
    ).fetchone()
    assert ro_row is not None and ro_row[0] is True

    # And TEMP VIEW must actually work on the RO connection (the fallback path)
    ro.execute("CREATE OR REPLACE TEMP VIEW vtmp AS SELECT * FROM t")
    rows = ro.execute("SELECT COUNT(*) FROM vtmp").fetchone()
    assert rows[0] == 0  # table is empty but the view exists
    ro.close()


def test_update_iceberg_view_locked_does_not_downgrade_when_ingested_files_exist(monkeypatch):
    """The existing guard at iceberg.py:2068-2077 only checks _view_cache.
    On process restart (or after clear_source_caches), _view_cache is empty
    even when the service has millions of rows in its ingest sqlite metadata.
    A transient catalog-load failure on the first poll then writes
    "WHERE false" — and the persistent view is poisoned for the rest of the
    process lifetime (until a writer cron rebuilds successfully).

    Production scenario where this stuck the dashboard at 0 rows for >30
    minutes after a Phase 4 telemetry-proxy rollout: ingest sqlite showed
    1.6M rows, the persistent view was "WHERE false", and NGWAF queries
    blew up with "Binder Error: Referenced column 'dt' not found in FROM
    clause" because the empty fallback projection skips the strftime
    columns.

    Fix: refuse to write the empty fallback when ingested_files metadata
    shows ANY non-zero row count. We have data — the cloud is just blind
    this poll. Leave the prior view alone (which, in the worst case, is
    itself a "WHERE false" persisted from an earlier incident; this test
    locks in that we don't COMPOUND the problem by writing the empty SQL
    AGAIN and re-priming _view_cache with the empty entry)."""
    from backend.core import iceberg as _ice

    source_key = "stuck-svc-with-ingested-files"
    _ice._view_cache.pop(source_key, None)
    _ice._snapshot_files_cache.pop(source_key, None)

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = RuntimeError("transient FOS blip")

    source = {
        "name": source_key,
        "bucket": "b",
        "prefix": "p",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }

    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
    monkeypatch.setattr(_ice, "_read_metadata_pointer", lambda src, ident: None)
    monkeypatch.setattr(_ice, "_load_persistent_cache", lambda src: None)
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)
    monkeypatch.setattr("os.path.exists", lambda p: False)

    # Ingest metadata says we have 1.6M rows across 1100 files — the
    # exact production state when the view got stuck.
    from backend.core import metadata as _meta

    monkeypatch.setattr(
        _meta,
        "get_ingested_files_status_summary",
        lambda svc: {
            "file_count": 1100,
            "total_rows": 1100 * 1500,
            "total_bytes": 1100 * 1024,
            "count_with_bytes": 1100,
            "last_ingested": "2026-05-19T22:00:00Z",
            "latest_file_name": "raw/file_1099.gz",
        },
    )

    fake_con = MagicMock()
    _ice._update_iceberg_view_locked(fake_con, source)

    # The empty-view CREATE must NOT have been executed — we have data,
    # don't lie about it.
    create_view_calls = [
        c for c in fake_con.execute.call_args_list if c.args and "CREATE OR REPLACE VIEW" in str(c.args[0])
    ]
    empty_view_calls = [c for c in create_view_calls if "WHERE false" in str(c.args[0])]
    assert not empty_view_calls, (
        f"empty-view downgrade fired despite ingest metadata showing {1100} files / 1.65M rows; "
        f"calls: {empty_view_calls}"
    )

    # And _view_cache must not have been re-primed with the empty SQL —
    # otherwise the next poll's "prior_was_empty" guard lets it happen again.
    cached_sql = _ice._view_cache.get(source_key, (None,) * 6)[3]
    assert cached_sql is None or "WHERE false" not in cached_sql, (
        f"view cache was re-primed with empty SQL: {cached_sql!r}"
    )

    _ice._view_cache.pop(source_key, None)


def test_update_iceberg_view_waits_longer_when_cache_empty_and_no_persistent_view(monkeypatch):
    """Lock-contention regression: on a fresh process while ingest holds the
    per-source lock, the dashboard's RO connection calls
    ``update_iceberg_view``, the 5s acquire times out, the cache is empty,
    and the function returns WITHOUT creating a view. The next query then
    fails with ``Catalog Error: Table with name logs_xxx does not exist``
    — and stays broken until sync finishes its commit and releases the
    lock (typically 30–60s).

    Production hit: post-restart during an active sync, the dashboard,
    security top-bots, and query page all returned "Table does not exist"
    in lockstep. Recovery only happened once sync's own
    ``update_iceberg_view`` (scheduler.py:701) ran after commit.

    Fix: when the cache is empty AND no persistent view exists on the
    connection, retry the acquire with a much longer timeout (60s). Pay
    the wait once on first poll; subsequent polls hit the cache fast path.
    """
    import threading

    from backend.core import iceberg as _ice

    source_key = "lock-contention-fresh-svc"
    _ice._view_cache.pop(source_key, None)
    _ice._service_locks.pop(source_key, None)

    source = {"name": source_key}

    locked_calls: list = []

    def fake_locked(con, src):
        locked_calls.append((con, src))

    monkeypatch.setattr(_ice, "_update_iceberg_view_locked", fake_locked)

    lock = _ice._get_service_lock(source_key)

    fake_con = MagicMock()
    # No persistent view on this connection — the information_schema lookup
    # the fix performs should report nothing.
    fake_con.execute.return_value.fetchone.return_value = None

    # Hold the per-source RLock from a DIFFERENT thread (RLocks allow
    # reentrant acquisition by the SAME thread, which would defeat the
    # contention scenario we're modelling).
    holder_acquired = threading.Event()
    holder_release = threading.Event()

    def hold_lock():
        lock.acquire()
        holder_acquired.set()
        # Hold for 0.1s — longer than the test's lock_timeout (0.02s) so the
        # first acquire times out, well within the fix's extended 60s wait
        # so the second acquire succeeds.
        holder_release.wait(timeout=0.1)
        lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert holder_acquired.wait(timeout=2.0)
    try:
        _ice.update_iceberg_view(fake_con, source, lock_timeout=0.02)
    finally:
        holder_release.set()
        holder.join(timeout=5.0)

    assert len(locked_calls) == 1, (
        "expected update_iceberg_view to wait for the lock and run the locked "
        f"rebuild once cache was empty and no persistent view existed; "
        f"got {len(locked_calls)} locked calls"
    )

    _ice._view_cache.pop(source_key, None)
    _ice._service_locks.pop(source_key, None)


def test_update_iceberg_view_skips_rebuild_when_persistent_view_exists(monkeypatch):
    """Companion to the wait-longer test. If a persistent view already
    exists on this connection (from a prior writer), and we couldn't
    grab the lock, just leave it alone — the existing view is good
    enough for this poll. Avoids the long-block path entirely in the
    common case (restart with healthy persisted view + sync busy)."""
    import threading

    from backend.core import iceberg as _ice

    source_key = "lock-contention-persistent-view-svc"
    _ice._view_cache.pop(source_key, None)
    _ice._service_locks.pop(source_key, None)

    source = {"name": source_key}

    locked_calls: list = []

    def fake_locked(con, src):
        locked_calls.append((con, src))

    monkeypatch.setattr(_ice, "_update_iceberg_view_locked", fake_locked)

    lock = _ice._get_service_lock(source_key)

    fake_con = MagicMock()
    # Persistent view IS present — information_schema returns a row.
    fake_con.execute.return_value.fetchone.return_value = (1,)

    holder_acquired = threading.Event()
    holder_release = threading.Event()

    def hold_lock():
        lock.acquire()
        holder_acquired.set()
        holder_release.wait(timeout=5.0)
        lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert holder_acquired.wait(timeout=2.0)
    try:
        _ice.update_iceberg_view(fake_con, source, lock_timeout=0.2)
    finally:
        holder_release.set()
        holder.join(timeout=5.0)

    assert locked_calls == [], (
        "with a persistent view present, update_iceberg_view should not "
        "block waiting for the lock to do a rebuild — leave the existing "
        f"view alone; got {len(locked_calls)} locked calls"
    )

    _ice._view_cache.pop(source_key, None)
    _ice._service_locks.pop(source_key, None)


def test_update_iceberg_view_locked_creates_empty_view_for_fresh_service(monkeypatch):
    """Counterpart to the no-downgrade test: a service with NO prior cache
    entry (genuinely fresh, never seen) DOES get the empty view as
    expected. This is correct behavior for the case-(a) "no data yet" path."""
    from backend.core import iceberg as _ice

    source_key = "fresh-svc-no-prior-cache"
    _ice._view_cache.pop(source_key, None)
    _ice._snapshot_files_cache.pop(source_key, None)

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = RuntimeError("no table yet")

    source = {
        "name": source_key,
        "bucket": "b",
        "prefix": "p",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }

    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
    monkeypatch.setattr(_ice, "_read_metadata_pointer", lambda src, ident: None)
    monkeypatch.setattr(_ice, "_load_persistent_cache", lambda src: None)
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)
    monkeypatch.setattr("os.path.exists", lambda p: False)

    # Fresh service: no ingested files in sqlite metadata either.
    from backend.core import metadata as _meta

    monkeypatch.setattr(
        _meta,
        "get_ingested_files_status_summary",
        lambda svc: {
            "file_count": 0,
            "total_rows": 0,
            "total_bytes": 0,
            "count_with_bytes": 0,
            "last_ingested": None,
            "latest_file_name": None,
        },
    )

    fake_con = MagicMock()
    _ice._update_iceberg_view_locked(fake_con, source)

    create_view_calls = [
        c for c in fake_con.execute.call_args_list if c.args and "CREATE OR REPLACE VIEW" in str(c.args[0])
    ]
    assert create_view_calls, "fresh service should get the empty view created"
    assert "WHERE false" in str(create_view_calls[0].args[0])

    _ice._view_cache.pop(source_key, None)


# ── DO NOT load from cloud when local files exist (cost regression guard) ─


def test_fast_path_force_rebuild_when_cached_sql_is_s3_based_with_local_files(tmp_path, monkeypatch):
    """If the cached view SQL contains `iceberg_scan(` (i.e. was built
    when local files weren't synced yet) BUT the local data dir now
    has parquet files, the fast path MUST force a slow-path rebuild
    instead of re-using the S3-based SQL.

    Pinned because the production symptom was thousands of S3 Class B
    reads on every sync-status poll: the cached SQL was iceberg_scan,
    nothing was invalidating it, and every poll routed through S3.
    The fix invalidates the fast-path cache when local files exist
    so the next call rebuilds against local."""
    import duckdb as _duckdb

    from backend.core import iceberg as _ice

    db_path = str(tmp_path / "force_rebuild.duckdb")
    cache_dir = tmp_path / "cache"
    (cache_dir / "data" / "timestamp_hour=2026-05-19-00").mkdir(parents=True)
    # Drop a real parquet with the full iceberg schema so CREATE VIEW
    # succeeds and we can inspect the cached SQL.
    import pyarrow.parquet as pq

    arrow_schema = _ice.get_arrow_schema(None)
    pq.write_table(arrow_schema.empty_table(), str(cache_dir / "data" / "timestamp_hour=2026-05-19-00" / "x.parquet"))

    # Seed: S3-BASED cached SQL (the bug state)
    source_key = "cost-leak-svc"
    s3_view_sql = (
        "CREATE OR REPLACE VIEW logs_cost_leak_svc AS "
        "SELECT * FROM iceberg_scan('s3://bucket/iceberg/x', allow_moved_paths=true)"
    )
    metadata_loc = "fake-md-loc"
    buf_set = frozenset()
    schema_fields = tuple(sorted({f.name for f in _ice.get_arrow_schema(None)}))
    # 7-tuple: trailing slot is source_variant_fp (finding 002, 2026-06-15);
    # default empty fingerprint matches the test source below.
    _ice._view_cache[source_key] = (metadata_loc, buf_set, schema_fields, s3_view_sql, 1.0, False, ("", (), True))
    # Snapshot cache also needs entries so the slow-path rebuild has something
    # to build the union from. In production, this is the post-sync_data state.
    _ice._snapshot_files_cache[source_key] = (
        metadata_loc,
        1,
        "s3://bucket/iceberg/x",
        [str(cache_dir / "data" / "timestamp_hour=2026-05-19-00" / "x.parquet")],
    )

    # Track whether the function went down the slow-path rebuild branch.
    # If fast-path is taken (the bug), we'd see the cached SQL executed
    # against the connection (and it would fail because iceberg_scan can't
    # contact the fake S3). If slow-path is taken (the fix), we don't get
    # that execution attempt.
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(cache_dir))
    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)
    monkeypatch.setattr(_ice, "_load_persistent_cache", lambda src: None)

    # Make metadata_loc lookup succeed (matches the snapshot cache)
    import sqlite3 as _sqlite3

    class _FakeCat:
        def execute(self, *_a, **_k):
            class _R:
                def fetchone(_self):
                    return (metadata_loc,)

            return _R()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(_sqlite3, "connect", lambda *a, **k: _FakeCat())
    monkeypatch.setattr("backend.core.iceberg.os.path.exists", lambda p: "iceberg_catalog.db" in str(p))

    source = {"name": source_key, "bucket": "bucket", "prefix": "p"}

    con = _duckdb.connect(db_path)
    try:
        _ice._update_iceberg_view_locked(con, source)
    finally:
        con.close()

    # The new cached SQL must NOT contain iceberg_scan — the rebuild took
    # the local-glob path (the safety net's `if not local_paths` block
    # synthesizes a local_paths from the disk glob).
    new_cached_sql = _ice._view_cache.get(source_key, (None,) * 6)[3] or ""
    assert "iceberg_scan(" not in new_cached_sql, (
        f"fast-path failed to force rebuild — cached SQL is still S3-based: {new_cached_sql[:200]}"
    )
    assert "read_parquet(" in new_cached_sql, f"rebuild should use local read_parquet but got: {new_cached_sql[:200]}"

    _ice._view_cache.pop(source_key, None)
    _ice._snapshot_files_cache.pop(source_key, None)


def test_union_builder_uses_local_glob_when_local_iceberg_files_empty_but_disk_has_parquets(tmp_path, monkeypatch):
    """If plan_files runs at a moment when sync_data hasn't finished
    populating local_iceberg_files yet, but the data_dir HAS parquet
    files on disk (from an earlier sync), the union builder MUST still
    use the local read_parquet glob — not fall through to iceberg_scan.

    Defense-in-depth against silent cost regression: even if the fast-
    path force-rebuild misses a case, this glob safety net catches it."""
    import duckdb as _duckdb

    from backend.core import iceberg as _ice

    cache_dir = tmp_path / "cache"
    (cache_dir / "data" / "timestamp_hour=2026-05-19-01").mkdir(parents=True)
    import pyarrow.parquet as pq

    arrow_schema = _ice.get_arrow_schema(None)
    pq.write_table(arrow_schema.empty_table(), str(cache_dir / "data" / "timestamp_hour=2026-05-19-01" / "y.parquet"))

    source_key = "glob-safety-svc"
    # Seed snapshot cache with ONLY S3 URIs (the race condition state)
    _ice._snapshot_files_cache[source_key] = (
        "md-loc",
        1,
        "s3://bucket/iceberg/y",
        ["s3://bucket/iceberg/y/data/y.parquet"],  # No local paths
    )
    _ice._view_cache.pop(source_key, None)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(cache_dir))
    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)
    monkeypatch.setattr("backend.core.iceberg.os.path.exists", lambda p: "iceberg_catalog.db" in str(p))

    # Stub the catalog DB lookup so metadata_loc matches and fast-cached-files path is used
    import sqlite3 as _sqlite3

    class _FakeCat:
        def execute(self, *_a, **_k):
            class _R:
                def fetchone(_self):
                    return ("md-loc",)

            return _R()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(_sqlite3, "connect", lambda *a, **k: _FakeCat())

    source = {"name": source_key, "bucket": "bucket", "prefix": "p"}

    con = _duckdb.connect(":memory:")
    try:
        _ice._update_iceberg_view_locked(con, source)
    finally:
        con.close()

    cached_sql = _ice._view_cache.get(source_key, (None,) * 6)[3] or ""
    assert "iceberg_scan(" not in cached_sql, (
        f"glob safety net failed — union still uses iceberg_scan: {cached_sql[:200]}"
    )
    assert "read_parquet(" in cached_sql, (
        f"union should fall back to local read_parquet via glob; got: {cached_sql[:200]}"
    )

    _ice._view_cache.pop(source_key, None)
    _ice._snapshot_files_cache.pop(source_key, None)


# ── Phase 6: immutable-manifest bytes cache (avoid PyIceberg re-reading
# the same .avro / .metadata.json files on every table.scan()) ───────────────


def test_immutable_path_classification():
    """Only .avro and .metadata.json are immutable. Data parquets must NOT
    be cached — they're large and read-once anyway."""
    from backend.core import iceberg as _ice

    assert _ice._is_immutable_path("bucket/iceberg/default/logs/metadata/snap-abc.avro")
    assert _ice._is_immutable_path("bucket/iceberg/default/logs/metadata/00993-x.metadata.json")
    assert _ice._is_immutable_path("bucket/x/m0.avro")
    assert not _ice._is_immutable_path("bucket/iceberg/default/logs/data/part-0.parquet")
    assert not _ice._is_immutable_path("bucket/raw/2026-05-20/00/something.log.gz")
    assert not _ice._is_immutable_path("bucket/foo")


def test_manifest_cache_evicts_when_over_budget():
    """LRU eviction must keep total bytes under the budget. If we let it
    grow unbounded the worker process OOMs after a week of new manifests."""
    from backend.core import iceberg as _ice

    saved_max = _ice._MANIFEST_CACHE_MAX_BYTES
    _ice._MANIFEST_CACHE_MAX_BYTES = 100
    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    try:
        for i in range(20):
            _ice._cache_put(f"p{i}.avro", b"x" * 10)
        assert _ice._manifest_cache_size <= 100
        assert len(_ice._manifest_bytes_cache) == 10
        # Most-recently-put entries survived.
        assert "p19.avro" in _ice._manifest_bytes_cache
        assert "p0.avro" not in _ice._manifest_bytes_cache
    finally:
        _ice._MANIFEST_CACHE_MAX_BYTES = saved_max
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0


def test_manifest_cache_skips_oversize_single_file():
    """A single .avro larger than the whole budget must not be cached —
    otherwise it would evict every other entry on every put and never hit."""
    from backend.core import iceberg as _ice

    saved_max = _ice._MANIFEST_CACHE_MAX_BYTES
    _ice._MANIFEST_CACHE_MAX_BYTES = 100
    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    try:
        _ice._cache_put("small.avro", b"a" * 10)
        _ice._cache_put("huge.avro", b"a" * 200)
        assert "small.avro" in _ice._manifest_bytes_cache
        assert "huge.avro" not in _ice._manifest_bytes_cache
        assert _ice._manifest_cache_size == 10
    finally:
        _ice._MANIFEST_CACHE_MAX_BYTES = saved_max
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0


def test_patched_cat_file_serves_from_cache_after_first_fetch():
    """On the second read of the same manifest, _orig_cat_file must NOT be
    called — the bytes come from cache. This is the savings: 1100 manifests
    × 470 reads/file/day → 1100 fetches/day instead of 517K."""
    import asyncio

    import s3fs

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0

    fetch_count = {"n": 0}

    async def fake_orig(self, path, version_id=None, start=None, end=None, **kwargs):
        fetch_count["n"] += 1
        return b"AVRO:" + path.encode()

    saved = s3fs.S3FileSystem._cat_file
    try:
        # Reinstall the patched wrapper around our fake _orig. The patched
        # closure captured the *original* _orig_cat_file at import time, so
        # we re-exercise the cache logic via a thin shim that calls fake_orig.
        async def shim(self, path, **kwargs):
            if not _ice._is_immutable_path(path):
                return await fake_orig(self, path, **kwargs)
            cached = _ice._cache_get(path)
            if cached is None:
                cached = await fake_orig(self, path, **kwargs)
                _ice._cache_put(path, cached)
            start = kwargs.get("start")
            end = kwargs.get("end")
            if start is None and end is None:
                return cached
            return cached[start or 0 : end if end is not None else len(cached)]

        async def drive():
            r1 = await shim(None, "bucket/m0.avro")
            r2 = await shim(None, "bucket/m0.avro")
            r3 = await shim(None, "bucket/m0.avro", start=2, end=5)
            return r1, r2, r3

        r1, r2, r3 = asyncio.run(drive())
        assert r1 == r2 == b"AVRO:bucket/m0.avro"
        assert r3 == b"RO:"
        assert fetch_count["n"] == 1, f"expected 1 fetch, got {fetch_count['n']} (cache miss)"
    finally:
        s3fs.S3FileSystem._cat_file = saved
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0


def test_patched_info_skips_head_when_bytes_cached():
    """Once we have the bytes in cache, _info synthesizes Size from
    len(bytes) instead of calling head_object. Pinning this prevents a
    regression where someone refactors _patched_info and accidentally
    drops the cache check."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._cache_put("bucket/iceberg/.../m0.avro", b"AVRO_BYTES_____")

    head_count = {"n": 0}

    async def fake_orig_info(self, path, **kwargs):
        head_count["n"] += 1
        return {"name": path, "size": 999, "type": "file"}

    async def shim(self, path, **kwargs):
        if _ice._is_immutable_path(path) and not kwargs.get("refresh"):
            cached = _ice._cache_get(path)
            if cached is not None:
                return {"name": path, "Key": path, "size": len(cached), "Size": len(cached), "type": "file"}
        return await fake_orig_info(self, path, **kwargs)

    info = asyncio.run(shim(None, "bucket/iceberg/.../m0.avro"))
    assert info["size"] == len(b"AVRO_BYTES_____")
    assert head_count["n"] == 0, "HEAD must be skipped on cache hit"

    # refresh=True bypasses cache (force re-stat path)
    asyncio.run(shim(None, "bucket/iceberg/.../m0.avro", refresh=True))
    assert head_count["n"] == 1

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0


def test_patched_info_falls_through_to_head_on_cache_miss_does_not_prefetch():
    """Cache-miss contract for _patched_info: fall through to _orig_info
    (the upstream HEAD). Do NOT prefetch the full body via _orig_cat_file.

    Regression for the 2.0× duplicate-fetch ratio observed 2026-05-21: the
    prefetch path raced aiobotocore mid-stream
    ("ClientConnectionResetError: Cannot write to closing transport") on
    ~89% of m0.avro reads. The cancelled prefetch left the cache empty,
    so _patched_open had to issue a second wire fetch for the same file —
    doubling our cost. Letting open() be the only bytes-fetcher costs one
    HEAD per never-before-seen immutable file (subsequent ticks hit the
    LRU) but eliminates the duplicate-GET pattern."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0

    head_count = {"n": 0}
    cat_count = {"n": 0}

    async def fake_orig_info(self, path, **kwargs):
        head_count["n"] += 1
        return {"name": path, "size": 999, "type": "file"}

    async def fake_orig_cat(self, path, version_id=None, **kwargs):
        cat_count["n"] += 1
        return b"FETCHED_BYTES"

    # Production shim mirrors the new contract in _patched_info.
    async def shim(self, path, **kwargs):
        if _ice._is_immutable_path(path) and not kwargs.get("refresh"):
            cached = _ice._cache_get(path)
            if cached is not None:
                return {"name": path, "Key": path, "size": len(cached), "Size": len(cached), "type": "file"}
        return await fake_orig_info(self, path, **kwargs)

    info = asyncio.run(shim(None, "bucket/iceberg/.../new-manifest.avro"))
    assert info["size"] == 999, "miss must use upstream HEAD-derived size"
    assert head_count["n"] == 1, "cache miss must fall through to _orig_info"
    assert cat_count["n"] == 0, "must NOT prefetch full body — regression guard"

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0


def test_patched_open_fetches_into_cache_and_serves_bytesio_for_immutable():
    """Live trace on 2026-05-20 verified that pyiceberg's manifest-plan
    workflow calls _open WITHOUT first calling info() or cat_file (17
    _open calls, 0 _cat_file calls in a real plan_files run). That means
    _patched_open cannot rely on _patched_info to prime the cache — it
    must fetch the bytes itself on miss, otherwise the same file gets
    fetched anew by S3File's _fetch_range on every open, and the LRU
    never helps these reads.

    First open: cache miss → cat_file() fetches+caches → BytesIO returned.
    Second open: cache hit → BytesIO served, NO refetch."""
    import io as _io

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0

    orig_open_count = {"n": 0}
    cat_file_count = {"n": 0}

    class FakeS3FS:
        def cat_file(self, path):
            cat_file_count["n"] += 1
            return b"CACHED_MANIFEST_BYTES"

    def fake_orig_open(self, path, mode="rb", **kwargs):
        orig_open_count["n"] += 1

        class _FakeS3File:
            pass

        return _FakeS3File()

    # Mirror the shape of the production patched_open.
    def shim(self, path, mode="rb", **kwargs):
        if mode == "rb" and _ice._is_immutable_path(path):
            cached = _ice._cache_get(path)
            if cached is None:
                try:
                    cached = self.cat_file(path)
                    _ice._cache_put(path, cached)
                except Exception:
                    return fake_orig_open(self, path, mode=mode, **kwargs)
            return _io.BytesIO(cached)
        return fake_orig_open(self, path, mode=mode, **kwargs)

    fs = FakeS3FS()
    path = "bucket/iceberg/.../m0.avro"

    # First open: cache miss → cat_file fetches → cached → BytesIO returned.
    handle = shim(fs, path)
    assert isinstance(handle, _io.BytesIO)
    assert handle.read() == b"CACHED_MANIFEST_BYTES"
    assert cat_file_count["n"] == 1
    assert orig_open_count["n"] == 0, "miss must use cat_file, not _orig_open"

    # Second open: cache hit → BytesIO served, NO refetch.
    handle = shim(fs, path)
    assert isinstance(handle, _io.BytesIO)
    assert cat_file_count["n"] == 1, "second open must hit cache, not refetch"

    # Mutable path: never intercepted, always falls through.
    shim(fs, "bucket/iceberg/.../data/x.parquet")
    assert orig_open_count["n"] == 1

    # Write mode: never intercepted, always falls through.
    shim(fs, "bucket/iceberg/.../m1.avro", mode="wb")
    assert orig_open_count["n"] == 2

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0


def test_get_or_fetch_immutable_async_dedupes_concurrent_fetches():
    """Post-BytesIO-fix telemetry on 2026-05-20 showed 114/163 manifests
    still fetched twice per cron_compact (1.7x ratio, target 1.0x). Cause:
    fsspec's iothread scheduled many concurrent _patched_cat_file
    coroutines for the SAME path before any populated the LRU — each did
    its own wire fetch. 81-134 GETs landed in a single second during the
    plan-files burst, with many racing the same URL.

    With dedup: N concurrent fetches of the same immutable path coalesce
    to a single wire call. The first coroutine in becomes the leader and
    registers a shared Task in _inflight_async; followers await the same
    Task (under asyncio.shield) and all observe the same bytes."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    fetch_count = {"n": 0}

    async def run_test():
        leader_started = asyncio.Event()
        release_leader = asyncio.Event()

        async def slow_fetch(fs, path, version_id=None, **kwargs):
            fetch_count["n"] += 1
            leader_started.set()
            await release_leader.wait()
            return b"AVRO:" + path.encode()

        saved = _ice._orig_cat_file
        _ice._orig_cat_file = slow_fetch
        try:
            path = "bucket/iceberg/.../m0.avro"
            # Schedule the leader; it will block inside slow_fetch until
            # release_leader is set.
            leader = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, path))
            # Wait until the leader has registered itself in _inflight_async.
            await leader_started.wait()
            # Now schedule followers; they must see the inflight event.
            followers = [asyncio.create_task(_ice._get_or_fetch_immutable_async(None, path)) for _ in range(9)]
            # Yield so followers get a chance to start and queue on the event.
            await asyncio.sleep(0)
            # Release the leader → cache gets populated → followers wake.
            release_leader.set()
            results = await asyncio.gather(leader, *followers)
            return results
        finally:
            _ice._orig_cat_file = saved

    results = asyncio.run(run_test())

    assert fetch_count["n"] == 1, f"expected 1 wire fetch for 10 concurrent callers, got {fetch_count['n']}"
    assert all(r == b"AVRO:bucket/iceberg/.../m0.avro" for r in results), (
        "all 10 callers must observe the same bytes (the leader's fetch)"
    )

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()


def test_get_or_fetch_immutable_async_shared_task_error_then_next_call_retries():
    """When the shared fetch Task raises, every awaiter on the same
    inflight entry observes the same exception (they're all awaiting the
    one Task). The done callback then pops the inflight slot, so the very
    next caller starts a fresh Task and can succeed. This is simpler than
    the previous Event-based scheme where followers retried within the
    same call, and it's what the shield-based dedup requires."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    fetch_attempts = {"n": 0}

    async def run_test():
        leader_started = asyncio.Event()
        release_leader = asyncio.Event()

        async def fail_then_succeed(fs, path, version_id=None, **kwargs):
            fetch_attempts["n"] += 1
            attempt = fetch_attempts["n"]
            if attempt == 1:
                leader_started.set()
                await release_leader.wait()
                raise RuntimeError("transient blip")
            return b"RETRY_BYTES"

        saved = _ice._orig_cat_file
        _ice._orig_cat_file = fail_then_succeed
        try:
            path = "bucket/iceberg/.../boom.avro"
            leader = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, path))
            await leader_started.wait()
            follower = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, path))
            await asyncio.sleep(0)
            release_leader.set()

            leader_exc = None
            follower_exc = None
            try:
                await leader
            except RuntimeError as e:
                leader_exc = e
            try:
                await follower
            except RuntimeError as e:
                follower_exc = e

            # Let the done_callback fire so the inflight slot is released
            # before the retry call.
            for _ in range(3):
                await asyncio.sleep(0)
            retry_bytes = await _ice._get_or_fetch_immutable_async(None, path)
            return leader_exc, follower_exc, retry_bytes
        finally:
            _ice._orig_cat_file = saved

    leader_exc, follower_exc, retry_bytes = asyncio.run(run_test())

    assert isinstance(leader_exc, RuntimeError)
    assert isinstance(follower_exc, RuntimeError), "shared Task → follower sees same error"
    assert retry_bytes == b"RETRY_BYTES"
    assert fetch_attempts["n"] == 2, "1 failed shared fetch + 1 retry on next call = 2 attempts"

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()


def test_get_or_fetch_immutable_async_awaiter_cancel_does_not_abort_shared_fetch():
    """Regression for the 2.0× duplicate-fetch ratio (telemetry 2026-05-21).
    Root cause: aiobotocore disconnected mid-stream during pyiceberg's
    FsspecInputFile.__len__ path ("client disconnect mid-stream ...
    ClientConnectionResetError"). The cancellation propagated into the
    cat_file coroutine, which aborted before the bytes were cached.
    _patched_open then issued a SECOND wire fetch for the same file.

    Fix: the shared inflight Task is awaited under asyncio.shield, and a
    done_callback unconditionally populates the LRU. An awaiter being
    cancelled is isolated to that caller — the Task keeps running and the
    next caller hits cache instead of going to the wire."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    fetch_count = {"n": 0}
    fetch_completed = {"v": False}

    async def run_test():
        fetch_started = asyncio.Event()
        let_fetch_finish = asyncio.Event()

        async def slow_fetch(fs, path, version_id=None, **kwargs):
            fetch_count["n"] += 1
            fetch_started.set()
            await let_fetch_finish.wait()
            fetch_completed["v"] = True
            return b"FETCHED_BYTES"

        saved = _ice._orig_cat_file
        _ice._orig_cat_file = slow_fetch
        try:
            path = "bucket/iceberg/.../cancelled.avro"
            caller_a = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, path))
            await fetch_started.wait()
            caller_a.cancel()
            try:
                await caller_a
            except asyncio.CancelledError:
                pass
            assert not fetch_completed["v"], "shielded fetch must keep running after awaiter cancel"
            assert fetch_count["n"] == 1

            let_fetch_finish.set()
            for _ in range(5):
                await asyncio.sleep(0)
            assert fetch_completed["v"], "shielded fetch must complete after release"

            caller_b_bytes = await _ice._get_or_fetch_immutable_async(None, path)
            return caller_b_bytes
        finally:
            _ice._orig_cat_file = saved

    result = asyncio.run(run_test())

    assert result == b"FETCHED_BYTES"
    assert fetch_count["n"] == 1, f"awaiter cancel must NOT trigger a second wire fetch — got {fetch_count['n']}"

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()


def test_cache_key_canonicalized_across_scheme_prefix():
    """PyIceberg's FsspecInputFile.__len__ calls ``fs.info("s3://bucket/key.avro")``;
    sync_wrapper hands the s3:// form straight to ``_patched_info``. But
    ``FsspecInputFile.open()`` calls ``fs.open("s3://bucket/key.avro", "rb")``,
    and fsspec's base ``open()`` runs ``_strip_protocol`` *before* dispatching
    to ``_patched_open`` — so the path arrives as ``bucket/key.avro``.

    Without canonicalization, the LRU stores bytes under ``s3://bucket/key.avro``
    from the info() prefetch and misses on the open() lookup with the stripped
    form — every immutable manifest gets fetched twice. Telemetry on
    2026-05-20 pinned this as the cause of the 1.92× manifest GET/distinct-URL
    ratio post-dedup-fix (target 1.0×).
    """
    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0

    try:
        # Mimic info() priming the cache with the s3:// scheme intact.
        _ice._cache_put("s3://bucket/iceberg/.../m0.avro", b"AVRO_BYTES")
        # Mimic open() looking up with the protocol-stripped form.
        cached = _ice._cache_get("bucket/iceberg/.../m0.avro")
        assert cached == b"AVRO_BYTES", (
            "open()'s stripped path must hit the same LRU slot as info()'s s3:// path; "
            "otherwise every manifest is fetched twice (the 1.92× ratio regression)."
        )

        # Reverse direction: open() puts, info() gets.
        _ice._cache_put("bucket/iceberg/.../m1.avro", b"OTHER_BYTES")
        assert _ice._cache_get("s3://bucket/iceberg/.../m1.avro") == b"OTHER_BYTES"

        # s3a:// is treated as the same logical object too (some tools use it).
        assert _ice._cache_get("s3a://bucket/iceberg/.../m1.avro") == b"OTHER_BYTES"

        # Leading slash is also normalized — guards against accidental
        # absolute-path leakage from concatenated joins.
        assert _ice._cache_get("/bucket/iceberg/.../m1.avro") == b"OTHER_BYTES"

        # The canonical form should land in the LRU exactly once, regardless
        # of which variant the writer used (no duplicate slots eating budget).
        keys = list(_ice._manifest_bytes_cache.keys())
        assert keys == ["bucket/iceberg/.../m0.avro", "bucket/iceberg/.../m1.avro"], (
            f"LRU should hold canonical keys only; got {keys}"
        )
    finally:
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0


def test_inflight_dedup_canonicalizes_across_scheme_prefix():
    """The inflight-dedup dict must also key on canonical path. Otherwise a
    racing info("s3://x.avro") and open("x.avro") on fsspec's iothread each
    register their own asyncio.Event and both go to the wire — the dedup
    helper "works" but yields zero wins for the very pyiceberg access pattern
    it was added to fix.

    Models the race in a single event loop (which is exactly what fsspec's
    iothread is) by scheduling two _get_or_fetch_immutable_async coroutines
    for the same logical path under two different prefix forms.
    """
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    fetch_count = {"n": 0}

    async def run_test():
        leader_started = asyncio.Event()
        release_leader = asyncio.Event()

        async def slow_fetch(fs, path, version_id=None, **kwargs):
            fetch_count["n"] += 1
            leader_started.set()
            await release_leader.wait()
            # Return bytes keyed off the canonical form so both callers
            # observe the same payload regardless of which scheme they used.
            return b"BYTES_FOR:" + _ice._canonical_cache_key(path).encode()

        saved = _ice._orig_cat_file
        _ice._orig_cat_file = slow_fetch
        try:
            # Leader uses the s3:// form (info() path on the iothread).
            leader = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, "s3://bucket/iceberg/.../m0.avro"))
            await leader_started.wait()
            # Follower uses the stripped form (open() path post-_strip_protocol).
            follower = asyncio.create_task(_ice._get_or_fetch_immutable_async(None, "bucket/iceberg/.../m0.avro"))
            await asyncio.sleep(0)
            release_leader.set()
            return await asyncio.gather(leader, follower)
        finally:
            _ice._orig_cat_file = saved

    results = asyncio.run(run_test())

    assert fetch_count["n"] == 1, (
        f"s3:// and stripped paths must dedup to one wire fetch; got {fetch_count['n']}. "
        "If this fires, the inflight dict is keying on the raw path again and the "
        "pyiceberg info/open race re-doubles the manifest GET count."
    )
    # The follower reads from cache via the leader's canonical _cache_put,
    # so its bytes match the leader's even though it called with a different
    # path form.
    assert results[0] == results[1] == b"BYTES_FOR:bucket/iceberg/.../m0.avro"

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()


def test_get_or_fetch_immutable_async_passes_max_concurrency_one_to_orig_cat_file():
    """Pin the fix for the 2026-05-21 2.00x ratio (root cause #2).

    s3fs.S3FileSystem._cat_file defaults to ``max_concurrency=10``. When
    ``max_concurrency > 1`` AND no start/end is set (our case for full-file
    manifest reads), s3fs issues a "probe" get_object first to read the
    Content-Length header — then immediately closes the body and issues a
    SECOND get_object to actually read the bytes. Both GETs are billed by
    FOS (and both get logged by the telemetry proxy), so even a perfect
    process-local cache that dedups callers 1:1 produces a 2.00x wire
    ratio against the proxy.

    The fix forces ``max_concurrency=1`` on every fetch in
    _get_or_fetch_immutable_async so the probe branch is skipped and s3fs
    falls through to a single ``_call_and_read``. This test pins the
    contract: _orig_cat_file is invoked exactly once per wire fetch and
    must receive ``max_concurrency=1`` as a keyword argument."""
    import asyncio

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    seen_kwargs: list[dict] = []

    async def fake_orig_cat_file(fs, path, version_id=None, **kwargs):
        seen_kwargs.append(dict(kwargs))
        return b"BODY"

    saved = _ice._orig_cat_file
    _ice._orig_cat_file = fake_orig_cat_file
    try:

        async def run():
            return await _ice._get_or_fetch_immutable_async(None, "bucket/iceberg/.../m0.avro")

        result = asyncio.run(run())
    finally:
        _ice._orig_cat_file = saved
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0
        _ice._inflight_async.clear()

    assert result == b"BODY"
    assert len(seen_kwargs) == 1, f"expected exactly one wire fetch, got {len(seen_kwargs)}"
    assert seen_kwargs[0].get("max_concurrency") == 1, (
        "must pass max_concurrency=1 to skip s3fs._cat_file's probe-GET path. "
        f"Got kwargs={seen_kwargs[0]!r}. If this regresses, every manifest "
        "fetch billed 2x against FOS (the 2026-05-21 2.00x ratio bug)."
    )


def test_patched_open_routes_through_iothread_not_stale_cat_file_alias():
    """Pin the fix for the 2026-05-21 2.00x ratio bug.

    fsspec's ``sync_wrapper`` generates ``S3FileSystem.cat_file`` at class
    definition time, capturing the ORIGINAL ``_cat_file`` reference. When
    we later reassign ``S3FileSystem._cat_file = _patched_cat_file``, the
    auto-generated ``cat_file`` alias still calls the unpatched method.
    If ``_patched_open`` calls ``self.cat_file(path)`` it therefore goes
    straight to the wire and the LRU is never populated — every pyiceberg
    open() of the same manifest fetches twice.

    This test sabotages ``self.cat_file`` to a sentinel that would fail
    the test if invoked, and proves that ``_patched_open`` instead bridges
    into the iothread loop to call our patched async helper, populates the
    LRU on first call, and serves from cache on the second."""
    import io as _io

    import fsspec.asyn as _fsspec_asyn

    from backend.core import iceberg as _ice

    _ice._manifest_bytes_cache.clear()
    _ice._manifest_cache_size = 0
    _ice._inflight_async.clear()

    fetch_count = {"n": 0}
    sync_alias_called = {"n": 0}

    async def fake_orig_cat_file(fs, path, version_id=None, **kwargs):
        fetch_count["n"] += 1
        return b"WIRE_BYTES_FROM_ORIG_CAT_FILE"

    class FakeS3FS:
        # ``.loop`` mirrors what s3fs.S3FileSystem exposes — fsspec.asyn.sync
        # bridges the caller into THIS loop (the iothread loop).
        loop = _fsspec_asyn.get_loop()

        def cat_file(self, path):
            # If _patched_open regresses to ``self.cat_file(path)``, this
            # counter trips and the fix-pin fails loudly. We return WRONG
            # bytes so even a "tolerant" regression that ignores the count
            # would still fail the content assertion below.
            sync_alias_called["n"] += 1
            return b"STALE_CAT_FILE_ALIAS_BYTES"

    saved_orig_cat_file = _ice._orig_cat_file
    _ice._orig_cat_file = fake_orig_cat_file
    try:
        fs = FakeS3FS()
        path = "bucket/iceberg/.../m0.avro"

        # First open: cache miss → must reach _orig_cat_file via the iothread,
        # NOT the stale ``cat_file`` sync alias.
        handle = _ice._patched_open(fs, path)
        assert isinstance(handle, _io.BytesIO)
        assert handle.read() == b"WIRE_BYTES_FROM_ORIG_CAT_FILE", (
            "first open must route through _get_or_fetch_immutable_async "
            "(via fsspec.asyn.sync into the iothread), not self.cat_file"
        )
        assert fetch_count["n"] == 1
        assert sync_alias_called["n"] == 0, (
            "self.cat_file is the fsspec sync_wrapper alias captured at class "
            "definition time — calling it bypasses _patched_cat_file entirely "
            "and the LRU never fills. Regression: every immutable file is "
            "fetched twice (the 2026-05-21 2.00x ratio bug)."
        )

        # Second open: cache hit, NO new wire fetch.
        handle = _ice._patched_open(fs, path)
        assert isinstance(handle, _io.BytesIO)
        assert handle.read() == b"WIRE_BYTES_FROM_ORIG_CAT_FILE"
        assert fetch_count["n"] == 1, "second open must hit LRU, not refetch"
        assert sync_alias_called["n"] == 0
    finally:
        _ice._orig_cat_file = saved_orig_cat_file
        _ice._manifest_bytes_cache.clear()
        _ice._manifest_cache_size = 0
        _ice._inflight_async.clear()


def test_update_iceberg_view_locked_escapes_single_quotes(monkeypatch):
    from backend.core import iceberg as _ice

    source_key = "test-escapes-single-quotes"
    _ice._view_cache.pop(source_key, None)

    metadata_loc = None
    snapshot_id = 12345
    # The vulnerability key: table location contains a single quote
    malicious_loc = "s3://bucket/O'Brien/logs"
    _ice._snapshot_files_cache[source_key] = (metadata_loc, snapshot_id, malicious_loc, [])

    source = {
        "name": source_key,
        "bucket": "b",
        "prefix": "p",
    }

    monkeypatch.setattr(_ice, "buffer_files", lambda src: [])
    monkeypatch.setattr(_ice, "_read_metadata_pointer", lambda src, ident: metadata_loc)
    monkeypatch.setattr(_ice, "_load_persistent_cache", lambda src: None)
    monkeypatch.setattr(_ice, "configure_duckdb_s3", lambda con: None)
    monkeypatch.setattr("os.path.exists", lambda p: False)

    # Fresh service: no ingested files in sqlite metadata either.
    from backend.core import metadata as _meta

    monkeypatch.setattr(
        _meta,
        "get_ingested_files_status_summary",
        lambda svc: {
            "file_count": 0,
            "total_rows": 0,
            "total_bytes": 0,
            "count_with_bytes": 0,
            "last_ingested": None,
            "latest_file_name": None,
        },
    )

    fake_con = MagicMock()
    _ice._update_iceberg_view_locked(fake_con, source)

    create_view_calls = [
        c
        for c in fake_con.execute.call_args_list
        if c.args and "CREATE OR REPLACE" in str(c.args[0]) and "VIEW" in str(c.args[0])
    ]
    assert create_view_calls, "View should have been created"
    view_sql = str(create_view_calls[0].args[0])
    # Ensure that single quotes are correctly escaped to '' inside iceberg_scan
    assert "iceberg_scan('s3://bucket/O''Brien/logs'" in view_sql
    assert "s3://bucket/O'Brien/logs" not in view_sql.replace("s3://bucket/O''Brien/logs", "")

    _ice._view_cache.pop(source_key, None)


def test_optimize_table_escapes_single_quotes(monkeypatch):
    from backend.core import iceberg as _ice

    source = {
        "name": "test-optimize-escapes",
        "bucket": "b",
        "prefix": "p",
    }

    # Mock catalog and table loading
    fake_catalog = MagicMock()
    fake_table = MagicMock()
    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "_load_table_cached", lambda src, ident, cat: fake_table)

    # Mock scan().plan_files() to return our single-quoted file path
    mock_file = MagicMock()
    mock_file.file.partition = (12345,)
    mock_file.file.file_path = "s3://bucket/O'Brien/logs/data.parquet"
    fake_table.scan().plan_files.return_value = [mock_file]

    # Mock get_connection to capture executed SQL
    captured_queries = []
    fake_con = MagicMock()

    def mock_execute(sql, *args, **kwargs):
        captured_queries.append(sql)
        # Raise an exception so it fails after executing read_parquet and doesn't proceed to overwrite
        raise RuntimeError("Stop simulation")

    fake_con.execute.side_effect = mock_execute

    from backend.core import duckdb as _duckdb

    monkeypatch.setattr(_duckdb, "get_connection", lambda src, **kwargs: fake_con)

    # Run optimization with min_files_per_partition=0 to ensure it selects the partition
    _ice.optimize_table(source, min_files_per_partition=0)

    # Assert that DuckDB was called with read_parquet enclosing our escaped single quotes
    assert captured_queries, "Should have executed DuckDB query"
    parquet_query = captured_queries[0]
    assert "read_parquet(['s3://bucket/O''Brien/logs/data.parquet']" in parquet_query
    assert "s3://bucket/O'Brien/logs/data.parquet" not in parquet_query.replace(
        "s3://bucket/O''Brien/logs/data.parquet", ""
    )


def test_optimize_table_uses_union_by_name(monkeypatch):
    """Regression for the 2026-06-06 optimize-cron warning: when a partition
    contains files written before AND after a schema bump (e.g. ``edge_sid``
    added mid-day), the DuckDB ``read_parquet([...])`` defaults to positional
    union and raises ``Schema mismatch ... try setting union_by_name=True``.
    Prod had two partitions (494541, 494542) stuck at 14+ files each since
    2026-06-01 because every nightly optimize attempt raised here.

    The fix passes ``union_by_name=true`` so DuckDB merges the column sets
    and fills missing columns with NULL — matching how Iceberg already
    presents the merged schema to readers."""
    from backend.core import iceberg as _ice

    source = {"name": "test-optimize-union", "bucket": "b", "prefix": "p"}

    fake_catalog = MagicMock()
    fake_table = MagicMock()
    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "_load_table_cached", lambda src, ident, cat: fake_table)

    mock_file = MagicMock()
    mock_file.file.partition = (12345,)
    mock_file.file.file_path = "s3://bucket/data.parquet"
    fake_table.scan().plan_files.return_value = [mock_file, mock_file]

    captured_queries: list[str] = []
    fake_con = MagicMock()

    def mock_execute(sql, *args, **kwargs):
        captured_queries.append(sql)
        raise RuntimeError("Stop simulation")

    fake_con.execute.side_effect = mock_execute

    from backend.core import duckdb as _duckdb

    monkeypatch.setattr(_duckdb, "get_connection", lambda src, **kwargs: fake_con)

    _ice.optimize_table(source, min_files_per_partition=0)

    assert captured_queries
    parquet_query = captured_queries[0]
    assert "union_by_name=true" in parquet_query, (
        f"read_parquet must pass union_by_name=true so partitions with "
        f"schema-evolved files (pre- vs post-schema-bump) can coalesce. "
        f"Removing this argument re-introduces the 2026-06-06 prod warnings. "
        f"Got SQL: {parquet_query[:300]}"
    )


def test_optimize_table_retries_on_sequence_number_cas_conflict(monkeypatch):
    """Regression for the 2026-06-04 sporadic CAS conflict during optimize:
    ``ValueError: Cannot add snapshot with sequence number 2159 older than
    last sequence number 2159`` — raised when an ingest commit lands between
    optimize's plan_files read and its overwrite. The fix wraps overwrite()
    in a bounded retry that reloads the table head and tries once more.

    This test pins the retry behavior:
      1. First overwrite() raises the CAS ValueError → caught + reloaded
      2. Second overwrite() succeeds → loop exits
      3. Catalog.load_table is invoked exactly once for the reload

    If the retry is removed, the test fails because optimize raises on the
    first attempt and the partition lands in partition_errors with
    status='warning' on the cron row."""
    from backend.core import iceberg as _ice

    source = {"name": "test-optimize-cas-retry", "bucket": "b", "prefix": "p"}

    overwrite_calls = {"n": 0}
    reload_calls = {"n": 0}

    fake_catalog = MagicMock()
    initial_table = MagicMock()
    reloaded_table = MagicMock()

    def _initial_overwrite(*a, **kw):
        overwrite_calls["n"] += 1
        raise ValueError("Cannot add snapshot with sequence number 2159 older than last sequence number 2159")

    def _reloaded_overwrite(*a, **kw):
        overwrite_calls["n"] += 1  # increments the SAME counter so we count total attempts

    initial_table.overwrite.side_effect = _initial_overwrite
    reloaded_table.overwrite.side_effect = _reloaded_overwrite

    def _catalog_load(ident):
        reload_calls["n"] += 1
        return reloaded_table

    fake_catalog.load_table.side_effect = _catalog_load

    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "_load_table_cached", lambda src, ident, cat: initial_table)
    monkeypatch.setattr(_ice, "_set_cached_table", lambda src, ident, tbl: None)
    monkeypatch.setattr(_ice, "_write_metadata_pointer", lambda src, loc, table=None: None)
    monkeypatch.setattr(_ice, "sync_data", lambda src: {"files_cached": 0, "rows_cached": 0})

    mock_file = MagicMock()
    mock_file.file.partition = (12345,)
    mock_file.file.file_path = "s3://bucket/data.parquet"
    initial_table.scan().plan_files.return_value = [mock_file, mock_file]
    reloaded_table.scan().plan_files.return_value = [mock_file, mock_file]

    fake_con = MagicMock()
    fake_arrow = MagicMock()
    fake_con.execute.return_value.to_arrow_table.return_value = fake_arrow

    from backend.core import duckdb as _duckdb

    monkeypatch.setattr(_duckdb, "get_connection", lambda src, **kwargs: fake_con)

    result = _ice.optimize_table(source, min_files_per_partition=0)

    assert overwrite_calls["n"] == 2, (
        f"expected exactly 2 overwrite() calls (1 fail + 1 retry success); got {overwrite_calls['n']}"
    )
    assert reload_calls["n"] == 1, (
        f"expected exactly 1 catalog.load_table() reload after the CAS conflict; got {reload_calls['n']}"
    )
    # Partition should not appear in partition_errors — the retry succeeded.
    assert not result.get("partition_errors"), (
        f"retry should have prevented the partition from landing in partition_errors. "
        f"Got: {result.get('partition_errors')}"
    )


def test_optimize_table_does_not_retry_unrelated_value_errors(monkeypatch):
    """The CAS-conflict retry must ONLY catch the specific sequence-number
    message. Unrelated ValueErrors (e.g. a real schema bug, a corrupted
    file path) should propagate immediately so they aren't silently
    retried 3 times before showing up as errors."""
    from backend.core import iceberg as _ice

    source = {"name": "test-optimize-no-spurious-retry", "bucket": "b", "prefix": "p"}

    overwrite_calls = {"n": 0}
    reload_calls = {"n": 0}

    def _bad_overwrite(*a, **kw):
        overwrite_calls["n"] += 1
        raise ValueError("Some unrelated value error")

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = lambda ident: (
        reload_calls.__setitem__("n", reload_calls["n"] + 1),
        MagicMock(),
    )[1]

    fake_table = MagicMock()
    fake_table.overwrite.side_effect = _bad_overwrite
    mock_file = MagicMock()
    mock_file.file.partition = (12345,)
    mock_file.file.file_path = "s3://bucket/data.parquet"
    fake_table.scan().plan_files.return_value = [mock_file, mock_file]

    monkeypatch.setattr(_ice, "_get_catalog", lambda src: fake_catalog)
    monkeypatch.setattr(_ice, "_load_table_cached", lambda src, ident, cat: fake_table)
    monkeypatch.setattr(_ice, "_set_cached_table", lambda src, ident, tbl: None)
    monkeypatch.setattr(_ice, "_write_metadata_pointer", lambda src, loc, table=None: None)
    monkeypatch.setattr(_ice, "sync_data", lambda src: {"files_cached": 0, "rows_cached": 0})

    fake_con = MagicMock()
    fake_con.execute.return_value.to_arrow_table.return_value = MagicMock()

    from backend.core import duckdb as _duckdb

    monkeypatch.setattr(_duckdb, "get_connection", lambda src, **kwargs: fake_con)

    _ice.optimize_table(source, min_files_per_partition=0)

    # The partition_errors path swallows the raised ValueError into
    # partition_errors — but the IMPORTANT invariant is that overwrite
    # was only attempted ONCE and reload was NEVER called.
    assert overwrite_calls["n"] == 1, (
        f"unrelated ValueErrors must propagate immediately, not trigger CAS retry. "
        f"Got {overwrite_calls['n']} overwrite calls."
    )
    assert reload_calls["n"] == 0, (
        f"unrelated ValueErrors must NOT trigger table reload. Got {reload_calls['n']} reloads."
    )


# ---------------------------------------------------------------------------
# run_cloud_maintenance — snapshot expiry (pyiceberg 0.11.1 API)
# ---------------------------------------------------------------------------


def _maintenance_table(snapshots_before: int, snapshots_after: int, commit_side_effect=None):
    """Build a MagicMock pyiceberg.Table that mimics the maintenance chain
    table.maintenance.expire_snapshots().older_than(<dt>).commit() and reports
    `snapshots_before` snapshots until .commit() runs (after which it reports
    `snapshots_after`). Returns (table, captured_calls dict)."""
    captured = {
        "maintenance_property_reads": 0,
        "expire_snapshots_calls": 0,
        "older_than_args": [],
        "commit_calls": 0,
    }

    state = {"snapshot_count": snapshots_before}
    fake_table = MagicMock()
    fake_table.metadata_location = "s3://bucket/m.json"

    def _snapshots_list():
        return [MagicMock(snapshot_id=i) for i in range(state["snapshot_count"])]

    type(fake_table.metadata).snapshots = property(lambda _self: _snapshots_list())

    def _commit():
        captured["commit_calls"] += 1
        if commit_side_effect is not None:
            exc = commit_side_effect()
            if exc is not None:
                raise exc
        state["snapshot_count"] = snapshots_after

    def _older_than(dt):
        captured["older_than_args"].append(dt)
        builder = MagicMock()
        builder.commit.side_effect = _commit
        return builder

    def _expire_snapshots():
        captured["expire_snapshots_calls"] += 1
        builder = MagicMock()
        builder.older_than.side_effect = _older_than
        return builder

    def _maintenance_getter(_self):
        captured["maintenance_property_reads"] += 1
        m = MagicMock()
        m.expire_snapshots.side_effect = _expire_snapshots
        return m

    # `.maintenance` is a @property in pyiceberg 0.11.1 — accessing it with
    # parens (e.g. table.maintenance()) must NOT work, otherwise we'd never
    # catch the original AttributeError-shaped bug returning.
    type(fake_table).maintenance = property(_maintenance_getter)

    return fake_table, captured


def _maintenance_source():
    return {
        "name": "expire-test",
        "service_id": "expire-test",
        "bucket": "b",
        "prefix": "p",
    }


def _patch_maintenance_deps(monkeypatch, table, catalog):
    """Wire up the minimum set of patches so run_cloud_maintenance reaches the
    expire-snapshots block without touching data deletion or local cache."""
    from backend import config as _svcconfig
    from backend.core import iceberg as _ice

    monkeypatch.setattr(_svcconfig, "load_config", lambda sid: {})
    monkeypatch.setattr(_ice, "_get_catalog", lambda src: catalog)
    monkeypatch.setattr(_ice, "_load_table_cached", lambda src, ident, cat: table)
    monkeypatch.setattr(_ice, "_set_cached_table", lambda src, ident, tbl: None)
    monkeypatch.setattr(_ice, "_write_metadata_pointer", lambda src, loc, table=None: None)
    # Step 1 (data delete) succeeds as a no-op via the table's delete mock.
    # Step 3 (local cache) is skipped because cache_retention_days falls back
    # to 90 but _cache_dir won't exist for this synthetic source.


def test_run_cloud_maintenance_calls_correct_expire_snapshots_api(monkeypatch):
    """Pins the EXACT pyiceberg 0.11.1 maintenance chain so a future API drift
    fails CI loudly instead of silently swallowing AttributeError into
    snapshot_expiry_error (the original bug, which lived undetected for the
    service's entire lifetime because the wrapper test only mocked
    run_cloud_maintenance as a whole).

    Invariants pinned:
      - .maintenance is accessed as a PROPERTY (one read per call, no parens)
      - .expire_snapshots() returns a builder (not table.expire_snapshots)
      - .older_than(<arg>) is called with a tz-aware datetime (NOT int millis)
      - .commit() is invoked exactly once on the success path
    """
    from datetime import datetime as _dt

    from backend.core import iceberg as _ice

    table, captured = _maintenance_table(snapshots_before=5, snapshots_after=2)
    catalog = MagicMock()
    catalog.load_table.return_value = table
    _patch_maintenance_deps(monkeypatch, table, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert captured["maintenance_property_reads"] >= 1, (
        ".maintenance must be accessed (as a @property — no parens). If this is 0, "
        "the call chain is broken and the cron will silently AttributeError again."
    )
    assert captured["expire_snapshots_calls"] == 1, (
        f"expire_snapshots() must be invoked exactly once on success; got {captured['expire_snapshots_calls']}"
    )
    assert len(captured["older_than_args"]) == 1, (
        f"older_than(...) must be called exactly once; got {len(captured['older_than_args'])} calls"
    )
    arg = captured["older_than_args"][0]
    assert isinstance(arg, _dt), (
        f"older_than(...) must receive a datetime, not {type(arg).__name__}. "
        f"Passing int millis (the original bug) raises TypeError in pyiceberg 0.11.1."
    )
    assert arg.tzinfo is not None, (
        f"older_than(...) must receive a tz-aware datetime to avoid silent UTC-vs-local drift. "
        f"Got naive datetime: {arg!r}"
    )
    assert captured["commit_calls"] == 1, (
        f"commit() must be invoked exactly once on success; got {captured['commit_calls']}"
    )
    assert "snapshot_expiry_error" not in result, (
        f"successful path must NOT populate snapshot_expiry_error. Got: {result}"
    )


def test_run_cloud_maintenance_reports_snapshot_counts(monkeypatch):
    """Locks in the observability contract: results include snapshots_before,
    snapshots_after, snapshots_expired_count, snapshots_expired_before_days,
    and (when count > 0) snapshot_expiry_note explaining the file-cleanup gap.
    Without these keys operators cannot detect future silent failures (the
    original bug went undetected precisely because no count was reported)."""
    from backend.core import iceberg as _ice

    table, _ = _maintenance_table(snapshots_before=10, snapshots_after=3)
    catalog = MagicMock()
    catalog.load_table.return_value = table
    _patch_maintenance_deps(monkeypatch, table, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert result.get("snapshots_before") == 10
    assert result.get("snapshots_after") == 3
    assert result.get("snapshots_expired_count") == 7
    assert result.get("snapshots_expired_before_days") == 7
    note = result.get("snapshot_expiry_note", "")
    assert "metadata" in note.lower() and "not deleted" in note.lower(), (
        f"when snapshots are actually expired, the result must include a note explaining "
        f"that underlying data/manifest files are not removed by pyiceberg 0.11.1. Got: {note!r}"
    )


def test_run_cloud_maintenance_skips_snapshot_note_on_noop(monkeypatch):
    """When pre and post snapshot counts are equal (nothing eligible for
    expiry), snapshot_expiry_note must be ABSENT from results — prevents the
    orphan-files caveat from becoming weekly log noise on healthy services
    whose entire history fits within the 7-day retention window."""
    from backend.core import iceberg as _ice

    table, _ = _maintenance_table(snapshots_before=2, snapshots_after=2)
    catalog = MagicMock()
    catalog.load_table.return_value = table
    _patch_maintenance_deps(monkeypatch, table, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert result.get("snapshots_expired_count") == 0
    assert "snapshot_expiry_note" not in result, (
        f"snapshot_expiry_note must be absent on no-op runs (expired_count == 0). Got: {result}"
    )


def test_run_cloud_maintenance_retries_on_commit_failed_exception(monkeypatch):
    """Mirrors the optimize-table CAS retry: a CommitFailedException from the
    expire-snapshots commit triggers catalog.load_table reload + retry. After
    one failure followed by a success, snapshots_before must be re-pinned from
    the RELOADED head (otherwise snapshots_expired_count would conflate
    'we expired N' with 'concurrent writer added M while we retried')."""
    from pyiceberg.exceptions import CommitFailedException

    from backend.core import iceberg as _ice

    call_state = {"commit_attempts": 0}

    def _commit_side():
        call_state["commit_attempts"] += 1
        if call_state["commit_attempts"] == 1:
            return CommitFailedException("Table has been updated by another process: ns.tbl")
        return None

    table_initial, captured_initial = _maintenance_table(
        snapshots_before=10, snapshots_after=10, commit_side_effect=_commit_side
    )
    # After the CAS conflict the reload returns a table whose CURRENT snapshot
    # count is 12 (a concurrent writer added 2 since our first load). The
    # successful commit on this reloaded table drops it to 5.
    table_reloaded, captured_reloaded = _maintenance_table(
        snapshots_before=12, snapshots_after=5, commit_side_effect=_commit_side
    )

    reload_returns = [table_reloaded]
    catalog = MagicMock()

    def _catalog_load(_ident):
        # 1st call = initial load_table at top of expiry block; subsequent
        # calls = post-CAS reloads.
        if not reload_returns:
            return table_reloaded
        return reload_returns.pop() if reload_returns and catalog.load_table.call_count > 1 else table_initial

    catalog.load_table.side_effect = _catalog_load
    _patch_maintenance_deps(monkeypatch, table_initial, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert call_state["commit_attempts"] == 2, (
        f"expected exactly 2 commit attempts (1 CAS fail + 1 retry success); got {call_state['commit_attempts']}"
    )
    assert catalog.load_table.call_count == 2, (
        f"expected exactly 2 catalog.load_table calls (initial + 1 reload after CAS); "
        f"got {catalog.load_table.call_count}"
    )
    # snapshots_before MUST be re-pinned from the reloaded head (12), not the
    # initial load (10). Otherwise snapshots_expired_count = max(0, 10-5) = 5
    # instead of the correct max(0, 12-5) = 7.
    assert result.get("snapshots_before") == 12, (
        f"snapshots_before must be re-pinned after CAS reload; got {result.get('snapshots_before')}. "
        f"Reporting the stale pre-reload count would misrepresent the diff on the path the retry exists for."
    )
    assert result.get("snapshots_after") == 5
    assert result.get("snapshots_expired_count") == 7
    assert "snapshot_expiry_error" not in result


def test_run_cloud_maintenance_retries_on_concurrent_expire_value_error(monkeypatch):
    """When another expire run (admin re-trigger overlapping the scheduled
    run) already removed snapshots in our expire set, pyiceberg's
    RemoveSnapshotsUpdate handler raises:
        ValueError('Snapshot with snapshot id N does not exist: ...')
    The retry must self-heal this by reloading and recomputing the expire
    set via older_than against the post-overlap snapshot list. Pinning
    because catching ONLY CommitFailedException would turn this into a
    weekly false-positive snapshot_expiry_error on a multi-trigger fleet."""
    from backend.core import iceberg as _ice

    commit_attempts = {"n": 0}

    def _commit_side():
        commit_attempts["n"] += 1
        if commit_attempts["n"] == 1:
            return ValueError("Snapshot with snapshot id 12345 does not exist: ['67890', '11111']")
        return None

    table_initial, _ = _maintenance_table(snapshots_before=8, snapshots_after=8, commit_side_effect=_commit_side)
    table_reloaded, _ = _maintenance_table(snapshots_before=6, snapshots_after=3, commit_side_effect=_commit_side)

    catalog = MagicMock()
    load_calls = {"n": 0}

    def _catalog_load(_ident):
        load_calls["n"] += 1
        return table_initial if load_calls["n"] == 1 else table_reloaded

    catalog.load_table.side_effect = _catalog_load
    _patch_maintenance_deps(monkeypatch, table_initial, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert commit_attempts["n"] == 2, (
        f"expected 2 commit attempts (1 ValueError + 1 retry success); got {commit_attempts['n']}. "
        f"If 1, the retry never fired — catching only CommitFailedException would miss the "
        f"concurrent-expire ValueError shape."
    )
    assert load_calls["n"] == 2, f"expected 2 load_table calls; got {load_calls['n']}"
    assert "snapshot_expiry_error" not in result, (
        f"successful retry must NOT populate snapshot_expiry_error. Got: {result.get('snapshot_expiry_error')!r}"
    )
    assert result.get("snapshots_expired_count") == 3


def test_run_cloud_maintenance_does_not_retry_unrelated_value_errors(monkeypatch):
    """The ValueError retry must ONLY match the 'does not exist' message.
    A generic ValueError (real bug — schema mismatch, type error) must
    propagate immediately, not get retried 3 times before surfacing.
    Mirrors the optimize_table 'does not retry unrelated' invariant."""
    from backend.core import iceberg as _ice

    commit_attempts = {"n": 0}

    def _commit_side():
        commit_attempts["n"] += 1
        return ValueError("Some unrelated schema bug")

    table, _ = _maintenance_table(snapshots_before=5, snapshots_after=5, commit_side_effect=_commit_side)
    catalog = MagicMock()
    load_calls = {"n": 0}

    def _catalog_load(_ident):
        load_calls["n"] += 1
        return table

    catalog.load_table.side_effect = _catalog_load
    _patch_maintenance_deps(monkeypatch, table, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert commit_attempts["n"] == 1, (
        f"unrelated ValueError must propagate immediately, not trigger retry. Got {commit_attempts['n']} attempts."
    )
    assert load_calls["n"] == 1, f"unrelated ValueError must NOT trigger reload. Got {load_calls['n']} reloads."
    assert "snapshot_expiry_error" in result
    assert "unrelated schema bug" in result["snapshot_expiry_error"]


def test_run_cloud_maintenance_invalidates_pointer_cache_before_retry_reload(monkeypatch):
    """FosSqlCatalog.load_table consults a 2-sec _read_metadata_pointer
    cache (_POINTER_CACHE_TTL_SEC). Without explicit invalidation, all 3
    CAS retries finish in microseconds and read the same pre-conflict
    pointer entry — the retry exhausts without ever seeing post-conflict
    state. Pinning so a future refactor that drops the
    _pointer_cache_invalidate call re-introduces the silent CAS death-loop."""
    from pyiceberg.exceptions import CommitFailedException

    from backend.core import iceberg as _ice

    invalidate_calls: list = []
    real_invalidate = _ice._pointer_cache_invalidate

    def _spy_invalidate(src, ident):
        invalidate_calls.append((src.get("name"), ident))
        real_invalidate(src, ident)

    monkeypatch.setattr(_ice, "_pointer_cache_invalidate", _spy_invalidate)

    commit_attempts = {"n": 0}

    def _commit_side():
        commit_attempts["n"] += 1
        if commit_attempts["n"] == 1:
            return CommitFailedException("pointer race")
        return None

    table_initial, _ = _maintenance_table(snapshots_before=10, snapshots_after=10, commit_side_effect=_commit_side)
    table_reloaded, _ = _maintenance_table(snapshots_before=10, snapshots_after=7, commit_side_effect=_commit_side)

    catalog = MagicMock()
    load_calls = {"n": 0}

    def _catalog_load(_ident):
        load_calls["n"] += 1
        return table_initial if load_calls["n"] == 1 else table_reloaded

    catalog.load_table.side_effect = _catalog_load
    _patch_maintenance_deps(monkeypatch, table_initial, catalog)

    _ice.run_cloud_maintenance(_maintenance_source())

    # _pointer_cache_invalidate must be called BEFORE each retry reload
    # (i.e. at least once for the single CAS conflict in this test).
    assert any(call[0] == "expire-test" for call in invalidate_calls), (
        f"_pointer_cache_invalidate must be called before catalog.load_table retry to bypass the "
        f"2-sec pointer cache. Got invalidate_calls={invalidate_calls}. Without this, the retry "
        f"reads the same pre-conflict cache entry 3 times and exhausts in microseconds."
    )


def test_run_cloud_maintenance_records_snapshots_before_even_on_cas_exhaustion(monkeypatch):
    """When every CAS retry fails, results must still surface snapshots_before
    (so operators can see the snapshot pile size at the moment of failure,
    which is when they MOST need that signal). snapshots_after and
    snapshots_expired_count must NOT be present — reporting a bogus 0 there
    would lie about what happened."""
    from pyiceberg.exceptions import CommitFailedException

    from backend.core import iceberg as _ice

    def _always_cas():
        return CommitFailedException("Table has been updated by another process: ns.tbl")

    table, _ = _maintenance_table(snapshots_before=42, snapshots_after=42, commit_side_effect=_always_cas)
    catalog = MagicMock()
    catalog.load_table.return_value = table
    _patch_maintenance_deps(monkeypatch, table, catalog)

    result = _ice.run_cloud_maintenance(_maintenance_source())

    assert "snapshot_expiry_error" in result, (
        f"on CAS exhaustion the error must surface in snapshot_expiry_error. Got: {result}"
    )
    assert result.get("snapshots_before") == 42, (
        f"snapshots_before must be reported even when commit ultimately fails (it's the operator's "
        f"key signal at failure time). Got: {result.get('snapshots_before')}"
    )
    assert "snapshots_after" not in result, (
        "snapshots_after must NOT be reported on failure — would mislead about actual end state"
    )
    assert "snapshots_expired_count" not in result, (
        "snapshots_expired_count must NOT be reported on failure — would falsely claim expirations succeeded"
    )


def test_run_cloud_maintenance_does_not_invalidate_snapshot_files_cache_on_expire(monkeypatch):
    """expire_snapshots removes OLD snapshot metadata entries; the CURRENT
    snapshot's file membership is unchanged. So unlike step 1 (data_delete)
    and optimize_table, this step must NOT pop _snapshot_files_cache /
    _view_cache. A future 'helpful' refactor that adds the pops here would
    break the post-expire snapshot fast-path.

    Pinned because the discipline 'expire is metadata-only, do not bust the
    file-membership cache' is exactly the kind of invariant that gets
    accidentally violated during cleanup passes."""
    from backend import config as _svcconfig
    from backend.core import iceberg as _ice

    # Pre-populate the caches as a real workload would.
    _ice._snapshot_files_cache["expire-test"] = {"sentinel": "preserve-me"}
    _ice._view_cache["expire-test"] = {"sentinel": "preserve-me-too"}

    try:
        table, _ = _maintenance_table(snapshots_before=5, snapshots_after=3)
        catalog = MagicMock()
        catalog.load_table.return_value = table
        _patch_maintenance_deps(monkeypatch, table, catalog)
        # Disable step 1 (data deletion) so its cache pop doesn't mask step 2.
        monkeypatch.setattr(
            _svcconfig,
            "load_config",
            lambda sid: {"provisioning": {"cron_sync": {"data_retention_days": 0, "cache_retention_days": 0}}},
        )

        _ice.run_cloud_maintenance(_maintenance_source())

        assert _ice._snapshot_files_cache.get("expire-test") == {"sentinel": "preserve-me"}, (
            "expire_snapshots must NOT invalidate _snapshot_files_cache — current snapshot's file "
            "membership is unchanged. Bug would silently slow down every post-expire dashboard load."
        )
        assert _ice._view_cache.get("expire-test") == {"sentinel": "preserve-me-too"}, (
            "expire_snapshots must NOT invalidate _view_cache — same reason as above."
        )
    finally:
        _ice._snapshot_files_cache.pop("expire-test", None)
        _ice._view_cache.pop("expire-test", None)


# ── Finding 002: view-cache variant fingerprint ──────────────────────────


def test_source_variant_fp_changes_when_access_or_time_range_changes():
    """Finding 002 (2026-06-15): the iceberg view-SQL builder appends a
    WHERE clause derived from ``source["time_range"]`` and the analyst
    flag derived from ``source["access_level"]``. Without these in the
    ``_view_cache`` key, a cached SQL string from one variant could be
    served back to a request that wrote a different one — silently
    applying the wrong WHERE clause. ``_source_variant_fp`` is the
    fingerprint folded into the cache key tuple."""
    from backend.core.iceberg.view import _source_variant_fp

    base = {"name": "svc-002", "access_level": "read_write"}
    assert _source_variant_fp(base) == ("read_write", (), True)

    analyst = {"name": "svc-002", "access_level": "read_only"}
    assert _source_variant_fp(analyst) == ("read_only", (), True)
    assert _source_variant_fp(analyst) != _source_variant_fp(base), (
        "different access_level must produce different cache fingerprints"
    )

    bounded = {
        "name": "svc-002",
        "access_level": "read_only",
        "time_range": {"start": "2026-06-01T00:00:00Z", "end": "2026-06-02T00:00:00Z"},
    }
    fp_bounded = _source_variant_fp(bounded)
    assert fp_bounded != _source_variant_fp(analyst), (
        "adding a time_range must change the fingerprint so the cached SQL from the unbounded variant is not reused"
    )
    assert fp_bounded[1] == ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z")

    cron_disabled = {
        "name": "svc-002",
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": False}},
    }
    assert _source_variant_fp(cron_disabled) != _source_variant_fp(base), (
        "the same WHERE-clause branch fires when cron is disabled — fingerprint must capture it"
    )
