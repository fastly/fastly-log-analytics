"""Tests for ``backend.core.iceberg`` — pure helpers + small wrappers.

The big functions (`init_iceberg_table`, `commit_buffer`, `optimize_table`,
`sync_data`, `update_iceberg_view`) need a real PyIceberg catalog + S3
stack and are covered by the integration tests in
[test_iceberg.py](tests/core/test_iceberg.py).

This file pins the **pure helpers + small wrappers** that the bigger
code shares:

  - `_buffer_dir`, `_catalog_db_path` — per-source local paths
  - `get_arrow_schema`, `get_schema_field_names` — schema conversion
  - `_write_metadata_pointer` — S3 pointer write + CDN purge
  - `clear_source_caches` — module-global cache reset
  - `_load_persistent_cache` / `_save_persistent_cache` — snapshot
    files cache roundtrip
  - `get_last_view_stats` / `inject_view_debug` — view-cache reader
  - `_get_cache_file` — directory-creating cache file helper
  - `_get_service_lock` — per-source RLock factory
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

# ── _buffer_dir / _catalog_db_path / _get_cache_file ──────────────────────


def test_buffer_dir_is_under_cache_dir(tmp_path):
    """Buffer dir lives under the source's _cache_dir as ``/buffer``.
    Pinned because the commit_buffer job reads from this path —
    relocating without updating commit_buffer would silently lose
    every batch."""
    from backend.core.iceberg import _buffer_dir

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = _buffer_dir({"name": "svc"})
    assert out == str(tmp_path / "buffer")


def test_catalog_db_path_creates_cache_dir_if_missing(tmp_path):
    """The cache dir is mkdir'd before returning the catalog path
    (so PyIceberg's SQLite open() doesn't fail with ENOENT). Pinned
    because the first call after fresh provisioning hits this code
    path."""
    from backend.core.iceberg import _catalog_db_path

    target = tmp_path / "fresh-cache-dir"
    # Note: directory does NOT exist yet
    assert not target.exists()
    with patch("backend.core.duckdb._cache_dir", return_value=str(target)):
        out = _catalog_db_path({"name": "svc"})
    # The dir was created
    assert target.exists()
    # And the catalog path is inside
    assert out == str(target / "iceberg_catalog.db")


def test_get_cache_file_creates_cache_dir_and_returns_full_path(tmp_path):
    """`_get_cache_file` ensures the cache dir exists then returns the
    joined path. Pinned because callers do
    `with open(_get_cache_file(src, "x.json"), "w")` — a missing
    parent dir would crash."""
    from backend.core.iceberg import _get_cache_file

    target = tmp_path / "scratch"
    with patch("backend.core.duckdb._cache_dir", return_value=str(target)):
        out = _get_cache_file({"name": "svc"}, "my-cache.json")
    assert target.exists()
    assert out == str(target / "my-cache.json")


# ── get_arrow_schema / get_schema_field_names ────────────────────────────


def test_get_arrow_schema_returns_pyarrow_schema_with_standard_fields():
    """``get_arrow_schema(None)`` → pa.Schema with the standard
    preset fields. Pinned because the ingest staging path keys on
    this schema's field names for column projection."""
    import pyarrow as pa

    from backend.core.iceberg import get_arrow_schema

    schema = get_arrow_schema(None)
    assert isinstance(schema, pa.Schema)
    # Core fields are always present regardless of preset
    field_names = {f.name for f in schema}
    assert "timestamp" in field_names
    assert "status" in field_names


def test_get_schema_field_names_returns_set_of_field_strings():
    """`get_schema_field_names` returns a set (not a list) for fast
    membership checks. Pinned because ingest checks
    ``if "ip" in field_names`` on every row — O(1) set lookup
    matters."""
    from backend.core.iceberg import get_schema_field_names

    names = get_schema_field_names(None)
    assert isinstance(names, set)
    assert len(names) > 0


def test_get_arrow_schema_includes_enabled_custom_fields():
    """Enabled custom_fields land in the arrow schema. Pinned because
    losing this would silently drop custom-field columns during
    ingest write."""
    from backend.core.iceberg import get_arrow_schema

    cfg = {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "my_custom",
                "enabled": True,
                "duckdb_type": "VARCHAR",
                "iceberg_type": "string",
                "vcl_log_expression": "X",
            }
        ],
    }
    schema = get_arrow_schema(cfg)
    field_names = {f.name for f in schema}
    assert "my_custom" in field_names


def test_get_arrow_schema_omits_disabled_custom_fields():
    """`enabled=False` custom_fields don't appear in the schema.
    Pinned because the UI's "disable" toggle for a custom field must
    actually stop ingesting that column."""
    from backend.core.iceberg import get_arrow_schema

    cfg = {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "disabled_field",
                "enabled": False,
                "duckdb_type": "VARCHAR",
                "iceberg_type": "string",
                "vcl_log_expression": "X",
            }
        ],
    }
    schema = get_arrow_schema(cfg)
    field_names = {f.name for f in schema}
    assert "disabled_field" not in field_names


# ── _write_metadata_pointer (S3 PUT + CDN purge) ──────────────────────────


def test_write_metadata_pointer_writes_to_s3_with_short_cache_control():
    """The pointer file is written with `CacheControl: max-age=10`
    (short cache so analyst clients pick up new snapshots quickly).
    Pinned because losing the cache-control header would let CDNs
    serve stale pointers for the default TTL, hiding fresh data
    from analysts for hours."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()

    src = {
        "bucket": "test-bucket",
        "prefix": "",
        "name": "svc",
    }

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._write_table_summary_async"),
    ):
        _write_metadata_pointer(src, "s3://test-bucket/iceberg/metadata/v1.json")

    # PUT was called with the right shape
    fake_s3.put_object.assert_called_once()
    kwargs = fake_s3.put_object.call_args[1]
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"].endswith("metadata_location.txt")
    assert kwargs["CacheControl"] == "max-age=10"
    assert kwargs["Body"] == b"s3://test-bucket/iceberg/metadata/v1.json"


def test_write_metadata_pointer_includes_prefix_in_key():
    """When the source has a `prefix`, the pointer key lives under
    `{prefix}/iceberg/...`. Pinned because losing the prefix would
    overwrite another service's pointer in a shared bucket."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()
    src = {"bucket": "b", "prefix": "my-org", "name": "svc"}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._write_table_summary_async"),
    ):
        _write_metadata_pointer(src, "s3://b/x.json")

    key = fake_s3.put_object.call_args[1]["Key"]
    assert key.startswith("my-org/iceberg/")


def test_write_metadata_pointer_swallows_s3_exception():
    """If S3 PUT raises, the function logs and returns (doesn't
    propagate). Pinned because the pointer write happens AFTER the
    real Iceberg snapshot commit — an S3 transient failure here
    must not roll back the snapshot."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()
    fake_s3.put_object.side_effect = RuntimeError("S3 timeout")

    src = {"bucket": "b", "prefix": "", "name": "svc"}

    with patch("backend.core.duckdb._get_fos_client", return_value=fake_s3):
        # Should NOT raise
        _write_metadata_pointer(src, "s3://b/x.json")


def test_write_metadata_pointer_purges_cdn_surrogate_key_when_cdn_configured():
    """When the source has `cdn_service_id` AND an API key, purge the
    `iceberg-metadata-pointer` surrogate key on the CDN. Pinned
    because losing this would let CDN-cached pointers serve stale
    data for the cache-control max-age window."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()
    fake_fastly = MagicMock()

    src = {
        "bucket": "b",
        "prefix": "",
        "name": "svc",
        "cdn_service_id": "cdn-id",
    }

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._write_table_summary_async"),
        patch("backend.config.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        _write_metadata_pointer(src, "s3://b/x.json")

    # The CDN purge call was made
    assert fake_fastly.called
    args = fake_fastly.call_args[0]
    assert args[0] == "POST"
    assert "iceberg-metadata-pointer" in args[1]
    assert "/service/cdn-id/purge/" in args[1]


def test_write_metadata_pointer_swallows_cdn_purge_exception():
    """A failed CDN purge (network, auth) must not break the pointer
    write. Pinned because admins frequently rotate API keys and a
    stale token shouldn't prevent the Iceberg snapshot from being
    advertised to FOS."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()

    src = {"bucket": "b", "prefix": "", "name": "svc", "cdn_service_id": "cdn-id"}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._write_table_summary_async"),
        patch("backend.config.get_fastly_api_key", return_value="bad-tok"),
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("401 unauthorized")),
    ):
        # Should NOT raise
        _write_metadata_pointer(src, "s3://b/x.json")

    # S3 PUT still happened
    fake_s3.put_object.assert_called_once()


# ── clear_source_caches / _service_lock ──────────────────────────────────


def test_clear_source_caches_removes_view_and_snapshot_entries():
    """Clears both `_view_cache` and `_snapshot_files_cache` for a
    given source key. Pinned because teardown must purge in-memory
    state to prevent the next service-with-same-name from seeing
    stale schema/files."""
    import backend.core.iceberg as iceberg_mod

    # Seed both caches
    iceberg_mod._view_cache["test-svc"] = ("loc", set(), (), "sql", 1.0, True)
    iceberg_mod._snapshot_files_cache["test-svc"] = ("loc", 1, "iloc", [])

    iceberg_mod.clear_source_caches("test-svc")

    assert "test-svc" not in iceberg_mod._view_cache
    assert "test-svc" not in iceberg_mod._snapshot_files_cache


def test_clear_source_caches_is_noop_for_unknown_source():
    """Clearing an absent source doesn't raise. Pinned because
    teardown calls this unconditionally; raising on absent caches
    would mask the real teardown error."""
    from backend.core.iceberg import clear_source_caches

    # Should NOT raise
    clear_source_caches("never-seen-svc")


def test_get_service_lock_returns_same_lock_for_repeated_calls():
    """Per-source RLock factory: same source_key → same lock object.
    Pinned because losing the cache would create new locks each
    call, defeating the per-source serialization invariant
    `update_iceberg_view` relies on."""
    from backend.core.iceberg import _get_service_lock

    lock1 = _get_service_lock("svc-X")
    lock2 = _get_service_lock("svc-X")
    assert lock1 is lock2


def test_get_service_lock_returns_distinct_locks_for_different_sources():
    """Different source_keys → distinct lock objects. Pinned because
    sharing a lock across services would serialize all-service
    view updates (slow). Per-source isolation is the goal."""
    from backend.core.iceberg import _get_service_lock

    lock_a = _get_service_lock("svc-A")
    lock_b = _get_service_lock("svc-B")
    assert lock_a is not lock_b


# ── _load_persistent_cache / _save_persistent_cache ──────────────────────


def test_save_then_load_persistent_cache_roundtrips(tmp_path):
    """Save → Load roundtrips the snapshot files cache through the
    on-disk JSON. Pinned because the persistent cache survives
    process restarts so the dashboard's first query doesn't pay
    the full S3 manifest-resolve cost."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-svc"}
    iceberg_mod._snapshot_files_cache["test-svc"] = (
        "s3://b/meta.json",
        42,
        "s3://b/iceberg/",
        ["f1.parquet", "f2.parquet"],
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._save_persistent_cache(src)
        # Cache file was written
        cache_file = tmp_path / "snapshot_files_cache.json"
        assert cache_file.exists()

        # Round-trip: clear in-memory, then load from disk
        iceberg_mod._snapshot_files_cache.pop("test-svc", None)
        iceberg_mod._load_persistent_cache(src)
        loaded = iceberg_mod._snapshot_files_cache.get("test-svc")
        assert loaded is not None
        assert loaded[0] == "s3://b/meta.json"
        assert loaded[1] == 42
        assert loaded[3] == ["f1.parquet", "f2.parquet"]


def test_load_persistent_cache_skips_when_already_in_memory(tmp_path):
    """If the cache is already populated in-memory, don't re-read
    from disk. Pinned because the load is a no-op fast path —
    re-reading would burn an open() syscall on every dashboard
    render."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-svc-already"}
    iceberg_mod._snapshot_files_cache["test-svc-already"] = ("in-mem", 1, "i", [])

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        # Write a different value to disk
        (tmp_path / "snapshot_files_cache.json").write_text(
            json.dumps({"metadata_loc": "from-disk", "snapshot_id": 999, "iceberg_loc": "x", "local_iceberg_files": []})
        )
        iceberg_mod._load_persistent_cache(src)

    # The in-memory value is NOT clobbered by the disk value
    assert iceberg_mod._snapshot_files_cache["test-svc-already"][0] == "in-mem"


def test_load_persistent_cache_swallows_corrupt_json(tmp_path):
    """A malformed JSON file (mid-write crash) → silent no-op.
    Pinned because losing this would break the dashboard if a
    prior process crashed mid-flush."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-svc-corrupt"}
    iceberg_mod._snapshot_files_cache.pop("test-svc-corrupt", None)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        (tmp_path / "snapshot_files_cache.json").write_text("{not json")
        # Should NOT raise
        iceberg_mod._load_persistent_cache(src)

    # No entry was added
    assert "test-svc-corrupt" not in iceberg_mod._snapshot_files_cache


def test_save_persistent_cache_skips_when_no_in_memory_entry(tmp_path):
    """Saving when nothing's in `_snapshot_files_cache` for the
    source → no-op (don't write an empty file). Pinned because
    saving an empty entry would mask the "no snapshot yet" state
    on next process restart."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-svc-empty"}
    iceberg_mod._snapshot_files_cache.pop("test-svc-empty", None)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._save_persistent_cache(src)

    # No file was written
    assert not (tmp_path / "snapshot_files_cache.json").exists()


def test_save_persistent_cache_swallows_io_error(tmp_path):
    """A read-only cache dir → silent no-op (don't crash the
    snapshot pipeline). Pinned because losing this would break
    every snapshot commit on a Docker volume mounted read-only."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-svc-readonly"}
    iceberg_mod._snapshot_files_cache["test-svc-readonly"] = ("x", 1, "i", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("builtins.open", side_effect=OSError("Read-only filesystem")),
    ):
        # Should NOT raise
        iceberg_mod._save_persistent_cache(src)


# ── get_last_view_stats / inject_view_debug ──────────────────────────────


def test_get_last_view_stats_returns_empty_when_no_cache():
    """Unknown source → empty dict (debug-overlay-safe). Pinned
    because the dashboard's debug overlay calls this on every
    render — None would crash the overlay component."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache.pop("never-seen", None)
    out = iceberg_mod.get_last_view_stats({"name": "never-seen"})
    assert out == {}


def test_get_last_view_stats_returns_sql_time_and_path_mode():
    """Returns the cached SQL + time_ms + was_fast_path. Pinned
    because the debug overlay's "View Resolution" section renders
    these three exact fields."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache["test-svc-stats"] = (
        "metadata_loc",
        frozenset(),
        ("col1", "col2"),
        "CREATE VIEW x AS ...",
        12.3,
        True,  # was_fast_path
    )
    out = iceberg_mod.get_last_view_stats({"name": "test-svc-stats"})
    assert out == {"sql": "CREATE VIEW x AS ...", "time_ms": 12.3, "was_fast_path": True}


def test_inject_view_debug_prepends_view_resolution_to_debug_list():
    """`inject_view_debug` inserts a view-resolution row at the FRONT
    of the debug_queries list. Pinned because the dashboard's debug
    panel keys on the prepend position to render this above the
    user's main query."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache["test-svc-inject"] = (
        "loc",
        frozenset(),
        (),
        "SELECT 1",
        5.0,
        True,
    )
    debug_list = [{"sql": "user query", "time_ms": 100}]
    iceberg_mod.inject_view_debug(debug_list, {"name": "test-svc-inject"})

    # The view-resolution row is now at index 0
    assert "View Resolution" in debug_list[0]["sql"]
    assert "FAST PATH" in debug_list[0]["sql"]
    # The original user query is still at index 1
    assert debug_list[1]["sql"] == "user query"


def test_inject_view_debug_labels_slow_path_when_was_fast_path_false():
    """`was_fast_path=False` → "SLOW PATH" label. Pinned because
    the FE colors the row red on SLOW PATH to surface S3-fetch
    overhead in the debug overlay."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache["test-svc-slow"] = (
        "loc",
        frozenset(),
        (),
        "SELECT 1",
        50.0,
        False,  # was_fast_path
    )
    debug_list = []
    iceberg_mod.inject_view_debug(debug_list, {"name": "test-svc-slow"})

    assert "SLOW PATH" in debug_list[0]["sql"]


def test_inject_view_debug_is_noop_when_no_cached_stats():
    """No cached stats for the source → debug_list unchanged. Pinned
    because losing this would either crash or prepend an empty row
    that confuses admins."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache.pop("never-seen-inject", None)
    debug_list = [{"sql": "user query", "time_ms": 100}]
    iceberg_mod.inject_view_debug(debug_list, {"name": "never-seen-inject"})

    # Unchanged
    assert len(debug_list) == 1
    assert debug_list[0]["sql"] == "user query"


# ── table_location ──────────────────────────────────────────────────────


def test_table_location_returns_none_when_catalog_load_fails():
    """If `_get_catalog().load_table()` raises (table not initialised
    yet), return None. Pinned because callers do `if loc:` to
    distinguish a missing table from an empty location."""
    from backend.core.iceberg import table_location

    with patch("backend.core.iceberg._get_catalog", side_effect=RuntimeError("no catalog")):
        assert table_location({"name": "svc", "bucket": "b", "prefix": ""}) is None


def test_table_location_returns_s3_uri_from_loaded_table():
    """When the catalog + table load succeed, return the table's
    .location(). Pinned because the dashboard's "table location"
    indicator renders this URI verbatim."""
    from backend.core.iceberg import table_location

    fake_table = MagicMock()
    fake_table.location.return_value = "s3://my-bucket/iceberg/default/logs"
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    with patch("backend.core.iceberg._get_catalog", return_value=fake_catalog):
        out = table_location({"name": "svc", "bucket": "b", "prefix": ""})

    assert out == "s3://my-bucket/iceberg/default/logs"


# ── buffer_files ────────────────────────────────────────────────────────


def test_buffer_files_returns_empty_when_buffer_dir_missing(tmp_path):
    """No buffer dir → empty list (not crash). Pinned because the
    commit_buffer job calls this on every run; missing dir during
    first-run shouldn't error."""
    from backend.core.iceberg import buffer_files

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        # _buffer_dir = tmp_path/buffer (doesn't exist)
        assert buffer_files({"name": "svc"}) == []


def test_buffer_files_returns_sorted_parquet_files(tmp_path):
    """Returns parquet files sorted (stable commit order). Pinned
    because commit_buffer reads in this order — losing the sort
    would create non-deterministic Iceberg snapshots."""
    from backend.core.iceberg import buffer_files

    buf = tmp_path / "buffer"
    buf.mkdir()
    # Create out-of-order
    (buf / "c.parquet").write_bytes(b"")
    (buf / "a.parquet").write_bytes(b"")
    (buf / "b.parquet").write_bytes(b"")
    # Non-parquet ignored
    (buf / "ignore.txt").write_bytes(b"")

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        files = buffer_files({"name": "svc"})

    # Sorted by path
    names = [os.path.basename(f) for f in files]
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


def test_buffer_files_walks_subdirs_recursively(tmp_path):
    """Files in nested subdirs (e.g. partitioned writes) are included.
    Pinned because PyIceberg's write layout uses date-partitioned
    subdirs; flat-only walk would miss them."""
    from backend.core.iceberg import buffer_files

    buf = tmp_path / "buffer" / "2026" / "05" / "18"
    buf.mkdir(parents=True)
    (buf / "deep.parquet").write_bytes(b"")
    (tmp_path / "buffer" / "shallow.parquet").write_bytes(b"")

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        files = buffer_files({"name": "svc"})

    names = [os.path.basename(f) for f in files]
    assert "deep.parquet" in names
    assert "shallow.parquet" in names


# ── write_to_buffer ─────────────────────────────────────────────────────


def test_write_to_buffer_creates_buffer_dir_if_missing(tmp_path):
    """`write_to_buffer` ensures the buffer dir exists before writing.
    Pinned because the first ingest after teardown hits this path —
    a missing parent dir would crash pq.write_table."""
    import pyarrow as pa

    from backend.core.iceberg import write_to_buffer

    target = tmp_path / "fresh-cache"
    # target doesn't exist yet
    assert not target.exists()

    # Minimal table — just a timestamp col so _align_to_schema can resolve
    fake_table = pa.table({"timestamp": pa.array([], type=pa.timestamp("us", tz="UTC"))})

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(target)),
        patch("backend.core.iceberg._align_to_schema", return_value=fake_table),
        patch("backend.core.iceberg.pq.write_table") as mock_write,
    ):
        out = write_to_buffer({"name": "svc"}, fake_table, "x.parquet")

    # Dir was created
    assert (target / "buffer").exists()
    # Path returned is under buffer
    assert out == str(target / "buffer" / "x.parquet")
    mock_write.assert_called_once()


def test_write_to_buffer_uses_zstd_compression_level_1():
    """ZSTD level 1 is fast — buffer files are short-lived hot data
    so we prioritize write speed over compression ratio. Pinned
    because bumping to level 22 would 10x the ingest latency."""
    import pyarrow as pa

    from backend.core.iceberg import write_to_buffer

    fake_table = pa.table({"timestamp": pa.array([], type=pa.timestamp("us", tz="UTC"))})

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/x"),
        patch("backend.core.iceberg._align_to_schema", return_value=fake_table),
        patch("backend.core.iceberg.pq.write_table") as mock_write,
        patch("os.makedirs"),
    ):
        write_to_buffer({"name": "svc"}, fake_table, "x.parquet")

    _, kwargs = mock_write.call_args
    assert kwargs.get("compression") == "zstd"
    assert kwargs.get("compression_level") == 1


# ── commit_buffer no-op short-circuit ───────────────────────────────────


def test_commit_buffer_returns_zero_summary_when_no_buffer_files(tmp_path):
    """No buffer files → summary with zeros, no Iceberg interaction.
    Pinned because the commit cron fires every N minutes — losing
    this would create empty Iceberg snapshots on idle services."""
    from backend.core.iceberg import commit_buffer

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.iceberg._init_iceberg_table_locked") as mock_init,
    ):
        out = commit_buffer({"name": "svc"})

    assert out == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    # No Iceberg catalog access
    mock_init.assert_not_called()


# ── Quarantine of corrupt buffer files ─────────────────────────────────────


def test_quarantine_buffer_file_moves_into_subdir_with_sidecar(tmp_path):
    """A corrupt buffer parquet must be moved into ``.quarantine/`` with a
    sidecar JSON describing the failure. Pinned because the alternative
    (skip and leave in place) creates an infinite re-skip loop every
    commit cycle."""
    from backend.core.iceberg import _quarantine_buffer_file

    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)
    bad = buffer_dir / "batch_deadbeef.parquet"
    bad.write_bytes(b"not a parquet file")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        err = RuntimeError("parquet footer is invalid")
        new_path = _quarantine_buffer_file({"name": "svc"}, str(bad), err)

    assert new_path is not None
    assert ".quarantine" in new_path
    assert not bad.exists(), "original file must be moved, not copied"
    assert __import__("os").path.exists(new_path), "quarantined file must exist at new path"

    sidecar = new_path + ".json"
    assert __import__("os").path.exists(sidecar), "sidecar JSON must accompany quarantined file"
    sidecar_data = json.loads(open(sidecar).read())
    assert sidecar_data["error_type"] == "RuntimeError"
    assert "footer is invalid" in sidecar_data["error_message"]
    assert sidecar_data["original_path"] == str(bad)


def test_quarantine_handles_filename_collision(tmp_path):
    """Two quarantines of the same basename within the same second must not
    overwrite the earlier evidence."""
    from backend.core.iceberg import _quarantine_buffer_file

    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)
    bad1 = buffer_dir / "batch_x.parquet"
    bad1.write_bytes(b"first bad")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        p1 = _quarantine_buffer_file({"name": "svc"}, str(bad1), ValueError("first"))

    # Same basename, same timestamp window — quarantine a second file.
    bad2 = buffer_dir / "batch_x.parquet"
    bad2.write_bytes(b"second bad")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        p2 = _quarantine_buffer_file({"name": "svc"}, str(bad2), ValueError("second"))

    assert p1 != p2, "second quarantine must not overwrite first"
    assert __import__("os").path.exists(p1)
    assert __import__("os").path.exists(p2)


def test_commit_buffer_quarantines_unreadable_files_instead_of_skipping(tmp_path):
    """The hot-path integration: commit_buffer encounters a corrupt parquet,
    moves it into .quarantine/, increments the counter, and the good files
    are still committed normally. The corrupt file is NOT deleted from disk."""
    import pyarrow as pa

    from backend.core import iceberg as ice

    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)

    # Good file
    good = buffer_dir / "batch_good.parquet"
    good_table = pa.table({"timestamp": pa.array([1, 2, 3], type=pa.timestamp("us", tz="UTC"))})
    import pyarrow.parquet as pq

    pq.write_table(good_table, str(good))

    # Bad file (not a valid parquet)
    bad = buffer_dir / "batch_bad.parquet"
    bad.write_bytes(b"definitely not parquet")

    # Mock the Iceberg side so we don't actually need a catalog.
    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.properties = {"schema.name-mapping.default": "{}"}
    mock_table.current_snapshot.return_value = MagicMock(snapshot_id=999)
    mock_table.metadata_location = "s3://x/metadata.json"

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")),
        patch("backend.core.iceberg._init_iceberg_table_locked", return_value=mock_table),
        patch("backend.core.iceberg._align_to_schema", side_effect=lambda t, **kw: t),
        patch("backend.core.iceberg.schema_to_pyarrow", return_value=None, create=True),
        patch("backend.core.iceberg._set_cached_table"),
        patch("backend.core.iceberg._update_snapshot_cache_from_delta"),
        patch("backend.core.iceberg._write_metadata_pointer"),
    ):
        result = ice.commit_buffer({"name": "svc"})

    assert result["files_committed"] == 1
    assert result["quarantined_files"] == 1
    assert result["rows_committed"] == 3

    # Good file is deleted (committed).
    assert not good.exists()
    # Bad file is no longer at its original path — it was moved to quarantine.
    assert not bad.exists()
    quarantine_dir = buffer_dir / ".quarantine"
    assert quarantine_dir.exists()
    quarantined = list(quarantine_dir.glob("*batch_bad.parquet"))
    assert len(quarantined) == 1, f"expected one quarantined file, got {quarantined}"
    sidecar = list(quarantine_dir.glob("*.parquet.json"))
    assert len(sidecar) == 1


def test_commit_buffer_does_not_retry_quarantined_files_on_next_run(tmp_path):
    """After a corrupt file is quarantined, the next commit_buffer must
    NOT re-encounter it (because buffer_files() ignores the .quarantine
    subdir by way of the standard glob pattern)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.core import iceberg as ice

    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)

    bad = buffer_dir / "batch_bad.parquet"
    bad.write_bytes(b"not parquet")

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.properties = {"schema.name-mapping.default": "{}"}
    mock_table.current_snapshot.return_value = MagicMock(snapshot_id=1)
    mock_table.metadata_location = "s3://x/metadata.json"

    patches = [
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")),
        patch("backend.core.iceberg._init_iceberg_table_locked", return_value=mock_table),
        patch("backend.core.iceberg._align_to_schema", side_effect=lambda t, **kw: t),
        patch("backend.core.iceberg._set_cached_table"),
        patch("backend.core.iceberg._update_snapshot_cache_from_delta"),
        patch("backend.core.iceberg._write_metadata_pointer"),
    ]
    for p in patches:
        p.start()
    try:
        first = ice.commit_buffer({"name": "svc"})
        assert first["quarantined_files"] == 1

        # Second invocation should see ZERO buffer files — quarantined file
        # is in .quarantine/ which the glob does not match (only *.parquet
        # under buffer/, recursive=True would catch them, but the dot-prefix
        # convention is what we rely on for skip).
        good = buffer_dir / "batch_good.parquet"
        pq.write_table(pa.table({"timestamp": pa.array([1], type=pa.timestamp("us", tz="UTC"))}), str(good))
        second = ice.commit_buffer({"name": "svc"})
        assert second["quarantined_files"] == 0, (
            f"quarantined file was re-discovered (count={second['quarantined_files']})"
        )
    finally:
        for p in patches:
            p.stop()


def test_commit_buffer_chunks_appends_when_files_exceed_chunk_size(tmp_path, monkeypatch):
    """With chunk_size=3 and 7 buffer files, commit_buffer must call
    table.append() exactly 3 times (ceil(7/3)) and delete each chunk's
    files BEFORE moving to the next — that's the crash-safety
    invariant. Pinned because regressing to a single concat() would
    silently restore the OOM risk."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.core import iceberg as ice

    monkeypatch.setattr(ice, "_BUFFER_COMMIT_CHUNK_SIZE", 3)

    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)
    paths = []
    for i in range(7):
        p = buffer_dir / f"batch_{i:02d}.parquet"
        pq.write_table(pa.table({"timestamp": pa.array([i], type=pa.timestamp("us", tz="UTC"))}), str(p))
        paths.append(p)

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.properties = {"schema.name-mapping.default": "{}"}
    mock_table.current_snapshot.return_value = MagicMock(snapshot_id=102)
    mock_table.metadata_location = "s3://x/metadata.json"

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")),
        patch("backend.core.iceberg._init_iceberg_table_locked", return_value=mock_table),
        patch("backend.core.iceberg._align_to_schema", side_effect=lambda t, **kw: t),
        patch("backend.core.iceberg.schema_to_pyarrow", return_value=None, create=True),
        patch("backend.core.iceberg._set_cached_table"),
        patch("backend.core.iceberg._update_snapshot_cache_from_delta"),
        patch("backend.core.iceberg._write_metadata_pointer") as write_pointer,
    ):
        result = ice.commit_buffer({"name": "svc"})

    # 7 files, chunk_size=3 → 3 append() calls (3 + 3 + 1).
    assert mock_table.append.call_count == 3
    # Final snapshot id is the last chunk's.
    assert result["snapshot_id"] == 102
    assert result["files_committed"] == 7
    assert result["rows_committed"] == 7
    # All buffer files were deleted.
    for p in paths:
        assert not p.exists(), f"{p} was not cleaned up"
    # Pointer write happens ONCE — not per chunk — to keep CDN purges bounded.
    assert write_pointer.call_count == 1


# ── _write_table_summary_async (background S3 write) ──────────────────


def test_write_table_summary_async_spawns_daemon_thread():
    """`_write_table_summary_async` returns immediately + spawns a
    daemon thread. Pinned because losing daemon=True would prevent
    the process from exiting cleanly on shutdown."""
    import threading

    from backend.core.iceberg import _write_table_summary_async

    thread_starts = []
    original = threading.Thread.start

    def track_start(self):
        thread_starts.append(self.daemon)
        # Don't actually run — avoid the S3 calls
        return None

    with patch("threading.Thread.start", track_start):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""})

    # Thread was created as daemon (and we tracked its start)
    assert len(thread_starts) == 1
    assert thread_starts[0] is True


def test_write_table_summary_async_returns_immediately_without_waiting():
    """The function must NOT block on the thread — it spawns and
    returns. Pinned because callers are on the request hot path —
    blocking would add S3-roundtrip latency to every commit."""
    import time

    from backend.core.iceberg import _write_table_summary_async

    # Use a real thread but mock the inner work so it completes fast
    with (
        patch("backend.core.iceberg._get_catalog", side_effect=RuntimeError("skip")),
    ):
        start = time.monotonic()
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""})
        elapsed = time.monotonic() - start

    # Returned in well under 1 second (no blocking S3 call)
    assert elapsed < 0.5


def _run_thread_synchronously():
    """Context-manager-free patch: replace threading.Thread.start with one
    that calls self.run() inline. Returns the patch object so caller can
    `with` it. Used to drive _write_table_summary_async's _run() body in
    the calling thread so we can assert on what it touched."""

    def sync_start(self):
        self.run()

    return patch("threading.Thread.start", sync_start)


def test_write_table_summary_async_skips_catalog_load_when_table_passed():
    """When the caller passes the freshly-committed `table`, the async
    summary worker must NOT call `_get_catalog` or `catalog.load_table()`
    — those would re-GET the just-written ~850 KB metadata.json. Pinned
    because losing this would double the per-tick steady-state cost of
    the summary builder."""
    from backend.core import iceberg as _ice
    from backend.core.iceberg import _write_table_summary_async

    fake_table = MagicMock()
    fake_s3 = MagicMock()

    # Reset the per-process unchanged-payload hash cache so the put_object
    # assert isn't skipped by a sibling test having populated it first under
    # pytest-randomly ordering.
    _ice._table_summary_hash_cache.clear()

    with (
        _run_thread_synchronously(),
        patch("backend.core.iceberg._get_catalog") as mock_get_catalog,
        patch("backend.core.iceberg.get_table_info", return_value={"min_timestamp": "a", "max_timestamp": "b"}),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""}, table=fake_table)

    mock_get_catalog.assert_not_called()
    # The summary PUT still happens (the work isn't skipped — only the re-load is)
    fake_s3.put_object.assert_called_once()


def test_write_table_summary_async_loads_catalog_when_table_is_none():
    """When called without `table` (legacy path), the worker still falls
    back to `catalog.load_table()`. Pinned so the optimization stays
    opt-in — code paths that can't pass the table (e.g. an out-of-band
    dashboard refresher) still work."""
    from backend.core.iceberg import _write_table_summary_async

    fake_catalog = MagicMock()
    fake_loaded_table = MagicMock()
    fake_catalog.load_table.return_value = fake_loaded_table
    fake_s3 = MagicMock()

    with (
        _run_thread_synchronously(),
        patch("backend.core.iceberg._get_catalog", return_value=fake_catalog) as mock_get_catalog,
        patch("backend.core.iceberg.get_table_info", return_value={"min_timestamp": "a", "max_timestamp": "b"}),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""})

    mock_get_catalog.assert_called_once()
    fake_catalog.load_table.assert_called_once()


def test_write_table_summary_async_skips_put_when_payload_unchanged():
    """Hash-cached second call with identical (info, calendar) must skip the
    FOS PUT. Pinned because the throttle would silently regress to "always
    PUT" if the cache key/value derivation drifted."""
    from backend.core import iceberg as _ice
    from backend.core.iceberg import _write_table_summary_async

    fake_table = MagicMock()
    fake_s3 = MagicMock()
    _ice._table_summary_hash_cache.clear()

    with (
        _run_thread_synchronously(),
        patch("backend.core.iceberg.get_table_info", return_value={"min_timestamp": "a", "max_timestamp": "b"}),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""}, table=fake_table)
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""}, table=fake_table)

    assert fake_s3.put_object.call_count == 1


def test_write_table_summary_async_writes_again_when_payload_changes():
    """When info or calendar shifts (e.g. a new snapshot lands), the second
    call must PUT a fresh body. Pinned because a too-aggressive cache key
    (e.g. only on source identity) would stall the summary file."""
    from backend.core import iceberg as _ice
    from backend.core.iceberg import _write_table_summary_async

    fake_table = MagicMock()
    fake_s3 = MagicMock()
    _ice._table_summary_hash_cache.clear()

    with (
        _run_thread_synchronously(),
        patch("backend.core.iceberg.get_table_info", return_value={"min_timestamp": "a", "max_timestamp": "b"}),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""}, table=fake_table)
    with (
        _run_thread_synchronously(),
        patch("backend.core.iceberg.get_table_info", return_value={"min_timestamp": "a", "max_timestamp": "c"}),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        _write_table_summary_async({"name": "svc", "bucket": "b", "prefix": ""}, table=fake_table)

    assert fake_s3.put_object.call_count == 2


def test_write_metadata_pointer_forwards_table_to_summary_writer():
    """`_write_metadata_pointer` must thread the committed `table` through
    to `_write_table_summary_async`. Pinned because without forwarding,
    the optimization in Stream E is silently inert: callers pass `table`
    to the pointer writer but the summary worker never sees it."""
    from backend.core.iceberg import _write_metadata_pointer

    fake_s3 = MagicMock()
    fake_table = MagicMock()
    src = {"bucket": "b", "prefix": "", "name": "svc"}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._write_table_summary_async") as mock_async,
    ):
        _write_metadata_pointer(src, "s3://b/x.json", table=fake_table)

    mock_async.assert_called_once()
    # Confirm the table was forwarded (kwarg, not positional)
    assert mock_async.call_args.kwargs.get("table") is fake_table


# ── _read_metadata_pointer error swallow paths ─────────────────────────


def test_read_metadata_pointer_returns_none_on_s3_failure():
    """Failures from S3 GET → None. Pinned because the caller does
    `if loc:` to fall back to scanning manifests — None signals
    "use the slow path"."""
    from backend.core.iceberg import _read_metadata_pointer

    fake_s3 = MagicMock()
    fake_s3.get_object.side_effect = RuntimeError("Connection refused")

    with patch("backend.core.duckdb._get_fos_client", return_value=fake_s3):
        out = _read_metadata_pointer(
            {"name": "svc", "bucket": "b", "prefix": ""},
            ("default", "logs"),
        )

    assert out is None


# ── _load_table_cached (Stream G — post-commit metadata.json GET avoidance) ──


def test_load_table_cached_returns_cached_when_metadata_location_matches():
    """Cache hit: pointer matches cached table's metadata_location → return
    cached object, never touch the catalog. Pinned because this is the
    sole code path that eliminates the ~865 KB metadata.json refetch
    inside metadata_sync's init_iceberg_table after every commit."""
    import backend.core.iceberg as iceberg_mod

    src = {"bucket": "b", "prefix": "", "name": "svc-cache-hit"}
    identifier = ("default", "logs")

    cached = MagicMock()
    cached.metadata_location = "s3://b/iceberg/default/logs/metadata/01.metadata.json"
    iceberg_mod._set_cached_table(src, identifier, cached)

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = AssertionError("must not call catalog.load_table on cache hit")

    try:
        with (
            patch("backend.core.iceberg._read_metadata_pointer", return_value=cached.metadata_location),
            patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        ):
            result = iceberg_mod._load_table_cached(src, identifier)

        assert result is cached
        fake_catalog.load_table.assert_not_called()
    finally:
        iceberg_mod._invalidate_cached_table(src, identifier)


def test_load_table_cached_calls_catalog_when_pointer_mismatches():
    """Cache miss by mismatch: pointer advanced (another writer committed)
    → fall through to catalog.load_table and replace the cache entry.
    Pinned to keep the cross-process freshness invariant."""
    import backend.core.iceberg as iceberg_mod

    src = {"bucket": "b", "prefix": "", "name": "svc-cache-miss"}
    identifier = ("default", "logs")

    stale = MagicMock()
    stale.metadata_location = "s3://b/iceberg/default/logs/metadata/01.metadata.json"
    iceberg_mod._set_cached_table(src, identifier, stale)

    fresh = MagicMock()
    fresh.metadata_location = "s3://b/iceberg/default/logs/metadata/02.metadata.json"
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fresh

    try:
        with (
            patch("backend.core.iceberg._read_metadata_pointer", return_value=fresh.metadata_location),
            patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        ):
            result = iceberg_mod._load_table_cached(src, identifier)

        assert result is fresh
        fake_catalog.load_table.assert_called_once_with(identifier)
        # Cache now reflects the fresh table
        assert iceberg_mod._get_cached_table(src, identifier, fresh.metadata_location) is fresh
        assert iceberg_mod._get_cached_table(src, identifier, stale.metadata_location) is None
    finally:
        iceberg_mod._invalidate_cached_table(src, identifier)


def test_load_table_cached_calls_catalog_when_cache_empty():
    """Cold cache: must call catalog.load_table once and store the result.
    Pinned to confirm the fast path is opt-in via prior population, not
    eager."""
    import backend.core.iceberg as iceberg_mod

    src = {"bucket": "b", "prefix": "", "name": "svc-cache-empty"}
    identifier = ("default", "logs")
    iceberg_mod._invalidate_cached_table(src, identifier)

    loaded = MagicMock()
    loaded.metadata_location = "s3://b/iceberg/default/logs/metadata/01.metadata.json"
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = loaded

    try:
        with (
            patch("backend.core.iceberg._read_metadata_pointer", return_value=loaded.metadata_location),
            patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        ):
            result = iceberg_mod._load_table_cached(src, identifier)

        assert result is loaded
        fake_catalog.load_table.assert_called_once_with(identifier)
        assert iceberg_mod._get_cached_table(src, identifier, loaded.metadata_location) is loaded
    finally:
        iceberg_mod._invalidate_cached_table(src, identifier)


def test_load_table_cached_falls_through_when_pointer_returns_none():
    """Pointer read fails (None): cannot validate cache freshness, so call
    catalog.load_table. Pinned because the pointer is the integrity check
    — without it we must trust the catalog, not stale local state."""
    import backend.core.iceberg as iceberg_mod

    src = {"bucket": "b", "prefix": "", "name": "svc-no-pointer"}
    identifier = ("default", "logs")

    # Even with something in cache, no pointer means we cannot prove freshness
    stale = MagicMock()
    stale.metadata_location = "s3://b/iceberg/default/logs/metadata/01.metadata.json"
    iceberg_mod._set_cached_table(src, identifier, stale)

    fresh = MagicMock()
    fresh.metadata_location = "s3://b/iceberg/default/logs/metadata/02.metadata.json"
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fresh

    try:
        with (
            patch("backend.core.iceberg._read_metadata_pointer", return_value=None),
            patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        ):
            result = iceberg_mod._load_table_cached(src, identifier)

        assert result is fresh
        fake_catalog.load_table.assert_called_once_with(identifier)
    finally:
        iceberg_mod._invalidate_cached_table(src, identifier)


# ── _update_snapshot_cache_from_delta ────────────────────────────────────


def _make_fake_table(metadata_loc, snapshot_id, parent_id, location, manifests):
    """Build a minimal stand-in for a PyIceberg Table sufficient for the
    delta helper: it touches table.current_snapshot(), table.metadata_location,
    table.location(), table.io, plus snap.manifests(io) and
    snap.parent_snapshot_id / snap.snapshot_id."""
    fake_snap = MagicMock()
    fake_snap.snapshot_id = snapshot_id
    fake_snap.parent_snapshot_id = parent_id
    fake_snap.manifests.return_value = manifests

    fake_table = MagicMock()
    fake_table.metadata_location = metadata_loc
    fake_table.location.return_value = location
    fake_table.current_snapshot.return_value = fake_snap
    fake_table.io = MagicMock()
    return fake_table


def _make_fake_manifest(added_snapshot_id, has_added_files, entries):
    """Stand-in for ManifestFile. entries is a list of (status, file_path) tuples."""
    from pyiceberg.manifest import ManifestEntryStatus

    fake_entries = []
    for status, file_path in entries:
        e = MagicMock()
        e.status = status if isinstance(status, int) else ManifestEntryStatus.ADDED
        e.data_file.file_path = file_path
        fake_entries.append(e)

    m = MagicMock()
    m.added_snapshot_id = added_snapshot_id
    m.has_added_files = has_added_files
    m.fetch_manifest_entry.return_value = fake_entries
    return m


def test_update_snapshot_cache_from_delta_appends_added_files(tmp_path):
    """Happy path: parent_snapshot_id matches the cached snapshot,
    so we fetch the one new manifest's added entries and append them
    to the cached file list. Pinned because this is the
    cost-reduction lever — re-reading every manifest on every commit
    is what we are deliberately avoiding."""
    from pyiceberg.manifest import ManifestEntryStatus

    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-delta-svc"}
    iceberg_mod._snapshot_files_cache["test-delta-svc"] = (
        "s3://b/old-md.json",
        100,
        "s3://b/iceberg/",
        ["s3://b/iceberg/data/old.parquet"],
    )

    new_manifest = _make_fake_manifest(
        added_snapshot_id=200,
        has_added_files=True,
        entries=[
            (ManifestEntryStatus.ADDED, "s3://b/iceberg/data/timestamp_hour=2026-05-20-00/new1.parquet"),
            (ManifestEntryStatus.ADDED, "s3://b/iceberg/data/timestamp_hour=2026-05-20-00/new2.parquet"),
        ],
    )
    fake_table = _make_fake_table(
        metadata_loc="s3://b/new-md.json",
        snapshot_id=200,
        parent_id=100,
        location="s3://b/iceberg/",
        manifests=[new_manifest],
    )

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is True
        cached = iceberg_mod._snapshot_files_cache["test-delta-svc"]
        assert cached[0] == "s3://b/new-md.json"
        assert cached[1] == 200
        # Old + 2 new = 3 entries; new ones land as s3:// URIs because no
        # local files exist on disk for them.
        assert len(cached[3]) == 3
        assert cached[3][0] == "s3://b/iceberg/data/old.parquet"
        assert all("new" in p for p in cached[3][1:])
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-delta-svc", None)


def test_update_snapshot_cache_from_delta_rejects_non_linear_history(tmp_path):
    """If the new snapshot's parent != cached snapshot_id, we may have
    missed an intermediate commit (concurrent writer, process restart
    between commits). The helper MUST refuse the shortcut and let the
    caller fall back to a full scan — silently dropping files would
    surface much later as missing rows in the dashboard."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-skip-svc"}
    iceberg_mod._snapshot_files_cache["test-skip-svc"] = (
        "s3://b/md.json",
        100,
        "s3://b/iceberg/",
        ["s3://b/iceberg/data/old.parquet"],
    )

    fake_table = _make_fake_table(
        metadata_loc="s3://b/md2.json",
        snapshot_id=300,
        parent_id=150,  # parent is 150, not the cached 100 — gap detected
        location="s3://b/iceberg/",
        manifests=[],
    )

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is False
        # Cache must be left untouched so the caller knows to rebuild
        cached = iceberg_mod._snapshot_files_cache["test-skip-svc"]
        assert cached[0] == "s3://b/md.json"
        assert cached[1] == 100
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-skip-svc", None)


def test_update_snapshot_cache_from_delta_returns_false_without_existing_cache(tmp_path):
    """No prior cache entry → we can't delta-update (we don't know the
    previous file list). Return False so the caller does a full scan."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-empty-svc"}
    iceberg_mod._snapshot_files_cache.pop("test-empty-svc", None)

    fake_table = _make_fake_table("md", 1, None, "loc", [])

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

    assert updated is False
    assert "test-empty-svc" not in iceberg_mod._snapshot_files_cache


def test_update_snapshot_cache_from_delta_handles_schema_only_commit(tmp_path):
    """A snapshot with no new manifests (schema-only commit) still
    advances metadata_loc and snapshot_id but keeps the prev file list.
    Pinned so a schema change doesn't invalidate the cache and force
    an avoidable full rescan."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-schema-svc"}
    iceberg_mod._snapshot_files_cache["test-schema-svc"] = (
        "s3://b/md.json",
        100,
        "s3://b/iceberg/",
        ["s3://b/iceberg/data/a.parquet", "s3://b/iceberg/data/b.parquet"],
    )

    # Manifest whose added_snapshot_id is NOT 200 → filtered out as "not from this commit"
    stale_manifest = _make_fake_manifest(added_snapshot_id=99, has_added_files=True, entries=[])
    fake_table = _make_fake_table("s3://b/md2.json", 200, 100, "s3://b/iceberg/", [stale_manifest])

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is True
        cached = iceberg_mod._snapshot_files_cache["test-schema-svc"]
        assert cached[0] == "s3://b/md2.json"
        assert cached[1] == 200
        assert len(cached[3]) == 2  # file list preserved
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-schema-svc", None)


def test_update_snapshot_cache_from_delta_prefers_local_path_when_file_on_disk(tmp_path):
    """When a newly-added file already exists locally (rare for a brand-
    new commit, but possible if sync_data happened in parallel), the
    cache entry should record the local path — not the s3:// URI —
    so the view-builder uses read_parquet instead of iceberg_scan."""
    from pyiceberg.manifest import ManifestEntryStatus

    import backend.core.iceberg as iceberg_mod

    # Pre-create the local file the manifest will reference
    part_dir = tmp_path / "data" / "timestamp_hour=2026-05-20-01"
    part_dir.mkdir(parents=True)
    local_file = part_dir / "new.parquet"
    local_file.write_bytes(b"")

    src = {"name": "test-local-svc"}
    iceberg_mod._snapshot_files_cache["test-local-svc"] = (
        "md1",
        100,
        "loc",
        [],
    )

    new_manifest = _make_fake_manifest(
        added_snapshot_id=200,
        has_added_files=True,
        entries=[
            (
                ManifestEntryStatus.ADDED,
                "s3://b/iceberg/data/timestamp_hour=2026-05-20-01/new.parquet",
            ),
        ],
    )
    fake_table = _make_fake_table("md2", 200, 100, "loc", [new_manifest])

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is True
        cached = iceberg_mod._snapshot_files_cache["test-local-svc"]
        assert len(cached[3]) == 1
        # Entry must be the local path, not the s3:// URI
        assert cached[3][0] == str(local_file)
        assert not cached[3][0].startswith("s3://")
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-local-svc", None)


def test_update_snapshot_cache_from_delta_analyst_skips_uri_when_no_local(tmp_path):
    """Analyst (read_only) sources must NEVER record s3:// URIs in the
    cache — if the local file isn't present, the entry is skipped
    entirely. Pinned because the rule "analysts never read from cloud"
    is the cost guardrail; recording a URI would silently leak a
    cloud read on the next dashboard render."""
    from pyiceberg.manifest import ManifestEntryStatus

    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-analyst-svc", "access_level": "read_only"}
    iceberg_mod._snapshot_files_cache["test-analyst-svc"] = ("md1", 100, "loc", [])

    new_manifest = _make_fake_manifest(
        added_snapshot_id=200,
        has_added_files=True,
        entries=[
            (ManifestEntryStatus.ADDED, "s3://b/iceberg/data/missing.parquet"),
        ],
    )
    fake_table = _make_fake_table("md2", 200, 100, "loc", [new_manifest])

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is True
        cached = iceberg_mod._snapshot_files_cache["test-analyst-svc"]
        # Missing-on-disk + analyst = entry dropped entirely (no s3:// leak)
        assert cached[3] == []
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-analyst-svc", None)


# ── _reconcile_snapshot_cache_after_sync ────────────────────────────────


def test_reconcile_snapshot_cache_flips_uris_to_local_paths(tmp_path):
    """After sync_data downloads a file referenced by s3:// URI in the
    cache, the next view build should see the local path instead.
    Pinned because without this, the union-builder's local-vs-S3
    selection rule would keep falling through to iceberg_scan even
    after the file is on disk."""
    import backend.core.iceberg as iceberg_mod

    # Create the local file that sync_data would have downloaded
    part_dir = tmp_path / "data" / "timestamp_hour=2026-05-20-02"
    part_dir.mkdir(parents=True)
    downloaded = part_dir / "f.parquet"
    downloaded.write_bytes(b"")

    src = {"name": "test-rec-svc"}
    iceberg_mod._snapshot_files_cache["test-rec-svc"] = (
        "md",
        100,
        "loc",
        [
            "s3://b/iceberg/data/timestamp_hour=2026-05-20-02/f.parquet",  # just downloaded
            "s3://b/iceberg/data/timestamp_hour=2026-05-20-02/missing.parquet",  # still missing
            str(tmp_path / "data" / "already_local.parquet"),  # already-local entry
        ],
    )

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            iceberg_mod._reconcile_snapshot_cache_after_sync(src)

        cached = iceberg_mod._snapshot_files_cache["test-rec-svc"]
        assert cached[3][0] == str(downloaded)  # flipped to local
        assert cached[3][1].startswith("s3://")  # still URI
        assert cached[3][2] == str(tmp_path / "data" / "already_local.parquet")  # unchanged
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-rec-svc", None)


def test_reconcile_snapshot_cache_noop_when_cache_missing(tmp_path):
    """No cache entry for the source → silent no-op (don't raise).
    Pinned because sync_data's slow-path branch builds the cache from
    scratch; calling reconcile in that case shouldn't blow up if the
    cache wasn't seeded yet."""
    import backend.core.iceberg as iceberg_mod

    src = {"name": "never-cached"}
    iceberg_mod._snapshot_files_cache.pop("never-cached", None)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._reconcile_snapshot_cache_after_sync(src)  # must not raise

    assert "never-cached" not in iceberg_mod._snapshot_files_cache


# ── _manifest_metadata_cache (per-manifest aggregate reuse) ─────────────


def _make_calendar_manifest(manifest_path, data_files_with_partitions):
    """Stand-in for a ManifestFile whose fetch_manifest_entry returns
    entries with the given (status_name, partition_hour, file_size)
    tuples. partition_hour=None marks the entry as having no partition
    (which the scanner maps to date_str='unknown')."""

    fake_entries = []
    for status_name, hour_val, file_size in data_files_with_partitions:
        entry = MagicMock()
        entry.status.name = status_name
        if status_name == "DELETED":
            entry.data_file = None
        else:
            df = MagicMock()
            df.file_size_in_bytes = file_size
            df.partition = [hour_val] if hour_val is not None else []
            entry.data_file = df
        fake_entries.append(entry)

    m = MagicMock()
    m.manifest_path = manifest_path
    m.has_added_files = True
    m.has_existing_files = False
    m.fetch_manifest_entry.return_value = fake_entries
    return m


def test_manifest_metadata_cache_reuses_aggregate_across_snapshots(tmp_path):
    """A given manifest is fetched (its .avro entries parsed) at most
    once per process lifetime. The second scan that includes the same
    manifest must use the cached aggregate — no second
    fetch_manifest_entry call. Pinned because this is the cost win:
    after a commit, only the ONE new manifest costs a cloud .avro GET;
    the other ~1199 hit the cache."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._ui_metadata_cache.pop("test-mfc-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-mfc-svc")

    # One manifest: 2 files in partition hour 492000 (2026-04-22 00:00 UTC)
    m1 = _make_calendar_manifest(
        "s3://b/m1.avro",
        [("ADDED", 492000, 1024), ("ADDED", 492000, 2048)],
    )
    m1_other = _make_calendar_manifest(
        "s3://b/m2.avro",
        [("ADDED", 492001, 4096)],
    )

    # Fake snapshot+table
    snap = MagicMock()
    snap.summary = {}
    snap.manifests.return_value = [m1, m1_other]

    table = MagicMock()
    table.metadata_location = "md-snap-1"
    table.current_snapshot.return_value = snap
    table.io = MagicMock()

    src = {"name": "test-mfc-svc"}

    # Patch _cache_dir to tmp_path so the persistence side-effect from
    # _get_cached_or_scan_metadata doesn't write to the shared cache/default/
    # location and pollute future test runs (or get polluted by past ones).
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._get_cached_or_scan_metadata(src, table)
        # First scan: both manifests fetched once
        assert m1.fetch_manifest_entry.call_count == 1
        assert m1_other.fetch_manifest_entry.call_count == 1

        # Simulate a new commit: outer cache invalidates (new metadata_loc)
        # but the same TWO manifests are still in the snapshot. The new
        # snapshot would normally also add a third manifest; we model only
        # the reuse half here to isolate that behavior.
        table.metadata_location = "md-snap-2"

        iceberg_mod._get_cached_or_scan_metadata(src, table)

    # Per-manifest cache must have prevented a second fetch of either
    assert m1.fetch_manifest_entry.call_count == 1, (
        f"manifest m1 was re-fetched on the next snapshot — per-manifest cache "
        f"is not engaging (call_count={m1.fetch_manifest_entry.call_count})"
    )
    assert m1_other.fetch_manifest_entry.call_count == 1

    iceberg_mod._ui_metadata_cache.pop("test-mfc-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-mfc-svc")


def test_manifest_metadata_cache_new_manifest_in_next_snapshot_costs_one_fetch(tmp_path):
    """When the next snapshot adds a new manifest, ONLY that new manifest
    is fetched — the old ones hit the cache. This pins the per-commit
    cost shape: 1 .avro fetch instead of N."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._ui_metadata_cache.pop("test-newmf-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-newmf-svc")

    old_m = _make_calendar_manifest("s3://b/old.avro", [("ADDED", 492000, 1024)])

    snap1 = MagicMock()
    snap1.summary = {}
    snap1.manifests.return_value = [old_m]
    table = MagicMock()
    table.metadata_location = "md-1"
    table.current_snapshot.return_value = snap1
    table.io = MagicMock()

    src = {"name": "test-newmf-svc"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._get_cached_or_scan_metadata(src, table)
        assert old_m.fetch_manifest_entry.call_count == 1

        # Next commit: new manifest joins the old one. Total = 1 fetch (new only).
        new_m = _make_calendar_manifest("s3://b/new.avro", [("ADDED", 492100, 2048)])
        snap2 = MagicMock()
        snap2.summary = {}
        snap2.manifests.return_value = [old_m, new_m]
        table.metadata_location = "md-2"
        table.current_snapshot.return_value = snap2

        iceberg_mod._get_cached_or_scan_metadata(src, table)

    assert old_m.fetch_manifest_entry.call_count == 1, "old manifest re-fetched"
    assert new_m.fetch_manifest_entry.call_count == 1, "new manifest must be fetched once"

    iceberg_mod._ui_metadata_cache.pop("test-newmf-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-newmf-svc")


def test_manifest_metadata_cache_correct_aggregate_after_partial_reuse(tmp_path):
    """When some manifests come from cache and some are freshly fetched,
    the aggregated calendar must reflect BOTH — partial-reuse must not
    silently drop the cached manifests' contributions."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._ui_metadata_cache.pop("test-agg-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-agg-svc")

    # Pre-seed: scan with one manifest first to warm the cache
    m_a = _make_calendar_manifest("s3://b/a.avro", [("ADDED", 492000, 100)])
    snap1 = MagicMock()
    snap1.summary = {}
    snap1.manifests.return_value = [m_a]
    table = MagicMock()
    table.metadata_location = "md-1"
    table.current_snapshot.return_value = snap1
    table.io = MagicMock()

    src = {"name": "test-agg-svc"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._get_cached_or_scan_metadata(src, table)

        # Now snap 2: m_a (cached) + m_b (fresh). The result must include both.
        m_b = _make_calendar_manifest("s3://b/b.avro", [("ADDED", 492100, 200)])
        snap2 = MagicMock()
        snap2.summary = {}
        snap2.manifests.return_value = [m_a, m_b]
        table.metadata_location = "md-2"
        table.current_snapshot.return_value = snap2

        data_files, size_bytes, calendar, _min, _max = iceberg_mod._get_cached_or_scan_metadata(src, table)

    assert data_files == 2, f"expected 2 data files across both manifests, got {data_files}"
    assert size_bytes == 300, f"expected 100+200=300 bytes, got {size_bytes}"
    assert len(calendar) == 2, f"expected 2 dates in calendar (one per partition hour), got {calendar}"

    iceberg_mod._ui_metadata_cache.pop("test-agg-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-agg-svc")


def test_manifest_metadata_cache_persists_across_restart(tmp_path):
    """A scan writes the live manifests' aggregates to disk; a subsequent
    process (simulated by clearing the in-memory cache and the
    "loaded" sentinel) sees a scan that does NOT re-fetch those
    manifests. Pinned because without this, every restart pays a
    ~1250-manifest cold-scan burst (~12 MB of .avro GETs)."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._ui_metadata_cache.pop("test-persist-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-persist-svc")

    m1 = _make_calendar_manifest("s3://b/persist-m1.avro", [("ADDED", 492000, 1024)])
    m2 = _make_calendar_manifest("s3://b/persist-m2.avro", [("ADDED", 492001, 2048)])
    snap = MagicMock()
    snap.summary = {}
    snap.manifests.return_value = [m1, m2]

    table = MagicMock()
    table.metadata_location = "md-persist-1"
    table.current_snapshot.return_value = snap
    table.io = MagicMock()

    src = {"name": "test-persist-svc"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        # First scan — cold; both manifests fetched, then persisted.
        iceberg_mod._get_cached_or_scan_metadata(src, table)
        assert m1.fetch_manifest_entry.call_count == 1
        assert m2.fetch_manifest_entry.call_count == 1
        assert (tmp_path / "manifest_metadata_cache.json").exists()

        # Simulate a restart: drop in-memory state AND the load sentinel
        # AND the outer metadata_loc cache (which lives in-memory only).
        iceberg_mod._manifest_metadata_cache.clear()
        iceberg_mod._manifest_metadata_loaded.discard("test-persist-svc")
        iceberg_mod._ui_metadata_cache.pop("test-persist-svc", None)

        # Next scan — warm via on-disk load. Neither manifest should be re-fetched.
        iceberg_mod._get_cached_or_scan_metadata(src, table)

    assert m1.fetch_manifest_entry.call_count == 1, (
        f"m1 re-fetched after restart — disk persistence did not engage "
        f"(call_count={m1.fetch_manifest_entry.call_count})"
    )
    assert m2.fetch_manifest_entry.call_count == 1, (
        f"m2 re-fetched after restart (call_count={m2.fetch_manifest_entry.call_count})"
    )

    iceberg_mod._ui_metadata_cache.pop("test-persist-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()
    iceberg_mod._manifest_metadata_loaded.discard("test-persist-svc")


def test_update_snapshot_cache_from_delta_preseeds_manifest_cache(tmp_path):
    """`_update_snapshot_cache_from_delta` reads each new manifest's
    entries to build its file list. It MUST also populate
    `_manifest_metadata_cache` for that manifest so the subsequent
    `_get_cached_or_scan_metadata` call (fired by
    `_write_table_summary_async` after every commit) doesn't re-GET
    the same .avro a few seconds later. Pinned because losing this
    re-introduces a per-tick wasted .avro GET (~10 KB)."""
    from pyiceberg.manifest import ManifestEntryStatus

    import backend.core.iceberg as iceberg_mod

    src = {"name": "test-preseed-svc"}
    iceberg_mod._snapshot_files_cache["test-preseed-svc"] = (
        "s3://b/old-md.json",
        100,
        "s3://b/iceberg/",
        [],
    )
    iceberg_mod._manifest_metadata_cache.pop("s3://b/preseed-m0.avro", None)

    new_manifest = _make_fake_manifest(
        added_snapshot_id=200,
        has_added_files=True,
        entries=[(ManifestEntryStatus.ADDED, "s3://b/iceberg/data/x.parquet")],
    )
    new_manifest.manifest_path = "s3://b/preseed-m0.avro"
    fake_table = _make_fake_table(
        metadata_loc="s3://b/new-md.json",
        snapshot_id=200,
        parent_id=100,
        location="s3://b/iceberg/",
        manifests=[new_manifest],
    )

    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
            patch.object(iceberg_mod, "_save_persistent_cache"),
        ):
            updated = iceberg_mod._update_snapshot_cache_from_delta(src, fake_table)

        assert updated is True
        assert "s3://b/preseed-m0.avro" in iceberg_mod._manifest_metadata_cache, (
            "delta update did not pre-seed the per-manifest cache — "
            "the subsequent scan_manifest call will re-GET this .avro"
        )
        # First call already happened (during delta). A second iteration would
        # mean we're double-fetching the same manifest in the same tick.
        assert new_manifest.fetch_manifest_entry.call_count == 1
    finally:
        iceberg_mod._snapshot_files_cache.pop("test-preseed-svc", None)
        iceberg_mod._manifest_metadata_cache.pop("s3://b/preseed-m0.avro", None)


def test_get_cached_or_scan_metadata_skips_manifest_preseeded_by_delta(tmp_path):
    """End-to-end pin: after a delta-update has pre-seeded the cache,
    a follow-up `_get_cached_or_scan_metadata` call for the same
    snapshot must NOT call `fetch_manifest_entry` on the pre-seeded
    manifest. This is the user-visible win — one fewer .avro GET
    per commit cycle."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._ui_metadata_cache.pop("test-skip-fetch-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()

    m1 = _make_calendar_manifest("s3://b/skip-m1.avro", [("ADDED", 492000, 1024)])
    # Pre-seed the per-manifest cache as the delta-update path would.
    iceberg_mod._manifest_metadata_cache["s3://b/skip-m1.avro"] = ({}, None, None, 1, 1024)

    snap = MagicMock()
    snap.summary = {}
    snap.manifests.return_value = [m1]
    table = MagicMock()
    table.metadata_location = "md-skip-1"
    table.current_snapshot.return_value = snap
    table.io = MagicMock()

    src = {"name": "test-skip-fetch-svc"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._get_cached_or_scan_metadata(src, table)

    assert m1.fetch_manifest_entry.call_count == 0, (
        f"manifest with pre-seeded cache should NOT be re-fetched (call_count={m1.fetch_manifest_entry.call_count})"
    )

    iceberg_mod._ui_metadata_cache.pop("test-skip-fetch-svc", None)
    iceberg_mod._manifest_metadata_cache.clear()


def test_manifest_metadata_cache_save_prunes_dropped_manifests(tmp_path):
    """Saving filters by `live_manifest_paths`, so manifests dropped by
    snapshot expiry don't accumulate in the on-disk cache. Pinned
    because without pruning, the JSON file grows monotonically and
    eventually dominates restart-time load cost."""
    import json

    import backend.core.iceberg as iceberg_mod

    iceberg_mod._manifest_metadata_cache.clear()

    iceberg_mod._manifest_metadata_cache["s3://b/keep.avro"] = ({}, None, None, 1, 100)
    iceberg_mod._manifest_metadata_cache["s3://b/expire.avro"] = ({}, None, None, 1, 100)

    src = {"name": "test-prune-svc"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg_mod._save_manifest_metadata_cache(src, ["s3://b/keep.avro"])

    payload = json.loads((tmp_path / "manifest_metadata_cache.json").read_text())
    assert "s3://b/keep.avro" in payload
    assert "s3://b/expire.avro" not in payload

    iceberg_mod._manifest_metadata_cache.clear()


# ── Stream I: _ImmutableWriteCacheTee (write-time bytes cache seed) ───────


class _FakeWriteHandle:
    """Minimal file-like stand-in for an s3fs.S3File write handle.

    Mirrors only the surface the tee actually touches: write(b), close(),
    and optional context-manager protocol. Tests inject a configurable
    close-fail to verify cache-poisoning protection.
    """

    def __init__(self, fail_on_close: bool = False):
        self.buf = bytearray()
        self.closed = False
        self.fail_on_close = fail_on_close

    def write(self, data):
        self.buf.extend(data if isinstance(data, (bytes, bytearray, memoryview)) else bytes(data))
        return len(data)

    def close(self):
        if self.fail_on_close:
            raise OSError("upload failed")
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _clear_manifest_cache():
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._manifest_bytes_cache.clear()
    # _manifest_cache_size is a module-level int; reset for test isolation.
    # The cache_put accounting reads/writes it via `global`, so reaching in
    # is the standard way to reset bookkeeping between tests.
    iceberg_mod._manifest_cache_size = 0


def test_immutable_write_cache_tee_seeds_cache_on_clean_close():
    """Writing through the tee to a successful close populates
    _manifest_bytes_cache under the canonical key — exactly what the
    subsequent _update_snapshot_cache_from_delta GET will look for."""
    import backend.core.iceberg as iceberg_mod

    _clear_manifest_cache()
    handle = _FakeWriteHandle()
    path = "s3://bucket/iceberg/default/logs/metadata/snap-12345.avro"
    payload = b"avro-bytes-payload-x" * 64

    tee = iceberg_mod._ImmutableWriteCacheTee(handle, path)
    tee.write(payload)
    tee.close()

    assert handle.closed, "tee.close() must invoke the underlying handle.close()"
    cached = iceberg_mod._cache_get(path)
    assert cached == payload, "cache should contain the just-written bytes under the canonical key"


def test_immutable_write_cache_tee_does_not_seed_on_close_failure():
    """If the underlying upload (handle.close) fails, the LRU must NOT
    be poisoned with bytes that never landed in FOS — a subsequent GET
    would otherwise return stale local bytes for a file that the cloud
    never received."""
    import backend.core.iceberg as iceberg_mod

    _clear_manifest_cache()
    handle = _FakeWriteHandle(fail_on_close=True)
    path = "s3://bucket/iceberg/default/logs/metadata/snap-doomed.avro"

    tee = iceberg_mod._ImmutableWriteCacheTee(handle, path)
    tee.write(b"will-not-land-in-fos")
    with pytest.raises(OSError):
        tee.close()

    assert iceberg_mod._cache_get(path) is None, "cache must stay empty when the underlying upload fails"


def test_immutable_write_cache_tee_canonicalizes_key_for_round_trip():
    """The reader looks up by canonical key (scheme + leading-slash
    stripped). The tee must seed under the same canonical form so a
    write at ``s3://b/...`` hits a read at ``b/...`` and vice versa.
    """
    import backend.core.iceberg as iceberg_mod

    _clear_manifest_cache()
    handle = _FakeWriteHandle()
    write_path = "s3://bucket/iceberg/default/logs/metadata/snap-roundtrip.avro"
    read_path = "bucket/iceberg/default/logs/metadata/snap-roundtrip.avro"
    payload = b"round-trip-bytes" * 32

    tee = iceberg_mod._ImmutableWriteCacheTee(handle, write_path)
    tee.write(payload)
    tee.close()

    assert iceberg_mod._cache_get(read_path) == payload, (
        "scheme-prefixed write should be visible to scheme-stripped read via _canonical_cache_key"
    )


def test_buffer_backlog_stats_empty_buffer(tmp_path):
    from backend.core import iceberg as ice

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "svc")):
        stats = ice.buffer_backlog_stats({"name": "svc"})
    assert stats == {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}


def test_buffer_backlog_stats_reports_count_bytes_age(tmp_path):
    """The hot signal: file_count, total_bytes, and oldest_age_seconds
    must all reflect actual on-disk state. Backdate the oldest file so the
    test isn't flaky on fast machines."""
    import os as _os
    import time as _time

    from backend.core import iceberg as ice

    cache_root = tmp_path / "svc"
    buf = cache_root / "buffer"
    buf.mkdir(parents=True)
    f1 = buf / "batch_a.parquet"
    f1.write_bytes(b"a" * 100)
    f2 = buf / "batch_b.parquet"
    f2.write_bytes(b"b" * 200)
    # Backdate f1 by 10 minutes
    past = _time.time() - 600
    _os.utime(str(f1), (past, past))

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        stats = ice.buffer_backlog_stats({"name": "svc"})
    assert stats["file_count"] == 2
    assert stats["total_bytes"] == 300
    assert stats["oldest_age_seconds"] >= 590  # within 10s of the 600s backdate
    assert stats["oldest_path"] == str(f1)


# Silence ruff unused-imports
import os  # noqa: E402

_ = MagicMock
_ = pytest
_ = os
