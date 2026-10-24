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
import os
from pathlib import Path
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


# ── Tombstone scheme: race-safe buffer file consumption ───────────────────


def _make_buffer(tmp_path, *names: str) -> tuple[dict, list[str]]:
    """Helper: pretend a source's buffer dir lives under tmp_path and
    create ``names`` as empty parquet files. Returns (src, paths)."""
    src = {"name": "svc-tomb"}
    buf = tmp_path / "buffer"
    buf.mkdir(parents=True, exist_ok=True)
    paths = []
    for n in names:
        p = buf / n
        p.write_bytes(b"")
        paths.append(str(p))
    return src, paths


def test_tombstone_buffer_files_writes_sidecar_marker_and_leaves_parquet(tmp_path):
    """Tombstoning a buffer parquet must (a) write a ``.consumed-<ts>``
    sidecar next to it and (b) leave the original file untouched. The
    race fix depends on the parquet staying readable for the grace
    window so any view bound BEFORE the tombstone can still query it.
    """
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_A.parquet", "batch_B.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        tombstoned = iceberg.tombstone_buffer_files(src, paths, ts=1717_000_000)

    assert tombstoned == paths
    for p in paths:
        assert os.path.exists(p), f"parquet must remain on disk after tombstone: {p}"
        assert os.path.exists(p + ".consumed-1717000000"), f"tombstone sidecar missing for {p}"


def test_tombstone_buffer_files_does_not_fallback_to_immediate_unlink_on_failure(tmp_path):
    """Regression: a prior implementation fell back to ``os.remove(path)``
    whenever marker creation raised. That fallback re-opened the race the
    grace window was added to close — a query bound to the parquet path
    just before the failed tombstone could read a half-deleted file. The
    contract is now log-and-skip: the parquet stays on disk, the next
    commit cycle retries the tombstone, and the failed path is NOT
    counted as tombstoned (so callers comparing lengths see the gap).
    """
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_fail.parquet")
    target_marker = paths[0] + ".consumed-3000000000"

    real_open = open

    def open_that_blocks_only_the_marker(file, mode="r", *args, **kwargs):
        if file == target_marker and mode == "x":
            raise PermissionError("simulated tombstone creation failure")
        return real_open(file, mode, *args, **kwargs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("builtins.open", side_effect=open_that_blocks_only_the_marker),
    ):
        tombstoned = iceberg.tombstone_buffer_files(src, paths, ts=3_000_000_000)

    # Failed path stays on disk — no fallback unlink.
    assert os.path.exists(paths[0]), (
        "parquet was unlinked despite tombstone failure — the immediate-unlink "
        "fallback was reintroduced. That fallback re-opens the in-flight-query "
        "race the tombstone grace window exists to prevent."
    )
    # And callers must see this path is NOT in the success list.
    assert paths[0] not in tombstoned, (
        "failed tombstone reported as success — callers comparing "
        "len(tombstoned) == len(paths) lose their atomicity check."
    )


def test_buffer_files_excludes_tombstoned_parquets(tmp_path):
    """``buffer_files()`` must filter out parquets that have a tombstone
    sibling. View rebuilds rely on this to stop binding paths that are
    about to be swept."""
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_keep.parquet", "batch_consumed.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, [paths[1]], ts=1717_000_000)
        listing = iceberg.buffer_files(src)

    assert listing == [paths[0]], f"tombstoned file leaked into buffer_files(): {listing}"


def test_buffer_files_excludes_tombstone_markers_themselves(tmp_path):
    """The glob picks up *.parquet — the tombstone sidecar is named
    ``...parquet.consumed-N``, NOT a ``.parquet``, so it must not appear.
    Defends against a future glob change that accidentally widens to
    include the markers."""
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_X.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, paths, ts=1717_000_000)
        # Sanity: make sure a marker actually exists on disk.
        assert os.path.exists(paths[0] + ".consumed-1717000000")
        listing = iceberg.buffer_files(src)

    for entry in listing:
        assert not entry.endswith("1717000000"), f"tombstone marker leaked: {entry}"


def test_sweep_tombstoned_buffer_files_skips_within_grace(tmp_path):
    """A tombstone younger than ``grace_seconds`` must NOT be swept —
    a view bound during that window could still need the file. This is
    the load-bearing invariant of the race fix."""
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_young.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, paths, ts=2_000_000_000)
        # now = ts + 10s, grace = 60s → must be skipped
        swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_010)

    assert swept == 0
    assert os.path.exists(paths[0]), "parquet should NOT be deleted within grace window"
    assert os.path.exists(paths[0] + ".consumed-2000000000"), "tombstone should still exist"


def test_sweep_tombstoned_buffer_files_unlinks_past_grace(tmp_path):
    """Past the grace window, the sweeper must unlink BOTH the parquet
    and the tombstone sidecar. Otherwise the buffer dir grows unbounded."""
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_old.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, paths, ts=2_000_000_000)
        # now = ts + 90s, grace = 60s → must sweep
        swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_090)

    assert swept == 1
    assert not os.path.exists(paths[0]), "parquet should be unlinked past grace"
    assert not os.path.exists(paths[0] + ".consumed-2000000000"), "tombstone should be unlinked past grace"


def test_sweep_tolerates_malformed_marker_filenames(tmp_path):
    """A garbage filename ending in ``.consumed-NaN`` must not crash
    the sweeper — production has had stray files from interrupted
    operations before, and a sweeper crash blocks every subsequent
    commit."""
    from backend.core import iceberg

    src, _paths = _make_buffer(tmp_path, "batch_X.parquet")
    buf = tmp_path / "buffer"
    (buf / "batch_X.parquet.consumed-notanumber").write_bytes(b"")
    (buf / "batch_X.parquet.consumed-").write_bytes(b"")  # empty ts

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_000)

    # The malformed markers stay — neither matches the strict tombstone shape.
    assert swept == 0


def test_tombstone_then_query_race_keeps_parquet_readable_during_grace(tmp_path):
    """End-to-end race regression: simulate the 2026-06-05 prod incident
    pattern — a query binds a view to a buffer parquet, the commit
    tombstones it, then the query reads the file. Pre-fix, the equivalent
    ``os.remove`` would have made the read fail with ``No files found``.
    With the tombstone scheme, the file stays on disk for the grace
    window so the in-flight query succeeds.
    """
    import duckdb

    from backend.core import iceberg

    src = {"name": "svc-race"}
    buf = tmp_path / "buffer"
    buf.mkdir(parents=True, exist_ok=True)
    parquet_path = str(buf / "batch_race.parquet")

    # Write a real parquet so DuckDB can actually read it.
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"v": [1, 2, 3]}), parquet_path)

    con = duckdb.connect(":memory:")
    try:
        with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
            # Step 1: "bind the view" — pin the path into a SQL string,
            # mirroring what update_iceberg_view does.
            view_sql = f"CREATE OR REPLACE VIEW race_view AS SELECT * FROM read_parquet('{parquet_path}')"
            con.execute(view_sql)

            # Step 2: commit tombstones the file (was ``os.remove`` pre-fix).
            iceberg.tombstone_buffer_files(src, [parquet_path], ts=2_000_000_000)

            # Step 3: query that was racing the commit — must succeed.
            row = con.execute("SELECT count(*) FROM race_view").fetchone()
            assert row == (3,), (
                "Query against view bound BEFORE tombstone must still succeed during grace. "
                "If this fails, the race fix has regressed and we're back to the 2026-06-05 incident."
            )

            # Step 4: buffer_files() correctly excludes the tombstoned file
            # so the NEXT view rebuild won't bind a doomed path.
            assert iceberg.buffer_files(src) == []

            # Step 5: after grace, sweep actually unlinks.
            swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_090)
            assert swept == 1
            assert not os.path.exists(parquet_path)
    finally:
        con.close()


def test_tombstone_logs_and_skips_on_marker_write_failure(tmp_path):
    """If creating the sidecar fails (disk full, EROFS, etc.), the
    parquet stays on disk and the failed path is NOT reported as
    tombstoned. The prior implementation fell back to immediate
    ``os.remove(path)``, which re-opened the in-flight-query race the
    grace window was designed to close — a query bound to the parquet
    just before the failed tombstone could read a half-deleted file.
    The new contract is log-and-skip: the next commit cycle retries
    the tombstone naturally, and an unbounded buffer dir size becomes
    the operational signal.
    """
    from backend.core import iceberg

    src, paths = _make_buffer(tmp_path, "batch_failwrite.parquet")
    target_marker = paths[0] + ".consumed-1717000000"
    real_open = open

    def _selective_boom(file, mode="r", *args, **kwargs):
        if file == target_marker and mode == "x":
            raise OSError("simulated EROFS")
        return real_open(file, mode, *args, **kwargs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        with patch("builtins.open", side_effect=_selective_boom):
            tombstoned = iceberg.tombstone_buffer_files(src, paths, ts=1717_000_000)

    # Failed path stays on disk — no fallback unlink.
    assert os.path.exists(paths[0]), (
        "parquet was unlinked despite tombstone failure — the immediate-unlink "
        "fallback was reintroduced, re-opening the in-flight-query race."
    )
    # And it must not appear in the success list.
    assert paths[0] not in tombstoned


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


def test_invalidate_service_caches_clears_every_cache():
    """``invalidate_service_caches`` must purge EVERY per-service cache, not just
    the view/snapshot ones ``clear_source_caches`` handles. Regression: a stale
    ``_table_object_cache`` entry survived teardown (which also called the old
    clearer with the service_id instead of the source name), so a same-process
    re-provision of the same bucket returned the cached Table and ``init_iceberg_table``
    SKIPPED creation — the table never landed in the fresh bucket."""
    import backend.core.iceberg as iceberg_mod
    from backend.core.iceberg import _core

    src = {"name": "inv-test-svc", "bucket": "inv-test-bucket", "prefix": ""}
    ident = _core._table_identifier(src)
    name = src["name"]

    # Seed every per-service cache.
    _core._catalog_cache[name] = object()
    _core._snapshot_files_cache[name] = ("loc", 1, "iloc", [])
    _core._view_cache[name] = ("loc", set(), (), "sql", 1.0, True)

    class _T:
        metadata_location = "meta-x"

    _core._set_cached_table(src, ident, _T())
    _core._pointer_cache[_core._pointer_cache_key(src, ident)] = (1.0, "meta-x")

    iceberg_mod.invalidate_service_caches(src)

    assert name not in _core._catalog_cache
    assert name not in _core._snapshot_files_cache
    assert name not in _core._view_cache
    assert _core._get_cached_table(src, ident, "meta-x") is None
    assert _core._pointer_cache_key(src, ident) not in _core._pointer_cache


def test_invalidate_service_caches_is_noop_for_unknown_source():
    """Teardown calls this unconditionally — an absent service must not raise."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod.invalidate_service_caches({"name": "never-seen", "bucket": "nb", "prefix": ""})


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


# ── _save_persistent_cache ───────────────────────────────────────────────


def test_save_persistent_cache_writes_json(tmp_path):
    """`_save_persistent_cache` serializes the snapshot files cache entry to
    on-disk JSON."""
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
        cache_file = tmp_path / "snapshot_files_cache.json"
        assert cache_file.exists()
        data = json.loads(cache_file.read_text())
        assert data["metadata_loc"] == "s3://b/meta.json"
        assert data["snapshot_id"] == 42
        assert data["local_iceberg_files"] == ["f1.parquet", "f2.parquet"]


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
    """Files in nested subdirs (e.g. partitioned writes) are included."""
    from backend.core.iceberg import buffer_files

    buf = tmp_path / "buffer" / "rum_perf" / "2026" / "05" / "18"
    buf.mkdir(parents=True)
    (buf / "deep.parquet").write_bytes(b"")
    (tmp_path / "buffer" / "rum_perf" / "shallow.parquet").write_bytes(b"")

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        files = buffer_files({"name": "svc"}, table_name="rum_perf")

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
        patch("backend.core.iceberg.buffer.pq.write_table") as mock_write,
        patch("os.rename"),
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
        patch("backend.core.iceberg.buffer.pq.write_table") as mock_write,
        patch("os.makedirs"),
        patch("os.rename"),
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


def _ducklake_source(tmp_path, prefix: str) -> dict:
    """Source dict backed by a real DuckLake commit target. A unique service_id
    per call keeps tables from colliding in the shared per-worker catalog."""
    import uuid

    name = f"{prefix}{uuid.uuid4().hex[:8]}"
    cache = tmp_path / f"cache-{name}"
    (cache / "buffer").mkdir(parents=True)
    return {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }


def _write_buffer_parquet(source: dict, filename: str, rows: int, offset: int = 0):
    from datetime import UTC, datetime, timedelta

    import pyarrow as pa
    import pyarrow.parquet as pq

    base = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
    table = pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(seconds=offset + i) for i in range(rows)], type=pa.timestamp("us", tz="UTC")
            ),
            "ip": pa.array([f"10.0.0.{offset + i}" for i in range(rows)]),
            "_source_file": pa.array([f"s3://b/raw/{filename}"] * rows),
        }
    )
    path = Path(source["_cache_dir_override"]) / "buffer" / filename
    pq.write_table(table, str(path))
    return path


def _lake_row_count(source: dict) -> int:
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import ducklake_table_name

    con = get_connection(source)
    try:
        row = con.execute(f'SELECT count(*) FROM lake."{ducklake_table_name(source)}"').fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


def test_commit_buffer_quarantines_unreadable_files_instead_of_skipping(tmp_path):
    """The hot-path integration: commit_buffer encounters a corrupt parquet,
    moves it into .quarantine/, increments the counter, and the good files
    are still committed normally (into a real DuckLake table). The corrupt
    file is NOT deleted from disk."""
    from backend.core import iceberg as ice

    source = _ducklake_source(tmp_path, "qh")
    good = _write_buffer_parquet(source, "batch_good.parquet", rows=3)
    buffer_dir = good.parent
    bad = buffer_dir / "batch_bad.parquet"
    bad.write_bytes(b"definitely not parquet")

    result = ice.commit_buffer(source)

    assert result["files_committed"] == 1
    assert result["quarantined_files"] == 1
    assert result["rows_committed"] == 3
    assert _lake_row_count(source) == 3

    # Good file is committed: the parquet stays on disk (tombstoned for
    # the grace window so any in-flight query bound before the commit
    # can still read it), but it now has a ``.consumed-<ts>`` sidecar
    # and ``buffer_files()`` excludes it from the next commit cycle.
    assert good.exists(), "committed parquet must remain on disk during the tombstone grace window"
    good_tombstones = list(good.parent.glob(good.name + ".consumed-*"))
    assert len(good_tombstones) == 1, f"expected one tombstone for committed file, got {good_tombstones}"
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
    from backend.core import iceberg as ice

    source = _ducklake_source(tmp_path, "qr")
    bad = Path(source["_cache_dir_override"]) / "buffer" / "batch_bad.parquet"
    bad.write_bytes(b"not parquet")

    first = ice.commit_buffer(source)
    assert first["quarantined_files"] == 1
    assert first["files_committed"] == 0

    # A later good file commits cleanly and the quarantined one is never
    # re-discovered (the .quarantine/ dir is outside buffer_files()).
    _write_buffer_parquet(source, "batch_good.parquet", rows=1)
    second = ice.commit_buffer(source)
    assert second["quarantined_files"] == 0, f"quarantined file was re-discovered (count={second['quarantined_files']})"
    assert second["files_committed"] == 1
    assert _lake_row_count(source) == 1


def test_commit_buffer_chunks_commits_when_files_exceed_chunk_size(tmp_path, monkeypatch):
    """With chunk_size=3 and 7 buffer files, commit_buffer must open a
    DuckLake write-attach exactly 3 times (ceil(7/3)) and tombstone every
    committed file. Pinned because regressing to a single whole-backlog
    commit would silently restore the OOM / exclusive-attach-starvation
    risk."""
    from backend.core import iceberg as ice
    from backend.core.iceberg import buffer as buffer_mod

    monkeypatch.setattr(buffer_mod, "_BUFFER_COMMIT_CHUNK_SIZE", 3)
    attaches = {"n": 0}
    orig = buffer_mod._ducklake_write_connection

    def counting(source):
        attaches["n"] += 1
        return orig(source)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", counting)

    source = _ducklake_source(tmp_path, "ck")
    paths = [_write_buffer_parquet(source, f"batch_{i:02d}.parquet", rows=1, offset=i) for i in range(7)]

    result = ice.commit_buffer(source)

    # 7 files, chunk_size=3 -> 3 write-attaches (3 + 3 + 1).
    assert attaches["n"] == 3
    assert result["files_committed"] == 7
    assert result["rows_committed"] == 7
    assert _lake_row_count(source) == 7
    # All buffer files were tombstoned — the parquets stay on disk until
    # the grace window elapses (see ``tombstone_buffer_files`` docstring),
    # but each one has a ``.consumed-<ts>`` sidecar and is filtered out
    # of ``buffer_files()`` so the next commit cycle won't re-process them.
    for p in paths:
        assert list(p.parent.glob(p.name + ".consumed-*")), f"{p} should have a tombstone sidecar after commit"
    assert ice.buffer_files(source) == [], (
        "tombstoned files must NOT appear in buffer_files() — they would be re-processed forever otherwise."
    )


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

_ = MagicMock
_ = pytest
_ = os
