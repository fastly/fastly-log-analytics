"""Tests for the per-service lock guarding commit_buffer / optimize_table /
run_cloud_maintenance against backend.core.reset.reset_service_logs.

Root cause of a production incident: reset_service_logs() checks
metadata_db.cron_busy() exactly once, then holds the per-service lock
(_get_service_lock) for its whole run while it purges iceberg/ from FOS.
But commit_buffer/optimize_table/run_cloud_maintenance never acquired that
same lock, so a commit already past the one-time cron_busy() check could
append a new snapshot while the reset's purge deleted the manifest files it
depended on — corrupting even the freshly-recreated table. These tests
pin that the three cloud writers now serialize against the same lock a
reset holds, mirroring test_concurrent_reset_is_excluded_by_service_lock in
test_reset.py (which pins reset's own side of the exclusion).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from backend.core import iceberg
from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg.view import _get_service_lock


# ── Merged from tests/core/test_iceberg.py ──
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


def _ducklake_source(tmp_path, prefix: str) -> dict:
    """Source dict backed by a real DuckLake catalog. A unique service_id per
    call keeps tables from colliding in the shared per-worker Postgres catalog."""
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


def test_init_iceberg_table_attaches_catalog_without_creating_table(tmp_path):
    """``init_iceberg_table`` only verifies the DuckLake catalog is attachable;
    the per-service table is created lazily by the first buffer commit (so
    ``create`` is inert). Calling it repeatedly is a no-op that must neither
    create a table nor fail."""
    from backend.core.iceberg.manifest import ducklake_table_exists

    source = _ducklake_source(tmp_path, "init")

    assert iceberg.init_iceberg_table(source) is True
    assert iceberg.init_iceberg_table(source, create=True) is True
    assert ducklake_table_exists(source) is False


def test_init_iceberg_table_returns_none_when_catalog_unattachable(tmp_path):
    """A failed attach must surface as ``None`` (not True) so callers can tell
    "catalog unavailable" from "catalog ready"."""
    source = _ducklake_source(tmp_path, "initfail")
    fake_con = MagicMock()

    with (
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=False),
    ):
        assert iceberg.init_iceberg_table(source) is None
    fake_con.close.assert_called_once()


def test_buffer_dir():
    source = {"name": "test-svc"}
    # backend.core.duckdb._cache_dir usually returns cache/{svc}
    with patch("backend.core.duckdb._cache_dir", return_value="cache/test-svc"):
        assert iceberg._buffer_dir(source) == "cache/test-svc/buffer"


def test_get_arrow_schema_no_custom_fields():
    """Returns base schema when no custom fields are configured."""
    base_schema = iceberg.get_arrow_schema()
    assert iceberg.get_arrow_schema(None) == base_schema
    assert iceberg.get_arrow_schema({}) == base_schema
    assert iceberg.get_arrow_schema({"custom_fields": []}) == base_schema


def test_get_arrow_schema_varchar_custom_field():
    """VARCHAR custom fields are added after the base fields."""
    config = {"custom_fields": [{"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True}]}
    schema = iceberg.get_arrow_schema(config)
    field_names = [f.name for f in schema]
    assert "my_field" in field_names
    assert field_names.index("my_field") > field_names.index("timestamp")

    field = schema.field("my_field")
    assert field.type == pa.string()


def test_get_arrow_schema_numeric_custom_fields():
    """INTEGER, BIGINT, and DOUBLE custom fields map to correct Arrow types (not string)."""
    config = {
        "custom_fields": [
            {"name": "cf_int", "duckdb_type": "INTEGER", "enabled": True},
            {"name": "cf_bigint", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "cf_double", "duckdb_type": "DOUBLE", "enabled": True},
        ]
    }
    schema = iceberg.get_arrow_schema(config)
    assert schema.field("cf_int").type == pa.int32()
    assert schema.field("cf_bigint").type == pa.int64()
    assert schema.field("cf_double").type == pa.float64()


def test_get_arrow_schema_disabled_field_excluded():
    """Disabled custom fields are not added to the schema."""
    config = {
        "custom_fields": [
            {"name": "active_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "inactive_field", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    schema = iceberg.get_arrow_schema(config)
    field_names = {f.name for f in schema}
    assert "active_field" in field_names
    assert "inactive_field" not in field_names


def test_get_arrow_schema_sort_order():
    """Custom fields are sorted alphabetically for deterministic field ordering."""
    config = {
        "custom_fields": [
            {"name": "zzz_last", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "aaa_first", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    }
    schema = iceberg.get_arrow_schema(config)
    field_names = [f.name for f in schema]
    assert field_names.index("aaa_first") < field_names.index("zzz_last")


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
    schema_names = {f.name for f in iceberg.get_arrow_schema()}
    unmaterialized = storage_ids - schema_names
    assert not unmaterialized, f"Storage fields absent from the emitted Arrow/DuckDB schema: {sorted(unmaterialized)}"


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


def _recording_write_connection(monkeypatch, fail_on: str | None = None):
    """Swap the DuckLake write-attach for a fake connection that records every
    SQL statement, so ``optimize_table``'s real control flow runs without a
    catalog. ``fail_on`` makes statements containing that text raise."""
    from contextlib import contextmanager

    from backend.core.iceberg import buffer as buffer_mod

    executed: list[str] = []

    class _Result:
        def fetchall(self):
            return []

    class _Con:
        def execute(self, sql, *a, **kw):
            executed.append(sql)
            if fail_on and fail_on in sql:
                raise RuntimeError("simulated optimize failure")
            return _Result()

    @contextmanager
    def fake(_source):
        yield _Con()

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", fake)
    monkeypatch.setattr(buffer_mod, "_lake_columns", lambda _con, _tbl: {"timestamp"})
    return executed


def test_optimize_table_hostile_service_id_cannot_break_out_of_call_literal(monkeypatch):
    """The compaction CALLs interpolate the table name inside a single-quoted
    SQL literal. ``ducklake_table_name`` must sanitize a hostile service id so
    a single quote can never terminate that literal (the DuckLake successor of
    the old read_parquet single-quote escaping guard)."""
    import re

    from backend.core import iceberg as _ice

    executed = _recording_write_connection(monkeypatch)
    source = {"name": "svc", "service_id": "O'Brien'); DROP TABLE lake.logs;--"}

    _ice.optimize_table(source, table_name="logs")

    calls = [q for q in executed if q.startswith("CALL ducklake_merge_adjacent_files")]
    assert calls, f"merge CALL was never issued: {executed}"
    for q in executed:
        if q.startswith("CALL ducklake_merge_adjacent_files") or q.startswith("CALL ducklake_rewrite_data_files"):
            assert re.fullmatch(r"CALL ducklake_\w+\('lake', '[a-z0-9_]+'\)", q), f"unsafe table literal in: {q}"
    assert not any("DROP" in q for q in executed)


def test_optimize_table_compacts_schema_evolved_files(tmp_path):
    """Regression for the 2026-06-06 optimize-cron warning: a table holding
    files written before AND after a schema bump (a column added mid-stream)
    must compact without error and without losing or corrupting rows —
    old rows read the new column as NULL, new rows keep their values."""
    from datetime import UTC, datetime, timedelta

    import pyarrow.parquet as pq

    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import ducklake_table_name
    from backend.core.iceberg.buffer import _commit_buffer_impl

    source = _ducklake_source(tmp_path, "evolve")
    buffer_dir = os.path.join(source["_cache_dir_override"], "buffer")
    base = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)

    def _write(name: str, offset: int, extra: dict | None = None):
        cols = {
            "timestamp": pa.array(
                [base + timedelta(seconds=offset + i) for i in range(2)], type=pa.timestamp("us", tz="UTC")
            ),
            "ip": pa.array([f"10.0.0.{offset + i}" for i in range(2)]),
            "_source_file": pa.array([f"s3://b/raw/{name}"] * 2),
        }
        for k, v in (extra or {}).items():
            cols[k] = pa.array([v] * 2)
        pq.write_table(pa.table(cols), os.path.join(buffer_dir, name))

    _write("batch_old_schema.parquet", 0)
    assert _commit_buffer_impl(source)["rows_committed"] == 2
    _write("batch_new_schema.parquet", 10, extra={"edge_sid": "evolved"})
    assert _commit_buffer_impl(source)["rows_committed"] == 2

    result = iceberg.optimize_table(source)

    assert not result.get("error"), result
    assert result["partition_errors"] == [], result["partition_errors"]
    con = get_connection(source)
    try:
        rows = con.execute(f'SELECT ip, edge_sid FROM lake."{ducklake_table_name(source)}" ORDER BY ip').fetchall()
    finally:
        con.close()
    assert rows == [
        ("10.0.0.0", None),
        ("10.0.0.1", None),
        ("10.0.0.10", "evolved"),
        ("10.0.0.11", "evolved"),
    ]


def test_optimize_table_merge_failure_is_recorded_not_retried_and_rewrite_still_runs(monkeypatch):
    """A failing compaction step must surface in ``partition_errors`` (so the
    cron row lands as a warning) without being silently retried, and must not
    stop the sibling rewrite step. The old Iceberg CAS-retry guard
    ("unrelated errors are not retried") maps onto this: one attempt, error
    reported."""
    from backend.core import iceberg as _ice

    executed = _recording_write_connection(monkeypatch, fail_on="ducklake_merge_adjacent_files")

    result = _ice.optimize_table({"name": "svc", "service_id": "svc-opt"}, table_name="logs")

    merges = [q for q in executed if "ducklake_merge_adjacent_files" in q and q.endswith("'logs_svc_opt')")]
    rewrites = [q for q in executed if "ducklake_rewrite_data_files" in q and q.endswith("'logs_svc_opt')")]
    assert len(merges) == 1, f"failed merge must not be retried; saw {len(merges)} attempts"
    assert len(rewrites) == 1, "rewrite must still run after a merge failure"
    assert any("merge" in e for e in result["partition_errors"]), result["partition_errors"]


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


# ── Merged from tests/core/test_iceberg_cloud_write_lock.py ──
@pytest.fixture
def lock_held_by_other_thread(fos_source):
    """Simulate reset_service_logs holding the per-service lock."""
    lock = _get_service_lock(fos_source["name"])
    held = threading.Event()
    release = threading.Event()

    def _hold():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    held.wait(timeout=5)
    yield
    release.set()
    holder.join(timeout=5)


def test_commit_buffer_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    result = buffer_mod.commit_buffer(fos_source)

    impl.assert_not_called()
    assert result == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}


def test_optimize_table_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_optimize_table_impl", impl)

    result = buffer_mod.optimize_table(fos_source)

    impl.assert_not_called()
    assert result["files_rewritten"] == 0
    assert "busy" in result["error"]


def test_run_cloud_maintenance_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_run_cloud_maintenance_impl", impl)

    result = buffer_mod.run_cloud_maintenance(fos_source)

    impl.assert_not_called()
    assert "busy" in result["error"]


def test_commit_buffer_calls_impl_when_lock_free(monkeypatch, fos_source):
    """The wrapper must not change behavior on the normal, uncontended path."""
    impl = MagicMock(
        return_value={"files_committed": 3, "rows_committed": 30, "snapshot_id": 1, "quarantined_files": 0}
    )
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    result = buffer_mod.commit_buffer(fos_source)

    impl.assert_called_once_with(fos_source, None, table_name="logs")
    assert result["files_committed"] == 3

    # Lock must be released afterward — a later contender shouldn't block.
    lock = _get_service_lock(fos_source["name"])
    assert lock.acquire(timeout=0.1)
    lock.release()


def test_reset_excludes_a_concurrent_commit_attempt(monkeypatch, s3_mock, fos_source):
    """Integration-shaped version of the actual incident: while
    reset_service_logs holds the lock, a commit_buffer call racing it must
    be excluded rather than proceeding to touch FOS."""
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core import reset as reset_mod

    monkeypatch.setattr("backend.core.iceberg._core.init_iceberg_table", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.view.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.2)

    svcconfig.save_config(
        fos_source["service_id"],
        {
            "service_id": fos_source["service_id"],
            "name": fos_source["name"],
            "fos_bucket": fos_source["bucket"],
            "fos_prefix": "",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test-key",
            "fos_secret_access_key": "test-secret",
            "access_level": "read_write",
            "provisioning": {"cron_sync": {"enabled": True}},
        },
    )

    reached_middle = threading.Event()
    release_reset = threading.Event()

    def _slow_delete(*args, **kwargs):
        reached_middle.set()
        release_reset.wait(timeout=5)
        return 0

    monkeypatch.setattr("backend.core.ingest._delete_objects_robust", _slow_delete)
    s3_mock.put_object(Bucket=fos_source["bucket"], Key="iceberg/default/logs/data/foo.parquet", Body=b"x")

    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    def _run_reset():
        list(reset_mod.reset_service_logs(fos_source["service_id"], reload_scheduler=MagicMock()))

    reset_thread = threading.Thread(target=_run_reset, daemon=True)
    reset_thread.start()
    try:
        assert reached_middle.wait(timeout=30), "reset never reached the FOS purge"
        # Resolve the source the same way _run_commit does in production
        # (get_source_for_service), NOT the raw fos_source fixture — its
        # "name" field doesn't equal its "service_id" the way a real
        # config-derived source's does, which would silently lock on a
        # different key than reset_service_logs does.
        real_source = _db.get_source_for_service(fos_source["service_id"])
        result = buffer_mod.commit_buffer(real_source)
        impl.assert_not_called()
        assert result["files_committed"] == 0
    finally:
        release_reset.set()
        reset_thread.join(timeout=30)


# ── Merged from tests/core/test_iceberg_helpers.py ──
def test_buffer_dir_is_under_cache_dir(tmp_path):
    """Buffer dir lives under the source's _cache_dir as ``/buffer``.
    Pinned because the commit_buffer job reads from this path —
    relocating without updating commit_buffer would silently lose
    every batch."""
    from backend.core.iceberg import _buffer_dir

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = _buffer_dir({"name": "svc"})
    assert out == str(tmp_path / "buffer")


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


def test_clear_source_caches_removes_view_and_snapshot_entries():
    """Clears `_view_cache` for a given source key. Pinned because teardown
    must purge in-memory state to prevent the next service-with-same-name
    from seeing stale schema/files."""
    import backend.core.iceberg as iceberg_mod

    iceberg_mod._view_cache["test-svc"] = ("loc", set(), (), "sql", 1.0, True)

    iceberg_mod.clear_source_caches("test-svc")

    assert "test-svc" not in iceberg_mod._view_cache


def test_clear_source_caches_is_noop_for_unknown_source():
    """Clearing an absent source doesn't raise. Pinned because
    teardown calls this unconditionally; raising on absent caches
    would mask the real teardown error."""
    from backend.core.iceberg import clear_source_caches

    # Should NOT raise
    clear_source_caches("never-seen-svc")


def test_invalidate_service_caches_clears_every_cache():
    """``invalidate_service_caches`` must purge per-service caches."""
    import backend.core.iceberg as iceberg_mod
    from backend.core.iceberg import _core

    src = {"name": "inv-test-svc", "bucket": "inv-test-bucket", "prefix": ""}
    name = src["name"]

    _core._view_cache[name] = ("loc", set(), (), "sql", 1.0, True)

    iceberg_mod.invalidate_service_caches(src)

    assert name not in _core._view_cache


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


def test_buffer_files_respects_table_name_for_recursive_glob(tmp_path, monkeypatch):
    """Non-'logs' table_name triggers recursive glob under buffer/<table_name>/."""
    from backend.core.iceberg import buffer as buf_mod

    buf = tmp_path / "buffer" / "rum_events" / "sub"
    buf.mkdir(parents=True)
    target = buf / "ev1.parquet"
    target.write_bytes(b"x")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path))

    out = buf_mod.buffer_files({"name": "svc"}, table_name="rum_events")
    assert str(target) in out


def test_sweep_respects_table_name_for_recursive_glob(tmp_path, monkeypatch):
    """Non-'logs' table_name triggers recursive tombstone sweep under buffer/<table_name>/."""
    from backend.core.iceberg import buffer as buf_mod

    buf = tmp_path / "buffer" / "rum_events" / "sub"
    buf.mkdir(parents=True)
    pq_file = buf / "ev.parquet"
    pq_file.write_bytes(b"x")
    marker = buf / "ev.parquet.consumed-100"
    marker.write_text("")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path))

    swept = buf_mod.sweep_tombstoned_buffer_files({"name": "svc"}, grace_seconds=10, now=500, table_name="rum_events")
    assert swept == 1
    assert not pq_file.exists()
    assert not marker.exists()


_ = MagicMock

_ = pytest

_ = os
