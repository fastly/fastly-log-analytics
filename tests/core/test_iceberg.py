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
