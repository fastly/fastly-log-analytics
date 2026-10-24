"""PyIceberg integration for Fastly Object Storage log analysis.

Handles:
- Iceberg table initialisation in FOS via SqlCatalog (SQLite index in the
  per-service cache dir; table data files live in the FOS bucket)
- Committing local buffer Parquet files to Iceberg as atomic snapshots
- Table optimisation (small-file compaction via rewrite_data_files)
- Snapshot expiry and orphan file cleanup
- DuckDB view wiring: iceberg_scan(FOS table) UNION ALL read_parquet(local buffer)
- Snapshot metadata for the admin UI

Buffer strategy
---------------
Raw logs are ingested into a local buffer directory (cache/{svc}/buffer/).
Every few minutes the scheduler calls commit_buffer(), which appends the
accumulated buffer files as a single Iceberg snapshot and deletes them.
The DuckDB view always unions the committed Iceberg data with whatever is
still in the buffer, so the dashboard is never stale.

Catalog layout
--------------
warehouse = s3://{bucket}/{prefix}iceberg/
table     = default.logs
DuckDB iceberg_scan path = {table.location()}
"""

from __future__ import annotations

import logging
import os

import pyarrow as pa

_C = "\x1b[36m"  # Cyan — iceberg operations
_R = "\x1b[0m"
_ICE = f"🧊 {_C}[iceberg]{_R}"

logger = logging.getLogger(__name__)

from backend.core.field_registry import LOG_FIELD_CATALOG

# ---------------------------------------------------------------------------
# Arrow Schema — derived from LOG_FIELD_CATALOG (single source of truth).
#
# Unsigned DuckDB types are widened to the next signed Arrow type
# (UTINYINT/USMALLINT → int32, UINTEGER/UBIGINT → int64). Values are never
# truncated. All fields except timestamp are nullable because not every
# service enables every log field group — absent fields are written as nulls
# by _align_to_schema() so the Parquet schema stays uniform.
#
# Adding a storage-backed field to LOG_FIELD_CATALOG is NOT enough on its own:
# its id must ALSO be appended to the hand-maintained _FIELD_ORDER list below,
# which is what actually drives the Arrow schema and the DuckDB view. A
# catalog field left out of _FIELD_ORDER silently never materializes as a
# column. The drift guard
# tests/core/test_iceberg.py::test_field_order_covers_ingest_storage_fields
# fails CI if that happens. Once a field is in _FIELD_ORDER, the buffer
# commit path adds it (by name) to the existing DuckLake table on the next
# commit — see the ALTER TABLE ... ADD COLUMN loop in
# backend/core/iceberg/buffer.py, which diffs the buffer parquet's columns
# against the live table.
# ---------------------------------------------------------------------------

_DUCKDB_TO_ARROW: dict[str, pa.DataType] = {
    "TIMESTAMP": pa.timestamp("us", tz="UTC"),
    "VARCHAR": pa.string(),
    "BIGINT": pa.int64(),
    "UBIGINT": pa.int64(),
    "UINTEGER": pa.int64(),
    "INTEGER": pa.int32(),
    "INT": pa.int32(),
    "USMALLINT": pa.int32(),
    "UTINYINT": pa.int32(),
    "DOUBLE": pa.float64(),
    "FLOAT": pa.float32(),
    "BOOLEAN": pa.bool_(),
}

# Field order is FIXED — Iceberg assigns field IDs by position and existing
# tables in FOS carry those IDs in their metadata. New fields must be appended
# at the end; reordering would cause a field-ID mismatch on commit.
# The order below matches the original hardcoded list (IDs 1–58).
_FIELD_ORDER = [
    # Always-on (IDs 1–6)
    "timestamp",
    "ip",
    "status",
    "elapsed",
    "cache",
    "resp_bytes",
    # Group A (IDs 7–13)
    "host",
    "url",
    "method",
    "proto",
    "ua",
    "referer",
    "req_bytes",
    # Group B (IDs 14–17)
    "ttl",
    "age",
    "hits",
    "digest",
    # Group C (IDs 18–22)
    "pop",
    "backend",
    "edge",
    "ttfb",
    "tls",
    # Group D (IDs 23–25)
    "country",
    "city",
    "region",
    # Group E (IDs 26–28)
    "lat",
    "lon",
    "metro",
    # Group F (IDs 29–31)
    "asn",
    "tcp_rtt",
    "transport",
    # Group G (IDs 32–38)
    "ploss",
    "rtt_min",
    "rtt_var",
    "retrans",
    "bw",
    "c_speed",
    "c_type",
    # Group H (IDs 39–40)
    "ja3",
    "ja4",
    # Group I (IDs 41–42)
    "p_type",
    "p_desc",
    # Group J (IDs 43–47)
    "waf",
    "waf_resp",
    "waf_ms",
    "waf_sig",
    "waf_req_id",
    # Group K (IDs 48–51)
    "q_rtt",
    "q_rtt_var",
    "q_lost",
    "q_cwnd",
    # Later additions — always append new fields here (IDs 52+)
    "req_header_bytes",
    "server_region",
    "is_ipv6",
    "conn_requests",
    "delivery_rate",
    "data_segs_out",
    "tls_ciphers_sha",
    # Group L — Origin Metrics (IDs 59–66)
    "ottfb",
    "ottlb",
    "ost",
    "obytes",
    "oip",
    "oretries",
    "rid",
    "prid",
    # Late-adds that CONSUME the two former reserved positional slots: they take
    # field_ids 67 and 68 EXACTLY (zero shift vs the old _reserved_67/_reserved_68),
    # so _source_file keeps its committed field_id 69.
    "resp_header_content_encoding",  # Group A — id 67 (was _reserved_67)
    "oconnect_ms",  # Group L — id 68 (was _reserved_68)
    # Internal fields (ID 69)
    "_source_file",
    # Reserved buffer now exhausted; this real field takes a fresh slot (id 70).
    # base_count grows 69→70, so the base_count-anchored *computed* custom-field
    # ids shift 70-78 → 71-79. SAFE for existing tables: every read path binds
    # columns by NAME (read_parquet union_by_name / UNION ALL BY NAME /
    # iceberg_scan over the table's own metadata), writes align to
    # schema_to_pyarrow(table.schema()), and schema-evolution add_column matches
    # by NAME and mints fresh ids. Positional ids are consumed only by the
    # initial create_table for a brand-new table.
    "cookie_session",  # Group H — id 70; default_off + pii
    "io_input_bytes",  # Group M — id 71
    "io_output_bytes",  # Group M — id 72
    "io_input_format",  # Group M — id 73
    "io_output_format",  # Group M — id 74
]

# No non-emitted positional slots remain — the reserved buffer was consumed by
# resp_header_content_encoding (67) and oconnect_ms (68) above. Append future
# storage-backed fields to the end of _FIELD_ORDER; the drift guard
# (tests/core/test_iceberg.py::test_field_order_covers_ingest_storage_fields)
# fails CI if a catalog field ingest stores is ever left out of _FIELD_ORDER.
_RESERVED_FIELD_SLOTS: set[str] = set()

_CATALOG_TYPE_MAP = {f["id"]: f["duckdb_type"] for f in LOG_FIELD_CATALOG}


def _effective_field_types(log_fields_config: dict | None = None) -> list[tuple[str, str]]:
    """Return ordered ``(field_name, duckdb_type)`` pairs for the logs schema."""
    base: list[tuple[str, str]] = [
        (fid, _CATALOG_TYPE_MAP.get(fid, "VARCHAR")) for fid in _FIELD_ORDER if fid not in _RESERVED_FIELD_SLOTS
    ]
    base_names = {name for name, _ in base}
    custom_fields: list[dict] = []
    if isinstance(log_fields_config, dict):
        if isinstance(log_fields_config.get("custom_fields"), list):
            custom_fields = log_fields_config["custom_fields"]
        elif isinstance((log_fields_config.get("log_fields") or {}).get("custom_fields"), list):
            custom_fields = log_fields_config["log_fields"]["custom_fields"]

    sorted_customs = sorted(custom_fields, key=lambda x: x.get("name", ""))
    for cf in sorted_customs:
        name = cf.get("name")
        if not name or not cf.get("enabled", True) or name in base_names:
            continue
        dtype = (cf.get("duckdb_type") or "VARCHAR").upper()
        base.append((name, dtype))
        base_names.add(name)
    return base


def get_arrow_schema(log_fields_config: dict | None = None, table_name: str = "logs") -> pa.Schema:
    if table_name != "logs":
        from backend.core.iceberg.rum_schema import RUM_TABLE_SCHEMAS

        return RUM_TABLE_SCHEMAS[table_name]
    fields = [
        pa.field(
            name,
            _DUCKDB_TO_ARROW.get(dtype.upper(), pa.string()),
            nullable=True,
        )
        for name, dtype in _effective_field_types(log_fields_config)
    ]
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _buffer_dir(source: dict, table_name: str = "logs") -> str:
    from backend.core.duckdb import _cache_dir

    base = os.path.join(_cache_dir(source), "buffer")
    if table_name != "logs":
        return os.path.join(base, table_name)
    return base


def invalidate_service_caches(source: dict) -> dict[str, int]:
    """Drop process-local view/DuckLake caches for one service."""
    name = source.get("name", "default")
    cleared = sum(1 for k in _view_cache if k == name or k.startswith(f"{name}::"))
    clear_source_caches(name)
    return {"view_entries_cleared": cleared}


def init_iceberg_table(source: dict, create: bool = True, table_name: str = "logs"):
    source_key = source.get("name", "default")
    with _get_service_lock(source_key):
        return _init_iceberg_table_locked(source, create, table_name=table_name)


def _init_iceberg_table_locked(source: dict, create: bool = True, table_name: str = "logs"):
    """Ensure the service's DuckLake catalog is attachable. True, or None on failure.

    Safe to call on every provision and on every scheduler tick — it is a
    no-op once the catalog exists.

    ``create`` is retained for its five existing call sites but is inert
    under DuckLake, deliberately: the per-service table is created from the
    first buffer parquet's own schema in the commit path
    (``buffer.py``'s ``CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM
    read_parquet(...) LIMIT 0``), which is also where column evolution
    happens. Declaring the columns up front here would fork the schema
    definition across two places for no gain. Callers that need to know
    whether the table itself exists yet should use
    :func:`ducklake_table_exists`, which distinguishes "not created yet"
    from "could not check".
    """
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach

    con = get_connection(source, read_only=True)
    try:
        if _ducklake_attach(con, source, read_only=True):
            return True
        return None
    finally:
        con.close()


# ── Buffer / commit / optimize / cloud-maintenance (carved out) ──
# Defined in backend.core.iceberg.buffer; re-imported here so other
# code in _core.py + the package proxy + test monkeypatch sites
# resolve the same canonical binding.
from backend.core.iceberg.buffer import (  # noqa: F401, E402
    _BUFFER_COMMIT_CHUNK_SIZE,
    _TOMBSTONE_GRACE_SECONDS,
    _TOMBSTONE_SUFFIX,
    _is_tombstone_marker,
    _quarantine_buffer_file,
    _quarantine_dir,
    _tombstone_marker_path,
    _tombstoned_parquet_paths,
    buffer_backlog_stats,
    buffer_files,
    commit_buffer,
    optimize_table,
    run_cloud_maintenance,
    sweep_tombstoned_buffer_files,
    tombstone_buffer_files,
    write_to_buffer,
)
from backend.core.iceberg.manifest import (  # noqa: F401, E402
    _align_to_schema,
    _arrow_to_duckdb,
    _prune_empty_dirs,
    ducklake_session,
    ducklake_table_exists,
    get_snapshot_calendar,
    get_table_info,
)

# ── sync_data (DuckLake no-op shim) ──
from backend.core.iceberg.sync import (  # noqa: F401, E402
    sync_data,
)

# ── View binding + stale-view self-heal (carved) ──
from backend.core.iceberg.view import (  # noqa: F401, E402
    _get_service_lock,
    _persistent_view_exists,
    _rebuild_locked,
    _rebuild_signals,
    _rebuild_signals_lock,
    _service_locks,
    _service_locks_lock,
    _try_fast_path_view,
    _update_iceberg_view_locked,
    _view_cache,
    clear_source_caches,
    configure_duckdb_s3,
    execute_with_stale_view_retry,
    get_last_view_stats,
    inject_view_debug,
    is_stale_view_error,
    update_iceberg_view,
)

# A-3 (CacheRegistry): register every iceberg cache so the test
# harness can drain them centrally via CacheRegistry.clear_all().
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("iceberg._view_cache", _view_cache)
