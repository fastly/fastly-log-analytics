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
import time
from typing import Any

import pyarrow as pa

_C = "\x1b[36m"  # Cyan — iceberg operations
_C2 = "\x1b[94m"  # Bright Blue — sync_data operations
_R = "\x1b[0m"
_ICE = f"🧊 {_C}[iceberg]{_R}"
_ICE_PLAIN = f"{_C}[iceberg]{_R}"
_SYNC = f"⬇️  {_C2}[sync_data]{_R}"

# --- s3fs/botocore monkeypatches: extracted to backend.core.iceberg.fs ---
# Re-import the names that callers and tests reach in by attribute lookup so
# every existing import path (from backend.core.iceberg import _orig_s3fs_init,
# monkeypatching backend.core.iceberg._manifest_bytes_cache from tests,
# etc.) keeps resolving. The wildcard import installs the monkeypatches as a
# side effect, but defining them in this module's namespace too means tests
# that monkeypatch this module's attributes still see effects in the real
# call sites that import from here.
from backend.core.iceberg.fs import *  # noqa: F401,F403
from backend.core.iceberg.fs import (  # noqa: F401
    _LAST_FS_SOURCE,
    _PENDING_FS_SOURCE,
    _manifest_bytes_cache,
    _proxy_targets_from_endpoint,
    _register_proxy_event_hook,
)

try:
    from backend.core.iceberg.fs import (  # noqa: F401
        _cache_get,
        _cache_put,
        _canonical_cache_key,
        _get_or_fetch_immutable_async,
        _ImmutableWriteCacheTee,
        _inflight_async,
        _is_immutable_path,
        _manifest_cache_lock,
        _orig_cat_file,
        _orig_info,
        _orig_open,
        _orig_s3fs_init,
        _orig_s3fs_set_session,
        _patched_cat_file,
        _patched_info,
        _patched_open,
        _patched_s3fs_init,
        _patched_s3fs_set_session,
    )
except ImportError:
    # s3fs unavailable — the monkeypatch block in fs.py also no-ops in this case.
    pass
# ------------------------------------------------------------

logger = logging.getLogger(__name__)

from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table.name_mapping import create_mapping_from_schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from backend.core.field_registry import LOG_FIELD_CATALOG

# ---------------------------------------------------------------------------
# Iceberg Schema — derived from LOG_FIELD_CATALOG (single source of truth).
#
# Iceberg does not support unsigned integer types, so unsigned DuckDB types are
# widened to the next signed type (UTINYINT/USMALLINT → int32, UINTEGER/UBIGINT
# → int64). Values are never truncated. All fields are nullable because not
# every service enables every log field group — absent fields are written as
# nulls by _align_to_schema() so the Parquet schema stays uniform.
#
# Adding a storage-backed field to LOG_FIELD_CATALOG is NOT enough on its own:
# its id must ALSO be appended to the hand-maintained _FIELD_ORDER list below,
# which is what actually drives this schema, the Arrow schema, and the DuckDB
# view (field ids are position-assigned). A catalog field left out of
# _FIELD_ORDER silently never materializes as a column. The drift guard
# tests/core/test_iceberg.py::test_field_order_covers_ingest_storage_fields
# fails CI if that happens. Once a field is in _FIELD_ORDER, the schema
# evolution code in _init_iceberg_table_locked adds it (by name) to existing
# tables on the next tick.
# ---------------------------------------------------------------------------

_DUCKDB_TO_ICEBERG = {
    "TIMESTAMP": TimestamptzType(),  # always store as tz-aware
    "VARCHAR": StringType(),
    "BOOLEAN": BooleanType(),
    "FLOAT": FloatType(),
    "DOUBLE": DoubleType(),
    "INTEGER": IntegerType(),
    "BIGINT": LongType(),
    "USMALLINT": IntegerType(),  # widen unsigned → signed (no truncation)
    "UTINYINT": IntegerType(),
    "UINTEGER": LongType(),
    "UBIGINT": LongType(),
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

# Every current _FIELD_ORDER entry is a real catalog field, so the VARCHAR
# fallback below is now a defensive placeholder only (the reserved slots that
# once needed it have been consumed). It keeps enumerate() positions aligned if
# a future non-catalog positional slot is ever reintroduced.
_fields = [
    (fid, _DUCKDB_TO_ICEBERG[_CATALOG_TYPE_MAP[fid]] if fid in _CATALOG_TYPE_MAP else _DUCKDB_TO_ICEBERG["VARCHAR"])
    for fid in _FIELD_ORDER
]


def get_iceberg_schema(log_fields_config: dict | None = None) -> Schema:
    """Return the Iceberg schema dynamically, including custom fields if configured.

    **Field-id stability contract.** Iceberg expects a column's ``field_id``
    to be stable for the life of the table — Parquet files written under an
    ID can only be read back through the same ID. We therefore:

      1. Sort ALL custom fields (including disabled ones) by name and
         enumerate them with stable IDs. A disabled field's slot stays
         reserved.
      2. Drop disabled fields from the emitted schema.

    The old behaviour enumerated the post-filter list, so disabling
    ``beta`` would shift ``gamma`` into ``beta``'s old ID slot — a silent
    corruption pattern.
    """
    custom_fields = log_fields_config.get("custom_fields", []) if log_fields_config else []
    base_count = len(_fields)

    # Build (id, name, type, enabled) tuples for ALL custom fields so IDs
    # are derived from the full sorted list, not just the enabled subset.
    sorted_customs = sorted(custom_fields, key=lambda x: x["name"])
    custom_with_ids = [
        (
            base_count + idx + 1,
            cf["name"],
            _DUCKDB_TO_ICEBERG.get(cf.get("duckdb_type", "VARCHAR"), StringType()),
            cf.get("enabled", True),
        )
        for idx, cf in enumerate(sorted_customs)
    ]

    base_nested = [
        NestedField(field_id=i, name=name, field_type=ftype, required=False)
        for i, (name, ftype) in enumerate(_fields, 1)
        if name not in _RESERVED_FIELD_SLOTS
    ]
    custom_nested = [
        NestedField(field_id=fid, name=name, field_type=ftype, required=False)
        for fid, name, ftype, enabled in custom_with_ids
        if enabled
    ]
    return Schema(*base_nested, *custom_nested)


def get_arrow_schema(log_fields_config: dict | None = None) -> pa.Schema:
    return schema_to_pyarrow(get_iceberg_schema(log_fields_config))


def get_schema_field_names(log_fields_config: dict | None = None) -> set[str]:
    return {f.name for f in get_arrow_schema(log_fields_config)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _buffer_dir(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "buffer")


def _table_identifier(source: dict) -> tuple[str, str]:
    """Return the PyIceberg table identifier tuple (namespace, name)."""
    return ("default", "logs")


def _is_local_only_source(source: dict) -> bool:
    """True when this source is configured to use local files instead of FOS/S3.

    Triggered by ``fos_local_warehouse: true`` in the source config, OR by
    the conventional ``fos_endpoint: "http://localhost:0"`` scrub marker
    (see CLAUDE.md ``dev-sandbox-scrub`` memory). Used by load-test and
    other dev-only services to commit Iceberg snapshots to local disk
    without touching real object storage.
    """
    if source.get("fos_local_warehouse") is True:
        return True
    endpoint = source.get("fos_endpoint") or source.get("endpoint") or ""
    return endpoint in ("http://localhost:0", "http://127.0.0.1:0")


def _warehouse_uri(source: dict) -> str:
    if _is_local_only_source(source):
        # Local-only: Iceberg writes commits, manifests, and data files into
        # cache/{bucket}/iceberg/ on disk. Catalog stays SQLite (already local).
        from backend.core.duckdb import _cache_dir

        cache = _cache_dir(source)
        return f"file://{os.path.abspath(os.path.join(cache, 'iceberg'))}"
    prefix = source.get("prefix", "").strip("/")
    base = f"{prefix}/iceberg" if prefix else "iceberg"
    return f"s3://{source['bucket']}/{base}"


def _catalog_db_path(source: dict) -> str:
    """Return path to the per-service SQLite catalog file."""
    from backend.core.duckdb import _cache_dir

    cache = _cache_dir(source)
    os.makedirs(cache, exist_ok=True)
    return os.path.join(cache, "iceberg_catalog.db")


def _cloud_uri_to_local_path(uri: str, base_dir: str) -> str | None:
    """Map an Iceberg data-file URI to its local cache path under ``base_dir``.

    PyIceberg writes data files to ``.../data/timestamp_hour=.../file.parquet``;
    preserve the partition folder structure (for Hive partition pruning) by
    keeping everything after ``/data/``, falling back to the bare filename when
    the URI has no ``/data/`` segment. Returns ``None`` when the resolved path
    escapes ``base_dir`` (path-traversal guard) so callers can ``continue``.
    """
    rel_path = uri.split("/data/")[-1] if "/data/" in uri else uri.split("/")[-1]
    local = os.path.abspath(os.path.join(base_dir, rel_path))
    if not local.startswith(os.path.abspath(base_dir) + os.sep):
        return None
    return local


import threading

# Cache for catalogs to avoid leaking SQLite connections and repeated initialization
_catalog_cache: dict[str, Any] = {}
_catalog_lock = threading.Lock()


def _get_catalog(source: dict):
    """Return a configured PyIceberg SqlCatalog backed by a local SQLite file."""
    source_key = source.get("name", "default")
    # Stamp the process-global fallback so s3fs instances created on
    # threads without the ContextVar (fsspec iothread, lazy per-FS
    # creations) still get a non-empty source in ``_patched_s3fs_init``.
    # See the comment on ``_LAST_FS_SOURCE`` above for the failure mode
    # this defends against. Always update on every call so a future
    # multi-service deployment at least always has a recent source —
    # though that case would need a proper per-bucket lookup, not this.
    #
    # NOTE: the canonical storage lives in ``backend.core.iceberg.fs`` so
    # ``_patched_s3fs_init`` (which closes over fs.py's module globals)
    # sees the update. A local ``global _LAST_FS_SOURCE`` here would only
    # rebind the name on this module — the patched init would still read
    # the stale fs.py value. Update the fs module attribute directly.
    from backend.core.iceberg import fs as _fs

    _fs._LAST_FS_SOURCE = source
    global _LAST_FS_SOURCE
    _LAST_FS_SOURCE = source
    with _catalog_lock:
        if source_key in _catalog_cache:
            return _catalog_cache[source_key]

        # PyIceberg both reads and writes metadata/data files. The CDN VCL
        # only proxies GET/HEAD on object keys, so writes (commits) and the
        # metadata.json HEAD/GET must hit native FOS, not the CDN.
        endpoint = source.get("fos_native_endpoint") or source.get("endpoint", "")
        access_key = source.get("access_key_id", "")
        secret_key = source.get("secret_access_key", "")
        warehouse = _warehouse_uri(source)
        db_path = _catalog_db_path(source)

        # Hand the source dict to the s3fs patched __init__ via ContextVar.
        # This covers the main thread, and we patched ThreadPoolExecutor
        # to propagate ContextVars to PyIceberg's thread-pool workers.
        _PENDING_FS_SOURCE.set(source)

        if _is_local_only_source(source):
            # Local-only warehouse: skip S3 client config entirely. PyIceberg's
            # default PyArrowFileIO handles file:// URIs natively without any
            # network round-trip.
            props = {
                "uri": f"sqlite:///{db_path}",
                "warehouse": warehouse,
            }
        else:
            props = {
                "uri": f"sqlite:///{db_path}",
                "warehouse": warehouse,
                "s3.endpoint": f"https://{endpoint}",
                "s3.access-key-id": access_key,
                "s3.secret-access-key": secret_key,
                "s3.path-style-access": "true",
                "s3.region": source.get("region", "us-east-1"),
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "s3.client.config": '{"retries": {"max_attempts": 5, "mode": "adaptive"}, "read_timeout": 30, "connect_timeout": 10}',
            }

        catalog_cls = _get_fos_catalog_class()
        catalog = catalog_cls("fos", **props)
        # Stream H: tag the catalog with its source so FosSqlCatalog.load_table
        # can find the right _table_object_cache key. Without this, pyiceberg's
        # internal commit_table.load_table cannot consult the cache and
        # re-fetches ~865 KB metadata.json per commit.
        catalog._fos_source = source
        _catalog_cache[source_key] = catalog
        return catalog


# Observability counter for the cached load_table fall-through path. Only
# increments when FosSqlCatalog had to call the real SqlCatalog.load_table
# (i.e. cache miss). Tests pin Stream H by asserting this stays zero across
# a full commit cycle.
_sql_load_table_real_calls: dict[str, int] = {"n": 0}

# Cached FosSqlCatalog subclass. Built lazily on first _get_catalog call so
# tests that monkeypatch pyiceberg.catalog.sql.SqlCatalog (e.g.
# tests/core/test_endpoint_routing.py) get a subclass of *their* stub. The
# base-class identity check below invalidates this cache if SqlCatalog
# changes between calls.
_FOS_CATALOG_CLASS: type | None = None


def _get_fos_catalog_class() -> type:
    """Return a SqlCatalog subclass whose load_table consults _table_object_cache.

    PyIceberg's SqlCatalog.commit_table (inside Transaction.commit_transaction,
    inside table.append) calls self.load_table to get current_table for its CAS
    check. That load_table unconditionally GETs metadata.json from FOS — the
    very file we typically PUT seconds earlier and still have fully parsed in
    _table_object_cache. The override short-circuits when:

      1. The catalog is one of ours (has _fos_source attached by _get_catalog).
      2. The FOS pointer is readable (~free; CDN + 2s TTL).
      3. The cached Table's metadata_location matches the pointer exactly.

    On any mismatch falls through to super().load_table so correctness is
    preserved (a cross-process commit always invalidates via pointer mismatch).
    """
    global _FOS_CATALOG_CLASS
    from pyiceberg.catalog.sql import SqlCatalog

    # Identity-by-base, not subclass: tests can monkeypatch SqlCatalog out
    # from under us, and we want a cache miss in that case. Looking through
    # all bases (not just [0]) is robust to a future mixin landing in front
    # of SqlCatalog in the MRO.
    if _FOS_CATALOG_CLASS is not None and SqlCatalog in _FOS_CATALOG_CLASS.__bases__:
        return _FOS_CATALOG_CLASS

    class FosSqlCatalog(SqlCatalog):  # type: ignore[misc, valid-type]
        def load_table(self, identifier):  # type: ignore[override]
            source = getattr(self, "_fos_source", None)
            if source is not None:
                ident = _table_identifier(source) if isinstance(identifier, str) else tuple(identifier)
                latest_loc = _read_metadata_pointer(source, ident)
                if latest_loc:
                    cached = _get_cached_table(source, ident, latest_loc)
                    if cached is not None:
                        return cached
            _sql_load_table_real_calls["n"] += 1
            return super().load_table(identifier)

    _FOS_CATALOG_CLASS = FosSqlCatalog
    return FosSqlCatalog


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


_table_summary_hash_cache: dict[tuple[str, str, str], str] = {}
_table_summary_hash_lock = threading.Lock()


def _write_table_summary_async(source: dict, table=None) -> None:
    """Generate and write a table_summary.json to FOS in the background.

    This provides analysts with instant access to the table's range and calendar
    without needing to download and parse large Iceberg manifests.

    Pass `table` from the caller (the just-committed Table object) to skip
    the `catalog.load_table()` round-trip — that re-downloads the same
    metadata.json (~850 KB) we wrote one second earlier.

    Skips the PUT when the serialized payload matches the last write in this
    process — defensive against commits that don't shift the summary (schema-
    only, expire-snapshot, etc.). In steady-state ingest the snapshot count
    advances each commit so the cache rarely hits.
    """
    import hashlib
    import json
    import threading

    def _run():
        try:
            identifier = _table_identifier(source)
            # We don't want to use the global UI cache, we want fresh data.
            # When the caller hands us the freshly-committed table, skip the
            # catalog.load_table() — it would re-GET the just-written metadata.json.
            local_table = table
            if local_table is None:
                catalog = _get_catalog(source)
                local_table = catalog.load_table(identifier)

            info = get_table_info(source, table=local_table)
            calendar = get_snapshot_calendar(source, table=local_table)

            summary = {
                "info": info,
                "calendar": calendar,
                "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
            }

            from backend.core.duckdb import _get_fos_client

            s3 = _get_fos_client(source)
            bucket = source["bucket"]
            base_prefix = source.get("prefix", "").strip("/")
            namespace, table_name = identifier

            iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
            summary_key = f"{iceberg_root}/{namespace}/{table_name}/table_summary.json"

            body = json.dumps(summary, sort_keys=True).encode("utf-8")
            body_hash = hashlib.sha256(body).hexdigest()
            cache_key = (bucket, namespace, table_name)
            with _table_summary_hash_lock:
                if _table_summary_hash_cache.get(cache_key) == body_hash:
                    logger.debug("[iceberg] table_summary unchanged for %s, skipping PUT", summary_key)
                    return

            s3.put_object(
                Bucket=bucket,
                Key=summary_key,
                Body=body,
                ContentType="application/json",
                CacheControl="max-age=10",
            )
            with _table_summary_hash_lock:
                _table_summary_hash_cache[cache_key] = body_hash
            logger.debug("[iceberg] Wrote table summary to %s", summary_key)

            # Also purge CDN if configured
            _purge_surrogate_key(source, "iceberg-table-summary")
        except Exception as e:
            logger.warning("[iceberg] Failed to write async table summary: %s", e)

    threading.Thread(target=_run, daemon=True).start()


# Process-local cache for metadata-pointer reads. A single cron_compact run
# triggers _read_metadata_pointer up to 4× in the same second (init_table,
# sync_data, get_table_info, get_snapshot_calendar), each costing ~200ms via
# the CDN. The pointer changes only on commit; this in-process cache
# collapses redundant reads to one. Bounded by _POINTER_CACHE_TTL_SEC so
# even without explicit invalidation, staleness is capped — and writers in
# the same process invalidate explicitly below.
_POINTER_CACHE_TTL_SEC = 2.0
_pointer_cache: dict[tuple[str, str, str, str], tuple[float, str | None]] = {}
_pointer_cache_lock = threading.Lock()


def _pointer_cache_key(source: dict, identifier: tuple) -> tuple[str, str, str, str]:
    namespace, table_name = identifier
    return (source.get("bucket", ""), source.get("prefix", ""), namespace, table_name)


def _pointer_cache_invalidate(source: dict, identifier: tuple) -> None:
    key = _pointer_cache_key(source, identifier)
    with _pointer_cache_lock:
        _pointer_cache.pop(key, None)


# Process-local cache for loaded PyIceberg Table objects, keyed by
# (bucket, namespace, table_name). Cross-process freshness is enforced by
# comparing each cached table's metadata_location against the FOS pointer
# (itself CDN-cached + TTL-cached above). A pointer mismatch is exhaustive
# proof of staleness because every snapshot commit produces a new
# metadata.json and a new pointer value.
_table_object_cache: dict[tuple[str, str, str, str], object] = {}
_table_object_cache_lock = threading.Lock()


def _get_cached_table(source: dict, identifier: tuple, expected_metadata_loc: str):
    """Return cached Table iff its metadata_location matches expected, else None."""
    key = _pointer_cache_key(source, identifier)
    with _table_object_cache_lock:
        cached = _table_object_cache.get(key)
    if cached is None or getattr(cached, "metadata_location", None) != expected_metadata_loc:
        return None
    return cached


def _set_cached_table(source: dict, identifier: tuple, table) -> None:
    key = _pointer_cache_key(source, identifier)
    with _table_object_cache_lock:
        _table_object_cache[key] = table


def _invalidate_cached_table(source: dict, identifier: tuple) -> None:
    key = _pointer_cache_key(source, identifier)
    with _table_object_cache_lock:
        _table_object_cache.pop(key, None)


def invalidate_service_caches(source: dict) -> None:
    """Drop ALL process-local iceberg caches for one service.

    Teardown must call this (with the *runtime* source, so keys match what the
    cron writer populated) before deleting the FOS bucket. Otherwise a
    same-process re-provision of the same bucket resurrects a stale Table from
    ``_table_object_cache`` — ``init_iceberg_table`` returns the cached object
    and SKIPS creation (the table never lands in the fresh bucket), and
    ``commit_buffer`` then appends against deleted metadata.

    ``clear_source_caches`` only wipes the view/snapshot caches (and the
    teardown router called it with the service_id instead of the source name,
    so it missed even those), leaving the table-object / catalog / pointer
    caches stale. This clears every cache, keyed correctly: the name-keyed
    caches by ``source["name"]`` and the tuple-keyed ones by
    ``(bucket, prefix, namespace, table)``.
    """
    name = source.get("name", "default")
    identifier = _table_identifier(source)
    # name-keyed caches (catalog handle, snapshot-files, view SQL)
    for cache in (_catalog_cache, _snapshot_files_cache, _view_cache):
        try:
            cache.pop(name, None)
        except Exception:
            pass
    # tuple-keyed caches: loaded Table object + FOS metadata pointer
    for fn in (_invalidate_cached_table, _pointer_cache_invalidate):
        try:
            fn(source, identifier)
        except Exception:
            pass


def _load_table_cached(source: dict, identifier: tuple, catalog=None):
    """catalog.load_table() with per-service metadata_location-keyed cache.

    Pointer-driven freshness: read the FOS pointer (free; CDN + 2s TTL) and
    reuse the cached Table only when its metadata_location matches. Cross-
    process commits invalidate naturally via pointer mismatch.
    """
    latest_loc = _read_metadata_pointer(source, identifier)
    if latest_loc:
        cached = _get_cached_table(source, identifier, latest_loc)
        if cached is not None:
            return cached
    if catalog is None:
        catalog = _get_catalog(source)
    try:
        table = catalog.load_table(identifier)
        _set_cached_table(source, identifier, table)
        return table
    except (FileNotFoundError, OSError) as e:
        if "No such file or directory" in str(e) or "not found" in str(e).lower() or isinstance(e, FileNotFoundError):
            logger.warning("⚠️ [iceberg] Missing metadata file detected for %s: %s. Healing catalog...", identifier, e)
            # Remove from local catalog database
            db_path = _catalog_db_path(source)
            if os.path.exists(db_path):
                import sqlite3

                try:
                    namespace, table_name = identifier
                    with sqlite3.connect(db_path, timeout=5.0) as cat_con:
                        cat_con.execute(
                            "DELETE FROM iceberg_tables WHERE table_namespace = ? AND table_name = ?",
                            (namespace, table_name),
                        )
                        cat_con.commit()
                    logger.info(
                        "[iceberg] Successfully removed out-of-sync table %s from local SQLite catalog.", identifier
                    )
                except Exception as del_err:
                    logger.warning("[iceberg] Failed to remove out-of-sync table from SQLite: %s", del_err)

            # Clear cached table
            _invalidate_cached_table(source, identifier)

            from pyiceberg.exceptions import NoSuchTableError

            raise NoSuchTableError(f"Table {identifier} metadata is missing from S3: {e}")
        raise


def _purge_surrogate_key(source: dict, key: str) -> None:
    """Fire-and-forget CDN surrogate-key purge against the service's
    Fastly. No-op when no ``cdn_service_id`` is configured or no API
    key is available. Logs at debug on success and at warning on
    failure; never raises (CDN unreachability must not block the
    writer that called us)."""
    cdn_service_id = source.get("cdn_service_id", "")
    if not cdn_service_id:
        return
    try:
        from backend import config as _cfg

        api_key = _cfg.get_fastly_api_key(source.get("name", ""))
        if not api_key:
            return
        from backend.core.fastly.client import fastly as _fastly

        _fastly(
            "POST",
            f"/service/{cdn_service_id}/purge/{key}",
            token=api_key,
            expect_empty=True,
        )
        logger.debug("[iceberg] Purged CDN surrogate key %s", key)
    except Exception as e:
        logger.warning("[iceberg] CDN purge failed for surrogate key %s (non-fatal): %s", key, e)


def _iceberg_root_prefix(source: dict) -> str:
    """Return the FOS-root iceberg prefix for ``source`` (e.g.
    ``"prefix/iceberg"`` or ``"iceberg"``).

    The strip+conditional was hand-rolled at 4 sites in this file
    (write_pointer, read_pointer, the read-pointer search-fallback,
    register_table). Centralises the empty-prefix special case so a
    future "default prefix" decision lands in one place.
    """
    base_prefix = source.get("prefix", "").strip("/")
    return f"{base_prefix}/iceberg" if base_prefix else "iceberg"


def _metadata_pointer_candidates(source: dict, namespace: str, table_name: str) -> list[str]:
    """Slash- and dot-namespace variants for the metadata-pointer object key.

    Some writers used ``namespace/table_name``, others ``namespace.table_name``
    (the divergence pre-dates the standardisation on the slash form).
    Readers try both so any historic on-disk shape resolves; writers use
    the first variant.
    """
    root = _iceberg_root_prefix(source)
    return [
        f"{root}/{namespace}/{table_name}/metadata_location.txt",
        f"{root}/{namespace}.{table_name}/metadata_location.txt",
    ]


def _metadata_search_prefixes(source: dict, namespace: str, table_name: str) -> list[str]:
    """Slash- and dot-namespace variants for listing ``metadata.json`` files.

    Same rationale as :func:`_metadata_pointer_candidates` — both variants
    are tried by the discovery fallbacks (register_table + read_pointer).
    """
    root = _iceberg_root_prefix(source)
    return [
        f"{root}/{namespace}/{table_name}/metadata/",
        f"{root}/{namespace}.{table_name}/metadata/",
    ]


def _write_metadata_pointer(source: dict, location: str, table=None) -> None:
    """Write a pointer to the latest metadata.json to FOS.

    This allows Analyst (read-only) users to discover the latest snapshot
    without requiring ListBucket permissions.

    Pass `table` so the async table-summary writer can reuse the
    just-committed in-memory metadata instead of re-downloading it.
    """
    if _is_local_only_source(source):
        # Local-only warehouse: SQLite catalog already tracks metadata_location;
        # no separate FOS pointer to maintain. No-op.
        return
    try:
        from backend.core.duckdb import _get_fos_client

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        namespace, table_name = _table_identifier(source)

        # Write to e.g. iceberg/default/logs/metadata_location.txt — the
        # canonical slash-namespace variant. Readers try both this and the
        # dot-namespace fallback (see ``_metadata_pointer_candidates``) so
        # historic dot-form files keep resolving until they get rewritten.
        pointer_key = _metadata_pointer_candidates(source, namespace, table_name)[0]

        s3.put_object(
            Bucket=bucket,
            Key=pointer_key,
            Body=location.encode("utf-8"),
            ContentType="text/plain",
            CacheControl="max-age=10",
        )
        # Bust the local cache so the next reader in this process sees the
        # value we just wrote, not a stale pre-commit pointer.
        _pointer_cache_invalidate(source, (namespace, table_name))
        logger.debug("[iceberg] Wrote metadata pointer to %s", pointer_key)

        # Trigger async summary update — pass the just-committed table so
        # the worker doesn't re-GET the same metadata.json we just wrote.
        _write_table_summary_async(source, table=table)

        # Purge the CDN surrogate key so the next read always gets the new pointer.
        _purge_surrogate_key(source, "iceberg-metadata-pointer")
    except Exception as e:
        logger.warning("[iceberg] Failed to write metadata pointer: %s", e)


def _read_metadata_pointer(source: dict, identifier: tuple) -> str | None:
    """Read the latest metadata pointer from FOS via CDN if configured, else direct S3."""
    if _is_local_only_source(source):
        # Local-only warehouse: no FOS pointer to read. SqlCatalog already
        # knows the metadata_location from its SQLite-backed iceberg_tables row.
        return None
    namespace, table_name = identifier

    # In-process TTL cache. The 4-call-in-1-second pattern from cron_compact
    # collapses to a single wire call within the TTL window. Writers in this
    # process invalidate explicitly; cross-process freshness still rides on
    # the CDN's max-age=10 + surrogate-key purge.
    cache_key = _pointer_cache_key(source, identifier)
    now = time.time()
    with _pointer_cache_lock:
        entry = _pointer_cache.get(cache_key)
        if entry is not None and now - entry[0] < _POINTER_CACHE_TTL_SEC:
            return entry[1]

    try:
        from backend.core.duckdb import _get_fos_client
        from backend.core.iceberg.lake_info import _safe_cdn_url

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        # SSRF guard: only follow ``cdn_url`` when it parses as an https
        # Fastly hostname. Otherwise fall through to the S3 SDK.
        cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
        cdn_secret = source.get("cdn_secret") or ""

        pointer_keys = _metadata_pointer_candidates(source, namespace, table_name)

        resolved: str | None = None
        for pointer_key in pointer_keys:
            try:
                if cdn_url:
                    import time as _time
                    import urllib.parse
                    import urllib.request

                    from backend.utils.telemetry import record_cdn_call

                    url = f"{cdn_url}/{urllib.parse.quote(pointer_key, safe='/')}"
                    if cdn_secret:
                        url += f"?key={urllib.parse.quote(cdn_secret)}"
                    req = urllib.request.Request(url)
                    t0 = _time.time()
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        body = resp.read()
                        loc = body.decode("utf-8").strip()
                        headers = resp.headers
                    elapsed = round((_time.time() - t0) * 1000, 2)
                    record_cdn_call(
                        "GET",
                        pointer_key,
                        elapsed,
                        headers=headers,
                        bytes_count=len(body),
                        caller="_read_metadata_pointer",
                    )
                else:
                    resp = s3.get_object(Bucket=bucket, Key=pointer_key)
                    loc = resp["Body"].read().decode("utf-8").strip()
                if loc.startswith("s3://"):
                    resolved = loc
                    break
            except Exception:
                continue

        if resolved is None:
            # Fallback: try listing the bucket
            search_prefixes = _metadata_search_prefixes(source, namespace, table_name)
            for search_prefix in search_prefixes:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix)
                metadata_files = [
                    obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".metadata.json")
                ]
                if metadata_files:
                    latest_key = sorted(metadata_files)[-1]
                    resolved = f"s3://{bucket}/{latest_key}"
                    break

        if resolved is None:
            resolved = source.get("iceberg_metadata_location")

        with _pointer_cache_lock:
            _pointer_cache[cache_key] = (time.time(), resolved)
        return resolved
    except Exception as e:
        logger.warning("[iceberg] Failed to read metadata pointer: %s", e)

    # Cache the fallback so a sustained CDN/S3 outage doesn't loop the wire
    # call on every caller. Bounded by _POINTER_CACHE_TTL_SEC so recovery is
    # capped at the same staleness window as the happy path.
    fallback = source.get("iceberg_metadata_location")
    with _pointer_cache_lock:
        _pointer_cache[cache_key] = (time.time(), fallback)
    return fallback


def _refresh_local_catalog_metadata(catalog, source: dict, identifier: tuple) -> bool:
    """Find the latest metadata.json in FOS and force update the local SQLite catalog.

    This ensures Analyst users (read-only) see the latest snapshots committed by Admins,
    even though they don't share the same local SQLite database file.
    """
    namespace, table_name = identifier
    try:
        latest_loc = _read_metadata_pointer(source, identifier)
        if not latest_loc:
            return False

        # Check current location in SQLite
        db_path = _catalog_db_path(source)
        if not os.path.exists(db_path):
            return False

        import sqlite3

        with sqlite3.connect(db_path, timeout=5.0) as cat_con:
            row = cat_con.execute(
                "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = ? AND table_name = ?",
                (namespace, table_name),
            ).fetchone()

            if row:
                current_loc = row[0]
                if current_loc != latest_loc:
                    logger.info(
                        "[iceberg] Updating local catalog metadata pointer from %s to %s",
                        current_loc.split("/")[-1],
                        latest_loc.split("/")[-1],
                    )
                    cat_con.execute(
                        "UPDATE iceberg_tables SET previous_metadata_location = metadata_location, metadata_location = ? WHERE table_namespace = ? AND table_name = ?",
                        (latest_loc, namespace, table_name),
                    )
                    return True
    except Exception as e:
        logger.warning("[iceberg] Failed to refresh local catalog metadata: %s", e)

    return False


def _try_register_from_fos(catalog, source: dict, identifier: tuple):
    """Register an existing Iceberg table into the analyst's local SQLite catalog.

    The analyst's read-only FOS key only has GetObject permission (no ListBucket),
    so we rely on the metadata location exported by the admin at invite time.
    Falls back to boto3 listing if the location is not stored (e.g. older exports).
    Returns the registered table on success, or None.
    """
    namespace = identifier[0]

    # Ensure the namespace exists before any registration attempt.
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    # ── Fast path: admin-exported metadata location ───────────────────────────
    metadata_location = source.get("iceberg_metadata_location")
    if metadata_location:
        try:
            logger.info("[iceberg] Registering table %s from stored location %s", identifier, metadata_location)
            return catalog.register_table(identifier, metadata_location)
        except Exception as e:
            logger.warning("[iceberg] register_table with stored location failed: %s — falling through to discovery", e)

    # ── Fallback: list FOS bucket to find metadata (requires ListBucket) ──────
    try:
        from backend.core.duckdb import _get_fos_client

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        _, table_name = identifier

        search_prefixes = _metadata_search_prefixes(source, namespace, table_name)

        for search_prefix in search_prefixes:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix)
            metadata_files = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".metadata.json")]
            if not metadata_files:
                continue

            latest_key = sorted(metadata_files)[-1]
            loc = f"s3://{bucket}/{latest_key}"
            logger.info("[iceberg] Registering table %s via discovery from %s", identifier, loc)
            return catalog.register_table(identifier, loc)

    except Exception as e:
        logger.warning("[iceberg] Discovery-based registration failed: %s", e)

    return None


def init_iceberg_table(source: dict, create: bool = True):
    source_key = source.get("name", "default")
    with _get_service_lock(source_key):
        return _init_iceberg_table_locked(source, create)


def _init_iceberg_table_locked(source: dict, create: bool = True):
    """Create the Iceberg table in FOS if it does not exist; return the table.

    Safe to call on every provision and on every scheduler tick — it is a
    no-op when the table already exists.
    """
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.table.sorting import SortField, SortOrder
    from pyiceberg.transforms import HourTransform, IdentityTransform

    catalog = _get_catalog(source)
    identifier = _table_identifier(source)
    namespace = identifier[0]

    # Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # already exists

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_iceberg_schema = get_iceberg_schema(log_fields_config)

    try:
        if not create:
            _refresh_local_catalog_metadata(catalog, source, identifier)

        table = _load_table_cached(source, identifier, catalog)
        # Check for missing fields to support schema evolution
        missing_fields = []
        table_field_names = {f.name for f in table.schema().fields}
        for field in dynamic_iceberg_schema.fields:
            if field.name not in table_field_names:
                missing_fields.append(field)

        if missing_fields:
            logger.info(
                "🧬  \x1b[95m[commit]\x1b[0m %s: Evolving schema: adding %d fields.",
                source.get("name"),
                len(missing_fields),
            )
            try:
                with table.update_schema() as update:
                    for field in missing_fields:
                        update.add_column(field.name, field.field_type)
                # Schema evolution PUT a new metadata.json — refresh cache so the
                # next caller doesn't reload the previous (stale) location.
                _set_cached_table(source, identifier, table)
                # Republish the FOS pointer so cross-process readers (analyst
                # CLIs, any other process that hits _read_metadata_pointer) see
                # the new schema. Without this, the pointer keeps pointing at
                # the pre-evolution metadata.json until the next commit_buffer
                # finally calls _write_metadata_pointer at line 1484 — newly
                # added fields silently drop in the meantime.
                _write_metadata_pointer(source, table.metadata_location, table=table)
            except Exception as e:
                logger.error(f"[iceberg] Failed to evolve schema: {e}")
                _invalidate_cached_table(source, identifier)
        return table
    except NoSuchTableError:
        if not create:
            # Try to discover and register the table from FOS metadata.
            # This handles a fresh analyst install whose local SQLite catalog is
            # empty but the table already exists in the shared FOS bucket.
            registered = _try_register_from_fos(catalog, source, identifier)
            if registered is not None:
                return registered
            raise
        pass

    # Use natively defined Iceberg schema
    iceberg_schema = dynamic_iceberg_schema

    # Partition by hour(timestamp) — hidden partitioning, no dt= prefix in paths
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=iceberg_schema.find_field("timestamp").field_id,
            field_id=1000,
            transform=HourTransform(),
            name="timestamp_hour",
        )
    )

    # Sort by timestamp within each partition for efficient time-range pruning
    sort_order = SortOrder(
        SortField(
            source_id=iceberg_schema.find_field("timestamp").field_id,
            transform=IdentityTransform(),
        )
    )

    table = catalog.create_table(
        identifier=identifier,
        schema=iceberg_schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "schema.name-mapping.default": create_mapping_from_schema(iceberg_schema).model_dump_json(),
            "write.parquet.compression-codec": "zstd",
            "write.parquet.compression-level": "3",
            "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128 MB
        },
    )
    logger.info("🏗️  \x1b[95m[commit]\x1b[0m %s: Created table at %s", source.get("name"), table.location())
    return table


def table_location(source: dict) -> str | None:
    """Return the S3 URI of the Iceberg table root, or None if not initialised."""
    try:
        catalog = _get_catalog(source)
        table = _load_table_cached(source, _table_identifier(source), catalog)
        return table.location()
    except Exception:
        return None


# ── Manifest cache + table-info (carved out for file-size budget) ──
# Names defined in backend.core.iceberg.manifest; re-imported here so
# (a) other code in _core.py that references them by bare name still
# resolves via _core's globals, and (b) the package proxy's mirror
# treats _core as the canonical home for monkeypatch.setattr targets.
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
    _get_cached_or_scan_metadata,
    _get_scan_lock,
    _load_manifest_metadata_cache,
    _manifest_metadata_cache,
    _manifest_metadata_cache_lock,
    _manifest_metadata_loaded,
    _manifest_metadata_loaded_lock,
    _prune_empty_dirs,
    _save_manifest_metadata_cache,
    get_snapshot_calendar,
    get_table_info,
)

# ── sync_data (carved out for file-size budget) ──
from backend.core.iceberg.sync import (  # noqa: F401, E402
    _ui_metadata_cache,
    _ui_metadata_scan_locks,
    _ui_metadata_scan_locks_lock,
    sync_data,
)

# ── View binding + snapshot cache + stale-view self-heal (carved) ──
# Defined in backend.core.iceberg.view; re-imported here so the
# package proxy keeps mirroring monkeypatch.setattr writes to the
# canonical binding (tests patch e.g. update_iceberg_view,
# clear_source_caches, _update_iceberg_view_locked).
from backend.core.iceberg.view import (  # noqa: F401, E402
    _get_cache_file,
    _get_service_lock,
    _load_persistent_cache,
    _persistent_view_exists,
    _rebuild_locked,
    _rebuild_signals,
    _rebuild_signals_lock,
    _reconcile_snapshot_cache_after_sync,
    _save_persistent_cache,
    _service_locks,
    _service_locks_lock,
    _snapshot_files_cache,
    _try_fast_path_view,
    _update_iceberg_view_locked,
    _update_snapshot_cache_from_delta,
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
# These were the R-1 leaks; registering here means the next module-
# level cache that lands ships its own register() call in the same PR
# and never repeats the order-dependent failure surface.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("iceberg._view_cache", _view_cache)
_CacheRegistry.register("iceberg._snapshot_files_cache", _snapshot_files_cache)
_CacheRegistry.register("iceberg._catalog_cache", _catalog_cache)
_CacheRegistry.register("iceberg._table_object_cache", _table_object_cache)
_CacheRegistry.register("iceberg._table_summary_hash_cache", _table_summary_hash_cache)
_CacheRegistry.register("iceberg._pointer_cache", _pointer_cache)
_CacheRegistry.register("iceberg._manifest_metadata_cache", _manifest_metadata_cache)
_CacheRegistry.register("iceberg._manifest_metadata_loaded", _manifest_metadata_loaded)
_CacheRegistry.register("iceberg._ui_metadata_cache", _ui_metadata_cache)
