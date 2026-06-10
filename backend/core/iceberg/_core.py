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

import glob as _glob
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

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

from pyiceberg.exceptions import CommitFailedException
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
from backend.utils.sql_validator import escape_sql_literal

# ---------------------------------------------------------------------------
# Iceberg Schema — derived from LOG_FIELD_CATALOG (single source of truth).
#
# Iceberg does not support unsigned integer types, so unsigned DuckDB types are
# widened to the next signed type (UTINYINT/USMALLINT → int32, UINTEGER/UBIGINT
# → int64). Values are never truncated. All fields are nullable because not
# every service enables every log field group — absent fields are written as
# nulls by _align_to_schema() so the Parquet schema stays uniform.
#
# Adding a new field to LOG_FIELD_CATALOG automatically flows through to this
# schema, the Arrow schema, and the DuckDB view. The schema evolution code in
# _init_iceberg_table_locked handles adding new columns to existing tables.
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
    # Internal fields (IDs 67+)
    "_source_file",
]

_CATALOG_TYPE_MAP = {f["id"]: f["duckdb_type"] for f in LOG_FIELD_CATALOG}

_fields = [(fid, _DUCKDB_TO_ICEBERG[_CATALOG_TYPE_MAP[fid]]) for fid in _FIELD_ORDER]


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
            cdn_service_id = source.get("cdn_service_id", "")
            if cdn_service_id:
                try:
                    from backend import config as _cfg

                    api_key = _cfg.get_fastly_api_key(source.get("name", ""))
                    if api_key:
                        from backend.core.fastly.client import fastly as _fastly

                        _fastly(
                            "POST",
                            f"/service/{cdn_service_id}/purge/iceberg-table-summary",
                            token=api_key,
                            expect_empty=True,
                        )
                except Exception:
                    pass
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
    table = catalog.load_table(identifier)
    _set_cached_table(source, identifier, table)
    return table


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
        base_prefix = source.get("prefix", "").strip("/")
        namespace, table_name = _table_identifier(source)

        iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
        # Write to e.g. iceberg/default/logs/metadata_location.txt
        pointer_key = f"{iceberg_root}/{namespace}/{table_name}/metadata_location.txt"

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
        cdn_service_id = source.get("cdn_service_id", "")
        if cdn_service_id:
            try:
                from backend import config as _cfg

                api_key = _cfg.get_fastly_api_key(source.get("name", ""))
                if api_key:
                    from backend.core.fastly.client import fastly as _fastly

                    _fastly(
                        "POST",
                        f"/service/{cdn_service_id}/purge/iceberg-metadata-pointer",
                        token=api_key,
                        expect_empty=True,
                    )
                    logger.debug("[iceberg] Purged CDN surrogate key iceberg-metadata-pointer")
            except Exception as e:
                logger.warning("[iceberg] CDN purge failed (non-fatal): %s", e)
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
        from backend.models.lake import _safe_cdn_url

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        base_prefix = source.get("prefix", "").strip("/")
        # SSRF guard: only follow ``cdn_url`` when it parses as an https
        # Fastly hostname. Otherwise fall through to the S3 SDK.
        cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
        cdn_secret = source.get("cdn_secret") or ""

        iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
        pointer_keys = [
            f"{iceberg_root}/{namespace}/{table_name}/metadata_location.txt",
            f"{iceberg_root}/{namespace}.{table_name}/metadata_location.txt",
        ]

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
            search_prefixes = [
                f"{iceberg_root}/{namespace}/{table_name}/metadata/",
                f"{iceberg_root}/{namespace}.{table_name}/metadata/",
            ]
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
        base_prefix = source.get("prefix", "").strip("/")
        _, table_name = identifier

        iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
        search_prefixes = [
            f"{iceberg_root}/{namespace}/{table_name}/metadata/",
            f"{iceberg_root}/{namespace}.{table_name}/metadata/",
        ]

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

def sync_data(source: dict, progress_callback=None, start_time: str | None = None, end_time: str | None = None) -> dict:
    """Download data files from FOS that are present in the Iceberg table but missing locally.

    If start_time and end_time (ISO strings) are provided, only files matching that range
    are considered for download. Files already present locally but outside this range
    are NOT deleted if a range is specified (to allow incremental multi-range imports).
    """
    source_key = source.get("name", "default")

    # Phase 1: Brief lock just for catalog init — table object is captured, then lock released.
    # The manifest scan (plan_files) runs outside the lock so dashboard queries are not blocked.
    try:
        with _get_service_lock(source_key):
            catalog = _get_catalog(source)
            identifier = _table_identifier(source)
            _refresh_local_catalog_metadata(catalog, source, identifier)
            try:
                table = _load_table_cached(source, identifier, catalog)
            except Exception:
                table = _try_register_from_fos(catalog, source, identifier)
                if table is None:
                    return {
                        "error": "Iceberg table not found in FOS — the admin may not have committed any data yet.",
                        "files_downloaded": 0,
                    }
    except Exception as e:
        return {"error": f"Could not load table: {e}", "files_downloaded": 0}

    # Phase 2: Manifest scan — runs without the service lock so the dashboard is never blocked.
    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(cache_dir, exist_ok=True)

    # 1. Map cloud paths to local paths
    cloud_files: dict[str, tuple[str, int]] = {}  # cloud_uri -> (local_path, record_count)

    # Fast path: when no time filter is requested and the snapshot cache is
    # fresh (commit_buffer's delta update kept it aligned with this
    # metadata_loc), use the cached file list instead of doing another full
    # tbl.scan().plan_files() — that scan would re-read every immutable
    # manifest just to discover that nothing has changed. record_count
    # is not stored in the cache; downloaded-rows reporting falls back to 0
    # for delta-tracked files, which is fine for steady-state cron runs.
    cached_snapshot = _snapshot_files_cache.get(source_key)
    fast_path_used = False
    # Pre-fetch the set of basenames that local_compaction has intentionally
    # removed (merged into a bigger local file). Without this exclusion, the
    # missing_local check below treats them as "lost — re-download" and
    # forces the slow path on every tick.
    compacted_basenames: set[str] = set()
    try:
        from backend.core import metadata_db as _meta

        compacted_basenames = _meta.get_locally_compacted_basenames(
            source.get("service_id") or source.get("name") or ""
        )
    except Exception:
        pass

    if not start_time and not end_time and cached_snapshot and cached_snapshot[0] == table.metadata_location:
        try:
            cached_files = cached_snapshot[3]
            # A local-path entry in the cache means "this file was previously
            # downloaded". If any of those files are now missing on disk we
            # cannot use the fast path UNLESS local_compaction merged them
            # away (in which case "missing" is the desired state).
            missing_local = next(
                (
                    p
                    for p in cached_files
                    if not p.startswith("s3://")
                    and not os.path.exists(p)
                    and os.path.basename(p) not in compacted_basenames
                ),
                None,
            )
            if missing_local is not None:
                logger.warning(
                    "%s %s: snapshot cache references missing local file %s — falling back to full plan_files scan to recover",
                    _SYNC,
                    source.get("name"),
                    missing_local,
                )
            else:
                for entry in cached_files:
                    if entry.startswith("s3://"):
                        uri = entry
                        rel_path = uri.split("/data/")[-1] if "/data/" in uri else uri.split("/")[-1]
                        local_path = os.path.abspath(os.path.join(cache_dir, rel_path))
                        if not local_path.startswith(os.path.abspath(cache_dir) + os.sep):
                            continue
                        cloud_files[uri] = (local_path, 0)
                    else:
                        # Already-downloaded entry. Must populate cloud_files
                        # so the orphan-cleanup loop below sees its local_path
                        # in ``active_paths`` and does NOT delete it. Without
                        # this, once _reconcile_snapshot_cache_after_sync has
                        # converted every s3:// to a local path, cloud_files /
                        # active_paths would be empty and the cleanup loop
                        # would nuke the entire local cache — leaving only the
                        # next commit's freshly-arrived file. Safe because we
                        # confirmed above that every local-path entry exists
                        # on disk (so files_to_download won't try to fetch
                        # using a local path as a fake s3 key).
                        cloud_files[entry] = (entry, 0)
                fast_path_used = True
                logger.info(
                    "%s %s: sync_data using snapshot cache (%d total files, all locally present)",
                    _SYNC,
                    source.get("name"),
                    len(cached_files),
                )
        except Exception as e:
            logger.warning("[sync_data] %s: cache fast-path failed (%s) — falling back to full scan", source_key, e)
            cloud_files = {}
            fast_path_used = False

    if not fast_path_used:
        try:
            import dateutil.parser
            from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

            scan = table.scan()

            # Helper to normalize ISO strings to datetime for comparison
            def _parse_ts(ts_str: str) -> datetime:
                dt = dateutil.parser.isoparse(ts_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt

            st_dt = _parse_ts(start_time) if start_time else None
            et_dt = _parse_ts(end_time) if end_time else None

            if st_dt and et_dt and st_dt > et_dt:
                logger.warning(
                    "[sync_data] %s: Start time (%s) is after end time (%s). No files will be matched.",
                    source.get("name"),
                    start_time,
                    end_time,
                )
                return {"files_downloaded": 0, "rows_downloaded": 0, "message": "Invalid time range: start after end."}

            if start_time:
                scan = scan.filter(GreaterThanOrEqual("timestamp", st_dt.isoformat()))
            if end_time:
                scan = scan.filter(LessThanOrEqual("timestamp", et_dt.isoformat()))

            for f in scan.plan_files():
                uri = f.file.file_path
                record_count = getattr(f.file, "record_count", 0)
                # Preserve the partition folder structure for Hive partition pruning
                # PyIceberg writes to .../data/timestamp_hour=.../file.parquet
                if "/data/" in uri:
                    rel_path = uri.split("/data/")[-1]
                else:
                    rel_path = uri.split("/")[-1]

                local_path = os.path.abspath(os.path.join(cache_dir, rel_path))
                if not local_path.startswith(os.path.abspath(cache_dir) + os.sep):
                    continue
                cloud_files[uri] = (local_path, record_count)
        except Exception as e:
            return {"error": f"Metadata scan failed: {e}", "files_downloaded": 0}

    # Phase 3: File downloads — no lock held

    # 2. Download missing files
    downloaded = 0
    rows_downloaded = 0
    bytes_downloaded = 0

    # Pre-count so the callback can report X/total progress
    total_to_download = sum(1 for local_path, _ in cloud_files.values() if not os.path.exists(local_path))
    already_cached = sum(1 for local_path, _ in cloud_files.values() if os.path.exists(local_path))

    from backend.core.duckdb import _get_fos_client

    s3 = _get_fos_client(source)
    bucket = source["bucket"]
    cdn_url = (source.get("cdn_url") or "").rstrip("/")
    cdn_secret = source.get("cdn_secret") or ""

    import concurrent.futures
    import shutil

    download_lock = threading.Lock()

    def _download_file(uri, local_path, record_count):
        nonlocal downloaded, rows_downloaded, bytes_downloaded
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        key = uri.replace(f"s3://{bucket}/", "").lstrip("/")
        # Thread-safe temp file name
        tmp_path = local_path + f".tmp.{threading.get_ident()}"

        try:
            success = False
            if cdn_url:
                import urllib.parse

                # Check if the secret is provided. The CDN might expect it as a query parameter
                # 'key' (as seen in the working curl command) or as a header. We will append it
                # to the URL if a secret is configured.
                if cdn_secret:
                    # Parse the cdn_url to see if it already has query params
                    url_parts = urllib.parse.urlparse(cdn_url)
                    query = urllib.parse.parse_qs(url_parts.query)
                    query["key"] = [cdn_secret]
                    new_query = urllib.parse.urlencode(query, doseq=True)

                    # Append the key to the path so it comes before the query string
                    safe_key = urllib.parse.quote(key, safe="/=")
                    new_path = url_parts.path.rstrip("/") + "/" + safe_key

                    download_url = urllib.parse.urlunparse(
                        (url_parts.scheme, url_parts.netloc, new_path, url_parts.params, new_query, url_parts.fragment)
                    )
                else:
                    download_url = f"{cdn_url}/{urllib.parse.quote(key, safe='/=')}"

                req = urllib.request.Request(download_url)
                if cdn_secret:
                    req.add_header("x-fastly-key", cdn_secret)

                last_err = None
                cdn_headers = None
                # Measure wall-clock of the successful attempt only so the
                # usage_log row's elapsed reflects actual CDN service time,
                # not the cumulative cost of retries.
                cdn_elapsed_ms = 0.0
                for attempt in range(3):
                    try:
                        t0 = time.time()
                        with urllib.request.urlopen(req, timeout=30) as response, open(tmp_path, "wb") as out_file:
                            cdn_headers = response.headers
                            shutil.copyfileobj(response, out_file)
                        cdn_elapsed_ms = round((time.time() - t0) * 1000, 2)
                        success = True
                        break
                    except urllib.error.HTTPError as e:
                        last_err = e
                        if e.code in (401, 403):
                            # Don't retry on auth errors
                            break
                        if attempt < 2:
                            time.sleep(1)
                    except Exception as e:
                        last_err = e
                        if attempt < 2:
                            time.sleep(1)

                if not success:
                    raise RuntimeError(
                        f"CDN download failed for {key}: {last_err}. Check CDN URL, secret, and VCL configuration. URL attempted: {download_url.split('?')[0]}?key=***"
                    )
            else:
                s3.download_file(bucket, key, tmp_path)
                success = True

            os.rename(tmp_path, local_path)

            if cdn_url:
                try:
                    from backend.utils.telemetry import record_cdn_call

                    record_cdn_call(
                        "GET",
                        key,
                        cdn_elapsed_ms,
                        headers=cdn_headers,
                        bytes_count=os.path.getsize(local_path),
                        caller="sync_data_files",
                    )
                except Exception:
                    pass

            with download_lock:
                downloaded += 1
                rows_downloaded += record_count
                bytes_downloaded += os.path.getsize(local_path)
                curr_dl = downloaded

            if progress_callback:
                progress_callback(curr_dl, total_to_download, os.path.basename(local_path), record_count)

        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            raise e

    # Skip files whose basename is in the local-compacted registry: they
    # were intentionally deleted by local_compaction after being merged
    # into a larger local file. Without this filter the slow-path
    # download loop pulls them right back, starting the cycle over.
    files_to_download = [
        (u, p, c)
        for u, (p, c) in cloud_files.items()
        if not os.path.exists(p) and os.path.basename(p) not in compacted_basenames
    ]

    # 10 concurrent connections is a good balance between speed and avoiding rate limits/socket exhaustion
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_download_file, u, p, c) for u, p, c in files_to_download]
        # Iterate over as_completed to bubble up exceptions immediately
        for f in concurrent.futures.as_completed(futures):
            f.result()

    # 3. Clean up orphaned local files (not in current snapshot)
    # We skip this if a range was specified to avoid deleting files outside the range
    # that are still part of the table snapshot.
    #
    # Local-compaction writes merged rollups in two places:
    #   • <cache>/data/daily/ and <cache>/data/weekly/   (multi-day tier)
    #   • <cache>/data/timestamp_hour=*/compacted_*.parquet  (intra-hour tier)
    # Both kinds are LOCAL-ONLY — they're not part of the iceberg snapshot, so
    # they never appear in ``active_paths``. Without the skip, every sync
    # deletes them and the next sync's registry-filter blocks the iceberg
    # source files from being re-downloaded — silently dropping rows from the
    # view (production hit ~31k missing rows on 2026-06-01). Restrict the scan
    # to ``timestamp_hour=*`` dirs AND ignore ``compacted_*.parquet`` outputs.
    deleted = 0
    if not start_time and not end_time:
        active_paths = {p for p, _ in cloud_files.values()}
        try:
            data_root = os.path.join(cache_dir, "data")
            scan_root = data_root if os.path.isdir(data_root) else cache_dir
            for entry in os.listdir(scan_root) if os.path.isdir(scan_root) else []:
                if not entry.startswith("timestamp_hour="):
                    continue  # skip daily/ weekly/ and any other local-only dirs
                part_dir = os.path.join(scan_root, entry)
                for root, _, files in os.walk(part_dir):
                    for file in files:
                        if not file.endswith(".parquet"):
                            continue
                        if file.startswith("compacted_"):
                            continue  # hourly-tier compaction output (local-only)
                        local_path = os.path.abspath(os.path.join(root, file))
                        if local_path not in active_paths:
                            os.remove(local_path)
                            deleted += 1
            _prune_empty_dirs(cache_dir)
        except Exception as e:
            logger.warning(f"[iceberg] Failed to cleanup orphaned files: {e}")

    # 4. Update the resolved files cache so the next dashboard load uses the local paths
    #
    # FOS occasionally returns "[Errno 16] Reduce your request rate" right after
    # a heavy sync — the catalog reload + manifest scan piles more reads onto
    # an already-busy bucket. We retry rate-limit errors only (with backoff);
    # other failures bubble straight to the warning so they stay visible.
    import time as _time

    _MAX_RETRIES = 3

    def _is_rate_limited(err: Exception) -> bool:
        msg = str(err).lower()
        return any(
            tok in msg for tok in ("reduce your request rate", "errno 16", "slowdown", "throttl", "too many requests")
        )

    for attempt in range(_MAX_RETRIES):
        try:
            source_key = source.get("name", "default")
            with _get_service_lock(source_key):
                # Fast path: if commit_buffer's snapshot-delta update kept
                # _snapshot_files_cache aligned with the table we loaded in
                # Phase 1, we can skip the catalog reload + full plan_files()
                # scan entirely. Just flip any s3:// entries to local paths
                # for files we just downloaded.
                cached = _snapshot_files_cache.get(source_key)
                if cached and cached[0] == table.metadata_location:
                    _reconcile_snapshot_cache_after_sync(source)
                    _view_cache.pop(source_key, None)
                    break

                # Slow path: cache miss/stale — re-resolve via catalog scan.
                catalog = _get_catalog(source)
                table = _load_table_cached(source, _table_identifier(source), catalog)
                snap = table.current_snapshot()
                snapshot_id = snap.snapshot_id if snap else None

                from backend.core.duckdb import _cache_dir

                data_dir = os.path.join(_cache_dir(source), "data")

                resolved_files = []
                for f in table.scan().plan_files():
                    uri = f.file.file_path
                    if "/data/" in uri:
                        rel_path = uri.split("/data/")[-1]
                    else:
                        rel_path = uri.split("/")[-1]

                    local_path = os.path.abspath(os.path.join(data_dir, rel_path))
                    if not local_path.startswith(os.path.abspath(data_dir) + os.sep):
                        continue
                    if os.path.exists(local_path):
                        resolved_files.append(local_path)
                    else:
                        resolved_files.append(uri)

                _snapshot_files_cache[source_key] = (
                    table.metadata_location,
                    snapshot_id,
                    table.location(),
                    resolved_files,
                )
                _save_persistent_cache(source)

                # Invalidate the view SQL cache so it generates a new union with local paths
                _view_cache.pop(source_key, None)
            break  # success
        except Exception as e:
            if _is_rate_limited(e) and attempt < _MAX_RETRIES - 1:
                backoff_s = 0.5 * (2**attempt)  # 0.5s, 1s, 2s
                logger.info("[iceberg] FOS rate-limited during cache update, retrying in %.1fs", backoff_s)
                _time.sleep(backoff_s)
                continue
            logger.warning("[iceberg] Failed to update cache after sync: %s", e)
            break

    return {
        "files_downloaded": downloaded,
        "rows_downloaded": rows_downloaded,
        "bytes_downloaded": bytes_downloaded,
        "files_removed": deleted,
        "files_skipped": already_cached,
    }


def configure_duckdb_s3(con) -> None:
    """Install/load DuckDB extensions for Iceberg + httpfs.

    The fos_proxy SECRET (created in backend.core.duckdb._configure_fos) is
    the sole S3 routing config; this function used to also `SET s3_endpoint`
    etc., but those settings would clobber the proxy's endpoint scoping for
    unmatched URLs and silently bypass telemetry.
    """
    try:
        con.execute("LOAD iceberg; LOAD avro; LOAD httpfs; LOAD parquet;")
    except Exception:
        try:
            con.execute("INSTALL iceberg; INSTALL avro; INSTALL httpfs; INSTALL parquet;")
            con.execute("LOAD iceberg; LOAD avro; LOAD httpfs; LOAD parquet;")
        except Exception:
            pass


import threading

# Per-service locks to avoid global bottleneck during S3 manifest scans
_service_locks: dict[str, threading.RLock] = {}
_service_locks_lock = threading.Lock()


def _get_service_lock(source_key: str) -> threading.RLock:
    with _service_locks_lock:
        if source_key not in _service_locks:
            _service_locks[source_key] = threading.RLock()
        return _service_locks[source_key]


# Per-source view cache: source_key -> (metadata_loc, buf_set, schema_fields_tuple, view_sql, time_ms, was_fast_path)
_view_cache: dict[str, tuple] = {}

# Per-source files cache: source_key -> (metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files)
_snapshot_files_cache: dict[str, tuple] = {}

# Per-source rebuild signal: source_key -> Event set when an in-progress
# slow-path rebuild finishes. Lets cold parallel waiters wake and use
# fast-path-without-lock instead of stepping through the lock serially.
_rebuild_signals: dict[str, threading.Event] = {}
_rebuild_signals_lock = threading.Lock()


def is_stale_view_error(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like the iceberg view referencing a
    buffer parquet the commit cycle has already swept.

    Mirror of :func:`backend.repositories._base._is_stale_view_error` —
    promoted here so non-repository call paths (background crons /
    discovery / rollup writers) can share the same detection without
    importing through the repositories layer (would invert the
    architecture). The repositories alias still resolves the same way;
    new sites should import :func:`is_stale_view_error` directly from
    :mod:`backend.core.iceberg`.
    """
    msg = str(exc)
    return (
        "No files found" in msg
        or "Catalog Error: Table with name" in msg
        or "does not exist" in msg
        or "No such file or directory" in msg
    )


def execute_with_stale_view_retry(con, source: dict, fn, *args, **kwargs):
    """Run ``fn(con, *args, **kwargs)`` with one stale-view self-heal retry.

    On a :func:`is_stale_view_error`-shaped failure, bust the cached view
    SQL via :func:`clear_source_caches` (keep_snapshot_cache=True, same
    pattern QueryRunner.execute uses), force-rebind the view via
    :func:`update_iceberg_view`, then re-invoke ``fn`` once.

    Non-stale errors propagate immediately. Second-attempt failures
    (including non-stale ones) propagate too — the caller decides
    whether to log + fall through or treat as fatal.

    Use this in background-job code paths that open a raw DuckDB
    connection and don't have QueryRunner's built-in retry. Three
    documented sites today (one in rdns_cache discovery, two in
    rollups DESCRIBE) all surfaced the same buffer-deletion-race
    symptom in prod on 2026-06-10 between the deploy at 06:49 UTC
    and an external restart at 14:39 UTC.

    The arguments mirror the inline retry pattern in
    :meth:`QueryRunner.execute_with_retry` so a caller refactor can
    swap one for the other.
    """
    try:
        return fn(con, *args, **kwargs)
    except Exception as e:
        if not is_stale_view_error(e):
            raise
        clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        update_iceberg_view(con, source, force=True)
        return fn(con, *args, **kwargs)


def clear_source_caches(source_key: str, *, keep_snapshot_cache: bool = False) -> None:
    """Remove in-memory cache entries for a service.

    ``keep_snapshot_cache=True`` is used by the get_sync_status retry path
    when the cached view SQL points at a since-deleted buffer parquet. We
    want to force the view SQL to be regenerated, but we MUST NOT wipe
    ``_snapshot_files_cache`` — that's the snapshot/path cache that lets
    ``_update_iceberg_view_locked`` skip a catalog reload. Without it, a
    transient catalog-load failure (FOS rate limit, network blip) causes
    ``_update_iceberg_view_locked`` to fall into its empty-view branch and
    downgrade the working view to "WHERE false", which then sticks until
    a writer cron eventually re-fetches the catalog successfully.

    Defaults match the original semantics (full wipe) so teardown still
    clears everything.
    """
    _view_cache.pop(source_key, None)
    if not keep_snapshot_cache:
        _snapshot_files_cache.pop(source_key, None)
    with _service_locks_lock:
        _service_locks.pop(source_key, None)


def _get_cache_file(source: dict, name: str) -> str:
    from backend.core.duckdb import _cache_dir

    d = _cache_dir(source)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def _load_persistent_cache(source: dict):
    source_key = source.get("name", "default")
    if source_key in _snapshot_files_cache:
        return

    import json

    cache_file = _get_cache_file(source, "snapshot_files_cache.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
                # metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files
                _snapshot_files_cache[source_key] = (
                    data.get("metadata_loc"),
                    data.get("snapshot_id"),
                    data.get("iceberg_loc"),
                    data.get("local_iceberg_files", []),
                )
        except Exception:
            pass


def _save_persistent_cache(source: dict):
    source_key = source.get("name", "default")
    if source_key not in _snapshot_files_cache:
        return

    import json

    cache_file = _get_cache_file(source, "snapshot_files_cache.json")
    data = {
        "metadata_loc": _snapshot_files_cache[source_key][0],
        "snapshot_id": _snapshot_files_cache[source_key][1],
        "iceberg_loc": _snapshot_files_cache[source_key][2],
        "local_iceberg_files": _snapshot_files_cache[source_key][3],
    }
    try:
        with open(cache_file, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _update_snapshot_cache_from_delta(source: dict, table) -> bool:
    """Apply a just-committed snapshot's added-files delta to _snapshot_files_cache.

    Iceberg manifests are immutable: a commit only ADDS a new manifest listing
    the files this snapshot added. By reading only that one new manifest
    (typically ~1 .avro file) instead of re-scanning all manifests via
    ``tbl.scan().plan_files()`` (which re-reads ~1080 .avro files in the
    steady state of this service), we get the same "list of files in the
    table" answer at a fraction of the cloud I/O.

    Only applies the delta when the cached snapshot is the direct parent of
    the new one — if we missed an intermediate commit (concurrent writers,
    process restart between commits, etc.) we'd silently lose files, so fall
    back to the full scan in that case.

    Returns True if the cache was updated (caller can skip its own
    plan_files); False if the caller should let the normal full-scan path
    rebuild the cache.
    """
    source_key = source.get("name", "default")
    snap = table.current_snapshot()
    if snap is None:
        return False

    new_metadata_loc = table.metadata_location
    new_snapshot_id = snap.snapshot_id
    iceberg_loc = table.location()

    prev = _snapshot_files_cache.get(source_key)
    if not prev:
        return False

    prev_metadata_loc, prev_snapshot_id, _prev_iceberg_loc, prev_files = prev

    # No-op commit: same snapshot (shouldn't really happen after a successful
    # append, but guard for safety) — just refresh metadata_loc.
    if prev_snapshot_id == new_snapshot_id:
        _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, list(prev_files))
        try:
            _save_persistent_cache(source)
        except Exception:
            pass
        return True

    # Linear-history check: the cached snapshot must be the direct parent of
    # the new one. If not, we may have skipped intermediate snapshots whose
    # added files we never recorded — refuse the shortcut.
    parent_id = getattr(snap, "parent_snapshot_id", None)
    if parent_id is not None and parent_id != prev_snapshot_id:
        logger.info(
            "%s %s: skipping delta cache update — cached snapshot %s is not parent of new snapshot %s (parent=%s)",
            _ICE,
            source_key,
            prev_snapshot_id,
            new_snapshot_id,
            parent_id,
        )
        return False

    io = table.io
    try:
        new_manifests = [
            m
            for m in snap.manifests(io)
            if getattr(m, "added_snapshot_id", None) == new_snapshot_id and m.has_added_files
        ]
    except Exception as e:
        logger.warning("[iceberg] %s: delta cache update failed reading manifests: %s", source_key, e)
        return False

    if not new_manifests:
        # Snapshot exists but added no data files (e.g., schema-only change).
        # Reuse the previous file list, just refresh metadata_loc/snapshot_id.
        _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, list(prev_files))
        try:
            _save_persistent_cache(source)
        except Exception:
            pass
        return True

    from pyiceberg.manifest import ManifestEntryStatus

    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
    is_analyst = source.get("access_level") == "read_only"

    added: list[str] = []
    # Pre-seed per-manifest aggregates while we have the entries open — saves
    # `_get_cached_or_scan_metadata` (which fires after every commit via
    # `_write_table_summary_async`) from re-GETting the same .avro seconds
    # later. A fresh-commit manifest contains only ADDED entries, so the
    # ADDED-only sweep here produces the same aggregate scan_manifest would.
    per_manifest_agg: dict[str, tuple[dict, datetime | None, datetime | None, int, int]] = {}
    try:
        for manifest in new_manifests:
            manifest_key = getattr(manifest, "manifest_path", None) or repr(manifest)
            m_calendar: dict[str, dict] = {}
            m_min: datetime | None = None
            m_max: datetime | None = None
            m_files = 0
            m_size = 0
            for entry in manifest.fetch_manifest_entry(io):
                if entry.status != ManifestEntryStatus.ADDED:
                    continue
                uri = entry.data_file.file_path
                rel_path = uri.split("/data/")[-1] if "/data/" in uri else uri.split("/")[-1]
                local = os.path.abspath(os.path.join(cache_dir, rel_path))
                if not local.startswith(os.path.abspath(cache_dir) + os.sep):
                    continue
                # Match the same local-vs-URI selection rule used by
                # _update_iceberg_view_locked: prefer local file when present,
                # else fall back to the cloud URI for admins (analysts never
                # see URIs to avoid surprise S3 GETs).
                if os.path.exists(local):
                    added.append(local)
                elif not is_analyst:
                    added.append(uri)

                f = entry.data_file
                m_files += 1
                m_size += f.file_size_in_bytes
                try:
                    hour_val = f.partition[0] if f.partition else None
                    if hour_val is not None:
                        dt = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
                        if m_min is None or dt < m_min:
                            m_min = dt
                        dt_end = dt + timedelta(hours=1)
                        if m_max is None or dt_end > m_max:
                            m_max = dt_end
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = "unknown"
                except Exception:
                    date_str = "unknown"
                if date_str not in m_calendar:
                    m_calendar[date_str] = {"data_files": 0, "size_bytes": 0}
                m_calendar[date_str]["data_files"] += 1
                m_calendar[date_str]["size_bytes"] += f.file_size_in_bytes
            per_manifest_agg[manifest_key] = (m_calendar, m_min, m_max, m_files, m_size)
    except Exception as e:
        logger.warning("[iceberg] %s: delta cache update failed reading entries: %s", source_key, e)
        return False

    with _manifest_metadata_cache_lock:
        for manifest_key, agg in per_manifest_agg.items():
            _manifest_metadata_cache.setdefault(manifest_key, agg)

    updated_files = list(prev_files) + added
    _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, updated_files)
    try:
        _save_persistent_cache(source)
    except Exception:
        pass

    logger.info(
        "%s %s: snapshot cache +%d via delta (was %d, now %d) snapshot=%s parent=%s",
        _ICE,
        source_key,
        len(added),
        len(prev_files),
        len(updated_files),
        new_snapshot_id,
        prev_snapshot_id,
    )
    return True


def _reconcile_snapshot_cache_after_sync(source: dict) -> None:
    """Convert any s3:// URI entries in the cache to local paths for files
    that have since been downloaded. Called after sync_data finishes a batch
    so subsequent view builds see the local paths (avoids the URI-vs-glob
    inconsistency that would silently leave us on the iceberg_scan fallback).
    """
    source_key = source.get("name", "default")
    cached = _snapshot_files_cache.get(source_key)
    if not cached:
        return

    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
    metadata_loc, snapshot_id, iceberg_loc, files = cached

    changed = False
    new_entries: list[str] = []
    for p in files:
        if p.startswith("s3://"):
            rel_path = p.split("/data/")[-1] if "/data/" in p else p.split("/")[-1]
            local = os.path.abspath(os.path.join(cache_dir, rel_path))
            if not local.startswith(os.path.abspath(cache_dir) + os.sep):
                continue
            if os.path.exists(local):
                new_entries.append(local)
                changed = True
            else:
                new_entries.append(p)
        else:
            new_entries.append(p)

    if changed:
        _snapshot_files_cache[source_key] = (metadata_loc, snapshot_id, iceberg_loc, new_entries)
        try:
            _save_persistent_cache(source)
        except Exception:
            pass


def get_last_view_stats(source: dict) -> dict:
    source_key = source.get("name", "default")
    cached = _view_cache.get(source_key)
    if cached and len(cached) >= 6:
        return {"sql": cached[3], "time_ms": cached[4], "was_fast_path": cached[5]}
    return {}


def inject_view_debug(debug_list: list, source: dict):
    stats = get_last_view_stats(source)
    if stats and stats.get("sql"):
        # Apply the same path-list compaction as the per-query recorder
        # in repositories/_base. The view-build SQL is the WORST offender
        # because it inlines every buffer file twice (in the UNION ALL
        # RHS) — pre-compaction it accounted for ~30 KB on its own in
        # the dashboard response.
        from backend.repositories._base import _compact_sql_for_debug

        mode = (
            "FAST PATH (Local Cache / Buffer Match)"
            if stats.get("was_fast_path")
            else "SLOW PATH (S3 Read / Manifest Resolve)"
        )
        debug_list.insert(
            0,
            {
                "sql": _compact_sql_for_debug(f"-- DuckDB Iceberg View Resolution [{mode}] --\n{stats['sql']}"),
                "time_ms": stats["time_ms"],
            },
        )


def _try_fast_path_view(con, source: dict) -> bool:
    """Bind the per-service view from cache without acquiring the lock.

    Returns True if the view was bound; False if a slow-path rebuild is
    needed. Safe to call concurrently — all reads are race-free against
    a concurrent slow-path writer (cached tuple refs are stable; the
    only write here is a benign timestamp update on _view_cache).

    This split exists so 6 parallel dashboard requests for the same
    source don't serialize on the per-service RLock that ingest also
    holds during buffer commits.
    """
    import sqlite3
    import time

    from backend.core.duckdb import _cache_dir

    t_start = time.time()
    source_key = source.get("name", "default")
    cache_dir = _cache_dir(source)
    catalog_db_path = os.path.join(cache_dir, "iceberg_catalog.db")

    configure_duckdb_s3(con)

    buf_files = buffer_files(source)
    buf_set = frozenset(buf_files)

    metadata_loc = None
    try:
        if os.path.exists(catalog_db_path):
            with sqlite3.connect(catalog_db_path, timeout=5.0) as cat_con:
                row = cat_con.execute(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'default' AND table_name = 'logs'"
                ).fetchone()
                if row:
                    metadata_loc = row[0]
    except Exception:
        pass

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = get_arrow_schema(log_fields_config)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    cached = _view_cache.get(source_key)

    # See matching block in _update_iceberg_view_locked: if cached SQL is
    # S3-based but local parquets exist, refuse fast path so caller takes
    # slow path under the lock and rebuilds to local reads.
    if cached and cached[3] and "iceberg_scan(" in cached[3]:
        try:
            import glob

            data_dir = os.path.join(cache_dir, "data")
            if glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True):
                return False
        except Exception:
            pass

    if not (
        cached
        and cached[0] == metadata_loc
        and cached[1] == buf_set
        and cached[2] == tuple(sorted(dynamic_schema_field_names))
    ):
        return False

    view_sql = cached[3]
    if view_sql:
        # Always bind as a TEMP view on the fast path — the persistent view
        # is maintained by the locked rebuild path.  Concurrent fast-path
        # callers (pool checkouts) would otherwise race on the shared catalog
        # and trigger "write-write conflict on alter".
        exec_sql = view_sql
        if view_sql.startswith("CREATE OR REPLACE VIEW "):
            exec_sql = view_sql.replace("CREATE OR REPLACE VIEW ", "CREATE OR REPLACE TEMP VIEW ", 1)
        try:
            con.execute(exec_sql)
        except Exception as e:
            logger.warning("[iceberg] fast-path view re-bind failed for %s: %s", source_key, e)
            return False

    t_end = time.time()
    _view_cache[source_key] = (
        metadata_loc,
        buf_set,
        tuple(sorted(dynamic_schema_field_names)),
        view_sql,
        round((t_end - t_start) * 1000, 2),
        True,
    )
    return True


def _rebuild_locked(con, source: dict, source_key: str) -> None:
    """Run the slow path under the lock and signal completion."""
    ev = threading.Event()
    with _rebuild_signals_lock:
        _rebuild_signals[source_key] = ev
    try:
        _update_iceberg_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(source_key) is ev:
                del _rebuild_signals[source_key]


def update_iceberg_view(con, source: dict, lock_timeout: float = 5.0, force: bool = False) -> None:
    """Refresh the per-service DuckDB view over the Iceberg table + buffer.

    ``lock_timeout`` (default 5s) caps how long we wait on the per-service
    RLock that ingest also acquires for buffer commits. Prior default was
    1s, which was often shorter than a buffer-commit cycle — when callers
    landed in that window, this function fell back to executing the
    cached view SQL, which after a recent commit could reference a
    just-deleted buffer parquet and surface as ``No files found that
    match the pattern …/buffer/batch_*.parquet`` on the next read. Five
    seconds is long enough to outlast a typical commit without making
    sync-status polls feel sticky.

    ``force=True`` skips the lock-free fast path and goes straight to a
    full rebuild under the lock. The QueryRunner self-heal path uses
    this: when a query already failed with a stale-view IOException,
    the fast path can't help — its buf_set check might match cached
    state that's still inconsistent with what the DuckDB query planner
    just saw on disk, OR (the symptom-from-prod) the cached view SQL
    has hardcoded file paths and re-executing it just re-binds the same
    bad SQL. Force-rebuild reads disk fresh under the lock and
    regenerates the SQL.
    """
    source_key = source.get("name", "default")

    # Lock-free fast path first. Parallel dashboard reads (6+ endpoints
    # per page load) only need the lock when a real rebuild is required.
    # Skipped on ``force=True`` (see self-heal path in QueryRunner).
    if not force and _try_fast_path_view(con, source):
        return

    lock = _get_service_lock(source_key)

    # If the lock is held, another caller is rebuilding. Wait on their
    # completion signal, then retry the fast path WITHOUT the lock — N
    # cold-parallel waiters can then run fast-path concurrently instead
    # of stepping through the lock serially.
    if not lock.acquire(blocking=False):
        with _rebuild_signals_lock:
            ev = _rebuild_signals.get(source_key)
        if ev is not None and ev.wait(timeout=lock_timeout):
            if _try_fast_path_view(con, source):
                return
        # Either we raced ahead of _rebuild_locked setting the signal,
        # or the rebuild produced no fast-path-cacheable result. Fall
        # through to the original blocking-acquire path.
        if not lock.acquire(timeout=lock_timeout):
            # Ingest is still holding the lock. Fallback order:
            #   1. Cached view SQL → re-execute on this connection.
            #   2. Persistent view on this DB → no-op (slightly stale).
            #   3. Neither — extend the lock wait so the caller has a
            #      view to query (production-observed: restart-during-
            #      sync left RO sessions with "table not found").
            cached = _view_cache.get(source_key)
            if cached and cached[3]:
                try:
                    con.execute(cached[3])
                except Exception:
                    pass
                return
            if _persistent_view_exists(con, source):
                return
            logger.info(
                "[iceberg] %s: cache empty and no persistent view; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[iceberg] %s: extended 60s lock wait timed out; view rebuild deferred",
                    source_key,
                )
                return
            try:
                _rebuild_locked(con, source, source_key)
            finally:
                lock.release()
            return
    try:
        _rebuild_locked(con, source, source_key)
    finally:
        lock.release()


def _persistent_view_exists(con, source: dict) -> bool:
    """Return True if the per-service Iceberg view already exists on this
    connection's database. Used by ``update_iceberg_view`` to skip the
    extended lock wait when the caller can already query the view (even
    if it's slightly stale)."""
    try:
        from backend.core.duckdb import _safe_table_name

        table_name = _safe_table_name(source["name"])
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _update_iceberg_view_locked(con, source: dict) -> None:
    import sqlite3
    import time

    from backend.core.duckdb import _cache_dir, _safe_table_name

    # Re-check the fast path under the lock — state may have become
    # cacheable while we waited (a concurrent slow-path writer just
    # finished and primed _view_cache).
    if _try_fast_path_view(con, source):
        return

    t_start = time.time()
    table_name = _safe_table_name(source["name"])
    source_key = source.get("name", "default")
    cache_dir = _cache_dir(source)
    catalog_db_path = os.path.join(cache_dir, "iceberg_catalog.db")

    configure_duckdb_s3(con)

    buf_files = buffer_files(source)
    buf_set = frozenset(buf_files)

    metadata_loc = None
    try:
        if os.path.exists(catalog_db_path):
            with sqlite3.connect(catalog_db_path, timeout=5.0) as cat_con:
                row = cat_con.execute(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'default' AND table_name = 'logs'"
                ).fetchone()
                if row:
                    metadata_loc = row[0]
    except Exception:
        pass

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None

    dynamic_arrow_schema = get_arrow_schema(log_fields_config)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    logger.info("▶️  %s %s: View refresh started...", _ICE_PLAIN, source_key)

    # Try to load from persistent cache if memory cache is empty
    _load_persistent_cache(source)

    iceberg_loc = None
    local_iceberg_files = []

    # We can skip reading from S3 entirely if ONLY the buffer changed.
    cached_files = _snapshot_files_cache.get(source_key)
    if cached_files and cached_files[0] == metadata_loc:
        snapshot_id = cached_files[1]
        iceberg_loc = cached_files[2]
        local_iceberg_files = cached_files[3]
    elif metadata_loc is None:
        # Never-committed service: the local SQLite catalog has no metadata_location
        # row for this table, so there is no Iceberg snapshot to fetch. Skipping
        # the S3 round-trip here saves 6-14s on every cold dashboard query for
        # services that haven't ingested anything (or whose init_iceberg_table
        # call silently failed to write metadata.json to FOS — observed when
        # fos_endpoint is unreachable, e.g. local dev / load-test services).
        # The view will be built from buffer files only (if any) below, or
        # downgraded to an empty WHERE-false view by the existing fall-through.
        snapshot_id = None
        tbl = None
        snap = None
    else:
        # The table committed (new metadata_loc) or we had a full cache miss.
        try:
            catalog = _get_catalog(source)
            tbl = _load_table_cached(source, _table_identifier(source), catalog)
            snap = tbl.current_snapshot()
            snapshot_id = snap.snapshot_id if snap else None
        except Exception:
            snapshot_id = None
            tbl = None
            snap = None

        if tbl is not None and snap is not None:
            try:
                from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

                iceberg_loc = tbl.location()
                data_dir = os.path.join(cache_dir, "data")

                scan = tbl.scan()
                tr = source.get("time_range")
                if tr:
                    import dateutil.parser

                    if tr.get("start"):
                        st_dt = dateutil.parser.isoparse(tr["start"])
                        if st_dt.tzinfo is None:
                            st_dt = st_dt.replace(tzinfo=UTC)
                        scan = scan.filter(GreaterThanOrEqual("timestamp", st_dt.isoformat()))

                    # For Analysts (read_only), we always honor end_time to bound their manual imports.
                    # For Admins, we usually don't filter by end_time to allow new logs to stream in,
                    # unless they have explicitly disabled cron sync.
                    is_analyst = source.get("access_level") == "read_only"
                    if tr.get("end") and (
                        is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)
                    ):
                        et_dt = dateutil.parser.isoparse(tr["end"])
                        if et_dt.tzinfo is None:
                            et_dt = et_dt.replace(tzinfo=UTC)
                        scan = scan.filter(LessThanOrEqual("timestamp", et_dt.isoformat()))

                for f in scan.plan_files():
                    uri = f.file.file_path
                    if uri.startswith("file://"):
                        # Local-only warehouse: the URI IS the local path.
                        # Skip the FOS-style /data/ rewrite and just use it.
                        local_path = uri[len("file://") :]
                        if os.path.exists(local_path):
                            local_iceberg_files.append(local_path)
                        continue
                    if "/data/" in uri:
                        rel_path = uri.split("/data/")[-1]
                    else:
                        rel_path = uri.split("/")[-1]

                    local_path = os.path.abspath(os.path.join(data_dir, rel_path))
                    if not local_path.startswith(os.path.abspath(data_dir) + os.sep):
                        continue
                    if os.path.exists(local_path):
                        local_iceberg_files.append(local_path)
                    elif source.get("access_level") != "read_only":
                        # Admins fall back to S3 so they can query immediately.
                        # Analysts only query what they have explicitly synced to avoid massive S3 GET costs.
                        local_iceberg_files.append(uri)

                # Cache by metadata_loc instead of snapshot_id
                _snapshot_files_cache[source_key] = (metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files)
                _save_persistent_cache(source)
            except Exception as e:
                logger.warning("[iceberg] plan_files() failed for %s: %s", source_key, e)

    if not iceberg_loc and not buf_files and not local_iceberg_files:
        # All three "data source" channels are empty. There are two reasons
        # this happens:
        #   (a) genuinely fresh service — no data anywhere yet. Empty view
        #       is correct.
        #   (b) transient catalog-load failure (FOS rate limit / network
        #       blip / lock contention). We previously HAD a working
        #       snapshot, but the in-memory cache was wiped and the
        #       re-fetch failed this attempt.
        #
        # In case (b) we must NOT downgrade — replacing a working view
        # with "WHERE false" makes the dashboard show 0 logs and persists
        # in _view_cache until a writer cron eventually rebuilds. Two
        # signals tell us this is case (b):
        #
        # 1. _view_cache already has a non-empty entry. Cheapest check;
        #    catches the steady-state recurrence.
        # 2. The service's ingest sqlite metadata shows files with rows.
        #    Catches the post-process-restart case where _view_cache is
        #    empty even though we have real data on disk / in the table.
        #    Without this, a transient FOS failure on the FIRST poll after
        #    a restart poisons the persistent view to "WHERE false" and
        #    no future poll can recover (the next "prior_was_empty" check
        #    lets the same downgrade happen again).
        prior = _view_cache.get(source_key)
        prior_sql = prior[3] if prior else None
        prior_was_empty = (not prior_sql) or ("WHERE false" in prior_sql)
        if prior_sql and not prior_was_empty:
            logger.info(
                "[iceberg] %s: skipping empty-view downgrade (catalog re-fetch "
                "returned no data but cached view is non-empty — likely transient)",
                source_key,
            )
            return

        # Second signal: ingest metadata. We have rows recorded as ingested
        # → refuse to overwrite with WHERE false. The data exists; this
        # poll is just blind.
        try:
            from backend.core import metadata_db as _meta

            _summary = _meta.get_ingested_files_status_summary(source_key)
            ingested_rows = _summary["total_rows"]
            ingested_files = _summary["file_count"]
        except Exception:
            ingested_rows = 0
            ingested_files = 0
        if ingested_rows > 0:
            logger.info(
                "[iceberg] %s: skipping empty-view downgrade — ingest metadata shows "
                "%d rows across %d files (catalog blind this poll, not a fresh service)",
                source_key,
                ingested_rows,
                ingested_files,
            )
            return

        empty_sql: str | None = None
        try:
            cols = ", ".join(f"NULL::{_arrow_to_duckdb(f.type)} AS {f.name}" for f in dynamic_arrow_schema)
            empty_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT {cols} WHERE false"
            con.execute(empty_sql)
        except Exception:
            empty_sql = None
        t_end = time.time()
        _view_cache[source_key] = (
            metadata_loc,
            buf_set,
            tuple(sorted(dynamic_schema_field_names)),
            empty_sql,
            round((t_end - t_start) * 1000, 2),
            False,
        )
        return

    parts: list[str] = []

    local_paths = [p for p in local_iceberg_files if not p.startswith("s3://")]
    s3_paths = [p for p in local_iceberg_files if p.startswith("s3://")]

    # Belt-and-suspenders against costly S3 fallback: even if local_paths is
    # empty (because plan_files happened to run before sync_data finished),
    # check the local data_dir directly. If it has parquet files on disk, we
    # MUST use them — otherwise dashboard queries route through iceberg_scan
    # over S3 and rack up Class B reads on every poll.
    #
    # Local-only (file://) warehouse: Iceberg writes data files under
    # warehouse/<namespace>/<table>/data/ rather than cache/{bucket}/data/.
    # Point data_dir at the actual on-disk location so the glob below and the
    # eventual read_parquet view SQL hit real files.
    if _is_local_only_source(source) and iceberg_loc and iceberg_loc.startswith("file://"):
        data_dir = os.path.join(iceberg_loc[len("file://") :], "data")
    else:
        data_dir = os.path.join(cache_dir, "data")
    if not local_paths:
        try:
            import glob as _glob

            disk_parquets = _glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
            if disk_parquets:
                # Synthesize a sentinel so the local-read branch fires below
                local_paths = disk_parquets[:1]
                logger.info(
                    "[iceberg] %s: plan_files returned 0 local paths but data/ has %d parquets — "
                    "using local glob anyway to avoid cloud reads",
                    source_key,
                    len(disk_parquets),
                )
        except Exception:
            pass

    # Defensive: some parquet files may already include the computed
    # timestamp_hour / dt columns (e.g., after a PyIceberg-routed compaction
    # that preserves partition columns in the output file). If we then add
    # `, ... AS timestamp_hour` in the outer SELECT, the resulting view
    # branch has TWO columns named timestamp_hour and UNION ALL BY NAME
    # fails with a Binder Error. EXCLUDE them defensively before re-adding.
    def _strip_computed(read_parquet_expr: str) -> str:
        try:
            probe = con.execute(f"SELECT * FROM {read_parquet_expr} LIMIT 0").description or []
            existing = {d[0] for d in probe}
        except Exception:
            existing = set()
        cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in existing)
        exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
        return (
            f"SELECT *{exclude_clause}, "
            f"CAST(strftime(timestamp, '%Y-%m-%d-%H') AS VARCHAR) as timestamp_hour, "
            f"CAST(strftime(timestamp, '%Y-%m-%d') AS VARCHAR) as dt "
            f"FROM {read_parquet_expr}"
        )

    if local_paths:
        parts.append(
            _strip_computed(
                f"read_parquet('{data_dir}/**/*.parquet', union_by_name=true, filename=true, hive_partitioning=false)"
            )
        )

    # Use iceberg_scan when:
    # (a) plan_files() returned S3 URIs and no local files are cached yet, OR
    # (b) plan_files() failed silently but iceberg_loc is known (avoids WHERE false view)
    if iceberg_loc and not local_paths and (s3_paths or not local_iceberg_files):
        parts.append(_strip_computed(f"iceberg_scan('{escape_sql_literal(iceberg_loc)}', allow_moved_paths=true)"))
        logger.info(
            "%s Falling back to iceberg_scan for %s (s3_paths=%d, local_iceberg_files=%d).",
            _ICE,
            source_key,
            len(s3_paths),
            len(local_iceberg_files),
        )
    elif s3_paths:
        # Demoted from INFO to DEBUG (2026-06-01): this fires on every
        # view refresh whenever the local cache lags the iceberg manifest
        # (very common during catch-up / right after a commit). Useful for
        # debugging stale-view issues, not useful as a routine signal —
        # was spamming the prod VM backend log every few seconds with no
        # actionable content.
        logger.debug(
            "%s Skipping %d missing cloud files in view (local files present, CDN sync pending).",
            _ICE,
            len(s3_paths),
        )

    # Re-check existence: commit_buffer() may have deleted files during the metadata
    # scan above (which can take seconds), causing an IO Error in CREATE VIEW.
    buf_files = [p for p in buf_files if os.path.isfile(p)]

    if buf_files:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in buf_files)
        parts.append(_strip_computed(f"read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"))

    if not parts:
        cols = ", ".join(f"NULL::{_arrow_to_duckdb(f.type)} AS {f.name}" for f in dynamic_arrow_schema)
        union_sql = f"SELECT {cols} WHERE false"
    else:
        union_sql = " UNION ALL BY NAME ".join(parts)

        from backend.utils import field_codes as fc

        c_speed_case = fc.duckdb_decode_case("c_speed", fc.CONN_SPEED_ENCODE)
        p_type_case = fc.duckdb_decode_case("p_type", fc.PROXY_TYPE_ENCODE)
        p_desc_case = fc.duckdb_decode_case("p_desc", fc.PROXY_DESC_ENCODE)

        # ttl/age are stored as FLOAT in iceberg (Fastly emits jittery
        # microsecond-precision values, e.g. "3600.027s"), but they're integer
        # seconds semantically. Surface them as INTEGER so Top-N GROUP BY
        # buckets cleanly instead of fragmenting into ~10 sub-second values.
        # Only EXCLUDE columns that exist in the schema — group B is optional.
        exclude_cols = ["c_speed", "p_type", "p_desc"]
        select_extras = [
            f"{c_speed_case} AS c_speed",
            f"{p_type_case} AS p_type",
            f"{p_desc_case} AS p_desc",
        ]
        if "ttl" in dynamic_schema_field_names:
            exclude_cols.append("ttl")
            select_extras.append('CAST(ROUND("ttl") AS INTEGER) AS ttl')
        if "age" in dynamic_schema_field_names:
            exclude_cols.append("age")
            select_extras.append('CAST(ROUND("age") AS INTEGER) AS age')

        # Wrap the union to decode any previously ingested raw enum values
        # and coerce float-stored integer fields to integer.
        union_sql = f"SELECT * EXCLUDE ({', '.join(exclude_cols)}), {', '.join(select_extras)} FROM ({union_sql})"

        # Apply strict time-bounding for analyst manual imports so they don't see
        # the "ragged edges" of the underlying hourly files.
        tr = source.get("time_range")
        is_analyst = source.get("access_level") == "read_only"

        if tr and (is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)):
            # Security: validate via isoparse before interpolation. Without
            # this, an attacker-controlled tr["start"] / tr["end"] dict value
            # (these come from saved-view JSON which originates from the
            # frontend) is interpolated raw into DuckDB SQL — a payload like
            #   "2024-01-01'; ATTACH '/tmp/x.db' AS y; --"
            # would execute multi-statement SQL against the connection.
            # isoparse rejects anything that isn't a valid ISO-8601 timestamp;
            # we then interpolate the canonical .isoformat() output, which
            # contains only digits, ":", "-", "T", "+", and "Z".
            import dateutil.parser as _dt

            where_clauses = []
            if tr.get("start"):
                try:
                    start_iso = _dt.isoparse(str(tr["start"])).isoformat()
                except (ValueError, TypeError) as e:
                    raise ValueError(f"invalid time_range start: {e}") from e
                where_clauses.append(f"timestamp >= '{start_iso}'::TIMESTAMPTZ")
            if tr.get("end"):
                try:
                    end_iso = _dt.isoparse(str(tr["end"])).isoformat()
                except (ValueError, TypeError) as e:
                    raise ValueError(f"invalid time_range end: {e}") from e
                where_clauses.append(f"timestamp <= '{end_iso}'::TIMESTAMPTZ")
            if where_clauses:
                union_sql = f"SELECT * FROM ({union_sql}) WHERE {' AND '.join(where_clauses)}"

    view_sql_created: str | None = None
    try:
        # Detect read-only mode so we can switch to CREATE OR REPLACE TEMP VIEW
        # (which works on RO connections — regular CREATE VIEW does not).
        #
        # The previous detection used `PRAGMA database_list` and checked
        # `row[2] == "read-only"` — but row[2] is the FILE PATH, not a
        # readonly flag (database_list returns (seq, name, file)). The check
        # was always False, so RO connections always tried CREATE VIEW and
        # surfaced "ERROR Failed to create view … Cannot execute statement
        # of type CREATE on database … attached in read-only mode!" on every
        # dashboard query. Result: the view was effectively never refreshed
        # from any RO connection, and reads against the stale/empty view
        # showed "No data available" on the dashboard.
        #
        # `duckdb_databases()` is the documented system function for this;
        # it has a `readonly` boolean column.
        is_read_only = False
        try:
            res = con.execute(
                "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
            ).fetchone()
            if res is not None and bool(res[0]):
                is_read_only = True
        except Exception:
            pass

        if is_read_only:
            create_stmt = f"CREATE OR REPLACE TEMP VIEW {table_name} AS {union_sql}"
        else:
            create_stmt = f"CREATE OR REPLACE VIEW {table_name} AS {union_sql}"

        con.execute(create_stmt)

        if not is_read_only:
            view_sql_created = create_stmt
            # Clear the schema cache only when the column set actually
            # changed. Previously this was unconditional, but the post-ingest
            # view refresh runs on a writer connection every cron tick where
            # rows_inserted > 0 (i.e. virtually every tick on a busy
            # service), which blew away duckdb._schema_cache and made its
            # 60 s TTL irrelevant. Result: the next heavy refresh_config_status
            # paid the full ~800 ms SUMMARIZE every minute even though the
            # underlying columns are stable across hundreds of ticks.
            # Comparing tuple(sorted(field_names)) against the prior cache
            # entry catches all column add/remove/rename cases (the only
            # thing get_schema cares about); per-row data churn doesn't
            # invalidate column metadata, so it's safe to keep the cache.
            try:
                new_columns = tuple(sorted(dynamic_schema_field_names))
                prior = _view_cache.get(source_key)
                prior_columns = prior[2] if prior else None
                if prior_columns != new_columns:
                    from backend.core.duckdb import _clear_schema_cache

                    _clear_schema_cache(source_key)
            except Exception:
                pass
    except Exception as e:
        logger.error("[iceberg] Failed to create view %s: %s", table_name, e)

    t_end = time.time()
    duration_ms = (t_end - t_start) * 1000
    logger.info("⏹️  %s %s: View refresh complete (%.0f ms).", _ICE_PLAIN, source_key, duration_ms)
    _view_cache[source_key] = (
        metadata_loc,
        buf_set,
        tuple(sorted(dynamic_schema_field_names)),
        view_sql_created,
        round((t_end - t_start) * 1000, 2),
        False,
    )


# ---------------------------------------------------------------------------
# Admin / UI metadata
# ---------------------------------------------------------------------------

# Cache for UI metadata scans which are very slow on large tables
# source_key -> (metadata_location, (data_files, size_bytes, calendar))
_ui_metadata_cache: dict[str, tuple] = {}
_ui_metadata_scan_locks: dict[str, threading.Lock] = {}
_ui_metadata_scan_locks_lock = threading.Lock()

# Per-manifest aggregate cache: manifest_path -> (calendar, min_ts, max_ts, files, size).
# Iceberg manifests are immutable once written — a given manifest's entries (and
# therefore its calendar/min/max contribution) never change. This cache lets
# `_get_cached_or_scan_metadata` skip re-fetching every manifest after each
# commit; only manifests new to the current snapshot trigger an .avro GET.
# Persisted to disk per-service so restarts don't pay a ~1250-manifest cold
# scan (~12 MB FOS GETs) on the first cron_compact tick.

# ── Manifest cache + table-info (carved out for file-size budget) ──
# Names defined in backend.core.iceberg.manifest; re-imported here so
# (a) other code in _core.py that references them by bare name still
# resolves via _core's globals, and (b) the package proxy's mirror
# treats _core as the canonical home for monkeypatch.setattr targets.
from backend.core.iceberg.manifest import (  # noqa: F401, E402
    _manifest_metadata_cache,
    _manifest_metadata_cache_lock,
    _manifest_metadata_loaded,
    _manifest_metadata_loaded_lock,
    _load_manifest_metadata_cache,
    _save_manifest_metadata_cache,
    _get_scan_lock,
    _get_cached_or_scan_metadata,
    get_table_info,
    get_snapshot_calendar,
    _align_to_schema,
    _arrow_to_duckdb,
    _prune_empty_dirs,
)

# ── Buffer / commit / optimize / cloud-maintenance (carved out) ──
# Defined in backend.core.iceberg.buffer; re-imported here so other
# code in _core.py + the package proxy + test monkeypatch sites
# resolve the same canonical binding.
from backend.core.iceberg.buffer import (  # noqa: F401, E402
    _TOMBSTONE_SUFFIX,
    _TOMBSTONE_GRACE_SECONDS,
    _tombstone_marker_path,
    _is_tombstone_marker,
    _tombstoned_parquet_paths,
    tombstone_buffer_files,
    sweep_tombstoned_buffer_files,
    buffer_files,
    _quarantine_dir,
    _quarantine_buffer_file,
    buffer_backlog_stats,
    write_to_buffer,
    commit_buffer,
    optimize_table,
    run_cloud_maintenance,
    _BUFFER_COMMIT_CHUNK_SIZE,
)
