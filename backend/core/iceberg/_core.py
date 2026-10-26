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
import sys
import threading
import urllib.parse
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pyarrow as pa

_C = "\x1b[36m"  # Cyan — iceberg operations
_R = "\x1b[0m"
_ICE = f"🧊 {_C}[iceberg]{_R}"

logger = logging.getLogger(__name__)

from backend.core.log_fields import LOG_FIELD_CATALOG

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

CLIENT_VITALS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("metric_name", pa.string()),
        ("metric_value", pa.float64()),
        ("metric_rating", pa.string()),
        ("pathname", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("device", pa.string()),
        ("cid", pa.string()),
        ("req_id", pa.string()),
        ("city", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
        ("pop", pa.string()),
        ("tls", pa.string()),
        ("ttfb", pa.float64()),
    ]
)

CLIENT_ERRORS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("error_message", pa.string()),
        ("error_file", pa.string()),
        ("error_line", pa.int32()),
        ("error_col", pa.int32()),
        ("pathname", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("device", pa.string()),
        ("cid", pa.string()),
        ("req_id", pa.string()),
        ("city", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
        ("pop", pa.string()),
        ("tls", pa.string()),
        ("ttfb", pa.float64()),
    ]
)

RUM_TABLE_SCHEMAS = {
    "client_vitals": CLIENT_VITALS_ARROW_SCHEMA,
    "client_errors": CLIENT_ERRORS_ARROW_SCHEMA,
}

RUM_NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "client_vitals": ("timestamp", "metric_name", "pathname", "cid", "req_id"),
    "client_errors": ("timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id"),
}


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
# ── DuckLake catalog attach + registration helpers (folded from _ducklake.py) ──

import hashlib
import logging
import re
import time
from collections.abc import Iterator

from backend import config
from backend.core.duckdb import escape_sql_literal, require_safe_ident

logger = logging.getLogger(__name__)


# Serializes ``ATTACH ... AS lake`` across every connection in this process.
# Concurrent connections attaching a DuckLake catalog under the SAME alias
# name ("lake") race and silently corrupt each other's local catalog
# metadata — the ATTACH statement on EVERY racing connection reports
# success (no exception), but ``ducklake_snapshots('lake')`` (and anything
# else touching the catalog) then fails forever on that connection with
# ``Catalog "__ducklake_metadata_lake" does not exist``. Verified
# empirically: 6 threads each opening their own read-only connection to the
# same on-disk .duckdb file and calling ``_ducklake_attach`` concurrently
# reproduced the corruption on ALL 6 connections every time; serializing
# the attach with this lock made all 6 succeed, every time. This is what
# poisons pooled connections built during startup, when several requests
# concurrently trigger fresh connection builds. A single-file DuckDB attach
# to the same physical file is unaffected (that case is already
# process-exclusive per ADR-18) — this is specifically about the ``lake``
# alias/extension state, not the main database file.
_attach_lock = threading.Lock()

# Bounds how long a WAITING caller will queue for _attach_lock before giving
# up. The lock itself must stay unconditional mutual exclusion (see above) —
# this only lets an impatient caller stop waiting and fail fast instead of
# hanging its DuckDB pool connection slot forever when the current holder's
# ATTACH is blocked on a genuinely slow or stuck OS-level file lock (e.g. a
# concurrent cron writer mid-ingest). That blocking call has no internal
# timeout of its own — DuckDB's ATTACH can sit in an uninterruptible kernel
# wait for the file lock — so without this bound a queued reader can wait
# indefinitely, and pool exhaustion cascades from there.
_ATTACH_LOCK_TIMEOUT_S = float(os.environ.get("DUCKLAKE_ATTACH_LOCK_TIMEOUT_S", "60") or "60")

# Bounds how long a caller retries the ATTACH statement itself after hitting
# a "unique file handle conflict" — a DIFFERENT connection in this process
# still holds an open read-write attach on the same local .ducklake file
# (the lock above only serializes the brief ATTACH *call*, not how long a
# winner keeps its attach open while it does real work). Observed in
# production under bursty concurrent cron activity (log_discovery + commit
# + rum_commit + rollups overlapping): the conflict reliably clears within
# one or two 10s cron ticks, so the previous 5-attempt/1.5s (~7.5s total)
# budget was too short and surfaced as a logged error + a skipped ingest
# tick even though the very next tick always succeeded. Each sleep releases
# _attach_lock first (see the loop below) so an unrelated connection isn't
# blocked queuing behind this one's wait for a THIRD, unrelated connection.
_ATTACH_CONFLICT_RETRY_ATTEMPTS = int(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_ATTEMPTS", "45") or "45")
_ATTACH_CONFLICT_RETRY_SLEEP_S = float(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_SLEEP_S", "1.5") or "1.5")

# Same knob the local tiered compaction honors (backend/core/local_compaction.py
# _MAX_PARTITION_BYTES). Keeping the two caps on one env var means DuckLake
# merges and local compaction can never disagree about the ceiling.
_DUCKLAKE_TARGET_FILE_SIZE_MB = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256") or "256")


def _safe_ident(name: str) -> str:
    """Sanitize an arbitrary string into a bare SQL identifier fragment."""
    clean = re.sub(r"[^A-Za-z0-9_]", "_", name or "").strip("_").lower()
    return clean or "default"


def ducklake_table_name(src: dict, table_name: str = "logs") -> str:
    """Per-service DuckLake table name for ``src``.

    Under a shared catalog (``DUCKLAKE_CATALOG`` set) every service attaches
    the SAME ``lake`` — a bare ``lake.logs`` mixes tenants. This helper is
    the single naming authority used by the view builder, the buffer
    commit, and ingest; anything that reads or writes ``lake.*`` must go
    through it. Non-``logs`` tables (RUM ``client_vitals`` /
    ``client_errors``) get a per-service suffix too.
    """
    service_id = src.get("service_id") or src.get("name") or "default"
    base = f"logs_{_safe_ident(str(service_id))}"
    if table_name != "logs":
        base = f"{base}__{_safe_ident(table_name)}"
    return base


def _is_local_only(source: dict) -> bool:
    """Mirror of ``_core._is_local_only_source`` (kept inline to avoid a
    module-load cycle: _core imports this module during package init)."""
    if source.get("fos_local_warehouse") is True:
        return True
    endpoint = source.get("fos_endpoint") or source.get("endpoint") or ""
    return endpoint in ("http://localhost:0", "http://127.0.0.1:0")


def _default_data_path(source: dict) -> str:
    """Default DuckLake DATA_PATH for ``source``.

    High-scale workers and the serving pod may not share a filesystem,
    so cloud-backed sources use durable FOS storage in that topology. Standard
    deployments keep analytical data local for low-latency reads.
    """
    force_local = os.getenv("DUCKLAKE_LOCAL_STORAGE", "").lower() in ("1", "true", "yes")
    if config.is_high_scale_mode(source) and not force_local and source.get("bucket") and not _is_local_only(source):
        prefix = (source.get("prefix") or "").strip("/")
        base = f"{prefix}/ducklake" if prefix else "ducklake"
        return f"s3://{source['bucket']}/{base}/"

    service_id = source.get("service_id") or source.get("name", "default")
    local_path = config.SERVICES_DATA_DIR / str(service_id) / "ducklake"
    local_path.mkdir(parents=True, exist_ok=True)
    return str(local_path)


def _resolve_catalog_dsn(service_id: object) -> str | None:
    """Resolve and normalize the mandatory PostgreSQL DuckLake catalog DSN."""
    dsn = config.DUCKLAKE_CATALOG
    if not dsn:
        logger.error(
            "[ducklake] %s: DUCKLAKE_CATALOG is unset; PostgreSQL DSN is required",
            service_id,
        )
        return None
    if dsn.startswith(("postgres://", "postgresql://")):
        return f"postgres:{dsn}"
    if dsn.startswith("postgres:"):
        return dsn
    env_dsn = os.environ.get("DUCKLAKE_CATALOG", "")
    if env_dsn.startswith(("postgres://", "postgresql://")):
        return f"postgres:{env_dsn}"
    logger.error(
        "[ducklake] %s: DUCKLAKE_CATALOG must be a postgres DSN, got: %s",
        service_id,
        dsn,
    )
    return None


def _resolve_data_path(source: dict, service_id: object) -> str:
    """Resolve DATA_PATH for ``source`` and enforce the remote-S3 scan guard."""
    data_path = config.DUCKLAKE_DATA_PATH or _default_data_path(source)
    if data_path.startswith(("s3://", "http://", "https://")):
        allow_remote = os.getenv("ALLOW_REMOTE_S3_PARQUET_SCAN", "0").lower() in (
            "1",
            "true",
            "yes",
        ) or config.is_high_scale_mode(source)
        if not allow_remote and not source.get("allow_remote_parquet_scan"):
            local_fallback = _default_data_path(source)
            logger.error(
                "🚨 [storage-guard] BLOCKED remote S3 DuckLake parquet scan for %s (data_path=%s). "
                "All analytical data must be stored locally on disk. Falling back to local data path: %s. "
                "Set ALLOW_REMOTE_S3_PARQUET_SCAN=1 if emergency S3 access is strictly required.",
                service_id,
                data_path,
                local_fallback,
            )
            return local_fallback
        logger.warning(
            "⚠️ [storage-guard] Remote S3 DuckLake parquet scan is actively allowed via ALLOW_REMOTE_S3_PARQUET_SCAN=1 "
            "(service=%s, data_path=%s). Queries will incur S3 network latency.",
            service_id,
            data_path,
        )
    return data_path


def _acquire_attach_lock(service_id: object, reason: str) -> bool:
    """Acquire the process-wide ``_attach_lock`` with timeout logging."""
    if _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
        return True
    logger.warning("[ducklake] %s: %s", service_id, reason)
    return False


def _sleep_releasing_attach_lock(service_id: object, reason: str) -> bool:
    """Release ``_attach_lock``, sleep for the conflict backoff, and re-acquire."""
    _attach_lock.release()
    time.sleep(_ATTACH_CONFLICT_RETRY_SLEEP_S)
    return _acquire_attach_lock(
        service_id,
        f"timed out re-acquiring the attach lock while retrying after {reason}",
    )


def _detach_unlocked(con, aliases: tuple[str, ...] = ("lake",), *, ignore_errors: bool = True) -> None:
    """Execute ``DETACH <alias>`` for each alias when caller already holds ``_attach_lock``."""
    for alias in aliases:
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            if not ignore_errors:
                raise
            logger.debug("_ducklake: DETACH %s failed (ignored)", alias, exc_info=True)


def _probe_lake_usable(con) -> Exception | None:
    """Return None when ``ducklake_snapshots('lake')`` succeeds, else the exception."""
    try:
        con.execute("SELECT 1 FROM ducklake_snapshots('lake') LIMIT 1").fetchone()
        return None
    except Exception as exc:
        return exc


def _load_ducklake_extension(con, service_id: object) -> bool:
    """Configure extension directory and load the DuckLake extension."""
    bundled_extension = os.getenv("DUCKLAKE_EXTENSION_PATH")
    try:
        extension_directory = os.getenv("DUCKDB_EXTENSION_DIRECTORY")
        if extension_directory:
            os.makedirs(extension_directory, exist_ok=True)
            con.execute(f"SET extension_directory = '{escape_sql_literal(extension_directory)}';")
        if bundled_extension:
            con.execute(f"LOAD '{escape_sql_literal(bundled_extension)}';")
        else:
            try:
                con.execute("LOAD ducklake;")
            except Exception:
                con.execute("INSTALL ducklake; LOAD ducklake;")
        return True
    except Exception as e:
        source_description = "bundled" if bundled_extension else "installed"
        logger.warning("[ducklake] %s: failed to load %s DuckLake extension: %s", service_id, source_description, e)
        return False


def _ducklake_attach(con, source: dict, read_only: bool = False) -> bool:
    """Attach the DuckLake catalog for ``source`` as ``lake`` on ``con``.

    Returns True when the catalog is attached (including "already
    attached"), False on failure. On a read-write attach the size-cap
    option is (re)asserted — see module docstring.
    """
    service_id = source.get("service_id") or source.get("name", "default")
    dsn = _resolve_catalog_dsn(service_id)
    if not dsn:
        return False
    data_path = _resolve_data_path(source, service_id)

    if not _acquire_attach_lock(
        service_id,
        f"timed out after {_ATTACH_LOCK_TIMEOUT_S:.0f}s waiting for the process-wide attach lock — "
        "another connection's ATTACH is taking unusually long (e.g. a concurrent cron writer "
        "mid-ingest). Failing fast instead of hanging this caller's pool connection indefinitely.",
    ):
        return False

    lock_held = True
    try:
        if not _load_ducklake_extension(con, service_id):
            return False

        try:
            attached = con.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
        except Exception as e:
            logger.warning("[ducklake] %s: failed to inspect existing lake attachment: %s", service_id, e)
            return False
        if attached:
            existing_read_only = bool(attached[0])
            # Read-only callers reuse an existing read-write attach without detaching;
            # see module docstring and ADR-18 on avoiding unique file handle conflicts.
            if existing_read_only or not read_only:
                if existing_read_only == read_only:
                    probe_err = _probe_lake_usable(con)
                    if probe_err is None:
                        if not read_only:
                            _apply_target_file_size(con)
                        return True
                    try:
                        _detach_unlocked(con, ("lake",), ignore_errors=False)
                    except Exception as detach_err:
                        logger.warning(
                            "[ducklake] %s: existing lake catalog is unusable (%s) and detach failed: %s",
                            service_id,
                            probe_err,
                            detach_err,
                        )
                        return False
                else:
                    try:
                        _detach_unlocked(con, ("lake",), ignore_errors=False)
                    except Exception as detach_err:
                        logger.warning(
                            "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                            service_id,
                            detach_err,
                        )
                        return False
            else:
                probe_err = _probe_lake_usable(con)
                if probe_err is not None:
                    logger.warning(
                        "[ducklake] %s: existing read-write lake catalog is unusable for read: %s",
                        service_id,
                        probe_err,
                    )
                    return False
                return True

        try:
            orphaned_metadata = con.execute(
                "SELECT 1 FROM duckdb_databases() WHERE database_name = '__ducklake_metadata_lake'"
            ).fetchone()
        except Exception as e:
            logger.warning("[ducklake] %s: failed to inspect DuckLake metadata attachment: %s", service_id, e)
            return False
        if orphaned_metadata:
            try:
                _detach_unlocked(con, ("__ducklake_metadata_lake",), ignore_errors=False)
            except Exception as detach_err:
                logger.warning(
                    "[ducklake] %s: failed to detach orphaned DuckLake metadata catalog: %s",
                    service_id,
                    detach_err,
                )
                return False

        ro = ", READ_ONLY" if read_only else ""
        base_opts = f"DATA_PATH '{escape_sql_literal(data_path)}', OVERRIDE_DATA_PATH TRUE, DATA_INLINING_ROW_LIMIT 0"
        attach_sql = f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS lake ({base_opts}{ro});"
        for attempt in range(_ATTACH_CONFLICT_RETRY_ATTEMPTS):
            try:
                con.execute(attach_sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if read_only and attempt == 0 and ("does not exist" in msg or "explicitly disabled" in msg):
                    try:
                        con.execute(f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS __lake_init ({base_opts});")
                        _detach_unlocked(con, ("__lake_init",), ignore_errors=False)
                        continue
                    except Exception as init_err:
                        if "already attached" not in str(init_err) and "already exists" not in str(init_err):
                            logger.info(
                                "[ducklake] %s: could not pre-create catalog for read-only attach: %s",
                                service_id,
                                init_err,
                            )
                if "unique file handle conflict" in msg or "already attached by database" in msg:
                    _detach_unlocked(con, ("lake", "__ducklake_metadata_lake"))
                    if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                        lock_held = _sleep_releasing_attach_lock(service_id, "a file-handle conflict")
                        if not lock_held:
                            return False
                        continue
                    logger.warning(
                        "[ducklake] %s: failed to attach ducklake catalog after %d attempts over ~%.0fs "
                        "(zombie lock timeout): %s",
                        service_id,
                        _ATTACH_CONFLICT_RETRY_ATTEMPTS,
                        _ATTACH_CONFLICT_RETRY_ATTEMPTS * _ATTACH_CONFLICT_RETRY_SLEEP_S,
                        e,
                    )
                    return False
                if ("database with name" in msg and "already exists" in msg) or ("already attached" in msg):
                    try:
                        row = con.execute(
                            "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
                        ).fetchone()
                        if row and bool(row[0]) == read_only:
                            return True
                        if row:
                            try:
                                _detach_unlocked(con, ("lake",), ignore_errors=False)
                                if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                                    lock_held = _sleep_releasing_attach_lock(service_id, "a mode mismatch")
                                    if not lock_held:
                                        return False
                                    continue
                            except Exception as detach_err:
                                logger.warning(
                                    "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                                    service_id,
                                    detach_err,
                                )
                                return False
                    except Exception:
                        logger.debug("_ducklake_attach: step failed (ignored)", exc_info=True)
                    return False
                logger.warning("[ducklake] %s: failed to attach ducklake catalog: %s", service_id, e)
                return False
        else:
            logger.warning("[ducklake] %s: failed to attach ducklake catalog (retries exhausted)", service_id)
            return False

        if not read_only:
            _apply_target_file_size(con)
        return True
    finally:
        if lock_held:
            _attach_lock.release()


def _ducklake_detach(con, aliases: tuple[str, ...] = ("lake",), service_id: str = "default") -> None:
    """Detach ``aliases`` from ``con`` under the same process-wide lock ``_ducklake_attach`` uses."""
    if not _acquire_attach_lock(
        service_id,
        f"timed out after {_ATTACH_LOCK_TIMEOUT_S:.0f}s waiting for the process-wide attach lock to detach {aliases}",
    ):
        return
    try:
        _detach_unlocked(con, aliases)
    finally:
        _attach_lock.release()


def _apply_target_file_size(con, alias: str = "lake") -> None:
    """Pin DuckLake's target file size to the compaction cap (idempotent).

    ``ducklake_merge_adjacent_files`` and table rewrites honor this option,
    so setting it at attach time is what keeps DuckLake-side compaction
    size-capped like the local tiered compaction. Checked before set so a
    steady-state attach doesn't write the catalog every time.
    """
    require_safe_ident(alias, "invalid ducklake alias")
    target = f"{_DUCKLAKE_TARGET_FILE_SIZE_MB}MiB"
    target_bytes = str(_DUCKLAKE_TARGET_FILE_SIZE_MB * 1024 * 1024)
    try:
        row = con.execute(f"SELECT value FROM {alias}.options() WHERE option_name = 'target_file_size'").fetchone()
        if row and str(row[0]) == target_bytes:
            return
        con.execute(f"CALL {alias}.set_option('target_file_size', '{target}')")
    except Exception as e:
        logger.warning("[ducklake] failed to apply target_file_size=%s: %s", target, e)


def ducklake_current_snapshot_id(con, alias: str = "lake") -> int | None:
    """Latest snapshot id of the attached DuckLake catalog, or None.

    Used as the fast-path staleness token for the per-service ``logs``
    view: a commit (from any process sharing the catalog) bumps the
    snapshot id, which invalidates the cached view SQL. Catalog-wide by
    design — under a shared catalog this is conservative (another
    tenant's commit forces a rebuild) but never stale.
    """
    require_safe_ident(alias, "invalid ducklake alias")
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{alias}') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.info("[ducklake] snapshot-id probe failed (lake not attached?): %s", e)
        return None


# ── Bounded per-service admission for DuckLake writers ───────────────────────

DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S = float(os.getenv("DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S", "30"))


class DuckLakeAdmissionTimeout(TimeoutError):
    """A DuckLake writer could not acquire the service admission in time."""


_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()
_stats: dict[str, dict[str, float]] = {}
_stats_guard = threading.Lock()


def _local_lock(service_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(service_id, threading.RLock())


def _advisory_key(service_id: str) -> int:
    # PostgreSQL advisory locks take signed BIGINT values.
    value = int.from_bytes(hashlib.sha256(service_id.encode()).digest()[:8], "big", signed=False)
    return value - (1 << 64) if value >= (1 << 63) else value


def _record_stat(service_id: str, key: str, value: float = 1.0) -> None:
    with _stats_guard:
        bucket = _stats.setdefault(
            service_id,
            {"acquisitions": 0.0, "timeouts": 0.0, "wait_ms_total": 0.0, "hold_ms_total": 0.0},
        )
        bucket[key] += value


def get_admission_stats() -> dict[str, dict[str, float]]:
    """Return cumulative writer-admission counters for diagnostics."""
    with _stats_guard:
        return {service_id: dict(values) for service_id, values in _stats.items()}


@contextmanager
def ducklake_write_admission(
    service_id: str,
    *,
    timeout_s: float = DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S,
) -> Iterator[None]:
    """Serialize one service's DuckLake writer across threads and processes.

    Besides the process-local reentrant lock, the same service also takes a
    PostgreSQL advisory lock (metadata is Postgres-only, ADR-22), fencing writers
    in separate worker processes/pods.
    """
    lock = _local_lock(service_id)
    wait_started = time.monotonic()
    if not lock.acquire(timeout=max(0.0, timeout_s)):
        _record_stat(service_id, "timeouts")
        raise DuckLakeAdmissionTimeout(f"DuckLake writer admission timed out for {service_id}")
    _record_stat(service_id, "wait_ms_total", (time.monotonic() - wait_started) * 1000.0)

    pg_con = None
    advisory = False
    deadline = time.monotonic() + max(0.0, timeout_s)
    key = _advisory_key(service_id)
    hold_started = time.monotonic()
    try:
        import psycopg

        from backend.core.metadata.base import get_pg_thread_connection

        pg_con = get_pg_thread_connection()
        if pg_con is None:
            raise RuntimeError("pg_con is unexpectedly None")
        # Fair FIFO acquisition: block in PostgreSQL's lock-wait queue
        # (pg_advisory_lock) bounded by lock_timeout, instead of busy-polling
        # the non-blocking pg_try_advisory_lock. Polling has no queue, so under
        # write saturation whichever writer keeps losing the 0.1s race starves
        # and discards its whole already-parsed batch — a measured congestion
        # collapse (RUM 229 lock timeouts vs requests' 58, 792 files wedged).
        # lock_timeout is a session GUC; reset it afterward since these
        # connections are long-lived and reused across services.
        remaining_ms = int(max(1.0, (deadline - time.monotonic()) * 1000.0))
        pg_con.execute(f"SET lock_timeout = {remaining_ms}")
        try:
            pg_con.execute("SELECT pg_advisory_lock(?)", (key,))
            advisory = True
        except psycopg.errors.LockNotAvailable as exc:
            _record_stat(service_id, "timeouts")
            raise DuckLakeAdmissionTimeout(f"DuckLake advisory lock timed out for {service_id}") from exc
        finally:
            try:
                pg_con.execute("SET lock_timeout = 0")
            except Exception:
                logger.exception("Failed to reset lock_timeout for %s", service_id)
        yield
    finally:
        if advisory and pg_con is not None:
            try:
                pg_con.execute("SELECT pg_advisory_unlock(?)", (key,))
            except Exception:
                logger.exception("Failed to release DuckLake advisory lock for %s", service_id)
        lock.release()
        _record_stat(service_id, "acquisitions")
        _record_stat(service_id, "hold_ms_total", (time.monotonic() - hold_started) * 1000.0)


def _reset_for_tests() -> None:
    with _locks_guard:
        _locks.clear()
    with _stats_guard:
        _stats.clear()


sys.modules["backend.core.iceberg._ducklake"] = sys.modules[__name__]

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

# ── DuckLake-native table-info / calendar + schema alignment (folded from manifest.py) ──
#
# Every commit goes through `backend.core.iceberg.buffer._commit_buffer_impl`'s
# DuckLake attach (see `backend/core/iceberg/_ducklake.py` module docstring),
# and `get_table_info`/`get_snapshot_calendar` below read DuckLake state
# directly.

_core_mod = sys.modules[__name__]

_ducklake_table_metadata_cache: dict[tuple[str, str], tuple[int | None, tuple[int, str | None, str | None, dict]]] = {}
_ducklake_table_metadata_cache_lock = threading.Lock()
_ducklake_session_local = threading.local()

from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("iceberg._ducklake_table_metadata_cache", _ducklake_table_metadata_cache)


@contextmanager
def ducklake_session(source: dict):
    """Share a single read-only DuckDB connection and DuckLake attachment across
    multiple ``ducklake_table_exists`` / ``get_table_info`` / ``get_snapshot_calendar``
    calls on the current thread. Connection creation and catalog attachment are
    deferred until the first unmocked helper actually needs a connection.
    """
    prev = getattr(_ducklake_session_local, "current", None)
    state: dict = {
        "source": source,
        "con": None,
        "attached": False,
        "existing_tables": set(),
    }
    _ducklake_session_local.current = state
    try:
        yield state
    finally:
        _ducklake_session_local.current = prev
        con = state.get("con")
        if con is not None:
            try:
                con.close()
            except Exception:
                logger.debug("ducklake_session: con.close failed (ignored)", exc_info=True)


def _acquire_ducklake_con(source: dict, con=None) -> tuple[Any, bool, bool, set[str] | None]:
    """Return ``(con, attached, owns_con, existing_tables)`` for *source*.

    Reuses an explicit *con* or an active :func:`ducklake_session` when present;
    otherwise opens a fresh read-only connection that the caller must close.
    """
    from backend.core.duckdb import get_connection

    if con is not None:
        attached = bool(_ducklake_attach(con, source, read_only=True))
        return con, attached, False, None

    session = getattr(_ducklake_session_local, "current", None)
    if session is not None and session.get("source") is source:
        s_con = session.get("con")
        if s_con is None:
            s_con = get_connection(source, read_only=True, skip_view_update=True)
            session["con"] = s_con
            session["attached"] = bool(_ducklake_attach(s_con, source, read_only=True))
        return s_con, bool(session.get("attached")), False, session.get("existing_tables")

    fresh_con = get_connection(source, read_only=True, skip_view_update=True)
    try:
        attached = bool(_ducklake_attach(fresh_con, source, read_only=True))
    except Exception:
        try:
            fresh_con.close()
        except Exception:
            logger.debug("_acquire_ducklake_con: fresh_con.close failed (ignored)", exc_info=True)
        raise
    return fresh_con, attached, True, None


def _lake_ident(lake_table: str) -> str:
    return 'lake."{}"'.format(lake_table.replace('"', '""'))


def _scan_ducklake_table_metadata(con, lake_table: str) -> tuple[int, str | None, str | None, dict]:
    """Full scan of ``lake.<lake_table>`` for row count, min/max timestamp,
    and a per-day row-count calendar.

    DuckLake exposes no manifest/partition-value API at the SQL level the
    way pyiceberg's manifests did, and small commits get "inlined" into the
    catalog metadata rather than written as parquet files — so there are no
    reliable per-file stats to read either way (see
    ``backend/core/iceberg/_ducklake.py`` module docstring). A direct scan
    is the only approach that stays correct in both cases. Callers should
    go through ``_get_cached_or_scan_ducklake_metadata`` rather than call
    this directly — it's O(table size) and this codebase treats "MANY
    small files, cheap incremental reads" as the standing perf contract.
    """
    ident = _lake_ident(lake_table)
    rows = con.execute(
        f"SELECT date_trunc('day', timestamp)::DATE AS d, count(*) AS n, "
        f"min(timestamp) AS mn, max(timestamp) AS mx "
        f"FROM {ident} WHERE timestamp IS NOT NULL GROUP BY 1 ORDER BY 1"
    ).fetchall()
    calendar: dict[str, dict] = {}
    total = 0
    min_ts: datetime | None = None
    max_ts: datetime | None = None
    for d, n, mn, mx in rows:
        n = int(n)
        calendar[d.isoformat()] = {"data_files": n, "size_bytes": 0}
        total += n
        if mn is not None and (min_ts is None or mn < min_ts):
            min_ts = mn
        if mx is not None and (max_ts is None or mx > max_ts):
            max_ts = mx
    return (
        total,
        min_ts.isoformat() if min_ts else None,
        max_ts.isoformat() if max_ts else None,
        calendar,
    )


def _get_cached_or_scan_ducklake_metadata(
    con, source: dict, lake_table: str
) -> tuple[int, str | None, str | None, dict]:
    """Cached wrapper around :func:`_scan_ducklake_table_metadata`.

    Cached by the DuckLake catalog's current snapshot id (catalog-wide,
    same conservative-but-never-stale token ``ducklake_current_snapshot_id``
    documents) so a status poller calling ``get_table_info`` on a ~60s
    cadence (see ``backend/core/_duckdb_status.py::refresh_config_status``)
    only re-scans after a NEW commit, not on every tick.
    """

    source_key = source.get("service_id") or source.get("name") or "default"
    cache_key = (str(source_key), lake_table)
    snap_id = ducklake_current_snapshot_id(con)

    with _ducklake_table_metadata_cache_lock:
        cached = _ducklake_table_metadata_cache.get(cache_key)
    if cached is not None and cached[0] == snap_id:
        return cached[1]

    result = _scan_ducklake_table_metadata(con, lake_table)
    with _ducklake_table_metadata_cache_lock:
        _ducklake_table_metadata_cache[cache_key] = (snap_id, result)
    return result


def ducklake_table_exists(source: dict, table_name: str = "logs", *, con=None) -> bool:
    """Whether ``table_name``'s DuckLake table has been created for ``source``."""

    active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
    try:
        if not attached:
            raise RuntimeError("failed to attach DuckLake catalog")
        lake_table = ducklake_table_name(source, table_name)
        row = active_con.execute(
            "SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]
        ).fetchone()
        exists = row is not None
        if exists and existing_tables is not None:
            existing_tables.add(lake_table)
        return exists
    finally:
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("ducklake_table_exists: con.close failed (ignored)", exc_info=True)


def get_table_info(source: dict, table=None, table_name: str = "logs", *, con=None) -> dict:
    """Return snapshot count, data file count, total size, and latest snapshot time."""
    from backend import config as svcconfig

    zeroed = {
        "table_name": table_name,
        "snapshots": 0,
        "data_files": 0,
        "size_bytes": 0,
        "latest_snapshot_at": None,
        "buffer_files": 0,
        "buffer_size_bytes": 0,
        "table_location": None,
        "region": source.get("region"),
        "min_timestamp": None,
        "max_timestamp": None,
    }

    active_con = None
    owns_con = False
    try:
        active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
        if not attached:
            raise RuntimeError("failed to attach DuckLake catalog")

        lake_table = ducklake_table_name(source, table_name)
        info_row = active_con.execute(
            "SELECT table_id, file_count, file_size_bytes FROM ducklake_table_info('lake') WHERE table_name = ?",
            [lake_table],
        ).fetchone()
        if info_row is None:
            return {**zeroed, "error": f"DuckLake table not found: {lake_table}"}
        if existing_tables is not None:
            existing_tables.add(lake_table)

        table_id, file_count, file_size_bytes = info_row
        file_count = int(file_count or 0)
        file_size_bytes = int(file_size_bytes or 0)

        snap_row = active_con.execute(
            "SELECT count(*), max(snapshot_time) FROM ducklake_snapshots('lake') "
            "WHERE list_contains(flatten(map_values(changes)), ?)",
            [str(table_id)],
        ).fetchone()
        snapshot_count = int(snap_row[0] or 0) if snap_row else 0
        latest_ts = snap_row[1].isoformat() if snap_row and snap_row[1] is not None else None

        row_count, min_ts, max_ts, _calendar = _get_cached_or_scan_ducklake_metadata(active_con, source, lake_table)

        data_files = file_count if file_count > 0 else row_count

        buf = _core_mod.buffer_files(source, table_name=table_name)
        buf_size = sum(os.path.getsize(p) for p in buf if os.path.exists(p))

        data_path = svcconfig.DUCKLAKE_DATA_PATH or _default_data_path(source)

        return {
            "table_name": table_name,
            "snapshots": snapshot_count,
            "data_files": data_files,
            "size_bytes": file_size_bytes,
            "latest_snapshot_at": latest_ts,
            "buffer_files": len(buf),
            "buffer_size_bytes": buf_size,
            "table_location": data_path,
            "region": source.get("region"),
            "min_timestamp": min_ts,
            "max_timestamp": max_ts,
        }
    except Exception as e:
        return {**zeroed, "error": str(e)}
    finally:
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("get_table_info: con.close failed (ignored)", exc_info=True)


def get_snapshot_calendar(source: dict, table=None, table_name: str = "logs", *, con=None) -> dict:
    """Return per-date row counts scanned directly from the DuckLake table."""

    active_con = None
    owns_con = False
    try:
        active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
        if not attached:
            return {}

        lake_table = ducklake_table_name(source, table_name)
        if existing_tables is None or lake_table not in existing_tables:
            exists = active_con.execute(
                "SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]
            ).fetchone()
            if exists is None:
                return {}
            if existing_tables is not None:
                existing_tables.add(lake_table)

        _row_count, _min_ts, _max_ts, calendar = _get_cached_or_scan_ducklake_metadata(active_con, source, lake_table)
        return calendar
    except Exception:
        return {}
    finally:
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("get_snapshot_calendar: con.close failed (ignored)", exc_info=True)


def _align_to_schema(table: pa.Table, target_schema: pa.Schema | None = None, source: dict | None = None) -> pa.Table:
    """Align a PyArrow table to a target schema (or dynamically generated if none provided)."""
    if target_schema is not None:
        schema = target_schema
    else:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) if source else None
        log_fields_config = cfg.get("log_fields", {}) if cfg else None
        schema = _core_mod.get_arrow_schema(log_fields_config)

    dynamic_schema_field_names = {f.name for f in schema}
    existing = {f.name: table.schema.field(f.name) for f in table.schema if f.name in dynamic_schema_field_names}
    arrays = {}
    for field in schema:
        name = field.name
        if name in existing:
            col = table.column(name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type, safe=False)
                except Exception:
                    try:
                        col = col.cast(field.type, safe=True)
                    except Exception:
                        col = pa.nulls(len(table), type=field.type)
            arrays[name] = col
        else:
            arrays[name] = pa.nulls(len(table), type=field.type)
    return pa.table(arrays, schema=schema)


def _arrow_to_duckdb(arrow_type: pa.DataType) -> str:
    """Map a PyArrow type to a DuckDB type string for the empty-view fallback."""
    mapping = {
        pa.string(): "VARCHAR",
        pa.bool_(): "BOOLEAN",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
    }
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ"
    return mapping.get(arrow_type, "VARCHAR")


def _prune_empty_dirs(root: str) -> None:
    """Remove empty subdirectories under root (bottom-up)."""
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        if dirpath == root:
            continue
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
            except Exception:
                logger.debug("_prune_empty_dirs: os.rmdir failed (ignored)", exc_info=True)


# ── sync_data (DuckLake no-op shim) + lake_info helpers ──
def sync_data(
    source: dict,
    progress_callback: Any = None,
    start_time: str | None = None,
    end_time: str | None = None,
    table_name: str = "logs",
    **kwargs: Any,
) -> dict[str, int]:
    """No-op under DuckLake (files are written locally on commit)."""
    return {"files_downloaded": 0, "rows_downloaded": 0}


_CDN_URL_ALLOWED_HOST_SUFFIXES = (
    ".fastly.net",
    ".fastlystorage.app",
)


def _safe_cdn_url(cdn_url: str) -> str | None:
    """Return ``cdn_url`` only if it's an https:// URL on an allowlisted
    Fastly hostname, else None.
    """
    if not cdn_url:
        return None
    try:
        parsed = urllib.parse.urlsplit(cdn_url)
    except ValueError:
        return None
    if parsed.scheme != "https":
        return None
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return None
    for suffix in _CDN_URL_ALLOWED_HOST_SUFFIXES:
        if hostname.endswith(suffix):
            return cdn_url
    return None


def fetch_lake_info(source: dict, use_temp_cache: bool = False) -> dict:
    """Return DuckLake table range and calendar for *source*."""
    from backend.core import iceberg as db_iceberg

    if use_temp_cache:
        return _fetch_with_temp_cache(source, db_iceberg)
    return _fetch_direct(source, db_iceberg)


def _fetch_direct(source: dict, db_iceberg) -> dict:
    """DuckLake-native fallback (v3 write-path cutover)."""
    try:
        with db_iceberg.ducklake_session(source):
            if not db_iceberg.ducklake_table_exists(source):
                return {"ok": True, "table_exists": False, "message": "DuckLake table not found."}

            info = db_iceberg.get_table_info(source)
            if "error" in info:
                return {"ok": False, "error": info["error"]}
            calendar = db_iceberg.get_snapshot_calendar(source)

            return {
                "ok": True,
                "table_exists": True,
                "info": info,
                "calendar": calendar,
                "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _fetch_with_temp_cache(source: dict, db_iceberg) -> dict:
    """Same DuckLake-native fallback as :func:`_fetch_direct`, but scoped to
    a throwaway ``_cache_dir_override`` and cleared via
    ``clear_source_caches`` afterward."""
    import tempfile

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            src = {**source, "_cache_dir_override": tmp_dir}
            if "iceberg_metadata_location" in source:
                src["iceberg_metadata_location"] = source["iceberg_metadata_location"]
            try:
                return _fetch_direct(src, db_iceberg)
            finally:
                db_iceberg.clear_source_caches(src["name"])
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── View binding + stale-view self-heal (folded from view.py) ──

from backend.core.retry import retry_call


def configure_duckdb_s3(con) -> None:
    """Install/load DuckDB extensions for httpfs + parquet.

    The fos_proxy SECRET (created in backend.core.duckdb._configure_fos) is
    the sole S3 routing config; this function used to also `SET s3_endpoint`
    etc., but those settings would clobber the proxy's endpoint scoping for
    unmatched URLs and silently bypass telemetry.
    """
    try:
        con.execute("LOAD httpfs; LOAD parquet;")
    except Exception:
        try:
            con.execute("INSTALL httpfs; INSTALL parquet;")
            con.execute("LOAD httpfs; LOAD parquet;")
        except Exception:
            logger.exception("configure_duckdb_s3: failed to INSTALL/LOAD DuckDB extensions")
            raise


# Per-service locks to avoid global bottleneck during S3 manifest scans
_service_locks: dict[str, threading.RLock] = {}
_service_locks_lock = threading.Lock()


def _get_service_lock(source_key: str) -> threading.RLock:
    base_key = source_key[:-5] if source_key.endswith("::rum") else source_key
    with _service_locks_lock:
        if base_key not in _service_locks:
            _service_locks[base_key] = threading.RLock()
        return _service_locks[base_key]


# Per-source view cache: source_key -> (metadata_loc, buf_set, schema_fields_tuple, view_sql, time_ms, was_fast_path, source_variant_fp)
#
# ``source_variant_fp`` (added 2026-06-15, finding 002) is a short tuple
# capturing the per-source-dict inputs that materially affect the
# generated view SQL beyond the schema fields: ``access_level`` and
# ``time_range``. Both come from the per-service config today, but the
# cron's manual-import path mutates ``time_range`` temporarily on the
# in-memory source dict during analyst-driven backfills — without the
# fingerprint, a concurrent dashboard call could read back the cached
# SQL from a moment when the WHERE clause had a different time bound.
# Cheap defense-in-depth: cache miss on any drift, cache hit when the
# variant is stable. See ``_source_variant_fp`` below.
_view_cache: dict[str, tuple] = {}


def _source_variant_fp(source: dict) -> tuple:
    """Hashable fingerprint of the per-source inputs that materially
    affect the generated view SQL beyond the schema fields.

    Finding 002 (2026-06-15): ``_update_iceberg_view_locked`` appends a
    WHERE clause derived from ``source["time_range"]`` and the analyst
    flag derived from ``source["access_level"]``. Without these in the
    ``_view_cache`` key tuple, a cached SQL string from one variant
    could be served back to a request that wrote a different time
    range — silently applying the wrong WHERE clause to the result
    set. Cheap defense in depth: include both in the key.

    ``provisioning.cron_sync.enabled`` is also folded in because the
    same WHERE-clause branch fires when cron sync is disabled.
    """
    tr = source.get("time_range")
    if isinstance(tr, dict):
        tr_fp: tuple = (tr.get("start"), tr.get("end"))
    else:
        tr_fp = ()
    access = source.get("access_level") or ""
    cron_enabled = bool(((source.get("provisioning") or {}).get("cron_sync") or {}).get("enabled", True))
    return (access, tr_fp, cron_enabled)


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
        or "No such file or directory" in msg
        or "Cannot open file" in msg
    )


def execute_with_stale_view_retry(con, source: dict, fn, *args, table_name="logs", **kwargs):
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

    def _recover(_exc: BaseException) -> None:
        _core_mod.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        _core_mod.update_iceberg_view(con, source, force=True, target_table=table_name)

    return retry_call(
        lambda: fn(con, *args, **kwargs),
        retry_on=is_stale_view_error,
        attempts=2,
        before_retry=_recover,
    )


def clear_source_caches(source_key: str, *, keep_snapshot_cache: bool = False) -> None:
    """Remove in-memory cache entries for a service."""
    base_key = source_key[:-5] if source_key.endswith("::rum") else source_key
    # Pop the primary and any sub-table view cache keys (e.g. client_vitals, client_errors, ::rum)
    for k in list(_view_cache.keys()):
        if k == base_key or k.startswith(f"{base_key}::"):
            _view_cache.pop(k, None)

    if not keep_snapshot_cache:
        try:
            from backend.core._duckdb_status import clear_ducklake_stats_cache

            clear_ducklake_stats_cache(base_key)
        except Exception:
            logger.debug("clear_source_caches: clear_ducklake_stats_cache failed (ignored)", exc_info=True)


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
                "engine": "DuckDB",
            },
        )


def _view_target_name(source: dict, target_table: str) -> str:
    """The DuckDB view name both paths bind — repositories query this name
    via ``_safe_table(src["name"])``, so fast path and slow path MUST agree
    (the fast-path reconstruction previously bound the literal ``logs``)."""
    from backend.core.duckdb import _safe_table_name

    if target_table != "logs":
        return target_table
    return _safe_table_name(source.get("name") or source.get("service_id") or "default")


def _ducklake_view_token(con) -> str | None:
    """Staleness token for the cached view SQL, derived from the DuckLake
    catalog's current snapshot id. A commit anywhere in the catalog bumps
    it, forcing a rebuild; in the steady state (no commit since the last
    build) the token matches and reads stay on the lock-free fast path.
    Returns None when the token can't be derived (lake not attached) —
    callers must treat None as "cannot verify freshness" and rebuild.
    """

    snap = ducklake_current_snapshot_id(con)
    return None if snap is None else f"ducklake:{snap}"


def _empty_schema_select(dynamic_arrow_schema) -> str:
    """Zero-row SELECT carrying every schema column (typed NULLs) so a
    service with no lake table and no buffer still binds a queryable view."""
    cols = ", ".join(
        'NULL::{} AS "{}"'.format(_core_mod._arrow_to_duckdb(f.type), f.name.replace('"', '""'))
        for f in dynamic_arrow_schema
    )
    return f"SELECT {cols} WHERE false"


def _existing_union_cols(con, committed_parts: list[str], buf_probe_path: str | None) -> set[str]:
    """Column names present in the raw (lake ∪ buffer) union output.

    Probes each committed part with ``LIMIT 0`` (catalog-only, no data
    read) plus a SINGLE buffer parquet (all buffer files are aligned to
    the same schema by ``write_to_buffer``, so one probe suffices — the
    same trick the pre-v3 fast path used to avoid schema-binding every
    buffer file).
    """
    cols: set[str] = set()
    for part in committed_parts:
        try:
            desc = con.execute(f"SELECT * FROM ({part}) LIMIT 0").description or []
            cols.update(d[0] for d in desc)
        except Exception as e:
            logger.info("[iceberg] view column probe failed for committed part: %s", e)
    if buf_probe_path:
        try:
            desc = (
                con.execute(
                    f"SELECT * FROM read_parquet('{escape_sql_literal(buf_probe_path)}', hive_partitioning=false) LIMIT 0"
                ).description
                or []
            )
            cols.update(d[0] for d in desc)
        except Exception as e:
            logger.info("[iceberg] view column probe failed for buffer parquet %s: %s", buf_probe_path, e)
    return cols


def _finalize_view_sql(
    union_sql: str,
    source: dict,
    target_table: str,
    dynamic_schema_field_names: set[str],
    existing_cols: set[str],
) -> str:
    """Shared projection + clamp applied to the raw (lake ∪ buffer) union.

    Single source of truth for BOTH the lock-free fast-path reconstruction
    and the locked slow-path rebuild, so the two can never drift. Restores
    everything the pre-DuckLake view guaranteed downstream:

    - ``timestamp_hour`` / ``dt`` computed columns (partition-pruning
      WHEREs in alerts.py and the rollup readers depend on them);
    - the ``c_speed`` / ``p_type`` / ``p_desc`` decode CASEs and the
      ``ttl`` / ``age`` integer rounding (logs table only);
    - the ``time_range`` WHERE clamp for ``read_only`` (analyst) sources
      and cron-disabled manual imports — a DATA-SCOPING boundary, not an
      optimization: an analyst share must never expose rows outside the
      granted window.
    """
    sql = union_sql

    # 1. Computed partition columns. Strip any real column of the same
    #    name first (legacy hive-partition reads may carry one) so the
    #    computed definition wins.
    cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in existing_cols)
    exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
    sql = (
        f"SELECT *{exclude_clause}, "
        f"CAST(strftime(timestamp, '%Y-%m-%d-%H') AS VARCHAR) as timestamp_hour, "
        f"CAST(strftime(timestamp, '%Y-%m-%d') AS VARCHAR) as dt "
        f"FROM ({sql})"
    )

    # 2. Storage-code decodes + rounding (logs only).
    if target_table == "logs":
        from backend.core import log_fields as fc

        exclude_cols: list[str] = []
        select_extras: list[str] = []
        for col, encode_map in (
            ("c_speed", fc.CONN_SPEED_ENCODE),
            ("p_type", fc.PROXY_TYPE_ENCODE),
            ("p_desc", fc.PROXY_DESC_ENCODE),
        ):
            if col in existing_cols:
                exclude_cols.append(col)
                select_extras.append(f"{fc.duckdb_decode_case(col, encode_map)} AS {col}")
        if "ttl" in existing_cols and "ttl" in dynamic_schema_field_names:
            exclude_cols.append("ttl")
            select_extras.append('CAST(ROUND("ttl") AS INTEGER) AS ttl')
        if "age" in existing_cols and "age" in dynamic_schema_field_names:
            exclude_cols.append("age")
            select_extras.append('CAST(ROUND("age") AS INTEGER) AS age')
        if exclude_cols:
            sql = f"SELECT * EXCLUDE ({', '.join(exclude_cols)}), {', '.join(select_extras)} FROM ({sql})"

    # 3. Analyst / manual-import time clamp.
    tr = source.get("time_range")
    is_analyst = source.get("access_level") == "read_only"
    if tr and (is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)):
        import dateutil.parser as _dt

        where_clauses = []
        if tr.get("start"):
            try:
                start_iso = _dt.isoparse(str(tr["start"])).isoformat()
            except (ValueError, TypeError) as e:
                raise ValueError(f"invalid time_range start: {e}") from e
            where_clauses.append(f"timestamp >= '{escape_sql_literal(start_iso)}'::TIMESTAMPTZ")
        if tr.get("end"):
            try:
                end_iso = _dt.isoparse(str(tr["end"])).isoformat()
            except (ValueError, TypeError) as e:
                raise ValueError(f"invalid time_range end: {e}") from e
            where_clauses.append(f"timestamp <= '{escape_sql_literal(end_iso)}'::TIMESTAMPTZ")
        if where_clauses:
            sql = f"SELECT * FROM ({sql}) WHERE {' AND '.join(where_clauses)}"

    return sql


def _try_fast_path_view(con, source: dict, target_table: str = "logs") -> bool:
    """Bind the per-service view from cache without acquiring the lock.

    Returns True if the view was bound; False if a slow-path rebuild is
    needed. Safe to call concurrently — all reads are race-free against
    a concurrent slow-path writer (cached tuple refs are stable; the
    only write here is a benign timestamp update on _view_cache).

    This split exists so 6 parallel dashboard requests for the same
    source don't serialize on the per-service RLock that ingest also
    holds during buffer commits.
    """
    t_start = time.time()
    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key

    from backend.core.duckdb_pool import _get_conn_state, _set_conn_state

    fp_key = f"fast_path_view_fp_{target_table}"
    if _get_conn_state(con, fp_key) is None:
        configure_duckdb_s3(con)

    from backend import config as svcconfig

    durable_serving = svcconfig.is_durable_serving_mode(source)
    if durable_serving:
        buf_files = []
    else:
        try:
            buf_files = _core_mod.buffer_files(source, table_name=target_table)
        except TypeError:
            buf_files = _core_mod.buffer_files(source)
    buf_set = frozenset(buf_files)

    # DuckLake-aware staleness token (was: the pyiceberg metadata_location
    # from iceberg_catalog.db, which never matched the slow path's cached
    # literal post-DuckLake — every read paid the RLock + full rebuild,
    # the 12s+ page-load class regression this fast path exists to avoid).
    metadata_loc = _ducklake_view_token(con)
    if metadata_loc is None:
        return False

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    cached = _view_cache.get(cache_key)

    # We rely on the background ingest/commit cron jobs to naturally migrate views from
    # S3 to local reads when files are downloaded. Removing the synchronous glob/check
    # here prevents parallel reader threads from getting serialized and waiting on the RLock
    # (which multiplied page load times up to 12s+ on dashboard reloads). It also avoids
    # running expensive recursive filesystem glob scans on every read-side query checkout.

    schema_fp = tuple(sorted(dynamic_schema_field_names))
    variant_fp = _source_variant_fp(source)

    only_buf_changed = False
    if (
        cached
        and len(cached) > 7
        and cached[0] == metadata_loc
        and cached[1] != buf_set
        and cached[2] == schema_fp
        and (len(cached) > 6 and cached[6] == variant_fp)
    ):
        only_buf_changed = True

    if not only_buf_changed and not (
        cached
        and cached[0] == metadata_loc
        and cached[1] == buf_set
        and cached[2] == schema_fp
        and (len(cached) > 6 and cached[6] == variant_fp)
    ):
        return False

    if only_buf_changed:
        if cached is None:
            raise RuntimeError("cached is unexpectedly None")
        try:
            # Reconstruct the view SQL with the new local buffer files
            # instantly — no lock, no catalog walk. cached[7] holds the RAW
            # committed parts (the lake SELECT) written by the slow path;
            # append a fresh raw buffer part and run the SAME finalize
            # helper the slow path uses.
            committed_sql_parts = cached[7]
            new_parts = list(committed_sql_parts) if committed_sql_parts else []

            # Recheck buffer files existence
            active_buf_files = [p for p in buf_files if os.path.isfile(p)]
            if active_buf_files:
                paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in active_buf_files)
                new_parts.append(
                    f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
                )

            if not new_parts:
                union_sql = _empty_schema_select(dynamic_arrow_schema)
                existing_cols = set(dynamic_schema_field_names)
            else:
                union_sql = " UNION ALL BY NAME ".join(new_parts)
                if len(cached) > 8 and cached[8] and (cached[1] or cached[7]):
                    existing_cols = set(cached[8])
                else:
                    existing_cols = _existing_union_cols(
                        con,
                        list(committed_sql_parts) if committed_sql_parts else [],
                        active_buf_files[0] if active_buf_files else None,
                    )

            final_sql = _finalize_view_sql(union_sql, source, target_table, dynamic_schema_field_names, existing_cols)

            # Format the CREATE OR REPLACE VIEW statement
            create_stmt = f"CREATE OR REPLACE VIEW {_view_target_name(source, target_table)} AS {final_sql}"

            # Update cache so the view_sql below matches our reconstructed version!
            _view_cache[cache_key] = (
                metadata_loc,
                buf_set,
                schema_fp,
                create_stmt,
                round((time.time() - t_start) * 1000, 2),
                True,  # was_fast_path = True
                variant_fp,
                committed_sql_parts,
                frozenset(existing_cols),
            )
            cached = _view_cache[cache_key]
        except Exception as e:
            logger.warning("[iceberg] Incremental fast-path reconstruction failed: %s", e)
            return False

    if cached is None:
        raise RuntimeError("cached is unexpectedly None")
    view_sql = cached[3]
    committed_sql_parts = cached[7] if len(cached) > 7 else ()
    cached_existing_cols = cached[8] if len(cached) > 8 else frozenset()
    if view_sql:
        # Per-connection cache: if THIS connection has already bound the
        # same (metadata_loc, buf_set, schema_fp) on the fast path, skip
        # the CREATE OR REPLACE TEMP VIEW execute entirely. The view
        # SQL is deterministic from the fingerprint tuple, so re-running
        # it produces the same DuckDB state — at ~70 ms of catalog work
        # per call that adds up across every read-side request.
        bound_fp = _get_conn_state(con, fp_key)
        target_fp = (metadata_loc, buf_set, schema_fp, variant_fp, target_table)
        if bound_fp != target_fp:
            # TEMP views avoid shared-catalog write conflicts between pooled readers.
            exec_sql = view_sql
            if view_sql.startswith("CREATE OR REPLACE VIEW "):
                exec_sql = view_sql.replace("CREATE OR REPLACE VIEW ", "CREATE OR REPLACE TEMP VIEW ", 1)
            try:
                con.execute(exec_sql)
            except Exception as e:
                logger.warning("[iceberg] fast-path view re-bind failed for %s: %s", source_key, e)
                return False
            _set_conn_state(con, **{fp_key: target_fp})

    if not cached[5]:
        t_end = time.time()
        _view_cache[cache_key] = (
            metadata_loc,
            buf_set,
            tuple(sorted(dynamic_schema_field_names)),
            view_sql,
            round((t_end - t_start) * 1000, 2),
            True,
            variant_fp,
            committed_sql_parts,
            cached_existing_cols,
        )
    return True


def _rebuild_locked(con, source: dict, source_key: str, target_table: str = "logs", force: bool = False) -> None:
    """Run the slow path under the lock and signal completion."""
    ev = threading.Event()
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key
    with _rebuild_signals_lock:
        _rebuild_signals[cache_key] = ev
    try:
        try:
            _core_mod._update_iceberg_view_locked(con, source, target_table=target_table, force=force)
        except TypeError:
            _core_mod._update_iceberg_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(cache_key) is ev:
                del _rebuild_signals[cache_key]


def update_iceberg_view(
    con, source: dict, lock_timeout: float = 5.0, force: bool = False, target_table: str = "logs"
) -> None:
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
    if source_key.endswith("::rum") and target_table == "logs":
        update_iceberg_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_vitals")
        update_iceberg_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_errors")
        return

    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key

    # Lock-free fast path first. Parallel dashboard reads (6+ endpoints
    # per page load) only need the lock when a real rebuild is required.
    # Skipped on ``force=True`` (see self-heal path in QueryRunner).
    if not force and _try_fast_path_view(con, source, target_table=target_table):
        return

    lock = _get_service_lock(source_key)

    # If the lock is held, another caller is rebuilding. Wait on their
    # completion signal, then retry the fast path WITHOUT the lock — N
    # cold-parallel waiters can then run fast-path concurrently instead
    # of stepping through the lock serially.
    if not lock.acquire(blocking=False):
        with _rebuild_signals_lock:
            ev = _rebuild_signals.get(cache_key)
        if ev is not None and ev.wait(timeout=lock_timeout):
            if _try_fast_path_view(con, source, target_table=target_table):
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
            cached = _view_cache.get(cache_key)
            if cached and cached[3] and len(cached) > 6 and cached[6] == _source_variant_fp(source):
                try:
                    con.execute(cached[3])
                except Exception:
                    logger.debug("update_iceberg_view: con.execute failed (ignored)", exc_info=True)
                return
            if not force and _persistent_view_exists(con, source, target_table=target_table):
                return
            logger.info(
                "[iceberg] %s: cache empty and no persistent view for %s; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
                target_table,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[iceberg] %s: extended 60s lock wait timed out; view %s rebuild deferred",
                    source_key,
                    target_table,
                )
                return
            try:
                _rebuild_locked(con, source, source_key, target_table=target_table, force=force)
            finally:
                lock.release()
            return
    try:
        _rebuild_locked(con, source, source_key, target_table=target_table, force=force)
    finally:
        lock.release()


def _persistent_view_exists(con, source: dict, target_table: str = "logs") -> bool:
    """Return True if the per-service Iceberg view already exists on this
    connection's database. Used by ``update_iceberg_view`` to skip the
    extended lock wait when the caller can already query the view (even
    if it's slightly stale)."""
    try:
        from backend.core.duckdb import _safe_table_name

        table_name = (
            target_table
            if target_table != "logs"
            else _safe_table_name(source.get("name") or source.get("service_id") or "default")
        )
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _update_iceberg_view_locked(con, source: dict, target_table: str = "logs", force: bool = False) -> None:

    t_start = time.time()
    view_name = _view_target_name(source, target_table)

    # Determine the connection's actual read-only mode up front — this
    # function also runs on read-only, POOLED request connections (the
    # slow-path rebuild triggered when the fast-path view token can't be
    # verified), not only on cron's dedicated read-write connections.
    is_read_only = False
    try:
        res = con.execute(
            "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp','lake') LIMIT 1"
        ).fetchone()
        is_read_only = bool(res and res[0])
    except Exception as e:
        logger.error("[iceberg] duckdb_databases probe failed: %s", e)

    # Only attach "lake" if it isn't already attached on this connection,
    # and match the connection's real read-only mode when we do. Blindly
    # re-attaching with read_only=False (the old behavior) on a connection
    # where "lake" is already attached READ_ONLY throws "database with
    # name 'lake' already exists" — caught below as a harmless no-op by
    # _ducklake_attach's broad "already exists" match, but the mode-
    # mismatched re-attach attempt itself corrupts the internal ducklake
    # metadata catalog as a side effect, leaving every later
    # ducklake_snapshots('lake') call on this (pooled, long-lived)
    # connection failing with "Catalog __ducklake_metadata_lake does not
    # exist" for the rest of its life. See AGENTS.md trap #33.
    lake_attached = con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake' LIMIT 1").fetchone()
    if not lake_attached:
        _ducklake_attach(con, source, read_only=is_read_only)

    from backend import config as svcconfig

    durable_serving = svcconfig.is_durable_serving_mode(source)
    if durable_serving:
        buf_files = []
    else:
        try:
            buf_files = _core_mod.buffer_files(source, table_name=target_table)
        except TypeError:
            buf_files = _core_mod.buffer_files(source)

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    # Committed rows live in the per-service DuckLake table (NOT a bare
    # lake.logs — under a shared catalog that would mix tenants).
    lake_table = ducklake_table_name(source, target_table)
    committed_parts: list[str] = []
    token = _ducklake_view_token(con)
    res = con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
        [lake_table],
    ).fetchone()
    if res:
        snapshot_id = ducklake_current_snapshot_id(con)
        if snapshot_id is None:
            raise RuntimeError("Cannot verify the DuckLake snapshot for the committed view")
        token = f"ducklake:{snapshot_id}"
        # Buffer paths are fixed at bind time; the committed side must not advance past them.
        committed_parts.append(f'SELECT * FROM lake."{lake_table}" AT (VERSION => {snapshot_id})')

    parts = list(committed_parts)
    if buf_files:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in buf_files)
        parts.append(f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)")

    if not parts:
        union_sql = _empty_schema_select(dynamic_arrow_schema)
        existing_cols = set(dynamic_schema_field_names)
    else:
        union_sql = " UNION ALL BY NAME ".join(parts)
        existing_cols = _existing_union_cols(con, committed_parts, buf_files[0] if buf_files else None)

    final_sql = _finalize_view_sql(union_sql, source, target_table, dynamic_schema_field_names, existing_cols)

    if is_read_only or durable_serving:
        create_stmt = f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}"
    else:
        try:
            con.execute(f"DROP VIEW IF EXISTS temp.main.{view_name}")
        except Exception:
            logger.debug("_update_iceberg_view_locked: drop temp view %s failed (ignored)", view_name, exc_info=True)
        create_stmt = f"CREATE OR REPLACE VIEW {view_name} AS {final_sql}"

    try:
        con.execute(create_stmt)
    except Exception as e:
        if "write-write conflict" in str(e).lower():
            try:
                con.rollback()
            except Exception:
                logger.debug("_update_iceberg_view_locked: rollback failed (ignored)", exc_info=True)
            try:
                con.execute(create_stmt)
            except Exception:
                try:
                    con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}")
                except Exception as fallback_err:
                    logger.error("[iceberg] Failed to create view %s: %s", view_name, fallback_err)
                    if durable_serving:
                        raise
        else:
            logger.error("[iceberg] Failed to create view %s: %s", view_name, e)
            if durable_serving:
                raise

    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key
    variant_fp = _source_variant_fp(source)
    schema_fp = tuple(sorted(dynamic_schema_field_names))
    _view_cache[cache_key] = (
        token,
        frozenset(buf_files),
        schema_fp,
        create_stmt,
        round((time.time() - t_start) * 1000, 2),
        False,
        variant_fp,
        tuple(committed_parts),
        frozenset(existing_cols),
    )
    try:
        from backend.core.duckdb_pool import _set_conn_state

        fp_key = f"fast_path_view_fp_{target_table}"
        _set_conn_state(con, **{fp_key: (token, frozenset(buf_files), schema_fp, variant_fp, target_table)})
    except Exception:
        logger.debug("_update_iceberg_view_locked: _set_conn_state failed (ignored)", exc_info=True)


# ---------------------------------------------------------------------------
# Admin / UI metadata

# A-3 (CacheRegistry): register every iceberg cache so the test
# harness can drain them centrally via CacheRegistry.clear_all().
_CacheRegistry.register("iceberg._view_cache", _view_cache)

manifest = sys.modules[__name__]
view = sys.modules[__name__]
sys.modules["backend.core.iceberg.manifest"] = sys.modules[__name__]
sys.modules["backend.core.iceberg.view"] = sys.modules[__name__]
