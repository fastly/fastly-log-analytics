"""DuckLake catalog attachment, schema registry, write admission, and shared storage locks/caches."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pyarrow as pa

from backend import config
from backend.core.duckdb import escape_sql_literal, require_safe_ident
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

_C, _R = "\x1b[36m", "\x1b[0m"
_ICE = f"🧊 {_C}[storage]{_R}"
logger = logging.getLogger("backend.core.storage._core")

# Arrow Schema — derived from LOG_FIELD_CATALOG (unsigned DuckDB types widened to signed Arrow types).
# Append new storage-backed fields to _FIELD_ORDER (guarded by test_field_order_covers_ingest_storage_fields).
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

# Field order is FIXED; append new fields at the end.
_FIELD_ORDER = (
    "timestamp ip status elapsed cache resp_bytes "
    "host url method proto ua referer req_bytes "
    "ttl age hits digest "
    "pop backend edge ttfb tls "
    "country city region "
    "lat lon metro "
    "asn tcp_rtt transport "
    "ploss rtt_min rtt_var retrans bw c_speed c_type "
    "ja3 ja4 "
    "p_type p_desc "
    "waf waf_resp waf_ms waf_sig waf_req_id "
    "q_rtt q_rtt_var q_lost q_cwnd "
    "req_header_bytes server_region is_ipv6 conn_requests delivery_rate data_segs_out tls_ciphers_sha "
    "ottfb ottlb ost obytes oip oretries rid prid "
    "resp_header_content_encoding oconnect_ms _source_file "
    "cookie_session io_input_bytes io_output_bytes io_input_format io_output_format"
).split()

_RESERVED_FIELD_SLOTS: set[str] = set()
_CATALOG_TYPE_MAP = {f["id"]: f["duckdb_type"] for f in LOG_FIELD_CATALOG}

_RUM_TAIL_FIELDS = [
    (k, pa.string())
    for k in ("pathname", "browser", "os", "device", "cid", "req_id", "city", "region", "country", "pop", "tls")
] + [("ttfb", pa.float64())]

CLIENT_VITALS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("metric_name", pa.string()),
        ("metric_value", pa.float64()),
        ("metric_rating", pa.string()),
        *_RUM_TAIL_FIELDS,
    ]
)

CLIENT_ERRORS_ARROW_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("error_message", pa.string()),
        ("error_file", pa.string()),
        ("error_line", pa.int32()),
        ("error_col", pa.int32()),
        *_RUM_TAIL_FIELDS,
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

_RUM_BEACON_TABLES = ("client_vitals", "client_errors")
_CLOUD_WRITE_LOCK_TIMEOUT_S = 30.0


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


def _align_to_schema(table: pa.Table, target_schema: pa.Schema | None = None, source: dict | None = None) -> pa.Table:
    """Align a PyArrow table to a target schema (or dynamically generated if none provided)."""
    if target_schema is not None:
        schema = target_schema
    else:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) if source else None
        log_fields_config = cfg.get("log_fields", {}) if cfg else None
        schema = get_arrow_schema(log_fields_config)

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
_view_cache: dict[str, tuple] = {}
_CacheRegistry.register("storage._view_cache", _view_cache)


def clear_source_caches(source_key: str, *, keep_snapshot_cache: bool = False) -> None:
    """Remove in-memory cache entries for a service."""
    base_key = source_key[:-5] if source_key.endswith("::rum") else source_key
    # Pop the primary and any sub-table view cache keys (e.g. client_vitals, client_errors, ::rum)
    for k in list(_view_cache.keys()):
        if k == base_key or k.startswith(f"{base_key}::"):
            _view_cache.pop(k, None)

    if not keep_snapshot_cache:
        try:
            from backend.core.duckdb_status import clear_ducklake_stats_cache

            clear_ducklake_stats_cache(base_key)
        except Exception:
            logger.debug("clear_source_caches: clear_ducklake_stats_cache failed (ignored)", exc_info=True)


def invalidate_service_caches(source: dict) -> dict[str, int]:
    """Drop process-local view/DuckLake caches for one service."""
    name = source.get("name", "default")
    cleared = sum(1 for k in _view_cache if k == name or k.startswith(f"{name}::"))
    clear_source_caches(name)
    return {"view_entries_cleared": cleared}


def init_storage_table(source: dict, create: bool = True, table_name: str = "logs"):
    with _get_service_lock(source.get("name", "default")):
        return _init_storage_table_locked(source, create, table_name=table_name)


def _init_storage_table_locked(source: dict, create: bool = True, table_name: str = "logs"):
    """Ensure the service's DuckLake catalog is attachable (True, or None on failure)."""
    from backend.core.duckdb import get_connection

    con = get_connection(source, read_only=True)
    try:
        return True if _ducklake_attach(con, source, read_only=True) else None
    finally:
        con.close()


# ── DuckLake catalog attach + registration helpers ──
# Serializes ``ATTACH ... AS lake`` across every connection in this process to prevent
# concurrent alias corruption of ``__ducklake_metadata_lake``.
_attach_lock = threading.Lock()
_ATTACH_LOCK_TIMEOUT_S = float(os.environ.get("DUCKLAKE_ATTACH_LOCK_TIMEOUT_S", "60") or "60")
_ATTACH_CONFLICT_RETRY_ATTEMPTS = int(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_ATTEMPTS", "45") or "45")
_ATTACH_CONFLICT_RETRY_SLEEP_S = float(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_SLEEP_S", "1.5") or "1.5")
_DUCKLAKE_TARGET_FILE_SIZE_MB = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256") or "256")


def _safe_ident(name: str) -> str:
    """Sanitize an arbitrary string into a bare SQL identifier fragment."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name or "").strip("_").lower() or "default"


def ducklake_table_name(src: dict, table_name: str = "logs") -> str:
    """Per-service DuckLake table name for ``src`` under the shared ``lake`` catalog."""
    service_id = src.get("service_id") or src.get("name") or "default"
    base = f"logs_{_safe_ident(str(service_id))}"
    return f"{base}__{_safe_ident(table_name)}" if table_name != "logs" else base


def _is_local_only(source: dict) -> bool:
    if source.get("fos_local_warehouse") is True:
        return True
    return (source.get("fos_endpoint") or source.get("endpoint") or "") in ("http://localhost:0", "http://127.0.0.1:0")


def _default_data_path(source: dict) -> str:
    """Default DuckLake DATA_PATH for ``source`` (S3 in cloud High-Scale, local disk in standard)."""
    force_local = os.getenv("DUCKLAKE_LOCAL_STORAGE", "").lower() in ("1", "true", "yes")
    if config.is_high_scale_mode(source) and not force_local and source.get("bucket") and not _is_local_only(source):
        prefix = (source.get("prefix") or "").strip("/")
        return f"s3://{source['bucket']}/{f'{prefix}/ducklake' if prefix else 'ducklake'}/"
    local_path = config.SERVICES_DATA_DIR / str(source.get("service_id") or source.get("name", "default")) / "ducklake"
    local_path.mkdir(parents=True, exist_ok=True)
    return str(local_path)


def _resolve_catalog_dsn(service_id: object) -> str | None:
    """Resolve and normalize the mandatory PostgreSQL DuckLake catalog DSN."""
    dsn = config.DUCKLAKE_CATALOG
    if not dsn:
        logger.error("[ducklake] %s: DUCKLAKE_CATALOG is unset; PostgreSQL DSN is required", service_id)
        return None
    for candidate in (dsn, os.environ.get("DUCKLAKE_CATALOG", "")):
        if candidate.startswith(("postgres://", "postgresql://")):
            return f"postgres:{candidate}"
        if candidate.startswith("postgres:"):
            return candidate
    logger.error("[ducklake] %s: DUCKLAKE_CATALOG must be a postgres DSN, got: %s", service_id, dsn)
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
    if _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
        return True
    logger.warning("[ducklake] %s: %s", service_id, reason)
    return False


def _sleep_releasing_attach_lock(service_id: object, reason: str) -> bool:
    _attach_lock.release()
    time.sleep(_ATTACH_CONFLICT_RETRY_SLEEP_S)
    return _acquire_attach_lock(service_id, f"timed out re-acquiring the attach lock while retrying after {reason}")


def _detach_unlocked(con, aliases: tuple[str, ...] = ("lake",), *, ignore_errors: bool = True) -> None:
    for alias in aliases:
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            if not ignore_errors:
                raise
            logger.debug("_ducklake: DETACH %s failed (ignored)", alias, exc_info=True)


def _probe_lake_usable(con) -> Exception | None:
    try:
        con.execute("SELECT 1 FROM ducklake_snapshots('lake') LIMIT 1").fetchone()
        return None
    except Exception as exc:
        return exc


def _load_ducklake_extension(con, service_id: object) -> bool:
    bundled = os.getenv("DUCKLAKE_EXTENSION_PATH")
    try:
        ext_dir = os.getenv("DUCKDB_EXTENSION_DIRECTORY")
        if ext_dir:
            os.makedirs(ext_dir, exist_ok=True)
            con.execute(f"SET extension_directory = '{escape_sql_literal(ext_dir)}';")
        if bundled:
            con.execute(f"LOAD '{escape_sql_literal(bundled)}';")
        else:
            try:
                con.execute("LOAD ducklake;")
            except Exception:
                con.execute("INSTALL ducklake; LOAD ducklake;")
        return True
    except Exception as e:
        logger.warning(
            "[ducklake] %s: failed to load %s DuckLake extension: %s",
            service_id,
            "bundled" if bundled else "installed",
            e,
        )
        return False


def _ducklake_attach(con, source: dict, read_only: bool = False) -> bool:
    """Attach the DuckLake catalog for ``source`` as ``lake`` on ``con``."""
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
            existing_ro = bool(attached[0])
            if not existing_ro and read_only:
                probe_err = _probe_lake_usable(con)
                if probe_err is not None:
                    logger.warning(
                        "[ducklake] %s: existing read-write lake catalog is unusable for read: %s",
                        service_id,
                        probe_err,
                    )
                    return False
                return True
            if existing_ro == read_only:
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
                        "[ducklake] %s: failed to detach mismatched lake catalog: %s", service_id, detach_err
                    )
                    return False

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
                    "[ducklake] %s: failed to detach orphaned DuckLake metadata catalog: %s", service_id, detach_err
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
    """Pin DuckLake's target file size to the compaction cap (idempotent)."""
    require_safe_ident(alias, "invalid ducklake alias")
    target = f"{_DUCKLAKE_TARGET_FILE_SIZE_MB}MiB"
    target_bytes = str(_DUCKLAKE_TARGET_FILE_SIZE_MB * 1024 * 1024)
    try:
        row = con.execute(f"SELECT value FROM {alias}.options() WHERE option_name = 'target_file_size'").fetchone()
        if not (row and str(row[0]) == target_bytes):
            con.execute(f"CALL {alias}.set_option('target_file_size', '{target}')")
    except Exception as e:
        logger.warning("[ducklake] failed to apply target_file_size=%s: %s", target, e)


def ducklake_current_snapshot_id(con, alias: str = "lake") -> int | None:
    """Latest snapshot id of the attached DuckLake catalog, or None."""
    require_safe_ident(alias, "invalid ducklake alias")
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{alias}') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.info("[ducklake] snapshot-id probe failed (lake not attached?): %s", e)
        return None


# ── Bounded per-service admission for DuckLake writers ──

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
    value = int.from_bytes(hashlib.sha256(service_id.encode()).digest()[:8], "big", signed=False)
    return value - (1 << 64) if value >= (1 << 63) else value


def _record_stat(service_id: str, key: str, value: float = 1.0) -> None:
    with _stats_guard:
        bucket = _stats.setdefault(
            service_id, {"acquisitions": 0.0, "timeouts": 0.0, "wait_ms_total": 0.0, "hold_ms_total": 0.0}
        )
        bucket[key] += value


def get_admission_stats() -> dict[str, dict[str, float]]:
    """Return cumulative writer-admission counters for diagnostics."""
    with _stats_guard:
        return {service_id: dict(values) for service_id, values in _stats.items()}


@contextmanager
def ducklake_write_admission(
    service_id: str, *, timeout_s: float = DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S
) -> Iterator[None]:
    """Serialize one service's DuckLake writer across threads (RLock) and processes (PG advisory lock)."""
    lock = _local_lock(service_id)
    wait_started = time.monotonic()
    if not lock.acquire(timeout=max(0.0, timeout_s)):
        _record_stat(service_id, "timeouts")
        raise DuckLakeAdmissionTimeout(f"DuckLake writer admission timed out for {service_id}")
    _record_stat(service_id, "wait_ms_total", (time.monotonic() - wait_started) * 1000.0)

    pg_con, advisory = None, False
    deadline = time.monotonic() + max(0.0, timeout_s)
    key = _advisory_key(service_id)
    hold_started = time.monotonic()
    try:
        import psycopg

        from backend.core.metadata.base import get_pg_thread_connection

        pg_con = get_pg_thread_connection()
        if pg_con is None:
            raise RuntimeError("pg_con is unexpectedly None")
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


@contextmanager
def _ducklake_write_connection(source: dict) -> Iterator[Any]:
    """Context manager yielding a DuckDB connection with DuckLake attached read-write."""
    from backend.core.duckdb import get_connection, get_memory_connection

    uses_postgres = (config.DUCKLAKE_CATALOG or "").startswith(("postgres://", "postgresql://", "postgres:"))
    service_id = source.get("service_id") or source.get("name", "default")
    con = None
    try:
        if uses_postgres:
            con = get_memory_connection(source)
        else:
            con = get_connection(source, read_only=True)
            _ducklake_detach(con, service_id=service_id)
        if not _ducklake_attach(con, source, read_only=False):
            raise RuntimeError("Failed to attach DuckLake in read-write mode")
        yield con
    finally:
        if con is not None:
            if not uses_postgres:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    logger.debug("_ducklake_write_connection: con.execute failed (ignored)", exc_info=True)
            _ducklake_detach(con, service_id=service_id)
            try:
                con.close()
            except Exception:
                logger.debug("_ducklake_write_connection: con.close failed (ignored)", exc_info=True)


def _lake_ident(name: str) -> str:
    return require_safe_ident(name, "unsafe ducklake table name")


_validate_lake_ident = _lake_ident


def _lake_columns(con, table: str) -> set[str]:
    """Column names of ``lake.<table>``, or an empty set if it doesn't exist."""
    try:
        return {str(r[0]) for r in con.execute(f'DESCRIBE lake."{_lake_ident(table)}"').fetchall()}
    except Exception:
        return set()
