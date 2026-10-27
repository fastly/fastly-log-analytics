"""DuckLake integration: table init, buffer commits, optimization, snapshot expiry, DuckDB view wiring, and admin metadata."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
import threading
import time
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pyarrow as pa

from backend import config
from backend.core.duckdb import escape_sql_literal, require_safe_ident
from backend.core.log_fields import LOG_FIELD_CATALOG

_C, _R = "\x1b[36m", "\x1b[0m"
_ICE = f"🧊 {_C}[storage]{_R}"
logger = logging.getLogger(__name__)

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
    return os.path.join(base, table_name) if table_name != "logs" else base


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


sys.modules["backend.core.storage._ducklake"] = sys.modules[__name__]

# ── Local Parquet buffer & DuckLake commit/maintenance (folded from buffer.py) ──

import glob as _glob
from datetime import UTC, timedelta

import pyarrow.parquet as pq  # noqa: E402

from backend.core.query_instrumentation import InstrumentedDuckDBConnection  # noqa: E402

_core_mod = sys.modules[__name__]
_TOMBSTONE_SUFFIX = ".consumed-"
_TOMBSTONE_GRACE_SECONDS = 300


def _tombstone_marker_path(parquet_path: str, ts: int) -> str:
    return f"{parquet_path}{_TOMBSTONE_SUFFIX}{ts}"


def _is_tombstone_marker(name: str) -> bool:
    """True iff ``name`` is a tombstone sidecar (``<basename>.parquet.consumed-<ts>``)."""
    if _TOMBSTONE_SUFFIX not in name:
        return False
    head, _, tail = name.rpartition(_TOMBSTONE_SUFFIX)
    return head.endswith(".parquet") and tail.isdigit()


def _iter_tombstone_markers(buf_dir: str, recursive: bool = True) -> list[str]:
    if not os.path.isdir(buf_dir):
        return []
    pat = (
        os.path.join(buf_dir, "**", f"*{_TOMBSTONE_SUFFIX}*")
        if recursive
        else os.path.join(buf_dir, f"*{_TOMBSTONE_SUFFIX}*")
    )
    return [p for p in _glob.glob(pat, recursive=recursive) if _is_tombstone_marker(os.path.basename(p))]


def _tombstoned_parquet_paths(buf_dir: str, recursive: bool = True) -> set[str]:
    """Return buffer parquet paths that have an active tombstone sibling."""
    return {p.rsplit(_TOMBSTONE_SUFFIX, 1)[0] for p in _iter_tombstone_markers(buf_dir, recursive=recursive)}


def tombstone_buffer_files(source: dict, paths: list[str], *, ts: int | None = None) -> list[str]:
    """Mark buffer parquet files as logically consumed via ``.consumed-<ts>`` sidecars without unlinking."""
    if ts is None:
        ts = int(time.time())
    tombstoned: list[str] = []
    for path in paths:
        try:
            with open(_tombstone_marker_path(path, ts), "x"):
                pass
            tombstoned.append(path)
        except FileExistsError:
            tombstoned.append(path)
        except Exception as e:
            logger.warning(
                "%s Failed to tombstone buffer file %s — will retry next sweep. Error: %s",
                _core_mod._ICE,
                path,
                e,
            )
    return tombstoned


def sweep_tombstoned_buffer_files(
    source: dict, *, table_name: str = "logs", grace_seconds: int = _TOMBSTONE_GRACE_SECONDS, now: int | None = None
) -> int:
    """Unlink tombstoned buffer parquets whose grace window has elapsed."""
    if now is None:
        now = int(time.time())
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    swept = 0
    for marker in _iter_tombstone_markers(buf, recursive=(table_name != "logs")):
        try:
            ts = int(marker.rsplit(_TOMBSTONE_SUFFIX, 1)[1])
        except (ValueError, IndexError):
            continue
        if now - ts < grace_seconds:
            continue
        parquet_path = marker.rsplit(_TOMBSTONE_SUFFIX, 1)[0]
        try:
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink %s: %s", _core_mod._ICE, parquet_path, e)
            continue
        try:
            os.remove(marker)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink tombstone %s: %s", _core_mod._ICE, marker, e)
        swept += 1
    return swept


def buffer_files(source: dict, table_name: str = "logs") -> list[str]:
    """Return sorted list of non-tombstoned Parquet files currently in the local buffer."""
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    if not os.path.isdir(buf):
        return []
    recursive = table_name != "logs"
    tombstoned = _tombstoned_parquet_paths(buf, recursive=recursive)
    pattern = os.path.join(buf, "**", "*.parquet") if recursive else os.path.join(buf, "*.parquet")
    return sorted(
        p
        for p in _glob.glob(pattern, recursive=recursive)
        if os.path.isfile(p) and p not in tombstoned and not _is_tombstone_marker(os.path.basename(p))
    )


_QUARANTINE_SUBDIR = ".quarantine"


def _quarantine_dir(source: dict, table_name: str = "logs") -> str:
    return os.path.join(_core_mod._buffer_dir(source, table_name=table_name), _QUARANTINE_SUBDIR)


def _quarantine_buffer_file(source: dict, path: str, error: BaseException, table_name: str = "logs") -> str | None:
    """Move a corrupt buffer parquet into ``.quarantine/`` with a JSON sidecar."""
    try:
        import json

        from backend.utils.date_utils import iso_z_now

        qdir = _quarantine_dir(source, table_name=table_name)
        os.makedirs(qdir, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        new_path = os.path.join(qdir, f"{ts}__{os.path.basename(path)}")
        if os.path.exists(new_path):
            i = 1
            while os.path.exists(f"{new_path}.{i}"):
                i += 1
            new_path = f"{new_path}.{i}"
        os.rename(path, new_path)
        with open(new_path + ".json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "original_path": path,
                    "quarantined_at": iso_z_now(),
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                },
                f,
                indent=2,
            )
        logger.error(
            "%s Quarantined unreadable buffer parquet %s -> %s (%s: %s)",
            _core_mod._ICE,
            path,
            new_path,
            type(error).__name__,
            str(error)[:200],
        )
        return new_path
    except Exception as quarantine_err:
        logger.error(
            "%s Failed to quarantine buffer file %s — leaving in place. Quarantine error: %s",
            _core_mod._ICE,
            path,
            quarantine_err,
        )
        return None


def buffer_backlog_stats(source: dict, table_name: str = "logs") -> dict:
    """Snapshot of the local buffer: file count, total bytes, and oldest file age in seconds."""
    files = buffer_files(source, table_name=table_name)
    if not files:
        return {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}
    now, total_bytes, oldest_mtime, oldest_path = time.time(), 0, time.time(), files[0]
    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            continue
        total_bytes += st.st_size
        if st.st_mtime < oldest_mtime:
            oldest_mtime, oldest_path = st.st_mtime, p
    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "oldest_age_seconds": int(max(0, now - oldest_mtime)),
        "oldest_path": oldest_path,
    }


def write_to_buffer(source: dict, arrow_table: pa.Table, filename: str, table_name: str = "logs") -> str:
    """Write a PyArrow table to the local buffer as a ZSTD(1) Parquet file."""
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    os.makedirs(buf, exist_ok=True)
    path = os.path.join(buf, filename)
    cfg = config.load_config(source.get("service_id") or source.get("name"))
    target_schema = _core_mod.get_arrow_schema(cfg.get("log_fields", {}) if cfg else None, table_name=table_name)
    aligned = _core_mod._align_to_schema(arrow_table, target_schema=target_schema, source=source)
    if "timestamp" in aligned.column_names:
        sort_keys = [("timestamp", "ascending")] + ([("ip", "ascending")] if "ip" in aligned.column_names else [])
        aligned = aligned.sort_by(sort_keys)
    tmp_path = path + ".tmp"
    pq.write_table(aligned, tmp_path, compression="zstd", compression_level=1)
    os.rename(tmp_path, path)
    return path


_BUFFER_COMMIT_CHUNK_SIZE = int(os.environ.get("BUFFER_COMMIT_CHUNK_SIZE", "50") or "50")
_CLOUD_WRITE_LOCK_TIMEOUT_S = 30.0


def commit_buffer(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    """Append all local buffer files to the DuckLake table under the per-service lock."""
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s commit_buffer: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    try:
        return _commit_buffer_impl(source, progress_callback, table_name=table_name)
    finally:
        lock.release()


@contextmanager
def _ducklake_write_connection(source: dict):
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


def _commit_one_chunk(
    source: dict, lake_ident: str, files: list[str], table_name: str
) -> tuple[int, list[str], int, list[str]]:
    """Commit a single bounded chunk of buffer files under one DuckLake write-attach."""
    rows_committed, committed_paths, quarantined_files, file_errors = 0, [], 0, []

    with _ducklake_write_connection(source) as con:

        def _commit_paths(paths: list[str]) -> None:
            paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)
            scan_sql = f"read_parquet([{paths_sql}], union_by_name=true)"
            con.execute(f"CREATE TABLE IF NOT EXISTS {lake_ident} AS SELECT * FROM {scan_sql} LIMIT 0")
            table_cols: set[str] = set()
            try:
                table_cols = {r[0] for r in con.execute(f"DESCRIBE {lake_ident}").fetchall()}
                parquet_cols_res = con.execute(f"DESCRIBE SELECT * FROM {scan_sql} LIMIT 0").fetchall()
                parquet_cols = {r[0] for r in parquet_cols_res}
                for p_col, p_type, *_ in parquet_cols_res:
                    if p_col not in table_cols:
                        col_ident = '"{}"'.format(p_col.replace('"', '""'))
                        con.execute(f"ALTER TABLE {lake_ident} ADD COLUMN {col_ident} {p_type}")
                        table_cols.add(p_col)
            except Exception as e:
                logger.warning("%s Failed to sync schema for paths: %s", _core_mod._ICE, e)
                parquet_cols = set()

            delete_by_source = "_source_file" in table_cols and "_source_file" in parquet_cols
            nat_keys = RUM_NATURAL_KEYS.get(table_name)
            delete_by_natural = (
                not delete_by_source
                and nat_keys is not None
                and all(k in table_cols and k in parquet_cols for k in nat_keys)
            )
            con.execute("BEGIN TRANSACTION")
            try:
                if delete_by_source:
                    con.execute(
                        f"DELETE FROM {lake_ident} WHERE _source_file IN (SELECT DISTINCT _source_file FROM {scan_sql})"
                    )
                    con.execute(f"INSERT INTO {lake_ident} BY NAME SELECT * FROM {scan_sql}")
                elif delete_by_natural and nat_keys:
                    join_cond = " AND ".join(f'lake_tbl."{k}" IS NOT DISTINCT FROM incoming."{k}"' for k in nat_keys)
                    con.execute(
                        f"DELETE FROM {lake_ident} AS lake_tbl WHERE EXISTS (SELECT 1 FROM {scan_sql} AS incoming WHERE {join_cond})"
                    )
                    nat_cols_expr = ", ".join(f'"{k}"' for k in nat_keys)
                    con.execute(
                        f"INSERT INTO {lake_ident} BY NAME SELECT * EXCLUDE (_dup_rn) FROM ("
                        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {nat_cols_expr} ORDER BY timestamp) AS _dup_rn FROM {scan_sql}"
                        f") WHERE _dup_rn = 1"
                    )
                else:
                    con.execute(f"INSERT INTO {lake_ident} BY NAME SELECT * FROM {scan_sql}")
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                raise e

        try:
            batch_rows = sum(pq.read_metadata(p).num_rows for p in files)
            _commit_paths(files)
            rows_committed, committed_paths = batch_rows, list(files)
        except Exception as batch_err:
            logger.warning(
                "%s Full-batch commit failed (%s) — falling back to per-file commits with quarantine",
                _core_mod._ICE,
                batch_err,
            )
            for f in files:
                try:
                    file_rows = pq.read_metadata(f).num_rows
                    _commit_paths([f])
                    rows_committed += file_rows
                    committed_paths.append(f)
                except Exception as inner_err:
                    logger.error("%s Commit buffer error on %s: %s", _core_mod._ICE, f, inner_err)
                    if (
                        isinstance(inner_err, pa.ArrowInvalid)
                        or any(
                            tok in str(inner_err)
                            for tok in ("is not a valid Parquet file", "No files found", "Missing page")
                        )
                    ) and _quarantine_buffer_file(source, f, inner_err, table_name):
                        quarantined_files += 1
                        continue
                    file_errors.append(f"{os.path.basename(f)}: {inner_err}")

    return rows_committed, committed_paths, quarantined_files, file_errors


def _commit_buffer_impl(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    swept = sweep_tombstoned_buffer_files(source, table_name=table_name)
    files = buffer_files(source, table_name=table_name)
    if not files:
        if swept:
            _core_mod.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}

    lake_table = ducklake_table_name(source, table_name)
    lake_ident = 'lake."{}"'.format(lake_table.replace('"', '""'))
    rows_committed, committed_paths, quarantined_files, commit_errors = 0, [], 0, []

    for i in range(0, len(files), _BUFFER_COMMIT_CHUNK_SIZE):
        chunk = files[i : i + _BUFFER_COMMIT_CHUNK_SIZE]
        try:
            chunk_rows, chunk_paths, chunk_quarantined, chunk_errors = _commit_one_chunk(
                source, lake_ident, chunk, table_name
            )
        except Exception as chunk_err:
            logger.error("%s DuckLake buffer chunk failed: %s", _core_mod._ICE, chunk_err)
            commit_errors.append(str(chunk_err))
            break
        rows_committed += chunk_rows
        committed_paths.extend(chunk_paths)
        quarantined_files += chunk_quarantined
        commit_errors.extend(chunk_errors)

    if committed_paths:
        tombstone_buffer_files(source, committed_paths)
        _core_mod.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)

    result = {
        "files_committed": len(committed_paths),
        "rows_committed": rows_committed,
        "snapshot_id": "ducklake",
        "quarantined_files": quarantined_files,
    }
    if commit_errors:
        result["error"] = "; ".join(commit_errors)
    return result


def optimize_table(
    source: dict,
    target_file_size_mb: int = 128,
    min_files_per_partition: int | None = None,
    table_name: str | None = None,
) -> dict:
    """Compact small DuckLake data files into larger ones (flush inlined data, merge, rewrite)."""
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s optimize_table: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"error": "service busy (reset or another writer in progress)", "files_rewritten": 0, "files_added": 0}
    try:
        return _optimize_table_impl(source, target_file_size_mb, min_files_per_partition, table_name=table_name)
    finally:
        lock.release()


def _optimize_table_impl(
    source: dict,
    target_file_size_mb: int = 128,
    min_files_per_partition: int | None = None,
    table_name: str | None = None,
) -> dict:
    try:
        with _ducklake_write_connection(source) as con:
            # Flush inlined rows first so merge/rewrite see materialized parquet files.
            con.execute("CALL ducklake_flush_inlined_data('lake')").fetchall()
            sid = source.get("service_id") or source.get("name", "default")
            logger.info("[ducklake] %s: ducklake_flush_inlined_data executed successfully", sid)

            candidate_tables = (
                [table_name]
                if table_name and table_name not in ("logs", "all", "*")
                else ["logs", "client_vitals", "client_errors"]
            )
            op_rows, partition_errors, tables_processed = [], [], []

            for t_name in candidate_tables:
                tbl = ducklake_table_name(source, table_name=t_name)
                if not _lake_columns(con, tbl):
                    continue
                tables_processed.append(t_name)
                for fn_name, label in (
                    ("ducklake_merge_adjacent_files", "merge"),
                    ("ducklake_rewrite_data_files", "rewrite"),
                ):
                    try:
                        op_rows.extend(con.execute(f"CALL {fn_name}('lake', '{tbl}')").fetchall())
                        logger.info("[ducklake] %s: %s executed successfully for table %s", sid, fn_name, tbl)
                    except Exception as exc:
                        logger.warning("%s optimize_table: %s failed on %s: %s", sid, label, tbl, exc)
                        partition_errors.append(f"{t_name} {label}: {exc}")

            logger.info("[ducklake] %s: ducklake_rewrite_data_files executed successfully", sid)
            return {
                "files_rewritten": sum(int(r[2]) for r in op_rows if len(r) >= 4),
                "files_added": sum(int(r[3]) for r in op_rows if len(r) >= 4),
                "eligible_partitions": len(tables_processed) or 1,
                "partition_errors": partition_errors,
            }
    except Exception as e:
        return {"error": str(e), "files_rewritten": 0, "files_added": 0}


def run_cloud_maintenance(source: dict) -> dict:
    """Run weekly maintenance: expire old metadata, delete old data, and purge old local cache."""
    sid = source.get("service_id") or source.get("name", "default")
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s run_cloud_maintenance: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"error": "service busy (reset or another writer in progress)"}
    try:
        with ducklake_write_admission(sid):
            return _run_cloud_maintenance_impl(source)
    finally:
        lock.release()


_RUM_BEACON_TABLES = ("client_vitals", "client_errors")


def _lake_ident(name: str) -> str:
    return require_safe_ident(name, "unsafe ducklake table name")


_validate_lake_ident = _lake_ident


def _lake_columns(con, table: str) -> set[str]:
    """Column names of ``lake.<table>``, or an empty set if it doesn't exist."""
    try:
        return {str(r[0]) for r in con.execute(f'DESCRIBE lake."{_lake_ident(table)}"').fetchall()}
    except Exception:
        return set()


def _lake_delete_before(con, table: str, cutoff: datetime, extra_predicate: str = "") -> int:
    where = f"timestamp < ? AND {extra_predicate}" if extra_predicate else "timestamp < ?"
    row = con.execute(f'DELETE FROM lake."{_lake_ident(table)}" WHERE {where}', [cutoff]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ducklake_retention_delete(con, source: dict, data_retention_days: int, rum_retention_days: int) -> dict:
    """Step 1: enforce ``data_retention_days`` / ``rum_retention_days`` (0 means keep forever)."""
    out: dict[str, Any] = {}
    now = datetime.now(UTC)
    logs_table = ducklake_table_name(source, table_name="logs")
    deleted_anything = False

    if data_retention_days > 0:
        cutoff = now - timedelta(days=data_retention_days)
        predicate = (
            "rum_cid IS NULL"
            if rum_retention_days > data_retention_days and "rum_cid" in _lake_columns(con, logs_table)
            else ""
        )
        out["data_deleted_before_days"] = data_retention_days
        out["data_rows_deleted"] = _lake_delete_before(con, logs_table, cutoff, predicate)
        deleted_anything = True
        if rum_retention_days > 0:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_log_rows_deleted"] = _lake_delete_before(con, logs_table, now - timedelta(days=rum_retention_days))

    if rum_retention_days > 0:
        rum_cutoff = now - timedelta(days=rum_retention_days)
        beacon_rows, beacon_tables = 0, 0
        for t_name in _RUM_BEACON_TABLES:
            tbl = ducklake_table_name(source, table_name=t_name)
            if not _lake_columns(con, tbl):
                continue
            beacon_rows += _lake_delete_before(con, tbl, rum_cutoff)
            beacon_tables += 1
        if beacon_tables:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_beacon_rows_deleted"] = beacon_rows
            deleted_anything = True

    if deleted_anything:
        _core_mod.clear_source_caches(source.get("name", "default"))
    return out


def _remote_data_path(source: dict) -> bool:
    return _resolve_data_path(source, source.get("service_id") or source.get("name", "default")).startswith("s3://")


def _fos_object_size(source: dict, bucket: str, key: str) -> int:
    from backend.core.duckdb import _get_fos_client

    try:
        return int(_get_fos_client(source).head_object(Bucket=bucket, Key=key)["ContentLength"])
    except Exception:
        return 0


def _scheduled_file_sizes(source: dict, cutoff: datetime) -> dict[str, int]:
    """Size by file name of this service's FOS files DuckLake has scheduled for deletion and the cleanup will unlink.

    DuckLake keeps only a path once it schedules a file (a merge or rewrite schedules the replaced files at once), so the
    size is read from FOS just before the cleanup deletes the object.
    """
    import psycopg

    dsn = _resolve_catalog_dsn(source.get("service_id") or source.get("name"))
    if not dsn:
        return {}
    root = _resolve_data_path(source, source.get("service_id") or source.get("name", "default"))
    bucket, _, key = root[len("s3://") :].partition("/")
    base = f"{key.rstrip('/')}/" if key.strip("/") else ""
    patterns = [f"main/{ducklake_table_name(source, t)}/%" for t in ("logs", "client_vitals", "client_errors")]
    with psycopg.connect(dsn.removeprefix("postgres:")) as pg:
        rows = pg.execute(
            "SELECT path FROM ducklake_files_scheduled_for_deletion WHERE path_is_relative AND schedule_start < %s "
            "AND path LIKE ANY(%s)",
            (cutoff, patterns),
        ).fetchall()
    return {os.path.basename(str(r[0])): _fos_object_size(source, bucket, base + str(r[0])) for r in rows}


def _ducklake_expire_snapshots(con, source: dict, keep_snapshot_days: int) -> dict:
    """Step 2: prune DuckLake catalog snapshots older than ``keep_snapshot_days``."""
    out: dict[str, Any] = {}
    cutoff = datetime.now(UTC) - timedelta(days=keep_snapshot_days)
    snapshot_errors: list[str] = []
    snapshots_before: int | None = None
    snapshots_after: int | None = None

    def _count_snapshots(phase: str) -> int | None:
        try:
            row = con.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()
            return int(row[0]) if row else 0
        except Exception as e:
            snapshot_errors.append(str(e))
            logger.warning("[ducklake] %s: could not count snapshots %s expiry: %s", source.get("name"), phase, e)
            return None

    snapshots_before = _count_snapshots("before")
    if snapshots_before is not None:
        out["snapshots_before"] = snapshots_before
    try:
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff]).fetchall()
    except Exception as e:
        snapshot_errors.append(str(e))
        logger.warning("[ducklake] %s: snapshot expiry failed: %s", source.get("name"), e)
    snapshots_after = _count_snapshots("after")
    if snapshots_after is not None:
        out["snapshots_after"] = snapshots_after

    if snapshots_before is not None and snapshots_after is not None:
        out["snapshots_expired_before_days"] = keep_snapshot_days
        out["snapshots_expired_count"] = max(0, snapshots_before - snapshots_after)
        if snapshots_before == 0:
            out["snapshot_expiry_note"] = "no snapshots present in the DuckLake catalog"
            logger.info("[ducklake] %s: no snapshots present to expire", source.get("name"))
    if snapshot_errors:
        out["snapshot_expiry_error"] = "; ".join(snapshot_errors)

    billing_id = str(source.get("service_id") or source.get("name"))
    sizes: dict[str, int] = {}
    if _remote_data_path(source):
        # FOS bills a deleted object for 30 days; read the sizes before the cleanup unlinks them.
        try:
            sizes = _scheduled_file_sizes(source, cutoff)
        except Exception:
            logger.debug("[ducklake] %s: could not size files for billing", source.get("name"), exc_info=True)
    try:
        cleaned = con.execute("CALL ducklake_cleanup_old_files('lake', older_than => ?)", [cutoff]).fetchall()
        out["data_files_cleaned"] = len(cleaned)
        if sizes and cleaned:
            try:
                from backend.core.metadata.usage_log import record_cloud_copy_deletion

                names = [os.path.basename(str(r[0])) for r in cleaned]
                billed = sum(sizes.get(n, 0) for n in names)
                record_cloud_copy_deletion(billing_id, billed, len(names))
                out["fos_deleted_bytes_billed"] = billed
            except Exception:
                logger.debug("[ducklake] %s: could not bill deleted files", source.get("name"), exc_info=True)
    except Exception as e:
        logger.warning("[ducklake] %s: old-file cleanup after expiry failed: %s", source.get("name"), e)
        out["data_file_cleanup_error"] = str(e)

    snapshots_expired = out.get("snapshots_expired_count", 0)
    if snapshots_expired > 0 or out.get("data_files_cleaned", 0):
        out["snapshot_expiry_note"] = (
            "catalog metadata entries only; parquet is not deleted by the expiry itself — "
            f"ducklake_cleanup_old_files (older_than={keep_snapshot_days}d) unlinked "
            f"{out.get('data_files_cleaned', 'unknown')} file(s), and rows deleted "
            "by retention persist physically until the daily ducklake_rewrite_data_files rewrite"
        )
        logger.info(
            "[ducklake] %s: expired %d snapshots (%s -> %s), unlinked %s file(s)",
            source.get("name"),
            snapshots_expired,
            snapshots_before,
            snapshots_after,
            out.get("data_files_cleaned", "unknown"),
        )
    return out


def _run_ducklake_maintenance(
    source: dict, *, data_retention_days: int, rum_retention_days: int, keep_snapshot_days: int
) -> dict:
    """Run retention delete and snapshot expiry on one read-write ``lake`` attach."""
    out: dict[str, Any] = {}
    try:
        with _ducklake_write_connection(source) as raw_con:
            con = InstrumentedDuckDBConnection(
                raw_con, service_id=source.get("service_id") or source.get("name", "default")
            )
            if data_retention_days > 0 or rum_retention_days > 0:
                try:
                    out.update(_ducklake_retention_delete(con, source, data_retention_days, rum_retention_days))
                except Exception as e:
                    logger.warning("[ducklake] Data deletion skipped: %s", e)
                    out["data_deletion_error"] = str(e)
            try:
                out.update(_ducklake_expire_snapshots(con, source, keep_snapshot_days))
            except Exception as e:
                logger.warning("[ducklake] Snapshot expiry skipped: %s", e)
                out["snapshot_expiry_error"] = str(e)
    except Exception as e:
        logger.warning("[ducklake] %s: maintenance could not open the lake: %s", source.get("name"), e)
        return {"data_deletion_error": str(e), "snapshot_expiry_error": str(e)}
    return out


def _run_cloud_maintenance_impl(source: dict) -> dict:
    """Run retention deletion, snapshot expiry, and independent local purges."""
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        data_retention_days = int(cron_sync.get("data_retention_days", 30))
        rum_retention_days = int(cron_sync.get("rum_retention_days", data_retention_days))
        cache_retention_days = int(cron_sync.get("cache_retention_days", 90))
        keep_snapshot_days = max(1, int(cron_sync.get("keep_snapshot_days", 7)))
    except Exception as e:
        return {"error": str(e)}

    results: dict[str, Any] = {
        "local_cache_files_deleted": 0,
        "local_temp_files_deleted": 0,
        "local_rollup_files_deleted": 0,
    }
    results.update(
        _run_ducklake_maintenance(
            source,
            data_retention_days=data_retention_days,
            rum_retention_days=rum_retention_days,
            keep_snapshot_days=keep_snapshot_days,
        )
    )

    def _purge_old_files(
        dir_path: str,
        suffixes: str | tuple[str, ...],
        cutoff: datetime,
        errors: list[str],
        label: str,
        prune: bool = False,
    ) -> int:
        if not os.path.exists(dir_path):
            return 0
        deleted = 0

        def _on_walk_err(error: OSError) -> None:
            errors.append(str(error))
            logger.warning("[storage] Local %s scan failed: %s", label, error)

        for root, _, files in os.walk(dir_path, onerror=_on_walk_err):
            for file in files:
                if not file.endswith(suffixes):
                    continue
                filepath = os.path.join(root, file)
                try:
                    if datetime.fromtimestamp(os.path.getmtime(filepath), tz=UTC) < cutoff:
                        os.remove(filepath)
                        deleted += 1
                except Exception as e:
                    errors.append(f"{filepath}: {e}")
                    logger.warning("[storage] Could not remove expired %s %s: %s", label, filepath, e)
        if prune:
            _core_mod._prune_empty_dirs(dir_path)
        return deleted

    cache_errors: list[str] = []
    try:
        from backend.core.duckdb import _cache_dir

        cache_root = _cache_dir(source)
        candidate_dirs = [os.path.join(cache_root, "data"), os.path.join(cache_root, "buffer")] + [
            os.path.join(cache_root, f"data_{t_name}") for t_name in _RUM_BEACON_TABLES
        ]
        now_utc = datetime.now(UTC)
        if cache_retention_days > 0:
            cache_cutoff = now_utc - timedelta(days=cache_retention_days)
            for c_dir in candidate_dirs:
                results["local_cache_files_deleted"] += _purge_old_files(
                    c_dir, ".parquet", cache_cutoff, cache_errors, "cache file", prune=True
                )
        results["local_temp_files_deleted"] += _purge_old_files(
            cache_root, (".tmp", ".part", ".bad.jsonl.tmp"), now_utc - timedelta(hours=2), cache_errors, "temp file"
        )
    except Exception as e:
        cache_errors.append(str(e))
        logger.warning("[storage] Local cache cleanup failed: %s", e)
    if cache_errors:
        results["local_cache_error"] = "; ".join(cache_errors)

    rollup_retention_months = int(cron_sync.get("rollup_retention_months") or 12)
    if rollup_retention_months > 0:
        try:
            from backend.core.duckdb import _cache_dir

            rollup_errors: list[str] = []
            results["local_rollup_files_deleted"] += _purge_old_files(
                os.path.join(_cache_dir(source), "rollups"),
                ".parquet",
                datetime.now(UTC) - timedelta(days=rollup_retention_months * 30),
                rollup_errors,
                "rollup",
                prune=True,
            )
            if rollup_errors:
                results["local_rollup_error"] = "; ".join(rollup_errors)
        except Exception as e:
            logger.warning("[storage] Local rollup cleanup skipped: %s", e)
            results["local_rollup_error"] = str(e)

    results["retention_deleted_rows"] = sum(
        int(results.get(key, 0) or 0)
        for key in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
    )
    results["snapshots_expired"] = int(results.get("snapshots_expired_count", 0) or 0)
    results["files_unlinked"] = int(results.get("data_files_cleaned", 0) or 0)
    return results


# ---------------------------------------------------------------------------
# DuckDB integration
# ---------------------------------------------------------------------------

# ── DuckLake-native table-info / calendar + schema alignment (folded from manifest.py) ──
#
# Every commit goes through `backend.core.storage.buffer._commit_buffer_impl`'s
# DuckLake attach (see `backend/core/storage/_ducklake.py` module docstring),
# and `get_table_info`/`get_snapshot_calendar` below read DuckLake state
# directly.


_ducklake_table_metadata_cache: dict[tuple[str, str], tuple[int | None, tuple[int, str | None, str | None, dict]]] = {}
_ducklake_table_metadata_cache_lock = threading.Lock()
_ducklake_session_local = threading.local()

from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("storage._ducklake_table_metadata_cache", _ducklake_table_metadata_cache)


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


def _qualified_lake_ident(lake_table: str) -> str:
    return 'lake."{}"'.format(lake_table.replace('"', '""'))


def _scan_ducklake_table_metadata(con, lake_table: str) -> tuple[int, str | None, str | None, dict]:
    """Full scan of ``lake.<lake_table>`` for row count, min/max timestamp,
    and a per-day row-count calendar.

    DuckLake exposes no manifest/partition-value API at the SQL level the
    way the pre-v3 table manifests did, and small commits get "inlined" into the
    catalog metadata rather than written as parquet files — so there are no
    reliable per-file stats to read either way (see
    ``backend/core/storage/_ducklake.py`` module docstring). A direct scan
    is the only approach that stays correct in both cases. Callers should
    go through ``_get_cached_or_scan_ducklake_metadata`` rather than call
    this directly — it's O(table size) and this codebase treats "MANY
    small files, cheap incremental reads" as the standing perf contract.
    """
    ident = _qualified_lake_ident(lake_table)
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
# generated view SQL beyond the schema fields: ``time_range`` and
# ``provisioning.cron_sync.enabled``. The cron's manual-import path mutates
# ``time_range`` temporarily on the in-memory source dict — without the
# fingerprint, a concurrent dashboard call could read back the cached
# SQL from a moment when the WHERE clause had a different time bound.
# Cheap defense-in-depth: cache miss on any drift, cache hit when the
# variant is stable. See ``_source_variant_fp`` below.
_view_cache: dict[str, tuple] = {}


def _source_variant_fp(source: dict) -> tuple:
    """Hashable fingerprint of the per-source inputs that materially
    affect the generated view SQL beyond the schema fields.

    Finding 002 (2026-06-15): ``_update_storage_view_locked`` appends a
    WHERE clause derived from ``source["time_range"]``. Without it in the
    ``_view_cache`` key tuple, a cached SQL string from one variant
    could be served back to a request that wrote a different time
    range — silently applying the wrong WHERE clause to the result
    set. Cheap defense in depth: include it in the key.

    ``provisioning.cron_sync.enabled`` is also folded in because the
    same WHERE-clause branch fires when cron sync is disabled.
    """
    tr = source.get("time_range")
    if isinstance(tr, dict):
        tr_fp: tuple = (tr.get("start"), tr.get("end"))
    else:
        tr_fp = ()
    cron_enabled = bool(((source.get("provisioning") or {}).get("cron_sync") or {}).get("enabled", True))
    return (tr_fp, cron_enabled)


# Per-source rebuild signal: source_key -> Event set when an in-progress
# slow-path rebuild finishes. Lets cold parallel waiters wake and use
# fast-path-without-lock instead of stepping through the lock serially.
_rebuild_signals: dict[str, threading.Event] = {}
_rebuild_signals_lock = threading.Lock()


def is_stale_view_error(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like the storage view referencing a
    buffer parquet the commit cycle has already swept.

    Mirror of :func:`backend.repositories._base._is_stale_view_error` —
    promoted here so non-repository call paths (background crons /
    discovery / rollup writers) can share the same detection without
    importing through the repositories layer (would invert the
    architecture). The repositories alias still resolves the same way;
    new sites should import :func:`is_stale_view_error` directly from
    :mod:`backend.core.storage`.
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
    :func:`update_storage_view`, then re-invoke ``fn`` once.

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
        _core_mod.update_storage_view(con, source, force=True, target_table=table_name)

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
                "sql": _compact_sql_for_debug(f"-- DuckDB Storage View Resolution [{mode}] --\n{stats['sql']}"),
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
            logger.info("[storage] view column probe failed for committed part: %s", e)
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
            logger.info("[storage] view column probe failed for buffer parquet %s: %s", buf_probe_path, e)
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
    - the ``time_range`` WHERE clamp for cron-disabled manual imports.
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

    # 3. Manual-import time clamp.
    tr = source.get("time_range")
    if tr and not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True):
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

    # DuckLake-aware staleness token (the old metadata_location
    # pointer never matched the slow path's cached
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
            logger.warning("[storage] Incremental fast-path reconstruction failed: %s", e)
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
                logger.warning("[storage] fast-path view re-bind failed for %s: %s", source_key, e)
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
            _core_mod._update_storage_view_locked(con, source, target_table=target_table, force=force)
        except TypeError:
            _core_mod._update_storage_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(cache_key) is ev:
                del _rebuild_signals[cache_key]


def update_storage_view(
    con, source: dict, lock_timeout: float = 5.0, force: bool = False, target_table: str = "logs"
) -> None:
    """Refresh the per-service DuckDB view over the DuckLake table + buffer.

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
        update_storage_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_vitals")
        update_storage_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_errors")
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
                    logger.debug("update_storage_view: con.execute failed (ignored)", exc_info=True)
                return
            if not force and _persistent_view_exists(con, source, target_table=target_table):
                return
            logger.info(
                "[storage] %s: cache empty and no persistent view for %s; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
                target_table,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[storage] %s: extended 60s lock wait timed out; view %s rebuild deferred",
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
    """Return True if the per-service storage view already exists on this
    connection's database. Used by ``update_storage_view`` to skip the
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


def _update_storage_view_locked(con, source: dict, target_table: str = "logs", force: bool = False) -> None:

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
        logger.error("[storage] duckdb_databases probe failed: %s", e)

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
            logger.debug("_update_storage_view_locked: drop temp view %s failed (ignored)", view_name, exc_info=True)
        create_stmt = f"CREATE OR REPLACE VIEW {view_name} AS {final_sql}"

    try:
        con.execute(create_stmt)
    except Exception as e:
        if "write-write conflict" in str(e).lower():
            try:
                con.rollback()
            except Exception:
                logger.debug("_update_storage_view_locked: rollback failed (ignored)", exc_info=True)
            try:
                con.execute(create_stmt)
            except Exception:
                try:
                    con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}")
                except Exception as fallback_err:
                    logger.error("[storage] Failed to create view %s: %s", view_name, fallback_err)
                    if durable_serving:
                        raise
        else:
            logger.error("[storage] Failed to create view %s: %s", view_name, e)
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
        logger.debug("_update_storage_view_locked: _set_conn_state failed (ignored)", exc_info=True)


# ---------------------------------------------------------------------------
# Admin / UI metadata

# A-3 (CacheRegistry): register every storage cache so the test
# harness can drain them centrally via CacheRegistry.clear_all().
_CacheRegistry.register("storage._view_cache", _view_cache)

manifest = sys.modules[__name__]
view = sys.modules[__name__]
sys.modules["backend.core.storage.manifest"] = sys.modules[__name__]
sys.modules["backend.core.storage.view"] = sys.modules[__name__]

# ── Local tiered Parquet compaction (folded from local_compaction.py) ──

import contextlib
import logging
import os
import shutil
import uuid
from collections import defaultdict

from backend.core.duckdb import get_memory_connection
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.date_utils import parse_day_key_strict

# Don't merge a partition whose total parquet size already exceeds this — we'd
# just produce one absurdly huge file that destroys query parallelism on the
# next scan. Tune via env var if a hot site needs different ergonomics.
_MAX_PARTITION_BYTES = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256")) * 1024 * 1024

# Partitions older than this become eligible for cross-hour daily compaction.
# Recent hours stay hourly so the dashboard's time-range pruning stays tight
# at the file level (each scan opens one daily file vs 24 hourly files).
_DAILY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_DAILY_TIER_DAYS", "1"))

# Daily files older than this become eligible for cross-day weekly compaction.
# Only useful when log_retention_days > this; otherwise daily files just age
# out of the cache before any weekly rollup could happen. Default 30 matches
# the common retention window — a no-op for shorter retentions.
_WEEKLY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_WEEKLY_TIER_DAYS", "30"))

# Directories under cache/<bucket>/data/ that hold cross-hour merged files.
# The dashboard's view-glob is data/**/*.parquet so sibling dirs are fine.
_DAILY_DIR = "daily"
_WEEKLY_DIR = "weekly"

_HOUR_PART_RE = re.compile(r"^timestamp_hour=(\d{4}-\d{2}-\d{2})-(\d{2})$")
# Daily file naming from _compact_daily_tier: daily_YYYY-MM-DD_<hex>.parquet
_DAILY_FILE_RE = re.compile(r"^daily_(\d{4}-\d{2}-\d{2})_[0-9a-f]+\.parquet$")


def _detect_natural_keys(col_names: set[str]) -> tuple[str, ...] | None:
    """Detect if the given parquet columns match known RUM composite natural keys."""
    for natural_keys in RUM_NATURAL_KEYS.values():
        if set(natural_keys).issubset(col_names):
            return natural_keys
    return None


def _build_merge_select_sql(
    paths_sql: str,
    cols_to_strip: list[str],
    has_rid: bool = False,
    natural_keys: tuple[str, ...] | list[str] | None = None,
) -> str:
    """SELECT clause for the COPY that produces a merged parquet.

    When the schema has a ``rid`` column, dedupe by ``rid`` keeping the
    earliest-timestamp occurrence. Without this, the buffer-commit ↔
    tombstone window (``buffer.py:463-477`` — table.append succeeded but
    tombstone_buffer_files crashed before running) causes the same buffer
    file's rows to be committed twice on the retry tick → every row in
    that batch counted twice in every dashboard query. The 2026-06-12
    audit found ~12 days of ~2× duplication from exactly this race.

    ``rid`` is Fastly's per-request id and is unique per logical request,
    so it's the right key. NULL-rid rows pass through unchanged — the row
    is preserved without a uniqueness guarantee. In practice prod data
    has zero NULL rids (verified 2026-06-12), so this branch is defensive.

    When ``natural_keys`` is supplied (e.g. for RUM tables ``client_vitals``
    and ``client_errors``), dedupe by the composite natural key using
    ``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY timestamp)``.

    When the schema has no ``rid`` column and no ``natural_keys`` (older
    sources, test fixtures), fall through to a plain SELECT — no dedup,
    original behaviour.
    """
    exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
    if has_rid:
        # Add _dup_rn to the EXCLUDE list so the helper column doesn't bleed
        # into the output parquet schema.
        inner_exclude_clause = f" EXCLUDE ({', '.join([*cols_to_strip, '_dup_rn'])})"
        return (
            # Non-NULL rid: dedupe, keep earliest occurrence.
            f"SELECT *{inner_exclude_clause} FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY timestamp) AS _dup_rn"
            f"  FROM read_parquet([{paths_sql}], union_by_name=true)"
            f"  WHERE rid IS NOT NULL"
            f") WHERE _dup_rn = 1"
            f" UNION ALL BY NAME "
            # NULL rid: pass through.
            f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"
            f" WHERE rid IS NULL"
        )
    if natural_keys:
        inner_exclude_clause = f" EXCLUDE ({', '.join([*cols_to_strip, '_dup_rn'])})"
        partition_expr = ", ".join(f'"{k}"' for k in natural_keys)
        return (
            f"SELECT *{inner_exclude_clause} FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_expr} ORDER BY timestamp) AS _dup_rn"
            f"  FROM read_parquet([{paths_sql}], union_by_name=true)"
            f") WHERE _dup_rn = 1"
        )
    return f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"


def _bin_pack_files(file_paths: list[str], max_bin_size_bytes: int) -> list[list[str]]:
    """Group file_paths into bins such that the sum of file sizes in each bin
    does not exceed max_bin_size_bytes. Preserves the original file order.
    If any single file exceeds max_bin_size_bytes, it goes in its own bin.
    """
    bins: list[list[str]] = []
    current_bin: list[str] = []
    current_size = 0

    for path in file_paths:
        try:
            file_size = os.path.getsize(path)
        except OSError:
            continue

        if current_size + file_size > max_bin_size_bytes:
            if current_bin:
                bins.append(current_bin)
                current_bin = []
                current_size = 0

            if file_size >= max_bin_size_bytes:
                bins.append([path])
            else:
                current_bin.append(path)
                current_size = file_size
        else:
            current_bin.append(path)
            current_size += file_size

    if current_bin:
        bins.append(current_bin)

    return bins


def compact_local_partitions(
    source: dict,
    min_files_per_partition: int = 1,
    dry_run: bool = False,
    table_name: str = "logs",
    compact_active_hour: bool = False,
) -> dict[str, Any]:
    """Merge small parquet files within each hour-partition directory into
    a single larger file. Additionally rolls partitions older than
    ``_DAILY_TIER_AGE_DAYS`` into per-day merged files.

    Args:
        source: service source dict (used to resolve cache path)
        min_files_per_partition: only partitions with strictly more than
            this many files are touched. Default 1 — every multi-file
            partition is eligible. This is required for the dedup-on-merge
            pass (see ``_build_merge_select_sql``) to clean up the
            orphan-file dup pattern: a partition with exactly 3 files (one
            ``compacted_*`` + a 2-split ``00000-N-*`` orphan pair from a
            buffer-commit replay) needs ``> 1`` to be considered, not the
            previous ``> 3``. Without this, the historic 12 days of ~2×
            duplication would never self-heal.
        dry_run: if True, report what would be done without writing.
        table_name: table name identifier ("logs", "client_vitals", "client_errors")

    Returns:
        Result dict — see implementation for fields.
    """
    from backend.core.duckdb import _cache_dir

    t0 = time.time()
    cache_root = _cache_dir(source)
    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    result: dict[str, Any] = {
        "partitions_scanned": 0,
        "partitions_compacted": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "daily_rollups": 0,
        "weekly_rollups": 0,
        "active_hour_skipped": False,
        "stale_tmp_removed": 0,
        "errors": [],
        "duration_ms": 0,
        "dry_run": dry_run,
    }

    if not os.path.isdir(data_dir):
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    # Acquire the per-service RLock around the file-system mutation
    # phase so concurrent dashboard queries via the view-build path
    # don't race with our delete-then-rename and hit FileNotFoundError /
    # IO Error mid-glob. Architecture-review Finding #3.
    from backend.core.storage._core import _get_service_lock

    service_key = source.get("name", "default")
    publish_lock = _get_service_lock(service_key)

    # Accumulate every basename we delete across all three tiers so we
    # can register them in one SQLite write at the end (vs N small writes).
    removed_basenames: list[str] = []

    # ── Cleanup pass: remove orphaned .tmp_ files from previous crashed
    # runs. Safe because the publish step renames .tmp_<name> → <name>;
    # any leftover .tmp_ is by definition incomplete. The dashboard glob
    # matches *.parquet so leftovers don't pollute queries, but they
    # do waste disk and confuse the file-count metric.
    if not dry_run:
        with publish_lock:
            result["stale_tmp_removed"] = _cleanup_stale_tmp(data_dir)
            result["watermark_evicted"] = _enforce_disk_watermark(data_dir, removed_basenames=removed_basenames)

    # ── Active-hour guard: do NOT compact the current UTC hour (unless compact_active_hour is True). The sync
    # cron may be flushing buffer files into this partition mid-pass; our
    # delete-then-rename is atomic per-file but a half-second window after
    # we listdir() and before we delete is enough for a freshly-arrived
    # file to be in the listing of one operation and gone from the other.
    # Skipping the active hour is cheap and removes the race entirely.
    active_hour = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    result["active_hour_skipped"] = (
        os.path.isdir(os.path.join(data_dir, active_hour)) if not compact_active_hour else False
    )

    # ── Hourly tier: walk each partition dir, merge if multi-file.
    for entry in sorted(os.listdir(data_dir)):
        if entry == active_hour and not compact_active_hour:
            continue
        if entry in (_DAILY_DIR, _WEEKLY_DIR):
            continue  # daily/weekly rollup dirs handled in subsequent passes
        part_dir = os.path.join(data_dir, entry)
        if not os.path.isdir(part_dir):
            continue
        parquets = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
        if len(parquets) <= min_files_per_partition:
            continue

        # Sort files alphabetically for deterministic sequential binning
        parquets_sorted = sorted(parquets)
        full_paths = [os.path.join(part_dir, f) for f in parquets_sorted]
        bins = _bin_pack_files(full_paths, _MAX_PARTITION_BYTES)

        # In normal compaction (``min_files_per_partition >= 1``) a single-
        # file bin is a no-op — there's nothing to merge. In force-rewrite
        # mode (``== 0``, one-shot dedup pass) we DO want to rewrite even
        # singletons so intra-file dups in long-stable partitions get the
        # dedup-by-rid pass.
        if min_files_per_partition == 0:
            eligible_bins = bins
        else:
            eligible_bins = [b for b in bins if len(b) > 1]
        if not eligible_bins:
            continue

        result["partitions_scanned"] += 1
        partition_compacted = False

        for bin_paths in eligible_bins:
            bin_basenames = [os.path.basename(p) for p in bin_paths]
            try:
                # Lock held only during the actual file-system mutation (delete +
                # rename) inside _compact_single_partition; the parquet COPY
                # write happens before that on an in-memory DuckDB connection and
                # doesn't need the lock. Holding the lock during the COPY would
                # block dashboard reads for ~1s per partition.
                r = _compact_single_partition(part_dir, bin_basenames, dry_run=dry_run, publish_lock=publish_lock)
                if r.get("skipped_enospc"):
                    result["errors"].append(f"{part_dir}: ENOSPC pre-check failed (< 2x needed free disk)")
                    continue
                partition_compacted = True
                result["files_merged"] += r["files_merged"]
                result["files_removed"] += r["files_removed"]
                result["bytes_before"] += r["bytes_before"]
                result["bytes_after"] += r["bytes_after"]
                removed_basenames.extend(r.get("removed_basenames", []))
            except Exception as e:
                msg = f"{part_dir} (bin): {type(e).__name__}: {e}"
                logger.warning("[local-compact] %s", msg)
                result["errors"].append(msg)

        if partition_compacted:
            result["partitions_compacted"] += 1

    # ── Daily tier: roll up hour-partitions older than threshold into one
    # daily file. After this, the partition's hour dirs are removed.
    try:
        # Same RLock as the hourly path — file-system mutation phase only.
        r = _compact_daily_tier(data_dir, dry_run=dry_run, publish_lock=publish_lock)
        result["daily_rollups"] = r["daily_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"daily-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    # ── Weekly tier: roll up daily files older than threshold into one
    # weekly file. Only does work when local retention extends past
    # _WEEKLY_TIER_AGE_DAYS (default 30); for shorter retentions the
    # daily files age out before becoming weekly-eligible.
    try:
        r = _compact_weekly_tier(data_dir, dry_run=dry_run, publish_lock=publish_lock)
        result["weekly_rollups"] = r["weekly_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"weekly-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    result["duration_ms"] = int((time.time() - t0) * 1000)
    logger.info(
        "🧹 [local-compact] %s: hourly=%d/%d merged=%d removed=%d daily=%d weekly=%d tmp_cleaned=%d in %dms",
        source.get("name"),
        result["partitions_compacted"],
        result["partitions_scanned"],
        result["files_merged"],
        result["files_removed"],
        result["daily_rollups"],
        result["weekly_rollups"],
        result["stale_tmp_removed"],
        result["duration_ms"],
    )
    return result


def _enforce_disk_watermark(
    data_dir: str,
    threshold_pct: float = 0.85,
    target_pct: float = 0.70,
    *,
    removed_basenames: list[str] | None = None,
) -> int:
    """Check disk pressure and aggressively evict oldest local parquet if exceeded.

    Prevents high-volume services from exhausting the shared 64TB PVC by automatically
    shrinking their local query cache. Ensures we gracefully degrade the local drill-down
    window rather than crashing the pod. Rollups and S3 durable data are unaffected.
    """
    import shutil

    try:
        usage = shutil.disk_usage(data_dir)
    except OSError:
        return 0

    current_pct = usage.used / max(usage.total, 1)
    if current_pct <= threshold_pct:
        return 0

    target_used_bytes = usage.total * target_pct
    bytes_to_free = usage.used - target_used_bytes

    if bytes_to_free <= 0:
        return 0

    logger.warning(
        "Disk pressure hit %.1f%% (threshold %.1f%%). Evicting oldest cache to reach %.1f%%...",
        current_pct * 100,
        threshold_pct * 100,
        target_pct * 100,
    )

    files_with_mtime = []
    for root, _, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet"):
                path = os.path.join(root, f)
                try:
                    stat = os.stat(path)
                    files_with_mtime.append((stat.st_mtime, stat.st_size, path))
                except OSError:
                    pass

    # Sort oldest first
    files_with_mtime.sort(key=lambda x: x[0])

    freed = 0
    removed_count = 0
    for mtime, size, path in files_with_mtime:
        if freed >= bytes_to_free:
            break
        try:
            os.remove(path)
            freed += size
            removed_count += 1
            if removed_basenames is not None:
                removed_basenames.append(os.path.basename(path))
        except OSError:
            pass

    if removed_count > 0:
        logger.warning(
            "Evicted %d old cache files (%.2f MB) to relieve disk pressure.", removed_count, freed / (1024 * 1024)
        )

    return removed_count


def _cleanup_stale_tmp(data_dir: str) -> int:
    """Walk data_dir and rm any *.parquet.tmp left over from crashed runs.

    Also cleans up the OLD naming convention (.tmp_*.parquet) from the
    earlier version of this module so a deploy doesn't leave orphans
    that the parquet glob picks up (causing view-build errors).
    """
    n = 0
    for root, _, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet.tmp") or (f.startswith(".tmp_") and f.endswith(".parquet")):
                try:
                    os.remove(os.path.join(root, f))
                    n += 1
                except OSError:
                    pass
    return n


def _rollup_bins(
    bins: list[list[str]],
    out_root: str,
    name_prefix: str,
    token: str,
    rollup_key: str,
    result: dict[str, Any],
    *,
    dry_run: bool,
    publish_lock: Any = None,
    log_migrate,
    log_migrate_fail,
    log_merge,
    log_merge_fail,
) -> None:
    """Roll up each pre-packed bin into one output parquet under ``out_root``.

    Shared inner loop of the daily and weekly tiers. A single-file bin is
    migrated via ``os.rename``; a multi-file bin is merged via a DuckDB COPY
    into a tmp file then atomically renamed, with originals deleted into
    ``result['removed_basenames']`` and the tmp cleaned on failure. ``token``
    is the per-group date key embedded in the output filename (day_str /
    week_key); ``rollup_key`` is the result counter to bump (daily_rollups /
    weekly_rollups). The four log callables let each tier keep its exact log
    strings (emoji + wording) unchanged.
    """
    lock_ctx = publish_lock if publish_lock is not None else contextlib.nullcontext()
    for bin_paths in bins:
        bytes_before = sum(os.path.getsize(p) for p in bin_paths)
        if dry_run:
            result[rollup_key] += 1
            result["files_merged"] += len(bin_paths)
            result["bytes_before"] += bytes_before
            continue

        # Pre-check available disk space before merge (Section 7)
        try:
            free_bytes = shutil.disk_usage(out_root).free
            if free_bytes < 2 * bytes_before:
                logger.warning(
                    "[local-compact] Insufficient disk space in %s for %s: %d bytes free < 2x needed (%d bytes). Skipping.",
                    out_root,
                    rollup_key,
                    free_bytes,
                    2 * bytes_before,
                )
                continue
        except OSError:
            pass

        if len(bin_paths) == 1:
            # Migrate single-file bin to the tier folder to retire the source.
            old_path = bin_paths[0]
            old_name = os.path.basename(old_path)
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            out_path = os.path.join(out_root, out_name)
            try:
                with lock_ctx:
                    os.rename(old_path, out_path)
                result["files_removed"] += 1
                result.setdefault("removed_basenames", []).append(old_name)
                result[rollup_key] += 1
                result["files_merged"] += 1
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_before
                log_migrate(old_name, out_name)
            except Exception as e:
                log_migrate_fail(old_path, e)
        else:
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            tmp_path = os.path.join(out_root, f"{out_name}.tmp")
            out_path = os.path.join(out_root, out_name)
            try:
                con = get_memory_connection()
                try:
                    paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in bin_paths)
                    probe = (
                        con.execute(
                            f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0"
                        ).description
                        or []
                    )
                    col_names = {d[0] for d in probe}
                    cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
                    has_rid = "rid" in col_names
                    natural_keys = None if has_rid else _detect_natural_keys(col_names)
                    select_sql = _build_merge_select_sql(
                        paths_sql, cols_to_strip, has_rid=has_rid, natural_keys=natural_keys
                    )
                    order_by = "ORDER BY timestamp"
                    if "ip" in col_names:
                        order_by += ", ip"
                    con.execute(
                        f"COPY ({select_sql} {order_by}) "
                        f"TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                finally:
                    con.close()

                # Atomic publish: rename tmp_path -> out_path FIRST so new compacted file is
                # atomically present on disk before unlinking originals (Section 5 Step 4 & Section 7).
                with lock_ctx:
                    os.rename(tmp_path, out_path)
                    bytes_after = os.path.getsize(out_path)

                    for p in bin_paths:
                        try:
                            os.remove(p)
                            result["files_removed"] += 1
                            result.setdefault("removed_basenames", []).append(os.path.basename(p))
                        except OSError as e:
                            logger.warning("[local-compact] failed to remove %s: %s", p, e)

                result[rollup_key] += 1
                result["files_merged"] += len(bin_paths)
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_after
                log_merge(token, len(bin_paths))
            except Exception as e:
                # Clean the tmp on failure so we don't leak.
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    pass
                log_merge_fail(token, e)


def _compact_daily_tier(data_dir: str, dry_run: bool = False, *, publish_lock: Any = None) -> dict[str, Any]:
    """Group hour-partitions older than _DAILY_TIER_AGE_DAYS by day, merge
    each day's parquets into size-capped daily files under data/daily/, and
    remove the now-empty hour partition dirs.

    Returns {daily_rollups, files_merged, files_removed, bytes_before, bytes_after}.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=_DAILY_TIER_AGE_DAYS)).date()
    daily_root = os.path.join(data_dir, _DAILY_DIR)

    # day_str -> [(hour_part_dir, [parquet_paths])]
    by_day: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    for entry in os.listdir(data_dir):
        m = _HOUR_PART_RE.fullmatch(entry)
        if not m:
            continue
        day_str = m.group(1)
        try:
            day = parse_day_key_strict(day_str).date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        part_dir = os.path.join(data_dir, entry)
        parquets = [
            os.path.join(part_dir, f)
            for f in os.listdir(part_dir)
            if f.endswith(".parquet") and not f.startswith(".tmp_")
        ]
        if not parquets:
            continue
        by_day[day_str].append((part_dir, parquets))

    result: dict[str, Any] = {
        "daily_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not by_day:
        return result

    if not dry_run:
        os.makedirs(daily_root, exist_ok=True)

    for day_str, parts in by_day.items():
        all_paths: list[str] = []
        for _, paths in parts:
            all_paths.extend(paths)

        # Sort files alphabetically/chronologically for deterministic sequential binning
        all_paths = sorted(all_paths)
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            daily_root,
            "daily_",
            day_str,
            "daily_rollups",
            result,
            dry_run=dry_run,
            publish_lock=publish_lock,
            log_migrate=lambda old, out: logger.info("🚚 [local-compact] migrated single-file bin %s to %s", old, out),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info("📦 [local-compact] daily bin rollup %s: %d files → 1", tok, n),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] daily bin rollup %s failed: %s", tok, e),
        )

        # Try to rmdir the now-empty hour partition dirs.
        if not dry_run:
            for part_dir, _ in parts:
                try:
                    os.rmdir(part_dir)
                except OSError:
                    pass  # dir not empty (concurrent write) — leave it

    return result


def _compact_weekly_tier(data_dir: str, dry_run: bool = False, *, publish_lock: Any = None) -> dict[str, Any]:
    """Group daily files older than _WEEKLY_TIER_AGE_DAYS by ISO week, merge
    each week's parquets into size-capped weekly files under data/weekly/,
    and delete originals.

    Operates on files in data/daily/ produced by _compact_daily_tier. The
    daily filenames embed YYYY-MM-DD (the rollup date), which we parse with
    _DAILY_FILE_RE to derive ISO week → group.

    Returns {weekly_rollups, files_merged, files_removed, bytes_before, bytes_after}.
    """
    daily_root = os.path.join(data_dir, _DAILY_DIR)
    weekly_root = os.path.join(data_dir, _WEEKLY_DIR)
    result: dict[str, Any] = {
        "weekly_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not os.path.isdir(daily_root):
        return result

    from datetime import date as _date

    cutoff = (datetime.now(UTC) - timedelta(days=_WEEKLY_TIER_AGE_DAYS)).date()
    # week_key → [(path, date)]
    by_week: dict[str, list[tuple[str, _date]]] = defaultdict(list)
    for fname in os.listdir(daily_root):
        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
            continue
        m = _DAILY_FILE_RE.fullmatch(fname)
        if not m:
            continue
        try:
            day = parse_day_key_strict(m.group(1)).date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        # ISO week key like "2026-W22". Days in the same calendar week
        # share the key; year-week handles year boundaries (W52→W01).
        iso = day.isocalendar()
        week_key = f"{iso[0]:04d}-W{iso[1]:02d}"
        by_week[week_key].append((os.path.join(daily_root, fname), day))

    if not by_week:
        return result

    if not dry_run:
        os.makedirs(weekly_root, exist_ok=True)

    for week_key, items in by_week.items():
        if len(items) < 2:
            continue  # nothing to merge for a single-day week

        # Sort daily files alphabetically/chronologically for deterministic sequential binning
        items_sorted = sorted(items, key=lambda x: x[0])
        all_paths = [p for p, _ in items_sorted]
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            weekly_root,
            "weekly_",
            week_key,
            "weekly_rollups",
            result,
            dry_run=dry_run,
            publish_lock=publish_lock,
            log_migrate=lambda old, out: logger.info(
                "🚚 [local-compact] migrated single-file weekly bin %s to %s", old, out
            ),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file weekly bin %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info(
                "🗓️  [local-compact] weekly bin rollup %s: %d daily file(s) → 1", tok, n
            ),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] weekly bin rollup %s failed: %s", tok, e),
        )

    return result


def _compact_single_partition(
    part_dir: str, parquets: list[str], dry_run: bool = False, *, publish_lock: Any = None
) -> dict[str, Any]:
    """Merge `parquets` (relative names) in `part_dir` into one new parquet.

    Uses DuckDB COPY to read+write since it's already in the dep tree and
    handles the union-by-name semantics the view uses.
    """
    paths = [os.path.join(part_dir, p) for p in parquets]
    bytes_before = sum(os.path.getsize(p) for p in paths)

    if dry_run:
        return {
            "files_merged": len(parquets),
            "files_removed": 0,
            "bytes_before": bytes_before,
            "bytes_after": 0,
        }

    # Pre-check available disk space before merge (Section 7): abort if free space < 2x target bin size
    try:
        free_bytes = shutil.disk_usage(part_dir).free
        if free_bytes < 2 * bytes_before:
            logger.warning(
                "[local-compact] Insufficient disk space in %s: %d bytes free < 2x needed (%d bytes). Skipping.",
                part_dir,
                free_bytes,
                2 * bytes_before,
            )
            return {
                "files_merged": 0,
                "files_removed": 0,
                "removed_basenames": [],
                "bytes_before": bytes_before,
                "bytes_after": 0,
                "skipped_enospc": True,
            }
    except OSError:
        pass

    # Write to a temp file in the same directory so the atomic rename
    # below stays within one filesystem (rename across filesystems is
    # NOT atomic on POSIX).
    out_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
    tmp_name = f"{out_name}.tmp"
    tmp_path = os.path.join(part_dir, tmp_name)
    out_path = os.path.join(part_dir, out_name)

    # Use in-memory DuckDB so we don't contend with the per-service writer
    # lock. read_parquet with explicit list + union_by_name matches the
    # view's semantics so the resulting file is query-compatible.
    con = get_memory_connection()
    try:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)
        # Strip the computed `timestamp_hour` / `dt` columns from output
        # if they exist in input files. The storage view-build re-adds them
        # via `SELECT *, strftime(...) AS timestamp_hour` and would error
        # with a duplicate-column UNION ALL BY NAME on a merged file
        # that already contained them.
        probe = con.execute(f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0").description or []
        col_names = {d[0] for d in probe}
        cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
        has_rid = "rid" in col_names
        natural_keys = None if has_rid else _detect_natural_keys(col_names)
        select_sql = _build_merge_select_sql(paths_sql, cols_to_strip, has_rid=has_rid, natural_keys=natural_keys)
        order_by = "ORDER BY timestamp"
        if "ip" in col_names:
            order_by += ", ip"
        # zstd compression matches Fastly's parquet output and the
        # buffer-commit writer; keeps decompression cost stable.
        con.execute(
            f"COPY ({select_sql} {order_by}) TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()

    # Atomic publish: rename tmp_path -> out_path FIRST so new compacted file is
    # atomically present on disk before unlinking originals (Section 5 Step 4 & Section 7).
    # Original source files remain untouched until atomic rename succeeds.
    lock_ctx = publish_lock if publish_lock is not None else contextlib.nullcontext()
    with lock_ctx:
        try:
            os.rename(tmp_path, out_path)
            bytes_after = os.path.getsize(out_path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise

        files_removed = 0
        removed_basenames: list[str] = []
        for p in paths:
            try:
                os.remove(p)
                files_removed += 1
                removed_basenames.append(os.path.basename(p))
            except OSError as e:
                logger.warning("[local-compact] failed to remove %s: %s", p, e)

    return {
        "files_merged": len(parquets),
        "files_removed": files_removed,
        "removed_basenames": removed_basenames,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
    }


# Short-TTL memo for compaction_stats so the 5 s health-snapshot poll
# in the admin UI doesn't redo a fan-out os.listdir per service per
# tick. The cron's actual local-compact runs every 2 min, so even a
# 5 s lag is well inside the staleness budget the dashboard already
# tolerates.
_COMPACTION_STATS_TTL = 5.0
_COMPACTION_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_COMPACTION_STATS_TTL)


def compaction_stats(source: dict, table_name: str = "logs") -> dict[str, Any]:
    """Snapshot of file-count distribution across local cache partitions.

    Returns counts that downstream metrics / health endpoints can graph
    to spot small-file regressions (e.g., if the cron stops running and
    files start accumulating, ``partitions_above_threshold`` climbs).

    Results are memoised per cache_root + table_name for ``_COMPACTION_STATS_TTL`` s
    so the admin health-snapshot poll doesn't re-walk the data dir on
    every tick.
    """
    from backend.core.duckdb import _cache_dir

    cache_root = _cache_dir(source)
    cache_key = f"{cache_root}:{table_name}"
    cached = _COMPACTION_STATS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    total_files = 0
    partitions = 0
    above_3 = 0
    above_10 = 0
    daily_files = 0
    weekly_files = 0
    if not os.path.isdir(data_dir):
        result: dict[str, Any] = {
            "total_files": 0,
            "partitions": 0,
            "partitions_above_3": 0,
            "partitions_above_10": 0,
            "daily_files": 0,
            "weekly_files": 0,
            "avg_files_per_partition": 0.0,
        }
        _COMPACTION_STATS_CACHE[cache_key] = result
        return result
    for entry in os.listdir(data_dir):
        full = os.path.join(data_dir, entry)
        if not os.path.isdir(full):
            continue
        n = sum(1 for f in os.listdir(full) if f.endswith(".parquet") and not f.startswith(".tmp_"))
        if entry == _DAILY_DIR:
            daily_files += n
        elif entry == _WEEKLY_DIR:
            weekly_files += n
        else:
            partitions += 1
            total_files += n
            if n > 3:
                above_3 += 1
            if n > 10:
                above_10 += 1
    result = {
        "total_files": total_files + daily_files + weekly_files,
        "partitions": partitions,
        "partitions_above_3": above_3,
        "partitions_above_10": above_10,
        "daily_files": daily_files,
        "weekly_files": weekly_files,
        "avg_files_per_partition": (total_files / partitions) if partitions else 0.0,
    }
    _COMPACTION_STATS_CACHE[cache_key] = result
    return result


# R-1: drain the per-service compaction stats TTL cache between tests.
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("core.local_compaction._COMPACTION_STATS_CACHE", _COMPACTION_STATS_CACHE)

buffer = sys.modules[__name__]
local_compaction = sys.modules[__name__]
for _mod_name in (
    "backend.core.storage.manifest",
    "backend.core.storage.view",
    "backend.core.storage.buffer",
    "backend.core.storage._ducklake",
    "backend.core.local_compaction",
):
    sys.modules[_mod_name] = sys.modules[__name__]
if "backend.core.storage" in sys.modules:
    _ice_pkg = sys.modules["backend.core.storage"]
    for _attr in ("manifest", "view", "buffer", "_ducklake"):
        setattr(_ice_pkg, _attr, sys.modules[__name__])
if "backend.core" in sys.modules:
    setattr(sys.modules["backend.core"], "local_compaction", sys.modules[__name__])
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    for _mod_name in (
        "backend.core.storage.manifest",
        "backend.core.storage.view",
        "backend.core.storage.buffer",
        "backend.core.storage._ducklake",
        "backend.core.local_compaction",
    ):
        sys.modules["backend"]._MODULE_ALIASES[_mod_name] = __name__
