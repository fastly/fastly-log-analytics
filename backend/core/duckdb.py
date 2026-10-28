"""DuckDB analytical connection management, httpfs/FOS proxy configuration, and RUM connection helpers.

Also re-exports symbols from:
- ``backend.core.duckdb_status``
- ``backend.core.sql_validator``
- ``backend.core.reset``
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import re
import secrets
import socket
import sys
import threading
import time
import types
import weakref
from datetime import UTC, datetime
from typing import Any

import boto3
import duckdb
import tenacity
from botocore import UNSIGNED
from botocore.config import Config

from backend import config as svcconfig
from backend.core import (
    duckdb_status as _duckdb_status_mod,
)
from backend.core import (
    reset as _reset_mod,
)
from backend.core import (
    sql_validator as _sql_validator_mod,
)
from backend.core.duckdb_status import (  # noqa: F401
    _SCHEMA_CACHE_TTL,
    _STATUS_BUSY_WINDOW_MINS,
    _TASK_TO_CRON_KEY,
    _TOP_VALUE_FIELDS,
    ASN_CACHE_TTL_DAYS,
    _authoritative_direct_stats,
    _clear_schema_cache,
    _data_stats_cache,
    _data_stats_cache_lock,
    _data_stats_fingerprint,
    _ducklake_stats_cache,
    _ducklake_stats_cache_lock,
    _DucklakeStatsCacheAdapter,
    _query_buffer_stats,
    _query_or_cache_lake_stats,
    _schema_cache,
    _top_values_cache,
    _top_values_cache_lock,
    _top_values_fingerprint,
    backfill_fastly_edge_writes,
    clear_ducklake_stats_cache,
    enrich_asn_labels,
    finalize_cron_run_if_running,
    format_asn_label,
    get_asn_names,
    get_ingested_files,
    get_schema,
    get_sync_status,
    log_cron_run,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    refresh_config_status,
    start_cron_run,
    update_cron_duration,
    update_top_values,
)
from backend.core.reset import (  # noqa: F401
    _DRAIN_GRACE_S,
    _DRAIN_TIMEOUT_S,
    _LOCK_TIMEOUT_S,
    _RUM_CH_TABLES,
    _RUM_TABLES,
    _drain_and_wipe_db_files,
    _pause_rum_flags,
    _purge_clickhouse_tables_gen,
    _purge_dropped_catalog_rows,
    _purge_ducklake_remote_dirs,
    _purge_prefix,
    _reinit_ducklake_tables,
    _remove_tree_with_progress,
    _restore_rum_flags,
    _wipe_ducklake_table_dirs,
    drop_service_ducklake_state,
    reset_service_logs,
    reset_service_rum,
)
from backend.core.sql_validator import (  # noqa: F401
    _ALLOWED_TABLE_FUNCTIONS,
    _BASELINE_TABLE_FUNCTIONS,
    _BLOCKED_FUNCTION_PREFIXES,
    _BLOCKED_FUNCTIONS,
    _BLOCKED_SCHEMA_NAMES,
    _BLOCKED_TABLE_PREFIXES,
    _TABLE_FUNCTIONS,
    ALLOWED_CATALOG,
    ALLOWED_STATEMENT_TYPES,
    MAX_INPUT_BYTES,
    SAFE_IDENT_RE,
    SQLValidationError,
    ValidationResult,
    _audit_logger,
    _check_function_node,
    _check_table_reference,
    _enumerate_table_functions,
    _parse_single_select_node,
    _reject,
    _walk_and_validate,
    apply_user_query_limits,
    escape_sql_literal,
    is_safe_ident,
    is_simple_select_statement,
    require_safe_ident,
    validate_user_sql,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.telemetry import HyperLogLog as HyperLogLog
from backend.utils.telemetry import retry_call

logger = logging.getLogger(__name__)

_db_main = sys.modules[__name__]
_duckdb_status = _duckdb_status_mod
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_POOL_CONN_MEMORY_LIMIT = os.getenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "")
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS", "")
INGEST_CHUNK_SIZE = int(os.getenv("INGEST_CHUNK_SIZE", "500"))
DUCKDB_PATH = "logs.duckdb"
_cached_n_threads: int | None = None
_cached_mem_limit_gb: int | None = None
_dma_map_cache: dict[str, str] | None = None
_STALE_VIEW_ERR_MARKERS = (
    "No files found",
    "Catalog Error: Table with name",
    "does not exist",
    "No such file or directory",
)


def _is_stale_view_error(err: object) -> bool:
    msg = str(err)
    return any(m in msg for m in _STALE_VIEW_ERR_MARKERS)


def _source_service_id(source: dict | None) -> str:
    return str(source.get("name") or source.get("service_id") or "") if source else ""


def _exec_quietly(con: duckdb.DuckDBPyConnection, sql: str, msg: str = "con.execute failed (ignored)") -> None:
    try:
        con.execute(sql)
    except Exception:
        logger.debug(msg, exc_info=True)


def _get_dma_map() -> dict[str, str]:
    global _dma_map_cache
    if _dma_map_cache is not None:
        return _dma_map_cache
    _dma_map_cache = {}
    for fname in ("data/system/dma_geojson.json", "data/system/dma.json"):
        if not os.path.exists(fname):
            continue
        try:
            with open(fname) as f:
                data = json.load(f)
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                code, name = props.get("dma_code"), props.get("name") or props.get("dma_name") or props.get("dma1")
                if code is not None and name:
                    _dma_map_cache[str(int(code))] = name
            break
        except Exception:
            logger.warning("Could not load %s", fname, exc_info=True)
    return _dma_map_cache


def _source_from_config(cfg: dict) -> dict:
    return svcconfig.config_to_source(cfg)


def _build_default_source() -> dict:
    cfgs = svcconfig.list_configs()
    if cfgs:
        return _source_from_config(cfgs[0])
    return {
        "name": "default",
        "endpoint": "",
        "access_key_id": "",
        "secret_access_key": "",
        "bucket": "",
        "prefix": "",
        "region": "us-east-1",
        "cdn_url": "",
        "cdn_secret": "",
        "cdn_service_id": "",
        "logging_service_id": "",
        "duckdb_path": "logs.duckdb",
        "storage_mode": "cloud",
    }


_DEFAULT_SOURCE = _build_default_source()
DUCKDB_PATH = _DEFAULT_SOURCE.get("duckdb_path") or DUCKDB_PATH


def get_source_for_service(service_id: str) -> dict | None:
    if (cfg := svcconfig.load_config(service_id)) is not None:
        return _source_from_config(cfg)
    return next(
        (_source_from_config(c) for c in svcconfig.list_configs() if c.get("cdn_service_id") == service_id), None
    )


def reload_default_source():
    global DUCKDB_PATH
    _DEFAULT_SOURCE.update(_build_default_source())
    DUCKDB_PATH = _DEFAULT_SOURCE.get("duckdb_path", "logs.duckdb")


def is_configured(source: dict | None = None) -> bool:
    s = source or _DEFAULT_SOURCE
    return all(s.get(k) for k in ("endpoint", "access_key_id", "secret_access_key", "bucket"))


def _safe_table_name(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()
    return f"logs_{clean}" if clean != "default" else "logs"


_httpfs_lock = threading.Lock()
_fos_proxy_secret_lock = threading.Lock()
_fos_secret_fingerprints: weakref.WeakKeyDictionary[duckdb.DuckDBPyConnection, tuple[str, str, str, str]] = (
    weakref.WeakKeyDictionary()
)


def _load_httpfs(con: duckdb.DuckDBPyConnection):
    with _httpfs_lock:
        try:
            con.execute("LOAD httpfs;")
        except duckdb.Error:
            try:
                con.execute("INSTALL httpfs;")
                con.execute("LOAD httpfs;")
            except duckdb.InvalidInputException as e:
                if "already registered secret type" not in str(e):
                    raise


def _proxy_target_for(source: dict) -> str:
    cdn_url = (source.get("cdn_url") or "").strip()
    edge_only = source.get("provisioning", {}).get("edge_only")
    if edge_only is None:
        edge_only = source.get("edge_only")
    if cdn_url and edge_only is not False:
        return cdn_url.replace("https://", "").replace("http://", "").split("/", 1)[0].lower()
    return source.get("fos_native_endpoint") or source.get("endpoint") or ""


def _configure_fos(con: duckdb.DuckDBPyConnection, source: dict):
    _load_httpfs(con)
    from backend.utils import telemetry as telemetry_proxy
    from backend.utils.telemetry import get_process_context

    telemetry_proxy.start_proxy_server()
    proxy_ep = telemetry_proxy.proxy_endpoint().replace("http://", "")
    target_host = _proxy_target_for(source)
    headers: dict[str, str] = {
        "X-Fos-Target": target_host,
        "X-Telemetry-Service-Id": source.get("service_id") or source.get("name", "default"),
        "X-Telemetry-Caller": "duckdb.httpfs",
    }
    if ctx := (get_process_context() or ""):
        headers["X-Telemetry-Context"] = ctx
    if target_host != source.get("fos_native_endpoint") and source.get("cdn_secret"):
        headers["x-fastly-key"] = source["cdn_secret"]
    hdr_map_sql = "MAP {" + ", ".join(f"'{k}': ?" for k in headers) + "}"
    create_secret_sql = (
        "CREATE OR REPLACE SECRET fos_proxy (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, "
        f"USE_SSL false, URL_STYLE 'path', EXTRA_HTTP_HEADERS {hdr_map_sql})"
    )
    ak, sk, reg = source.get("access_key_id") or "", source.get("secret_access_key") or "", source.get("region") or ""
    secret_params = [ak, sk, reg, proxy_ep, *headers.values()]
    fp = (ak, sk, reg, proxy_ep)
    with _fos_proxy_secret_lock:
        if _fos_secret_fingerprints.get(con) != fp:

            def _create_secret() -> None:
                try:
                    con.rollback()
                except Exception:
                    logger.debug("_configure_fos: con.rollback failed (ignored)", exc_info=True)
                con.execute(create_secret_sql, secret_params)
                _fos_secret_fingerprints[con] = fp

            retry_call(_create_secret, retry_on=lambda e: "write-write conflict" in str(e).lower(), attempts=3)
    try:
        con.execute("SET http_timeout=60; SET http_retries=5; SET httpfs_client_implementation = 'curl';")
        con.execute("SET custom_user_agent = 'FastlyObjectStorageLogAnalysis/1.0'; SET http_keep_alive = true;")
    except Exception:
        con.execute("SET http_keep_alive = false;")


_fos_client_cache: dict[tuple[str, str, str, str], Any] = {}
_fos_client_lock = threading.Lock()


class _ProxyPaginatorShim:
    def __init__(self, paginator, caller_hint: str | None):
        self._paginator, self._caller_hint = paginator, caller_hint

    def _set_caller_hint_context(self):
        from backend.utils.telemetry import _BOTO3_CALLER_HINT

        return _BOTO3_CALLER_HINT.set(self._caller_hint)

    def _reset_caller_hint_context(self, token) -> None:
        from backend.utils.telemetry import _BOTO3_CALLER_HINT

        _BOTO3_CALLER_HINT.reset(token)

    def paginate(self, *args, **kwargs):
        token = self._set_caller_hint_context()
        try:
            yield from self._paginator.paginate(*args, **kwargs)
        finally:
            self._reset_caller_hint_context(token)

    def __getattr__(self, name):
        return getattr(self._paginator, name)


_FOS_SPAN_METHODS = frozenset(
    {
        "list_objects_v2",
        "list_objects",
        "get_object",
        "head_object",
        "put_object",
        "copy_object",
        "delete_object",
        "delete_objects",
    }
)


class _ProxyClientShim:
    def __init__(self, obj):
        self._obj = obj

    def get_paginator(self, s3_method: str, caller_hint: str | None = None):
        return _ProxyPaginatorShim(self._obj.get_paginator(s3_method), caller_hint)

    def __getattr__(self, name):
        attr = getattr(self._obj, name)
        if name not in _FOS_SPAN_METHODS or not callable(attr):
            return attr

        def _span_wrapped(*args, **kwargs):
            try:
                from backend.core.request_telemetry import (
                    _end_db_span,
                    _start_db_span,
                )

                span = _start_db_span("fos", name, None)
            except Exception:
                span = None
            try:
                result = attr(*args, **kwargs)
            except BaseException as exc:
                _end_db_span(span, exc)
                raise
            _end_db_span(span, None)
            return result

        return _span_wrapped


def _get_fos_client(source: dict):
    source_key = (
        source.get("name", "default"),
        source.get("access_key_id", ""),
        source.get("cdn_url", ""),
        source.get("cdn_secret", ""),
    )
    with _fos_client_lock:
        if source_key in _fos_client_cache:
            return _fos_client_cache[source_key]
        from backend.utils import telemetry as telemetry_proxy

        telemetry_proxy.start_proxy_server()
        proxy_config = Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=30,
            signature_version=UNSIGNED,
            s3={"addressing_style": "path"},
            max_pool_connections=128,
        )
        raw_client = boto3.client("s3", endpoint_url=telemetry_proxy.proxy_endpoint(), config=proxy_config)
        telemetry_proxy.install_boto3_proxy_hook(raw_client, source)
        client = _ProxyClientShim(raw_client)
        _fos_client_cache[source_key] = client
        return client


def clear_fos_client(source_or_name) -> None:
    name = source_or_name.get("name", "default") if isinstance(source_or_name, dict) else source_or_name
    with _fos_client_lock:
        for k in [k for k in _fos_client_cache if (k[0] if isinstance(k, tuple) else k) == name]:
            _fos_client_cache.pop(k, None)


def fos_reachable(source: dict) -> dict:
    try:
        client = _get_fos_client(source)
        client.list_objects_v2(Bucket=source.get("bucket"), Prefix=(source.get("prefix") or "").strip("/"), MaxKeys=1)
        return {"reachable": True, "error": None}
    except Exception as e:  # noqa: BLE001
        return {"reachable": False, "error": str(e)[:200]}


def _is_transient_query_error(exc: BaseException) -> bool:
    if isinstance(exc, RuntimeError):
        return False
    err_lower = str(exc).lower()
    return any(
        kw in err_lower for kw in ("io error", "http", "network", "timeout", "connection", "could not resolve hostname")
    )


def _execute_query_with_retry(con: duckdb.DuckDBPyConnection, query: str, max_retries: int = 5):
    def _attempt():
        try:
            return con.execute(query)
        except Exception as e:
            err_msg, err_lower = str(e), str(e).lower()
            for code in ("401", "403"):
                if f"http {code}" in err_lower or f"(http {code}" in err_lower:
                    url_match = re.search(r"'(https?://[^']+)'", err_msg)
                    url_hint = f" on {url_match.group(1)}" if url_match else ""
                    raise RuntimeError(
                        f"HTTP {code} Unauthorized{url_hint}. Check your FASTLY_CDN_SECRET and CDN VCL configuration."
                    ) from e
            raise

    return retry_call(
        _attempt, retry_on=_is_transient_query_error, attempts=max_retries + 1, base_delay=1.0, multiplier=2.0
    )


def _cache_dir(source: dict) -> str:
    if "_cache_dir_override" in source:
        return source["_cache_dir_override"]
    return os.path.join("cache", (source.get("bucket") or source.get("fos_bucket") or "default").strip())


def get_raw_tree_node(source: dict | None, prefix_filter: str = "", root: str = "raw") -> dict[str, Any]:
    src = source or _DEFAULT_SOURCE
    if not is_configured(src):
        return {"children": []}
    if prefix_filter and not prefix_filter.endswith("/"):
        prefix_filter += "/"
    prefix = src["prefix"].strip().rstrip("/")
    target_prefix = f"{prefix + '/' if prefix else ''}{root}/{prefix_filter}"
    children = []
    try:
        paginator = _get_fos_client(src).get_paginator("list_objects_v2")
        dirs_map: dict[str, dict[str, Any]] = {}
        files = []
        object_count = 0
        for page in paginator.paginate(Bucket=src["bucket"], Prefix=target_prefix):
            for obj in page.get("Contents", ()):
                object_count += 1
                key = obj["Key"]
                rel_path = key[len(target_prefix) :] if key != target_prefix else ""
                if not rel_path:
                    continue
                parts = rel_path.split("/")
                if len(parts) == 1:
                    mtime = obj["LastModified"].isoformat() if "LastModified" in obj else None
                    files.append({"name": parts[0], "type": "file", "size": obj["Size"], "mtime": mtime, "key": key})
                else:
                    dmeta = dirs_map.setdefault(
                        parts[0], {"size": 0, "count": 0, "prefix": f"{prefix_filter}{parts[0]}"}
                    )
                    dmeta["size"] += obj["Size"]
                    dmeta["count"] += 1
            if object_count >= 10000:
                break
        dirs = [
            {"name": dname, "type": "directory", "size": meta["size"], "prefix": meta["prefix"]}
            for dname, meta in sorted(dirs_map.items())
        ]
        files.sort(key=lambda x: x["name"])
        children = dirs + files
    except Exception:
        logger.error("Error in get_raw_tree_node", exc_info=True)
    return {"children": children}


def get_storage_tree_node(source: dict | None, prefix_filter: str = "") -> dict[str, Any]:
    """List the service's DuckLake data files: the local data directory (Standard) or the FOS ``ducklake/`` prefix (High-Scale)."""
    from backend.core.storage._core import _resolve_data_path

    src = source or _DEFAULT_SOURCE
    root = _resolve_data_path(src, src.get("service_id") or src.get("name", "default"))
    if root.startswith(("s3://", "http://", "https://")):
        return get_raw_tree_node(src, prefix_filter, root="ducklake")
    base = os.path.realpath(root)
    target = os.path.realpath(os.path.join(base, prefix_filter))
    if os.path.commonpath([base, target]) != base or not os.path.isdir(target):
        return {"children": []}
    rel = prefix_filter.strip("/")
    dirs: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    scanned = 0
    for entry in sorted(os.scandir(target), key=lambda e: e.name):
        if entry.is_dir(follow_symlinks=False):
            size = 0
            for dirpath, _, names in os.walk(entry.path):
                for name in names:
                    scanned += 1
                    try:
                        size += os.path.getsize(os.path.join(dirpath, name))
                    except OSError:
                        continue
                if scanned >= 10000:
                    break
            dirs.append(
                {"name": entry.name, "type": "directory", "size": size, "prefix": f"{rel}/{entry.name}".lstrip("/")}
            )
        elif entry.is_file(follow_symlinks=False):
            st = entry.stat(follow_symlinks=False)
            mtime = datetime.fromtimestamp(st.st_mtime, UTC).isoformat()
            files.append({"name": entry.name, "type": "file", "size": st.st_size, "mtime": mtime, "key": entry.name})
    return {"children": dirs + files}


_initialized_paths: set[str] = set()
_init_lock = threading.Lock()


def clear_initialization_state(db_path: str):
    with _init_lock:
        _initialized_paths.discard(db_path)


def close_all_connections():
    from backend.core.duckdb_pool import close_all_service_pools

    close_all_service_pools()


class DBBusyError(Exception):
    """Raised when a DuckDB connection cannot be acquired within the timeout."""


def _lock_context(source: dict, db_path: str, read_only: bool) -> dict[str, object]:
    catalog = os.getenv("DUCKLAKE_CATALOG", "")
    return {
        "service_id": source.get("logging_service_id") or source.get("name") or "unknown",
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "db_path": db_path,
        "read_only": read_only,
        "deployment_mode": os.getenv("DEPLOYMENT_MODE", "standard"),
        "catalog_mode": "postgres" if catalog.startswith(("postgres://", "postgresql://")) else "file",
    }


_LOCK_RETRY_INITIAL_DELAY = 0.05
_LOCK_RETRY_MAX_DELAY = 0.5
_LOCK_RETRY_MULTIPLIER = 2.0
_recycle_barrier_cond = threading.Condition()
_recycle_barrier_active: set[str] = set()
_live_conns: dict[str, weakref.WeakSet] = {}
_live_conns_lock = threading.Lock()


def _recycle_barrier_cap_s() -> float:
    try:
        return max(0.0, float(os.getenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "10000")) / 1000.0)
    except (TypeError, ValueError):
        return 10.0


def db_path_for_source(src: dict | None) -> str:
    s = src or _DEFAULT_SOURCE
    if svcconfig.is_durable_serving_mode(s):
        return f":memory:{s.get('service_id') or s.get('name') or 'default'}"
    return os.path.abspath(s.get("duckdb_path") or DUCKDB_PATH)


def _barrier_wait(db_path: str) -> None:
    deadline = time.monotonic() + _recycle_barrier_cap_s()
    with _recycle_barrier_cond:
        while db_path in _recycle_barrier_active:
            if (remaining := deadline - time.monotonic()) <= 0:
                return
            _recycle_barrier_cond.wait(timeout=remaining)


def set_recycle_barrier(db_path: str, on: bool) -> None:
    with _recycle_barrier_cond:
        if on:
            _recycle_barrier_active.add(db_path)
        else:
            _recycle_barrier_active.discard(db_path)
            _recycle_barrier_cond.notify_all()


def is_recycle_barrier_active(db_path: str | None = None) -> bool | set[str]:
    with _recycle_barrier_cond:
        return (db_path in _recycle_barrier_active) if db_path is not None else set(_recycle_barrier_active)


def _register_live_connection(con, db_path: str) -> None:
    with _live_conns_lock:
        try:
            _live_conns.setdefault(db_path, weakref.WeakSet()).add(con)
        except TypeError:
            pass


def live_connection_count(db_path: str) -> int:
    with _live_conns_lock:
        s = _live_conns.get(db_path)
        return len(s) if s is not None else 0


def current_rss_bytes() -> int | None:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, OSError, ValueError, IndexError):
        try:
            import resource

            raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return raw if sys.platform == "darwin" else raw * 1024
        except Exception:
            return None


_CORRUPTION_MARKERS = (
    "not a valid duckdb database file",
    "database file is corrupt",
    "could not read from file",
    "failure while replaying wal file",
    "unrecognized magic bytes",
    "checksum mismatch",
    "failed to deserialize",
    "serialization error",
    "field id mismatch",
)


def _duckdb_connect(database: str, **kwargs: Any) -> duckdb.DuckDBPyConnection:
    if os.getenv("DUCKLAKE_EXTENSION_PATH"):
        kwargs["config"] = {**(kwargs.pop("config", None) or {}), "allow_unsigned_extensions": "true"}
    return duckdb.connect(database, **kwargs)


def _ensure_threads(con: duckdb.DuckDBPyConnection, n_threads: int) -> None:
    current = con.execute("SELECT current_setting('threads')").fetchone()
    if current is None or int(current[0]) != n_threads:
        con.execute(f"SET threads = {int(n_threads)};")


def get_memory_connection(source: dict | None = None) -> duckdb.DuckDBPyConnection:
    con = _duckdb_connect(":memory:")
    _temp_dir = os.getenv("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb")
    os.makedirs(_temp_dir, exist_ok=True)
    con.execute(f"SET temp_directory = '{_temp_dir}';")
    if DUCKDB_MEMORY_LIMIT:
        _exec_quietly(con, f"SET max_memory = '{DUCKDB_MEMORY_LIMIT}';", "get_memory_connection: step failed (ignored)")
    try:
        n_threads = int(DUCKDB_THREADS) if DUCKDB_THREADS else min(multiprocessing.cpu_count(), 8)
        con.execute(f"SET threads = {n_threads};")
    except Exception:
        logger.debug("get_memory_connection: con.execute failed (ignored)", exc_info=True)
    con.execute("SET TimeZone='UTC';")
    if source is not None:
        _configure_fos(con, source)
    con.execute("SET enable_http_metadata_cache=true; SET enable_object_cache=true;")
    return con


def open_serving_connection(
    source: dict, mode: str = "read_only", *, max_wait: float = 300.0, skip_view_update: bool = False
) -> duckdb.DuckDBPyConnection:
    if mode != "read_only":
        raise ValueError(f"unsupported serving connection mode: {mode!r}")
    return get_connection(source=source, max_wait=max_wait, skip_view_update=skip_view_update, read_only=True)


def get_connection(
    source: dict | None = None, max_wait: float = 300.0, skip_view_update: bool = False, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    src = source or _DEFAULT_SOURCE
    durable_serving = svcconfig.is_durable_serving_mode(src)
    if durable_serving and not read_only:
        raise RuntimeError("durable serving mode only permits read-only DuckDB connections")
    db_path = db_path_for_source(src)
    connect_path = ":memory:" if durable_serving else db_path
    con, delay, deadline = None, _LOCK_RETRY_INITIAL_DELAY, time.monotonic() + max_wait
    _barrier_wait(db_path)

    class _CorruptRecovered(Exception):
        pass

    class _LockConflict(Exception):
        def __init__(self, wait_s: float) -> None:
            self.wait_s = wait_s

    for attempt in tenacity.Retrying(
        retry=tenacity.retry_if_exception_type((_CorruptRecovered, _LockConflict)),
        wait=lambda rs: float(getattr(rs.outcome.exception(), "wait_s", 0.0)) if rs.outcome else 0.0,
        sleep=lambda s: time.sleep(s) if s > 0 else None,
        reraise=True,
    ):
        with attempt:
            try:
                if read_only and not durable_serving and not os.path.exists(db_path):
                    try:
                        _duckdb_connect(db_path, read_only=False).close()
                    except Exception:
                        logger.debug("get_connection: _duckdb_connect failed (ignored)", exc_info=True)
                con = _duckdb_connect(connect_path, read_only=False)
                _register_live_connection(con, db_path)
            except Exception as e:
                err_str = str(e).lower()
                if any(m in err_str for m in _CORRUPTION_MARKERS) and not read_only and not durable_serving:
                    logger.error(f"[duckdb] Database corruption detected for {db_path}: {e}")
                    logger.info(f"[duckdb] Attempting stateless recovery by deleting corrupted database: {db_path}")
                    for f in (db_path, db_path + ".wal"):
                        if os.path.exists(f):
                            try:
                                os.remove(f)
                            except Exception as rm_e:
                                logger.error(f"[duckdb] Failed to remove corrupted file {f}: {rm_e}")
                    raise _CorruptRecovered() from e
                if not ("conflict" in err_str or "locked" in err_str or "different configuration" in err_str):
                    raise
                if (remaining := deadline - time.monotonic()) <= 0:
                    context = _lock_context(src, db_path, read_only)
                    logger.error("duckdb_connection_lock_timeout %s", context, exc_info=True)
                    raise DBBusyError(
                        f"Database is locked by another process (path={db_path}, pid={os.getpid()}, read_only={read_only}, "
                        f"deployment_mode={context['deployment_mode']}, catalog_mode={context['catalog_mode']}). "
                        "Try again in a few seconds."
                    ) from e
                wait_s = min(delay, remaining)
                if wait_s <= 0:
                    time.sleep(0)
                delay = min(delay * _LOCK_RETRY_MULTIPLIER, _LOCK_RETRY_MAX_DELAY)
                raise _LockConflict(wait_s) from e
    if con is None:
        raise RuntimeError("DuckDB connection was not established")
    limit = (
        (DUCKDB_POOL_CONN_MEMORY_LIMIT or DUCKDB_MEMORY_LIMIT)
        if durable_serving
        else (DUCKDB_MEMORY_LIMIT or DUCKDB_POOL_CONN_MEMORY_LIMIT)
    )
    if limit:
        _exec_quietly(con, f"SET max_memory = '{limit}';", "get_connection: step failed (ignored)")
    _exec_quietly(con, "SET preserve_insertion_order = false;", "get_connection: preserve_insertion_order failed")
    if durable_serving:
        _service_temp_dir: str | None = os.getenv("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb")
    elif db_path and not db_path.startswith(":memory:"):
        _service_temp_dir = os.path.join(os.path.dirname(db_path), ".tmp")
    else:
        _service_temp_dir = None
    if _service_temp_dir:
        try:
            os.makedirs(_service_temp_dir, exist_ok=True)
            con.execute(f"SET temp_directory = '{escape_sql_literal(_service_temp_dir)}';")
        except Exception as e:
            if "cannot switch temporary directory" in str(e).lower():
                logger.debug(f"[duckdb] Failed to configure temp_directory: {e}")
            else:
                logger.error(f"[duckdb] Failed to configure temp_directory: {e}")
    try:
        con.execute(f"SET max_temp_directory_size = '{os.getenv('DUCKDB_MAX_TEMP_DIRECTORY_SIZE', '10GB')}';")
    except Exception as e:
        err_lower = str(e).lower()
        is_active = "cannot switch temporary directory" in err_lower or "cannot change" in err_lower
        if is_active:
            logger.debug(f"[duckdb] Failed to configure max_temp_directory_size: {e}")
        else:
            logger.error(f"[duckdb] Failed to configure max_temp_directory_size: {e}")
    for stmt in ("SET hive_partitioning = true;", "SET TimeZone='UTC';"):
        _exec_quietly(con, stmt, "get_connection: con.execute failed (ignored)")
    _configure_fos(con, src)
    try:
        from backend.core import storage
        from backend.core.storage._core import _ducklake_attach

        storage.configure_duckdb_s3(con)
        if not _ducklake_attach(con, src, read_only=True):
            raise RuntimeError("failed to attach DuckLake catalog")
    except Exception:
        try:
            con.close()
        except Exception:
            logger.debug("get_connection: con.close failed (ignored)", exc_info=True)
        raise
    con.execute("SET enable_http_metadata_cache=true; SET enable_object_cache=true;")
    global _cached_n_threads, _cached_mem_limit_gb
    if _cached_n_threads is None:
        _cached_n_threads = int(DUCKDB_THREADS) if DUCKDB_THREADS else min(multiprocessing.cpu_count(), 8)
    try:
        _ensure_threads(con, _cached_n_threads)
    except Exception:
        logger.debug("get_connection: _ensure_threads failed (ignored)", exc_info=True)
    if not os.getenv("DUCKDB_MEMORY_LIMIT"):
        if _cached_mem_limit_gb is None:
            try:
                _cached_mem_limit_gb = max(
                    1, int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") * 0.6 / (1024**3))
                )
            except (AttributeError, ValueError):
                _cached_mem_limit_gb = 4
        con.execute(f"SET memory_limit = '{_cached_mem_limit_gb}GB';")
    con.execute("SET checkpoint_threshold = '512MB';")
    if not skip_view_update:
        try:
            from backend.core import storage

            storage.update_storage_view(con, src)
        except Exception as e:
            if durable_serving:
                con.close()
                raise
            logger.debug("[duckdb] update_storage_view skipped on RO connection: %s", e)
    _initialized_paths.add(db_path)
    return con


def _ensure_source_registered(source: dict) -> str:
    from backend.core import metadata as metadata_db

    name = source["name"]
    table_name = _safe_table_name(name)
    config_json = json.dumps(
        {
            "endpoint": source["endpoint"],
            "bucket": source["bucket"],
            "prefix": source["prefix"],
            "region": source["region"],
            "cdn_url": source.get("cdn_url", ""),
            "cdn_secret": source.get("cdn_secret", ""),
        }
    )
    metadata_db.register_source(name, name, config_json, table_name)
    return table_name


_CacheRegistry.register("duckdb._fos_client_cache", _fos_client_cache)
_CacheRegistry.register("duckdb._initialized_paths", _initialized_paths)


def rum_duckdb_path(path: str) -> str:
    return path.removesuffix(".duckdb") + ".rum.duckdb"


def rum_source_for(source: dict) -> dict:
    rum_source = {**source, "name": f"{source.get('name') or source.get('service_id') or 'default'}::rum"}
    if source.get("duckdb_path"):
        rum_source["duckdb_path"] = rum_duckdb_path(source["duckdb_path"])
    return rum_source


_cid_salt_lock = threading.Lock()
_process_cid_salt: str | None = None


def ensure_cid_salt(rum_cfg: dict) -> str:
    if not (salt := rum_cfg.get("cid_salt")):
        rum_cfg["cid_salt"] = salt = secrets.token_hex(32)
    return salt


def get_or_generate_cid_salt(service_id: str) -> str:
    global _process_cid_salt
    from backend.config import load_config, mutate_config

    if cfg := load_config(service_id):
        if isinstance(rum_cfg := cfg.get("rum"), dict) and rum_cfg.get("cid_salt"):
            return rum_cfg["cid_salt"]
    else:
        with _cid_salt_lock:
            if _process_cid_salt is None:
                _process_cid_salt = secrets.token_hex(32)
            logger.warning(
                "cid_salt_no_config service_id=%s: using a non-persistent per-process salt; cids will not be stable",
                service_id,
            )
            return _process_cid_salt
    logger.warning("cid_salt_missing service_id=%s: generating and persisting under cross-process lock", service_id)

    def _set(c: dict) -> None:
        raw_rum = c.get("rum")
        rum: dict[str, Any] = raw_rum if isinstance(raw_rum, dict) else {}
        ensure_cid_salt(rum)
        c["rum"] = rum

    saved = mutate_config(service_id, _set)
    return get_or_generate_cid_salt(service_id) if saved is None else saved["rum"]["cid_salt"]


_SUBMODULES = (_duckdb_status_mod, _sql_validator_mod, _reset_mod)


class _DuckDBShimModule(types.ModuleType):
    """Forwards attribute assignments on ``backend.core.duckdb`` to owning submodules."""

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        for submod in _SUBMODULES:
            if hasattr(submod, name):
                setattr(submod, name, value)


_self_mod = sys.modules.get(__name__)
if _self_mod is not None and not isinstance(_self_mod, _DuckDBShimModule):
    _self_mod.__class__ = _DuckDBShimModule
