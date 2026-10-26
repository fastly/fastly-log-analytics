"""DuckDB connection management, FOS integration, sync-status, schema cache, ASN, and usage-log helpers."""

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
import weakref
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import duckdb
from botocore import UNSIGNED
from botocore.config import Config

from backend import config as svcconfig
from backend.core.retry import retry_call
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.date_utils import safe_iso as _safe_iso
from backend.utils.telemetry import HyperLogLog as HyperLogLog
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)

# Self-referencing module aliases for legacy callers/tests.
_db_main = sys.modules[__name__]
_duckdb_status = sys.modules[__name__]
sys.modules["backend.core._duckdb_status"] = sys.modules[__name__]

DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_POOL_CONN_MEMORY_LIMIT = os.getenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "")
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS", "")
INGEST_CHUNK_SIZE = int(os.getenv("INGEST_CHUNK_SIZE", "500"))
DUCKDB_PATH = "logs.duckdb"
_STATUS_BUSY_WINDOW_MINS = 5
_TASK_TO_CRON_KEY = {"sync": "cron_sync", "local_compact": "cron_compact"}

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
    """Load dma_geojson.json and return a mapping of DMA code (string) to DMA name."""
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
                code = props.get("dma_code")
                name = props.get("name") or props.get("dma_name") or props.get("dma1")
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
        "access_level": "read_write",
        "storage_mode": "cloud",
    }


_DEFAULT_SOURCE = _build_default_source()
DUCKDB_PATH = _DEFAULT_SOURCE.get("duckdb_path") or DUCKDB_PATH


def get_source_for_service(service_id: str) -> dict | None:
    """Load the source dict for a specific service ID. Returns None if not found."""
    cfg = svcconfig.load_config(service_id)
    if cfg is not None:
        return _source_from_config(cfg)
    for c in svcconfig.list_configs():
        if c.get("cdn_service_id") == service_id:
            return _source_from_config(c)
    return None


def reload_default_source():
    """Reload config from disk and refresh the in-memory default source."""
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
    """Configure FOS credentials routed through the local telemetry proxy."""
    _load_httpfs(con)
    from backend.utils import telemetry_proxy
    from backend.utils.telemetry import get_process_context

    telemetry_proxy.start_proxy_server()
    proxy_ep = telemetry_proxy.proxy_endpoint().replace("http://", "")
    target_host = _proxy_target_for(source)
    ctx = get_process_context() or ""
    headers: dict[str, str] = {
        "X-Fos-Target": target_host,
        "X-Telemetry-Service-Id": source.get("service_id") or source.get("name", "default"),
        "X-Telemetry-Caller": "duckdb.httpfs",
    }
    if ctx:
        headers["X-Telemetry-Context"] = ctx
    if target_host != source.get("fos_native_endpoint") and source.get("cdn_secret"):
        headers["x-fastly-key"] = source["cdn_secret"]
    hdr_map_sql = "MAP {" + ", ".join(f"'{k}': ?" for k in headers) + "}"
    create_secret_sql = (
        "CREATE OR REPLACE SECRET fos_proxy (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, "
        f"USE_SSL false, URL_STYLE 'path', EXTRA_HTTP_HEADERS {hdr_map_sql})"
    )
    secret_params = [
        source.get("access_key_id") or "",
        source.get("secret_access_key") or "",
        source.get("region") or "",
        proxy_ep,
        *headers.values(),
    ]
    fp = (secret_params[0], secret_params[1], secret_params[2], proxy_ep)
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
        self._paginator = paginator
        self._caller_hint = caller_hint

    def _set_caller_hint_context(self):
        from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

        return _BOTO3_CALLER_HINT.set(self._caller_hint)

    def _reset_caller_hint_context(self, token) -> None:
        from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

        _BOTO3_CALLER_HINT.reset(token)

    def paginate(self, *args, **kwargs):
        token = self._set_caller_hint_context()
        try:
            yield from self._paginator.paginate(*args, **kwargs)
        finally:
            self._reset_caller_hint_context(token)

    def __getattr__(self, name):
        return getattr(self._paginator, name)


class _ProxyClientShim:
    def __init__(self, obj):
        self._obj = obj

    def get_paginator(self, s3_method: str, caller_hint: str | None = None):
        return _ProxyPaginatorShim(self._obj.get_paginator(s3_method), caller_hint)

    def __getattr__(self, name):
        return getattr(self._obj, name)


def _get_fos_client(source: dict):
    """Create or return a cached boto3 client for FOS routed through the local telemetry proxy."""
    source_key = (
        source.get("name", "default"),
        source.get("access_key_id", ""),
        source.get("cdn_url", ""),
        source.get("cdn_secret", ""),
    )
    with _fos_client_lock:
        if source_key in _fos_client_cache:
            return _fos_client_cache[source_key]

        from backend.utils import telemetry_proxy

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
    """Best-effort FOS reachability probe for the admin health surface (SRE-13)."""
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
    """Execute a DuckDB query with retry on transient network failures."""

    def _attempt():
        try:
            return con.execute(query)
        except Exception as e:
            err_msg = str(e)
            err_lower = err_msg.lower()
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


def get_raw_tree_node(source, prefix_filter="", root="raw"):
    """Return a single level of files in FOS under a root (up to 10,000 objects)."""
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
                    files.append(
                        {
                            "name": parts[0],
                            "type": "file",
                            "size": obj["Size"],
                            "mtime": obj["LastModified"].isoformat() if "LastModified" in obj else None,
                            "key": key,
                        }
                    )
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
            remaining = deadline - time.monotonic()
            if remaining <= 0:
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
        s = _live_conns.setdefault(db_path, weakref.WeakSet())
        try:
            s.add(con)
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
        connection_config = dict(kwargs.pop("config", None) or {})
        connection_config["allow_unsigned_extensions"] = "true"
        kwargs["config"] = connection_config
    return duckdb.connect(database, **kwargs)


def _ensure_threads(con: duckdb.DuckDBPyConnection, n_threads: int) -> None:
    """Set the instance-global DuckDB thread count only when it differs (Trap #42)."""
    current = con.execute("SELECT current_setting('threads')").fetchone()
    if current is None or int(current[0]) != n_threads:
        con.execute(f"SET threads = {int(n_threads)};")


def get_memory_connection(source: dict | None = None) -> duckdb.DuckDBPyConnection:
    """Return a tracked DuckDB connection in memory."""
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
    """Create a configured DuckDB connection (must be called from a synchronous context)."""
    src = source or _DEFAULT_SOURCE
    durable_serving = svcconfig.is_durable_serving_mode(src)
    if durable_serving and not read_only:
        raise RuntimeError("durable serving mode only permits read-only DuckDB connections")

    db_path = db_path_for_source(src)
    connect_path = ":memory:" if durable_serving else db_path
    con = None
    delay = _LOCK_RETRY_INITIAL_DELAY
    deadline = time.monotonic() + max_wait

    _barrier_wait(db_path)
    while True:
        try:
            if read_only and not durable_serving and not os.path.exists(db_path):
                try:
                    _duckdb_connect(db_path, read_only=False).close()
                except Exception:
                    logger.debug("get_connection: _duckdb_connect failed (ignored)", exc_info=True)
            con = _duckdb_connect(connect_path, read_only=False)
            _register_live_connection(con, db_path)
            break
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
                continue

            if not ("conflict" in err_str or "locked" in err_str or "different configuration" in err_str):
                raise e

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                context = _lock_context(src, db_path, read_only)
                logger.error("duckdb_connection_lock_timeout %s", context, exc_info=True)
                raise DBBusyError(
                    f"Database is locked by another process (path={db_path}, pid={os.getpid()}, read_only={read_only}, "
                    f"deployment_mode={context['deployment_mode']}, catalog_mode={context['catalog_mode']}). "
                    "Try again in a few seconds."
                ) from e
            time.sleep(min(delay, remaining))
            delay = min(delay * _LOCK_RETRY_MULTIPLIER, _LOCK_RETRY_MAX_DELAY)

    limit = (
        (DUCKDB_POOL_CONN_MEMORY_LIMIT or DUCKDB_MEMORY_LIMIT)
        if durable_serving
        else (DUCKDB_MEMORY_LIMIT or DUCKDB_POOL_CONN_MEMORY_LIMIT)
    )
    if limit:
        _exec_quietly(con, f"SET max_memory = '{limit}';", "get_connection: step failed (ignored)")
    _exec_quietly(
        con, "SET preserve_insertion_order = false;", "get_connection: preserve_insertion_order failed (ignored)"
    )

    if durable_serving:
        _service_temp_dir = os.getenv("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb")
    elif db_path and db_path != ":memory:" and not db_path.startswith(":memory:"):
        _service_temp_dir = os.path.join(os.path.dirname(db_path), ".tmp")
    else:
        _service_temp_dir = None
    if _service_temp_dir:
        try:
            os.makedirs(_service_temp_dir, exist_ok=True)
            from backend.utils.sql_validator import escape_sql_literal

            con.execute(f"SET temp_directory = '{escape_sql_literal(_service_temp_dir)}';")
        except Exception as e:
            if "cannot switch temporary directory" in str(e).lower():
                logger.debug(f"[duckdb] Cannot switch temp_directory (already in use by active query): {e}")
            else:
                logger.error(f"[duckdb] Failed to configure temp_directory: {e}")

    try:
        con.execute(f"SET max_temp_directory_size = '{os.getenv('DUCKDB_MAX_TEMP_DIRECTORY_SIZE', '10GB')}';")
    except Exception as e:
        err_lower = str(e).lower()
        if "cannot switch temporary directory" in err_lower or "cannot change" in err_lower:
            logger.debug(f"[duckdb] Cannot change max_temp_directory_size (already in use by active query): {e}")
        else:
            logger.error(f"[duckdb] Failed to configure max_temp_directory_size: {e}")

    for stmt in ("SET hive_partitioning = true;", "SET TimeZone='UTC';"):
        _exec_quietly(con, stmt, "get_connection: con.execute failed (ignored)")

    _configure_fos(con, src)

    try:
        from backend.core import iceberg
        from backend.core.iceberg._ducklake import _ducklake_attach

        iceberg.configure_duckdb_s3(con)
        con.execute("SET unsafe_enable_version_guessing=true;")
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
                _total_ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
                _cached_mem_limit_gb = max(1, int(_total_ram * 0.6 / (1024**3)))
            except (AttributeError, ValueError):
                _cached_mem_limit_gb = 4
        con.execute(f"SET memory_limit = '{_cached_mem_limit_gb}GB';")
    con.execute("SET checkpoint_threshold = '512MB';")

    if not skip_view_update:
        try:
            from backend.core import iceberg

            iceberg.update_iceberg_view(con, src)
        except Exception as e:
            if durable_serving:
                con.close()
                raise
            logger.debug("[duckdb] update_iceberg_view skipped on RO connection: %s", e)

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


def start_cron_run(source: dict, task: str) -> int | None:
    from backend.core import metadata as metadata_db

    return metadata_db.start_cron_run(source["name"], task)


def log_cron_run(
    source: dict,
    task: str,
    duration_s: float,
    status: str,
    error_message: str = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list = None,
    summary: str = None,
    log_output: str = None,
    run_id: int | None = None,
    outcome_counters: dict[str, int] | None = None,
):
    from backend.core import metadata as metadata_db

    service_id = source["name"]
    prov = (svcconfig.load_config(service_id) or {}).get("provisioning", {})
    cron_key = _TASK_TO_CRON_KEY.get(task)
    log_enabled = prov.get(cron_key, {}).get("log_enabled", True) if cron_key else True

    if status == "success" and corrupt_rows and corrupt_rows > 0:
        status = "partial_success"

    if not log_enabled and status == "success":
        if run_id is not None:
            try:
                metadata_db.delete_cron_run(service_id, run_id)
            except Exception:
                logger.warning("log_cron_run: metadata_db.delete_cron_run failed (ignored)", exc_info=True)
        return

    try:
        metadata_db.log_cron_run(
            service_id,
            task,
            duration_s,
            status,
            error_message=error_message,
            files_downloaded=files_downloaded or 0,
            files_deleted_fos=files_deleted_fos or 0,
            rows_ingested=rows_ingested or 0,
            corrupt_rows=corrupt_rows or 0,
            parquet_files_created=parquet_files_created or 0,
            parquet_files_optimized=parquet_files_optimized or 0,
            parquet_keys=parquet_keys or [],
            summary=summary,
            log_output=log_output,
            run_id=run_id,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.warning("[cron_log] failed to persist cron run for %s/%s: %s", service_id, task, e)


def finalize_cron_run_if_running(
    source: dict,
    task: str,
    run_id: int | None,
    *,
    duration_s: float = 0.0,
    summary: str | None = None,
    error_message: str | None = None,
) -> bool:
    if run_id is None:
        return False
    from backend.core import metadata as metadata_db

    try:
        return metadata_db.finalize_cron_run_if_running(
            source["name"], task, run_id, duration_s=duration_s, summary=summary, error_message=error_message
        )
    except Exception as e:
        logger.warning("[cron_log] finalize-backstop failed for %s/%s: %s", source.get("name"), task, e)
        return False


# ── Sync-status / schema / ASN / usage-log helpers ────────────────────────

_data_stats_cache: dict[str, tuple[tuple, int, Any, Any]] = {}
_data_stats_cache_lock = threading.Lock()
_top_values_cache: dict[str, tuple] = {}
_top_values_cache_lock = threading.Lock()
_ducklake_stats_cache: dict[tuple[str, str], tuple[int, int, Any, Any]] = {}
_ducklake_stats_cache_lock = threading.Lock()


def _data_stats_fingerprint(source: dict) -> tuple | None:
    """Return a cheap `(mtime_ns_sum, count)` fingerprint of `cache/<bucket>/data/`."""
    data_dir = os.path.join(_cache_dir(source), "data")
    if not os.path.isdir(data_dir):
        return None
    data_sum = data_count = 0
    try:
        for entry in os.scandir(data_dir):
            try:
                data_sum += entry.stat().st_mtime_ns
                data_count += 1
            except OSError:
                pass
    except OSError:
        return None
    return (data_sum, data_count)


def _top_values_fingerprint(con: duckdb.DuckDBPyConnection, source: dict) -> tuple | None:
    """Return data-dir fingerprint or DuckLake snapshot fingerprint for `update_top_values`."""
    fp = _data_stats_fingerprint(source)
    if fp is not None:
        return fp
    try:
        from backend.core.iceberg._ducklake import ducklake_current_snapshot_id

        snap_id = ducklake_current_snapshot_id(con)
        return ("ducklake", snap_id) if snap_id is not None else None
    except Exception:
        logger.debug("update_top_values: step failed (ignored)", exc_info=True)
        return None


def clear_ducklake_stats_cache(source_key: str | None = None) -> None:
    """Clear DuckLake table stats cache for a source or globally."""
    with _ducklake_stats_cache_lock:
        if source_key is None:
            _ducklake_stats_cache.clear()
        else:
            for k in [k for k in _ducklake_stats_cache if k[0] == source_key]:
                _ducklake_stats_cache.pop(k, None)
    with _data_stats_cache_lock:
        if source_key is None:
            _data_stats_cache.clear()
        else:
            _data_stats_cache.pop(source_key, None)


def _query_buffer_stats(con, src) -> tuple[int, Any, Any]:
    """Read `(count, min_ts, max_ts)` across live local buffer parquet files."""
    from backend.core import iceberg as _ice
    from backend.utils.sql_validator import escape_sql_literal as _esl

    buf_paths = [p for p in _ice.buffer_files(src) if os.path.isfile(p)]
    if not buf_paths:
        return (0, None, None)
    paths_sql = ", ".join(f"'{_esl(p)}'" for p in buf_paths)
    row = con.execute(
        "SELECT count(*), min(timestamp), max(timestamp) "
        f"FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
    ).fetchone()
    return ((row[0] or 0), row[1], row[2]) if row else (0, None, None)


def _query_or_cache_lake_stats(con, src_name: str, lake_table: str, snap_id: Any) -> tuple[int, Any, Any]:
    """Fetch `(count, min_ts, max_ts)` for `lake."{lake_table}"`, consulting and populating both caches."""
    cached = None
    if snap_id is not None:
        with _ducklake_stats_cache_lock:
            cached = _ducklake_stats_cache.get((src_name, lake_table))
        if (cached is None or cached[0] != snap_id) and lake_table == "logs":
            with _data_stats_cache_lock:
                data_cached = _data_stats_cache.get(src_name)
            if data_cached is not None and data_cached[0] == ("ducklake", snap_id):
                cached = (snap_id, data_cached[1], data_cached[2], data_cached[3])
                with _ducklake_stats_cache_lock:
                    _ducklake_stats_cache[(src_name, lake_table)] = cached
    if cached is not None and cached[0] == snap_id:
        return cached[1], cached[2], cached[3]
    row = con.execute(f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{lake_table}"').fetchone()
    c, mn, mx = ((row[0] or 0), row[1], row[2]) if row else (0, None, None)
    if snap_id is not None:
        with _ducklake_stats_cache_lock:
            _ducklake_stats_cache[(src_name, lake_table)] = (snap_id, c, mn, mx)
        if lake_table == "logs":
            with _data_stats_cache_lock:
                _data_stats_cache[src_name] = (("ducklake", snap_id), c, mn, mx)
    return c, mn, mx


def _authoritative_direct_stats(
    con, src, *, buffer_stats: tuple[int, Any, Any] | None = None
) -> tuple[int, Any, Any] | None:
    """Read `(count, min_ts, max_ts)` straight from the committed DuckLake table + live buffer."""
    counts: list[int] = []
    mins: list[Any] = []
    maxs: list[Any] = []
    src_name = src.get("name", "default")
    try:
        from backend.core.iceberg._ducklake import ducklake_current_snapshot_id, ducklake_table_name

        lake_table = ducklake_table_name(src)
        exists = con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [lake_table]
        ).fetchone()
        if exists:
            lake_count, lake_min, lake_max = _query_or_cache_lake_stats(
                con, src_name, lake_table, ducklake_current_snapshot_id(con)
            )
            if lake_count > 0:
                counts.append(lake_count)
                if lake_min is not None:
                    mins.append(lake_min)
                if lake_max is not None:
                    maxs.append(lake_max)
    except Exception as e:
        logger.debug("[sync-status] direct lake stats unavailable: %s", e)
    try:
        b_cnt, b_min, b_max = buffer_stats if buffer_stats is not None else _query_buffer_stats(con, src)
        if b_cnt > 0:
            counts.append(b_cnt)
        if b_min is not None:
            mins.append(b_min)
        if b_max is not None:
            maxs.append(b_max)
    except Exception as e:
        logger.debug("[sync-status] direct buffer stats unavailable: %s", e)

    total = sum(counts)
    return (total, min(mins) if mins else None, max(maxs) if maxs else None) if total > 0 else None


def get_sync_status(
    con: duckdb.DuckDBPyConnection, source: dict | None = None, skip_fos: bool = False, force: bool = False
) -> dict:
    """Check sync state for a source."""
    src = source or _DEFAULT_SOURCE
    if not is_configured(src):
        return {
            "configured": False,
            "local_rows": 0,
            "ingested": 0,
            "fos_total": 0,
            "storage_mode": "cloud",
            "access_level": "read_write",
        }

    cached_status = svcconfig.get_status(src["name"])
    if cached_status and not force and skip_fos:
        cached_status["access_level"] = src.get("access_level", "read_write")
        cached_status["storage_mode"] = src.get("storage_mode", "cloud")
        cached_status["configured"] = True
        return cached_status

    table_name = _safe_table_name(src["name"])
    try:
        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(src["name"])
    except Exception:
        summary = {
            "file_count": 0,
            "total_rows": 0,
            "total_bytes": 0,
            "count_with_bytes": 0,
            "last_ingested": None,
            "latest_file_name": None,
        }

    file_count, local_rows_ingested = summary["file_count"], summary["total_rows"]
    last_ingested, latest_file_name = summary["last_ingested"], summary["latest_file_name"]
    total_bytes, count_with_bytes = summary["total_bytes"], summary["count_with_bytes"]
    avg_log_size_kb = (total_bytes / count_with_bytes / 1024.0) if count_with_bytes > 0 else None

    latest_ingested_file_at = None
    if latest_file_name:
        m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", latest_file_name.split("/")[-1])
        if m:
            latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"

    latest_log_at = earliest_log_at = None
    local_rows = local_rows_ingested

    try:
        stats = None
        data_fp = _data_stats_fingerprint(src)
        lake_table: str | None = None
        if data_fp is None:
            try:
                from backend.core.iceberg._ducklake import ducklake_current_snapshot_id, ducklake_table_name

                lt = ducklake_table_name(src)
                exists = con.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [lt]
                ).fetchone()
                if exists and exists[0]:
                    snap_id = ducklake_current_snapshot_id(con)
                    if snap_id is not None:
                        data_fp, lake_table = ("ducklake", snap_id), lt
            except Exception:
                logger.debug("get_sync_status: step failed (ignored)", exc_info=True)

        cache_key = src["name"]
        direct_stats = None
        queried_buffer_stats: tuple[int, Any, Any] | None = None
        if data_fp is not None:
            try:
                if (
                    lake_table is not None
                    and isinstance(data_fp, tuple)
                    and len(data_fp) == 2
                    and data_fp[0] == "ducklake"
                ):
                    d_count, d_min, d_max = _query_or_cache_lake_stats(con, cache_key, lake_table, data_fp[1])
                else:
                    with _data_stats_cache_lock:
                        cached = _data_stats_cache.get(cache_key)
                    if cached is not None and cached[0] == data_fp:
                        d_count, d_min, d_max = cached[1], cached[2], cached[3]
                    else:
                        from backend.utils.sql_validator import escape_sql_literal

                        safe_glob = escape_sql_literal(os.path.join(_cache_dir(src), "data", "**", "*.parquet"))
                        d_row = con.execute(
                            "SELECT count(*), min(timestamp), max(timestamp) "
                            f"FROM read_parquet('{safe_glob}', union_by_name=true, hive_partitioning=false)"
                        ).fetchone()
                        d_count, d_min, d_max = ((d_row[0] or 0), d_row[1], d_row[2]) if d_row else (0, None, None)
                        with _data_stats_cache_lock:
                            _data_stats_cache[cache_key] = (data_fp, d_count, d_min, d_max)

                b_count, b_min, b_max = _query_buffer_stats(con, src)
                queried_buffer_stats = (b_count, b_min, b_max)
                mins = [m for m in (d_min, b_min) if m is not None]
                maxs = [m for m in (d_max, b_max) if m is not None]
                stats = (d_count + b_count, min(mins) if mins else None, max(maxs) if maxs else None)
                if lake_table is not None and stats[0] > 0:
                    direct_stats = stats
            except Exception as split_err:
                with _data_stats_cache_lock:
                    _data_stats_cache.pop(cache_key, None)
                if _is_stale_view_error(split_err):
                    raise
                logger.debug("[sync-status] split-stats query failed, falling back to view: %s", split_err)

        if stats is None:
            stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
        if stats:
            view_rows = stats[0] if stats[0] is not None else 0
            direct = (
                direct_stats
                if direct_stats is not None
                else _authoritative_direct_stats(con, src, buffer_stats=queried_buffer_stats)
            )
            if view_rows > 0:
                local_rows, earliest_log_at, latest_log_at = view_rows, stats[1], stats[2]
                if direct is not None:
                    d_earliest, d_latest = direct[1], direct[2]
                    if d_latest is not None and (latest_log_at is None or d_latest > latest_log_at):
                        latest_log_at = d_latest
                    if d_earliest is not None and (earliest_log_at is None or d_earliest < earliest_log_at):
                        earliest_log_at = d_earliest
            elif direct is not None:
                local_rows, earliest_log_at, latest_log_at = direct[0], direct[1], direct[2]
            else:
                prior_rows = (cached_status or {}).get("local_rows") or 0
                local_rows = prior_rows if prior_rows > 0 else local_rows_ingested
    except Exception as e:
        if _is_stale_view_error(e):
            try:
                from backend.core import iceberg

                iceberg.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
                iceberg.update_iceberg_view(con, src)
                stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
                if stats:
                    local_rows, earliest_log_at, latest_log_at = (
                        stats[0] if stats[0] is not None else 0,
                        stats[1],
                        stats[2],
                    )
            except Exception as retry_e:
                logger.debug("[sync-status] log stats unavailable mid-rebuild: %s", retry_e)
                local_rows = local_rows_ingested
        else:
            logger.warning("[sync-status] Failed to get log stats from view: %s", e)
            local_rows = local_rows_ingested

    try:
        cron_stats = {}
        time_cutoff = (
            (datetime.now(UTC) - timedelta(minutes=_STATUS_BUSY_WINDOW_MINS))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        from backend.core.metadata.cron_log import latest_cron_per_task

        crons = latest_cron_per_task(_source_service_id(src))
        busy = False
        for t, data in crons.items():
            st = data.get("started_at")
            if data.get("status") == "running" and st and str(st) > time_cutoff:
                busy = True
            cron_stats[t] = {
                "last_run": _safe_iso(st),
                "duration_s": data.get("duration_s"),
                "status": data.get("status"),
                "error_message": data.get("error_message"),
                "summary": data.get("summary"),
            }
    except Exception:
        busy, cron_stats = False, {}

    return {
        "busy": busy,
        "fos_total": file_count,
        "ingested": file_count,
        "local_rows": local_rows,
        "ingested_bytes": total_bytes,
        "avg_log_size_kb": avg_log_size_kb,
        "table_name": table_name,
        "last_ingested_at": _safe_iso(last_ingested),
        "latest_log_at": _safe_iso(latest_log_at),
        "earliest_log_at": _safe_iso(earliest_log_at),
        "latest_ingested_file_at": latest_ingested_file_at,
        "latest_available_file_at": latest_ingested_file_at,
        "access_level": src.get("access_level", "read_write"),
        "configured": is_configured(src),
        "storage_mode": src.get("storage_mode", "cloud"),
        "logging_service_id": src.get("logging_service_id", ""),
        "cdn_service_id": src.get("cdn_service_id", ""),
        "cron_stats": cron_stats,
    }


def refresh_config_status(service_id: str, include_top_values: bool = True):
    """Fetch latest stats from DuckDB and write them into the service config JSON."""
    src = svcconfig.load_config(service_id)
    if not src:
        return

    source = svcconfig.config_to_source(src)
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    if get_high_scale_service_registry().resolve(service_id) is not None:
        logger.info("[refresh_status] %s: high-scale service uses ClickHouse status", service_id)
        return

    durable_serving = svcconfig.is_durable_serving_mode(source)
    cached_status = src.get("status") or {}
    buf_bytes = None
    if not include_top_values and "buffer_size_bytes" in cached_status:
        buf_bytes = cached_status.get("buffer_size_bytes")
    else:
        try:
            from backend.utils.dir_size import _get_dir_size

            buf_dir = _cache_dir(source)
            if os.path.isdir(buf_dir):
                buf_bytes = _get_dir_size(buf_dir)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

    iceberg_bytes = iceberg_files = None
    if not include_top_values and "iceberg_bytes" in cached_status and "iceberg_files" in cached_status:
        iceberg_bytes, iceberg_files = cached_status.get("iceberg_bytes"), cached_status.get("iceberg_files")
    else:
        try:
            from backend.core import iceberg as _db_iceberg

            info = _db_iceberg.get_table_info(source)
            if not info.get("error"):
                iceberg_bytes = int(info.get("size_bytes", 0) or 0)
                iceberg_files = int(info.get("data_files", 0) or 0)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

    con = None
    try:
        con = get_connection(source, skip_view_update=not durable_serving, read_only=True)
        status = {} if durable_serving else get_sync_status(con, source, skip_fos=False, force=True)
        if buf_bytes is not None:
            status["buffer_size_bytes"] = buf_bytes
        if iceberg_bytes is not None:
            status["iceberg_bytes"] = iceberg_bytes
        if iceberg_files is not None:
            status["iceberg_files"] = iceberg_files

        if not include_top_values:
            if "edge_ratio" in cached_status:
                status["edge_ratio"] = cached_status.get("edge_ratio")
        else:
            try:
                from backend.repositories import assets as _usage_repo

                ratio, _ = _usage_repo.get_edge_ratio(con, source)
                if ratio is not None:
                    status["edge_ratio"] = ratio
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)
            status["schema"] = get_schema(con, source)

        rum_latest = rum_last_sync = request_last_sync = None
        rum_total = 0
        request_total, request_latest = status.get("local_rows", 0), status.get("latest_log_at")
        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            cron_data = latest_cron_per_task(service_id)
            rum_last_sync = cron_data.get("rum_sync", {}).get("started_at")
            request_last_sync = cron_data.get("log_discovery", {}).get("started_at") or cron_data.get("sync", {}).get(
                "started_at"
            )
        except Exception:
            logger.debug("refresh_config_status: cron_data.get failed (ignored)", exc_info=True)

        if svcconfig.is_rum_enabled(source):
            try:
                persisted_rum = (src.get("status") or {}).get("rum") or {}
                rum_total = persisted_rum.get("total_rows", 0) or 0
                rum_latest = persisted_rum.get("latest_log_at")
                if persisted_rum.get("last_sync_at"):
                    rum_last_sync = persisted_rum.get("last_sync_at")
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

        status["rum"] = {"latest_log_at": rum_latest, "total_rows": rum_total, "last_sync_at": rum_last_sync}
        if not durable_serving:
            status["request"] = {
                "latest_log_at": request_latest,
                "total_rows": request_total,
                "last_sync_at": request_last_sync,
            }
            svcconfig.update_status(service_id, status)
        else:
            from backend.core.request_metrics import refresh_durable_request_metrics

            refresh_durable_request_metrics(source, extra_status=status)

        if include_top_values:
            logger.info("[refresh_status] %s: Updating top-values cache for filter suggestions...", service_id)
            update_top_values(con, source)
    except Exception:
        logger.warning("Failed to refresh config status for %s", service_id, exc_info=True)
    finally:
        if con:
            con.close()


_TOP_VALUE_FIELDS = (
    "ip", "country", "city", "host", "url", "method", "ua", "status", "cache", "waf",
    "waf_resp", "waf_ms", "waf_sig", "waf_sig_ind", "ja3", "ja4", "asn", "edge",
    "proto", "tls", "referer", "p_type", "p_desc", "backend", "pop",
)  # fmt: skip


def update_top_values(con: duckdb.DuckDBPyConnection, source: dict):
    """Pre-calculate top values for filter suggestions from a 100k reservoir sample."""
    service_id = source["name"]
    table_name = _safe_table_name(service_id)
    cached_top_values_path = os.path.join(_cache_dir(source), "top_values.json")
    data_fp = _top_values_fingerprint(con, source)
    if data_fp is not None and os.path.exists(cached_top_values_path):
        with _top_values_cache_lock:
            prior_fp = _top_values_cache.get(service_id)
        if prior_fp == data_fp:
            return

    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        return

    schema_cols = {f["name"] for f in get_schema(con, source, stats=False)}
    fields = [f for f in _TOP_VALUE_FIELDS if f in schema_cols or (f == "waf_sig_ind" and "waf_sig" in schema_cols)]
    if not fields:
        return

    select_parts = [
        f'"{"waf_sig" if f == "waf_sig_ind" else f}"'
        for f in fields
        if ("waf_sig" if f == "waf_sig_ind" else f) in schema_cols
    ]
    sel = ", ".join(dict.fromkeys(select_parts))
    sample_table = f"_top_sample_{service_id.replace('-', '_')}"
    top_values: dict = {}
    sample_sql = (
        f"CREATE TEMP TABLE {sample_table} AS SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
    )

    try:
        con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        try:
            con.execute(sample_sql)
        except Exception as _e:
            if _is_stale_view_error(_e):
                from backend.core import iceberg

                iceberg.update_iceberg_view(con, source)
                con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
                con.execute(sample_sql)
            else:
                raise

        queries, field_order = [], []
        for f in fields:
            col = "waf_sig" if f == "waf_sig_ind" else f
            if col not in schema_cols:
                continue
            if f == "waf_sig_ind":
                queries.append(
                    f"(SELECT '{f}' AS _field, trim(signal) AS _value, count(*) AS _cnt "
                    f"FROM (SELECT unnest(string_split(\"{col}\", ',')) AS signal FROM {sample_table} "
                    f'WHERE "{col}" IS NOT NULL AND "{col}" != \'\') '
                    "WHERE trim(signal) != '' GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)"
                )
            else:
                queries.append(
                    f"(SELECT '{f}' AS _field, CAST(\"{col}\" AS VARCHAR) AS _value, count(*) AS _cnt "
                    f'FROM {sample_table} WHERE "{col}" IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)'
                )
            field_order.append(f)

        if queries:
            rows = con.execute(" UNION ALL ".join(queries)).fetchall()
            top_values = {fname: [] for fname in field_order}
            for fname, fval, fcnt in rows:
                if fname in top_values and len(top_values[fname]) < 200:
                    top_values[fname].append({"value": fval, "count": fcnt})
    except Exception:
        logger.warning("Failed to build top-values index", exc_info=True)
    finally:
        try:
            con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        except Exception:
            if log_once_per("update_top_values: DROP TABLE failed (ignored)", 60):
                logger.debug("update_top_values: DROP TABLE failed (ignored)", exc_info=True)

    if top_values:
        cache_dir = _cache_dir(source)
        os.makedirs(cache_dir, exist_ok=True)
        with open(os.path.join(cache_dir, "top_values.json"), "w") as fp:
            json.dump(top_values, fp)
        post_fp = _top_values_fingerprint(con, source)
        if post_fp is not None:
            with _top_values_cache_lock:
                _top_values_cache[service_id] = post_fp


def get_ingested_files(con: duckdb.DuckDBPyConnection | None, source: dict | None = None) -> list[dict]:
    """Return list of ingested files for a source from metadata storage."""
    from backend.core import metadata as metadata_db

    return metadata_db.list_ingested_files((source or _DEFAULT_SOURCE)["name"])


_SCHEMA_CACHE_TTL = 300
_schema_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_SCHEMA_CACHE_TTL)


def _clear_schema_cache(source_name: str | None = None):
    """Clear the schema cache globally or for a single `source_name`."""
    if source_name:
        for k in [k for k in _schema_cache.keys() if k[0] == source_name]:
            _schema_cache.pop(k, None)
    else:
        _schema_cache.clear()


def get_schema(con: duckdb.DuckDBPyConnection, source: dict | None = None, stats: bool = True) -> list[dict]:
    """Return column names and types (plus optional SUMMARIZE stats) for a source's table."""
    src = source or _DEFAULT_SOURCE
    source_name = src["name"]
    table_name = _safe_table_name(source_name)
    cache_key = (source_name, table_name, stats)
    cached = _schema_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        row = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table_name]
        ).fetchone()
        if row is None:
            raise RuntimeError("row is unexpectedly None")
        if row[0] <= 0:
            return []

        if not stats:
            result = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"name": r[0], "type": r[1]} for r in result]
        else:
            result = con.execute(f"SUMMARIZE SELECT * FROM {table_name} LIMIT 10000").fetchall()
            schema = [
                {
                    "name": r[0],
                    "type": r[1],
                    "min": str(r[2]) if r[2] is not None else None,
                    "max": str(r[3]) if r[3] is not None else None,
                    "approx_unique": r[4],
                    "null_percentage": float(r[11]) if r[11] is not None else (100.0 if r[10] == 0 else 0.0),
                    "count": r[10],
                }
                for r in result
            ]
        if len(schema) == 1 and schema[0]["name"] == "timestamp":
            schema = []
        _schema_cache[cache_key] = schema
        return schema
    except Exception:
        try:
            result = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"name": row[0], "type": row[1]} for row in result]
            _schema_cache[cache_key] = schema
            return schema
        except Exception:
            return []


ASN_CACHE_TTL_DAYS = 30


def get_asn_names(service_id: str, asns: list) -> dict:
    """Return `{asn: name}` for all requested ASNs via metadata cache + Team Cymru WHOIS."""
    if not asns:
        return {}
    asns_clean = [int(a) for a in asns if a is not None]
    if not asns_clean or not service_id:
        return {}

    from backend.core import metadata as metadata_db

    try:
        cached = metadata_db.lookup_asn_names(service_id, asns_clean, max_age_days=ASN_CACHE_TTL_DAYS)
    except Exception:
        cached = {}

    need = [a for a in asns_clean if a not in cached]
    resolved: dict[int, str] = {}
    if need:
        try:
            import cymruwhois  # type: ignore

            c = cymruwhois.Client()
            orig_connect = getattr(c, "_connect", None)
            if callable(orig_connect):

                def _safe_connect() -> None:
                    host, port = getattr(c, "host", None), getattr(c, "port", None)
                    if isinstance(host, str) and isinstance(port, int):
                        sock = socket.create_connection((host, port), timeout=2.0)
                        c.socket = sock
                        c.file = sock.makefile("r", encoding="utf-8", errors="replace")
                    else:
                        orig_connect()
                        sock_obj = getattr(c, "socket", None)
                        if sock_obj is not None and hasattr(sock_obj, "settimeout"):
                            sock_obj.settimeout(2.0)
                        reconf = getattr(getattr(c, "file", None), "reconfigure", None)
                        if callable(reconf):
                            reconf(encoding="utf-8", errors="replace")

                c._connect = _safe_connect
            for result in c.lookupmany([f"AS{asn}" for asn in need]):
                if result and result.asn:
                    asn_int = int(result.asn)
                    raw_owner = result.owner or f"AS{asn_int}"
                    resolved[asn_int] = raw_owner.split(" - ", 1)[1] if " - " in raw_owner else raw_owner
        except Exception as exc:
            logger.warning("ASN resolution failed: %s", type(exc).__name__)

        for asn in need:
            resolved.setdefault(asn, f"AS{asn}")
        if resolved:
            try:
                metadata_db.upsert_asn_names(service_id, resolved)
            except Exception:
                logger.warning("get_asn_names: metadata_db.upsert_asn_names failed (ignored)", exc_info=True)

    result = {**cached, **resolved}
    for asn in need:
        result.setdefault(asn, f"AS{asn}")
    return result


def format_asn_label(asn: int, name: str) -> str:
    """Format an ASN for display: `'Comcast Cable Communications (7922)'` or `'AS7922'`."""
    if not name or (name.startswith("AS") and name[2:].isdigit()):
        return f"AS{asn}"
    return f"{name} ({asn})"


def enrich_asn_labels(values: list[dict], service_id: str) -> list[dict]:
    """Resolve ASN names and set a `'label'` key on matching value dicts in-place."""
    asn_list = [int(v["value"]) for v in values if str(v["value"]).isdigit()]
    if not asn_list:
        return values
    names_map = _db_main.get_asn_names(service_id, asn_list)
    for v in values:
        if str(v["value"]).isdigit():
            v["label"] = format_asn_label(int(v["value"]), names_map.get(int(v["value"]), ""))
    return values


def update_cron_duration(source: dict, run_id: int, duration_s: float, log_output: str | None = None):
    """Update the duration (and optional `log_output`) of a specific cron run record."""
    from backend.core import metadata as metadata_db

    service_id = _source_service_id(source)
    if service_id:
        metadata_db.update_cron_duration(service_id, run_id, duration_s, log_output=log_output)


def log_usage_calls(source: dict, calls: list[dict], process_context: str | None = None):
    """Persist tracked calls to the per-service usage log via `metadata_db`."""
    if not svcconfig.is_usage_logging_enabled():
        return
    service_id = _source_service_id(source)
    if service_id:
        from backend.core import metadata as metadata_db

        metadata_db.log_usage_calls(service_id, calls, process_context=process_context)


def backfill_fastly_edge_writes(source: dict) -> int:
    """Synthesise one Class A PUT_OBJECT row per ingested file in the usage log."""
    if not svcconfig.is_usage_logging_enabled():
        return 0
    service_id = _source_service_id(source)
    if not service_id:
        return 0
    try:
        from backend.core import metadata as metadata_db

        since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        files = metadata_db.list_unbackfilled_fastly_edge_files(service_id, since=since)
        if not files:
            return 0
        calls = []
        for f_name, f_ingested, _row_count, f_size in files:
            if f_name == "__seeding_attempted__":
                continue
            ts_match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", f_name)
            calls.append(
                {
                    "method": "PUT_OBJECT",
                    "path": f_name,
                    "service": "FOS",
                    "details": "Class A · synthesized from ingest",
                    "bytes": f_size,
                    "status": "OK",
                    "caller": "fastly.edge",
                    "time_ms": 0,
                    "_timestamp_override": (ts_match.group(1) + "Z") if ts_match else f_ingested,
                }
            )
        return metadata_db.log_synthetic_usage(service_id, calls)
    except Exception as e:
        logger.debug("[usage_log] Fastly-edge write backfill failed: %s", e)
        return 0


def reconcile_fastly_stats(source: dict, hours_back: int = 12) -> int:
    """Pull Fastly's hourly `/stats/aggregate` counts and write reconciliation rows into `usage_log`."""
    if not svcconfig.is_usage_logging_enabled():
        return 0
    service_id, logging_svc_id = _source_service_id(source), source.get("logging_service_id", "")
    if not service_id or not logging_svc_id:
        return 0
    api_key = svcconfig.get_fastly_api_key(logging_svc_id)
    if not api_key:
        return 0
    try:
        from backend.core import metadata as metadata_db
        from backend.core.fastly.client import StatsResponse, fastly

        now_dt = datetime.now(UTC)
        latest_recon = metadata_db.get_latest_reconciliation_ts(service_id)
        if latest_recon:
            latest_dt = parse_iso_utc(latest_recon) if isinstance(latest_recon, str) else None
            if latest_dt is not None and (now_dt - latest_dt) < timedelta(hours=1):
                return 0

        now = now_dt.replace(minute=0, second=0, microsecond=0)
        from_ts = int((now - timedelta(hours=hours_back)).timestamp())
        to_ts = int((now + timedelta(hours=1)).timestamp())
        payload = fastly(
            "GET", f"/stats/aggregate?by=hour&from={from_ts}&to={to_ts}", token=api_key, max_retries=0, timeout=30
        )
        hourly: list[dict] = []
        for rec in StatsResponse.model_validate(payload).data:
            if rec.start_time is None:
                continue
            hour_iso = datetime.fromtimestamp(rec.start_time, tz=UTC).strftime("%Y-%m-%dT%H:00:00Z")
            class_a, class_b = rec.fos_ops()
            hourly.append({"hour_iso": hour_iso, "class_a": class_a, "class_b": class_b})
        return metadata_db.reconcile_fastly_stats(service_id, hourly)
    except Exception as e:
        logger.debug("[usage_log] Fastly stats reconciliation failed: %s", e)
        return 0


def purge_usage_log(source: dict):
    """Delete usage logs older than the retention period via `metadata_db`."""
    retention_days = int(svcconfig.load_usage_logging_config().get("retention_days", 30))
    service_id = _source_service_id(source)
    if service_id:
        from backend.core import metadata as metadata_db

        metadata_db.purge_usage_log(service_id, retention_days)


_CacheRegistry.register("duckdb._fos_client_cache", _fos_client_cache)
_CacheRegistry.register("duckdb._initialized_paths", _initialized_paths)
_CacheRegistry.register("core._duckdb_status._schema_cache", _schema_cache)


class _DucklakeStatsCacheAdapter:
    def clear(self) -> None:
        clear_ducklake_stats_cache()


_CacheRegistry.register("duckdb._ducklake_stats_cache", _DucklakeStatsCacheAdapter())


def rum_duckdb_path(path: str) -> str:
    return path.removesuffix(".duckdb") + ".rum.duckdb"


def rum_source_for(source: dict) -> dict:
    """Return a shallow-copied RUM source dict (`name` suffixed with `::rum`, `.rum.duckdb` path)."""
    rum_source = dict(source)
    rum_source["name"] = f"{source.get('name') or source.get('service_id') or 'default'}::rum"
    if source.get("duckdb_path"):
        rum_source["duckdb_path"] = rum_duckdb_path(source["duckdb_path"])
    return rum_source


_cid_salt_lock = threading.Lock()
_process_cid_salt: str | None = None


def ensure_cid_salt(rum_cfg: dict) -> str:
    """Return `rum_cfg["cid_salt"]`, generating and setting a fresh 32-byte hex salt if absent."""
    salt = rum_cfg.get("cid_salt")
    if not salt:
        salt = secrets.token_hex(32)
        rum_cfg["cid_salt"] = salt
    return salt


def get_or_generate_cid_salt(service_id: str) -> str:
    """Retrieve or generate a persistent 32-byte hex-encoded salt for `cid` pseudonymous hashing."""
    global _process_cid_salt
    from backend.config import load_config, mutate_config

    cfg = load_config(service_id)
    if cfg:
        rum_cfg = cfg.get("rum")
        if isinstance(rum_cfg, dict) and rum_cfg.get("cid_salt"):
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
        existing = c.get("rum")
        rum: dict = existing if isinstance(existing, dict) else {}
        ensure_cid_salt(rum)
        c["rum"] = rum

    saved = mutate_config(service_id, _set)
    return get_or_generate_cid_salt(service_id) if saved is None else saved["rum"]["cid_salt"]
