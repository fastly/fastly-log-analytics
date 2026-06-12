"""DuckDB connection management with Fastly Object Storage integration.

Supports multiple log sources — each endpoint/bucket/prefix combo gets its own
table with an auto-detected schema from the first ingested file.
"""

import json
import logging
import multiprocessing
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import boto3
import duckdb
from botocore import UNSIGNED
from botocore.config import Config

from backend import config as svcconfig

logger = logging.getLogger(__name__)

# App-wide settings (not per-service)
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS", "")
INGEST_CHUNK_SIZE = int(os.getenv("INGEST_CHUNK_SIZE", "500"))

# Per-service globals — reflect the currently active service.
# These are updated by _load_service_globals() when the active service changes.
DUCKDB_PATH = "logs.duckdb"  # overridden per-service
STORAGE_MODE = "cloud"  # always cloud for new services
ACCESS_LEVEL = "read_write"  # per-service from config

_ORPHAN_THRESHOLD_MINS = 5


# Cached per-process constants — computed once, reused on every connection open.
_cached_n_threads: int | None = None
_cached_mem_limit_gb: int | None = None

_dma_map_cache = None


def _get_dma_map():
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
        except Exception as e:
            print(f"Warning: Could not load {fname}: {e}")

    return _dma_map_cache


def _source_from_config(cfg: dict) -> dict:
    """Build the internal source dict from a service config dict."""
    return svcconfig.config_to_source(cfg)


def _build_default_source() -> dict:
    """Return the source dict for the first configured service, or empty dict."""
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
# Sync module-level globals with the first configured service on import
DUCKDB_PATH = _DEFAULT_SOURCE.get("duckdb_path") or DUCKDB_PATH

# Set by main.py lifespan on startup so start_cron_run can treat rows from
# previous processes as orphans without a blocking pre-scheduler cleanup loop.
_PROCESS_START_UTC: "datetime | None" = None
STORAGE_MODE = _DEFAULT_SOURCE.get("storage_mode") or STORAGE_MODE
ACCESS_LEVEL = _DEFAULT_SOURCE.get("access_level") or ACCESS_LEVEL


def get_source_for_service(service_id: str) -> dict | None:
    """Load the source dict for a specific service ID. Returns None if not found."""
    cfg = svcconfig.load_config(service_id)
    if cfg is None:
        # Fallback: check if service_id matches any cdn_service_id in other configs
        for c in svcconfig.list_configs():
            if c.get("cdn_service_id") == service_id:
                return _source_from_config(c)
        return None
    return _source_from_config(cfg)


def reload_default_source():
    """Reload config from disk and refresh the in-memory default source.

    Called after provisioning or teardown to pick up new/removed services
    without restarting the app.
    """
    global STORAGE_MODE, ACCESS_LEVEL, DUCKDB_PATH
    _DEFAULT_SOURCE.update(_build_default_source())
    STORAGE_MODE = _DEFAULT_SOURCE.get("storage_mode", "cloud")
    ACCESS_LEVEL = _DEFAULT_SOURCE.get("access_level", "read_write")
    DUCKDB_PATH = _DEFAULT_SOURCE.get("duckdb_path", "logs.duckdb")


def is_configured(source: dict | None = None) -> bool:
    """Check if the source has the minimum required configuration for FOS access."""
    s = source or _DEFAULT_SOURCE
    # Required FOS settings
    required = ["endpoint", "access_key_id", "secret_access_key", "bucket"]
    return all(s.get(k) for k in required)


def _safe_table_name(name: str) -> str:
    """Convert a source name to a valid SQL table identifier."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()
    return f"logs_{clean}" if clean != "default" else "logs"


def _fos_glob(source: dict) -> str:
    """Return the FOS glob pattern for a source's log files.

    Now uses the partitioned 'raw/' directory for Fastly logs to avoid
    scanning millions of processed parquet files during discovery.
    """
    prefix = source["prefix"].strip().strip("/")
    bucket = source["bucket"]
    if prefix:
        return f"s3://{bucket}/{prefix}/raw/**/*.gz"
    return f"s3://{bucket}/raw/**/*.gz"


_httpfs_installed = False
_httpfs_lock = threading.Lock()

# Process-wide lock for ``CREATE OR REPLACE SECRET fos_proxy``.
# Why: DuckDB's SECRET catalog is MVCC-protected per database file. When
# two connections on the same file race the CREATE concurrently, the loser
# raises "TransactionContext Error: Catalog write-write conflict on create
# with 'fos_proxy'", which surfaces as ASGI 500s and an empty dashboard
# because every connection setup in _configure_fos crashes. The SECRET is
# process-wide once created, so serialising the writes is sufficient.
_fos_proxy_secret_lock = threading.Lock()


def _load_httpfs(con: duckdb.DuckDBPyConnection):
    """Install (once) and load httpfs, serialised to prevent concurrent-init heap corruption.

    DuckDB's process-level extension registry is not thread-safe during initialisation.
    Two connections calling LOAD httpfs simultaneously corrupts global state and causes
    an Abort trap / malloc heap corruption on macOS.  Holding the lock for both
    INSTALL and LOAD eliminates that race.
    """
    global _httpfs_installed
    with _httpfs_lock:
        if not _httpfs_installed:
            con.execute("INSTALL httpfs;")
            _httpfs_installed = True
        try:
            con.execute("LOAD httpfs;")
        except duckdb.InvalidInputException as e:
            if "already registered secret type" not in str(e):
                raise


def _proxy_target_for(source: dict) -> str:
    """Where the telemetry proxy should forward DuckDB httpfs requests.

    Returns the CDN host (lowercased, scheme/path-stripped) when ``cdn_url``
    is set so the proxy's _sign_request short-circuits SigV4 and the CDN
    classifier (_service_for_target) tags the row as CDN. Otherwise returns
    the native FOS endpoint as-is — moto-style ``http://host:port`` strings
    are honored by the proxy's scheme-aware branch (telemetry_proxy.py:239).
    """
    cdn_url = (source.get("cdn_url") or "").strip()
    if cdn_url:
        return cdn_url.replace("https://", "").replace("http://", "").split("/", 1)[0].lower()
    return source.get("fos_native_endpoint") or source["endpoint"]


def _configure_fos(con: duckdb.DuckDBPyConnection, source: dict):
    """Configure Fastly Object Storage credentials for a specific source.

    Routes every httpfs request through the local telemetry proxy on
    127.0.0.1 so we capture one usage_log row per S3 GET/HEAD. The proxy
    forwards to the target host named in X-Fos-Target (native FOS or CDN
    edge, depending on whether ``source`` has a ``cdn_url``).
    """
    _load_httpfs(con)

    from backend.utils import telemetry_proxy
    from backend.utils.telemetry import get_process_context

    telemetry_proxy.start_proxy_server()  # idempotent
    # Proxy is plain HTTP on localhost; strip the scheme so DuckDB
    # doesn't see "http://http://...". USE_SSL=false in the SECRET
    # below tells httpfs to use http://.
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
    if source.get("cdn_secret"):
        # CDN reads use x-fastly-key for auth; proxy passes it through.
        headers["x-fastly-key"] = source["cdn_secret"]
    # EXTRA_HTTP_HEADERS doesn't accept parameterized map literals when
    # nested in CREATE SECRET, so the keys go in as a literal SQL
    # fragment. Keys are a hardcoded set, never user input.
    hdr_map_sql = "MAP {" + ", ".join(f"'{k}': ?" for k in headers) + "}"
    create_secret_sql = f"""
        CREATE OR REPLACE SECRET fos_proxy (
            TYPE S3,
            KEY_ID ?,
            SECRET ?,
            REGION ?,
            ENDPOINT ?,
            USE_SSL false,
            URL_STYLE 'path',
            EXTRA_HTTP_HEADERS {hdr_map_sql}
        )
    """
    secret_params = [
        source["access_key_id"],
        source["secret_access_key"],
        source["region"],
        proxy_ep,
        *headers.values(),
    ]
    with _fos_proxy_secret_lock:
        # _load_httpfs above runs INSTALL/LOAD httpfs, which starts an implicit
        # transaction with a catalog snapshot taken BEFORE we acquired the lock.
        # If another thread committed its own CREATE OR REPLACE SECRET while we
        # were waiting, our stale snapshot trips a write-write conflict even
        # though only one thread is inside this critical section. Rolling back
        # discards the stale snapshot so CREATE OR REPLACE sees current catalog
        # state. The retry handles the rare case where the rollback itself
        # races with another commit (e.g. a third thread queued behind us).
        for attempt in range(3):
            try:
                con.rollback()
            except Exception:
                pass
            try:
                con.execute(create_secret_sql, secret_params)
                break
            except Exception as e:
                if "write-write conflict" in str(e).lower() and attempt < 2:
                    continue
                raise
    try:
        con.execute("SET http_timeout=60;")
        con.execute("SET http_retries=5;")
        con.execute("SET httpfs_client_implementation = 'curl';")
        con.execute("SET custom_user_agent = 'FastlyObjectStorageLogAnalysis/1.0';")
        con.execute("SET http_keep_alive = true;")
    except Exception:
        con.execute("SET http_keep_alive = false;")


_fos_client_cache: dict[str, Any] = {}
_fos_client_lock = threading.Lock()


class _ProxyPaginatorShim:
    """Thin paginator wrapper used by the flag-on ``_ProxyClientShim``.

    Sets the ``_BOTO3_CALLER_HINT`` ContextVar for the duration of
    ``paginate()`` iteration so the proxy's before-send hook tags every
    underlying S3 request with the caller's hint (e.g. ``ingest_scan``)
    instead of the default ``boto3.listobjectsv2``. The proxy logs each
    S3 request once on its own, so this shim never calls ``record_call``.
    """

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
    """Wraps the flag-on boto3 client so ``get_paginator(method, caller_hint=...)``
    is accepted as a kwarg even though boto3's ``BaseClient.get_paginator``
    doesn't know it. Three production sites pass the hint (ingest_scan,
    download_zip, download_all) and would explode otherwise. Everything
    else is delegated to the underlying boto3 client unchanged.
    """

    def __init__(self, obj):
        self._obj = obj

    def get_paginator(self, s3_method: str, caller_hint: str | None = None):
        return _ProxyPaginatorShim(self._obj.get_paginator(s3_method), caller_hint)

    def __getattr__(self, name):
        return getattr(self._obj, name)


def _get_fos_client(source: dict):
    """Create a boto3 client for Fastly Object Storage routed through the
    local telemetry proxy.

    The boto3 client uses UNSIGNED signing (the proxy re-signs with SigV4
    against the upstream FOS endpoint) and a before-send hook that injects
    the X-Fos-Target / X-Telemetry-* headers the proxy keys on. The proxy
    logs every request, so callers must not wrap this client with anything
    that records its own usage rows.
    """
    source_key = source.get("name", "default")
    with _fos_client_lock:
        if source_key in _fos_client_cache:
            return _fos_client_cache[source_key]

        from backend.utils import telemetry_proxy

        telemetry_proxy.start_proxy_server()  # idempotent
        proxy_config = Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=30,
            signature_version=UNSIGNED,
            s3={"addressing_style": "path"},
            # Match the per-tick burst from _download_chunk_to_local
            # (max_workers=32) so threads don't queue on the default 10-slot
            # urllib3 pool. Telemetry proxy supports up to 32 upstream
            # connections per host (telemetry_proxy._POOL_PER_HOST), so this
            # is the matched ceiling end-to-end.
            max_pool_connections=32,
        )
        raw_client = boto3.client(
            "s3",
            endpoint_url=telemetry_proxy.proxy_endpoint(),
            config=proxy_config,
        )
        telemetry_proxy.install_boto3_proxy_hook(raw_client, source)
        client = _ProxyClientShim(raw_client)
        _fos_client_cache[source_key] = client
        return client


def _execute_query_with_retry(con: duckdb.DuckDBPyConnection, query: str, max_retries: int = 5):
    """Execute a DuckDB query with retry on transient network failures."""
    backoff = 1.0
    for attempt in range(max_retries + 1):
        try:
            return con.execute(query)
        except Exception as e:
            err_msg = str(e)
            err_lower = err_msg.lower()

            # Fail immediately on auth/permission errors — retrying won't help
            for code in ("401", "403"):
                if f"http {code}" in err_lower or f"(http {code}" in err_lower:
                    # Extract the URL from the error message for a clear diagnostic
                    url_match = re.search(r"'(https?://[^']+)'", err_msg)
                    url_hint = f" on {url_match.group(1)}" if url_match else ""
                    raise RuntimeError(
                        f"HTTP {code} Unauthorized{url_hint}. Check your FASTLY_CDN_SECRET and CDN VCL configuration."
                    ) from e

            # Only retry on network/IO errors to avoid 30s delays on data errors
            is_transient = any(
                kw in err_lower
                for kw in ["io error", "http", "network", "timeout", "connection", "could not resolve hostname"]
            )
            if not is_transient:
                raise e

            if attempt == max_retries:
                raise e
            time.sleep(backoff)
            backoff *= 2


def _cache_dir(source: dict) -> str:
    """Return the local cache directory for a source, scoped to its bucket name.

    Scoping by bucket prevents filename collisions when running multiple
    log collection configurations that produce identically-named parquet files.
    """
    if "_cache_dir_override" in source:
        return source["_cache_dir_override"]

    bucket = source.get("bucket") or source.get("fos_bucket") or "default"
    bucket = bucket.strip()
    # Use a relative path so DuckDB views are portable between Mac and Docker
    return os.path.join("cache", bucket)


def get_raw_tree_node(source, prefix_filter="", root="raw"):
    """Return a single level of files in Fastly Object Storage under a root (raw/ or iceberg/).

    Calculates recursive folder sizes by listing up to 10,000 objects under the prefix.
    """
    src = source or _DEFAULT_SOURCE
    if not is_configured(src):
        return {"children": []}

    if prefix_filter and not prefix_filter.endswith("/"):
        prefix_filter += "/"

    prefix = src["prefix"].strip().rstrip("/")
    base_path = f"{prefix}/" if prefix else ""
    target_prefix = f"{base_path}{root}/{prefix_filter}"

    children = []
    try:
        fos_client = _get_fos_client(src)
        paginator = fos_client.get_paginator("list_objects_v2")

        dirs_map = {}  # name -> {"size": bytes, "count": int}
        files = []

        # We list recursively to calculate folder sizes.
        # We limit to 10,000 objects to keep it responsive.
        object_count = 0
        max_objects = 10000

        for page in paginator.paginate(Bucket=src["bucket"], Prefix=target_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                object_count += 1
                key = obj["Key"]
                if key == target_prefix:
                    continue

                rel_path = key[len(target_prefix) :]
                if not rel_path:
                    continue

                parts = rel_path.split("/")
                if len(parts) == 1:
                    # Immediate file
                    files.append(
                        {
                            "name": parts[0],
                            "type": "file",
                            "size": obj["Size"],
                            "mtime": obj["LastModified"].isoformat() if "LastModified" in obj else None,
                            "key": obj["Key"],
                        }
                    )
                else:
                    # File inside a subfolder
                    dir_name = parts[0]
                    if dir_name not in dirs_map:
                        dirs_map[dir_name] = {"size": 0, "count": 0, "prefix": f"{prefix_filter}{dir_name}"}
                    dirs_map[dir_name]["size"] += obj["Size"]
                    dirs_map[dir_name]["count"] += 1

            if object_count >= max_objects:
                break

        dirs = []
        for dname, meta in dirs_map.items():
            dirs.append({"name": dname, "type": "directory", "size": meta["size"], "prefix": meta["prefix"]})

        # Sort dirs and files by name
        dirs.sort(key=lambda x: x["name"])
        files.sort(key=lambda x: x["name"])
        children = dirs + files
    except Exception as e:
        print(f"Error in get_raw_tree_node: {e}")
        pass

    return {"children": children}


# Track which db paths have already been initialized (DDL run, views created).
# This avoids re-running expensive setup on every request while keeping
# each request's connection independent (no shared cursor serialization).
_initialized_paths: set[str] = set()
_init_lock = threading.Lock()


def clear_initialization_state(db_path: str):
    """Remove a database path from the initialized cache.

    Call this when a database file is physically deleted so that DDL
    is run again if the file is recreated without restarting the process.
    """
    with _init_lock:
        _initialized_paths.discard(db_path)


def close_all_connections():
    """No-op: connections are per-request and closed by callers."""
    pass


class DBBusyError(Exception):
    """Raised when a DuckDB connection cannot be acquired within the timeout."""


# ── Lock-contention observability (TESTING_PLAN_3 item 13) ───────────────────
#
# Every lock retry inside ``get_connection`` increments this counter.
# Exposed via ``get_lock_retry_count`` for tests and operational dashboards
# so contention becomes visible (the contract from TESTING_PLAN_3 §7).
#
# Thread-safe: ``threading.Lock`` is cheap and guards against the +=
# read-modify-write race that would otherwise drop increments under load.
_lock_retry_count = 0
_lock_retry_count_lock = threading.Lock()

# Exponential-backoff parameters for transient connection-lock retries.
# Initial 50 ms (matches the pre-refactor constant) doubles up to a cap.
# Capping individual sleeps keeps us responsive under heavy contention
# even when ``max_wait`` is generous (the cron default is 300 s); without
# a cap a single 300 s wait would burn the first ~9 attempts and then
# sleep for 4+ minutes between retries.
_LOCK_RETRY_INITIAL_DELAY = 0.05
_LOCK_RETRY_MAX_DELAY = 0.5
_LOCK_RETRY_MULTIPLIER = 2.0


def get_lock_retry_count() -> int:
    """Return the process-lifetime count of DuckDB connection-lock retries.

    Useful for tests that want to assert the retry path was exercised and
    for operational dashboards that want a rough contention signal.
    """
    with _lock_retry_count_lock:
        return _lock_retry_count


def _reset_lock_retry_count() -> None:
    """Test-only helper. Production code never needs to zero the counter."""
    global _lock_retry_count
    with _lock_retry_count_lock:
        _lock_retry_count = 0


def _record_lock_retry() -> None:
    global _lock_retry_count
    with _lock_retry_count_lock:
        _lock_retry_count += 1


# Substrings of DuckDB error messages that indicate file-level corruption
# or a non-DuckDB file at the target path. These are the only conditions
# under which get_safe_duckdb_connection will delete and recreate the file.
_CORRUPTION_MARKERS = (
    "not a valid duckdb database file",
    "database file is corrupt",
    "could not read from file",
    "failure while replaying wal file",
    "unrecognized magic bytes",
    "checksum mismatch",
)


def _is_corruption_error(err: Exception) -> bool:
    """True if the exception text looks like on-disk corruption.

    Conservative on purpose: only IOException-derived messages that name
    file-level damage. User-query errors (BinderException, ParserException,
    InvalidInputException) must NOT trigger file deletion.
    """
    if not isinstance(err, duckdb.IOException):
        return False
    msg = str(err).lower()
    return any(marker in msg for marker in _CORRUPTION_MARKERS)


@contextmanager
def get_safe_duckdb_connection(db_path: str, read_only: bool = False):
    """Open a DuckDB file; on corruption, delete and reopen empty.

    Recovers from file-level corruption (truncated WAL, garbage bytes,
    checksum mismatch) by removing the offending file plus its sidecars
    (``.wal``, ``.tmp``) and reopening a fresh database. Use this when
    the caller can tolerate losing the local DuckDB cache and rebuilding
    state from the source of truth (Iceberg on FOS, SQLite metadata).

    Only ``duckdb.IOException`` messages matching ``_CORRUPTION_MARKERS``
    trigger the recovery path. Query-time errors raised inside the
    ``with`` block propagate normally — this is not a generic error
    suppressor.

    Read-only callers do not get recovery; they raise on corruption so
    the writer side can repair, and so concurrent dashboard requests
    don't race to delete the file out from under each other.
    """
    conn = None
    try:
        conn = duckdb.connect(db_path, read_only=read_only)
    except Exception as e:
        if read_only or not _is_corruption_error(e):
            raise
        logger.warning(
            "duckdb_recovering_corrupt_file path=%s err=%s",
            db_path,
            str(e).splitlines()[0] if str(e) else "",
        )
        for sidecar in (db_path, db_path + ".wal", db_path + ".tmp"):
            try:
                os.remove(sidecar)
            except FileNotFoundError:
                pass
            except OSError as rm_err:
                logger.error("duckdb_cleanup_failed path=%s err=%s", sidecar, rm_err)
        clear_initialization_state(db_path)
        conn = duckdb.connect(db_path, read_only=False)

    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_memory_connection(source: dict) -> duckdb.DuckDBPyConnection:
    """Return a tracked DuckDB connection in memory."""
    con = duckdb.connect(":memory:")
    # Copy relevant settings from main connection logic
    try:
        if DUCKDB_MEMORY_LIMIT:
            con.execute(f"SET max_memory = '{DUCKDB_MEMORY_LIMIT}';")
    except Exception:
        pass

    con.execute("SET TimeZone='UTC';")
    _configure_fos(con, source)

    con.execute("SET enable_http_metadata_cache=true;")
    con.execute("SET enable_object_cache=true;")

    return con


def get_connection(
    source: dict | None = None, max_wait: float = 300.0, skip_view_update: bool = False, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """Create a configured DuckDB connection.

    ``read_only`` is accepted for API compatibility but always overridden
    to False.  Within a single process DuckDB shares the database instance
    across connections, so mixing ``read_only=True`` (pool / API) with
    ``read_only=False`` (cron writes) raises "different configuration".
    Using False everywhere avoids the conflict; concurrent reads are still
    safe because DuckDB serialises via its internal WAL.
    """
    read_only = False
    src = source or _DEFAULT_SOURCE

    # Use per-source duckdb_path if present, fall back to global DUCKDB_PATH
    db_path = os.path.abspath(src.get("duckdb_path") or DUCKDB_PATH)

    # Per-source access level (from config) takes precedence over the global default
    src_access_level = src.get("access_level") or ACCESS_LEVEL

    # Open a fresh connection per request — DuckDB cursors from a shared connection
    # serialize execution, which is slower than independent connections under load.
    # Lock retries use exponential backoff (TESTING_PLAN_3 item 13 contract):
    # initial 50 ms doubling to a 500 ms cap, total wait bounded by ``max_wait``.
    con = None
    delay = _LOCK_RETRY_INITIAL_DELAY
    deadline = time.monotonic() + max_wait
    while True:
        try:
            con = duckdb.connect(db_path, read_only=read_only)
            break
        except Exception as e:
            err_str = str(e).lower()

            # Handle WAL corruption by deleting the database and attempting stateless recovery.
            # DuckDB internal errors during WAL replay are usually unrecoverable without
            # deleting the WAL (and potentially the DB). Since this app is designed for
            # stateless recovery (metadata/tracking is reconstructed from Iceberg/FOS),
            # deleting the local cache is safe.
            if "failure while replaying wal file" in err_str and not read_only:
                logger.error(f"[duckdb] WAL corruption detected for {db_path}: {e}")
                logger.info(f"[duckdb] Attempting stateless recovery by deleting corrupted database: {db_path}")
                for f in [db_path, db_path + ".wal"]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                        except Exception as rm_e:
                            logger.error(f"[duckdb] Failed to remove corrupted file {f}: {rm_e}")
                continue  # Retry connection (will create fresh DB)

            # "different configuration" is DuckDB's message when a connection
            # already exists on the file with a different ``read_only`` flag —
            # e.g. a cron is writing while a dashboard request tries to open
            # read-only. Treat it as a transient lock so the request retries
            # rather than 500-ing.
            is_lock = "conflict" in err_str or "locked" in err_str or "different configuration" in err_str
            if not is_lock:
                raise e

            # Exponential backoff with a hard deadline. Count every retry
            # so operational dashboards / tests can see contention.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DBBusyError(
                    "Database is locked by another process (cron job may be running). Try again in a few seconds."
                ) from e
            _record_lock_retry()
            time.sleep(min(delay, remaining))
            delay = min(delay * _LOCK_RETRY_MULTIPLIER, _LOCK_RETRY_MAX_DELAY)

    # Apply per-connection settings. DuckDB applies these to the session only.
    try:
        if DUCKDB_MEMORY_LIMIT:
            con.execute(f"SET max_memory = '{DUCKDB_MEMORY_LIMIT}';")
    except Exception:
        pass
    try:
        if DUCKDB_THREADS:
            con.execute(f"SET threads = {DUCKDB_THREADS};")
    except Exception:
        pass
    try:
        con.execute("SET hive_partitioning = true;")
    except Exception:
        pass
    try:
        con.execute("SET TimeZone='UTC';")
    except Exception:
        pass

    _configure_fos(con, src)

    # Configure DuckDB to read Iceberg tables from FOS
    try:
        from backend.core import iceberg

        iceberg.configure_duckdb_s3(con)
        con.execute("SET unsafe_enable_version_guessing=true;")
    except Exception:
        pass

    con.execute("SET enable_http_metadata_cache=true;")
    con.execute("SET enable_object_cache=true;")

    global _cached_n_threads, _cached_mem_limit_gb
    if _cached_n_threads is None:
        _cached_n_threads = min(multiprocessing.cpu_count(), 8)
    con.execute(f"SET threads = {_cached_n_threads};")
    # CRITICAL: only auto-derive memory_limit when DUCKDB_MEMORY_LIMIT is
    # UNSET. Pre-fix, the env-based ``SET max_memory`` at line 762 was
    # silently overridden here by ``SET memory_limit`` (they're aliases
    # in DuckDB — the second SET wins). Container env DUCKDB_MEMORY_LIMIT=8GB
    # was clobbered by ~60% of physical RAM (~9-10GB on the 16GB VM),
    # leaving only ~6GB headroom for Python + pyiceberg + aiohttp + OS +
    # frontend + caddy — recurring host OOM-kills followed.
    if not os.getenv("DUCKDB_MEMORY_LIMIT"):
        if _cached_mem_limit_gb is None:
            try:
                _total_ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
                _cached_mem_limit_gb = max(1, int(_total_ram * 0.6 / (1024**3)))
            except (AttributeError, ValueError):
                _cached_mem_limit_gb = 4
        con.execute(f"SET memory_limit = '{_cached_mem_limit_gb}GB';")
    con.execute("SET checkpoint_threshold = '512MB';")

    # ALWAYS update the view to ensure local buffer files
    # are included. DuckDB views are session-scoped when they reference temp tables
    # or specific file lists, so we must ensure the view is fresh.
    if not skip_view_update:
        try:
            from backend.core import iceberg

            iceberg.update_iceberg_view(con, src)
        except Exception as e:
            # Expected on RO connections — CREATE OR REPLACE VIEW requires
            # write mode, so this fails by design when a reader opens after
            # ingest released its writer lock. The reader just sees whatever
            # view the last writer published. logger.debug instead of print
            # to keep stderr clean during the dashboard's RO query path.
            logger.debug("[duckdb] update_iceberg_view skipped on RO connection: %s", e)

    # Operational metadata (alerts, views, audit, cron, sources, ingested_files,
    # asn_names, usage_log) lives in per-service SQLite — see backend/core/metadata_db.py.
    # DuckDB now holds nothing but session-scoped Iceberg views and temp tables.
    _initialized_paths.add(db_path)

    return con


def _source_row_to_dict(name: str, config_json: str, table_name: str) -> dict:
    cfg = json.loads(config_json)
    return {
        "name": name,
        "table_name": table_name,
        "endpoint": cfg.get("endpoint", ""),
        "bucket": cfg.get("bucket", ""),
        "prefix": cfg.get("prefix", ""),
        "region": cfg.get("region", ""),
        "cdn_url": cfg.get("cdn_url", ""),
        "cdn_secret": cfg.get("cdn_secret", ""),
    }


def get_sources(con: duckdb.DuckDBPyConnection | None = None) -> list[dict]:
    """Return all registered sources across all configured services.

    The ``con`` argument is kept for signature compatibility but unused — sources
    now live in per-service SQLite metadata.
    """
    from backend import config as svcconfig
    from backend.core import metadata_db

    out: list[dict] = []
    for cfg in svcconfig.list_configs():
        sid = cfg.get("service_id")
        if not sid:
            continue
        for cfg_row in [metadata_db.get_source_by_name(sid, sid)]:
            if cfg_row is not None:
                out.append(_source_row_to_dict(cfg_row["name"], cfg_row["config"], cfg_row["table_name"]))
    return out


def get_source_by_name(con: duckdb.DuckDBPyConnection | None, name: str) -> dict | None:
    """Return a single source dict by name, or None if not found."""
    from backend.core import metadata_db

    row = metadata_db.get_source_by_name(name, name)
    if not row:
        return None
    return _source_row_to_dict(row["name"], row["config"], row["table_name"])


def _ensure_source_registered(source: dict) -> str:
    """Register a source in the per-service metadata SQLite. Returns the table name."""
    from backend.core import metadata_db

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
    """Begin a cron run; returns the run id. Raises RuntimeError if already running.

    Storage lives in per-service SQLite (``backend.core.metadata_db``). Retention
    pruning happens here to keep the table bounded over time.
    """
    from backend import config as svcconfig
    from backend.core import metadata_db

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id) or {}
    prov = cfg.get("provisioning", {})
    cron_key = "cron_sync" if task == "sync" else "cron_compact"
    cron_cfg = prov.get(cron_key, {})
    retention_days = int(cron_cfg.get("log_retention_days", 7))

    if retention_days > 0:
        try:
            metadata_db.purge_cron_runs(service_id, task=task, days=retention_days)
        except Exception:
            pass

    return metadata_db.start_cron_run(service_id, task)


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
):
    """Persist the terminal state of a cron run.

    Always records errors and partial successes; respects ``cron_*.log_enabled``
    for pure successes (in which case any pending 'running' row is removed).
    """
    from backend import config as svcconfig
    from backend.core import metadata_db

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id) or {}
    prov = cfg.get("provisioning", {})
    # Map each cron task to the cfg block whose log_enabled flag governs it.
    # Tasks not in the map always log — the prior ``"cron_sync" if task ==
    # "sync" else "cron_compact"`` ternary silently coupled metadata_cleanup,
    # optimize, expire, full_sync, gap_heal, alerts, ngwaf_sync, etc. to
    # cron_compact's log_enabled. Setting cron_compact.log_enabled=false on
    # a service would suppress success rows for every task except sync.
    _TASK_TO_CRON_KEY = {
        "sync": "cron_sync",
        "local_compact": "cron_compact",
    }
    cron_key = _TASK_TO_CRON_KEY.get(task)
    log_enabled = prov.get(cron_key, {}).get("log_enabled", True) if cron_key else True

    if status == "success" and corrupt_rows and corrupt_rows > 0:
        status = "partial_success"

    if not log_enabled and status == "success":
        if run_id is not None:
            try:
                metadata_db.delete_cron_run(service_id, run_id)
            except Exception:
                pass
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
        )
    except Exception as e:
        logger.warning("[cron_log] failed to persist cron run for %s/%s: %s", service_id, task, e)


# Cache for FOS file listings to avoid redundant glob() calls during polling
_fos_cache: dict[str, Any] = {
    "gz_last_check": 0,
    "parquet_count": 0,
    "manifest_last_mod": None,
    "gz_files": [],
    "source_name": None,
}

# Cache for the data-side half of the get_sync_status COUNT/MIN/MAX query —
# the second-largest cost in the sync cron path (~240 ms warm with ~1.7 k
# small parquets, called every tick via refresh_config_status). Keyed by a
# cheap data-dir mtime fingerprint; buffer-side stats are recomputed fresh
# each call (~1 ms, <100 files) and merged. Process-local — workers don't
# share — which is fine because the cost we avoid is per-process anyway.
#
# Why data-only: an earlier version of this fingerprint also covered the
# buffer dir, but buffer writes land every tick (~1 s) on any busy service
# and the cron only runs every ~10 s, so the fingerprint changed on virtually
# every poll and the cache effectively never hit. The data dir only mutates
# on commit/optimize (rare), so a data-only fingerprint hits ~every tick and
# the split-out fresh buffer query keeps the merged count exactly accurate.
_data_stats_cache: dict[str, tuple[tuple, int, Any, Any]] = {}
_data_stats_cache_lock = threading.Lock()

# Cache for update_top_values output. Same data-only fingerprint as the
# get_sync_status split-stats cache: top values are dominated by committed
# parquets (RESERVOIR samples 100 k rows from a view that scans every data
# parquet's footer + sample), so when the data dir hasn't changed there is
# nothing new to compute. Buffer churn is excluded deliberately — buffer
# files contribute <0.1 % of a 100 k sample on any non-trivial service, and
# filter-picker autocomplete already falls back to a live query when the
# user types a string that isn't in the cached top-200 (get_field_values).
# Process-local: refresh_config_status only runs in the APScheduler thread.
_top_values_cache: dict[str, tuple] = {}
_top_values_cache_lock = threading.Lock()


def _data_stats_fingerprint(source: dict) -> tuple | None:
    """Return a cheap fingerprint of the committed (data-side) view state.

    Sums the mtime of every partition dir under ``cache/<bucket>/data/``
    along with the count. New commits update partition dir mtimes (parquet
    adds/removes); compaction/optimize also moves the mtimes. Cost is
    ~0.5 ms for ~150 stat calls vs ~155 ms for the data-side COUNT/MIN/MAX
    over the underlying parquets.

    Buffer files are deliberately excluded — they churn every tick and would
    bust the cache pointlessly. The buffer-side stats are merged in fresh by
    the caller.

    Returns None if the data dir is missing — callers should treat that as
    "no cache" and fall back to the full view query.
    """
    cache_dir = _cache_dir(source)
    data_dir = os.path.join(cache_dir, "data")
    if not os.path.isdir(data_dir):
        return None
    data_sum = 0
    data_count = 0
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


# ── Sync-status / schema / ASN / usage-log helpers ────────────────────────
#
# Carved out to backend/core/_duckdb_status.py for the v2.0 file-size
# sweep (the original module ran ~2110 lines). Re-importing every public
# name back into this module preserves the historical flat-import
# surface: ``from backend.core.duckdb import get_sync_status`` etc.
from backend.core._duckdb_status import (  # noqa: E402, F401
    _SCHEMA_CACHE_TTL,
    ASN_CACHE_TTL_DAYS,
    _clear_schema_cache,
    _schema_cache,
    backfill_fastly_edge_writes,
    delete_ingested_files,
    enrich_asn_labels,
    format_asn_label,
    get_asn_names,
    get_ingested_files,
    get_schema,
    get_sync_status,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    refresh_config_status,
    update_cron_duration,
    update_top_values,
)
from backend.utils.date_utils import safe_iso as _safe_iso  # noqa: E402, F401
