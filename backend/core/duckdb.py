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
from datetime import UTC, datetime, timedelta
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


from backend.utils.date_utils import safe_iso as _safe_iso  # noqa: E402

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

    When read_only=True, multiple processes can share the database file.
    When read_only=False (default), only one process can have a connection.
    """
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
_fos_cache = {"gz_last_check": 0, "parquet_count": 0, "manifest_last_mod": None, "gz_files": [], "source_name": None}

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


def get_sync_status(
    con: duckdb.DuckDBPyConnection, source: dict | None = None, skip_fos: bool = False, force: bool = False
) -> dict:
    """Check sync state for a source.

    skip_fos=True skips the S3 object listing (Class A operations) and returns
    only local-DB-derived fields. Use this for lightweight header status checks
    on pages that don't need the new-file count.

    force=True performs a fresh listing.
    """
    global _fos_cache
    src = source or _DEFAULT_SOURCE
    configured = is_configured(src)

    if not configured:
        return {
            "configured": False,
            "local_rows": 0,
            "ingested": 0,
            "fos_total": 0,
            "storage_mode": "cloud",
            "access_level": "read_write",
        }

    # Attempt to return cached status from config if possible
    from backend import config as svcconfig

    cached_status = svcconfig.get_status(src["name"])
    if cached_status and not force:
        # If we just want a lightweight status (skip_fos=True),
        # return it immediately without hitting the DB or S3.
        # The background cron job keeps this cache fresh every minute.
        if skip_fos:
            # Re-inject current runtime fields that might have changed
            cached_status["access_level"] = src.get("access_level", "read_write")
            cached_status["storage_mode"] = STORAGE_MODE
            cached_status["configured"] = True
            return cached_status
    table_name = _safe_table_name(src["name"])

    # Pull the ingested-files snapshot from per-service SQLite metadata.
    # The aggregate summary reads a single rollup row (O(1)) rather than
    # scanning the full ingested_files table — on busy services with >1 M
    # files, the legacy fetchall+Python-sum hit ~5 s per cron tick and
    # dominated the post-ingest housekeeping budget.
    try:
        from backend.core import metadata_db

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

    file_count = summary["file_count"]
    local_rows_ingested = summary["total_rows"]
    last_ingested = summary["last_ingested"]
    latest_file_name = summary["latest_file_name"]
    total_bytes = summary["total_bytes"]
    count_with_bytes = summary["count_with_bytes"]
    avg_log_size_kb = (total_bytes / count_with_bytes / 1024.0) if count_with_bytes > 0 else None

    # Parse timestamp from most recently ingested filename (YYYY-MM-DDTHH-MM-SS pattern)
    latest_ingested_file_at = None
    if latest_file_name:
        fname = latest_file_name.split("/")[-1]
        m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
        if m:
            latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"

    # The iceberg view is always the source of truth for row counts.
    # We fetch row counts and time extents if the table exists, even if skip_fos=True,
    # because these are derived from local metadata (Iceberg manifests) and are
    # relatively cheap. This allows the UI to auto-range correctly even during
    # lightweight status polls.
    #
    # The split-path query inside the try block reads parquet DIRECTLY via
    # read_parquet() and doesn't need the iceberg view to exist in the
    # current connection.
    # This matters because sync-status opens a fresh RO connection that
    # doesn't yet have the per-session view; without this, every sync-
    # status poll fell through to ingested_files.row_count (which sums
    # raw FOS line counts BEFORE the timestamp filter and consistently
    # over-reports ~2-3×).
    latest_log_at = None
    earliest_log_at = None
    local_rows = local_rows_ingested

    try:
        # Fetch row count and time extents. The view is built with
        # read_parquet('cache/<bucket>/data/**/*.parquet') UNION ALL
        # read_parquet([buffer_paths]) — DuckDB opens every parquet
        # footer (~150 µs × 1.7 k data files = ~155 ms warm) plus the
        # cheap buffer side. Split the query: cache the data-side
        # count/min/max keyed by a data-dir mtime fingerprint (only
        # changes on commit/optimize), run the buffer side fresh each
        # call (~1 ms for <100 files), then merge. Cache hits go from
        # ~240 ms full-view query down to ~1 ms (data cached + buffer
        # query + fingerprint stat).
        stats = None
        data_fp = _data_stats_fingerprint(src)
        cache_key = src["name"]
        if data_fp is not None:
            try:
                with _data_stats_cache_lock:
                    cached = _data_stats_cache.get(cache_key)
                if cached is not None and cached[0] == data_fp:
                    d_count, d_min, d_max = cached[1], cached[2], cached[3]
                else:
                    data_glob = os.path.join(_cache_dir(src), "data", "**", "*.parquet")
                    d_row = con.execute(
                        "SELECT count(*), min(timestamp), max(timestamp) "
                        f"FROM read_parquet('{data_glob}', union_by_name=true, hive_partitioning=false)"
                    ).fetchone()
                    d_count = (d_row[0] or 0) if d_row else 0
                    d_min = d_row[1] if d_row else None
                    d_max = d_row[2] if d_row else None
                    with _data_stats_cache_lock:
                        _data_stats_cache[cache_key] = (data_fp, d_count, d_min, d_max)

                from backend.core import iceberg as _ice

                buf_paths = [p for p in _ice.buffer_files(src) if os.path.isfile(p)]
                if buf_paths:
                    paths_sql = ", ".join(f"'{p}'" for p in buf_paths)
                    b_row = con.execute(
                        "SELECT count(*), min(timestamp), max(timestamp) "
                        f"FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
                    ).fetchone()
                    b_count = (b_row[0] or 0) if b_row else 0
                    b_min = b_row[1] if b_row else None
                    b_max = b_row[2] if b_row else None
                else:
                    b_count, b_min, b_max = 0, None, None

                mins = [m for m in (d_min, b_min) if m is not None]
                maxs = [m for m in (d_max, b_max) if m is not None]
                stats = (
                    d_count + b_count,
                    min(mins) if mins else None,
                    max(maxs) if maxs else None,
                )
            except Exception as split_err:
                # Bust the data cache so we don't pin a half-built result.
                with _data_stats_cache_lock:
                    _data_stats_cache.pop(cache_key, None)
                # Stale-cache failure modes ("No files found", missing
                # catalog entries) must flow to the outer view-rebuild
                # handler below — the cure is the same. Re-raise here
                # rather than swallowing, so the existing recovery path
                # still triggers clear_source_caches+update_iceberg_view.
                err_str = str(split_err)
                if (
                    "No files found" in err_str
                    or "Catalog Error: Table with name" in err_str
                    or "does not exist" in err_str
                    or "No such file or directory" in err_str
                ):
                    raise
                logger.debug("[sync-status] split-stats query failed, falling back to view: %s", split_err)

        if stats is None:
            stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
        if stats:
            view_rows = stats[0] if stats[0] is not None else 0
            # When the view returns a real (non-zero) count, trust it
            # as the source of truth — it reflects the rows actually
            # queryable in Iceberg. ingested_files.row_count records
            # the raw JSON line count from each FOS file BEFORE the
            # `WHERE timestamp IS NOT NULL` filter and any time-range
            # filter, and never reflects post-compaction dedup, so it
            # consistently over-reports. Only fall back when the view
            # itself is empty (the "WHERE false" transient-failure
            # fallback) — there we degrade to the metadata sum so the
            # header doesn't read 0 while we have data on disk.
            if view_rows > 0:
                local_rows = view_rows
                earliest_log_at = stats[1]
                latest_log_at = stats[2]
            else:
                local_rows = local_rows_ingested
    except Exception as e:
        if (
            "No files found" in str(e)
            or "Catalog Error: Table with name" in str(e)
            or "does not exist" in str(e)
            or "No such file or directory" in str(e)
        ):
            try:
                from backend.core import iceberg

                # Bust the cached view SQL FIRST. Without this, when ingest
                # is mid-commit and holding the per-service lock,
                # update_iceberg_view falls back to executing the cached
                # SQL — which is exactly the stale SQL that referenced
                # the missing parquet, looping us right back into the same
                # error. Clearing the cache forces a real rebuild on the
                # next view-update window (possibly the next poll).
                #
                # ``keep_snapshot_cache=True``: do NOT also wipe the
                # snapshot/path cache. If we wipe both, then a transient
                # catalog-load failure (FOS rate limit, network blip)
                # causes update_iceberg_view to fall through to its
                # empty-view branch — "WHERE false" — which then sticks
                # in _view_cache and shows the user "Total Logs: 0"
                # despite millions of rows being in the table.
                iceberg.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
                iceberg.update_iceberg_view(con, src)
                stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
                if stats:
                    local_rows = stats[0] if stats[0] is not None else 0
                    earliest_log_at = stats[1]
                    latest_log_at = stats[2]
            except Exception as retry_e:
                # The fallback to ``local_rows_ingested`` below is the
                # designed degradation path — when the cache is mid-
                # rebuild and we couldn't acquire the lock, ``local_rows``
                # still reflects the row count we tracked at ingest time.
                # Demoted from print/warning to debug because the cascade
                # spams stderr on every sync-status poll until ingest
                # releases the lock; the bust above breaks the loop on
                # the next attempt regardless.
                logger.debug("[sync-status] log stats unavailable mid-rebuild: %s", retry_e)
                local_rows = local_rows_ingested
        else:
            # Unexpected exception — this one is worth keeping as a
            # warning since it doesn't match any of the known "stale
            # cache" patterns above and the fallback may hide real bugs.
            logger.warning("[sync-status] Failed to get log stats from view: %s", e)
            local_rows = local_rows_ingested

    # Latest available filename mirrors latest_file_name since FOS LIST is
    # not consulted here (comment above explains why). Reuse the summary's
    # latest_file_name directly — both fields tracked the same thing.
    latest_available_file_at = latest_ingested_file_at

    try:
        cron_stats = {}
        time_cutoff = (
            (datetime.now(UTC) - timedelta(minutes=_ORPHAN_THRESHOLD_MINS))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

        busy_row = con.execute(
            """
            SELECT count(*) FROM _cron_run_log
            WHERE status = 'running' AND started_at > ?
        """,
            [time_cutoff],
        ).fetchone()
        busy = (busy_row[0] > 0) if busy_row else False

        for row in con.execute(
            """
            SELECT task, started_at, duration_s, status, error_message, summary
            FROM (
                SELECT task, started_at, duration_s, status, error_message, summary,
                       ROW_NUMBER() OVER (PARTITION BY task ORDER BY started_at DESC) AS rn
                FROM _cron_run_log
                WHERE task IN ('sync', 'commit')
            )
            WHERE rn = 1
            """,
        ).fetchall():
            cron_stats[row[0]] = {
                "last_run": _safe_iso(row[1]),
                "duration_s": row[2],
                "status": row[3],
                "error_message": row[4],
                "summary": row[5],
            }
    except Exception:
        busy = False
        cron_stats = {}

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
        "latest_available_file_at": latest_available_file_at,
        "access_level": src.get("access_level", "read_write"),
        "configured": is_configured(src),
        "storage_mode": STORAGE_MODE,
        "logging_service_id": src.get("logging_service_id", ""),
        "cdn_service_id": src.get("cdn_service_id", ""),
        "cron_stats": cron_stats,
    }


def refresh_config_status(service_id: str, include_top_values: bool = True):
    """Fetch latest stats from DuckDB and write them into the service config JSON.

    This allows the UI to read 'latest update' info without having to open the DB
    and risk locking issues when a cron/ingest is busy.

    ``include_top_values`` gates the heavy reservoir-sample + 24-field GROUP BY
    that backs the filter-picker autocomplete cache. The cheap status fields
    (ingested count, latest file, buffer size, iceberg row counts) populate
    regardless, so the dashboard header stays current. Callers from a high-
    cadence cron path (1s log_period → 5s tick) should pass False on most
    ticks and True every ~60s.
    """
    from backend import config as svcconfig

    src = svcconfig.load_config(service_id)
    if not src:
        return

    source = svcconfig.config_to_source(src)
    con = None
    try:
        # Connect in read-only mode to avoid locking. (Comment was here but the
        # code passed neither flag, so this cron actually took an exclusive
        # writer lock every minute and serialised with ingest.) We also
        # skip_view_update because:
        #   - on RO, CREATE OR REPLACE VIEW would fail silently anyway
        #   - if the cached view is stale, get_sync_status' retry path busts
        #     the view cache so the NEXT writer connection rebuilds clean
        con = get_connection(source, skip_view_update=True, read_only=True)
        # skip_fos=False so we do the full Parquet scan for accurate row counts
        # and timestamps. force=True bypasses any stale config-file cache.
        status = get_sync_status(con, source, skip_fos=False, force=True)

        # Add storage size from the buffer directory + any local parquet cache
        try:
            import os as _os

            buf_dir = _cache_dir(source)
            buf_bytes = (
                sum(_os.path.getsize(_os.path.join(r, f)) for r, _, files in _os.walk(buf_dir) for f in files)
                if _os.path.isdir(buf_dir)
                else 0
            )
            status["buffer_size_bytes"] = buf_bytes
        except Exception:
            pass

        # Schema (SUMMARIZE over the iceberg view) costs ~800 ms because
        # update_iceberg_view runs post-ingest on every tick and clears the
        # schema cache. Only refresh schema on the heavy tick (~once/min):
        # the underlying columns rarely change, the per-column min/max/count
        # stats already lag the live data by up to a tick, and update_status
        # uses dict.update() so the prior status['schema'] stays intact when
        # we omit the key. Bootstrap reads from cache (bootstrap.py:135) or
        # falls back to a fresh get_schema() if cache is empty, so freshness
        # remains bounded by the 60 s heavy cadence either way.
        if include_top_values:
            status["schema"] = get_schema(con, source)

        svcconfig.update_status(service_id, status)

        # Also update the top values cache for fast filter suggestions
        if include_top_values:
            logger.info("[refresh_status] %s: Updating top-values cache for filter suggestions...", service_id)
            update_top_values(con, source)
    except Exception as e:
        print(f"Warning: Failed to refresh config status for {service_id}: {e}")
    finally:
        if con:
            con.close()


def update_top_values(con: duckdb.DuckDBPyConnection, source: dict):
    """Pre-calculate top values for filter suggestions and save to local cache.

    Scans the Iceberg + buffer view exactly ONCE with a RESERVOIR sample of at
    most 100 000 rows (small enough to be fast even for million-row tables), then
    computes per-field top-200 lists from that in-memory temp table.  This avoids
    N separate S3 scans — one round-trip for all fields.
    """
    service_id = source["name"]
    table_name = _safe_table_name(service_id)

    # Skip the 100 k reservoir + 24-field GROUP BY entirely when the committed
    # data hasn't changed since the last successful regeneration. The cached
    # top_values.json on disk is still valid; nothing in the heavy path needs
    # to read it during the cron tick. See _top_values_cache docstring above
    # for why buffer-side changes are intentionally not invalidated.
    #
    # Run this BEFORE the "SELECT 1 FROM view LIMIT 1" existence check — that
    # probe is ~150 ms on a multi-thousand-parquet service (DuckDB cracks the
    # view definition open), and we already have proof-of-life (cache file +
    # non-None fingerprint) without touching DuckDB.
    cached_top_values_path = os.path.join(_cache_dir(source), "top_values.json")
    data_fp = _data_stats_fingerprint(source)
    if data_fp is not None and os.path.exists(cached_top_values_path):
        with _top_values_cache_lock:
            prior_fp = _top_values_cache.get(service_id)
        if prior_fp == data_fp:
            return

    # Check if table exists / has data
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        return

    fields = [
        "ip",
        "country",
        "city",
        "host",
        "url",
        "method",
        "ua",
        "status",
        "cache",
        "waf",
        "waf_resp",
        "waf_ms",
        "waf_sig",
        "waf_sig_ind",
        "ja3",
        "ja4",
        "asn",
        "edge",
        "proto",
        "tls",
        "referer",
        "p_type",
        "p_desc",
        "backend",
        "pop",
    ]

    schema_cols = {f["name"] for f in get_schema(con, source)}
    fields = [f for f in fields if f in schema_cols or (f == "waf_sig_ind" and "waf_sig" in schema_cols)]

    if not fields:
        return

    # Build the SELECT list: ordinary fields + waf_sig for waf_sig_ind
    select_parts = []
    for f in fields:
        col = "waf_sig" if f == "waf_sig_ind" else f
        if col in schema_cols:
            select_parts.append(f'"{col}"')

    sel = ", ".join(dict.fromkeys(select_parts))  # deduplicate waf_sig

    sample_table = f"_top_sample_{service_id.replace('-', '_')}"
    top_values: dict = {}

    try:
        # Single scan — reservoir sample capped at 100 000 rows
        con.execute(f"DROP TABLE IF EXISTS {sample_table}")
        try:
            con.execute(
                f"CREATE TEMP TABLE {sample_table} AS "
                f"SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
            )
        except Exception as _e:
            if (
                "No files found" in str(_e)
                or "Catalog Error: Table with name" in str(_e)
                or "does not exist" in str(_e)
                or "No such file or directory" in str(_e)
            ):
                # Buffer file deleted by a commit job — refresh the view and retry
                from backend.core import iceberg

                iceberg.update_iceberg_view(con, source)
                con.execute(f"DROP TABLE IF EXISTS {sample_table}")
                con.execute(
                    f"CREATE TEMP TABLE {sample_table} AS "
                    f"SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
                )
            else:
                raise

        queries = []
        field_order = []
        for f in fields:
            col = "waf_sig" if f == "waf_sig_ind" else f
            if col not in schema_cols:
                continue
            if f == "waf_sig_ind":
                queries.append(f"""
                    (SELECT '{f}' AS _field, trim(signal) AS _value, count(*) AS _cnt
                     FROM (SELECT unnest(string_split("{col}", ',')) AS signal
                           FROM {sample_table}
                           WHERE "{col}" IS NOT NULL AND "{col}" != '')
                     WHERE trim(signal) != ''
                     GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)
                """)
            else:
                queries.append(f"""
                    (SELECT '{f}' AS _field, CAST("{col}" AS VARCHAR) AS _value, count(*) AS _cnt
                     FROM {sample_table}
                     WHERE "{col}" IS NOT NULL
                     GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)
                """)
            field_order.append(f)

        if queries:
            union_sql = " UNION ALL ".join(queries)
            rows = con.execute(union_sql).fetchall()
            for fname in field_order:
                top_values[fname] = []
            for fname, fval, fcnt in rows:
                if fname in top_values:
                    if len(top_values[fname]) < 200:
                        top_values[fname].append({"value": fval, "count": fcnt})

    except Exception as e:
        print(f"Warning: Failed to build top-values index: {e}")
    finally:
        try:
            con.execute(f"DROP TABLE IF EXISTS {sample_table}")
        except Exception:
            pass

    if top_values:
        cache_dir = _cache_dir(source)
        os.makedirs(cache_dir, exist_ok=True)
        with open(os.path.join(cache_dir, "top_values.json"), "w") as f:
            json.dump(top_values, f)
        # Re-read the fingerprint AFTER the write — using the pre-work
        # fingerprint would let a commit that landed mid-sample lock the
        # cache to a stale value. _data_stats_fingerprint is ~0.5 ms.
        post_fp = _data_stats_fingerprint(source)
        if post_fp is not None:
            with _top_values_cache_lock:
                _top_values_cache[service_id] = post_fp


def get_ingested_files(con: duckdb.DuckDBPyConnection, source: dict | None = None) -> list[dict]:
    """Return list of ingested files for a source.

    The ``con`` argument is kept for signature compatibility but unused — the
    data lives in per-service SQLite metadata.
    """
    src = source or _DEFAULT_SOURCE
    from backend.core import metadata_db

    return metadata_db.list_ingested_files(src["name"])


def delete_ingested_files(
    con: duckdb.DuckDBPyConnection, source: dict | None = None, explicit_files: list[str] | None = None
):
    """Delete already-ingested files from Fastly Object Storage for a source.

    Iterative process: performs multiple passes (max 3) to ensure any files
    ingested or uploaded during the deletion window are caught. Uses bulk
    deletion for maximum performance and robustness.
    """
    src = source or _DEFAULT_SOURCE
    if src.get("access_level") == "read_only":
        yield {"type": "error", "message": "Write operations are disabled in read-only mode."}
        return
    glob_pattern = _fos_glob(src)
    fos_client = _get_fos_client(src)
    total_deleted = 0

    from backend.core.ingest import _delete_objects_robust

    if explicit_files:
        keys_to_delete = [
            f[len(f"s3://{src['bucket']}/") :] for f in explicit_files if f.startswith(f"s3://{src['bucket']}/")
        ]
        if not keys_to_delete:
            yield {"type": "status", "message": "No valid files provided for deletion."}
            return

        yield {"type": "status", "message": f"Deleting {len(keys_to_delete)} files directly..."}
        batch_size = 500
        for i in range(0, len(keys_to_delete), batch_size):
            batch = keys_to_delete[i : i + batch_size]
            current_deleted = _delete_objects_robust(fos_client, src["bucket"], batch)
            total_deleted += current_deleted
            yield {
                "type": "progress",
                "current": min(i + batch_size, len(keys_to_delete)),
                "total": len(keys_to_delete),
                "message": f"Deleted {min(i + batch_size, len(keys_to_delete))} of {len(keys_to_delete)} files",
            }

        yield {
            "type": "done",
            "deleted_files": total_deleted,
            "message": f"Successfully deleted {total_deleted} ingested files from Fastly Object Storage.",
        }
        return

    for pass_num in range(1, 4):
        yield {"type": "status", "message": f"Pass {pass_num}/3: Checking for ingested files..."}

        try:
            # Query the bucket for current file list
            all_files = _execute_query_with_retry(con, f"SELECT file FROM glob('{glob_pattern}')").fetchall()
        except Exception as e:
            yield {"type": "error", "message": f"Failed to list bucket during pass {pass_num}: {e}"}
            break

        all_file_names = {row[0] for row in all_files}

        # Query local SQLite metadata for ingested list
        from backend.core import metadata_db

        ingested_set = metadata_db.get_ingested_filenames(src["name"])

        # Files to delete: intersection of what exists in FOS and what we've already ingested
        to_delete_paths = sorted(all_file_names & ingested_set)

        if not to_delete_paths:
            if pass_num == 1:
                yield {"type": "status", "message": "No ingested files found to delete."}
            else:
                yield {"type": "status", "message": "Verification complete: no remaining ingested files found."}
            break

        # Convert full glob() paths (s3://bucket/key) back to raw keys
        keys_to_delete = []
        for path in to_delete_paths:
            key = path[len(f"s3://{src['bucket']}/") :]
            keys_to_delete.append(key)

        yield {
            "type": "status",
            "message": f"Pass {pass_num}/3: Deleting {len(keys_to_delete)} files in bulk batches...",
        }

        # Use progress updates for the deletion batches
        batch_size = 500
        for i in range(0, len(keys_to_delete), batch_size):
            batch = keys_to_delete[i : i + batch_size]
            current_deleted = _delete_objects_robust(fos_client, src["bucket"], batch)
            total_deleted += current_deleted

            yield {
                "type": "progress",
                "current": min(i + batch_size, len(keys_to_delete)),
                "total": len(keys_to_delete),
                "message": f"Pass {pass_num}/3: Deleted {min(i + batch_size, len(keys_to_delete))} of {len(keys_to_delete)} files",
            }

        # Small pause before next pass to allow for eventual consistency
        if pass_num < 3:
            time.sleep(0.5)

    yield {
        "type": "done",
        "deleted_files": total_deleted,
        "message": f"Successfully deleted {total_deleted} ingested files from Fastly Object Storage.",
    }


_schema_cache = {}  # (source_name, table_name) -> (timestamp, schema_list)
# The heavy refresh_config_status path fires SUMMARIZE every 60 s. With the
# previous 60 s TTL the cache aged out at exactly the heavy-tick interval —
# now-ts hit 60.0 right when the next call landed, so we missed every time
# and paid ~800 ms per heavy tick (and per any /schema endpoint call landing
# at a similar phase). 300 s gives heavy ticks a comfortable hit window
# (5 ticks per refresh) and per-page-load /schema calls land on a hit on the
# common case. The cached values are SUMMARIZE-over-100k-sample stats
# (min/max/null_percentage/approx_unique), which drift slowly enough that a
# 5-minute lag is acceptable for the autocomplete + filter-picker UI that
# consumes them. Schema column adds/removes still invalidate immediately via
# the column-set comparison in update_iceberg_view.
_SCHEMA_CACHE_TTL = 300


def _clear_schema_cache(source_name: str | None = None):
    """Clear the schema cache. If source_name is provided, only clear that source."""
    global _schema_cache
    if source_name:
        _schema_cache = {k: v for k, v in _schema_cache.items() if k[0] != source_name}
    else:
        _schema_cache = {}


def get_schema(con: duckdb.DuckDBPyConnection, source: dict | None = None) -> list[dict]:
    """Return column names and types for a source's table."""
    src = source or _DEFAULT_SOURCE
    source_name = src["name"]
    table_name = _safe_table_name(source_name)

    now = time.time()
    cache_key = (source_name, table_name)
    if cache_key in _schema_cache:
        ts, schema = _schema_cache[cache_key]
        if now - ts < _SCHEMA_CACHE_TTL:
            return schema

    try:
        table_exists = (
            con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            > 0
        )
        if not table_exists:
            return []

        # Use SUMMARIZE to get rich metadata instead of just DESCRIBE.
        # We LIMIT 100000 to ensure this remains instantaneous even on billion-row tables.
        # It provides a highly accurate statistical sample of null %, min/max, etc.
        result = con.execute(f"SUMMARIZE SELECT * FROM {table_name} LIMIT 100000").fetchall()
        schema = []
        for row in result:
            count = row[10]
            null_pct = float(row[11]) if row[11] is not None else (100.0 if count == 0 else 0.0)
            schema.append(
                {
                    "name": row[0],
                    "type": row[1],
                    "min": str(row[2]) if row[2] is not None else None,
                    "max": str(row[3]) if row[3] is not None else None,
                    "approx_unique": row[4],
                    "null_percentage": null_pct,
                    "count": count,
                }
            )

        _schema_cache[cache_key] = (now, schema)
        return schema
    except Exception:
        # If SUMMARIZE fails, fallback to DESCRIBE
        try:
            result = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"name": row[0], "type": row[1]} for row in result]
            _schema_cache[cache_key] = (now, schema)
            return schema
        except Exception:
            return []


# ---------------------------------------------------------------------------
# ASN name resolution
# ---------------------------------------------------------------------------

ASN_CACHE_TTL_DAYS = 30


def get_asn_names(service_id: str, asns: list) -> dict:
    """Return {asn: name} for all requested ASNs.

    Reads the per-service asn_names SQLite cache first; resolves stale or
    unknown entries via cymruwhois (Team Cymru DNS whois, batch, no API key)
    and writes them back to the cache. Falls back to 'AS{number}' on failure.
    """
    if not asns:
        return {}

    asns_clean = [int(a) for a in asns if a is not None]
    if not asns_clean or not service_id:
        return {}

    from backend.core import metadata_db

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
            queries = [f"AS{asn}" for asn in need]
            for result in c.lookupmany(queries):
                if result and result.asn:
                    asn_int = int(result.asn)
                    raw_owner = result.owner or f"AS{asn_int}"
                    if " - " in raw_owner:
                        name = raw_owner.split(" - ", 1)[1]
                    else:
                        name = raw_owner
                    resolved[asn_int] = name
        except Exception as e:
            print(f"Warning: ASN resolution failed: {e}")

        if resolved:
            try:
                metadata_db.upsert_asn_names(service_id, resolved)
            except Exception:
                pass

    result = {**cached, **resolved}
    for asn in need:
        if asn not in result:
            result[asn] = f"AS{asn}"

    return result


def format_asn_label(asn: int, name: str) -> str:
    """Format an ASN for display: 'Comcast Cable Communications (7922)' or 'AS7922'."""
    if not name or (name.startswith("AS") and name[2:].isdigit()):
        return f"AS{asn}"
    return f"{name} ({asn})"


def enrich_asn_labels(values: list[dict], service_id: str) -> list[dict]:
    """Resolve ASN names and set a 'label' key on matching value dicts in-place.

    Each dict in `values` must have a 'value' key. Dicts whose value is a
    digit string are treated as ASN numbers and enriched with a formatted label.
    Returns the same list (mutated in place).
    """
    asn_list = [int(v["value"]) for v in values if str(v["value"]).isdigit()]
    if not asn_list:
        return values
    names_map = get_asn_names(service_id, asn_list)
    for v in values:
        if str(v["value"]).isdigit():
            v["label"] = format_asn_label(int(v["value"]), names_map.get(int(v["value"]), ""))
    return values


def update_cron_duration(
    source: dict,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
):
    """Update the duration of a specific cron run record.

    Optionally refresh log_output too — useful when post-ingest phases emit
    status events after the initial log_cron_run snapshot.
    """
    from backend.core import metadata_db

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return
    metadata_db.update_cron_duration(service_id, run_id, duration_s, log_output=log_output)


def log_usage_calls(source: dict, calls: list[dict], process_context: str | None = None):
    """Persist tracked calls to the per-service SQLite usage log via metadata_db.

    Only writes when usage_logging is enabled globally.
    Skips gracefully on any error so it never breaks the calling path.
    """
    from backend import config as svcconfig

    if not svcconfig.is_usage_logging_enabled():
        return

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return
    from backend.core import metadata_db

    metadata_db.log_usage_calls(service_id, calls, process_context=process_context)


def backfill_fastly_edge_writes(source: dict) -> int:
    """Synthesise one Class A PUT_OBJECT row per ingested file in the usage log.

    Each raw log file in FOS was written by Fastly's edge — that's a billable
    Class A op the user pays for, but we never observe it directly. Idempotent:
    deduplicates against existing 'fastly.edge' rows by URL.
    """
    from backend import config as svcconfig

    if not svcconfig.is_usage_logging_enabled():
        return 0

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return 0

    try:
        from backend.core import metadata_db

        # Incremental: NOT EXISTS join skips files that already have a
        # 'fastly.edge' row in usage_log. Steady-state this returns 0 rows
        # so we avoid the 15-chunk 500-IN dedup scan in log_synthetic_usage.
        # Bounded outer scan to the last hour — unbackfilled files only
        # accumulate when the cron tick that ingested them failed to backfill,
        # which is a same-tick concern. Older unbackfilled rows would only
        # appear if the backfill step crashed; admin sweep tools can call
        # without a `since` bound to repair. Without this bound, the outer
        # scan paid ~7 s per tick on services with >1 M ingested_files even
        # when 0 rows needed work.
        since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        files = metadata_db.list_unbackfilled_fastly_edge_files(service_id, since=since)
        if not files:
            return 0

        import re as _re

        calls = []
        for f_name, f_ingested, _row_count, f_size in files:
            if f_name == "__seeding_attempted__":
                continue
            ts_match = _re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", f_name)
            ts = (ts_match.group(1) + "Z") if ts_match else f_ingested

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
                    "_timestamp_override": ts,
                }
            )

        return metadata_db.log_synthetic_usage(service_id, calls)
    except Exception as e:
        logger.debug("[usage_log] Fastly-edge write backfill failed: %s", e)
        return 0


def reconcile_fastly_stats(source: dict, hours_back: int = 12) -> int:
    """Pull Fastly's authoritative hourly /stats/aggregate counts and write one
    reconciliation row per (hour, class) gap into usage_log.

    Why: our synthetic `fastly.edge` backfill counts 1 PUT_OBJECT per ingested
    file, but Fastly's multipart upload pattern actually emits ~3 Class A ops
    per file (CREATE_MULTIPART + UPLOAD_PART + COMPLETE_MULTIPART) and
    additional bookkeeping. The proxy never observes those — they happen
    inside Fastly's edge before any download path. To make the Usage Log page
    agree with Fastly's invoice, we periodically pull /stats/aggregate and
    write a compact reconciliation delta per hour. See
    [metadata_db.reconcile_fastly_stats][] for the per-hour upsert math.

    Idempotent: re-running for an overlapping window replaces prior
    reconciliation rows for those hours rather than stacking them. The
    aggregate is account-wide (Fastly cannot scope FOS ops to a CDN service),
    so this attributes ALL Fastly object-storage ops to the current service.
    For a single-service deployment this is exact; for multi-service the
    estimate is documented as inflated by the /stats/aggregate note already
    surfaced on the Usage Operations chart.
    """
    from backend import config as svcconfig

    if not svcconfig.is_usage_logging_enabled():
        return 0

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return 0

    logging_svc_id = source.get("logging_service_id", "")
    if not logging_svc_id:
        return 0

    api_key = svcconfig.get_fastly_api_key(logging_svc_id)
    if not api_key:
        return 0

    try:
        import json
        import urllib.request
        from datetime import UTC, datetime, timedelta

        from backend.core import metadata_db

        # Hourly gate — Fastly's hourly /stats/aggregate snaps to the wall
        # clock so re-pulling more than once per hour is pure waste, and the
        # per-class SUBSTR scan over `usage_log` for the 26h window costs
        # ~700ms per call on a populated DB. Skip if we already reconciled
        # within the last hour.
        now_dt = datetime.now(UTC)
        latest_recon = metadata_db.get_latest_reconciliation_ts(service_id)
        if latest_recon:
            try:
                latest_dt = datetime.strptime(latest_recon.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
                if (now_dt - latest_dt) < timedelta(hours=1):
                    return 0
            except (ValueError, AttributeError):
                pass

        now = now_dt.replace(minute=0, second=0, microsecond=0)
        from_ts = int((now - timedelta(hours=hours_back)).timestamp())
        to_ts = int((now + timedelta(hours=1)).timestamp())

        req = urllib.request.Request(
            f"https://api.fastly.com/stats/aggregate?by=hour&from={from_ts}&to={to_ts}",
            headers={"Fastly-Key": api_key, "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())

        records = payload.get("data", []) or []
        hourly: list[dict] = []
        for r in records:
            ts = r.get("start_time")
            if ts is None:
                continue
            hour_iso = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:00:00Z")
            class_a = int(r.get("object_storage_class_a_operations_count") or 0)
            class_b = int(r.get("object_storage_class_b_operations_count") or 0)
            if class_a == 0 and class_b == 0:
                sub = r.get("object_storage") or {}
                if isinstance(sub, dict):
                    class_a = int(sub.get("class_a_operations_count") or 0)
                    class_b = int(sub.get("class_b_operations_count") or 0)
            hourly.append({"hour_iso": hour_iso, "class_a": class_a, "class_b": class_b})

        return metadata_db.reconcile_fastly_stats(service_id, hourly)
    except Exception as e:
        logger.debug("[usage_log] Fastly stats reconciliation failed: %s", e)
        return 0


def purge_usage_log(source: dict):
    """Delete usage logs older than the retention period via metadata_db."""
    from backend import config as svcconfig

    ul_cfg = svcconfig.load_usage_logging_config()
    retention_days = int(ul_cfg.get("retention_days", 30))

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return

    from backend.core import metadata_db

    metadata_db.purge_usage_log(service_id, retention_days)
