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

# --- Monkey-patch s3fs to disable AWS Chunked / Checksums ---
# Fastly Object Storage (and many other S3-compatible endpoints) does not support
# the streaming unsigned payload chunking / CRC32 checksums that botocore tries
# to use by default on new clients. We must set request_checksum_calculation="when_required".
#
# The same construction seam also routes s3fs through the local telemetry
# proxy. _get_catalog sets _PENDING_FS_SOURCE before constructing the catalog;
# the patched __init__ pops it and stashes the source on the instance for the
# deferred before-send.s3.* header injector.
import contextvars as _contextvars
import threading as _threading

_PENDING_FS_SOURCE: _contextvars.ContextVar[dict | None] = _contextvars.ContextVar("_PENDING_FS_SOURCE", default=None)

# Thread-safe fallback registry. PyIceberg writes parquet data files via
# concurrent.futures.ThreadPoolExecutor in pyiceberg/io/pyarrow.py, and
# ContextVars do NOT propagate to executor workers (PEP 567 covers asyncio
# only). Each worker thread's first FsspecFileIO call constructs a fresh
# S3FileSystem; without this registry the worker's _PENDING_FS_SOURCE.get()
# returns the default (None), the before-send hook is never registered, and
# the proxy 400s with "Missing X-Fos-Target header".
_PROXY_SOURCE_REGISTRY: dict[str, dict] = {}
_PROXY_REGISTRY_LOCK = _threading.Lock()


def _normalize_endpoint(endpoint_url: str | None) -> str:
    if not endpoint_url:
        return ""
    return endpoint_url.replace("https://", "").replace("http://", "").rstrip("/").lower()


def _register_proxy_source(source: dict) -> None:
    """Register source by endpoint so worker threads can resolve it even
    when the ContextVar is empty."""
    endpoint = source.get("fos_native_endpoint") or source.get("endpoint", "")
    normalized = _normalize_endpoint(endpoint)
    if normalized:
        with _PROXY_REGISTRY_LOCK:
            _PROXY_SOURCE_REGISTRY[normalized] = source


def _lookup_proxy_source(endpoint_url: str | None) -> dict:
    normalized = _normalize_endpoint(endpoint_url)
    if not normalized:
        return {}
    with _PROXY_REGISTRY_LOCK:
        return _PROXY_SOURCE_REGISTRY.get(normalized, {})


def _proxy_targets_from_endpoint(endpoint_url: str, source: dict | None) -> tuple[str | None, str]:
    """Where the proxy should forward S3 traffic, split by request method.

    Returns ``(cdn_target, fos_native_target)``:
      - ``cdn_target`` — the CDN host (lowercased, scheme/path-stripped) when
        source has ``cdn_url``; else ``None``. The proxy's ``_sign_request``
        short-circuits SigV4 for CDN and the row is tagged ``service='CDN'``.
      - ``fos_native_target`` — the FOS native endpoint (or caller's
        endpoint_url as fallback). The proxy SigV4-signs requests going here.

    Callers must dispatch per-request — see ``_register_proxy_event_hook``.
    GET/HEAD can use ``cdn_target`` (cached reads); PUT/POST/DELETE MUST use
    ``fos_native_target`` because Fastly's CDN VCL only authorizes object
    reads — writes routed via CDN return ``HTTP 503`` every time.
    """
    cdn_target: str | None = None
    fos_native_target = endpoint_url
    if source:
        cdn_url = (source.get("cdn_url") or "").strip()
        if cdn_url:
            cdn_target = cdn_url.replace("https://", "").replace("http://", "").split("/", 1)[0].lower()
        native = source.get("fos_native_endpoint")
        if native:
            fos_native_target = native
    return cdn_target, fos_native_target


def _register_proxy_event_hook(
    client,
    cdn_target: str | None,
    fos_native_target: str,
    source: dict,
) -> None:
    """Register a ``before-send.s3.*`` handler on an aiobotocore S3 client
    that injects telemetry-proxy headers per-request.

    The handler reads ``request.method`` at request time and routes:
      - GET/HEAD → ``cdn_target`` when configured (else FOS native). Attaches
        ``x-fastly-key`` for CDN auth.
      - PUT/POST/DELETE/PATCH (and any other write verb) → ``fos_native_target``
        unconditionally. Fastly's CDN VCL only authorizes object reads;
        writes routed via CDN return ``HTTP 503 Service Unavailable`` every
        time. The commit cron silently failed for 2+ hours on 2026-05-19
        because of exactly this — the precomputed target was always CDN.

    ``process_context`` is also read at request time so it propagates per-call.
    """
    service_id = source.get("service_id") or source.get("name", "default")
    cdn_secret = source.get("cdn_secret")

    def _inject(request, **_kwargs):
        from urllib.parse import urlparse

        from backend.utils.telemetry import get_process_context_with_fallback

        # CDN VCL only authorizes object-level reads (no query string).
        # Bucket-level S3 API calls (LIST = ?list-type=2, multi-delete =
        # ?delete, multipart-init = ?uploads, etc.) carry a query string
        # and the CDN rejects them with HTTP 403 SignatureDoesNotMatch.
        # pyiceberg's exists() falls back to a LIST when HEAD 404s, which
        # silently killed the 2026-05-19 commit cron until we routed any
        # GET/HEAD-with-query to FOS native.
        has_query = bool(urlparse(str(request.url)).query) if getattr(request, "url", None) else False
        is_object_read = request.method in ("GET", "HEAD") and not has_query

        if is_object_read and cdn_target:
            request.headers["X-Fos-Target"] = cdn_target
            if cdn_secret:
                request.headers["x-fastly-key"] = cdn_secret
        else:
            request.headers["X-Fos-Target"] = fos_native_target

        request.headers["X-Telemetry-Service-Id"] = service_id
        request.headers["X-Telemetry-Caller"] = "pyiceberg.s3fs"
        # _inject typically fires on fsspec's iothread (a single process-wide
        # asyncio loop thread), NOT the cron thread that called set_process_context.
        # The ContextVar is invisible across that boundary; the fallback returns
        # the most-recently-set value process-wide so the row gets tagged.
        # If still empty (no caller ever tagged), emit the thread name so the
        # row is attributable instead of landing as NULL — telemetry on
        # 2026-05-20 showed 426K rows/day in the NULL bucket, blocking
        # cost attribution.
        ctx = get_process_context_with_fallback()
        if not ctx:
            import threading as _threading

            ctx = f"untagged:{_threading.current_thread().name}"
        request.headers["X-Telemetry-Context"] = ctx

    client.meta.events.register("before-send.s3.*", _inject)


try:
    import botocore as _botocore
    from s3fs import S3FileSystem

    _orig_s3fs_init = S3FileSystem.__init__
    _orig_s3fs_set_session = S3FileSystem.set_session

    def _patched_s3fs_init(self, *args, **kwargs):
        if "config_kwargs" not in kwargs:
            kwargs["config_kwargs"] = {}
        kwargs["config_kwargs"]["request_checksum_calculation"] = "when_required"

        from backend.utils import telemetry_proxy as _proxy

        _proxy.start_proxy_server()  # idempotent

        client_kwargs = kwargs.setdefault("client_kwargs", {})
        original_endpoint = client_kwargs.get("endpoint_url") or kwargs.get("endpoint_url") or ""
        # ContextVar covers the main thread; PyIceberg's thread-pool
        # writers fall through to the endpoint-keyed registry.
        source = _PENDING_FS_SOURCE.get() or _lookup_proxy_source(original_endpoint) or {}
        cdn_target, fos_native_target = _proxy_targets_from_endpoint(original_endpoint, source)
        self._fos_proxy_cdn_target = cdn_target
        # _fos_proxy_target retained as the FOS native endpoint — existing
        # callers and tests treat it as "the canonical S3 origin".
        self._fos_proxy_target = fos_native_target
        # ENDPOINT must be the proxy with explicit http:// scheme — proxy
        # is plain HTTP on localhost.
        client_kwargs["endpoint_url"] = _proxy.proxy_endpoint()
        # Proxy is the sole signer (and skips signing for CDN). UNSIGNED
        # avoids double-signing causing 'SignatureDoesNotMatch' upstream.
        kwargs["config_kwargs"]["signature_version"] = _botocore.UNSIGNED
        kwargs["config_kwargs"].setdefault("s3", {})["addressing_style"] = "path"
        # Stash source so the deferred before-send.s3.* handler (set up
        # on first set_session) can read service_id / cdn config.
        self._fos_proxy_source = source

        _orig_s3fs_init(self, *args, **kwargs)

    async def _patched_s3fs_set_session(self, *args, **kwargs):
        # _s3 may be cached — refresh forces a new client which then needs
        # the event hook re-registered. We always re-register because
        # botocore dedupes handlers internally.
        result = await _orig_s3fs_set_session(self, *args, **kwargs)
        source = getattr(self, "_fos_proxy_source", None)
        fos_native_target = getattr(self, "_fos_proxy_target", None)
        cdn_target = getattr(self, "_fos_proxy_cdn_target", None)
        if source and fos_native_target and self._s3 is not None:
            _register_proxy_event_hook(self._s3, cdn_target, fos_native_target, source)
        return result

    # ── Immutable-manifest bytes cache ───────────────────────────────────
    # PyIceberg's table.scan().plan_files() re-reads every manifest .avro on
    # every query. Telemetry on 2026-05-20 showed 1,104 distinct manifests
    # being fetched ~470× each (517K reads, 2.4 GB CDN) in 13 hours. Iceberg
    # manifests and metadata.json files are immutable once written, so a
    # process-local bytes cache eliminates the redundant fetches.
    import collections as _collections
    import threading as _threading

    _MANIFEST_CACHE_MAX_BYTES = int(os.getenv("FOS_MANIFEST_CACHE_MB", "256")) * 1024 * 1024
    _manifest_bytes_cache: _collections.OrderedDict[str, bytes] = _collections.OrderedDict()
    _manifest_cache_size = 0
    _manifest_cache_lock = _threading.Lock()

    def _is_immutable_path(path: str) -> bool:
        return path.endswith(".avro") or path.endswith(".metadata.json")

    def _canonical_cache_key(path: str) -> str:
        """Same logical S3 object → same cache key, regardless of caller-side
        formatting. PyIceberg's FsspecInputFile passes ``s3://bucket/key`` to
        ``info()`` (sync_wrapper bypasses fsspec's _strip_protocol), but
        fsspec's ``open()`` strips the scheme before calling ``_open``. Without
        normalizing here, the LRU stores under ``s3://bucket/key`` from the
        info path and misses on the lookup with ``bucket/key`` from the open
        path — every manifest is then fetched twice (telemetry 2026-05-20:
        post-dedup ratio stuck at 2.0× because of this exact mismatch)."""
        if path.startswith("s3://"):
            return path[len("s3://") :]
        if path.startswith("s3a://"):
            return path[len("s3a://") :]
        return path.lstrip("/")

    def _cache_get(path: str) -> bytes | None:
        key = _canonical_cache_key(path)
        with _manifest_cache_lock:
            data = _manifest_bytes_cache.get(key)
            if data is not None:
                _manifest_bytes_cache.move_to_end(key)
        return data

    def _cache_put(path: str, data: bytes) -> None:
        global _manifest_cache_size
        n = len(data)
        if n > _MANIFEST_CACHE_MAX_BYTES:
            return  # single file larger than budget; skip caching
        key = _canonical_cache_key(path)
        with _manifest_cache_lock:
            if key in _manifest_bytes_cache:
                _manifest_cache_size -= len(_manifest_bytes_cache[key])
                _manifest_bytes_cache.move_to_end(key)
            _manifest_bytes_cache[key] = data
            _manifest_cache_size += n
            while _manifest_cache_size > _MANIFEST_CACHE_MAX_BYTES and _manifest_bytes_cache:
                _evicted_key, evicted_data = _manifest_bytes_cache.popitem(last=False)
                _manifest_cache_size -= len(evicted_data)

    _orig_cat_file = S3FileSystem._cat_file
    _orig_info = S3FileSystem._info
    _orig_open = S3FileSystem._open

    # In-flight async dedup for immutable fetches. Lives on fsspec's iothread
    # event loop. Without this, the cron_compact "burst" pattern (134 GETs in
    # one second on 2026-05-20) lets the iothread schedule many concurrent
    # cat_file coroutines for the SAME path before any of them populates the
    # LRU — each does its own wire fetch.
    #
    # Dedup is keyed on the canonical path and holds the underlying fetch
    # Task. Multiple awaiters share the same Task; the Task's done callback
    # populates the cache *unconditionally*. This matters because pyiceberg's
    # ``FsspecInputFile.__len__`` path can have its info() future cancelled
    # mid-stream by aiobotocore (observed 2026-05-21: ``client disconnect
    # mid-stream ... ClientConnectionResetError``). Awaiting under
    # ``asyncio.shield`` keeps the underlying Task alive so the bytes still
    # land in the LRU; the next open() then hits the cache instead of doing
    # a second wire fetch (post-fix telemetry: 2.0× → 1.0× ratio).
    import asyncio as _asyncio

    import fsspec.asyn as _asyn

    _inflight_async: dict[str, _asyncio.Future] = {}

    async def _get_or_fetch_immutable_async(fs, path, version_id=None):
        """Cache-aware async fetch with in-flight dedup. Caller must verify
        the path is immutable. Returns full bytes (range slicing is the
        caller's job).

        ``max_concurrency=1`` is critical. s3fs.S3FileSystem._cat_file
        defaults to max_concurrency=10. When max_concurrency > 1 AND no
        start/end is set (our case for manifests), s3fs issues a "probe"
        get_object first to discover Content-Length, closes the body
        immediately, then issues a SECOND get_object via ``_call_and_read``
        to actually fetch the bytes (s3fs/core.py:_cat_file). That probe
        request is fully billed by FOS even though we throw the body away
        — telemetry 2026-05-21 confirmed 2.00× ratio against the proxy
        with our cache already deduping calls 1:1 at the helper level
        (1242 _orig_cat_file calls → 2485 proxy GETs). Forcing
        max_concurrency=1 skips the probe path entirely and falls through
        to a single ``_call_and_read``, restoring 1.00×.
        """
        cached = _cache_get(path)
        if cached is not None:
            return cached

        # Inflight key must use the canonical form too, otherwise an
        # ``info("s3://x")`` and an ``open("x")`` racing on fsspec's iothread
        # would each acquire their own Task and both go to the wire.
        inflight_key = _canonical_cache_key(path)
        task = _inflight_async.get(inflight_key)
        if task is None:
            task = _asyncio.ensure_future(_orig_cat_file(fs, path, version_id=version_id, max_concurrency=1))
            _inflight_async[inflight_key] = task

            def _on_done(t: _asyncio.Future, _key: str = inflight_key, _path: str = path) -> None:
                _inflight_async.pop(_key, None)
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    return
                try:
                    _cache_put(_path, t.result())
                except Exception:
                    pass

            task.add_done_callback(_on_done)

        # shield prevents an awaiter cancellation (e.g. pyiceberg
        # discarding the FsspecInputFile.__len__ future once size is read)
        # from cancelling the shared fetch Task — the task keeps running
        # and its done_callback still populates the LRU.
        return await _asyncio.shield(task)

    async def _patched_cat_file(self, path, version_id=None, start=None, end=None, **kwargs):
        if not _is_immutable_path(path):
            return await _orig_cat_file(self, path, version_id=version_id, start=start, end=end, **kwargs)
        cached = await _get_or_fetch_immutable_async(self, path, version_id=version_id)
        if start is None and end is None:
            return cached
        return cached[start or 0 : end if end is not None else len(cached)]

    async def _patched_info(self, path, bucket=None, key=None, refresh=False, version_id=None):
        # For immutable manifests/metadata: if the bytes are already cached
        # (open()-bridged cat_file populated the LRU on a prior cron tick),
        # synthesize the dict from the cached length and skip the HEAD round
        # trip entirely. On a real cache miss, fall through to the upstream
        # HEAD — do NOT pre-emptively GET the full body here. Telemetry on
        # 2026-05-21 showed the prefetch path racing aiobotocore: ~89% of
        # m0.avro reads disconnected the proxy mid-stream
        # ("ClientConnectionResetError: Cannot write to closing transport"),
        # leaving the cache empty and forcing _patched_open to issue a
        # SECOND wire fetch (2.0× duplicate-fetch ratio). Letting open()
        # be the sole bytes-fetcher restores 1.0× at the cost of one HEAD
        # per never-before-seen immutable file (subsequent ticks hit the
        # LRU). LRU eviction is bounded so this is per-process worst case.
        if _is_immutable_path(path) and not refresh:
            cached = _cache_get(path)
            if cached is not None:
                return {"name": path, "Key": path, "size": len(cached), "Size": len(cached), "type": "file"}
        return await _orig_info(self, path, bucket=bucket, key=key, refresh=refresh, version_id=version_id)

    class _ImmutableWriteCacheTee:
        """Tee writes of immutable manifests into _manifest_bytes_cache.

        PyIceberg writes snap-*.avro and m*.avro via fsspec.open(path, 'wb').
        Seconds later _update_snapshot_cache_from_delta GETs the same files
        to discover which data files the new snapshot added — re-reading
        bytes we just PUT. Stream I, 2026-05-21: this wrapper buffers the
        write bytes alongside the real upload and seeds the LRU on a
        successful close, so the subsequent GETs hit the cache.

        Cache seeding happens only AFTER self._handle.close() succeeds. A
        failed upload must not poison the LRU with bytes that never
        landed in FOS. The buffer is best-effort: any allocation hiccup
        disables tee for this file rather than risking the underlying
        write.
        """

        def __init__(self, handle, path: str):
            self._handle = handle
            self._path = path
            self._buf: bytearray | None = bytearray()
            self._closed = False

        def write(self, data):
            n = self._handle.write(data)
            if data and self._buf is not None:
                try:
                    if isinstance(data, (bytes, bytearray, memoryview)):
                        self._buf.extend(data)
                    else:
                        self._buf.extend(bytes(data))
                except Exception:
                    self._buf = None
            return n

        def close(self):
            if self._closed:
                return
            self._handle.close()
            self._closed = True
            if self._buf:
                try:
                    _cache_put(self._path, bytes(self._buf))
                except Exception:
                    pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.close()
            else:
                try:
                    self._handle.__exit__(exc_type, exc, tb)
                except Exception:
                    pass
                self._closed = True

        def __getattr__(self, name):
            return getattr(self._handle, name)

    def _patched_open(self, path, mode="rb", **kwargs):
        # PyIceberg's FsspecInputFile.open() calls fs.open(...), which goes
        # through _open and returns an S3File whose reads go via
        # _fetch_range, BYPASSING _patched_cat_file entirely. Telemetry on
        # 2026-05-20 showed 3,374 GETs against only 1,122 distinct manifest
        # URLs in a single cron_compact run (3x re-reads per file).
        #
        # Live trace verified that pyiceberg's manifest-plan workflow opens
        # files via _open WITHOUT first calling info() or cat_file (17
        # _open calls, 0 _cat_file calls on a real plan_files run), so the
        # cache must be populated here — not just in _patched_info.
        #
        # We MUST bypass ``self.cat_file`` here. fsspec auto-generates that
        # sync alias from the async ``_cat_file`` method at class definition
        # time via ``sync_wrapper``, which captures the original method
        # reference — so reassigning ``S3FileSystem._cat_file`` does NOT
        # update ``cat_file``. Calling ``self.cat_file(path)`` goes to the
        # wire WITHOUT caching, leaving the LRU empty on the second open()
        # of the same file. Telemetry 2026-05-21 confirmed: m0.avro showed
        # 2.00× ratio (every immutable file fetched twice) because of this.
        # We sync into the iothread and call our patched helper directly so
        # the inflight dedup runs and the bytes land in the LRU.
        if mode == "rb" and _is_immutable_path(path):
            cached = _cache_get(path)
            if cached is None:
                try:
                    cached = _asyn.sync(self.loop, _get_or_fetch_immutable_async, self, path)
                except Exception:
                    # If the sync fetch fails (auth/missing/etc.), fall
                    # back to the original opener so the caller surfaces
                    # the real error rather than an opaque cache miss.
                    return _orig_open(self, path, mode=mode, **kwargs)
            import io as _io

            return _io.BytesIO(cached)
        if "w" in mode and _is_immutable_path(path):
            handle = _orig_open(self, path, mode=mode, **kwargs)
            return _ImmutableWriteCacheTee(handle, path)
        return _orig_open(self, path, mode=mode, **kwargs)

    S3FileSystem._cat_file = _patched_cat_file
    S3FileSystem._info = _patched_info
    S3FileSystem._open = _patched_open

    S3FileSystem.__init__ = _patched_s3fs_init
    S3FileSystem.set_session = _patched_s3fs_set_session
    S3FileSystem._connect = _patched_s3fs_set_session
except ImportError:
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

from backend.core.log_fields import LOG_FIELD_CATALOG

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


def _warehouse_uri(source: dict) -> str:
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

        # Hand the source dict to the s3fs patched __init__ via TWO parallel
        # channels: a ContextVar (covers the main thread / any asyncio task
        # that inherits the context), AND an endpoint-keyed registry (covers
        # PyIceberg's parquet-write thread-pool workers, which don't inherit
        # ContextVars). The patched __init__ tries the ContextVar first, then
        # falls back to the registry.
        _PENDING_FS_SOURCE.set(source)
        _register_proxy_source(source)

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
_pointer_cache: dict[tuple[str, str, str], tuple[float, str | None]] = {}
_pointer_cache_lock = threading.Lock()


def _pointer_cache_key(source: dict, identifier: tuple) -> tuple[str, str, str]:
    namespace, table_name = identifier
    return (source.get("bucket", ""), namespace, table_name)


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
_table_object_cache: dict[tuple[str, str, str], object] = {}
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

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        base_prefix = source.get("prefix", "").strip("/")
        cdn_url = (source.get("cdn_url") or "").rstrip("/")
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


# ---------------------------------------------------------------------------
# Buffer management
# ---------------------------------------------------------------------------


def buffer_files(source: dict) -> list[str]:
    """Return sorted list of Parquet files currently in the local buffer."""
    buf = _buffer_dir(source)
    if not os.path.isdir(buf):
        return []
    return sorted(p for p in _glob.glob(os.path.join(buf, "**", "*.parquet"), recursive=True) if os.path.isfile(p))


_QUARANTINE_SUBDIR = ".quarantine"


def _quarantine_dir(source: dict) -> str:
    """Path to the quarantine bucket for unreadable buffer parquet files.
    Lives under the buffer dir so the path is bucket-scoped and survives
    re-mount of the cache root."""
    return os.path.join(_buffer_dir(source), _QUARANTINE_SUBDIR)


def _quarantine_buffer_file(source: dict, path: str, error: BaseException) -> str | None:
    """Move a corrupt buffer parquet into the quarantine subdir with a
    timestamped name and a sidecar JSON describing the failure.

    Why: without this, ``commit_buffer`` would re-read the same unreadable
    file on every cron tick forever, re-logging the same warning. Quarantine
    keeps the file on disk for human inspection (we never lose data) while
    removing it from the active commit path.

    Returns the new path, or None on failure (in which case the file is left
    in place — quarantine MUST NOT propagate exceptions back to commit_buffer).
    """
    try:
        import json
        from datetime import UTC, datetime

        qdir = _quarantine_dir(source)
        os.makedirs(qdir, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        base = os.path.basename(path)
        new_path = os.path.join(qdir, f"{ts}__{base}")
        # If a same-timestamp collision happens (extreme edge case), append a
        # counter rather than overwriting evidence.
        if os.path.exists(new_path):
            i = 1
            while os.path.exists(f"{new_path}.{i}"):
                i += 1
            new_path = f"{new_path}.{i}"
        os.rename(path, new_path)
        sidecar = new_path + ".json"
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "original_path": path,
                    "quarantined_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                },
                f,
                indent=2,
            )
        logger.error(
            "%s Quarantined unreadable buffer parquet %s -> %s (%s: %s)",
            _ICE,
            path,
            new_path,
            type(error).__name__,
            str(error)[:200],
        )
        return new_path
    except Exception as quarantine_err:
        logger.error(
            "%s Failed to quarantine buffer file %s — leaving in place. Quarantine error: %s",
            _ICE,
            path,
            quarantine_err,
        )
        return None


def buffer_backlog_stats(source: dict) -> dict:
    """Snapshot of the local buffer right now: file count, total bytes, and
    age of the oldest file in seconds.

    Why: a healthy buffer is drained on every commit cycle. If commits start
    failing silently — catalog perms revoked, FOS unreachable, persistent
    schema mismatch — the buffer fills up and the only visible signal is
    growing disk usage. Surfacing oldest_age + file count lets the cron
    summary line shout when the drain is stuck.
    """
    files = buffer_files(source)
    if not files:
        return {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}
    now = time.time()
    total_bytes = 0
    oldest_mtime = now
    oldest_path = files[0]
    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            continue
        total_bytes += st.st_size
        if st.st_mtime < oldest_mtime:
            oldest_mtime = st.st_mtime
            oldest_path = p
    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "oldest_age_seconds": int(max(0, now - oldest_mtime)),
        "oldest_path": oldest_path,
    }


def write_to_buffer(source: dict, arrow_table: pa.Table, filename: str) -> str:
    """Write a PyArrow table to the local buffer as a Parquet file.

    Called by ingest() for each batch of processed rows. The file is written
    with ZSTD level 1 (fast) since it is short-lived hot data.

    Returns the path of the written file.
    """
    buf = _buffer_dir(source)
    os.makedirs(buf, exist_ok=True)
    path = os.path.join(buf, filename)
    aligned = _align_to_schema(arrow_table, source=source)
    pq.write_table(aligned, path, compression="zstd", compression_level=1)
    return path


# Max number of buffer parquets read+concatenated into a single
# table.append() call. At the project's typical row sizes a 50-file chunk
# materializes ~500-800 MB of pyarrow data in memory — large enough to
# amortize commit overhead, small enough to avoid OOM on a cron host with
# limited heap. Overridable via the BUFFER_COMMIT_CHUNK_SIZE env var so a
# user with a large machine + huge backlog can crank it without a deploy.
_BUFFER_COMMIT_CHUNK_SIZE = int(os.environ.get("BUFFER_COMMIT_CHUNK_SIZE", "50") or "50")


def commit_buffer(source: dict, progress_callback=None) -> dict:
    """Append all local buffer files to the Iceberg table.

    Splits the buffer into chunks of ``_BUFFER_COMMIT_CHUNK_SIZE`` files,
    appending each chunk as its own Iceberg snapshot. Why chunked:
      * **Memory bound** — the old code concatenated every buffer file
        into a single in-process pa.Table. At 200+ files this OOM'd the
        commit cron. Chunking caps peak memory at one chunk's worth.
      * **Crash safety** — each chunk that lands becomes a durable
        snapshot, and its files are deleted from the buffer immediately.
        If the process dies mid-loop, the next commit cron picks up the
        un-committed remainder rather than redoing work.

    Returns ``{files_committed, rows_committed, snapshot_id, quarantined_files}``.
    ``snapshot_id`` is the LAST snapshot id produced by the loop (the one
    the metadata pointer now references).
    """
    files = buffer_files(source)
    if not files:
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}

    if progress_callback:
        progress_callback("status", f"Found {len(files)} buffer file(s) to commit")

    table = _init_iceberg_table_locked(source, create=False)
    if not table:
        table = init_iceberg_table(source)

    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        target_arrow_schema = schema_to_pyarrow(table.schema())
    except Exception as e:
        logger.warning(f"[iceberg] Failed to extract arrow schema from iceberg table: {e}")
        target_arrow_schema = None

    # Apply name-mapping once up-front so we don't repeat the check per chunk.
    if "schema.name-mapping.default" not in table.properties:
        if progress_callback:
            progress_callback("status", "Updating table name-mapping...")
        from backend import config as _cfg_mod

        _cfg = _cfg_mod.load_config(source.get("service_id") or source.get("name"))
        _lf_cfg = _cfg.get("log_fields", {}) if _cfg else None
        _mapping = create_mapping_from_schema(get_iceberg_schema(_lf_cfg)).model_dump_json()
        table.transaction().set_properties({"schema.name-mapping.default": _mapping}).commit()

    chunk_size = max(1, _BUFFER_COMMIT_CHUNK_SIZE)
    total_files = len(files)
    total_chunks = (total_files + chunk_size - 1) // chunk_size
    total_rows = 0
    total_committed_paths: list[str] = []
    quarantined_count = 0
    snapshot_id: int | None = None

    for chunk_idx in range(total_chunks):
        chunk_paths = files[chunk_idx * chunk_size : (chunk_idx + 1) * chunk_size]
        if progress_callback:
            progress_callback(
                "status",
                f"Reading chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_paths)} files)...",
            )
        tables: list[pa.Table] = []
        chunk_successful: list[str] = []
        for path in chunk_paths:
            try:
                t = pq.read_table(path)
                tables.append(_align_to_schema(t, target_schema=target_arrow_schema, source=source))
                chunk_successful.append(path)
            except Exception as e:
                _quarantine_buffer_file(source, path, e)
                quarantined_count += 1
        if not tables:
            continue
        combined = pa.concat_tables(tables, promote_options="default")
        chunk_rows = len(combined)
        if progress_callback:
            progress_callback(
                "status",
                f"Appending chunk {chunk_idx + 1}/{total_chunks} ({chunk_rows:,} rows) to Iceberg table in FOS...",
            )
        table.append(combined)
        # Free the chunk's in-memory tables before the next iteration so
        # peak RSS doesn't accumulate across chunks.
        del tables, combined
        snapshot_id = table.current_snapshot().snapshot_id if table.current_snapshot() else snapshot_id
        total_rows += chunk_rows
        # Per-chunk delete: if we crash on a later chunk, the next commit
        # cron only re-processes the un-committed remainder.
        for path in chunk_successful:
            try:
                os.remove(path)
            except Exception:
                pass
        total_committed_paths.extend(chunk_successful)

    if not total_committed_paths:
        return {
            "files_committed": 0,
            "rows_committed": 0,
            "snapshot_id": snapshot_id,
            "quarantined_files": quarantined_count,
        }

    # Cache the post-commit table so the metadata_sync that fires next on this
    # thread (scheduler.py: _run_metadata_sync → init_iceberg_table) reuses it
    # instead of paying another ~865 KB metadata.json GET for the file we
    # just PUT seconds ago. Pointer-mismatch in _load_table_cached protects
    # cross-process correctness.
    _set_cached_table(source, _table_identifier(source), table)

    # Apply the new snapshot's added-files delta to _snapshot_files_cache
    # BEFORE _write_metadata_pointer spawns the async table-summary thread.
    # Order matters: the async thread races straight into _get_cached_or_scan_metadata
    # which reads _manifest_metadata_cache; the delta path pre-seeds that cache for
    # the new manifest, eliminating a redundant ~10 KB .avro GET per commit. Without
    # the swap, the async worker can scan the manifest before the delta seed lands.
    # The delta also avoids the next sync_data's full tbl.scan().plan_files() —
    # re-reading ~1080 immutable manifest files just to find the handful we added.
    try:
        _update_snapshot_cache_from_delta(source, table)
    except Exception as e:
        logger.warning("[iceberg] snapshot cache delta update raised: %s", e)

    _write_metadata_pointer(source, table.metadata_location, table=table)

    if progress_callback:
        progress_callback("status", "Cleaning up local buffer files...")
    _prune_empty_dirs(_buffer_dir(source))

    if quarantined_count:
        logger.warning(
            "%s Committed %d rows from %d buffer file(s) in %d chunk(s); quarantined %d unreadable file(s), snapshot %s",
            _ICE,
            total_rows,
            len(total_committed_paths),
            total_chunks,
            quarantined_count,
            snapshot_id,
        )
    else:
        logger.info(
            "%s Committed %d rows from %d buffer file(s) in %d chunk(s), snapshot %s",
            _ICE,
            total_rows,
            len(total_committed_paths),
            total_chunks,
            snapshot_id,
        )
    return {
        "files_committed": len(total_committed_paths),
        "rows_committed": total_rows,
        "snapshot_id": snapshot_id,
        "quarantined_files": quarantined_count,
    }


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


def optimize_table(source: dict, target_file_size_mb: int = 128, min_files_per_partition: int | None = None) -> dict:
    """Compact small Iceberg data files into larger ones using rewrite_data_files.

    Identifies partitions with too many small files and rewrites them into
    single larger files to maintain metadata health and query performance.

    Args:
      min_files_per_partition: only partitions with strictly more than this
        many files are eligible for compaction. When None (default), the
        threshold is auto-derived from observed file counts so the cron
        self-tunes to traffic volume:

          - Low-traffic site (avg ~3 files/partition): threshold ~2, very
            aggressive — every multi-file partition gets compacted.
          - High-traffic site (avg ~50 files/partition): threshold scales
            up so we don't churn freshly-written files that the next sync
            will append to anyway.

        Pass an explicit number to override (e.g. 1 for a one-shot
        aggressive cleanup on first migration).
    """
    try:
        catalog = _get_catalog(source)
        table = _load_table_cached(source, _table_identifier(source), catalog)
    except Exception as e:
        if "does not exist" in str(e):
            return {"error": "Iceberg table does not exist.", "files_rewritten": 0}
        return {"error": str(e), "files_rewritten": 0}

    # 1. Group files by partition to identify candidates for compaction
    partition_groups: dict[tuple, list] = {}  # partition_values -> [DataFile]

    try:
        for f in table.scan().plan_files():
            # partition is a Record of values like Record[492000]
            # We convert it to a tuple to use as a dict key
            p_val = tuple(f.file.partition)
            if p_val not in partition_groups:
                partition_groups[p_val] = []
            partition_groups[p_val].append(f.file)
    except Exception as e:
        return {"error": f"Failed to scan partitions: {e}", "files_rewritten": 0}

    # Auto-derive threshold from observed file counts when not pinned by the
    # caller. Use the median: robust against outlier hot partitions (e.g. a
    # spike during DDoS) skewing the threshold up. Floor at 2 so we always
    # compact ANY partition with 3+ files; ceiling at 50 to avoid silly
    # numbers from extreme spikes.
    if min_files_per_partition is None:
        sizes = sorted(len(files) for files in partition_groups.values())
        if sizes:
            median = sizes[len(sizes) // 2]
            min_files_per_partition = max(2, min(50, median))
        else:
            min_files_per_partition = 10
        logger.info(
            "🗜️  [optimize] %s: auto-derived threshold=%d (median files/partition=%d across %d partitions)",
            source.get("name"),
            min_files_per_partition,
            sizes[len(sizes) // 2] if sizes else 0,
            len(sizes),
        )

    total_rewritten = 0
    total_added = 0
    partition_errors: list[str] = []
    eligible_partitions = sum(1 for files in partition_groups.values() if len(files) > min_files_per_partition)

    from backend.core.duckdb import get_connection

    # optimize_table only uses DuckDB to read parquet files for partition
    # rewrites; the actual writes happen through PyIceberg's overwrite path.
    # RO + skip-view avoids contending with the writer lock and the view
    # refresh that we don't need here.
    con = get_connection(source, skip_view_update=True, read_only=True)

    try:
        for p_val, files in partition_groups.items():
            if len(files) <= min_files_per_partition:
                continue

            # We want to rewrite these files.
            # We'll use DuckDB to read them and PyIceberg's overwrite logic.
            # But wait, PyIceberg's overwrite() with a filter is the safest way.
            # We need to build a filter for this specific partition.

            # Since we only partition by timestamp_hour (ID 1000):
            hour_val = p_val[0]
            # Convert hour since epoch back to a timestamp for the filter
            from datetime import datetime

            start_ts = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
            end_ts = datetime.fromtimestamp((hour_val + 1) * 3600, tz=UTC)

            # Use DuckDB to read only these files (most efficient)
            paths = [f.file_path for f in files]
            paths_sql = ", ".join(f"'{p}'" for p in paths)

            try:
                # Read into PyArrow. Must materialise to a Table — pyiceberg's
                # overwrite() rejects RecordBatchReader with
                # "Expected PyArrow table". DuckDB 1.5.x's .arrow() now returns
                # a streaming reader, so use to_arrow_table() (or the older
                # fetch_arrow_table() alias) to force materialisation. Skipping
                # this turned every nightly optimize run into a silent no-op
                # — the ValueError got logged as a warning to stderr and the
                # cron recorded success with 0 files rewritten.
                arrow_table = con.execute(
                    f"SELECT * FROM read_parquet([{paths_sql}], hive_partitioning=false)"
                ).to_arrow_table()

                # Perform an atomic overwrite of the specific time range.
                # In Iceberg, this will delete the old files and add the new one.
                table.overwrite(
                    df=arrow_table,
                    overwrite_filter=f"timestamp >= '{start_ts.isoformat()}' AND timestamp < '{end_ts.isoformat()}'",
                )
                _set_cached_table(source, _table_identifier(source), table)
                _write_metadata_pointer(source, table.metadata_location, table=table)

                # File rewrites can't be cleanly delta-tracked (old files are
                # marked DELETED, a new file is ADDED — the cache's prev_files
                # list now contains stale entries). Invalidate so the next
                # sync_data falls into the slow path and rebuilds from scratch.
                _snapshot_files_cache.pop(source.get("name", "default"), None)
                _view_cache.pop(source.get("name", "default"), None)

                total_rewritten += len(files)
                total_added += 1
                logger.info(
                    "🗜️ \x1b[92m[optimize]\x1b[0m %s: Compacted %d files into 1 for hour %d",
                    source.get("name"),
                    len(files),
                    hour_val,
                )

                # Immediately cache the newly rewritten large file
                try:
                    sync_data(source)
                except Exception as e:
                    logger.warning("[iceberg] Failed to eagerly sync data after optimize: %s", e)
            except Exception as e:
                logger.warning("[iceberg] Failed to compact partition %s: %s", p_val, e)
                partition_errors.append(f"partition {p_val}: {type(e).__name__}: {e}")
                continue

    finally:
        con.close()

    result = {"files_rewritten": total_rewritten, "files_added": total_added}
    # Surface partial failures so the cron wrapper can flag them — silent
    # per-partition warnings turned a real regression (pyiceberg rejecting
    # DuckDB's RecordBatchReader from .arrow()) into a week of "Rewrote 0
    # files into 0 files" successes.
    if partition_errors:
        result["partition_errors"] = partition_errors
        result["eligible_partitions"] = eligible_partitions
    return result


def run_cloud_maintenance(source: dict) -> dict:
    """Run weekly maintenance: expire old metadata, delete old data, and purge old local cache.

    1. Deletes log data from Iceberg older than `data_retention_days` (default 30).
    2. Deletes local Parquet files older than `cache_retention_days` (default 90).
    3. Expires Iceberg snapshots older than 7 days to reclaim metadata storage.
    """
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        data_retention_days = int(cron_sync.get("data_retention_days", 30))
        cache_retention_days = int(cron_sync.get("cache_retention_days", 90))

        catalog = _get_catalog(source)
        table = _load_table_cached(source, _table_identifier(source), catalog)
    except Exception as e:
        return {"error": str(e)}

    results = {}

    # 1. Delete old data from Iceberg table
    if data_retention_days > 0:
        data_cutoff_ms = int((datetime.now(UTC) - timedelta(days=data_retention_days)).timestamp() * 1000)
        try:
            # Delete directly from the table using the timestamp column
            from pyiceberg.expressions import LessThan

            table.delete(LessThan("timestamp", (datetime.now(UTC) - timedelta(days=data_retention_days)).isoformat()))
            _set_cached_table(source, _table_identifier(source), table)
            results["data_deleted_before_days"] = data_retention_days
            # Retention delete removes files from the snapshot — the cache's
            # prev_files list would still reference them. Invalidate so the
            # next sync_data rebuilds from a fresh manifest scan.
            _snapshot_files_cache.pop(source.get("name", "default"), None)
            _view_cache.pop(source.get("name", "default"), None)
        except Exception as e:
            logger.warning("[iceberg] Data deletion skipped: %s", e)
            results["data_deletion_error"] = str(e)

    # 2. Expire snapshots (keep last 7 days of metadata)
    keep_snapshot_days = 7
    cutoff_ms = int((datetime.now(UTC) - timedelta(days=keep_snapshot_days)).timestamp() * 1000)
    try:
        table.expire_snapshots().expire_older_than(cutoff_ms).commit()
        _set_cached_table(source, _table_identifier(source), table)
        _write_metadata_pointer(source, table.metadata_location, table=table)
        results["snapshots_expired_before_days"] = keep_snapshot_days
    except Exception as e:
        logger.warning("[iceberg] Snapshot expiry skipped: %s", e)
        results["snapshot_expiry_error"] = str(e)

    # 3. Clean up local cache
    if cache_retention_days > 0:
        try:
            from backend.core.duckdb import _cache_dir

            cache_dir = os.path.join(_cache_dir(source), "data")
            if os.path.exists(cache_dir):
                cache_cutoff = datetime.now(UTC) - timedelta(days=cache_retention_days)
                deleted_files = 0
                for root, _, files in os.walk(cache_dir):
                    for file in files:
                        if not file.endswith(".parquet"):
                            continue
                        filepath = os.path.join(root, file)
                        # Use file modification time as a proxy for file age
                        mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=UTC)
                        if mtime < cache_cutoff:
                            try:
                                os.remove(filepath)
                                deleted_files += 1
                            except Exception:
                                pass
                _prune_empty_dirs(cache_dir)
                results["local_cache_files_deleted"] = deleted_files
        except Exception as e:
            logger.warning("[iceberg] Local cache cleanup skipped: %s", e)
            results["local_cache_error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# DuckDB integration
# ---------------------------------------------------------------------------


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
        con.execute("INSTALL iceberg; INSTALL avro; INSTALL httpfs; INSTALL parquet;")
        con.execute("LOAD iceberg; LOAD avro; LOAD httpfs; LOAD parquet;")
    except Exception:
        try:
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
        mode = (
            "FAST PATH (Local Cache / Buffer Match)"
            if stats.get("was_fast_path")
            else "SLOW PATH (S3 Read / Manifest Resolve)"
        )
        debug_list.insert(
            0, {"sql": f"-- DuckDB Iceberg View Resolution [{mode}] --\n{stats['sql']}", "time_ms": stats["time_ms"]}
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
        try:
            ro_row = con.execute(
                "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
            ).fetchone()
            is_ro = bool(ro_row[0]) if ro_row is not None else False
        except Exception:
            is_ro = False

        exec_sql = view_sql
        if is_ro and view_sql.startswith("CREATE OR REPLACE VIEW "):
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


def update_iceberg_view(con, source: dict, lock_timeout: float = 5.0) -> None:
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
    """
    source_key = source.get("name", "default")

    # Lock-free fast path first. Parallel dashboard reads (6+ endpoints
    # per page load) only need the lock when a real rebuild is required.
    if _try_fast_path_view(con, source):
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

    logger.info("▶️  %s %s: View refresh started.", _ICE_PLAIN, source_key)
    logger.info("%s %s: Refreshing view...", _ICE, source_key)

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
                    if "/data/" in uri:
                        rel_path = uri.split("/data/")[-1]
                    else:
                        rel_path = uri.split("/")[-1]

                    local_path = os.path.abspath(os.path.join(data_dir, rel_path))
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
        parts.append(_strip_computed(f"iceberg_scan('{iceberg_loc}', allow_moved_paths=true)"))
        logger.info(
            "%s Falling back to iceberg_scan for %s (s3_paths=%d, local_iceberg_files=%d).",
            _ICE,
            source_key,
            len(s3_paths),
            len(local_iceberg_files),
        )
    elif s3_paths:
        logger.info(
            "%s Skipping %d missing cloud files in view (local files present, CDN sync pending).",
            _ICE,
            len(s3_paths),
        )

    # Re-check existence: commit_buffer() may have deleted files during the metadata
    # scan above (which can take seconds), causing an IO Error in CREATE VIEW.
    buf_files = [p for p in buf_files if os.path.isfile(p)]

    if buf_files:
        paths_sql = ", ".join(f"'{p}'" for p in buf_files)
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
            where_clauses = []
            if tr.get("start"):
                where_clauses.append(f"timestamp >= '{tr['start']}'::TIMESTAMPTZ")
            if tr.get("end"):
                where_clauses.append(f"timestamp <= '{tr['end']}'::TIMESTAMPTZ")
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
    logger.info("%s %s: View refresh complete (%.0f ms).", _ICE, source_key, duration_ms)
    logger.info("⏹️  %s %s: View refresh finished.", _ICE_PLAIN, source_key)
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
_manifest_metadata_cache: dict[str, tuple] = {}
_manifest_metadata_cache_lock = threading.Lock()
_manifest_metadata_loaded: set[str] = set()
_manifest_metadata_loaded_lock = threading.Lock()


def _load_manifest_metadata_cache(source: dict) -> None:
    """Restore persisted per-manifest aggregates into the in-memory cache.

    Per-manifest aggregates are deterministic functions of an immutable
    manifest .avro, so they survive process restarts. Without this load,
    every restart's first `_get_cached_or_scan_metadata` call cold-scans
    every manifest in the current snapshot — a ~1250-GET burst in the
    steady state.
    """
    source_key = source.get("name", "default")
    with _manifest_metadata_loaded_lock:
        if source_key in _manifest_metadata_loaded:
            return
        _manifest_metadata_loaded.add(source_key)

    import json

    cache_file = _get_cache_file(source, "manifest_metadata_cache.json")
    if not os.path.exists(cache_file):
        return
    try:
        with open(cache_file) as f:
            data = json.load(f)
    except Exception:
        return

    with _manifest_metadata_cache_lock:
        for manifest_path, entry in data.items():
            if manifest_path in _manifest_metadata_cache:
                continue
            try:
                m_calendar = entry.get("calendar") or {}
                m_min_raw = entry.get("min_ts")
                m_max_raw = entry.get("max_ts")
                m_min = datetime.fromisoformat(m_min_raw) if m_min_raw else None
                m_max = datetime.fromisoformat(m_max_raw) if m_max_raw else None
                m_files = int(entry.get("files", 0))
                m_size = int(entry.get("size", 0))
                _manifest_metadata_cache[manifest_path] = (m_calendar, m_min, m_max, m_files, m_size)
            except Exception:
                continue


def _save_manifest_metadata_cache(source: dict, live_manifest_paths: list[str]) -> None:
    """Persist the current snapshot's manifest aggregates to disk.

    Filtering to `live_manifest_paths` prunes manifests dropped by snapshot
    expiry so the file stays bounded by the current snapshot's manifest count.
    """
    import json

    cache_file = _get_cache_file(source, "manifest_metadata_cache.json")
    payload: dict[str, dict] = {}

    with _manifest_metadata_cache_lock:
        for manifest_path in live_manifest_paths:
            entry = _manifest_metadata_cache.get(manifest_path)
            if entry is None:
                continue
            m_calendar, m_min, m_max, m_files, m_size = entry
            payload[manifest_path] = {
                "calendar": m_calendar,
                "min_ts": m_min.isoformat() if m_min else None,
                "max_ts": m_max.isoformat() if m_max else None,
                "files": m_files,
                "size": m_size,
            }

    try:
        tmp = cache_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, cache_file)
    except Exception:
        pass


def _get_scan_lock(source_key: str) -> threading.Lock:
    with _ui_metadata_scan_locks_lock:
        if source_key not in _ui_metadata_scan_locks:
            _ui_metadata_scan_locks[source_key] = threading.Lock()
        return _ui_metadata_scan_locks[source_key]


def _get_cached_or_scan_metadata(source: dict, table) -> tuple[int, int, dict, str | None, str | None]:
    """Scan the Iceberg table for file counts, sizes, calendar, and min/max timestamps.

    Optimized to read manifest files directly rather than planning all data files,
    which is significantly faster.
    """
    source_key = source.get("name", "default")
    metadata_loc = table.metadata_location

    # Check cache by metadata location (version-specific)
    cached = _ui_metadata_cache.get(source_key)
    if cached and cached[0] == metadata_loc:
        return cached[1]

    # Restore persisted per-manifest aggregates before the scan so a
    # post-restart scan only fetches the new manifest, not every manifest.
    _load_manifest_metadata_cache(source)

    # Use a lock to prevent concurrent redundant scans for the same service
    with _get_scan_lock(source_key):
        # Re-check cache inside the lock in case another thread finished the scan while we waited
        cached = _ui_metadata_cache.get(source_key)
        if cached and cached[0] == metadata_loc:
            return cached[1]

        data_files = 0
        size_bytes = 0
        calendar: dict[str, dict] = {}
        min_ts: datetime | None = None
        max_ts: datetime | None = None
        live_manifest_paths: list[str] = []

        t0 = time.time()
        logger.info(
            "%s %s: Scanning table metadata for calendar (location: %s)...",
            _ICE,
            source_key,
            metadata_loc.split("/")[-1],
        )
        try:
            current_snap = table.current_snapshot()
            if current_snap:
                # Quick totals from summary
                data_files = int(current_snap.summary.get("total-data-files", 0))
                size_bytes = int(current_snap.summary.get("total-files-size", 0))

                # Detailed calendar from manifests
                io = table.io

                def scan_manifest(manifest):
                    # Per-manifest cache hit: immutable manifests never change
                    # their entry set, so the previously-computed aggregate
                    # is still correct. Skips the .avro GET entirely.
                    manifest_key = getattr(manifest, "manifest_path", None) or repr(manifest)
                    with _manifest_metadata_cache_lock:
                        cached_agg = _manifest_metadata_cache.get(manifest_key)
                    if cached_agg is not None:
                        return cached_agg

                    m_calendar = {}
                    m_min = None
                    m_max = None
                    m_files = 0
                    m_size = 0

                    manifest_file = manifest.fetch_manifest_entry(io)
                    for entry in manifest_file:
                        if entry.status.name == "DELETED" or not entry.data_file:
                            continue

                        f = entry.data_file
                        m_files += 1
                        m_size += f.file_size_in_bytes

                        # Calendar building via partition values
                        try:
                            # f.partition is a Record. For our spec, field 0 is timestamp_hour
                            hour_val = f.partition[0] if f.partition else None
                            if hour_val is not None:
                                dt = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
                                if m_min is None or dt < m_min:
                                    m_min = dt
                                # Add 1 hour to max_ts if using partition value to cover the full range
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

                    result = (m_calendar, m_min, m_max, m_files, m_size)
                    with _manifest_metadata_cache_lock:
                        _manifest_metadata_cache[manifest_key] = result
                    return result

                manifests = [m for m in current_snap.manifests(io) if m.has_added_files or m.has_existing_files]
                live_manifest_paths = [getattr(m, "manifest_path", None) or repr(m) for m in manifests]

                # Use parallel execution to speed up S3/CDN manifest fetches
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(max_workers=16) as executor:
                    results = list(executor.map(scan_manifest, manifests))

                # Merge results
                total_scanned_files = 0
                total_scanned_size = 0
                for m_cal, m_min, m_max, m_files, m_size in results:
                    total_scanned_files += m_files
                    total_scanned_size += m_size
                    if m_min and (min_ts is None or m_min < min_ts):
                        min_ts = m_min
                    if m_max and (max_ts is None or m_max > max_ts):
                        max_ts = m_max
                    for date_str, stats in m_cal.items():
                        if date_str not in calendar:
                            calendar[date_str] = {"data_files": 0, "size_bytes": 0}
                        calendar[date_str]["data_files"] += stats["data_files"]
                        calendar[date_str]["size_bytes"] += stats["size_bytes"]

                # If summary stats were missing or lower than what we scanned, update them
                if total_scanned_files > data_files:
                    data_files = total_scanned_files
                    size_bytes = total_scanned_size

        except Exception as e:
            logger.warning("[iceberg] %s: Metadata scan failed: %s", source_key, e)

        elapsed = time.time() - t0
        logger.info(
            "%s %s: Metadata scan completed in %.2fs (%d files, %d bytes)",
            _ICE,
            source_key,
            elapsed,
            data_files,
            size_bytes,
        )

        result = (
            data_files,
            size_bytes,
            calendar,
            min_ts.isoformat() if min_ts else None,
            max_ts.isoformat() if max_ts else None,
        )
        _ui_metadata_cache[source_key] = (metadata_loc, result)

        # Persist the current snapshot's manifest aggregates so the next
        # process restart skips the cold scan.
        if live_manifest_paths:
            try:
                _save_manifest_metadata_cache(source, live_manifest_paths)
            except Exception:
                pass

        return result


def get_table_info(source: dict, table=None) -> dict:
    """Return snapshot count, data file count, total size, and latest snapshot time."""
    try:
        if table is None:
            catalog = _get_catalog(source)
            identifier = _table_identifier(source)

            # Ensure our local view of the table is up-to-date with FOS
            _refresh_local_catalog_metadata(catalog, source, identifier)

            table = _load_table_cached(source, identifier, catalog)
    except Exception as e:
        return {
            "error": str(e),
            "snapshots": 0,
            "data_files": 0,
            "size_bytes": 0,
            "table_name": source.get("name", "unknown"),
        }

    snapshots = list(table.snapshots())
    current = table.current_snapshot()

    # Pre-populate total stats from snapshot summary if available (O(1) vs O(N) scan)
    summary_data_files = 0
    summary_size_bytes = 0
    if current:
        summary_data_files = int(current.summary.get("total-data-files", 0))
        summary_size_bytes = int(current.summary.get("total-files-size", 0))

    # Fetch (or scan) for calendar and min/max timestamps
    data_files, size_bytes, _, min_ts, max_ts = _get_cached_or_scan_metadata(source, table)

    # Use the more accurate summary stats if the scan was partial or failed
    if summary_data_files > data_files:
        data_files = summary_data_files
        size_bytes = summary_size_bytes

    latest_ts = None
    if current:
        latest_ts = datetime.fromtimestamp(current.timestamp_ms / 1000, tz=UTC).isoformat()

    buf = buffer_files(source)
    buf_size = sum(os.path.getsize(p) for p in buf if os.path.exists(p))

    return {
        "table_name": source.get("name", "unknown"),
        "snapshots": len(snapshots),
        "data_files": data_files,
        "size_bytes": size_bytes,
        "latest_snapshot_at": latest_ts,
        "buffer_files": len(buf),
        "buffer_size_bytes": buf_size,
        "table_location": table.location() if snapshots else None,
        "region": source.get("region"),
        "min_timestamp": min_ts,
        "max_timestamp": max_ts,
    }


def get_snapshot_calendar(source: dict, table=None) -> dict:
    """Return per-date file counts derived from Iceberg partition metadata."""
    try:
        if table is None:
            catalog = _get_catalog(source)
            identifier = _table_identifier(source)

            _refresh_local_catalog_metadata(catalog, source, identifier)

            table = _load_table_cached(source, identifier, catalog)
    except Exception:
        return {}

    _, _, calendar, _, _ = _get_cached_or_scan_metadata(source, table)
    return calendar


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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
                pass
    pass
