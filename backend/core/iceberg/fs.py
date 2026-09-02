"""s3fs / botocore monkeypatches for FOS-aware Iceberg I/O.

This module exists to carve the s3fs construction-seam patches out of
``backend.core.iceberg._core`` while preserving exact behavior. The patches
install on import side-effect — ``backend.core.iceberg.__init__`` imports
``fs`` BEFORE any other submodule (and before pyiceberg/s3fs are imported)
so the seams are in place by the time pyiceberg ever instantiates an
``S3FileSystem``.

All symbols here are re-exported from the package via ``__init__.py`` for
backwards compatibility with callers and tests that reach in by name
(``_PENDING_FS_SOURCE``, ``_LAST_FS_SOURCE``, ``_orig_s3fs_init``,
``_orig_s3fs_set_session``, ``_orig_cat_file``, ``_orig_info``,
``_orig_open``, ``_manifest_bytes_cache``, etc.).
"""

from __future__ import annotations

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
import os
import threading as _threading

_PENDING_FS_SOURCE: _contextvars.ContextVar[dict | None] = _contextvars.ContextVar("_PENDING_FS_SOURCE", default=None)

# Process-wide fallback for the ContextVar. PyIceberg / aiobotocore create
# new s3fs instances on threads that the ``_patched_submit`` shim above
# can't cover (fsspec's own iothread, asyncio's default executor, lazy
# per-FS-call instantiations). Those threads see ``_PENDING_FS_SOURCE.get()
# == None``, the proxy hook never registers, and every subsequent S3 call
# reaches the proxy without ``X-Fos-Target`` so the proxy 400s silently.
# The 2026-06-09 audit confirmed 68 silent 400s in 6 minutes with
# ``caller-hint=None ua='aiobotocore/...'`` and an empty service-id header
# — strong signal that the hook was missing.
#
# ``_get_catalog`` stamps the latest source it sees into this dict (keyed
# by service name) AND keeps the most-recent value under
# ``_LAST_FS_SOURCE`` as a last-resort fallback. The patched s3fs init
# below now reads ``_PENDING_FS_SOURCE.get() or _LAST_FS_SOURCE`` so the
# hook registers even on hostile threads. Multi-service deployments would
# need the proxy to derive the source from the URL bucket name; today
# this app is single-service in production so the last-source fallback is
# always correct.
_LAST_FS_SOURCE: dict | None = None

# PyIceberg writes parquet data files via concurrent.futures.ThreadPoolExecutor
# in pyiceberg/io/pyarrow.py. ContextVars do NOT propagate to executor workers
# natively in Python 3, so we patch submit() to copy the context. Without this,
# the worker's _PENDING_FS_SOURCE.get() returns None, the proxy hook is never
# registered, and the proxy 400s with "Missing X-Fos-Target header".
import concurrent.futures as _futures

_orig_submit = _futures.ThreadPoolExecutor.submit


def _patched_submit(self, fn, /, *args, **kwargs):
    ctx = _contextvars.copy_context()
    return _orig_submit(self, ctx.run, fn, *args, **kwargs)


# method-assign on a stdlib class — the load-bearing pattern is documented
# in MONKEYPATCHES.md §6 (cross-tenant ContextVar propagation, 2026-06-06
# security audit finding). mypy's method-assign warning is correct in the
# general case but not the right call here.
_futures.ThreadPoolExecutor.submit = _patched_submit  # type: ignore[method-assign]


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
        # asyncio loop thread), NOT the cron thread that entered process_context_scope.
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

    # Contract guard: if s3fs renames any of these slots, our patches would
    # silently no-op on the new name and the proxy hook would never fire.
    # Fail loudly at import so an upgrade is caught in CI, not in prod.
    _REQUIRED_S3FS_SLOTS = ("__init__", "set_session", "_connect", "_cat_file", "_info", "_open")
    for _slot in _REQUIRED_S3FS_SLOTS:
        if not hasattr(S3FileSystem, _slot):
            raise RuntimeError(
                f"s3fs.S3FileSystem.{_slot} missing — FOS monkeypatch contract broken. "
                "Pin s3fs in pyproject.toml or update backend/core/iceberg/fs.py."
            )
    del _slot

    _orig_s3fs_init = S3FileSystem.__init__
    _orig_s3fs_set_session = S3FileSystem.set_session

    def _patched_s3fs_init(self, *args, **kwargs):
        if "config_kwargs" not in kwargs:
            kwargs["config_kwargs"] = {}
        kwargs["config_kwargs"]["request_checksum_calculation"] = "when_required"
        kwargs["config_kwargs"]["retries"] = {"max_attempts": 10, "mode": "standard"}

        from backend.utils import telemetry_proxy as _proxy

        _proxy.start_proxy_server()  # idempotent

        client_kwargs = kwargs.setdefault("client_kwargs", {})
        original_endpoint = client_kwargs.get("endpoint_url") or kwargs.get("endpoint_url") or ""
        # ContextVar covers the main thread, and we patch ThreadPoolExecutor
        # to propagate it to PyIceberg's thread-pool writers. Fallback to the
        # process-wide ``_LAST_FS_SOURCE`` for threads neither path reaches
        # (fsspec iothread, lazy per-FS-call instantiations, asyncio's
        # default executor) — see comment on _LAST_FS_SOURCE for full
        # context.
        source = _PENDING_FS_SOURCE.get() or _LAST_FS_SOURCE or {}
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

    # Contract guard, part 2 (signature drift): the slot-existence check above
    # only catches a slot being renamed/removed. It does NOT catch a parameter
    # being renamed or removed — that passes the hasattr check and then breaks
    # only at the first manifest fetch in prod. Our wrappers forward specific
    # kwargs: _get_or_fetch_immutable_async passes version_id + max_concurrency
    # to the original _cat_file, and _patched_info forwards
    # path/bucket/key/refresh/version_id to the original _info. Verify the saved
    # originals still accept them so a bad s3fs bump fails loudly at import.
    # (Mirrored at the test layer in tests/core/test_iceberg_fs.py.)
    import inspect as _inspect

    for _orig_meth, _required_params in (
        (_orig_cat_file, ("version_id", "max_concurrency")),
        (_orig_info, ("path", "bucket", "key", "refresh", "version_id")),
    ):
        _sig_params = _inspect.signature(_orig_meth).parameters
        for _p in _required_params:
            if _p not in _sig_params:
                raise RuntimeError(
                    f"s3fs.S3FileSystem.{_orig_meth.__name__} no longer accepts {_p!r} — "
                    "FOS monkeypatch contract broken (signature drift). "
                    "Update backend/core/iceberg/fs.py."
                )
    del _orig_meth, _required_params, _sig_params, _p
except ImportError:
    pass
