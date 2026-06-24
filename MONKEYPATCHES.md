# Monkeypatch Inventory & Elimination TODO

This file catalogs every third-party class/function we monkeypatch at import
time so we can audit, justify, and eventually replace them with cleaner
abstractions (subclasses, fsspec hooks, custom catalogs, etc.).

All patches today live in [backend/core/iceberg/fs.py](backend/core/iceberg/fs.py).
Five patches form a single **s3fs cache + telemetry-proxy** category, all
behind a single `try: ... except ImportError` block
([fs.py:168-529](backend/core/iceberg/fs.py#L168-L529)). One additional
**stdlib** patch (`ThreadPoolExecutor.submit`) propagates ContextVars to
worker threads so cross-tenant proxy routing stays correct.

A boot-time contract guard ([fs.py:175-182](backend/core/iceberg/fs.py#L175-L182))
verifies the s3fs slots we monkey with still exist — if a future s3fs
release renames any of `__init__`, `set_session`, `_connect`,
`_cat_file`, `_info`, or `_open`, boot fails loudly naming the missing
slot instead of silently leaving the proxy hook unregistered.

A preemptive `from backend.core.iceberg import fs as _iceberg_fs_patches`
at the top of [backend/main.py](backend/main.py) installs the patches
before any other backend import can trigger lazy s3fs/pyiceberg loading.

(A sixth `SqlCatalog.load_table` patch lived here until 2026-05-21; it has
been replaced by a clean `FosSqlCatalog` subclass — see the "Replaced patches"
section at the bottom.)

Each entry below records the patched target, the line, what it does, why we
needed it (with the telemetry incident that motivated it where applicable),
and the cleanup path.

---

## 1. `S3FileSystem.__init__`

- **Site:** [fs.py:187, 188-221, 501](backend/core/iceberg/fs.py#L187)
- **What:** Forces `request_checksum_calculation=when_required`, routes the
  endpoint through the in-process telemetry proxy, switches signing to
  `UNSIGNED` (the proxy re-signs), and stashes proxy-context attributes
  (`_fos_proxy_source`, `_fos_proxy_target`, `_fos_proxy_cdn_target`) on the
  instance so the later `set_session` patch can wire up event hooks.
- **Why:** Every s3fs instance pyiceberg creates must transparently transit the
  telemetry proxy without callers (or pyiceberg) knowing. We can't subclass
  because pyiceberg itself instantiates `S3FileSystem` directly.
- **Cleanup:** Introduce a `FosS3FileSystem(S3FileSystem)` subclass and use
  pyiceberg's `FileIO` registration to inject it (`pyiceberg.io.PY_IO_IMPL` /
  `FileIO.from_location`). Until pyiceberg exposes a stable hook for fsspec
  instance construction, monkeypatch is the only path.

## 2. `S3FileSystem.set_session` (and `S3FileSystem._connect`)

- **Site:** [fs.py:223, 224-289, 502-503](backend/core/iceberg/fs.py#L223)
- **What:** Wraps the async session bootstrap and re-registers our
  `before-send.s3.*` event handler on the (possibly recreated) underlying
  botocore client.
- **Why:** s3fs caches `_s3` and may recreate it on session refresh; botocore
  event handlers attach to the client, not the filesystem. We re-register on
  every refresh because botocore dedupes internally.
- **Cleanup:** Same subclass strategy as #1 — override `set_session` cleanly.

## 3. `S3FileSystem._cat_file`

- **Site:** [fs.py:291, 367-373, 497](backend/core/iceberg/fs.py#L367)
- **What:** Adds an immutable-bytes LRU cache + in-flight async dedup for
  `*.avro` / `*.metadata.json` reads, and forces `max_concurrency=1` on the
  underlying call.
- **Why (telemetry):**
  - 2026-05-20: pyiceberg re-read 1,104 distinct manifests ~470× each (517K
    reads, 2.4 GB CDN egress) in 13 hours. Cache fixes the redundant reads.
  - 2026-05-21: `max_concurrency=10` (s3fs default) triggers a probe GET +
    real GET for any non-range read — 1242 helper calls → 2485 proxy GETs
    (2.00× ratio). `max_concurrency=1` skips the probe path and restores
    1.00×.
  - 2026-05-21: aiobotocore disconnect mid-stream cancels the awaiter; we
    wrap the shared fetch Task with `asyncio.shield` so the LRU still gets
    populated.
- **Cleanup:** Push the LRU into a dedicated `CachedS3FileSystem` subclass.
  The `max_concurrency=1` part is harder — it requires either upstream s3fs
  changing the default behavior or us always going through our own helper.

## 4. `S3FileSystem._info`

- **Site:** [fs.py:292, 375-393, 498](backend/core/iceberg/fs.py#L375)
- **What:** For immutable paths, synthesize the info dict from cached bytes
  if present (skip the HEAD round-trip).
- **Why:** Without this, `FsspecInputFile.__len__()` issues a HEAD even when
  we have the bytes cached. The earlier "pre-emptively GET in info()" attempt
  caused ~89% of m0.avro reads to disconnect mid-stream
  (`ClientConnectionResetError: Cannot write to closing transport`) and left
  the cache empty; current shape is HEAD-on-miss, synthesize-on-hit.
- **Cleanup:** Subclass.

## 5. `S3FileSystem._open`

- **Site:** [fs.py:293, 457-495, 499](backend/core/iceberg/fs.py#L457)
- **What:** For immutable reads, hits the LRU first; on miss, synchronizes
  into fsspec's iothread and calls our cached `_get_or_fetch_immutable_async`
  helper directly (bypassing `self.cat_file`, which is the auto-generated
  sync wrapper that captured the *original* unpatched `_cat_file`). For
  immutable **writes** (Stream I, 2026-05-21), wraps the underlying write
  handle in `_ImmutableWriteCacheTee` so the bytes are tee'd into the LRU
  on a successful upload close — the subsequent
  `_update_snapshot_cache_from_delta` GET reads the same bytes from cache
  instead of re-fetching them from FOS.
- **Why (read):** PyIceberg's manifest-plan workflow opens files via `_open`
  without calling `info()` or `cat_file()` first (17 `_open` calls, 0
  `_cat_file` calls on a real `plan_files` run on 2026-05-20). If we only
  patch `_cat_file`, manifest reads never populate the LRU. The
  `self.cat_file` trap is documented inline at
  [fs.py:469-478](backend/core/iceberg/fs.py#L469).
- **Why (write — Stream I):** Every `table.append()` writes one snap-*.avro
  (~64 KB) and one m*.avro (~10 KB) which `_update_snapshot_cache_from_delta`
  GETs back seconds later — pyiceberg has no API to hand back the
  just-written bytes. Tee-on-write seeds the LRU so the read is a cache
  hit. Per-commit savings: ~74 KB. Failure-mode safety: the LRU is seeded
  only after the upload `close()` succeeds, so a failed upload can't
  poison the cache with bytes the cloud never received.
- **Cleanup:** Subclass + override.

---

## Elimination strategy

**Short-term** (low risk): Each patch already includes a paragraph-long
incident postmortem inline. Keep those comments accurate — they're how the
next reader (human or AI) understands whether the patch is still needed.

**Medium-term (investigated 2026-05-21, NOT worth doing yet)**: We
considered collapsing #3-#5 into a `CachedFosS3FileSystem(S3FileSystem)`
subclass injected via a custom `FosFsspecFileIO(FsspecFileIO)` registered
through pyiceberg's `py-io-impl` property. Reading
[pyiceberg/io/fsspec.py:168-220](../.venv/lib/python3.13/site-packages/pyiceberg/io/fsspec.py)
shows pyiceberg's `_s3()` factory constructs `S3FileSystem(**kwargs)`
directly with ~50 lines of signing/retry/endpoint setup. The three
extraction paths all lose:
1. Monkeypatch `pyiceberg.io.fsspec._s3` — adds a patch instead of removing one.
2. Custom FsspecFileIO subclass with replaced `_scheme_to_fs["s3"]` factory
   — requires mirroring pyiceberg's `_s3()` body, which churns across
   pyiceberg releases (maintenance trap).
3. Class-swap (`fs.__class__ = CachedFosS3FileSystem`) post-construction
   — works in Python but `__init__` already ran on the parent; dirtier
   than the current monkeypatch.

Conclusion: the 5-patch block is structurally optimal until pyiceberg
upstream adds a "supply your own FileSystem class" hook. Revisit when
that lands.

**Long-term**: If pyiceberg upstream gains a "supply your own FileSystem"
hook (there's discussion in the project), all five remaining patches become
obsolete.

---

## 6. `concurrent.futures.ThreadPoolExecutor.submit`

- **Site:** [fs.py:61-73](backend/core/iceberg/fs.py#L61) (top-level, runs
  at module import — does NOT live behind the s3fs `try: ... except
  ImportError` block).
- **What:** Wraps `submit(fn, *args, **kwargs)` so the worker thread runs
  `fn` inside `contextvars.copy_context()` instead of an empty context.
  All other behavior (Future return, error propagation, cancellation) is
  unchanged.
- **Why (security incident, audit finding 003, 2026-06-06):** PyIceberg
  writes parquet data files via a `ThreadPoolExecutor` inside
  `pyiceberg/io/pyarrow.py`. The s3fs `__init__` patch (#1) reads
  `_PENDING_FS_SOURCE` (a ContextVar set by `_get_catalog`) to discover
  which tenant's source/CDN/proxy config to use. ContextVars do NOT
  propagate to executor workers natively — PEP 567 covers asyncio tasks
  only. The previous fix was an endpoint-keyed global registry
  (`_PROXY_SOURCE_REGISTRY`) that worker threads queried as a fallback.
  That registry was vulnerable to cross-tenant overwrite: if two tenants
  shared an endpoint URL, the second `_get_catalog` overwrote the first
  tenant's source, and the first tenant's still-running worker threads
  resolved the wrong source — wrong CDN target, wrong `x-fastly-key`,
  wrong `X-Telemetry-Service-Id`. This patch eliminates the registry by
  making ContextVars propagate the way they propagate for asyncio.
- **Scope of effect:** GLOBAL — affects every `ThreadPoolExecutor` in the
  process, not just pyiceberg's. The semantic change is benign for all
  known callers (FastAPI, aiobotocore, etc.) because submitting work with
  the caller's ContextVars is the more-defensive default and matches
  asyncio's `loop.run_in_executor` semantics. Workers that previously saw
  empty ContextVars now see the submitter's context.
- **Cleanup:** Remove if/when CPython adds first-class context propagation
  to `concurrent.futures` (proposals exist) or if PyIceberg switches to
  asyncio for parquet writes. A narrower alternative — injecting a
  `ContextVarPropagatingThreadPoolExecutor` into PyIceberg's writer pool
  only — is preferable to the process-wide patch but is contingent on
  PyIceberg exposing an executor-injection hook (none today). Until one
  of those lands, the global patch is the smallest correct fix.

---

## Replaced patches

### `SqlCatalog.load_table` → `FosSqlCatalog` subclass (Stream H, 2026-05-21)

Originally installed as a monkeypatch by `_install_cached_sql_load_table()`,
this was replaced the same day by a clean `FosSqlCatalog(SqlCatalog)`
subclass returned by `_get_fos_catalog_class()`
([_core.py:428-470](backend/core/iceberg/_core.py#L428-L470)). `_get_catalog()`
instantiates the subclass instead of `SqlCatalog` directly, so pyiceberg's
internal `commit_table.load_table` call dispatches to the subclass override
that consults `_table_object_cache`. The subclass is built lazily on first
catalog construction with a base-class identity check, so test fixtures that
swap `pyiceberg.catalog.sql.SqlCatalog` (e.g.
[tests/core/test_endpoint_routing.py](tests/core/test_endpoint_routing.py))
still receive a subclass of *their* stub.

Behavior is identical to the original patch (same cache lookup, same
fall-through counter, same correctness invariant). Memory savings per
commit: ~865 KB metadata.json GET eliminated. Documented for posterity so
git-blame for the old `_install_cached_sql_load_table` symbol resolves to
this section.

## How to add an entry

When you add a new monkeypatch:

1. Add the patch with an inline comment block explaining **what telemetry
   incident** justified it (date + 1-2 sentence symptom).
2. Add an entry here with site/what/why/cleanup.
3. Cross-link from the patch site to the relevant section header in this
   file (`# 6. SqlCatalog.load_table` etc.).

When you remove a monkeypatch: delete the entry, don't leave a tombstone.
