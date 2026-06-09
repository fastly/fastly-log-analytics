# Telemetry Surface Map — Phase 1.1

The 4 telemetry surfaces (5 files, ~2,023 lines) that Phase 1 consolidates onto OpenTelemetry + structlog. Inventory only — replacement strategy is per Phase 1.3–1.5.

---

## Surfaces

### 1. `backend/utils/telemetry.py` (443 lines) — request-scoped tracker

**Owns:**
- `_CALLS: ContextVar[list[dict]]` — per-request call log (API + FOS + CDN ops)
- `_QUERIES: ContextVar[list[dict]]` — per-request DuckDB query log
- `_PROCESS_CONTEXT: ContextVar[str]` — request/cron attribution string ("api:/dashboard/aggregates" / "cron_sync")
- `_ACTIVE_CONTEXTS: list[str]` — process-global stack mirroring ContextVar for iothread/pool readers
- `_LATEST_PROCESS_CONTEXT: str` — top-of-stack mirror for cheap reads
- Public surface: `set_process_context`, `get_process_context`, `get_process_context_with_fallback`, `process_context_scope`, `start_call_tracking`, `get_tracked_calls`, `get_queries`, `record_call`, `track_query`, `tracked_call`, `record_cdn_call`

**Owned phenomena:**
- The `process_context_scope` vs `set_process_context` distinction (surprises log entry #1)
- The iothread mirror (`_LATEST_PROCESS_CONTEXT` + `_ACTIVE_CONTEXTS`) — exists because ContextVars don't propagate to `fsspec.asyn` iothread or `pyiceberg.io.pyarrow` `ThreadPoolExecutor` workers. Patch #6 in MONKEYPATCHES.md fixes the ThreadPoolExecutor case; iothread still relies on the mirror.

### 2. `backend/utils/telemetry_proxy.py` (773 lines) — in-process S3 proxy + usage logger

**Owns:**
- The local HTTP proxy that signs every S3/FOS request with the per-tenant key + records the op into `usage_log` SQLite table
- SigV4 signing helpers
- `_get_session()` / `set_session` for botocore session interception
- The CDN-vs-FOS class-A/B/C classification + per-op byte accounting
- `_flush_log_writes_for_tests()` coalescer (batches usage_log writes to keep WAL contention low)
- Header-passthrough rules: `X-Telemetry-Service-Id`, `X-Telemetry-Context`, `X-Telemetry-Caller`

**Owned phenomena:**
- The cross-tenant `_PROXY_SOURCE_REGISTRY` fallback was an earlier shape of patch #6; current shape is ContextVar propagation via the ThreadPoolExecutor patch (see MONKEYPATCHES.md §6).
- The `TODO(proxy-mem)` for large PUTs (`tech_debt_audit.md`).

### 3. `backend/utils/telemetry_response_middleware.py` (235 lines) — debug-panel renderer

**Owns:**
- The ASGI middleware that intercepts JSON responses and injects `_debug_queries`, `_debug_calls`, `_section_timings`, `_phase_log` fields into them when `DEBUG_RESPONSES` is on
- The JSON-body buffering that lets it splice fields into already-serialized responses without re-running the route handler
- The "strip on responses where DEBUG_RESPONSES is off" gate (lives in `backend/models/common.py` `BaseResponse`)

**Reads from:** surfaces #1 + #2 (via `get_tracked_calls`, `get_queries`)

### 4. `backend/utils/rdns_cache.py` (572 lines) — IP→hostname cache

**Owns:**
- SQLite-backed `rdns_cache.db` (WAL mode, file at `data/cache/rdns_cache.db`)
- Sync `_do_lookup(ip)` — blocking `socket.gethostbyaddr` + forward `socket.getaddrinfo` for FCrDNS verification
- `enrich_batch` / `enrich_batch_gen` — sequential loop over pending IPs, one SQLite transaction per IP
- Discovery pass: scans every DuckDB source for new IPs

**Owned phenomena:**
- THE bottleneck Phase 1.4a fixes: sequential blocking sync lookups in a loop, one SQLite txn per IP (O(N) IO + O(N) lock-acquire).

### 5. Cross-cutting: `_section_timings` / `_phase_log` / `is_cached` on `BaseResponse`

**Owns:** `backend/models/common.py` defines the response-base shape that carries the debug fields. The fields are populated by surface #1 and rendered by surface #3.

---

## What overlaps

```
                     reads from               writes to
                    ┌──────────────┐         ┌─────────────────────┐
record_call() ─────►│ telemetry.py │────────►│  _CALLS ContextVar  │
                    │  (#1)        │         └─────────────────────┘
                    └──────────────┘                  │
                                                      │ read at end-of-request
                                                      ▼
                    ┌──────────────────────────────────────────┐
                    │ telemetry_response_middleware.py (#3)    │
                    │ injects _debug_calls into JSON response  │
                    └──────────────────────────────────────────┘
                              ▲
                              │ also reads
                              │
                    ┌─────────┴──────────┐         ┌─────────────────┐
boto3 S3 ──────────►│ telemetry_proxy.py │────────►│  usage_log (#2) │
                    │   (#2)             │         │   SQLite        │
                    └────────────────────┘         └─────────────────┘
                              │                            ▲
                              │                            │ iothread/pool
                              │  end-of-request augment    │ calls
                              └────────────────────────────┘
                                  (telemetry.py reads
                                   usage_log for ctx-tagged
                                   iothread rows)
```

The overlap that motivates Phase 1:

- **Surface #1 + #3** both maintain "per-request state" — one as ContextVars, one as response-body fields. Both should be views over the same OTel root span attributes.
- **Surface #1 + #2** both attribute work to a process context. Surface #2 writes the attribution to disk for the iothread case; surface #1 reads it back from disk. OTel context propagation (Phase 1) removes the round-trip.
- **The `process_context_scope` / `set_process_context` distinction** exists only because ContextVar reset semantics differ from "fire and forget" semantics. OTel spans carry their own context that closes correctly on `__exit__` regardless of how the span got opened.

---

## Replacement strategy (informational — implemented in Phase 1.3–1.5)

- **`RequestTelemetry`** (new, `backend/core/request_telemetry.py`) owns: the root request span, per-section child spans (`tracer.start_as_current_span("section:dashboard.aggregates")`), call attribution (span events), query attribution (span attributes), thread-wait metric (custom OTel metric, instrumented at `_Pool.acquire`, required by Phase 6 for cron-isolation decision).
- **Surface #1's ContextVars** become OTel context. The iothread mirror (`_LATEST_PROCESS_CONTEXT` / `_ACTIVE_CONTEXTS`) becomes redundant once OTel context propagation lands — drop in Phase 10.3.
- **Surface #2** keeps its proxy + signing logic; emits OTel spans for each S3 op instead of writing usage_log rows for in-request iothread attribution (the usage_log table itself stays — it's also the source for the Admin → Usage Log page).
- **Surface #3** renders the same `_debug_calls` / `_debug_queries` JSON shape from OTel spans (read at end-of-request from the trace). Wire shape preserved; consumers (frontend debug panel) don't change.
- **Surface #4** (rdns_cache.py) replaced per `design_rdns_async.md` — concurrent aiodns + asyncio.gather + aiosqlite single-transaction bulk write.
- **structlog** replaces `logging.getLogger(__name__)` patterns. A custom processor injects active OTel `trace_id` + `span_id` into every log line.

---

## What stays unchanged

- The wire shape of the debug panel response (frontend depends on `_debug_calls`, `_section_timings`, `_phase_log`, `is_cached`, `_debug_queries`).
- The `usage_log` SQLite schema — Admin → Usage Log page consumes it.
- The telemetry-proxy URL signing + per-tenant key routing.
- The CDN class-B/C op classification.
- Patch #6 (ContextVar propagation to `ThreadPoolExecutor` workers) — needed until CPython adds first-class propagation.

---

## Sequencing within Phase 1

Phase 1 ships in this order so each step can ship + verify before the next:

1. **1.1** This document. (done)
2. **1.2** Add deps (opentelemetry-*, structlog, aiodns, aiosqlite). No code touched.
3. **1.3** New `request_telemetry.py` module + configure global tracer/exporter at boot. Existing code untouched; OTel spans flow but nothing consumes them yet.
4. **1.4a** Rewrite rdns_cache.py per design_rdns_async.md. Public API unchanged.
5. **1.4** structlog config module; main.py adoption is opt-in per-logger via `structlog.get_logger(__name__)` calls (existing `logging.getLogger` calls keep working in parallel).
6. **1.5** Debug-panel renderer reads OTel spans alongside ContextVars (additive — both sources merged). Wire shape unchanged.
7. **1.6** Load harness emits OTel-trace JSON (Phase 0.9 CI gate consumes it).
8. **1.7–1.8** Snapshot test + unit coverage on RequestTelemetry + structlog config + new rdns flow.
9. **1.9** mypy strict on the new modules.
10. **1.10** CI cov gate bump to 79.
11. **1.11** Verify dev → deploy → verify prod.

Each step is independently revertable. Subsequent phases (2, 4, 6) get the OTel substrate they need (thread-wait metrics for Phase 6, span context for Phase 2 RequestContext).
