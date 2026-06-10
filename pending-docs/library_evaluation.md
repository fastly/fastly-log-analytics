# Library Evaluation — Final Summary (Phase 10.11)

Tracks spike outcomes for libraries the cleanup plan flagged as "evaluate, then adopt if clear win." Each entry: what the spike tried, what it measured, the verdict.

Status legend:
- 🟢 **adopt** — clear net win, shipped in the named phase
- 🟡 **partial** — adopted for some surfaces, custom code wins for others
- 🔴 **skip** — custom code stays, reason documented
- ⏳ **deferred** — spike not yet run (decision deferred to a future phase with a documented trigger)

---

## Spike-style evaluations

### fastly SDK (`pip install fastly`)

**Phase:** 7 (field registry + provision spike)

**Hypothesis:** the official `fastly` SDK could replace large parts of [backend/provision/fastly_api.py](../backend/provision/fastly_api.py) (1,214 lines) with less code and equivalent edge-case handling.

**Spike target:** one workflow — VCL snippet upload (`create_vcl_snippet`, `update_vcl_snippet`, `delete_vcl_snippet`).

**Verdict:** ⏳ **deferred.** No spike was executed in this cleanup window. The custom client handles the real edge cases (shield-map, conditions, dependency-ordered upserts) and tenacity has already replaced the ad-hoc retry loops, so the spike's likely upside has shrunk. Decision deferred to a future phase; the spike re-opens if a new provisioning workflow needs a Fastly API surface the custom client doesn't already cover.

**Trigger to re-open:** any new Fastly endpoint family the custom client doesn't already wrap, OR a Fastly API auth/transport change that would require non-trivial custom-client work.

**Lines saved:** 0.

---

### APScheduler v4 (alpha)

**Phase:** 6 (cron isolation)

**Hypothesis:** APScheduler v4 supports separate-process scheduling natively. If Phase 6.1 picked separate-process based on Phase 1 thread-wait data, v4 would replace custom IPC plumbing.

**Spike trigger:** ONLY if Phase 6.1 picked separate-process. The trigger never fired.

**Verdict:** ⏳ **deferred.** Pool-vs-process is decided on Phase 1 OTel thread-wait data; the current production deploy hasn't yet collected enough wait-time samples under representative cron load to force the separate-process branch. Until that telemetry says otherwise, v3 + the carved [`backend/cron/`](../backend/cron/) package is fine. v4 is still alpha; adopting it speculatively would import alpha-stability risk we don't need.

**Trigger to re-open:** Phase 1 OTel thread-wait p95 during cron windows ≥ 50 ms sustained, OR a real cron job that needs cross-process isolation we can't get with the current single-process scheduler.

**Lines saved:** 0.

---

## Adopted libraries

Verdicts for the libraries the plan flagged as "adopted at plan level" (not spikes — these landed without a comparative spike).

| Library | Phase | Status | Replaces / surfaces touched | LOC saved (approx.) |
|---|---|---|---|---|
| **OpenTelemetry** (api + sdk + fastapi + botocore + aiohttp-client instrumentors) | 1 | 🟢 adopt | Replaced ~600 lines of bespoke telemetry plumbing in `backend/utils/telemetry.py` + contextvar mirrors. Spans now emit through the OTel tracer; `_section_timings` / `_debug_queries` are rendered from span attributes on the response path so the debug panel keeps its shape. | ~600 |
| **structlog** | 1 | 🟢 adopt | Replaced scattered `logger.info("%s ...", a)` patterns with structured key-value events. Custom processor injects active OTel `trace_id` / `span_id` into every log line. | ~150 |
| **aiodns** | 1 | 🟢 adopt | Re-architected `backend/utils/rdns_cache.py` to do concurrent async DNS resolution with FCrDNS verification + a single-transaction bulk SQLite write. Eliminated the per-IP sequential lookup loop that was blocking the sync worker for minutes at a time. | ~100 |
| **aiosqlite** | 1 | 🟢 adopt (scoped) | Scoped to `backend/utils/rdns_cache.py` only — the rdns flow is already async via aiodns + `asyncio.gather`, so aiosqlite is a natural fit there. Everywhere else stays on sync `sqlite3` (FastAPI's threadpool already keeps the calls off the event loop). | n/a (enables the aiodns flow) |
| **tenacity** | 3 | 🟢 adopt | Declarative retry decorators replaced fragmented custom try/except loops in `backend/provision/fastly_api.py`, `backend/utils/ngwaf.py`, and the SQLite write paths (`@sync_db_retry` policy for `OperationalError` busy/locked under WAL contention). | ~100 |
| **pydantic-settings** | 3 | 🟢 adopt | `backend/core/settings.py` collapses scattered `os.environ.get("FOO", default)` reads into a single `Settings(BaseSettings)` class. Required-in-prod env vars (`TRUSTED_PROXY_IPS`) become pydantic validators. | ~100 |
| **argon2-cffi** | 10 (share_db carve-up) | 🟢 adopt | `backend/core/share_db/passcode.py` hashes new invite passcodes with argon2id (2026 OWASP recommendation). Legacy scrypt hashes still verify; the next successful login rehashes them transparently. | n/a (security upgrade) |
| **rich + typer** | 10 | 🟢 adopt | Replaced `backend/provision/utils.py` ANSI helpers (`BOLD/_c/fail/info/ok/warn`) with `rich.console.Console`; wrapped `backend/provision/cli.py` handlers as typer subcommands so `python -m backend.provision.cli --help` is real. | ~150 |
| **httpx (everywhere)** | 10 | 🟢 adopt | aiohttp dropped from non-proxy paths; only `backend/utils/telemetry_proxy.py` keeps aiohttp (it's a server). | ~50 |

### Adopted but not counted in the spike list

These were named in the plan but landed alongside the surfaces they touched without a comparative spike:

- **cachetools** — Phase 5b "opportunistic" adoption. The carve-up surfaced that the existing bounded/TTL caches in `backend/utils/bounded_cache.py`, `backend/utils/rdns_cache.py`, and `backend/utils/ngwaf_bot_cache.py` already had tight, behavior-correct hand-rolled LRU/TTL semantics. The behavior-preserving swap to `cachetools.LRUCache` / `TTLCache` was deferred — the custom code is small, well-tested, and the swap is mechanical when needed. **Verdict:** 🔴 skip in v2.0 (no net win); revisit if the cache surface grows.
- **orjson + FastAPI `ORJSONResponse`** — Phase 8 plan-level adoption. Not landed in this window; FastAPI's default JSON encoder is fine for current payload sizes and switching is a one-line change when a composite endpoint payload starts costing measurable CPU on the response path. **Verdict:** 🔴 skip in v2.0; revisit if a real composite endpoint's serialization shows up on the OTel hot path.

### Explicitly skipped (recorded for completeness)

- **msgspec** — overlaps with orjson + Pydantic v2 + DuckDB; no remaining hot path that benefits.
- **anyio direct dep** — already transitive via FastAPI / Starlette; `asyncio.to_thread` / `concurrent.futures.ProcessPoolExecutor` cover our needs.
- **sqlglot** — DuckDB's own `json_serialize_sql` is more correct for DuckDB-specific SQL.
- **alembic** — overkill for single-file SQLite metadata DBs; the in-repo `backend/core/sqlite_migrations.py` framework stays.
- **Schemathesis** — existing openapi.json → tsc check is enough for a solo dev.

---

## Total LOC saved by library swaps

Approximate total: **~1,250 lines** removed from the backend in service of replacing custom plumbing with battle-tested libraries. That's below the cleanup plan's ≥ 1,600 success-criteria target — the gap is largely the deferred cachetools (~300) and orjson (no LOC win, just perf) adoptions plus the deferred fastly-SDK spike (~200 if it had landed). The shipped wins still cleared the largest single chunk (OTel telemetry at ~600 lines) and the asynchronous rDNS bottleneck (the highest-value qualitative win, not measured as LOC).
