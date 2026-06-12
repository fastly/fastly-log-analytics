# Fastly Log Analytics — Architecture Cleanup Plan

> **Note on `pending-docs/`:** Files in this directory are working artifacts for the v2.0 cleanup effort. They get committed to the `refactor/cleanup` branch as we go so progress is reviewable, then **deleted before the squash-merge to main**. Nothing in `pending-docs/` should appear in main's git history after v2.0 ships.

## Context

The `performance-improvement` branch (129 commits, +17,886/-2,561 across 159 files) closed the most painful read-path latency issues but did so by stacking remediation on top of an architecture that wasn't designed for the workload. The cleanup that work made visible — but did not address — is the subject of this plan.

### What the perf branch made visible

- **Telemetry was Phase 1 of the perf remediation, not Phase 0 of the product.** A query-heavy analytics app shipped without per-section timings, query attribution, or a load harness. Every regression became archaeology.
- **Three query layers** (`backend/routers/` → `backend/repositories/` → `backend/core/`) with SQL leaking across all three. Single largest file is `backend/core/iceberg.py` at 4,232 lines.
- **Five storage tiers stitched together** (live buffer, Parquet, Iceberg, local compaction, rollups). The F3 wedge (Iceberg view-rebuild holding `_Pool.acquire`'s `_cond` lock) was a layering bug between two subsystems that should never have shared a lock.
- **Composite endpoints landed additive.** The granular endpoints they replace are still wired into the UI (see `pending-docs/performance_remediation_remaining.md §1`).
- **Tenancy was retrofitted, not designed.** Writer contention between cron and API, cross-tenant remediation, service-scope desync on path-params, per-service slug rollup view names, and a security-load-bearing private attribute in [deps.py:84](../backend/deps.py#L84) that exists because FastAPI converts primitive-typed dep params into query params.
- **Middleware ordering keeps biting.** [main.py:434-501](../backend/main.py#L434-L501) carries paragraph-long comments documenting a 2026-06-09 audit. No invariant test.
- **Frontend hydration/preload is a war of attrition.** Hidden-Plotly pre-warm, MapLibre `styledata` swap, LazyMount `visible=false`, per-page modulepreload vs dynamic import, route prefetch chips — all symptoms of not having decided RSC/CSR/code-split up front.

### Decisions taken in planning round

- **Compat posture:** Phases 0–7 non-breaking. **Phase 8 is a hard cutover** (no deprecation shims) — backend granular endpoints + composites + frontend swap all ship in one deploy, with 24–48h advance notice (CHANGELOG + README migration section + direct outreach to any known external integrators). Single `v2.0.0` tag at end of Phase 10.
- **Cadence:** AI-driven execution at sessions-per-phase, not days-per-phase. Phase-by-phase ship to prod with smoke-test + monitoring-clean gates (no multi-day soaks). Goal: full refactor in **~2–5 calendar days**, not weeks.
- **Scope:** Backend + frontend. Includes `backend/scoring/` Python (reference impl); excludes `compute/` (Rust/Wasm production scorer).
- **Success criteria (all four):** latency targets held, backend LOC drops materially, new-feature touch-count drops, operational surface shrinks. **Zero tech debt at end** — no `post_v2_followups.md`, no `TODO: split` markers left behind.
- **Test strategy:** Per-phase prune-as-we-go. Every phase that touches a module reviews its tests (delete dead, fix flaky, rewrite stale, add coverage for new abstractions). Test changes ship in same commit as code.
- **Coverage commitment:** Full unit coverage (~80%) of every touched module per phase.
- **ADR location:** `pending-docs/adr/` (per `local-only-docs` + `public-comms-style` memories).
- **v1 maintenance:** Hotfix on the cleanup branch and deploy immediately. The in-flight phase pauses, hotfix ships, then phase resumes.
- **Perf branch:** `performance-improvement` merges to main **before** Phase 0 begins. Cleanup branch forks from post-merge main.
- **Bake time:** No mandatory soaks. After each phase deploy: run smoke test, verify OTel dashboards stay clean for ~15 min, then advance. Hours not days.
- **Admin access:** Stays as **SSH-port-forward + `frontend/proxy.ts` X-Proxied-By-Caddy gate**. SSH proves machine ownership at the network layer (strongest primitive). No SSH-signed-challenge → session token (it would trade convenience for security regression). No changes to admin auth in this refactor.
- **Analyst sharing:** **Direct-mode only.** Delete ~400 lines of SSH-to-localhost.run + sleep-listener + reconnect logic in tunnel.py. Production uses `direct_mode` against the Fastly+Caddy public URL; the laptop-admin SSH-tunnel use case is not supported in v2.0.
- **Deployment portability:** VM-agnostic — runs on any Linux VM with Docker (AWS EC2, GCE, Azure VM, Linode, DigitalOcean, bare metal). **Storage stays Fastly Object Storage** (S3-compatible API; boto3 keeps working). CDN stays Fastly. Phase 0 grep-audit for hardcoded GCE assumptions; deploy runbooks for each major cloud VM platform.
- **Surprises log:** `pending-docs/surprises.md` — open at Phase 0, append any undocumented gotcha or non-obvious design choice as it surfaces during execution. Each entry: what surprised, what it broke (or nearly broke), the corrected mental model. Drives ad-hoc plan amendments at phase boundaries.
- **Library swaps (adopted at plan level):**
  - **OpenTelemetry** (Python SDK + FastAPI/botocore/aiohttp instrumentors) — Phase 1 builds on OTel rather than a custom collector; replaces ~600 lines of [telemetry.py](../backend/utils/telemetry.py) plumbing
  - **structlog** — Phase 1 alongside OTel; replaces scattered `logger.info("%s ...", a)` patterns
  - **aiodns** — Phase 1; implements high-concurrency async parallel reverse DNS and FCrDNS resolution in [rdns_cache.py](../backend/utils/rdns_cache.py) using `asyncio.gather()` and single-transaction SQLite bulk-writes, completely eliminating sequential lookups blocking the sync worker (saves ~100 lines and fixes a major bottleneck).
  - **tenacity** — Phase 3; provides standardized, declarative, highly configurable retry decorators for Fastly API and NGWAF requests, replacing fragmented custom try-except retry loops (saves ~100 lines of manual error recovery boilerplates).
  - **cachetools** — Phase 5b opportunistic; replaces [bounded_cache.py](../backend/utils/bounded_cache.py), [rdns_cache.py](../backend/utils/rdns_cache.py), [ngwaf_bot_cache.py](../backend/utils/ngwaf_bot_cache.py)
  - **structured .tf.json generation** — Phase 5b; replaces programmatic string-concatenation in [terraform_gen.py](../backend/utils/terraform_gen.py) and custom regex-based `_hcl_escape` sanitization with standardized Terraform JSON configurations, completely eliminating the custom-HCL escaping injection vector and deleting 200+ lines of fragile templates.
  - **pydantic-settings** — Phase 3; replaces scattered env-var reads
  - **nuqs (Next.js use-query-state)** — Phase 9; hooks URL query parameters as the single source of truth for frontend dynamic filters, active service selection, and active time-windows, replacing custom store-syncing useEffect patterns and eliminating hydration desync (saves ~150 lines of complex Zustand/Effect orchestration).
  - **rich + typer** — Phase 10; replaces custom CLI printers in [provision/utils.py](../backend/provision/utils.py) and [provision/cli.py](../backend/provision/cli.py)
  - **httpx everywhere** — Phase 10; drop aiohttp from non-proxy paths (it stays in `telemetry_proxy.py` as a server)
  - **orjson + FastAPI `ORJSONResponse`** — Phase 8; 5–10× faster JSON serialization on composite endpoint payloads. Drop-in replacement, ~10 lines of change for major perf win.
  - **react-hook-form + zod** — Phase 9b; for the ProvisionWizard.tsx split. RHF + zod schemas replace hundreds of lines of useState/useEffect form-state soup and add runtime validation.
  - **argon2-cffi** — Phase 10 share_db carveup; replaces scrypt for passcode hashing (argon2id is the 2026 OWASP-recommended algorithm). Tiny change in the carved-out `share_db/passcode.py`.
  - **freezegun** (or **time-machine**) — Phase 10; freeze-time for tunnel + share_db session timeout / TTL tests.
  - **pytest-httpx** — Phase 10; cleaner mocking for httpx calls once aiohttp is dropped from non-proxy paths. Complements vcrpy.
  - **pytest-benchmark** — Phase 0; structured perf-regression tracking for the load-harness CI gate.
  - **aiosqlite** — Phase 1; **scoped to `rdns_cache.py` only** (the rdns flow is already async via aiodns + asyncio.gather, so aiosqlite is a natural fit there). Metadata mixins, ngwaf_bot_cache, and share_db stay sync — FastAPI's threadpool already keeps sync sqlite3 calls off the event loop. Any future async route needing metadata uses `await asyncio.to_thread(con.execute, ...)`.
  - **msw** (Mock Service Worker) — Phase 9b; (already in `devDependencies`) standardizes Vitest tests to mock network requests at the Service Worker level rather than raw fetch mocks.


- **No SaaS dependencies rule:** Every library above is pure open-source / pip-or-npm installable / runs in-process. No external SaaS subscriptions required (Fastly is the one allowed exception — and that's already the storage + CDN). Specifically:
  - **OTel ships with console exporter only.** Adding Jaeger / Tempo / Honeycomb / Grafana Cloud / Datadog / etc. is a deploy-config decision **post-v2.0**, not part of this plan.
  - **No Sentry, no LogRocket, no Bugsnag** — error tracking stays via structured logs + OTel spans.
  - **No SaaS auth provider** — admin SSH-port-forward (network-layer) stays the auth model.
  - **No managed tunnel service** — SSH-to-localhost.run gets deleted in Phase 10; no Cloudflare Tunnel / Tailscale / ngrok replacement.
- **Library spikes (evaluation, then adopt-if-clear):**
  - **Official `fastly` SDK** — Phase 7 spike; if it can replace parts of [provision/fastly_api.py](../backend/provision/fastly_api.py) (1,214 lines) cleanly, adopt. Result logged in `pending-docs/library_evaluation.md`.
  - **APScheduler v4 (alpha)** — Phase 6 spike, only if Phase 6 picks separate-process isolation; v4 supports separate-process scheduling natively.
- **Library swaps explicitly skipped:** sqlglot (DuckDB's own `json_serialize_sql` is more correct for our SQL); alembic (overkill for single-file SQLite metadata DBs); Schemathesis (existing openapi.json → tsc check is enough for solo dev); **msgspec** (overlaps with orjson + Pydantic v2 + DuckDB — no remaining hot path that benefits); **anyio direct dep** (already transitive via FastAPI/Starlette; `asyncio.to_thread` / `concurrent.futures.ProcessPoolExecutor` are stdlib equivalents — adopt only if Phase 1 telemetry shows real Python-side GIL contention).
- **Cross-cutting workstream — mypy ratchet:** [pyproject.toml:105-122](../pyproject.toml#L105-L122) currently `ignore_errors = true` on every backend module. Every phase removes the ignore for its touched modules and ratchets mypy strict on. CI mypy step starts gating per-module. Adds ~10% per-phase effort, baked into estimates.
- **Cross-cutting workstream — CI gate ratchet:** Every phase bumps `--cov-fail-under` for backend (current gate 78%, current actual 83%) and frontend `coverage.thresholds.lines` (current gate 44%, current actual 46.5%). Floor is "current actual − 2pp" per existing convention.
- **Cross-cutting workstream — load-harness regression CI step:** Phase 0 adds a small synthetic-data load-harness job to CI. Fails on >10% cold-path regression vs the recorded baseline.
- **Cross-cutting workstream — security-regression pytest mark:** Phase 0 tags every test deriving from an `audit-findings/` finding with `@pytest.mark.security_regression`. CI asserts the count never decreases across phases — protects the 24 verified security fixes from silent regression during refactor.

### Success criteria — concrete

| Dimension | Target |
|---|---|
| Cold-path p95, 36M rows / 1h, Iceberg-committed | ≤ 2.8s (matches `pending-docs/performance_load_test_plan.md` §0.2) |
| Warm-path p50, 36M rows / 1h | ≤ 1.9s |
| Backend Python LOC | -18% (from 54,070 to ≤ 44,300) |
| Backend files > 2,500 lines | 0 (today: 4 — iceberg, metadata_db, scheduler, session_scoring router) |
| Backend files > 1,500 lines | ≤ 2 (today: 7) |
| Files touched to add a new dashboard widget | ≤ 4 (today: ≥ 7) |
| `# Why this exists` paragraph comments in `main.py` + `deps.py` | -75% (replaced by typed invariants + boot assertions) |
| Hidden frontend warm-up hacks | 0 |
| Middleware-order assertion on boot | yes |
| Backend unit-test coverage (touched modules) | ≥ 80% |
| Dead tests (asserting against removed/renamed surfaces) | 0 |
| Backend mypy strict modules | every module touched by phases 1–10 (today: 0 — all under `ignore_errors`) |
| CI coverage gate `--cov-fail-under` | ≥ 85% backend (today: 78%), ≥ 56% frontend (today: 44%) |
| Load-harness regression CI step | green on every PR (today: not in CI) |
| `@pytest.mark.security_regression` count | monotonically ≥ baseline set in Phase 0 (count from `audit-findings/` = 24 findings → ≥ 24 marked tests) |
| Telemetry surface | OpenTelemetry-emitted spans + metrics + structlog (today: 4 fragmented custom surfaces) |
| Backend lines saved by library swaps | ≥ 1,600 (estimate from OTel 600 + cachetools 300 + structlog 150 + pydantic-settings 100 + rich/typer 150 + aiodns 100 + tenacity 100 + tfjson 200) |
| Monkeypatches in `backend/core/iceberg.py` | ≤ 1 (today: 6 per [MONKEYPATCHES.md](../MONKEYPATCHES.md)) |
| `[v2.0-pending]` banners in `docs/ARCHITECTURE.md` | 0 (post-Phase 10) |
| Fastly CIDR refresh script | present (`scripts/refresh_fastly_cidrs.py`) |
| Post-v2.0 follow-up document | NONE — zero tech debt at end. tunnel.py + share_db.py carved up in Phase 10. |
| VM-agnostic deploy runbooks | present for AWS EC2, GCE, Azure VM (in `docs/` or `local-docs/`) |
| SSH-to-localhost.run code | deleted (-400 lines) |
| Files > 500 lines on frontend | 0 (today: 8 — ProvisionWizard 3,582 is the elephant) |
| Trust-topology snapshot tests | Caddyfile + compose + middleware all asserted in pytest |

---

## Phasing & cutover map

```
Non-breaking refactor ─────────────────────────────────────────────► hard cutover ──► v2.0 tag
Phase 0 → 1 ──────────→ 2 → 3 ─────────→ 4 → 5a → 5b ─────────→ 6 → 7 ────────► 8 → 9a → 9b → 10
baseline OTel+sl+aiodns ctx mw+ps+ten store sql  split+cache+tf cron field+sdk  cutover RSC+nuqs FE-split tunnel+share_db+rich+typer+httpx+CIDR
```

Phase headline contents (libraries + cross-cuts in-line):
- **Phase 1** — OpenTelemetry + structlog + `aiodns` async reverse-DNS (per `design_rdns_async.md`)
- **Phase 3** — pydantic-settings + `tenacity` declarative retry + Caddyfile/compose/proxy.ts trust-topology snapshot tests
- **Phase 4** — iceberg.py carve-up + monkeypatch elimination (6 → ≤1)
- **Phase 5b** — repository file splits + cachetools opportunistic + structured `.tf.json` Terraform (per `design_terraform_json.md`) + metadata_db carve-up (per `design_metadata_carveup.md`)
- **Phase 6** — scheduler.py carve-up to `backend/cron/` (per `design_scheduler_carveup.md`) + cron isolation
- **Phase 7** — Field registry + fastly SDK evaluation spike + `compute/` parity invariant + falco invariant + backend/scoring/ Python cleanup
- **Phase 8** — **HARD CUTOVER:** composites + frontend swap + delete granular endpoints + drop `_meta_con` + drop `is_cached` alias + drop `AnalyticsDeps` alias. 24–48h advance notice.
- **Phase 9a** — Frontend RSC/CSR boundary + stores audit + `nuqs` URL state + drop warm-up hacks
- **Phase 9b** — Frontend large-file splits (ProvisionWizard 3,582 → wizard/steps/* + 7 more files >500 lines)
- **Phase 10** — tunnel carve-up (per `design_tunnel_carveup.md`, delete SSH-to-localhost.run) + share_db carve-up (per `design_share_db_carveup.md`) + rich + typer + httpx everywhere + Fastly CIDR refresh script + VM-agnostic deploy runbooks + final library_evaluation.md + `v2.0.0` tag
- **Every phase** — mypy strict ratchet + CI coverage gate bump + security-regression count check + load-harness CI gate + ARCHITECTURE.md banner removal for touched sections

Total: **~2–5 calendar days end-to-end** at AI-driven execution pace. Each phase ships in 1–8 hours of focused work; human review at phase boundaries. Single `v2.0.0` tag at end of Phase 10. No 48h soaks, no 90-day shim TTL, no months-long calendar.

Approximate phase sizing (hours, not days):
- Phase 0: 2–4 h (write 2 new design docs, baseline metrics, test audit, ADRs, rollback runbook, VM-agnostic grep)
- Phase 1: 3–6 h (OTel + structlog + aiodns)
- Phase 2: 2–4 h (RequestContext + tenancy)
- Phase 3: 1–3 h (middleware invariants + pydantic-settings + tenacity + trust-topology tests)
- Phase 4: 4–8 h (iceberg.py carve-up + monkeypatch elimination 6→≤1)
- Phase 5a: 2–4 h (SQL extraction)
- Phase 5b: 3–6 h (repository file splits + cachetools + tfjson + metadata_db carve-up per design doc)
- Phase 6: 2–4 h (scheduler carve-up per design doc + cron isolation)
- Phase 7: 3–5 h (field registry + fastly SDK spike + backend/scoring/ Python cleanup)
- Phase 8: 2–4 h (hard cutover — composites + frontend swap + dead-code removal)
- Phase 9a: 2–4 h (RSC/CSR + nuqs + stores audit + drop warm-up hacks)
- Phase 9b: 4–8 h (frontend large-file splits — ProvisionWizard 3,582 is the time sink)
- Phase 10: 4–8 h (tunnel + share_db carve-ups + rich/typer/httpx + CIDR refresh + VM runbooks + tag)

**Total: ~34–68 hours = ~4–9 working sessions = ~2–5 calendar days at sustained pace.**

---

## Phase 0 — Baseline, ADRs, design docs, test audit, branch setup (2–4 hours)

**Goal:** Pin the decisions that gate every later phase. Capture today's numbers so success is measurable. Inventory the test suite so per-phase cleanup is informed, not blind.

- **0.1** Merge `performance-improvement` → `main` with a rollback path:
  - **Pre-merge snapshot** on the VM: tar the per-service DuckDB files, Iceberg catalog SQLite, and `backend.db` metadata to `/mnt/app-data/snapshots/pre-cleanup-<timestamp>/`. Keep for ≥ 30 days.
  - **Rollback runbook** at `pending-docs/rollback_runbook.md` — exact commands to revert the deploy to the pre-merge commit, restore snapshot, restart, verify. Tested locally on a copied service catalog before the merge happens.
  - **Smoke verify on post-merge main** before cutting the cleanup branch (no 24h hold at AI pace — just full smoke through dashboard/security/query/admin during a cron tick). If anything regresses, roll back via the runbook.
  - Tag the post-merge commit (`cleanup-baseline`) so the cleanup branch forks cleanly and baseline metrics are unambiguous.
- **0.2** Write five short ADRs (1 page each) in `pending-docs/adr/`:
  - `01-storage-model.md` — live-buffer → Iceberg is the persistence model. Rollups are a query optimization, not a tier. Local-warehouse fallback rule (per `dev-sandbox-scrub` memory).
  - `02-request-lifecycle.md` — one `RequestContext` per request owns service, connection, telemetry, window, cached temps. No re-resolution mid-request.
  - `03-tenancy.md` — service is injected by middleware, never parsed by routes. Cron and API never share a DuckDB connection or pool. Analyst-session tenancy enforced at the context boundary, not at each route.
  - `04-middleware-order.md` — declared order (CORS → RemoteAccess → TelemetryBody → telemetry decorator → Compress outermost). Asserted at boot.
  - `05-frontend-rendering-boundary.md` — per-route RSC vs CSR rules, code-split policy, prefetch policy.
- **0.3** Baseline metrics dump (script in `scripts/baseline_metrics.sh`):
  - `cloc backend/` + `wc -l backend/**/*.py` snapshot
  - Load-harness run (cold + warm, 36M rows / 1h, per `pending-docs/performance_load_test_plan.md`)
  - Middleware order dump from a `print()` at boot
  - File-touch count for a representative widget add (manual count)
  - Coverage baseline: `pytest --cov=backend --cov=frontend` → record per-module %
- **0.4** Test suite + tech-debt triage. Two outputs:
  - `pending-docs/test_audit.md` mapping every test file to one of:
    - `keep` — still load-bearing, will need cosmetic updates
    - `rewrite` — covers something real but tests the old shape
    - `delete` — asserts against removed/renamed surfaces, redundant, or only-ever-passed-by-accident
    - `flaky` — needs fix before it becomes useful
  - `pending-docs/tech_debt_audit.md` — grep `TODO|FIXME|XXX|HACK` across backend + frontend (Phase 0 verification found only **2 markers** today). Each marker assigned to the phase that should resolve it. Phase 10.12's final sweep asserts count = 0.
  Both maps drive per-phase test cleanup + zero-tech-debt closure.
- **0.5** Cut `refactor/cleanup-baseline` tag on the post-merge main commit. Cut `refactor/cleanup` working branch from the same commit.
- **0.6** Open `pending-docs/surprises.md` with an empty template. Add the first entry: the existing `process_context_scope` vs `set_process_context` distinction (already a known gotcha — formalize or eliminate in Phase 10).
- **0.7** Audit [MONKEYPATCHES.md](../MONKEYPATCHES.md). List every patch with its target library, what it works around, and the phase that should evaluate eliminating it. Output appended to `pending-docs/surprises.md`.
- **0.8** Set up the security-regression test mark. Source: `audit-findings/` is now empty (24 findings verified + applied + per-finding artifacts removed per `audit-findings/README.md`), so derive the baseline from (a) git log on `performance-improvement` for security-tagged commits (e.g., 9bc0ea1 cross-tenant/CSRF/authz/DoS/policy-bypass; 41f806e service-scope desync + secret-in-URL leak) and (b) source-comment grep for `# Security:` prefixes. Add `@pytest.mark.security_regression` to every test covering one of those fixes (≥ 24 marked tests as a floor). CI asserts the count is monotonically ≥ baseline. Marker registered in `pyproject.toml` `[tool.pytest.ini_options].markers`.
- **0.9** Set up load-harness CI baseline. Add a new CI job (`backend-perf`) that runs the synthetic-data harness against a fixed dataset and records numbers to `tests/perf/baseline.json`. PR job fails on >10% cold-path regression. Job runtime budget: ≤ 5 min.
- **0.10** Open `pending-docs/library_evaluation.md` skeleton. Sections per spike (fastly SDK, APScheduler v4) with empty result tables.
- **0.11** Makefile audit. Confirm every plan command has a Makefile target (test, lint, format, typecheck, gen-types, osv, secret-scan, outdated). Add `make perf` for the load harness, `make verify` for full pre-deploy gate, `make ratchet` to bump CI gates after a phase. Update CONTRIBUTING.md.
- **0.12** Pre-commit audit. Existing config covers ruff + mypy + gitleaks + regen-openapi + typecheck-frontend. Add: `pytest -m security_regression` as a fast pre-push hook so the security-regression baseline can't silently drop locally.
- **0.13** ARCHITECTURE.md prep. Add a banner: "⚠️ Sections marked `[v2.0-pending]` are being rewritten per `pending-docs/cleanup_plan.md` — see ADRs in `pending-docs/adr/` for target state." Each ADR ships with a draft section update that lands incrementally.
- **0.14** **Write 2 new design docs** (joining the 4 user-authored ones, all now in `pending-docs/`):
  - `design_tunnel_carveup.md` — split `backend/utils/tunnel.py` (1,022 lines) into `tunnel/{manager,session,rate_limiter,state}.py`. **Deletes the SSH-to-localhost.run path entirely** (~400 lines): `_TUNNEL_URL_RE`, SSH subprocess management, sleep listener, reconnect logic, `use_tunnel=True` branches. Keeps direct-mode (the production path), AnalystSession lifecycle, rate limiter, fingerprint hashing, session rehydration.
  - `design_share_db_carveup.md` — split `backend/core/share_db.py` (1,312 lines) into `share_db/{connection,schema,invites,sessions,audit,passcode,tos}.py`. Preserves: corruption self-heal with quarantine, thread-local pooled connections, scrypt passcode hashing, own MIGRATIONS dict + apply_pending pattern. `share_db/__init__.py` re-exports for compat.
- **0.15** **VM-agnostic verification + comment cleanup.** Phase 0 verification grep already confirmed: zero GCE-specific logic; 7 GCE references all in comments/docstrings (notably `remote_access.py:128` calls 169.254.169.254 "GCE metadata" but that's the same link-local IP AWS uses — the SSRF gate works on both clouds). This sub-task: replace "GCE" with "cloud" / "VM" in the 7 affected comments (`backend/state_sync.py:143,272`, `backend/provision/session_scoring_orchestrator.py:614`, `backend/core/iceberg.py:3652`, `backend/utils/remote_access.py:128`, `backend/models/lake.py:14`). Output: `pending-docs/cloud_portability_audit.md` confirming the change. `gce-deploy-rebuild` memory becomes "vm-deploy-rebuild" applicable to any cloud.
- **0.16** **Drop** any references to `post_v2_followups.md` from earlier plan drafts — zero tech debt at end means everything ships in v2.0, not deferred.

---

## Phase 1 — Telemetry on OpenTelemetry + structlog & aiodns parallelization (3–6 hours)

**Goal:** Replace the four fragmented custom telemetry surfaces with OpenTelemetry (spans, metrics, traces) and structlog (structured logging), and refactor the rDNS module with high-concurrency async parallel lookups to solve sequential latency blocking.

Existing surfaces being replaced or wrapped:
- [backend/utils/telemetry.py](../backend/utils/telemetry.py) (443 lines) — call tracking, `process_context_scope` → OTel context propagation
- [backend/utils/telemetry_proxy.py](../backend/utils/telemetry_proxy.py) (773 lines) — boto3/S3 op attribution → emits OTel spans; SigV4 + guardrail logic stays
- [backend/utils/telemetry_response_middleware.py](../backend/utils/telemetry_response_middleware.py) (235 lines) — JSON body backstop → kept for debug-panel injection only
- _section_timings, _phase_log, _debug_queries, _debug_calls — derived from OTel span attributes on read
- [backend/utils/rdns_cache.py](../backend/utils/rdns_cache.py) — blocking sequential lookup loops using synchronous `socket.gethostbyaddr` inside `_write_lock`, making per-IP SQLite transactions.

- **1.1** Inventory map: one diagram showing what each surface owns and where they overlap. Output: `pending-docs/telemetry_map.md`.
- **1.2** Add OTel + parallel-network dependencies: `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-botocore`, `opentelemetry-instrumentation-aiohttp-client`, `structlog`, `aiodns`, `aiosqlite`. Configure SDK with console exporter for dev (Jaeger/Tempo can be added post-v2.0). (msgspec + anyio direct dep dropped per planning round — overlap with orjson/Pydantic v2/DuckDB and FastAPI's transitive anyio respectively.)
- **1.3** Define `RequestTelemetry` — thin wrapper around the OTel tracer that owns: section spans (`tracer.start_as_current_span("section:dashboard.aggregates")`), query attribution (span attribute `db.statement`), call log (span events), cache state, **Python thread wait times** (custom metric `app.thread_wait_ms` instrumented at `_Pool.acquire`). The thread-wait metric is specifically required by Phase 6 to make the pool-vs-process escalation decision on data rather than guess. Lives on the (future) `RequestContext`.
- **1.4** Migrate emitters: replace contextvar mirror + `_section_timings` dict appends with `tracer.start_as_current_span`. Replace scattered `logger.info("%s ...", a)` with `structlog.get_logger().info("event", a=a)`. Configure a custom `structlog` processor to automatically extract and inject active OTel `trace_id` and `span_id` as structured logging metadata on all emitted log messages.
- **1.4a** **Refactor rDNS caching workflow (`rdns_cache.py`):** Re-architect the lookup flow to perform concurrent asynchronous DNS resolution using `aiodns` and `asyncio.gather()`. Execute bulk lookups in parallel outside of thread-blocking loops. Utilize `aiosqlite` for the SQLite reads/writes inside the async flow (scoped to this module only — see Decisions section). Batch commit all resolved IP-host mappings in a single SQLite bulk transaction via `con.executemany()` in an `async with` write lock block. Resolves the major sync-worker bottleneck where bulk IP lookups block for several minutes. See [design_rdns_async.md](design_rdns_async.md) for full design specification. (Raw log ingest's CPU-bound decompression stays as-is until Phase 1 telemetry shows it's actually a Python-side GIL bottleneck rather than DuckDB-side work that already releases the GIL.)
- **1.5** Debug-panel renderer (in `telemetry_response_middleware.py`) reads span attributes off the current trace and renders the existing `_debug_queries` / `_debug_calls` JSON shape for compat.
- **1.6** Hook the load harness (`scripts/loadtest_generator.py` + read-path probe at commit 9b6c5d0) to emit OTel-trace JSON per run. CI's new load-harness job (Phase 0.9) consumes this output.
- **1.7** Snapshot test: representative endpoint emits a fully-populated trace tree (root request span + section child spans + query attributes).
- **1.8** **Test cleanup:** review `tests/utils/test_telemetry*.py` (4 files) and `tests/utils/test_rdns_cache.py`. Per audit, delete tests asserting on contextvar mirror behavior now owned by OTel; rewrite `_section_timings`-shape tests against span exporters; add unit coverage for `RequestTelemetry` wrapper (≥ 80%) and the debug-panel renderer. Add integration and parallel lookup validation tests for the `aiodns` integration under mocked network scenarios.
- **1.9** **mypy strict:** enable on `backend/utils/telemetry*.py`, `backend/core/request_telemetry.py` (new), and `backend/utils/rdns_cache.py`. Remove from `pyproject.toml` overrides.
- **1.10** **CI gate ratchet:** bump `--cov-fail-under` to 79% backend (current actual 83%, keeps 4pp buffer post-changes).
- **1.11** Verify dev → deploy → verify prod.

**Compat:** No public API change. Existing `_debug_queries` field shape preserved on responses (rendered from OTel spans instead of contextvars).

---

## Phase 2 — RequestContext + tenancy guardrail (2–4 hours)

**Goal:** Replace the [deps.py:177](../backend/deps.py#L177) `AnalyticsDeps` bundle with a real context object. Tenancy stops being a separate check.

- **2.1** Design `RequestContext` (in `backend/core/request_context.py`):
  - `service_id`, `source`, `con`, `telemetry: TelemetryCollector`, `analyst_session`, `cached_temps: dict[str, str]`
  - Replaces `AnalyticsDeps`. Replaces standalone calls to `require_service_access`.
- **2.2** Implement as a single FastAPI dependency. Move the security-load-bearing private `read_only` attribute from [deps.py:84-92](../backend/deps.py#L84-L92) into the context constructor where it's not exposable as a query param by construction.
- **2.3** Fold `require_service_access` ([deps.py:200](../backend/deps.py#L200)) into context construction. There is no path that builds a context without enforcing tenancy.
- **2.4** Migrate dashboard router ([backend/routers/dashboard.py](../backend/routers/dashboard.py) — 4 endpoints). Measure no behavior change with load harness.
- **2.5** Migrate `query`, `security` routers (highest read traffic).
- **2.6** Migrate the rest: `alerts`, `network`, `performance`, `origin`, `sessions`, `insights`, `views`, `bootstrap`. The admin / provision / share routers use a different connection pattern; defer to Phase 5.
- **2.7** Keep `AnalyticsDeps` as a thin alias (`AnalyticsDeps = RequestContext`) so any internal-API caller compiles. Remove at v2.0 cut.
- **2.8** **Test cleanup:** review `tests/test_deps.py` + any router test that mocks `AnalyticsDeps`. Per audit, delete tests that asserted the private `_read_only` attribute trick (the new context makes it structurally impossible); rewrite router fixtures around `RequestContext`; add ≥ 80% unit coverage on the new context module including all tenancy paths (admin bypass, scoped analyst, unscoped analyst, missing service). **Tag any tenancy/auth test with `@pytest.mark.security_regression`** so the monotonic-decrease assertion catches a regression.
- **2.9** **mypy strict:** enable on `backend/deps.py`, `backend/core/request_context.py` (new), and every migrated router. Remove from `pyproject.toml` overrides.
- **2.10** **CI gate ratchet:** bump `--cov-fail-under` to 80% backend.
- **2.11** Verify dev → deploy → verify prod. Smoke + OTel-dashboard clean for ~15 min, then advance.

**Compat:** Public API unchanged. `AnalyticsDeps` aliased.

---

## Phase 3 — Middleware invariants & Tenacity retries (1–3 hours)

**Goal:** Eliminate paragraph-long comments by encoding invariants in code, adopt `pydantic-settings` for robust configuration, and standardize external API call retries via `tenacity`.

- **3.1** Replace [main.py:434-501](../backend/main.py#L434-L501) prose with one-line invariants in code, e.g. `# INVARIANT: CompressMiddleware must be outermost (see ADR-04)`.
- **3.2** Boot-time assertion: dump `app.user_middleware` and compare against a declared order tuple. Crash on mismatch.
- **3.3** Snapshot test: pytest asserts middleware order matches the declaration.
- **3.4** **Trust topology invariants** — add snapshot tests for the full security chain, not just app middleware:
  - [Caddyfile](../Caddyfile): assert `@from_fastly` remote_ip matcher present, `header_up X-Forwarded-For {http.request.header.Fastly-Client-IP}` present, rate_limit on `/share-login` present.
  - [docker-compose.prod.yml](../docker-compose.prod.yml): assert backend `command` contains `--host 127.0.0.1`, `--proxy-headers`, `--forwarded-allow-ips=127.0.0.1`. Memory cap set.
  - These tests catch the regression class that Caddyfile / compose comments warn about (XFF spoof, Host-spoof bypass, OOM cascade).
- **3.5** **pydantic-settings adoption.** Replace scattered `os.environ.get("FOO", default)` reads (in `main.py`, `config.py`, `telemetry_proxy.py`, etc.) with a single `Settings(BaseSettings)` class. Document every env var in one place. Boot validates required settings; the "TRUSTED_PROXY_IPS required in prod" guard becomes a pydantic validator.
- **3.5a** **tenacity adoption.** Add `tenacity` to the dependencies. Implement standardized, declarative, and highly-configurable retry decorators for all network-bound external API integration calls (specifically Fastly API and NGWAF requests), as well as database-bound operations to mitigate WAL concurrency limits. Replace fragile, ad-hoc, fragmented custom manual try-except-retry loops scattered across `backend/provision/fastly_api.py`, `backend/utils/ngwaf.py`, and raw S3 access paths. Establish a dedicated `tenacity` retry policy specifically targeting `sqlite3.OperationalError` (such as "database is locked" busy errors) across metadata database and share database writes to prevent writer starvation during concurrent sync/API write workloads. Define a unified retry policy (exponential backoff, random jitter, maximum attempts limit, logging of retries, and explicit exception filtering) via the global `Settings` class.
- **3.6** **Test cleanup:** review any existing middleware tests; the new snapshot subsumes most. Confirm `test_proxy_headers_regression.py` still passes — it's load-bearing for the trust topology and predates this plan. Add unit coverage for the boot-time assertion itself (success + failure path). Add unit tests for the `Settings` validators (missing required env vars in strict mode, type coercion). Add dedicated unit tests for retry behaviors, using mock network failures to verify exponential backoff, jitter, and eventual failure/escalation correctness.
- **3.7** **ARCHITECTURE.md sync:** update the "Security Isolation Layers" section with the asserted trust-topology invariants (Caddy + compose + middleware).
- **3.8** **mypy strict:** enable on `backend/main.py`, `backend/config.py`, the new `Settings` module, and any new retry helper modules.
- **3.9** **CI gate ratchet:** bump `--cov-fail-under` to 81% backend.
- **3.10** Verify dev → deploy → verify prod.

**Compat:** None affected.

---

## Phase 4 — Storage layer ownership + monkeypatch elimination (4–8 hours)

**Goal:** Carve [backend/core/iceberg.py](../backend/core/iceberg.py) (4,232 lines). Eliminate the layering bug class that produced the F3 wedge.

- **4.1** Carve `iceberg.py` by concern:
  - `iceberg/view.py` — DuckDB view binding only
  - `iceberg/catalog.py` — local SQLite catalog management. **DDL likely needed here** — if catalog access patterns change, write a migration via [backend/core/sqlite_migrations.py](../backend/core/sqlite_migrations.py) with pre+post seed tests on a copied catalog.
  - `iceberg/warehouse.py` — S3 vs file:// warehouse selection (per `dev-sandbox-scrub`)
  - `iceberg/manifest.py` — `plan_files` wrapping, manifest reading
  - `iceberg/fs.py` — **monkeypatch elimination target.** Per [MONKEYPATCHES.md](../MONKEYPATCHES.md), 5 of 6 patches are s3fs+telemetry-proxy integration. Replace with `FosS3FileSystem(S3FileSystem)` + `CachedS3FileSystem` subclasses + pyiceberg `FileIO` registration. Success criterion: monkeypatches in `backend/core/iceberg.py` drop from 6 → ≤ 1 (the ThreadPoolExecutor ContextVar patch may need to stay).
  - `iceberg/__init__.py` — re-exports for compat
- **4.2** Move view binding out of [backend/core/duckdb_pool.py](../backend/core/duckdb_pool.py) `_Pool.acquire`'s lock path. Audit that the F3 fix (commit dc5b37d) doesn't regress. Add a stress test in the load harness for the wedge scenario.
- **4.3** Define `StorageReader` protocol:
  - `live_buffer_paths(service, window) -> list[str]`
  - `iceberg_view_sql(service) -> str`
  - One implementation per backend. Read path asks the reader for what to scan; doesn't reach into either subsystem.
- **4.4** Rollups become a query-rewriter (in `backend/core/query_planner.py`, new): when a request's window + filter shape is rollup-eligible, rewrite the SQL to read the rollup parquets instead of the raw view. Routers don't know rollups exist.
- **4.5** **Data backup pre-flight.** Before the GCE deploy: tar per-service DuckDB files, Iceberg catalog SQLite (`iceberg_catalog.db`), and `backend.db` metadata to `/mnt/app-data/snapshots/phase-4-<timestamp>/`. Phase-specific rollback runbook in `pending-docs/rollback_runbook.md`. If 4.1 catalog DDL changed, dry-run the migration on a copied catalog before the prod migration runs.
- **4.6** **Test cleanup:** review `tests/test_e2e_pyiceberg_s3.py`, any test importing from `backend.core.iceberg` or `backend.core.duckdb_pool`. Per audit, delete tests asserting on the old single-file shape (~30+ likely candidates); rewrite to assert against the carved modules; add ≥ 80% unit coverage per carved module (`view`, `catalog`, `warehouse`, `manifest`). Dedicated unit tests for `StorageReader` implementations and the query rewriter. Wedge stress test goes in `tests/perf/`. Ensure integration coverage in `tests/core/test_local_compaction.py` (specifically `test_compaction_outputs_survive_iceberg_sync_orphan_cleanup`) continues to pass — verifying that `sync_data` orphan-cleanup correctly restricts its walk and skips `compacted_*.parquet` file and daily/weekly patterns, preventing row-dropping regressions (Trap #21).
- **4.7** **mypy strict:** enable on `backend/core/iceberg/*`, `backend/core/duckdb_pool.py`, `backend/core/query_planner.py` (new), `backend/core/storage_reader.py` (new).
- **4.8** **CI gate ratchet:** bump `--cov-fail-under` to 82% backend.
- **4.9** **ARCHITECTURE.md sync:** rewrite §1 (Directory & Storage Layout) and §2 (Ingest Pipeline & Atomic Guarantees) against the carved structure. Remove `[v2.0-pending]` banners on those sections.
- **4.10** **Memory budget check:** with the new storage layer + any retained 8-conn DuckDB pool, peak RSS during a 20-VU read burst must stay below the container Memory cap set in [docker-compose.prod.yml](../docker-compose.prod.yml). If not, the cap needs to be raised (and the GCE VM sized accordingly) before deploy.
- **4.11** Verify dev → deploy → verify prod with load harness. Smoke + monitor pool wait p95 / RSS / Iceberg catalog query latency for ~15 min, then advance.
- **4.12** Storage stays Fastly Object Storage (per VM-agnostic clarification) — no fsspec abstraction needed. Storage backend reaffirmed in the ADR-01 storage-model doc. (metadata_db carve-up lives in Phase 5b.3b per `design_metadata_carveup.md`, not duplicated here.)

**Compat:** `backend/core/iceberg` import path preserved via re-exports.

---

## Phase 5a — SQL parameterization & extraction (2–4 hours)

**Goal:** Move SQL fragments out of inline strings into named, parameterized templates **without altering function scopes or splitting files**. Mechanical, low-risk, easy to review.

- **5a.1** SQL ownership audit — for every SQL string in the backend, note whether it lives in a router, a repository, or `core/`. Output: `pending-docs/sql_ownership_audit.md`.
- **5a.2** SQL templates → `backend/repositories/_sql/` as named, parameterized strings. Each template documented (window, filters expected, output shape). Repository functions keep their current names and signatures — they now call into the templates instead of holding the SQL inline.
- **5a.3** **Test cleanup:** add SQL-template-rendering tests under `tests/repositories/_sql/` (one per template). Existing router/repository tests should continue to pass unchanged. Coverage target ≥ 80% on the `_sql/` package.
- **5a.4** **mypy strict:** enable on `backend/repositories/_sql/*`.
- **5a.5** **CI gate ratchet:** bump `--cov-fail-under` to 83% backend.
- **5a.6** Verify dev → deploy → verify prod.

**Compat:** Internal-only — no public API change. Function signatures preserved.

---

## Phase 5b — Repository splitting, consolidation & structured Terraform (3–6 hours)

**Goal:** Carve the largest repository files, enforce router → core import blocks, adopt cachetools, and replace fragile programmatic HCL string interpolation with structured `.tf.json` generation.

- **5b.1** Routers stop importing `backend.core.*` directly. Forced via a lint rule (`ruff` custom rule or simple grep in CI).
- **5b.2** `RequestContext.cached_temps` populated by the first repository that builds a per-window temp; subsequent repositories in the same request reuse it. Replaces the ad-hoc "share live-hour temp between dashboard CTEs and top_n_rollups" attempts (commits ef44282 / 172537c — landed, reverted, then redone differently).
- **5b.3** Split the largest repository files:
  - [backend/repositories/dashboard.py](../backend/repositories/dashboard.py) (1,057 lines) → aggregates / raw / field_values / csv
  - [backend/repositories/origin.py](../backend/repositories/origin.py) (1,257 lines) — by section
  - [backend/repositories/_base.py](../backend/repositories/_base.py) (1,146 lines) — `_compact_sql_for_debug` moves to a render concern, not core
  - [backend/repositories/insights/definitions.py](../backend/repositories/insights/definitions.py) (1,291 lines) → already a definitions file; review whether it should be data-driven instead
- **5b.3a** ✅ **SHIPPED 2026-06-11.** Migrated `backend/utils/terraform_gen.py` from f-string HCL to `.tf.json` (Terraform's JSON config syntax). `_hcl_escape` deleted entirely — `json.dumps` owns quote/backslash/control-byte escaping; a 4-line `_terraform_template_escape` retains the one Terraform-level concern JSON doesn't own (`${`/`%{` template prefix). Verified end-to-end with real `terraform fmt -check` (passes) and `terraform validate` against pinned Fastly + AWS providers (returns "Success! The configuration is valid"). Tests rewritten to assert JSON shape via `json.loads()`; injection-fuzz test now passes structurally rather than relying on a regex escape. Files now have `.tf.json` extension — Terraform accepts `.tf` and `.tf.json` interchangeably.
- **5b.3b** **Carve-up of `backend/core/metadata_db.py` (3,168 lines):**
  Deconstruct the monolithic SQLite metadata database manager into a modular package directory `backend/core/metadata/` with concerns partitioned across sub-modules:
  - `backend/core/metadata/base.py` (SQLite connection pooling, WAL mode, migrations executor, and database locks — **sync sqlite3 only**; async callers use `await asyncio.to_thread(con.execute, ...)` if ever needed)
  - `backend/core/metadata/alerts.py` (Alert rule definitions and CRUD operations)
  - `backend/core/metadata/views.py` (Saved dashboard dashboard metrics and custom filters views logic)
  - `backend/core/metadata/ingest_log.py` (Ingested files manifest, in-flight transactions registry, and transactional recovery)
  - `backend/core/metadata/cron_log.py` (APScheduler execution metrics and status logging)
  - `backend/core/metadata/asn_cache.py` (Geo-ASN names resolution and cached mappings)
  - `backend/core/metadata/usage_log.py` (Cloud operation usage logger tracking Class A/B API ops)
  - `backend/core/metadata/reconciliation.py` (Fastly emission stats vs local ingestion reconciliation audits)
  - `backend/core/metadata/state.py` (Metadata state-sync backups and configuration state export/import)
  Refactor `backend/core/metadata_db.py` into a backward-compatible shim using multi-inheritance composition to extend all concern sub-modules. Mixins stay synchronous — FastAPI's threadpool dispatches sync routes off the event loop already, so there's no measurable win from rewriting CRUD methods as `async def + await aiosqlite`. Any future async route that needs metadata uses `await asyncio.to_thread(metadata.method, ...)`. All write paths get `@sync_db_retry` (tenacity-backed) to handle SQLite `OperationalError` busy/locked under WAL contention. See [design_metadata_carveup.md](design_metadata_carveup.md) for full design blueprint.
- **5b.4** **cachetools adoption (opportunistic).** As repository splits surface cache usage, replace [bounded_cache.py](../backend/utils/bounded_cache.py), [rdns_cache.py](../backend/utils/rdns_cache.py), and [ngwaf_bot_cache.py](../backend/utils/ngwaf_bot_cache.py) with `cachetools.LRUCache` / `TTLCache`. Behavior-preserving (TTLs, eviction policies match current impl).
- **5b.5** **Test cleanup:** review every `tests/routers/` and `tests/repositories/` file. Per audit, delete tests asserting on the leaky-abstraction shape (router calling `core.*`); rewrite repository tests against the split files; add ≥ 80% unit coverage per repository module. End-to-end coverage through the load harness for each repository's hot path. Cache tests rewritten against cachetools-backed implementations.
- **5b.5a** **Test cleanup (terraform):** Update `tests/utils/test_terraform_gen.py` to validate correct dictionary schema structure and native `.tf.json` output format, ensuring compatibility with Terraform checks.
- **5b.6** **mypy strict:** enable on every `backend/repositories/*` module, including the split files, and on `backend/utils/terraform_gen.py`. Add ruff rule preventing routers from importing `backend.core.*` directly.
- **5b.7** **CI gate ratchet:** bump `--cov-fail-under` to 84% backend.
- **5b.8** Verify dev → deploy → verify prod.

**Compat:** Internal import paths preserved with re-exports. `from backend.repositories import dashboard as repo` keeps working.

---

## Phase 6 — Scheduler carve-up + cron isolation (2–4 hours)

**Goal:** Eliminate writer contention as a category. The "defer view-cache invalidation during cron windows" hack (commit 8364335, immediately reverted in 395a194) goes away because it's structurally unnecessary.

**Open decision (defer to start of phase):** separate process vs separate pool. **Decided on Phase 1 thread-wait-time data** (Phase 1.2), not by recommendation.
- *Separate process* — true isolation, but requires IPC for status (SQLite metadata writes can stay shared; cron progress dict moves to file or SQLite — likely **DDL change** via [backend/core/sqlite_migrations.py](../backend/core/sqlite_migrations.py)).
- *Separate pool* — same process, two `_Pool` instances bound to different connection sets. No IPC, but file-level disk locks (DuckDB single-writer, SQLite WAL) and thread-scheduling contention remain. Caveat: DuckDB releases the GIL during query execution, so Python GIL is rarely the bottleneck — disk lock contention usually is.

Default if Phase 1 thread-wait data is inconclusive: **separate pool first** (1 day). Escalate to separate process if `_Pool._cond` wait times during cron windows are >50ms p95.

- **6.1** Read Phase 1's thread-wait-time data for cron windows. Decide and document in `pending-docs/adr/06-cron-isolation.md`.
- **6.2** Implement chosen isolation in [backend/scheduler.py](../backend/scheduler.py). If cron progress moves to SQLite, write the migration via the existing framework.
- **6.2a** **Carve-up of `backend/scheduler.py` (2,843 lines):**
  Deconstruct the monolithic scheduler module to isolate background daemon loops and locks from cron job execution routines, reducing file size below 400 lines:
  - Create package `backend/cron/` containing `backend/cron/scheduler.py` (handling BackgroundScheduler lifecycle, daemon triggers, and configuration reloading).
  - Extract the `@cron_task` decorator and telemetry tracking wrappers to `backend/cron/decorators.py`.
  - Decompose individual background cron-job routines into a structured sub-package under `backend/cron/jobs/`:
    - `backend/cron/jobs/sync.py` (S3/FOS raw files listing, downloading, atomic transformation, and buffer tracking)
    - `backend/cron/jobs/commit.py` (Local buffer to S3 Iceberg commits and transactional snapshot generation)
    - `backend/cron/jobs/compaction.py` (Hot-tier local Parquet file sequential size-capped bin-packing and tier rollups)
    - `backend/cron/jobs/optimize.py` (Iceberg remote data files consolidation and orphan records cleanup)
    - `backend/cron/jobs/expire.py` (Iceberg snapshot expiration and ancient commit files deletion)
    - `backend/cron/jobs/metadata.py` (Metadata state-sync to FOS and cron progress audits)
  Refactor the original `backend/scheduler.py` into a thin, backward-compatible wrapper (<100 lines) delegating to `backend/cron/scheduler.py`. See [design_scheduler_carveup.md](design_scheduler_carveup.md) for detailed structural spec.
- **6.3** Remove the deferred-invalidation hack permanently.
- **6.4** **APScheduler v4 evaluation spike** (only if 6.1 picks separate-process). Stand up a tiny prototype using v4's process-pool execution against a copy of the scheduler config. Compare against custom-IPC approach. Log result in `pending-docs/library_evaluation.md` with: lines saved, behavioral differences, alpha-stability risk. Adopt v4 only if win is clear and stability acceptable.
- **6.5** **Data backup pre-flight.** Before deploy: tar per-service `backend.db` (the cron-progress source) and `cron_runs` history rows. If 6.2 migrated cron progress to SQLite, dry-run the migration on a copied catalog.
- **6.6** Load-harness regression: 20-VU read burst during an active cron sync. Reads must not 503.
- **6.7** **Test cleanup:** review `tests/test_scheduler.py`. Per audit, delete tests asserting on the deferred-invalidation hack (now gone); add ≥ 80% unit coverage for the cron-side pool and writer-connection lifecycle. Concurrency tests for the read/write pool isolation.
- **6.8** **mypy strict:** enable on `backend/scheduler.py`, `backend/cron_progress.py`.
- **6.9** **CI gate ratchet:** bump `--cov-fail-under` to 85% backend.
- **6.10** Verify dev → deploy → verify prod. Smoke + monitor cron run durations / read p95 during cron windows / RSS for ~15 min, then advance. (Carve-up itself is covered by 6.2a above.)

**Compat:** None affected (scheduler is internal).

---

## Phase 7 — Field registry + backend/scoring/ Python cleanup + fastly SDK spike (3–5 hours)

**Goal:** Lift [backend/core/log_fields.py](../backend/core/log_fields.py) (1,888 lines) implicit shape into one typed registry. Last non-breaking phase before v2.0 cut.

- **7.1** Define `FieldRegistry` — each field declares: code, display name, type, valid aggregations, valid filter ops, derivations, security-regex hooks.
- **7.2** Migrate readers one at a time: dashboard CTE generator, rollup spec builder, top_n logic, SQL validator, scoring matrix labels.
- **7.3** **Cross-language parity invariant.** `compute/` (the Rust/Wasm edge scorer) is the production scoring path; [backend/scoring/scorer.py](../backend/scoring/scorer.py) is the reference impl. `tests/scoring/fixtures/` is byte-pinned across the two. After the field-registry migration, **rerun the full parity suite** — any drift fails the phase. Document any new field semantics that the registry expresses but the Rust scorer doesn't yet support.
- **7.4** **Falco VCL semantic invariant.** Phase 7 touches VCL generation paths (`provision/fastly_api.py` uses `field_codes` from `log_fields`). The falco-tagged tests in `tests/core/test_vcl_semantics.py` must stay green; CI's existing falco step enforces this.
- **7.5** **Fastly SDK evaluation spike.** Pick one workflow from [backend/provision/fastly_api.py](../backend/provision/fastly_api.py) (e.g. VCL snippet upload). Re-implement against the official `fastly` SDK. Compare: lines saved, behavior parity (snippets, conditions, shield-map handling), test impact. Log result in `pending-docs/library_evaluation.md`. Adopt SDK-wide only if the spike shows ≥ 200 lines saved and parity on edge cases. **Default:** don't adopt without strong evidence — the custom client handles real edge cases.
- **7.6** **Test cleanup:** review `tests/utils/test_sql_validator.py`, any test importing from `log_fields`. Per audit, delete tests asserting on the old field-code constants; rewrite to query the registry; add ≥ 80% unit coverage on the registry including a property-based test (hypothesis) that every field declaration round-trips through the validator.
- **7.7** **mypy strict:** enable on `backend/core/log_fields.py`, `backend/core/field_registry.py` (new), `backend/utils/field_codes.py`, `backend/utils/sql_validator.py`, `backend/scoring/*`.
- **7.8** **CI gate ratchet:** bump `--cov-fail-under` to 86% backend.
- **7.9** Verify dev → deploy → verify prod.

**Compat:** `log_fields.py` keeps re-exporting its public API. `compute/` Rust scorer stays byte-pinned via fixture suite.

---

## ═══ HARD CUTOVER (Phase 8) ═══

Pre-cutover checklist:
- All seven prior phases shipped + verified in prod
- Load-harness numbers unchanged or better vs baseline
- 24–48h advance notice sent: CHANGELOG entry, README "Migrating from v1.x" section, direct email/Slack to any known external integrators
- Dead-code inventory complete: granular endpoints (with `frontend/` grep proving frontend-only callers), `_meta_con`, `is_cached`/`_is_cached` alias, `AnalyticsDeps` alias, `process_context_scope` vs `set_process_context` distinction

---

## Phase 8 — Composite-first API + frontend swap, hard cutover (2–4 hours)

**Goal:** Backend granular endpoints replaced with composites, frontend swaps to composites, dead code removed — all in one deploy. **No deprecation shims.** External integrators who haven't migrated get 404s; they were warned 24–48h ahead.

- **8.1** Execute frontend swaps from `pending-docs/performance_remediation_remaining.md §1` — every granular call replaced with its composite.
- **8.2** Delete granular endpoints. Verified internal usage via grep on `frontend/`; external usage was warned to migrate.
- **8.3** Drop `_meta_con` parallel path from [deps.py:233](../backend/deps.py#L233). The Phase 4 storage carve-up means metadata queries no longer pay the Iceberg view cost.
- **8.4** Drop `is_cached`/`_is_cached` Pydantic alias on `BaseResponse` (commit 571810b workaround). Pick one canonical name.
- **8.5** Drop the `AnalyticsDeps = RequestContext` alias from Phase 2.7.
- **8.6** Snapshot rendered HAR/network traces; assert no duplicate URLs per page (the "pre-merge gate" from the perf plan appendix).
- **8.7** **Test cleanup:** rewrite frontend hook tests against composite responses. Add ≥ 80% coverage on the composite endpoint handlers and the frontend hooks. Delete every test asserting on the dropped granular endpoints / aliases. Visual regression snapshots on dashboard / security / network pages. **Tag any tenancy-affecting test with `@pytest.mark.security_regression`** (composite endpoints aggregate data across boundaries — tenancy enforcement remains the critical invariant).
- **8.8** **mypy strict:** enable on all composite endpoint handlers.
- **8.9** **CI gate ratchet:** bump `--cov-fail-under` to 87% backend, `coverage.thresholds.lines` to 50% frontend.
- **8.10** **Type-generation pipeline check.** `npm run gen:types` regenerates `frontend/types/api.generated.ts`; new composite shapes will land there. tsc check is the drift guard.
- **8.11** Verify dev → deploy → verify prod. Smoke + 15-min monitoring-clean check. No tag yet — single `v2.0.0` tag at end of Phase 10.

**Breaking:** Yes. Frontend ↔ backend deploy together. External integrators were notified 24–48h ahead.

---

## Phase 9a — Frontend rendering boundary & nuqs adoption (2–4 hours)

**Goal:** Remove hydration/preload hacks, define clear RSC/CSR route specifications, adopt `nuqs` for URL-driven state.

- **9a.1** `frontend/app/_routing.md` — table of routes with RSC | CSR | hybrid, code-split policy, prefetch policy. Encodes ADR-05.
- **9a.2** Drop the hidden-1-point Plotly pre-warm and hidden-MapLibre pre-warm (commits 2d3a663 / 0762acf). Replace with `modulepreload` on the right routes per the new policy.
- **9a.3** Replace `setTimeout` / poll patterns with the styledata-event swap pattern used in aa1a096, as the default.
- **9a.4** Consolidate hydration fixes (`PlotlyChart` `visible=false`, `LazyMount`, per-page `dynamic` imports) into one rule documented in the routing table.
- **9a.5** **Audit `frontend/stores/`.** For each of the 4 stores (`debugStore`, `filterStore`, `serviceStore`, `timezoneStore`), decide: stays client-side (interactive UI state), moves to RSC fetch (data the server already owns), or splits (selectors stay, fetched data moves). Document decisions in the routing table.
- **9a.6** **nuqs (Next.js use-query-state) integration.** Adopt `nuqs` as the core URL state sync library. Refactor filter state, selected active service ID, custom visual metrics, and timeframe selections to treat URL query params as the single source of truth. Delete fragile custom syncing useEffects (the `useUrlFilterSync` / `useUrlServiceSync` hooks become thin wrappers or disappear), resolving page-refresh and hydration state desync.
- **9a.7** **Test cleanup:** delete `PlotlyChart` / `LazyMount` tests asserting on the warm-up workaround. Rewrite per the new routing rule. Add ≥ 80% coverage on the new shared lazy-loading utility. Add Vitest tests for nuqs-synced components (query-string changes trigger correct API requests and filter updates). Vitest + axe-a11y for each route's first-paint shape.
- **9a.8** **CI gate ratchet:** bump `coverage.thresholds.lines` to 56% frontend.
- **9a.9** Verify dev → deploy → verify prod.

**Breaking:** Internal frontend only. No public API change.

---

## Phase 9b — Frontend large-file splits (4–8 hours)

**Goal:** No frontend file > 500 lines. Phase 0 baseline re-grep found **16 files** above that threshold (combined ~16,400 lines), not 8 as originally enumerated — `app/*/page.tsx` route files were missed. Sub-tasks below cover all 16.

### Component splits (8 files — original 9b.1–9b.8)

- **9b.1** [components/ProvisionWizard/ProvisionWizard.tsx](../frontend/components/ProvisionWizard/ProvisionWizard.tsx) (**3,582 lines** — the elephant). Carve into:
  - `ProvisionWizard/index.tsx` — top-level shell + step navigation
  - `ProvisionWizard/steps/{intro,fastly,fos,cdn,review,deploy}.tsx` — one file per wizard step
  - `ProvisionWizard/state.ts` — wizard state machine (extracted from in-component useState soup)
  - `ProvisionWizard/api.ts` — API calls grouped by step
  - `ProvisionWizard/types.ts` — shared types
- **9b.2** [components/LogSettingsModal/LogSettingsModal.tsx](../frontend/components/LogSettingsModal/LogSettingsModal.tsx) (836) — split into LogSettingsModal/{index, FieldGroups, CustomFields, Preview}.tsx
- **9b.3** [components/CostCalculator/CostCalculator.tsx](../frontend/components/CostCalculator/CostCalculator.tsx) (634) — split into CostCalculator/{index, Inputs, Results, Breakdown}.tsx
- **9b.4** [components/SessionScoring/ThresholdSlider.tsx](../frontend/components/SessionScoring/ThresholdSlider.tsx) (573) — split into ThresholdSlider/{index, Slider, Preview, Matrix}.tsx
- **9b.5** [components/Insights/InsightHelpModal.tsx](../frontend/components/Insights/InsightHelpModal.tsx) (566) — split into InsightHelpModal/{index, sections/*}.tsx
- **9b.6** [components/Map/NetworkMap.tsx](../frontend/components/Map/NetworkMap.tsx) (562) — split into NetworkMap/{index, MapLayer, OverlayLayer, controls}.tsx
- **9b.7** [components/DataTable/DataTable.tsx](../frontend/components/DataTable/DataTable.tsx) (514) — split into DataTable/{index, Header, Body, Toolbar, ColumnPicker}.tsx
- **9b.8** [components/CronSettingsModal/CronSettingsModal.tsx](../frontend/components/CronSettingsModal/CronSettingsModal.tsx) (510) — split into CronSettingsModal/{index, Schedule, Triggers, Preview}.tsx

### Route splits (8 files — added 2026-06-09 after Phase 0 baseline)

Pattern: extract per-section components into `app/<route>/_sections/<Section>.tsx` (Next.js underscore-prefixed folders aren't routable). The `page.tsx` becomes the RSC/CSR shell per ADR-05 + the Phase 9a routing table.

- **9b.12** [app/logs/page.tsx](../frontend/app/logs/page.tsx) (**2,136 lines** — second largest after ProvisionWizard). Carve into:
  - `app/logs/page.tsx` — shell + URL state via nuqs (post-Phase 9a)
  - `app/logs/_sections/{Filters,ResultsTable,FieldPicker,SavedQueries,DetailsDrawer}.tsx`
  - `app/logs/_state.ts` — derived filter/window state hooks
- **9b.13** [app/admin/page.tsx](../frontend/app/admin/page.tsx) (1,438) — split into `app/admin/_sections/{ServicesTable,GlobalSettings,SystemStatus,DiagnosticsPanel}.tsx`
- **9b.14** [app/dashboard/page.tsx](../frontend/app/dashboard/page.tsx) (1,184) — split into `app/dashboard/_sections/{Aggregates,Timeseries,TopN,GeoMap}.tsx`
- **9b.15** [app/alerts/page.tsx](../frontend/app/alerts/page.tsx) (959) — split into `app/alerts/_sections/{AlertsList,AlertEditor,AlertPreview}.tsx`
- **9b.16** [app/admin/usage-log/page.tsx](../frontend/app/admin/usage-log/page.tsx) (656) — split into `app/admin/usage-log/_sections/{UsageTable,UsageChart,Filters}.tsx`
- **9b.17** [app/security/page.tsx](../frontend/app/security/page.tsx) (628) — split into `app/security/_sections/{AnomaliesTable,SignatureLanding,ThreatMap}.tsx`
- **9b.18** [app/origin/page.tsx](../frontend/app/origin/page.tsx) (562) — split into `app/origin/_sections/{Aggregates,Timeseries,LatencyHeatmap}.tsx`
- **9b.19** [app/sessions/page.tsx](../frontend/app/sessions/page.tsx) (510) — split into `app/sessions/_sections/{SessionsTable,SessionDetail,ScoringControls}.tsx`

### Closing

- **9b.9** **Test cleanup:** keep existing test coverage; rewrite imports as files move. Add coverage for any newly-extracted utility modules (wizard state machine, datatable column picker logic, route-section hooks). Standardize all Vitest frontend integration tests for step-by-step form submission flows (especially the split step components under `ProvisionWizard/steps/*`) on `msw` (Mock Service Worker) to mock backend responses at the network level.
- **9b.10** **CI gate ratchet:** bump `coverage.thresholds.lines` to 58% frontend.
- **9b.11** Verify dev → deploy → verify prod.

**Breaking:** Internal frontend only. No public API change. No file > 500 lines target met.

---

## Phase 10 — tunnel + share_db carve-ups, final cleanup, v2.0 tag (4–8 hours)

**Goal:** Zero tech debt at end. Carve the last two large security-critical files (tunnel + share_db) per their design docs. Delete SSH-to-localhost.run. Final library swaps. VM-agnostic deploy runbooks. Single `v2.0.0` tag.

- **10.1** **Execute `design_tunnel_carveup.md`** — carve [backend/utils/tunnel.py](../backend/utils/tunnel.py) (1,022 lines) into:
  - `backend/utils/tunnel/manager.py` — TunnelManager singleton (direct-mode only)
  - `backend/utils/tunnel/session.py` — AnalystSession + rehydration + validation + invite-permission re-sync
  - `backend/utils/tunnel/rate_limiter.py` — `_LoginRateLimiter` (sliding-window + lockouts)
  - `backend/utils/tunnel/state.py` — direct-mode state persistence (tunnel_state.json)
  - `backend/utils/tunnel/__init__.py` — re-exports for compat (`get_tunnel_manager`, `AnalystSession`, `compute_fingerprint`)
  - **DELETE** the SSH-to-localhost.run path entirely (~400 lines): `_TUNNEL_URL_RE`, SSH subprocess + lifecycle, sleep listener thread, reconnect logic, `use_tunnel=True` branches, OS power-event handlers.
- **10.2** **Execute `design_share_db_carveup.md`** — carve [backend/core/share_db.py](../backend/core/share_db.py) (1,312 lines) into:
  - `backend/core/share_db/connection.py` — pool + corruption self-heal with quarantine
  - `backend/core/share_db/schema.py` — `_SCHEMA` + migrations framework (own MIGRATIONS dict + `apply_pending` + PRAGMA user_version)
  - `backend/core/share_db/invites.py` — invite CRUD (`get_remote_invite`, `get_remote_invite_services`, etc.)
  - `backend/core/share_db/sessions.py` — session CRUD (`upsert_session`, `delete_session`, `get_session`, `get_all_sessions`)
  - `backend/core/share_db/audit.py` — `log_share_audit_event`
  - `backend/core/share_db/passcode.py` — scrypt hashing + verification
  - `backend/core/share_db/tos.py` — TOS version management
  - `backend/core/share_db/__init__.py` — re-exports for compat
- **10.3** Drop `process_context_scope` vs `set_process_context` distinction — Phase 1 OTel context propagation makes the iothread mirror redundant. If anything remains, formalize as typed scopes.
- **10.4** Drop the background warmups in `main.py` that the Phase 1 telemetry made unnecessary. Keep the genuinely-load-bearing ones (POP cache, scoring matrix, Iceberg view pre-warm).
- **10.5** **rich + typer adoption.** Replace [provision/utils.py](../backend/provision/utils.py) `BOLD/_c/fail/info/ok/warn` printers with `rich.console.Console`. Convert [provision/cli.py](../backend/provision/cli.py) to typer.
- **10.6** **httpx-everywhere sweep.** Drop aiohttp from non-proxy paths (it stays in [telemetry_proxy.py](../backend/utils/telemetry_proxy.py) — it's a server). Standardize on httpx.
- **10.7** **Fastly CIDR refresh script.** Add `scripts/refresh_fastly_cidrs.py` that pulls `https://api.fastly.com/public-ip-list` and rewrites the Caddy `@from_fastly` block. Manual or cron-scheduled.
- **10.8** **VM-agnostic deploy runbooks.** Add `docs/deploy/` (or `local-docs/deploy/`) with one runbook per platform: `aws_ec2.md`, `gce.md`, `azure_vm.md`, `generic_linux.md`. Each covers: docker install, env vars, volume mounts (`/mnt/app-data`), Caddy/SSL setup, first-deploy + restart flow. The existing `~/restart.sh` works on any of them since it's just `git pull + docker compose --build`.
- **10.9** File-size sweep: top-5 backend files ≤ 1,500 lines (per success criteria); no frontend file > 500 lines. **No `# TODO: split` markers** — split it or justify in writing why it can't be split. Zero tech debt.
- **10.10** Update `AGENTS.md` (529 lines), `CLAUDE.md`, and finalize [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — remove all `[v2.0-pending]` banners; confirm each section reflects shipped state.
- **10.11** **Final library evaluation summary** in `pending-docs/library_evaluation.md` — fastly SDK + APScheduler v4 verdicts, lines saved, decisions adopted vs not (no "deferred to v2.1" — everything decided).
- **10.12** **Final test sweep:** any test still marked `delete`/`flaky` from Phase 0 that survived must be resolved. Final coverage report — every backend module touched by phases 1–10 at ≥ 80%. Coverage diff committed. Security-regression count ≥ Phase 0 baseline.
- **10.13** **Final mypy sweep:** every backend module either mypy-strict or explicitly justified in `pyproject.toml` overrides with a comment. Goal: `ignore_errors` list ≤ 3 modules max.
- **10.14** **Final CI gate:** `--cov-fail-under` at 85% backend, `coverage.thresholds.lines` at 58% frontend. Load-harness CI step green. Security-regression count ≥ baseline. mypy strict on the touched-module list.
- **10.15** Verify dev → deploy → verify prod.
- **10.16** Tag `v2.0.0`. Update `pyproject.toml` and `frontend/package.json` versions.

---

## Verification (per-phase + final)

**Per phase:**
1. Test cleanup completed against the Phase 0 audit map for the touched area (delete dead, fix flaky, rewrite stale).
2. Coverage on every touched module ≥ 80%. Coverage delta committed alongside the code PR.
3. **DDL migration check** — if any SQLite schema changed (most likely in Phase 4 catalog work and Phase 6 cron progress), migration written via [backend/core/sqlite_migrations.py](../backend/core/sqlite_migrations.py) with pre+post seed tests on a copied catalog. Rollback path documented.
4. **Data backup pre-flight** (Phase 4 + Phase 6 only) — pre-deploy snapshot of affected DuckDB / Iceberg catalog / SQLite files; phase-specific rollback runbook entry.
5. **Security-regression count** ≥ baseline (`@pytest.mark.security_regression` CI assertion green).
6. **mypy strict** enabled on the modules the phase touched. `ignore_errors` override removed for those modules.
7. **CI gate ratchet** — `--cov-fail-under` bumped per the per-phase target above; floor is "current actual − 2pp."
8. **Load-harness CI step** green on the PR. >10% cold-path regression is a blocker.
9. Dev verify on `13002/18002` (per `verify-dev-first` memory).
10. GCE deploy via `~/restart.sh` (per `gce-deploy-rebuild` memory). Hard-refresh browser.
11. Production verify — smoke through dashboard, security, query, admin nav during a cron tick.
12. **For phases 2 / 4 / 6:** Monitor OTel dashboards (thread-wait p95, pool checkout failures, RSS, SQLite/DuckDB lock contention) for ~15 minutes after deploy. At AI pace there's no multi-day soak — anomalies that surface in 15 min are the signal; deeper slow-burn bugs get caught by the Phase 0 surprises log + post-deploy spot-checks.
13. **For library-swap phases (1, 3, 5b, 6, 7, 10):**
    - `make osv` (existing target — runs `scripts/check_osv.py`) clean on new deps.
    - If HTTP-call shapes change (OTel boto instrumentation, fastly SDK adoption, etc.), re-record the affected `tests/cassettes/` vcrpy cassettes and commit them.
    - `make outdated` reviewed (informational — surfaces drift introduced by the swap).
14. **For phases that ship architectural change (1, 2, 3, 4, 5b, 6, 7):** update the relevant section of [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) (remove `[v2.0-pending]` banner on the section as it's confirmed).

**Final (after Phase 10):**
- Run baseline_metrics.sh again. Compare against Phase 0 snapshot.
- New-feature touch-count test: add a trivial dashboard widget. Count files touched. Must be ≤ 4.
- Operational surface check: grep `main.py` and `deps.py` for `# Why this exists` paragraphs. Should be ≤ 25% of baseline.
- Coverage report final: every backend module touched by phases 1–10 at ≥ 80%, zero dead tests remaining.
- mypy strict enabled on all touched modules; `ignore_errors` override list ≤ 3 modules.
- CI: `--cov-fail-under` ≥ 85% backend, `coverage.thresholds.lines` ≥ 55% frontend, load-harness step green, security-regression count ≥ Phase 0 baseline.
- Backend LOC saved by library swaps ≥ 1,600. Verify via `cloc` diff vs Phase 0.
- `compute/` parity tests green; `tests/scoring/fixtures/` unchanged.

---

## Risks & mitigations

| Risk | Phase | Mitigation |
|---|---|---|
| Perf-improvement merge surfaces a prod-only regression | 0 | Pre-merge snapshot of GCE state; rollback runbook tested locally before the merge; 24h hold-and-verify on post-merge main before forking cleanup branch |
| Storage carve-up regresses F3 wedge | 4 | Phase 1 OTel telemetry (thread-wait metric) catches it; dedicated 20-VU wedge test in harness; 15-min OTel-dashboard check post-deploy; phase is reversible by reverting one PR + restoring Phase 4 data snapshot |
| Slow-burn concurrency / leak bugs from RequestContext or storage split | 2, 4 | 48h production soak with thread-wait + RSS + pool-checkout monitoring before next phase starts |
| Cron isolation choice is wrong (pool when process needed) | 6 | Decision made on Phase 1 OTel thread-wait data, not guesswork. Old code path behind a flag for one deploy cycle. 15-min OTel-dashboard check post-deploy. Data snapshot allows rollback. |
| Hard cutover (Phase 8) causes 404s for stale frontend tabs / external integrators | 8 | 24–48h advance notice (CHANGELOG + README migration section + direct outreach). User-controlled risk acceptance: hard cutover chosen over months-long shim TTL.
| Deleting SSH-to-localhost.run breaks an active laptop-admin sharing use case | 10 | User confirmed prod is GCE+Fastly direct-mode only; SSH-tunnel path is unused. If someone needs laptop-admin sharing later, document Cloudflare Tunnel as the recommended path.
| AI-pace execution misses a slow-burn regression that 48h soak would have caught | any | Mitigated by: (a) load-harness CI gate on every PR, (b) OTel dashboards monitored 15 min post-deploy, (c) per-phase data backup for phases 4/6, (d) rollback runbook, (e) surprises log captures unknowns at phase boundaries.
| Phase 5b file splits break an obscure import | 5b | Phase 5a (SQL extraction) is mechanical and ships first; 5b builds on its clean base; re-exports preserved; mypy strict + grep-CI check verifies no router imports `backend.core.*` |
| DDL change ships without a migration | 4, 6 | Migration check is item #3 on every per-phase verification list. CI grep for raw `con.execute("CREATE`/`ALTER`/`DROP")` outside the migration framework. |
| OTel adoption breaks debug panel | 1 | Debug-panel renderer reads span attributes on the response path and re-emits the existing `_debug_queries` shape; snapshot test enforces the shape. Compat preserved through v2.0. |
| structlog format change breaks downstream log consumers | 1 | Solo dev → no external log consumers. If a downstream emerges, structlog supports key-value AND JSON renderers — switchable per env. |
| Library swap (cachetools / pydantic-settings / rich / typer) introduces behavior drift | 5b / 3 / 10 | Each swap is opportunistic and behavior-preserving (TTLs, validators, prints match prior behavior). Snapshot tests on call sites catch drift. |
| fastly SDK spike reveals SDK can't replace custom client | 7 | Spike is opt-in; default is "don't adopt without evidence." Logged in `pending-docs/library_evaluation.md` with reasoning. Custom client stays. |
| APScheduler v4 alpha-instability bites | 6 | v4 is the spike target only if Phase 6 picks separate-process. Default is custom IPC if alpha is too rough. |
| compute/ Rust scorer drifts from Python reference | 7 | Fixture parity suite is the gate; runs as part of Phase 7 verification. Any field-registry semantic change requires a paired `compute/` PR before Phase 7 ships. |
| mypy strict surfaces type bugs that were silently passing | every | Treated as a finding, not a failure — each surfaced bug gets fixed alongside the phase that uncovered it. Adds ~10% per-phase effort, already baked into estimates. |
| Long-tail debt surfaces partway through (e.g. a sixth root-cause group) | any | Phases are independently shippable; pause + extend at any boundary; surprises log captures unknowns |

---

## Out of scope

- Iceberg table-format upgrade (v2 → v3)
- Orphan-file cleanup for Iceberg/FOS (per `orphan-files-defer` memory — wait for pyiceberg PR #3361)
- Multi-region deploy
- Replacing DuckDB, Iceberg, or Fastly Object Storage as the storage backend
- Adding non-Fastly storage backends (gcsfs/adlfs/etc.) — storage stays Fastly per the VM-agnostic clarification
- Adding new product features during the refactor window
- **`compute/` (Rust/Wasm edge scorer)** — out of scope as code; in scope as a parity invariant (Phase 7.3). Any changes required there are tracked as a paired PR.
- **SSH-signed-challenge admin auth** — explicitly evaluated and rejected. Admin access stays as SSH-port-forward + `proxy.ts` X-Proxied-By-Caddy gate (most secure primitive).
- **SSH-to-localhost.run analyst sharing** — explicitly deleted in Phase 10, not refactored. Direct-mode only.
- **API contract testing beyond tsc** (Schemathesis, OpenAPI fuzzing) — existing `test_openapi_snapshot.py` + `test_response_contract.py` + tsc check are sufficient
- **Frontend bundle-size / Web Vitals / Lighthouse CI** — Phase 9 establishes the rendering boundary; bundle perf is a follow-on if needed
- **Custom telemetry backends (Jaeger / Tempo / Honeycomb)** — Phase 1 ships with OTel console exporter only. Adding a real backend is a deploy-config change.
- **Replacing custom SQL validator with sqlglot** — current impl uses DuckDB's own `json_serialize_sql`, more correct for DuckDB-specific SQL
- **Replacing `sqlite_migrations.py` with alembic** — overkill for single-file SQLite metadata DBs

---

## Open questions / decisions during execution

These don't block the plan but need an answer when each phase starts:

1. **Cron isolation** (Phase 6) — separate pool or separate process? Decided on Phase 1's OTel thread-wait-time data. Default: separate pool if `_Pool._cond` wait p95 < 50ms during cron windows; separate process otherwise.
2. **APScheduler v4 adoption** (Phase 6) — only if 6.1 picks separate-process. Spike, decide before phase ships. Default: stay on v3 if v4 alpha is rough.
3. **fastly SDK adoption** (Phase 7) — spike, decide before phase ships. Default: don't adopt without ≥ 200-line saving + edge-case parity.
4. **Insights `definitions.py` shape** (Phase 5b) — split by section, or convert to data-driven YAML/JSON? Decide after reading the file end-to-end at the start of Phase 5b.
5. **OTel exporter for prod** (Post-Phase 10) — console-only ships in v2.0; an actual backend (Jaeger, Tempo, Honeycomb, Grafana Cloud) is a separate config decision when there's a reason to consume traces externally.
6. **Promote ADRs to public after v2.0?** (Post-Phase 10) — separate sanitization pass, or leave local-only forever? Default: keep local-only.
