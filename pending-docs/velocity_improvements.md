# Velocity Improvements — Actions from the 2026-06-10 Retrospective

**Goal:** keep technical debt low so new features can ship fast without rebuild cycles.

**Methodology:** 236-agent retrospective ran 7 mapping lenses + 7 pain lenses + 3-way adversarial verification (solo-dev feasibility, would-have-caused-new-problems, evidence-grounded). 68 pain points found; 55 high/medium got recommendations; 53 were rejected as hindsight bias. The 2 survivors + 3 cross-cutting themes + named operational gaps below are what passed scrutiny.

The big meta-finding: **most reflexive "should have done X upfront" advice failed verification** for this project's profile (solo dev, public repo, MVP-then-iterate). The lever for future velocity is not more upfront architecture — it's a few cheap process habits plus filling specific operational gaps.

---

## Tier 0 — Do this week (concrete, evidence-backed)

### 0.1 Split `usePageContext` into focused hooks
**File:** [frontend/hooks/usePageContext.ts](frontend/hooks/usePageContext.ts) (currently compounds 3 orthogonal Zustand stores → 10 props)

**Action:** Replace with three single-concern hooks:
- `useActiveService()` → `serviceStore` slice (activeServiceId, services)
- `useTimeRange()` → `filterStore` time slice (startTime, endTime, compareMode, compareStartTime, compareEndTime)
- `useTimezone()` → `timezoneStore`

Then update consumers — SecurityPage only needs 3 of 10 props; ReportLayout needs 4; DashboardBody needs all three.

**Cost:** ~30 min. **Payoff:** import-site visibility into which store mutations a component depends on; cleaner test mocks; eliminates surprise re-renders.

### 0.2 Delete `RequestContext.cached_temps`
**File:** [backend/core/request_context.py:63](backend/core/request_context.py#L63) + docstring at [line 19](backend/core/request_context.py#L19)

**Action:** Delete the field. Zero non-test consumers (verified: `grep -r cached_temps backend/ frontend/` returns only the definition and docstring). The "First repository that builds…" docstring describes a contract no repository implements.

**Cost:** ~15 min. **Rule it establishes:** unused scaffolding gets deleted on a 7-day clock. If the optimization is real later, reintroducing it is cheap; leaving placeholders rot is a visible debt smell.

### 0.3 Wire `refresh_fastly_cidrs.py` into CI
**Files:** new [.github/workflows/cidr-refresh.yml](.github/workflows/cidr-refresh.yml); [Caddyfile:24-27](Caddyfile#L24-L27) flags the list as manually maintained.

**Action:** Weekly scheduled job runs `uv run python scripts/refresh_fastly_cidrs.py --check`. On diff, open a PR with the new list. Optionally: on merge, trigger `~/restart.sh caddy` on the VM.

**Cost:** ~30 min for `--check` + PR opener. Reload trigger can wait. **Payoff:** new Fastly POPs trusted within a week instead of "when somebody remembers."

### 0.4 Fix the latent suffix-encoding bug in filter URL codec
**File:** [frontend/hooks/useFilterUrlSync.ts:70](frontend/hooks/useFilterUrlSync.ts#L70)

**Issue:** `rawCol.replace(/_\d+$/, '')` strips any trailing `_<digit>`, which silently corrupts column names that legitimately end that way (e.g. a future field literally named `response_1` decodes as `response`). Backend has the symmetric strip at [backend/.../filters.py:103](backend/) — same hazard.

**Action:** Either (a) rename the duplication-suffix scheme to something that can't collide (e.g. `__dup<N>`), or (b) keep the suffix but add a validation guard that rejects column names matching `_\d+$` at filter creation time, with a property test for the round-trip.

**Cost:** ~1 hour (test + fix + backend mirror).

---

## Tier 1 — Habits to adopt today (free; just process)

These are the rules the retrospective surfaced as cheap wins that compound. Each one is enforceable via PR checklist; none require tooling investment.

### Rule A: Split compound abstractions at first sign of orthogonal use
**Trigger:** any consumer uses fewer than half of a hook/context/mixin's properties, or doesn't need a co-bundled store.
**Action:** split immediately (30-min PR). Don't defer to a future cleanup phase.

### Rule B: Introduce mutable config and its detection in the same commit
**Trigger:** adding a value that will drift over time (IP allowlist, public key, version pin, third-party API list).
**Action:** the same PR adds either a CI check or a scheduled refresh job. No "we'll automate when the pain scales" — it never does.

### Rule C: Delete unused scaffolding on a 7-day clock
**Trigger:** field, function, type, or stub added "for future use" with zero consumers after a week.
**Action:** delete. Reintroducing is cheap; rot is not. `RequestContext.cached_temps` is Exhibit A.

### Rule D: SLOs and trust contracts live next to the code, not in a separate wiki
**Trigger:** code with operational implications — trust boundaries, freshness contracts, rate limits, retry semantics.
**Action:** comment block at the point of definition stating the expected behavior and what breaks if it's violated. The [Caddyfile](Caddyfile) trust-topology comment is the existing good example to copy.

### Rule E: When pain is observed under load, capture an ADR
**Trigger:** any production incident or perf bottleneck that required > 1 hour to diagnose.
**Action:** ADR documenting the decision the fix represents (what was wrong, what alternatives were rejected, why). This is *already* how ADRs 01–06 got written, and the retrospective explicitly endorsed it as the right cadence — formalize it as a rule so it doesn't lapse.

### PR checklist additions
Add to [CONTRIBUTING.md](CONTRIBUTING.md):
- [ ] Any new mutable operational config — is drift detection wired in this PR?
- [ ] Any "placeholder" field/stub added — does it have a delete-by date in a comment?
- [ ] Any new compound abstraction (hook, context, mixin) — do all consumers need all parts?
- [ ] Any new endpoint — does it state a target p95 latency and storage cost in its docstring? (see Tier 2.2)

---

## Tier 2 — Operational gaps to fill before the next big feature

The retrospective's completeness critic flagged these as material gaps. Each will silently accumulate debt as you add features. Pick 2–3 to formalize before the next feature workstream; the rest can follow.

| Priority | Gap | Why it matters for velocity | First step | Cost |
|---|---|---|---|---|
| **High** | **Observability strategy** ([docs/adr/07-observability.md](docs/adr/)) | Phase 1 wired OTel + structlog but there's no consuming doc — next incident is code-reading, not dashboard-reading | Document what's monitored, alert thresholds, debug playbook for top 3 failure modes | ~2 hr |
| **High** | **Per-feature perf/cost budgets** ([docs/adr/08-feature-budgets.md](docs/adr/) or CONTRIBUTING addendum) | v1.2.0 perf wins were reactive; budget-at-PR-time catches debt-inducing designs before merge | Define template: each new endpoint declares target p95, storage/mo growth, query cost ceiling | ~1 hr policy + 5 min/PR |
| **Med** | **Schema evolution contract** ([docs/adr/09-schema-evolution.md](docs/adr/)) | README claims handled gracefully; no contract exists. First customer asking for retroactive coverage = one-off | Process for adding log fields, backfill rules, deprecation timeline | ~2 hr |
| **Med** | **Secret rotation policy** ([SECURITY.md](SECURITY.md) addition) | Session-scoring has a grace-window pattern that works; generalize | Apply same pattern to Fastly API token, FOS keys, CDN secrets | ~1 hr |
| **Med** | **Error/retry/idempotency philosophy** ([docs/adr/10-error-handling.md](docs/adr/)) | Three retry policies + in-flight manifest pattern are coherent but undocumented; new routers will reinvent inconsistently | Document when to retry vs fail-fast, name the in-flight manifest as load-bearing crash-safety | ~2 hr |
| **Low** | **API versioning doctrine** ([docs/adr/11-api-versioning.md](docs/adr/)) | openapi-typescript wired but no breakage rules; cost shows when external integrators land | Define what counts as breaking, deprecation timeline, version surface | ~1 hr |
| **Low** | **Backup / DR / data-replay** | [rollback_runbook.md](local-docs/rollback_runbook.md) is dev/test, not prod | Document RTO/RPO, FOS bucket restore procedure | ~1-2 hr |

---

## Things to preserve (don't rewrite these in a reimagining)

The audit was unusually positive on several structural choices. Name them so they survive any future "let's start over" exercise:

- **ADR-driven architecture with decisions captured *after* the lesson lands.** This is the velocity strategy, not a debt — the retrospective explicitly endorsed it over "decide everything upfront."
- **[MONKEYPATCHES.md](MONKEYPATCHES.md) as a living inventory** with root-cause attribution per patch (incident date, why upstream can't fix, removal criteria).
- **Property-based testing** (Hypothesis) for filter/query roundtrips. Catches drift without hand-written matrices.
- **RequestContext** making tenancy structurally impossible to bypass (can't construct without `_enforce_service_access`).
- **Modular package carves with re-export shims** for backward compat during refactor (the `metadata_db.py` / `scheduler.py` pattern).
- **Named exception classes + explicit retry policies** (vs. generic `except Exception`).
- **Three-tier docs scheme** (pending-docs / local-docs / docs) — intentional and works for a public-repo solo project.
- **MVP-then-iterate cadence with phase-based cleanup.** The retrospective's verifiers repeatedly rejected "spike before shipping" recs by pointing to this cadence as the correct trade-off given solo bandwidth and information unavailability at v1.0 time.

---

## Anti-patterns the audit warned against (do NOT adopt)

The 53 killed recommendations clustered around a few seductive but wrong refactors. If anyone (including future-you) proposes one of these, push back:

- **Generic "schema codegen" infrastructure** for FilterSpec — `openapi-typescript` already handles the 80% case; codegen can't express the procedural collision-handling logic that's the actual duplication.
- **Premature `usePagination` / `PaginationConfig` context** when there are only 2 paginated endpoints with genuinely different sort semantics.
- **Centralized `RoleProvider` context** — role is 2 orthogonal flags (`analyst_session` × `is_remote_analyst`), not a hierarchy; an enum would have locked in a false model when SHARE-INVITED was added.
- **Multi-language scoring codegen** (Python ↔ Rust) — parity is enforced cheaply by fixture tests; codegen adds versioned-schema overhead and constrains schema evolution.
- **Pre-formatted server-side response values** — `TopTenTable` needs raw values for click handlers and map ops; pre-formatting forces double payload and locks display format into the API contract.
- **Cache-coherence "state machine" abstractions** — the bottleneck is DuckDB view rebuild time, not cache layer policy; a state machine wouldn't have prevented the 2026-06-09 transient-empty-result incident.
- **Unified `QueryExecutor`** for retry — stale-view and compaction-race are different error classes with different recovery costs; collapsing them creates a leaky abstraction.
- **Tentacle-parameter threading** through repository signatures (e.g., passing `RequestContext.cached_temps` to every repo function) — couples request scope to data layer.
- **Custom `FsspecFileIO` subclass to "fix" the monkeypatches** — already investigated 2026-05-21 and rejected; pyiceberg instantiates `S3FileSystem` directly inside its `_s3()` builder, bypassing the FileIO layer entirely. Wait for upstream `supply-your-own-FileSystem-class` hook.

---

## Execution order

1. **This week:** Tier 0 (~2.5 hours total). All four are small, evidence-backed, no design debate needed.
2. **Immediately:** Tier 1 rules — add the four PR checklist items, link this doc from [CONTRIBUTING.md](CONTRIBUTING.md). Free to adopt.
3. **Before next feature workstream:** pick the two High-priority Tier 2 items (observability strategy + per-feature budgets). They unlock "ship, then *watch* under load" instead of "ship, then *discover* under load" — which is the highest-leverage velocity unlock available.
4. **Defer indefinitely:** anti-patterns above. If they ever look tempting again, re-read this doc.
