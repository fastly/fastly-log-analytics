# ADR-03 — Tenancy

**Status:** Accepted (Phase 0)
**Decided by:** v2.0 cleanup planning
**Supersedes:** the "tenancy is whatever each route remembered to enforce" pattern

## Context

Tenancy was retrofitted onto a single-tenant codebase. The symptoms:

- Cross-tenant remediation findings in `audit-findings/` (now resolved, but the underlying shape that allowed them remains)
- Service-scope desync on path-params (`fastly_service_id` in URL diverging from the service the connection was opened against — finding 41f806e)
- Writer contention between cron and API on the same DuckDB connection pool
- Per-service slug rollup view names (carry the service id in the SQL identifier; identifier collisions become tenancy bugs)
- A security-load-bearing private attribute on `AnalyticsDeps` that exists to keep query-string params from forging it (`deps.py:84`)
- The cross-tenant `ThreadPoolExecutor` ContextVar leak fixed by monkeypatch #6 (`MONKEYPATCHES.md`)

Each of these is a separate fix on top of the same root cause: tenancy isn't a structural property of the system, it's an aspirational check each surface remembers (or forgets) to make.

## Decision

Three structural invariants make tenancy load-bearing:

### 1. Service is injected by middleware, never parsed by routes.

The middleware that constructs `RequestContext` (ADR-02) is the sole resolver of `service_id` from the request. Routes receive a `RequestContext` parameter, not a `service_id` path param. If a route function signature contains `service_id: str`, that is a bug — it must be reachable only via the context.

### 2. Cron and API never share a DuckDB connection or pool.

Phase 6 splits the pool: API requests use the read pool; cron jobs use a dedicated writer connection (or a separate process, decided on Phase 1 thread-wait data). The deferred-invalidation hack (commit 8364335, reverted 395a194) goes away because writer contention is no longer possible by construction.

### 3. Analyst-session tenancy is enforced at the context boundary, not at each route.

`RequestContext` construction checks that the resolved service is one the analyst session is permitted to see (per the invite + permission table). Routes don't repeat the check. This subsumes today's standalone `require_service_access` calls.

## Boundary-crossing rules

- **Background workers (cron, scheduler)** construct a `BackgroundContext` with explicit `service_id` (never inferred from anywhere). The pyiceberg `ThreadPoolExecutor` ContextVar propagation patch (monkeypatch #6) stays in place until CPython gains first-class context propagation for `concurrent.futures`.
- **Composite endpoints** (Phase 8) aggregate within a single service. Cross-service aggregation is not a v2.0 feature.
- **Admin endpoints** that operate on a specific service still construct a context for that service — admin bypass means the tenancy check passes, not that it is skipped.

## Consequences

- Phase 2 (RequestContext) and Phase 6 (cron isolation) jointly close the tenancy gap.
- The `_read_only` private-attribute trick is structurally eliminated by ADR-02; this ADR makes its security guarantee load-bearing in a different way.
- `tests/routers/test_cross_tenant_scope.py` keeps every existing assertion and gets new ones tagged `@pytest.mark.security_regression`.
- Per-service slug-named views (e.g., per-tenant rollup view identifiers) get reviewed in Phase 4 carve-up to confirm identifier collisions can't become tenancy bugs (UUIDs or service-id hashes instead of slugs).
- The `audit-findings/` security-regression count baseline (Phase 0.8) protects the 24 verified fixes against silent regression during refactor.

## Out of scope

- Per-tenant rate limiting (Caddy handles it at the edge today; not changing in v2.0)
- Tenant deletion / GDPR erasure flows
- Cross-tenant data sharing UI
