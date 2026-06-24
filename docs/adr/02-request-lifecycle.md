# ADR-02 — Request Lifecycle

**Status:** Accepted (Phase 0)
**Decided by:** v2.0 cleanup planning
**Supersedes:** `AnalyticsDeps` bundle + standalone `require_service_access` calls (backend/deps.py)

## Context

Today's request handling is fragmented across:

- **`AnalyticsDeps`** (`backend/deps.py:177`) — a dataclass-shaped bundle of `(service_id, source, con, read_only, …)` built by a FastAPI dependency
- **`require_service_access`** (`backend/deps.py:200`) — standalone tenancy check, separately called from many routes
- **`_meta_con`** parallel path (`backend/deps.py:233`) — a metadata-only connection variant carried alongside the read pool
- **Ad-hoc temp tables** — per-window temps built by the first repository that needs them, recomputed by later repositories in the same request
- **`_read_only` private attribute trick** (`backend/deps.py:84-92`) — security-load-bearing guard that exists *only* because FastAPI converts primitive-typed dep params into query params, which would expose `read_only=False` to attackers
- **`process_context_scope` vs `set_process_context`** — two slightly different ways to install per-request state into the iothread mirror

Every router has to know the difference. New routes get one of these wrong roughly half the time.

## Decision

One `RequestContext` object lives in `backend/core/request_context.py`. It is constructed by a single FastAPI dependency and owns everything per-request:

```python
@dataclass(slots=True)
class RequestContext:
    service_id: str
    source: dict          # service config dict (read-only)
    con: duckdb.DuckDBPyConnection
    telemetry: RequestTelemetry  # the OTel-backed wrapper (ADR-04, Phase 1)
    analyst_session: AnalystSession | None
    cached_temps: dict[str, str]  # window-hash → temp-table name
    read_only: bool       # constructor-only; not exposable as a query param
    time_bounds: TimeBounds | None  # analyst clamp window, resolved once at build
    # Plus a private `_holder` field (the pool connection holder; the
    # dependency's finally hands it back to the pool) and a
    # `clamp(start, end)` method that applies `time_bounds` against the
    # analyst session so the standard handlers don't each carry the clamp
    # dependency. Both are implementation detail, not public surface.
```

Rules:

- **No re-resolution mid-request.** Once the context is built, service / source / connection don't change.
- **Tenancy is structural.** `RequestContext` cannot be constructed without `require_service_access`-equivalent enforcement running first. There is no path that builds a context that hasn't been gated.
- **`read_only` is a constructor argument, not a dep param.** Phase 2 moves it out of the `AnalyticsDeps` public dataclass into the context constructor, eliminating the `_read_only` private-attribute workaround.
- **`cached_temps` is shared across repositories.** First repo to need a window-temp builds and inserts; later repos in the same request reuse via the shared dict. Eliminates the recurring "share live-hour temp between dashboard CTEs and top_n_rollups" rework (commits ef44282 / 172537c).
- **Background work uses a `BackgroundContext`**, not `RequestContext`. The two get separated in Phase 10 once Phase 1 OTel context propagation makes the iothread mirror redundant.

## Consequences

- `AnalyticsDeps` was aliased to `RequestContext` through Phases 2–7 for backward compat; the alias was removed at the Phase 8 hard cutover (**done** — pinned by `tests/test_deps.py::test_analytics_deps_symbol_removed`).
- The `_meta_con` parallel path was dropped in Phase 8 (**done** — pinned by `tests/test_deps.py::test_get_meta_con_symbol_removed`): the Phase 4 storage carve-up means metadata queries no longer pay the Iceberg view cost; they share the same connection.
- Phase 2 migrates routers in order: dashboard → query → security → alerts/network/performance/origin/sessions/insights/views/bootstrap. Admin / provision / share routers stay on the old shape until Phase 5b (different connection pattern).
- The `_read_only` private-attribute pattern (and its security regression test) is structurally eliminated; tests asserting it become dead.

## Out of scope

- Multi-tenant request fan-out (one request → multiple services). Not a product requirement.
- Async-aware DuckDB connections. DuckDB is sync; FastAPI's threadpool handles it.
