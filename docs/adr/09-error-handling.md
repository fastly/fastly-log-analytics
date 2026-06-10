# ADR-09 — Error Handling, Retry, and Idempotency

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective ([pending-docs/velocity_improvements.md](../../pending-docs/velocity_improvements.md) Tier 2)

## 1. Context & Motivation

The codebase has three retry policies ([backend/utils/retry.py](../../backend/utils/retry.py)), a canonical self-heal primitive ([`execute_with_stale_view_retry`](../../backend/core/iceberg/view.py)), a two-layer compaction-race retry in [backend/routers/query.py:32–71](../../backend/routers/query.py), and an in-flight manifest pattern for crash-safe ingest ([backend/core/ingest.py:222–250](../../backend/core/ingest.py)). They work; they were each added in response to a specific incident; nobody has written down the model they collectively express.

The cost of not having the model written down: every new router and every new cron job re-derives whether/how to retry, and gets it slightly wrong. The 2026-06-10 stale-view incident, the 2026-05-21 ContextVar leak ([MONKEYPATCHES.md §6](../../MONKEYPATCHES.md)), and the compaction-race retry in `query.py` were all caught only because the same operator reviewed the same patterns three times. A second reviewer wouldn't have.

This ADR captures the model the existing code already expresses and makes it the contract for new code. It is not a redesign — every concrete claim below points at code that already works.

## 2. Decision

The codebase classifies operations along two axes: **what kind of failure can it survive** (retry classification) and **what must be true to make it safe to re-run** (idempotency contract). Every new endpoint, cron job, or sub-operation declares both implicitly by structure, and explicitly in its docstring when non-obvious.

### 2.1 Three retry tiers

| Tier | Trigger | Mechanism | Examples |
|---|---|---|---|
| **Fast-fail** | Auth, permission, validation errors (HTTP 4xx that aren't 429) | Raise to caller immediately; no retry adds value | `PermissionError`, `HTTPException(403)`, Pydantic validation |
| **Self-heal once** | Known transient races between writer and reader (compaction, view staleness) | Dedicated retry primitive with cache invalidation in the rebuild step | [`execute_with_stale_view_retry`](../../backend/core/iceberg/view.py) for "No files found"; outer loop in [backend/routers/query.py:32–71](../../backend/routers/query.py) for "Cannot open file" |
| **Bounded backoff** | Network / SDK / DB-WAL contention (HTTP 5xx, 429, connection drops, SQLite `database is locked`) | One of the three policies in [backend/utils/retry.py](../../backend/utils/retry.py) — `http_api_retry`, `sqlite_busy_retry`, `generic_network_retry` | Fastly API calls, share_db writes, DuckDB httpfs ops |

**Rules of thumb when adding a new retryable surface:**

- If the existing policy class fits, use it. Don't define a 4th retry decorator.
- A new self-heal primitive needs its own ADR amendment — it implies a new failure class we've decided is worth treating as recoverable. Don't quietly add one.
- Never retry a 401/403 / `PermissionError`. The user/auth state isn't going to change in the retry window.

### 2.2 The idempotency contract

**All cron jobs and all state-mutating operations MUST be safe to invoke twice.** "Twice" here means: if the process dies mid-operation, the next tick (or next request) can re-run the same logic without producing duplicate or corrupted state.

The codebase achieves this via four building-block patterns. Use one (or compose them); don't reinvent.

#### Pattern A — Mark-before-write
The ingest path is the canonical example: [`record_in_flight`](../../backend/core/metadata_db.py) writes a row to `ingest_in_flight` BEFORE the buffer parquet write; [`_recover_in_flight`](../../backend/core/ingest.py) reconciles on startup (promote if buffer exists, drop if missing). A crash between mark and commit leaves consistent state because the recovery scan always converges.

**Use when:** the operation writes to disk OR mutates external state (FOS, CDN config, etc.). The mark must include enough information to reconstruct intent (e.g., the deterministic buffer filename via `_deterministic_buffer_name`).

#### Pattern B — Deterministic naming
Buffer parquet files use [`_deterministic_buffer_name`](../../backend/core/ingest.py): a stable hash of the sorted input set. Re-running the same ingest produces the same filename; the second write is a no-op or trivially overwrites identical content.

**Use when:** you need the second run to find/recognize the first run's output without separate bookkeeping.

#### Pattern C — Upsert over delete-then-insert
The 2026-06-09 incident (CHANGELOG v1.2.0) showed delete-then-insert under concurrent flushes can lose rows. Use `INSERT … ON CONFLICT DO UPDATE` for repeated writes to the same logical key (e.g., service-config `last_updated` rollups, `usage_log` aggregation).

**Use when:** the second invocation should update existing state, not duplicate it.

#### Pattern D — Per-service RLock around file-system mutations
[`_get_service_lock`](../../backend/core/iceberg/view.py) serializes file-system mutations (compaction, view rebind) with dashboard query enumeration so the reader doesn't see "Cannot open file" mid-glob. The lock is held only for the FS mutation window — never across blocking I/O.

**Use when:** you delete, rename, or move files that a concurrent reader might enumerate. Document the held-region in a code comment.

### 2.3 Error classification at the boundary

What the HTTP layer surfaces to clients:

- **400-class** for caller-fault: validation, missing required param, unknown service ID, malformed query.
- **403** for tenancy/permission violations (the canonical surface; the [RequestContext](../../backend/core/request_context.py) gate enforces this structurally).
- **429** for self-imposed rate limits (e.g., long-running query already in flight for this service).
- **500-class** for server-fault, with the exception class name in the `detail` field when it doesn't leak sensitive info. **Never** include stack traces, query bodies, or internal paths in the response — those go to structlog only.
- **503** for "transient — retry later" when self-heal-once exhausts. Clients with React-Query default behavior will retry; clients without should treat as 5xx-class.

Internal background work (cron, rollups, gap_heal) does not have an HTTP surface. Failures are logged at `error` level with structured fields (`service_id`, `task`, `attempt`) and surface via `/api/admin/health-snapshot` `recent_cron_failures`.

### 2.4 What gets retried at the React-Query / frontend boundary

[React-Query](../../frontend/) defaults apply unless explicitly overridden:

- 4xx responses (except 408, 429) do **NOT** retry. The `STALE_VIEW_RETRY_OPTIONS` ([frontend/lib/staleViewRetry.ts](../../frontend/lib/)) variant exists for endpoints that legitimately need client-side patience while the backend rebuilds; use it sparingly.
- 5xx responses retry per React-Query's default exponential backoff up to 3 attempts.
- Network errors retry the same way.

Don't add request-level retry logic in the frontend on top of React-Query. The backend self-heal pattern (§2.2) means the second client call almost always succeeds; React-Query handles the wait.

## 3. Out of Scope

- **Circuit breakers / bulkheads / hystrix-style isolation.** Single-backend project; the failure surface that justifies a circuit breaker doesn't exist. Reach for the pattern only if a downstream dependency starts failing in a way that causes cascading slowness, which has not happened.
- **Graceful degradation cache layer.** The dashboard cache that v1.2.0 added then disabled (TTL=0) demonstrates the smell — caching to mask transient errors creates a coherence problem worse than the original symptom. Out of scope until a concrete cache-coherence design lands (it would itself need an ADR).
- **Per-request wall-clock timeout policy.** Uvicorn / DuckDB-level timeouts exist; how aggressively the application enforces them is per-endpoint and lives in [ADR-07](07-feature-budgets.md) budget statements.
- **Third-party SDK retry behavior modification.** The `aiobotocore` and `s3fs` patches in MONKEYPATCHES.md are the exception; future patches need their own MONKEYPATCHES.md entry with the incident date and a removal trigger.
- **Frontend error handling beyond React-Query.** Toast styling, error boundary UX, retry-affordance buttons live in the frontend codebase and are not the backend philosophy's concern.
- **Distributed consistency primitives.** Single process, single backend. We don't need consensus or distributed locks.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| New retry decorator added without ADR amendment | Code review catches; the three policies in `retry.py` cover everything that's come up. A 4th policy is a smell. |
| Cron job ships without idempotency contract | The next time the process dies mid-tick, the recovery path either no-ops (good) or duplicates state (bad). Adding tests in `tests/cron/test_*_idempotent_*` is the gate; the CONTRIBUTING.md PR checklist will flag a new cron without one. |
| Self-heal retry runs in a loop indefinitely | All self-heal primitives are bounded to a single retry. If the second attempt fails, the error surfaces. If you see infinite-retry behavior, the wrapping layer is recursing — read the call chain. |
| ContextVar isolation regression (cross-tenant leak) | Caught by [security_regression](../../tests/) test marker + audit-findings/ verified-fix list. The 2026-06-06 incident is the canonical case; the test floor (24) prevents silent removal of coverage. |
| Compaction-race retry exhausts | "Cannot open file" surfaces to caller. Indicates compaction is mutating files faster than the retry window; investigate compaction cadence + per-service lock contention. |
| Stale-view retry exhausts | "No files found" surfaces to caller after view rebuild. Indicates the rebuild itself returned an empty view — usually means the writer hasn't committed yet OR a tombstone-grace window edge. ADR-06 §5 covers diagnosis. |
| SQLite "database is locked" past `sqlite_busy_retry` window | WAL mode + bounded retry means this should be vanishingly rare; if it happens, log a `metadata_db_lock_exhaustion` event with the failing operation and investigate cron overlap. |

## 5. Verification

This ADR succeeds if:

- A new contributor adding a cron job grep-reads this doc, finds the "Pattern A — Mark-before-write" section, and applies it without help.
- `git log --grep="retry"` over the next year shows additions that fit one of the three tiers; nobody invents a 4th decorator.
- Production incidents that involve retry behavior cite a specific tier ("self-heal exhausted", "bounded backoff didn't recover") rather than "the retry didn't work."
- The security_regression test count stays at or above 24 — the floor that prevents regression of audited fixes.

It fails if the codebase accumulates `try/except`-with-sleep retry loops in router handlers, or if a new cron job ships without an `tests/cron/test_*_idempotent_*` companion.

## 6. Rollback

This ADR describes existing patterns; the patterns are load-bearing and cannot be rolled back without re-introducing the incidents that produced them. Rolling back the ADR means deleting the doc; the code stays.

If a specific decision in this ADR turns out wrong (e.g., we decide we DO want a 4th retry tier), amend the relevant section with the new decision and a one-line rationale. Don't delete and re-write — the change history is the audit trail.
