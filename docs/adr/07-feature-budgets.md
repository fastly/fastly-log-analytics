# ADR-07 — Per-Feature Performance & Cost Budgets

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective (pending-docs/velocity_improvements.md)

## 1. Context & Motivation

The v1.2.0 dashboard performance overhaul (connection pool, rollup precompute, view warming per ADR-06) was driven by post-release telemetry rather than upfront design — by the time perf showed up as a problem, the fix touched eight files and two architecture layers. The retrospective ([pending-docs/velocity_improvements.md](../../pending-docs/velocity_improvements.md)) named this as the highest-leverage gap remaining: every new analytics endpoint that ships without a stated budget is a candidate for the same reactive cycle.

The cost of catching debt at PR time is roughly zero — five minutes of self-questioning per route. The cost of catching it after merge is hours of refactoring once the slow query is in production and users have noticed. This ADR captures the discipline that turns the post-hoc cleanup pattern into a pre-commit pattern.

The budget is **not** an SLO and is **not** wired into CI. It is a forcing function for the PR author to think about scale before merging. A budget that turns out to be wrong is fine; an endpoint that shipped without anyone thinking about its cost is the failure mode.

## 2. Decision

Every new query/analytics endpoint declares a **performance budget** and a **cost budget** in its route docstring (FastAPI) or route comment, at the point of definition. Endpoints that pre-date this ADR are grandfathered; budgets get added when they're next materially modified.

Existing endpoints that already have documented perf characteristics (e.g. the warmed dashboard panel queries, the rollup-backed aggregates) need no immediate change — those characteristics ARE the budget.

### 2.1 What "budget" means concretely

Two numbers and one boundary:

| Field | What it states | Typical values |
|---|---|---|
| **`p95_target`** | Wall-clock latency at p95 under realistic load (cold + warm). | `< 800ms` for dashboard panels; `< 200ms` for nav/bootstrap; `< 3s` for sessions/raw-logs tables; `< 10s` for admin-only one-shot reports. |
| **`storage_growth`** | Expected storage cost per service per month at the documented log volume. Zero if the endpoint is read-only over existing tables. | `+0 GB/mo` (pure read); `+X MB/mo per Y reqs/s` (writes a rollup); `negligible` (caches only). |
| **`scale_boundary`** | The traffic / data shape past which the budget no longer holds and a different design is needed. | "10× current request rate;" "100× current service count;" "single-service, single-operator." |

A fourth optional field — **`degrades_to`** — names the graceful-degradation behavior past the boundary (slower, partial, cached, returns 503). Optional because not every endpoint needs it; required for anything in the dashboard's critical path.

### 2.2 Format

In a FastAPI route docstring:

```python
@router.post("/api/security/aggregates")
async def security_aggregates(...):
    """Bot fingerprint + header anomaly aggregates for the security page.

    Budget (ADR-07):
        p95_target:     < 800ms warm, < 1.5s cold
        storage_growth: +0 GB/mo (pure read over iceberg + ngwaf cache)
        scale_boundary: ~5M req/day per service; degrades to longer
                        bucket_seconds past that.
        degrades_to:    falls back to 1-hour buckets above 5M req/day.
    """
```

In a cron job or background task, the same block goes in the function docstring or the YAML config.

No template enforcement. The format above is the canonical one; minor variations (different field names, prose justifications) are fine as long as the four ideas are present. Cargo-culted "p95: TBD" entries are worse than no budget — if you genuinely don't know, write that.

### 2.3 What triggers budget review

A PR triggers budget review (author writes one; reviewer checks it) when ANY of these is true:

- Adds a new HTTP route under `/api/*`
- Adds a new repository function that runs an unbounded SQL query (no `LIMIT`, no time-window filter, scans full iceberg table)
- Adds a new cron job, scheduled task, or background worker
- Materially changes the query shape or data path of an existing endpoint with a documented budget (the budget gets re-stated, possibly revised)
- Adds a new persistent cache, materialized view, or rollup table

A PR does NOT trigger budget review when:

- Only frontend code changes (UI-only)
- Only documentation, tests, or refactors with no behavioral change
- Configuration/secret changes
- Adding a new admin-only endpoint that's called manually and isn't on a hot path (state this explicitly in the budget rather than skipping it)

### 2.4 What happens when a budget is missed in production

The endpoint stays in production; the budget gets revised in a follow-up PR with a one-line rationale (`"raised p95_target from 800ms → 1.2s on 2026-XX-XX after sessions table grew past 50M rows"`). The revised budget triggers a separate conversation about whether the design needs to change.

A budget miss is not a bug to fire-drill — it's a signal that the scale model changed. The fire-drill threshold is "budget missed AND users noticed."

## 3. Out of Scope

- **Global SLOs.** This ADR is per-feature, not service-wide. Service SLOs (uptime, error rate) belong in a future observability ADR (see [pending-docs/velocity_improvements.md](../../pending-docs/velocity_improvements.md) Tier 2 — "Observability strategy").
- **CI enforcement.** No linter checks for the budget block. The cost of enforcement (lint rule, parser, escape hatches) outweighs the benefit for a solo-dev project where every PR has a human reviewer (the author). Re-evaluate if/when the project grows beyond one regular contributor.
- **Cost-per-request accounting.** Real-time cost attribution per endpoint (FOS Class A/B ops, CDN egress, DuckDB compute time) is a separate problem. The `storage_growth` and `scale_boundary` fields here are coarse-grained estimates for design-time reasoning, not finance-grade.
- **Frontend perf budgets.** Bundle size, route-level LCP/TBT, and chart render time live in [docs/adr/05-frontend-rendering-boundary.md](05-frontend-rendering-boundary.md). This ADR is backend/API only.
- **Cost-modeling for the existing endpoint catalog.** Retrofitting budgets to every endpoint is busywork; grandfather them and document on next material change.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| Author writes a vague budget (`"p95: fast enough"`) | Reviewer pushes back; if reviewer is the same person as author, they push back on themselves at re-read time. The format above gives concrete anchor points. |
| Budget turns out to be optimistic and the endpoint is slow in prod | Revise budget in a follow-up; investigate whether the design needs to change. Slow endpoint stays in production; revising the budget is the cheap path. |
| Budget is so conservative it blocks a useful feature | Loosen it. The budget is a forcing function for thinking, not a contract with users. Document the loosening with a one-line rationale. |
| PR author skips the budget block entirely | Reviewer flags it as a missing checklist item ([CONTRIBUTING.md](../../CONTRIBUTING.md)). For solo work, leaving it out is a signal to stop and think — not a bug. |
| Endpoint genuinely cannot be budgeted (e.g., an open-ended user query) | State that explicitly: `"unbounded by design — capped at 30s server-side timeout, otherwise returns 504."` That IS the budget. |

## 5. Verification

This ADR has succeeded if, six months from now, the project history shows:

- New endpoints have documented budgets in their docstrings (spot-check via `grep -r 'Budget (ADR-07)' backend/routers/`).
- At least one PR explicitly re-stated a budget that turned out wrong (evidence the discipline is being used, not performed).
- The next perf incident is investigated against an existing budget rather than against a vague memory of "should be fast."
- No CI workflow exists to enforce the format (we did not over-engineer it).

It has failed if budgets appear once and then stop, or every endpoint ships with the same boilerplate budget regardless of actual scale assumptions.

## 6. Rollback

If the discipline turns out to be ceremony-without-value:

- Delete this ADR.
- Remove the corresponding PR checklist item from [CONTRIBUTING.md](../../CONTRIBUTING.md).
- Leave existing `Budget (ADR-07):` docstrings in place — they're free-text and harmless even without an enforcing ADR.

No code changes, no schema changes, no infrastructure changes. The cost of rollback is one Edit and one git revert.
