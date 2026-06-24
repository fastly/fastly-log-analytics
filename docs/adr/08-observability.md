# ADR-08 — Observability Strategy

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective (2026-06-10)

## 1. Context & Motivation

Phase 1 of the v2.0 cleanup wired OpenTelemetry (spans + metrics + structlog correlation), and Phase 6 added a custom pool-wait histogram. The mechanics work; what's missing is a doc that says **what** is monitored, **what** the operator should look at when something is wrong, and **how** the exporter is supposed to be turned on in a real production environment.

The 2026-06-10 OTel-spam incident is the canonical motivating failure: `OTEL_EXPORTER` defaulted to `console`, so every metric tick wrote a 50-line JSON blob to backend stdout and every request emitted a span dump. ~1 MB/min of unconsumable noise polluted prod logs for weeks before anyone noticed. The fix was structural — make the exporter opt-in — but the absence of a strategy doc meant nobody had a checklist that would have caught it pre-ship.

This ADR codifies the four things that need to be true to call the observability story "done enough" for a solo-dev project at this scale: a named set of signals, a named exporter target (or an honest "none, by design"), a debug playbook for the three failure modes that have actually happened, and a contract for what future endpoints/jobs MUST emit.

## 2. Decision

Observability is structured as four layers, each with a named owner, a defined output target, and a documented use case. Operators consult the layers in the order listed — the higher layers are cheap; the lower layers exist for when the higher ones aren't enough.

### 2.1 The four signal layers

| Layer | What it captures | Output | Consumed via |
|---|---|---|---|
| **HTTP responses** | per-request status, `BaseResponse._section_timings`, `_debug_queries` when `DEBUG_RESPONSES=1` | response body | browser devtools, ad-hoc `curl` |
| **Structlog (stdlib bridge)** | named events (`logger.info("event", key=val)`), warnings, errors, exceptions, OTel trace_id/span_id when a span is active; **uvicorn access/server logs** too — `bridge_uvicorn_loggers()` (lifespan startup) re-points uvicorn's loggers onto the root handler, so they render through the same chain (the two pre-startup boot lines are the only exception) | container stdout — console by default (what dev **and prod** emit today, since prod doesn't set `STRUCTLOG_FORMAT`); JSON lines when `STRUCTLOG_FORMAT=json`, wired but dormant until a log aggregator exists (TBD per §3) | `docker compose logs backend`, future log aggregator (TBD per §3) |
| **OpenTelemetry spans + metrics** | request-scoped spans, query/call sub-events, `app.thread_wait_ms` histogram, `app.duckdb_pool.*` counters | configurable via `OTEL_EXPORTER` (`none` / `console` / `otlp` when wired) | exporter destination |
| **Per-service usage_log SQLite** | every FOS/CDN op with byte count, duration, function caller, Class-A/B classification | per-service `metadata.db` → `/api/admin/log-accounting` endpoint | admin UI's cost panel |

### 2.2 Endpoint-class p95 targets (initial, revise on real data)

These are the budgets [ADR-07](07-feature-budgets.md) endpoints declare against. They are intentionally coarse — narrow per-endpoint targets live in the endpoint's docstring.

| Class | p95 warm | p95 cold | Notes |
|---|---|---|---|
| `/api/health`, `/api/log-extents`, nav/bootstrap | < 200ms | < 500ms | Cache-only or trivial |
| Dashboard analytics panels | < 800ms | < 1.5s | ADR-06 view warming covers cold |
| Sessions / raw-logs tables (paginated) | < 3s | < 5s | DuckDB query bound |
| Admin one-shot reports (`/api/admin/health-snapshot`, `/api/admin/log-accounting`) | < 10s | n/a | Manual operator pull |

These are not SLOs (we make no uptime promise). They are the line at which a regression is worth investigating; budget misses get revised, not fire-drilled, per ADR-07 §2.4.

### 2.3 Exporter wiring contract

`OTEL_EXPORTER` (read in [backend/core/request_telemetry.py](../../backend/core/request_telemetry.py)) is the single switch:

- `none` (default in prod) — SDK uninitialised; spans/metrics record against global no-op providers; zero network/disk cost. Right answer for "we're not running a collector yet."
- `console` (default in dev) — `ConsoleSpanExporter` + `ConsoleMetricExporter`. Writes to stdout. **Never set in prod** — the 2026-06-10 incident is the load-bearing reason.
- `otlp` (when a collector is provisioned) — wire in `_setup_sdk` ([backend/core/request_telemetry.py:86](../../backend/core/request_telemetry.py)). Sampling strategy: head-based 10% for spans, 60s metric export interval. Re-evaluate once we have a real exporter target.

The exporter choice is honest about reality: there is no production collector today, so the production default is `none`. The frontend bug-report flow + per-service `usage_log` queries cover the cost-attribution use case that an OTLP backend would otherwise own.

### 2.4 Debug playbook — the three failure modes we've actually seen

Each entry: symptom → first thing to look at → next thing → resolution if known. Add new entries here when an incident requires more than 10 minutes of code-reading to diagnose.

#### A. Dashboard panels stall during cron activity
**Symptom:** browser sees panel queries take seconds where they normally take hundreds of ms; correlated with sync/commit cron firing.
**Look at:** `app.thread_wait_ms` p95 on the dashboard endpoint. Pool-wait p95 > 50ms = pool saturation; > 200ms = red flag.
**Then:** check `/api/admin/health-snapshot` for `in_flight_runs` overlap. Check `cron_progress.list_active_runs()` output.
**Resolution:** ADR-06 (writer-driven view warming) — if cron isn't calling `warm_pool_for_service` after view-fingerprint mutations, dashboard readers eat the rebuild cost. Verify the `warm_pool_for_service` call landed in sync.py + commit.py.

#### B. Analyst dashboard 403 spam on admin endpoint
**Symptom:** backend stdout shows repeated `GET /api/<some-admin-endpoint> HTTP/1.1 403 Forbidden` from non-loopback IPs.
**Look at:** the endpoint path. Is it under `_ANALYST_BLOCKED_SUBPATHS` in [backend/utils/remote_access.py](../../backend/utils/remote_access.py)?
**Then:** find the FE caller. The frontend probably needs to switch to an analyst-safe sibling endpoint (ADR-12 sibling pattern), not paper over with a query param.
**Resolution:** the 2026-06-10 incident shipped `/api/log-extents` as the analyst-safe projection of `/api/sync-status`. New similar incidents follow the same pattern.

#### C. FOS / CDN cost spike
**Symptom:** Fastly billing or Class-A counter in `/api/admin/log-accounting` shows order-of-magnitude jump from baseline.
**Look at:** `usage_log` aggregates per `function_caller`. The 2026-05-20 incident was 517K manifest reads from one function path.
**Then:** check MONKEYPATCHES.md for related patches; if a patch was reverted or rebased away, it likely re-opened the regression. Check pyiceberg version pin in pyproject.toml.
**Resolution:** depends on root cause. Re-apply the relevant patch from MONKEYPATCHES.md or pin pyiceberg back to a known-good version.

### 2.5 What every new endpoint / cron job MUST emit

Minimum bar for "good citizen" instrumentation. Reviewer enforces at PR time:

- **HTTP endpoint**: nothing extra — `BaseResponse._section_timings` + structlog access log covers it. Only add a span event for sub-operations that take > 100ms.
- **Cron job**: must call `start_progress` / `finish_progress` ([backend/cron_progress.py](../../backend/cron_progress.py)) so `/api/admin/health-snapshot` reflects it. Errors must be `logger.exception("event", service_id=..., ...)`.
- **Sub-operation that hits FOS or CDN**: must flow through the existing helpers that write `usage_log` rows. Don't bypass; if a new code path needs FOS access, route it through the existing wrapper or add a wrapper that tags `function_caller`.
- **Anything user-facing > 100ms**: name an OTel sub-span. `start_as_current_span("descriptive_name")` so traces are inspectable when the exporter is wired.

## 3. Out of Scope

- **Picking a production OTLP collector vendor.** Vendor selection (Honeycomb, Grafana Cloud, self-hosted Jaeger) is deferred until there's a concrete need a `usage_log` query cannot answer. The exporter switch is in place; flipping it on is one commit.
- **Synthetic uptime monitoring.** External (UptimeRobot-style) probes aren't part of the application. Provision via infrastructure when an SLO is committed.
- **Alerting + paging infrastructure.** No SLO → no alerts to page on. Operator monitors `/api/admin/health-snapshot` manually until that changes.
- **Per-user cost attribution.** `usage_log.function_caller` is process-side; user-level billing is a different problem and is not implied by this ADR.
- **Distributed tracing across services.** Single backend, no service mesh. Not applicable.
- **Frontend perf budgets (LCP, TBT, bundle size).** Owned by [ADR-05](05-frontend-rendering-boundary.md).
- **Cache-coherence state-machine abstractions.** Explicitly rejected by the v2.0 retrospective based on the 2026-06-09 incident analysis — the bottleneck is DuckDB view rebuild time, not cache policy.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| `OTEL_EXPORTER=console` accidentally set in prod | Backend stdout floods with JSON blobs. Detection: `docker compose logs --tail 200 backend \| grep -c resource_metrics`. Recovery: unset env var or set to `none`, restart container. The 2026-06-10 incident is the canonical case; the doc above § "what every new endpoint MUST emit" exists so this can't recur silently. |
| OTel SDK init fails at boot | `request_telemetry.py` catches and logs at warning; SDK falls back to no-op providers. Recovery: the app starts; instrumentation is silently no-op. Look at boot logs for "OTel SDK init failed". |
| structlog config breaks (e.g. processor exception) | Process exit at boot. Recovery: revert the structlog change; structlog is load-bearing for the access log. Verify locally with `STRUCTLOG_FORMAT=json` before shipping any structlog-config change. |
| `usage_log` per-service SQLite locks / fills disk | Cron writes start failing with `database is locked` or `disk full`. Recovery: archive or truncate `usage_log` table (it's a rolling log, no retention contract). Investigate disk pressure. |
| `app.thread_wait_ms` histogram reports p95 > 200ms sustained | Real pool saturation. Recovery: per ADR-06 escalation order — verify warm_pool_for_service is wired, then consider raising `DUCKDB_POOL_MAX_SIZE` per-service, then consider separate cron-side pool. |
| `cron_progress` shows a stuck "running" status for a job that's actually dead | Process restart should have reaped via `_check_terminal_status_from_db`. Recovery: if it didn't, mark manually via SQL: `UPDATE cron_runs SET status='error' WHERE id=...`. |

## 5. Verification

This ADR has succeeded if, six months from now:

- A new endpoint shipped without instrumentation triggers a PR comment ("does this need a sub-span? does it write `usage_log`?") rather than landing silently.
- A new operator can read the four-layer table + debug playbook above and diagnose B (analyst 403) or A (cron stall) without code archaeology.
- `OTEL_EXPORTER=console` does not appear in any deployed env file — enforced by [scripts/check_no_console_otel.sh](../../scripts/check_no_console_otel.sh), wired into the CI backend job and `make ci` (the grep CI step this section called for).
- The Phase 6 thread-wait histogram is still emitting (instrumentation hasn't bit-rotted) — verifiable via `/api/admin/health-snapshot` `pool_wait` field.

It has failed if observability decisions get made ad-hoc per endpoint, if the debug playbook hasn't grown to include incidents that demonstrably required code-reading to diagnose, or if `OTEL_EXPORTER` defaults change without an explicit ADR amendment.

## 6. Rollback

This ADR documents existing behavior; rolling back means undoing the strategy, not the code. Concretely:

- Delete this ADR; the OTel + structlog + usage_log code stays in place and keeps working.
- Remove the `STRUCTLOG_FORMAT` ([backend/utils/structlog_config.py](../../backend/utils/structlog_config.py)) / `OTEL_EXPORTER` ([backend/core/request_telemetry.py](../../backend/core/request_telemetry.py)) defaults if reverting to "no observability strategy."

No code changes, no infrastructure changes. The instrumentation is decoupled from the strategy doc.
