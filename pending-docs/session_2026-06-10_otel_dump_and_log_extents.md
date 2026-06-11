# Prod log spam + analyst 403 polling — session report (2026-06-10)

**Branch:** `refactor/cleanup` · **HEAD at end of session:** `0f8331c`
**Deployed:** GCE `fastly-log-analysis` in `us-central1-a` at `0f8331c`

## What this doc is

The user pasted a snippet of prod backend logs showing (a) a `403 Forbidden` on `GET /api/sync-status?skip_fos=true` from a non-loopback IP, and (b) a JSON-encoded OpenTelemetry metric dump that's been polluting the log stream every 60s. This doc captures the diagnosis, the two fixes that landed, the deploy, and the follow-ups that didn't.

## What was observed

```
backend-1  | INFO:     167.82.237.92:0 - "GET /api/sync-status?skip_fos=true HTTP/1.1" 403 Forbidden
backend-1  | {"resource_metrics":[…full OTel SDK dump, ~50 lines…]}
```

Two separate signals, two unrelated causes.

## Root cause #1 — the 403

`/api/sync-status` is listed in [`_ANALYST_BLOCKED_SUBPATHS`](backend/utils/remote_access.py#L94) (N-3: leaks `ngwaf_workspace_id` + active cron task state). A non-loopback peer is classified as a remote analyst by [`is_request_remote`](backend/utils/remote_access.py#L222), the middleware fast-returns 403 before the route runs.

**The middleware is doing its job.** The bug is on the frontend: [`FilterBar.tsx:118-139`](frontend/components/FilterBar/FilterBar.tsx#L118-L139) unconditionally polls `/api/sync-status?skip_fos=true` with a 3s `refetchInterval` until it sees populated `earliest_log_at` — which never arrives for an analyst because `data` is `undefined` on 403. Result for every analyst dashboard load:

- A 403 every 3s, indefinitely
- FilterBar never auto-snaps to available log extents; falls back to "last 24h from now"

## Root cause #2 — the OTel JSON dump

[`backend/core/request_telemetry.py:79-88`](backend/core/request_telemetry.py#L79-L88) hardcoded `ConsoleMetricExporter` + `ConsoleSpanExporter`. The module's own docstring already flagged this as a v2.0-ship placeholder pending a "post-v2.0 deploy-config decision." Defaults were `OTEL_ENABLED=1` (on) with no exporter knob, so prod dumped:

- A `BatchSpanProcessor → ConsoleSpanExporter` JSON blob for every request
- A `PeriodicExportingMetricReader` snapshot every 60s

That's the bulk of recent prod log volume.

## Decision

User chose **both** fixes:

1. **OTEL gate** — make the exporter explicit and opt-in; prod default = no exporter installed.
2. **Analyst-safe `/api/log-extents`** — sibling of `/api/sync-status` that returns only the two fields FilterBar reads, with no `ngwaf_workspace_id` / `active_run` / cron state. Middleware allows it through because it isn't under any blocked prefix; the service-scope gate still enforces the analyst's allowlist.

Frontend FilterBar swap was deliberately deferred — the user had parallel in-flight work in [`FilterBar.tsx`](frontend/components/FilterBar/FilterBar.tsx) that I shouldn't have collided with.

## What landed

Two commits, two files I wrote that landed bundled into a third (someone else's commit ran while I was working and swept up the staged-but-unstaged tree):

- **`a672124` "feat: UX, accessibility, and performance improvements"** (not mine, but swept up my edits to):
  - [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py) — split `_otel_enabled()` from new `_otel_exporter()`. Master switch + exporter choice. Default `OTEL_EXPORTER=none` installs no exporter — providers aren't even spun up, so spans/metrics record against no-op globals and nothing leaves the process. `OTEL_EXPORTER=console` restores the old behavior for local dev. Unknown values log a warning and fall through to no-exporter.
  - [`backend/core/settings.py`](backend/core/settings.py#L79-L94) — new `otel_exporter: str` field, default `"none"`.
  - [`.env.example`](.env.example#L50-L55) — documented the opt-in.
  - [`backend/models/admin.py`](backend/models/admin.py#L69-L83) — new `LogExtentsResponse` with only `{configured, earliest_log_at, latest_log_at}`.
  - [`backend/routers/admin.py`](backend/routers/admin.py#L742-L774) — new `GET /api/log-extents`. Cache-only (reads `svcconfig.get_status`); no DuckDB connection, no 503 path, no contention with cron.
  - [`tests/core/test_request_telemetry.py`](tests/core/test_request_telemetry.py#L39-L66) — 3 tests covering the new gate (default OFF, `OTEL_EXPORTER=console` ON, `OTEL_ENABLED=0` overrides).
  - [`tests/core/test_settings.py`](tests/core/test_settings.py#L33-L57) — default assertion.

- **`0f8331c` "test/admin: cover /api/log-extents — extents reach FE, sensitive fields don't"** (mine):
  - [`tests/routers/test_admin_get_endpoints.py`](tests/routers/test_admin_get_endpoints.py#L110-L168) — 3 cases: unconfigured, cached extents, pre-first-tick empty. Each asserts `ngwaf_workspace_id` and `active_run` are NOT in the response.

271 tests passed across `tests/remote_access/`, `tests/routers/test_rbac_audit_fixes.py`, `tests/routers/test_cross_tenant_scope.py`, `tests/core/test_request_telemetry.py`, `tests/core/test_settings.py`, `tests/routers/test_admin_get_endpoints.py`.

## Deploy + verification

```
gcloud compute ssh fastly-log-analysis --zone=us-central1-a --command='~/restart.sh'
```

Restart pulled `0f8331c`, rebuilt all three containers, backend went healthy.

Post-restart loopback checks:

| Check | Result |
|---|---|
| Deployed commit | `0f8331c` |
| `/api/health` | `200` |
| `/api/log-extents` | `{"configured":true,"earliest_log_at":null,"latest_log_at":null}` — null because cron hadn't repopulated `cached_status` yet; fills on next tick |
| OTel JSON dump count in last 200 backend log lines | **0** |

Spam is dead. New endpoint is live and projects the response shape down correctly.

## Pre-commit mypy debt — discovered during commit

When I tried to commit `0f8331c`, pre-commit's mypy hook installed for the first time on this machine and surfaced **126 pre-existing errors across 17 files**, none in files I touched. Confirmed by checking `git show --stat a672124 | grep` against the failing files — `a672124` didn't touch any of them either. Previous commits got through because the mypy hook had never actually run before (the `pre-commit/mirrors-mypy` env wasn't installed in pre-commit's cache).

User approved `SKIP=mypy` for `0f8331c` (ruff, gitleaks, openapi-regen still ran) and asked me to scope a mypy-baseline plan. Below.

## Mypy debt — scoping (not started)

The 126 errors fall into 5 buckets. Recommended overall approach: **adopt mypy-baseline** (pin current 126 as the baseline, block only *new* errors), then burn the 126 down in small mergeable PRs.

| # | Bucket | Files | Approx errors | Effort |
|---|---|---|---|---|
| 1 | Missing stubs | `iceberg/sync.py`, `iceberg/view.py` (`dateutil`) | ~6 | 15 min — add `types-python-dateutil` to mypy's `additional_dependencies` in `.pre-commit-config.yaml` |
| 2 | Stale imports | `rollups.py` (`_get_service_lock`) | ~4 | 10 min — re-export from `backend.core.iceberg/__init__.py` or update the call sites |
| 3 | `int \| None` → `int` params | `cron/jobs/{metadata,compaction,commit}.py` | ~30 | 1 hr — relax helper signatures to accept `int \| None` (the helpers already handle unset), or narrow at call sites |
| 4 | `load_config(Any \| None)` | `iceberg/{view,buffer}.py` | ~5 | 30 min — relax `load_config` signature to `str \| None` (already gracefully handles unset) |
| 5 | Genuinely interesting | `field_registry.py`, `iceberg/fs.py`, `iceberg/buffer.py`, `main.py` | ~80 | the long tail; see below |

Bucket 5 detail:

- `field_registry.py:373-415` — `object` not narrowed before iteration / `int()`. Wants a `TypedDict` or `Mapping[str, object]` typing pass on the field-spec dicts.
- `iceberg/fs.py:69` — `Cannot assign to a method` (monkey-patching pattern). Either `# type: ignore[method-assign]` with a docstring pointing at [`MONKEYPATCHES.md`](MONKEYPATCHES.md), or refactor to a proper subclass.
- `iceberg/buffer.py:759,803` — `int` target assigned `list[str]` / `str`. Real-looking bugs; needs reading the surrounding code.
- `main.py:443` — `_MiddlewareFactory[P]` `.__name__` access (starlette typing gap). `getattr(mw, "__name__", repr(mw))` would shut it up.

**Suggested rollout:** bucket 1 first (15-min PR, unblocks dateutil import errors), then mypy-baseline pinning so pre-commit stops blocking unrelated commits, then 2 → 3 → 4 → 5 split further.

## Open follow-ups

1. **FilterBar swap to `/api/log-extents`** — backend is ready, frontend was deliberately not touched. Two-line change in [`FilterBar.tsx:118-125`](frontend/components/FilterBar/FilterBar.tsx#L118-L125):
   ```ts
   queryKey: ['log-extents', activeServiceId],
   queryFn: async () => {
     const { data } = await client.GET("/api/log-extents")
     return data
   },
   ```
   Drop `skip_fos: true`. Run `npm run gen:types` afterward to refresh [`frontend/openapi.json`](frontend/openapi.json) + [`frontend/types/api.generated.ts`](frontend/types/api.generated.ts). Once shipped, the 3s analyst-403 polling stops and snap-to-extents works for analysts.

2. **Mypy-baseline rollout** — buckets above.

3. **OTEL exporter wiring** — when there's a real OTLP collector to send to, add `OTEL_EXPORTER=otlp` plus the corresponding processor in [`_setup_sdk`](backend/core/request_telemetry.py#L86). The plumbing is already in place; today's change just defaults to no-exporter.
