# QA UX & RBAC Audit — June 10, 2026

**Targets:** `https://fastly-log-analytics.global.ssl.fastly.net/` (analyst, shared session) and `http://localhost:3001/` (admin, loopback).
**Methodology:** live browser drive (browser-use CLI / Chromium), authenticated `fetch()` RBAC probes from inside the analyst session, static walk of `backend/routers/` (172 endpoints across 22 router files), and side-by-side visual comparison against the June 9 audit doc.
**Predecessor:** [qa_ux_and_rbac_security_audit.md](qa_ux_and_rbac_security_audit.md). This doc supersedes that one for fix-state tracking and adds 10 new findings (N-1 through N-10) that surfaced today.

---

## TL;DR

Commit `1b53dcf RBAC + UX audit fixes: H-1, H-2, H-3, H-4, M-1, L-1, L-2` landed most of the June 9 high-severity items. Re-probing the same surface today:

- **Closed:** H-1 (usage), H-2 (download), H-4 (scoring config/status/audit/threshold/exclude-regex/enforce-status-code), partial H-3 (cron-schedule + lake-info), M-2 (origin loading state), M-7 (admin usage-log empty cards). Frontend gates for `/usage`, `/admin`, `/admin/share`, `/admin/session-scoring` confirmed working.
- **Still open from June 9:** H-3 partial (`logging-settings`, `log-fields`), H-5 (cron-runs, audit-logs), H-6 (custom-fields export), H-7 (alerts surface), M-3 (body service_id silent fallback), M-4 (network map empty), M-6 partial (still leaks one IO error + DuckDB path, but consolidated to one card/retry).
- **New today:** 10 findings, of which **N-1 (operator-internal `_debug_calls` envelope leaks Fastly KV store ID on every response, including 403s)** is the highest-severity discovery — it is the same class of leak H-4 was meant to fix, via a different vector that defeats every endpoint-level block. Also **N-4 (the `/logs` Data Management page is open to analysts)** is effectively a regression / never-implemented frontend gate, broader than the H-5 endpoint leak.

Highest single fix-priority right now: **strip the `_debug_*` envelope for analyst responses before any other RBAC line-item.** It moots the per-endpoint scoring KV-ID hardening because the operator's KV store ID, outbound API paths, and execution timings leak through every authorized response anyway.

---

## Section 1 — Fix-state since the June 9 audit

Re-ran the same authenticated `fetch()` probe today (20 endpoints, plus 17 service-scoped, plus 29 deep-surface probes — 66 in total).

### 1.1 Confirmed FIXED ✅

| Item | June 9 status | June 10 status | How verified |
|---|---|---|---|
| H-1 `/api/usage/{prefill,current-storage,operations,bandwidth,log-activity}` | 5× 200 (data leak) | 5× 403 | live probe + `_ANALYST_BLOCKED_PREFIXES` now contains `/api/usage/` |
| H-2 `/api/download`, `/api/download-all`, `/api/download-folder` | 200/502/200 | 3× 403 | live probe + `_ANALYST_BLOCKED_SUBPATHS` list |
| H-3 (partial) `/api/cron-schedule`, `/api/services/{id}/lake-info` | 200 / 200 | 403 / 403 | live probe + blocked subpath regex |
| H-4 `/api/services/{id}/scoring/{config,status,audit,threshold,exclude-regex,enforce-status-code}` | 6× 200 | 6× 403 | live probe + `_ANALYST_BLOCKED_SCORING_SUFFIXES` |
| Frontend gate `/usage` | renders for analyst | redirects to `/dashboard` | typed in address bar |
| Frontend gate `/admin`, `/admin/share`, `/admin/session-scoring` | redirects | still redirects | typed in address bar |
| M-2 Origin metric cards loading overlap | placeholder values under spinner | values render cleanly (no overlap) | screenshot |
| M-7 Admin `/admin/usage-log` top metric cards empty | skeletons never resolved | renders full breakdown (Class A 535.2K, Class B 194.7K, CDN Egress 1.33GB, $3.03) | screenshot |
| L-1 / L-2 / M-1 (per commit message) | — | claimed in commit `1b53dcf` | not separately re-verified |

### 1.2 Confirmed STILL OPEN ❌

| Item | Endpoint / Page | Status today | Bytes | Notes |
|---|---|---|---|---|
| H-3 partial | `GET /api/services/{id}/logging-settings` | 200 | 729 | not in blocked-subpath regex |
| H-3 partial | `GET /api/services/{id}/log-fields` | 200 | 463 | not blocked; schema leak |
| H-5 | `GET /api/cron-runs` | 200 | **19,161** | full ingestion task history with absolute paths |
| H-5 | `GET /api/audit-logs` | 200 | 1,104 | admin audit trail |
| H-6 | `GET /api/services/{id}/custom-fields/export` | 200 | 2,006 | VCL schema dump |
| H-7 | `GET /api/alerts/`, `/api/alerts/{service_id}` | 200 / 200 | 140 each | analyst-visible alert config |
| M-3 | `POST /api/dashboard/aggregates` with `body.service_id = "FAKE"` | **200, 1.6 MB** | — | silently falls back to authorized service; body field ignored |
| M-4 | `/network` world map | empty at default range, "Worst Region: --" | — | visual |
| M-6 (partial) | `/admin/session-scoring` Scoring Health card | 1 error card (down from 3), 1 Retry button (down from 3) | — | but still surfaces `IO Error: No files found that match the pattern "cache/fos-<redacted-service-id>-logs/buffer/batch_1c41b1ca5dcf1eac.parquet"` verbatim |

### 1.3 Control endpoints (still SECURE)

All `/api/admin/*` (incl. `share/banner`, `share/status`, `share/audit-logs`, `share/wordphrase`), `/api/provision/*`, `/api/debug/*`, `/api/cron-runs/{id}/stream` (SSE), and every PUT/PATCH/DELETE/non-allowed POST returned 403. The middleware's verb-gate and prefix-block work as designed for the items that ARE in the block list.

---

## Section 2 — NEW findings (June 10)

### 🚨 N-1 (CRITICAL) — `_debug_calls` envelope leaks Fastly KV store ID on every response

**Severity:** Critical — operator infra ID leak. Same class as H-4, defeats it.
**Where:** every JSON response from the backend to the analyst.
**What:** Every payload includes a 4-field envelope:
```json
{ "...real fields...",
  "_debug_queries":  [ { "sql": "CREATE TEMP TABLE ... SELECT ...", ... } ],
  "_debug_calls":    [ { "service": "Fastly API",
                          "method":  "GET",
                          "path":    "/resources/stores/config/<redacted-kv-store-id>/item/enforce_threshold",
                          "time_ms": 217.82,
                          "status":  "Error" } ],
  "_section_timings": [...],
  "_is_cached":       false }
```

Concrete leak observed today on the live analyst session:
- `GET /api/services/{id}/scoring/enforce-threshold` (which itself is N-5 below) → `_debug_calls[0].path` = `/resources/stores/config/<redacted-kv-store-id>/item/enforce_threshold` — **discloses the Fastly KV store ID `<redacted-kv-store-id>`** to the analyst.
- `POST /api/dashboard/aggregates` → `_debug_queries[0].sql` contains the full raw DuckDB SQL the backend executed, including internal temp-table names (`t_live_hour_ddbc4cc387824a9c9e35114177ddcc3a`), column lists, and filter clauses.
- `POST /api/views/` (which returns 403 read_only) → response body is `{"error":"read_only","_debug_queries":[],"_debug_calls":[],"_is_cached":false}` — **envelope leaks on error responses too**.

**Why this is critical:** H-4 was conceived as protecting the KV store ID from leaking through `/scoring/config`. The fix correctly 403s `/scoring/config`. But `_debug_calls` carries the *same ID* on every other authorized response that happens to make a Fastly KV API call. Per memory note `infra-stays-local` (fastly/fastly-log-analytics is a PUBLIC repo and any specific KV/bucket/service IDs must stay out of analyst-reachable surfaces), this is exactly the kind of operator string that must not appear in payloads to share-analysts.

**Recommended fix:**
- In the response middleware (or wherever the debug envelope is attached), gate the four `_debug_*` keys on `is_remote_analyst() == False`. Operators in the loopback admin still get them.
- Add a regression test: authenticated analyst session `fetch("/api/services/{id}/scoring/enforce-threshold")` (or any handler that performs a Fastly KV read) → response JSON must NOT contain `_debug_calls`, `_debug_queries`, `_section_timings`, `_is_cached`.
- Alternative if removal would change frontend behavior: scrub the `path` field to strip `/resources/stores/config/[^/]+/`, strip the SQL body, keep aggregate timings only.

---

### 🚨 N-2 (HIGH) — `/api/services` over-shares operator config

**Where:** `GET /api/services` (defined in `backend/routers/services/core.py`).
**Status:** 200, 11,443 bytes for an analyst with one authorized service.

The response includes the authorized service object with these operator-internal fields:
- `cdn_url` — Fastly-fronted bucket URL
- `cdn_service_id` — the Fastly CDN service ID
- `fos_bucket` — Fastly Object Storage bucket name
- `ngwaf_workspace_id` — e.g. `"<redacted-workspace>"`
- `duckdb_exists`, `duckdb_size_bytes` (475,865,440), `cache_file_count`, `log_row_count` — backend storage internals
- `storage_mode`, `access_level`, `status` — operator config flags

Per memory note `infra-stays-local` and `dev-sandbox-scrub`, `cdn_url` and `ngwaf_workspace_id` should never reach an analyst payload (the Fastly-fronted bucket is publicly readable without creds; the NGWAF workspace ID identifies the operator's WAF tenancy).

**Recommended fix:** Build an `AnalystServiceView` whitelist with only the fields the frontend actually consumes (likely `service_id`, `name`, `is_active`, and maybe `log_row_count` for the "Total Logs" header counter — confirm via `git grep "services\[" frontend/`). Strip everything else for non-admin responses.

---

### 🚨 N-3 (HIGH) — `/api/sync-status` mounted outside `/api/admin/`, leaks operator state

**Where:** `GET /api/sync-status` (in `backend/routers/admin.py`, but the path lacks the `/admin/` prefix so the block-prefix gate misses it).
**Status:** 200, 514 bytes to analyst.

Today's response (formatted):
```json
{"configured":true,"busy":true,"storage_mode":"cloud","access_level":"read_write",
 "local_rows":7568974,"latest_ingested_file_at":"2026-06-10 17:01:10",
 "duckdb_size_bytes":475865440,"duckdb_exists":true,
 "active_run":{"type":"status","message":"0.0s   ↳ Downloading and parsing the latest catalog metadata (this may take 5-10 seconds)...","task":"metadata_sync"},
 "ngwaf_workspace_id":"<redacted-workspace>",
 "_debug_queries":[],"_debug_calls":[],"_is_cached":false,"_section_timings":[]}
```

Leaks: NGWAF workspace ID, DuckDB internals, the active background task's internal task name and live progress message. The analyst frontend almost certainly doesn't need any of this — the analyst doesn't trigger syncs.

**Recommended fix:** Move `/api/sync-status` under `/api/admin/sync-status` (the prefix block then catches it for free). Update the one or two frontend admin-side callers. If there's an analyst-side caller (verify with `git grep "/api/sync-status" frontend/`), replace it with a 2-field stub (`configured`, `local_rows`) returned from `/api/bootstrap`.

---

### 🚨 N-4 (HIGH) — `/logs` (Data Management) page is open to analysts

**Where:** Frontend `/logs` route (the "Data Management" nav entry, hidden in the analyst sidebar but reachable by typing the URL or via a stale tab/bookmark).

**Observed today as analyst:** Typed `https://fastly-log-analytics.global.ssl.fastly.net/logs` — the page renders the full Data Management UI, including:
- 4 admin-write quick-action buttons: `Import Logs`, `Commit Buffer`, `NGWAF Bot Sync`, `View Recent Logs`
- 7 tabs: Cron Runs (works via H-5 leak), Service History (works), Ingestion History (works), Iceberg Storage (broken, 403), Metadata Storage (broken, 403), Available Logs (broken, 403), DuckDB Schema (broken, 403)
- A `Purge Logs` button
- The "Background Sync Completed" toast popping in real time

The button clicks themselves would fail (the backend verb-gate + admin prefix block correctly returns 403 — verified for `/api/admin/{raw-tree,iceberg-tree,iceberg-info,metadata-storage,log-accounting,health-snapshot,compaction-stats,pop-locations,bot-sources}` — 9 of 9 returned 403). But:
- The page render itself is a UX failure (broken tabs, dead buttons).
- The `Cron Runs` / `Service History` / `Ingestion History` tabs DO populate because they call H-5's still-unfixed `/api/cron-runs` endpoint. So the analyst sees real ingestion task history including durations and file counts.
- The previous audit doc claimed `/logs` was redirected for analysts. It isn't — there is no `pathname.startsWith('/logs')` branch in the AppLayout gate (search for it). Either it was never added, or it was removed when `/usage` and `/alerts` were added to the redirect block.

**Recommended fix:** Extend the analyst frontend redirect block to cover `/logs`:
```diff
-    if (isAnalyst && (pathname.startsWith('/usage') || pathname.startsWith('/alerts'))) {
+    if (isAnalyst && (pathname.startsWith('/usage') || pathname.startsWith('/alerts') || pathname.startsWith('/logs'))) {
       React.startTransition(() => router.replace(activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'))
       return
     }
```
Defense-in-depth: also gate the `/api/cron-runs` and `/api/audit-logs` GETs (those are H-5 above) — fixing the frontend redirect without those leaves the H-5 endpoints separately reachable by direct `fetch()`.

---

### 🚨 N-5 (HIGH) — Additional `session_scoring_admin` bypasses outside the H-4 suffix list

**Where:** `backend/routers/services/session_scoring_admin.py` mounts under `/api/services/{id}/scoring/`. The H-4 fix's `_ANALYST_BLOCKED_SCORING_SUFFIXES` lists 6 suffixes (`/config`, `/status`, `/audit`, `/threshold`, `/exclude-regex`, `/enforce-status-code`) but the router has MORE admin reads not in the list:

| Endpoint | Status today | Bytes | Leak |
|---|---|---|---|
| `GET /api/services/{id}/scoring/matrix-versions` | 200 | 395 | ML matrix version history (empty here but exposed) |
| `GET /api/services/{id}/scoring/enforce-threshold` | 200 | 343 | operator's enforcement decision **AND** Fastly KV store ID via `_debug_calls` (see N-1) |

The H-4 design comment in `remote_access.py:88-100` was careful to keep `/threshold-preview`, `/labels`, `/sessions/.../events`, `/top-flagged`, `/score-distribution`, `/compliance-breakdown`, `/health`, `/evaluation`, `/curves` reachable for analysts (they're analytics, not config). But `matrix-versions` and `enforce-threshold` slipped through — both are operator-only.

**Recommended fix:** Add `"/matrix-versions"` and `"/enforce-threshold"` to `_ANALYST_BLOCKED_SCORING_SUFFIXES`. While re-auditing this list, also check `/scoring/dashboard` and `/scoring/evaluation/per-reason` — both returned 400 today (so they reached the handler but failed), which means the block didn't fire on them either; the handler simply didn't accept the analyst's request shape. A malformed-but-accepted request might leak more. Either block these too or confirm the handler-level guard.

---

### ⚠️ N-6 (MEDIUM) — Save View silently fails for analyst (same pattern as Alerts M-1)

**Where:** `/dashboard` → "Save View" button → "Save Current View" modal.
**Observed:** Filled "QA test view" in the Name field, clicked "Save View". Modal stays open, button re-enables, no toast, no banner, no field-level error. Console shows the 403 from `POST /api/views/` (`{"error":"read_only",...}`). User has zero UI signal that the action failed.

Unlike Alerts (which the user has directed to be removed entirely for analysts per H-7), Saved Views is a legitimate analyst-facing feature — they should at minimum see a clean "Read-only access — saving views is unavailable for shared sessions" toast.

**Recommended fix:** The June 9 doc's D.3 already proposed exactly this pattern — apply it now since Save View is the second confirmed instance of the same silent-403 bug. In the shared API client (`frontend/lib/api.ts` or wrapper), intercept `403` + `body.error === "read_only"` and raise a global toast. Saved Views and any future analyst-visible mutation path get the right feedback for free.

---

### ⚠️ N-7 (MEDIUM) — `/api/services/{id}/custom-fields` list endpoint (sibling of H-6 export)

**Where:** `GET /api/services/{id}/custom-fields` (in `core.py`).
**Status:** 200, 2,556 bytes — returns the 6 configured custom fields.

H-6 correctly blocks the `/export` sibling, but the list GET leaks the same schema (field names, types, mappings). The frontend Custom Fields admin UI is the only consumer.

**Recommended fix:** Add `"/custom-fields"` substring match (or `re.compile(r"^/api/services/[^/]+/custom-fields$")` to be surgical) to the blocked list — same pattern as the existing `/custom-fields/export` block. Also re-audit all GETs under `/api/services/{id}/` for analogous sibling-leak pairs (`/log-fields` vs `/log-fields/catalog`, etc.).

---

### ⚠️ N-8 (MEDIUM) — Origin Error Rate metric impossible (2181.11%) and HTTP 829 in status distribution

**Where:** `/origin` page metric cards and donut chart.
**Observed today as analyst:**
- "Origin Error Rate" card = **2181.11%** (red). An error rate cannot exceed 100%.
- "Status Code Distribution" donut includes legend entry "HTTP 829" at 0.196%. 829 is not a defined HTTP status code (IANA registry stops at 511 + a few extension ranges).

Possible root causes:
- Error rate is being summed/aggregated over time without normalization (e.g., `SUM(error_count) / 1 timeframe` instead of `SUM(error_count) / SUM(total)`).
- Logs ingestion is accepting non-integer or out-of-range status fields and the donut groups them as `"HTTP " + str(value)`.

**Recommended fix:** Trace the SQL behind the Origin Error Rate card and the status-code distribution. Add a server-side clamp + log-on-OOB for status codes (anything outside `100-599` → log + bucket as `unknown`), and verify the error-rate denominator on the timeseries query. While there, consider showing the raw count + denominator in the tooltip so QA can spot this faster next time.

---

### ⚠️ N-9 (MEDIUM) — Admin Pricing & Retention Defaults editor opens with empty inputs

**Where:** `/admin` → scroll to "Pricing & Retention Defaults" → click `EDIT`.
**Observed:** All 5 input fields (`CLASS A OPS ($/1K)`, `CLASS B OPS ($/10K)`, `CDN EGRESS ($/GB)`, `STORAGE ($/GB/MO)`, `MIN. DAYS BILLED/OBJECT`) render empty in the editor. The display-mode cards directly above were also blank — so the values aren't loaded into the form at all, not just hidden.

If the admin types nothing and clicks SAVE CHANGES, depending on backend validation, they'd either: (a) overwrite the global defaults with empty/zero values (silent corruption), or (b) get a validation error with no context for what was wrong.

**Recommended fix:**
- On EDIT, prefill from the current backend values (`GET /api/admin/pricing-defaults` or wherever they live).
- Add a server-side guard: reject the PATCH if any of the 5 fields is missing or non-positive.
- Bonus: show the previous value as the placeholder so the admin can see what they're about to overwrite.

---

### ⚠️ N-10 (LOW) — Debug envelope leaks on 403 error responses too

**Where:** Any analyst-blocked endpoint, e.g. `POST /api/views/` from an analyst session.
**Observed body:**
```
{"error": "read_only", "_debug_queries": [], "_debug_calls": [], "_is_cached": false}
```

The arrays are empty here, but the SHAPE alone leaks that the server has a debug-instrumentation pipeline that runs even on rejected requests. More importantly: if any pre-rejection middleware code (auth lookup, fingerprint check) made a Fastly KV call before the 403 fired, that call's path/timing would surface here just like N-1.

**Recommended fix:** Bundled with N-1's fix — strip the envelope wholesale for analyst responses, success and error alike.

---

## Section 3 — Endpoint inventory (current, static)

`backend/routers/` defines **172 route declarations** across **22 router files** (counted via `grep -rEho "@router\.(verb)\(['\"]([^'\"]+)" backend/routers/`). The distribution:

| Router file | Routes | Mostly-admin? | Notes |
|---|---:|---|---|
| `admin.py` | 25 | yes | All under `/api/admin/` except `/api/sync-status`, `/api/download*` (N-3, H-2) |
| `admin_usage.py` | 7 | yes | `/admin/usage-log*`, `/admin/usage-logging`, `/admin/system-jobs` |
| `alerts.py` | 6 | mixed | GETs leak today (H-7) |
| `bootstrap.py` | 6 | no | analyst-needed |
| `services/core.py` | 23 | mixed | mounts `/api/services/*` — H-3 + N-2 + N-7 live here |
| `services/cron.py` | 3 | yes | `/api/cron-runs` GET still leaks (H-5) |
| `services/audit.py` | 1 | yes | `/api/audit-logs` still leaks (H-5) |
| `dashboard.py` | 4 | no | analyst analytics |
| `debug.py` | 3 | yes | `/api/debug/*` — blocked |
| `insights.py` | 1 | no | analyst |
| `network.py` | 2 | no | analyst |
| `origin.py` | 9 | no | analyst |
| `performance.py` | 2 | no | analyst |
| `provision.py` | 13 | yes | `/api/provision/*` — blocked |
| `query.py` | 2 | no | analyst |
| `security.py` | 2 | no | analyst |
| `session_scoring.py` | 16 | mixed | analytics |
| `session_scoring_admin.py` | 17 | yes | H-4 covers 6 of these; N-5 covers 2 more; rest are write-verbs (gated) |
| `sessions.py` | 1 | no | analyst |
| `share_admin.py` | 18 | yes | `/api/admin/share/*` — blocked |
| `share_auth.py` | 6 | no | login/logout |
| `usage.py` | 5 | yes | `/api/usage/*` — blocked |
| `views.py` | 3 | mixed | GET analyst-needed; POST/DELETE gated |

### 3.1 Live RBAC probe summary today (analyst session)

Of the 66 endpoints probed:
- **40 returned 403 as expected** ✅ (every `/api/admin/*`, `/api/provision/*`, `/api/debug/*`, every SSE, every PUT/PATCH/DELETE, every non-allowed POST, every now-blocked H-1/H-2/H-3 partial/H-4 endpoint, every cross-tenant probe).
- **18 returned 200 as expected** ✅ (bootstrap reads, analyst-needed scoring analytics, dashboard analytics, views list, `/api/services` filtered to authorized service).
- **8 returned 200 unexpectedly** ❌ → the H-3 partial / H-5 / H-6 / H-7 / N-2 / N-3 / N-5 / N-7 leaks above.

### 3.2 Cross-tenant probe (confirms scope gate works)

Forged `service_id = "FAKE-OTHER-SERVICE-ID"` in URL path against the still-leaking endpoints: **all returned 403 `service_not_authorized`** (`/api/services/FAKE/logging-settings`, `/log-fields`, `/api/alerts/FAKE`, `/api/views/FAKE`). So the existing leaks are bounded to data the analyst owns — they're not a cross-tenant breach. M-3 (body service_id silent fallback) is the one remaining gap; it didn't leak cross-tenant either (it silently used the authorized service), but it should still 4xx for correctness.

---

## Section 4 — Carry-over UX & a11y items NOT separately re-verified

These were exhaustively documented in [qa_ux_and_rbac_security_audit.md](qa_ux_and_rbac_security_audit.md) Sections G and H (Lighthouse + axe-core passes against 26 page-scans, 641 total violations). I did not re-run that toolchain today — design-token-level fixes don't appear to have landed in the commits since (none of `1b53dcf` or `2c..7e21c60` touch `tailwind.config` or the muted-foreground tokens). The 5 leverage-point fixes from that doc's H.4 still apply unchanged:

1. **M-8** — `<Select.Trigger>` accessible names (service + region picker, every page).
2. **M-9 + M-13** — muted-text and amber-banner color token contrast (sidebar + admin sharing banner).
3. **M-14** — `<Switch>` accessible names on `/admin` and `/usage`.
4. **M-15** — `<input>` ↔ `<label>` association on `/usage` rate cards and `/admin` forms.
5. **M-16** — reserve space for metric cards + share banner to fix admin CLS.

Visual check today confirms M-13 (amber banner contrast) is still present on every admin page; M-8 service-picker is still an empty-text `<button>` (no aria-label) in the rendered DOM today.

---

## Section 5 — Prioritized remediation order

I'd order the work this way (highest impact first), not strictly by severity:

1. **N-1 (debug envelope strip)** — single change in the response middleware/serialization layer. Moots N-3's KV-ID leak vector, defangs N-5's KV-ID leak vector, and removes the SQL-leak surface entirely for analyst responses. ~30 min of code + 1 regression test. **Do this first.**

2. **N-4 (/logs frontend gate + H-5 backend block)** — analyst-visible page exposing admin UI surfaces. Two-line frontend patch + two-line backend prefix add. ~15 min.

3. **H-6 + N-7 (custom-fields list + export)** + **H-3 partial (logging-settings, log-fields)** + **N-5 (matrix-versions + enforce-threshold)** — all the same pattern: add to the appropriate blocked subpath/suffix list in `remote_access.py`. Single commit. ~30 min.

4. **H-7 (alerts surface removal)** — per user directive: hide nav entry, gate the page, block GET endpoints. The Create button is already hidden for analysts (saw today), so most of the work is done — finishing the cut is small. The fact that today's analyst still sees `/alerts` rendering with no Create button is the worst state (looks broken, leaks endpoint).

5. **N-2 (`/api/services` payload trim)** — needs the frontend-consumer audit first (`git grep "services\["` in `frontend/`), then a `AnalystServiceView` whitelist. ~1 hr.

6. **N-6 (Save View silent failure) + the reusable global 403 toast** — the toast is the lasting fix; Save View is the visible benefit. ~1 hr including tests.

7. **N-9 (Admin Pricing & Retention Defaults empty)** — backend fix + frontend prefill + server-side validation. Admin-only, low blast radius, but ugly. ~1 hr.

8. **N-8 (Origin Error Rate 2181% + HTTP 829)** — needs SQL trace first, then both a query fix and an ingestion guard. ~half day.

9. **M-3 (body service_id silent fallback)** — strict reject when body field doesn't match the URL/session-authorized service. ~30 min + tests across the 8 analytics POST routes.

10. **M-6 (session-scoring IO error wrapping)** — already partially done (consolidated to 1 card/1 retry), just needs the raw-error-to-friendly-copy mapping. ~30 min.

11. **M-4 (network map empty)** — trace the map data binding; not a regression, just unconverted state.

12. **The 5 a11y leverage-point fixes** (M-8/M-9/M-13/M-14/M-15/M-16) — design-token + component-level. Independent track; one good afternoon of work removes ~90% of the 641-violation count from yesterday's pass.

---

## Section 6 — Acceptance test (after the above lands)

Re-run today's 66-probe matrix from an analyst browser context:
- [ ] Every endpoint in Section 1.2 returns 403.
- [ ] Every N-2/N-3/N-5/N-7 endpoint returns 403 or returns a payload with no operator-internal fields.
- [ ] **No** analyst response body contains the substrings `_debug_queries`, `_debug_calls`, `_section_timings`, `_is_cached`, `cdn_url`, `cdn_service_id`, `fos_bucket`, `ngwaf_workspace_id`, `duckdb_size_bytes`, or any raw SQL (`CREATE TEMP TABLE`, `SELECT`).
- [ ] Typing `/logs` in the address bar redirects to `/dashboard` for analyst.
- [ ] Typing `/alerts` either redirects to `/dashboard` OR the page is removed from the build entirely (per user directive).
- [ ] Clicking Save View as analyst produces a global toast within 500 ms.
- [ ] Admin Pricing & Retention Defaults EDIT shows the current values, not empty inputs.
- [ ] Origin Error Rate ≤ 100% on every probe. No `HTTP 829`-style entries in the status-code donut.
- [ ] Loopback admin retains 200 on every endpoint above and continues to see the `_debug_*` envelope (verified by switching back to localhost:3001 and re-running the same probes).

---

## Section 7 — Tools used + what's still on the table

**Used today:**
- `browser-use` CLI (Chromium, default profile) for live navigation, screenshotting, and JS-eval inside the authenticated analyst session.
- Authenticated `fetch()` probes (66 endpoints) — same vector a malicious analyst would use.
- Static walk of `backend/routers/` via grep/Python — 22 files, 172 routes.

**Best-in-class tools the user listed, with a note on each:**
- **Lighthouse / axe-core** — covered yesterday (Sections G and H of the prior doc); design tokens haven't changed, so the 641-violation count and the 5 leverage fixes still apply. Worth re-running once those tokens are touched, to confirm the delta.
- **FullStory / Hotjar / LogRocket** — session replay would be ideal for catching the silent-failure pattern (N-6, M-1). Recommend wiring LogRocket on a staging/sandbox instance only (do NOT enable on the shared production analyst — it would capture the analyst's keystrokes inside the Query Editor, including any SQL they author). Cost <$500/mo for the volume here.
- **UserTesting / Maze** — overkill for an internal tool with 1 admin and ≤10 invited analysts. Skip.
- **WebPageTest** — would surface the M-12 unused-JS finding (~500 KB shipped per page) more diagnostically than Lighthouse's score. Worth one run after the bundle-analyzer pass.
- **Figma / Zeplin** — N/A unless there's a design source-of-truth being maintained.
- **Could not use:** none of the listed tools failed; everything I needed was available via browser-use + the local toolchain. The session-replay tools are deliberately deferred (privacy/cost) rather than blocked.

**Still on the table:**
- Lighthouse mobile pass on both roles.
- Keyboard-only walk on every page (focus traps, missing skip-links, modals that don't return focus).
- Bundle-analyzer pass (`ANALYZE=true next build`) to confirm M-12 code-split targets.
- Visual regression baseline (Playwright screenshot diff) — set up BEFORE the design-token fixes so the before/after is reviewable.
