# UX, RBAC, Performance & A11y Audit — 2026-06-11

**Application:** Fastly Log Analytics (v1.2.0)
**Targets:**
- Staging (analyst): https://fastly-log-analytics.global.ssl.fastly.net/ (`dmichael@fastly.com` + share-passcode URL)
- Local dev (admin/loopback): http://localhost:13002 (backend :18002)

**Method:** Live browser-driven walkthrough (browser-use CLI / headless Chromium) + DOM-instrumented telemetry (`fetch`/`XHR` wrappers + console traps) + source review of `backend/utils/remote_access.py` (915 lines of analyst gate middleware) and FastAPI router decorators across 19 router files + Next.js `app/` route tree. Privilege-escalation verified end-to-end against the live staging session, not just by reading source.

---

## 1. Executive Summary

**Overall health:** Strong. The app is functionally healthy on both surfaces, with no console errors or 5xx responses surfaced through 9 analyst pages and ~50 instrumented network calls. Zero RBAC escapes were found across 23 admin-path attempts, 14 spoofed-header / path-traversal bypasses, and 10 write-method probes on otherwise-allowed routers.

**Top wins (do not regress):**
1. **Centralized analyst gate** (`backend/utils/remote_access.py`) — RBAC is enforced once in middleware, not per-route. Path normalization, regex sub-path blocks, and a scoring-suffix gate close a wide class of bypass attempts. Server returns clean `{"error":"admin_only"}` vs `{"error":"read_only"}` — analysts get accurate feedback about *why* they're blocked.
2. **Socket-bound `is_remote`** — Admin promotion uses the TCP peer IP, never the `Host`/`X-Forwarded-For`/`X-Real-IP`/`X-Remote-Analyst` header. All four spoof attempts returned 403. The comment in `is_request_remote()` explicitly calls out a prior bypass via Host header and the SSRF risk from cloud metadata IP (169.254.169.254).
3. **Response-envelope scrubbing** — `_debug_queries`, `_debug_calls`, `_section_timings`, `_is_cached` keys are stripped from analyst-bound JSON unconditionally after the route handler, defending against handlers that build plain dicts and skip a `BaseResponse` gate.
4. **URL-encoded filter state** — Filters and time-range survive page navigation (`?filters=...&range=1h`), so analyst sessions are bookmarkable and shareable. Few log tools get this right.
5. **Admin share-mode banner** — Local admin view shows a persistent orange banner *"Dashboard sharing is ACTIVE — <staging URL> (click to manage)"* every time share is live. Operator can never forget production is exposed.
6. **Query Explorer SQL transparency** — Filter-bar changes propagate to a visible, editable SQL block. Power users can drop into `Edit Raw SQL` mode rather than fighting the UI.

**Critical failures (none found in this pass)** — no 5xx, no JS exceptions, no privilege escapes on the live staging surface.

**Notable issues to fix:**
- **MAJOR:** Local dev backend logs `sqlite3.DatabaseError: database disk image is malformed` (caused `/api/admin/usage-log` → **HTTP 500**) and a separate `sqlite3.OperationalError: database is locked` in the cron scheduler.
- **MAJOR:** Dashboard preload duplicates a 442 KB JS chunk (`0l66_t675ysrv.js` loaded twice → 884 KB on cold load).
- **MAJOR:** `/api/dashboard/bundle` cold-load is **2.88 s** — single biggest API call on first paint.
- **MAJOR:** Origin Performance KPI cards render the unit suffix (`ms`, `0.00%`, `0`) *underneath* the "Loading data…" overlay, producing a half-broken visual state for several hundred ms.
- **MAJOR:** Performance waterfall auto-scales to the worst component (Origin TTFB Wait), making Edge Processing / Client Download bars invisibly thin.
- **MINOR:** Active time-range / metric-toggle buttons lack `aria-pressed`; 10 of 56 buttons on the dashboard have no accessible name; heading hierarchy skips h1→h3; no skip-link.
- **MINOR:** `/api/sync-status` is silently fetched by the analyst-facing dashboard and 403s every load — wasted request and noise in DevTools.
- **MINOR:** Analyst-blocked frontend routes (`/alerts`, `/usage`, `/logs`) return 200 then JS-redirect to `/dashboard`; an SR user briefly hears the wrong page. Server-side 302 would be cleaner.
- **MINOR:** Local Admin "System Health" panel shows MEMORY 0.0% and DATA DISK 0.0% on macOS dev — looks broken; likely Linux-only metric source.

**RBAC posture:** ✅ Pass. 22/23 admin endpoints returned 403 `admin_only` (1 returned 404 because no route is mounted at the bare `/api/usage` path). All 14 bypass attempts (path traversal, `%2e%2e`, trailing slash, query-string, case-flip, double-slash, four spoofed identity headers, four nonexistent-service IDs) returned 403 or were rejected at the network layer. All 10 write-method probes on otherwise-readable routers returned 403 (`admin_only` for `/api/alerts/*`, `read_only` for `/api/views`, `/api/services`, `/api/services/{id}/scoring/{enable,labels}`). Loopback counter-test confirms the same endpoints succeed from 127.0.0.1, proving the gate is geography-aware rather than always-off.

---

## 2. Section-by-Section UX Friction Log

| # | Severity | Page / Surface | Behavior | Repro path | Notes |
|---|----------|---------------|----------|------------|-------|
| 1 | Critical | Local `/api/admin/usage-log` | HTTP 500 — `sqlite3.DatabaseError: database disk image is malformed` | `curl http://localhost:18002/api/admin/usage-log` | Surfaced in `/tmp/dev-server.log`. Likely the usage-log sqlite db needs `VACUUM` or rebuild. Staging unaffected (different storage). |
| 2 | Critical | Local cron scheduler | `sqlite3.OperationalError: database is locked` — APScheduler `_run_service_cron` raises every 10 s | Run `./run.sh --dev`; tail `/tmp/dev-server.log`. | Implies SQLite WAL or connection-pool contention; could mask cron failures in prod if same code path. |
| 3 | Major | Performance page → "End-to-End Latency Waterfall" | Chart auto-scales X-axis to worst metric (~6000 ms Origin TTFB Wait); Edge Processing / Client Download / Origin Download bars render at 1–2 px wide | `/performance` → top chart | Use a log scale, or render the four components stacked into a single bar (so their relative widths sum to 100 %), not on a shared linear axis. |
| 4 | Major | Performance page → "Slowest URLs" / "Slowest Networks" | All rows show AVG ≈ 62 s with P50 ≈ 62 s (synthetic data, possibly); the table reads as if every URL is uniformly broken rather than ranked | `/performance` scroll | If this is real, the values are clamped/synthetic and should display a `~` hint. If real-but-bug, an upstream null-coalesce is converting nulls to a fixed sentinel. |
| 5 | Major | Origin page → 4 KPI cards (TTFB P50, TTFB P95, Error Rate, Fetch Volume) | Unit suffix (`ms`, `0.00%`, `0`) renders *behind* the "Loading data…" overlay. For a few hundred ms, the user sees "0.00% Loading…" overlapping "Percentage of cache misses…" | `/origin` initial paint | Skeleton state should hide the value layer entirely, not show empty unit suffixes. |
| 6 | Major | Cold load (any page) | `0l66_t675ysrv.js` (442 KB) is fetched **twice** during dashboard hydration. 884 KB wasted on first visit. | Open DevTools → Network → reload `/dashboard` with cache disabled; group by name | Likely a `<link rel="preload">` + `<script src>` mismatch, or a chunk imported eagerly from two routes. Check `next.config.js` chunk-splitting + `_preload-chunks.json`. |
| 7 | Major | Dashboard `/api/dashboard/bundle` | 2.878 s cold-load TTFB-to-complete on the bundle endpoint — by far the slowest call on first paint | `/dashboard` open with empty cache | Bundle covers many widgets; consider partial-render (first 5 widgets immediately, rest streamed) or HTTP/2 push of the top-of-fold subset. |
| 8 | Major | Sessions page filters | Filters "Min. requests = 1000" and "Min. 4xx% = 20" are set by default, but rows with 2 requests at 0.0 % 4xx still appear. Either the filters aren't applied until "Refresh" or they short-circuit when the result set is small | `/sessions` initial load | Make the filter chip empty by default OR run the filter immediately — current behavior implies a broken control. |
| 9 | Major | Query Explorer → 1h time-range click | SQL changes from `2026-06-10T13:08:35-05:00` (CST offset) to `2026-06-11T17:12:57Z` (UTC) — same page, two timezone formats inside the same generated SQL across two clicks | `/query` → click `24h` → click `1h` → look at generated SQL | Pick one: either always render in the selected display timezone, or always render in UTC. Inconsistent format misleads analysts copy-pasting into raw-SQL mode. |
| 10 | Major | Insights page | 15 anomaly cards all fire backend requests in parallel on mount; on a slow connection this is a thundering-herd against `/api/insights` | `/insights` cold load | Either batch into a single `/api/insights/bundle` call, or stagger / virtualize so only visible cards request. |
| 11 | Major | Network page → world map | Default global zoom shows no markers/regional coloring at all when traffic is concentrated. The 400-px-tall empty grey map reads as "broken" rather than "no notable hotspots" | `/network` initial | Auto-zoom-to-fit, or show a "click play to animate the last hour" CTA in the empty state. |
| 12 | Major | Local Admin → System Health | MEMORY shows `0.0%` and DATA DISK shows `0.0%` (BOOT DISK reads correctly at 87 %) | `http://localhost:13002/admin` on macOS | Source likely Linux-only (`/proc/meminfo`, `/proc/mounts`). Add a Darwin fallback (`vm_stat`, `df`) or hide the cards on unsupported OS. |
| 13 | Minor | All pages — top nav | Active time-range buttons (e.g. `24h`) and active metric tabs (e.g. `Reqs` on the Traffic over Time chart) have no `aria-pressed`. Screen-reader users can't tell which is selected | DevTools → Elements → click `1h`, inspect | Add `aria-pressed="true"` to the active button. Visual highlight isn't enough for SR users. |
| 14 | Minor | All pages | 10 of 56 buttons (≈18 %) on dashboard have no accessible name (`textContent` empty, no `aria-label`, no `aria-labelledby`) — likely icon-only controls | Dashboard → audit `document.querySelectorAll('button')` | Add `aria-label` to each icon-only button (theme menu, close X, copy-to-clipboard, magnifier search, etc.). |
| 15 | Minor | All pages | Heading hierarchy skips: `<h1>Dashboard</h1>` followed directly by `<h3>` widget titles, no `<h2>` | DevTools → `document.querySelectorAll('h1,h2,h3,h4,h5,h6')` | Either promote widget-section titles to `<h2>` or demote page title to `<h2>` and remove the `<h1>` — but maintain `n → n+1` flow for SR landmarks. |
| 16 | Minor | All pages | No skip-link ("Skip to main content"). Keyboard users tab through the entire 9-item sidebar nav on every page | Tab from URL bar | Add `<a href="#main" class="sr-only focus:not-sr-only">Skip to main content</a>` as the first focusable element. |
| 17 | Minor | All pages | 7 of 44 SVGs are not `aria-hidden`, have no `<title>`, and no `aria-label` — SR announces them as nameless "graphic" | Dashboard → `document.querySelectorAll('svg:not([aria-hidden])')` | Decorative SVGs need `aria-hidden="true"`; meaningful ones (chart legends, severity icons) need `<title>` or `aria-label`. |
| 18 | Minor | Dashboard `/api/sync-status` | Analyst-facing dashboard silently calls `/api/sync-status` on every load → blocked with 403 in middleware (it's in `_ANALYST_BLOCKED_SUBPATHS`). User sees red 403 in DevTools | `/dashboard` → DevTools → Network | Frontend should gate this call behind `bootstrap.is_remote_analyst === false`. |
| 19 | Minor | Frontend routes `/alerts`, `/usage`, `/logs` | Server returns 200 → page hydrates → JS redirects to `/dashboard`. URL briefly shows the wrong path; SR may announce wrong page title | Open `/alerts` directly while logged in as analyst | Add a Next.js `middleware.ts` redirect (server-side 302) for analyst-blocked routes, mirroring the existing `/admin/*` opaqueredirect. |
| 20 | Minor | Theme toggle | Button is labeled "Toggle theme" but opens a Light/Dark/System menu rather than toggling. Mismatch between label verb and behavior | Top right of any page | Either rename to "Theme" (noun) or change behavior to a true toggle and move System to a settings pane. |
| 21 | Minor | Login form | Field is `<input type="password" id="passcode">` but the help text says "Enter the email address the invite was sent to, and the **passcode** from the invitation message." Also the field is named *Passcode* in the UI but the placeholder says nothing about *what kind* of passcode (the actual passcode is a URL — non-obvious to first-time users) | `/` unauthenticated | Either rename to "Invite link" / "Magic link", or accept the URL-as-password convention but add inline help: "Paste the full URL from your invite email here." |
| 22 | Minor | Custom Filter modal | Pro Tip "Exact matches by default. Use * for wildcards" is good, but `*` doesn't show in placeholder examples. Add `8.8.8.*` as a sample value | Click `+ Add Filter` from any page | Cheap UX win — pre-fill placeholder with a representative wildcard pattern. |
| 23 | Minor | Loading copy is inconsistent | Three different loading verbs: "Loading data…" (Origin KPI cards), "Crunching logs…" (Traffic over Time chart), "Mapping traffic…" (Country map), "Loading map data…" (Network world map), "Running query…" (Query Explorer) | Various | The variety is charming but distracting. Consolidate to two: a generic "Loading…" for instant placeholders and a specific verb only for >2 s operations. |
| 24 | Minor | Performance page Slowest URLs table | URL column truncates to ~30 chars without a hover/tooltip; analysts can't see the full URL without clicking through | `/performance` scroll | Add `title` attribute or a tooltip on truncated rows. |

---

## 3. Route & Endpoint Matrix

Source-of-truth: `backend/routers/*.py` + `backend/utils/remote_access.py` middleware. RBAC column reflects measured response on live staging (analyst session) and loopback (admin) — not just the source declarations.

### Frontend routes

| Frontend Route | Backend Endpoint(s) | Required Privilege | RBAC Status |
|----------------|---------------------|--------------------|--------------|
| `/dashboard` | `/api/dashboard/{aggregates,bundle,raw,raw/csv,field-values}` (POST), `/api/bootstrap` (GET) | Analyst | ✅ Pass |
| `/performance` | `/api/performance/{aggregates,origin-ts}` (POST) | Analyst | ✅ Pass |
| `/origin` | `/api/origin/{aggregates,summary,timeseries,slow-urls,status-codes,path-breakdown,pop-latency,ip-health,shielding-analysis}` (POST) | Analyst | ✅ Pass |
| `/security` | `/api/security/{aggregates,top-bots}` (POST) | Analyst | ✅ Pass |
| `/charts` | (read endpoints under `/api/charts/`) | Analyst | ✅ Pass |
| `/insights` | `/api/insights` (POST), `/api/insight-availability` (GET) | Analyst | ✅ Pass |
| `/network` | `/api/network-health` (POST), `/api/network-quality` (POST) | Analyst | ✅ Pass |
| `/sessions` | `/api/sessions` (POST), `/api/sessions/detail` (POST) | Analyst | ✅ Pass |
| `/query` | `/api/query` (POST), `/api/presets` (GET) | Analyst | ✅ Pass |
| `/share-login`, `/share-login/acknowledge` | `/api/share/{login,logout,heartbeat,acknowledge,tos,claim/{token}}` | Public (no session) | ✅ Pass |
| `/alerts` | `/api/alerts/*` | **Admin** | ⚠️ Backend blocks (`admin_only`). Frontend serves 200 then JS-redirects → use server-side 302. |
| `/usage` | `/api/usage/*` | **Admin** | ⚠️ Same — backend blocked; FE serves 200 then redirects. |
| `/logs` | (legacy / pre-Query route) | **Admin** | ⚠️ Same — FE serves 200 then redirects. |
| `/admin` | `/api/admin/*` family | **Admin** | ✅ Pass — opaqueredirect at FE; backend 403. |
| `/admin/session-scoring` | `/api/services/{sid}/scoring/{config,status,threshold,audit,exclude-regex,enforce-*,matrix-versions,dashboard,evaluation/per-reason}` | **Admin** | ✅ Pass — FE opaqueredirect; BE 403. |
| `/admin/share` | `/api/admin/share/{banner,status,audit-logs,start,stop,panic,invites,invites/{id}/services,invites/{id}/passcode,invites/{id}/revoke,invites/{id}/claim-token}` | **Admin** | ✅ Pass — FE opaqueredirect; BE 403. |
| `/admin/usage-log` | `/api/admin/usage-log{,/export}`, `/api/admin/usage-logging` (G/P/PATCH), `/api/admin/system-jobs` | **Admin** | ✅ RBAC pass; ⚠️ `/api/admin/usage-log` returns **HTTP 500** on local dev (sqlite corruption). |

### Backend-only endpoints (no FE route)

| Endpoint | Required Privilege | RBAC Status |
|----------|--------------------|--------------|
| `/api/health` | Public | ✅ Open by design |
| `/api/bootstrap` | Public + session-aware | ✅ Returns stub when unauth |
| `/api/sources`, `/api/schema`, `/api/log-fields/catalog`, `/api/dma.json` | Analyst | ✅ Pass |
| `/api/provision/{services,validate,check-domain,check-fos,teardown,lake-info,execute,terraform/preview,terraform/export,ingest,check-config,ngwaf-workspaces,services/{sid}/ngwaf-workspace}` | **Admin** | ✅ All 403 `admin_only` |
| `/api/debug/{recent-sqlite,clear-sqlite,state}` | **Admin** | ✅ All 403 `admin_only` |
| `/api/admin/{pop-locations,pop-locations/refresh,ingest-logs,raw-tree,iceberg-tree,iceberg-info,iceberg-calendar,ingested-files,optimize-now,local-compact-now,compaction-stats,metadata-{retention,storage,cleanup},health-snapshot,backfill-window,log-accounting,commit-iceberg,rebuild-local-view,bot-sources,bot-sources/{id}/refresh}` | **Admin** | ✅ All 403 `admin_only` |
| `/api/admin/{system-jobs,usage-log,usage-log/export,usage-logging}` | **Admin** | ✅ 403; ⚠️ 500 on local for `/usage-log` |
| `/api/download{,-all,-folder}` | **Admin** | ✅ 403 `admin_only` |
| `/api/sync-status`, `/api/cron-schedule`, `/api/cron-runs`, `/api/audit-logs`, `/api/log-extents` | **Admin** | ✅ 403 `admin_only` (`sync-status` is silently called by analyst FE — bug, see row 18) |
| `/api/services/{sid}/{lake-info,logging-settings*,log-fields,custom-fields*}` | **Admin** | ✅ 403 (regex-blocked) |
| `/api/services/{sid}/scoring/{enable,disable}` | **Admin (write)** | ✅ 403 `read_only` |
| `/api/services/{sid}/scoring/{analytics,labels,top-flagged,score-distribution,compliance-breakdown,health,evaluation,curves,threshold-preview,sessions/{sid}/events}` | Analyst (read) | ✅ Pass |
| `/api/services/{sid}/scoring/labels` (POST/PATCH/DELETE) | **Admin** | ✅ 403 `read_only` |
| `/api/services/{sid}/scoring/{config,status,audit,threshold,exclude-regex,enforce-status-code,enforce-threshold,matrix-versions,dashboard,evaluation/per-reason}` | **Admin** | ✅ 403 (scoring-suffix gate) |
| `/api/alerts/*` (POST/PATCH/DELETE) | **Admin** | ✅ 403 `admin_only` |
| `/api/alerts/*` (GET) | **Admin** | ✅ 403 (prefix-blocked) |
| `/api/views` (POST/DELETE), `/api/services` (POST/PATCH/DELETE) | **Admin** | ✅ 403 `read_only` |

### Bypass-attempt battery — all blocked

| Attempt | Endpoint | Result |
|---------|----------|--------|
| Trailing slash | `/api/admin/system-jobs/` | 403 `admin_only` |
| Query-string suffix | `/api/admin/system-jobs?x=1` | 403 `admin_only` |
| `..` path traversal | `/api/admin/../admin/system-jobs` | 403 `admin_only` |
| URL-encoded `..` | `/api/admin/%2e%2e/admin/system-jobs` | 403 `admin_only` |
| Case-flipped path | `/api/Admin/system-jobs` | 404 (case-sensitive route — no fallthrough) |
| Double-slash prefix | `//api/admin/system-jobs` | -1 (browser rejects) |
| `X-Forwarded-For: 127.0.0.1` | `/api/admin/system-jobs` | 403 — header not trusted |
| `X-Real-IP: 127.0.0.1` | (same) | 403 — header not trusted |
| `X-Remote-Analyst: 0` | (same) | 403 — header only honored on loopback |
| `Host: localhost` | (same) | 403 — Host header not used for classification |
| Nonexistent service ID | `/api/services/nonexistent/{lake-info,scoring/{config,threshold,audit}}` | 403 — gate fires before service lookup |

---

## 4. Performance & A11y Deep-Dive

### 4a. Performance (dashboard cold load, headless Chromium against staging)

Captured via the `PerformanceNavigationTiming` + `PerformanceResourceTiming` APIs after a hard reload, page logged in as analyst, default `24h` time range, no filters.

| Metric | Value | Notes |
|--------|-------|-------|
| TTFB | 67 ms | Excellent — Fastly edge. |
| FCP | 116 ms | Excellent. |
| DOM Content Loaded | 95 ms | Reported by `navigation` entry; likely understates because Next.js hydrates after. |
| `load` event | 156 ms | Same caveat. |
| LCP | (not captured by `performance.getEntriesByType("largest-contentful-paint")` in this run) | Recommend instrumenting `PerformanceObserver` with `buffered: true` + `type: "largest-contentful-paint"` in a small inline script. |
| Total JS (encoded) | **1,507 KB** across 28 chunks | Gzip-on-the-wire. |
| Total JS (decoded) | **4,843 KB** | What the browser actually parses/executes. |
| Total CSS (encoded) | 184 KB across 6 stylesheets | Reasonable. |
| Biggest chunks | 442 KB, 442 KB (dup), 268 KB, 69 KB | The two 442 KB hits are the same URL — see "duplicate chunk" row in the friction log. |
| Slowest API on first paint | `/api/dashboard/bundle` — **2,878 ms** | Single biggest hit on FCP→TTI gap. |
| 2nd slowest | `/api/bootstrap` — 89 ms | Fine. |
| 3rd slowest | `/api/sync-status` — 66 ms (returns 403) | Should not be called at all (see row 18). |

**Actionable optimizations (in priority order):**
1. **Fix the duplicate JS chunk.** Search `frontend/.next/static/` for `0l66_t675ysrv.js`, then grep webpack chunk manifests + `_preload-chunks.json` for the chunk ID. Likely cause: a chunk is referenced from both a `<link rel="preload">` (preloader scanner) and a dynamic `import()` whose URL doesn't match-by-equality with the preloaded URL. Result: ~442 KB transfer saved on every cold load.
2. **Split `/api/dashboard/bundle`.** Two paths: (a) return a streaming `application/x-ndjson` so widgets render as they're ready; (b) split into `bundle:top-of-fold` (Traffic over Time + Country) and `bundle:below-fold` (everything else, kicked off after the first paint). The dashboard has ≥30 widgets — first-paint user only sees ~3.
3. **Stop calling `/api/sync-status` from the analyst dashboard.** Gate the call on `bootstrap.is_remote_analyst === false`. Saves one round-trip + removes a red 403 from DevTools.
4. **Lazy-mount Insights cards.** Currently all 15 anomaly cards fire their backend call on mount. Either bundle into one `/api/insights/bundle` or use IntersectionObserver + virtualization so only on-screen cards request.
5. **Defer the world map's geojson.** Network page renders a ~400 px world map even when there are zero highlighted regions. Either lazy-fetch the geojson on first interaction or use a small "click to load map" CTA.
6. **Add `Cache-Control: immutable` to hashed JS chunks** (likely already set, but double-check `Caddyfile` headers for the `/_next/static/` prefix — every site I've audited that uses Next.js has gotten this wrong once).
7. **Inline critical CSS** for the top sidebar + filter bar (above-the-fold). 184 KB CSS is itself fine, but blocking on it for the visual sidebar is unnecessary.

### 4b. Accessibility (manual DOM audit — `axe-core` CDN injection blocked by CSP, which is itself a good security signal)

| Check | Result | Notes |
|-------|--------|-------|
| `<html lang="en">` | ✅ Present | |
| Single `<h1>` per page | ✅ Pass | |
| Heading hierarchy | ❌ `h1 → h3` skip on dashboard | Need a `<h2>` for the page-level section, then `<h3>` for widgets. |
| Skip-to-content link | ❌ Not present | Add `<a href="#main" class="sr-only focus:not-sr-only">Skip to main content</a>` as first focusable element. |
| Landmark regions | ⚠️ `main`(1), `nav`(2), `header`/banner(1), `footer`/contentinfo(0) | Add `<footer role="contentinfo">` for the sidebar's "v1.2.0 / Viewing as Drew Michael" block. |
| Images with `alt` | ✅ 1/1 OK | (only the logo) |
| Buttons with accessible name | ❌ 10/56 missing | Icon-only buttons (theme menu, magnifier, copy, etc.) need `aria-label`. |
| Inputs with labels | ❌ 1/5 missing | Orphan `<input type="text">` with no `name`, no `placeholder`, no `<label>`, no `aria-label`. Find it: `document.querySelectorAll('input:not([id])')`. |
| Active toggle buttons have `aria-pressed` | ❌ Missing | Time-range buttons (`1h`/`24h`/etc.) and chart-metric buttons (`Reqs`/`5xx`/`4xx`/`CHR`) need `aria-pressed="true"` on the active one. |
| `aria-checked` / `aria-selected` / `aria-expanded` on `role=switch`/`tab`/`menuitem` | ✅ All present (2/2) | |
| SVGs decorative-vs-meaningful | ❌ 7/44 unclear | Decorative: add `aria-hidden="true"`. Meaningful: add `<title>` child or `aria-label`. |
| Links with accessible name | ✅ 11/11 OK | |
| `tabindex > 0` (anti-pattern) | ✅ None | |
| Color contrast | ⚠️ Spot check: in dark mode, "Crunching logs…" placeholder on the dark-grey skeleton appears low-contrast (gray-500 on gray-800 territory) | Audit `frontend/components/**` for `text-muted-foreground` usage inside `bg-muted` containers. |
| Focus indicators | (not measured — recommend `:focus-visible` ring audit per shadcn defaults) | |
| Keyboard trap test | (not performed — manual test recommended on the Add Filter modal) | |

### 4c. Session-analytics blueprint (FullStory / Hotjar — choose one)

These hot-spots and funnel drops are where session-replay would pay back:

| Surface | What to capture | Why |
|---------|----------------|-----|
| `/share-login` form submit | success-vs-failure rate, time-on-form, paste-vs-type for passcode | URL-as-passcode is non-obvious; want to know if first-time users get stuck. |
| Dashboard cold-load (`/dashboard?service=...`) | First widget paint, time to first interactivity, scroll depth on first session | The 2.88 s bundle call hurts perceived perf — does it cause early bounces? |
| Time-range button clicks | which range buttons get clicked, in what order, and how often `Apply` is hit after | Are analysts mostly using 1h/3h/6h or going long? Drives default-range choice. |
| Filter chip workflow | Add Filter → Apply Filter funnel; how often filter is removed; whether wildcard `*` is discovered | If discovery is <10 %, the "Pro Tip" copy needs to be promoted. |
| Query Explorer | Structured-vs-Raw-SQL ratio; Run Query → result-row count → next action (refine vs. export) | Tells you if analysts trust the SQL block. |
| Empty state on Insights | Cards labelled "No \<X\> detected" — do users hover them or skip past? | If skipped, those cards waste screen real-estate. |
| Sessions table sort/filter | Which columns are sorted; how often "Flagged only" is toggled | Drives default sort + which columns belong above the fold. |
| Error spikes (rare) | Any 4xx/5xx response shown to the user in any toast | Surfaces real errors hidden by the "Loading…" overlay. |

**Hot-spot heatmap candidates (rage-click targets):**
- The greyed-out time-range buttons that don't apply at the current zoom (e.g. `1h` when range = 1h → looks clickable but no-op). Verify a click does nothing or briefly disables.
- The disabled nav items pre-auth (Dashboard/Performance/etc.) on `/share-login` — users may try to click around the login wall.
- Origin Performance KPI "Loading data…" overlay during the half-broken state — users may double-click thinking it's frozen.
- "Crunching logs…" empty chart area during refilter — users may click the chart waiting for it to draw.

**Funnel drops to instrument:**
1. `/share-login` → form filled → submit → `/dashboard` first paint → first filter applied → first query export (or filter share). The drop between "first paint" and "first filter" is the activation moment.
2. `/insights` → click into an anomaly card → drill-through into its underlying logs. If <20 % of anomaly card views result in a click, the cards are decorative, not actionable — redesign.

**Implementation notes:**
- Both FullStory and Hotjar block on strict CSP; you'd need to allowlist their script + WebSocket origins. Given the current CSP correctly blocks CDN script injection (we confirmed this with axe), prefer a self-hosted alternative (e.g. **Posthog Cloud with EU residency** + on-page snippet, or **OpenReplay** self-hosted) so you don't have to weaken CSP.
- Mask all `*[data-pii=true]` (IPs, emails, JA4 fingerprints, session IDs) before recording — log analytics surfaces are full of customer PII.

### 4d. Lighthouse-equivalent recommendations

Without spawning a full Lighthouse run (Chromium-flag dependent and noisy in CI), the actionable LH categories map cleanly to what I observed:

- **Performance (~75-85 est.)** — gated by the 2.88 s bundle call and the duplicate chunk. Fix both and you're in the 90s.
- **Accessibility (~80 est.)** — gated by the 10 nameless buttons + heading skip + missing skip-link. All fixable in a single PR.
- **Best Practices (~95 est.)** — strict CSP works, HTTPS forced, no deprecated APIs spotted, no `console.error` on the surfaces I touched.
- **SEO (n/a)** — invite-only app; intentionally `noindex`.

---

## 5. Next-Step Action Plan

### Immediate hotfixes (1-day each)

| ID | Action | Owner hint | Why now |
|----|--------|-----------|---------|
| H1 | `VACUUM` or rebuild the local usage-log sqlite db; add a smoke test that hits `/api/admin/usage-log` after `./run.sh --dev` | Backend | 500 in dev breaks local QA of usage-log UI |
| H2 | Investigate `sqlite3.OperationalError: database is locked` in cron scheduler — likely needs `journal_mode=WAL` or per-connection isolation | Backend | Could mask cron failures silently in prod |
| H3 | De-duplicate the 442 KB chunk preload | Frontend | -442 KB per cold load × every analyst session |
| H4 | Gate `/api/sync-status` call on `!is_remote_analyst` | Frontend | Removes a 403 from every analyst's DevTools |
| H5 | Hide Origin KPI card unit suffixes during loading state | Frontend | Single highest-impact "this looks broken" perception fix |
| H6 | Add `aria-pressed="true"` to active time-range + metric-tab buttons | Frontend | A11y compliance, ~5 lines of TSX |
| H7 | Add `aria-label` to the 10 nameless icon-only buttons | Frontend | A11y compliance |
| H8 | Add a server-side 302 redirect for `/alerts`, `/usage`, `/logs` when `is_remote_analyst` | Frontend (middleware.ts) | Stops the brief 200-then-redirect flash |

### Short-term enhancements (1-3 days each)

| ID | Action | Why |
|----|--------|-----|
| S1 | Split `/api/dashboard/bundle` into top-of-fold + below-fold; OR stream as ndjson | Faster perceived FCP; lets analysts see something within 500 ms |
| S2 | Re-render the Performance waterfall as stacked-bar (sum-to-100%) rather than shared linear X-axis | Edge Processing / Client Download become readable instead of 1 px wide |
| S3 | Reconcile timezone format in Query Explorer SQL — pick UTC or display-tz, use it consistently | Reduces copy-paste foot-guns |
| S4 | Add skip-to-content link + adjust heading hierarchy (h1→h2→h3) | WCAG 2.1 AA |
| S5 | Add Darwin fallback for System Health memory/disk metrics | Local dev usability |
| S6 | Sessions page: either apply default filters immediately or initialize them empty | Stops the "broken filter" appearance |
| S7 | Insights: bundle 15 anomaly cards into one request or lazy-mount via IntersectionObserver | -14 backend calls on `/insights` cold load |
| S8 | Annotate decorative SVGs `aria-hidden="true"`; add `<title>` to meaningful ones | A11y; 7 affected on dashboard alone |
| S9 | Network page world map: auto-fit zoom to data, or show CTA in empty state | Stops the "broken page" impression |

### Long-term design enhancements (1-2 weeks each)

| ID | Action | Why |
|----|--------|-----|
| L1 | Self-host session analytics (Posthog or OpenReplay) — don't weaken CSP for FullStory/Hotjar | Captures the funnel drops in §4c without compromising the strict CSP that's already paying off in this audit |
| L2 | First-run onboarding for the share-login URL-as-passcode flow | The single most-confusing first-touch moment in the app |
| L3 | Make the Add-Filter modal a slide-in side panel instead of centered modal — analysts often want to keep filtering while viewing the chart | Reduces the modal-then-close-then-modal-again loop |
| L4 | Server-render the first widget of `/dashboard` (Traffic over Time as SVG) so it shows up under the FCP curve rather than as a skeleton | Massive perceived-perf win |
| L5 | Add a `noindex,nofollow` meta + `Referrer-Policy: same-origin` header check (audit Caddyfile) for share-mode pages | Prevent leaks via referer to upstream destinations linked from log payloads |

---

## 6. Other Useful Things — Audit Add-Ons

The user's prompt asked "what else is best practice" — these were not on the list but I think are worth surfacing:

### 6a. Threat-model spot-check

Beyond the RBAC tests I ran, three classes of attack I'd want covered next:
- **CSRF on `/api/share/login`** — what's the cookie SameSite policy? If `Lax` or `None`, an attacker site could trick a logged-in analyst into re-authenticating against a malicious payload (depends on whether the cookie is HttpOnly and whether the login endpoint requires an Origin header check). Grep for `SameSite` in `share_auth.py`.
- **Open redirect on `/share-login/acknowledge`** — does the acknowledge endpoint accept a `return_to` param? If so, validate it's same-origin.
- **Log-injection via filter values** — when an analyst types a filter value into the IP-include filter, does it land in any server-side logging path unescaped? An analyst-controlled value containing `\n` could split a log line and forge fake entries in the audit log.

I didn't probe any of these — they need source review of `share_auth.py` + the request-logging middleware. Calling out so they go on a future audit list.

### 6b. Bundle composition (Next.js)

Run `npx @next/bundle-analyzer` locally and check:
- Are charting libs (Recharts? Chart.js? D3?) imported tree-shakeably or as a single bundle?
- Is the donut-chart code on `/charts` shared with the donut on `/origin`?
- Is the map (Leaflet? Mapbox? D3-geo?) lazy-loaded only on `/network` and `/dashboard`?

If any of these are pulled into the main bundle, splitting them buys hundreds of KB.

### 6c. Observability instrumentation

If you have OpenTelemetry traces from the backend, plumb the Server-Timing header (or `traceparent`) to the frontend and surface "slow request → trace ID" in DevTools-style overlays for the admin. This makes the existing `_section_timings` + `_debug_queries` envelopes (which are already stripped for analysts) far more useful for the operator.

### 6d. Tests this audit would inform

- **RBAC regression test** that replays the 23-endpoint admin probe + 14 bypass probe + 10 write-method probe as a single pytest. Asserts every entry returns 403. Run on every PR.
- **Lighthouse CI** with budgets: JS encoded ≤ 1500 KB, LCP ≤ 2500 ms, INP ≤ 200 ms. Fail on regression.
- **axe-core Playwright test** on the dashboard, performance, origin, security, insights, sessions, query pages. Fail on any new "critical" or "serious" violation.

### 6e. RBAC documentation suggestion

The middleware in `backend/utils/remote_access.py` is well-commented but the *user-facing* RBAC model (what analysts can/can't do, who issues invites, how to revoke) isn't documented in `README.md` or any `docs/` file I noticed. A short "Operator's guide to share-mode" doc would help the next reader of the codebase understand the design intent, and serve as the spec for any future RBAC-related changes.

---

## Appendix A — Test telemetry

**RBAC probe (analyst session on staging) — 23 admin endpoints**

All returned `403 {"error":"admin_only"}` except:
- `/api/usage` (GET) → `404 {"detail":"Not Found"}` — no route mounted at the bare path; the prefix `/api/usage/` is the actual blocked surface and verified separately

**Bypass-attempt battery — 14 attempts**

All returned `403 {"error":"admin_only"}` except:
- `/api/Admin/system-jobs` → `404` (case-sensitive routing, not a bypass)
- `//api/admin/system-jobs` → fetch-level rejection (-1)

**Write-method probe — 10 attempts on otherwise-allowed routers**

Two distinct response shapes (intentional — different cause):
- `/api/alerts/*` → `403 {"error":"admin_only"}` (prefix-blocked)
- `/api/views`, `/api/services`, `/api/services/{sid}/scoring/{enable,labels}` → `403 {"error":"read_only"}` (verb-blocked)

**Counter-test (loopback as admin)**

- `GET /api/admin/system-jobs` → `200` (returns 3 jobs)
- `GET /api/admin/pop-locations` → `200` (returns ~180 POPs)
- `GET /api/admin/usage-log` → `500` (sqlite db corrupt) — see H1 above

**Screenshots collected** (in `/tmp/audit-*`, may be cleared on reboot)

- `audit-staging-01..20-*.png` — staging walkthrough across all 9 nav pages, plus filter, theme, RBAC test, login
- `audit-local-01,02-*.png` — local admin dashboard + admin page

---

*End of report. Audit performed 2026-06-11 by automated harness driving headless Chromium against the live staging deployment and the local dev server. No data was modified — all probes were GET, plus POST/DELETE/PATCH calls that were expected to (and did) 403.*
