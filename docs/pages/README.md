# Page Specifications & Verification Matrix (`docs/pages/`)

This directory contains the authoritative functional and testing specifications for every page in Fastly Log Analytics. Each document is a self-contained contract intended to guide both human contributors and autonomous AI testing sessions across all supported architectures and roles.

---

## 1. Directory Structure

```
docs/pages/
├── README.md                      # This index & testing guidelines
├── dashboard.md                   # /dashboard (Primary overview & Top-N drill-down)
├── control-room.md                # /control-room (Real-time operational monitoring)
├── fastly-value.md                # /fastly-value (Service summary & ROI/value metrics)
├── performance.md                 # /performance (Edge latency, TTFB, cache delivery)
├── origin.md                      # /origin (Backend latency, retries, shielding)
├── security.md                    # /security (WAF, bots, TLS fingerprints, attacks)
├── insights.md                    # /insights (Automated anomaly detection & baselines)
├── network.md                     # /network (RTT, packet loss, congestion, ASNs)
├── streaming.md                   # /streaming (Live log streaming & inspect modal)
├── rum.md                         # /rum (Real User Monitoring & Core Web Vitals)
├── sessions.md                    # /sessions (Client session scoring & clustering)
├── sessions-stream.md             # /sessions/stream (Real-time session stream)
├── usage-and-cost.md              # /usage (FOS storage, Class A/B ops, cost estimator)
├── query.md                       # /query (DuckDB SQL query editor & saved queries)
├── alerts.md                      # /alerts (Threshold rules, webhook destinations)
├── logs.md                        # /logs (Paginated raw log viewer & filter drill-down)
├── assets-shield.md               # /assets-shield (Asset caching & origin shielding)
├── high-scale-request-facts.md    # /high-scale/request-facts (ClickHouse/High-Scale facts)
├── share-login.md                 # /share-login (Remote analyst portal & TOS gate)
└── admin/                         # Administrative & infrastructure sub-pages
    ├── overview.md                # /admin (System status, sync health, services)
    ├── queries.md                 # /admin/queries (Live query monitor & profiler)
    ├── queue.md                   # /admin/queue (Celery/background task queue depth)
    ├── rum.md                     # /admin/rum (RUM beacon collection & settings)
    ├── session-scoring.md         # /admin/session-scoring (Scorer weights & retrain)
    ├── share.md                   # /admin/share (Invites, sessions, audit, TOS)
    ├── trends.md                  # /admin/trends (Historical system vitals & trends)
    └── usage-log.md               # /admin/usage-log (Attributed FOS Class A/B ledger)
```

---

## 2. Page Specification Catalog

| Route | Page Name | Primary Focus | Spec Status |
|---|---|---|---|
| `/dashboard` | [Dashboard](dashboard.md) | Top-level KPIs, multi-trace traffic chart, geo map, categorized Top-N cards | **Complete** |
| `/control-room` | Control Room | Real-time operations, live throughput, recent error stream | *Pending* |
| `/fastly-value` | Service Summary / Value | Bandwidth saved, edge compute efficiency, cost avoidance | *Pending* |
| `/performance` | Performance | Edge delivery latency, TTFB, TTLB, regional performance | *Pending* |
| `/origin` | Origin Health | Backend latency, connect times, shielding ratio, retries | *Pending* |
| `/security` | Security | WAF detections, Verified Bots, NGWAF signals, TLS JA3/JA4 | *Pending* |
| `/insights` | Insights | 45 automated anomaly detectors across 5 category tabs | *Pending* |
| `/network` | Network Path | TCP RTT, packet loss, retransmits, ASN health heatmap | *Pending* |
| `/streaming` | Streaming | Live SSE log tailing, regex search, inspect modal | *Pending* |
| `/rum` | RUM | Real user telemetry, LCP, INP, CLS, client errors | *Pending* |
| `/sessions` | Sessions | Session graph, threat scoring, suspicious session inspection | *Pending* |
| `/sessions/stream` | Sessions Stream | Real-time session transitions, score escalation alerts | *Pending* |
| `/usage` | Usage & Cost | FOS storage, Class A/B API calls, interactive cost estimator | *Pending* |
| `/query` | SQL Query Pad | Direct DuckDB SQL runner, schema explorer, CSV download | *Pending* |
| `/alerts` | Alerts | Alert rules list, status history, notification channels | *Pending* |
| `/logs` | Raw Logs | High-throughput paginated log viewer, column toggle, drill-down | *Pending* |
| `/assets-shield` | Assets & Shield | Origin shielding efficiency, PoP topology, asset types | *Pending* |
| `/high-scale/request-facts` | High-Scale Facts | ClickHouse raw facts table explorer (High-Scale only) | *Pending* |
| `/share-login` | Analyst Share Login | Passcode & SSO authentication, TOS acceptance gate | *Pending* |
| `/admin` | Admin Overview | Sync status, compaction, storage usage, services CRUD | *Pending* |
| `/admin/queries` | Live Query Monitor | Running & recent SQLite/DuckDB queries, durations, locks | *Pending* |
| `/admin/queue` | Task Queue | Ingestion ledger claims, Celery queue depth, RedBeat | *Pending* |
| `/admin/rum` | RUM Ingestion | Beacon ingestion status, sample rates, client scripts | *Pending* |
| `/admin/session-scoring` | Session Scoring Config | Matrix weights, threshold overrides, model retrain | *Pending* |
| `/admin/share` | Live Share Admin | Invites CRUD, active sessions, audit trail, server switch | *Pending* |
| `/admin/trends` | System Trends | Metric history, ingestion latency, CPU/memory over time | *Pending* |
| `/admin/usage-log` | FOS Usage Ledger | Per-route and per-cron attribution for FOS storage costs | *Pending* |


---

## 3. Global Time-Range & History Default Contract (App-Wide Standard)

Across almost all analytics pages in this application (`/dashboard`, `/origin`, `/security`, `/performance`, `/network`, `/fastly-value`, `/sessions`, `/assets-shield`, `/logs`, `/query`), the default display window must adhere to this unified time-window contract:

1. **Default Window (Mature Services with >= 24h Data):**
   - Loads the **last 24 hours of data** (`[now - 24h, now]`) by default.
   - Provides an immediate high-fidelity rolling view of recent operational traffic and performance.

2. **Adaptive Max-Range Fallback (Services with < 24h Data):**
   - If the active service has **less than 24 hours of total data** in the system (e.g. newly provisioned services, fresh staging environments, test topologies, or any dataset where `latest_log_at - earliest_log_at < 24h`):
     - The page **must dynamically display the current maximum available range of data** (`[earliest_log_at, latest_log_at]` or `[earliest_log_at, now]`).
     - It must **not** render a naive 24-hour window where 90%+ of the time-series is blank space or squashed against the right edge.
     - The time controls and filter bar reflect this discovered extent without throwing zero-width window errors (`clamped time range is empty`).

3. **Smooth Maturation:**
   - As new log batches arrive and continuous ingest pushes the history span past 24 hours, the default rolling window smoothly transitions to capping at 24 hours.

4. **Page Exemptions:**
   - **Real-Time Stream Tailing:** `/control-room`, `/streaming`, and `/sessions/stream` operate on live sliding event buffers (e.g. last 100–1,000 events or 1m–5m live window) rather than the 24h historical window.
   - **Admin Operational Trend Vitals:** `/admin/trends` and `/admin/usage-log` use administrative monitoring windows (e.g. 7d or 30d host vitals / FOS ledger).

---

## 4. Global UI/UX Responsiveness & Layout Architecture Standard

To deliver a best-in-class, instantaneous, and fluid user experience across all pages, every page implementation and test must strictly enforce these frontend architectural standards:

1. **Instant Page Shell Render (< 400ms FCP):**
   - The page shell, global navigation, service selector, filter bar, and panel grid containers must paint immediately upon navigation without blocking on analytical backend queries.
   - Server-side rendering (SSR) seeds initial keys and quantization metadata so the browser paints the complete layout structure on first frame.

2. **Pre-Allocated Layout & Zero Cumulative Layout Shift (CLS = 0.00):**
   - **No Pop-In or Layout Jumping:** Under no circumstances may panels, tables, or charts pop in late and shove other content down the page.
   - Every chart, map, and card container must reserve its full vertical and horizontal space in advance using CSS layout containment (e.g. `h-[300px]`, `contain-intrinsic-size: 300px`, `[content-visibility:auto]`).
   - Static categories and section skeletons render immediately, even before the dynamic catalog query returns.

3. **In-Place Per-Panel Skeletons with Contextual Loading Messages:**
   - While data is fetching or computing, each individual panel renders an unobtrusive skeleton inside its pre-allocated container featuring an animated pulse and a clear, contextual loading message:
     - **Time-Series Charts:** `"Crunching logs..."` (or `"Initializing..."` during warm-up).
     - **Geographic Maps:** `"Mapping traffic..."` (or `"Loading map..."`).
     - **Top-N Metric Cards:** `"Loading..."` inside reserved 300px card boxes.
     - **Raw Log Grids:** Reserved table row skeletons with pulse indicators.

4. **Progressive Hydration & Non-Destructive Background Refresh:**
   - As data arrives, skeletons transition smoothly to rendered visuals (`transition-opacity duration-100`).
   - **Preserve Visual Context:** When the user changes a filter, modifies a time range, or an auto-refresh fires, already-rendered panels must **never** collapse back into blank skeletons. Instead, they remain visible with a subtle dim (`opacity-40 pointer-events-none`) while background queries execute, ensuring uninterrupted context.

5. **Main-Thread Responsiveness & Web Worker Offloading:**
   - Heavy data transformations (such as multi-trace time-series grouping, percentile interpolations, and client-side histogram computations) are offloaded to dedicated Web Workers, preserving a continuous 60fps main thread with no scroll stutter or input lag.

---

## 5. Universal Shared Primitives & Cross-Architecture, Cross-Role Parity

To prevent fragmentation and duplicate logic as we implement and test individual pages, every page **must actively share common primitives** and adhere to universal cross-architecture and cross-role design contracts:

### 1. Universal Frontend Shared Primitives
- **Shared Page Shell (`ReportLayout`):**
  - All analytics pages (`/dashboard`, `/origin`, `/security`, `/performance`, `/network`, `/fastly-value`, `/sessions`, `/assets-shield`, `/logs`, `/query`) must be wrapped in `ReportLayout`.
  - It centralizes the service selector, time-range presets, quick filters, custom filter bar, timezone switcher, saved views dropdown, compare mode switch, sync health badge, and role footer.
  - **No bespoke headers:** Individual pages must never reimplement their own filter bars, time pickers, or service dropdowns.
- **Global Zustand Stores:**
  - `useFilterStore`: Shared filter pills (`filters`, `addFilter`, `removeFilter`, `clearFilters`, `toggleNegate`). Filters applied on the dashboard persist when navigating to `/origin`, `/security`, or `/query`.
  - `useTimeRangeStore`: Shared time bounds (`startTime`, `endTime`, `timezone`, `quickPreset`). Enforces the universal 24h default and adaptive history fallback across all pages.
  - `useServiceStore`: Active service selection, service switching, and viewer/read-only mode flags.
  - `useActiveLogFields`: Fastly VCL active logging fields catalog. Used across all pages to detect whether a dimension's field group is active in Fastly logging, gracefully rendering missing-field diagnostic instructions instead of empty or broken charts.
- **Standardized Skeleton & Dimming Primitives:**
  - Every panel on every page must use the unified layout reservation pattern: `data-empty-placeholder="true"`, fixed height/containment (`h-[300px]`, `min-h-[300px]`, `contain-intrinsic-size: 300px`, `[content-visibility:auto]`), and animated pulse copy.
  - Every panel must implement non-destructive background re-fetch dimming (`transition-opacity duration-100`, `opacity-40 pointer-events-none`) so existing visuals remain visible during filter changes.
- **Shared Visualization Components:**
  - `TimeSeriesChart`: Standardized multi-trace time-series charts with synchronized tooltips, drag-to-zoom setting `useTimeRangeStore`, and off-thread Web Worker transform offloading (`buildTrafficDataAsync`).
  - `TopNTable` / `CardGrid`: Uniform table rankings with click-to-filter drill-down (`onRowClick`), copy-to-clipboard, and bot-badge integration.
  - `ChoroplethMap`: Client-only SVG world map with country click-to-filter drill-down.

### 2. Multi-Role Parity Contract (Admin vs Remote Share vs Standalone)
Every page feature, drill-down, and visualization must work properly across all three supported user personas:
- **Admin (`read_write`):**
  - Full access to all analytics, admin configuration (`/admin/*`), custom field management, alert rule creation, raw IPs, and SQL execution (`/query`).
- **Analyst Path B (Remote Share - Live Instance):**
  - Read-only analytics access through authenticated live share sessions (`/share-login`).
  - Strict security boundaries: Administrative mutation endpoints are blocked; mutation buttons (e.g. "Save View", "Create Alert", "Edit Custom Fields") are hidden or read-only; IP addresses are masked if the administrator enabled privacy masking; direct filesystem and raw FOS credentials are never exposed.
- **Analyst Path A (Standalone Instance - JSON Join):**
  - Independent instance running against read-only FOS bucket credentials.
- **Enforcement:** Tenancy, role permissions, and service isolation are enforced server-side via `RequestContext` (`backend/core/request_context.py`). No client-side bypass is possible.

### 3. Multi-Architecture Parity Contract (Standard vs High-Scale)
Every page's underlying queries, aggregations, and data pipelines must function identically across both deployment topologies:
- **Standard Deployment Mode (`DEPLOYMENT_MODE=standard`):**
  - Single-node synchronous ingest with local Parquet buffer and local DuckLake catalog.
  - Serving queries execute against thread-local DuckDB connections stitching the local buffer and DuckLake table.
- **High-Scale Deployment Mode (`DEPLOYMENT_MODE=high_throughput`):**
  - Distributed Celery + Valkey + RedBeat worker ingest with shared Postgres DuckLake catalog (`DUCKLAKE_CATALOG`) and `ingest_ledger`.
  - Serving queries execute against ephemeral in-memory DuckDB instances reading durable DuckLake parquet directly from cloud storage.
- **No Local Filesystem Assumptions:** Analytics pages and queries must never assume local cache files or local buffer parquet exist when running under the high-throughput topology.

### 4. Single-Round-Trip Composite API Pattern (No N+1 Waterfall)
- Pages must avoid firing N separate HTTP requests for N different panels or cards.
- Where appropriate, pages must expose and consume composite endpoints (e.g. `/api/dashboard/bundle` or section-level bundles) that assemble time-series aggregates, Top-N dimensions, and summary KPIs in a single round-trip query execution.
- This prevents DuckDB connection pool starvation, minimizes network latency, and ensures all panels on a page hydrate concurrently without cascading layout reflows.

---

## 6. Template for New Page Specifications

When creating or updating a page specification, use the following standard structure:

1. **Overview & Objectives:** User goals, page purpose, key design tenets.
2. **Routes, URLs & Query Parameters:** Supported params (`service`, `from`, `to`, `filters`, etc.).
3. **Role & Permission Matrix:** Admin vs Analyst Path B (Remote Share) vs Analyst Path A.
4. **Architecture Execution Matrix:** Standard (DuckDB + Parquet) vs High-Scale (Postgres + DuckLake/ClickHouse).
5. **Backend APIs & Query Attributions:** Endpoints hit, request/response contracts, SQL templates.
6. **UI Components, Panels & Tabs:** Visual breakdown of all cards, charts, toolbars, and modals.
7. **Interactive Workflows & Edge Cases:** Filters, time presets, drill-downs, empty states, zero-data.
8. **Performance, Cost & Telemetry Budgets:** Target p95 latencies, query limits, OTel spans.
9. **AI Session Automated Verification Checklist:** Step-by-step checklist for Playwright/curl verification.
