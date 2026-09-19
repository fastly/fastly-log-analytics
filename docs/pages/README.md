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
| `/control-room` | [Control Room](control-room.md) | Real-time operations, live throughput, recent error stream | **Scaffolded (Pending AI Verification)** |
| `/fastly-value` | [Service Summary / Value](fastly-value.md) | Bandwidth saved, edge compute efficiency, cost avoidance | **Scaffolded (Pending AI Verification)** |
| `/performance` | [Performance](performance.md) | Edge delivery latency, TTFB, TTLB, regional performance | **Scaffolded (Pending AI Verification)** |
| `/origin` | [Origin Health](origin.md) | Backend latency, connect times, shielding ratio, retries | **Scaffolded (Pending AI Verification)** |
| `/security` | [Security](security.md) | WAF detections, Verified Bots, NGWAF signals, TLS JA3/JA4 | **Scaffolded (Pending AI Verification)** |
| `/insights` | [Insights](insights.md) | 45 automated anomaly detectors across 5 category tabs | **Scaffolded (Pending AI Verification)** |
| `/network` | [Network Path](network.md) | TCP RTT, packet loss, retransmits, ASN health heatmap | **Scaffolded (Pending AI Verification)** |
| `/streaming` | [Streaming](streaming.md) | Live SSE log tailing, regex search, inspect modal | **Scaffolded (Pending AI Verification)** |
| `/rum` | [RUM](rum.md) | Real user telemetry, LCP, INP, CLS, client errors | **Scaffolded (Pending AI Verification)** |
| `/sessions` | [Sessions](sessions.md) | Session graph, threat scoring, suspicious session inspection | **Scaffolded (Pending AI Verification)** |
| `/sessions/stream` | [Sessions Stream](sessions-stream.md) | Real-time session transitions, score escalation alerts | **Scaffolded (Pending AI Verification)** |
| `/usage` | [Usage & Cost](usage-and-cost.md) | FOS storage, Class A/B API calls, interactive cost estimator | **Scaffolded (Pending AI Verification)** |
| `/query` | [SQL Query Pad](query.md) | Direct DuckDB SQL runner, schema explorer, CSV download | **Scaffolded (Pending AI Verification)** |
| `/alerts` | [Alerts](alerts.md) | Alert rules list, status history, notification channels | **Scaffolded (Pending AI Verification)** |
| `/logs` | [Raw Logs](logs.md) | High-throughput paginated log viewer, column toggle, drill-down | **Scaffolded (Pending AI Verification)** |
| `/assets-shield` | [Assets & Shield](assets-shield.md) | Origin shielding efficiency, PoP topology, asset types | **Scaffolded (Pending AI Verification)** |
| `/high-scale/request-facts` | [High-Scale Facts](high-scale-request-facts.md) | ClickHouse raw facts table explorer (High-Scale only) | **Scaffolded (Pending AI Verification)** |
| `/share-login` | [Analyst Share Login](share-login.md) | Passcode & SSO authentication, TOS acceptance gate | **Scaffolded (Pending AI Verification)** |
| `/admin` | [Admin Overview](admin/overview.md) | Sync status, compaction, storage usage, services CRUD | **Scaffolded (Pending AI Verification)** |
| `/admin/queries` | [Live Query Monitor](admin/queries.md) | Running & recent SQLite/DuckDB queries, durations, locks | **Scaffolded (Pending AI Verification)** |
| `/admin/queue` | [Task Queue](admin/queue.md) | Ingestion ledger claims, Celery queue depth, RedBeat | **Scaffolded (Pending AI Verification)** |
| `/admin/rum` | [RUM Ingestion](admin/rum.md) | Beacon ingestion status, sample rates, client scripts | **Scaffolded (Pending AI Verification)** |
| `/admin/session-scoring` | [Session Scoring Config](admin/session-scoring.md) | Matrix weights, threshold overrides, model retrain | **Scaffolded (Pending AI Verification)** |
| `/admin/share` | [Live Share Admin](admin/share.md) | Invites CRUD, active sessions, audit trail, server switch | **Scaffolded (Pending AI Verification)** |
| `/admin/trends` | [System Trends](admin/trends.md) | Metric history, ingestion latency, CPU/memory over time | **Scaffolded (Pending AI Verification)** |
| `/admin/usage-log` | [FOS Usage Ledger](admin/usage-log.md) | Per-route and per-cron attribution for FOS storage costs | **Scaffolded (Pending AI Verification)** |
| `/admin/clickhouse` | [ClickHouse Cluster Admin](admin/clickhouse.md) | Cluster health, schema alignment, dataset generation, bounded replay | **Scaffolded (Pending AI Verification)** |


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
Every page's underlying queries, aggregations, and data pipelines must function predictably across both deployment topologies:
- **Standard Deployment Mode (`DEPLOYMENT_MODE=standard`):**
  - Single-node synchronous ingest with local Parquet buffer and local DuckLake catalog.
  - Serving queries execute against thread-local DuckDB connections stitching the local buffer and DuckLake table.
  - SQLite WAL databases manage service metadata, cron logs, usage tracking, and NGWAF bot caches.
- **High-Scale Deployment Mode (`DEPLOYMENT_MODE=high_throughput`):**
  - Distributed Celery + Valkey + RedBeat worker ingest with shared Postgres DuckLake catalog (`DUCKLAKE_CATALOG`) and `ingest_ledger`.
  - **ClickHouse Serving & Fact Engine:** Ingests massive event streams into partitioned MergeTree tables (`request_facts`, `high_scale_batch_publications`, `cmcd_projection_facts`, and minute-level dimensions for origin, security, network, and performance). Supports bounded diagnostic replay via `PgManifest` and native incremental backup (`backend/high_scale/clickhouse_backup.py`).
  - Serving queries execute against ephemeral in-memory DuckDB instances reading durable DuckLake parquet directly from cloud storage, with ClickHouse fact exploration via `/high-scale/request-facts`.
- **No Local Filesystem Assumptions:** Analytics pages and queries must never assume local cache files or local buffer parquet exist when running under the high-throughput topology.

### 4. Single-Round-Trip Composite API Pattern (No N+1 Waterfall)
- Pages must avoid firing N separate HTTP requests for N different panels or cards.
- Where appropriate, pages must expose and consume composite endpoints (e.g. `/api/dashboard/bundle` or section-level bundles) that assemble time-series aggregates, Top-N dimensions, and summary KPIs in a single round-trip query execution.
- This prevents DuckDB connection pool starvation, minimizes network latency, and ensures all panels on a page hydrate concurrently without cascading layout reflows.

### 5. Telemetry & Query Audit Contract (Comprehensive Observability Across All Engines)
Fastly Log Analytics features an integrated observability and telemetry architecture (`X-Page-Load-ID`, `RequestTelemetry`, multi-engine query profilers, and the interactive Debug Panel). As we test and audit every page, we must rigorously verify telemetry capture and audit the resulting queries:
- **100% Instrumentation (Zero "Dark" Queries or Calls Across All Engines):**
  - Every analytical query across **DuckDB** and **ClickHouse** (`query_registry.register("ClickHouse", ...)`), operational query across **SQLite** and **Postgres**, external Fastly/FOS API call, and logical execution section executed to render a page MUST be captured and attributed to that request's `X-Page-Load-ID` in `telemetry_queries`, `telemetry_sections`, and `usage_log`.
  - The UI Debug Panel explicitly surfaces both DuckDB and ClickHouse queries under `"Data Queries (DuckDB / ClickHouse)"` alongside SQLite and HTTP calls.
  - No database query or network call may execute silently without instrumentation.
- **Mandatory Query & Call Audit on Every Page Load:**
  - Automated tests and AI verification sessions must fetch `/api/debug/page-telemetry?service_id={id}&page_load_id={id}` (or inspect the Debug Panel) after every page load.
  - **Efficiency Audit:** Confirm queries utilize partition pruning, index hits, zero redundant or duplicate statements, no N+1 query loops, and appropriate rollup parquet or ClickHouse minute-level aggregates over raw full-table scans.
  - **Propriety Audit:** Confirm strict tenancy (`service_id` isolation), parameterized SQL templates, correct caller attribution, and proper error/status codes.
  - **Latency & Resource Budgets:** Confirm database execution time, connection acquisition wait time (`app.thread_wait_ms`), and total page load time fall well within the page's defined p95 performance budget.

### 5.6 Full Tooling & Automation Ecosystem Contract
All page specifications, tests, and deployment verification procedures must account for our full operational tooling stack:
- **Fastly VCL Linter & Dialect Simulator (`falco`):** Any VCL generated for log format strings, custom field expressions (`vcl_log_expression`), or edge snippets must pass `falco lint` and simulation tests before edge deployment.
- **Distributed Ingestion & Scheduler (Celery, RedBeat, Valkey/Redis):** Asynchronous task distribution, high-throughput ingest workers, crash-net recovery sweeps (`ledger_sweep`), and shared distributed state.
- **Telemetry & Monitoring (OpenTelemetry, Prometheus, Grafana):** OTel tracing propagation, Prometheus scrape rules (`observability/clickhouse.rules.yml`, `observability/prometheus.yml`), and multi-pod dashboards (`observability/dashboards/fla-multipod.json`).
- **End-to-End Testing & Verification (Playwright, Vitest, Pytest):** Dual-role Playwright E2E suites verifying Admin vs Analyst Path B access, network HAR performance capture, Core Web Vitals audits (LCP, INP, CLS), and unit tests (`pytest -n` with strict database test isolation).

### 5.7 Scheduled Jobs & Cron Testing Contract
In addition to user-facing page requests, Fastly Log Analytics relies on 25 background automation jobs that continuously drive ingest, compaction, table optimization, snapshot expiry, and metadata housekeeping across Standard and High-Scale modes.
- **Authoritative Specification:** See [docs/cron/README.md](../cron/README.md) for the exhaustive breakdown of all scheduled jobs, intervals, database locks, execution lifecycles, and testing runbooks.
- **Cron Testing Mandate:** Any testing session or automated test suite verifying system health MUST verify:
  1. **Scheduler Registration:** All expected jobs for the deployment mode (`standard` vs `high_throughput`) are registered in `APScheduler` or `RedBeat`.
  2. **Manual Triggerability:** Admin trigger endpoints (`POST /api/admin/sync/{id}`, `POST /api/admin/commit/{id}`, etc.) respond with HTTP 200 and complete successfully.
  3. **Zero Dark Cron Work:** All database operations and FOS API calls made by background jobs must be attributed in `usage_log.db` and recorded in `cron_runs`.
  4. **Error Recovery & Dead-Letter:** Crash-recovery jobs (`ledger_sweep`, `gap_heal`) must be verified to reclaim orphaned tasks without data loss.


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
10. **Automated Test Suite & Traffic Generation:** Playwright spec files and traffic profile requirements.
