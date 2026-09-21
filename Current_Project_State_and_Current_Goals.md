# Current Project State and Current Goals

## Current Project State

v3.0.0 is designed to add a new architecture for this project which we call "high-scale". The current architecture we call "standard". The "standard" architecture is meant for sites or situations when you only want to log like 10k RPS (we need to confirm what we think is the max for "standard"). The "high-scale" architecture is designed for sites that get much more traffic like 2M RPS on average with bursts to up to 5M RPS.

The "standard" architecture is meant to be run on a laptop or a small to medium sized VM. The "high-scale" architecture is meant to run on a Kubernetes cluster (or equivalent) with auto-scaling for one or more of the pods.

We have written and deployed a significant amount of code for the "high-scale" architecture, but there is still work to do to make sure it is 100% complete.

We deployed the "standard" architecture to the GCE machine (see the .claude/skills/deploy-to-gce-and-verify/SKILL.md skill) which you can connect to here:

gcloud compute ssh fastly-log-analysis --zone=us-central1-a --project=se-development-9566

That site has active logs coming in from service cVnu9mYB3Cvmob3lsqjQU3 and has many days of historical logs.

We deployed the "high-scale" architecture to Fastly Elevation dev-usc1 cluster (see the local-docs/elevation-deployment.md file for more info). That architecture has the site ZEZ4mcAjoSFDTg7tpkDKV2 deployed to it, but that site does not get active traffic. We have some scripts to push logs to that service and you can send test traffic to that service at will as well. We have sent some logs in the past but also have done some cleanup of the logs along the way.

The "standard" deployment can also be run on my laptop.

## Current Goals

We need to complete all of the code for the "high-scale" architecture. We also need to confirm that the "standard" architecture still works flawlessly alongside it. This means that all pages on the site work the same with each architeture, the only difference being how they load the data for each page.

Here is the comprehensive inventory of top-level pages, sub-pages, and tabs, each mapped to its authoritative functional specification in [`docs/pages/`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/):

### Core Analytics & Monitoring Pages
- [**Dashboard**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/dashboard.md) (`/dashboard`) — Top-level KPIs, multi-trace traffic chart, interactive world map, 9 categorized Top-N cards, drag-to-zoom, row click-to-filter.
- [**Control Room**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/control-room.md) (`/control-room`) — Real-time operational command center, live throughput gauges, active error streams.
- [**Service Summary / Value**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/fastly-value.md) (`/fastly-value`) — Fastly edge value metrics, bandwidth savings, compute offload, cache efficiency ROI.
- [**Performance**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/performance.md) (`/performance`) — Edge delivery latency, TTFB, TTLB, regional percentiles, compression efficiency.
- [**Origin Health**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/origin.md) (`/origin`) — Backend response times, connect latencies, shielding ratio, retries, origin error breakdown.
- [**Security**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/security.md) (`/security`) — WAF rule triggers, Verified Bots, NGWAF signals, TLS JA3/JA4 fingerprints, suspicious actors.
- [**Insights**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/insights.md) (`/insights`) — 45 automated anomaly detectors across 5 category tabs (Security, Origin, Edge, Network, Volumetrics).
- [**Network Path**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/network.md) (`/network`) — TCP RTT, packet loss, retransmits, delivery rate, ASN health heatmap, congestion.
- [**Streaming**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/streaming.md) (`/streaming`) — Live SSE log tailing, regex search filter, inspect modal with full JSON decode.
- [**RUM (Real User Monitoring)**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/rum.md) (`/rum`) — Client Core Web Vitals (LCP, INP, CLS), client-side JS errors, browser/device breakdown.
- [**Sessions**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/sessions.md) (`/sessions`) — Behavioral session tracking, threat scoring, score escalation, session inspection modal.
- [**Sessions Stream**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/sessions-stream.md) (`/sessions/stream`) — Real-time stream of transitioning client sessions and threat scoring triggers.
- [**Usage & Cost**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/usage-and-cost.md) (`/usage`) — Fastly Object Storage Class A/B operation breakdown, storage volume, interactive cost estimator.
- [**SQL Query Editor**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/query.md) (`/query`) — Ad-hoc DuckDB SQL pad, schema tree, query history, CSV/JSON export.
- [**Alerts**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/alerts.md) (`/alerts`) — Threshold alert rules, evaluation history, webhook notifications, status code gating.
- [**Raw Logs Viewer**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/logs.md) (`/logs`) — High-throughput paginated raw log viewer, column customizer, click-to-filter drill-down.
- [**Assets & Shield**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/assets-shield.md) (`/assets-shield`) — Origin shielding topology, asset caching performance, PoP-to-shield latency.
- [**High-Scale Request Facts**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/high-scale-request-facts.md) (`/high-scale/request-facts`) — ClickHouse / High-Scale raw facts explorer (available in High-Scale deployments).
- [**Analyst Share Login**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/share-login.md) (`/share-login`) — Remote analyst authentication portal (Passcode & SSO) and TOS acceptance gate.

### Admin & Infrastructure Pages (`/admin/*`)
- [**Admin Overview**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/overview.md) (`/admin`) — System health vitals, per-service sync status, compaction status, disk usage, service CRUD.
- [**Live Query Monitor**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/queries.md) (`/admin/queries`) — Active and recent queries across DuckDB and SQLite, execution times, lock status.
- [**Task Queue**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/queue.md) (`/admin/queue`) — Celery task queues, Ingest Ledger claim/commit state, RedBeat schedules.
- [**RUM Beacon Settings**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/rum.md) (`/admin/rum`) — RUM ingestion toggle, sample rates, edge script injection status.
- [**Session Scoring Admin**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/session-scoring.md) (`/admin/session-scoring`) — Scorer matrix weights, threshold tuners, model retrain triggers.
- [**Live Share Management**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/share.md) (`/admin/share`) — Analyst invite generation, active session list, audit trail, server switch.
- [**System Metric Trends**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/trends.md) (`/admin/trends`) — Long-term trend graphs for CPU, memory, ingest lag, and query latencies.
- [**FOS Usage Ledger**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/usage-log.md) (`/admin/usage-log`) — Attributed timeline of every Fastly Object Storage Class A / Class B API call.
- [**ClickHouse Cluster Admin**](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/clickhouse.md) (`/admin/clickhouse`) — ClickHouse cluster status, dataset generations, bounded replay execution, mutation tracking, schema health.

Many of these pages feature sub-tabs (e.g. Insights has 5 category tabs, Admin has 9 sub-sections, Origin has shield vs backend tabs), modals with deep drill-down links (e.g. session scoring inspector, streaming log inspector), and filter propagation to `/logs` and `/dashboard`.

We also need to make sure everything works for both the admin role (direct access / SSH port forward) and an analyst viewing over the remote share dashboard (`/share-login`).

Many pages include the ability to filter that page by one or more fields, or to link to the dashboard pre-filtered. The dashboard itself also allows for filtering by one or more fields. We need to confirm that all filtering works on all pages (accounting for all sections on all of those pages).

### Core Architectural Principle: Universal Shared Primitives Across All Pages & Roles
As we work on individual pages (starting with Dashboard, then Origin, Security, Performance, Network, etc.), **we must actively ensure that functionality is shared smartly and consistently across all pages rather than fragmented or duplicated**:
1. **Shared Layout & Shell:** Every analytics page must leverage `ReportLayout` and `FilterBar`. Individual pages must never build bespoke navigation, time pickers, or filter inputs.
2. **Shared State Stores:** Filters, time ranges, and active services must flow through the global Zustand stores (`useFilterStore`, `useTimeRangeStore`, `useServiceStore`, `useActiveLogFields`). Navigating between pages preserves active filters and time bounds seamlessly.
3. **Universal Time-Window Standard:** Every page follows the rolling 24h default for mature datasets and the adaptive max-range fallback for datasets with <24h history.
4. **Uniform Loading & Skeleton Pattern:** Every panel across every page must enforce pre-allocated geometry (`contain-intrinsic-size`, fixed container heights), in-place contextual loading skeletons (`Crunching logs...`, `Loading...`), zero layout shift (CLS = 0.00), and non-destructive background dimming (`opacity-40 pointer-events-none`).
5. **Cross-Role Parity:** Every page must work properly for Admin (`read_write`), Analyst Path B (Remote Share read-only with IP masking), and Analyst Path A (JSON join). Administrative mutations must be securely blocked and cleanly disabled for analysts.
6. **Cross-Architecture Parity:** Every page must work properly under both Standard (`DEPLOYMENT_MODE=standard`) and High-Scale (`DEPLOYMENT_MODE=high_throughput`), never assuming local filesystem buffers exist under high-scale.
7. **Composite Data Loading:** Avoid N+1 waterfall requests; consolidate multi-panel telemetry into single-round-trip composite endpoints (`bundle`).
8. **Comprehensive Observability & Query/API Call Auditing:** We built extensive telemetry and logging into the app (`X-Page-Load-ID`, `RequestTelemetry`, `telemetry_queries`, DuckDB/ClickHouse/SQLite profilers, and the interactive Debug Panel) to audit everything that happens on a page load:
   - **Complete Instrumentation:** Verify that 100% of the queries (DuckDB and ClickHouse analytical queries, SQLite and Postgres operational queries), external Fastly/FOS API calls, and section timings involved in generating each page are captured and attributed. No "dark" or unmeasured operations may execute.
   - **Audit Queries and Calls on Each Page Load:** Examine every captured query and API call to confirm they are **efficient** (proper partition pruning, index utilization, zero duplicate/redundant queries, no N+1 query loops, and appropriate rollup table usage over raw full scans) and **proper** (strict tenancy filtering, parameterized SQL, valid caller attribution, and execution durations within the p95 latency budget).
9. **Full Database Engine & Tooling Ecosystem Codification (ClickHouse, DuckDB/DuckLake, Postgres, SQLite, Celery, Falco, OTel, Playwright):**
   Every document, test runbook, and verification session must explicitly account for all data engines and architectural tools in the stack:
   - **ClickHouse (High-Scale Analytical Store & Serving Engine):** In `DEPLOYMENT_MODE=high_throughput`, ClickHouse stores raw high-scale event facts and pre-aggregated minute dimensions (`request_facts`, `high_scale_batch_publications`, `cmcd_projection_facts`, `origin_minute_summary`, `network_minute_dimensions`, `security_minute_dimensions`, `performance_minute_dimensions`). Supports bounded diagnostic replay (`PgManifest`, `/api/admin/clickhouse/replay`), incremental table backups (`backend/high_scale/clickhouse_backup.py`), and registers all internal queries into `query_registry.register("ClickHouse", ...)` for complete Live Query Monitor and Debug Panel visibility.
   - **DuckDB & DuckLake (Embedded Analytical Engine & Lakehouse Catalog):** Primary analytical querying engine across all modes. In Standard mode, stitches local Parquet buffers and DuckLake tables. In High-Scale mode, serves ephemeral in-memory queries over durable cloud DuckLake storage without local buffer dependencies. Durability is maintained via `ducklake_flush_inlined_data` and compaction via `ducklake_rewrite_data_files`.
   - **Postgres (Metadata DB & Multi-Writer State Machine):** Powers `METADATA_DSN` and `DUCKLAKE_CATALOG` in High-Scale mode. Owns the distributed `ingest_ledger` state machine (`discovered → claimed → committed/quarantined`), distributed row locks, and ClickHouse replay manifests (`pg_manifest_datasets`, `pg_manifest_artifacts`).
   - **SQLite (WAL-Mode Operational & Metadata Stores):** Thread-local pooled SQLite databases (`ThreadLocalPool`) managing per-service metadata (`metadata.db`), Fastly Object Storage accounting (`usage_log.db`), remote share invites and audit sessions (`remote_share.db`), and NGWAF bot intelligence (`ngwaf_bot_cache.db`).
   - **Celery, RedBeat & Valkey/Redis (Distributed Ingestion Engine):** Distributed background task queues and periodic schedulers handling high-throughput log discovery, conversion, compaction, and crash recovery sweeps (`ledger_sweep`, `ledger_rum_sweep`).
   - **Fastly VCL Linter & Dialect Simulator (`falco`):** All edge log format strings, custom field expressions (`vcl_log_expression`), and dynamic VCL snippets must pass `falco lint` and simulation before edge deployment.
   - **Observability Stack (OpenTelemetry, Prometheus, Grafana, Debug Panel):** Complete trace context propagation (`RequestTelemetry`, `X-Page-Load-ID`), Prometheus alerting and scrape rules (`observability/clickhouse.rules.yml`, `observability/prometheus.yml`), Grafana dashboards (`observability/dashboards/fla-multipod.json`), and the frontend `DebugPanel` auditing DuckDB, ClickHouse, SQLite, and external API timings.
   - **Testing & Quality Frameworks (Playwright, Vitest, Pytest):** End-to-end browser verification under dual roles (Admin vs Analyst Path B), Core Web Vitals profiling (LCP, INP, CLS), network HAR captures, and high-load stress testing via synthetic log generation.
10. **Interactive Clarification & Inquiry Mandate (Never Guess, Always Clarify):**
   Before beginning implementation, testing, or refactoring on any page, background job (cron), API, or architectural feature:
   - **Read & Synthesize First:** Read the target specification (`docs/pages/{page}.md`, `docs/cron/jobs/{cron}.md`), supporting architecture guides (`docs/ARCHITECTURE.md`, `AGENTS.md`), deployment runbooks, and existing test suites.
   - **Ask Clarifying Questions:** Proactively ask the operator as many clarifying questions as necessary about requirements, ambiguous behaviors, test traffic profiles, UI expectations, deployment nuances, or architecture-specific edge cases. **Never make unvalidated assumptions or guess intent** when details can be clarified.
   - **Document First, Then Execute:** Incorporate all answers, clarifications, and design decisions directly back into the authoritative documentation before executing code changes or test suites.

In addition to confirming that all functionality works, we also need to confirm that we're loading all data and pages in an optimal way. Log all queries that were involved in the page loads along with all API calls. Examine all of the queries and API calls to confirm they are appropriate and optimized for performance and cost. Also look for any queries or API we did not include in our logging properly and confirm they are included. We also need to confirm the user experience is optimal. Therefore, you should look at how quickly the page becomes interactive, how quickly all data loads in and how fast the page becomes fully interactive. Use real browser interactions and things like HAR files to analyze what is happening, and do it over repeated iterations to look at averages as well as things like p95 performance.

To repeat, all functionality, pages, and interactions need to be tested for both roles and both architectures and while under expected load.

## What you are allowed to do

You have full control over the `release/v3.0.0-beta2` branch (current active branch — supersedes the earlier `v3.0.0-beta1` this doc originally referenced) as well as the GCE machine, my local laptop and Elevation dev-usc1 cluster for compiling and testing everything. You are free to build and deploy to those systems as needed, and if you need image tags from me for Elevation deploys please ask.

You are welcome to upload to each service's raw log bucket as many logs as you want or send real synthetic traffic and also delete log data at will if needed. You are also authorized to deploy VCL updates during testing to either service or its ancillary services using the tokens from the existing services or better yet using the mechanisms already built into the code and UI.

If you do send real traffic, do not go over 25k RPS. You can push sythetic logs to simulate traffic higher than that.

## Local Access & Port Forwarding

When testing locally, it is critical to confirm that port forwarding is active and functioning correctly after every deployment so you can view and test the changes alongside the automated tools.

**Note:** GCE and Elevation architectures run the frontend in production mode (via `next build` and Next.js Turbopack where applicable) to validate true production performance. The Local instance runs in dev mode (`next dev`) for easier debugging.

**For "Standard" Architecture (GCE):**
The GCE deployment uses a direct SSH tunnel to forward the internal loops to your local machine.
1. Run this command on your laptop to start the tunnel:
   `gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000`
2. Access the Admin UI locally at: `http://localhost:3001/admin`
3. Access the Analyst view via the public Fastly URL at `/share-login`.
4. Confirm the footer shows that we're connected to the GCE host.

**For "High-Scale" Architecture (Elevation cluster):**
The Elevation cluster uses standard Kubernetes port-forwarding to the `se-demo` namespace.
1. Forward the frontend service to your local machine:
   `kubectl port-forward svc/frontend-svc -n se-demo 3002:3000`
2. Forward the backend service:
   `kubectl port-forward svc/backend-svc -n se-demo 8002:8000`
3. Check the UI locally at `http://localhost:3002/admin`
4. Confirm the footer shows that we're connected to the Elevation cluster.

**For Local Development (Native Standard):**
If you are running the standard stack natively on your laptop (e.g., using `docker compose -p fla-standard up -d`):
1. The frontend typically runs on `http://localhost:3000` (via Caddy on `localhost:80`)
2. The backend typically runs on `http://localhost:8000`
3. Check the UI locally at `http://localhost/admin`
4. Confirm the footer shows that we're connected to the local laptop.

**For Local Development (High-Scale):**
We now have a dedicated local high-scale topology leveraging Docker Compose overrides to isolate ports and data.
1. Run this command on your laptop to start the local high-scale cluster:
   `docker compose -p fla-hs -f docker-compose.multipod.yml -f docker-compose.clickhouse-prototype.yml -f docker-compose.high-scale-local.yml up -d`
2. The high-scale frontend/proxy is mapped to `127.0.0.1:8081`.
3. Check the UI locally at `http://127.0.0.1:8081/admin`
4. We are using the dedicated test service `qI4D8yXXFYOIpZEMrkJy65` with the domain `fla-local-high-scale-test.global.ssl.fastly.net` for this environment.

*Reminder: Always confirm your port forwards are running and haven't dropped after triggering redeployments or container restarts.*

## Fastly Service ID Mappings

The project currently maintains 4 isolated deployments for testing, each fronted by its own dedicated Fastly service:

| Environment | Architecture | Fastly Service ID | Local / Direct URL | Fastly CDN Domain |
|---|---|---|---|---|
| **Local** | Standard | `ZU15BvY2LX7WcEp43T9VwU` | http://localhost/dashboard?service=ZU15BvY2LX7WcEp43T9VwU | `fla-local-standard-test.global.ssl.fastly.net` |
| **Local** | High-Scale | `qI4D8yXXFYOIpZEMrkJy65` | http://127.0.0.1:8081/dashboard?service=qI4D8yXXFYOIpZEMrkJy65 | `fla-local-high-scale-test.global.ssl.fastly.net` |
| **GCE** | Standard | `cVnu9mYB3Cvmob3lsqjQU3` | http://localhost:3001/dashboard?service=cVnu9mYB3Cvmob3lsqjQU3 | `fastly-se-demo.global.ssl.fastly.net` |
| **GCE** | Standard (Prod URL) | `cVnu9mYB3Cvmob3lsqjQU3` | https://fastly-log-analytics.global.ssl.fastly.net/dashboard?service=cVnu9mYB3Cvmob3lsqjQU3 | `fastly-se-demo.global.ssl.fastly.net` |
| **Elevation** | High-Scale | `ZEZ4mcAjoSFDTg7tpkDKV2` | http://localhost:3002/dashboard?service=ZEZ4mcAjoSFDTg7tpkDKV2 | `fla-k8s-scaling-test-elevation.global.ssl.fastly.net` |
| **Elevation** | High-Scale (Prod URL) | `ZEZ4mcAjoSFDTg7tpkDKV2` (Remote Frontend: `zgJQdMaqVleT2VDe2ELCEM`) | https://fla-elevation-analyst.global.ssl.fastly.net/dashboard?service=ZEZ4mcAjoSFDTg7tpkDKV2 | `fla-k8s-scaling-test-elevation.global.ssl.fastly.net` |

## Missing Architecture Gaps & Auto-Discovery

As we work through the finalization of "high-scale" (`release/v3.0.0-beta2`), our approach is to tackle issues one at a time, auto-discovering edge cases and bugs during testing, and continuously updating this document.

A few immediate architecture decisions/gaps to address:
- [x] **Disable Analyst Path A (Standalone Mode) for High-Scale**: Since the DuckLake cutover means the catalog is no longer FOS-resident, Analyst Path A (where analysts use FOS credentials to download files locally/standalone) is incompatible with high-scale. We will disable this flow entirely for high-scale services rather than building a workaround.
- [ ] **Broad System Robustness**: Investigate all aspects of ingestion and the serving tier to ensure absolute robustness. This includes auto-discovering and fixing any concurrency or pipeline recovery issues as they arise under load.

## Expanded Testing Plan & Harness

To properly validate both "standard" and "high-scale" architectures, we follow a strict **one item per AI coding session** workflow:

1.  **Strict Sequential Protocol (Crons First, Then Pages — One Item per AI Session)**:
    - **Single-Item Scope:** Each independent AI session must focus on **exactly ONE cron job or ONE page at a time**. Never combine multiple crons or pages into a single session.
    - **Order of Execution:**
      1. **Crons First:** Complete the background cron jobs one by one in order. Every page is a downstream consumer of data produced, compacted, and committed by the background tasks. Ensuring each cron is properly scheduled, invoked, locked, logged, and optimized prevents chasing phantom bugs on page loads.
      2. **Pages Second:** Once all background jobs are verified, validate each page and sub-page one by one in order.
    - **Cron Job Session Standard:** For each cron job session:
      - Confirm proper registration and schedule across all applicable environments (Standard APScheduler, High-Scale RedBeat/Celery, and Web Pod APScheduler).
      - Confirm invocation, mutual exclusion locking, error handling, and backpressure recovery.
      - Confirm 100% telemetry & query auditing: DuckDB `telemetry_queries`, ClickHouse `query_registry`, Postgres `ingest_ledger`, SQLite `ThreadLocalPool` wait times (`app.thread_wait_ms`), and FOS `usage_log.db` Class A/B accounting.
      - Confirm code efficiency and perform a library review: ensure custom code isn't reinventing wheels where a world-class, battle-tested library could reduce code size, improve reliability, and boost performance.
    - **Page Session Standard:** For each page session:
      - Use the corresponding specification file in `docs/pages/<page>.md` as the authoritative contract.
      - Test across applicable roles (Admin direct vs Analyst Path B remote share) and architectures (Standard vs High-Scale).
      - Execute the 10-step verification checklist at the bottom of the page spec using Playwright, curl, and network HAR analysis.
      - Inspect the Live Query Monitor (`/admin/queries`) and server logs to verify all queries have proper query runner attribution, use precomputed rollups where expected (e.g. 7d/30d), and avoid duplicate or unindexed scans.
      - Enforce performance budgets (< 300ms warm bundle, < 800ms TTI, CLS = 0.00).

2.  **Automated End-to-End (E2E) & Performance Harness**:
    - Build a best-in-class Playwright testing harness following industry best practices.
    - Automatically load each of the 28 top-level and sub-level pages (and their sub-tabs/modals) for all roles.
    - Capture network HAR files, API call durations, and page interactive timings (LCP, INP, fully loaded) during runs to calculate p95 metrics over repeated iterations.
    - Implement a synthetic log generator script capable of pushing sustained 2M RPS (with 5M bursts) to push limits.

3.  **Multi-Tier Deployment Automation & Port-Forward Healing (`scripts/dev/deploy_test_all.sh`)**:
    - Orchestrates concurrent, zero-downtime parallel deployments across all 4 environments (Local Standard, Local High-Scale, GCE VM Standard, Elevation K8s High-Scale).
    - Automatically heals, tests, and locks SSH tunnels (ports 3001/8001) and Kubernetes port-forwards (ports 3002/8002).
    - Executes headless Playwright commit-hash verification (`scripts/verify_dashboard.js`) asserting that every environment's footer and architecture badge match the deployed commit.
    - Seamlessly transitions into a 5-minute real-time audit stability watch (`--watch --duration 5 --interval 10`) post-deployment to ensure no scheduler lag, memory leaks, or cron failures occur after rollout.
    - **Harness hardening (2026-09-21)**: the Jenkins registry poll was serializing in front of ALL four deploys even though only Elevation reads that registry (GCE/local build from source); it now runs inside the Elevation subshell so it overlaps with the other three. `kubectl rollout status` calls now carry `--timeout=300s` so a bad image can't hang the script forever. `scale_harness.py` invocations now pass `--backend` explicitly — without it every checkpoint probe (freshness lag, OTel p95s) silently hit a nonexistent default port. `audit_environments.py`'s dashboard probe was sending a dead `range=24h` query param instead of the real `range_token` body field, so with no time bounds set it ran an unbounded full-table scan (no partition pruning) on every audit tick — fixed to bound it to 24h, and its 4-5 per-target HTTP probes are now fired concurrently instead of sequentially.

4.  **Multi-Environment Health, Tenancy & RBAC Verification (`scripts/check_environment_health.py`)**:
    - Validates connectivity, strict tenancy isolation (confirming each environment serves only its authorized Fastly service ID), and role-based access control (Admin direct vs Analyst remote share).
    - Seamlessly chains deep pipeline audits via `--audit` and `--watch` flags.

5.  **Multi-Environment Deep Pipeline & Resource Audit Tool (`scripts/dev/audit_environments.py`)**:
    - Continuously inspects and diagnoses all 4 environments across 7 critical dimensions:
      1. Host vitals (vCPU-normalized load, RAM % and available headroom, disk usage).
      2. Ingestion pipeline currency and FOS queue depth (distinguishing genuine IDLE vs active STREAMING vs STALLED states).
      3. Scheduler health and heartbeat lag (>30s alert threshold).
      4. Recent cron execution history and failure logs over the last 24h.
      5. Local Parquet bin-packing, hourly compaction, and daily/weekly tier consolidation.
      6. DuckDB connection pool p95/p99 checkout wait latencies.
      7. High-scale Celery worker concurrency, task queue depth, and ClickHouse ingestion status.
    - Supports point-in-time snapshots (`make audit`) and real-time live terminal monitoring (`make audit-watch`).

6.  **Comprehensive Ingestion & System Robustness**:
    - Investigate all aspects of the ingest pipelines (both standard and high-scale) under heavy load.
    - Ensure all fault-tolerance mechanisms, state recoveries, and data retention policies are fully robust and operate flawlessly without impacting the serving tier.

7.  **Role & Topology Validation**:
    - **Roles**: Test as Admin (`read_write`) vs Analyst Path B (live shared instance via `/share-login`). (Analyst Path A is disabled for high-scale).
    - **Provisioning**: Test the full Provision Wizard flow to ensure new high-scale services can be instantiated from scratch. *Note: We are authorized to repeatedly tear down the existing test service `ZEZ4mcAjoSFDTg7tpkDKV2` and its ancillary services on the Elevation cluster to start fresh and validate the full provisioning lifecycle.*
    - **VCL Deployments**: Confirm that deploying VCL updates from the UI correctly propagates to the Fastly edge.

## To-Dos

We will tackle these one at a time, strictly dedicating **only ONE cron job or ONE page per AI coding session**, auto-discovering issues and updating this list dynamically.

### Completed Milestones
- [x] Stabilize "standard" architecture (GCE) — fixed DuckDB connection pool saturation and stuck cron ingestion.
- [x] Tear down `ZEZ4mcAjoSFDTg7tpkDKV2` (and ancillary services) to test the full provisioning flow for "high-scale" architecture (Elevation cluster). Redeployed from scratch with all log fields including `cmcd` enabled.
- [x] Establish page specification architecture in [`docs/pages/`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/) and create master index [`docs/pages/README.md`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/README.md).
- [x] Create comprehensive functional & testing specification for [Dashboard (`/dashboard`)](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/dashboard.md).
- [x] Create comprehensive background automation & cron jobs specification in [`docs/cron/README.md`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/README.md) cataloging all 25 jobs across Standard and High-Scale architectures.
- [x] Author and scaffold all 26 analytics and admin page specifications in [`docs/pages/`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/) with standard template, role/architecture matrices, and pending verification TODOs.
- [x] Disable Analyst Path A for high-scale architectures.
- [x] Develop a best-in-class Playwright E2E performance testing harness (`tests/perf/harness.py`, `scripts/verify_dashboard.js`).
- [x] Build automated multi-tier parallel deployment & verification script (`scripts/dev/deploy_test_all.sh`) with automated port healing and Playwright commit-hash verification.
- [x] Build automated environment health, tenancy isolation, and RBAC verification suite (`scripts/check_environment_health.py`).
- [x] Build multi-environment system and pipeline audit monitoring tool (`scripts/dev/audit_environments.py`, `make audit`, `make audit-watch`).
- [x] Resolve High-Scale RUM discovery visibility and `last_sync_at` synchronization across frontend badges, SSR headers, and backend cron filtering.
- [x] Integrate mandatory 5-minute post-deployment stability watch into deployment orchestration flow.
- [x] Author and execute strict, multi-stage Playwright E2E positive-data verifications, validating that 24h charts populate, 5m ranges contain recent edge traffic, header ingestion times are live, and 30d header counts exactly match page query metrics.
- [x] Investigate and audit all background cron jobs and ingestion pipelines across both architectures to ensure zero warnings or errors.

### Phase 1: High-Throughput Synthetic Traffic Generation (Completed)
- [x] Develop and finalize synthetic log generator for 50k sustained / 100k RPS burst load testing (`scripts/load_test/generate_synthetic_traffic.py`).
- [x] Document deployment architectures and throughput sizing guidelines (Standard vs. High-Scale) in `README.md`.

### Phase 2: Background Tasks & Cron Jobs Audit (One Single Session per Cron)

> **Gotcha (verify before trusting `cron_runs`/telemetry queries):** the DB-persisted `task` string for a job is not always the same token used in its APScheduler job id or this list. Confirmed aliases in `backend/cron/schedule.py`'s `_TASK_MAP`: `sync_metadata` → `metadata_sync`, `expire` → `expire_snapshots`, `alerts_evaluation` → `alerts`, `rollup_heal` → `rollup_hour_heal`, `rollup_compact` → `rollup_compact_daily`. Querying `cron_runs`/`recent_cron_failures` by the job-id token instead of the DB task string will silently return zero rows, not an error.

- [x] Cron 1 documentation/design: `log_discovery_{id}` — Log Discovery, Download & Conversion ([docs/cron/jobs/log-discovery.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/log-discovery.md)); implementation and runtime verification remain deferred.
- [ ] Cron 2: `log_commit_{id}` — Parquet Buffer to DuckLake Catalog Commit ([docs/cron/jobs/commit.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/commit.md))
- [ ] Cron 3: `local_compact_{id}` — Local Hourly & Daily/Weekly Tier Compaction ([docs/cron/jobs/local-compact.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/local-compact.md))
- [ ] Cron 4: `partial_hour_merge_{id}` — Active Partial-Hour Ingest Merge ([docs/cron/jobs/partial-hour-merge.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/partial-hour-merge.md))
- [ ] Cron 5: `rollup_heal_{id}` — Top-N Rollup Backfill & Self-Healing ([docs/cron/jobs/rollup-heal.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rollup-heal.md))
- [ ] Cron 6: `rollup_compact_{id}` — Daily Top-N Rollup Consolidation ([docs/cron/jobs/rollup-compact.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rollup-compact.md))
- [ ] Cron 7: `optimize_{id}` — DuckLake Durability Flush & File Rewrite ([docs/cron/jobs/optimize.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/optimize.md))
- [ ] Cron 8: `expire_{id}` — Snapshot Expiry, Retention Pruning & Cache Purge ([docs/cron/jobs/expire.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/expire.md))
- [ ] Cron 9: `full_sync_{id}` — Periodic 6-Hour Ingestion Reconciliation Full Sweep ([docs/cron/jobs/full-sync.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/full-sync.md))
- [ ] Cron 10: `gap_heal_{id}` — Ingest Gap Discovery & Fast-Forward Sweep ([docs/cron/jobs/gap-heal.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/gap-heal.md))
- [ ] Cron 11: `metadata_cleanup_{id}` — Operational SQLite/Postgres Retention & Vacuum ([docs/cron/jobs/metadata-cleanup.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/metadata-cleanup.md))
- [ ] Cron 12: `alerts_evaluation_{id}` — Real-Time Alert Threshold Rule Evaluation ([docs/cron/jobs/alerts-evaluation.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/alerts-evaluation.md))
- [ ] Cron 13: `insights_prewarmer_{id}` — Background Anomaly Detection Insight Prewarming ([docs/cron/jobs/insights-prewarmer.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/insights-prewarmer.md))
- [ ] Cron 14: `sync_metadata_{id}` — Analyst Path A State Sync ([docs/cron/jobs/sync-metadata.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/sync-metadata.md))
- [ ] Cron 15: `ledger_sweep_{id}` — High-Scale Ingest Ledger Crash-Net Sweep ([docs/cron/jobs/ledger-sweep.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/ledger-sweep.md))
- [ ] Cron 16: `rum_discovery_{id}` — RUM Beacon Discovery & Staging (Standard and High-Scale execution variants) ([docs/cron/jobs/rum-sync.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rum-sync.md), [high-scale variant](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rum-discovery.md))
- [ ] Cron 17: `rum_commit_{id}` — Standard Mode RUM Beacon DuckLake Commit ([docs/cron/jobs/rum-commit.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rum-commit.md))
- [ ] Cron 18: `ledger_rum_sweep_{id}` — High-Scale RUM Ledger Crash-Net Sweep ([docs/cron/jobs/ledger-rum-sweep.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/ledger-rum-sweep.md))
- [ ] Cron 19: `metric_snapshot` — Global System Vitals & Host Metrics Snapshot ([docs/cron/jobs/metric-snapshot.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/metric-snapshot.md))
- [ ] Cron 20: `rdns_enrichment` — Client IP Reverse DNS Background Enrichment ([docs/cron/jobs/rdns-enrichment.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/rdns-enrichment.md))
- [ ] Cron 21: `bot_data_refresh` — NGWAF & Verified Bot Seed Refresh ([docs/cron/jobs/bot-data-refresh.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/bot-data-refresh.md))
- [ ] Cron 22: `ngwaf_sync_{id}` — NGWAF Security Workspace Feed Ingestion ([docs/cron/jobs/ngwaf-sync.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/ngwaf-sync.md))
- [ ] Cron 23: `share_audit_purge` — Remote Share Audit Trail Retention Purge ([docs/cron/jobs/share-audit-purge.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/share-audit-purge.md))
- [ ] Cron 24: `duckdb_recycle` — DuckDB Native Memory Pool Recycling & Heap Trim ([docs/cron/jobs/duckdb-recycle.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/cron/jobs/duckdb-recycle.md))

#### Current Work Item: Phase 2, Cron 1 — `log_discovery_{service_id}` documentation/design complete; implementation and runtime verification are the next session

The current session is in requirements clarification and documentation only; implementation
and end-to-end verification are intentionally deferred to a new session. The agreed
quarantine behavior is:

- Request logs and RUM remain separate ingestion cron families, with shared transport,
  telemetry, and cost-attribution primitives.
- Valid rows continue ingesting when individual lines are malformed. Each malformed line
  is retained as exact original bytes, with source object, line ordinal, byte offset when
  known, parser error, and size metadata.
- A corrupt gzip container is retained as one complete original gzip evidence item because
  no line-level decode is trustworthy.
- Evidence is local-only under `data/services/{service_id}/quarantine/`, outside static
  web roots. Each malformed line or corrupt gzip is one item with its own collision-safe
  evidence file. Metadata includes source type (request/RUM), source FOS key, line ordinal,
  byte offset/length when known, normalized error category, bounded error text, and
  SHA-256 for integrity verification. Source key plus line ordinal/byte range remains the
  primary identity.
- The FOS source object is always deleted after processing, including when local evidence
  capture fails. Capture is best effort: the system records as much error information as
  possible, marks the run `error`, and continues safely. Every source line must receive a
  durable success or failure outcome; no source is silently acknowledged.
- Quarantine is diagnostic evidence, not a re-ingest queue. There is one 1,000-item cap
  per service shared by malformed-line and corrupt-gzip items, with no age-based expiry
  and no configurable override. When new items exceed the cap, oldest individual items
  are evicted immediately; eviction failures are recorded and do not block other eligible
  evictions.
- High-Scale workers capture directly into shared quarantine storage/database with bounded
  retries for transient storage failures. The serving pod owns the admin surface and cap
  enforcement; workers do not run duplicate maintenance. Read-only Analyst Path A
  instances do not write or maintain quarantine evidence.
- Quarantine access is backend-enforced admin/read-write only for both analyst paths. The
  admin UI treats each line as an individual item, supports pagination, source-type and
  error-category filters, readable previews by default, authenticated exact-byte streaming
  downloads with size limits, individual purge, and service-scoped purge-all.
- Existing legacy FOS-backed quarantine records are out of scope; only newly quarantined
  items use this design. Existing metadata maintenance may run a bounded orphan-repair
  sweep, removing stale metadata references while preserving and reporting unexpected
  evidence files.

### Phase 3: Analytics & Admin Pages Audit (One Single Session per Page)
- [ ] Page 1: Dashboard (`/dashboard`) ([docs/pages/dashboard.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/dashboard.md))
- [ ] Page 2: Control Room (`/control-room`) ([docs/pages/control-room.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/control-room.md))
- [ ] Page 3: Service Summary / Value (`/fastly-value`) ([docs/pages/fastly-value.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/fastly-value.md))
- [ ] Page 4: Performance (`/performance`) ([docs/pages/performance.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/performance.md))
- [ ] Page 5: Origin Health (`/origin`) ([docs/pages/origin.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/origin.md))
- [ ] Page 6: Security (`/security`) ([docs/pages/security.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/security.md))
- [ ] Page 7: Insights (`/insights`) ([docs/pages/insights.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/insights.md))
- [ ] Page 8: Network Path (`/network`) ([docs/pages/network.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/network.md))
- [ ] Page 9: Streaming (`/streaming`) ([docs/pages/streaming.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/streaming.md))
- [ ] Page 10: RUM (`/rum`) ([docs/pages/rum.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/rum.md))
- [ ] Page 11: Sessions (`/sessions`) ([docs/pages/sessions.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/sessions.md))
- [ ] Page 12: Sessions Stream (`/sessions/stream`) ([docs/pages/sessions-stream.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/sessions-stream.md))
- [ ] Page 13: Usage & Cost (`/usage`) ([docs/pages/usage-and-cost.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/usage-and-cost.md))
- [ ] Page 14: SQL Query Editor (`/query`) ([docs/pages/query.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/query.md))
- [ ] Page 15: Alerts (`/alerts`) ([docs/pages/alerts.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/alerts.md))
- [ ] Page 16: Raw Logs Viewer (`/logs`) ([docs/pages/logs.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/logs.md))
- [ ] Page 17: Assets & Shield (`/assets-shield`) ([docs/pages/assets-shield.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/assets-shield.md))
- [ ] Page 18: High-Scale Request Facts (`/high-scale/request-facts`) ([docs/pages/high-scale-request-facts.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/high-scale-request-facts.md))
- [ ] Page 19: Analyst Share Login (`/share-login`) ([docs/pages/share-login.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/share-login.md))
- [ ] Page 20: Admin Overview (`/admin`) ([docs/pages/admin/overview.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/overview.md))
- [ ] Page 21: Live Query Monitor (`/admin/queries`) ([docs/pages/admin/queries.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/queries.md))
- [ ] Page 22: Task Queue (`/admin/queue`) ([docs/pages/admin/queue.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/queue.md))
- [ ] Page 23: RUM Beacon Settings (`/admin/rum`) ([docs/pages/admin/rum.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/rum.md))
- [ ] Page 24: Session Scoring Admin (`/admin/session-scoring`) ([docs/pages/admin/session-scoring.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/session-scoring.md))
- [ ] Page 25: Live Share Management (`/admin/share`) ([docs/pages/admin/share.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/share.md))
- [ ] Page 26: System Metric Trends (`/admin/trends`) ([docs/pages/admin/trends.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/trends.md))
- [ ] Page 27: FOS Usage Ledger (`/admin/usage-log`) ([docs/pages/admin/usage-log.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/usage-log.md))
- [ ] Page 28: ClickHouse Cluster Admin (`/admin/clickhouse`) ([docs/pages/admin/clickhouse.md](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/admin/clickhouse.md)) — **BUILD, not just verify**: confirmed the backend API exists (`backend/routers/admin/clickhouse.py`, `/api/admin/clickhouse/status` + `/replay`) but there is no `frontend/app/admin/clickhouse` route yet. This session needs to build the page per the spec before it can run the verification checklist.

### Phase 4: Full System Load & Stress Testing
- [ ] Execute baseline performance tests on "standard" architecture (GCE) under target load.
- [ ] Execute baseline performance tests on "high-scale" architecture (Elevation) under 2M sustained / 5M burst load.
- [ ] Validate end-to-end system robustness under sustained heavy load.

## First Steps

Keep this document updated as we go and track it in git. We'll delete it and squash the branch at the end when we're done and record everything we accomplish in the docs and ADRs.
