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

Many of these pages feature sub-tabs (e.g. Insights has 5 category tabs, Admin has 8 sub-sections, Origin has shield vs backend tabs), modals with deep drill-down links (e.g. session scoring inspector, streaming log inspector), and filter propagation to `/logs` and `/dashboard`.

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
8. **Comprehensive Observability & Query/API Call Auditing:** We built extensive telemetry and logging into the app (`X-Page-Load-ID`, `RequestTelemetry`, `telemetry_queries`, DuckDB/SQLite profilers, and the interactive Debug Panel) to audit everything that happens on a page load:
   - **Complete Instrumentation:** Verify that 100% of the queries (DuckDB analytical queries, SQLite metadata lookups), external Fastly/FOS API calls, and section timings involved in generating each page are captured and attributed. No "dark" or unmeasured operations may execute.
   - **Audit Queries and Calls on Each Page Load:** Examine every captured query and API call to confirm they are **efficient** (proper partition pruning, index utilization, zero duplicate/redundant queries, no N+1 query loops, and appropriate rollup table usage over raw full scans) and **proper** (strict tenancy filtering, parameterized SQL, valid caller attribution, and execution durations within the p95 latency budget).

In addition to confirming that all functionality works, we also need to confirm that we're loading all data and pages in an optimal way. Log all queries that were involved in the page loads along with all API calls. Examine all of the queries and API calls to confirm they are appropriate and optimized for performance and cost. Also look for any queries or API we did not include in our logging properly and confirm they are included. We also need to confirm the user experience is optimal. Therefore, you should look at how quickly the page becomes interactive, how quickly all data loads in and how fast the page becomes fully interactive. Use real browser interactions and things like HAR files to analyze what is happening, and do it over repeated iterations to look at averages as well as things like p95 performance.

To repeat, all functionality, pages, and interactions need to be tested for both roles and both architectures and while under expected load.

## What you are allowed to do

You have full control over the v3.0.0-beta1 branch as well as the GCE machine, my local laptop and Elevation dev-usc1 cluster for compiling and testing everything. You are free to build and deploy to those systems as needed, and if you need image tags from me for Elevation deploys please ask.

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
4. We are using the dedicated test service `qI4D8yXXFYOIpZEMrkJy65` with the domain `fla-local-hs-test.demo.fastly.com` for this environment.

*Reminder: Always confirm your port forwards are running and haven't dropped after triggering redeployments or container restarts.*

## Fastly Service ID Mappings

The project currently maintains 4 isolated deployments for testing, each fronted by its own dedicated Fastly service:

| Environment | Architecture | Fastly Service ID | Notes |
|---|---|---|---|
| **Local** | Standard | `ZU15BvY2LX7WcEp43T9VwU` | Uses `fla-local-standard-test.global.ssl.fastly.net` |
| **Local** | High-Scale | `qI4D8yXXFYOIpZEMrkJy65` | Isolated via Docker Compose overrides on port 8081 |
| **GCE** | Standard | `cVnu9mYB3Cvmob3lsqjQU3` | Long-lived test environment with active traffic |
| **Elevation** | High-Scale | `ZEZ4mcAjoSFDTg7tpkDKV2` | Kubernetes cluster environment for high load |

## Missing Architecture Gaps & Auto-Discovery

As we work through the finalization of "high-scale" (v3.0.0-beta1), our approach is to tackle issues one at a time, auto-discovering edge cases and bugs during testing, and continuously updating this document.

A few immediate architecture decisions/gaps to address:
- [ ] **Disable Analyst Path A (Standalone Mode) for High-Scale**: Since the DuckLake cutover means the catalog is no longer FOS-resident, Analyst Path A (where analysts use FOS credentials to download files locally/standalone) is incompatible with high-scale. We will disable this flow entirely for high-scale services rather than building a workaround.
- [ ] **Broad System Robustness**: Investigate all aspects of ingestion and the serving tier to ensure absolute robustness. This includes auto-discovering and fixing any concurrency or pipeline recovery issues as they arise under load.

## Expanded Testing Plan & Harness

To properly validate both "standard" and "high-scale" architectures, we need a robust testing harness driven by explicit per-page functional specifications in [`docs/pages/`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/):

1.  **Multi-AI Testing Session Protocol (Page-by-Page Focus)**:
    - **Session Scope:** Each independent AI session will focus on exactly one page (or cohesive sub-domain), using the corresponding spec file in `docs/pages/<page>.md` as its contract.
    - **Verification Matrix:** The session tests the page across all 4 environments (Local Standard, Local High-Scale, GCE VM Standard, Elevation K8s High-Scale) and applicable roles (Admin direct vs Analyst Path B remote share).
    - **Automated Validation:** Follow the 10-step verification checklist at the bottom of the page's spec document using Playwright, curl, and network HAR analysis.
    - **Telemetry & Query Attribution:** Inspect the Live Query Monitor (`/admin/queries`) and server logs to verify all queries fired during the page load have proper query runner attribution, use precomputed rollups where expected (e.g. 7d/30d), and do not execute duplicate or unindexed queries.
    - **Performance Budget Enforcement:** Verify p95 load times meet the targets defined in the page spec (< 300ms warm bundle, < 800ms TTI, CLS = 0).
    - **Issue Remediation & Bug Tracking:** Any auto-discovered bugs, race conditions, or layout shifts are fixed, verified, and documented directly in the page spec and PR commit.

2.  **Automated End-to-End (E2E) & Performance Harness**:
    - Build a best-in-class Playwright testing harness following industry best practices.
    - Automatically load each of the 27 top-level and sub-level pages (and their sub-tabs/modals) for all roles.
    - Capture network HAR files, API call durations, and page interactive timings (LCP, INP, fully loaded) during runs to calculate p95 metrics over repeated iterations.
    - Implement a synthetic log generator script capable of pushing sustained 2M RPS (with 5M bursts) to push limits.

3.  **Comprehensive Ingestion & System Robustness**:
    - Investigate all aspects of the ingest pipelines (both standard and high-scale) under heavy load.
    - Ensure all fault-tolerance mechanisms, state recoveries, and data retention policies are fully robust and operate flawlessly without impacting the serving tier.

4.  **Role & Topology Validation**:
    - **Roles**: Test as Admin (`read_write`) vs Analyst Path B (live shared instance via `/share-login`). (Analyst Path A is disabled for high-scale).
    - **Provisioning**: Test the full Provision Wizard flow to ensure new high-scale services can be instantiated from scratch. *Note: We are authorized to repeatedly tear down the existing test service `ZEZ4mcAjoSFDTg7tpkDKV2` and its ancillary services on the Elevation cluster to start fresh and validate the full provisioning lifecycle.*
    - **VCL Deployments**: Confirm that deploying VCL updates from the UI correctly propagates to the Fastly edge.

## To-Dos

We will tackle these one at a time, auto-discovering issues and updating this list dynamically.

- [x] Stabilize "standard" architecture (GCE) — fixed DuckDB connection pool saturation and stuck cron ingestion.
- [x] Tear down `ZEZ4mcAjoSFDTg7tpkDKV2` (and ancillary services) to test the full provisioning flow for "high-scale" architecture (Elevation cluster). Redeployed from scratch with all log fields including `cmcd` enabled.
- [x] Establish page specification architecture in [`docs/pages/`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/) and create master index [`docs/pages/README.md`](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/README.md).
- [x] Create comprehensive functional & testing specification for [Dashboard (`/dashboard`)](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/pages/dashboard.md).
- [ ] Author remaining page specifications across the 26 analytics and admin pages in `docs/pages/`.
- [ ] Disable Analyst Path A for high-scale architectures.
- [ ] Develop a best-in-class Playwright E2E performance testing harness.
- [ ] Develop synthetic log generator for 5M RPS load testing.
- [ ] Execute baseline performance tests on the "standard" architecture (GCE).
- [ ] Execute baseline performance tests on the "high-scale" architecture.
- [ ] Validate all 27 pages, sub-pages, filters, and modals under both architectures via dedicated AI test sessions.
- [ ] Investigate and validate all aspects of ingestion, cron jobs, and general system robustness under expected load.

## First Steps

Keep this document updated as we go and track it in git. We'll delete it and squash the branch at the end when we're done and record everything we accomplish in the docs and ADRs.
