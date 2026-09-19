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

## 3. Template for New Page Specifications

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
