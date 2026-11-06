# Fastly Log Analytics

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Version: 3.0.0-rc3](https://img.shields.io/badge/version-3.0.0--rc3-blue.svg)

A self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to [Fastly Object Storage](https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/).

Fastly's [historical stats](https://www.fastly.com/documentation/reference/api/metrics-stats/historical-stats/) give you aggregates. When you need to drill into individual requests — by IP, URL, status, WAF signal, or any field you log — Fastly's [real-time log streaming](https://www.fastly.com/documentation/guides/integrations/streaming-logs/about-fastlys-realtime-log-streaming-features/) makes the raw data available, but you still need somewhere to put it and something to query it with. This project fills that gap using only Fastly products. Costs are limited to Fastly Object Storage [class operations](https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/#using-the-s3-compatible-api) and [storage](https://docs.fastly.com/products/object-storage#billing) — no third-party logging vendor required.

> **Coming from v2?** There is no upgrade path. Tear down your services on v2, install v3 fresh, and re-provision (see [docs/MIGRATION_v3.0.0_GUIDE.md](docs/MIGRATION_v3.0.0_GUIDE.md)). This is a release candidate (3.0.0-rc3).

### 📺 Watch the install walkthrough

A short video walks through installation and provisioning end to end:

[![Watch the Fastly Log Analytics install walkthrough](https://img.youtube.com/vi/7-3XWzesuAY/maxresdefault.jpg)](https://youtu.be/7-3XWzesuAY)

---

## Before You Start

You'll need:

- A **Fastly account** with permission to create [services](https://www.fastly.com/documentation/guides/getting-started/services/about-services/) and Object Storage buckets
- **Object Storage enabled** on the account — it's a separately activated product, not on by default
- At least one **VCL service** to stream logs from
- **Docker** (recommended) — or Python 3.12+, Node.js 24+, **ClickHouse**, and a **PostgreSQL 16** server for a manual install
- *Optional:* a Fastly API token with the **Billing** permission to power the [Usage & Cost page](docs/features.md#usage--cost-page)
- *Optional:* [`falco`](https://github.com/ysugimoto/falco) to validate VCL during provisioning (highly recommended; the app degrades gracefully without it)
- *Optional:* **Rust 1.90+** with the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`) — only needed if you plan to rebuild the [Session Scoring](docs/session_scoring_runbook.md) Compute Wasm scorer from source

---

## Server Specifications

Fastly Log Analytics uses a single unified architecture — **ClickHouse + PostgreSQL 16 + stateless ingest worker (`python -m backend.high_scale.worker`) + Fastly Object Storage (FOS)** — that scales from a laptop Docker Compose stack up to multi-node Kubernetes clusters:

- **Laptop / Local Evaluation (Docker Compose):**
  - Runs comfortably in **~2–4 GiB RAM** with a single stateless worker configured with lightweight defaults (`HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`, `2G` worker memory ceiling, `2G`–`4G` ClickHouse memory limit).
- **Shared Server / Production VM:**
  - **RAM:** 16 GB recommended
  - **CPU:** 4 vCPUs recommended (2 vCPUs minimum)
  - **Disk:** 40 GB minimum — the Next.js build needs ~1 GB and ClickHouse tables grow with retained log volume while durable canonical Parquet archives live in Fastly Object Storage
  - **Swap:** 4 GB minimum, 8 GB preferred
  - **Open file descriptors:** set the host (and Docker) `nofile` limit to at least 65536. The production overlay (`docker-compose.prod.yml`) sets this automatically.

---

## Architecture & Throughput Sizing

A single unified architecture powers every deployment topology:

| Deployment Topology | Intended Throughput (requests/sec at your VCL service, 100% logged) | Analytical & Control Plane | Ingest Topology | Best For |
|---|---|---|---|---|
| **Cloud Serverless Mode** | **Serverless zero-origin analytics (1 monitored service per install)** *(32 backend calls / 1000 ms vCPU per tick)* | Fastly Compute (`flp` + `flp-worker` Rust/Wasm) + VCL (`flp-front`) + Fastly Object Storage (FOS Parquet) + Browser DuckDB-Wasm (`frontend/lib/cloud-engine/`) — no ClickHouse or PostgreSQL 16 required | Autonomous VCL `/_tick` probe waking Fastly Compute coordinator + worker fan-out writing immutable FOS Parquet windows & rollups | Zero-infrastructure serverless analytics running 100% on Fastly Compute, FOS, and Browser DuckDB-Wasm ([`compute/cloud/`](compute/cloud/) · [`docs/cloud/recommendation.md`](docs/cloud/recommendation.md)) |
| **Single-Host Docker Compose** | **Up to ~2,175 sustained RPS per worker** *(absorbs larger bursts via Fastly Object Storage)* | ClickHouse + PostgreSQL 16 + FOS Archive Parquet | 1 stateless worker (`HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`) | Local development on laptops (~2–4 GiB RAM), single-VM production deployments, and moderate services |
| **Multi-Pod Kubernetes (Helm)** | **~13,000–28,000+ sustained ingest RPS on 6–12 worker pods** *(absorbs bursts to 1,000,000 RPS via Fastly Object Storage — see [measured capacity](#measured-capacity))* | ClickHouse + PostgreSQL 16 + FOS Archive Parquet | Horizontally scaled stateless workers (`python -m backend.high_scale.worker`) with fenced partition cursors | Kubernetes clusters, multi-container deployments, and high-volume enterprise services |

> [!NOTE]
> **Requests per second at your VCL service:**
> Sizing figures are the request rate at your Fastly VCL service when every request is logged (one log line per request, streamed to Fastly Object Storage). Fastly services handling tens or hundreds of thousands of requests per second can also reduce ingest volume by leveraging Fastly VCL log conditions:
> - **Sampling:** Stream 1-in-10 (`randombool(1, 10)`) or 1-in-50 requests for statistical sampling of high-volume traffic.
> - **Conditional Logging:** Log only requests meeting specific criteria (e.g., HTTP errors `resp.status >= 400`, backend origin fetches, requests with origin latency above a threshold, or security/WAF events).
>
> As performance optimizations continue to evolve, these throughput guidelines will be updated with each release.

### Measured capacity

Tested on a Kubernetes deployment running a single ClickHouse replica (6 vCPU / 8 GB RAM), PostgreSQL 16 metadata and control plane, one backend API replica, and stateless ingest workers (`python -m backend.high_scale.worker`) with all background crons enabled. Using a synthetic traffic generator and step-up harness, we stepped load from 500 RPS to 1,000,000 RPS across 34 stages without clearing data between stages, growing the live `request_facts` table to **101.1 million rows**. Every stage ingested **100% of expected rows** with zero data loss.

#### 6 worker pods (`500` → `30,000` RPS across `10 s`, `60 s`, and `300 s` periods)

Each worker pod sustains **~2,175 rows/s** end-to-end (Fastly Object Storage download, gzip + JSON decode, canonical Parquet archive checkpoint + FOS upload, and 50,000-row micro-batched ClickHouse insert), giving a 6-pod cluster a steady-state ingest ceiling of **~13,050 rows/s**. When edge traffic exceeds the steady-state worker drain rate during a burst, Fastly Object Storage buffers the `.log.gz` objects cleanly until the workers catch up.

| Log rate | `10 s` period (`lag_p50` / `lag_p95`) | `60 s` period (`lag_p50` / `lag_p95`) | `300 s` period (`lag_p50` / `lag_p95`) | Warm page load p50 (Dash / Origin / Perf / Net / Sec) | Cold 24 h page load p50 (Dash / Origin / Perf) |
|---|---|---|---|---|---|
| 500–2,000 RPS | 3.8–4.5 s / 4.9–6.4 s ✅ | 7.5–19.1 s / 18.2–20.9 s ✅ | 18.2–22.0 s / 58.9–67.0 s ✅ | 9–35 ms | 560–870 ms / 240–350 ms / 610–935 ms |
| 5,000 RPS | 5.3 s / 9.9 s ✅ | 18.2 s / 24.8 s ✅ | 23.8 s / 92.2 s ✅ | 9–29 ms | 560–675 ms / 225–265 ms / 705–720 ms |
| 10,000 RPS | 8.5 s / 20.9 s ✅ | 21.0 s / 75.7 s ✅ | 29.6 s / 186.3 s ✅ | 10–70 ms | 645–1,115 ms / 225–335 ms / 750–1,045 ms |
| 15,000 RPS | 14.7 s / 27.9 s ✅ | 27.6 s / 75.7 s ✅ | 89.9 s / 343.7 s ✅ | 10–935 ms | 1,255–1,815 ms / 330–355 ms / 850–1,455 ms |
| 20,000 RPS | 20.5 s / 46.8 s ✅ | 55.3 s / 102.4 s ✅ | 114.2 s / 402.6 s ✅ | 10–1,125 ms | 1,230–3,480 ms / 320–620 ms / 1,085–2,545 ms |
| 25,000 RPS | 29.8 s / 65.0 s (drains backlog) | 62.2 s / 125.7 s ✅ | 222.2 s / 505.6 s ✅ | 11–2,635 ms | 855–5,320 ms / 275–580 ms / 900–1,985 ms |
| 30,000 RPS | 40.8 s / 78.2 s (drains backlog) | 83.7 s / 149.1 s ✅ | 277.2 s / 615.0 s (drains backlog) | 11–1,755 ms | 1,220–6,980 ms / 355–600 ms / 1,385–2,235 ms |

#### 12 worker pods / 24 worker threads (`50,000` → `1,000,000` RPS bursts, up to `101.1M` rows)

Scaling the ingest deployment to 12 pods (`HIGH_SCALE_WORKER_THREADS=2`, 24 concurrent worker threads) raises sustained cluster ingest throughput to **27,500–28,250 rows/s** and lets the cluster absorb multi-million-row edge bursts up to **1,000,000 RPS** without dropping a single log line:

| Period | Target RPS | Rows Ingested | Peak Window Ingest Rate | `lag_p50` / `lag_p95` | `lag_ok` | Notes |
|---|---|---|---|---|---|---|
| `10 s` | 50,000 RPS | 1,000,000 / 1,000,000 | 9,804 rows/s | 10.3 s / 21.6 s | ✅ | 1M rows drained in 25 s |
| `10 s` | 100,000 RPS | 2,000,000 / 2,000,000 | 13,158 rows/s | 21.1 s / 38.2 s | ✅ | 2M rows drained in 50 s; full Dashboard 1.07 s, Origin 0.73 s |
| `10 s` | 250,000 RPS | 2,500,000 / 2,500,000 | 17,361 rows/s | 26.2 s / 48.3 s | ✅ | 2.5M rows drained in 50 s |
| `10 s` | 500,000 RPS | 5,000,000 / 5,000,000 | **28,249 rows/s** | 13.0 s / 20.1 s | ✅ | 5M rows drained in 65 s; cold Dashboard 1.32 s, Origin 0.27 s |
| `10 s` | **1,000,000 RPS** | **10,000,000 / 10,000,000** | 13,175 rows/s | 117.6 s / 256.4 s | Drains in ~6 m | 10M rows in 10 s; Dashboard 1.48 s warm / 3.03 s cold, Origin 0.28 s warm / 0.36 s cold |
| `60 s` | 50,000 RPS | 3,000,000 / 3,000,000 | 18,868 rows/s | 27.1 s / 62.7 s | ✅ | 3M rows drained in 60 s |
| `60 s` | 100,000 RPS | 6,000,000 / 6,000,000 | **27,523 rows/s** | 30.2 s / 138.3 s | ✅ | 6M rows drained in 85 s; full Dashboard 2.23 s, Origin 0.40 s |

#### Page load latencies at `102.5M` rows in ClickHouse

Measured against the **102,484,916-row** ClickHouse dataset on a single 6 vCPU / 8 GB ClickHouse replica:

- **Warm (30 s response cache hit):** **13–21 ms** for Dashboard (`57 ms` SSR HTML), Origin, Performance, Security (`42 ms` full waterfall), and Sessions (`62 ms`); **25 ms** (`170 ms` full 5-endpoint waterfall) for Network.
- **Cold (uncached arbitrary 24 h window across 102.5M rows):** **219–407 ms** for Origin, **261–449 ms** for Dashboard, **490–491 ms** for Performance, **882–1,052 ms** for Sessions, **1,603–1,617 ms** for Security, and **2,178–2,358 ms** for Network (all 6 full-page waterfalls complete in **under 2.4 seconds**).
- **Scaling out:** Ingest throughput scales linearly with worker replicas (~2,175–2,350 rows/s per worker pod). When scaling worker pods, keep `METADATA_PG_POOL_MAX=64` (the default) on the `backend` API deployment so its 40 FastAPI worker threads and APScheduler crons never wait on a metadata connection, and set `METADATA_PG_POOL_MAX=6` on stateless worker pods so horizontal worker scaling stays well inside PostgreSQL's `max_connections`.

### Storage sizing

ClickHouse stores compressed MergeTree parts on disk for low-latency serving while Fastly Object Storage holds the canonical Archive Parquet batches (`s3://{bucket}/{prefix}/archive/`), and PostgreSQL 16 holds operational metadata and ingest control state. On synthetic traffic, compressed storage is roughly 40 bytes/row; **plan for 50–150 bytes/row** on real traffic (long URLs, user agents, and high IP cardinality compress less).

| Log rate (100% logged) | Rows/day | Compressed storage per day | 7 days retained |
|---|---|---|---|
| 500 RPS | 43M | 2–6 GB | 15–45 GB |
| 1,000 RPS | 86M | 4–13 GB | 30–90 GB |
| 2,000 RPS | 173M | 9–26 GB | 60–180 GB |
| 5,000 RPS | 432M | 22–65 GB | 150–450 GB |
| 10,000 RPS | 864M | 43–130 GB | 300–900 GB |

Allow ~20% headroom on the ClickHouse data volume for background MergeTree merges. Put the ClickHouse data volume on its own disk where possible so large merges cannot starve PostgreSQL or the OS.

---

## Quick Start

> 📺 Prefer to watch? See the [video walkthrough](https://youtu.be/7-3XWzesuAY).

```bash
docker compose up --build
```

> Requires Docker Compose v2 (the `docker compose` subcommand). The standalone `docker-compose` v1 binary reached end-of-life in 2023 and isn't shipped with current Docker installs.

This local stack starts PostgreSQL 16, ClickHouse, the stateless ingest worker (`python -m backend.high_scale.worker`), the FastAPI backend, the Next.js frontend, and Caddy. Production single-host deployments use `docker-compose.yml` plus the `docker-compose.prod.yml` overlay rather than a plain `docker compose up` (see [docs/deploy/](docs/deploy/README.md)); Kubernetes deployments use the Helm chart in [`deploy/chart/`](deploy/chart/).

Open **http://localhost** and follow the provisioning wizard. The wizard creates your Object Storage bucket, access keys, a CDN-fronting service, and the logging endpoint on the VCL service you select.

> The stack is fronted by Caddy on port **80** (a single ingress, mirroring production). A friendlier alias **http://fastly.localhost** works with no setup — browsers resolve `*.localhost` to `127.0.0.1`. For a custom name like **http://fastly.analytics**, add `127.0.0.1 fastly.analytics` to your hosts file.

For manual install (no Docker), see [Manual Installation](#manual-installation). The non-Docker [`run.sh`](run.sh) path serves the app directly on **http://localhost:3000**.

---

## How It Works

The **admin** runs the provisioning wizard to set up a Fastly Object Storage bucket, deploy a CDN-fronting service for cheap log reads, and attach a structured JSON logging endpoint to a chosen VCL service. Once that's in place, the stateless ingest worker continuously discovers new `.gz` / `.zst` log files from the bucket, checkpoints canonical Parquet archive batches to Fastly Object Storage, and publishes verified batches into ClickHouse.

![Provisioning Process](docs/assets/provisioning.png)

![ETL Process](docs/assets/etl.png)

Once ingestion is active, you can collaborate with teammates by sharing the live dashboard:

![Sharing Process](docs/assets/sharing.png)

---

### Live Shared Server (Web-Accessible Host)

You run the application as a central web-accessible server on a dedicated VM (or a laptop reachable at its own hostname / IP). Your associates connect using a standard web browser and sign in with a passcode — or, if you configure an OAuth/OIDC identity provider, with single sign-on.

#### How it works:
1. **Admin:** Click **Share Dashboard** in your dashboard. The sharing manager prompts for your server's public URL — a custom domain or IP that the analyst can reach over HTTPS.
2. **Admin:** Mint an analyst invitation in the sharing manager by specifying their name, an optional IP allowlist, and either a passcode or (with an OAuth/OIDC provider configured) their email for single sign-on. Invites are single-seat by default; a per-invite toggle allows shared logins. Give them the public URL (and passcode, if used).
3. **Analyst:** Open the shared link in a standard browser, accept the Terms of Service, enter the passcode or sign in with your identity provider, and view the live read-only dashboard. All database queries are executed securely on your host server. You can revoke access or **Sever All Access** instantly.

---

## Features

- **Unified ClickHouse + FOS Archive storage** — sub-second analytical queries powered by ClickHouse MergeTree tables and projections, backed by canonical Archive Parquet in Fastly Object Storage and a PostgreSQL 16 operational and ingest control plane
- **Automated provisioning** — wizard creates the bucket, access keys, CDN-fronting service, and logging endpoint (`token → service → storage → ngwaf → fields → execute`)
- **CDN-accelerated reads** — every FOS read goes through a Fastly service to minimize egress and maximize caching
- **Crash-safe batched ingestion** — page-batched ingestion writes verified archive Parquet to FOS and publishes atomically to ClickHouse through a `pending` → `visible` publication gate; interrupted imports never expose partial batches
- **Retention and storage maintenance** — configurable request/RUM retention, ClickHouse partition and FOS archive retention cleanup, and operational metadata pruning; see the [`expire` job specification](docs/cron/jobs/expire.md) and [`metadata_cleanup` job specification](docs/cron/jobs/metadata-cleanup.md)
- **Schema evolution** — new and missing JSON fields handled gracefully; corrupt lines isolated and surfaced
- **Log sampling** — optionally log a random percentage of requests to manage cost on high-traffic services
- **Multi-source support** — analyze logs from multiple services side by side
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations, raw log viewer with click-to-filter
- **Insights** — automated anomaly detection, 51 detections organized into traffic, security, network, edge, and origin tabs with severity indicators (error spikes, regional surges, botnet IP-spread, content-discovery scans, session harvesting, scripted-traffic cadence, WAF signal changes, cache regressions, latency, and more)
- **Usage & Cost** — live storage breakdown, FOS operation counts, period totals, interactive cost estimator
- **Log field configuration** — built-in field groups (HTTP, network, geo, TLS, NGWAF) plus custom VCL expressions
- **Alerts** — threshold-based, webhook-delivered
- **Live dashboard sharing** — direct-mode via your own hostname or IP, with per-analyst invites (passcode or OAuth/OIDC single sign-on), IP allowlisting, optional client-IP anonymization, and instant revoke
- **Control room** — real-time operational dashboard with live metrics from rt.fastly.com at 1-second cadence across six tabs (summary, CDN & caching, security, performance, network, admin), with rolling bar charts, KPI cards, PoP traffic heatmap, contextual help, and drill-down time windows
- **Service summary** — executive view of the value Fastly delivers: cache hit ratio, offload, WAF blocks, bot mitigation, TLS adoption, latency percentiles, and Image Optimizer savings in a single tabbed page
- **Streaming analytics** — CMCD (Common Media Client Data) dashboard for services with v1 or v2 streaming-log fields enabled, showing buffer health, bitrate, throughput, concurrent sessions, and per-session detail drill-down with auto-refresh
- **Ingest error quarantine** — corrupt or unparseable log lines are isolated, classified, and surfaced in the admin UI with export/download, instead of silently dropped
- **Session scoring** — edge-computed 0-100 risk score per request combining cookie/timing signals with a PageRank transition matrix, with live threshold enforcement, audit logging, key rotation, and matrix version history. See the [runbook](docs/session_scoring_runbook.md) and [feature reference](docs/features.md)
- **Real User Monitoring (RUM)** — client-side Core Web Vitals, performance, and error analytics powered by a self-hosted, version-controlled [Grafana Faro Web SDK](https://grafana.com/oss/faro/) loaded dynamically from FOS, featuring a secure server-side beacon receiver, crash-safe ingestion into ClickHouse (`rum_vitals` and `rum_errors`), and an active SSE streaming live-event ticker
- **Assets Shield** — static asset delivery and cache performance analyzer with hit/miss/pass ratios segmented across custom Fastly cache states, distinct document class filtering, and deep-link filtering that opens pre-filtered analytics views in a new tab, integrated directly as an executive tab under Service Summary
- **Remote Frontend Deployment** — a secure, enterprise-grade edge proxy architecture fronted by a dedicated Fastly service, allowing admins to deploy and tear down a public reverse proxy for their local dashboard VM in a single click from the share control panel

See [docs/features.md](docs/features.md) for the full feature reference.

---

## Manual Installation

If you'd rather not use Docker:

```bash
# Recommended: install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# PostgreSQL 16 and ClickHouse must be running. Configure connection settings
# in your environment or .env (see .env.example):
#   export METADATA_DSN=postgresql://USER:PASS@127.0.0.1:5432/fla
#   export CLICKHOUSE_URL=http://127.0.0.1:8123

# Backend dependencies
uv sync

# Frontend dependencies
cd frontend && npm ci && cd ..

# Start the app (production mode)
./run.sh

# ...or development mode with hot reload
./run.sh --dev

# In a separate terminal, run the stateless ingest worker:
uv run python -m backend.high_scale.worker
```

Then open **http://localhost:3000**.

### CLI provisioning (alternative to the wizard)

If you have a [Fastly API token](https://www.fastly.com/documentation/guides/account-info/user-and-account-management/using-api-tokens/) with **Superuser** permissions (provisioning creates new services, which an Engineer token can't do), you can provision from the command line (with `METADATA_DSN` and `CLICKHOUSE_URL` exported in your shell, or via `docker compose exec backend`):

```bash
# Guided
uv run python -m backend.provision.cli provision

# Non-interactive
uv run python -m backend.provision.cli provision --token <YOUR_TOKEN> --service-id <ID> --yes

# Teardown (include --remove-data with --yes to also remove local data and metadata rows)
uv run python -m backend.provision.cli teardown --service-id <ID> --remove-data --yes
```

Common flags: `--region us-east-1`, `--bucket <name>`, `--sample-rate 100`, `--period "1 minute"`, `--cdn-prefix <subdomain>`, `--remove-data` (on teardown), `-y` / `--yes` (accept defaults). Provisioning auto-rolls back on failure to leave your Fastly account clean.

### Bring your own bucket

If you already have a bucket, drop a JSON config file in `configs/` instead (create the directory if your checkout doesn't have one yet). See [`config.example.json`](config.example.json) at the repo root for the schema (`fos_endpoint`, `fos_bucket`, `fos_access_key_id`, `fos_secret_access_key`, `fos_region`, optional `cdn_url` + `cdn_secret`, optional `fastly_api_key`).

---

## Configuration

All app-level configuration is via environment variables. Copy [.env.example](.env.example) to `.env` and uncomment any value you want to override. The app starts with sensible defaults if you skip this entirely.

Per-service configuration (credentials, log field selection, custom fields, sync intervals) lives in `configs/{service_id}.json` and is managed via the UI or the provisioning CLI.

The **Fields** button on each service card opens the log field configurator — pick which JSON fields to log and the app generates the matching VCL log format (and any required Edge Data Capture snippets). See [docs/features.md](docs/features.md#log-field-configuration) for the field-group reference.

### CDN fronting (optional but strongly recommended)

To route FOS reads through a Fastly CDN service (for free egress and edge caching) the wizard creates this for you. If you're configuring manually:

1. Create a Fastly Delivery service with your FOS bucket as the backend origin
2. Configure the VCL to handle AWS4 signing on the request to FOS and shared-secret query-param authentication (`?key=…`) from the backend. The provisioning wizard generates this VCL automatically — if you've already enabled a service through the wizard, you can copy the active VCL from the Fastly UI or API as a starting point for a hand-managed service.
3. Set `cdn_url` and `cdn_secret` in your service config

---

## Development

```bash
make install         # uv sync + frontend npm ci
make fast-ci         # smoke gate (~1-2m): linters + typechecks + focused unit/contract tests
make ci              # full gate: lint + format + typecheck + tests (back + front) + security scans
make dev             # backend + frontend with hot reload (./run.sh --dev)
make test            # backend pytest only
make test-frontend   # frontend vitest only
make typecheck       # mypy backend/
make lint-fix        # ruff check --fix
make format          # ruff format
```

Pre-commit hooks:

```bash
make install-hooks   # runs uv run pre-commit install once
```

After this, every `git commit` runs ruff (lint + format), mypy, and standard file checks.

The Next.js frontend uses a typed API client generated from the FastAPI OpenAPI schema. `run.sh` and production builds regenerate types on startup; after manual backend model changes, regenerate manually:

```bash
cd frontend && npm run gen:types
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the system design, and [AGENTS.md](AGENTS.md) for the deeper contributor notes: canonical patterns and the list of known traps.

---

## Troubleshooting

### Port already in use (3000 / 8000)
The frontend listens on **3000** and the backend on **8000** by default. If another process holds one, find it with `lsof -i :3000` or `lsof -i :8000`, then either stop it or run on different ports: set `FRONTEND_PORT` / `BACKEND_PORT` (and `API_PROXY_URL` to match the backend) in `.env`, or pass `--frontend-port` / `--backend-port` to `run.sh`.

### Browser: `ERR_ALPN_NEGOTIATION_FAILED`
Usually a protocol mismatch from a security setting forcing HTTPS on a port that only speaks HTTP. Hit `http://localhost:3000` (not `127.0.0.1`), and make sure the frontend is going through the Next.js proxy rather than calling the backend port directly.

### Next.js startup crash on hardened macOS (`uv_interface_addresses returned Unknown system error 1`)
The `dev` script in `frontend/package.json` binds with `-H 127.0.0.1` to bypass network interface enumeration. Don't drop that flag in any custom dev setup.

### Dashboard shows "No data" but the header has a row count
- Check the time range overlaps with your log data.
- Check the browser console for failed `POST`s (often the ALPN issue above).
- A newly provisioned service can take a few minutes for the first ingestion batch to publish. Check the **Log Management** page or **ClickHouse Diagnostics** (`/admin/clickhouse`) for ingest status.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md). Vulnerability reports should go through [Fastly's security issue reporting process](https://www.fastly.com/security/report-security-issue) — please don't file public GitHub issues for security problems.

## License

[Apache License 2.0](LICENSE). Copyright 2026 Fastly, Inc.
