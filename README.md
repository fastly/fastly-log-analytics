# Fastly Log Analytics

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to [Fastly Object Storage](https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/).

Fastly's [historical stats](https://www.fastly.com/documentation/reference/api/metrics-stats/historical-stats/) give you aggregates. When you need to drill into individual requests — by IP, URL, status, WAF signal, or any field you log — Fastly's [real-time log streaming](https://www.fastly.com/documentation/guides/integrations/streaming-logs/about-fastlys-realtime-log-streaming-features/) makes the raw data available, but you still need somewhere to put it and something to query it with. This project fills that gap using only Fastly products. Costs are limited to Fastly Object Storage [class operations](https://www.fastly.com/documentation/guides/platform/object-storage/working-with-object-storage/#using-the-s3-compatible-api) and [storage](https://docs.fastly.com/products/object-storage#billing) — no third-party logging vendor required.

---

## Before You Start

You'll need:

- A **Fastly account** with permission to create [services](https://www.fastly.com/documentation/guides/getting-started/services/about-services/) and Object Storage buckets
- **Object Storage enabled** on the account — it's a separately activated product, not on by default
- At least one **VCL service** to stream logs from
- **Docker** (recommended) — or Python 3.10+ and Node.js 24+ for a manual install
- *Optional:* a Fastly API token with the **Billing** permission to power the [Usage & Cost page](docs/features.md#usage--cost-page)
- *Optional:* [`falco`](https://github.com/ysugimoto/falco) to validate VCL during provisioning (highly recommended; the app degrades gracefully without it)
- *Optional:* **Rust 1.90+** with the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`) — only needed if you plan to rebuild the [Session Scoring](docs/session_scoring_runbook.md) Compute Wasm scorer from source

---

## Quick Start

```bash
docker-compose up --build
```

Open **http://localhost:3000** and follow the provisioning wizard. The wizard creates your Object Storage bucket, access keys, a CDN-fronting service, and the logging endpoint on the VCL service you select.

For manual install (no Docker), see [Manual Installation](#manual-installation).

---

## How It Works

The **admin** runs the provisioning wizard to set up a Fastly Object Storage bucket, deploy a CDN-fronting service for cheap log reads, and attach a structured JSON logging endpoint to a chosen VCL service. Once that's in place, the app continuously ingests new `.gz` log files from the bucket into an Apache Iceberg table.

![Provisioning Process](docs/assets/provisioning.png)

![ETL Process](docs/assets/etl.png)

Once the data lake is healthy, you can collaborate with teammates using two different approaches depending on your hosting setup and security needs:

![Sharing Process](docs/assets/sharing.png)

| Model | Path A: Independent Copy | Path B: Live Shared Server |
| :--- | :--- | :--- |
| **Analyst Setup** | Runs their own local copy of the app | Standard web browser only |
| **Admin Setup** | **Offline-friendly.** Your laptop/server can go offline. | **Always-on.** Your machine hosts the active web server. |
| **Data Source** | Analyst queries FOS bucket directly | Analyst queries the Admin's database over HTTP |
| **Credential Sharing** | Shares read-only FOS bucket keys | **Zero keys shared.** Admin handles all credentials. |
| **Best For** | Long-term analysts, laptop-only admins. | Quick screen-shares & non-technical associates. |

---

### Path A: Independent Copy (Direct Bucket Access)

The analyst runs their own independent copy of the app on their laptop or server. They use a read-only credential package to sync and query the Fastly Object Storage bucket directly.

#### How it works:
1. **Admin:** Click **Invite Analyst** in your dashboard. The app packages your FOS bucket name, region, and a set of read-only access keys into a secure JSON string. Send this JSON securely to your teammate.
2. **Analyst:** Start your own copy of the app (e.g., using `docker-compose`), select **Join Service** on the setup screen, and paste the JSON config.
3. Your teammate's app automatically configures itself in `read_only` mode and syncs directly from the bucket. *Note: Because only the Admin's machine runs the active raw log ingestion pipeline, if the admin is offline, no new logs will be written to the database (though the analyst can still query all historical data). Once the admin is back online, the analyst's dashboard will automatically sync the newly committed logs.*

---

### Path B: Live Shared Server (Web-Accessible Host)

You run the application as a central web-accessible server (either on a dedicated VM or from your laptop using a secure tunnel). Your associates connect to your server using a standard web browser and enter a passcode.

#### How it works:
1. **Admin:** Click **Share Dashboard** in your dashboard. Choose how to make your server reachable over the web:
   - **SSH Tunnel (via localhost.run):** Easiest for local laptops. Spawns an automatic reverse SSH tunnel to assign you a public `https://*.lhrun.dev` link.
   - **Your Own Hostname/IP:** Best for public servers. Direct connections via a custom domain name or IP (requires HTTPS setup).
2. **Admin:** Mint an analyst invitation in the sharing manager by specifying their name, an optional IP allowlist, and a passcode. Give them the public URL and passcode.
3. **Analyst:** Open the shared link in a standard browser, accept the Terms of Service, enter the passcode, and view the live read-only dashboard. All database queries are executed securely on your host server. You can revoke access or **Sever All Access** instantly.

---

## Features

- **Apache Iceberg data lake** — ACID-compliant log storage in FOS, safe for concurrent readers and writers
- **Automated provisioning** — wizard creates the bucket, access keys, CDN-fronting service, and logging endpoint
- **CDN-accelerated reads** — every FOS read goes through a Fastly service to minimize egress and maximize caching
- **Crash-safe ingestion** — buffered locally, atomically committed; interrupted imports never corrupt the table
- **Schema evolution** — new and missing JSON fields handled gracefully; corrupt lines isolated and surfaced
- **Log sampling** — optionally log a random percentage of requests to manage cost on high-traffic services
- **Multi-source support** — analyze logs from multiple services side by side
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations, raw log viewer with click-to-filter
- **Insights** — automated anomaly detection (error spikes, regional surges, new IPs, WAF signal changes, cache regressions, latency)
- **Usage & Cost** — live storage breakdown, FOS operation counts, period totals, interactive cost estimator
- **Log field configuration** — built-in field groups (HTTP, network, geo, TLS, NGWAF) plus custom VCL expressions
- **Alerts** — threshold-based, webhook-delivered
- **Live dashboard sharing** — three modes (SSH tunnel, your own hostname, your own IP) with per-analyst passcode invites, IP allowlisting, and instant revoke
- **Session scoring** — edge-computed 0-100 risk score per request combining cookie/timing signals with a PageRank transition matrix, with live threshold enforcement, audit logging, key rotation, and matrix version history. See the [runbook](docs/session_scoring_runbook.md) and [feature reference](docs/features.md)

See [docs/features.md](docs/features.md) for the full feature reference.

---

## Manual Installation

If you'd rather not use Docker:

```bash
# Recommended: install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Backend dependencies
uv sync

# Frontend dependencies
cd frontend && npm ci && cd ..

# Start the app (production mode)
./run.sh

# ...or development mode with hot reload
./run.sh --dev
```

Then open **http://localhost:3000**.

### CLI provisioning (alternative to the wizard)

If you have a [Fastly API token](https://www.fastly.com/documentation/guides/account-info/user-and-account-management/using-api-tokens/) with **Engineer** or **Superuser** permissions, you can provision from the command line:

```bash
# Guided
uv run python backend/provision.py

# Non-interactive
uv run python backend/provision.py --token <YOUR_TOKEN> --service-id <ID> --yes

# Teardown
uv run python backend/provision.py --teardown --service-id <ID> --yes
```

Common flags: `--region us-east-1`, `--bucket <name>`, `--prefix <path>`, `--sample-rate 100`, `--period "1 minute"`, `--cdn-prefix <subdomain>`, `--remove-data` (on teardown), `-y` / `--yes` (accept defaults). Provisioning auto-rolls back on failure to leave your Fastly account clean.

### Bring your own bucket

If you already have a bucket, drop a JSON config file in `configs/` instead. See `config.example.json` for the schema (`fos_endpoint`, `fos_bucket`, `fos_access_key_id`, `fos_secret_access_key`, `fos_region`, optional `cdn_url` + `cdn_secret`, optional `fastly_api_key`).

---

## Configuration

All app-level configuration is via environment variables. Copy [.env.example](.env.example) to `.env` and uncomment any value you want to override. The app starts with sensible defaults if you skip this entirely.

Per-service configuration (credentials, log field selection, custom fields, sync intervals) lives in `configs/{service_id}.json` and is managed via the UI or the provisioning CLI.

The **Fields** button on each service card opens the log field configurator — pick which JSON fields to log and the app generates the matching VCL log format (and any required Edge Data Capture snippets). See [docs/features.md](docs/features.md#log-field-configuration) for the field-group reference.

### CDN fronting (optional but strongly recommended)

To route FOS reads through a Fastly CDN service (for free egress and edge caching) the wizard creates this for you. If you're configuring manually:

1. Create a Fastly Delivery service with your FOS bucket as the backend origin
2. Use the included [`sample-vcl.vcl`](sample-vcl.vcl) — it handles AWS4 signing and shared-secret query-param authentication
3. Set `cdn_url` and `cdn_secret` in your service config

---

## Development

```bash
make install         # uv sync + frontend npm ci
make ci              # full check: lint + format + typecheck + tests + osv scan
make dev             # backend + frontend with hot reload
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

See [AGENTS.md](AGENTS.md) for the architecture deep-dive, canonical patterns, and the (extensive) list of known traps.

---

## Troubleshooting

### Port 8080 conflict on macOS
Another process is using port 8080. Find it with `lsof -i :8080`, or run the backend on a different port and update `BACKEND_PORT` / `API_PROXY_URL` accordingly.

### Browser: `ERR_ALPN_NEGOTIATION_FAILED`
Usually a protocol mismatch from a security setting forcing HTTPS on a port that only speaks HTTP. Hit `http://localhost:3000` (not `127.0.0.1`), and make sure the frontend is going through the Next.js proxy rather than calling the backend port directly.

### Next.js startup crash on hardened macOS (`uv_interface_addresses returned Unknown system error 1`)
The `dev` script in `frontend/package.json` binds with `-H 127.0.0.1` to bypass network interface enumeration. Don't drop that flag in any custom dev setup.

### Dashboard shows "No data" but the header has a row count
- Check the time range overlaps with your log data.
- Check the browser console for failed `POST`s (often the ALPN issue above).
- A newly provisioned service can take a few minutes for the first ingestion + commit. Check the **Log Management** page for sync status.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md). Vulnerability reports should go through [Fastly's security issue reporting process](https://www.fastly.com/security/report-security-issue) — please don't file public GitHub issues for security problems.

## License

[Apache License 2.0](LICENSE). Copyright 2026 Fastly, Inc.
