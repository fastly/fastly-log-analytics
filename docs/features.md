# Features

Detailed reference for every feature in the app. The [README](../README.md) has the short list; this page has the why and how.

## Ingestion & Storage

### Apache Iceberg data lake
Logs are stored in a fully ACID-compliant Apache Iceberg table inside your Fastly Object Storage bucket. Multiple readers and writers can operate concurrently without locking each other out, and snapshots make point-in-time reads trivial.

### Crash-safe ingestion
Raw `.gz` log files are downloaded, parsed, buffered locally as Parquet, and atomically committed to the Iceberg catalog. An in-flight manifest (`ingest_in_flight` table) guards the window between buffer write and metadata commit so an interrupted process never corrupts the table or loses data. On restart, recovery either promotes the buffer to a permanent file or drops the in-flight row and re-ingests cleanly on the next tick.

### Schema evolution
JSON log fields are mapped to typed Iceberg columns. New fields are added to the schema on the fly; missing fields are filled with NULLs. Invalid or corrupt log lines are isolated and surfaced in the ingestion history rather than failing the batch.

### Automated Iceberg compaction
Small, frequently committed data files are periodically merged into larger files to keep query performance high and Iceberg metadata size in check. Old snapshots are expired on a weekly schedule.

### Smart local caching
When syncing the Iceberg table, data files are cached locally to give DuckDB instant query speeds. Missing files are transparently fetched from Fastly Object Storage at query time.

### Log cleanup (optional)
After successful Iceberg commits, the original `.gz` raw log files can be removed from the bucket automatically to keep storage costs down.

### Multi-source support
Every Fastly service / bucket / prefix combination gets its own configuration JSON, DuckDB engine file, and Iceberg table. Switch between services from the navigation bar to analyze logs from multiple Fastly services side by side.

## Provisioning

### Automated provisioning
A guided wizard (or the `python -m backend.provision.cli` CLI) creates the FOS bucket, a read-write access key scoped to it, a CDN-fronting Fastly Delivery service, and the logging endpoint on your target VCL service in one flow. Provisioning failures auto-rollback to leave your Fastly account in a clean state.

### Log sampling
Optionally log only a random percentage (1–100%) of requests to manage storage costs on high-traffic services. Sampling is implemented via Fastly [logging conditions](https://www.fastly.com/documentation/guides/full-site-delivery/conditions/using-conditions/), which the app creates and manages automatically.

### Teardown
A single command (or button in the UI) removes everything provisioning created: the logging endpoint on the VCL service, the CDN service, the FOS access key, the local config, the local DuckDB, and the local Parquet cache. Optionally also removes the FOS bucket contents.

## Log Field Configuration

### Built-in field groups
The **Fields** button on each service card opens a configurator with thirteen field groups: core HTTP (always on), Request Identity, Cache Deep-Dive, Infrastructure, Geolocation (basic + precision), Network Quality (core + deep), TLS Fingerprinting, Proxy & Anonymization, WAF / NGWAF, QUIC / HTTP3, and Origin Metrics. Each group toggles a curated set of fields and unlocks the corresponding analytics on the Insights and dedicated analysis pages. A few privacy-sensitive fields are opt-in and stay off even when their group is enabled — notably `cookie_session`, the client's session cookie hashed at the edge into a pseudonymous per-session id. Capturing a per-session client identifier is a deliberate per-field choice (and unlocks the session-harvesting insight), never a side effect of picking a preset.

### Edge Data Capture VCL snippets
For high-fidelity metrics (TTFB / TTLB, network quality, edge timings) the tool generates VCL capture snippets that read the right variables at the network edge before shielding or backend fetches can mask the data. The **Logging Settings** modal can deploy the updated format and VCL snippets to your Fastly service when you change field selections.

### Custom log fields
Admins can define arbitrary additional VCL fields appended to the log format. Each custom field has a name, a VCL log expression (e.g. `%{req.http.X-My-Header}V`), optional per-hook VCL snippets, and a DuckDB column type. Custom fields appear in the dashboard, raw logs viewer, and filter panel alongside built-in fields. Definitions sync to analyst instances automatically via the shared admin state in FOS.

### Log format size guard
Fastly enforces a practical limit (~8,000 characters) on log format strings. The configurator warns if your selection approaches the cap and estimates the per-line byte cost before deploying.

## Analytics

### Interactive charts and dashboards
Traffic-over-time with click-and-drag time range selection (1h / 24h / 7d / 30d quick presets), a global request map, top-N aggregations across every dimension, and a paginated raw-logs viewer with click-to-filter for instant drill-downs.

### Insights
Automated anomaly detection that compares a recent window against a longer baseline. 45 insights are organized into five tabbed categories — Security & Threat Detection, Origin Health & Stability, Edge & Delivery Performance, Network Path, and Traffic & Volumetrics — with per-tab severity badges and a full severity breakdown in each panel; only the active tab computes, so cards load when you open them. Coverage spans error spikes, credential enumeration, content-discovery (404) scanning, session harvesting, low-and-slow probing, regional traffic surges, new IPs / geographies / ASNs, WAF signal changes, cache efficiency collapses and hit-ratio cliffs, latency regressions (regional and per-PoP), HTTP/3 fallback, per-ASN network degradation, method and referer drift, and more. A few insights require optional log fields (e.g. `cookie_session` for session harvesting) and stay dormant until the field is enabled and history accrues.

### Free-form query editor
A DuckDB SQL pad pre-seeded with the logs view, useful for ad-hoc questions that don't fit the canned dashboards.

### Saved views
Pin filter sets + time ranges as saved views from the filter bar. Restore them in one click from any analytics page.

### Alerts
Threshold-based alerts that fire webhooks when a metric crosses an operator/threshold over a time window. Supports both absolute thresholds and comparison-period evaluation. Per-alert status code filters narrow scope to specific response classes.

## Cost & Usage Visibility

### Usage & Cost page
Live breakdown of storage usage (raw logs vs. Iceberg Parquet), a bar chart of Fastly Object Storage Class A / Class B operations, period totals with estimated cost, and an interactive cost estimator. The estimator pre-fills from your active service's traffic stats and the observed average log-line size from your ingested data.

Requires a `FASTLY_API_KEY` with the [Billing permission](https://www.fastly.com/documentation/guides/account-info/user-and-account-management/about-user-roles-and-permissions/#account-management-roles).

### Log-line accounting
A dedicated admin panel reconciles Fastly's authoritative `/stats/service/{id}` log emission counter against locally-ingested rows, bucket-by-bucket. Surfaces per-bucket gaps, a worst-bucket callout, and a sustained-loss alert (≥2 consecutive hours with ≥5% one-sided gap) so pipeline loss is visible immediately rather than hidden in aggregate totals.

### FOS usage logging
Every FOS Class A / Class B operation and CDN download is recorded with its process context (cron job name or API route) for cost analysis. The `/admin/usage-log` page surfaces a filterable, exportable timeline with cost attribution by job / route.

## Cost Optimization (Built-In)

The app is designed to minimize Fastly Object Storage operation costs:

- **CDN-fronted reads.** With `cdn_url` configured, all Parquet data reads go through a Fastly Delivery service that fronts the FOS bucket. Edge caching means repeated queries cost **zero Class B operations** at the storage layer. The provisioning wizard generates the CDN VCL that handles AWS4 signing and shared-secret authentication automatically (see [CDN fronting](../README.md#cdn-fronting-optional-but-strongly-recommended) for hand-managed setups).
- **Lazy listing.** The UI relies on local metadata for status updates; FOS `LIST` (Class A) only fires when polling for new raw logs. Ingestion uses S3 pagination markers (`StartAfter`) to scan only *new* files instead of iterating the bucket.
- **Batch processing.** Bulk deletion and multi-file ingestion are batched to minimize API round-trips.
- **Change-gated metadata writes.** `table_summary.json` is content-hashed before each PUT; identical-payload writes are skipped, eliminating a redundant FOS PUT per Iceberg commit in steady state.
- **Dashboards never hit the cloud.** All analytics queries serve from the local DuckDB + Iceberg cache and per-service SQLite metadata. The only cloud reads are the cron-driven ingest cycle and the on-demand Fastly Stats reconciliation in the log-accounting panel (rate-limited to ≤1 call/minute per open admin tab).

## Collaboration

### Independent instance (durable JSON-config join)
Admin generates a read-only invite from the **Invite Analyst** dialog. The dialog produces a JSON blob (bucket, region, read-only FOS credentials, CDN config, Iceberg metadata location). The analyst pastes the JSON into the **Join** flow of their own copy of the app. Their app connects to the admin's bucket, imports the saved admin state (log format history, saved views, custom field definitions), writes a `read_only` config, and starts syncing data. Best for long-lived collaboration — the credentials work until rotated.

### Live shared instance (admin-as-server)
Admin opens **Share Dashboard**, picks how analysts will reach the host, then mints per-analyst invites (name, email, scoped services, optional IP allowlist, optional expiry). Each invite authenticates with either a local passcode or — when an OIDC provider is configured — single sign-on (Google first; any OpenID Connect provider by discovery). OAuth logins converge on the same analyst session as passcode logins, so RBAC, IP masking, TOS gating, revocation, and audit apply unchanged; a per-provider `auto_provision` flag can additionally create invites just-in-time for verified logins from a trusted, org-restricted IdP. SSO is default-off and inert until a provider registry is configured. Invites are single-seat by default — a new login boots any existing session on the same invite. An optional per-invite **Allow shared logins** toggle lets several people be logged in under one invite at once (still bounded by the global session cap), at the cost of per-person audit attribution; it can be set at creation or edited later. Analysts log into the public URL, accept a TOS, and reach a read-only dashboard served from the admin's machine. No FOS credentials are issued; sessions exist only while sharing is active. Heartbeat polling surfaces a "Connection interrupted" overlay if the listener drops; admin can revoke a single invite or hit **Sever All Access** to evict everyone in one click. The invitations table shows who is actually using the shared dashboard at a glance — last login, an online-now presence dot, the invite's auth method, and a Shared badge for multi-seat invites — and all three admin tables (invitations, sessions, audit log) sort by any column.

Sharing runs in direct mode against a public HTTPS endpoint you control (no third-party relay). Two connectivity options:

1. **Your own hostname** — requires a publicly resolvable hostname pointing at the admin's machine, a TLS cert (Caddy / Cloudflare / Let's Encrypt), and the forward port reachable from the internet.
2. **Your public IP** — no DNS required. Still needs HTTPS because analyst session cookies are issued with `secure=true`. Public CAs do not issue certs for raw IPs, so plan on a self-signed cert (analysts must trust the browser warning) or a reverse proxy that terminates TLS for a hostname.

(An earlier SSH-reverse-tunnel-via-relay option was removed in v2.0.)

See [SECURITY.md](../SECURITY.md#live-dashboard-sharing--trust-model) for the trust model and per-mode caveats. Best for short-lived collaboration where provisioning a second instance is overkill.

## Operational

### Log Management view
Dedicated page for managing the ingestion pipeline, reviewing import / commit / sync history, and updating log settings (including automated log format alignment when you change field selections).

### Cron run history
Every scheduled job (sync, commit, optimize, expire) writes a row to `cron_runs` with start/end timestamps, status, and counters. Surfaced under the **Cron Runs** tab on the Log Management page.

### Automated syncing
Sync, commit, optimization, and snapshot expiration all run in the background via an in-process APScheduler. No crontab setup required. Jobs are added and removed per service automatically when you provision or tear down.

### Health probe
`GET /api/health` is a cheap liveness check. `GET /api/health?deep=1` also verifies per-service ingest freshness and returns 503 when any service is degraded (last ingest older than the configured `stale_minutes`, adaptively widened per-service from that service's own ingest history to avoid false positives on low-traffic services, or last sync errored). Safe to wire into a load balancer.

### NGWAF integration
If a Next-Gen WAF workspace is linked during provisioning, the app syncs verified-bot signal data from the Fastly NGWAF API into a shared local SQLite cache and enriches matching log rows for analysis.

### Bring your own bucket (manual config)
If you already have a bucket, you can configure the app by writing a JSON file in `configs/` instead of running the provisioning wizard. See `config.example.json` for the schema. Manual configuration supports the same `read_write` / `read_only` access levels as the wizard.

## Session Scoring

### What it does
Real-time edge scoring of every request as it transits Fastly. A two-layer scorer (L1 cookie+timing heuristics, L2 PageRank transition matrix over URL paths) produces a 0–100 score per request along with a reason code. Scores are logged alongside every line and land in DuckDB as ordinary columns, queryable from the dashboard, raw logs viewer, and SQL pad.

### Custom log fields
Enabling scoring on a service appends six custom fields to the log format:

- `sid` — rotating AES-encrypted session identifier carried in a first-party cookie (30 min idle / 24 h hard cap)
- `edge_score` — final 0–100 score for the request
- `edge_score_reason` — short reason code explaining the score (e.g. `tampered_cookie`, `path_anomaly`)
- `edge_cookie_compliance` — whether the request presented a valid, untampered session cookie
- `edge_l1_score` — L1 contribution (cookie integrity + inter-request timing)
- `edge_l2_score` — L2 contribution (PageRank transition probability for the current path given prior paths in the session)

### VCL pattern
Scoring is wired into the request lifecycle through six VCL snippets that share state via a single restart. The order is `recv → pass → fetch → deliver → miss → enforce`, with the `enforce` snippet only firing on `req.restarts == 1` after the scorer has annotated the request. `recv` also unsets six client-controllable `X-Edge-*` headers to prevent score injection from the wire.

### Admin UI
The `/admin/session-scoring` page is the operator console:

- **StatusPanel** — per-service enable/disable, current threshold, enforcement state
- **ScoringHealthCard** — scorer counters (requests, tampered cookies, enforcement blocks, matrix load failures)
- **ThresholdSlider** — preview score distribution at a candidate threshold, then commit; toggles live enforcement
- **ROC + PR curves, per-reason AUC** — evaluation against the labeled set
- **Top-flagged, labels** — review the highest-scoring requests and assign ground-truth labels for retraining
- **Matrix history** — list past PageRank matrix versions and restore any of them (pre-restore snapshot is taken automatically)
- **Audit log** — every mutation (enable, threshold change, key rotation, matrix restore, etc.) is recorded per-service
- **Retrain, key rotation** — rebuild the L2 matrix from labeled data; rotate the AES sid key (current → previous slot)

### Key endpoints
`/scoring/enable`, `/scoring/disable`, `/scoring/status`, `/scoring/labels`, `/scoring/top-flagged`, `/scoring/score-distribution`, `/scoring/compliance-breakdown`, `/scoring/threshold`, `/scoring/threshold-preview`, `/scoring/enforce-threshold`, `/scoring/retrain`, `/scoring/dashboard`, `/scoring/evaluation/per-reason`, `/scoring/audit`, `/scoring/rotate-key`, `/scoring/matrix-versions`, `/scoring/matrix-versions/{v}/restore`.

### DDoS gate
The Compute scorer is bypassed when `fastly.ddos_detected` fires. Volumetric defense is Fastly's job; rate limiting is explicitly out of scope for session scoring. The score column on requests served during a DDoS event will be absent rather than synthesized.

### Operations
See [session_scoring_runbook.md](session_scoring_runbook.md) for enable/disable procedures, threshold tuning, key rotation, matrix restore, and incident response.
