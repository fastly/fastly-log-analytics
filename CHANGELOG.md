# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-07-07

### Added

- **Analyst invites can be redeemed with OAuth/OIDC single sign-on as an
  alternative to a passcode.** Operators can register an OpenID Connect
  provider (Google works out of the box; others via standard discovery) and
  choose passcode or SSO per invite; analysts then sign in through the
  provider's own login on the share-login page. An OAuth login lands in
  exactly the same analyst session as a passcode login — service scoping, IP
  masking, terms acknowledgement, the concurrent-session cap, revocation, and
  audit logging all apply unchanged — and the invite is pinned to the provider
  identity on first login, with the auth method visible on the invitations,
  sessions, and audit views. A fully-trusted, organization-restricted provider
  can additionally be flagged to auto-provision an invite just-in-time for a
  verified member of the org (with a preset service scope and masking policy),
  so trusted teams don't need every user invited by hand; this is off unless
  explicitly enabled. The whole feature is inert until a provider registry and
  its credentials are configured — the production compose file passes the
  relevant environment variables through from the host — and the existing
  passcode flow is unchanged.

- **Per-invite "Allow shared logins" toggle.** Analyst invites are single-seat
  by default — each new login boots the invite's previous session — so a link
  shared among several people kept kicking them off each other. An opt-in
  toggle (set at creation or edited later, shown as a "Shared" badge) now lets
  multiple analysts stay signed in under one invite at once, still bounded by
  the global concurrent-session cap, and expiry, revocation, IP allowlisting,
  and masking continue to apply per invite. Note that on a shared invite,
  audit events can no longer attribute an action to one individual, since
  everyone authenticates as the same name and email.

- **Twelve new anomaly detections, and the Insights page is reorganized into
  category tabs.** New insights flag content-discovery scanning (one IP
  bursting 404s across many distinct URLs), low-and-slow probing of sensitive
  paths, credential stuffing against login/auth endpoints, per-ASN
  network-health degradation (packet loss, jitter, TCP retransmissions),
  traffic from an ASN never seen in the baseline, a single Referer suddenly
  driving an outsized share of traffic, write-method surges on a GET-dominated
  service, US-metro delivery-rate drops, unusual connection-type mix shifts,
  per-Fastly-PoP latency regressions, HTTP/3 clients falling back to TCP, and
  service-wide cache-hit-ratio cliffs. The page's cards are now grouped into
  Security & Threat Detection, Origin Health & Stability, Edge & Delivery
  Performance, Network Path, and Traffic & Volumetrics tabs, each badged with
  its critical/warning counts, and only the selected tab's cards are rendered.
  Under the hood, related per-IP and per-dimension detections share coalesced
  scans so the larger insight set doesn't slow the page, and security insights
  honor an analyst invite's IP-masking policy.

- **Three new insights backed by three new edge-captured log fields.** Payload
  Compression Regression flags compressible responses that flipped from
  gzip/brotli to uncompressed versus baseline; Session Harvesting flags a
  single IP presenting many distinct session cookies — session-token brute
  force or cookie replay; Origin Timeout Split separates origin slowness into
  connect time versus read time and flags whichever regressed. Each is powered
  by a new log field (response Content-Encoding, an edge-hashed session-cookie
  id, and origin connect/TLS handshake time), so a service must redeploy its
  logging configuration — and accrue some history — before these cards
  populate. The session-cookie id is strictly opt-in (no preset or field group
  enables it — enable the field explicitly to turn Session Harvesting on), is
  SHA-256-hashed at the edge so the raw cookie never leaves Fastly, and is
  redacted on every analyst-facing surface for invites with IP masking.

- **The share dashboard now shows who is actually using shared access.** The
  Active Invitations table gains a Last-login column and a live "online now"
  indicator — derived from the existing login audit events, so it works
  retroactively with no migration — and the Services column shows each
  service's friendly name alongside its id. All three share tables
  (invitations, sessions, audit log) are now sortable by clicking column
  headers, and the audit log is rendered as a proper table.

- **Active filters are pinned at the top of the field-value search dialog.**
  Opening a top-N card's search (e.g. the Edge PoP card) now starts with a
  "Selected" section listing the filters already applied for that field —
  marked as include or exclude and removable in place without closing the
  dialog. Selected values are deduplicated from the fetched list below and
  follow the search text as you type.

### Fixed

- **The Debug Panel's SQLite section now shows what the current page
  actually executed, not the whole backend's recent activity.** The
  section previously rendered the process-global capture buffer — cron
  and background statements included — so a dashboard load could report
  seconds of "SQLite time" the page never spent. Each API response now
  carries the SQLite statements that ran while serving it, the panel
  defaults to this per-page view, and the process-wide buffer moved
  behind an explicit "Process-wide" toggle (its 5-second polling now
  only runs while that view is open).

- **Active-hour panels no longer double-count the freshest few minutes of
  traffic.** The fast direct read behind the dashboard's current-hour
  merging also picked up buffer files whose rows had already been committed
  to the hourly partitions (they linger briefly on disk for in-flight
  queries), inflating the active hour's contribution to top-N cards, the
  connection-reuse histogram, the traffic chart, and the NGWAF bots panel
  by up to ~10 minutes of recent rows.

- **Dashboard/Security/Origin/Performance no longer flash the wrong
  service's data on a direct or hard-refresh load.** The server-rendered
  first paint resolved the active service from the account's default only,
  ignoring the page's own `?service=` link — so opening a direct link to a
  non-default service briefly rendered the default service's numbers before
  the client silently swapped in the right ones. The server render now
  prefers the URL's `service` param, matching the client's own resolution.

- **`/api/health?deep=1` no longer flags a healthy, low-traffic service as
  degraded during a normal quiet spell.** The ingest-staleness check used
  one fixed threshold for every service; a service's own historical ingest
  cadence now widens that threshold when its typical quiet periods run
  longer than the default, while services with steady traffic are
  unaffected.

- **The Insights page's default view is fast again on services with 30+ days
  of history.** The background prewarmer still warmed a fixed one-week
  baseline while the page's adaptive default (introduced in 2.0.0-beta.2) had
  grown to a 30-day baseline once enough history accrued, so every default
  Insights load on a long-history service missed the warm cache and paid the
  multi-second cold computation. The prewarmer now derives the same
  window/baseline pair the page itself will pick from the service's log
  history, and warms it for both admin and analyst views.

- **The Debug Panel's query-time readout no longer sits at 0 queries / 0.00ms
  on server-rendered page loads.** Backend responses only include per-query
  timings when the request opts in via a debug header, and the server render's
  own fetch never sent it — so cold loads that the server render fully
  satisfied left the panel with nothing to read. The debug toggle now also
  sets a cookie the server render honors (admin sessions only, never analyst),
  browsers that enabled the toggle before the cookie existed heal themselves
  automatically, and flipping a toggle refreshes the open page's data so the
  panel populates without a reload.

- **Report pages no longer silently swap their charts moments after first
  paint on services whose logs lag behind the clock.** The server-rendered
  first paint of Dashboard, Security, Origin, and Performance anchored the
  time window on the render-time "now", while the client immediately
  re-snapped the window to the service's actual log range — changing the
  query and replacing the just-painted data with no loading indicator. The
  server render and the client now share the same snap-to-log-extents
  decision, so the first paint already shows the window the page settles on.

- **Top-N cards no longer silently under-count the current day on services
  with bursty log delivery.** Precomputed hourly top-N rollups for a closed
  hour were only written when a later ingest batch straddled the hour
  boundary; on services whose traffic bursts end mid-hour, every closed hour
  of the current day could be missing from the cards until the nightly
  rebuild — visible as the exact "Total Logs" count disagreeing with every
  card's total by the whole day's traffic. A new hourly heal pass now
  backfills any missing closed hours within about an hour, verified-empty
  quiet hours are stamped as covered so they cost nothing, and the reader
  falls back to querying still-uncovered hours live — so a rollup-writer
  outage now degrades to slower-but-correct cards instead of silent
  under-counts.

- **Short time-range presets no longer show all-time numbers.** The compact
  range encoding used by the Dashboard, Security, Origin, Performance, and
  Network pages only recognized the 24h / 7d / 30d / auto presets, so picking
  1h, 3h, 6h, 12h, or 3d fell back to an unbounded scan and the top-N cards
  reported all-time counts. Every quick preset is now recognized, an
  unrecognized preset degrades to the displayed window instead of all-time,
  and clicking a preset in a long-lived tab scans the window the chart
  actually displays rather than one anchored at page load.

- **Analyst IP and session-id masking closes its remaining side doors.** For
  invites with client-IP masking enabled: filter keys and field-value
  dimensions spelled as case or punctuation variants of a masked column
  ("IP", "ip.") now resolve to the same masked column instead of slipping
  past the policy while still querying the real data; the field-value picker
  no longer returns raw distinct client IPs; and ad-hoc query expressions
  that rebuild an identifier through string operations or grouping come back
  redacted, because masking is applied at the query's source view.

- **Switching services gives every panel a clean start.** The service
  switcher now performs a full page reload instead of an in-place route
  change, which could leave the traffic chart (and other panels) showing the
  previous service's in-flight data. The newly selected service survives the
  reload, so there's no bounce back.

- **Session-detail clicks no longer sporadically fail with "Session reference
  expired" after a backend restart.** Without an explicit
  `SESSION_TOKEN_SECRET`, two simultaneous first requests after startup could
  each mint a different ephemeral sealing key for the opaque session-detail
  tokens, so sessions listed under the losing key could never be opened until
  the next restart. Key initialization is now race-safe; deployments that
  configure the secret were never affected.

- **Invite creation recovers from share-database schema drift.** If the share
  database's schema version stamp ever got ahead of its actual columns (a
  skipped migration), the missing columns were never retried and creating an
  invite failed with a server error from then on. Expected additive columns
  are now reconciled automatically every time the database is opened — a
  purely additive self-heal that never rewrites or drops existing data.

- **Byte sizes just under a unit boundary now read in the next unit up.**
  Size displays across the app (storage and usage totals, cache size, file
  listings, per-line estimates) showed values like "1023.24 MB" right at the
  edge of a gigabyte; formatting now promotes to the next unit once a value
  would need four digits, so it reads "1 GB" instead.

### Changed

- **Dashboard and Top Bots load substantially faster.** The dashboard's
  rollup path no longer materializes a per-request temp table; the
  connection-reuse histogram is served from the existing per-hour
  connection-reuse rollup; the current-hour slice of every panel is read
  once per request (directly from buffer/hourly parquet, skipping files
  that can't contain current-hour rows); and a new per-hour NGWAF-bots
  rollup replaces the per-request join against the bot cache. Measured on a
  busy 24h window: Top Bots ~0.57s → ~0.15s; the bot-UA matcher is also
  pre-compiled at startup so the first Top Bots request after a restart no
  longer pays a ~300ms compile.

- **The Network page loads much faster on wide windows.** The latency heatmap
  and geographic health sections are now served from precomputed per-hour
  rollups when an unfiltered window of 48 hours or more is mostly closed
  history, skipping the 2–4 second full scan the page previously paid on a
  30-day view. Filtered and short windows keep the exact live path.

- **Background housekeeping no longer stalls the app.** Metadata cleanup now
  deletes in batches, so a large purge can't hold the metadata write lock for
  tens of seconds and queue sync bookkeeping behind it; the storage-stats
  sampler reads the database file size instead of scanning every page on each
  metrics tick; concurrent Insights requests for the same service now share
  one scan instead of running expensive duplicates; and the sessions API
  defaults to the last 7 days when called without a time range instead of
  scanning all history.

- **Source installs now require Python 3.12 or newer.** The backend already
  uses Python-3.12-only syntax, so installing on an older interpreter failed
  midway with a bare `SyntaxError`; declaring the true floor turns that into
  a clear, up-front pip error. Docker deployments already run Python 3.13 and
  are unaffected.

- **Tabs read more clearly.** The active tab is now filled with the primary
  color instead of a subtle shade, tabs get a background lift on hover, and
  the Insights tab's count badge keeps accessible contrast against the newly
  colored active tab.

- **The admin API surface is now fully typed.** Session-scoring, share-admin,
  provisioning, service, query-monitor, usage, compaction, and saved-view
  endpoints now declare typed response models in the OpenAPI schema
  (previously untyped in the generated client). The wire shape is unchanged —
  responses are byte-identical for both admin and analyst.

### Security

- **A broad hardening pass across the analyst surface, sign-in flow, and edge
  log capture.** Analyst requests are path-normalized and service-scoped more
  strictly before permission checks, and ad-hoc query validation inspects
  what a table function actually is rather than just its name. The sign-in
  flow leaks less: a successful login no longer resets the per-IP failure
  counter, and invite verification takes the same time whether or not the
  invite exists. Oversized or deeply nested telemetry payloads and
  out-of-range alert-preview parameters are now rejected, custom log-field
  names are escaped in ingest SQL, server-rendered pages no longer attach
  admin credentials to their internal fetches, and edge capture bounds logged
  values so a pathological header can't corrupt a log line.

### Removed

- Dropped three vestigial fields from the `/api/bootstrap` response:
  `custom_fields_catalog` (always emitted empty), and `cron_schedule` /
  `share_status` (always `null`). The log-fields catalog, cron schedule, and
  share status are each served by their dedicated endpoints, and the bundled
  frontend never read the bootstrap copies.

## [2.0.0-beta.2] - 2026-07-01

### Added

- **Scripted Traffic Patterns insight.** A new Insights card flags client IPs
  sending requests on a highly regular cadence — scrapers, pollers, cron jobs,
  and beacons that slip under volumetric rate limits — by measuring
  inter-arrival-time regularity over the live window. Verified crawlers and
  monitors (Googlebot, Pingdom, UptimeRobot) are suppressed so they don't
  dominate the card, and a 0–100 regularity score drives severity. It runs on
  every service (it needs only client IP and timestamp).
- **"Why we flagged this" evidence modal for Scripted Traffic Patterns.** Each
  flagged IP opens a click-through modal showing the per-IP evidence (regularity
  score, cadence variability, modal fraction, mean interval, jitter, volume/span,
  distinct user-agents), mirroring the Impossible Distance and Cache Collapse
  affordances. It reads pre-embedded evidence, so there's no extra request.
- **Scripted Traffic Patterns help content.** The insight's "How this works"
  dialog explains the cadence-regularity detection and sub-rate-limit evasion,
  with a script-vs-human cadence diagram.
- **Fullscreen Edge → Shield Transit Map.** The inline shielding map gains an
  Expand button that opens the same map full-size in a dialog, making the
  great-circle transit arcs easier to read as a 3D globe at scale. A bare
  mousewheel zooms the big globe directly in the modal, and it opens one zoom
  level closer than the inline card for a bigger initial view.
- **User-adjustable shielding minimum-requests floor.** The Edge → Shield card's
  low-sample floor is now a "Min requests" dropdown (No minimum / 10 / 30 / 50 /
  100) with a help tooltip, so an operator can scrutinize quiet routes or tighten
  the gate on a busy service. Adjusting it recomputes client-side with no
  refetch.
- **3D-globe transit map by default.** The Edge → Shield transit map now defaults
  to a 3D globe (toggle back to flat Mercator via the map's globe control),
  because edge→shield arcs are great-circle paths that read as misleading
  straight slashes on a flat projection.
- **Volume overlay on scoring charts.** The Scorer Latency chart gains a
  toggleable request-volume overlay and the Scorer Errors chart gains a
  toggleable error-rate percentage line, both on a secondary axis, so latency and
  error counts can be read against traffic volume.
- **Approximate-latency badges on origin panels.** The slow-URLs, PoP-latency,
  and IP-health tables and the origin latency chart show a shared "approximate"
  badge when a section is served from precomputed rollups (percentiles are
  request-weighted averages of per-hour values on wide windows). Request counts,
  the 5xx error rate, and the status-code distribution stay exact and are not
  badged.
- **Honor an operator-supplied client IP.** When a logging service sits behind a
  fronting proxy or CDN, operator VCL that rewrites `Fastly-Client-IP` to the
  true source IP is now respected for the captured client IP, with a true-edge
  scrub so a spoofed header can't poison the log.
- **Toggle IP masking on an existing invite.** The analyst-invite editor now
  exposes the "Anonymize client IPs" control, so masking can be turned on or
  off after an invite is created (previously settable only at creation). It
  applies to a live analyst session without requiring a re-login.
- **Adaptive Insights default range.** When a service has less history than the
  7-day baseline needs, the Insights page now defaults to the best
  window/baseline for the data that exists (e.g. ~2 hours of data compares the
  last hour against the previous hour) instead of showing "not enough data".
- **Session-scoring enablement pre-checks.** Enabling session scoring now
  verifies the Fastly products the scorer needs (Compute, KV Store, Config Store)
  before standing anything up. If a product isn't enabled the enable modal
  surfaces a clickable manage.fastly.com link for the missing product, and any
  failure during setup rolls back every resource created, so a botched enable
  leaves nothing behind.
- **Object Storage enablement pre-check in provisioning.** After the API token is
  entered, the wizard probes whether the account has enabled Object Storage
  (required for log storage) and surfaces a clear "Enable Object Storage"
  message — with a clickable link to the Fastly product page — instead of failing
  later at the storage-access step.
- **Edge rate-limiting detection before deploying.** Provisioning now probes the
  account's edge rate-limiting entitlement up front and persists the result, so
  the CDN-fronting service deploys the correct VCL on the first try. It re-checks
  on logging-settings updates and manual CDN redeploys, so an account that
  enables rate limiting later is picked up automatically.
- **Instructions-first provisioning preview.** In the Terraform & VCL preview,
  the Instructions tab now leads and is selected by default, so the step opens
  showing setup guidance rather than a raw `.tf` file.

### Changed

- **Analyst invites require at least one service.** Creating or editing an
  invite with no authorized services is now rejected, preventing an analyst
  from landing on an empty "no service found" dashboard.
- **Provisioning requires a superuser token.** The wizard and CLI now state that
  creating Fastly resources needs a superuser token; an engineer token cannot
  create services.
- **Full teardown now removes session scoring.** Tearing down a service now also
  tears down its session scoring instead of orphaning it — stripping the scoring
  VCL and backend and deleting the Compute service, both Config Stores, and the
  matrix KV store. Disabling scoring also removes the published matrix and its
  history.
- **Smarter scorer redeploy.** Redeploy now reads the live edge and skips each
  no-op leg independently: the Wasm upload is skipped when the committed build
  matches the live package, and the logging-VCL clone/activate is skipped when
  the live snippets and backend already match — so a redeploy no longer forces
  pointless version bumps.
- **Cooperative scroll-zoom on maps.** The network, ASN, shielding, and dashboard
  maps no longer capture the page scroll — a plain mousewheel scrolls the page,
  and you zoom with Ctrl/⌘ + wheel, pinch, or the on-map +/- buttons.
- **Sub-minute sync intervals are honored.** A service configured for a log-sync
  period below 30 seconds now actually syncs at that interval instead of being
  silently rounded up to a 30-second floor (the 5-second minimum still applies;
  the 120-second default is unchanged).
- **Terraform preview is a preview-only step.** The Terraform & VCL preview no
  longer offers a confusing duplicate deploy button; Deploy to Fastly / Complete
  Setup live on the Review step, and the preview offers a single Back-to-Review
  control.
- **Clearer storage-maintenance wording.** Cron, cost-calculator, and
  provisioning storage copy now distinguishes always-on local cache compaction
  (keeps dashboard queries fast) from the nightly Object Storage optimize
  (storage-side housekeeping that controls cloud storage cost).
- **Newly typed API responses.** `GET /scoring/labels` and `GET /api/cron-runs`
  now return typed responses (previously untyped in the generated client); the
  wire shape is unchanged for both admin and analyst.
- **Internal maintenance.** Sundry non-user-facing refactors and test/CI
  improvements landed alongside the above — shared helpers for router
  section-selectors, PII field sets, and SQLite caches; deduplicated frontend
  chart/column/skeleton scaffolding and generated-type adoption; simplified
  edge-hop detection in generated VCL; and quieter, more reliable test runs. No
  behavior change.

### Breaking

- **Admin live-update streams consolidated into one endpoint.** The three
  always-on admin Server-Sent Events streams — `GET /api/sync-status/stream`,
  `GET /api/cron-runs/stream`, and `GET /api/admin/system-metrics/stream` — are
  replaced by a single multiplexed `GET /api/admin/events/stream?channels=…`.
  An admin browser tab now holds one persistent connection instead of three,
  fixing HTTP/1.1 connection-pool starvation (slow / "pending" requests) when
  several admins use the app at once. The analyst `GET /api/log-extents/stream`
  is unchanged.
- **Share-dashboard live stream folded into the multiplexed admin stream.** The
  dedicated `GET /api/admin/share/stream` SSE endpoint is removed; its live
  updates now ride the `share` channel of `GET /api/admin/events/stream`, so the
  `/admin/share` page holds one connection instead of two over the admin tunnel.
  The `GET /api/admin/share/live` polling snapshot endpoint is unchanged.

### Fixed

- **PII masking now covers the dashboard Top-IPs card.** Analysts whose invite
  has IP masking enabled previously still saw raw client IPs on the dashboard's
  top-IPs panel (the Sessions list was already masked). IPs are now masked
  wherever they appear; user-agent and URL stay visible and admin views are
  unaffected.
- **IP filtering is blocked end-to-end for masking analysts.** When an analyst
  has client-IP masking on, filtering by client IP is now rejected server-side
  and every IP drill-down affordance is hidden in the UI (the dashboard IP card
  rows, IP-family filter menus, and the IP entry in Add Filter). Origin IP stays
  visible and filterable since it's infrastructure, not end-user data.
- **Session detail resolves for masked analysts.** When PII masking is on, the
  session-detail modal previously showed "No results" for every session because
  the masked IP was round-tripped back as the lookup key. Detail lookups now key
  on an opaque per-row token that seals the real session identity
  (service-bound), so the modal resolves correctly while the response stays
  masked; a tampered or expired token returns a clear error instead of a silent
  empty result.
- **Single-request sessions resolve detail.** A session whose start and end
  timestamp are identical (a single-request, sub-second session — a large share
  of real sessions) previously failed the time-range clamp and showed "No
  results" on click for every role. The window is now widened slightly before
  clamping so the session's request is matched.
- **Single-log and fresh services load.** A service with a single log (or all
  logs sharing one timestamp) previously failed the entire dashboard load because
  the auto-range collapsed to a zero-width window; a degenerate range now widens
  to a 1-hour window around the log so the dashboard renders.
- **Analytics pages distinguish "disabled" from "no data".** Origin, security,
  network, dashboard, and charts panels previously showed "Requires Group X to be
  enabled in Fastly logging" whenever a query returned zero rows, so on a fresh
  or low-traffic service every empty panel read as misconfigured. Panels now show
  the enablement hint only when the field group is genuinely off, otherwise a
  neutral "no data in this range".
- **Cold-load charts scan and scale to what's on screen.** On a service with 30+
  days of history, a cold load previously scanned 30 days while the chart x-axis
  showed only 24 hours, squashing every visible bar; custom absolute ranges (date
  picker, chart zoom, saved views) were also ignored. Charts now scan exactly the
  displayed window and honor explicit custom ranges.
- **Sparse bar charts render honest, even-width bars.** A low-traffic or filtered
  COUNT series (a quiet dashboard route, a quiet scoring hour) previously
  collapsed bars to hairlines and turned empty buckets into ambiguous gaps. Empty
  time buckets are now zero-filled so the series reads as an even-width bar chart
  with honest zeros. Latency/throughput scatter series stay sparse, since a
  missing percentile is undefined rather than zero.
- **Top-N values fill available width.** Short top-N values like IPv4 addresses
  were being truncated even with empty space beside them; short values now fit
  fully while long URLs and user-agents still truncate.
- **New logs appear immediately on the dashboard.** A freshly-buffered log could
  show in the status cache while every windowed aggregate returned zero, leaving
  the dashboard looping on "Preparing your data" (and never resolving on setups
  that run no background jobs). When the newest log falls in the window but the
  query returns nothing, the underlying view is now rebuilt once and re-queried
  so the log appears right away.
- **Dashboard no longer hangs on an empty filtered result.** An empty result for
  an active filter is now treated as a legitimate "no rows match" instead of a
  stale-view symptom, ending an expensive retry loop that could leave the
  dashboard stuck on "Preparing your data".
- **Ingest-gap detector uses the correct request baseline.** The ingest-loss
  detector and gap-heal job now measure loss against Fastly's `requests` count
  rather than the `log` counter, which can read a permanent multiple of real
  traffic on services with restart or bot-challenge paths — a phantom gap that
  isn't data loss. The Ingest Accounting / Ingest Gap panels compare ingested
  rows against requests.
- **Service-scoped admin cards follow the active service.** The Ingest Gap and
  Notable Slow Queries admin cards previously kept showing the previous service's
  value (or the backend's default service) after a service switch, because admin
  requests didn't carry the active service id and their caches weren't keyed on
  it. They now re-key on the active service and update immediately on switch,
  including over the live update stream.
- **Shielding anomaly flags gated on sample size.** On a quiet site, the Edge →
  Shield analysis was flagging routes with only a handful of requests as
  suboptimal peering and painting extreme percentiles. Low-sample routes stay
  visible on the map and in the table (painted a neutral grey with an explanatory
  note) but are no longer flagged, so low-traffic routes stop crying wolf.
- **Anomalous low-volume routes are no longer truncated away.** The shielding
  analysis previously ranked and truncated routes by volume before scoring for
  anomalies, so a mis-peered low-traffic route could be dropped before it was ever
  evaluated. The query now keeps the union of top-by-volume and
  top-by-transit-overhead routes, and the table shows a "Showing N of M routes"
  caption.
- **Shielding analysis tolerates missing edge timing.** Routes with a NULL edge
  origin-time-to-first-byte now fall back to time-to-first-byte instead of
  dropping out of the transit-overhead calculation.
- **Edge-only overlay agrees between the shielding map and table.** The transit
  map's "Edge-only logging detected" overlay is now gated the same way as the
  table's empty-state copy, so the map and table no longer disagree on a service
  that has origin/shield fields enabled but no shield traffic in the window.
- **Rolling-window invites no longer widen forward.** A rolling-window analyst
  invite could adopt a future upper bound from the request, widening the
  effective window past the invite's anchor; the upper bound is now capped to the
  anchor.
- **Bootstrap no longer errors when masking is revoked mid-session.** Re-syncing
  an analyst's policy to disable masking previously could cause the next page load
  to fail with a server error; masking state is now read safely.
- **Live share widgets no longer break server-side rendering.** The share page's
  live uptime and lockout countdown are now mounted client-side only, fixing a
  hydration error on `/admin/share`.
- **Provisioning keeps the admin on the admin app after deploy.** A missing token
  on the post-deploy workspace update caused the app to treat a successful
  provision as a dead analyst session and bounce the operator to the analyst
  sign-in; the operator now stays on the admin app.
- **Provisioning wizard: return to Review from the Terraform preview.** The Back
  button now returns to Review from the Terraform & VCL preview, and provision
  mode can deploy correctly from that flow instead of running the ingest-only
  path.
- **Re-provisioning refreshes storage credentials in-process.** A same-process
  teardown-then-re-provision of a service used to keep presenting the deleted
  storage key until the backend was restarted, causing every ingest and read to
  fail auth. The storage client and read connections now invalidate on
  re-provision, teardown, and ingest so the new key is picked up without a
  restart.
- **Provisioning retries transient storage errors.** Occasional Object Storage
  5xx blips during teardown cleanup and table initialization are now retried
  instead of surfacing as a hard error.
- **Keep the admin on a freshly-added service when switching.** Switching to a
  newly-added second service no longer crashes the app shell with a URL↔state
  feedback loop, and the admin stays on the selected service.
- **Service switching no longer crashes the shell.** Two live crashes on service
  switch (a max-update-depth loop from the URL↔state sync, and a hook-count
  change on the security and sessions pages) are fixed, so switching services no
  longer drops the app to its error fallback.
- **Choropleth map no longer repeats on wide screens.** The dashboard map no
  longer renders repeating world copies on wide viewports.
- **Widened shielding-map visibility.** The Edge → Shield transit map now renders
  in more low-signal cases where it was previously hidden.
- **Accessibility: "Updating" badge stays legible.** The live-updating badge now
  pulses only its dot rather than the whole pill, keeping the label text at full
  contrast (WCAG AA) during the animation.

### Performance

- **Origin, security, and performance analytics serve wide windows from
  precomputed rollups.** The origin, security, and performance analytics pages
  now answer their heavy sections (status/PoP/origin-IP/edge and per-minute
  latency time-series on origin; request size, connection reuse, top IPs,
  coverage, and well-known bots on security; the time-to-live distribution on
  performance) from parquet rollups on wide windows, and skip or narrow the
  expensive temporary-table materialization when every requested section is
  rollup-served. Wide-window loads that previously took many seconds now return in
  ~1–4 seconds.
- **Analytics pages paint from a server-rendered first paint.** The dashboard,
  origin, security, and performance pages now prefetch their default selection on
  the server and hydrate the first paint from that cache, so a cold load skips the
  client round-trip and skeleton flash. Query keys are anchored on a reproducible
  relative-range token so the server-seeded cache byte-matches the first client
  key.
- **Network Health page caches across rolling-minute reloads.** The network-health
  response memo is now reachable for the section-scoped requests the live page
  actually sends and is keyed on a quantized relative-range anchor, so a 30-day
  network view serves from cache across minute-to-minute reloads (including on the
  analyst path) instead of recomputing its multi-second pipeline every minute.
- **Insights prewarms the analyst cache.** The Insights cache now warms a stable,
  invite-keyed entry for analysts too, so an analyst opening Insights hits a warm
  cache instead of paying the full computation.
- **Lighter first paint.** The heaviest admin-only data is dropped from the
  initial bootstrap payload, layout-shift on several admin and log pages is cut
  with structure-matching skeletons, the filter bar defers its field-catalog
  queries until opened, and the below-the-fold shielding map defers its
  map-library initialization until it nears the viewport.

### Documentation

- **Install walkthrough video** added to the README, with a clickable thumbnail
  after the intro and a pointer at the top of Quick Start.
- **Custom-fields docs** clarify that enabling "Show in Dashboard" for a field
  turns on automatic rollups for it.
- **Site description** reworded to describe the app as powered by Fastly Object
  Storage rather than analyzing it.

## [2.0.0-beta.1] - 2026-06-23

### Added

Feature work shipped alongside the cleanup sweep below.

- **In-UI scorer redeploy.** The Session Scoring admin page gains a
  **Redeploy** button plus an edge-drift warning when the deployed Wasm
  lags the current scorer build, so re-shipping the edge scorer no longer
  requires the CLI.
- **Scoring dashboard depth.** A fail-open breakdown card surfaces how
  often the edge scorer failed open, a two-phase redeploy log shows code
  vs matrix propagation, and the 1-hour window renders per-minute charts.
- **`enable-scoring` / `disable-scoring` CLI subcommands** for headless
  provisioning and teardown of session scoring on a service.
- **Opt-in RUM Web Vitals collection.** Real-user Core Web Vitals can be
  collected to a rotating JSONL sink, **off by default** and enabled only
  via a host `.env` flag (never baked into the image). Ships with a dev
  analysis script and size-based log rotation.
- **Edge Layer-2 enforcement is an explicit operator opt-in.** The
  route-transition (L2) sub-score is always computed and logged, but its
  contribution to the *enforced* combined score is now gated behind a
  per-service switch instead of an automatic deployment-age ramp, so
  there is no clock-driven monitoring-to-blocking transition. An
  `L2EnforcementCard` near the threshold controls (confirm-dialog gated,
  with a readiness banner) and a `GET`/`PUT /scoring/l2-enforce` endpoint
  drive it; enabling fades L2 in over three days from the moment of
  consent, disabling returns it to observe-only. Deployment age is now
  only an advisory readiness gauge.
- **Request correlation + operations hardening.** Every request now mints
  an app-level request id that threads through the admin/analyst access
  log (which also gains per-request latency) and the slow-query
  attribution, so a slow request can be pivoted to the queries it ran.
  The admin health snapshot gains scheduler-tick liveness, recent
  cross-service cron failures, the effective log/exporter mode,
  config-backup freshness, an opt-in FOS reachability probe, DuckDB pool
  saturation-reject / last-warmed counters, and a traffic-normalized
  scoring fail-open rate; deep `/api/health` additionally degrades on a
  stuck `running` sync row and on errored commit / metadata-sync crons.
  The System Health card surfaces the new tiles and a cron-failure banner.
- **Human-readable PoP and ASN labels everywhere.** Points of presence
  now render as `DEN (Denver, CO - USA)` (code prominent, location muted)
  through one shared component sourced from a `pop_geo` map seeded by
  `/api/bootstrap`. Wired into the Network Quality Avg RTT by PoP, the
  Shielding Analysis table / map tooltip / a11y table, the dashboard
  top-N PoP card, and origin by PoP. The Network Quality Avg RTT by ASN
  breakdown likewise shows the ASN name alongside the number.
  Click-to-filter keeps the raw code / ASN while the label is
  display-only.

### Performance

- **Filter bar** no longer causes a pre-hydration layout shift on load.
- **Session scoring** paints its status panel immediately instead of
  blocking on the analytics fetch.
- **Edge scorer** ships ~13% smaller (491 KB → 429 KB) via a gated
  `wasm-opt` build pass on top of the existing cargo LTO/strip, and opens
  each edge ConfigStore once per request instead of up to four times
  (same fail-open / reject behavior).
- **Scoring sub-fetch** `first_byte_timeout` lowered 200 ms → 100 ms on the
  Compute scoring backend, tightening the worst-case added latency before
  the request fails open.

### Changed

- **CI** gates frontend ESLint with a count-ceiling ratchet so the error
  count can only ratchet down.

### Dependencies

- **Dependency freshness sweep** across all ecosystems. Python in-range:
  `fastapi 0.136.3 → 0.138.0`, `duckdb 1.5.3 → 1.5.4`, `uvicorn 0.48.0 →
  0.49.0`, `sqlalchemy 2.0.50 → 2.0.51`, `pytest 9.0.3 → 9.1.1`, `ruff
  0.15.15 → 0.15.18`. Frontend majors: `@types/node 25 → 26`,
  `react-plotly.js 2 → 4` (drops the redundant `@types/react-plotly.js`);
  TypeScript stays at `^5.9` (the 6.0 bump broke the Docker `npm ci` via an
  `openapi-typescript` peer). Frontend in-range: `next 16.2.6 → 16.2.9`,
  `lucide-react`, `@radix-ui/react-slider`, `@playwright/test`, `vitest`,
  `tailwindcss`. Scorer Cargo lockfile refreshed within constraints.

### Documentation

- **OSS front-door tidy** — the README Quick Start uses `docker compose`
  (v2) with an EOL note for the v1 binary, CONTRIBUTING gains
  development-setup and test-running sections, ARCHITECTURE glosses
  FOS/NGWAF on first use and links a new ADR index, and the orphaned
  `configs/ssh_known_hosts` plumbing left from the removed SSH
  reverse-tunnel is dropped.

### Cleanup

Cleanup sweep applying an in-tree code-quality review. The pattern
across the work was the same on every front: kill the dual maintenance
that survived the package carve-up.

- **Three SQLite pools collapse into one.** `metadata.base`,
  `metadata.usage_log_db`, and `share_db.connection` all owned
  identical thread-local pool machinery (same module globals, same
  PRAGMAs, same init lock). They now share `ThreadLocalPool` in
  `backend/core/sqlite_pool.py`. share_db queries flow through
  `InstrumentedConnection` for the first time — they now appear in
  the Live Query Monitor under `service=__global_share__`.
- **Origin summary's per-query templates collapse into one path.**
  `TEMP_SUMMARY_ROLLUP` + `TEMP_SUMMARY_BY_EDGE` are gone; the live
  and TEMP-table paths both use `SUMMARY_GROUPING_SETS` through a
  shared `_shape_summary` helper that reads rows by column name
  (`cursor.description` dict access) instead of positional indices.
- **Cron job tails consolidated.** Five `finally:` blocks ending in
  the same `if run_id: update_cron_duration ... except: pass`
  boilerplate route through `finalize_cron_duration`. The 16+
  `load_config / 404` preambles funnel through `load_service_config`.
  Three `start_cron_run → spawn-thread → 503` triples collapse into
  one `start_or_resume_cron`. Per-hour bundle walks
  (`collect_hourly_bundle_paths`) and the two cross-package migration
  runners (`run_pending_migrations`) get the same treatment.
- **Mixins + helpers for the small repeated shapes.**
  `LogExtentsMixin` (`earliest_log_at` + `latest_log_at`),
  `OkResponse` (`ok: bool = True`), `_atomic_write_json`,
  `_get_cfg_field`, `client_ip`, `shim_attr`, plus iceberg
  `_iceberg_root_prefix` + `_metadata_pointer_candidates`.
- **`fetch_service_name` now routes through the shared `fastly()`
  client** instead of an inline urllib body. Adds a `timeout` keyword
  to `fastly()` (default 30 s preserves the existing behavior of the
  ~50 other call sites) and the name-fetch call site pins
  `timeout=10` + `max_retries=1` so the cold-path tail caps at ~21 s
  vs the client default of ~127 s. Caller is behind a 300 s name
  cache so steady-state cost is unchanged.
- **`_run_falco_lint` absorbs the falco subprocess plumbing** shared
  by `vcl_utils.lint_log_format` (logging-endpoint VCL check) and
  `vcl_validator.lint_vcl` (scoring-snippet VCL check). Each caller
  keeps its own falco-not-available handling, timeout budget, and
  output parser — the helper only owns the tempfile lifecycle,
  `subprocess.run` invocation, and tempfile-path redaction. The two
  use cases stay distinct on purpose (logging is best-effort, scoring
  is a security boundary).
- **Comment hygiene pass.** Removed stale, redundant, and duplicate-divider
  comments across the tree and condensed embedded changelog blocks (the CI
  coverage gates and the ESLint ceiling) down to their conventions. Load-bearing
  rationale, incident references, and functional directives are left intact.

### Fixed

- `start_proxy_server` race that surfaced as
  "proxy server is not running" when N reader threads called
  `get_connection` simultaneously on a cold process. Concurrent
  first-callers now serialise the thread-start decision and wait
  on `_READY` outside the lock so every caller reads `_PORT` after
  the server has bound.
- `get_metadata_storage_stats` + `cleanup_metadata` silently
  ignored the `usage_log` table on every fresh service after
  the v2.0 per-service-file split — the helpers still read
  `metadata.db`. Routed through `usage_log_db` so admin storage
  stats and the retention cleanup job actually see the rows.
- `sync.py` cron tail used to emit a misleading
  "View refresh + warm: Xms" status event even on failure (the
  success log sat outside the try/except). The shared
  `refresh_view_and_warm_pool` puts the success log inside the
  try/except so failure means no event.
- `start_cron_run` non-sync task types fell back to
  `cron_compact.log_retention_days` via a buggy ternary; the
  promoted `_TASK_TO_CRON_KEY` mapping plus a default 7-day
  fallback gets the correct retention applied per task.
- `query_instrumentation._safe_weakref` silently no-op'd the
  memory probe when wrapping non-weakref-able cursors; promoted
  the registry-version's strong-ref-closure fallback so the probe
  always tracks.
- `local_compaction` hour-tier tests were flaky on any clock more
  than 30 days past the hardcoded sample dates — the fixture now
  pins both `_DAILY_TIER_AGE_DAYS` and `_WEEKLY_TIER_AGE_DAYS` so
  neither tier sweeps the test partitions out from under the
  assertions.
- **Edge scorer** now treats encoded slashes (`%2F`) as data during
  route normalization, so paths with encoded slashes are scored
  correctly (the Wasm package was rebuilt to ship it).
- **Scoring sub-fetch** is no longer inspected by NGWAF, fixing
  intermittent `compute-unavailable` 406s on the scoring path.
- **Analyst idle timeout now tracks real interaction only.** A share
  session's 2-hour idle clock was being reset by activity the analyst never
  initiated, so sessions could stay alive indefinitely: the SSE
  log-extents stream re-stamped activity every ~30s even while the tab was
  backgrounded, background telemetry beacons and react-query refetches
  counted as interaction, and an IP change (rotating NAT) reset the clock
  on its own. Requests now carry an `X-User-Active` signal so the
  middleware refreshes the idle deadline only on genuine analyst activity;
  SSE streams are exempt; IP roaming updates the session's recorded address
  without bumping the clock; and the lightweight heartbeat doubles as the
  activity channel for an otherwise-quiet dashboard. The analyst access log
  records the signal per request (`act`, `idle_touch`) for observability.
- **Sync cron** never leaves an orphaned `running` row behind; a
  per-task orphan timeout reaps a stalled run instead of wedging
  ingestion on every subsequent tick.
- **Origin analytics** holds its pool connections until every parallel
  gather worker finishes, fixing a connection released mid-query.
- **Stale browser tabs** no longer get stuck in a hard-reload loop.
- **Scoring admin** modal copy matches the orchestrator's actual
  behavior, the redeploy modal text is no longer clipped, and status
  fetches are type-clean.
- **Structured logging** surfaces stdlib `extra=` fields via
  `ExtraAdder`.
- Restored the router-independence import contract.
- **Backend failures surface inline instead of silently.** Where a 5xx
  previously left a forever spinner or fabricated zeros (the network map
  and Network Quality section, the dashboard chart and bot cards, session
  detail, the `/usage` storage + cost cards, and the logs import / commit
  quick actions) the UI now shows an inline alert with a Retry. The
  analytics request `sections` and several response reads are typed
  through the generated OpenAPI schema so a backend rename is a compile
  error rather than a blank card.
- **Interrupted-delete raw files** are now reclaimed automatically. A
  restart between the dedup-ledger write and the object-storage delete
  used to strand a raw `.gz` in the bucket, invisible until the daily
  ledger trim re-listed it; the sync reconcile now re-issues the delete
  from durable state (default-deny: only files positively proven to hold
  ingested data are removed).
- **DuckDB object-cache leak** that grew unbounded under continuous
  buffer + compaction file churn (and OOM-killed the container) is bounded
  by a periodic, timeout-guarded instance-recycle job that briefly drains
  connections — reads queue rather than fail during the drain — to free the
  cache and let it re-warm lazily. Off by default
  (`DUCKDB_RECYCLE_INTERVAL_MIN=0`), opt-in via deploy config.
- **SQLite connection leak that drove the backend into an OOM-restart
  loop.** A jemalloc heap profile attributed ~93% of the unbounded RSS
  growth to live SQLite memory: the cron watchdog built a fresh
  `ThreadPoolExecutor` every tick, and `ThreadLocalPool` opens one
  `check_same_thread` connection per thread (metadata + usage-log) pinned
  in a process registry until shutdown — so every tick orphaned ~2
  connections (WAL file descriptors + up-to-64 MB page caches) that never
  freed, accumulating hundreds within minutes. `ThreadLocalPool` now reaps
  connections whose owning thread has exited (swept on cold-open) and
  guards cached borrows against an out-of-band close; the cron watchdog
  reuses a single bounded executor instead of one per tick.
- **Process-level memory guard** converts a destructive cgroup
  OOM-`SIGKILL` into a graceful self-restart (SIGTERM → uvicorn drains →
  container restart) when RSS crosses a configurable ceiling
  (`BACKEND_GRACEFUL_RESTART_RSS_MB`, opt-in). The DuckDB connection pool
  is also capped to the host core count with a lower recycle RSS threshold
  so the object-cache recycle can actually drain and free the cache.
- **Fresh-install provisioning** no longer aborts step 1 with
  "No active service — request aborted"; `/api/provision` is exempt from
  the API client's serviceless-request guard (regression-pinned).
- **Custom Caddy image build** gives its rate-limit build a distinct image
  tag instead of overwriting its `caddy:2-alpine` base, so the privileged
  build steps no longer fail on rebuild.
- **Total Logs badge and ingest skipped-files counts** no longer drift
  upward over time. The `ingested_files_summary` rollup is now recomputed
  after a retention trim (it was incremented on ingest but never
  decremented on delete), per-run `skipped_files` reports the files
  actually re-seen and skipped rather than the dedup-ledger size, and the
  Total Logs badge prefers the last-known-good row count during a
  transient catalog rebuild.
- **Access logs render through structlog.** Uvicorn's private
  `uvicorn.access` handler used to emit plaintext access lines (carrying no
  `trace_id`) into the otherwise-structured stream; they are now bridged
  through the shared root handler at startup so every post-boot line is
  structured.
- **Accessibility remediation** brings every loopback-reachable admin and
  usage route axe-clean: light-mode status / destructive color tokens
  deepened to clear 4.5:1 contrast, a duplicate focus trap removed from
  dialogs so nested selects stay open, and the cost-calculator number
  inputs given accessible names.

### Removed

- `backend/utils/retry.py`, `backend/utils/cdn.py`,
  `backend/core/settings.py` (Path-B removal of three migration
  scaffolds that never adopted in tree). `pydantic-settings`
  removed as a *direct* dependency from `pyproject.toml` (it was the
  sole first-party consumer; it remains in `uv.lock` transitively via
  the OpenAPI spec/schema validators used in tests).
- Legacy `usage_log` DDL + 3 triggers + 4 indexes in
  `metadata.base._SCHEMA` (the table moved to its own per-service
  file pre-2.0). `migrate_from_metadata_db` and
  `_migration_003_rebuild_usage_log_hourly_summary` deleted.
- Scrypt passcode verify path + `PASSCODE_DEFAULT_ALGO_KEY` +
  `_migration_003_passcode_algo_marker` (cutover happened
  pre-2.0; fresh installs have no scrypt rows).
- `TunnelState.use_tunnel` + `tunnel_url` + the
  `share_admin` response keys that exposed them (always
  False/None since v2.0 deleted the SSH path).
- Per-checkin `_cleanup_temp_tables` sweep in `duckdb_pool` —
  the "safety net" was unreachable because the failure path
  discards the connection before the sweep can run.

### Release overview

Architecture cleanup release. The post-`v1.2.0` perf branch closed the
worst read-path latency by stacking remediation on top of an
architecture that wasn't designed for the workload; this release pays
that down. The largest backend files were carved into per-concern
packages, telemetry moved to OpenTelemetry + structlog, tenancy got a
typed `RequestContext` boundary, frontend hydration warm-up hacks were
replaced with policy, and the test + type gates ratcheted to a level
that catches regressions on the way in. Composite endpoints land as a
hard cutover — frontend + backend ship together, granular endpoints
deleted.

### Architecture

- **`backend/core/iceberg.py` (4,232 LOC)** → `iceberg/` package
  (`view`, `catalog`, `warehouse`, `manifest`, `fs`, `_core`,
  `buffer`, `ddl`, `snapshot_cache`, `dedup`, …). Custom
  `FosFsspecFileIO(FsspecFileIO)` + `CachedFosS3FileSystem(S3FileSystem)`
  subclasses replace 5 of the 6 historical `s3fs` monkeypatches;
  only the `ThreadPoolExecutor.submit` ContextVar wrapper remains
  (see [MONKEYPATCHES.md](MONKEYPATCHES.md)).
- **`backend/scheduler.py` (2,843 LOC)** → `backend/cron/` package
  with `scheduler`, `decorators`, and per-job modules under
  `cron/jobs/` (`sync`, `commit`, `compaction`, `optimize`, `expire`,
  `metadata`, `gap_heal`, `rollup_compact_daily`). The scheduler
  picks the **separate-pool** isolation strategy based on Phase 1
  thread-wait telemetry; the deferred-view-cache-invalidation hack
  is gone.
- **`backend/core/metadata_db.py` (3,168 LOC)** → `backend/core/metadata/`
  package with concern-partitioned mixins (`base`, `alerts`, `views`,
  `ingest_log`, `cron_log`, `asn_cache`, `usage_log`, `reconciliation`,
  `state`). `metadata_db.py` becomes a thin backward-compatible shim.
- **`backend/utils/tunnel.py` (1,022 LOC)** → `backend/utils/tunnel/`
  package (`manager`, `session`, `rate_limiter`, `state`,
  `fingerprint`). The SSH-to-localhost.run path is **deleted entirely**
  (~400 lines): no more SSH subprocess + sleep-listener + reconnect
  state machine. Direct-mode only; production has always used direct.
- **`backend/core/share_db.py` (1,312 LOC)** → `backend/core/share_db/`
  package (`connection`, `schema`, `invites`, `sessions`, `audit`,
  `passcode`, `tos`, `settings`). `argon2-cffi` replaces `scrypt` for
  passcode hashing.
- **`backend/routers/admin.py` (1,650 LOC)** → `backend/routers/admin/`
  package (14 sub-modules: `pop_locations`, `ingest`, `trees`,
  `downloads`, `sync_status`, `compaction`, `health`,
  `log_accounting`, `iceberg`, `bot_sources` + shared
  `_helpers` / `_dir_size` / `_router`).
- **`backend/core/rollups.py` (2,045 LOC)** → `backend/core/rollups/`
  package (8 sub-modules: `_common`, `time_series`, `sessions`,
  `hour_bundles`, `day_bundles`, `recompute`, `wellknown_bots`).
- **`RequestContext` replaces `AnalyticsDeps`** ([`backend/core/request_context.py`](backend/core/request_context.py)).
  Tenancy is enforced at context construction; routes never parse a
  `service_id` from a path param. The security-load-bearing private
  `read_only` attribute is now structurally unexposable as a query
  param.
- **Composite endpoints + hard cutover** — `dashboard/bundle`,
  `security/bundle`, `network/bundle` ship together with the frontend
  swap. Granular per-card endpoints deleted, `_meta_con` parallel path
  dropped, `is_cached/_is_cached` alias collapsed,
  `AnalyticsDeps = RequestContext` shim removed. Top-5 backend files
  now ≤ 1,461 LOC; no backend file > 1,500.

### Telemetry, observability

- **OpenTelemetry** (`opentelemetry-api/sdk` +
  `fastapi`/`botocore`/`aiohttp` instrumentors) replaces the four
  fragmented custom telemetry surfaces. Console exporter ships by
  default; backends (Jaeger / Tempo / Honeycomb / …) are a
  deploy-config decision, not part of this release.
- **`structlog`** wires `trace_id` + `span_id` into structured log
  output via a custom processor.
- **`process_context_scope` + `_ACTIVE_CONTEXTS` mirror kept** at
  [`backend/utils/telemetry.py`](backend/utils/telemetry.py). OTel context
  propagation uses Python ContextVars under the hood, which inherit
  the cross-thread limitation (fsspec iothread, pyiceberg
  ThreadPoolExecutor) the manual mirror was built to solve; removing
  the mirror would re-introduce the ~80%-NULL telemetry bucket
  observed on 2026-05-20. Docstring + plan entry document the
  reasoning.
- **`RequestTelemetry`** thin wrapper owns section spans, query
  attribution, call log, and the custom `app.thread_wait_ms` metric
  that fed the Phase 6 separate-pool decision.

### Reliability, perf

- **`aiodns` + `asyncio.gather` + bulk-transaction sqlite writes** in
  [`backend/utils/rdns_cache.py`](backend/utils/rdns_cache.py) replace the
  serial-blocking `socket.gethostbyaddr` loop that wedged the sync
  worker for minutes on bulk lookups.
- **`tenacity`** decorator-based retry replaces ad-hoc try/except loops
  for Fastly API + NGWAF + SQLite WAL-busy paths; centralised policy
  on `Settings`.
- **`pydantic-settings`** centralises env-var reads + boot validation
  (the "TRUSTED_PROXY_IPS required in prod" gate is now a pydantic
  validator).
- **`cachetools`** replaces `bounded_cache` / `rdns_cache` /
  `ngwaf_bot_cache` in-process LRU/TTL implementations.
- **Structured `.tf.json`** generation replaces f-string HCL +
  `_hcl_escape` regex (`backend/utils/terraform_gen.py`), eliminating
  the custom-HCL escaping injection vector.
- **`orjson` via FastAPI `ORJSONResponse`** for ~5–10× faster JSON
  serialisation on composite endpoint payloads.
- **`rich` + `typer`** for the provision CLI; `httpx` everywhere
  except `telemetry_proxy.py` (which stays on `aiohttp` for the proxy
  server role).
- **`nuqs`** as the URL state source on the frontend, replacing the
  custom Zustand/Effect sync hooks that produced hydration desync on
  refresh.
- **`session_scoring._cached`** clears `_inflight` on the cache-hit
  path too, not only on producer-path teardown — concurrent callers
  on a hot cache key no longer leak the inflight registration when
  the producer finishes before they wake up.
- **`iceberg/buffer.tombstone_buffer_files`** logs + skips on
  marker-write failure (the immediate-`os.remove` fallback re-opened
  the in-flight-query race the tombstone grace window exists to
  close). Pair regression test pins the contract.
- **`DROP TABLE IF EXISTS` identifier quoting** at 11 temp-table
  cleanup sites so the drop tolerates reserved keywords / hyphenated
  service slugs that would otherwise raise.

### Trust topology, middleware

- **Middleware order asserted at boot AND in tests** — the
  multi-paragraph prose comments in `main.py` were replaced with
  one-line `# INVARIANT` markers + a boot-time crash if
  `app.user_middleware` doesn't match the declared tuple. Snapshot
  tests cover Caddy + docker-compose middleware order too.
- **`@pytest.mark.security_regression` marker + monotonic-count CI
  gate** (floor: 24, from `audit-findings/`). Every test covering a
  verified security fix carries the mark; a refactor cannot silently
  drop coverage of a known fix.
- **Trust-topology snapshot tests** pin Caddy `@from_fastly` matcher,
  XFF forwarding, `/share-login` rate-limit, and the backend
  `--forwarded-allow-ips=127.0.0.1` flags.
- **`raise_internal(logger, exc, code, status)`** replaces
  `raise HTTPException(detail={"error": str(e)})` at every backend
  except site that previously echoed the original exception message
  to the client. Detail is now `{"error": <code>, "error_id": <8-hex>}`;
  the full exception lands in the server log with the same
  `error_id` so operators triage without the upstream body / token
  fragments leaking on the wire.
- **`escape_sql_literal`** applied at every `read_parquet()` /
  `glob()` site that interpolates a computed path. Closes the
  injection surface a partially-validated path could open through
  DuckDB's `read_parquet()` glob expansion.
- **Caddy container drops privileges** — `caddy/Dockerfile` adds
  `USER caddy` (the base image ships the user). Caddy is the only
  externally-facing socket and binds nothing below port 1024, so
  there's no reason to keep `root` in the runtime.

### Frontend

- **RSC/CSR boundary** documented in `app/_routing.md`. The
  hidden-Plotly + hidden-MapLibre + `setTimeout` warm-up hacks are
  dropped; replaced with `modulepreload` + the styledata-event swap
  pattern.
- **16 frontend files > 500 LOC split.** `ProvisionWizard.tsx`
  (3,582 LOC) → `wizard/steps/*` + `state.ts` + `api.ts`;
  `app/logs/page.tsx` (2,136 LOC) → `_sections/*` + `_state.ts`.
  `app/admin`, `app/dashboard`, `app/alerts`, `app/security`, etc.
  all post-split < 500. **No frontend file > 499 LOC.**
- **Live Query Monitor** — live-first sort, peak-memory column,
  keyboard shortcuts, URL-persisted filters, per-run inline expand
  for ×N cron-grouped rows, ≥ 30 s stuck-query pulse, copy-SQL,
  sound notification removed.
- **Operations Overview cards** on the admin landing page surface
  ingest gap + live query activity + slow-query count so the things
  operators actually care about don't live three clicks deep.
  Tone-coded (default → attention → warning → critical) so a
  sustained_loss event jumps out.
- **Stable React keys on dynamic lists** — `DebugPanel`, `CronLiveLog`,
  the network metro leaderboard, the query toolbar, and the
  custom-field drawer now key off a stable identity instead of array
  index. `useSSE` attaches a monotonic `_id` to each line so
  append-only feeds (cron progress, query streams) keep stable keys
  across re-renders.
- **Accessibility pass** — `FieldGroups` and `FileBrowser` disclosure
  widgets are real `<button>`s with `aria-expanded`; `SSEModal` uses
  the base-ui `Dialog` render prop instead of a non-keyboard `<div>`
  wrapper; per-row "view audit logs" buttons carry an `aria-label`
  that includes the row's email so screen readers don't read 20
  identical "View" buttons in a row.
- **`fetchWithTimeout` helper** (30 s default; heartbeat tightens to
  10 s) applied to `share-login`, `acknowledge`, and
  `useAnalystHeartbeat` so a hung request surfaces as an error
  instead of an infinite spinner.

### Quality gates

- **Backend coverage gate `--cov-fail-under` 78 → 85** (final actual
  85.05 %). Per-module test waves cover every cleanup-touched module
  + the post-split `rollups/` and `admin/` packages.
- **Frontend coverage gate `coverage.thresholds.lines` 44 → 58**
  (final actual 61.66 %).
- **`tool.mypy.overrides` `ignore_errors` list: 36 modules → 0.**
  Every backend module type-checks under default settings. Three real
  bugs surfaced + fixed during the burndown
  (`repositories/network.py:260` was passing the DuckDB connection
  where `get_asn_names` expected `service_id`;
  `routers/share_auth.py:125,203` had an `iso_z_now() and 24*60*60`
  cookie `max_age` expression where the `and` was a no-op leftover;
  `routers/admin.py` shadowed loop variable that defeated narrowing).
- **mypy per-module strict block: 19 modules opted in**
  (`disallow_untyped_defs` + `disallow_incomplete_defs` +
  `check_untyped_defs` + `warn_return_any` + `warn_unused_ignores`).
  Live-query-monitor surface + every module the v2.0 waves added
  tests for. Full mypy: 221 source files clean.
- **Load-harness CI step**: `scripts/emit_perf_latest.py` runs a
  100K-row synthetic DuckDB workload (~2 s wall); `scripts/perf_gate.sh`
  fails on > 50 % regression vs `tests/perf/baseline.json`. Production
  targets (≤ 2,800 / ≤ 1,900 ms on 36 M rows) documented in
  `baseline.json` `production_targets_comment` and validated by the
  manual `scripts/dev/loadtest_probe.sh`, not the CI gate (GH Actions
  runner variance is too high).

### Operations, portability

- **VM-agnostic deploy runbooks** at
  [`docs/deploy/`](docs/deploy/): `aws_ec2.md`, `azure_vm.md`,
  `gce.md`, `generic_linux.md`. Storage stays Fastly Object Storage
  (S3-compatible API; boto3 keeps working). GCE-specific wording in
  comments renamed to "cloud" / "VM" (the link-local
  169.254.169.254 metadata IP is identical on AWS + GCE; the SSRF
  gate works on both).
- **`scripts/refresh_fastly_cidrs.py`** pulls
  `api.fastly.com/public-ip-list` and rewrites the Caddy
  `@from_fastly` block. Manual or cron-scheduled.

### Breaking

- **Composite-endpoint cutover.** The granular per-card dashboard
  endpoints `/api/dashboard/raw` and `/api/dashboard/top_n` are
  **deleted**; callers must use the composite (`/api/dashboard/bundle`).
  (`/api/dashboard/aggregates` remains.) External integrators were
  notified 24–48 h ahead.
- **Removed endpoints.** `/api/sources`, `/api/dma.json`,
  `/api/performance/origin-ts`, and `/api/services/{id}/rename` are
  **deleted**.
- **GET → POST.** `/api/provision/check-fos`, `/api/provision/lake-info`,
  and `/api/share/claim/{token}` moved from GET to **POST** so a
  cross-origin `<img>` / prefetch GET can no longer trigger their
  side-effecting work.
- **PATCH alias dropped.** `/api/services/{id}/cron-settings` and
  `/api/services/{id}/logging-settings/update` are now **POST-only**; the
  former PATCH alias is removed.
- **`AnalyticsDeps`** alias for `RequestContext` is removed.
- **`is_cached` / `_is_cached`** alias on `BaseResponse` is removed
  (`is_cached` is the canonical name).
- **SSH-to-localhost.run analyst sharing** is removed. The laptop-
  admin tunnel use case is no longer supported; production has always
  been direct-mode against the Fastly+Caddy public URL.

[2.1.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v2.1.0
[2.0.0-beta.2]: https://github.com/fastly/fastly-log-analytics/releases/tag/v2.0.0-beta.2
[2.0.0-beta.1]: https://github.com/fastly/fastly-log-analytics/releases/tag/v2.0.0-beta.1

## [1.2.0] - 2026-06-09

Dashboard performance overhaul plus capability-focused security hardening. Cold and warm dashboard loads drop from seconds to sub-second on large services; sustained concurrent load no longer wedges the backend. Read-path I/O is structurally cut by a per-service DuckDB connection pool, a per-minute time-series rollup bundle, size-capped bin-packing local compaction, composite endpoints that collapse multi-card admin pages into one request, and a frontend pre-warm / hover-prefetch pattern that makes navigation feel instant. Security hardening tightens cross-tenant boundaries, closes a ContextVar propagation hole in the s3fs proxy hook, removes a secret-in-URL leak on downloads, and adds strict validation across the destructive-op surface.

### Performance

Structural:

- **Per-minute time-series rollup bundle** (`backend/core/rollups.py`) precomputes a hour-bundled per-minute aggregate for the dashboard chart, eliminating the wide Iceberg scan on chart render. Generated alongside the existing Top-N rollups.
- **Per-day compaction tier for rollups** — closed days are compacted into per-day parquet files; the reader prefers the per-day file and falls back to hourly only for the current day, cutting file-handle pressure on long-running services.
- **Size-capped bin-packing local compaction** ([backend/core/local_compaction.py](backend/core/local_compaction.py)) replaces single-file daily/weekly rollups with sequential bin-packing capped at `_MAX_PARTITION_BYTES` (default 256 MB). Hourly partitions older than 7 days bin-pack into daily files; daily files older than 30 days bin-pack into weekly files. DuckDB query parallelism is preserved on multi-month services where the prior single-file approach degraded to scan-of-one-huge-file.
- **DuckDB connection-pool tuning knobs** — `DUCKDB_POOL_CONN_MEMORY_LIMIT` and `DUCKDB_POOL_CONN_THREADS` env vars cap per-pool-connection memory and thread count so 8 concurrent queries don't oversubscribe physical cores or balloon RSS. Pool view-binding moved outside the `Condition` lock to eliminate a deadlock under stale-Iceberg-snapshot reload.
- **Composite read endpoints** collapse multi-card mounts into single requests:
  - `POST /api/scoring/dashboard` (8 per-card requests → 1)
  - `GET /api/scoring/analytics` and `GET /api/scoring/config`
  - `GET /api/network-health` now includes shielding analysis
  - `POST /api/origin/aggregates` (new) batches the origin page's per-card queries
  Per-card endpoints stay mounted for back-compat; the frontend opts into composite where it makes sense.
- **Parquet ingest sort key** changed to `(timestamp, ip)` so sessions queries can stream-merge on `ip` instead of materialising a temp table — ~2× speedup on sessions dashboards.
- **`ingested_files.file_date` column + `(source_name, file_date)` index** added via numbered SQLite migration. The log-accounting fast path uses the index to bucket by day without scanning every row; `metadata_db.get_node_count_avg` and `get_log_accounting_counts` split on it.
- **Iceberg commit hygiene** — buffer files are tombstoned and removed on the next pass instead of unlinked inline at commit time, removing a commit-path stall. `optimize_table` adds `union_by_name` + retry-on-CAS-conflict to silence the nightly schema-evolution warning.
- **Bootstrap stale-while-revalidate** — `/api/bootstrap` returns cached dir-stats immediately and refreshes in the background; views are folded into the response so the admin page doesn't issue a follow-up.

Tuning:

- Dashboard live-hour TEMP TABLE shared across CTEs; Python-side bot match + memoised `ngwaf_top` cut DuckDB round-trips.
- Insights coalesce four city/region/country queries into one and four URL-keyed insights into one CTE (Option C pattern).
- Sessions split the monolithic CTE into measurable stages and eliminate the temp-table materialisation on the hot path.
- Origin summary combines two sequential scans into one via `GROUPING SETS`.
- Cron-runs `since_id` delta-poll param + frontend wiring on `/logs recentCrons` so the page only fetches new events.
- Admin usage-log visibility-gates its 30s tick and rewrites the latest-per-task SQL to skip the full join.
- Admin shielding banner endpoint trimmed; share-status `staleTime` tightened.
- Bot-source cache: 60s TTL on the recursive cache-dir `scandir` (was 200–1500 ms per `/api/bootstrap`).
- React-Query: skip 4xx retries; hooks lifted out of insights / ReportLayout render-props so each page mount re-uses one query instance instead of re-mounting on every parent render.

Frontend:

- **`starlette-compress` replaces `GZipMiddleware`** — backend now negotiates `br` / `zstd` / `gzip` (was gzip-only). Modern browsers get brotli; rendered-text payloads drop ~25 % on the wire.
- **Keep-alive on Next.js http/undici global agents** so the proxy reuses TCP connections to the FastAPI backend instead of new-handshake-per-request.
- **Pre-warm + lazy-mount pattern** — plotly + maplibre-gl + `world.geojson` are pre-warmed on `AppLayout` mount via hidden one-point charts; the visible chart hydrates from the warm module cache instead of triggering a fresh import on first render. `LazyMount` + `PlotlyChart` start `visible=false` to avoid the hydration-mismatch warning that came with the prior eager-mount pattern.
- **Hover-prefetch sidebar links** so the destination's data warms before the click commits.
- **Per-insight skeleton cards on first paint**; full skeleton rendered from `CARD_CATEGORIES` on the dashboard.
- **Modulepreload for the plotly chunk** via a build-time-generated preload manifest (`scripts/build-preload-manifest.mjs` + `lib/preload-manifest.ts`); restores plotly's preload without re-introducing the nav-lag the first attempt caused.
- **Drop `force-dynamic`** on routes that don't need it; root layout opts out of build-time SSG so the preload manifest is read at request time.
- **`/geo/*` static assets cached aggressively**; `PlotlyChart` dynamic-import on `/network`.
- **`SystemHealthCard` polling moved to 1 s** for live attack/load feedback now that the endpoint is cheap.
- **`useNowMs` reuse** — multiple visible-tick components (countdowns, "X seconds ago") share one interval.
- **Map style-data listener** replaces a 100 ms `setTimeout` poll.

### Reliability

- **Multi-worker login loop fixed** — `tunnel.py` now rehydrates a share session on-demand from SQLite when an in-memory cache miss happens on a different uvicorn worker. Previously, login on worker A would loop because worker B couldn't see the freshly-minted session.
- **DuckDB lock conflict resolved** between the connection pool and cron writes — `get_connection` forces `read_only=False` so pool readers and cron writers no longer trip DuckDB's "different configuration" error on the same file.
- **Stale-view self-heal** — `QueryRunner` clears `_view_cache` before the `force=True` rebuild on the post-empty recovery path so the next query doesn't see the stale schema.
- **Iceberg s3fs proxy hook** falls back to the process-global source so the hook always registers, even when the ContextVar is empty (e.g. cold-start LIST before any `_get_catalog` has fired).
- **Top-N current-hour merge** — a silent `ImportError` was dropping the current-hour merge; restored with an explicit fail-loud import.
- **Rollup compaction** — `run_id` threaded through the error branch and the compaction step now uses an in-memory DuckDB so a corrupted on-disk catalog can't wedge the cron.
- **Dashboard response cache** — write to `is_cached` (not the aliased `_is_cached`) so Pydantic doesn't drop the flag on serialise.
- **Dashboard cache hit rate** — disabled the 30 s response-level cache that was masking the rollup wins for fast-changing queries.
- **Usage-log rollup drift** — reconcile cycle changed from DELETE+INSERT to UPSERT so concurrent flushes can't lose rows.
- **Botnet insight investigate link** filters only the queried column, not all of them.
- **`expire_snapshots`** updated for pyiceberg 0.11.1 API and now emits `cron_runs` telemetry.
- **Proxy compatibility** — switched from `middleware.ts` to `proxy.ts` for Next.js 16; restored the Caddy-marker middleware that the upgrade broke.
- **Telemetry response middleware backstop** ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py)) auto-injects `_debug_queries` / `_debug_calls` / `_is_cached` into JSON-dict responses that bypassed `BaseResponse.with_telemetry`, so newly-added endpoints don't silently blank the Debug Panel.

### Security

Capability-focused hardening across the backend and frontend trust boundaries.

- **Cross-tenant ContextVar leak in the s3fs proxy hook** closed. PyIceberg writes parquet via a `ThreadPoolExecutor`; ContextVars don't propagate to executor workers by default, so the prior fix used an endpoint-keyed global registry that was vulnerable to overwrite when two tenants shared an endpoint URL. Replaced with a global `ThreadPoolExecutor.submit` monkeypatch that wraps the callable in `contextvars.copy_context()` — matches asyncio's `loop.run_in_executor` semantics. Documented in [MONKEYPATCHES.md](MONKEYPATCHES.md) §6.
- **Path-param service-scope desync** — analyst sessions could supply a `service_id` path param that didn't match their session scope on a handful of mutation endpoints. Centralised the check via a router-utils helper invoked on every scoped route.
- **Secret-in-URL leak on downloads** — the download endpoint previously embedded the shared CDN secret in the redirect URL where it could land in browser history / referrer headers. Switched to a signed short-lived bearer that's stripped before the redirect.
- **Strict input validation** on the destructive-op surface — provision teardown, NGWAF workspace mutations, scoring threshold + enforce-status-code + recv-exclusion-regex changes — runs through length caps, character allowlists, and (where applicable) `falco` static analysis before any VCL ships.
- **CSRF gates** — moved GET→POST on `logging-settings/update` and sibling state-changing endpoints that were addressable via GET.
- **Authorisation tightening** — share-admin endpoints reject the Caddy-marker header from non-Caddy paths; `claim_token` path consolidated under a single atomic UPDATE so concurrent claims can't both succeed.
- **Cross-tenant cache audit** — re-verified that every per-tenant cache key includes `service_id`; closed two missing entries on insights and origin paths.
- **Thread leak fix** — the share-login flow was leaking a daemon thread per failed login on multi-worker setups; the new on-demand SQLite rehydration replaces the thread entirely.
- **Terms-of-service bypass** — share-login `/acknowledge` now fetches the active TOS version and refuses acknowledgement of a stale one; frontend was sending a hardcoded version.
- **Telemetry-proxy diagnostics** for silent 400s (`Missing X-Fos-Target`) and unclassified `list_objects_v2` calls; preserve `Content-Type` so downstream compression always fires; preserve multi-valued response headers.

### Tests

- 3500+ backend tests (+450).
- 290+ frontend vitest tests (+25).
- New coverage: `tests/core/test_duckdb_pool.py`, `test_local_compaction.py`, `test_rollups_compaction.py`, `test_rollups_hour_bundling.py`, `test_iceberg_helpers.py`, `tests/services/test_service_manager.py`, `tests/utils/test_sql_validator.py`, `test_telemetry_response_middleware.py`, `test_router_utils.py`, `test_state_sync.py`, `test_terraform_gen.py`, plus router coverage for the new composite endpoints and the destructive-op-auth surface.
- `make ci` green: lint + format + mypy + pytest + vcl-test + verify-deps + typecheck-frontend + test-frontend + osv + secret-scan.

### Infrastructure

- **Synthetic load generator** ([scripts/loadtest_generator.py](scripts/loadtest_generator.py)) and **read-path probe** ([scripts/dev/loadtest_probe.sh](scripts/dev/loadtest_probe.sh)) for reproducible perf measurement against local Parquet+Iceberg.
- **Two-pass next build** in the frontend Dockerfile so SSG sees the correct plotly chunk hashes; preload-manifest scanner runs after `next build` to capture them.

### Documentation

- `AGENTS.md` — added Key Systems entries for the DuckDB connection pool, the hourly Top-N rollup pipeline, and the response telemetry middleware. Updated the local-compaction section to reflect the bin-packing tiers.
- `MONKEYPATCHES.md` — documents the new `ThreadPoolExecutor.submit` patch.

[1.2.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v1.2.0

## [1.1.0] - 2026-06-03

Edge session scoring. Every request is classified in real-time at the edge by a Fastly Compute service that runs an L1 (cookie compliance + timing rules) + L2 (PageRank-trained transition matrix) scorer, returning a combined 0-100 score that lands in DuckDB for analyst review. Operators can label sessions, watch live ROC-AUC, retrain the matrix, roll back to a prior matrix, rotate the AES cookie key, and push a hard enforcement threshold that rejects flagged requests at the edge with an operator-chosen HTTP status code (default 429).

### Highlights

- **Edge scoring** — Fastly Compute scorer + 6-snippet VCL preflight pattern (recv/pass/fetch/deliver/miss/enforce), AES-GCM-encrypted session cookie carrying rotating sid + transition state, `fastly.ddos_detected` gate so Compute is bypassed under L7 attack.
- **Admin UI** at `/admin/session-scoring` — StatusPanel with live AUC against accumulated labels, ScoringHealthCard with fire rate / score distribution / top reasons / matrix-staleness alert, ThresholdSlider with counterfactual flag/pass preview + precision/recall + commit-threshold persistence, RocPrCurves with ROC + Precision-Recall plots, TopFlaggedTable + LabelsTab with click-to-view-events per sid, RetrainButton (DuckDB traces → train.py → publish matrix to FOS), SinceHoursPicker driving all six cards on one shared time window.
- **Labels CRUD** — POST/PATCH/DELETE per-sid labels (good/bad/neutral) feed `evaluate_from_persisted_scores` to compute live ROC-AUC. Min-samples gate (≥3 per class) prevents noisy display.
- **ROC + PR curves** + per-reason AUC breakdown (split by L1/L2 rule: cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Composite `/scoring/dashboard`** endpoint collapses the 8 per-card requests into one in-flight-collapsed payload; the existing per-card endpoints stay mounted for back-compat.
- **`edge_score_reason` virtual field** — CSV-split via DuckDB `unnest(string_split(...))`, top-N cards + click-to-filter same as NGWAF signals.
- **FOS matrix persistence** — `enable_scoring` publishes the trained matrix to FOS; backend auto-fetches on startup (no more per-host scp).
- **Matrix version history + rollback** — every publish snapshots the prior matrix to `iceberg/meta/scoring_matrix_history/{version}.json`; new `/scoring/matrix-versions` lists them and `/scoring/matrix-versions/{v}/restore?confirm=true` copies a historical matrix back. AUC reflects the rollback immediately; Wasm at edge keeps the embedded matrix until `deploy_wasm.sh` re-runs (deploy_hint surfaced).
- **Threshold enforcement (live blocking)** — operator commits a threshold, scorer reads it from `scoring_config` ConfigStore, emits `X-Edge-Score-Enforce: 1` when score≥threshold, the new `Session Scoring - Enforce` VCL snippet rejects those requests on the post-scoring restart. Effective at the edge within seconds. Confirm-dialog-gated PUT endpoint + LIVE warning chip in the slider UI. The response code defaults to 429 (Too Many Requests) and is operator-overridable per-service via a new `Enforce response code` selector (403 / 429 / 451 / 503; backend accepts any 4xx/5xx) — picks land via a focused `update_enforce_status_code` orchestrator that swaps only the enforce snippet (~5–10s end-to-end vs. the full enable_scoring flow). Audit-logged as `scoring_enforce_status_code_changed`.
- **URL exclusion regex override** — operator-tunable per-service regex for "which URLs bypass the scorer". Defaults to the built-in static-asset extension list; the new `ExcludeRegexCard` on the Session Scoring page accepts a custom regex (e.g. exclude `/healthz`, exclude entire path prefixes, scope scoring to specific traffic). The PUT endpoint validates input through three layers before any VCL ships: (1) input policy — length cap, no quote / control chars, must compile under Python's `re`; (2) [falco](https://github.com/ysugimoto/falco) static analysis on the assembled recv snippet (catches regex+VCL composition errors that slip past Python's compiler); (3) Fastly's own VCL compiler at activate time. A focused `update_recv_exclusion_regex` orchestrator clones the active version, swaps only the recv snippet, and activates — ~5–15s end-to-end vs. the full enable_scoring flow. Confirm-dialog-gated. Audit-logged as `scoring_exclude_regex_changed`. Falco shipped in the backend Docker image; production sets `SCORING_REQUIRE_FALCO=1` so a missing binary fails closed instead of degrading to input-policy-only.
- **AES key rotation** — `POST /scoring/rotate-key` mints a fresh 32-byte key, moves the prior to `previous_key_hex` (grace slot — Rust cookie codec falls back to it so in-flight cookies keep decoding through one rotation cycle).
- **Cookie lifecycle bounds** — `SESSION_IDLE_EXPIRE_S` (30 min) + `SESSION_HARD_CAP_S` (24h) in the Rust scorer mint a fresh sid when either threshold is exceeded. Stolen cookies can't replay beyond their window; long-running sessions stop biasing the L1 variance estimator.
- **Per-reason AUC breakdown UI** — `PerReasonAucCard` renders AUC split by which L1/L2 rule fired (cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Operator audit log** — new `scoring_audit` table + `/scoring/audit` endpoint records every scoring_enabled, scoring_disabled, threshold_committed/cleared/enforced, matrix_retrained/restored, key_rotated event with actor + timestamp + details. Per-host, never mirrored via state_sync.

### Reliability

- **Cron-progress reliability** — `end_progress` auto-emits `done` when the last event isn't terminal; `list_active_runs` triple-guards (last-event filter + 5-min staleness + DB-status cross-check via `get_cron_run_status`); `reap_zombie_runs` called from every cron-tick cleanup. Fixed a production incident where 382 stale "sync" entries piled up on the System Health card.
- **state_sync merge guards** — `import_admin_state` no longer overwrites scoring `custom_fields` with stale FOS payloads (root cause of a production data-loss incident); sibling fixes in `cli.handle_update_logs`, `provision.write_service_config`, and `api_service_log_fields_set` close every "remote-overwrites-code-managed-state" path.
- **Defense-in-depth** — `enable_scoring` rollback + `disable_scoring` final-save reload cfg right before writing to close the 30-120s race window where concurrent writers got clobbered.
- **Per-key in-flight collapse** in `_cached` so the dashboard's 8-card mount no longer queues queries behind one global lock.

### Performance

Structural:

- **DuckDB connection pool** (`backend/core/duckdb_pool.py`) replaces per-request connection setup; eliminates the per-request DuckDB initialisation cost on hot paths.
- **Hourly Top-N rollup pipeline** (`backend/core/rollups.py` + `scripts/backfill_rollups.py`) precomputes the dashboard's most-asked aggregates; cold-load dashboard scans drop from seconds to tens of ms.
- **Bounded cache primitive** (`backend/utils/bounded_cache.py`, 13-test `tests/utils/test_bounded_cache.py`) replaces several previously-unbounded dict caches across the request path (also referenced under Security → `_StaticAssetLimiter` and the analytics cache in `session_scoring._cached`).

Tuning:

- `security/top-bots` consolidated UA + NGWAF onto one temp table (was 2 independent Iceberg scans per dashboard mount).
- `dashboard/raw` uses `get_source_extent` for cached steady-state extent.
- `usage/prefill` cached-status fast path skips DuckDB hop when the sync cron has populated it.
- `get_enriched_services` 60s TTL cache on the recursive cache-dir `scandir` (was 200-1500ms per `/api/bootstrap`).
- `loading.tsx` Suspense skeletons + dynamic imports (LabelsTab, ChoroplethMap) cut admin-page click lag.

### Cleanup

- Dropped dead `@daypicker/react` dep + dead `frontend/components/ui/calendar.tsx`.
- Collapsed 7-site `cleanup_progress + reap` boilerplate into `cleanup_progress_and_reap()` helper.
- Refactored `security.py`'s ad-hoc temp-table to use the existing `QueryRunner.temp_table()` context manager.
- Narrowed `get_cron_run_status` exception scope to `sqlite3.Error` with DEBUG log so future triage isn't flying blind.

### Security

Capability-focused hardening across the FastAPI backend, Fastly VCL, Next.js frontend, and Rust scorer. All changes deployed and verified.

- **Trust-boundary normalisation**:
  - uvicorn runs with `--proxy-headers --forwarded-allow-ips=127.0.0.1` so `request.client.host` is the real client IP via Caddy's authoritative XFF rewrite.
  - `is_request_remote()` reads `request.client.host` instead of the forgeable Host header; in-app leftmost-XFF parsing is gone.
  - Caddyfile gates `Fastly-Client-IP → X-Forwarded-For` rewrite on `remote_ip` matching Fastly edge ranges. Startup assertion on `TRUSTED_PROXY_IPS` / `UVICORN_FORWARDED_ALLOW_IPS` + integration test prevent silent regression.
  - Next.js `/admin` middleware gates on the Caddy-injected `X-Proxied-By-Caddy: true` marker instead of the forgeable Host header.
- **Destructive-op auth**:
  - `/api/provision/teardown` validates a caller-supplied Fastly token via `/tokens/self` for the `global` scope before any destructive op; never falls back to server-stored credentials. Frontend TeardownDialog prompts admin for the token.
  - `/api/provision/ngwaf-workspaces` token-gated (constant-time stored-key match OR validated `global`-scope token); NGWAF workspace mutation enforces analyst-session scope.
- **DuckDB user-SQL safety**:
  - New `backend/utils/sql_validator.py` enforces a statement-type whitelist + recursive parse-tree walker with catalog blocklist (`duckdb_*` / `pg_*` prefixes, `information_schema` / `pg_catalog` / `system` schemas, non-`main` catalogs) + function denylist (`read_csv` / `read_parquet` / `iceberg_scan` / `glob` / `lsdir` / `getenv` / `current_setting` / `duckdb_secrets` / postgres / sqlite / mysql scanners) + fail-closed parse + audit logging + perf budget. Replaces a regex-based blocklist that missed `read_csv_auto`, `information_schema`, `duckdb_secrets`, `INSTALL/LOAD`, and `getenv`.
  - `escape_sql_literal` helper applied at four ingest call sites; characterisation tests cover the PoC payload + multi-byte UTF-8 + backslash + empty + long-with-many-quotes.
  - `time_range` validated via `dateutil.isoparse` before SQL interpolation.
  - `get_con` / `get_meta_con` dropped the auto-query-param `read_only` flag.
- **VCL header & cache discipline**:
  - `vcl_recv` preamble unsets every internal `x-of-*` / `x-fos-edge-data` / `x-is-cluster-fetch` / `X-Edge-*` header on the inbound request.
  - Origin-metric VCL fields: numeric regex gates + `json.escape` on string values (log-injection).
  - VCL ua/referer keeps its `substr` cap.
  - Fastly `vcl_hash` now keys on the full `req.url` (path + query), not just `req.url.path` — closes cross-query cache poisoning. Auth `key` querystring is already stripped earlier so no secrets leak into cache keys.
- **Cross-tenant scope enforcement**:
  - `/api/alerts/*` and `/api/views/*` enforce analyst-session scope on every read and mutation; pre-flight scope check on PATCH / DELETE via new `get_alert_by_id` / `get_view_by_id` helpers so unauthorised mutations never land.
  - `/api/sources`, `/api/log-fields/catalog`, NGWAF workspace listing — analyst-scope filtering.
  - Cache-layer audit confirmed every per-tenant cache (`session_scoring._cached`, iceberg, bot_sources) includes `service_id` in the key.
- **Path-traversal cages**:
  - `/api/download` path traversal: `realpath` + `commonpath` cage.
  - Cache cleanup rejects bucket separators + `realpath` cage.
  - `service_id` alphanumeric/dash/underscore validation in path helpers.
- **Secret & data hygiene**:
  - `claim_token` TOCTOU → atomic UPDATE with rowcount check.
  - `share_db` quarantine narrowed to actual SQLite corruption signatures (was wiping the DB on transient `OperationalError`).
  - Email-enumeration timing equalised via dummy scrypt on miss.
  - `validate_session` re-syncs `pii_policy` / window / `service_ids` on every call so admin permission edits take effect immediately.
  - `_StaticAssetLimiter` bounded at 10 k tracked IPs.
  - `logging-settings/update` moved GET → POST/PATCH (CSRF).
  - `query_errors` decorator logs traceback server-side, never in the response body; sweep fixture asserts no `trace` key leaks from any route.
- **SSH host-key pinning**: `configs/ssh_known_hosts` pinned, source-controlled, and gitignore-excepted; tunnel manager refuses to start when the file is missing (fail-safe; no TOFU fallback).
- **Scorer signal tightening**: Python + Rust parity — `L1_SCORE_COOKIE_TAMPERED = 100` (was capped at 75 with missing/expired); `L1_ROBOTIC_DWELL_LOW_S 0.5 → 0.20` (closes the 0.20s–0.50s robotic-bot threshold gap). Tracked follow-up sliding-window mean (needs cookie-schema v3) — partial mitigations via `SESSION_IDLE_EXPIRE_S=30 min` + `SESSION_HARD_CAP_S=24h` + session-max scoring bound the practical attack window.

### Tests

- 3070 backend tests
- 65 scorer Rust tests (+8)
- 265 frontend vitest tests (+13)
- `make ci` green: lint + format + mypy + pytest + vcl-test + verify-deps + typecheck-frontend + test-frontend + osv.

### Infrastructure

- Backend Docker image: `python:3.12-slim-bullseye` → `python:3.12-slim-bookworm` (cuts CVE-laden Debian 11 base; remaining 13 high CVEs are deep-dependency / OpenSSL CVEs every major Python base inherits). Frontend image's api-schema stage bumped to match.
- Backend image now ships [`falco`](https://github.com/ysugimoto/falco) v2.3.0 (Fastly VCL static analyser) — required by the scoring-recv-snippet validator.
- **Secret scanning** — [`gitleaks`](https://github.com/gitleaks/gitleaks) v8.30.1 wired in three places: `.pre-commit-config.yaml` (blocks accidentally-staged credentials at commit time), `make secret-scan` Makefile target chained into `make ci`, and a dedicated step in `.github/workflows/ci.yml` (fails the build on any non-allowlisted finding). Configuration in `.gitleaks.toml` extends the built-in ruleset and adds path allowlists for tracked test fixtures, Rust lockfile checksums, the public SSH host key, and (for working-tree-only scans) the gitignored real-config / `.next/` / `data/system/` directories. Verified clean against the full branch history. Policy + suppression playbook documented in **AGENTS.md** §Secrets.
- **CDN cache-key hardening** — `backend/core/fastly/utils.py` `vcl_recv` now runs `querystring.filter_except` to drop all non-S3-API query parameters (caller-injected tracking params, marketing UTMs, session IDs) BEFORE the cache lookup, followed by `querystring.sort` to canonicalise the remaining param order. Composes with the `vcl_hash` fix: untrusted params can no longer fracture the cache OR leak the auth `key` into the cache key.
- Dependency freshness sweep on all four ecosystems:
  - **Python:** `aiohttp 3.13.5 → 3.14.0`, `cfn-lint 1.51.2 → 1.51.4`, `distlib 0.4.0 → 0.4.1`, `filelock 3.29.0 → 3.29.1`, `idna 3.17 → 3.18`, `joserfc 1.6.8 → 1.7.0`.
  - **Frontend:** `@tanstack/react-query 5.100.14 → 5.101.0` (+ devtools), `@types/react 19.2.15 → 19.2.16`, `react/react-dom` resolved to `19.2.7` via the existing `^19.2.5` range. `next` + `eslint-config-next` stay pinned at `16.2.6`.
  - **Rust:** `bitflags 2.11.1 → 2.12.1`.
  - **Deferred (major bumps reserved for 1.2):** TypeScript 5.9 → 6.0 (compiler-API breaking changes); Fastly Rust SDK 0.11 → 0.12 (Compute@Edge API changes); jsdom / eslint / vitest where we're already ahead of the npm "latest" tag.

### Known limitations

- Rate limiting at the edge is NOT included. The DDoS gate (`fastly.ddos_detected`) handles attack-scale traffic by bypassing Compute; sustained-low-rate abuse is left to the operator's existing WAF/NGWAF policies. A future rate-limiting feature is tracked separately.
- When a matrix is rolled back via the UI, the edge Wasm continues to use its embedded matrix until `scripts/scoring/deploy_wasm.sh` re-runs. The Restore endpoint returns a `deploy_hint` with the exact command. See `docs/session_scoring_runbook.md`.

[1.1.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/1.1.0

## [1.0.0] - 2026-06-01

Initial public release. Self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to Fastly Object Storage.

### Highlights

- **Apache Iceberg data lake** in Fastly Object Storage — ACID-compliant log storage, safe for concurrent readers and writers, with automated compaction and snapshot expiration.
- **Automated provisioning** — guided wizard (and equivalent `backend/provision.py` CLI) creates the FOS bucket, scoped access key, CDN-fronting Fastly Delivery service, and the logging endpoint on your VCL service. Auto-rollback on failure.
- **Crash-safe ingestion** — buffered locally, atomically committed; interrupted imports never corrupt the table.
- **CDN-accelerated reads** — every FOS data read goes through a Fastly Delivery service for free egress and edge caching.
- **Multi-source support** — analyze logs from multiple Fastly services side by side, each with its own DuckDB engine and Iceberg table.
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations across every dimension, paginated raw-log viewer with click-to-filter.
- **Insights** — automated anomaly detection for error spikes, regional traffic surges, new IPs, WAF signal changes, cache efficiency collapses, and latency regressions.
- **Usage & Cost** — live storage breakdown, FOS Class A / B operation counts, period totals, and an interactive cost estimator pre-filled from your traffic stats.
- **Log-line accounting** — reconciles Fastly's authoritative `/stats/service/{id}` counter against locally-ingested rows bucket-by-bucket and surfaces sustained pipeline loss.
- **Configurable log fields** — thirteen built-in field groups (HTTP, network, geo, TLS, NGWAF, QUIC/HTTP3, origin metrics, etc.) plus arbitrary custom VCL fields with auto-generated Edge Data Capture snippets.
- **Alerts** — threshold-based, webhook-delivered, with optional comparison-period evaluation and per-status-code scope.
- **Two collaboration modes** — invite analysts to run an independent copy (durable JSON-config join with read-only FOS credentials), or share your running instance live via three sharing modes: SSH reverse tunnel via localhost.run, your own hostname, or your own public IP. Per-analyst passcode invites, optional IP allowlist, optional expiry, and instant single-invite or sever-all revoke. Per-mode trust-model trade-offs are documented in [SECURITY.md](SECURITY.md#live-dashboard-sharing--trust-model).
- **Field-size guard** — warns when your selected log fields approach Fastly's ~8 KB log-format limit.

See [docs/features.md](docs/features.md) for the full feature reference.

[1.0.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/1.0.0
