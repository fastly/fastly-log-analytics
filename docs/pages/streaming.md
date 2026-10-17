# Page Specification: Live Log Streaming (`/streaming`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/streaming`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Live Log Streaming** page delivers a high-speed, interactive terminal-style live tail of incoming CDN edge log lines via Server-Sent Events (SSE). It supports pause/resume, client-side regex search, JSON inspect modal, and column formatting.

### Key Tenets:
- **Low-Latency Streaming:** Pushes raw edge request events as they land in ingest buffers.
- **Interactive Inspection:** Clicking any streamed line opens the deep JSON inspector showing all captured Fastly VCL variables.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/streaming`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `filter`: Live regex/string search pattern.
  - Standard drill-down filters: `status`, `pop`, `client_ip`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full raw JSON log records, unmasked client IPs, full request headers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Live stream viewable; client IPs masked (if enabled by invite policy). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Streams from local buffer/lake. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Stream Transport** | FastAPI SSE streaming endpoint (`/api/stream/{service_id}`). | FastAPI SSE backed by Redis Pub/Sub stream bridge. |
| **Buffer Source** | Reads newest rows from local memory ring buffer. | Reads newest events from Valkey / Redis stream. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/stream/{service_id}` (Server-Sent Events):
   - Streams JSON chunks with request fields (timestamp, client_ip, method, url, status, pop, time_elapsed).

### Telemetry Attribution:
- Connection establishment and stream duration recorded in `usage_log.db` and telemetry logs.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Terminal Stream Box, Stream Controls Toolbar, and Inspector Drawer enforce container intrinsic sizes (`contain-intrinsic-size: 600px`).
- **In-Place Skeletons:** Renders a connected stream skeleton (`Connecting to live Fastly log stream...`).
- **Zero CLS Log Append:** Appending log rows utilizes virtualized list rendering (`@tanstack/react-virtual`) to eliminate DOM jitter and scroll jump.

---

## 7. Interactive Workflows & Edge Cases

- **Pause / Resume:** Spacebar or UI button pauses stream without dropping the SSE connection; resumed stream flushes recent buffer.
- **JSON Inspector Modal:** Clicking any log line opens the slide-out drawer with syntax-highlighted VCL JSON.
- **Stream Disconnect Recovery:** Automatically reconnects with exponential backoff if SSE stream breaks.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Stream Event Latency** | < 1000 ms from edge ingest | < 1000 ms from edge ingest | Timestamp diff |
| **Telemetry Capture** | 100% streams captured | 100% streams captured | `usage_log.db` audit |
| **Memory Leak Guard** | Max 1000 DOM lines capped | Max 1000 DOM lines capped | Chrome memory profiling |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /streaming` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. SSE Connection:** Verify `/api/stream/{service_id}` establishes connection and receives valid JSON events.
- [ ] **4. Pause/Resume:** Toggle pause; verify stream freezes in place without UI errors.
- [ ] **5. Line Inspector:** Click a streamed row; verify inspector drawer opens with full JSON attributes.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs in streamed lines.
- [ ] **7. Telemetry & Query Audit:** Verify SSE stream connection is recorded in `usage_log.db`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/streaming.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Continuous streaming traffic with varying HTTP status codes.

---

## 11. Content Security policies (admin-only)

The **Content Security** tab (`?tab=security`) has two views, chosen with `?csview=`: **Monitor** (the default, the analytics page) and **Policies** (`?csview=policies`). An admin acts on what Monitor shows by adding rules to a **policy**, and decides where a policy applies in the Policies view. Policies are per service; nothing is shared across services.

### Concepts
- **Policy:** a named rule set. It holds a country mass-block **regex**, per-category rules for countries, referers, ASNs, hosts, proxy types and proxy descriptions (each value is **Block**, **Throttle** to 128, 256 or 512 kbps, or **Allow**), and **individual** subscriber-ID and IP blocks. Individual rules are block-only: they cannot be throttled or allowed, and they take precedence over Allow rules. The regex is block-only too.
- **Policy schedule:** each entry applies a policy to **all content** (the default) or to one **content ID**, in a **mode** (**Test only** logs and does nothing, **Enforce** blocks and throttles), between an optional start (inclusive) and end (exclusive) time. A service has at most one all-content entry, and one content ID can only be scheduled for one policy at a time (windows may not overlap). The most specific active entry wins: the content ID's own, else the all-content one. A policy that is not scheduled enforces nothing.
- **Live edits:** editing a policy changes it in place everywhere it is scheduled, immediately. The editor lists those schedule entries and warns before you save.
- **Sync:** every change is saved first and then published to the operator's blocker as one idempotent whole-service update. If the publish fails, the change is **saved but not yet live**: the UI shows **Not yet live** with a **Retry** (or **Sync now**) action. "Live" means the blocker accepted it; it can take a few seconds to reach every edge.

### Configuration
- **Where:** Admin → Overall Settings → **Content Security**. One global URL (not per service), stored in `data/system/content_security.json`. Empty means blocking is off: policies are still saved, but nothing is published or enforced.
- **Validation:** absolute `http://` or `https://` URL, ≤ 2048 chars, no `user:pass@`. The URL is returned only to admins and never logged (its path or query may carry a token).
- **Authorization header (optional):** one `Name: value` header (e.g. `Authorization: Bearer <token>`) sent with every call to the blocker. It is a credential, so:
  - The settings API returns only `auth_header_name`, never the value, and it's never logged or written to the audit log. Validation errors never echo it.
  - `PUT` `auth_header`: omitted keeps the saved header, `""` removes it, and a value replaces it. In the UI, the masked box starts empty; leaving it blank keeps the saved header, and **Remove** clears it.
  - It must be a single line with a valid header name. `Host`, `Content-Type`, `Content-Length`, `Transfer-Encoding`, `Connection`, `Upgrade`, `TE` and `Trailer` are rejected (`422 invalid_header`).
  - Changing the URL to a different scheme, host or port without re-entering or removing the header is rejected (`422 auth_header_reentry_required`), so a saved token never silently follows the URL to another server. Clearing the URL also drops the header.
- **Sync now:** a button in the same card that publishes the active service's policies and shows the result (`published`, `not_configured` or `failed` with the error).
- Redirects are **not** followed (a 3xx is a failure), and calls time out after 10 s.

### Policies view
- **Sync state:** a banner shows whether the blocker has the latest saved revision (**Live**), is behind it (**Not yet live**, with the last error and **Retry**), or has no endpoint configured (with a link to Admin settings).
- **Policy schedule table:** scope (All content or `Content: <id>`), policy, mode (Test only or Enforce), start, end (shown in UTC; "(always)" and "(none)" when open) and a status chip: **Scheduled**, **Active** or **Ended**, plus **Not yet live** while the blocker is behind. **Schedule a policy**, edit and remove (removing asks first).
- **Policies table:** name, a rules summary (for example `regex ^(CN|RU)$, 3 countries, 2 ASNs, 1 allowed`), where it is assigned, and when it was updated. **New policy**, edit and delete. A policy that is still assigned cannot be deleted; the server's message is shown.

### Policy editor
`?policy=<id>` (or `?policy=new`) opens the editor, so it deep-links.
- **Name**, unique within the service.
- **Mass-block countries by regex:** a live preview (debounced) lists the countries it matches, or the server's reason when the pattern is invalid or unsafe.
- **Tabs** for Countries, Referers, ASNs, Hosts, Proxy types and Proxy descriptions, each with a count badge. Every row is a value with **Block**, **Throttle** (a dropdown of 128, 256 or 512 kbps) or **Allow** (skip this policy's other rules for that value). The add-row suggests values already seen in the Monitor view's top lists. Values are normalized as the server does (country upper-case, ASN as a bare number, others lower-case).
- **Individuals tab:** subscriber IDs and IP addresses, one per line (blank lines and duplicates are dropped; subscriber IDs are case-sensitive; IPs are checked as IPv4 or IPv6).
- **Used by:** the schedule entries that use the policy, with the shared-edit warning.
- **History:** lists each revision with who changed it and when.
- **Unsaved changes** are guarded: closing asks before discarding, and the browser warns before leaving the page. A server error (for example `name_taken` or `invalid_regex`) is shown inline.

### Apply policy to content dialog
Scope is **All content** (the default) or **Content ID** (suggestions from the content IDs observed in the logs, with request counts; any text is accepted and matched exactly, case-sensitively). Then a policy, a mode (**Enforce** or **Test only**), and optional start and end date-times, entered in your browser's local time with the UTC value that is saved shown beside each. Server errors appear inline, for example that an all-content entry already exists (`assignment_overlap` for an overlapping content-ID window).

### Monitor view
- **Banner:** names the policy in effect for the selected content (`In effect for ev-123: Playoffs (Enforce)`) with an **Edit policy** link into the Policies view, or says that none is in effect. A **Not yet live** notice appears here too when the blocker is behind.
- **Where Block appears:** the leaderboards (Countries, ASNs, Referers, Hosts, Proxy Types, Proxy Descriptions) in each value's menu, the Subscriber Usage actions menu (**Block…**, subscriber IDs are block-only), and the Subscriber dialog (the only place to block an **IP address**).
- **Block dialog:** defaults to the clicked value; pick another from the same list or a **Custom value…**. The action is **Block**, **Throttle to** 128, 256 or 512 kbps, or **Allow** (not offered for subscriber IDs and IPs). The dialog says which policy the rule goes into, warns when that policy is shared with other assignments, and, when no policy is in effect, says a **Default** policy will be created. Confirm stays disabled while no blocker URL is configured, and the dialog links to Admin settings.
- **After confirming:** the rule is added to the policy in effect for the selected content, or, when none is, a `Default` policy scheduled for all content in Enforce mode is created and the toast says so. A success toast offers **Undo** for 10 s, which removes that rule. If the publish fails the toast says the rule is saved but not yet live and offers **Retry**.
- **Marks:** values covered by the policy in effect show a red ban (block), amber gauge (throttle, hover for the rate) or green shield (allow); their menu offers **Unblock**, **Remove throttle** or **Remove allow**. Subscriber IDs and IPs blocked individually are marked too.
- **Blocked & Throttled card:** lists every rule of the policy in effect (the country regex included, with its Allow rules) with a remove button each.

### Storage & audit
Policies, rules, revisions, assignments and sync state live in PostgreSQL (ADR-22). Every change is written to the service's audit log, including failed publishes.

### Endpoints
All are admin-only and scoped to the active service.

| Method | Path | Notes |
|---|---|---|
| `GET` / `PUT` | `/api/admin/content-security/settings` | Global URL and auth header |
| `GET` / `POST` | `/api/admin/content-security/policies` | List / create |
| `GET` / `PUT` / `DELETE` | `/api/admin/content-security/policies/{id}` | Delete is refused while assigned |
| `POST` / `DELETE` | `/api/admin/content-security/policies/{id}/rules` | One rule: body `{category, value, action, kbps}` / query `category`, `value` |
| `GET` | `/api/admin/content-security/policies/{id}/revisions` | History |
| `GET` / `POST` | `/api/admin/content-security/assignments` | List / create |
| `PUT` / `DELETE` | `/api/admin/content-security/assignments/{id}` | Update / delete |
| `GET` | `/api/admin/content-security/effective?content_id=` | The policy in effect now and how many schedule entries share it |
| `POST` | `/api/admin/content-security/live-rule` | Add a rule to the policy in effect (creates `Default` if none) |
| `POST` | `/api/admin/content-security/publish` | Retry / Sync now |
| `GET` | `/api/admin/content-security/sync-status` | Saved vs published revision |
| `GET` | `/api/admin/content-security/country-regex/preview?pattern=` | Countries a regex matches |

Writes return the saved object plus `publish: {status, rev, error}` where `status` is `published`, `not_configured` or `failed` (saved but not yet live).

### Roles
| Role | Behavior |
|---|---|
| Admin (`read_write`) | Full Monitor actions and the Policies view. |
| Analyst Path B | Monitor only: no Policies view, no block UI and no policy queries. `/api/admin/*` is blocked by `RemoteAccessMiddleware`, and every route also depends on `require_admin`. |
| Read-only service (Path A) | Endpoints return `403`. |

## 12. Content Security enforcement reporting

The Content Security tab reports what the edge actually did to requests, not just which rules exist.

- **Enable:** Log Settings → Streaming → **Content Security** (the former Token section). Checking it checks its three options: **Subscriber ID**, **Content ID** and **Log enforcement outcome**, each with its own checkbox, so any can be unchecked; a checked ID needs an expression, while enforcement needs none. Enforcement adds four log fields: `cs_status`, `cs_mode`, `cs_rl_mode`, `cs_pace_kbps`, read from the request headers `cs-status`, `cs-mode`, `cs-rl-mode` and `cs-pace-kbps` that your service's VCL sets.
- **Outcomes (`cs_status`):** `allowed`, `blocked`, `throttled`, `check_failed` (the block check failed and the request was let through), `counted` (counted against the rate limit, under it) and `rate_limited`.
- **Enforcement card:** requests per outcome, split into **Enforced** (mode `ENFORCE`) and **Warned** (mode `WARN`, the request was served normally). Outcomes that don't stop a request (`allowed`, `counted`, `check_failed`) show "—".
- **Enforcement over time card:** stacked requests per outcome per time bucket.
- **Availability:** a service without the `cs_status` column shows which Log Settings option to enable. High-scale (ClickHouse) services get no Content Security data.
- **By policy and rule:** under the outcome table, one row per policy, rule (`country_regex`, a category, an individual block, or `exempt:<category>`) and outcome, with **Requests**, **Enforced** and **Warned**. Only requests whose mode was `ENFORCE` count as enforced; the remainder of a stopping outcome was `WARN` only and was served normally. Admins see policy names; a policy that no longer exists shows its id.
