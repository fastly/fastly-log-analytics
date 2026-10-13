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

## 11. Content Security blocklist (admin-only)

The **Content Security** tab (`?tab=security`) lets an admin act on what it shows: push a value to the operator's own blocklist HTTP function, then undo it.

### Configuration
- **Where:** Admin → Overall Settings → **Content Security**. One global URL (not per service), stored in `data/system/content_security.json`. Empty means blocking is off.
- **Validation:** absolute `http://` or `https://` URL, ≤ 2048 chars, no `user:pass@`. The URL is returned only to admins and never logged (its path or query may carry a token).
- **Authorization header (optional):** one `Name: value` header (e.g. `Authorization: Bearer <token>`) sent with every `POST` and `DELETE`. It's stored as `blocklist_auth_header` in the same file (written owner-only, `0600`). It is a credential, so:
  - The settings API returns only `auth_header_name`, never the value, and it's never logged or written to the audit log. Validation errors never echo it.
  - `PUT` `auth_header`: omitted keeps the saved header, `""` removes it, and a value replaces it. In the UI, the masked box starts empty; leaving it blank keeps the saved header, and **Remove** clears it.
  - It must be a single line with a valid header name. `Host`, `Content-Type`, `Content-Length`, `Transfer-Encoding`, `Connection`, `Upgrade`, `TE` and `Trailer` are rejected (`422 invalid_header`).
  - Changing the URL to a different scheme, host or port without re-entering or removing the header is rejected (`422 auth_header_reentry_required`), so a saved token never silently follows the URL to another server. Clearing the URL also drops the header.

### HTTP function contract
| Action | Request | Success |
|---|---|---|
| Block / throttle | `POST <url>` with JSON `{"kbps": <n>, "conditions": [{"category": "<category>", "value": "<value>"}]}` | any 2xx |
| Revert | `DELETE <url>` with the same JSON body | any 2xx |

Example: `{"kbps": 0, "conditions": [{"category": "country", "value": "CN"}]}`.

- **`kbps`** sets the action: `0` blocks matching traffic outright, and a positive number throttles (paces) it to that many kbps. It's always present and is repeated on `DELETE`. The dialog offers **Block (no traffic allowed)** and **Throttle to: 128 / 256 / 512 kbps**; the API accepts any integer 0–10,000,000. A condition set has one active rule at a time, so to switch between block and a rate, remove the existing rule first (otherwise `409 already_blocked`).

- **`conditions` is always an array**, even for one condition, so receivers handle a single shape. **All conditions must match (AND)**: `[{country: RS}, {proxy_type: hosting}]` blocks hosting-proxy traffic from Serbia, not all of Serbia. The UI sends one condition today; the API accepts up to one per category.
- Conditions are sent sorted by `category`, and the same order is used on `DELETE`.
- `category` is one of `country`, `referer`, `host`, `proxy_type`, `proxy_description`, `subscriber_id`, `ip`. Two conditions on the same category are rejected (`422 duplicate_category`), since they could never both match.

`ip` values are validated and normalized (e.g. `2001:DB8::0001` → `2001:db8::1`). Redirects are **not** followed (a 3xx is a failure), and calls time out after 10 s.

### Where Block appears
- **Leaderboards** (Countries, Referers, Hosts, Proxy Types, Proxy Descriptions): **Block or throttle…** in each value's menu, below the filter and copy actions.
- **Subscriber IDs by Distinct IPs:** a new per-row actions menu (**⋯**) with **Show IP addresses** and **Block or throttle…**.
- **Subscriber dialog:** a **Block or throttle…** button. This is the only place to block an **IP address**.

### Block dialog
The dialog defaults to the value that was clicked. The user can pick another value from the same list or enter a **Custom value…**. From the Subscriber dialog the user can also switch **Category** between the subscriber ID and any of its IPs (an IP seen from several countries appears once). Confirm stays disabled while no blocklist URL is configured, and the dialog links to Admin settings.

### After confirming
- The backend calls the function. **Only a 2xx records the block**, so a rejected call leaves no row; the dialog stays open with the error.
- A success toast (`Blocked Country US`) offers **Undo** for 10 s; Undo sends the `DELETE`.
- Values with an active block show a red ban mark, and their menu offers **Unblock** instead of Block.
- The **Blocked & Throttled** card lists every active rule for the service with its **Action** (Block, or Throttle to N kbps) and an **Unblock** / **Remove throttle** button. Values with a block show a red ban mark; values with a throttle show an amber gauge mark (hover for the rate). If the `DELETE` fails, the block stays active and an error toast is shown.

### Storage & audit
- Postgres `content_security_blocklist` (`id`, `service_id`, `conditions`, `kbps`, `created_at/by`, `unblocked_at/by`). `conditions` is canonical JSON (sorted by category, compact), so the same set in any order is the same block. Unblocking stamps `unblocked_at` rather than deleting, so history is kept. A partial unique index allows at most one *active* block per (service, condition set), and a duplicate returns `409 already_blocked` without calling the function. A subset is a different, broader block.
- Only single-condition blocks put the red mark on a leaderboard value; a multi-condition block doesn't block that value on its own. The **Blocked & Throttled** card lists each rule's conditions joined by "and".
- Every attempt is written to the service's audit log: `content_security_block`, `content_security_unblock`, and `*_failed` variants with the error.

### Endpoints
| Method | Path | Notes |
|---|---|---|
| `GET` / `PUT` | `/api/admin/content-security/settings` | Global URL |
| `GET` | `/api/admin/content-security/blocks` | `{configured, blocks}`: active blocks for the active service |
| `POST` | `/api/admin/content-security/blocks` | `{conditions: [{category, value}]}` → `201` block · `409 blocklist_not_configured` / `already_blocked` · `502 blocklist_call_failed` |
| `DELETE` | `/api/admin/content-security/blocks/{block_id}` | Reverts; idempotent for an already-reverted block |

### Roles
| Role | Behavior |
|---|---|
| Admin (`read_write`) | Full block/unblock UI. |
| Analyst Path B | No block UI and no blocklist query. `/api/admin/*` is blocked by `RemoteAccessMiddleware`, and every route also depends on `require_admin`. |
| Read-only service (Path A) | Endpoints return `403`. |

## 12. Content Security enforcement reporting

The Content Security tab reports what the edge actually did to requests, not just which rules exist.

- **Enable:** Log Settings → Streaming → **Content Security** (the former Token section). Checking it checks its three options: **Subscriber ID**, **Content ID** and **Log enforcement outcome**, each with its own checkbox, so any can be unchecked; a checked ID needs an expression, while enforcement needs none. Enforcement adds four log fields: `cs_status`, `cs_mode`, `cs_rl_mode`, `cs_pace_kbps`, read from the request headers `cs-status`, `cs-mode`, `cs-rl-mode` and `cs-pace-kbps` that your service's VCL sets.
- **Outcomes (`cs_status`):** `allowed`, `blocked`, `throttled`, `check_failed` (the block check failed and the request was let through), `counted` (counted against the rate limit, under it) and `rate_limited`.
- **Enforcement card:** requests per outcome, split into **Enforced** (mode `ENFORCE`) and **Recorded only** (mode `WARN`, the request was served normally). Outcomes that don't stop a request (`allowed`, `counted`, `check_failed`) show "—".
- **Enforcement over time card:** stacked requests per outcome per time bucket.
- **Availability:** a service without the `cs_status` column shows which Log Settings option to enable. High-scale (ClickHouse) services get no Content Security data.

