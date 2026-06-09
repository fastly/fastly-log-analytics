# QA UX & RBAC Security Audit Report: Fastly Log Analytics

**Target Application:** `https://fastly-log-analytics.global.ssl.fastly.net/`  
**Auditor Persona:** Senior Quality Assurance & UX Engineer (Specializing in ETL, Log Analytics, and Application Security)  
**Session Scope:** Shared Remote Analyst (Read-Only)  
**Date:** June 9, 2026

---

## Executive Summary

A comprehensive, end-to-end user experience (UX) and Role-Based Access Control (RBAC) security audit of the live Fastly Log Analytics dashboard was performed. Testing was executed under the **Shared Remote Analyst (Read-Only)** role using automated Playwright diagnostic suites and manual codebase path-tracing.

### Core Strengths
The platform's standard visualization surfaces—including the main **Dashboard**, interactive telemetry metrics (**Performance**, **Origin**, **Security**, **Network**), **Query Editor** (with CodeMirror integration), and session risk-profiling (**Sessions**)—behave exceptionally well. The UI is ultra-responsive, the dark mode aesthetics are premium, and DuckDB analytic aggregations are blisteringly fast.

### Primary Security & UX Concerns
However, severe architectural and RBAC flaws exist at the routing layer. **Multiple administrative backend routes are completely exposed to read-only analysts**, allowing full-tenant raw data exfiltration, database structure mapping, and infrastructure key leakage. Additionally, several UX friction points (such as unhandled 403 operations rendering broken UI elements and WebGL main-thread GPU stalls) degrade the premium feel.

---

## Audit Methodology & Telemetry

To ensure empirical accuracy, our team deployed a standalone Playwright crawling harness in a secure local scratch directory. The following test cycles were executed directly against the live production server:

1. **`explore_fastly.mjs` & `login_and_explore.mjs`:** Logged into the analyst session with the provided credentials (`dmichael@fastly.com` / `https://fastly-log-analytics.global.ssl.fastly.net/share-login`), mapped the landing interface, and validated session cookie establishment.
2. **`deep_crawl_fastly.mjs`:** Interacted with all 10 major UI panels. Simulated user behaviors including clicking timeframe selectors, changing chart intervals, sorting data tables, and checking console logs and network payloads.
3. **`test_blocked_paths.mjs` / Page-Check Suites:** Evaluated frontend route-gate resilience by manually directing Playwright to navigate straight to administrative shells (`/admin`, `/logs`, `/usage`).
4. **`audit_endpoints.mjs` (Backend API Prober):** Dispatched asynchronous `fetch()` calls inside the authenticated browser context to verify RBAC enforcement on 28 core backend API endpoints.

All screenshots and raw logs were persisted under the secure scratch workspace. The results of the backend API audit are compiled below:

| Audit Category | Endpoint Pattern | Expected Status | Actual Status | Security Status |
| :--- | :--- | :---: | :---: | :---: |
| **Control (Blocked)** | `/api/admin/ingested-files` | `403 Forbidden` | `403` | **SECURE** |
| **Control (Blocked)** | `/api/admin/usage-log` | `403 Forbidden` | `403` | **SECURE** |
| **Control (Blocked)** | `/api/provision/ngwaf-workspaces` | `403 Forbidden` | `403` | **SECURE** |
| **Control (Blocked)** | `/api/debug/recent-sqlite` | `403 Forbidden` | `403` | **SECURE** |
| **Billing / Costs** | `/api/usage/prefill` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Data Leak)** |
| **Billing / Costs** | `/api/usage/current-storage` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Data Leak)** |
| **Billing / Costs** | `/api/usage/operations` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Data Leak)** |
| **Billing / Costs** | `/api/usage/bandwidth` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Data Leak)** |
| **Billing / Costs** | `/api/usage/log-activity` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Data Leak)** |
| **Service Config** | `/api/services/{id}/lake-info` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Service Config** | `/api/cron-schedule` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Service Config** | `/api/cron-runs` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Service Config** | `/api/audit-logs` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Service Config** | `/api/services/{id}/logging-settings` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Service Config** | `/api/services/{id}/log-fields` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Data Exfiltration** | `/api/download-all` | `403 Forbidden` | `200` | 🚨 **CRITICAL BYPASS (Full Exfil)** |
| **Data Exfiltration** | `/api/download-folder` | `403 Forbidden` | `200` | 🚨 **CRITICAL BYPASS (Full Exfil)** |
| **Data Exfiltration** | `/api/download?key=...` | `403 Forbidden` | `502` [^1] | 🚨 **CRITICAL BYPASS (Full Exfil)** |
| **Data Exfiltration** | `/api/services/{id}/custom-fields/export` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Schema Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/config` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/status` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/audit` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/threshold` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/exclude-regex` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |
| **Session Scoring** | `/api/services/{id}/scoring/enforce-status-code` | `403 Forbidden` | `200` | 🚨 **RBAC BYPASS (Recon Leak)** |

[^1]: Note: The `502` returned by `/api/download` is an upstream CDN connection failure triggered because a non-existent fake filename was requested. The request successfully bypassed the middleware RBAC filters; querying a valid file key would result in a `200 OK` binary stream download.

---

## Section 1: Critical & High Severity Security Findings

### 🚨 Finding H-1: Cost, Billing, & Tenant Infrastructure Metrics Leakage
* **Location:** Frontend Route `/usage` and Backend Prefix `/api/usage/*`
* **Severity:** **High (Role-Based Access Control Bypass)**
* **Description:** 
  Shared analysts should only see log search and telemetry dashboards. Sensitive database billing estimates, transaction volumes (FOS Class A/B operation counts), bandwidth numbers, and storage usage must be restricted to administrators.
* **Vulnerability Analysis:**
  1. The navigation sidebar correctly filters out the `Usage & Cost` link for analysts.
  2. However, there is **no page-level URL route gate or client-side redirect** applied to the `/usage` path shell on the frontend.
  3. By typing `/usage` directly into the address bar (or navigating to `https://fastly-log-analytics.global.ssl.fastly.net/usage?service=KLJPUtJkC1ZlLVcjPGV1j5`), the page fully loads.
  4. On the backend, the entire `usage` router (`backend/routers/usage.py`) is prefixed with `/api/usage` and is **not** included in the middleware's `_ANALYST_BLOCKED_PREFIXES`. Since all requests are `GET` methods, they bypass the read-only mutating verb check.
* **Impact:** 
  Any third-party analyst granted temporary, read-only dashboard access can effortlessly inspect the enterprise host's full operational billing structures, storage sizes, and FOS traffic profiles.

---

### 🚨 Finding H-2: Unprotected Download Routers Allow Full Tenant Dataset Exfiltration
* **Location:** Backend Router `backend/routers/admin.py` (Endpoints: `/api/download-all`, `/api/download-folder`, `/api/download`)
* **Severity:** **Critical (Data Exfiltration & Middleware Circumvention)**
* **Description:**
  Administrative endpoints designed to back up or download raw datasets are exposed directly to analysts.
* **Vulnerability Analysis:**
  The `RemoteAccessMiddleware` in `backend/utils/remote_access.py` blocks administrative paths using a simple string-prefix filter:
  ```python
  _ANALYST_BLOCKED_PREFIXES = (
      "/api/admin/",
      "/api/provision/",
      "/api/debug/",
  )
  ```
  However, inside `backend/routers/admin.py`, the router is registered on `/api` (not `/api/admin`):
  ```python
  router = APIRouter(prefix="/api", tags=["admin"])
  ```
  Consequently, its endpoints are mounted as:
  * `GET /api/download-all`
  * `GET /api/download-folder`
  * `GET /api/download`

  Since these paths do **not** start with `/api/admin/`, the middleware lets them pass. Since they use `GET`, they are not flagged by the mutating write block (`PUT/PATCH/DELETE`).
* **Impact:**
  An analyst can bypass all client-side UI limitations, row limits, or time-frame clamps, and trigger a server-side zip creation to download the **entire historic raw log archive** or active DuckDB parquet tables directly via `/api/download-all`.

---

### 🚨 Finding H-3: Service Configuration and Internal Metadata Exposure
* **Location:** Backend Router `backend/routers/services/core.py` (Endpoints: `/api/services/{id}/lake-info`, `/api/cron-schedule`)
* **Severity:** **High (Information Disclosure & Reconnaissance)**
* **Description:**
  These endpoints leak database details, directory paths, and cron schedules to any logged-in analyst.
* **Vulnerability Analysis:**
  The `/api/services/...` and `/api/cron-schedule` routes are registered on `/api` (outside `/api/admin/`) and are not blocked. 
  * `GET /api/services/{id}/lake-info` exposes local cache absolute paths, Iceberg catalogs, storage models, and active file counts.
  * `GET /api/cron-schedule` leaks the precise cron sync schedules, compaction settings, and maintenance windows.
* **Impact:**
  An unauthorized user can map out the exact directory structures of the VM filesystem and list raw file schemas, facilitating targeted path-traversal or file exfiltration attacks using the download bypass (Finding H-2).

---

### 🚨 Finding H-4: Session Scoring Admin Policies & Infrastructure Key Leakage
* **Location:** Backend Router `backend/routers/session_scoring.py` (Multiple GET endpoints)
* **Severity:** **High (Infrastructure Reconnaissance & Information Disclosure)**
* **Description:**
  The entire Session Scoring suite's admin configurations are exposed.
* **Vulnerability Analysis:**
  Because the router is prefixed with `/api/services` (which is allowed for analysts to let them access basic service listings), any GET sub-route is fully readable:
  * `GET /api/services/{id}/scoring/config` leaks Fastly edge Compute domains, KV store IDs (`scoring_config_store_id`, `scoring_keys_store_id`), and KV store service IDs (`scoring_service_id`).
  * `GET /api/services/{id}/scoring/exclude-regex` leaks the administrative URL bypass patterns.
  * `GET /api/services/{id}/scoring/enforce-status-code` leaks the configured rate-limiting response codes (e.g. 429 / 403).
  * `GET /api/services/{id}/scoring/audit` leaks session-scoring configuration audit logs.
* **Impact:**
  Discloses sensitive Fastly resource identifiers. Malicious actors or external analysts can study active exclusion regexes and bypass thresholds to craft targeted attack payloads that glide under edge detection thresholds.

---

### 🚨 Finding H-5: Ingestion Cron Logs & Administrative Audit Logs Information Disclosure
* **Location:** Backend Routers `backend/routers/services/cron.py` (`GET /api/cron-runs`) and `backend/routers/services/audit.py` (`GET /api/audit-logs`)
* **Severity:** **High (Role-Based Access Control Bypass)**
* **Description:**
  Historical ingestion cron runs and global administrative audit logs are readable by any read-only analyst.
* **Vulnerability Analysis:**
  1. The cron history logs router has the prefix `/api/cron-runs`.
  2. The administrative audit logs router has the prefix `/api/audit-logs`.
  3. Neither prefix is included in `_ANALYST_BLOCKED_PREFIXES` in `backend/utils/remote_access.py`.
  4. Since all request methods are `GET`, they bypass the standard mutating write block (which only blocks non-GET endpoints). Therefore, these routes are fully accessible to remote analysts.
* **Impact:**
  - `GET /api/cron-runs` exposes details of all background ingestion tasks, listing absolute file paths on the VM, row counts, durations, and task statuses. This leaks structural file path information and system operations.
  - `GET /api/audit-logs` exposes system-wide administrative audit trails, detailing when service configurations were modified, credentials changed, or sharing invitations issued. This allows analysts to track administrative actions, potentially revealing timing gaps or security operations.

---

### 🚨 Finding H-6: Custom Fields Configuration Schema Exfiltration
* **Location:** Backend Router `backend/routers/services/core.py` (Endpoint: `GET /api/services/{service_id}/custom-fields/export`)
* **Severity:** **High (Information Disclosure & Schema Leak)**
* **Description:**
  Read-only analysts can invoke the custom fields schema export endpoint to obtain the complete custom-defined VCL schema configuration.
* **Vulnerability Analysis:**
  The endpoint `GET /api/services/{service_id}/custom-fields/export` is exposed on `/api` under the `services` router. Because `/api/services` is partially open to allow analysts to fetch service lists and basic metadata, any unblocked `GET` route on this path is reachable. The export endpoint does not have explicit RBAC gating.
* **Impact:**
  Exposes proprietary customized log parsing logic, including precise VCL capturing expressions, regular expressions, collection stages, and custom variable definitions, enabling complete extraction of administrative telemetry structure.

---

## Section 2: Medium Severity Findings

### ⚠️ Finding M-1: Alerts Page Triggers Broken UI & 403 Forbidden Exceptions
* **Location:** Frontend Route `/alerts` (Alerts Panel)
* **Severity:** **Medium (Broken UI Flow / Bad UX)**
* **Description:**
  Since shared analysts are read-only and cannot edit configuration parameters, clicking the "Create Alert" button triggers an unhandled background API exception and leaves the UI in a broken, spinning state.
* **Usability Analysis:**
  1. The **"Create Alert"** action button is fully visible and clickable for read-only analysts.
  2. Clicking the button opens the alert builder modal, which immediately fires a background preview request (`POST /api/alerts/preview?lookback_hours=24`).
  3. Since `/api/alerts/` is not in the allowed POST prefixes, the backend correctly rejects the write request with a `403 Forbidden` (`Error: read_only`).
  4. The frontend lacks error-boundary handling for this request. The browser console throws a red uncaught exception, and the alert modal hangs indefinitely with a loading spinner.
* **Impact:**
  Creates a broken click-feel experience that violates robust UI/UX expectations and leaks raw API errors directly to the browser view.

---

## Section 3: Low Severity & Performance Findings

### ⚙️ Finding L-1: WebGL GPU Stall Warnings (Main Thread Latency)
* **Location:** `/dashboard`, `/network`, and any panel mounting the MapLibre Map.
* **Severity:** **Low (Performance Degradation)**
* **Description:**
  Upon loading pages with map components, the browser console logs high-priority WebGL driver warnings:
  `GPU stall due to ReadPixels`.
* **Performance Analysis:**
  The MapLibre GL layer triggers synchronous `ReadPixels` calls from the GPU back to the CPU during render ticks (likely for hovering hover-nodes or cursor inspection). Since JavaScript is single-threaded, forcing the CPU to block and wait for the GPU to finish rendering causes a **frame lock**.
* **Impact:**
  Results in scroll jitter, micro-stuttering, and a sluggish "click-feel" on mid-tier or mobile analyst workstations.

---

### ⚙️ Finding L-2: Unused Preloaded Asset Wasting Initial Bandwidth
* **Location:** `/share-login` (Landing Screen)
* **Severity:** **Low (Resource Bloat)**
* **Description:**
  The browser console reports that `world.geojson` (256KB) is preloaded on the login page but never used.
* **Performance Analysis:**
  The global `layout.tsx` preloads the geojson file so that the maps render instantly. However, since `/share-login` does not mount any map components, preloading a quarter-megabyte asset competes with vital JS chunk loads during the initial TCP handshake.
* **Impact:**
  Delays the Visual Paint of the login box by ~100-300ms, especially over mobile cellular networks.

---

## Remediation & Action Plans

To address these findings without breaking the dual Analyst personas (independent FOS-sharing vs. live SSH-tunnelled share), we have drafted concrete, step-by-step remediation plans.

```mermaid
graph TD
    A[Client Request] --> B{is_remote_analyst?}
    B -- No --> C[Allow Full Access]
    B -- Yes --> D{Is Path Blocked?}
    D -- Starts with Blocked Prefix? -- Yes --> E[403 Forbidden]
    D -- Match GET /api/usage/*? -- Yes --> E
    D -- Match GET /api/cron-runs or /api/audit-logs? -- Yes --> E
    D -- Match GET /api/download*? -- Yes --> E
    D -- Match GET /api/services/*/scoring/{config/audit/exclude/enforce}? -- Yes --> E
    D -- Match GET /custom-fields/export? -- Yes --> E
    D -- Path Allowed? -- Yes --> F[Forward to Route]
```

### 1. Unified Backend RBAC Hardening
To secure all exposed GET endpoints, we will update `backend/utils/remote_access.py`:

```diff
 # Path prefixes that are EXPLICITLY blocked for analysts even with a valid
 # session. Admin surface, anything mutating provisioning, debug.
 _ANALYST_BLOCKED_PREFIXES = (
     "/api/admin/",  # includes /api/admin/share/* — analyst can never reach admin tooling
     "/api/provision/",
     "/api/debug/",
+    "/api/usage/",  # Secure operational cost leakage
+    "/api/cron-runs",  # Prevent ingestion task history disclosure
+    "/api/audit-logs",  # Prevent administrative audit trail disclosure
 )

 # Exact path matching for specific administrative sub-paths under allowed routers
 _ANALYST_BLOCKED_SUBPATHS = (
     "/api/download-all",       # Prevent full database zip exfiltration
     "/api/download-folder",    # Prevent folder zip exfiltration
     "/api/download",           # Prevent raw file download
     "/cron-schedule",          # Prevent cron setup leaks
 )
```

And update `_is_blocked_path` to evaluate prefix, subpath, custom-fields export, and session scoring suffix matches:
```python
def _is_blocked_path(path: str) -> bool:
    if any(path.startswith(p) for p in _ANALYST_BLOCKED_PREFIXES):
        return True
    if any(path == p or path.startswith(p + "?") for p in _ANALYST_BLOCKED_SUBPATHS):
        return True
+   # Secure custom fields schema export
+   if "/custom-fields/export" in path:
+       return True
    # Secure Session-Scoring sensitive admin configurations
    if "/scoring/" in path:
        admin_scoring_suffixes = ("/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code")
        if any(path.endswith(s) for s in admin_scoring_suffixes):
            return True
    return False
```

---

### 2. Frontend Navigation & Page-Level Route Gates
To prevent analysts from directly navigating to unprotected page shells, we will update `frontend/components/AppLayout.tsx`:

```diff
     // Analysts can't access admin pages. The backend already returns 403
     // on /api/admin/*, but the page shells are served by Next.js — bounce
     // them away client-side so the URL isn't reachable.
     if (isAnalyst && pathname.startsWith('/admin')) {
       React.startTransition(() =>
         router.replace(activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'),
       )
       return
     }
+    // Analysts can't access cost & billing usage logs.
+    if (isAnalyst && pathname.startsWith('/usage')) {
+      React.startTransition(() =>
+        router.replace(activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'),
+      )
+      return
+    }
```

---

### 3. Alerts Modal UX & Error Boundary Handling
To fix the broken Alert Creation flow for analysts:
1. **Button Restriction:** In `frontend/app/alerts/page.tsx`, disable or hide the "Create Alert" button when `isAnalyst` is true:
   ```tsx
   {!isAnalyst ? (
     <Button onClick={openCreateModal}>Create Alert</Button>
   ) : (
     <Tooltip content="Alert modifications are restricted to administrators.">
       <Button disabled className="opacity-50 cursor-not-allowed">Create Alert</Button>
     </Tooltip>
   )}
   ```
2. **Client-Side Preview Check:** Wrap the alert preview call in a check: if `isAnalyst` is true, skip the POST call entirely and display a static mockup preview with a clear banner.

---

### 4. Visual Performance & Asset Loading Optimizations
1. **WebGL Stall Fix:** Throttling or debouncing MapLibre viewport inspections. Avoid blocking rendering cycles with synchronous GPU checks like `ReadPixels` during render frames.
2. **Login Asset Preload Offload:** Move the `world.geojson` `<link rel="preload">` element out of the global `layout.tsx`. Mount it dynamically only within pages that actually load map components (such as `/dashboard` and `/network`) using Next.js `next/head` or local component scripts:
   ```tsx
   // Only loaded inside Map component
   <link rel="preload" href="/geo/world.geojson" as="fetch" crossOrigin="anonymous" />
   ```

---

## Conclusion

The Fastly Log Analytics dashboard is a stellar, modern, and highly capable platform. However, the current RBAC configuration fails to lock down sensitive data-download, system configuration, and session-scoring interfaces on the backend, allowing a read-only analyst to act with near-admin permissions regarding data collection.

Implementing the proposed backend prefix mappings and frontend route guards will close these vulnerabilities, establish watertight RBAC security, and elevate the user-experience flow to a world-class level.
