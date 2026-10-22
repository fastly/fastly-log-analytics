# Page Specification: Admin System Trends (`/admin/trends`)

**Status:** **Live-certified** (2026-10-19 — Audited, hardened, and live-verified across Local Standard, Remote Standard GCE, and High-Scale Elevation on `release/v3.0.0-beta4`)

---

## 1. Overview & Purpose

The **Admin System Trends** page (`/admin/trends`) provides longitudinal time-series sparklines for host, process, DuckDB connection pool, and ingestion lag vitals over configurable lookback windows (`1h`, `6h`, `24h`, `7d`). It enables infrastructure operators to correlate CPU/memory spikes, disk utilization growth, DuckDB query concurrency, pool checkout wait latency, and per-service ingestion lag with traffic surges or background compaction runs.

- **Route:** `/admin/trends`
- **Frontend Entrypoint:**
  - [`frontend/app/admin/trends/page.tsx`](../../../frontend/app/admin/trends/page.tsx) (Force-dynamic RSC shell that pre-fetches the default `1h` batch via `fetchMetricHistoryBatchServerSide()` and dehydrates it into `HydrationBoundary` under `['admin', 'metric-history-batch', '1h']`).
  - [`frontend/app/admin/trends/loading.tsx`](../../../frontend/app/admin/trends/loading.tsx) (Geometry-matched route-level loading skeleton with zero-CLS layout reservations).
  - [`frontend/app/admin/trends/_sections/AdminTrendsClient.tsx`](../../../frontend/app/admin/trends/_sections/AdminTrendsClient.tsx) (Interactive client component rendering the 7 operational vital sparkline cards, lookback window selector, and manual refresh button).
  - [`frontend/lib/ssr/admin_trends.ts`](../../../frontend/lib/ssr/admin_trends.ts) (Server-side SSR fetcher for `/api/admin/metric-history/batch?since=1h`).
- **Backend Endpoints:**
  - `GET /api/admin/metric-history/batch?since={window}` ([`backend/routers/admin/metric_history.py`](../../../backend/routers/admin/metric_history.py))
- **Backing Store & Producer:**
  - PostgreSQL 16 `metric_snapshots` table (`METADATA_DSN`, per [ADR-22](../../adr/22-postgres-only-metadata.md)), managed by [`backend/core/metric_snapshots.py`](../../../backend/core/metric_snapshots.py).
  - Sampled every 60 seconds by the process-global `metric_snapshot` background job ([`backend/cron/jobs/metric_snapshot.py`](../../../backend/cron/jobs/metric_snapshot.py)) and pruned to a 30-day retention window by `metadata_cleanup`.

---

## 2. Architecture & Deployment Mode Behavior (`DEPLOYMENT_MODE`)

| Dimension | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Snapshot Storage** | Unified PostgreSQL 16 `metric_snapshots` table (`METADATA_DSN`). | Unified PostgreSQL 16 `metric_snapshots` table (`METADATA_DSN`). |
| **Sampler Cadence** | Every 60 seconds via APScheduler `metric_snapshot` job on the serving instance. | Every 60 seconds on the serving pod, plus Celery queue depth and active task metrics (`celery_queue_depth`, `celery_active_tasks`). |
| **Displayed Series (7 Cards)** | 1. `cpu_load_1m` (CPU load 1m avg)<br>2. `mem_used_pct` (Memory used %)<br>3. `disk_used_pct` (Data disk used %)<br>4. `disk_used_pct_root` (Boot disk used %)<br>5. `active_query_count` (Active DuckDB queries)<br>6. `pool_wait_p95_ms` (Pool wait p95 ms — max across services per timestamp)<br>7. `ingest_lag_s` (Ingest lag s — max across services per timestamp) | Identical 7-card operational layout; global host/process metrics reflect the serving pod while per-service series (`pool_wait_p95_ms|<svc>`, `ingest_lag_s|<svc>`) aggregate across active services via `maxAcrossServicesPerTs`. |
| **Serviceless Fresh-Install Behavior** | `/api/admin/metric-history` is registered in `SERVICELESS_PATH_PREFIXES` in [`frontend/lib/api.ts`](../../../frontend/lib/api.ts), so `/admin/trends` loads host/process vitals cleanly even before any Fastly service is provisioned. | Same. |

---

## 3. Role-Based Access Control (RBAC) & Security

| Persona | Access Level | Expected Behavior |
|---|---|---|
| **Admin** (`access_level: "read_write"` / mTLS Admin Gateway) | Full Access | `200 OK` on `/admin/trends` and `GET /api/admin/metric-history/batch`. |
| **Analyst Path A** (Read-Only FOS Instance) | Local Instance Admin UI | Can view local instance trends for their own process (`200 OK`). |
| **Analyst Path B** (Remote Live-Share Session) | **Blocked (`403 Forbidden`)** | Blocked at both `RemoteAccessMiddleware` (`/api/admin/*` prefix gate) and route-level `Depends(require_admin)`. `GET /api/admin/metric-history/batch` with `X-Remote-Analyst: 1` returns `403 Forbidden` with `{"error": "admin_only"}`. SSR helper refuses to inject the admin token on the analyst branch and fails closed. |

---

## 4. API & Wire Contract

### `GET /api/admin/metric-history/batch`

- **Query Parameters:**
  - `since` (`str`, default `"1h"`): Lookback window matching `^[1-9]\d*[mhd]$` (e.g., `30m`, `1h`, `6h`, `24h`, `7d`), capped at 30 days (`_MAX_LOOKBACK_DAYS = 30`).
  - Malformed or empty window values (e.g., `invalid`, `0h`, `-5h`, `1w`) return `400 Bad Request` with `{"detail": "Invalid lookback window ..."}`.
- **Response Model:** `MetricHistoryBatchResponse` ([`backend/models/admin.py`](../../../backend/models/admin.py))
  ```json
  {
    "series": {
      "cpu_load_1m": [
        { "ts": "2026-10-19T21:00:00Z", "value": 0.62 }
      ],
      "mem_used_pct": [
        { "ts": "2026-10-19T21:00:00Z", "value": 48.5 }
      ],
      "pool_wait_p95_ms|svc-id": [
        { "ts": "2026-10-19T21:00:00Z", "value": 3.4 }
      ],
      "ingest_lag_s|svc-id": [
        { "ts": "2026-10-19T21:00:00Z", "value": 14.0 }
      ],
      "cron_duration_ms|svc-id|sync": [
        { "ts": "2026-10-19T21:00:00Z", "value": 420.0 }
      ]
    }
  }
  ```
- **Composite Scope Keys:**
  - Global metrics: `"<metric>"` (e.g., `cpu_load_1m`, `mem_used_pct`, `disk_used_pct`, `disk_used_pct_root`, `active_query_count`)
  - Per-service metrics: `"<metric>|<service_id>"` (e.g., `pool_wait_p95_ms|<service_id>`, `ingest_lag_s|<service_id>`)
  - Per-task metrics: `"<metric>|<service_id>|<task>"` (e.g., `cron_duration_ms|<service_id>|<task>`)
  - Points within each series are ordered chronologically ascending (`ORDER BY ts ASC`).

---

## 5. Visual Stability, Zero-CLS & Interactive Controls Contract

1. **Pre-Allocated Zero-CLS Layout Reservations:**
   - Toolbar container (`data-testid="admin-trends-toolbar"`): `min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]`.
   - 7-card sparkline grid (`data-testid="admin-trends-grid-reservation"`): `min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]`.
   - [`frontend/app/admin/trends/loading.tsx`](../../../frontend/app/admin/trends/loading.tsx) mirrors the exact DOM hierarchy and reservation geometry of `AdminTrendsClient.tsx` so route transitions and SSR hydration produce zero layout shift (`CLS <= 0.05`, `FCP < 1200ms`).
2. **Contextual Skeleton & Non-Destructive Refresh Dimming:**
   - Initial unseeded loads render `AnalyticsCard` skeletons with `loadingMessage="Loading system vital trends..."`.
   - Subsequent window switches or manual refreshes keep existing sparklines mounted while dimming the grid (`opacity-40 pointer-events-none transition-opacity`) and spinning the `RefreshCw` icon.
3. **Interactive Controls & Test IDs:**
   - Root wrapper: `data-testid="admin-trends-root"`
   - Manual refresh button: `data-testid="admin-trends-refresh-btn"`
   - Window selector buttons: `data-testid="admin-trends-window-1h"`, `admin-trends-window-6h`, `admin-trends-window-24h`, `admin-trends-window-7d` (with `aria-pressed`)
   - Series cards: `data-testid="admin-trends-card-{metric}"` and `data-testid="admin-trends-latest-{metric}"`
   - `ServerFooter` rendered dynamically in `<footer>` via `AppLayout`.

---

## 6. Verification & Automated Test Suite

- **Backend Unit Tests:** [`tests/routers/test_admin_metric_history.py`](../../../tests/routers/test_admin_metric_history.py) (covers composite scope keys, chronological ordering, `30m`/`1h`/`6h`/`24h`/`7d` windows, `400 Bad Request` on invalid windows, and Analyst Path B `403` rejection).
- **Route Tripwire & OpenAPI Contract Tests:** [`tests/remote_access/test_route_table_tripwire.py`](../../../tests/remote_access/test_route_table_tripwire.py) and [`tests/test_openapi_snapshot.py`](../../../tests/test_openapi_snapshot.py).
- **Frontend Unit Tests:** [`frontend/__tests__/app/admin/trends.test.tsx`](../../../frontend/__tests__/app/admin/trends.test.tsx) (4 tests covering `loading.tsx` reservations, pending skeleton state, all 7 series cards + `max-per-ts` multi-service aggregation, and `1h` -> `6h` -> `24h` -> `7d` window switching + manual `Refresh`).
- **Playwright E2E Suite:** [`frontend/e2e/pages/admin-trends.spec.ts`](../../../frontend/e2e/pages/admin-trends.spec.ts) (5 end-to-end contract tests verified across Local Standard, Remote Standard GCE, and High-Scale Elevation).
