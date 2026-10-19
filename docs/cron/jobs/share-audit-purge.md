# Background Job Specification: `share_audit_purge`

## 1. Overview & Objectives
- **Job Identifier:** `share_audit_purge`
- **Category:** Live-Share Security, Compliance & Audit Retention
- **Status:** Verified (Automated Unit Tests & Lifecycle Audited)
- **Purpose:** Daily retention cleanup of expired remote analyst live-share invitations, invalidated authentication sessions, expired claim tokens, and historical share audit logs in PostgreSQL 16 per ADR-22.
- **Why It Runs:** Live Dashboard Sharing (Path B) generates audit events, session tokens, and invite records. For compliance and operational hygiene, expired invitations, stale session records, and audit logs beyond the retention window (`share_audit_retention_days`, default: 90 days) must be safely purged.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 03:45 UTC (`hour=3, minute=45`).
- **Timing Rationale:** Runs after `full_sync` (03:30 UTC) and before `optimize` (04:00 UTC) during the quiet daily maintenance window.
- **Scope:** Process-global singleton job.
- **Configurable Overrides:** `share_settings.share_audit_retention_days` (default: 90 days), `max_idle_session_days` (default: 30 days).
- **Executor Pool:** Routed explicitly to `maintenance` threadpool executor in `backend/cron/scheduler.py` to preserve streaming ingest capacity.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to PostgreSQL 16 via `backend.core.share_db` / `get_global_share_con()`, executes SQL deletes across share tables. Routed to `maintenance` executor pool. | Thread-local connection pool (`pg_connection.py`). Skips execution cleanly when `FLA_DEV_NO_CRONS=1` returning `("skipped", "skipped (dev_mode_no_crons)")`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_scale`)** | Pod APScheduler (Web Serving Pod Only) | Prunes remote share database on web serving pod via PostgreSQL 16; never scheduled to Celery workers. Routed to `maintenance` executor pool. | PostgreSQL transaction isolation / row-level locks. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/share/purge` | Share audit trail and invite management in Admin UI (`GET /api/admin/share/audit-logs`). |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prunes local share database if live-sharing enabled. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Analyst sessions are subject to retention pruning. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Dev-Mode Check:**
   - Evaluates `dev_mode_no_crons()`. If active (`FLA_DEV_NO_CRONS=1`), logs pause and returns `("skipped", "skipped (dev_mode_no_crons)")`.
2. **Retention Setting Resolution:**
   - Reads `share_audit_retention_days` from PostgreSQL table `share_settings` (falls back to 90 days).
3. **Expired Claim Token Cleanup:**
   - Deletes one-time claim tokens where `expires_at < iso_z_now()` from `remote_invite_claim_tokens`.
4. **Expired Invite Cleanup:**
   - Deletes invitation records where `expires_at IS NOT NULL AND expires_at < iso_z_now()` from `remote_invites`.
   - Foreign key CASCADE automatically prunes associated `invite_services`, claim tokens, and sessions.
5. **Stale Session Invalidation:**
   - Deletes sessions where `last_active_time < NOW() - max_idle_session_days` (default: 30 days) from `remote_sessions`.
6. **Audit Trail Purge:**
   - Calculates audit cutoff: `NOW() - share_audit_retention_days`.
   - Executes: `DELETE FROM remote_share_audit_logs WHERE timestamp < ?`.
7. **Telemetry & Log Recording:**
   - Records status and detail in `cron_runs` and `system_jobs` via `@global_job` decorator returning `("success", detail)`. Emits deleted record counts in stdout logs.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly relational database operations; zero cloud storage API calls.
  - **PostgreSQL Operations:** All queries execute via `backend.core.share_db` / `backend.core.metadata.pg_connection` and record profiling metrics.
  - **Duration Budget:** Execution completes in < 200ms.
- **Timing & Resource Budgets:**
  - Database purge queries: < 50ms.
- **Audit Checklist:**
  - Confirm active, non-expired sessions are NEVER deleted.
  - Verify `remote_share_audit_logs` correctly retains records within the retention window.

---

## 7. Failure Modes & Recovery Runbooks
- **Database Connectivity Issues:** Retries transparently via psycopg connection pool.
- **Empty / Fresh Instance:** If live-share feature has never been used, queries execute against empty tables with 0 deleted rows without error.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Purge:** `POST /api/admin/share/purge?audit_retention_days={days}&max_idle_session_days={days}`.
- **View Share Audit:** `GET /api/admin/share/audit-logs`.

---

## 9. Automated Verification Checklist
- [x] 1. Unit tests verify `purge_old_audit_logs` deletes expired logs and retains records within the retention window (ADR-22 PostgreSQL).
- [x] 2. Unit tests verify `purge_stale_share_records` prunes expired invites, claim tokens, and idle sessions (> 30 days).
- [x] 3. Unit tests verify valid, non-expired invites and recent active sessions remain intact.
- [x] 4. Unit tests verify `_run_share_audit_purge` job lifecycle, `maintenance` executor routing, and `("success", detail)` / `("skipped", ...)` return contract.
- [x] 5. Unit tests verify manual `POST /api/admin/share/purge` admin endpoint returns 200 with deletion counts.
- [x] 6. Unit tests verify `GET /api/admin/share/audit-logs` endpoint returns audit logs.
- [x] 7. Confirm zero FOS Class A/B calls.
