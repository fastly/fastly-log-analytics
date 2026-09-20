> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `share_audit_purge` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `share_audit_purge`

## 1. Overview & Objectives
- **Job Identifier:** `share_audit_purge`
- **Category:** Live-Share Security, Compliance & Audit Retention
- **Purpose:** Daily retention cleanup of expired remote analyst live-share invitations, invalidated authentication sessions, and historical share audit logs in `data/system/remote_share.db`.
- **Why It Runs:** Live Dashboard Sharing (Path B) generates audit events, session tokens, and invite records. For compliance and operational hygiene, expired invitations, stale session records, and audit logs beyond the retention window (`share_audit_retention_days`, default: 90 days) must be safely purged.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 03:45 UTC (`hour=3, minute=45`).
- **Timing Rationale:** Runs after `full_sync` (03:30 UTC) and before `optimize` (04:00 UTC) during the quiet daily maintenance window.
- **Scope:** Process-global singleton job.
- **Configurable Overrides:** `share_settings.share_audit_retention_days` (default: 90 days).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to `data/system/remote_share.db`, executes SQL deletes, checkpoints WAL. | ThreadLocalPool connection lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Prunes remote share database on web serving pod; never scheduled to Celery workers. | Local SQLite lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/share/purge` | Share audit trail and invite management in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prunes local share database if live-sharing enabled. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Analyst sessions are subject to retention pruning. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Retention Setting Resolution:** Reads `share_audit_retention_days` from `remote_share.db` table `settings` (falls back to 90 days).
2. **Expired Invite Cleanup:**
   - Deletes invitation records where `expires_at < NOW()` and `status = 'pending'`.
3. **Stale Session Invalidation:**
   - Deletes session tokens where `last_seen < NOW() - INTERVAL '30 DAYS'`.
4. **Audit Trail Purge:**
   - Calculates audit cutoff: `NOW() - share_audit_retention_days`.
   - Executes: `DELETE FROM share_audit_log WHERE timestamp < ?`.
5. **SQLite WAL Maintenance:**
   - Issues `PRAGMA wal_checkpoint(TRUNCATE)` on `remote_share.db`.
6. **Telemetry & Log Recording:**
   - Emits deleted records count in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local database operations; zero cloud API calls.
  - **SQLite Operations:** All queries execute via `ThreadLocalPool` and appear in Live Query Monitor under `service=__global_share__`.
  - **Duration Budget:** Execution must complete in < 200ms.
- **Timing & Resource Budgets:**
  - Database purge queries: < 50ms.
  - WAL checkpoint: < 50ms.
- **Audit Checklist:**
  - Confirm active, non-expired sessions are NEVER deleted.
  - Verify `share_audit_log` correctly retains records within the retention window.

---

## 7. Failure Modes & Recovery Runbooks
- **SQLite Write Lock Contention:** ThreadLocalPool handles retries transparently.
- **Database Missing:** If live-share feature has never been used, safely skips execution.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Purge:** `POST /api/admin/share/purge`.
- **View Share Audit:** `GET /api/share/audit`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Insert an expired invite and an audit record older than 90 days into `remote_share.db`.
- [ ] 2. Trigger `POST /api/admin/share/purge`; confirm HTTP 200.
- [ ] 3. Verify the expired records are purged from the database.
- [ ] 4. Verify valid, non-expired invites and recent audit records remain intact.
- [ ] 5. Confirm in `cron_runs`: status `success` with non-zero duration.
- [ ] 6. Confirm zero FOS Class A/B calls in `usage_log.db`.
