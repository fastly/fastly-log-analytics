# Background Job Specification: `clickhouse_backup_{service_id}` (RETIRED)

> [!NOTE]
> **Status: RETIRED (Architectural Invariant Decision)**
> This background job is officially **retired** from scheduled background execution.
>
> **Architectural Decision Rationale:**
> Fastly Object Storage (FOS) and the DuckLake lakehouse catalog are the authoritative, durable source of truth for all telemetry, logs, and RUM data. ClickHouse acts strictly as an ephemeral analytical serving projection and query accelerator in `DEPLOYMENT_MODE=high_throughput`.
>
> Running periodic scheduled backups of ClickHouse tables duplicated data, consumed unnecessary disk space, and created unnecessary failure modes. In the event of ClickHouse node failure or data loss, ClickHouse state is fully, reliably reconstructible on-demand directly from FOS sealed artifacts via the bounded administrative replay pipeline (`POST /api/admin/clickhouse/replay` and `backend.core.clickhouse_publication.full_rebuild`).

---

## 1. Overview & Historical Context
- **Job Identifier:** `clickhouse_backup_{service_id}`
- **Original Category:** High-Scale Fact Store Durability & Incremental Table Backup
- **Current Status:** Retired from background schedulers (APScheduler & RedBeat).
- **Authoritative Recovery Path:**
  - All log events reside durably in FOS raw directories (`raw/request/`, `raw/rum/`) and DuckLake Parquet tables.
  - The PostgreSQL `PgManifest` tracks published batches and generations.
  - Reconstruction is executed via `POST /api/admin/clickhouse/replay` with zero dependency on disk-level backups.

---

## 2. On-Demand Library Function
The underlying incremental table backup utility remains available as a low-level helper in `backend/high_scale/clickhouse_backup.py` (`run_incremental_clickhouse_backup`) for manual administrative maintenance or offline cluster migration, but it is not scheduled by any background daemon.

```python
from backend.high_scale.clickhouse_backup import run_incremental_clickhouse_backup

# Manual administrative snapshot to a configured ClickHouse storage disk:
receipt = run_incremental_clickhouse_backup(
    client,
    table="request_facts",
    destination="fos_backup",
)
```

---

## 3. Disaster Recovery & Reconstruction Flow
1. **Node Failure / Corrupted ClickHouse State:**
   - Deploy fresh ClickHouse instance or truncate tables.
   - Run `POST /api/admin/clickhouse/replay` with the target `service_id` and desired `dataset_id`.
2. **Replay Execution:**
   - `FosArtifacts` streams sealed Parquet artifacts directly from FOS.
   - `full_rebuild()` re-inserts facts and minute dimension summaries with transactional verification.
   - `PgManifest` activates the new generation upon successful verification.
