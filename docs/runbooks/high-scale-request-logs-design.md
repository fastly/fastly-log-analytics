# High-Scale Request Logs Ingestion Stream Design

- **Date:** 2026-10-09
- **Author:** Engineering Team
- **Status:** Proposed (not yet implemented; supersedes the ADR-21 per-source manifest, see §6.1)
- **Target Scale:** 2,000,000 RPS on Fastly Edge
- **Target Latency:** < 5s p95 from FOS object landing to dashboard visibility (< 2s stretch). Fastly's log flush period is upstream of this budget and must be measured separately (see §5.2).
- **Primary Environment:** Elevation Kubernetes (namespace from `ELEVATION_NAMESPACE`)
- **Relevant Standards & ADRs:**
  - [ADR-14: DuckLake Replacement](../adr/14-ducklake-replacement.md)
  - [ADR-15: Multi-Writer Topology](../adr/15-multi-writer-topology.md)
  - [ADR-16: Ingest Ledger](../adr/16-ingest-ledger.md)
  - [ADR-20: ClickHouse Serving Plane](../adr/20-clickhouse-serving-plane.md)
  - [ADR-21: High-Scale Ownership and Recovery Gate](../adr/21-high-scale-serving-architecture.md)
  - [ADR-22: Postgres-Only Metadata](../adr/22-postgres-only-metadata.md)
  - [Runbook: High-Scale Vertical Slice](high-scale-vertical-slice.md)

---

## 1. Overview & Problem Statement

At high scale (targeting 2,000,000 requests per second at the Fastly edge), streaming Fastly real-time VCL logs into DuckLake and Celery ledger workers introduces severe bottlenecks:
1. **Catalog Commit Bloat:** Workers committing files in small batches into a PostgreSQL DuckLake catalog cause snapshot churn, write amplification, and compaction debt.
2. **Per-Object I/O Serialization:** The initial prototype in `backend/high_scale/worker.py` and `backend/high_scale/orchestration.py` performed one S3 GET, one local Parquet serialization, one S3 PUT archive, multiple PostgreSQL transactions, and individual ClickHouse INSERTs for *every single `.gz` file*. When dozens of files arrive per second, round-trip serialization stalls ingestion, resulting in multi-minute lag.
3. **ClickHouse Insert Best Practices Violation:** ClickHouse is optimized for large block inserts (10,000 to 100,000 rows per block). Tiny, frequent inserts create excessive small data parts, triggering merge pressure and "too many parts" errors.
4. **System Maintenance Noise:** Periodic `TRUNCATE` / `ALTER ... MODIFY TTL` against `system.*` tables over the ClickHouse HTTP interface was observed failing with `HTTP 413`. The calls are already wrapped in `try/except` (`maintain_clickhouse_system_tables`), so they no longer abort the worker; the remaining problem is that the mechanism itself is wrong (see §4.4).

This design establishes a high-throughput, stream-batched ingestion pipeline specifically for **Fastly edge request logs**, maximizing **DRY code reuse** across existing components, eliminating per-file I/O overhead, and meeting the freshness budget above.

---

## 2. Architecture & Ingestion Flow

```
                      [Fastly Real-Time Edge Logs]
                                   │
                                   ▼
                      [FOS raw/request/*.gz]
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   high-scale-worker (K8s Replicas)                    │
│                                                                        │
│  1. S3 Source Discovery (Page of N objects, e.g. 50-100 keys)         │
│  2. Concurrent In-Memory Decompress & Decode (via field_registry)      │
│  3. In-Memory Micro-Batch Aggregation (Domain Events & Projections)   │
│                                                                        │
│        ┌─────────────────────────┼────────────────────────┐           │
│        ▼                         ▼                        ▼           │
│  [Batched Parquet Artifact] [Bulk ClickHouse Block]  [Batched Claims] │
│  (1 FOS archive PUT, FIRST) (1 INSERT/table/page)    (1 multi-row PG) │
└────────┬─────────────────────────┬────────────────────────┬───────────┘
         │                         │                        │
         ▼                         ▼                        ▼
  [FOS Archive Store]       [ClickHouse Cluster]    [PostgreSQL Control]
  - verified Parquet        - request_facts         - ownership epoch
  - ADR-21 manifest         - request_aggregates    - manifest state
                            - projection tables     - atomic cursor
                                     │
                                     ▼
                     [Sub-second Dashboard / API Serving]
```

### Ingestion Lifecycle Stages

The order below follows the ADR-21 archive state machine. FOS is the recovery authority, so the archive is verified and its manifest committed **before** rows become visible in ClickHouse, and sources are acknowledged only after both.

1. **Discovery & Leases:**
   - The coordinator lists a page of raw `.gz` objects under the current domain cursor (50–100 keys per page).
   - A new `claim_sources_batch` claims the page in PostgreSQL with one multi-row statement, bound to `expected_owner_epoch` and a 300-second lease (`HIGH_SCALE_LEASE_SECONDS`). Today only the per-source `claim_source` exists; `claim_batch` is the *publication*-batch claim and is a different object.
2. **Concurrent In-Memory Decompress & Decode:**
   - Worker threads decompress gzip payloads and parse lines (`backend/high_scale/decoder.py`); normalization reuses `backend/core/field_registry.py`.
   - Memory is bounded: cap in-flight decompressed bytes per page (a byte cap such as `HIGH_SCALE_PAGE_MAX_BYTES`, in addition to the object-count cap) and stream-decode rather than holding every payload in full.
   - Malformed lines go to `dead_letters` and never abort the page. The count is recorded in the manifest and exported as a metric. A page whose dead-letter ratio exceeds a configured threshold fails and alerts instead of silently publishing a mostly-empty batch.
3. **Archive First:**
   - The page is serialized to one Parquet artifact at `high-scale/archive/{manifest_id}.parquet` (`artifact_uploading -> artifact_verified`: checksum-verify the uploaded object before proceeding).
   - The manifest moves `manifest_prepared -> manifest_committed` in PostgreSQL, fenced by `owner_epoch`. It must list every source object with key, version, checksum and per-source row range, so any single source can be replayed and deleted independently (see §6.1).
4. **ClickHouse Sink (pending):**
   - Facts and projections (`request_aggregates`, `origin_summary`, `origin_dimensions`, `performance_dimensions`, `security_dimensions`, `network_dimensions`) are computed in memory across the page and inserted in one block per table with `publication_state = 'pending'`, one `batch_id`, and a deterministic `insert_deduplication_token` (`batch_id` + table), so a retried INSERT is dropped by ClickHouse block deduplication.
   - Use synchronous inserts (`async_insert=0`) with large blocks; do not depend on many small inserts plus `async_insert`.
5. **Publish:**
   - After the insert is durable on quorum, write the `high_scale_batch_publications` row (`pending -> visible`, with `expected_rows` / `visible_rows` / `quorum_acked`; see `backend/high_scale/sql/publication_schema.sql`). This is the atomic visibility point: readers see a batch only after it flips.
6. **Acknowledge & Advance Cursor:**
   - A new `acknowledge_sources_batch` acknowledges all claimed objects and advances the cursor in **one** PostgreSQL transaction, fenced by owner epoch.
   - **Partial failure:** the cursor advances only if the whole page succeeded (existing behavior in `orchestration.py`: `failed > 0` holds the cursor). Failed sources are retried via expired-claim recovery; sources already `manifest_committed` are treated as duplicates on retry.
7. **Adaptive Polling Loop:**
   - If the last page was full, loop immediately; if short or empty, sleep with jitter (500–1,000 ms). This replaces the current default `HIGH_SCALE_WORKER_INTERVAL_SECONDS=5`, which alone would consume the entire 5 s budget.

---

## 3. Data Models & DRY Code Reuse

The pipeline reuses existing models, registries, and schemas. **The source of truth for DDL is `backend/high_scale/sql/*.sql`; the snippets below are abridged and must not be copied.**

### 1. Unified Field Registry (`backend/core/field_registry.py`)
- Direct invocation of `LOG_FIELD_CATALOG` and its normalization functions.
- Core attributes: `timestamp`, `client_ip`, `country`, `url`, `status`, `method`, `resp_body_size`, `resp_header_content_type`, `fastly_pop`, `origin_host`, `cache_status`. The abridged `request_facts` below shows only a subset; the shipped DDL is authoritative for which are typed columns.
- Per-service custom fields (`configs/{service_id}.json`) go to `custom_fields Map(String, String)`; CMCD attributes go to `cmcd Map(String, String)`. Map lookups are slow: any field that dashboards filter or group on must be promoted to a typed (or materialized) column, not left in the map.
- `client_ip` is PII. Analyst masking is applied at the query layer under the existing RBAC model; ClickHouse tables hold raw values and are admin-only.

### 2. ClickHouse Fact & Aggregate Tables (`backend/high_scale/sql/`)
- **`request_facts`** (abridged):
  ```sql
  CREATE TABLE IF NOT EXISTS request_facts (
      service_id LowCardinality(String),
      event_id UUID,  -- deterministic: hash(service_id, source_object_key, source_object_version, line_ordinal)
      event_timestamp DateTime64(3, 'UTC'),
      ingest_timestamp DateTime64(3, 'UTC'),
      source_object_key String,
      source_object_version String,
      line_ordinal UInt64,
      transform_version LowCardinality(String),
      batch_id UUID,
      publication_state Enum8('pending' = 1, 'visible' = 2),
      country LowCardinality(String),
      client_ip String,
      url String,
      custom_fields Map(String, String),
      cmcd Map(String, String)
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/request_facts', '{replica}')
  PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
  ORDER BY (service_id, event_timestamp, event_id)
  SETTINGS index_granularity = 8192;
  ```
  - The engine is `ReplicatedMergeTree`, **not** `ReplacingMergeTree`. Idempotency comes from (a) the deterministic `event_id`, (b) insert deduplication tokens, and (c) publication gating, not from background merges. `ReplacingMergeTree` dedup is eventual and would not make reads exact without `FINAL`.
  - Before scale: `PARTITION BY (service_id, day)` creates `services x days` partitions, while ClickHouse guidance is low partition cardinality. Evaluate `PARTITION BY toYYYYMMDD(event_timestamp)` with `service_id` leading the sort key, add a TTL tied to `data_retention_days`, and decide the shard key, compression codecs for `url` / `client_ip`, and skip indexes (§6.3).
- **`request_aggregates`** (abridged): `ORDER BY (service_id, bucket_start, dimension, value, batch_id)`, also `ReplicatedMergeTree`. It holds one row per batch, so reads must `sum(request_count)`; or move to `SummingMergeTree` / `AggregatingMergeTree` if read cost matters.
- **Projection tables:** `origin_summary`, `origin_dimensions`, `performance_dimensions`, `security_dimensions`, `network_dimensions`, populated via existing builders (`build_origin_projection_rows`, `build_performance_projection_rows`, ...). They carry the same `batch_id` and become visible together with the facts under one publication row.

### 3. Control Plane & Manifest Storage (`backend/high_scale/postgres_control.py`)
- Reuse `PostgresControlPlane` and add batch variants (new; not yet in the codebase):
  - `claim_sources_batch(service_id, object_keys, worker_id, lease_seconds, expected_owner_epoch)`
  - `acknowledge_sources_batch(service_id, object_keys, manifest_id, next_cursor)`
  - `register_archive_manifest(manifest, owner_epoch)`, extended to accept multiple sources (§6.1)
- Each batch method is a single transaction that re-checks owner and epoch (as `claim_batch` does via `_locked_owner` / `_check_epoch`) and is idempotent on retry.

---

## 4. Error Handling, Recovery & Maintenance

1. **Fencing & Idempotency:**
   - Every PostgreSQL mutation is fenced by `expected_owner_epoch`. ClickHouse cannot check an epoch, so fencing there is by `batch_id`: a stale worker may write `pending` rows but cannot publish, because the epoch-checked manifest commit precedes publication. Orphaned `pending` rows are invisible and are dropped by a periodic sweeper (by `batch_id` / partition) once their batch is abandoned.
   - Re-attempted inserts are safe through the deterministic `event_id` and insert deduplication tokens.
   - Readers must filter on published batches using the latest publication row, as `query_service.py` does: `batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL WHERE publication_state = 'visible')`. `FINAL` is required because that table is a replacing engine.
2. **Lease Expiry & Crash Recovery:**
   - Leases expire after 300 s. `_recover_expired_claims` runs at the start of every page (`orchestration.py`) and re-claims abandoned objects regardless of cursor position. A 300 s lease also bounds crash-recovery latency; if the freshness target must hold across worker crashes, use a shorter lease with heartbeat renewal.
3. **Quarantine Without Stalling:**
   - Malformed lines are collected as `dead_letters`, written alongside the archive artifact, counted in the manifest, and alerted on when the ratio exceeds the threshold (§2, stage 2). An unreadable *object* (for example, corrupt gzip) is recorded as a terminal `missing`/failed source with its evidence preserved, not retried forever.
4. **ClickHouse System-Table Maintenance:**
   - Current state: `maintain_clickhouse_system_tables(client)` in `backend/core/clickhouse_schema.py` already catches and `log.debug`s failures from `TRUNCATE TABLE IF EXISTS system.{trace_log,processors_profile_log}` and `ALTER TABLE system.{...} MODIFY TTL event_date + INTERVAL 1 DAY`. The worker loops call it at most every 300 s and also swallow failures. No additional suppression is needed.
   - Better fix: stop mutating `system.*` from workers. Set retention declaratively in the ClickHouse server config (per-log `<ttl>`, or disable unneeded logs such as `trace_log` / `processors_profile_log`), then remove the periodic call and keep only the one-time call in schema creation if it still succeeds.
   - `HTTP 413` means the request was too large, which points at a proxy or server limit rather than a DDL-over-HTTP restriction. Confirm the root cause from the failing request before declaring it fixed. Log persistent failures at `warning` once per process rather than `debug` every cycle, so a broken maintenance path stays visible.
5. **Decoupled Background Deletion:**
   - Ingest workers never delete raw `.gz` files inline. `HighScaleDeletionSweeper` (through `DeletionController`, the only deletion authority per ADR-21) deletes after `manifest_committed -> deletion_eligible`, the owner-epoch check, no active replay lease, and the grace period (`HIGH_SCALE_DELETION_GRACE_SECONDS`, default 900 s).
   - Because the manifest's deletion deadline must be honored per source, the batch manifest must carry per-source deadlines.
6. **Backpressure:**
   - If ClickHouse insert latency or parts-per-partition rises ("too many parts" rejections), workers slow polling and cap concurrent inserts rather than retrying hot. If PostgreSQL is unavailable, workers stop claiming; they never proceed unfenced.

---

## 5. Testing & Verification Strategy

### 1. Unit & Contract Tests (`tests/high_scale/`)
- `test_batch_ingest.py`: a page of 50 `.gz` objects yields one INSERT per ClickHouse table, one archive artifact, one manifest covering all 50 sources, one batched claim, and one batched acknowledge.
- `test_batch_ordering.py`: assert the order archive verified -> manifest committed -> ClickHouse insert -> publication visible -> acknowledge; a crash after each step leaves a recoverable state that is either invisible or complete.
- `test_batch_idempotency.py`: replaying the same page (same `event_id`s and dedup token) leaves row counts unchanged; an overlapping retry under a different `batch_id` does not double count.
- `test_partial_page_failure.py`: one bad object holds the cursor; successful siblings are not re-ingested; the bad object is retried or quarantined.
- `test_fencing.py`: a stale-epoch worker cannot commit a manifest, publish, or acknowledge.
- `test_field_registry_reuse.py`: normalization parity with `LOG_FIELD_CATALOG`.
- `test_maintenance_suppression.py`: a maintenance failure (HTTP 413) does not stop the worker loop (behavior already implemented; this pins it).
- `test_recovery.py`: expired-claim recovery and deduplication.
- Follow repo conventions: `uv run pytest`, derive fixture keys from the producer, and keep the security-regression floor (206) intact.

### 2. Scale & Freshness Verification
- **Capacity model:** state the arithmetic for 2,000,000 RPS (events/s ÷ rows per page ÷ pages/s per worker = worker count), covering ClickHouse insert throughput, archive PUT throughput, and the ADR-21 recovery budget (256,000,000 events/s replay with a 25% live-ingest reservation, as that ADR states). At 10k–50k rows per page, 2M RPS is roughly 40–200 pages per second, so the per-page figures in this document do not by themselves show the target is reachable.
- **Per-stage latency:** measure list, GET, decode, archive PUT+verify, manifest commit, ClickHouse insert, publish, and acknowledge; report p50/p95 against the 5 s budget.
- **Fastly flush period:** measure separately. If it alone exceeds the budget, edge-to-dashboard freshness cannot be met by this pipeline and the target stays defined as FOS-landing-to-visible.
- Use `--target clickhouse` seeding and the vertical-slice runbook for load, and record results in the runbook rather than in this design.

### 3. Deployment & Verification
1. Commit (explicitly staged files only) and push to `origin/release/v3.0.0-beta3`.
2. After the push, run the deployment and verification script:
   ```bash
   export MONITOR_MINUTES=1 && ./scripts/dev/deploy_test_all.sh
   ```
3. Verify on Elevation K8s (`$ELEVATION_NAMESPACE`):
   - `deployment/high-scale-worker` logs show no maintenance warnings and batch loops completing within budget.
   - ClickHouse `request_facts` shows live arrival, and no `pending` batch is older than the lease window.
   - Dashboard metrics and Plotly charts render non-zero aggregates.
   - Report request-log recency lag across the three active environments (Local Standard, Remote Standard GCE, Remote High-Scale Elevation).

---

## 6. Open Issues Requiring Decision Before Implementation

1. **Decision: this design supersedes the ADR-21 per-source manifest.** `ArchiveManifest` (`backend/high_scale/archive_models.py`) currently holds a single `ArchiveSourceObject`. It becomes a batch manifest: one artifact plus a list of source entries, each with key, version, checksum, row range, and deletion deadline. Replay and deletion stay per source. ADR-21 carries an amendment note pointing here. Implementation work: change `ArchiveManifest`, `register_archive_manifest`, and the deletion ledger to iterate source entries.
2. **Runtime mode is not enabled.** The rest of ADR-21 still stands: the `high_scale` mode stays gated on operator selection, a continuous-ingest vertical slice, an archive-only recovery test, a differential canary, and rollback evidence. This design does not enable it.
3. **Partitioning and sharding** for `request_facts` at 2M RPS (§3.2).
4. **Fastly flush period** versus the freshness budget (§5.2).
