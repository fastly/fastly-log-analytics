# High-Scale Request Logs Ingestion Stream Design

- **Date:** 2026-10-09
- **Author:** Antigravity & Engineering Team
- **Status:** Approved
- **Target Scale:** 2,000,000 RPS on Fastly Edge
- **Target Latency:** < 5s edge-to-dashboard freshness (< 2s target)
- **Primary Environment:** Elevation Kubernetes (`se-demo` namespace)
- **Relevant Standards & ADRs:**
  - [ADR-14: DuckLake Replacement](docs/adr/14-ducklake-replacement.md)
  - [ADR-15: Multi-Writer Topology](docs/adr/15-multi-writer-topology.md)
  - [ADR-16: Ingest Ledger](docs/adr/16-ingest-ledger.md)
  - [ADR-21: High-Scale Ownership and Recovery Gate](docs/adr/21-high-scale-serving-architecture.md)
  - [ADR-22: Postgres-Only Metadata](docs/adr/22-postgres-only-metadata.md)
  - [Runbook: High-Scale Vertical Slice](docs/runbooks/high-scale-vertical-slice.md)

---

## 1. Overview & Problem Statement

At high scale (targeting 2,000,000 requests per second at the Fastly edge), streaming Fastly real-time VCL logs into DuckLake and Celery ledger workers introduces severe bottlenecks:
1. **Catalog Commit Bloat:** Workers committing files in small batches into a PostgreSQL DuckLake catalog cause snapshot churn, write amplification, and compaction debt.
2. **Per-Object I/O Serialization:** The initial prototype in `backend/high_scale/worker.py` and `backend/high_scale/orchestration.py` performed one S3 GET, one local Parquet serialization, one S3 PUT archive, multiple PostgreSQL transactions, and individual ClickHouse HTTP INSERTs for *every single `.gz` file*. When dozens of files arrive per second, round-trip serialization stalls ingestion, resulting in multi-minute lag.
3. **ClickHouse Insert Best Practices Violation:** ClickHouse is optimized for large block inserts (10,000 to 100,000 rows per block). Performing tiny, frequent inserts creates excessive small data parts, triggering merge pressure and "too many parts" errors.
4. **System Maintenance Failures:** Ingest workers encountered `HTTP 413` errors during periodic maintenance sweeps when executing DDL against `system.*` tables over the ClickHouse HTTP interface.

This design establishes a high-throughput, stream-batched ingestion pipeline specifically for **Fastly edge request logs**, maximizing **DRY code reuse** across existing components, eliminating per-file I/O overhead, and ensuring `< 5s` end-to-end freshness.

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
│  [Bulk ClickHouse Block]  [Batched Parquet Artifact]  [Batched Claims]│
│  (1 HTTP POST / 10k-50k)  (1 FOS Archive S3 PUT)     (1 multi-row PG) │
└────────┬─────────────────────────┬────────────────────────┬───────────┘
         │                         │                        │
         ▼                         ▼                        ▼
  [ClickHouse Cluster]      [FOS Archive Store]    [PostgreSQL Control]
  - request_facts           - verified Parquet      - ownership epoch
  - request_aggregates      - ADR-21 manifest       - atomic cursor
  - projection tables
         │
         ▼
  [Sub-second Dashboard / API Serving]
```

### Ingestion Lifecycle Stages

1. **Discovery & Leases:**
   - The coordinator lists a page of raw `.gz` objects under the current domain cursor (e.g., 50–100 keys per page).
   - The worker claims the discovered objects in PostgreSQL using a multi-row statement (`claim_sources_batch`), bound to the service's `expected_owner_epoch` and a 300-second lease.
2. **Concurrent In-Memory Decompress & Decode:**
   - Worker threads concurrently decompress gzip payloads in memory and parse JSON lines.
   - Field normalization and type casting reuse the central `backend/core/field_registry.py`.
   - Corrupted lines are quarantined into in-memory `dead_letters` without throwing or aborting the stream.
3. **Single Block ClickHouse Sink:**
   - All valid decoded rows across the page (10,000–50,000 events) are consolidated into a single `HighScaleBatch`.
   - The batch is inserted via a single ClickHouse HTTP POST into `request_facts`.
   - Aggregate dimensions and projections (`request_aggregates`, `origin_summary`, `origin_dimensions`, `performance_dimensions`, `security_dimensions`, `network_dimensions`) are computed in-memory across the block and bulk-inserted.
4. **Batched Archive & Manifest:**
   - Instead of writing an archive Parquet per `.gz` file, the entire page is serialized into a single consolidated Parquet archive artifact written to FOS under `high-scale/archive/{manifest_id}.parquet`.
   - An `ArchiveManifest` is registered in PostgreSQL tracking exact row counts, byte counts, canonical SHA-256 digests, and state transitions (`artifact_verified -> manifest_committed`).
5. **Atomic Commit & Cursor Advance:**
   - After ClickHouse confirms block insertion and FOS confirms archive write, PostgreSQL acknowledges all claimed objects in a single transaction (`acknowledge_sources_batch`).
   - The domain cursor advances to the next lexicographical boundary.
6. **Adaptive Polling Loop:**
   - Worker loop interval scales dynamically: when the last page processed full batches, the next loop fires immediately (0s sleep); when idle, it sleeps 500ms–1,000ms.

---

## 3. Data Models & DRY Code Reuse

To eliminate duplicate logic, the pipeline leverages existing models, registries, and schemas across the codebase:

### 1. Unified Field Registry (`backend/core/field_registry.py`)
- Direct invocation of `LOG_FIELD_CATALOG` and field normalization functions.
- Core extracted attributes: `timestamp`, `client_ip`, `country`, `url`, `status`, `method`, `resp_body_size`, `resp_header_content_type`, `fastly_pop`, `origin_host`, `cache_status`.
- Dynamic and custom fields configured per service (`configs/{service_id}.json`) are serialized into ClickHouse `custom_fields Map(String, String)`.
- CMCD querystring and header attributes are parsed and mapped to `cmcd Map(String, String)`.

### 2. ClickHouse Fact & Aggregate Tables (`backend/high_scale/sql/`)
- **`request_facts`:**
  ```sql
  CREATE TABLE IF NOT EXISTS request_facts (
      service_id LowCardinality(String),
      event_id UUID,
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
  ) ENGINE = ReplacingMergeTree
  PARTITION BY (service_id, toYYYYMMDD(event_timestamp))
  ORDER BY (service_id, event_timestamp, event_id);
  ```
- **`request_aggregates`:**
  ```sql
  CREATE TABLE IF NOT EXISTS request_aggregates (
      service_id LowCardinality(String),
      bucket_start DateTime('UTC'),
      dimension LowCardinality(String),
      value String,
      request_count UInt64,
      batch_id UUID,
      publication_state Enum8('pending' = 1, 'visible' = 2)
  ) ENGINE = ReplacingMergeTree
  PARTITION BY (service_id, toYYYYMMDD(bucket_start))
  ORDER BY (service_id, bucket_start, dimension, value, batch_id);
  ```
- **Projection Tables:** `origin_summary`, `origin_dimensions`, `performance_dimensions`, `security_dimensions`, and `network_dimensions` populated via existing builders (`build_origin_projection_rows`, `build_performance_projection_rows`, etc.).

### 3. Control Plane & Manifest Storage (`backend/high_scale/postgres_control.py`)
- Reuse `PostgresControlPlane` with batch-extended queries for:
  - `claim_sources_batch(service_id, object_keys, worker_id, lease_seconds, expected_owner_epoch)`
  - `acknowledge_sources_batch(service_id, object_keys, manifest_id)`
  - `register_archive_manifest(manifest, owner_epoch)`

---

## 4. Error Handling, Recovery & Maintenance

1. **Fencing & Idempotency:**
   - Every operation is fenced by `expected_owner_epoch`. If ownership changes or a lease expires, all in-flight inserts fail-fast.
   - ClickHouse uses `ReplacingMergeTree` deduplicating on `(service_id, event_timestamp, event_id)`. Re-attempting an insert is safe and idempotent.
   - Queries only scan rows where `batch_id IN (SELECT batch_id FROM high_scale_batch_publications WHERE publication_state = 'visible')`.
2. **Lease Expiry & Crash Recovery:**
   - Leases expire after 300 seconds. Sibling workers periodically run `_recover_expired_claims`, picking up abandoned objects without blocking the main cursor.
3. **Quarantine Without Stalling:**
   - Corrupted JSON lines are extracted into `dead_letters` and bundled into the manifest artifact. The pipeline never raises an unhandled exception on a malformed log line.
4. **Fixing the ClickHouse System Maintenance Bug (HTTP 413):**
   - In `backend/core/clickhouse_schema.py`, `maintain_clickhouse_system_tables(client)` attempts `ALTER TABLE system.metric_log MODIFY TTL ...` and `TRUNCATE TABLE system.trace_log`. ClickHouse rejects DDL on system tables over HTTP.
   - Fix: Wrap all system table maintenance calls in defensive exception suppression, log debug warnings without raising, and let ClickHouse manage its own system log rotations.
5. **Decoupled Background Deletion:**
   - Ingest workers do NOT delete raw `.gz` files inline.
   - The background thread `HighScaleDeletionSweeper` purges FOS raw files only after `manifest_committed` state is reached and a 15-minute grace period has elapsed.

---

## 5. Testing & Verification Strategy

### 1. Unit & Contract Tests (`tests/high_scale/`)
- `tests/high_scale/test_batch_ingest.py`: Tests processing a page of 50 `.gz` source objects, verifying they produce a single ClickHouse block insert, unified archive artifact, and batched Postgres claims.
- `tests/high_scale/test_field_registry_reuse.py`: Verifies field normalization and mapping against `LOG_FIELD_CATALOG`.
- `tests/high_scale/test_maintenance_suppression.py`: Simulates HTTP 413 and verifies worker loop continuity.
- `tests/high_scale/test_recovery.py`: Validates expired claim recovery and deduplication.

### 2. Physical Deployment & Continuous Verification Mandate
Following GEMINI.md quality directives:
1. Intentional Git commit and push to `origin/release/v3.0.0-beta3`.
2. Execution of the unified parallel deployment and verification script:
   ```bash
   export MONITOR_MINUTES=1 && ./scripts/dev/deploy_test_all.sh
   ```
3. Direct cluster verification on Elevation K8s (`se-demo`):
   - Inspect `deployment/high-scale-worker` logs to ensure zero HTTP 413 errors and active sub-second batch loops.
   - Query ClickHouse `request_facts` directly for live row arrival.
   - Verify browser dashboard metrics and Plotly charts render with non-zero aggregates.
   - Report request log recency lag across all 3 active environments (Local Standard, Remote Standard GCE, Remote High-Scale Elevation).
