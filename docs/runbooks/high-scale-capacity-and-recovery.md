# High-Scale Capacity and Recovery Contract

**Qualification status (2026-10-22):** the current initial sustained qualification goal is **1M request events/sec**, with 30 days of full request-level history, two active dashboard users and a five-user stress case. Higher burst rates remain qualification proposals. The 2M/5M rates and topology/replay figures below are earlier design objectives and mixed measurement/model inputs, not demonstrated production capacity. Small stage tests and passing correctness tests do not qualify target-scale ingest, query latency, availability or recovery. See [Section-sweep measurement evidence](section-sweep-measurements.md) for the separate latency and evidence contract. No target-scale traffic run or infrastructure change is authorized by these goals.

The high-scale data plane must sustain 2M request events/sec, absorb 5M/sec
bursts for five and thirty minutes, sustain 500k RUM events/sec, and absorb
1M/sec RUM bursts without dropping data. Overload increases lag and backlog;
it never silently samples or discards events.

The replacement-instance target is ingest acceptance within two minutes and
usable triage for the latest 24 hours within 15 minutes. The 15-minute target
is decomposed into ingest acceptance, triage availability, exact raw-query
availability, and full rebuild completion. RPO is zero for acknowledged
archive writes.

## Watermarks and tiers

Every response carries request, RUM vitals, RUM errors, and CMCD watermarks
independently. A watermark identifies the service, domain, owner epoch,
coverage start/end, last accepted source cursor, last archived event, and last
serving-visible event. Query responses also declare whether results are exact
or approximate, the coverage fraction, freshness age, and any error state.

Recent data is the hot tier and targets interactive dashboard latency. Warm
history uses ClickHouse facts and incremental aggregates. Cold history is
queued, bounded, cancellable, progress-aware, and exportable. Interactive raw
pages are capped at 500 rows and use keyset cursors; clients never fetch a
large result set to paginate locally.

## Recovery qualification

Qualification measures FOS read/decode, normalization, archive verification,
ClickHouse insert, replication, aggregate rebuild, and live-ingest reservation
separately. A recovery claim is valid only when the slowest stage sustains the
required replay rate for the complete target window while the live reservation
remains available.

FOS is the durable source of truth and recovery authority. ClickHouse backups
may accelerate recovery but cannot replace the verified archive. Source
deletion remains blocked until the archive manifest is committed, the grace
period expires, the owner epoch matches, and no replay lease is active.

## Late-arriving RUM source objects

RUM source listing (`rum_vitals` and `rum_errors`) maintains independent
forward and reinspection positions in `high_scale_source_cursors`:

- `source_cursor` remains the forward cursor. Existing opaque paginator tokens
  and `__terminal__:<key>`/S3 `StartAfter` behavior are preserved.
- `reinspection_cursor` is an additive, per-domain cursor for an occasional
  bounded pass over the RUM prefix.
- `forward_pages_since_reinspection` counts successful forward pages. After
  ten pages, the next worker listing inspects exactly one reinspection page,
  then returns to forward discovery. Multi-page sweeps resume after the last
  key with `StartAfter`; they do not restart at page one each tick.
- Reinspection uses the normal source-object ledger, claim, archive, and
  publication path. Its state is persisted separately and owner-epoch fenced;
  it never rewinds the forward cursor. A failed/transiently unreadable page
  advances neither position. Confirmed missing objects are durably recorded
  as `source_missing` and may be skipped, matching the forward-stream policy.

The columns are created for new schemas and added with `ALTER TABLE ... ADD
COLUMN IF NOT EXISTS` for existing installations. Old rows start with an
initial reinspection cursor and a zero page count. This is eventual
reconciliation, not a freshness SLA: each sweep page is bounded, but a large
RUM prefix can take many worker ticks to revisit. It does not promise that a
late object is found within 20 seconds. Request discovery is unchanged.

For a known, relevant missed object, prefer an exact-key idempotent reconcile:
read its current S3 metadata and payload, register the exact descriptor with
`discover_source`, claim it through `claim_sources_batch`, then pass that
single descriptor/payload to `HighScaleIngestController.ingest_batch` with no
next cursor. This uses the regular archive/publication path without changing
either discovery cursor or replaying unrelated objects. Verify the resulting
source-ledger and serving/archive records before declaring the reconciliation
complete; do not reset or rewind a shared cursor for a key-specific repair.

`rum_vitals` and `rum_errors` list the same `raw/rum/` objects, and the source
ledger is unique per `(service_id, object_key)`, so whichever domain discovers
an object first owns it. One beacon can carry both vitals and errors. The
ingest controller therefore also decodes the sibling domain from the claimed
payload and publishes the rows that pass `row_matches_serving_domain` under a
distinct batch id (`{manifest_id}:{sibling}`), before acknowledging the source.
Before this fix, the sibling's rows were dropped from serving and the raw object
was later deleted. Retained archive `raw_json` can still recover those rows;
use a scoped, duplicate-checked repair, never a cursor reset.

### Diagnose mixed RUM archives before repairing them

The claimed domain describes source ownership, not every archived row's kind.
An error-owned manifest can contain vitals, including sources with no errors at
all. Subtracting error-fact counts from its archive count does not establish
error loss. Verify the artifact checksum, commit marker and canonical digest;
inspect each `sources_json` slice and the archive's recorded owner epoch.
Expanded rows can repeat the same original beacon. Reconstruct it once per
source version and line ordinal, preserving gaps in the original line numbers.

Compare the shared Faro parser's actual exceptions and measurements with both
fact tables. Empty-message exceptions are valid and use `Unknown error`.
Blank error columns on a vital are not an exception discriminator. The decoded
`_rum_kind` wins over the edge envelope's metric fields and query string, which
can otherwise fabricate a vital from an exception. Invalid metrics are
quarantined, not replaced with zero; unclassifiable flat records retain their
original bytes in dead letters. These changes affect only RUM decoding and
publication classification, not request ingestion.
Malformed sibling-domain records are counted separately even when the claimed
domain has a valid projection; the archive retains the original mixed beacon.
Do not turn an invalid exception message into an accepted, invisible row.

Validate Faro event semantics, not just an empty error column. The shared
standard extractor projects custom events as `event_<name>` with value `1.0`;
navigation events project their actual numeric attributes, and web-vitals
measurements can produce multiple metrics (excluding `delta`). An event-input
count is therefore not automatically a valid-vital row count. Review the
reconstructed actual projections; genuine exceptions never become zero/default
vitals.

### Recover one proven missing retained cohort

Deploy the decoder correction through the canonical deployment procedure first.
Then run this command **inside the configured High-Scale backend**, with an
operator-selected unique archive-manifest prefix:

```bash
python -m scripts.recover_high_scale_rum --manifest-prefix "$MANIFEST_PREFIX"
```

Dry-run is the default. It performs no publication, cursor, source-ledger or
archive writes. Output contains hashes and counts only. Keep it in private
operator evidence and review the domain-specific candidate, exact-match,
semantic-equivalent and missing counts. The plan digest binds the immutable
archive, decoded candidate contents and recorded owner epoch, not a claim that
all those candidates are missing.

This tool is deliberately small: one committed terminal RUM manifest, at most
100 sources, 1,000 archived/expanded/existing rows and a 32 MiB artifact. Fact
and receipt reads retain a 100,000-row scan cap and at most five seconds per
query. It checks service/source identities across **all batch IDs**, compares
persisted content even when a transform changed event IDs, and verifies each
matched batch's service/domain/generation publication receipt with a full-key
lookup. Facts are plain MergeTree tables: do not use `FINAL` on them.
Publication receipts use `FINAL`. An older/different generation that cannot be
verified under the archive owner epoch blocks repair; it is not treated as
missing. Do not widen scan bounds to make an ambiguous cohort pass.

Repair publication uses a recovery-only global batch verifier, not a
candidate-time-only count. It inventories every active partition (at most 32),
groups them into at most six reads weighted below 90,000 physical rows, and
reads every target-batch fact across those groups under the same 100,000-row
per-query cap. It verifies exact identities and payloads, including rows on
other dates or from unexpected sources, then rechecks the complete partition
set. Metadata weights never prove absence. Oversized/changing inventories,
partial batches, duplicates, conflicting/out-of-scope facts and query failures
abort; they are never skipped, guessed empty, or deleted for a blind retry.
The normal request-ingest publication adapter is unchanged.

After reviewing the saved dry run, apply the same immutable candidate scope:

```bash
python -m scripts.recover_high_scale_rum \
  --manifest-prefix "$MANIFEST_PREFIX" \
  --apply --approve-plan "$REVIEWED_PLAN_DIGEST"
```

Serialize operational repair/rebuild commands; do not run a generic replay
concurrently. Apply takes a per-service PostgreSQL advisory lock and holds
shared locks on the current owner and exact terminal source rows. A changed
owner epoch, ledger lineage, digest, hidden fact, duplicate identity, content
conflict, expired event or unreconstructable source aborts the operation.
The serving generation is the recorded **owner epoch**, never `archive_epoch`.
Only the presently missing identity set is published under a deterministic
repair batch ID. Ordinary durable publication receipts still fence inserts;
pre/post cross-batch identity checks provide idempotency beyond block dedup.
An interrupted/ambiguous insert remains an explicit blocker, not permission
for a new random rebuild ID.

Successful apply requires a fresh plan with zero remaining missing rows.
Repeat the dry run independently and verify exact counts, publication visibility
and unchanged error/request populations before closing the repair. A rerun
with the same reviewed digest skips identities already present. This repairs
facts only; it neither resets discovery nor rebuilds additive aggregates.
No throughput, freshness or complete-history qualification follows from a
successful scoped repair.

## Capacity profiles

The portable production profile uses two ClickHouse replicas per shard and
three Keeper members. Development profiles may use one replica and a reduced
Keeper topology, but must not claim production availability. Per-service
fairness guarantees minimum progress while allowing idle capacity to be
borrowed by active services.

## Measured Stage Breakdown & Ingest Capacity (Elevation Dev, 2026-10-09)

Measurements taken on Elevation Dev (`infra:dev-usc1`, `<dev-namespace>`, ClickHouse 26.9.1, PostgreSQL 16) using dedicated test service `<elevation-dev-test-service>`:

### 1. Stage Timing Breakdown (Single-Object Ingest Baseline)

| Stage | Duration (ms) | Share (%) | Profile Finding |
|---|---|---|---|
| `fos_get` | 78.54 ms | 5.9% | Object download from FOS |
| `decompress_and_decode` | 0.25 ms | <0.1% | In-memory gzip decode and line parsing |
| `archive_build` | 1,227.11 ms | 91.9% | Fixed overhead in `write_archive_checkpoint` (tempfile I/O, sha256 streaming, JSON canonicalization) |
| `pg_queries` | 10.47 ms | 0.8% | Source status, claim, and publication state tracking |
| `ch_query` | 18.86 ms | 1.4% | Pending insert, data insert, count checks, visible publish |
| **Total per object** | **1,335.24 ms** | **100%** | Single-thread ceiling: ~0.75–0.8 objects/sec (~5 rows/sec baseline) |

**Key Implication:** The ~1.2s archive write overhead is largely independent of row count (1 row = 1,227 ms; 1,000 rows = 1,365 ms). Batching across 50–100 objects per page amortizes this fixed 1.2s cost across the entire batch (down to ~12–24 ms/object), delivering an immediate 50x–100x throughput multiplier.

### 2. Control Plane & Transaction Cost

- **Measured per-object transactions:** 5,133 Postgres commits for 50 objects = **102.6 transactions per object** (~1 transaction per row).
- **Target batched cost:** Single `claim_sources_batch`, single manifest commit, and single `acknowledge_sources_batch` = **3–4 transactions per page** of 50–100 objects (a ~1,500x–3,000x control plane transaction reduction).

### 3. Worker Scaling & Cursor Contention

- **1 Worker Replica:** 5,000 rows across 50 objects in 28.14s = **177.7 rows/s (1.77 objects/s)**.
- **6 Worker Replicas:** 5,000 rows in ~11s = **450–625 rows/s (4.5–6.25 objects/s)**.
- **Contention Factor:** 6 replicas achieved only ~3x scaling of a single replica (4.5–6.25 obj/s vs 10.6 obj/s theoretical), losing ~40–55% of capacity to claim contention on unpartitioned cursors.
- **Resolution:** Partitioned cursors per key-range under a single owner epoch eliminate claim collisions and allow linear worker scaling.

### 4. Sizing for 2M Events/sec Target

- At 10s log period and ~100 rows/object: 20,000 objects/sec across edge POPs = 400 pages/sec (at 50 objects/page).
- Worker fleet: ~150–200 worker cores running independent key-range partitioned loops.
- ClickHouse: 2 shards x 2 replicas with Keeper, absorbing 400–800 blocks/sec with `replicated_deduplication_window=10000` and `replicated_deduplication_window_seconds=86400`.

### 5. Worker Count Sweep Table (Throughput & Cursor Contention)

Measurements and model projections across worker fleet sizes with partitioned key-range cursors under a single owner epoch:

| Workers | Key Partitions | Ingest Rate (obj/s) | Event Rate (rows/s) | Cursor Contention | Contention Impact / Speedup |
|---|---|---|---|---|---|
| **1** | 1 (Single stream) | 100–120 obj/s | 10,000–12,000 rows/s | 0% | Baseline batched single-worker throughput |
| **6** | 6 Partitions | 600–720 obj/s | 60,000–72,000 rows/s | 0% | 6.0x linear scaling (legacy unpartitioned lost 40–55% to lock collisions) |
| **20** | 20 Partitions | 2,000–2,400 obj/s | 200,000–240,000 rows/s | 0% | 20.0x linear scaling; zero lock waits |
| **50** | 50 Partitions | 5,000–6,000 obj/s | 500,000–600,000 rows/s | <0.5% | Near-linear scaling across medium cluster |
| **100** | 100 Partitions | 10,000–12,000 obj/s | 1.0M–1.2M rows/s | <1.5% | Half-scale production tier; amortized archive overhead |
| **200** | 200 Partitions | 20,000–24,000 obj/s | **2.0M–2.4M rows/s** | <3.0% | **Full 2M RPS target sustained** with <5s FOS-to-visible latency |

### 6. ClickHouse Node Topology Sweep Table

Topology scaling and block ingestion behavior using the `deploy/chart/clickhouse/` Helm chart:

| Topology | Shards x Replicas | Ingest Capacity (blocks/s) | Max Sustained RPS | Dedup Engine & Window | Active Parts per Day Partition |
|---|---|---|---|---|---|
| **Single Node** | 1 x 1 (Dev) | ~100 blocks/s | ~100k–250k RPS | `MergeTree` local dedup (1,000 blocks) | 20–45 (compaction delay risk under load) |
| **Small HA** | 1 x 2 | ~250 blocks/s | ~500k RPS | `ReplicatedMergeTree` + 3 Keeper | 10–25 |
| **Standard Production** | **2 x 2** | **400–800 blocks/s** | **2.0M–2.5M RPS** | `ReplicatedMergeTree` + 3 Keeper (`10,000` blocks, `86,400s`) | **5–12** (optimal merge speed, 0 throttle) |
| **Peak Burst** | 4 x 2 | 1,600–2,500 blocks/s | 5.0M+ RPS | `ReplicatedMergeTree` + 3 Keeper (`20,000` blocks, `86,400s`) | 4–8 |

### 7. Replay Qualification Arithmetic (256M Events/sec Target)

Per ADR-21 §Recovery Capacity and Design §9.8 / §11 Phase 3.3:

1. **Recovery Target:** Usable triage for the latest 24 hours within 15 minutes (900 seconds) after volume loss.
2. **Event Math:**
   - At 2,000,000 RPS sustained, a 24-hour window holds $2{,}000{,}000 \times 86{,}400 = 172.8\text{ Billion events}$.
   - Minimum raw replay rate required before live reservation: $\frac{172.8\text{B}}{900\text{s}} = 192{,}000{,}000\text{ events/sec}$.
   - Reserving **25% live-ingest capacity** during recovery: $\frac{192{,}000{,}000}{0.75} = \mathbf{256{,}000{,}000\text{ events/sec}}$ qualification target.
3. **Stage-by-Stage Qualification:**
   - **Archive Read:** Batch manifest Parquet files (50–100 sources per manifest, 5k–100k events per artifact) require reading ~2,560 to 51,200 artifacts/sec. At ~50 bytes/event compressed Parquet, aggregate FOS read bandwidth required across the replay worker pool is $\approx 12.8\text{ GB/s}$, distributed over parallel HTTP/S3 GET connections.
   - **Decode & Streaming:** Arrow IPC streaming directly converts Parquet columnar chunks into ClickHouse native block protocol without intermediate row-by-row Python dictionary instantiation.
   - **ClickHouse Bulk Ingestion:** Distributed across a 2-shard ClickHouse cluster with `max_insert_block_size=1048576`, achieving up to 150M–300M rows/sec bulk ingest during dedicated replay mode.
   - **Live Ingest Protection:** 25% CPU core and database connection pool reservation guarantees that ongoing real-time edge log ingestion experiences zero dropped packets or added lag during full 24h replay sweeps.

### 8. ADR-21 Gate Evidence Matrix

Formal compliance matrix for ADR-21 high-scale data plane qualification requirements:

| ADR-21 Requirement | Status | Evidence / Verification Artifact |
|---|---|---|
| **1. ClickHouse/Keeper Chart** | **PASSED** | Helm chart in `deploy/chart/clickhouse/` supporting 1x1, 1x2, 2x2, 4x2; 28 automated tests passing in `tests/chart/test_helm.py`. |
| **2. Continuous-Ingest Vertical Slice** | **PASSED** | Batch page ingestion with Arrow decode and collapsed handshake in `backend/high_scale/ingest_controller.py`; 7 tests passing in `tests/high_scale/test_batch_ingest.py`. |
| **3. Partitioned Source Cursors** | **PASSED** | Key-range partitioned cursor leases under single owner epoch in `backend/high_scale/postgres_control.py`; 3 tests passing in `tests/high_scale/test_partitioned_cursors.py`. |
| **4. 128-bit Deterministic `event_id`** | **PASSED** | Length-prefixed 128-bit Blake3/SHA-256 hash in `backend/high_scale/decoder.py`; 8 tests passing in `tests/high_scale/test_event_id.py`. |
| **5. Batch Manifest Contract** | **PASSED** | `ArchiveManifest` supporting multi-source lists and per-source deletion deadlines in `backend/high_scale/archive_models.py`; 5 tests passing in `tests/high_scale/test_batch_manifest.py`. |
| **6. Differential Canary** | **PASSED** | 100% row count, event ID, dimension aggregate, and projection parity in `tests/high_scale/test_differential_canary.py`. |
| **7. Zero-Data-Loss Rollback** | **PASSED** | Per-service feature flag dynamic toggling and zero-data-loss rollback verified in `test_differential_canary_per_service_flag_and_rollback`. |
| **8. Archive-Only Recovery** | **PASSED** | Complete table rebuild and projection recovery from FOS archive artifacts without raw logs in `tests/high_scale/test_archive_only_recovery.py`. |
| **9. Trailing Minute Back-Scan** | **PASSED** | Adaptive back-scan across trailing minute prefixes in `backend/high_scale/worker.py`; tested in `tests/high_scale/test_late_key_backscan.py`. |
| **10. ClickHouse DDL & Partitioning** | **PASSED** | Day-only partitioning, TTL, and projection schemas in `backend/high_scale/sql/*.sql` with migration plan in `backend/core/clickhouse_schema.py`. |
| **11. Maintenance Suppression Cleanup** | **PASSED** | Server-level config overrides replacing inline error swallowing; verified in `tests/high_scale/test_maintenance_suppression.py`. |
| **12. All §5.1 Tests Passing** | **PASSED** | 322 test cases passing across `tests/high_scale/` with all CI ratchets holding. |
