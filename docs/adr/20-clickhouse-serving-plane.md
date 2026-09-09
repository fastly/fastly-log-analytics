# ADR-20 - Reject This ClickHouse Dashboard Serving Prototype

**Status:** Decided — this prototype rejected for dashboard serving; bounded
diagnostic index/replay tooling retained. Task 11 not implemented: Docker
prerequisite failed, as required.
**Date:** 2026-09-08 UTC

## Measured decision

Keep production dashboard routing on the existing DuckLake repository. Remove
the runtime hybrid dispatcher from both dashboard endpoints; do not introduce
another serving flag. `CLICKHOUSE_ENABLED` still enables the bounded client,
schema, observability, admin status/replay APIs and explicit export/replay tools.
It cannot select the dashboard engine.

This rejects **this implementation and measured workload**, not ClickHouse as
a platform. It is not a coexisting serving accelerator: the measured endpoint
was slower. `backend/repositories/clickhouse_dashboard.py` remains an archived
experimental evaluator for direct library comparisons, with its integration
tests and correctness checks intact. No live endpoint imports it.

Task 10 used a fixed 190,334-row source, 184,374 eligible rows, the same backend
image, warm storage/OS caches and uncached responses/readiness:

| Evidence | DuckLake | Experimental hybrid |
|---|---:|---:|
| Serial p50 / p95 / p99 | 0.793 / 1.055 / 1.314 s | 5.681 / 6.629 / 6.961 s |
| Serial successful throughput | 1.2071 requests/s | 0.1724 requests/s |
| Concurrency 8 errors / admitted attempts | 0 / 64 | 20 / 32; memory abort, 32 not admitted |
| Concurrency 32 errors / admitted attempts | 0 / 64 | 58 / 64; memory abort |
| Concurrency 64 errors / admitted attempts | 0 / 64 | 64 / 64; memory abort |

All measured hybrid errors were 45-second request deadlines. Memory aborts
make these incomplete qualification attempts, not completed passing load
stages. The backend had no cgroup limit; the driver stopped admission at
1,536 MiB without raising resource limits. No OOM kill was needed.

Selected-corpus correctness and bounded recovery passed. A new generation
rebuilt 184,374 rows from 19 retained Fastly Object Storage artifacts in
20.202443 seconds with the DuckLake catalog configuration absent, matching
the complete canonical digest. Retained artifacts plus the Postgres manifest
were sufficient for that rebuild; creating the export still requires DuckLake,
and this is not proof that arbitrary FOS Parquet or future ingestion can be
replayed independently. Raw deletion remains DuckLake-authorized.

The prototype adds a ClickHouse service, immutable artifact retention,
Postgres publication state and costly full-generation readiness on every
evaluation (4.037 s median at concurrency 1). The artifacts totaled 1,599,557
bytes; no representative total storage-cost comparison was measured. Source
ledger discovery-to-commit lag was 128 s; the 6,660 s retained-dataset-to-replay
publication age includes waiting and is not a streaming SLA. There is no
continuous ClickHouse ingest hook. Only country top-N and request buckets
were hybrid-served; the other tested request shapes stayed on DuckLake.

Latency/error/saturation gates failed. Task 11 Kubernetes packaging was
therefore **not executed**, and ADR-18's serving constraints remain unchanged.
Tasks 1–10 supply experiment evidence; Task 12 closes the experiment by taking
the rejection branch. This is not an all-acceptance-gates-passed claim.
Full measurements and limitations are in the
[runbook](../runbooks/clickhouse-prototype.md#task-10-measured-docker-gate--failed).

## Original context and retained publication design

The candidate-serving discussion below records the experimental design, not
current dashboard routing. Its publication/replay invariants remain applicable
to the retained diagnostic tooling.

ClickHouse is a candidate rebuildable serving index, not a replacement for
Fastly Object Storage or the Postgres control plane. DuckLake remains the
operational comparison baseline. The first target is one Docker host with
persistent ClickHouse storage; Kubernetes packaging follows a successful
Docker vertical slice.

Review before Task 1 found two conflicting requirements: preserve the current
DuckLake raw-deletion contract, but also prevent raw deletion whenever
ClickHouse is unavailable. The implementation only grants deletion after
DuckLake publication, a grace period, and a fenced deletion claim
(`backend/core/ingest.py`, `merge_lake_files` and `finalize_committed_raw`).
ClickHouse is not part of that authority.

The existing ledger publication timestamp is not a replay-batch manifest.
It does not identify immutable Parquet artifacts, their content hashes, or
source membership. DuckLake flush, compaction, and snapshot cleanup manage
physical files independently of source-object identities. Replaying a listing
of those files can include replaced rows or miss logical deletion semantics.

## Bounded publication boundary

### Execution decision after prerequisite review

The prototype uses bounded snapshot datasets, not a continuous ingest mirror.
An explicit export reads a pinned logical DuckLake snapshot into immutable
Parquet artifacts in Fastly Object Storage. Postgres records the dataset,
artifact hashes and ordinal intervals, target generations, fenced per-batch
publications, and active service selection. The existing ingest and deletion
paths remain unchanged. Automatic export scheduling is outside this vertical
slice; it requires a later durable outbox/reconciler.

Each artifact persists stable row ordinals, including separate ordinals for
identical log rows. Replays preserve those ordinals. The ClickHouse fact table
uses `ReplacingMergeTree` and every reader uses `FINAL`, deduplicating on
service, target generation, batch, and ordinal before aggregation. No
insert-triggered materialized aggregate is used in this prototype.

A building generation is never eligible for serving. Verify each batch's
ordered payload against its immutable artifact, then fence the publication
update by lease generation. Activate only a fully verified generation through
a real Postgres transaction. Rebuilds always create a new generation and load
all artifacts, regardless of publication markers on the old target.

Datasets declare inclusive UTC coverage bounds matching the existing API,
a pinned source snapshot, and a finite retention deadline. Serving requires
covered effective request bounds and matching DuckLake snapshot for the hybrid
response; otherwise the selected slice remains on DuckLake with an observable
eligibility reason. An expired dataset cannot serve or replay. Retained
artifacts cover only the exported dataset, not subsequent ingestion.

### Separate durability from serving publication

DuckLake publication continues to authorize raw deletion. A successful
ClickHouse insert must never advance that gate. A ClickHouse outage must leave
ClickHouse publication pending or failed; it does not invalidate an already
durable DuckLake commit.

Recovery evidence must distinguish these cases:

1. DuckLake unpublished, ClickHouse insert successful: raw deletion remains
   forbidden.
2. DuckLake published, ClickHouse unavailable: existing authorized raw deletion
   may continue, and replay must remain possible from retained artifacts.

No deletion or retention implementation changes with this ADR.

### Define replay artifacts before the loader

The proposed loader input is immutable Parquet exported from a
catalog-consistent DuckLake view into Fastly Object Storage, with a Postgres
manifest. It is not a serving-pod cache file or a listing of DuckLake's data
directory.

Before schema or loader implementation, specify artifact URI, content hash,
source membership, schema/transform version, stable row ordinal, logical
watermark, and retention ownership. Preserve legitimate identical log rows:
hashing row values alone is not a unique event identity. Artifact cleanup must
respect replay obligations and customer retention; indefinite retention is
not an acceptable default.

DuckLake may be required to produce this prototype's replay artifacts.
Rebuilding without DuckLake is a separate property to demonstrate from the
artifacts and manifest, not an inference from using object storage.

### Decide visibility before ClickHouse DDL

A Postgres claim does not settle whether ClickHouse committed an insert whose
acknowledgement was lost. Background merges are not an immediate uniqueness
guarantee either.

The implementation gate requires an explicit duplicate and partial-batch
visibility protocol, stable row/chunk identities, claim-generation fencing,
and target-generation-aware publication state. Rebuilding an empty target
must not skip batches marked published to the previous target.

Use real ClickHouse/Postgres failure scenarios to demonstrate counts before
background merges: lost acknowledgement, partial insertion, crash before the
publication update, stale claimant, and empty-target rebuild. The selected complete-generation protocol above must demonstrate these
guarantees against real engines; a batch ID alone is not sufficient.

### Historical evaluator: explicitly hybrid, not a live route

The existing dashboard acquires a DuckDB connection in `build_request_context`
before repository dispatch. Its bundle can also request bots, maps, and
connection-request aggregates. Replacing time-bucket counts and one top-N
dimension does not make that endpoint independent of DuckLake.

The proposed first slice preserves this dependency and labels it hybrid:
ClickHouse owns the selected aggregates; DuckLake owns remaining requested
sections and request-context connection acquisition. Freeze the exact request,
response, filters, null/ordering semantics, and shared publication watermark
in Task 1. Preserve tenancy, analyst time clamps, and masking.

## Baseline and acceptance

Tasks 4–6 verified the selected protocol against real Postgres and ClickHouse
25.8.4.13. The bounded baseline exported 184,374 rows into 19 immutable Fastly
Object Storage artifacts; a new generation rebuilt all rows with the DuckLake
catalog configuration absent and matched the complete ordered canonical
digest. See the [runbook](../runbooks/clickhouse-prototype.md) for interfaces,
bounds, retention ownership, commands and failure-test evidence. No ingest hook
or automatic export job was added. This was Tasks 4–6's
evidence; Tasks 7–10 subsequently added and measured the experimental
dispatcher and admin tooling. Task 12 removed only the dashboard dispatcher.

A baseline must identify its running revision and effective
`INGEST_MODE=celery`, `SERVING_MODE=durable`, Postgres metadata, and Postgres
DuckLake catalog. Healthy HTTP endpoints alone do not establish that topology.
The initial running Docker image inspected on 2026-09-07 did not expose the
`SERVING_MODE` config implementation; it is not the required baseline.

Capture at least 30 requests after warm-up, with dataset size, fixed query
corpus, concurrency, achieved throughput, p50/p95/p99, errors, ingest lag, and
freshness. Compare the same data watermark and report caching behavior.

ADR-18's single-backend deployment constraint remains in force. This proposal
does not establish horizontal serving scalability, ClickHouse replication,
or million-request-per-second capacity. The failed measured latency gate
requires rejection rather than promotion; the terminal decision above records it.
