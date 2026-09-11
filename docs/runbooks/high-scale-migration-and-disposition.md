# High-Scale Migration and Disposition Gate

This runbook is the Phase 0 boundary for introducing the future `high_scale`
mode. It is a planning and review contract; it does not enable a deployment
mode or mutate an existing service.

## Ownership handoff

For each service, record an incremented owner epoch, the current source cursor,
the old owner, and the replacement owner in durable control state. The old
owner must stop claiming new source objects, drain all claimed work, and
publish its terminal cursor before the replacement can claim work. The
cutover record must be committed atomically with the new owner epoch.

Shadow readers may compare results but may not claim, finalize, or delete
source objects. A failed cutover leaves the old owner authoritative. Rollback
requires the old cursor and a replayable normalized archive for every object
claimed by the replacement.

## Component disposition

| Component | High-scale disposition | Boundary |
|---|---|---|
| FOS credentials/listing | reuse/adapt | preserve credential and pagination safety; add leases and source versions |
| `backend/core/ingest.py` commit path | replace | reuse only pure normalization after identity review |
| DuckLake admission and writers | isolate | remain authoritative for standard/high-throughput only |
| `backend/cron/jobs/rum_ledger.py` | isolate | never runs for a high-scale service |
| RUM/CMCD temporary-table repositories | replace | ClickHouse facts and projections |
| DuckDB files/local buffers | preserve for current modes | not a high-scale serving dependency |
| RBAC, masking, audit, telemetry | reuse/extend | service tenancy remains mandatory |
| ADR-20 ClickHouse evaluator | retain as diagnostic evidence | not a serving dispatcher |
| application Helm chart | preserve current chart | ClickHouse/Keeper ownership must be one selected external operator/chart |

## Gate evidence

Before runtime enablement, attach evidence for:

1. continuous request, RUM, and CMCD ingest with deterministic identities;
2. archive manifest verification and source deletion fencing;
3. duplicate retry, lost acknowledgement, partial insert, and worker crash;
4. empty-target rebuild and replica replacement from FOS;
5. measured replay throughput with live-ingest reservation;
6. differential correctness and convergence watermarks against the current path;
7. a canary cutover, abort, and rollback with no dual source owner.

No source deletion, production deployment, Elevation mutation, or current-mode
retirement is authorized by this document.
