# High-Scale Throughput and Qualification Design

## Goal

Reduce source-to-ClickHouse publication lag under a sustained 50,000 events/sec
input to at most 10 seconds, while keeping page loads at or below 5 seconds
and preserving independent serving and ingestion capacity.

## Scope

This phase covers:

- high-scale worker throughput and Kubernetes resource tuning on Elevation
- a load-test mode that releases prebuilt FOS objects at a controlled rate
- operational metrics for source backlog, publication lag, throughput, and errors
- Grafana panels and qualification evidence for the Elevation environment

GCE, `elevation-data`, and `stg-usc1` remain out of scope until Elevation passes
the qualification gates.

## Architecture

The high-scale worker remains stateless and horizontally scalable. Each replica
keeps a unique worker identity, uses Postgres only for leases and publication
manifests, reads source objects from FOS, and writes facts to ClickHouse.
Worker replicas use pod-local data and the existing Secret-backed service
configuration; no shared RWO PVC is introduced.

Scaling proceeds in controlled steps:

1. Establish a baseline with the current three replicas.
2. Increase to six replicas with one CPU requested and four CPUs/4 GiB
   limited per worker.
3. Tune page size and polling interval only after measuring Postgres and
   ClickHouse saturation.
4. Increase replicas again only if the dependencies remain below saturation.

The backend serving deployment is not scaled as part of this phase. Its page
latency is measured independently while ingestion runs.

## Load-test flow

The load harness separates generation from release. It first creates complete
gzip objects locally, then releases object uploads at fixed period boundaries.
The report distinguishes:

- configured event rate
- actual upload/release rate
- FOS upload wall time
- source discovery time
- source-to-publication lag

This prevents a slow uploader from being mislabeled as a steady 50,000 events/sec
test. Request and RUM streams are released independently but share the same
period schedule.

## Observability

High-scale telemetry will expose, per service and domain:

- source objects by status
- oldest discovered and claimed object age
- accepted and published rows
- publication throughput
- newest source event and newest visible ClickHouse event
- source-to-publication lag
- pending publication manifests
- source-missing, dead-letter, claim-conflict, and publication-error counts
- worker count, CPU, memory, and restarts

Health must report high-scale lag separately from the existing standard-ingest
freshness status. A healthy HTTP endpoint must not imply that high-scale data is
within its freshness objective.

Grafana will chart backlog and lag alongside worker and dependency saturation,
with missing telemetry rendered as unavailable rather than zero.

## Validation

The qualification run must demonstrate all of the following over a sustained
window:

- request and RUM publication lag p95 at or below 10 seconds
- page-load p95 at or below 5 seconds, with a 2–3 second target
- no worker OOMs or restarts
- no stale-claim or duplicate-worker-identity errors
- no unbounded discovered/claimed backlog
- visible publication manifests for current test objects
- dependency saturation identified before adding further workers

The test records raw measurements and makes clear whether a result is steady
state, burst backlog, or recovery.
