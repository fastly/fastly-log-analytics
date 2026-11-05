# Adaptive browser analytics design

**Date:** 2026-11-03
**Status:** Approved for implementation and deployment on 2026-11-03; implementation in progress.
**Scope:** Request analytics in `app/`: ingestion publication, historical Parquet,
metadata discovery, browser caching, traffic-over-time, and top-10 charts.

This approved design supersedes earlier restrictions against server-side
compaction for this migration.

## Goal and agreed decisions

Show newly ingested request logs in dashboard traffic and top-10 charts as soon
as practical relative to the configured logging period. Compaction must never
be a prerequisite for showing a committed batch.

Freshness means time from a request reaching the logged VCL service until its
traffic and top-10 contributions appear on the dashboard. Verify that path
with a 10-second logging period, reporting upstream log delivery, ingestion,
notification, and chart application separately. Do not retain an artificial
two-minute ingestion wait for already available immutable log objects.
Preserve complete, exactly-once handling of late arrivals and safe historical
cache boundaries. A backend period-close-to-cleanup simulation is not an
end-to-end freshness measurement.

Keep Fastly Compute, Fastly Object Storage, the front VCL service, and the
Next.js static export. Keep the existing 10,000-RPS-per-service design target;
that is a target, not a claim that this migration has demonstrated it.

For this installation, the user confirmed a 1,000 ms per-execution CPU allowance.
Use that allowance with execution headroom, not the public 50 ms default.
Heap, backend-call, lookup, and wall-time bounds remain unchanged.

Use custom metadata manifests rather than introduce Iceberg or Delta Lake.
Preserve authorization boundaries. On 2026-11-03 the user approved starting
request analytics from scratch to avoid migrating historical data. Historical
top-10 approximations are acceptable when clearly identified and comparable in
accuracy to fastly-log-analytics. Traffic counts and time buckets remain exact.

Compaction is adaptive, not fixed at five minutes. It responds to bytes, file
count, and age, with the logging period determining when new inputs arrive.

## Review findings

The current request compactor is not the implementation of this design:

- It has a one-hour minimum source age and processes closed hours.
- It considers only the first 1,000 keys of an hour and does not revisit hours
  for later arrivals after advancing its cursor.
- Its stored upload identifier supports aborting interrupted work; it does not
  checkpoint a Parquet writer and resume that writer in another execution.
- Its 700-ms CPU stop threshold is not the documented 50-ms execution budget
  required by the repository instructions.

These are migration requirements, not reasons to abandon Compute. Implement
the new compaction path with explicit source coverage and bounded, durable
progress rather than just changing its schedule or target size.

The existing production file route already forwards Range requests and streams
the upstream body. Reuse that path. The current browser's complete-file
downloads are the part to replace.

Manifest statistics reduce discovery and prune candidate files. They do not
automatically replace the Parquet footer that DuckDB's reader expects.
Selected footer reads remain part of the read budget.

Evidence:

- `app/src/ingest/compaction.rs:1-14,38-51,108-145,284-337,436-573`
- `app/src/ingest/api.rs:1346-1387`
- `app/src/sdk.rs:1490-1515`
- `app/src/main.rs:91-127`
- `app/web/lib/dashboard/sources.ts:476-553`

## 1. Publication and catalog

Every ingested batch has an immutable identity and immutable file keys.
Files are uploaded and verified before a committed catalog generation exposes
them. Publishing an ingestion commit also records its byte count, file count,
source coverage, and chart-summary references.

Keep a small versioned catalog root pointing to immutable partition manifests
and recent commit deltas. Do not rewrite or download the entire retained
history on each ingestion period. Load the root first, then only manifests
for the requested time range and deltas after the browser's last generation.

Each file entry records its immutable identity, schema version, byte size, row
count, time bounds, available column statistics, and row-group metadata.
Missing or untrustworthy statistics mean "cannot prune", never "no matches".
Row-group offsets describe the exact published bytes, not a previous file
with the same logical contents.

Retain the existing `year=/month=/day=/hour=/` paths. Time-path pruning must
include overlapping windows and correctly published late arrivals.

A compaction commit replaces an explicit set of source file identities with
its replacement file identities. A query pins one committed generation and
selects exactly one representation of each covered source. Never combine a
replacement with any of the source rows it covers.

Use compare-and-swap on the authoritative generation to resolve concurrent
ingestion, compaction, and retention. Files and manifest shards remain
immutable; a conflicting publisher reloads the committed head and reconciles
its source set before publishing.

Deletion is separate from publication. Retired objects remain readable for a
bounded reader grace period. A reader whose snapshot is too old restarts from
the current catalog rather than silently returning partial results.

For this installation, initialize a fresh empty catalog epoch and index newly
committed request ingestion from that point onward. Historical request files
may remain physically present but are not part of the new epoch. Legacy
metadata backfill is not a release prerequisite. Do not reset users,
authorization, credentials, service settings, RUM, or operational logs.
An incomplete catalog in the active epoch must never masquerade as an empty
complete range.

## 2. Adaptive compaction

Evaluate eligibility from committed metadata, not repeated bucket-wide LISTs.
Evaluate after ingestion publication and on a bounded scheduled age check,
so quiet sites do not need another upload to release old work.

Initial policy:

- Target 128 MiB of compressed output per file, within the existing time and
  schema partition boundaries. This is a target, not a minimum or a heap size.
- Queue work when uncompacted source bytes approach the target, when a
  partition accumulates 32 small files, or when its oldest mergeable inputs
  have waited 15 minutes.
- Merge at least two files. Do not rewrite a lone small file just because it
  aged, repeatedly merge an unchanged source set, or churn already compacted
  files merely to meet an age trigger.
- Let high-volume partitions become eligible each logging period. A busy
  service with 10-second logs can compact every minute or sooner; there is no
  five-minute scheduling floor.
- Use committed source identities, not an hour-long age gate, for correctness.
  Later files join a subsequent job without invalidating earlier coverage.

The byte trigger uses observed input/output sizes and must not imply that
compressed source bytes predict the final output size exactly.

Ingestion and compaction have separate bounded executions. Ingestion takes
priority. Pending maintenance must still make progress without starting
unbounded work or starving behind a continuous ingestion backlog.

Bound every execution by backend calls, source bytes, rows or row-group work,
wall time, CPU, and peak heap. Stay below the documented 128-MB heap, 60-second
runtime, 32-backend-request, and 50-ms CPU limits; reserve resources for
checkpointing, completion, and error reporting.

Stream output through bounded multipart buffers. A 128-MiB output must not
be assembled in a Compute heap. Persist a versioned work checkpoint containing
source coverage, progress, upload state, and the metadata needed to finish a
valid Parquet object. Do not assume a process, writer, or heap survives a
request. Interrupted work must resume without re-reading all completed inputs;
unrecoverable or incompatible upload state is explicitly aborted and reported.

Publication happens only after the output footer, multipart completion, and
coverage checks succeed. A partly uploaded object never becomes queryable.

## 3. Historical browser reads and persistent caching

Use the authenticated same-origin file route for remote Parquet. DuckDB reads
selected files through HTTP Range requests instead of first downloading every
file into an ArrayBuffer and inserting all history into a resident table.
Projection and filters must reach the Parquet reader.

Preserve `206`, `Content-Range`, object length, immutable object identity,
and unsatisfiable-range behavior end to end. Authentication failures, expired
sessions, missing objects, and unsupported range behavior must be visible.
Do not silently replace selective reads of large files with full downloads.

OPFS is the persistent file store. Use the installed DuckDB filesystem APIs
for complete cached files and HTTP registration for remote files. Stream
downloads to OPFS; do not materialize the complete download in browser memory.
An interrupted download stays temporary and is not registered as a complete
Parquet file.

Do not assume DuckDB's HTTP cache automatically persists to OPFS. Remote
selective reads and completed OPFS files are distinct paths. Persist compact
chart aggregates and explicitly selected files within the storage budget;
do not background-download all historical files to make a chart usable.

Use bounded Arrow and DuckDB memory for active data, cached results, and
recent drill-downs. Do not materialize the entire retained history, or every
raw request in a 15-minute tail at 10,000 RPS, into browser memory.

All cache identities include the install/service, authorized user or data
variant, schema, and immutable object identity. Clear persistent and in-memory
data on sign-out or access-variant changes. Multiple tabs coordinate writes;
eviction must not delete files a query is actively reading.

Detect OPFS availability and quota failures. An explicit nonpersistent mode
may keep queries usable without OPFS; report that persistence is unavailable.
Never claim a download was saved when its write failed.

## 4. Fresh traffic and chart updates

Publish a durable ordered commit cursor with each ingestion commit. SSE
delivers notifications containing the committed generation, batch identities,
and chart-summary references. It does not carry every raw request.

The browser fetches only unseen chart batches, merges their aggregates into
its active state, and updates traffic and top-10 panels without waiting for
compaction or a refresh of unchanged historical files. Fetch full recent rows
only when an interaction needs them and the query fits the resource budget.

SSE is a delivery mechanism, not shared Compute process state. Use bounded
replay responses with a durable cursor and reconnection. A finite response can
return available commits and close; do not busy-wait on storage or leave a
Compute execution running indefinitely.

For the first implementation, reconnect at up to half the logging period,
capped at five seconds and floored at one second, while the dashboard is
visible. Back off on errors and suspend inactive-dashboard subscriptions.
This intentionally permits a short notification delay after ingestion; it is
not a claim of instantaneous server push.

Reconnects replay from the last successfully applied cursor, not merely the
last received event. Duplicate events are harmless. A cursor outside retained
replay history causes an explicit snapshot resynchronization.

Apply generation changes atomically. Historical aggregates and tail aggregates
carry source coverage so their overlap is excluded even when compaction or
summary bundling replaces files during a live session.

Traffic buckets use event time and the existing interval semantics. A late
batch updates the affected older buckets and rankings; it is not assigned
to the moment its notification arrived. Missing ingestion coverage is not
displayed as confirmed zero traffic.

Record ingestion completion, notification receipt, and chart application
timestamps. Report delivery and application latency separately from the
upstream logging period.

## 5. Accuracy and filter semantics

Traffic totals and time buckets sum exact committed counts for the selected
range. Unaligned boundary buckets and filters unsupported by a summary use
appropriate finer-grained data; do not prorate a coarse count or reuse an
unfiltered total as a filtered answer.

Use mergeable heavy-hitter summaries for approximate rankings. Retain more
than ten candidates and carry the algorithm's error information through
merges. Summing independently truncated top-10 lists is not sufficient.

Label estimated rankings and counts in the chart result. Any displayed
error bound must be supported by the selected algorithm and the actual
query path. Distinct estimates remain distinct from request-count estimates.

A summary can answer a filtered query only if it preserves that query's
required joint dimensions and semantics. Otherwise query raw Parquet through
the bounded range-read path. Never make an arbitrary filter "fast" by
silently ignoring it.

Compare ranking recall and count error against exact full-data queries and
the v3 reference on identical representative and adversarial datasets.
Accept the replacement only when it matches or improves the reference's
accuracy on the agreed benchmark corpus. Include candidates that become
globally frequent without appearing in individual batches' top ten, ties,
skew, changing traffic distributions, and late arrivals.

## 6. Implementation and acceptance order

Use three independently reviewable implementation plans:

1. **Publication, catalog, and adaptive compaction.** Prove complete discovery,
   atomic replacement, replayable commit metadata, crash recovery, late-arrival
   coverage, retention behavior, and resource admission before changing the
   default browser read path.
2. **Range reads and browser storage.** Prove actual browser `206` reads,
   projection, OPFS reload reuse, cancellation, quota handling, session
   isolation, and bounded heap. A complete-file fetch test is not a substitute
   for observing selective network reads.
3. **Live chart updates and approximate rankings.** Prove ingestion-to-chart
   freshness, replay and resynchronization, exact traffic totals, no overlap
   double-counting, ranking accuracy, and no refresh of unchanged history.

Each plan uses failing behavior tests before implementation, the existing
Rust/Vitest/Playwright tooling, and scoped lint/format checks. Preserve the
existing public dashboard behavior and query semantics unless this document
explicitly changes them.

Record cold-load requests and bytes, warm interaction latency, commit-to-chart
latency, storage operations, peak memory, and ranking accuracy against the
current implementation. Count footer reads in the measurements. Measure
Fastly work and browser work separately.

Native tests and fixtures verify behavior; they do not prove live accuracy,
resource-limit compliance, or 10,000-RPS capacity. Deployment, activation,
and billed live verification require separate explicit approval. Do not
retry the known Viceroy cache-import failure without a runtime update.

## Out of scope

No separate pre-ingestion request feed, non-Fastly infrastructure, new logging
transport, logging-period change, destructive reset of existing logs, or
unapproved deployment/load test. RUM and operational-log migrations are
separate follow-ons; preserve their existing behavior while shared interfaces
change.
