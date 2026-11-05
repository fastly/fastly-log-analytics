# Load Tracing and Capacity Measurement

## Goal

Measure where ingest and dashboard latency comes from, then use the measurements
to optimize the pipeline without weakening exactness or Fastly resource bounds.
The same trace model will support the live load harness and the authenticated
browser debug panel.

## Decisions

- Use W3C `traceparent` context propagation.
- Create one root trace per benchmark run and one stable `batch_id` per
  10-second input batch.
- Instrument each batch through:
  `generate/upload -> raw FOS visible -> settle/admitted -> ingest executions ->
  worker conversion -> Parquet published -> manifest visible -> browser download
  -> worker aggregation -> dashboard ready`.
- Keep detailed Compute spans debug-only. Do not write durable per-query trace
  rows by default.
- Show traces in the existing `DebugPanel`, which is the footer-adjacent
  telemetry surface. `BuildFooter` remains build and service identity only.
- Record full query and filter values only in authenticated debug sessions and
  local load-test artifacts. Durable operational telemetry remains bounded and
  redacted.
- Preserve exact row tags and manifests for completeness, duplicate, status, and
  schema verification.

## Trace model

Each span has a trace ID, optional parent span ID, batch ID when applicable,
phase name, start/end timestamps, status, error, and bounded attributes. Ingest
attributes include rows, bytes, files, vCPU milliseconds, backend requests,
retries, stop reason, and backlog depth. Browser attributes include range,
filter fields, cache state, files, bytes, rows, and worker timing.

The browser records request start-to-headers (TTFB), headers-to-body completion,
and start-to-body completion (TTLB). It also records manifest discovery, file
downloads, decode, worker aggregation, and data-ready/render milestones.

Compute exposes debug spans through `Server-Timing` and the existing bounded
debug-call response mechanism. Normal requests do not receive detailed query
values or unbounded trace payloads. The harness stores the complete local trace
and report.

## Ingest attribution

The replay harness maintains a manifest mapping each 10-second batch to its
raw object keys and tagged synthetic rows. Raw visibility, ingest-plan
admission, worker shards, Parquet publication, and dashboard observation are
correlated through batch identity, object/window identity, and row tags; event
timestamps alone are not used as identity.

Where runtime execution metadata does not currently carry batch identity, the
debug-only metadata will be extended. Customer data semantics and normal
request logging remain unchanged.

## Capacity qualification

Run stepped 10-second batches at 100, 500, 1k, 5k, 10k, 25k, and later higher
rates. A step passes only when:

1. every expected row is present exactly once;
2. no status, schema, quarantine, or platform errors occur;
3. raw backlog returns to zero;
4. resource usage stays within the applicable execution budgets; and
5. phase p50, p95, and maximum values are recorded.

The catalog source-index helper may decline a request when its observed
watermark differs from the coordinator's supplied base. This check precedes
plan lookup, credentials, and all writes. An answered `catalog` reply with
`status: "stale"` and `noop_reason: "source_index_base_mismatch"` is an explicit
read-only no-op, not an execution failure. The coordinator retries through its
existing pending path; plan fencing and watermark CAS still guard publication.
The resource gate measures these executions against the same CPU, call, and
memory limits. Unclassified stale replies, stale plans, missing measurements,
and unanswered workers still fail. This exception does not waive row accuracy,
backlog drainage, or freshness measurements, and does not qualify older
unclassified evidence.

The report classifies dominant delay as delivery, settle delay, queueing,
conversion, publication, discovery, download, or browser aggregation. A single
burst is not considered sustained capacity until its backlog has drained.

## Dashboard qualification

The browser harness will measure cold, warm, and cached dashboard loads across
absolute ranges such as 10 minutes, 1 hour, 6 hours, and 24 hours. It will
exercise representative include/exclude filters and combinations, distinguishing
uncached work from interactions over cached data. Each case records TTLB,
manifest/file counts and bytes, CDN/browser cache state, Compute vCPU and
backend calls where available, worker scan/aggregation time, and data-ready
time.

## Optimization workflow

Instrumentation is implemented and validated before optimization. The first
approved live baseline is 100 RPS. Subsequent optimizations are selected only
from the dominant measured spans and are compared against the same workload
with exactness gates unchanged.

Known candidates to measure include the minute-settle horizon, redundant FOS
LIST/metadata work, per-file dashboard downloads, and ingest/summary executions
that exceed the vCPU budget. No candidate is changed solely from a model or
native benchmark.

## Validation

- Unit tests cover trace propagation, span aggregation, redaction, bounds, and
  missing/failed phases.
- Browser tests cover request timing, worker timing, filter/range cases, and
  DebugPanel rendering.
- Harness tests cover per-batch correlation, FOS replay manifests, backlog
  drain, and exact row verification.
- Live runs require explicit approval and use the smallest next workload.
