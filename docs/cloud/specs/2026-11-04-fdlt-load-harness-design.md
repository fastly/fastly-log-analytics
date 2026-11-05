# Load harness on fdlt: design

Status: draft for review, 2026-11-04.

## Why

We want to load-test the live install in production and find the log periods
and request rates it can support while ingest lag and every page stay
reasonable. The old harness could not do this. Its in-repo sender ran from one
machine, and all three T2 attempts failed on sender capacity. `fdlt`
(`~/Projects/fastly-distributed-load-tester`) sends realistic traffic through
relay services on real Fastly POPs, up to about 5,000 RPS. Traffic therefore
reaches the monitored service from POPs around the world, the way customer
traffic does.

The new harness replaces all of the old load-sending, diffing and campaign
code. It uses fdlt for traffic and keeps whatever existing code is still the
best tool for a job, such as passkey sign-in, the Web Vitals observers and the
execution-log follower.

## What a run must answer

For each stage of a stepped ladder (for example 100, 500, 1,000, 2,500 and
5,000 RPS), and for each log period under test:

1. **Completeness.** Every request fdlt delivered to the monitored service
   appears in Parquet once, and only once. This is proved by request id, not
   just by counts.
2. **Ingest behaviour.** Ingest lag and freshness over time, raw backlog and
   whether it drains, and per-execution vCPU, backend calls, KV operations,
   heap and failures for each job kind (tick, ingest, workers, summaries).
3. **File shape.** How many raw objects and Parquet files a stage produces,
   with their sizes and row counts, by log period and rate. This shows what
   to expect when traffic hits POPs all over the world.
4. **Pages.** Every page and dashboard section loads and renders under load
   with no console errors, failed requests, or empty or broken panels. Its
   numbers agree with Parquet. Load timings and Web Vitals (TTFB, LCP, CLS,
   INP, long tasks) are recorded.
5. **Bottlenecks.** A ranked list of fixable hot spots, each with its
   evidence and the area of the code it points at. It is built from the
   DebugPanel telemetry, the browser timings and the ops data.
6. **Monthly cost.** What a month of this pipeline would cost at that log
   period and rate, projected from the stage's measured usage.

## Non-goals

- Fixing the bottlenecks it finds. The report names them, and each fix is a
  separate request.
- Changing fdlt. If the harness needs something from fdlt, such as a missing
  hint, report field or reconcile feature, write it up as a prompt that Drew
  can pass to the fdlt project.
- A CI gate. The harness runs against live infrastructure only on Drew's go.
- Rates above 5,000 RPS. Those come next, from a log seeder like
  fastly-log-analytics has: it writes fake log files straight into the raw
  bucket instead of sending requests. This harness's file-shape data (sizes
  and shapes of log objects from aggregators around the world) is what the
  seeder will be tuned against. See "Next after this".

## Anti-goals (failures even if it technically works)

- Comparing logged rows against `requests_sent` instead of
  `expected_log_lines` (delivered).
- A sampled ledger. Sampling breaks exact id matching, so `--ledger-sample` is
  always 1.0.
- Run ids or request ids silently truncated by the log-format byte budget, so
  that stages merge into one count. FLA hit exactly this.
- Failing a stage because a page was slow. The point is to find where things
  stop being acceptable, so performance is measured and reported, and Drew
  judges it.
- Wasting billed work: downloading the same Parquet object twice, polling
  faster than the log period, doing extra cold loads, or sending more traffic
  than a stage needs.
- Presenting fixture, native or local-browser results as live evidence.

## Constraints

- Stay within the Compute limits in `AGENTS.md`. Measurements are recorded
  against those limits, and the harness itself never adds load to Compute
  apart from what it exists to measure.
- Sending load, deploying, changing service config and activating versions
  each need Drew's explicit go. Before that, one or two single-request smoke
  `curl`s are allowed.
- Never push, open a PR, merge or deploy without approval. Commit by pathspec
  only.
- Never print Fastly tokens or FOS keys. When a tool needs a token on disk,
  write it to a mode-600 temp file and delete the file afterwards. Reading a
  logging endpoint from the API returns FOS keys in cleartext, so filter those
  fields before saving anything.
- Artifacts (ledgers, exports, downloads, reports) go under
  `~/.flp-state/loadharness/<campaign>/`, never in the repo. A 10-minute stage
  at 5,000 RPS writes a ledger of about 1.56 GB.
- Read fdlt's README, `fdlt run --help` and `fdlt reconcile --help` before
  writing code, because the flags change. The design is based on fdlt
  `6473dd4`.

## Pieces

### 1. Prepare the monitored service (this repo)

**Log fields.** Add two custom fields:

- `lt_run_id` from `req.http.X-LT-Run-Id`
- `lt_request_id` from `req.http.X-LT-Request-Id`

Both are VARCHAR and empty for non-synthetic traffic. Custom fields are
per-install config (`log_fields.custom_fields`). They are set through
`/admin` and applied by the reconciler (`app/src/admin/mod.rs:472-530`), so
adding them is an approved config change, not a catalog code change. That is
my call: putting them in the catalog would give every install two
load-test-only columns.

The harness ships a script that applies the two fields through the admin API
and verifies them.

**Field length caps.** A string custom field logs
`substr(expr, 0, byte_limit/6)` characters. With the default
`bytes_estimate`, the cap is 160 bytes, or 26 characters, which is too short
for a 32-character run id. Set `byte_limit` explicitly:

- at least 192 for `lt_run_id` (32 characters)
- at least 252 for `lt_request_id` (42 characters: a 32-character run id, a
  hyphen and up to 9 sequence digits)

The generator also silently clamps each cap to whatever is left of the
16 KiB line budget (`limit.min(budget)`), so the result is verified, not
assumed.

The live format is the positional array form (`wire_format: "array"`,
`generate_compact_log_format`), with custom fields sorted by name. The
read-back check parses that array format.

Everything else the reconcile needs is already logged. The harness SQL uses
these columns under their current names:

| fdlt dimension | Existing column | Source |
| --- | --- | --- |
| timestamp | `ts_ms` (and `timestamp`) | `time.start` |
| status | `status` | `resp.status` |
| method | `method` | `req.method` |
| cache_state | `cache` | `fastly_info.state` |
| pop | `pop` | `server.datacenter` (IATA, which fdlt accepts) |
| response bytes | `resp_bytes` | `resp.bytes_written` |

No existing column is renamed.

**VCL snippets for fdlt.** Add these to the monitored VCL service. Their names
must avoid everything `managed_name` matches (`app/src/admin/reconcile.rs:4-18`):

- the `flp-owner-` prefix
- names starting with "Fastly Log Analytics" or "Fastly Log Analysis"
- the listed exact names

A test must prove the reconciler leaves the fdlt snippets alone. Use an
`fdlt-` prefix.

- `recv`: copy `X-Source-Ip` into `Fastly-Client-IP` and
  `client.geo.ip_override`. No existing snippet in the repo does this. The
  app's "Fastly Log Analysis Reset Client IP" snippet, at priority -100,
  deliberately unsets a `Fastly-Client-IP` sent by the client. The fdlt
  snippet therefore has to run after the reset and before the capture
  snippets, at a priority between -100 and 1. Otherwise the captured `ip` is
  wrong. Before adding it, check the live service for a hand-added snippet
  that already does this. This snippet also strips volatile query
  parameters, such as CMCD, from the cache key.
- `miss` and `pass`: when `req.backend.is_origin`, translate `X-LT-Origin-*`
  into `HM-*`. VCL reads only the first instance of a repeated
  `X-LT-Origin-Header`, so collect them with `std.collect` and add each line
  back as its own `HM-Header`.
- `fetch`: map `X-LT-Cache` to TTLs. `pass` is a 120s TTL followed by
  `return(pass)`; `short` is 60s, `medium` 1h and `long` 1d; `swr` is a 30s TTL
  plus 300s `stale_while_revalidate`.
- `deliver`: return `X-Cache`.

**The snippets apply to every request, with no secret gate** (Drew's
decision). This is acceptable only because the monitored service carries
synthetic traffic only. A stranger sending `X-LT-*` headers can add rows only
to test data, and a forged run id shows up as an unexpected id and fails that
stage loudly. If the service ever takes real traffic, these snippets need a
shared-secret gate first, and that requires a change in fdlt.

Lint every snippet with `falco lint`. Apply them through the
clone → add snippet → validate → activate flow, and only after approval.

**Origin.** The monitored service's origin moves to
`https://http-me.fastly.dev` (Drew's decision), so the origin hints take
effect. The backend follows the repo's backend rules: a domain name, with
`override_host`, SNI and certificate hostname all matching it, and
`prefer_ipv6=false`. Any existing smoke checks that depend on the old echo
origin's `/anything/` paths are updated.

**Verify the destination without load:**

1. Read the active logging endpoint's format back from the Fastly API and
   confirm the `substr` cap on both id fields.
2. Send one smoke `curl` with known ids of 32 and 42 characters.
3. Wait for ingest, then read both full values back from Parquet.

### 2. Ladder orchestrator (new: `app/tools/loadharness/`)

This is a Python tool, run with `uv run`.

- **Stages** are written as `RPS:seconds`, for example
  `100:600,500:600,1000:600,2500:600,5000:600`. Any stage above 1,000 RPS
  needs `--allow-high-rate`. `--dry-run` prints every command and query and
  sends nothing.
- **The fdlt binary** is found from `--fdlt-bin`, then `$FDLT_BIN`, then
  `PATH`, then `~/Projects/fastly-distributed-load-tester/fdlt`.
- **Each stage is one fdlt run**, with run id `<campaign-prefix>-s<N>`. The
  harness refuses any run id longer than the cap read back from the active
  format. Each run passes:
  - `--rps`, `--duration`, and a short `--warmup` and `--ramp`
  - `--export-json` and `--ledger`, with `--ledger-sample 1.0`, on every
    stage. Unlike FLA, which writes the ledger only for stages of 100k
    requests or fewer, there is no size cap. Count checks alone can miss a
    lost row offset by a duplicated one, and the goal is "once and only once"
    at every rate. The cost is disk, about 1.56 GB per 10-minute stage at
    5,000 RPS. The harness checks free space before each stage and refuses to
    start if the ledger would not fit.
  - `--scenarios-file`, `--origin-profile` and `--cache-profile`
- **Numbers come from `run.json`:** `expected_log_lines`, relay errors,
  transport errors, requests cut at drain, `first_sent_at` and
  `last_response_at`, and `phase_bounds`. `requests_sent` is never the
  comparison basis.
- **Stages are strictly sequential:** send, then settle, then reconcile, then
  start the next stage. Running them in sequence also measures whether the
  backlog drains between stages. The ladder stops if a stage fails strict
  reconcile or its backlog does not drain in time. (My call.)

### 3. Monitors during each stage

The harness polls at the log period and never faster. It records:

- ingest freshness and lag (`latest_log_at` against the wall clock);
- backlog. There is no backlog-depth metric. The signal is `more_pending`
  in `cron_runs`, plus the gap between raw objects arriving and their
  `ingest_files` rows;
- per-execution `vcpu_ms`, `backend_requests_used`, `kv_ops`, `outcome` and
  `details_json` from the ops Parquet (`processed/ops/cron_runs`,
  `platform_errors`, `ingest_files`). Heap is only available as
  `details_json.heap_mib`, which is an allocated snapshot, not the peak. The
  report labels it that way;
- the execution-log stream, by reusing `loadtest/execution_logs.py`;
- a file-shape inventory:
  - Raw objects come from `ingest_files` (`raw_key`, `raw_size`, `rows`).
    Raw objects are deleted after conversion, so listing the raw bucket
    shows only what is still waiting.
  - Parquet files come from a LIST of the stage's hour prefixes only. The
    harness fetches only keys it hasn't seen before.

### 4. Settle and reconcile

**Settle.** After `last_response_at`, the harness polls until two things hold.
Freshness has passed `last_response_at` plus a margin, and the row count for
the run id has stayed the same for two log periods in a row.

The wait is bounded at 3 log periods plus 5 minutes, and the same bound
applies to backlog drain. Both bounds can be overridden with flags. If
either times out, the stage fails as incomplete. (My call.)

**Count.** DuckDB runs over the stage's Parquet files, pruned by
`dt`/`hour` from `first_sent_at` to `last_response_at` plus the margin.
Rows are filtered on `lt_run_id = <run id>`. If the monitored service has
shielding, a request can be logged at both the edge and the shield, so the
filter also keeps only edge rows (`edge = 1`); shield rows are counted and
reported separately. (My call.)

**Logged but not delivered.** Some requests reach the service and are logged
even though fdlt doesn't count them as delivered. The client timed out, the
run ended while they were in flight, or the relay errored after the service
had already answered. Fastly logs a client give-up as 499. If the service had
already sent the full response, it logs the normal status, such as 200.

The rule (Drew's decision):

- A logged id is **excused** if the ledger has it with a non-delivered
  outcome (`transport_error`, `cut_at_drain` or `relay_error`), whatever
  status was logged.
- Excused ids are reported per stage as their own count, by ledger outcome
  and logged status. 499s are called out separately, so the rate of client
  give-ups shows at every level.
- These still fail the stage:
  - a logged id that isn't in the ledger at all;
  - a duplicate;
  - a delivered request that's missing.

How the harness does it: DuckDB joins the run's Parquet rows to the ledger
NDJSON by request id, and excused rows are left out of both observed files
before reconcile runs. fdlt then compares delivered against delivered, and
its exit code keeps its meaning. (My call. It needs no fdlt change. A native
reconcile category in fdlt would be a nice extra, and I can write it up as a
request.)

The queries write two files:

- `observed.json` with the `total`, `status`, `method`, `pop`, `cache_state`
  and `minute` dimensions, in the row format `fdlt reconcile --help`
  documents;
- `observed-ids.txt` with every logged request id, duplicates included and no
  `DISTINCT`.

**Reconcile twice per stage:**

- **Strict, tolerance 0:** `total`, `status`, `method` and `pop`, plus exact
  ids (`--observed-ids` against the full ledger). `pop` is strict as my call.
  If the smoke stage shows that the relay-to-destination hop logs a different
  datacenter than fdlt recorded, it moves to advisory.
- **Advisory, about 1%:** `cache_state` through `--cache-map
  "HIT-CLUSTER=HIT,MISS-CLUSTER=MISS,PASS/HITPASS=PASS"`, and minute buckets.
  These can't match exactly: hit-for-pass logs PASS while the client sees
  MISS, and fdlt buckets minutes by send time while logs use the edge start
  time.

Only the dimensions present in an observed file get compared, so strict and
advisory use separate observed files. The exit codes are 0 for a match, 1 for
a mismatch and 2 for bad input. The harness parses the JSON after
`JSON Diff:` and saves it as `stage-N.<strict|advisory>.verdict.json`.

### 5. Browser probe (Node + Playwright)

The probe reuses the passkey sign-in and saved `storageState` from
`app/tools/browser_verify.mjs`. It also reuses that file's injected observers
(`window.__flpWebVitals` for CLS, LCP, paint and long tasks) and its
loading-stage timeline (`window.__flpLoadingStages`). It adds an INP observer
based on event timing. INP needs interactions, so the probe performs scripted
ones: a range change, an include filter and an exclude filter. That also
exercises the filter paths.

The probe runs twice per stage:

- **Mid-sustain (under load):** one cold pass. Each page is loaded once in a
  fresh context on the run-window range.
- **After settle:** warm loads only, across these ranges: the run window,
  1h, 6h and 24h.

This keeps cold loads to one per page per stage, which is the closest the
ladder can get to the `AGENTS.md` "one cold dashboard load" rule while still
measuring every page under load. (My call.) It covers every
route (`/dashboard` and every dashboard section, `/cache`, `/logs`, `/usage`,
`/admin`, `/admin/tick`), plus `/how-it-works` and `/setup` as static checks.

For each page it records:

- TTFB, LCP, CLS, INP and long tasks
- the loading-stage timeline and the time until data is ready
- console errors and failed requests
- panels that are empty or show an error
- the full DebugPanel telemetry: queries, API and FOS calls, spans, and
  `X-Page-Load-ID`

The probe reads that telemetry from the page's telemetry store, not by
scraping the DOM. If the store isn't reachable from Playwright today, add a
debug-session-only hook for it; it must not be exposed in normal sessions.

Insight correctness is checked by comparing the dashboard's totals for a
fixed absolute range against a DuckDB count of the same range in Parquet.

**Insights (Drew's decision: load check plus injected incidents).**

- **Every stage:** all 51 insights load and render without errors, across
  all five groups (origin, edge, network, traffic, security).
- **Each ladder:** add one short incident stage at 500 RPS for 5 minutes,
  run after the ladder's hour-close wait. It uses three fdlt profiles in
  turn:
  - `--origin-profile origin-incident` for 5xx bursts;
  - `--origin-profile degraded` for a slow origin;
  - `--cache-profile cold`.

  Then the harness checks that the matching origin and edge insights fire:
  for example `origin_error_rate`, `error_spikes`, `origin_latency_spike`,
  `tail_latency`, `cache_hit_cliff` and `cache_collapse`. It also checks
  that unrelated insights stay quiet. The builder maps each profile to its
  expected insights from each insight's definition in
  `app/src/logging/catalog.json`.
- **Security insights:** load-checked only. They need attack-like traffic
  that fdlt doesn't generate.
- The incident stages are reconciled just like the ladder stages, and the
  file-shape and cost reports leave them out, because they don't reflect
  steady traffic.

### 6. Bottleneck report

Each stage gets a report, and the ladder gets a summary. Both are written as
Markdown and JSON under the campaign directory. Each contains:

- the strict and advisory verdicts, plus id statistics (rows, unique ids,
  duplicates, blanks) and any mismatch rows;
- ingest curves: lag, backlog, and vCPU and backend calls for each job kind
  against their limits;
- a file-shape table by log period and rate;
- a page table: timings, Web Vitals, errors, and whether the numbers agreed
  with Parquet;
- a **ranked list of bottlenecks**. Each entry has its evidence (page load
  id, query SQL, call path or execution id) and points at the area of the
  code that is likely involved.

The ranking rules are advisory and never fail a stage:

- a query over 1s, or a large share of page time spent in one query
- low browser or CDN cache reuse for Parquet fetches
- Compute vCPU near the 1000ms limit (flag anything over 700ms, and rank by
  share of the limit, since the goal is to keep CPU down), backend calls near
  32, or a heap snapshot near
  128MB (labelled as a snapshot)
- LCP over 2.5s, CLS over 0.1, or INP over 200ms (Google's "good"
  thresholds)
- ingest lag above a few log periods, or a backlog that doesn't drain
- many small Parquet files, which is a sign compaction is falling behind

### 6a. Monthly cost projection

For each stage, the harness projects the monthly bill **a real Fastly
customer would pay to run this project** on a service with that traffic level
and log period (Drew's framing). The test reveals that cost, but the test's
own usage is never counted in it.

The projection has two parts:

- **Pipeline cost per month.** This is what the project costs just by being
  turned on: ingest, conversion, summaries, rollups, storage, retention, and
  the cron and tick work. It's projected from the stage's measured usage.
- **Dashboard viewing, priced per 1,000 page loads.** A real customer also
  opens the pages. The browser probe measures what one cold page load and one
  warm page load cost on each page: Compute requests and vCPU, FOS reads, CDN
  requests and bytes. The report prices those per 1,000 loads at that data
  volume, so any usage level can be added on top without guessing how often
  a customer looks.

Both parts use the install's configured rates, the same ones the Usage
page's estimator uses. The report states which rates were used.

The campaign summary has one **customer cost matrix**. It has a row for each
log period (10s, 1m, 5m) and a column for each rate (100, 500, 1,000, 2,500,
5,000 RPS), and every cell shows:

- the pipeline cost per month, with its line items;
- the dashboard cost per 1,000 cold page loads and per 1,000 warm page loads.

Every cell comes from that combination's own measured stage. No cell is
interpolated from its neighbours.

The pipeline is only what this project deploys (Drew's decision):

- the app and worker Compute services;
- the front CDN service;
- the KV store and the Secret Store;
- Fastly Object Storage (storage and operations).

Out of scope:

- the fdlt relay service;
- the monitored service's own delivery cost.

`estimateMonthly` has no Secret Store line today. The builder checks whether
the app's Secret Store reads are billable at this scale. If they are, they
add a line item. If not, the report records them as measured with a cost of
zero.

**Leave out the harness's own usage.** While a stage runs, the harness uses
the app itself: it downloads Parquet, polls ingest status, walks and samples
raw objects, and runs the browser probe. That usage would inflate the
projection.

- Every harness request carries the header `X-FLP-Harness: <campaign>-s<N>`,
  and the browser probe sends it too. The app's `api_telemetry` and
  `fos_usage` record a `harness` caller for those requests, and this needs
  only a small change in the app.
- The projection subtracts the `harness` caller's requests, vCPU, FOS
  operations and bytes from the app-measured numbers.
- Fastly's stats API can't be split by caller. For its Compute, KV and
  bandwidth figures, the harness subtracts the harness's share as the app
  measured it, and labels those lines "adjusted".
- The test's own cost isn't reported (Drew's decision). It is subtracted and
  nothing more.

Projections reuse the app's own cost model, `estimateMonthly` in
`app/web/lib/usage/estimate.ts`. That is the same function the Usage page's
Cost Estimator runs, so there is one source of truth for pricing. The
harness calls it through a small Node entry point and never reimplements it
in Python.

1. **Measured unit costs.** These come from the stage's own measured usage,
   bounded to the stage window:
   - from the app's usage data (`fos_usage` by caller, `api_telemetry`,
     `cron_runs`, `cron_ticks`, and the billed Compute and KV usage behind
     `compute_billing`): FOS Class A and B operations per raw file and per
     day, Compute requests, and vCPU ms per 1k lines and per day, both
     billed and logged;
   - from the stage's raw objects and Parquet: gzip bytes per logged line,
     Parquet bytes per row, raw files per day at that period and rate, and
     summary window files;
   - KV Class A and B operations, and front requests and bytes.
   **Fastly-reported usage** (Drew's request). The harness also pulls usage for
   the stage window straight from Fastly's Historical Stats API,
   `GET /stats/service/{id}/field/{field}`. It uses `by=minute` and
   `from`/`to` set to the stage's `first_sent_at` and `last_response_at`, plus
   the settle window. These are the same fields `compute_billing` reads
   (`app/src/sdk.rs`):
   - for the app and worker services: `compute_requests`,
     `compute_execution_time_ms`, `compute_request_time_billed_ms`,
     `bandwidth`, `kv_store_class_a_operations` and
     `kv_store_class_b_operations`;
   - for the front service: `bandwidth` plus `FRONT_BILLING_FIELDS`;
   - for the monitored service: `requests`, as a cross-check against
     `expected_log_lines`.

   It reads the Fastly API with the read-only token, which is never printed.

   Minute data usually lands within 2 to 15 minutes, so a pull right after
   settle can come back incomplete. The harness checks that every minute in
   the window has a bucket.
   - If any are missing, the stage's Fastly numbers are marked `pending`, and
     the stage is pulled again at campaign end. By then the earlier stages are
     hours old.
   - A `backfill <campaign>` command re-pulls any stage still pending later.

   Fastly-reported numbers are the source of truth for Compute, KV and
   bandwidth. The app's own measurements fill in what the stats API doesn't
   report, chiefly FOS Class A and B operations and storage. The builder
   confirms whether any Object Storage fields exist in the stats API. The
   report shows Fastly-reported next to app-measured for every overlapping
   line, and flags gaps over 10%, because that also checks the Usage page's
   own measurement.

   **Real-time stats during the run** (Drew's request). While each stage
   runs, from fdlt start through settle and drain, the harness collects
   per-second data from Fastly's real-time API
   (`https://rt.fastly.com/v1/channel/{service_id}/ts/{timestamp}`, using the
   read-only token). It covers the app, worker, front and monitored services.

   - The first call passes `0`. Every later call passes the `Timestamp` the
     previous response returned.
   - The API keeps only a short rolling buffer, so the collector polls each
     service every 10s, well inside that buffer. The builder confirms the
     buffer length from the docs and sets the interval to under half of it.
     These polls go to Fastly's stats API, not the app, so they don't break
     the "poll the app at the log period" rule, and they add no Compute
     usage.
   - It stores the `aggregated` and `datacenter` totals per second for these
     fields:
     - `compute_requests`, `compute_execution_time_ms`,
       `compute_request_time_billed_ms`, `compute_ram_used`,
       `compute_bereqs`;
     - `compute_resource_limit_exceeded`, `compute_heap_limit_exceeded`,
       `compute_service_vcpu_exceeded_error`;
     - `kv_store_class_a_operations` and `kv_store_class_b_operations`;
     - `object_storage_class_a_operations_count` and
       `object_storage_class_b_operations_count`;
     - `requests` and `edge_requests`;
     - `resp_body_bytes` and `resp_header_bytes` (bandwidth);
     - `origin_fetches`.
   - **Gaps are tracked.** If two consecutive responses don't line up, the
     missing seconds are recorded, and those seconds fall back to the
     minute-resolution Historical Stats for the same window. Every stage
     reports its real-time coverage.
   - **Precedence.** For any line real-time covers completely, its exact
     stage-window totals are the primary Fastly source: Compute, KV,
     bandwidth, and Object Storage operations if those fields are populated
     for these services. Historical minute stats become the cross-check and
     the fallback for gaps. Overlapping lines more than 10% apart are flagged.
   - The harness-share subtraction applies to these numbers the same way.
   - The per-second, per-POP series is also kept for the report. It shows
     Compute and KV load against time and RPS step, and which POPs carried
     the work. That feeds the bottleneck ranking too: a resource-limit or
     vCPU-exceeded error in any second is flagged with its timestamp.

   `/billing/v3/service-usage-metrics` is monthly, so it is too coarse for a
   stage. The campaign summary can use it as a month-level sanity check once
   that month's data is available.

   **Measure first, extrapolate from real data** (Drew's direction). Any
   `estimateMonthly` input that can be measured is measured. The report lists
   each remaining modelled input and why it couldn't be measured, and the
   list should be as short as possible.

   Per stage, the harness records the real count, bytes, rows, and FOS
   operations spent writing and reading for every artifact the pipeline
   produces:
   - raw objects;
   - window Parquet files;
   - window summaries;
   - hour summary bundles and day bundles;
   - compacted files;
   - catalog day records and manifests;
   - ops Parquet.

   Each artifact is extrapolated at its own real cadence:
   - per-window artifacts, multiplied by windows per day at that log period;
   - per-hour artifacts, multiplied by 24;
   - per-day artifacts, taken directly.

   Bytes per row, and the ratio of summary and bundle bytes to rows, come from
   the measured files. They are not defaults.

   **Rollups need closed hours.** Compaction and hour bundles act only on
   closed hours, and a 10-minute stage doesn't close one. So after each
   ladder (once per log period), the harness waits for the last stage's hour
   to close and for compaction and bundling to finish. It then measures the
   real rollups: compacted file count and size against the source windows,
   bundle sizes, and the FOS operations and vCPU those jobs spent. That
   gives real roll-up ratios for each rate in that ladder. The wait is
   bounded at the hour close plus 3 tick intervals. It is idle time, so it
   costs almost nothing. (My call.)

2. **Projection.** `estimateMonthly` is called with the stage's rate and log
   period, the service's current retention, delete-after and compaction
   settings, the measured unit costs, and the install's configured rates. It
   returns a monthly total and each line item. Storage is modelled over the
   retention period, not extrapolated from 10 minutes of data.
3. **Calibration.** `estimateMonthly` is called again with the default unit
   costs. When a line item differs from the measured projection by more than
   25%, the report flags it, because the Cost Estimator page would be wrong
   at that scale.

The report gets a cost table by log period × RPS, with line items. It also
says which inputs were measured and which were modelled. Projections assume
steady traffic at that rate. They are planning numbers, not a quote.

### 7. Removal of the old harness

Delete these:

- `app/tools/loadtest/generate.py`, `verify.py`, `replay.py`, `bench.py`
  and `README.md`
- `app/tools/capacity_campaign.py`
- `app/tools/generator_fixtures.py`, if nothing else uses it
- their tests (`test_loadtest.py`, `test_replay.py`,
  `test_sender_workers.py` and the related parts of other test files)

`deploy_test_all.py` imports `generate` and `verify` at module level. It
uses `verify.check_schema` for the Parquet schema check and `verify` for
per-stage ingestion checks. Those two checks move into the new harness
package, and `deploy_test_all.py` imports them from there.

Its stage 4, which sends 10/100 RPS of its own traffic, becomes two
single-request `curl`s carrying `X-LT-*` ids, followed by an exact id
read-back. That way `make deploy-test` doesn't need a provisioned relay or a
go for load. (My call.)

Keep `execution_logs.py`, moving it if that helps. Extract the reusable browser code out of
`browser_verify.mjs`. Before deleting anything, check its callers with graft.

### 8. Tests

The tests run offline. They cover:

- command building, binary resolution and the run-id cap check;
- SQL generation, and the format of the observed files;
- streaming id export at millions of rows;
- reconcile parsing, using a fake fdlt script that exits 0, 1 and 2;
- the settle loop;
- the bottleneck rules, run on fixtures;
- the browser probe, run against the local static export.

Also run the real `fdlt reconcile` against a hand-made export, observed file
and ledger (ledger lines need `"outcome":"delivered"`). Run the generated
queries read-only against live Parquet. None of this is live evidence.

## Live sequence (Drew's go at each step)

1. Commit the harness and destination prep on a branch, then merge and deploy
   with approval.
2. Drew runs `fdlt provision` for the destination hostname and raises the
   relay service's backend limit.
3. Send two smoke `curl`s and read their ids back in full.
4. Run a smoke stage (10 RPS for 60s) through the full harness.
5. On one go from Drew, run the whole campaign:
   - the ladder 100 → 500 → 1,000 → 2,500 → 5,000 RPS, 10 minutes per
     stage;
   - at a 10s log period, then 1m, then 5m;
   - with the harness changing the period itself between ladders.

   The campaign stops only on a strict failure, a backlog that doesn't
   drain, or a period change that doesn't take.
6. Write up each level: ingest lag and freshness, aggregator and worker
   behaviour, the verdicts, loss or duplication, file shape, pages and
   bottlenecks.

**Cost.** A 5,000 RPS stage of 10 minutes is 3M requests, a 1.56 GB ledger
and 3M logged ids. Its Parquet is downloaded once.

## Decided by Drew

- Production characterization: sequential ladder up to 5,000 RPS across log
  periods. It checks completeness, ingest lag, every page and its insights,
  UX metrics (including CLS), file counts and sizes, and fixable bottlenecks.
- fdlt is the only traffic source. All old harness work is replaced, and
  parts of it may be reused.
- This repo owns both the destination prep and the harness.
- Drew provisions the services, runs `fdlt provision` and raises the backend
  limit.
- The log periods under test are 10s, 1m and 5m.
- Once Drew gives the go for a campaign, the harness changes the log period
  between ladders itself, through the app's admin API, and carries on until
  the campaign is done. It doesn't stop to ask. Before each ladder it
  confirms the new period is active, and if the change doesn't take, it
  stops.
- The monitored service's origin becomes `https://http-me.fastly.dev`.
- Each stage projects a month of pipeline cost (not delivery cost) at its
  log period and rate, using the app's own `estimateMonthly`.
- Once a stage is validated, its per-request data is cleaned up and only the
  run-level record is kept.
- Requests that were logged but not delivered pass if the ledger has them as
  non-delivered. They are reported separately, with 499s counted on their
  own.
- The monitored service is synthetic-only, so the fdlt snippets apply to
  every request with no secret gate.
- The harness never deletes bucket data, so test data builds up across runs
  and days. Delete Data is a separate admin action that Drew runs manually.

## My calls (for Drew to check)

- Hard failures cover only completeness and correctness. Performance is
  reported, not gated.
- Stages run strictly in sequence, and the ladder stops on a strict failure or
  a backlog that doesn't drain.
- Logged rows are filtered to `edge = 1`, and shield rows are reported
  separately.
- `pop` is strict until the smoke stage shows otherwise.
- New columns are named `lt_run_id` and `lt_request_id` (FLA's names), and
  existing columns are reused, not renamed.
- Artifacts live under `~/.flp-state/loadharness/`.
- The two id fields are install config, applied through the admin API, not
  catalog code.
- `deploy-test` uses two `curl` smoke requests, not fdlt. The schema and
  ingestion checks move into the harness package.
- The browser probe does one cold pass per stage (under load), and the rest
  of its loads are warm.
- The settle and drain bounds are 3 log periods plus 5 minutes.
- After each ladder, the harness waits for the hour to close (plus 3 ticks)
  so it can measure real compaction and bundle rollups.
- Fastly's minute stats that aren't available yet are marked `pending`, then
  backfilled at campaign end or with `backfill`.

## Seeder evidence (kept permanently)

The next project is a log seeder for rates above 5,000 RPS. It has to produce
files that look like the real ones: files written by Fastly's log
aggregators, with traffic arriving from POPs all over the world. The real raw
files don't survive:

- the app removes raw objects after conversion;
- the harness deletes its own downloads after each stage passes;
- Parquet stays in the bucket, but only until retention or a manual Delete
  Data removes it, and it is a converted form, not the raw aggregator files.

So the harness records everything the seeder needs while the runs happen.
None of it is cleaned up.

**Raw samples.** During each stage, the monitor walks the raw prefix
(`/api/admin/raw-tree`) at each poll. It downloads a bounded sample of new
raw objects, exactly as stored and still gzipped, using the read-only FOS key
in `~/.flp-state/`.

- Sampling is stratified across object key patterns and arrival times.
- The default is up to 30 objects or 50 MB per stage, whichever comes first.
- Raw objects survive at least until the next ingest tick, so sampling at the
  log period catches them. The report counts any that were deleted before
  they could be sampled.

The samples are kept under
`~/.flp-state/loadharness/<campaign>/seeder/period-<p>/stage-<n>/`.

**Shape statistics** come from every raw object seen (via the listing and
`ingest_files`) and from the samples. They are written to
`seeder-profile.json` per stage, with a rollup per log period:

- objects per minute, and per log period interval;
- object key naming pattern, and the key's time compared to the time it
  arrived (`LastModified`);
- per object:
  - size, compressed and uncompressed, and the compression ratio;
  - line count;
  - the distribution of bytes per line;
- timestamps within an object: `ts_ms` min, max, spread and ordering, and how
  late lines are compared to the object's key time and `LastModified`;
- POP mix per object (`pop`), and whether one object holds one POP or many;
- how lines are batched across objects for the same POP and minute;
- for each field: null rate, value-length distribution, and cardinality of
  key fields (`pop`, `status`, `method`, `cache`, `url` path shapes,
  `country`, `asn`);
- the fdlt stage parameters that produced it (rate, scenario pack, profiles
  and seed from `run.json` provenance), so each profile can be tied to the
  traffic behind it;
- the end-to-end delay from fdlt's send time to the raw object's arrival
  (`LastModified`), by POP, using `lt_request_id` to join the samples to the
  ledger before the ledger is deleted.

## Cleanup after each stage

Once a stage passes strict reconcile, the harness deletes its bulky
per-request local artifacts: the ledger, `observed-ids.txt`, and the
downloaded Parquet and raw objects. It keeps the run-level record:

- `run.json` (fdlt's summary, phase bounds and provenance)
- the observed aggregate files and both verdict JSONs, including id
  statistics
- the monitor time series (lag, backlog, per-execution resource use)
- the file-shape inventory (keys, sizes and rows, but not the contents)
- the browser probe results, with DebugPanel telemetry summarized per page
- the stage report and the ladder summary

If a stage fails, everything is kept so the failure can be diagnosed, and the
report says which files can be deleted once it has been looked at. fdlt's own
`~/.fdlt/runs.db` keeps the run summaries, so the harness doesn't delete
them.

### Load-test rows in the bucket

**The harness never deletes bucket data** (Drew's decision, which
replaces the earlier plan to wipe at campaign end). That covers between
stages, between ladders, across hour boundaries, and at the end of a
campaign. Test data builds up across runs and across days, so:
- tests can run over days;
- the long-range views (6h, 24h and longer) are exercised on real history;
- hour-close rollups and look-back queries always have data.

The service's own retention settings are the only thing that ages data out.
Before a campaign, the harness records the retention in effect and warns if
it is shorter than the campaign's planned span.

Per-stage cleanup touches only the harness's local copies, such as the
ledger and downloaded Parquet. The app keeps every file.

**Delete Data** (its own design, built first) is a standalone admin action.
Drew runs it from the admin page whenever he wants a clean slate. The
harness never calls it.

## Next after this

- **Log seeder for rates above 5,000 RPS.** Port or rebuild the
  fastly-log-analytics seeder, which writes synthetic log objects into the raw
  FOS bucket. Base it on the raw samples and `seeder-profile.json` this
  harness keeps (see "Seeder evidence"). Then rerun the ingest and page
  checks at higher scale. This is a separate design.

## Before this

- **Delete Data** admin action, ported from fastly-log-analytics. It has its
  own design and is built first.
