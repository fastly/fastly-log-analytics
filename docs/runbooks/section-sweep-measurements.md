# Section-sweep measurement evidence

The reporting tools distinguish observed data from missing evidence. They do not currently certify Phase 4b or production capacity.

## Initial visible-results budgets

Both serving modes use the same initial p95 limits at their respective qualified data volumes, with two active users under live ingestion. Five active users is a separately reported stress case.

| Window | Common unfiltered view | Supported compound filters |
|---|---:|---:|
| 24h | 2 seconds | 5 seconds |
| 3d / 7d | 5 seconds | 10 seconds |
| 30d | 10 seconds | 30 seconds |

The machine-readable source is [`scripts/lib/performance_budgets.json`](../../scripts/lib/performance_budgets.json); the visible-results matrix was approved on 2026-10-22. Acceptance limits are not optimization stopping points. These limits include transport and rendering until requested visible results are usable; an API response alone cannot establish success. Request, section, render and interaction timings remain separate.

On 2026-10-23, the operator approved immediate-feedback acceptance at **p95 <=200 ms**, with **p95 <=100 ms** as an optimization target. Measure browser event arrival, including input delay, through the first meaningful painted acknowledgment: a selected filter, pending label, stale-results indication or navigation skeleton. A JavaScript state update alone is not a painted acknowledgment. Do not wait for the network or heavy chart processing before acknowledging an action. Feedback does not establish results completion and is not an INP certification.

Whole-request HTTP/API duration is explicitly **diagnostic-only**, approved on 2026-10-23. The null `whole_request_p95_ms` is intentional under `whole_request_budget_policy: diagnostic_only`, not a missing API deadline. Keep reporting request durations for attribution and optimization; use the approved end-to-end visible-results matrix for results-completion pass/fail. This policy supplies no missing browser observations and does not waive other qualification prerequisites.

The 500 ms section threshold is a triage trigger. With a 10-second logging period, the initial peak freshness target is p95 event-to-queryable lag <=20 seconds; this is not a query-response-time limit. Request and RUM freshness are distinct measurements. None of these targets establishes that the current hardware can meet the proposed ingest volume or retention.

### Future load-testing safety

Actual client HTTP traffic is capped at **2,500 RPS aggregate** to protect the Compute scorer; this supersedes the earlier 10k ceiling. Use real traffic at or below the ceiling. An authorized run must ramp gradually: 100, 500, 1,000, 1,500, 2,000 and at most 2,500 RPS, with at least five minutes and a health/freshness/backlog review at each stage. Reduce or stop on degradation; do not advance automatically just because time elapsed. Above 2,500 events/sec, tests write synthetic logs directly to FOS using `scripts/load_test/generate_synthetic_traffic.py --target fos`, not higher-rate edge requests or direct serving-table inserts. Report events/bytes/objects per second separately from client RPS; synthetic ingestion does not certify edge/VCL/Compute-scoring delivery or real event freshness and cannot substitute for missing retained history. This plan does not authorize load, uploads, seeding/backfill or infrastructure changes during prerequisite work.

## Response evidence

[`scripts/lib/section_measurements.py`](../../scripts/lib/section_measurements.py) and its browser-verifier counterpart preserve component boundaries. Dashboard aggregates and bots, and Network health, quality, transport and shielding can have independent cache states.

- An explicit `_response_cache.state` can report `hit`, `miss`, `bypass` or `unknown`. Existing true `_is_cached` / `is_cached` markers establish a hit for that component. A false default does not establish a miss.
- Conflicting positive hit and miss/bypass markers produce an unknown state and an explicit conflict gap. The positive hit evidence still prevents stale timing from being treated as current execution.
- An enclosing cache hit makes all nested section execution historical, even if a saved child payload says it missed its own cache.
- Section source and cache state are different dimensions. Sources include `rollup`, `live`, `projection`, `fact`, `projected_fact` and `unknown`. A name containing `rollup` is only name-derived evidence; it is not verified reader dispatch. A `_cache_hit` suffix is cache activity, not a rollup.
- Both `time_ms` and `elapsed_ms` are understood. Invalid, negative, boolean or missing durations are unknown, never zero. A measured numeric zero is preserved.
- Sections are not summed into request compute or overhead. Parallel spans can overlap.
- Reports do not copy response bodies, raw SQL, debug call URLs, headers or cookies. Section names and timing metadata are retained.

The explicit metadata parser is not a claim that all current server paths emit that metadata. Missing server instrumentation remains visible as gaps.

The CMCD facts route attributes its coverage and last-visible-event lookups to
`cmcd.watermark` (`projected_fact`, `activity="overhead"`), separately from
`cmcd.facts` computation. Both use the existing request correlation and debug
policy. Their durations are client wall observations, not server CPU.

### Cache-owner observation contract

Cache observations are stamped at the component's owner, not inferred from request order. `_response_cache` carries `state`, configured `ttl_seconds` when applicable, and `prewarmer` (`null` when not established). A second lookup that hits after an earlier miss reports a hit. Request-specific observations are excluded from stored payloads.

Single-flight is not a response cache. A leader that computes after a failed lookup reports `miss`; a leader whose recheck hits reports `hit`. Followers report `unknown` with `single_flight: follower` and measured `wait_ms`, never a computed miss. Their shared section timings are not current execution for that caller. An uncached component reports `bypass`, without claiming a cold database. These operational fields contain no cache keys, SQL, credentials or filter values and do not grant access to debug data.

Controlled-miss sampling uses normal TTL expiry, with a bounded wait and unchanged request payloads. The initial response is a baseline even when it misses: a qualifying request must start after at least its configured TTL has elapsed following that baseline's completion. It then requires explicit backend miss and subsequent hit observations for each applicable component; bypass components are recorded separately. Historical child observations inherited from a cached subtree or a single-flight follower cannot establish either side of the pair. Concurrent repopulation, unknown state, failed requests or an exhausted bound leave evidence incomplete. The operator selected best-practice behavior and clarified that concurrent users are not the norm; this does not waive the approved two-user qualification or separate five-user stress case. No bypass API, shared-cache invalidation or key-changing trick is authorized.

## Reader coverage observations — partial implementation

Standard dashboard aggregates and top bots can carry component-owned
`_reader_coverage` records for the connection-request histogram and NGWAF bot
rollup readers. Each records its actual eligibility reason, resolved bounds,
2-hour minimum and 366-day maximum, followed by each day-prefer/hour-fallback
attempt. The histogram records its specialized-bundle and all-fields attempts
separately. Both use the existing 50% closed-hour coverage floor.

The existing planning walk supplies the closed/covered hour counts, fraction,
active-hour boundary, partial-leading-hour exclusion and selected day/hour file
counts. Done markers are counted separately where supported. A pre-walk refusal
keeps unobserved counts null, not zero. These observations add no SQL or file
probes and contain no paths, SQL, filter values or exception text.

A `planned` attempt does not prove a successful read. Reader outcome and live
active-hour top-up are separate: a read can fall back, while an unavailable,
failed or missing-column top-up can leave the existing closed-hour result in
place. `served` is not a completeness claim. File presence does not verify the
rows or field membership inside an all-fields bundle, and a 50%-covered window
is not complete history.

Both reporters retain only allowlisted typed observations, separate current
observations from historical cache/follower reuse, and keep unknown cache state
unknown. Bundle wrappers do not invent missing-reader gaps for observations
owned by their children. Absent or malformed leaf-reader metadata remains a
gap. Observations cannot clear `history_evidence_missing` or certify a window.

Standard dashboard top-N and UA-bot reads also expose their selected tier counts,
any-field hour presence, bounded heal candidates and older unexamined hours.
These readers have no blanket minimum-window or 50% acceptance gate. Any-field
presence is not all-field completeness: bundle membership remains unverified,
and a compacted boundary day can suppress healing without serving that entire
boundary. Their live/heal/read outcomes stay separate from file presence.

Count and time-series readers retain the original window and denominator, daily
prefix hours/files, trailing-hour subwindow, verified-empty sentinels, heal gaps
and refusal reason. They use their own fail-closed/bounded-heal policy, not the
tolerant leaderboard threshold. A composite-query failure leaves live execution
unverified rather than declaring its live branch successful or independently
identifying which branch failed.

Optional `_history` records reuse the existing extent/count work and distinguish
status-cache extents, live count/min/max, count-only fallback and unavailable
observations. Failed observations retain null counts even where a legacy response
returns zero. Window rows identify raw, materialized-temp or rollup provenance.
`raw_extent_reaches_window_start` compares the requested start with the earliest
observed raw event: false means the observed extent begins after the requested
start, true means it reaches that boundary, and null means the comparison is
unavailable. False is an explicit incomplete-prefix warning for raw-versus-rollup
comparisons; true does not prove continuous coverage, exact rollup contents, or
why an extent begins where it does. In particular, an observed extent is not
proof that retention caused a gap. Raw-log `data_retention_days` and local
rollup `rollup_retention_months` are separate horizons; retained aggregates can
outlive the raw events needed to prove their contents. Cached extents and rolled
counts do not establish current exact history; even a live extent query does not
establish continuous retained coverage. No new scan is added merely to populate
these fields.

Skipped/filtered branches that do not invoke a rollup reader have no rollup
observation; absence is not evidence of eligibility or coverage. These are
instrumentation capabilities, not verified deployed history. The full
prerequisite gate remains closed.

### Targeted time-series bundle repair

When a matched raw-versus-rollup comparison proves that specific closed UTC
hour bundles are wrong, an administrator can rebuild only those hours with
`POST /api/admin/rebuild-time-series-rollups` and `{"hours":["YYYY-MM-DD-HH"]}`.
The endpoint accepts up to 24 unique, valid, closed UTC hours and atomically
replaces only their `time_series.parquet` files from the current serving view.
It does not clear caches, rebuild other rollups, or fill missing event history.
For a proven daily-tier discrepancy, send `{"days":["YYYY-MM-DD"]}` instead
(up to seven unique closed UTC days). At least one hour or day is required.
Each selected day recomputes all 24 hours from retained events into private
staging through one source connection, then atomically replaces only that
daily time-series derivative. Retired or partial official hourly inputs are
not used or rewritten. Incomplete recomputation leaves the existing daily
file intact. Verify retained source history before repair; this operation
cannot recover expired events.
After the normal aggregate response-cache TTL expires and a computed miss is
confirmed, compare the same half-open raw event-time bounds and minute buckets
again; do not flush shared caches, and do not treat a reported write count
alone as content-correctness evidence.

## High-Scale section and query correlation

The dashboard, Security, Origin, Performance, Network and CMCD readers instrument
existing task boundaries through `backend/core/high_scale_sections.py`. Each
copied worker context carries the real service, request/page-load identity,
component, section and unique section ID. Nested scopes restore their parent.
Source declarations follow actual projection/fact dispatch; mixed bot components
report `mixed` while their individual queries retain concrete sources. CMCD
explicitly reports `projected_fact`, not a preaggregated rollup. Named task
submission records executor queue wait separately from task execution wall time;
inline execution declares that no executor queue was used. The Network bundle retains
PoP query timing while preserving its list-shaped PoP payload. Cache hits and
single-flight followers do not create fresh execution sections.

Performance's `top_asns` section ends before ASN metadata enrichment.
The separate inline `performance.asn_labels` overhead section retains request
and page correlation without claiming a ClickHouse fact/projection source.
Cached responses reuse both observations as historical execution.

`ClickHouseClient._request` correlates the transmitted query ID and records:

- `admission_wait_ms`: the existing bounded client semaphore acquisition;
- `network_ms`: the HTTP exchange wall time, including server work, transfer and
  possible HTTPX connection/pool wait—not pure wire latency;
- `decode_ms`: JSON parsing and response validation;
- `client_elapsed_ms`: total client operation wall time;
- `server_elapsed_ms`: optional server-reported `statistics.elapsed`, converted
  from seconds, with `server_elapsed_source="statistics.elapsed"`. Invalid or
  absent values remain unknown. This is server wall time, not CPU.

Unattempted phases remain null. Attempted failed phases retain their elapsed
observation. These phases and overlapping sections are never summed into server
compute. Server CPU remains explicitly unobserved, not estimated from client RTT.
ClickHouse documents `statistics.elapsed` in its [JSON format](https://clickhouse.com/docs/interfaces/formats/JSON);
its [JSON writer](https://github.com/ClickHouse/ClickHouse/blob/master/src/Formats/JSONUtils.cpp)
emits `Stopwatch.elapsedSeconds()`. No extra server query is made to obtain it.
Phase histograms use operation/outcome labels only; correlation IDs stay in logs
and the existing debug-gated `_debug_queries[].clickhouse` envelope. Persistence
uses the scoped service/request identity; an unscoped ClickHouse call does not
fall back to whichever service is globally active.

Both reporters retain only allowlisted correlation IDs, outcomes and timings,
never raw SQL, URLs or response data. Cached query records and component/follower
reuse remain historical; missing query/section links and server CPU observations
stay explicit gaps. Existing debug permissions are unchanged. This instrumentation
alone is not deployed correlation evidence or performance certification.

## Disposable Analyst fixture

`scripts/lib/analyst_fixture.cjs` exports `withAnalystUsers(options, work)` and
`recoverOwnedInvites(options)`. This is a Chromium fixture, not a sharing setup
routine or permission to run a performance sweep. It requires explicit target
service and exact HTTPS administrator/analyst origins. Administrator identity
comes from the existing exact-origin mTLS configuration. Sharing must already be
active at the expected analyst endpoint, passcode login enabled, and capacity
available. It never starts sharing, changes settings, enrolls trust or changes
TOS policy.

Each requested user gets a distinct random, one-hour passcode invite scoped to
only that service with an explicit masking mode. The analyst context has no
administrator certificate, storage state or injected trust headers; TLS errors
are not ignored. The provided page uses Chromium Fetch interception to deny
cross-origin requests, including redirect hops, before credentials are filled.
Document origin is independently checked before credential/TOS interaction.
Playwright credential-logging/debug modes are refused. No HAR, trace or storage
state is recorded. Use the provided pages; additional pages are outside this
fixture's guarded-page contract.

The normal form and TOS UI perform login/acknowledgment. The fixture verifies
secure HttpOnly session cookies, pending-cookie removal and TOS session rotation,
then requires a target-only read-only bootstrap with the expected masking flag,
an administrator denial and an other-service analytical denial. This verifies
lifecycle and declared scope—not actual real-data masking, SSR payload privacy,
filter correctness, retained history or browser completion.

`journalPath` is a new owner-only append-only JSONL ownership journal. It contains
run markers, owned invite identifiers and non-secret scope metadata, never
passcodes, cookie values, hashes or raw responses. Fixture and recovery hold the
same exclusive `.lock` for their entire lifecycle; recovery cannot truncate an
active journal. A lock left by an interrupted process is not automatically
reaped: confirm the owning PID has terminated before removing that run's lock
and invoking recovery. Never remove a live run's lock.

Cleanup attempts logout and independently deletes only ownership-verified invites,
checks linked sessions by invite ID, closes every owned context and reports all
cleanup failures. A timed-out create with no observed durable row remains
`create_completion_unresolved`; two empty snapshots do not certify that a late
server commit cannot happen. Recovery can settle it only after observing the
matching durable invite and verifying its removal. Expiry is a backstop, not
proof of cleanup. The journal supports interrupted recovery; neither SIGKILL nor
network loss can be promised automatic cleanup.

Offline tests include real intercepted Chromium login/redirect cases and fake
transport failures. No deployed lifecycle is verified by those tests. The
section-sweep browser evidence and the guarded analyst fixture are the only
qualification paths; ad-hoc page-wide waterfall scripts are not part of the
protocol.

## Repeated samples

[`scripts/dev/section_sweep.py`](../../scripts/dev/section_sweep.py) keeps request order separate from cache state. It records exact request payloads, a page-load correlation ID per sample, environment, role, window, independent component observations and request wall time. It retains errors as failed samples without printing credential-bearing exception text.

Latency summaries use empirical nearest-rank percentiles and include count, minimum, mean, p50/p90/p95/p99, maximum and population standard deviation. Thirty samples per cache-state population is the harness's initial p95 sample floor; p99 has a 100-sample floor. Both are descriptive estimates, not confidence guarantees. Repeated section offenders require at least two distinct over-threshold samples with identical grouping dimensions. A handful of samples cannot establish a tail-latency qualification.

Exact payloads can contain sensitive filter values. JSON report files are owner-only (`0600`), existing permissive output files are tightened before writing, and symlink destinations are rejected. Keep these artifacts private and out of Git. Console reports omit the exact filter values; the private JSON retains them for reproducibility.

The optional `--controlled-cache-pair` adds a bounded observation sequence per scenario: at most four requests within a 120-second sampling deadline, including expiry waits. `scripts/lib/cache_sampling.py` accepts an injected request transport for offline tests; it cannot open a connection itself. The CLI uses `scripts/lib/admin_http.py` with the existing exact-origin certificate configuration and an asynchronous total network deadline covering headers and body. It refuses redirects and environment proxy discovery; it never sends gateway/bootstrap credentials. The initial baseline, each safe response observation and component miss/hit sample numbers are retained separately from ordinary latency populations. One observed pair does not establish a miss-population p95. Exhausting either bound leaves gaps; the overall report still cannot certify a page.

Current implementation covers Standard dashboard aggregates/bots, Security, Performance, CMCD and Network health/shielding cache owners, and the High-Scale dashboard, Security, Performance and CMCD memos. Origin composites, uncached High-Scale Network/bots, Standard Network quality/transport and the bundle assembly itself report bypass. Typed optional observations survive without granting debug privileges; omitted observations on other routes remain unknown. Network health declares `scope: self` because shielding is assembled independently after health lookup. A cached enclosing subtree still suppresses both children's execution and marks their lookup observations as historical.

Network preserves the list-shaped `pop_health` data and carries its own cache/timing envelope in `_component_observations.pop_health`. This sidecar is debug-gated and contains no duplicate data rows. Both reporters use it only when the named list component exists; absent or malformed sidecars remain unknown. An enclosing hit or single-flight follower still makes the child observation historical. The Standard owner records its actual rollup attempt and live fallback; High-Scale records its fact reader. No history or reader-coverage claim is inferred from either.

Standard PoP health falls back to the scoped raw reader when the window starts partway through an hour. Its whole-hour bundles omit that leading slice, and its live top-up restores only the active-hour tail. The fallback preserves the existing inclusive bounds; it does not repair historical bundles or establish the cause of a measured mismatch.

Standard Security, Origin, Performance, CMCD and Network dispatch sites stamp their actual reader source and current activity. Gated or missed rollup attempts are overhead with a fallback outcome, not evidence that a rollup served the data. Skipped materializations are not compute, and Performance ASN-label enrichment is separately timed overhead. Network quality retains its rollup timing envelope on the successful early-return path. These stamps do not supply missing reader-local coverage/history, prove raw-count parity, or qualify a page; ordinary cache hits still reuse historical work.

Standard granular Origin endpoints stamp the live reader only after it runs; SQL templates named `ROLLUP` do not imply a precomputed rollup source. Shielding records its no-match presence probe separately, only when executed. Missing required columns or latency expressions emit no compute claim. The existing response memo retains these timings as historical on a hit; cache reuse does not become new reader execution.

The current live sampler uses administrator mTLS only. It does not implement the Analyst lifecycle or imply analyst coverage. HTTP sampling does not establish page-ready, interactions, history, coverage, controlled misses or complete query/I/O attribution. Consequently it reports `qualification: incomplete` and exits with status 2 even when requests succeed.

## Prerequisite gate

No live request is made without a reviewed prerequisite report matching the selected environment and service. Each named check must be `verified` and reference a nonempty local evidence file:

- `cache_reporting`
- `controlled_miss`
- `filter_scenarios`
- `history_coverage`
- `query_io_attribution`
- `browser_budgets`
- `analyst_lifecycle`
- `live_traffic`

The gate checks evidence presence, not the truth of arbitrary JSON claims. Do not manufacture a report to bypass unresolved prerequisites. Partial instrumentation does not itself verify deployed history, correlation or browser evidence.

The CLI also requires a fixed anchor, supported windows, and two active filter dimensions. Filter syntax validation does not prove the fields are supported by every endpoint, match real rows, or constrain results. Those properties require scenario-specific evidence before sampling. No HTTP cache-control header, URL nonce or first/repeat convention establishes a controlled response-cache miss. Shared caches must not be flushed and environments must not be restarted for this purpose.

After the entire gate is genuinely verified, the required order is Remote Standard GCE, Local Standard, then Remote High-Scale Elevation. Keep required live request and RUM traffic flowing, preserve role boundaries, and save results under `performance-report/section-sweep-<env>-<date>.json`. Do not seed or backfill implicitly. Reopen affected pages only, in tracker order, and pause after the first affected page.

## Browser verifier

[`scripts/verify_dashboard.js`](../../scripts/verify_dashboard.js) now collects both aggregate and bundle responses. It preserves repeat responses rather than overwriting each endpoint with its last result, drains the response cohort before reporting, and prints component cache/source metadata and explicit unknowns on the `PERF` line.

This is diagnostic reporting, not page certification. The verifier still reports missing history, coverage, attribution, page-ready and interaction evidence. A populated chart and a short request duration cannot clear those gaps. Its existing deployment/authentication behavior is unchanged; do not run the deployment verifier merely to obtain benchmark samples.

The canonical runner forwards `--no-traffic` to the browser verifier when traffic
generation is disabled. Historical charts and deployment checks still run;
recent injected request/RUM counts and header freshness are explicitly not run.
In ordinary mode, recent counts require responses matching the selected absolute
15-minute bounds. Lifetime header totals are not window counts, and a recent
header snapshot does not prove advancement or event-correlated freshness.

### Painted action observations

`scripts/lib/section_measurements.cjs::measurePaintedAction(page, options, action)`
provides a bounded same-document action observation, independently of HTTP
response timing. Supply the action target, a state-specific feedback selector
with exact expected text, and one state-specific exact-text marker for **every**
requested visible result panel. The markers must not already be visible before
the action. Use the existing isolated analyst page when testing that role; the
observer neither creates a browser context nor adds credentials or trust headers.

The observer captures a trusted click or keydown's browser `event.timeStamp`,
not the driver's click start or the JavaScript handler start. Chromium Element
Timing supplies the text's `renderTime`; mutation/rAF callbacks, network-idle,
state changes and canvas presence do not supply a paint timestamp. Thus the
elapsed duration includes input queuing before dispatch. Event Timing can
separately supply `processingStart - startTime` as the input-delay breakdown;
short events may not emit it, which remains an explicit gap without removing
input delay from the end-to-end measurement. This is not INP certification.

Element Timing alone also reports text behind an opaque overlay. Chromium's
visibility-tracking Intersection Observer must independently confirm an
unoccluded marker at or before that paint; a later visibility confirmation
cannot backdate a hidden paint. Missing visibility support remains a gap.
Visibility callbacks validate paint; their delivery time is not substituted
for the paint timestamp. Page adapters must use text-only marker elements
without obscuring descendants or unrelated content.

Feedback and results have separate timestamps. All requested result markers must
have painted and still be simultaneously visible with their expected text;
their latest paint determines the observed results duration. A page-specific
adapter must establish that these markers represent the requested usable data,
including completed chart processing, rather than a generic heading or a
loading/empty state. Existing text without a new Element Timing entry, CSS-only
selection, canvas/chart completion and cross-document navigation are not inferred:
they remain unobserved until a suitable adapter supplies paint evidence.

Only timing/count/status fields leave the page. Marker text, selectors, URLs,
screenshots and response bodies are not recorded. Temporary `elementtiming`
attributes, listeners, timers and observers are removed in `finally`, including
failed actions. Unsupported paint observations, hidden or pre-paint-removed
acknowledgments, synthetic events and document changes remain explicit gaps.
The result is always `qualification: incomplete`: one local browser observation
does not verify scenario correctness, cache populations, active-user/live-ingest
conditions or the repeated-sample floor. The deployment verifier's page-ready
and interaction gaps are deliberately unchanged; this helper is not a page sweep.

The standalone scope check is explicit and does not run a performance sweep:

```bash
node scripts/dev/check_analyst_lifecycle.cjs \
  --environment "${MEASUREMENT_ENVIRONMENT:?}" \
  --admin-origin "${ADMIN_ORIGIN:?}" \
  --analyst-origin "${ANALYST_ORIGIN:?}" \
  --service-id "${FASTLY_SERVICE_ID:?}" \
  --role masked --users 2 --journal "${OWNERSHIP_JOURNAL:?}"
```

Run unmasked scope separately with `--role unmasked`. The command creates a new
owner-only evidence file with `qualification: incomplete` and explicit remaining
gaps; it does not generate a reviewed prerequisite artifact. Recovery uses the
same environment/origins/service plus `--recover --journal <private-jsonl>`.

## Offline verification

The Python tests below intentionally do not load the application's database fixtures: these reporting tests have no database dependency. Browser-recorder verification can use fully intercepted fixture requests without contacting any deployed environment.

```bash
uv run python -m pytest --confcutdir=tests/scripts -n 0 \
  tests/scripts/test_section_measurements.py tests/scripts/test_section_sweep.py \
  tests/scripts/test_cache_sampling.py tests/scripts/test_admin_json_deadline.py
node --test --test-timeout=45000 tests/scripts/section_measurements.test.cjs \
  tests/scripts/cache_observations.test.cjs tests/scripts/analyst_fixture.test.cjs \
  tests/scripts/analyst_lifecycle_cli.test.cjs tests/scripts/browser_paint.test.cjs
uv run ruff check scripts/dev/section_sweep.py scripts/lib/section_measurements.py \
  tests/scripts/test_section_sweep.py tests/scripts/test_section_measurements.py
uv run ruff format --check scripts/dev/section_sweep.py scripts/lib/section_measurements.py \
  tests/scripts/test_section_sweep.py tests/scripts/test_section_measurements.py
uv run mypy --follow-imports=silent scripts/lib/section_measurements.py scripts/dev/section_sweep.py
```
