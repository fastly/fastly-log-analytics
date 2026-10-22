# Section-sweep measurement evidence

The reporting tools distinguish observed data from missing evidence. They do not currently certify Phase 4b or production capacity.

## Initial visible-results budgets

Both serving modes use the same initial p95 limits at their respective qualified data volumes, with two active users under live ingestion. Five active users is a separately reported stress case.

| Window | Common unfiltered view | Supported compound filters |
|---|---:|---:|
| 24h | 2 seconds | 5 seconds |
| 3d / 7d | 5 seconds | 10 seconds |
| 30d | 10 seconds | 30 seconds |

The machine-readable source is [`scripts/lib/performance_budgets.json`](../../scripts/lib/performance_budgets.json), approved on 2026-10-22. Acceptance limits are not optimization stopping points. These limits include transport and rendering until requested visible results are usable; an API response alone cannot establish success. Request, section, render and interaction timings remain separate. Independent whole-request and immediate-feedback limits are not yet declared in that file and must not be invented by the reporter.

The 500 ms section threshold is a triage trigger. With a 10-second logging period, the initial peak freshness target is p95 event-to-queryable lag <=20 seconds; this is not a query-response-time limit. Request and RUM freshness are distinct measurements. None of these targets establishes that the current hardware can meet the proposed ingest volume or retention.

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

## Repeated samples

[`scripts/dev/section_sweep.py`](../../scripts/dev/section_sweep.py) keeps request order separate from cache state. It records exact request payloads, a page-load correlation ID per sample, environment, role, window, independent component observations and request wall time. It retains errors as failed samples without printing credential-bearing exception text.

Latency summaries use empirical nearest-rank percentiles and include count, minimum, mean, p50/p90/p95/p99, maximum and population standard deviation. Thirty samples per cache-state population is the harness's initial p95 sample floor; p99 has a 100-sample floor. Both are descriptive estimates, not confidence guarantees. Repeated section offenders require at least two distinct over-threshold samples with identical grouping dimensions. A handful of samples cannot establish a tail-latency qualification.

Exact payloads can contain sensitive filter values. JSON report files are owner-only (`0600`), existing permissive output files are tightened before writing, and symlink destinations are rejected. Keep these artifacts private and out of Git. Console reports omit the exact filter values; the private JSON retains them for reproducibility.

The current live sampler uses administrator mTLS only. It does not implement the Analyst Path B lifecycle or imply analyst coverage. HTTP sampling does not establish page-ready, interactions, history, coverage, controlled misses or complete query/I/O attribution. Consequently it reports `qualification: incomplete` and exits with status 2 even when requests succeed.

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

The gate checks evidence presence, not the truth of arbitrary JSON claims. Do not manufacture a report to bypass unresolved prerequisites. This reporting foundation does not provide the missing backend instrumentation or verification evidence.

The CLI also requires a fixed anchor, supported windows, and two active filter dimensions. Filter syntax validation does not prove the fields are supported by every endpoint, match real rows, or constrain results. Those properties require scenario-specific evidence before sampling. No HTTP cache-control header, URL nonce or first/repeat convention establishes a controlled response-cache miss. Shared caches must not be flushed and environments must not be restarted for this purpose.

After the entire gate is genuinely verified, the required order is Remote Standard GCE, Local Standard, then Remote High-Scale Elevation. Keep required live request and RUM traffic flowing, preserve role boundaries, and save results under `performance-report/section-sweep-<env>-<date>.json`. Do not seed or backfill implicitly. Reopen affected pages only, in tracker order, and pause after the first affected page.

## Browser verifier

[`scripts/verify_dashboard.js`](../../scripts/verify_dashboard.js) now collects both aggregate and bundle responses. It preserves repeat responses rather than overwriting each endpoint with its last result, drains the response cohort before reporting, and prints component cache/source metadata and explicit unknowns on the `PERF` line.

This is diagnostic reporting, not page certification. The verifier still reports missing history, coverage, attribution, page-ready and interaction evidence. A populated chart and a short request duration cannot clear those gaps. Its existing deployment/authentication behavior is unchanged; do not run the deployment verifier merely to obtain benchmark samples.

## Offline verification

The Python tests below intentionally do not load the application's database fixtures: these reporting tests have no database dependency. Browser-recorder verification can use fully intercepted fixture requests without contacting any deployed environment.

```bash
uv run python -m pytest --confcutdir=tests/scripts -n 0 \
  tests/scripts/test_section_measurements.py tests/scripts/test_section_sweep.py
node --test tests/scripts/section_measurements.test.cjs
uv run ruff check scripts/dev/section_sweep.py scripts/lib/section_measurements.py \
  tests/scripts/test_section_sweep.py tests/scripts/test_section_measurements.py
uv run ruff format --check scripts/dev/section_sweep.py scripts/lib/section_measurements.py \
  tests/scripts/test_section_sweep.py tests/scripts/test_section_measurements.py
uv run mypy --follow-imports=silent scripts/lib/section_measurements.py scripts/dev/section_sweep.py
```
