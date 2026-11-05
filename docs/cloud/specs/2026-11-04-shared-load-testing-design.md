# fastly-log-analytics-load-testing: design

Status: draft for review, 2026-11-04. It replaces the per-app harness plans in
fastly-logs-parquet (FLP: `docs/superpowers/plans/2026-11-04-fdlt-load-harness.md`,
paused after Task 2) and fastly-log-analytics (FLA: branch
`feat/fdlt-load-harness`, commits `c5877681..5d9c104f`).

## Why

FLP and FLA each grew their own fdlt load harness, and the two do the same
job: drive fdlt traffic, prove exact once-only delivery, then measure lag,
pages and cost. Keeping two copies means fixing every bug twice and letting
the two drift apart. One standalone project, `~/Projects/fastly-log-analytics-load-testing`,
holds the shared harness. Each app plugs in a thin adapter for the parts that
really differ: storage backend, auth, endpoints and cost estimator.

## Use (Drew's decisions)

- **Scope: Drew's laptop, for the next few weeks, to optimize both apps.**
  Build the simplest version that works: no CI integration, no packaging, no
  multi-user features, and no handling for failure modes nobody has seen.
  Harden later only if needed.
- **fdlt ledger drops** (fdlt silently drops ledger entries when its writer
  channel is full; this hasn't been seen at the rates run so far). Detect it
  (ledger lines ≠ `requests_sent`) and record the stage as
  `unverified: fdlt ledger incomplete (N dropped)`, never a pass. The run
  continues, with no retries. If it shows up at high rates, it's written up
  as an fdlt request.

- **One app per run.** Each project uses the tool on its own, during deploys
  and during testing. No run spans both apps, and no report compares them.
- **Two entry points share one stage engine:**
  - `deploy-check`: one short real fdlt stage of about 3 minutes. The default
    is 100 RPS (about 18k requests), and `--rps`/`--duration` override it.
    It replaces each app's current deploy smoke traffic (Drew's decision:
    a fast check, with stats filled in later). It:
    - runs at the service's **current** log period, with no period change;
    - settles and drains, then runs strict and advisory reconcile;
    - takes a real-time cost snapshot, without waiting for Historical stats;
      `backfill-stats` fills those in later;
    - sweeps the key pages cold and warm;
    - writes the history row.

    Seeder samples, long ranges and incident stages are campaign-only. It
    usually finishes in about 6–10 minutes at a 10s or 1m period. Running
    the app's deploy target counts as Drew's go, so there's no extra
    prompt.
  - `campaign`: the full ladder, 100 → 500 → 1,000 → 2,500 → 5,000 RPS,
    10 minutes per stage (`--stage-duration` overrides that), at log periods
    of 10s, then 1m, then 5m.
- **Each project runs whatever is in the local checkout** of
  `~/Projects/fastly-log-analytics-load-testing`. There is no versioning or
  packaging. Each app repo keeps a thin `make` target plus a small config
  file: install name, service ids, URLs, and paths to its own estimator and
  checkout.
- **The tool never deletes data from either app.** There is no delete
  subcommand, and a test enforces this. Logs build up across runs and days,
  so later runs can exercise long ranges such as yesterday, 7 days and 30
  days. Delete Data stays a manual admin-page action inside each app.
- **Keep a history.** Every run's summary goes into a permanent history for its
  app, and reports show trends against earlier runs.
- **Stop at the first critical failure.** Results from later stages would be
  invalid once the bug is fixed. A critical failure is any of these:
  - a missing, duplicate, unknown or blank id;
  - a strict count mismatch on total, status, method or pop;
  - a backlog that doesn't drain within its bound;
  - a log-period change that doesn't take.

  Performance findings never stop a run.
- **Cost:** settled Historical minute stats are the final number. Real-time
  covers the live window and the time while Historical is pending, and gives
  per-second detail. Gaps over 10% are flagged.
- **Live load** (any `deploy-check` or `campaign`) needs Drew's go, like any
  live load today.

## Shared core (target-agnostic)

These parts are built once. Where a part already exists in FLA or in the FLP
design, the base is named.

- **Plan.** Stage and period parsing; the high-rate gate (above 1,000 RPS
  needs `--allow-high-rate`); a 5,000 RPS ceiling; the disk check; and run ids
  of the form `<campaign>-p<period>-s<N>` (FLA's form). The caps come from the
  adapter's live read-back, which is FLP's approach.
- **fdlt.** Binary resolution, the run command (`--ledger-sample 1.0`, always),
  `run.json` parsing, and comparison against `expected_log_lines`, never
  `requests_sent`.
- **Ledger and ids.** The base is FLA `harness_ids.py`:
  - a memory-light ledger index, plus FLA's ledger-completeness check (fail
    when fdlt silently dropped ledger lines);
  - classification of each id as delivered, excused (logged but not
    delivered, with 499s counted separately), duplicate, unknown, blank, or
    late from an earlier stage (FLA's previous-run-late);
  - observed files and both reconcile passes: strict at tolerance 0 over
    total, status and method (FLA), plus pop (FLP's addition, kept strict
    unless the first live stage shows the relay hop logs a different POP),
    plus exact ids; advisory at about 1% over cache state and minute.
- **Settle and drain.** Polls at the app's log period, never faster. Settled
  means freshness has passed the last response and the row count is stable
  twice in a row. A drain gate checks the backlog. Both bounds are
  `3 × period + 300s`.
- **Period management.** Change the period, confirm it took (FLP), and restore
  the original period when the campaign ends (FLA, in `finally`).
- **Hour-close rollups.** FLA's `ladder_close` algorithm, extended to the
  file-shape measurements FLP designed.
- **Fastly stats.** Historical (`by=minute`) with a completeness check for
  every minute, and a `backfill-stats` command. The Real-Time collector
  follows the timestamp chain, polls every 10s, tracks gaps, and records
  limit-exceeded seconds. Precedence follows Drew's decision above.
- **Harness subtraction.** Tag every harness request with the adapter's
  header. Use FLA's per-page tags (`-pgNN-<mode>`) for per-page cost.
  Subtract the harness share from app-measured, Fastly-reported and CDN
  numbers alike.
- **Cost.** Call the app's own estimator through its adapter. Calibration
  re-runs it with default inputs and flags line items more than 25% off.
  Storage is modelled over retention. The output is one matrix per app with
  a row per log period and a column per rate, and each cell gives:
  - pipeline cost per month, with line items;
  - dashboard cost per 1,000 page loads, cold and warm.

  The report says which inputs were measured and which were modelled, and
  notes any costs outside Fastly (such as FLA's server).
- **Page sweep.** One Node and Playwright runner. The base is FLA
  `page_sweep.cjs`, plus FLP's:
  - INP from scripted interactions;
  - a panel classifier;
  - a dashboard-total versus store-count check.

  It records TTFB, FCP, LCP, CLS, INP, TBT and long tasks, cold and warm. It
  also finds redundant and duplicate API calls and duplicate query shapes,
  reads debug-panel telemetry, and fingerprints slow queries. Ranges
  include the run window, 1h, 6h and 24h, plus yesterday, 7d and 30d where
  the accumulated data covers them. The report says which ranges had real
  data.
- **Bottleneck ranking.** Advisory, and never fails a run. The core rules
  are:
  - fdlt saturation or shortfall (FLA);
  - redundant API calls (FLA);
  - web vitals against Google's "good" thresholds (LCP 2.5s, CLS 0.1, INP
    200ms);
  - lag above 3 periods, or no drain;
  - many small files.

  Adapters add their own rules. Every finding names the code area it points
  at.
- **Insights and incident stages** (Drew's earlier choice: load check plus
  injected incidents). Every page sweep checks that every insight loads.
  With `--incident-stages`, each ladder adds one 500 RPS, 5-minute stage per
  fdlt profile: `origin-incident`, `degraded` and `cold`. It then checks
  that the matching insights fire and unrelated ones stay quiet, and
  security insights are load-checked only.
  - FLA has an Insights page today, so it can run this now.
  - FLP reports `not_built` until its Insights page exists.
  - Incident stages are left out of the cost matrix and file-shape reports.
- **Seeder evidence** is kept permanently:
  - bounded raw samples per stage and period, stratified (FLP);
  - the shape profile (FLA and FLP);
  - RUM and ingest-batch profiles (FLA);
  - the fdlt parameters behind each sample.
- **fdlt destination VCL.** One set of snippet bodies:
  - recv source-IP. Its priority is set per adapter so it runs after the
    app's own client-IP handling and before log capture: -50 in FLP, and
    for FLA after checking its recv snippet stack. The CMCD strip applies to
    the cache key only, through `vcl_hash` inputs. `req.url` is never
    rewritten, so both apps still log the full URL and CMCD;
  - miss/pass origin hints (Body-Size and repeated headers, from FLA's
    working bodies);
  - fetch TTLs;
  - deliver X-Cache.

  All are `fdlt-`-prefixed and linted with falco. Each adapter applies them
  or prints the manual steps.
- **Reports and history.**
  - Stage, ladder and campaign reports in Markdown and JSON, each labelled
    live or offline fixture, under `~/.fastly-lt/<app>/<campaign>/`.
  - The history at `~/.fastly-lt/<app>/history.duckdb`: one row per run, plus
    one per stage, per page and per cost line. Each row records the app
    commit and deployed version, the tool's commit, the fdlt version and the
    run kind (`deploy-check` or `campaign`), and failed runs are flagged.
  - Reports compare against the last N runs of the same kind, rate and
    period.
- **Cleanup.** After a stage validates, only local per-request copies are
  deleted: the ledger, the id lists and downloaded store files. Failures keep
  everything.

## Adapter boundary

Each app supplies one Python class implementing the `Target` protocol. The
names below come from the comparison of the two harnesses; the builder may
refine signatures.

| Method | Purpose |
|---|---|
| `name()` | the app label |
| `harness_header()` | header name and accepted-value pattern |
| `id_field_caps()` / `ensure_id_fields(dry_run)` | read back and apply the run and request id log fields |
| `destination_prep(dry_run)` | apply the shared fdlt snippets and origin, or return manual steps |
| `owned_services()` | role → service id, for Fastly stats and real-time |
| `fastly_stats(path, query)` | execute one Fastly stats or real-time request and return its JSON. FLP sends it locally with the read-only token; FLA sends it inside its pod with the app's stored key, as it does today. The core never holds a token, and nothing is printed |
| `current_period()` / `set_period(p)` / `confirm_period(p)` | log-period management |
| `freshness()` / `backlog()` / `run_row_count(run_id)` | settle and drain probes |
| `observed_breakdown(run_id, dims, window)` / `iter_logged_ids(run_id)` | rows for reconcile, and the exact id stream (duplicates kept) |
| `poll_monitor(now)` / `inventory(window)` | lag and resources per poll; raw and processed file shape |
| `list_new_raw(window)` / `fetch_raw(key)` | seeder samples |
| `stage_usage(window)` | app-measured usage, split into harness and non-harness |
| `hour_close(ladder_end)` | wait for rollups and measure them |
| `estimate(measurements, mode)` | call the app's own estimator |
| `pages()` / `browser_auth(context)` / `page_telemetry_script()` | page sweep inputs |
| `extra_bottleneck_rules()` | target-only rules |
| `app_version()` | commit and deployed version for history |

**FLP adapter:**
- rows: DuckDB over downloaded Parquet, with a download-once cache;
- freshness: `/api/log-extents`; backlog: `cron_runs.more_pending` and
  `ingest_files`;
- auth: passkey, then the `flp_session` cookie;
- log period: `step_2_reconcile_vcl`, plus a read-back from the live logging
  endpoint;
- id fields: `log-fields/config` with `byte_limit` 192/252;
- estimator: `estimateMonthly` through Vite SSR;
- telemetry: `window.__flpPageTelemetry` (debug sessions only);
- header: `X-FLP-Harness`;
- extra bottleneck rules: Compute vCPU, backend calls and heap against the
  1000ms, 32-call and 128MB limits; Parquet cache reuse.

**FLA adapter:**
- rows: ClickHouse through `/api/query` with keyset paging;
- freshness: `latest_log_at` and OTel/Prometheus;
- auth: mTLS `admin_urlopen` plus `x-fastly-service-id`, with in-pod helpers
  where FLA needs them;
- log period: SSE logging settings and RUM cron settings;
- estimator: `priceQuantities` through FLA's `cost_model.mjs`. Compute and KV
  lines are reported as quantities until FLA's calculator can price them;
- telemetry: the `fla.debugResponses` envelope;
- header: `X-FLA-Harness`;
- extra bottleneck rules: pod CPU and memory, pool wait, publish.

## What stays in each app

- **FLP:**
  - harness attribution (the Task 2 commits on `feat/fdlt-harness`);
  - the debug-only `window.__flpPageTelemetry` hook (still to be built: FLP
    plan Task 13's web part moves into this project's build order as an
    FLP app change);
  - the `estimateMonthly`/`measureEstimate` exports;
  - Delete Data;
  - a `make` target and config that call the shared tool.

  `deploy_test_all.py` calls `deploy-check` instead of sending its own
  traffic. FLP's old `app/tools/loadtest/` sender and `capacity_campaign.py`
  are removed.
- **FLA:**
  - `X-FLA-Harness` attribution (`root_system.py`) and SSR forwarding
    (`apiSsrProxy.ts`);
  - the `priceQuantities` export;
  - the `DEBUG_RESPONSES` envelope;
  - a `make` target and config.

  `scale_harness.py`, `harness_*.py`, `loadtest/*`, their tests and its
  `delete-data` subcommand are removed after migration.

## Non-goals

- Comparing the two apps in one report.
- Packaging, publishing or versioning the tool.
- Changing fdlt. Needs are written as requests for Drew to pass along, and
  FLA's pending list joins FLP's.
- Rates above 5,000 RPS. The log seeder is a later project, and it uses the
  evidence kept here.

## Anti-goals

- Any delete path in the tool.
- A sampled ledger, or comparing against `requests_sent`.
- Run ids truncated by a log-format cap.
- Polling the app faster than its log period.
- Counting the test's own usage in customer cost.
- Treating offline fixtures as live evidence.

## Build order

1. Create the repo. Port FLA's harness as the base, behind an `fla` adapter.
   Port FLA's tests too and rewrite them to this design's rules (stop on
   critical failure, poll at the log period, pop strict, no delete
   subcommand). They are not kept passing unchanged.
2. Fold in FLP's design decisions where they win: cap read-back, drain gate,
   confirmed period change, INP and panel classifier, minute-completeness
   checks, full-line estimator reuse and subtraction, automated linted
   snippets, careful cleanup, the no-delete test and live labelling. Add
   history.
3. Write the `flp` adapter. Its parts come from FLP Task 1's `loadharness`
   helpers and the FLP plan's Tasks 3–17 designs.
4. Add `deploy-check`, then wire both apps' deploy targets to it.
5. Remove the old harness code from both apps, on each app's own branch.

## Decided by Drew

- One app per run; the tool is used by each project during deploys and
  testing.
- `deploy-check` runs a short fdlt stage of about 3 minutes.
- Each project runs the local checkout.
- The tool never deletes data from either app, and logs build up for
  long-range tests.
- Keep a run history with trends.
- Stop at the first strict failure.
- Settled Historical stats are the final cost number, with real-time live and
  as a fallback.

## My calls (for Drew to check)

- `deploy-check` defaults to 100 RPS.
- Adapters live in the shared repo, and each app keeps only the in-app hooks.
- Run ids are `<campaign>-p<period>-s<N>`. Campaign names are at most 20
  characters (`[a-z0-9-]`), so a page tag such as
  `<campaign>-p300-s5-pg12-cold` stays within FLP's 48-byte harness-label
  limit and FLA's 32-character logged run id. FLP's harness-label doc
  comment gets updated to the new form.
- Each adapter executes its own Fastly stats requests, so FLA needs no new
  token.
- Incident stages and insight checks live in the core, opt-in.
- The original log period is restored at campaign end.
- FLA's ledger-completeness check is adopted.
- Each app keeps its own harness header.
- Real-time polls every 10s.
- Artifacts and history live under `~/.fastly-lt/<app>/`.
- History records the app and tool versions so trends compare like with
  like.
