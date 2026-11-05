# Recommendation: Fastly logs → Parquet in Fastly Object Storage

> **Superseded prototype record.** The current system differs (each window file is written with one PUT inside the tick, with no worker fan-out or multipart; operational logs; retention). The capacity and monthly dollar figures below are benchmark-era
> prototype scenarios, not this deployment's bill; [What it costs per month](#what-it-costs-per-month) prices the current code. Current usage is on the hosted
> [`/usage`](https://flp-dpbk03nn.edgecompute.app/usage) page and the method is in
> [`cost-methodology.md`](cost-methodology.md). Operational-logging overhead
> (flush, compaction, retention) is **estimated** in
> [Spec §7.1](superpowers/specs/2026-10-30-operational-logging-and-telemetry-design.md#71-steady-state-overhead-of-this-system-quiet-install-defaults)
> (about $0.00030/day in FOS operations for a quiet install at list price) until measured live.

**Start here.** This is the plain-language summary. The option comparison is in [`architecture-options.md`](architecture-options.md), the live evidence is in [`research/07-live-verification.md`](research/07-live-verification.md), the capacity tables come from [`experiments/archive/capacity-model/model.py`](../experiments/archive/capacity-model/model.py), and the cost tables come from simulating the current tick code at list prices (see [What it costs per month](#what-it-costs-per-month)).

How much to trust each number:

- **Measured**: we ran it on Fastly and saw it.
- **Derived**: calculated from measured numbers. The arithmetic is in the model script.
- **Unverified**: not tested, or not documented. Treat it as a guess.

## In short

1. **Use Fastly's S3 logging endpoint to land raw logs in an FOS bucket, then convert them to Parquet with a Compute service.** HTTPS log streaming to anything hosted on Fastly is blocked by default, so a Compute service can't receive logs directly. S3 logging into FOS works and loses nothing (measured).
2. **A health check is the clock, and VCL's cache throttles it.** Compute has no scheduler, so a VCL backend health check is the trigger. Every POP that serves the VCL service probes every 30 s. The health check probes the VCL service itself, and VCL rewrites `/_tick` into a 2-minute cache bucket that is shielded at IAD. Only about one probe per bucket reaches the converter; the rest are cache hits. Measured: about 0.8 Compute executions a minute instead of 28–86 when probes went straight to Compute, IPv4 only.
3. **One converter execution handles about 1 million typical log lines** (23 fields) in a 30 s CPU budget. Wider logs cost more: CPU tracks the number of field values (measured).
4. **The real limit is 32 backend requests per execution.** Fastly wrote about 52 raw files per logging period in the benchmarks, more than one execution can read. The prototype grouped inputs across peer workers that uploaded parts of the final Parquet directly (measured). The current `app/` has no fan-out: one tick reads up to 16 raw files (about 12 in practice) and writes each window file with one PUT.
5. **Capacity:** the current code keeps up only to about 0.1 req/s with 1-minute S3 periods ([costs](#how-much-traffic-the-current-code-keeps-up-with)). The prototype, with 8 workers running at once (the most we tested), handled a site logging typical lines at about **125,000 requests/second at 1, 5 or 15-minute windows, and about 60,000 at 60 minutes** (derived). 1,000 req/s uses about 6% of one worker.
6. **At the traffic it handles, cost is about $0.79–1.87 a month, mostly KV and FOS operations.** An idle install costs about $0.79 a month at list price, most of it KV reads and writes from the 2-minute tick. Up to 0.1 req/s, Fastly's raw-file PUTs and raw GETs push it to about $1.87; storage is cents. Above about 0.01 req/s the retention sweep stops getting calls, and above about 0.03 req/s raw delete falls behind, so storage then grows month by month. The scheduled log flush never commits in simulation. Every 100+ req/s cell exceeds current per-tick capacity. See [costs](#what-it-costs-per-month).
7. **Prototype hourly and daily rollups work up to a point.** Archived experiments stitched window files again without decoding: hourly files up to about 10,000 req/s, daily up to about 1,000 req/s. Each rollup level stores another full copy of the Parquet data (measured up to a 17.7 GB file; see [Rolling up](#rolling-up-into-hourly-or-daily-files)). The current app has no request rollup; its daily compaction of operational logs only gets calls below about 0.01 req/s. Production hourly rollup remains a separate stretch gate in PLAN, not an implemented feature inferred from those experiments.

## How it works

The production app uses direct multipart parts, not staged Parquet objects.
The capacity tables below remain benchmark-era models, not a passed production
load test; current results are in the [accuracy report](research/09-accuracy-test.md).

```
 your VCL service ── S3 logging (period = S3 period) ──► raw bucket (FOS, us-east-1)
                                                             │  ~52 gzip files per period
 same VCL service, health check on itself (every 30 s, IPv4)│
   └─ /_tick → /_tick?b=<2-min bucket>, cached, IAD shield ──► converter (Compute, IAD)
                                              1. answer 200 straight away, keep working
                                              2. take the fenced KV lease; resume any saved plan
                                              3. LIST mature raw inputs; persist an immutable plan
                                              4. split the inputs across bounded worker groups
                                                 (peer Compute service, called in parallel)
                                                   each: CDN GET → gunzip → JSON → Arrow → Parquet
                                                         → upload direct multipart parts
                                              5. append the footer, complete and verify every output
                                              6. delete committed raw inputs; release the lease
                                                             ▼
                                   parquet bucket: service_id=<id>/dt=YYYY-MM-DD/hour=HH/<file>.parquet
```

- **Nothing is lost if the converter fails.** Raw files stay in the bucket until their Parquet exists. A crashed run is finished or rolled back by the next tick, using the plan stored in the KV lease (measured: five crash points, no lost or duplicate rows).
- **Late inputs stay immutable.** Each plan writes one output per selected service/window. Later arrivals get a different plan-ID filename in the same event-time partition; readers must include every overlapping plan.
- **Window** = how often the customer gets a Parquet file (1, 5, 15 or 60 minutes). **S3 period** = how often Fastly's logger writes raw files. The S3 period is the window or a divisor of it.
- **Latency:** a file lands about 2–4.5 minutes after its window closes (end of S3 period + upload lag + next tick, up to one 2-minute tick bucket, + conversion; derived from measured parts). A shorter tick bucket cuts the wait but costs more Compute executions.

## How much traffic it handles

### Max requests/second by window and log line size

Sustained requests per second a single site can log, getting **one Parquet file per window**. Main number: 8 workers in parallel (tested). In brackets: up to 28 workers, which fits the request limit but hasn't been tested.

| Log line | Lines per execution | 1 min | 5 min | 15 min | 60 min |
|---|---|---|---|---|---|
| small: 10 fields, ~190 B | 2.8M | 367k (≤1.3M) | 367k (≤745k) | 249k | 62k |
| **typical: 23 fields, ~620 B** (real Fastly VCL logs) | 938k | **125k** (≤436k) | **125k** (≤436k) | **125k** (≤242k) | **60k** |
| long values: 25 fields, ~1.1 KB | 779k | 103k (≤361k) | 103k (≤133k) | 44k | 11k |
| wide: 50 fields, ~1.1 KB | 497k | 66k (≤231k) | 66k (≤118k) | 39k | 9.8k |
| very wide: 100 fields, ~2.2 KB | 240k | 32k (≤112k) | 32k (≤60k) | 20k | 5.0k |

How to read it:

- **Number of fields matters far more than line length.** Converter CPU is about 1 µs per field value (measured, slow end). A 100-field line costs about 10× a 10-field line. Doubling the length of each value adds much less.
- **1–5 minute windows** are limited by how many workers run at once (each handles about 1 execution's worth of lines per minute).
- **15–60 minute windows** are limited by stitching. The final file is built by FOS copying byte ranges server-side at 45–64 MB/s per copy (measured), 8 copies at once (tested). An hour of a 60k req/s site is about 16 GB of Parquet. Above the 60-minute numbers, write 2 or more files per hour instead of one.
- "Lines per execution" uses a 30 s CPU budget, half the ~60 s limit, because identical runs vary up to 2×.

### Worked example: 1,000 requests/second, typical lines

| Window | Lines per window | Parquet file per window | Workers per period | Verdict |
|---|---|---|---|---|
| 1 min | 60,000 | 4.4 MB | 2 | Fine. Uses about 6% of one worker |
| 5 min | 300,000 | 22 MB | 2 | Fine |
| 15 min | 900,000 | 67 MB | 3 | Fine |
| 60 min | 3.6M | 266 MB | 4 | Fine |

Two workers is the minimum for any busy period: one execution can read about 28 raw files, and Fastly writes about 52 per period.

### Parquet file size per window (typical lines, about 74 B per line)

| req/s | 1 min | 5 min | 15 min | 60 min |
|---|---|---|---|---|
| 100 | 0.4 MB | 2.2 MB | 6.7 MB | 27 MB |
| 1,000 | 4.4 MB | 22 MB | 67 MB | 266 MB |
| 10,000 | 44 MB | 222 MB | 666 MB | 2.7 GB |
| 100,000 | 444 MB | 2.2 GB | 6.7 GB | 27 GB (too big for one file; split) |

## What it costs per month

These are estimates, not a bill. They price the code as it runs on `main` (2026-10-30): one Compute execution per 2-minute tick reads raw files, converts them inside that execution and writes each window file with **one PUT**. There is no worker fan-out, multipart upload, staged copy or padding. Measured usage for an install is on `/usage`; rules and rates are in the [cost methodology](cost-methodology.md). The older prototype estimates are kept in the [appendix](#historical-direct-parts-estimate-prototype).

The operation counts come from running the real tick (`admin::record_tick`) against the native test runtime with an operation counter, for 3 simulated days per scenario, counting the last day and multiplying by 30. Fastly's raw files were simulated from the measured file curve ([below](#assumptions)). The harness was temporary and isn't committed. Dollars are list prices.

### 1 s clock executions

The tables below price the 2-minute tick that ran when they were simulated; the 2-minute bucket in [In short](#in-short) item 2 and the latency note under [How it works](#how-it-works) are historical too. The clock now runs a 1 s dispatcher (`app/src/admin/schedule.rs`, [clock](crons/clock.md)): VCL caches `/_tick` and `/_flush` for 1 s at the IAD shield, so about one probe a second of each reaches Compute.

- `/_tick` alone is 86,400 executions a day, about **2.59M a month** ($1.30 at $0.50 per million). That matches Drew's estimate of about 2.6M.
- `/_flush` probed every second doubles it to about **5.18M a month** ($2.59).
- An idle probe makes 0 backend calls and 0 KV writes, and 1 Core Cache operation, so the idle KV figure below falls; the cost moves to Compute requests. Idle vCPU is reported per run as `idle_vcpu_ms` on `cron_runs` and is not yet measured live.
- Decided (Drew, 2026-10-31): keep `/_flush` at 1 s. A 10 s bucket (about 2.85M in total, $1.43) was considered; the extra ~$1.16 a month isn't worth the added flush delay.

### What one tick does

All calls share one budget of 32 backend requests (KV, FOS, CDN and secret reads; [`transport.rs:57-74`](../app/src/admin/transport.rs)), plus 8 for the tick's own log writes. Stages run in this order and each runs only if enough calls are left.

| Step | Operations per tick | Billed as | Code |
|---|---|---|---|
| Clock | 1 Compute request; a second probe in the same bucket returns `busy` after reading state and writing its log rows | Compute request | [`admin/mod.rs:1391`](../app/src/admin/mod.rs), `:1670` |
| Load state | `cron:state`, `admin:install`, `svc:<id>`, operational settings, flush cursor (overdue check): 5 KV reads. FOS keys: 2 secret reads (budget only) | KV Class B | `admin/mod.rs:1611`, `:1428-1447`, `:1449`, `:1666`, `:1675`; [`telemetry/api.rs:354`](../app/src/telemetry/api.rs) |
| Ingest: plan | plan and watermark: 2 KV reads; 1 raw LIST page (200 keys) | KV B; FOS Class A | [`ingest/mod.rs:288-289`](../app/src/ingest/mod.rs), `:311` |
| Ingest: read | up to 16 raw GETs through the `/fos` CDN path, while enough budget remains and within the 40 s wall guard. A CDN miss is one FOS Class B; a hit is free. Objects over 8 MiB fail | FOS Class B | `ingest/mod.rs:39`, `:335-355`; [`storage.rs:12`](../app/src/ingest/storage.rs), `:266` |
| Ingest: write | plan CAS; **one Parquet PUT per window present in this tick's batch**; delete-queue record; watermark CAS | KV A ×3; FOS Class A per file | `ingest/mod.rs:368`, `:382-407`, `:488-508` |
| Raw delete | list delete queues (≤ 20); per queue 1 KV read, 1 `DeleteObjects` (≤ 1,000 keys) and a KV delete | KV list (A, **unverified**), KV B, FOS Class A; KV deletes free | [`retention.rs:221-262`](../app/src/ingest/retention.rs); `storage.rs:207` |
| Tick logs | one Core Cache append per tick (`cron_ticks`); one KV outbox write per `cron_runs` row; `cron:state` CAS | KV Class A; Core Cache not priced | [`telemetry/mod.rs:373`](../app/src/telemetry/mod.rs), `:394-396`; `admin/mod.rs:1480-1499` |
| Log flush | cursor and manifest reads, outbox list, up to 480 Core Cache lookups. Committing a flush needs 19 free calls | KV B, KV A | [`flush.rs:37-39`](../app/src/telemetry/flush.rs), `:1004-1085`, `:1115` |
| Maintenance | compaction on even buckets, retention sweep every 6 h, history sweep and storage reconcile on the ticks between, each only if its calls are left | FOS A (LIST, PUT, `DeleteObjects`), FOS B, KV | [`admin/logs.rs:446-530`](../app/src/admin/logs.rs) |

Whether `DeleteObjects` and LIST count as Class A follows the [methodology](cost-methodology.md); KV list billing is **unverified** and counted as Class A.

### How much traffic the current code keeps up with

The tick reads at most about 12 raw files: the 32-call budget runs out before the 16-object cap. Fastly writes about 52 files per S3 period under load ([07 §3.2, §6.4](research/07-live-verification.md)), so with 1-minute S3 periods the tick falls behind at about **0.15 req/s**. Simulated backlog at 2-minute windows:

| req/s | Raw files a day | Raw files read a day | Backlog after day 2 → 3 | Stages that still run |
|---|---|---|---|---|
| 0 (idle) | 0 | 0 | 0 | all |
| 0.001 | 87 | 86 | 0 → 1 | all |
| 0.005 | 432 | 432 | 1 → 1 | all |
| 0.0075 | 648 | 648 | 1 → 1 | all; retention sweep just fits |
| 0.01 | 864 | 864 | 1 → 1 | ingest, raw delete; **no retention or compaction** |
| 0.03 | 2,592 | 2,592 | 4 → 4 | ingest, raw delete |
| 0.04 | 3,456 | 3,456 | 5 → 5 | ingest; raw delete runs on 144 of 720 ticks and falls behind |
| 0.1 | 8,640 | 8,640 | 12 → 12 | ingest only |
| 0.15 | 11,520 | 8,861 | 5,326 → 7,985 | ingest only; **falls behind** |
| 0.2 | 15,840 | 9,051 | 6,792 → 13,581 | ingest only; **falls behind** |

Four limits follow from the same budget:

- **Ingest** keeps up only to about 0.1 req/s.
- **Raw delete** keeps up only to about 0.03 req/s. Above that, delete queues pile up and raw files are never removed.
- **The retention sweep and compaction** only run when ingest leaves calls over, which stops at about 0.01 req/s. Above that, Parquet and operational files don't expire.
- **The scheduled log flush never commits.** After the earlier stages an idle tick has 16 calls left at the flush check and a commit needs 19, so every due flush is deferred. Operational rows stay in Core Cache (lost after 24 h) and in the KV outbox, which grows by 720–1,440 keys a day. This is from the simulation, not observed live; a manual flush from the admin API still works.

### Totals per month, typical lines

Every cell in the usual grid exceeds current per-tick capacity, so it isn't priced:

| req/s | 1 min | 2 min | 5 min | 15 min | 60 min |
|---|---|---|---|---|---|
| 100 | exceeds current per-tick capacity | exceeds | exceeds | exceeds | exceeds |
| 1,000 | exceeds current per-tick capacity | exceeds | exceeds | exceeds | exceeds |
| 10,000 | exceeds current per-tick capacity | exceeds | exceeds | exceeds | exceeds |
| 100,000 | exceeds current per-tick capacity | exceeds | exceeds | exceeds | exceeds |

At 100 req/s the tick reads about 9,300 of the 74,800 raw files written each day (12%); at 10,000 req/s each raw file is about 1 MB and the 8 MiB batch cap allows about 8 a tick; at 100,000 req/s each file is over the 8 MiB read limit and none can be read. The window setting (1–3,600 s, [`ingest/mod.rs:86-93`](../app/src/ingest/mod.rs)) doesn't change this, because the cap is on raw files read per tick.

The rates the code supports, at 2-minute windows (the provisioned default). FOS Class A includes Fastly's own raw-file PUTs, one per raw file, because they are billed to the bucket owner:

| req/s | FOS Class A | FOS Class B | KV Class A | KV Class B | FOS ops | KV | Compute | Raw + Parquet storage | Total |
|---|---|---|---|---|---|---|---|---|---|
| 0 (idle) | 24k | 0 | 87k | 271k | $0.06 | $0.72 | $0.011 | $0.00 | **$0.79** |
| 0.001 | 32k | 2.6k | 98k | 267k | $0.08 | $0.78 | $0.011 | $0.001 | **$0.87** |
| 0.005 | 63k | 13k | 139k | 251k | $0.16 | $1.04 | $0.011 | $0.005 | **$1.22** |
| 0.01 | 95k | 26k | 173k | 233k | $0.25 | $1.25 | $0.011 | $0.011 ‡ | **$1.52** |
| 0.03 | 164k | 78k | 173k | 216k | $0.44 | $1.24 | $0.011 | $0.018 ‡ | **$1.71** |
| 0.05 | 194k | 130k | 151k | 194k | $0.54 | $1.09 | $0.011 | $0.018 ‡ § | **$1.66** |
| 0.1 | 324k | 259k | 130k | 151k | $0.91 | $0.93 | $0.011 | $0.019 ‡ § | **$1.87** |

‡ Retention doesn't run, so Parquet isn't deleted: add the storage figure again for every month the install runs. § Raw delete falls behind, so raw files stay too; that adds about $0.0002–0.0005 a month, every month.

Window size changes only the Parquet PUT count. At 0.1 req/s a tick writes one file per window in its batch, and longer windows aren't merged across ticks:

| Window | 1 min | 2 min | 5 min | 15 min | 60 min |
|---|---|---|---|---|---|
| Parquet PUTs a day | 1,440 | 1,440 | 1,008 | 816 | 744 |
| Total per month | $1.87 | $1.87 | $1.83 | $1.81 | $1.81 |

### What drives these numbers

- **KV is most of the bill below about 0.05 req/s.** An idle tick makes about 5 KV writes or lists and 13 reads, so about 87k Class A and 271k Class B a month ($0.72) before any traffic. Ingest adds 3 writes per tick with files. KV list is counted as Class A (**unverified**); at list rates it is about $0.28 of the idle figure.
- **Compute requests are fixed** at 21,600 a month ($0.011) from the 2-minute tick. The prototype saw about 0.8 executions a minute (about 35k a month, $0.017) because of extra cache misses ([07 §7.13](research/07-live-verification.md)); extras return `busy` but still read and write state. There is no worker fan-out.
- **vCPU is small and mostly unmeasured.** Conversion is about 32 ms per 1,000 typical lines (measured on the prototype converter, [07 §7.8](research/07-live-verification.md)), so under a cent at these rates. Fixed per-tick vCPU for `app/` isn't measured ([compute-limits audit](compute-limits-audit.md)); every 10 ms per tick adds $0.011 a month.
- **FOS Class A** is Fastly's raw-file PUTs (about one per line at these rates: 259k a month, $0.65, at 0.1 req/s), the raw LIST every tick, one Parquet PUT per window per tick, and `DeleteObjects` per delete queue.
- **FOS Class B** is one raw GET per file through the `/fos` CDN path. Every read is assumed to miss.
- **Storage is cents.** Raw is billed for 30 days at about 88 B per line; Parquet per window file is about 20.7 KB of fixed overhead (measured in the simulation for one-row files) plus about 74 B per line.

### Operational logging overhead

The spec's quiet-install estimate ([§7.1](superpowers/specs/2026-10-30-operational-logging-and-telemetry-design.md#71-steady-state-overhead-of-this-system-quiet-install-defaults)) is about 93 Class A and 168 Class B FOS operations a day, about **$0.00030 a day** at list (93 × $0.0000025 + 168 × $0.0000004). In the simulation the scheduled flush never commits (see above), so those flush and compaction operations don't happen today. What does happen is already in the totals: the `logq:` outbox writes (about 22k–43k KV Class A a month, $0.14–0.28), the flush cursor reads and outbox list each tick, and, at idle only, retention and compaction LISTs over `processed/ops/` (about 12 a day per kind). Core Cache lookups (up to 480 a tick) and appends aren't priced.

### Assumptions

- List prices from the [pricing page](https://www.fastly.com/pricing), as in the [methodology](cost-methodology.md): FOS Class A $2.50 per million, Class B $0.40 per million, storage $0.020 per GB-month with a 30-day minimum; Compute $0.50 per million requests and $0.05 per million vCPU-ms; KV Class A $0.65 per 100k and Class B $0.55 per million. Free tiers, graduated tiers and contracts are ignored; they would cover all of this.
- Typical line ~620 B, 23 fields: 88 B gzip and 74 B Parquet per line in large files ([07 §7.8](research/07-live-verification.md)). Small raw files compress worse, so raw storage here is a floor; it stays under a cent.
- 1-minute S3 periods (the logger default). Raw files per period follow 52 × (1 − e^(−lines/52)) ([07 §3.2, §6.4](research/07-live-verification.md)). Longer periods mean fewer files, but files that land after their minute has settled are skipped by the watermark ([`ingest/mod.rs:12-13`](../app/src/ingest/mod.rs), `:38`), so they aren't priced as supported.
- One install, one service. Query, API, page and CDN delivery traffic is excluded.
- `DeleteObjects` is one Class A operation per call; per key is **unverified**.

## Rolling up into hourly or daily files

**Current status:** the app has no request rollup. Request and RUM window files are never merged on the server (master-plan D1); readers combine them in the browser. The only server compaction is `log_compact`, for operational logs: once a UTC day has been closed for 2 hours, it merges that day's window files for each kind into chunks of at most 24 files (at most 2 MiB of compressed input and 10,000 rows per chunk), then deletes the sources with `DeleteObjects` ([logging spec §4](superpowers/specs/2026-10-30-operational-logging-and-telemetry-design.md)). It cuts files to LIST and open, not storage; see [Operational logging overhead](#operational-logging-overhead). The minute-summary rollup benchmark failed its larger case and stays out of the query path ([10](research/10-rollup-costs.md)).

### Prototype stitch experiments (archived)

Customers who want fewer, bigger files can have a second pass combine the window files, for example 12 five-minute files into one hourly file, then 24 hourly files into one daily file. For inputs of 5 MiB or more this is a server-side stitch: FOS copies the bytes with `UploadPartCopy`, and Compute only writes a new footer. Nothing is decoded. Measured from Compute up to a 30.6 GB, 4,004-row-group file built in 19 s ([07 §7.10](research/07-live-verification.md), [RESULTS-rollup](../experiments/archive/stitch/RESULTS-rollup.md)).

**Short answer:** the files don't get too big to copy. Copying parallelizes, and one multipart upload can be filled from several executions. What runs out is the **footer**: Parquet's 32,767-row-group cap, and how slow a big footer is for readers. Row groups stay about 4–5 MB (Compute's heap limits how big a worker can make them), so file size turns directly into row-group count.

Typical lines, from 5-minute windows (derived from [`model.py`](../experiments/archive/capacity-model/model.py)):

| req/s | 5-min file | Hourly file | Daily file |
|---|---|---|---|
| 10 | 222 KB | ✅ 2.7 MB, re-encode | ✅ 64 MB, re-encode |
| 100 | 2.2 MB | ✅ 27 MB, re-encode | ✅ 639 MB, 144 row groups, copy |
| 1,000 | 22 MB | ✅ 266 MB, 72 row groups, copy | ✅ 6.4 GB, 1,728 row groups, 3.7 MB footer |
| 10,000 | 222 MB | ✅ 2.7 GB, 576 row groups, copy | ⚠️ 64 GB, 13,824 row groups, copy; a 30 MB footer is slow to open |
| 50,000 | 1.1 GB | ✅ 13 GB, 2,688 row groups, copy | ❌ 64,512 row groups, over Parquet's cap |
| 100,000 | 2.2 GB | ✅ 27 GB, 5,376 row groups, copy | ❌ 129,024 row groups |

What sets each limit:

- **Footer memory is no longer a limit.** The first planner ran out of Compute's heap at 4,004 row groups. The streaming footer writer keeps raw input footers and writes the merged footer as it goes. In Compute it built a 32,760-row-group footer in 98 MiB and 4.3 s of CPU, and a real 4,004-row-group stitch validated (measured, [07 §7.10](research/07-live-verification.md)).
- **Parquet's 32,767 row groups per file.** The row-group ordinal is a 16-bit number. With 5 MB row groups that's about 160 GB per file. Above about 20k req/s, a daily file has to be split (for example, 4 six-hour files) or its row groups made bigger by re-encoding.
- **Footer size, about 2.2 KB per row group** (23 fields). A reader must fetch and parse it before reading any data. A 5 MB footer took 3.2 s to open from a laptop. A 30 MB daily footer at 10k req/s would be slow for every query.
- **Small files must be re-encoded.** FOS can't copy parts under 5 MiB, so below about 250 req/s the rollup decodes and rewrites. That's cheap: 2.7–5.6 ms of CPU per 1,000 rows (measured), about 2 s of CPU for an hour at 100 req/s and 5 s for a day at 10 req/s. It also fixes the tiny row groups that low-traffic window files have. One execution can read at most about 28 inputs (the 32-request limit), which is enough for 12 or 24.
- **Copy time isn't a hard limit.** Each copy runs at about 50 MB/s. Several Compute executions can fill one upload ID: 0.96 GB/s with 4 copy workers and 1.74 GB/s with 6 (measured). The 10k req/s daily file (64 GB) would take about 40 s with 6 workers (derived).
- **Backend timeouts must outlast the biggest copy part.** `UploadPartCopy` answers only when the copy finishes. With the default 15 s first-byte timeout, 512 MB parts failed. Raise `first_byte_timeout` and `between_bytes_timeout` on the FOS and worker backends to 60 s, and keep parts at or below about 256 MB (measured).

What it costs:

- **Every rollup level stores another full copy of the Parquet data for at least 30 days**, even if the smaller files are deleted right away. That's about $0.38 a month per level at 100 req/s, $3.84 at 1k, $38 at 10k and $384 at 100k (typical lines, unpadded; the direct-parts estimator gives $38.36 and $383.62 for the 10k and 100k Parquet). The prototype's padded window files cost more than this while kept. Operations and Compute for rollups are negligible next to that: one stitch per hour is about 25 FOS operations.
- Cheaper alternative: **pick the bigger window up front** (for example, a 60-minute window instead of 5-minute windows plus an hourly rollup). That avoids the extra copy but gives up the fresher 5-minute files.

If request rollups are built: offer hourly rollups up to 100k req/s, and daily up to about 10k req/s. Above about 20k req/s, use 6-hour or hourly files as the largest unit, since a daily file passes Parquet's cap. Whether 30 MB+ footers are acceptable depends on the customer's query engine (**unverified**).

## Choosing the settings for a customer

| Customer wants | Set the S3 period to | Converter does | Notes |
|---|---|---|---|
| 1-minute files | 1 min | 2+ workers per minute, stitch their outputs | The most raw files (2.2M PUTs a month) and KV writes |
| 5-minute files | 5 min | 2+ workers per 5 min, stitch | Best default: 4× fewer raw files than 1 minute, same latency relative to the window |
| 15-minute files | 15 min (5 min above about 54k req/s) | Workers per period, stitch at the window | |
| 60-minute files | 60 min up to about 13k req/s, else 5–15 min | Workers per period, stitch at the hour (two levels when busy) | Above about 60k req/s, write several files per hour |

Each raw file must fit in one execution (about 940k typical lines), which is what caps the S3 period for busy sites.

## What was tested live

| What | Result | Where |
|---|---|---|
| HTTPS log streaming to a Fastly-hosted endpoint | **Blocked** (`log destination address is blocked`) | [07 §3.1](research/07-live-verification.md) |
| S3 logging → FOS | Works, nothing lost or duplicated; about 52 files per period | [07 §3.2, §6.4, §7.3](research/07-live-verification.md) |
| Health check as a clock | Works. Every POP that serves the VCL service probes, and keeps probing after traffic stops. Probing the service itself and caching `/_tick` in 2-minute buckets at the IAD shield cut Compute executions from 28–86 to about 0.8 a minute, IPv4 only | [07 §6.3, §7.2, §7.13](research/07-live-verification.md) |
| Answer first, keep working | Work after `send_to_client` keeps the full ~60 s CPU | [07 §6.1](research/07-live-verification.md) |
| Conversion speed and memory | Up to 2.08M lines per execution; CPU about 1 µs per field value; heap 51–113 MiB of 128 | [07 §6.5, §7.8](research/07-live-verification.md) |
| Fan-out to workers | 20 workers dispatched in 0.5 s; 8 heavy ones in parallel without throttling | [07 §6.6](research/07-live-verification.md) |
| Backend-request limit | **32 per execution**; KV calls don't count; going over returns an error | [07 §7.6](research/07-live-verification.md) |
| Full cycle with lease, crashes and concurrency | Exactly-once output over 27,447 real lines | [07 §7.7](research/07-live-verification.md) |
| Stitching Parquet files server-side | Works on FOS; 8 files → 62.8 MB in 2.4–2.9 s, byte-identical | [07 §7.4](research/07-live-verification.md) |
| Rollups of stitched files | 3 levels up to 17.7 GB / 2,366 row groups, validated; Parquet re-encode in Compute at 2.7–5.6 ms CPU per 1k rows | [07 §7.9](research/07-live-verification.md) |
| Workers write parts of the final file directly | No staged files. Crash recovery passed at 6 crash points; FOS rejected a stale part with `InvalidPart` and the next run redid it. 5 concurrent calls gave 1 `done` and 4 `busy`. Zero-padded parts read fine in pyarrow and DuckDB | [07 §7.11](research/07-live-verification.md) |
| Reads through the CDN, shielded at IAD | Raw and Parquet GETs from the converter and from people go through the VCL service, which signs SigV4 itself. Proven by reading a deleted raw file from the IAD cache | [07 §7.12](research/07-live-verification.md) |
| Stitch from Compute with a streaming footer | 30.6 GB / 4,004 row groups in 19 s with 6 copy workers sharing one upload (1.74 GB/s), validated; footer built to 32,760 row groups in 98 MiB | [07 §7.10](research/07-live-verification.md) |
| Hosted production codebase | Same Wasm on `flp` and `flp-worker`, now v18; all four pages pass unmocked checks in both themes. Cloud Files has live raw/Parquet previews; costs use the current three-service scope; dashboard panels expose actual APIs and timings | [PLAN](PLAN.md), [runbook](runbook.md) |
| Autonomous production clock | Historical raw backlog drained without skipped inputs. Current paired v18 passes 61 consecutive full-inventory zero-backlog observations over 3,651.771348 seconds, with fresh error-free status, zero pending/MPUs and exact unchanged build identities; no manual tick or test load. Prior v17 proof remains retained. This is sampled evidence, not continuous observation; higher-tier accuracy remains blocked | [Accuracy evidence](research/09-accuracy-test.md#autonomous-operation-evidence) |
| Dashboard fanout | All 16 first/repeat historical one-hour panel observations succeeded: 328 rows, 14 files, 15 executions and 54 calls. All 32 six-/24-hour observations rejected the file cap without partial data; controlled cold-cache results remain blocked | [Query evidence](research/08-dashboard-query-path.md) |
| Production accuracy | Corrected 150-request canary, full 30,000-request T1 and renewed HTTP/2 canary passed. All three T2 attempts failed at sender capacity, including HTTP/2 with connection reuse; no further load or T3 is authorized. Failed-client rows are accounted separately, not fully field-verified | [Accuracy evidence](research/09-accuracy-test.md) |
| Project-scoped cost data | Live stats and complete bucket inventories work. Unattributable billing stays omitted; nonempty service billing and exact billed charges are not claimed | [Cost methodology](cost-methodology.md) |

## Risks and open questions

| Risk | Why it matters | What to do |
|---|---|---|
| The 32-request limit changed under us (167 → 32 between sessions) | Everything is sized on it | Count requests in code. Ask Fastly whether it varies by account |
| More than 8 parallel workers untested | Caps the main capacity numbers | Test 16 and 28 |
| Raw files per period at high traffic | More files than 52 means more workers | Measure at 10k+ req/s |
| FOS public-endpoint cap (~150 Mbps, 100 req/s per bucket) | Typical lines at 125k req/s move about 160 Mbps through Compute | Not enforced in our tests (270 Mbps for 35 s; 207 PUT/s for 21 s). Sustained behaviour **unverified** |
| Per-tick capacity of the current code | One tick reads about 12 raw files, so ingest falls behind at about 0.15 req/s; raw delete stops keeping up at about 0.04 req/s and retention and compaction stop at about 0.01 req/s, so storage grows month by month | Fan out or split stages across ticks before any real traffic. Longer S3 periods cut files but risk skipped late files under the watermark (**untested**) |
| Scheduled log flush never commits (simulation) | A commit needs 19 free calls; an idle tick has 16 left at the check. Operational rows stay in Core Cache (lost after 24 h) and the KV outbox grows by 720–1,440 keys a day | Confirm on a live install with `/usage` and the outbox depth; give the flush its own tick or budget |
| KV operations from the tick | About 87k–173k Class A and 151k–271k Class B a month, most of the bill; near or above the 100k Class A the pricing page includes (older docs say 250k; see [02](research/02-kv-store-staging.md)). KV list billing is **unverified** | Pay for them ($0.72–1.25 a month), or cut per-tick reads and writes |
| Wide logs (50+ fields) near the 128 MB heap | Out-of-memory kills the run | Input concurrency 1 or smaller row groups for wide schemas |
| Converter falls behind | Raw backlog grows; a raw lifecycle rule could delete unconverted data | Alert on backlog; set any raw expiry well above the worst backlog |
| Credentials | The logging endpoint's FOS key lives in the logging config | Use a key scoped to the raw bucket and rotate it. The converter's keys go in a Secret Store. Never log `SendError` with `{:?}`; it prints the access key ID |
| Tick cache misses | An extra miss per bucket means an extra converter run (the KV gate and lease absorb it). A gated probe can delay a run by one bucket (2 min) | Observed over minutes, not hours. Measure for a day. If self-probing proves flaky, use a second small VCL service that health-checks the first |
| Backend timeouts on long copies | `UploadPartCopy` and copy workers answer only when done; 15 s defaults failed 512 MB parts | 60 s timeouts on the FOS and worker backends; parts ≤256 MB |
| Large footers | 30–70 MB footers (daily files at 10k+ req/s) slow every query's first read | Measure with real query engines; cap files at a few thousand row groups if needed |

## Next steps

1. **Finish the production proof.** The closed-window converter, durable plan and hosted apps are implemented, and the historical raw backlog drained. Pass the corrected canary and approved serial tiers, then measure sustained autonomous operation before calling it ready; follow [PLAN](PLAN.md).
2. **Measure the real costs.** The cost tables simulate the current tick code at list prices. Replace them with measured `/usage` figures (FOS and KV operations, vCPU, storage ledger) from a live install, measure fixed vCPU per tick, and confirm whether KV list is billed. Reprice when the write path changes.
3. **Scale test.** 16–28 parallel workers, and S3 logging at 10k+ req/s, to check the 52-file assumption and sustained FOS throughput.
4. **Tick over a day.** Watch the VCL-cached tick for misses per bucket and gaps between runs over 24 h, with traffic from many POPs.
5. **Confirm with Fastly:** the backend-request limit, and whether `DeleteObjects` is billed per call or per key.
6. **Footer read cost.** Measure how long DuckDB, Athena-style engines and pyarrow take to open 5–70 MB footers, to set the largest rollup offered.

## Appendix

### Why not the other write models

| Option | Why not |
|---|---|
| HTTPS logs straight to Compute, one Parquet per POST | Blocked unless the logging team allowlists each source service. Best option if they agree |
| Buffer in memory across reusable sessions | No affinity, no shutdown hook; a recycled instance loses its buffer |
| Stage in KV Store, then compact | Needs HTTPS ingest (blocked) and one KV write per POST |
| Keep a multipart upload open across requests | Needs HTTPS ingest; one bad part fails the whole file late |

Details: [architecture-options.md](architecture-options.md).

### How crash safety works

- Each batch's output key is a hash of its sorted input keys, so redoing a batch overwrites the same object.
- Raw inputs are deleted only after every output exists.
- The KV lease is taken with `Add` (or a generation match when it has expired); KV reads right after a write can be stale, so the lease never relies on read-back.
- A converter that takes over an expired lease finds the old plan in it and either finishes the delete, redoes the batch, or rolls back the outputs if a redo won't fit in 32 requests.
- Every request from Compute is sent with `pass`; otherwise Fastly's cache can serve a stale LIST. Object GETs go through the VCL service, which caches them; that's safe because raw keys are never rewritten, and an output is read only after its commit.

### Fixed decisions in `CLAUDE.md` to revisit

- **HTTPS ingest to a Compute receiver** is blocked by default. It needs an operator allowlist entry (`AllowListFastlyIPServices`). That comes from internal source; don't publish [05 §2.9](research/05-https-log-streaming.md).
- **The auth line is wrong for this design.** The challenge file contains `*` or `hex(sha256(service_id))`, not a Fastly-Customer-ID, and with S3 logging there's no ingest request to authenticate. Only the tick needs the shared secret.
- **`%{req.service_id}V` works only for VCL sources.** Here `service_id` is also in the raw file path.

### Historical: direct-parts estimate (prototype)

**Prototype evidence, not the current code.** These tables came from running the archived direct-parts estimator ([`model.ts`](../experiments/archive/prototype-app/web/src/routes/calculator/model.ts)) on 2026-10-30, with a 2-minute tick and S3 period = window. Peer workers uploaded parts of the final Parquet directly ([07 §7.11](research/07-live-verification.md)); each worker's trailing part was padded to 5 MiB, which is most of the 1-minute storage. Cells marked † needed more than 8 workers or 30 coordinator calls per window. The current `app/` writes each window file with one PUT and has none of this.

#### Operations, typical lines

"FOS Class A" includes Fastly's raw-file writes (in brackets), because those are billed to the bucket owner too. "Final file" includes padding.

| req/s | Window | Workers per window | Final file | Compute requests | vCPU hours | FOS Class A (raw PUTs) | FOS Class B | KV writes / reads |
|---|---|---|---|---|---|---|---|---|
| 100 | 1 min | 3 | 15.7 MB | 281k | 2.3 | 2.57M (2.25M) | 2.29M | 130k / 108k |
| 100 | 5 min | 3 | 15.7 MB | 73k | 2.3 | 531k (449k) | 458k | 26k / 39k |
| 100 | 15 min | 3 | 15.7 MB | 39k | 2.3 | 192k (150k) | 153k | 8.6k / 27k |
| 100 | 60 min | 6 | 31.5 MB | 30k | 2.3 | 66k (37k) | 38k | 2.2k / 23k |
| 1,000 | 1 min | 3 | 15.7 MB | 281k | 23 | 2.57M (2.25M) | 2.29M | 130k / 108k |
| 1,000 | 5 min | 5 | 26.2 MB | 108k | 23 | 549k (449k) | 458k | 26k / 39k |
| 1,000 | 15 min † | 13 | 68.2 MB | 96k | 23 | 220k (150k) | 153k | 8.6k / 27k |
| 1,000 | 60 min † | 51 | 267 MB | 95k | 23 | 99k (37k) | 38k | 2.2k / 23k |
| 10,000 | 1 min † | 9 | 47.2 MB | 799k | 230 | 2.83M (2.25M) | 2.29M | 130k / 108k |
| 10,000 | 5 min † | 42 | 222 MB | 747k | 230 | 868k (449k) | 458k | 26k / 39k |
| 10,000 | 15 min † | 126 | 666 MB | 747k | 230 | 546k (150k) | 153k | 8.6k / 27k |
| 10,000 | 60 min † | 504 | 2,664 MB | 747k | 230 | 425k (37k) | 38k | 2.2k / 23k |
| 100,000 | 1 min † | 84 | 444 MB | 7.28M | 2,304 | 6.07M (2.25M) | 2.29M | 130k / 108k |
| 100,000 | 5 min † | 420 | 2,220 MB | 7.28M | 2,304 | 4.13M (449k) | 458k | 26k / 39k |
| 100,000 | 15 min † | 1,259 | 6,660 MB | 7.27M | 2,304 | 3.81M (150k) | 153k | 8.6k / 27k |
| 100,000 | 60 min † | 5,036 | 26,640 MB | 7.27M | 2,304 | 3.69M (37k) | 38k | 2.2k / 23k |

#### Total per month, typical lines

Raw files are deleted once converted, but FOS bills every object for at least 30 days, so each month you pay one month for every raw byte written. Parquet you keep adds its "1 month kept" cost again for each month it stays. One month of Parquet is included in the total.

| req/s | Window | Raw billed (GB-month) | Parquet created (padding) | Compute | KV | FOS operations | Raw storage | Parquet storage, 1 month kept | Total |
|---|---|---|---|---|---|---|---|---|---|
| 100 | 1 min | 23 GB | 680 GB (660 GB) | $0.56 | $0.90 | $7.34 | $0.46 | $13.59 | $22.84 |
| 100 | 5 min | 23 GB | 136 GB (117 GB) | $0.45 | $0.19 | $1.51 | $0.46 | $2.72 | $5.33 |
| 100 | 15 min | 23 GB | 45 GB (26 GB) | $0.43 | $0.07 | $0.54 | $0.46 | $0.91 | $2.41 |
| 100 | 60 min | 23 GB | 23 GB (3.5 GB) | $0.43 | $0.03 | $0.18 | $0.46 | $0.45 | $1.55 |
| 1,000 | 1 min | 228 GB | 680 GB (488 GB) | $4.29 | $0.90 | $7.34 | $4.56 | $13.59 | $30.68 |
| 1,000 | 5 min | 228 GB | 227 GB (35 GB) | $4.20 | $0.19 | $1.55 | $4.56 | $4.53 | $15.04 |
| 1,000 | 15 min † | 228 GB | 196 GB (4.5 GB) | $4.20 | $0.07 | $0.61 | $4.56 | $3.93 | $13.37 |
| 1,000 | 60 min † | 228 GB | 193 GB (0.7 GB) | $4.19 | $0.03 | $0.26 | $4.56 | $3.85 | $12.90 |
| 10,000 | 1 min † | 2.3 TB | 2.0 TB (120 GB) | $42 | $0.90 | $7.99 | $46 | $41 | $137 |
| 10,000 | 5 min † | 2.3 TB | 1.9 TB (0) | $42 | $0.19 | $2.35 | $46 | $38 | $128 |
| 10,000 | 15 min † | 2.3 TB | 1.9 TB (0) | $42 | $0.07 | $1.43 | $46 | $38 | $127 |
| 10,000 | 60 min † | 2.3 TB | 1.9 TB (0) | $42 | $0.03 | $1.08 | $46 | $38 | $127 |
| 100,000 | 1 min † | 22.8 TB | 19.2 TB (0) | $418 | $0.90 | $16 | $456 | $384 | $1,275 |
| 100,000 | 5 min † | 22.8 TB | 19.2 TB (0) | $418 | $0.19 | $11 | $456 | $384 | $1,269 |
| 100,000 | 15 min † | 22.8 TB | 19.2 TB (0) | $418 | $0.07 | $9.58 | $456 | $384 | $1,268 |
| 100,000 | 60 min † | 22.8 TB | 19.2 TB (0) | $418 | $0.03 | $9.23 | $456 | $384 | $1,267 |

Compute is mostly vCPU time (about 23 vCPU-hours a month per 1,000 req/s). Included free tiers (10M Compute requests, 100M vCPU-ms, 100k KV writes, 5 GB of FOS storage and small FOS operation allowances) would cover most of the 100 req/s rows.

#### By log line size, 5-minute window

The synthetic shapes use random values, so their raw and Parquet sizes (and therefore storage cost) are **pessimistic**: real logs compress 2–3× better. Their CPU numbers are representative.

| Log line | req/s | Workers | Compute requests | vCPU hours | FOS Class A | FOS Class B | Raw billed | Parquet created | Total, 1 month kept |
|---|---|---|---|---|---|---|---|---|---|
| small: 10 fields, ~190 B | 1,000 | 3 | 73k | 7.8 | 531k | 458k | 145 GB | 187 GB | $9.79 |
| typical: 23 fields, ~620 B | 1,000 | 5 | 108k | 23 | 549k | 458k | 228 GB | 227 GB | $15.04 |
| long values: 25 fields, ~1.1 KB † | 1,000 | 15 | 281k | 28 | 635k | 458k | 806 GB | 1.0 TB | $44 |
| wide: 50 fields, ~1.1 KB † | 1,000 | 17 | 315k | 43 | 652k | 458k | 920 GB | 1.2 TB | $52 |
| very wide: 100 fields, ~2.2 KB † | 1,000 | 35 | 626k | 90 | 808k | 458k | 1.9 TB | 2.3 TB | $103 |
| small: 10 fields, ~190 B † | 10,000 | 27 | 488k | 78 | 739k | 458k | 1.5 TB | 1.9 TB | $83 |
| typical: 23 fields, ~620 B † | 10,000 | 42 | 747k | 230 | 868k | 458k | 2.3 TB | 1.9 TB | $128 |
| long values: 25 fields, ~1.1 KB † | 10,000 | 149 | 2.60M | 277 | 1.79M | 458k | 8.1 TB | 10.5 TB | $427 |
| wide: 50 fields, ~1.1 KB † | 10,000 | 170 | 2.96M | 435 | 1.97M | 458k | 9.2 TB | 11.8 TB | $506 |
| very wide: 100 fields, ~2.2 KB † | 10,000 | 346 | 6.00M | 901 | 3.49M | 458k | 18.8 TB | 23.4 TB | $1,017 |

### Historical: staged-copy benchmark (superseded)

**Superseded.** These are the benchmark-era cost tables from [`model.py`](../experiments/archive/capacity-model/model.py), kept as evidence. Workers wrote staged Parquet pieces that a stitch then copied server-side, so every Parquet byte was also billed once as a staged file for 30 days (twice for two-level stitches: the 3.8 TB and 38.4 TB cells). They used 0.8 ticks a minute, at least 2 workers per period and no multipart padding. The prototype's direct parts removed the staged copy ([07 §7.11](research/07-live-verification.md)); the current code has neither. The operations and line-size tables of this model are reproducible from `model.py`.

| req/s | Window | Raw billed (GB-month) | Staged billed (GB-month) | Parquet created | FOS operations | Raw + staged storage | Parquet storage per month kept | FOS total, 1 month kept |
|---|---|---|---|---|---|---|---|---|
| 100 | 1 min | 23 GB | 19 GB | 19 GB | $7.28 | $0.84 | $0.38 | $8.51 |
| 100 | 5 min | 23 GB | 19 GB | 19 GB | $1.53 | $0.84 | $0.38 | $2.75 |
| 100 | 15 min | 23 GB | 19 GB | 19 GB | $0.57 | $0.84 | $0.38 | $1.79 |
| 100 | 60 min | 23 GB | 19 GB | 19 GB | $0.21 | $0.84 | $0.38 | $1.44 |
| 1,000 | 1 min | 228 GB | 192 GB | 192 GB | $7.28 | $8.40 | $3.84 | $20 |
| 1,000 | 5 min | 228 GB | 192 GB | 192 GB | $1.61 | $8.40 | $3.84 | $14 |
| 1,000 | 15 min | 228 GB | 192 GB | 192 GB | $0.68 | $8.40 | $3.84 | $13 |
| 1,000 | 60 min | 228 GB | 192 GB | 192 GB | $0.27 | $8.40 | $3.84 | $13 |
| 10,000 | 1 min | 2.3 TB | 1.9 TB | 1.9 TB | $8.06 | $84 | $38 | $130 |
| 10,000 | 5 min | 2.3 TB | 1.9 TB | 1.9 TB | $2.18 | $84 | $38 | $125 |
| 10,000 | 15 min | 2.3 TB | 1.9 TB | 1.9 TB | $1.21 | $84 | $38 | $124 |
| 10,000 | 60 min | 2.3 TB | 3.8 TB | 1.9 TB | $0.87 | $122 | $38 | $162 |
| 100,000 | 1 min | 22.8 TB | 19.2 TB | 19.2 TB | $13 | $840 | $384 | $1,237 |
| 100,000 | 5 min | 22.8 TB | 38.4 TB | 19.2 TB | $8.17 | $1,223 | $384 | $1,615 |
| 100,000 | 15 min | 22.8 TB | 38.4 TB | 19.2 TB | $8.05 | $1,223 | $384 | $1,615 |
| 100,000 | 60 min | not feasible as one file | | | | | | |

| req/s | Window | Compute (requests + vCPU) | KV | FOS | Total |
|---|---|---|---|---|---|
| 100 | 1 min | $0.50 | $1.17 | $8.51 | $10 |
| 100 | 5 min | $0.44 | $0.23 | $2.75 | $3.43 |
| 100 | 60 min | $0.43 | $0.02 | $1.44 | $1.89 |
| 1,000 | 1 min | $4.23 | $1.17 | $20 | $25 |
| 1,000 | 5 min | $4.18 | $0.23 | $14 | $18 |
| 1,000 | 60 min | $4.17 | $0.02 | $13 | $17 |
| 10,000 | 1 min | $42 | $1.17 | $130 | $173 |
| 10,000 | 5 min | $42 | $0.23 | $125 | $166 |
| 10,000 | 60 min | $42 | $0.02 | $162 | $203 |
| 100,000 | 1 min | $415 | $1.17 | $1,237 | $1,653 |
| 100,000 | 5 min | $415 | $0.23 | $1,615 | $2,030 |
