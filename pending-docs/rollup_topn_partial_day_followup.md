# Follow-up: Dashboard Top-N vs Partial-Day Rollup Windows

**Context for a fresh session.** This doc captures an unfinished investigation from 2026-06-10 so the next session can pick it up without re-deriving the state. Two commits today are relevant:

- `c0de534` (kept): writer-driven view warming — unrelated to this thread, fixed a different dashboard slowness
- `c3d8822 → dcd5bd3` (added then reverted): a partial-day live fallback that fixed the correctness gap but cost ~3-5 s per dashboard load

---

## 1. The user-visible bug

A user picks a 24h dashboard window like `2026-06-09T17:36Z → 2026-06-10T17:36Z`. The Top-N panel for `edge_score` shows:

```
75   119500
null  63075
0     3390
50      154
```

User clicks `edge_score=50` to investigate. Dashboard correctly applies the filter and the panel updates. User then clicks "See raw logs" → the linked `/query` page generates correct SQL with the same time window AND `edge_score IN (50)` — but returns **zero rows**.

The 154 came from the per-day rollup for `2026-06-09`. The actual rows with `edge_score=50` are at hour `2026-06-09T05:00Z` — **12.5 hours before** the user's window starts. The per-day rollup file covers the entire UTC day, so the inclusion logic in `execute_top_n_rollups` pulls those rows into the panel even though they aren't in the user's window.

Concretely:

```sql
-- Live data: confirms edge_score=50 is OUT-OF-WINDOW for user's range
SELECT date_trunc('hour', timestamp), COUNT(*)
FROM logs WHERE edge_score = 50 AND timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY 1;
-- 2026-06-04T21Z  416
-- 2026-06-05T04Z    1
-- 2026-06-09T05Z  308
-- user's window starts at 2026-06-09T17:36Z
```

Per-day rollup file at `cache/.../rollups/day/field=edge_score/day=2026-06-09/compacted.parquet`:

```
field=edge_score | value=50 | count=154 | day=2026-06-09
```

(The 154 vs 308 discrepancy is a separate freshness gap — see §4.3.)

## 2. Why the obvious fix didn't work

A "partial-day live fallback" was added in `c3d8822`: walk each UTC day overlapping the window; for boundary days that have a per-day rollup file AND aren't fully contained, skip the day rollup and live-query the in-window portion instead.

Logic was correct. Performance was unacceptable on prod:

| Query (from prod log) | Time | What |
|---|---|---|
| #4 | **3266 ms** | `CREATE TEMP TABLE … SELECT <75 cols> … WHERE timestamp ∈ [partial-day range]` |
| #5 | **1365 ms** | UNION ALL of 75 `(SELECT field, value, COUNT(*) GROUP BY)` against the temp |

Total dashboard load went from ~1.8 s → 6.2 s. The wide-temp materialization for ~6 hours of data dominates. Reverted in `dcd5bd3`. The phantom `edge_score=50` symptom is back, but dashboard is fast.

Code-comment block left in `[backend/repositories/_base.py:~700](backend/repositories/_base.py)` (search "KNOWN GAP (2026-06-10)") documents the gap and rules out the live path.

## 3. The architecture as I understand it

Per-service cache layout at `<cache>/rollups/`:

```
hour/field=<F>/hour=YYYY-MM-DD-HH/<chunk>.parquet   ← per-field per-hour
hour_bundled/hour=YYYY-MM-DD-HH/all_fields.parquet  ← all fields, one file per hour
day/field=<F>/day=YYYY-MM-DD/compacted.parquet      ← per-field per-day (only count, no hour)
```

Writers ([backend/core/rollups.py](backend/core/rollups.py)):

- `recompute_touched_hours` — fires on every sync that lands rows; rebuilds per-field per-hour rollups for the touched hours, then calls `bundle_hours` to build the per-hour bundled file
- `bundle_hours` — combines per-field per-hour parquets into one per-hour bundled file. **Does NOT delete the source per-field files** (despite my earlier assumption)
- `compact_closed_days_to_daily` — for each (field, closed-day) where the per-day file is missing or stale vs per-hour mtimes, sum 24 per-hour parquets into one per-day file. **Does NOT delete the per-hour files either**

The only deletion path is `cleanup_old_rollups`, which is age-based (`max_age_days`) and intended for long-term retention, not for routine post-compaction cleanup.

Reader ([backend/repositories/_base.py:563 `execute_top_n_rollups`](backend/repositories/_base.py#L563)):

- Enumerates day rollups for fully-contained days, falls back to per-hour for active day
- Bundled-hour files cover hours that have been bundled
- Active hour is live-queried and merged

If per-hour rollups existed for every hour in the window, the partial-day boundary bug wouldn't fire — the day-rollup over-inclusion only matters because the reader prefers the day file when it's there. If per-hour files exist for the boundary day's in-window hours, the reader's `bundled_hours`/`hour_paths` enumeration already bounds them correctly.

## 4. Open questions for the next session

### 4.1 Why are per-hour rollups missing for some hours of compacted days?

Empirical: for `edge_score` on `2026-06-09`, per-hour rollups exist for hours `01, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 16`. **Missing**: hours `00, 02, 15, 17, 18, 19, 20, 21, 22, 23`.

Nothing in the codebase visibly deletes per-hour files. So either:
- They were never built (sync's `recompute_touched_hours` didn't fire for those hours' touched-hours set), or
- They were built and something else deleted them

Starting points:
- [backend/core/rollups.py:615 `recompute_touched_hours`](backend/core/rollups.py#L615) — what triggers it, with what hour set
- [backend/cron/jobs/sync.py](backend/cron/jobs/sync.py) — where `touched_hours` comes from on a sync tick
- Does the sync's `done_event["touched_hours"]` always include every hour that received rows during that tick?
- Is there a separate retention or compaction step that deletes per-hour files post-bundle?

### 4.2 If we ensure per-hour rollups exist for all hours, is the reader actually correct?

The reader's loop at [_base.py:759](backend/repositories/_base.py#L759) iterates per-hour rollup dirs and filters by `st_str_floor`/`et_str_floor`. If per-hour rollups exist for boundary-day hours and the day rollup is **also** present, both could be added to `day_paths` + `hour_paths`. Need to confirm the `covered_days` logic correctly suppresses per-hour reads when the day file is taken.

For partial days specifically: the current code would take BOTH the (over-inclusive) day file AND any per-hour files in the window — double-counting. So the fix isn't just "make per-hour files exist", it's also "skip the day file when window is partial".

The partial-day skip is what `c3d8822` did; the part that was expensive was the live fallback. If per-hour rollups reliably cover the in-window portion of boundary days, the live fallback isn't needed — just the day-file skip.

### 4.3 Why does the per-day rollup say `count=154` for `edge_score=50` when live has `count=308` for that hour?

The per-day file for `2026-06-09 / edge_score` is built by `compact_closed_days_to_daily` summing per-hour parquets. Per-hour file for hour `06-09T05Z` was presumably built when fewer than 308 rows existed in that hour, then never rebuilt. Subsequent file-landings into the same hour bumped the live count but didn't update the rollup.

Need to check:
- Does sync's `recompute_touched_hours` re-run when late files land in a closed hour?
- Does day-compaction detect per-hour mtime updates and rebuild the day file? ([_base.py:996](backend/repositories/_base.py#L996) suggests it does — `if day_mtime >= max_hour_mtime: continue`)
- If late files land but per-hour never rebuilds, both per-hour AND per-day are stale

This is a separate bug from the partial-day window thing, but they overlap: a fix that just adds per-hour coverage for missing hours would still be stale-relative-to-live.

### 4.4 Cheaper partial-day live path?

If §4.1 and §4.3 turn out to be hard / out-of-scope, the live fallback is still an option — IF the per-query cost can drop from ~5 s to ~500 ms. The 75-column wide-temp materialization is what made it slow. Alternatives:

- **Per-field direct queries against the base table** (no temp) — let DuckDB's parquet column-projection do the work. May not share-scan, so could be 75 small scans → still slow. Worth benchmarking.
- **Narrow temp with only the columns currently visible in panels** — would need plumbing the visible-card set from the frontend → router → repo. Bigger refactor.
- **Reuse the existing `live_temp`** built at [dashboard.py:292](backend/repositories/dashboard.py#L292) — it's narrow (12 cols) and covers the full request window, but doesn't include all top-N fields. Widening it has its own perf cost (~1.4 s per the existing comment).

## 5. What I'd start with

1. Read [backend/core/rollups.py:615 `recompute_touched_hours`](backend/core/rollups.py#L615) end-to-end and trace what `touched_hours` actually is on a routine sync tick (instrument or grep for the call site)
2. Confirm whether late files into a closed hour trigger per-hour rebuild
3. Pick ONE boundary day's missing-hour case and trace why its rollup is absent
4. Only then decide whether the right fix is "make per-hour reliable" or "find a cheaper live path"

Tests in scope:
- [tests/repositories/test_dashboard.py::test_get_aggregates_rollup_path_map_data_uses_per_field_limits](tests/repositories/test_dashboard.py) — the pinning test for the rollup-batch shape; should remain passing
- Any test that exercises `recompute_touched_hours` or `compact_closed_days_to_daily` (none I'm aware of for the touched-hours behavior — would need to add)

## 6. What NOT to do

- Don't reintroduce the wide-temp live fallback that `c3d8822` shipped. Comment block in [_base.py](backend/repositories/_base.py) explains the cost.
- Don't disable the rollup fast-path entirely (`f1348cb` did that — also too slow on the live path).
- Don't widen `live_temp` to include all top-N fields without first benchmarking — the existing comment at [dashboard.py:264](backend/repositories/dashboard.py) says the wide projection used to take ~1.4 s alone.

## 7. User-stated constraint

> "the top-n on the dash needs to be from the real data during that time period"

The fix should preserve this. Click-through workarounds (snap window to day boundaries) are explicitly less preferred than rollup-data correctness.
