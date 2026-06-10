# Follow-up: Dashboard Top-N vs Partial-Day Rollup Windows

**Status (2026-06-10, session 2).** Both bugs fixed in this commit:

1. The partial-day over-inclusion the original doc was about (§1).
2. A pre-existing day-vs-bundled double count on hour-aligned closed-day-only windows, surfaced while building the partial-day test fixture (§3).

The original doc's "missing per-hour rollups" detail (§4.1) turned out to be a dev-cache artifact — local dev doesn't run sync, so rollups froze at hour 06-09-16. On prod every closed hour has per-hour rollups, so both fixes are purely on the read path in [backend/repositories/_base.py](backend/repositories/_base.py).

## 1. What was wrong and what changed

`execute_top_n_rollups` enumerates per-day rollup files for each (field, day) where the day falls in the request window. The day file aggregates the entire UTC day, so when the request window starts or ends mid-day, the day file pulls in values from outside the window — concretely the user's repro:

- Request window `2026-06-09T17:36Z → 2026-06-10T17:36Z`
- `edge_score=50` rows actually exist only at hour `2026-06-09T05Z` (12.5h before window start)
- Day rollup for `2026-06-09` reports `edge_score=50, count=154`
- Top-N panel surfaces `edge_score=50`; click-through to `/query` returns zero rows

Fix in [backend/repositories/_base.py](backend/repositories/_base.py): add a `_day_fully_in_window(day_str)` predicate inside the per-field day-file walk. A day is "fully contained" iff `[day 00:00, +24h)` ⊆ `[st_dt, et_dt)`. Days that aren't fully contained fall through to the per-hour walk, which already filters by `hour >= st_str_floor` / `hour <= et_str_floor`.

What survives the fix:
- Hour-aligned full-day or multi-day windows still use the day rollups → preserves the 24×N → 1 file-open reduction on closed days.
- Active day already wasn't using the day file (active-day guard predates this fix).
- Boundary hours (e.g. `2026-06-09-17` for a window starting at 17:36) still over-include by up to 59 minutes within that hour. This is the rollup granularity floor — much smaller than the ~17 hour over-inclusion the day file caused — and any value that surfaces from a boundary hour is part of the same traffic profile as the rest of the window, so the "phantom value" symptom doesn't reoccur. Going finer than hourly would require the live-query path the original investigation ruled out (commit `c3d8822`, ~5s wide-temp cost).

Tests: [tests/repositories/test_base.py](tests/repositories/test_base.py) gains two regression tests — `test_execute_top_n_rollups_skips_day_file_on_partial_window` (pins the user repro shape) and `test_execute_top_n_rollups_uses_day_file_when_window_fully_contains_day` (pins that we didn't regress the 24×N → 1 optimization on aligned windows).

## 2. Why the original doc's §4.1 / §4.3 turned out to be red herrings

The original doc reported "per-hour rollups missing for hours 00, 02, 15, 17–23 of 2026-06-09 for `edge_score`" and asked whether `recompute_touched_hours` was skipping some hours.

Empirically: the same hours are missing for **every** field on that day (`country`, `ip`, `status`, `edge` all show identical 01, 03–14, 16 coverage). And the most-recent per-hour rollup in the dev cache is `2026-06-09-16` — nothing past that. Combined with the [dev-no-crons](../.../memory/dev-no-crons.md) memory ("local dev does NOT process cron jobs"), this is simply where dev stopped processing. On prod with a running sync, every closed hour gets per-hour rollups (touched_hours is built from `SELECT DISTINCT strftime(timestamp, '%Y-%m-%d-%H')` over the ingested rows — see [backend/core/ingest.py:694-701](backend/core/ingest.py#L694-L701) — so any hour with rows is in the set).

§4.3's "count=154 vs live=308" discrepancy is the same dev artifact: the per-hour file for `2026-06-09-05` was last rebuilt before all the rows in that hour had landed. On prod, late-landing files DO retrigger `recompute_touched_hours` for their hour because `chunk_hours` is computed from the staging table's timestamps regardless of whether the hour was already closed. And `compact_closed_days_to_daily` rebuilds the day file when any source per-hour file is newer than the day file — see [backend/core/rollups.py:996-1001](backend/core/rollups.py#L996-L1001).

## 3. Separate pre-existing bug, also fixed: day file double-counted vs bundled-hour files on closed-day-only windows

Discovered while building the partial-day test fixture. Pre-fix repro:

```python
runner.execute_top_n_rollups(
    ['edge_score'], '2026-06-08T00:00:00', '2026-06-09T00:00:00', limit=10
)
# Pre-fix returned ('edge_score', '75', 155200)
# Day file alone has 77600 — double-counted because the reader ALSO included
# hour_bundled/hour=2026-06-08-*/all_fields.parquet for the same hours.
```

Root cause: the bundled-hour walk in [backend/repositories/_base.py](backend/repositories/_base.py) added every in-window bundled file to `bundled_hour_paths` regardless of whether the corresponding day's per-day file was also being read. The per-field walk correctly avoided per-field-vs-bundled and per-field-vs-day double counts via its `bundled_hours` / `covered_days` checks, but nothing kept the day branch and the bundled branch from both feeding the UNION ALL and SUM doubling the count.

Fix in this same commit: a pre-pass before the bundled walk computes `day_covered_by_any_field` — the set of closed, fully-contained days where at least one safe field has a usable per-day parquet. The bundled-hour walk skips any hour whose day is in that set, so the day branch wins for any field that has a day file. Fields without a day file for that day (newly-added custom fields awaiting compaction) fall through to per-field per-hour via the existing per-field walk, which uses its OWN per-field `covered_days` set — so the global skip in the bundled walk doesn't strand them.

Trade-off: a newly-added field on a fully-compacted day pays per-field per-hour file opens (24 files for a 24h window) instead of bundled file opens (1 per hour, 24 total) for that day. Functionally identical file count; the per-field branch is marginally heavier per-open. Worst case is transient until the next compaction cycle builds the new field's day file.

Tests: `test_execute_top_n_rollups_no_day_vs_bundled_double_count` (pins that day-file count survives without double-counting bundled), `test_execute_top_n_rollups_bundled_still_used_when_no_day_file_for_field` (pins that fields without a day file fall back to per-field per-hour correctly).

Why this approach over the writer-side delete-bundled-after-compact option:
- No new invariant on disk to maintain — the writer keeps its current "build, never delete sibling layers" model.
- The `time_series.parquet` co-located in `hour_bundled/hour=H/` (consumed by `try_time_series_from_rollup`) is untouched.
- Backfilling a new field is straightforward: per-field per-hour gets written, reader uses it, next compaction tick builds the day file, reader switches over.

## 4. What NOT to do

- Don't reintroduce the wide-temp live fallback that `c3d8822` shipped. The cost was the 75-column temp materialization, not the aggregation, and the workaround is now in place via the day-skip + per-hour stitch.
- Don't widen `live_temp` ([dashboard.py:264](backend/repositories/dashboard.py#L264)) to include all top-N fields without first benchmarking — the existing comment warns it used to add ~1.4s alone.
- Don't try to "simplify" by deleting `hour_bundled/hour=H/all_fields.parquet` after day compaction. The `time_series.parquet` co-located in the same directory is consumed by `try_time_series_from_rollup` at [_base.py:977](backend/repositories/_base.py#L977) — easy to lose track of. The reader-side fix in §3 sidesteps that footgun entirely.
