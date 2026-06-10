# Tech-Debt Audit — Phase 0 + Re-sweep

Goal: zero `TODO|FIXME|XXX|HACK` markers in backend + frontend at v2.0 ship time.

## Current state (re-sweep 2026-06-09)

The post-phase sweep finds **3 markers** (Phase 0 baseline was 1):

| File | Line | Marker | Resolves in | Reason |
|---|---|---|---|---|
| `backend/utils/telemetry_proxy.py` | 406 | `TODO(proxy-mem)` | **Phase 10.5** (httpx-everywhere sweep) | Large PUTs (multi-GB compacted commits) are an OOM risk under buffered SigV4 signing. Need chunked-signing path or stream-and-re-sign. Note: this surface is the telemetry proxy server; httpx swap doesn't affect it (it stays aiohttp per Phase 10.6). Resolution path is either (a) chunked SigV4 (botocore exposes no public API; manual implementation), or (b) hard cap on proxied PUT size with a clear 413 response and a doc note that compacted commits >N MB must hit FOS directly. Decision deferred to Phase 10.5. |
| `frontend/app/sessions/_sections/SessionDetail.tsx` | 132 | `TODO` | **Phase 8** (composite-first API + frontend swap) — pending `npm run gen:types` after the sessions composite ships | `as any` cast on the openapi-fetch row payload so the new `edge_sid` column can render before the generated types pick it up. Mechanical resolution: run `cd frontend && npm run gen:types`, drop the cast, drop the comment. |
| `frontend/app/sessions/_sections/SessionsTable.tsx` | 150 | `TODO` | **Phase 8** (composite-first API + frontend swap) — pending `npm run gen:types` after the sessions composite ships | Same `as any` cast for the `has_edge_sid` / `edge_sid` fields. Same mechanical resolution: regenerate types, drop the cast. |

### Net delta vs Phase 0

- +2 frontend markers introduced by the Phase 9b.19 `app/sessions/` route split (the new `_sections/SessionDetail.tsx` + `_sections/SessionsTable.tsx` components had to widen their row types ahead of the backend composite landing).
- 0 markers resolved in the re-sweep window (the original `TODO(proxy-mem)` is still scheduled for Phase 10.5).
- Net: **+2 markers**. All three have a documented resolution phase and a known mechanical fix; none are open-ended "split later" / "FIXME" / "HACK" calls.

Note on the grep methodology: the baseline grep counts true `\b(TODO|FIXME|XXX|HACK)\b` boundaries and excludes the `\\uXXXX escapes` false positive in `backend/core/log_fields.py:896`.

---

## End-state assertion (Phase 10.9 / 10.12)

Final sweep re-runs the same grep:

```
grep -rn --include="*.py" --include="*.ts" --include="*.tsx" \
    -E "\b(TODO|FIXME|XXX|HACK)\b" backend/ frontend/ \
    | grep -v node_modules | grep -v ".next/" \
    | grep -v ".generated" | grep -v "\\\\uXXXX" \
    | grep -v "uXXXX escapes"
```

If the output is non-empty at v2.0 cut, Phase 10.9 fails. No `TODO: split` markers (per plan §10.9) — split it or write a paragraph in `pending-docs/surprises.md` justifying why it can't be split (with the marker still counting as debt).

---

## Adjacent debt categories (informational — not counted as markers)

### `[v2.0-pending]` banners in `docs/ARCHITECTURE.md`

Added in Phase 0.13 to flag sections being rewritten. Each phase that ships an architectural change removes the banner on the affected sections (per the per-phase verification §14). Re-sweep 2026-06-09: the three section banners on §1 (storage layout), §2 (ingest pipeline), and §4 (live-share architecture) have been removed; the top-of-file banner stays until Phase 10 confirms everything else (Phase 7 field-registry migration is still in progress; Phase 8/9 work is still ahead). Phase 10.10 confirms all banners removed.

End-state target: **0 banners**.

### `# Why this exists` paragraph comments in `main.py` + `deps.py`

Per success criteria (cleanup_plan.md §"Success criteria"): target -75% vs baseline. Replaced by typed invariants + boot assertions (per ADR-04).

### `MONKEYPATCHES.md` patch count

Re-sweep 2026-06-09: still **6 active patches** (per the sweep's `monkeypatch_count: 6`). Phase 4 carve-up replaced the import-time s3fs patches with `FosS3FileSystem` / `CachedS3FileSystem` subclasses inside [backend/core/iceberg/fs.py](../backend/core/iceberg/fs.py), but the catalog file still enumerates the originals for historical reference. Phase 10 closing target: drop to ≤ 1 (only the `ThreadPoolExecutor.submit` ContextVar propagation patch should remain after the file is rewritten against the live state). Surprise log entry covers the elimination strategy.

### `process_context_scope` vs `set_process_context` distinction

Per surprises.md: eliminated in Phase 10.3 once Phase 1 OTel context propagation makes the iothread mirror redundant.

### `_meta_con` parallel connection path

Per surprises.md: dropped in Phase 8.3 after Phase 4 storage carve-up removes the cost gap.

### `is_cached` / `_is_cached` Pydantic alias

Per surprises.md: dropped in Phase 8.4.

### `AnalyticsDeps = RequestContext` alias

Per Phase 2.7 → dropped in Phase 8.5 hard cutover.

### `# Security:` source-comment count baseline

Re-sweep 2026-06-09: **21 comments** today (down from 23 at Phase 0 baseline as some files were refactored). Used as a regression floor for the `@pytest.mark.security_regression` count (Phase 0.8). Source comments may decrease as code is refactored, but the corresponding test coverage stays (or grows) — the CI assertion is on the marked-test count, not on the comment count.

### Files-over-threshold (from re-sweep)

The sweep flagged 5 backend files >1,500 lines and 1 >2,500 lines (cleanup target: 0 >2,500, ≤2 >1,500). Phase 4/5b/6/7 carve-ups have shrunk this list but it is not yet at target:

- `backend/core/iceberg/_core.py` (3,812 lines) — Phase 4 carved out `fs.py` but `_core.py` is still the elephant. Further internal split (view / catalog / warehouse / manifest) is in scope for the closing Phase 4 follow-on.
- `backend/routers/session_scoring.py` (2,442 lines) — Phase 7 field-registry migration is the natural lever; not yet split.
- `backend/core/duckdb.py` (2,110 lines) — connection pool + view binding lives here; not yet carved.
- `backend/core/log_fields.py` (1,904 lines) — shrinks as Phase 7's field-registry migration replaces inline declarations.
- `backend/routers/admin.py` (1,739 lines) — Phase 9b parallel work; not yet split.

Frontend file >500 lines: 1 (down from 16 at Phase 0 baseline; Phase 9b route + component splits cleared the rest). The lone remaining offender is `frontend/app/dashboard/page.tsx` (960 lines), scheduled for Phase 9b.14.

### `_debug_queries` / mypy ignore counts

Re-sweep 2026-06-09: **14 mypy ignore entries** in `pyproject.toml`. Each removed module ratchets the count down per phase (every phase that touches a module is supposed to remove that module's ignore). Phase 10.13 target: ≤ 3 modules max.

---

## Why the marker count stays low

Even after introducing two new markers in the sessions route split, the project is still in the single-digit total — that's by design. Every new marker carries a documented resolution phase and a mechanical fix. The architectural debt is real and material (5 huge backend files, 1 huge frontend file, 14 mypy-ignored modules) but it does NOT manifest as accumulated marker comments. That's a positive — it means Phase 10's zero-marker target is achievable in 3 fixes, not 100.

The flip side: the absence of TODOs means architecture decisions live in **paragraph comments** (e.g., `main.py:434-501` middleware order, `MONKEYPATCHES.md`, the surprises log entries) rather than in TODOs. The cleanup eliminates those by encoding the invariants as code, not by deleting comments.
