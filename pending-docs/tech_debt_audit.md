# Tech-Debt Audit — Phase 0

Goal: zero `TODO|FIXME|XXX|HACK` markers in backend + frontend at v2.0 ship time.

Phase 0 verification (`scripts/baseline_metrics.sh` 2026-06-09) finds **1 marker** total:

| File | Line | Marker | Resolves in | Reason |
|---|---|---|---|---|
| `backend/utils/telemetry_proxy.py` | 406 | `TODO(proxy-mem)` | **Phase 10.5** (httpx-everywhere sweep) | Large PUTs (multi-GB compacted commits) are an OOM risk under buffered SigV4 signing. Need chunked-signing path or stream-and-re-sign. Note: this surface is the telemetry proxy server; httpx swap doesn't affect it (it stays aiohttp per Phase 10.6). Resolution path is either (a) chunked SigV4 (botocore exposes no public API; manual implementation), or (b) hard cap on proxied PUT size with a clear 413 response and a doc note that compacted commits >N MB must hit FOS directly. Decision deferred to Phase 10.5. |

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

Added in Phase 0.13 to flag sections being rewritten. Each phase that ships an architectural change removes the banner on the affected sections (per the per-phase verification §14). Phase 10.10 confirms all banners removed.

End-state target: **0 banners**.

### `# Why this exists` paragraph comments in `main.py` + `deps.py`

Per success criteria (cleanup_plan.md §"Success criteria"): target -75% vs baseline. Replaced by typed invariants + boot assertions (per ADR-04).

### `MONKEYPATCHES.md` patch count

Today: 6 active patches. Phase 4 target: drop to ≤ 1 (only the `ThreadPoolExecutor.submit` ContextVar propagation patch should remain). Surprise log entry covers the elimination strategy.

### `process_context_scope` vs `set_process_context` distinction

Per surprises.md: eliminated in Phase 10.3 once Phase 1 OTel context propagation makes the iothread mirror redundant.

### `_meta_con` parallel connection path

Per surprises.md: dropped in Phase 8.3 after Phase 4 storage carve-up removes the cost gap.

### `is_cached` / `_is_cached` Pydantic alias

Per surprises.md: dropped in Phase 8.4.

### `AnalyticsDeps = RequestContext` alias

Per Phase 2.7 → dropped in Phase 8.5 hard cutover.

### `# Security:` source-comment count baseline

23 today. Used as a regression floor for the `@pytest.mark.security_regression` count (Phase 0.8). Source comments may decrease as code is refactored, but the corresponding test coverage stays (or grows).

---

## Why the marker count is already low

A meaningful Phase 0 finding: this codebase is already disciplined about not leaving TODO breadcrumbs. The architectural debt is real and material (4 huge files, fragmented telemetry, leaky abstractions), but it does NOT manifest as accumulated marker comments. That's a positive — it means Phase 10's zero-marker target is achievable in 1 fix, not 100.

The flip side: the absence of TODOs means architecture decisions live in **paragraph comments** (e.g., `main.py:434-501` middleware order, `MONKEYPATCHES.md`, the surprises log entries) rather than in TODOs. The cleanup eliminates those by encoding the invariants as code, not by deleting comments.
