# Surprises Log

Append every undocumented gotcha or non-obvious design choice surfaced during v2.0 cleanup execution.

Each entry: **what surprised**, **what it broke (or nearly broke)**, the **corrected mental model**, and the **phase it should resolve in** (if any).

This log drives ad-hoc plan amendments at phase boundaries. Once an entry is fully addressed (code change shipped + the rule encoded somewhere lookup-able), append `**RESOLVED in phase X.Y**` to the entry rather than deleting it — the historical context stays useful.

---

## Template

```
### YYYY-MM-DD — short title

**Surprised by:** one-paragraph description of the gotcha.

**What it broke:** what nearly went wrong, what test caught it, etc.

**Corrected mental model:** the new rule / invariant.

**Resolves in:** Phase X.Y (or "not resolvable structurally — needs comment + test").
```

---

## Entries

### 2026-06-09 — `process_context_scope` vs `set_process_context` distinction (carry-over from pre-Phase-0)

**Surprised by:** the codebase has two near-identical functions for installing per-request state into the iothread mirror — `process_context_scope` is a context manager, `set_process_context` is a fire-and-forget setter. They are NOT interchangeable; using one where the other is expected silently leaks request state across requests when an error fires before cleanup.

**What it broke:** historically caused cross-tenant proxy mis-routing in the telemetry-proxy path when an `await` raised before the explicit `set_process_context(None)` reset. Resolved at the time by switching to `process_context_scope` (context manager) on that path.

**Corrected mental model:** the two functions exist only because Phase 1 (OTel context propagation) hasn't happened yet. OTel spans carry their own context; the iothread mirror becomes unnecessary once OTel is in place. **Plan to eliminate, not formalize.**

**Resolves in:** Phase 10.3 (drop the distinction after Phase 1 OTel context propagation makes the iothread mirror redundant).

---

### 2026-06-09 — Monkeypatch inventory (carry-over from MONKEYPATCHES.md)

**Surprised by:** [MONKEYPATCHES.md](../MONKEYPATCHES.md) documents 6 active monkeypatches:
- 5 of 6 (#1-#5) are an s3fs + telemetry-proxy integration block in `backend/core/iceberg.py` behind a single `try: ... except ImportError`.
- 1 of 6 (#6) is a `concurrent.futures.ThreadPoolExecutor.submit` patch that propagates ContextVars to worker threads (security finding 003, cross-tenant leak prevention).

The s3fs block was investigated in 2026-05-21 for replacement via a `CachedFosS3FileSystem` subclass injected through pyiceberg's `py-io-impl`; the three extraction paths all lose (see `MONKEYPATCHES.md §Medium-term`). The block is structurally optimal until pyiceberg upstream adds a "supply your own FileSystem class" hook.

**What it broke (historical):** failure to patch `#5 _open` left manifest reads from `plan_files` not populating the LRU (17 `_open` calls, 0 `_cat_file` calls on a real workload). The `self.cat_file` trap is documented inline at `iceberg.py:410-417`.

**Corrected mental model:**
- Phase 4 carves `iceberg.py` and creates `iceberg/fs.py` to elevate patches #1-#5 to a subclass (`FosS3FileSystem(S3FileSystem)` + `CachedS3FileSystem(S3FileSystem)`) registered via pyiceberg's `FileIO` mechanism. Success criterion: monkeypatches drop from 6 → ≤ 1 (only #6 should remain).
- Patch #6 (`ThreadPoolExecutor.submit`) stays until CPython adds first-class ContextVar propagation to `concurrent.futures` or pyiceberg switches to asyncio for parquet writes.

**Resolves in:** Phase 4.1 (subclass replacement); Phase 10.13 (formal mypy-strict justification for the surviving #6).

---

### 2026-06-09 — local-compaction outputs survive Iceberg orphan-cleanup (Trap #21)

**Surprised by:** `sync_data` orphan-cleanup walks `/mnt/app-data/raw/...` and was historically eager about deleting "unknown" parquets. The `compacted_*.parquet` and daily/weekly rollup files look unknown to the walker because they were committed by a different code path. Naive cleanup dropped rows.

**What it broke (historical):** dropped rows after a compaction window. Caught by `tests/core/test_local_compaction.py::test_compaction_outputs_survive_iceberg_sync_orphan_cleanup`.

**Corrected mental model:** orphan-cleanup must restrict its walk and explicitly skip `compacted_*.parquet`, daily, and weekly patterns. The test is the load-bearing invariant. Carrying it forward through the Phase 4 carve-up is gated by Phase 4.6.

**Resolves in:** Phase 4.6 (test reaffirmed against the carved structure). No structural fix needed — the rule needs to stay encoded in the orphan-walker code + test.

---

### 2026-06-09 — `_meta_con` parallel connection path

**Surprised by:** `backend/deps.py:233` exposes a separate `_meta_con` alongside the main DuckDB connection in `AnalyticsDeps`. Metadata reads (alerts, views, saved queries) used it to avoid paying the Iceberg view-rebuild cost on every request — a real optimization, but it created a parallel code path nobody documented.

**What it broke:** confusion about when to use which connection; intermittent latency spikes when a route used `con` for what should have been a `_meta_con` query.

**Corrected mental model:** after Phase 4 (storage carve-up), metadata queries don't pay the Iceberg view-rebuild cost because the view-binding moves out of the pool acquire path. `_meta_con` becomes redundant and gets dropped in Phase 8.

**Resolves in:** Phase 8.3 (drop `_meta_con` parallel path).

---

### 2026-06-09 — `is_cached` vs `_is_cached` Pydantic alias on `BaseResponse`

**Surprised by:** the `BaseResponse` Pydantic model carries both `is_cached` and `_is_cached` (commit 571810b). The underscore variant was added as a workaround for a Pydantic v2 alias-validation edge case where the public name was being eaten by the validator's allow-list logic. Both fields hold the same value; the underscore one is "for serialization only."

**What it broke:** confusion when adding cache fields to new responses; intermittent debug-panel reads of the wrong field.

**Corrected mental model:** pick one name (`is_cached`), make it the canonical Pydantic field, fix the Pydantic v2 alias logic so it doesn't need the workaround. The Phase 8 hard cutover is the natural place to drop the alias.

**Resolves in:** Phase 8.4 (drop `_is_cached` alias, pick canonical name).

---

### 2026-06-09 — Frontend > 500 LOC count is 16, not 8

**Surprised by:** Phase 0.3 `scripts/baseline_metrics.sh` finds **16** frontend files > 500 lines, but the cleanup plan's Phase 9b only enumerates **8** carve targets (the `components/*` files). The eight unaccounted-for files are `frontend/app/*/page.tsx` route handlers — they're not in `components/` so they slipped past the original audit:

| LOC | Path |
|---|---|
| 2136 | frontend/app/logs/page.tsx |
| 1438 | frontend/app/admin/page.tsx |
| 1184 | frontend/app/dashboard/page.tsx |
| 959 | frontend/app/alerts/page.tsx |
| 656 | frontend/app/admin/usage-log/page.tsx |
| 628 | frontend/app/security/page.tsx |
| 562 | frontend/app/origin/page.tsx |
| 510 | frontend/app/sessions/page.tsx |

**What it broke:** nothing yet — caught during Phase 0 baseline. If left unaddressed, Phase 10.9's "no frontend file > 500 lines" success criterion would fail at v2.0 cut.

**Corrected mental model:** Phase 9b carve list extends to these 8 page.tsx files. They split naturally: the RSC-server-component shell stays in `page.tsx` (small), CSR data-viz islands move to per-page `components/<route>/*` subdirectories per ADR-05. The work composes well with Phase 9a's nuqs adoption (URL state moves into the shell; islands consume via hooks).

**Resolves in:** Phase 9b — extend §9b.1–§9b.8 to also cover `frontend/app/*/page.tsx`. Sizing impact: doubles Phase 9b scope from 8 files to 16, but the largest two (logs at 2136, admin at 1438) drive most of the time anyway, and many page.tsx splits are mechanical once the components pattern is established. Phase 9b sizing estimate (4–8 h) should be re-checked at phase start.

---

### 2026-06-09 — Backend > 2500 LOC count is 3, not 4

**Surprised by:** Phase 0.3 baseline finds **3** backend files > 2500 lines (iceberg, metadata_db, scheduler), not 4. The cleanup plan listed `backend/routers/session_scoring.py` as the 4th carve target, but it's actually 2,442 lines — under the 2,500 threshold (it's > 1,500, so it still counts for the "files > 1,500" success criterion).

**What it broke:** nothing — Phase 5b / 6 / 7 / 10 already covers the three actual large files (metadata_db in 5b.3b, scheduler in 6.2a, iceberg in 4.1, share_db and tunnel in 10.1/10.2). `session_scoring.py` is not enumerated for carve-up in the plan; if it should be (it's 2,442 lines), it belongs in Phase 7 alongside the field-registry and scoring work.

**Corrected mental model:** the plan's "today: 4 files > 2,500" wording was off-by-one. The success criterion "0 backend files > 2,500 lines at v2.0 cut" is met by the three carve-ups already in the plan. `session_scoring.py` may still warrant splitting if Phase 7 review finds clear concern boundaries inside it; tag for re-evaluation at Phase 7.1.

**Resolves in:** Phase 7.1 review (decide whether to add `session_scoring.py` carve to Phase 7 scope). No plan change required yet.

---

### 2026-06-11 — Phase 5b.3a Terraform JSON migration is larger than planned

**Surprised by:** the cleanup plan flags Phase 5b.3a as a "200 LOC delete" win that eliminates a custom-HCL escaping injection vector. Walking the code, the actual conversion of [`backend/utils/terraform_gen.py`](../backend/utils/terraform_gen.py) (324 lines) is closer to 3–4 hours, not the 1–2 hours implied. Reasons the scope estimate was off:

- The file is mostly intricate HCL templating with multi-block resources (`fastly_service_vcl`, nested `domain`/`backend`/`vcl`/`dictionary`/`snippet` blocks), HCL function calls (`file("${path.module}/X")`), and HCL expressions (`{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }`). All of these can be expressed in `.tf.json` but each requires careful translation into JSON-with-HCL-interpolation strings.
- HCL has comments; JSON doesn't. The current generator emits multi-line comment banners that document customer-facing intent — those need to move into the generated `instructions` README (already exists in the output map), not just disappear.
- The companion test file [`tests/utils/test_terraform_gen.py`](../tests/utils/test_terraform_gen.py) (319 lines) gates on `terraform fmt -check` of HCL output. Every assertion needs rewriting to compare JSON structures instead, and the `tests/utils/terraform_tests/` fixture directory's `.tf` reference files become `.tf.json`.
- Customer impact: the generator output drives a real `terraform apply` against Fastly + AWS. A single mis-translated block shape breaks customer infra deploys silently — the test suite is the only safety net and it needs to come along.
- Realistic LOC delta: ~100 lines saved (dropping `_hcl_escape` + collapsing the f-string blocks), not the 200+ the plan claimed.

**What it broke:** nothing — caught before attempting. Avoided shipping a half-finished migration that would have either (a) broken customer terraform applies on next provision or (b) left both code paths around indefinitely.

**Corrected mental model:** Phase 5b.3a is a 3–4 hour focused session, not a side-quest in a larger cleanup batch. Its security value is real (deletes the regex-based escaping primitive entirely) but the risk profile demands its own session with full local + CI validation against the existing `tests/utils/test_terraform_gen.py` shape, then a careful re-deploy verification.

**Re-open trigger:** any of —
- A customer-reported terraform-apply failure that root-causes to a missing escape in `_hcl_escape`
- A pre-v2.0 dedicated session with the explicit goal of running this end-to-end
- A new HCL block needs adding (cheaper to add in JSON shape from the start than to add to the f-string templates and then migrate later)

**Resolves in:** Phase 5b.3a — re-scope to a dedicated session. **RESOLVED in dedicated session 2026-06-11 on the `cleanup/5b-3a-terraform-json` worktree.** Final scope landed close to the 3–4 h estimate: the rewrite itself was bounded (Python `dict`-builder + `json.dumps`), but the test-file rewrite (HCL string contains → JSON structural assertions + a redesigned template-prefix-escape test that avoided the `%%{ if true }` substring false positive) ate noticeable time. `terraform fmt -check` and `terraform validate` both pass with real Fastly + AWS providers loaded. Net LOC delta: ~-10 lines in the generator (more lean dict than f-strings) plus the entire `_hcl_escape` regex helper gone. Filenames changed from `.tf` → `.tf.json`; Terraform accepts both interchangeably in the same module. Surprise entry stays for the historical record on the scope-was-larger-than-planned mental-model correction.
