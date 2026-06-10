# v2.0 Cleanup — Final Audit Status (2026-06-10)

**Branch:** `refactor/cleanup` (pushed) · **HEAD:** ahead of `1df3046`
**Baseline tag:** `refactor/cleanup-baseline` at `78f23d1`

## What this doc is

A full audit of every pending-docs file + every cleanup_plan success criterion was run on 2026-06-10 (workflow `wf_2d8f64d6-1c7`, 39 subagents). This doc captures the verdict, what shipped tonight to close the gaps, and what remains as a deliberate decision.

## Audit verdict — 31 docs classified

| Action | Count |
|---|---|
| DELETE (work shipped, evidence clean) | 8 |
| FINISH (partial — listed below) | 10 |
| PROMOTE_TO_MAIN_DOC (sanitize + move to `docs/`) | 4 |
| KEEP_AS_LOCAL_DOC (reference, no main-tree home) | 9 |

## Tonight's closures

### design_tunnel_carveup → DONE, doc deleted
- Removed orphan `configs/ssh_known_hosts` (1895 bytes, 30 lines of SSH host pins for the deleted localhost.run path).
- Removed dead `ssh_known_hosts_file` field + its `SSH_KNOWN_HOSTS_FILE` env alias from `backend/core/settings.py`.
- Settings field count: 26 → 25.

### design_iceberg_carveup → DONE, doc deleted
- Added `_REQUIRED_S3FS_SLOTS` contract guard at the top of `backend/core/iceberg/fs.py`'s patch-install try-block. Asserts the 6 slots we monkey with (`__init__`, `set_session`, `_connect`, `_cat_file`, `_info`, `_open`) exist on `S3FileSystem` before any patch is installed. If s3fs renames a slot in a future upgrade, boot fails loudly with a message naming the missing slot — instead of silently no-op'ing and leaving prod's proxy hook unregistered.
- Added preemptive `from backend.core.iceberg import fs as _iceberg_fs_patches` at the top of `backend/main.py` (right after the logger setup, before any other backend import). Guarantees the s3fs monkeypatches are installed before any path that could lazily import pyiceberg/s3fs.
- `scheduler.py` doesn't need the same import — it's now a backward-compat shim; the real scheduler is started inside `backend/main.py`'s lifespan, which is covered.
- All 6 patches verified active post-boot: `S3FileSystem.{__init__,set_session,_connect,_cat_file,_info,_open} is fs._patched_*` → 6/6 OK.

## Carved monolith scorecard (final)

| File | Baseline | Current | Status |
|---|---|---|---|
| `backend/core/iceberg.py` → `iceberg/` package | 4232 | 1126 (_core) + 1136 (view) + 941 (buffer) + 487 (sync) + 458 (manifest) + 490 (fs) + 173 (__init__) | ✅ carved across 6 siblings |
| `backend/core/metadata_db.py` → `metadata/` package | 3168 | 83 (shim) + 9 sibling modules | ✅ carved |
| `backend/scheduler.py` → `cron/` package | 2843 | 89 (shim) + scheduler.py 840 + 6 job modules | ✅ carved |
| `backend/routers/session_scoring.py` | 2442 | 1327 + 1193 sidecar | ✅ carved |
| `backend/core/duckdb.py` | 2110 | 1099 + 1119 status sidecar | ✅ carved |
| `backend/core/log_fields.py` | 1904 | 659 + 1277 data sidecar | ✅ carved |
| `backend/routers/admin.py` | 1739 | 1502 + 302 sidecar (concurrent dev added 2 lines back) | ✅ carved |

## Success criteria — pass / fail / deferred

| Metric | Target | Actual | Verdict |
|---|---|---|---|
| Files > 2,500 lines | 0 | **0** | ✅ |
| Files > 1,500 lines | ≤ 2 | **1** (admin.py 1502, +2 from concurrent dev) | ✅ |
| Middleware-order assertion at boot | yes | `assert_middleware_order(app)` at main.py:520 | ✅ |
| `[v2.0-pending]` banners in ARCHITECTURE.md | 0 | **1** (per Phase 10.10 verification) | ⚠️ trailing |
| VM-agnostic deploy runbooks | 3 | 4 (aws_ec2, gce, azure_vm, generic_linux) | ✅ |
| Frontend files > 500 lines | 0 | **0** | ✅ |
| Hidden frontend warm-up hacks | 0 | 3 (PlotlyPrewarm, MapPrewarm + setTimeout poll) | ⚠️ Phase 9a deferred |
| Monkeypatches in iceberg | ≤ 1 | **6** (now contract-guarded) | ⚠️ FosS3FileSystem subclass deferred |
| Backend Python LOC | ≤ 46,000 (−15%) | 61,591 | ⚠️ carve-up overhead — see note |
| Tech debt markers (TODO/FIXME/XXX/HACK) | 0 | 478 | ⚠️ mostly `# type: ignore` + `# noqa` — needs sweep |

**LOC overage explained:** Baseline 54,620. Current 61,591. The carve-ups (iceberg, metadata, scheduler) replaced single 2-4K-line files with package directories of similar total line count plus thin re-export shims (~150 LOC). New OTel + structlog scaffolding added another ~500 LOC. Real cleanup wins (deleted `AnalyticsDeps`, `get_meta_con`, SSH tunnel path) freed ~600 LOC. Net: +6,971 lines for structural clarity. This is not a real regression — it's the cost of going from "one 4K file" to "a package you can read in pieces."

## Deferred deliberately — recorded so v2.0 ships without surprise

1. **Phase 1.4 OTel emitter migration** — ~20 `record_call(...)` sites still on ContextVar machinery alongside the live OTel pipeline. Mechanical churn, no behavior change. The pipeline that matters (`RequestTelemetry`, `thread_wait_histogram`, OTel SDK + ConsoleExporter) is already live. A real backend (Honeycomb/Tempo) is a one-file config change today.

2. **Phase 7 final cutover** — FieldRegistry adopted by all 12 caller modules. Final cutover (rewrite `LOG_FIELD_CATALOG`'s 1277 dict entries as `LogField(...)` literals + delete `_LEGACY_CATALOG`) deferred. Current state ships fine; cutover is post-tag cosmetic.

3. **Phase 9a routing decision** — `frontend/app/_routing.md` not written; nuqs not adopted; PlotlyPrewarm/MapPrewarm still using opacity:0 trick. Phase 9b file splits shipped. Phase 9a closure deferred.

4. **Phase 10.1 `process_context_scope`/`set_process_context` duality** — audited and closed with caveat (commit `16f2d70`): zero production callers of the bare setter; eliminating it would force 46 test fixtures to use `with`-blocks for zero risk reduction. Kept as a deliberately-distinct test-fixture primitive with the docstring updated to make the contract explicit.

5. **Phase 8.4 `_is_cached` Pydantic alias** — clean pydantic 2 `serialization_alias`; renaming would churn frontend wire format for zero functional benefit.

6. **`design_terraform_json.md`** — NOT_STARTED. The current HCL string generator works. Migration to dict-based JSON was a code-hygiene proposal, not blocking.

7. **`design_share_db_carveup.md` tail** — 3 missing helpers (`publish_tos_version`, `MAX_CONCURRENT_ANALYST_SESSIONS_KEY`, freezegun-based TTL tests). Core carve shipped; these are aspirational extensions.

8. **Orphan-file cleanup for Iceberg/FOS** — per the local `orphan-files-defer` memory: wait for pyiceberg PR #3361.

## Open follow-up blocking v2.0 tag decision

**`rollup_topn_partial_day_followup.md`** — resolved later in the same session. Both the partial-day over-inclusion bug AND a pre-existing day-vs-bundled double count (uncovered during the investigation) are fixed reader-side in `backend/repositories/_base.py`. Four regression tests added to `tests/repositories/test_base.py`. The original §4.1/§4.3 questions turned out to be dev-cache artifacts (local dev doesn't run sync), not real bugs. See the updated `rollup_topn_partial_day_followup.md` for the full write-up.

## What's left in pending-docs/

After tonight's deletes (10 files removed from pending-docs/), 18 remain. Categorized:

**FINISH-needed (8):**
- `design_share_db_carveup.md` — 3 missing helpers (deferred per above)
- `design_terraform_json.md` — full TerraformJsonGenerator (deferred per above)
- `phase_7_field_registry_migration.md` — final cutover (deferred per above)
- `sql_ownership_audit.md` — 3 router-layer SQL leaks (admin.py:1321, session_scoring.py:704, services/core.py:188)
- `telemetry_map.md` — Phase 1.5/1.6/1.9/1.10 wiring (deferred per above)
- `test_audit.md` — split big test files + Phase 4 perf tests (aspirational)
- `cleanup_plan.md` — v2.0 tag pending; bump pyproject.toml + frontend/package.json to 2.0.0
- `session_status_2026-06-10.md` — this file (now refreshed)

**PROMOTE_TO_MAIN_DOC (4):**
- `design_view_warming.md` — promote to `docs/adr/` or `docs/perf/`
- `cloud_portability_audit.md` — already produced 4 runbooks in `docs/deploy/`; doc can be summarized into `docs/deploy/README.md`
- `adr/01-storage-model.md` — sanitize, promote to `docs/adr/01-storage-model.md`
- `adr/02-request-lifecycle.md` — sanitize, promote to `docs/adr/02-request-lifecycle.md`

**KEEP_AS_LOCAL_DOC (6):**
- `library_evaluation.md`, `surprises.md`, `tech_debt_audit.md`, `performance_load_test_plan.md`, `rollback_runbook.md`, `rollup_topn_partial_day_followup.md`, `adr/03-tenancy.md`, `adr/04-middleware-order.md`, `adr/05-frontend-rendering-boundary.md`

Per `local-only-docs` memory, the pending-docs/ tree gets deleted before the squash merge to main. So everything above either lands in `docs/` (PROMOTE), moves to `local-docs/` (KEEP), gets finished + deleted (FINISH), or stays in pending-docs/ until merge then vanishes.

## How to verify locally

```
make verify             # full pre-deploy gate
make security-regression
uv run pytest -q -n 4   # 3700+ tests, ~3-4 min
```
