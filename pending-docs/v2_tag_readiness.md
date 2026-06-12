# v2.0.0 Tag Readiness

Tracks the gates that must be green before `pyproject.toml` and `frontend/package.json` bump to `2.0.0` and `git tag v2.0.0` runs (cleanup_plan §10.16).

This doc is the **single source of truth** for "is v2 ready?" — every PR that closes a gate adds a ✅ row here and updates the per-gate evidence link.

## Final-cut gates (cleanup_plan §10.12 / §10.13 / §10.14)

| Gate | Plan target | Current state | Status | Notes |
| --- | --- | --- | --- | --- |
| 10.12 — every backend module touched by phases 1–10 at ≥ 80% coverage | per-module ≥ 80% | total 82%, per-module not yet audited | ⚠️ partial | Per-module audit not yet run. Some modules (rollups.py at 34%, repositories/session_scoring.py at 34%, cron/jobs/compaction.py at 42%, etc.) are well below 80%. |
| 10.13 — `tool.mypy.overrides` ignore_errors list ≤ 3 modules | ≤ 3 | **0 entries** ✅ | ✅ green (2026-06-12) | Reduced 36 → 32 → 22 → 16 → 10 → **0** across five 2026-06-12 burndown waves (36 modules cleared, target exceeded — zero technical debt per user direction). Wave-5 cleared the heaviest tier: `core/ingest` (var-annotations + tuple|None narrowing), `provision/fastly_api` (Match|None narrowing + typed update_payload), `provision/orchestrator` (private iceberg attrs via `type: ignore[attr-defined]` + service_id narrowing + preset None-check), `repositories/origin` (single `assert rollup_row is not None` collapsed 11 indexing errors), `routers/admin` (renamed shadowed loop var + cast _QueueFile for zipfile + service_id narrowing), `routers/provision` (typed `cfg: dict[str, Any]` + bucket_name narrowing + log_fields_raw pull-out), `routers/services/core` (`type: ignore[attr-defined]` for iceberg private API), `routers/session_scoring_admin` (per-line `# type: ignore[has-type]` for circular-import limitation), `utils/remote_access` (assert datetime narrowing + `type: ignore[attr-defined]` for body_iterator on StreamingResponse), `utils/telemetry_proxy` (typed `data: Any` + asserts on _SESSION and _server). |
| 10.14 — backend `--cov-fail-under=85` | 85 | gate 82, actual 83 | ⚠️ partial (2026-06-12) | Gate ratcheted 80 → 82 (1pp buffer; tight). Coverage waves added tests for `metadata/reconciliation` (10→83%), `cron/jobs/compaction` (42→100%), `repositories/session_scoring` (34→100%), `core/data_migrations` (50→95%), `utils/tunnel/state` (62→95%), `routers/dashboard` (48→95%), `repositories/views` (66→100%), `utils/sqlite_profiler` (73→95%). Path to 85 % requires attacking one of the large under-covered modules (`rollups.py` 991 stmts/34 %, `routers/admin.py` 822 missing, `_duckdb_status.py` 487 missing, `core/ingest.py` 473 missing); each is several hours of fixtures + assertions. |
| 10.14 — frontend `coverage.thresholds.lines=58` | 58 | gate 58, actual 61.66 | ✅ green (2026-06-12) | Tests added for `lib/toast.ts` (8 → 95.5%), `lib/api/custom-fields.ts` (13 → 100%), `lib/workers/parseJson.ts` (0 → 37.5%; max in jsdom — Worker path unreachable in test env), `components/ProvisionWizard/wizard-config-helpers.ts` (1.8 → 96.4%), `components/ProvisionWizard/wizard-api.ts` (6.5 → 98.4%), and `components/ProvisionWizard/wizard-deploy.ts` (2.3 → 89.7%; runExportTerraform skipped — needs jsdom-unsupported URL.createObjectURL). Overall: 55.19 → 61.66%; gate 53 → 58. **v2.0 target hit.** |
| 10.14 — load-harness CI step green | green | emitter wired; gate enforces | ✅ green (2026-06-12) | `scripts/emit_perf_latest.py` runs a 100K-row synthetic DuckDB workload (~2 s wall) and writes `tests/perf/latest.json`. CI invokes it immediately before `scripts/perf_gate.sh`, which fails on >50 % regression vs `tests/perf/baseline.json` (50 % threshold tuned for GH Actions runner variance at CI scale). Production targets (≤2800 / ≤1900 ms on 36 M rows) documented in `baseline.json` `production_targets_comment` for traceability; enforced by the manual `scripts/dev/loadtest_probe.sh`, not the CI gate. |
| 10.14 — security-regression count ≥ Phase 0 baseline (24) | ≥ 24 | enforced by `scripts/check_security_regression_count.sh` | ✅ green | Pre-commit + CI both run this. |
| 10.14 — mypy strict on touched-module list | strict on touched | per-module strict block landed: 8 modules opted in | ⚠️ partial (2026-06-12) | Per-module `disallow_untyped_defs` / `disallow_incomplete_defs` / `check_untyped_defs` / `warn_return_any` / `warn_unused_ignores` ratchet shipped for the live-query-monitor surface (`query_registry`, `query_instrumentation`, `query_attribution`, `routers/admin_queries`) plus 4 modules covered this session (`metadata/reconciliation`, `cron/jobs/compaction`, `repositories/session_scoring`, `utils/tunnel/state`). Adding a module requires the file to already declare types on every fn/return; extending the list to the full touched-module set is a per-module annotation pass (estimated ~1 fn-per-second on tested modules). |

## Already shipped (cleanup_plan §10.x quick items)

| Item | Status | Evidence |
| --- | --- | --- |
| 10.4 background warmups in main.py | ✅ shipped | Only load-bearing trio remains (POP cache, scoring matrix, Iceberg view pre-warm) |
| 10.5 rich + typer adoption | ✅ shipped | `library_evaluation.md` records both adopted |
| 10.6 httpx-everywhere | ✅ shipped | Only `telemetry_proxy.py` keeps aiohttp (it's a server, allowed) |
| 10.7 Fastly CIDR refresh script | ✅ shipped | `scripts/refresh_fastly_cidrs.py` |
| 10.8 VM-agnostic deploy runbooks | ✅ shipped | `docs/deploy/{aws_ec2,azure_vm,gce,generic_linux}.md` |
| 10.9 file-size sweep | ✅ shipped | All backend files ≤ 1611 lines (rollups.py); all frontend files ≤ 499 lines (post-Phase 9b split) |
| 10.10 doc updates (AGENTS.md / CLAUDE.md / ARCHITECTURE.md) | ✅ shipped | AGENTS.md got the Live Query Monitor section; no `[v2.0-pending]` banners anywhere; no CLAUDE.md exists |
| 10.11 library evaluation summary | ✅ shipped | `local-docs/library_evaluation.md` is complete and accurate |

## What's needed for `git tag v2.0.0`

In order — each bullet unblocks the next:

1. ~~**10.13 burndown**~~ — ✅ DONE 2026-06-12. Override list at **0** (target was ≤ 3; user reset to zero-tech-debt). 36 modules cleared across five waves. Every backend module type-checks under default settings.
2. **10.14 backend coverage push** — current actual 83 %, gate 82. The path to gate=85 requires attacking the large under-covered modules: `rollups.py` (991 stmts, 34 %), `routers/admin.py` (822 missing), `_duckdb_status.py` (487 missing), `core/ingest.py` (473 missing), `repositories/origin.py` (402 missing). Each is several hours of fixtures + assertions.
3. ~~**10.14 frontend coverage push**~~ — ✅ DONE 2026-06-12. Final state: actual 61.66%, gate 58 (the v2.0 target). Tests added across `lib/toast.ts`, `lib/api/custom-fields.ts`, `lib/workers/parseJson.ts`, `components/ProvisionWizard/{wizard-config-helpers,wizard-api,wizard-deploy}.ts`.
4. **10.14 mypy strict on touched modules** — 8 opted-in 2026-06-12 (live-monitor surface + this session's tested modules). Extend the per-module override block in `pyproject.toml` for the rest as each module's `Any` return / untyped-def gaps get annotated.
5. **10.15 final verify** — full `make ci` green; deploy to prod; smoke-test; verify dashboards clean for 15 min (cleanup_plan §"Verification").
6. **10.16** — bump `pyproject.toml` and `frontend/package.json` versions to `2.0.0`, `git tag v2.0.0`, push tag.

## Realistic session estimate

- ~~**10.13 burndown:**~~ ✅ DONE 2026-06-12 — list at 0 entries.
- **10.14 backend coverage push:** Two waves landed 2026-06-12 covered 8 modules; gate ratcheted 80 → 82 (current actual 83 %). Remaining path to gate=85 requires ~500 more covered lines on the heavyweight modules (`rollups.py`, `routers/admin.py`, `_duckdb_status.py`, `core/ingest.py`). Estimate: **2-4 hours** focused on whichever module has the highest leverage per test-LOC.
- ~~**10.14 frontend coverage push:**~~ ✅ DONE 2026-06-12.
- **10.14 mypy strict on touched modules:** Per-phase opt-in; each module is a small ratchet. Estimate: **2-4 hours** across the modules that ARE clean today.
- **10.15 + 10.16:** Mechanical once the above land. **30 min - 1 hour**.

Total: **11-21 hours** of focused work across 3-5 sessions to clear all v2.0.0 gates.

## Cross-references

- Plan source: [cleanup_plan.md](cleanup_plan.md) §10 (Final cleanup).
- mypy burndown procedure: `pyproject.toml` `[[tool.mypy.overrides]]` comment block.
- Coverage convention: "current actual − 2pp" — documented in `.github/workflows/ci.yml` gate-line comments.
- Library decisions: [`local-docs/library_evaluation.md`](../local-docs/library_evaluation.md).
- Surprises across cleanup phases: [`local-docs/surprises.md`](../local-docs/surprises.md).
