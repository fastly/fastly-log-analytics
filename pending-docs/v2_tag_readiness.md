# v2.0.0 Tag Readiness

Tracks the gates that must be green before `pyproject.toml` and `frontend/package.json` bump to `2.0.0` and `git tag v2.0.0` runs (cleanup_plan §10.16).

This doc is the **single source of truth** for "is v2 ready?" — every PR that closes a gate adds a ✅ row here and updates the per-gate evidence link.

## Final-cut gates (cleanup_plan §10.12 / §10.13 / §10.14)

| Gate | Plan target | Current state | Status | Notes |
| --- | --- | --- | --- | --- |
| 10.12 — every backend module touched by phases 1–10 at ≥ 80% coverage | per-module ≥ 80% | total 82%, per-module not yet audited | ⚠️ partial | Per-module audit not yet run. Some modules (rollups.py at 34%, repositories/session_scoring.py at 34%, cron/jobs/compaction.py at 42%, etc.) are well below 80%. |
| 10.13 — `tool.mypy.overrides` ignore_errors list ≤ 3 modules | ≤ 3 | 16 explicit per-file entries | ⚠️ partial (2026-06-12) | Reduced 36 → 32 → 22 → 16 across three 2026-06-12 burndown waves (20 modules cleared total). Latest wave dropped `utils/rdns_cache` (typed generator return + pycares stub gap), `repositories/dashboard` (widened `_add_bot_columns` to `Collection[str]`), `utils/tunnel/manager` (explicit `pii_policy` narrowing), `repositories/network` (real bug: passed `con` instead of `src["name"]` to `get_asn_names`), `repositories/session_scoring` (None narrowing via if-else + typed list), `routers/admin_usage` (cast `_adm.router` + int-coerce id). 16 remaining are higher-error modules (`repositories/origin` 13 errors, `routers/session_scoring_admin` 11, `provision/fastly_api` 10, etc.). |
| 10.14 — backend `--cov-fail-under=85` | 85 | gate 80, actual 82 | ❌ pending | Bumping the gate alone breaks CI; need ~770 more covered lines first (back-of-envelope: 3pp on 25,611 statements). |
| 10.14 — frontend `coverage.thresholds.lines=58` | 58 | gate 58, actual 61.66 | ✅ green (2026-06-12) | Tests added for `lib/toast.ts` (8 → 95.5%), `lib/api/custom-fields.ts` (13 → 100%), `lib/workers/parseJson.ts` (0 → 37.5%; max in jsdom — Worker path unreachable in test env), `components/ProvisionWizard/wizard-config-helpers.ts` (1.8 → 96.4%), `components/ProvisionWizard/wizard-api.ts` (6.5 → 98.4%), and `components/ProvisionWizard/wizard-deploy.ts` (2.3 → 89.7%; runExportTerraform skipped — needs jsdom-unsupported URL.createObjectURL). Overall: 55.19 → 61.66%; gate 53 → 58. **v2.0 target hit.** |
| 10.14 — load-harness CI step green | green | scaffolded, not yet emitting samples | ⚠️ scaffold-only | `scripts/perf_gate.sh` ships as no-op until Phase 1.6 hooks the emitter. |
| 10.14 — security-regression count ≥ Phase 0 baseline (24) | ≥ 24 | enforced by `scripts/check_security_regression_count.sh` | ✅ green | Pre-commit + CI both run this. |
| 10.14 — mypy strict on touched-module list | strict on touched | non-strict project-wide; per-module strict deferred | ❌ pending | The project's `tool.mypy` block has `strict = false`. Per-phase ratchet not yet started. |

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

1. **10.13 burndown** — fix-or-annotate the 16 modules in the `tool.mypy.overrides` list. 2026-06-12 cleared 20 modules across three waves (zero / single / 1-2 error tiers). Remaining 16 are the multi-error tier: `repositories/origin` (13 errors), `routers/admin` (7), `routers/services/core` (8), `provision/fastly_api` (10), `routers/session_scoring_admin` (11), plus the 3-error trio (`config`, `deps`, `share_auth`, `_base`, `sessions`). Target: list ≤ 3 entries.
2. **10.14 backend coverage push** — write tests for the modules below 80% per `10.12` audit (the per-module ≥ 80% goal). Once total clears 85% with the 2pp convention buffer, ratchet `--cov-fail-under=85`.
3. ~~**10.14 frontend coverage push**~~ — ✅ DONE 2026-06-12. Final state: actual 61.66%, gate 58 (the v2.0 target). Tests added across `lib/toast.ts`, `lib/api/custom-fields.ts`, `lib/workers/parseJson.ts`, `components/ProvisionWizard/{wizard-config-helpers,wizard-api,wizard-deploy}.ts`.
4. **10.14 mypy strict on touched modules** — for each module not in `ignore_errors`, add per-module `strict = true` override. Project-wide `strict` flag stays false (intentional — opt-in module by module per cleanup_plan §"Cross-cutting workstream — mypy ratchet").
5. **10.15 final verify** — full `make ci` green; deploy to prod; smoke-test; verify dashboards clean for 15 min (cleanup_plan §"Verification").
6. **10.16** — bump `pyproject.toml` and `frontend/package.json` versions to `2.0.0`, `git tag v2.0.0`, push tag.

## Realistic session estimate

- **10.13 burndown (16 remaining):** 20 cleared 2026-06-12 across three waves (zero, single, and 1-2-error tiers). Remaining 16 are 3-13 actual errors each; the heaviest (`repositories/origin.py` with 13 `Any | None` indexing errors, `routers/session_scoring_admin` with 11, `provision/fastly_api` with 10) need bulk narrowing. Estimate: **2 hours** to clear or annotate-with-pragma all 16 down to ≤ 3.
- **10.14 backend coverage push:** ~770 covered lines needed. The lowest-coverage modules (`rollups.py` at 34%, `repositories/session_scoring.py` at 34%) are the highest-leverage. Estimate: **4-8 hours** focused.
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
