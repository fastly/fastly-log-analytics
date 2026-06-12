# v2.0.0 Tag Readiness

Tracks the gates that must be green before `pyproject.toml` and `frontend/package.json` bump to `2.0.0` and `git tag v2.0.0` runs (cleanup_plan §10.16).

This doc is the **single source of truth** for "is v2 ready?" — every PR that closes a gate adds a ✅ row here and updates the per-gate evidence link.

## Final-cut gates (cleanup_plan §10.12 / §10.13 / §10.14)

| Gate | Plan target | Current state | Status | Notes |
| --- | --- | --- | --- | --- |
| 10.12 — every backend module touched by phases 1–10 at ≥ 80% coverage | per-module ≥ 80% | total 82%, per-module not yet audited | ⚠️ partial | Per-module audit not yet run. Some modules (rollups.py at 34%, repositories/session_scoring.py at 34%, cron/jobs/compaction.py at 42%, etc.) are well below 80%. |
| 10.13 — `tool.mypy.overrides` ignore_errors list ≤ 3 modules | ≤ 3 | **0 entries** ✅ | ✅ green (2026-06-12) | Reduced 36 → 32 → 22 → 16 → 10 → **0** across five 2026-06-12 burndown waves (36 modules cleared, target exceeded — zero technical debt per user direction). Wave-5 cleared the heaviest tier: `core/ingest` (var-annotations + tuple|None narrowing), `provision/fastly_api` (Match|None narrowing + typed update_payload), `provision/orchestrator` (private iceberg attrs via `type: ignore[attr-defined]` + service_id narrowing + preset None-check), `repositories/origin` (single `assert rollup_row is not None` collapsed 11 indexing errors), `routers/admin` (renamed shadowed loop var + cast _QueueFile for zipfile + service_id narrowing), `routers/provision` (typed `cfg: dict[str, Any]` + bucket_name narrowing + log_fields_raw pull-out), `routers/services/core` (`type: ignore[attr-defined]` for iceberg private API), `routers/session_scoring_admin` (per-line `# type: ignore[has-type]` for circular-import limitation), `utils/remote_access` (assert datetime narrowing + `type: ignore[attr-defined]` for body_iterator on StreamingResponse), `utils/telemetry_proxy` (typed `data: Any` + asserts on _SESSION and _server). |
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

1. ~~**10.13 burndown**~~ — ✅ DONE 2026-06-12. Override list at **0** (target was ≤ 3; user reset to zero-tech-debt). 36 modules cleared across five waves. Every backend module type-checks under default settings.
2. **10.14 backend coverage push** — write tests for the modules below 80% per `10.12` audit (the per-module ≥ 80% goal). Once total clears 85% with the 2pp convention buffer, ratchet `--cov-fail-under=85`.
3. ~~**10.14 frontend coverage push**~~ — ✅ DONE 2026-06-12. Final state: actual 61.66%, gate 58 (the v2.0 target). Tests added across `lib/toast.ts`, `lib/api/custom-fields.ts`, `lib/workers/parseJson.ts`, `components/ProvisionWizard/{wizard-config-helpers,wizard-api,wizard-deploy}.ts`.
4. **10.14 mypy strict on touched modules** — for each module not in `ignore_errors`, add per-module `strict = true` override. Project-wide `strict` flag stays false (intentional — opt-in module by module per cleanup_plan §"Cross-cutting workstream — mypy ratchet").
5. **10.15 final verify** — full `make ci` green; deploy to prod; smoke-test; verify dashboards clean for 15 min (cleanup_plan §"Verification").
6. **10.16** — bump `pyproject.toml` and `frontend/package.json` versions to `2.0.0`, `git tag v2.0.0`, push tag.

## Realistic session estimate

- ~~**10.13 burndown:**~~ ✅ DONE 2026-06-12 — list at 0 entries.
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
