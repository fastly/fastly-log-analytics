# v2.0 Pick-Up Guide

Handoff for whoever (you, future-me, or another collaborator) opens the
next session on `refactor/cleanup`. Reads top-to-bottom in ~3 minutes.

The authoritative status table is [`v2_tag_readiness.md`](v2_tag_readiness.md).
This file is the **next-actions** companion: which gate to attack next,
what file to open first, how to verify you didn't break anything.

---

## What's done (don't re-do)

| Gate | State | Where to confirm |
|---|---|---|
| Frontend coverage `lines=58` | ✅ green (actual 62 %) | `.github/workflows/ci.yml` step "Tests (vitest with coverage)" |
| `tool.mypy.overrides` ignore_errors list ≤ 3 | ✅ at **0** | `pyproject.toml` — block is intentionally empty with a comment |
| Load-harness CI step | ✅ enforcing | `scripts/emit_perf_latest.py` + `scripts/perf_gate.sh`, wired in `ci.yml` |
| Security-regression count ≥ 24 | ✅ enforced | `scripts/check_security_regression_count.sh` |

If you ever find yourself touching one of these, you're probably solving
a *symptom* of something else. The gates above were closed in a focused
push and shouldn't regress silently — CI catches it.

---

## What's left (in priority order)

### 1. Backend coverage 83 → 85 % (~500 covered lines)

CI gate is currently `--cov-fail-under=82` (1 pp buffer over actual).
Each percentage point on the project is ~262 lines.

**The big four under-covered modules:**

| File | Stmts | Cov | Missing |
|---|---|---|---|
| [backend/core/rollups.py](../backend/core/rollups.py) | 991 | 34 % | 650 |
| [backend/routers/admin.py](../backend/routers/admin.py) | 822 | 74 % | 212 |
| [backend/core/_duckdb_status.py](../backend/core/_duckdb_status.py) | 487 | 60 % | 197 |
| [backend/core/ingest.py](../backend/core/ingest.py) | 473 | 75 % | 119 |

**Recommended next bite:**

`backend/routers/admin.py` — 26 endpoints, ~half tested. The
streaming-cleanup SSE handler (lines ~984-1061) and the metadata-
retention PUT (894-932) are the two biggest untested blocks. Both
exercise the `TestClient` + `monkeypatch` patterns already established
in [`tests/routers/test_dashboard_router.py`](../tests/routers/test_dashboard_router.py) (good template — it
covers a composite endpoint with stubbed repositories). Covering 100
lines here = +0.4 pp.

**The slow one — `rollups.py`** — biggest single win (650 missing) but
needs DuckDB + parquet fixtures. The 4 existing rollup test files
(`tests/core/test_rollups_*.py`) show the fixture pattern; reuse
them. Realistic target: cover the bundle-builders and the cleanup
helpers without trying to test the whole module. Budget ~2 h.

**How to measure:**

```bash
.venv/bin/pytest --cov=backend --cov-report=term --cov-fail-under=0 -q | tail -3
```

When actual clears 85 %, ratchet the gate in
[`ci.yml`](../.github/workflows/ci.yml) (search `--cov-fail-under=82`).

### 2. mypy strict on remaining touched modules

`pyproject.toml` has a per-module strict block (currently 8 modules):

```toml
[[tool.mypy.overrides]]
module = [
    "backend.core.query_registry",
    "backend.core.query_instrumentation",
    "backend.core.query_attribution",
    "backend.core.metadata.reconciliation",
    "backend.cron.jobs.compaction",
    "backend.repositories.session_scoring",
    "backend.routers.admin_queries",
    "backend.utils.tunnel.state",
]
disallow_untyped_defs = true
disallow_incomplete_defs = true
check_untyped_defs = true
warn_return_any = true
warn_unused_ignores = true
```

To add a module: append to the list, run `.venv/bin/mypy backend/`,
fix-or-annotate whatever surfaces (usually missing return types on
nested helpers + the occasional `Any`-return). The pattern from this
session: extract types from existing call sites, annotate function
signatures, then re-run.

Path of least resistance: modules that already have ~95 %+ test
coverage tend to be the cleanest typed. Check coverage first, then try
adding to the strict block.

### 3. Final 10.15 verify + 10.16 tag

Mechanical once the above two land. Sequence:

1. `make ci` green locally
2. Deploy refactor/cleanup to prod via `~/restart.sh` on the GCE VM
3. Hard-refresh browser, smoke-test dashboard / security / query / admin
4. Watch OTel dashboards for 15 min, confirm clean
5. Bump `pyproject.toml` version to `2.0.0`, bump `frontend/package.json` version
6. `git tag v2.0.0 && git push origin v2.0.0`

Per [verify-dev-first](../README.md) — local dev verify before prod.

---

## Day-1 recap for context

If you're reading this fresh (or weeks later), the 2026-06-12 session
moved the bar significantly:

- Frontend coverage gate: 53 → **58** (target hit; actual 55 → 62 %).
- mypy `ignore_errors` list: 36 → **0** (target was ≤3; user reset to
  zero tech debt mid-session, achieved by 5 burndown waves).
- Load-harness emitter: scaffold → **wired CI step**, enforcing >50 %
  regression vs synthetic 100K-row baseline.
- Backend coverage gate: 80 → **82** (actual 81 → 83 %, 9 modules
  covered including `metadata/reconciliation`, `cron/jobs/compaction`,
  `repositories/session_scoring`, `core/data_migrations`,
  `utils/tunnel/state`, `routers/dashboard`, `repositories/views`,
  `utils/sqlite_profiler`, `metadata/state`).
- mypy strict ratchet: **8 modules opted in** (live-query-monitor
  surface + the modules with high test coverage from this session).

Three real bugs surfaced during the mypy burndown and were fixed in
place (none cosmetic): `repositories/network.py:260` was passing the
DuckDB connection where `get_asn_names` expected `service_id` (the
suppressed override was hiding it); `routers/share_auth.py:125,203`
had a `iso_z_now() and 24*60*60` cookie max_age expression where the
`and` was a no-op leftover; `routers/admin.py:1383` had a shadowed `b`
loop variable that mypy couldn't reason about.

---

## Per-file change index (recent commits)

If you're trying to figure out *what changed and why* for any file,
the cleanup-branch git log is the source of truth — commits are
focused and message-rich. Key patterns to grep for:

- `chore(mypy):` — mypy burndown / strict ratchet
- `test(backend):` — backend coverage wave
- `test(frontend):` — frontend coverage wave
- `ci(perf):` — load-harness work
- `feat(live-monitor):` — live monitor v2 + sound removal
- `refactor(live-monitor):` — page.tsx split into hooks

```
git log --oneline --grep='cleanup_plan §10\|chore(mypy)\|test(backend)\|test(frontend)\|ci(perf)' refactor/cleanup
```

---

## Pitfalls observed this session

- **Parallel user-coding workflow** ([memory link](../README.md)).
  The user often codes in parallel; a mid-flight `git status` may show
  THEIR work in the tree. Verify with `git diff HEAD` before reacting
  to "your file was modified" reminders, and **commit by pathspec**
  rather than `git add -A` to avoid sweeping their staged work into
  your commit.
- **Pre-commit ruff format** rewrites files. After a hook-failure
  commit, re-stage the formatted files and try again — do NOT amend.
- **`coverage.thresholds.lines` ratchet convention** is "current
  actual − 2pp". The frontend gate's progression
  (53 → 55 → 56 → 57 → 58) shows the cadence.
- **mypy `[has-type]` errors from circular imports** can't be
  resolved with `cast()` because the type isn't yet determined at the
  cast site. Use per-line `# type: ignore[has-type]` instead
  (see `backend/routers/session_scoring_admin.py`).
- **Pyright/mypy + Playwright `?` keypress.** Real Chrome reports
  `event.key === '?'`; Playwright reports `event.key === '/'` +
  `shiftKey: true`. The `useKeyboardShortcuts` hook now normalises both
  via a `logicalKey()` helper — bear in mind for any other shortcut
  binding that uses a shifted-character key.

---

## How long this realistically takes

Both remaining items are bounded but slow:

- **Backend 83 → 85 %**: 2-4 focused hours. Each test file covers
  ~50-100 lines, so 5-10 test files. The big modules each need real
  fixtures, not just mocks.
- **mypy strict full ratchet**: 2-4 hours. ~100 candidate modules; most
  are 1-5 minutes of annotation per module once you know the pattern.

Plus 30 min for verify + tag. Call it **5-9 hours total** to ship
v2.0.0 from here.

---

## What this doc is NOT

- Not the v2.0 plan — that's [`cleanup_plan.md`](cleanup_plan.md) §10.
- Not the status table — that's [`v2_tag_readiness.md`](v2_tag_readiness.md).
- Not the design rationale for any specific feature — those live in the
  feature-specific docs in this folder or were squashed into commit
  messages (`git log --grep=...` is your friend).

This file is purely "where to put your hands first when you sit down".
Delete it after v2.0 ships.
