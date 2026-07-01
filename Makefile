.PHONY: test test-ci lint lint-frontend format typecheck ci install install-hooks dev clean gen-types verify-deps secret-scan security-scan-bandit deps-check knip osv outdated perf perf-ci security-regression baseline verify ratchet scorer-package scorer-test scorer-audit test-frontend-ci openapi-drift e2e

# Prevent a VIRTUAL_ENV from another project leaking into uv commands
unexport VIRTUAL_ENV

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	uv sync
	cd frontend && npm ci

install-hooks:
	uv run pre-commit install

# Backend + frontend with hot reload. Thin alias for the documented
# `./run.sh --dev` entry point so `make dev` (README Development section)
# works alongside the other make targets.
dev:
	./run.sh --dev

# ── Scorer (Fastly Compute) ─────────────────────────────────────────────────

# Rebuild the matrix-less scorer Wasm package and refresh the committed
# artifact the backend ships + deploys via the Fastly API. Run this off-VM
# (needs rustup-pinned Rust 1.90 + the Fastly CLI) whenever compute/scorer/src
# changes, then commit compute/scorer/pkg/session-scorer.tar.gz. The matrix is
# NOT embedded — it's served from the scoring_matrix KV Store at runtime.
# --auto-yes auto-approves the fastly.toml post_build (wasm-opt) script prompt;
# without it the CLI blocks on a confirmation that aborts non-interactively.
scorer-package:
	cd compute/scorer && PATH="$$HOME/.cargo/bin:$$PATH" fastly compute build --auto-yes
	@echo "Built compute/scorer/pkg/session-scorer.tar.gz — commit it."

# Run the Rust scorer's native unit tests (normalize/cookie/matrix
# cross-language parity, session-expiry boundaries, wire-format round-trips).
# These run at native speed (the dev profile builds for the host, not Wasm), so
# `cargo test` needs no Fastly CLI — just the rust-toolchain.toml-pinned Rust.
# Skips with a warning if cargo isn't on PATH (mirrors vcl-test / secret-scan);
# the Scorer CI job hard-requires it. The pre-push hook runs it when
# compute/scorer/ changes.
scorer-test:
	@if command -v cargo > /dev/null || [ -x "$$HOME/.cargo/bin/cargo" ]; then \
		PATH="$$HOME/.cargo/bin:$$PATH" cargo test --manifest-path compute/scorer/Cargo.toml --locked; \
	else \
		echo "⚠️  Skipping scorer-test: cargo not on PATH."; \
		echo "    Install: https://rustup.rs   (rust-toolchain.toml pins the version)"; \
	fi

# ── Backend ───────────────────────────────────────────────────────────────────

test:
	uv run pytest

test-v:
	uv run pytest -v

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run mypy backend/

# R-9: enforce no cross-router imports + core ↛ routers. Same gate CI
# runs in .github/workflows/ci.yml. Pre-existing edges are baselined
# in pyproject.toml [tool.importlinter]; new ones fail.
import-contracts:
	uv run lint-imports

# ── Frontend ──────────────────────────────────────────────────────────────────

gen-types:
	cd frontend && npm run gen:types

test-frontend:
	cd frontend && npm test

typecheck-frontend: gen-types
	cd frontend && npx tsc --noEmit

# Frontend ESLint count-ceiling gate. ESLint is otherwise gated nowhere
# (CI runs the Python import-linter; `make lint` is ruff), so `as any` and
# rules-of-hooks violations had been accumulating unchecked. The gate fails
# if the error count rises above the committed ceiling; ratchet it DOWN as
# violations are removed. See scripts/check_eslint_count.sh.
lint-frontend:
	bash scripts/check_eslint_count.sh

# ── Combined ──────────────────────────────────────────────────────────────────

outdated:
	@echo "Checking for outdated Python packages..."
	@uv pip list --outdated || true
	@echo "\nChecking for outdated Node packages..."
	@cd frontend && npm outdated || true

osv:
	uv run python scripts/check_osv.py

# Secret scanner — gitleaks, configured via .gitleaks.toml at repo root.
# Scans git history by default (no `--no-git`), so any committed credential
# trips the gate even if later removed. Use `gitleaks detect --no-git`
# locally to also scan the working tree (catches secrets in untracked /
# unstaged files before you accidentally `git add` them).
#
# Suppression mechanisms in increasing scope:
#   - inline `#gitleaks:allow` on the offending line
#   - .gitleaksignore — fingerprint list for one-off historical findings
#   - .gitleaks.toml [allowlist] paths — for whole files / directories
#
# Skips cleanly with a loud warning if the binary isn't on PATH. Production
# CI installs it via curl in .github/workflows/ci.yml (same pattern as falco).
secret-scan:
	@if command -v gitleaks > /dev/null; then \
		gitleaks detect --no-banner --redact --config .gitleaks.toml --exit-code 1; \
	else \
		echo "⚠️  Skipping secret-scan: gitleaks not on PATH."; \
		echo "    Install: brew install gitleaks   (or see https://github.com/gitleaks/gitleaks#installing)"; \
		echo "    Pre-commit + CI install it automatically — local dev is recommended."; \
	fi

# Bandit pattern-based Python security scan. Gated at HIGH severity
# only — the MEDIUM tier is dominated by B608 (string-built SQL) false
# positives the codebase defends against via backend/utils/sql_validator.py
# + escape_sql_literal, which bandit can't see. HIGH catches the real
# issues (weak-hash misuse, eval/exec, shell=True). Run ad-hoc with
# ``uv run bandit -r backend/ -ll`` to also see the noisy mediums.
security-scan-bandit:
	uv run bandit -r backend/ -lll --quiet

# Deptry dep-hygiene scan: catches direct imports of transitive deps
# (DEP003), unused declared deps (DEP002), missing declared deps
# (DEP001). Per-rule ignores live in pyproject.toml [tool.deptry] for
# the runtime-only and extras-served cases that can't be detected
# statically. A NEW transitive import or NEW unused dep fails this.
deps-check:
	uv run deptry backend/

# Knip frontend dead-code scan: dead exports + unused deps. Config in
# frontend/knip.config.ts. Advisory target — useful after a refactor
# to surface ``export``s no one consumes any more. NOT wired into
# ``make ci`` because the baseline still has ~27 intentional cases
# (public API types in types/api.ts, internal helpers that should
# drop their ``export`` keyword) that need per-export curation.
knip:
	# ``--no-exit-code`` keeps the target advisory — the report prints
	# but the build doesn't fail. Tightening to gating requires
	# per-export curation of the ~27 baseline cases (public API types
	# in types/api.ts, internal helpers that should drop their
	# ``export`` keyword).
	cd frontend && npx knip --no-progress --no-exit-code

# Verify package.json + package-lock.json resolve cleanly under `npm ci`.
# Local `make ci` previously used the already-installed node_modules and
# silently tolerated peer-dep conflicts that would break GitHub Actions
# (which runs `npm ci` from scratch). Use --dry-run so this stays fast.
verify-deps:
	@cd frontend && npm ci --dry-run --silent && echo "frontend deps resolve cleanly"

vcl-test:
	@if command -v falco > /dev/null; then \
		uv run pytest tests/core/test_vcl_semantics.py; \
	else \
		echo "Skipping VCL tests: falco linter not found in PATH"; \
	fi

# Run the underlying targets in parallel with a -j2 cap. Backend pytest
# (~26s) and frontend vitest (~35s) are the two long poles; running them
# concurrently saves ~25-30s wall vs. sequential, and the -j2 cap keeps
# them from oversubscribing the box (both invocations already parallelise
# internally via pytest-xdist / vitest workers).
#
# Order matters here — make's scheduler picks leftmost-available targets
# first, so the slow ones (`test`, `test-frontend`) are listed first to
# claim the two parallel slots immediately. Lighter checks fill in as
# slots free up.
# ── CI parity targets ─────────────────────────────────────────────────────────
# These mirror the exact steps .github/workflows/ci.yml runs so `make ci`
# catches what GitHub catches. The bare `test` / `test-frontend` above stay
# light for quick local loops; `ci` uses the gated variants below.

# Backend tests AS CI RUNS THEM: pytest-xdist + the coverage floor + the env
# flags that make falco/terraform-dependent tests HARD-FAIL instead of skip
# (ci.yml "Tests (pytest with coverage)"). MUST use `-n auto` (NOT `-n 4`) to
# match ci.yml's worker count: parallelism-dependent races (e.g. a per-test
# connection close colliding with a live worker thread → segfault) only surface
# at CI's worker density. A lower `-n` here was why `make ci` went green while
# the ci.yml pull_request run segfaulted (2026-06-23).
#
# `nice -n 15` keeps all `-n auto` workers (so race density is unchanged) but
# runs them at low scheduling priority — on a many-core dev Mac `-n auto` pins
# every core and the machine goes unresponsive; niced workers yield to your
# editor/browser so the UI stays smooth, and still get full CPU when you're
# idle. Local ergonomics only: ci.yml runs the `uv run pytest` lines directly
# (it does NOT call this target), and on a dedicated runner nice is a no-op, so
# CI behaviour and worker-density parity are untouched.
#
# Two runs, not one. The `terraform_cli`-marked tests shell out to the real
# `terraform` binary (init + test/validate). They're isolated subprocess work
# that gains NOTHING from xdist, and under the niced `-n auto` pool one such
# test's xdist worker HARD-CRASHED ("worker 'gwN' crashed", no traceback) —
# the niced parent's low priority is inherited by the terraform subprocess,
# starving it inside the already-saturated pool. So:
#   Run 1 — the fast suite, parallel + niced, with `-m "not terraform_cli"`.
#           Writes fresh coverage (NO --cov-append), no --cov-fail-under yet
#           (premature gate), report suppressed (--cov-report=).
#   Run 2 — the heavyweight terraform tests, SERIAL (`-n 0`) and un-niced
#           (normal priority so the subprocess isn't starved), APPENDING
#           coverage (--cov-append) so the single 86% gate evaluates the
#           COMBINED data. This run owns the final --cov-report=term +
#           --cov-fail-under=86.
test-ci:
	FALCO_REQUIRED=1 nice -n 15 uv run pytest -n auto -m "not terraform_cli" --cov=backend --cov-report=
	FALCO_REQUIRED=1 TERRAFORM_VALIDATE=1 uv run pytest -n 0 -m terraform_cli --cov=backend --cov-append --cov-report=term --cov-fail-under=86

# Frontend tests AS CI RUNS THEM: vitest with the four coverage floors
# (GATE-03). Bare `npm test` applies none of these.
test-frontend-ci:
	cd frontend && npx vitest run --coverage --coverage.thresholds.lines=66 --coverage.thresholds.statements=65 --coverage.thresholds.functions=54 --coverage.thresholds.branches=52

# RustSec advisory scan (ci.yml scorer "Audit dependencies"). cargo test
# does NOT check advisories. Skips with a warning if cargo-audit is absent.
scorer-audit:
	@if command -v cargo-audit > /dev/null || [ -x "$$HOME/.cargo/bin/cargo-audit" ]; then \
		PATH="$$HOME/.cargo/bin:$$PATH" cargo audit --file compute/scorer/Cargo.lock; \
	else \
		echo "⚠️  Skipping scorer-audit: cargo-audit not on PATH (cargo install cargo-audit --locked)."; \
	fi

# OpenAPI type-drift guard (ci.yml frontend "Detect drift in generated
# OpenAPI types"). Regenerates then fails if the tracked output changed.
openapi-drift: gen-types
	@cd frontend && git diff --exit-code types/api.generated.ts openapi.json \
		|| { echo "OpenAPI types out of sync — run 'make gen-types' and commit." >&2; exit 1; }

# Perf gate AS CI RUNS IT: emit latest.json from synthetic load, then compare
# to baseline.json (ci.yml "Emit perf samples" + "Perf gate").
perf-ci:
	uv run python scripts/emit_perf_latest.py && bash scripts/perf_gate.sh

# Playwright E2E — mirrors .github/workflows/e2e.yml. Kept SEPARATE from `ci`
# (it boots the backend + frontend + a browser, ~minutes) exactly like the
# split CI workflows. `npm run test:e2e` is chromium-only; this runs the full
# chromium+firefox+webkit matrix CI runs. THE gap that hid the a11y/hydration
# failures locally — run `make e2e` before pushing UI changes.
e2e:
	cd frontend && npm ci && npx playwright install --with-deps && npx playwright test

# Mirrors EVERY gating GitHub workflow (ci.yml AND e2e.yml) step-for-step, so a
# green `make ci` == green CI — no separate command to forget. Runs the full
# backend suite (-n auto, matching ci.yml's worker count), the frontend unit
# suite, every static/security gate, AND the full Playwright e2e matrix
# (chromium+firefox+webkit). Intentionally heavy (several minutes + a browser
# install): use `make test-ci` / `make test-frontend-ci` / `make e2e` for fast
# focused loops, but run the whole `make ci` before pushing to be confident.
# The e2e step runs LAST so the cheaper backend/frontend/static gates fail fast
# before the multi-minute browser matrix.
ci:
	$(MAKE) test-ci
	$(MAKE) test-frontend-ci
	@$(MAKE) -j2 typecheck-frontend lint-frontend lint format-check typecheck import-contracts vcl-test scorer-test scorer-audit verify-deps secret-scan osv otel-guard security-regression openapi-drift perf-ci
	$(MAKE) e2e

# ── v2.0 cleanup targets ──────────────────────────────────────────────────────

# Load-harness perf gate. Reads tests/perf/baseline.json + latest.json.
# Phase 0 ships scaffolding (no-op if latest.json missing). Phase 1.6
# hooks the emitter and turns the skip into a hard fail.
perf:
	bash scripts/perf_gate.sh

# Security-regression count gate. Asserts the
# @pytest.mark.security_regression count is monotonically >= floor
# (Phase 0.8 baseline: 24, from the since-removed audit-findings/ verified fixes).
security-regression:
	bash scripts/check_security_regression_count.sh

# SRE-10 / ADR-08 §5: fail if a tracked deploy file activates the
# OTEL_EXPORTER=console spam mode (the 2026-06-10 prod-stdout-flood incident).
otel-guard:
	bash scripts/check_no_console_otel.sh

# Architectural baseline snapshot. Captures LOC, large files,
# TODO/FIXME markers, # Security: comment count, and mypy ignore
# overrides into .metrics/baseline/<ts>/. Run at Phase 0 + end of
# Phase 10 for the success-criteria scorecard.
baseline:
	bash scripts/baseline_metrics.sh

# Pre-deploy gate. `ci` now runs every gating GitHub workflow itself (ci.yml +
# e2e.yml), so verify is just an alias kept for muscle memory / older docs.
verify: ci

# CI gate ratchet helper. After a phase lifts coverage in a touched
# module, bump the gate in .github/workflows/ci.yml's --cov-fail-under.
# This target prints the current values + suggests the next floor
# ("current actual − 2pp" per existing convention).
ratchet:
	@echo "Current backend gate:"
	@grep -E "cov-fail-under" .github/workflows/ci.yml
	@echo
	@echo "Current frontend gates (lines/statements/functions/branches):"
	@grep -E "coverage.thresholds" .github/workflows/ci.yml
	@echo
	@echo "Edit .github/workflows/ci.yml to bump. Floor: current actual − 2pp."


clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache
