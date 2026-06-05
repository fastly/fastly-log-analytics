.PHONY: test lint format typecheck ci install install-hooks clean gen-types verify-deps secret-scan osv outdated

# Prevent a VIRTUAL_ENV from another project leaking into uv commands
unexport VIRTUAL_ENV

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	uv sync
	cd frontend && npm ci

install-hooks:
	uv run pre-commit install

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

# ── Frontend ──────────────────────────────────────────────────────────────────

gen-types:
	cd frontend && npm run gen:types

test-frontend:
	cd frontend && npm test

typecheck-frontend: gen-types
	cd frontend && npx tsc --noEmit

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
ci:
	@$(MAKE) -j2 test test-frontend typecheck-frontend lint format-check typecheck vcl-test verify-deps secret-scan osv


clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache
