.PHONY: test lint format typecheck ci install install-hooks clean gen-types verify-deps

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

ci: lint format-check typecheck test vcl-test verify-deps typecheck-frontend test-frontend osv outdated

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache
