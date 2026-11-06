.PHONY: test soak lint format scan build deploy deploy-test deploy-check loadtest-campaign admin-reset trim-target

NAME ?= flp
VERBOSE ?= 0
CARGO_BUILD_JOBS ?= 8
export CARGO_BUILD_JOBS
TARGET_MAX_GB ?= 20
AR_wasm32_wasip1 ?= $(shell PATH="$$HOME/.cargo/bin:$$PATH" rustc +1.96.0 --print sysroot 2>/dev/null)/lib/rustlib/$(shell PATH="$$HOME/.cargo/bin:$$PATH" rustc +1.96.0 -vV 2>/dev/null | sed -n 's/^host: //p')/bin/llvm-ar
export AR_wasm32_wasip1
LT_REPO ?= $(HOME)/Projects/fastly-log-analytics-load-testing
LT_CONFIG := $(CURDIR)/fastly-lt.toml
REPO_ROOT := $(abspath $(CURDIR)/../..)

# Cargo never deletes superseded test binaries. Past the cap, drop this crate's
# artifacts and keep the compiled dependencies.
trim-target:
	@size_kb=$$(du -sk ../target 2>/dev/null | cut -f1); \
	if [ -n "$$size_kb" ] && [ "$$size_kb" -gt $$(( $(TARGET_MAX_GB) * 1024 * 1024 )) ]; then \
		echo "target/ is $$(( size_kb / 1024 / 1024 )) GB, over $(TARGET_MAX_GB) GB: removing stale fastly-logs-parquet-shell artifacts"; \
		cargo clean --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell && \
		cargo clean --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell --release && \
		cargo clean --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell --release --target wasm32-wasip1; \
	fi

test: trim-target
	@rc=0; \
	if command -v cargo-nextest > /dev/null || [ -x "$$HOME/.cargo/bin/cargo-nextest" ]; then \
		PATH="$$HOME/.cargo/bin:$$PATH" cargo nextest run --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell -p compute-common --locked --all-targets --no-fail-fast || rc=1; \
	else \
		PATH="$$HOME/.cargo/bin:$$PATH" cargo test --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell -p compute-common --locked --all-targets || rc=1; \
	fi; \
	if [ -d "$(REPO_ROOT)/frontend/lib/cloud-engine" ] && find "$(REPO_ROOT)/frontend/lib/cloud-engine" -name '*.test.ts*' 2>/dev/null | grep -q .; then \
		(cd "$(REPO_ROOT)/frontend" && npx vitest run lib/cloud-engine) || rc=1; \
	elif [ -d "web" ]; then \
		npm --prefix web test || rc=1; \
	fi; \
	PYTHONPATH="$(REPO_ROOT)" uv run --project "$(REPO_ROOT)" pytest -o addopts="-q --strict-markers" infra tools --tb=short || rc=1; \
	exit $$rc

soak:
	PATH="$$HOME/.cargo/bin:$$PATH" cargo nextest run --manifest-path ../Cargo.toml -p fastly-logs-parquet-shell --locked --test ingest --run-ignored ignored-only -E 'test(/^simulation::(two_days_of_1s_ticks_at_52_files_per_period_convert_exactly_without_a_backlog|every_maintenance_stage_gets_a_turn_within_four_intervals_under_load)$$/)'

lint:
	PATH="$$HOME/.cargo/bin:$$PATH" cargo fmt --manifest-path ../Cargo.toml --all -- --check
	PATH="$$HOME/.cargo/bin:$$PATH" cargo clippy --manifest-path ../Cargo.toml --workspace --locked --all-targets --all-features -- -D warnings
	uv run --project "$(REPO_ROOT)" ruff check infra tools
	uv run --project "$(REPO_ROOT)" ruff format --check infra tools
	@if [ -d "web" ]; then npm --prefix web run lint; fi

format:
	PATH="$$HOME/.cargo/bin:$$PATH" cargo fmt --manifest-path ../Cargo.toml --all
	uv run --project "$(REPO_ROOT)" ruff format infra tools

scan:
	uv run --project "$(REPO_ROOT)" python "$(REPO_ROOT)/scripts/check_secret_values.py"
	gitleaks git --staged --redact --no-banner

build:
	PYTHONPATH="$(REPO_ROOT)" uv run --project "$(REPO_ROOT)" tools/release.py build

deploy: scan
	PYTHONPATH="$(REPO_ROOT)" uv run --project "$(REPO_ROOT)" tools/release.py deploy --name $(NAME)

deploy-test: scan
	PYTHONPATH="$(REPO_ROOT)" uv run --project "$(REPO_ROOT)" tools/deploy_test_all.py --name $(NAME) --lt-repo $(LT_REPO)

deploy-check:
	uv run --project $(LT_REPO) lt deploy-check --config $(LT_CONFIG)

loadtest-campaign:
	$(if $(strip $(CAMPAIGN)),,$(error CAMPAIGN is required, e.g. make loadtest-campaign CAMPAIGN=c1))
	uv run --project $(LT_REPO) lt campaign --config $(LT_CONFIG) --campaign $(CAMPAIGN)

admin-reset:
	@set -e; \
	project_root=$$(cd ../.. && pwd); \
	if [ -f "$$project_root/.env" ]; then \
		uv run --env-file "$$project_root/.env" infra/provision.py admin-reset --name "$(NAME)" --apply $(if $(filter 1,$(VERBOSE)),--verbose); \
	else \
		uv run infra/provision.py admin-reset --name "$(NAME)" --apply $(if $(filter 1,$(VERBOSE)),--verbose); \
	fi
