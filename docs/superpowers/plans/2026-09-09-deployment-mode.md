# Unified Deployment Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the public `INGEST_MODE` and `SERVING_MODE` configuration with one `DEPLOYMENT_MODE` setting while preserving both supported v3 topologies and feature behavior.

**Architecture:** `DEPLOYMENT_MODE=standard` selects synchronous ingest and file-backed serving. `DEPLOYMENT_MODE=high_throughput` selects Celery ingest and durable serving. Backend branches use `DEPLOYMENT_MODE` or shared predicates directly; the removed mode variables are not retained as derived settings.

**Tech Stack:** Python/FastAPI, pytest, Helm templates, Docker Compose, TypeScript/Vitest fixtures, Markdown documentation, ruff, mypy.

## Global Constraints

- Supported deployment values are exactly `standard` and `high_throughput`.
- `standard` must preserve synchronous ingest and file-backed serving.
- `high_throughput` must preserve Celery ingest and durable serving.
- High-throughput mode still requires `CELERY_BROKER_URL`, a Postgres `DUCKLAKE_CATALOG`, and a Postgres `METADATA_DSN`.
- The serving tier remains single-pod; only ingest workers scale horizontally.
- Remove all code and documentation references to `INGEST_MODE` and `SERVING_MODE` as configuration variables.
- Do not change feature behavior, storage layout, or the supported v2 teardown/reprovision policy.
- Use `uv run pytest`, `npm run test`, and existing repository CI commands only.

---

### Task 1: Replace the configuration contract

**Files:**
- Modify: `backend/config.py:56-56, 385-386, 435, 693-775`
- Test: `tests/test_validate_ingest_mode.py`
- Test: `tests/core/test_durable_serving.py`
- Test: `tests/core/test_request_metrics.py`
- Test: `tests/repositories/test_durable_rollup_fallbacks.py`

**Interfaces:**
- Produces `DEPLOYMENT_MODE`, `is_high_throughput_mode(source: dict | None = None) -> bool`, and `validate_deployment_mode() -> None`.
- `config_to_source()` emits `deployment_mode`, not `ingest_mode` or `serving_mode`.
- `is_durable_serving_mode()` remains available for callers that need the serving semantic, but derives its result from `deployment_mode` and the Postgres catalog.

- [ ] **Step 1: Write failing configuration tests**

Add tests that set `DEPLOYMENT_MODE` to `standard` and `high_throughput`, assert the corresponding `config_to_source()` value, and assert that an unknown value raises `RuntimeError` containing `DEPLOYMENT_MODE`.

Add tests that assert high-throughput validation fails independently for a missing broker, non-Postgres DuckLake catalog, and non-Postgres metadata DSN. Add a test that standard mode does not require those dependencies.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
uv run pytest tests/test_validate_ingest_mode.py tests/core/test_durable_serving.py tests/core/test_request_metrics.py tests/repositories/test_durable_rollup_fallbacks.py -q
```

Expected: failures because `DEPLOYMENT_MODE` and the new predicates do not exist and current fixtures still patch the removed variables.

- [ ] **Step 3: Implement the unified configuration**

Replace the two module-level environment reads with:

```python
DEPLOYMENT_MODE = os.getenv("DEPLOYMENT_MODE", "standard").strip().lower()


def is_high_throughput_mode(source: dict | None = None) -> bool:
    mode = (source or {}).get("deployment_mode") or DEPLOYMENT_MODE
    return mode == "high_throughput"
```

Update `config_to_source()` to reject non-v3 raw layouts whenever `is_high_throughput_mode()` is true and to emit `deployment_mode`.

Update `is_durable_serving_mode()` to require `is_high_throughput_mode(source)` and a Postgres DuckLake catalog. Rename `validate_ingest_mode()` to `validate_deployment_mode()` and validate the two deployment values before applying high-throughput dependency checks.

- [ ] **Step 4: Update focused tests**

Replace monkeypatches of `INGEST_MODE` and `SERVING_MODE` with `DEPLOYMENT_MODE` and update assertions to use `deployment_mode`.

- [ ] **Step 5: Run the focused tests**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/config.py tests/test_validate_ingest_mode.py tests/core/test_durable_serving.py tests/core/test_request_metrics.py tests/repositories/test_durable_rollup_fallbacks.py
git commit -m "Unify deployment topology configuration"
```

### Task 2: Migrate backend runtime branches

**Files:**
- Modify: `backend/cron/jobs/compaction.py`
- Modify: `backend/cron/jobs/commit.py`
- Modify: `backend/cron/jobs/rum_ledger.py`
- Modify: `backend/cron/jobs/sync.py`
- Modify: `backend/cron/scheduler.py`
- Modify: `backend/main.py`
- Modify: `backend/core/duckdb.py`
- Modify: `backend/core/iceberg/_ducklake.py`
- Modify: `backend/provision/orchestrator.py`
- Modify: `backend/routers/admin/health.py`
- Test: `tests/test_provision_orchestrator.py`
- Test: `tests/routers/test_bootstrap.py`
- Test: `tests/routers/test_admin_health_snapshot.py`
- Test: `tests/routers/test_invite_analyst.py`

**Interfaces:**
- Runtime mode checks use `config.is_high_throughput_mode(source)` or `config.DEPLOYMENT_MODE`.
- Startup calls `config.validate_deployment_mode()`.
- Health, bootstrap, scheduler, RUM, commit, compaction, analyst invite, and DuckLake paths retain their current behavior for both deployment modes.

- [ ] **Step 1: Replace each direct mode check**

Convert checks such as:

```python
if svcconfig.INGEST_MODE == "celery":
```

to:

```python
if svcconfig.is_high_throughput_mode(source):
```

Use the module-level `config.DEPLOYMENT_MODE` only where no service source is available. Replace environment reads of `INGEST_MODE` with `config.DEPLOYMENT_MODE == "high_throughput"` and rename error messages to refer to `DEPLOYMENT_MODE=high_throughput`.

- [ ] **Step 2: Update source-field consumers**

Replace `source["serving_mode"]` reads with `source["deployment_mode"]` or the shared predicate. Remove all source construction and test fixtures for `ingest_mode` and `serving_mode`.

- [ ] **Step 3: Update tests before running them**

Replace patches of `backend.config.INGEST_MODE` and `backend.config.SERVING_MODE` with `backend.config.DEPLOYMENT_MODE`, and update fixture dictionaries to use `deployment_mode`.

- [ ] **Step 4: Run backend runtime tests**

Run:

```bash
uv run pytest tests/test_provision_orchestrator.py tests/routers/test_bootstrap.py tests/routers/test_admin_health_snapshot.py tests/routers/test_invite_analyst.py tests/core/test_durable_serving.py -q
```

Expected: PASS with both deployment modes covered.

- [ ] **Step 5: Run static checks for removed names**

Run:

```bash
rg -n 'INGEST_MODE|SERVING_MODE|ingest_mode|serving_mode' backend tests
```

Expected: no configuration-variable or source-field matches.

- [ ] **Step 6: Commit**

```bash
git add backend tests
git commit -m "Route runtime mode checks through deployment mode"
```

### Task 3: Update Compose and Helm deployment contracts

**Files:**
- Modify: `docker-compose.multipod.yml`
- Modify: `deploy/chart/fastly-log-analytics/values.yaml`
- Modify: `deploy/chart/fastly-log-analytics/templates/_helpers.tpl`
- Modify: `deploy/chart/fastly-log-analytics/templates/validate.yaml`
- Modify: `deploy/chart/fastly-log-analytics/README.md`
- Test: `tests/chart/test_helm.py`
- Test: `frontend/tests/backend-environment.ts`
- Test: `frontend/__tests__/tests/backend-launch.test.ts`

**Interfaces:**
- Compose high-throughput services receive `DEPLOYMENT_MODE=high_throughput`.
- Helm exposes `config.deploymentMode` with values `standard` and `high_throughput`.
- Helm emits one `DEPLOYMENT_MODE` variable to backend, worker, and beat.
- Helm validation derives all high-throughput prerequisites from `config.deploymentMode`.

- [ ] **Step 1: Write failing chart and environment assertions**

Update tests to expect `DEPLOYMENT_MODE=standard` for the default chart and `DEPLOYMENT_MODE=high_throughput` for the worker topology. Assert that rendered manifests contain no `INGEST_MODE` or `SERVING_MODE` environment variables.

- [ ] **Step 2: Update chart values and templates**

Replace `config.ingestMode` and `config.servingMode` with:

```yaml
config:
  deploymentMode: standard
```

Change the shared Helm environment block to emit:

```yaml
- name: DEPLOYMENT_MODE
  value: {{ .Values.config.deploymentMode | quote }}
```

Update template gates so `high_throughput` requires the existing Postgres catalog, metadata DSN, and broker values. Keep scheduler and SSE backplane settings independent.

- [ ] **Step 3: Update Compose**

Replace every paired mode assignment in `docker-compose.multipod.yml` with a single `DEPLOYMENT_MODE=high_throughput` assignment.

- [ ] **Step 4: Update frontend environment fixtures**

Replace the two environment keys in `frontend/tests/backend-environment.ts` and backend-launch tests with `DEPLOYMENT_MODE`, preserving standard and high-throughput fixture coverage.

- [ ] **Step 5: Run chart and frontend tests**

Run:

```bash
uv run pytest tests/chart/test_helm.py -q
cd frontend && npm run test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.multipod.yml deploy/chart/fastly-log-analytics frontend/tests frontend/__tests__/tests/backend-launch.test.ts tests/chart/test_helm.py
git commit -m "Expose deployment mode in deployment manifests"
```

### Task 4: Update examples, documentation, benchmarks, and operational text

**Files:**
- Modify: `.env.example`
- Modify: `AGENTS.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/MIGRATION_v3.0.0_GUIDE.md`
- Modify: `docs/adr/14-ducklake-replacement.md`
- Modify: `docs/adr/15-multi-writer-topology.md`
- Modify: `docs/adr/17-analyst-path-a-ducklake.md`
- Modify: `docs/adr/18-serving-tier-single-pod.md`
- Modify: `docs/adr/20-clickhouse-serving-plane.md`
- Modify: `docs/runbooks/clickhouse-prototype.md`
- Modify: `tests/load/clickhouse_serving_benchmark.py`
- Modify: `tests/load/test_clickhouse_serving_benchmark.py`

**Interfaces:**
- Operator-facing examples use `DEPLOYMENT_MODE=standard` or `DEPLOYMENT_MODE=high_throughput`.
- Documentation describes standard and high-throughput as two supported v3 deployment modes.
- No documentation presents `INGEST_MODE` or `SERVING_MODE` as configurable variables.

- [ ] **Step 1: Update the environment example**

Replace the paired commented examples with:

```text
# DEPLOYMENT_MODE=standard
# DEPLOYMENT_MODE=high_throughput
```

Explain that high-throughput mode requires the broker and both Postgres DSNs.

- [ ] **Step 2: Update architecture and ADR language**

Describe the two deployment modes using the new names and preserve the distinction that only ingest scales horizontally. Replace references to `validate_ingest_mode()` with `validate_deployment_mode()`.

- [ ] **Step 3: Update migration guidance**

Keep the teardown/reprovision policy, but describe the v3 choice as standard versus high-throughput deployment mode rather than a v2 versus v3 mode switch.

- [ ] **Step 4: Update benchmarks and changelog**

Change benchmark environment checks and command examples to use `DEPLOYMENT_MODE=high_throughput`. Update release text without removing historical context about the underlying sync and Celery data planes.

- [ ] **Step 5: Verify documentation has no old public configuration**

Run:

```bash
rg -n 'INGEST_MODE|SERVING_MODE|ingestMode|servingMode|validate_ingest_mode' . --glob '!docs/superpowers/**'
```

Expected: no matches except historical commit references or text explicitly identifying removed configuration names. Any remaining match must be rewritten or clearly marked as historical.

- [ ] **Step 6: Commit**

```bash
git add .env.example AGENTS.md CHANGELOG.md docs tests/load
git commit -m "Document unified deployment modes"
```

### Task 5: Run the complete validation gate

**Files:**
- Modify: Files identified by a failing formatter, type checker, or targeted test; only changes directly related to the deployment-mode rename are in scope.

**Interfaces:**
- The repository builds and tests with only `DEPLOYMENT_MODE` as the topology environment variable.

- [ ] **Step 1: Run targeted backend and chart tests**

```bash
uv run pytest tests/test_validate_ingest_mode.py tests/core/test_durable_serving.py tests/chart/test_helm.py tests/load/test_clickhouse_serving_benchmark.py -q
```

- [ ] **Step 2: Run frontend tests**

```bash
cd frontend && npm run test
```

- [ ] **Step 3: Run formatting and static checks**

```bash
make lint
make typecheck
make openapi-drift
```

- [ ] **Step 4: Run the repository CI gate**

```bash
make ci
```

Expected: all existing gates pass without lowering coverage, lint, security, or ratchet thresholds.

- [ ] **Step 5: Scan for temporary artifacts and removed configuration**

```bash
git status --short
rg -n 'INGEST_MODE|SERVING_MODE' . --glob '!docs/superpowers/**'
```

Remove any test-only artifacts created during validation and ensure only intentional historical references remain.

- [ ] **Step 6: Commit final validation fixes**

```bash
git add backend/config.py backend/core backend/cron backend/main.py backend/provision backend/routers \
  tests frontend/tests frontend/__tests__/tests/backend-launch.test.ts \
  deploy/chart/fastly-log-analytics docker-compose.multipod.yml .env.example AGENTS.md CHANGELOG.md docs
git commit -m "Verify unified deployment mode rollout"
```
