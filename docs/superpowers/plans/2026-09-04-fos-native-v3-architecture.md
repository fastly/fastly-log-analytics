# Fastly Object Storage–Native v3 Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make v3.0 a measured, scalable Fastly Object Storage (FOS) log-ingestion and dashboard system without treating local DuckDB files or pod-local Parquet as shared distributed state.

**Architecture:** FOS is the durable raw and Parquet object store. FOS discovery is at-least-once and is backed by periodic reconciliation plus a Postgres ledger with leases, retries, quarantine, and idempotent object identity. Postgres-backed DuckLake is the transactional catalog, with bounded commit concurrency; dashboard serving reads durable shared state through read-only or ephemeral DuckDB connections and uses only disposable local caches.

**Tech Stack:** FastAPI, DuckDB, DuckLake, Parquet, Fastly Object Storage (S3-compatible API), Postgres, Celery/Valkey, RedBeat, APScheduler only for explicitly local jobs, pytest, ruff, mypy, Playwright.

## Global Constraints

- Production v2.4.1 remains unchanged until the clean v3 topology passes all gates.
- FOS, not AWS S3, is the product's object-storage name and deployment target.
- FOS notifications are at-least-once hints; periodic FOS listing/reconciliation remains mandatory.
- No correctness-critical path may depend on a shared native DuckDB file or a file that exists only on one serving pod.
- `INGEST_MODE=celery` requires Postgres-backed metadata and a Postgres-backed DuckLake catalog.
- Raw FOS deletion is allowed only after durable catalog publication and required DuckLake inline-data flush.
- All source-object processing is idempotent and replay-safe.
- Preserve local corrupt runtime databases as explicit backups; never delete them as part of tests.
- Every non-trivial code change gets a regression test and targeted validation before `make ci`.

---

## File Map

**Architecture and deployment contract**

- Modify: `AGENTS.md` — correct any remaining v3 storage/concurrency wording and make FOS-specific guarantees discoverable.
- Modify: `docs/ARCHITECTURE.md` — describe the four planes: FOS landing, distributed ingest, catalog/data, and serving.
- Create: `docs/adr/19-fos-native-v3-topology.md` — record the accepted topology and rejected alternatives.
- Modify: `docker-compose.prod.yml` and deployment documentation only where the topology contract needs an enforceable setting.

**Lock ownership and commit boundary**

- Modify: `backend/core/duckdb.py` — classify and expose DuckDB file ownership/lock failures with process and topology context.
- Modify: `backend/cron/jobs/commit.py` — ensure only the supported commit execution plane can call the file-backed synchronous path.
- Modify: `backend/config.py` — validate the selected v3 topology at boot.
- Test: `tests/core/test_duckdb.py`, `tests/cron/test_commit.py`, `tests/test_config.py`.

**FOS discovery and durable ingest**

- Modify: `backend/core/metadata/ingest_log.py` — use a stable FOS object identity and explicit lease/retry state.
- Modify: `backend/core/ingest.py` — make worker output deterministic and replay-safe.
- Modify: `backend/cron/jobs/sync.py` and `backend/cron/jobs/ledger_sweep.py` — separate reconciliation from dispatch and enforce bounded work.
- Test: `tests/core/test_ingest_stateful.py`, `tests/core/test_ingest.py`, `tests/cron/test_ledger_sweep.py`.

**Bounded DuckLake publication**

- Modify: `backend/core/iceberg/buffer.py` or its DuckLake commit replacement — publish bounded microbatches through one explicit contract.
- Modify: `backend/core/ingest.py` — use the same publication contract from Celery workers.
- Modify: `backend/core/metadata/quarantine.py` — retain failed source identities and replay metadata.
- Test: `tests/test_e2e_pipeline.py`, `tests/core/test_ducklake_*`, `tests/core/test_ingest_stateful.py`.

**Serving isolation and shared acceleration**

- Modify: `backend/core/duckdb_pool.py`, `backend/core/duckdb.py`, and `backend/repositories/_base.py` — make the serving connection mode explicit and prevent accidental cross-process writable-file use.
- Modify: `backend/core/rollups/` and `backend/repositories/_base.py` — move correctness-critical rollup artifacts to shared FOS-backed storage or treat local copies as disposable mirrors.
- Test: `tests/core/test_duckdb_pool.py`, `tests/core/test_rollups_recompute.py`, `tests/repositories/`.

**Measured load verification**

- Create: `tests/load/fos_pipeline_load.py` — bounded, opt-in load driver using configured local credentials without committing them.
- Create: `tests/load/test_pipeline_load_contract.py` — validates metrics and failure criteria without requiring production credentials.
- Modify: `Makefile` — add an explicit local-only load target that refuses to run without a configured FOS/logging endpoint.
- Create: `docs/runbooks/v3-fos-load-test.md` — operator procedure and evidence checklist.

---

### Task 1: Establish and enforce the v3 topology contract

**Files:**
- Modify: `backend/config.py`
- Modify: `backend/core/duckdb.py`
- Modify: `AGENTS.md`
- Modify: `docs/ARCHITECTURE.md`
- Create: `docs/adr/19-fos-native-v3-topology.md`
- Test: `tests/test_config.py`, `tests/core/test_duckdb.py`

**Interfaces:**
- Produces `validate_ingest_mode()` errors that identify the missing Postgres metadata/catalog requirement.
- Produces a structured lock diagnostic containing `service_id`, `pid`, hostname, database path, `INGEST_MODE`, catalog mode, and caller plane.

- [ ] **Step 1: Write topology validation tests**

```python
def test_celery_mode_requires_postgres_catalog_and_metadata(monkeypatch):
    monkeypatch.setenv("INGEST_MODE", "celery")
    monkeypatch.delenv("DUCKLAKE_CATALOG", raising=False)
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with pytest.raises(RuntimeError, match="Postgres"):
        config.validate_ingest_mode()
```

- [ ] **Step 2: Run the focused tests and confirm the new contract is absent**

Run: `uv run pytest tests/test_config.py::test_celery_mode_requires_postgres_catalog_and_metadata -q`

Expected: the test fails because the validation does not yet enforce both requirements.

- [ ] **Step 3: Implement validation and lock diagnostics**

Use one explicit topology object at the config boundary. Do not infer distributed safety from a job name or a local file existing. Preserve the existing single-node sync mode, but reject Celery mode without Postgres-backed coordination.

- [ ] **Step 4: Run focused validation**

Run: `uv run pytest tests/test_config.py tests/core/test_duckdb.py -q`

Expected: all selected tests pass and lock errors contain topology context.

- [ ] **Step 5: Commit**

```bash
git add backend/config.py backend/core/duckdb.py AGENTS.md docs/ARCHITECTURE.md docs/adr/19-fos-native-v3-topology.md tests/test_config.py tests/core/test_duckdb.py
git commit -m "feat: enforce v3 FOS topology contract"
```

### Task 2: Make FOS object discovery replay-safe

**Files:**
- Modify: `backend/core/metadata/ingest_log.py`
- Modify: `backend/core/ingest.py`
- Modify: `backend/cron/jobs/sync.py`
- Modify: `backend/cron/jobs/ledger_sweep.py`
- Test: `tests/core/test_ingest.py`, `tests/core/test_ingest_stateful.py`, `tests/cron/test_ledger_sweep.py`

**Interfaces:**
- `object_identity(bucket, key, etag, size, version_id=None) -> str`
- `claim_ingest_object(identity, lease_owner, lease_ttl_s) -> bool`
- `reconcile_fos_objects(source) -> ReconciliationResult`

- [ ] **Step 1: Add failing identity and duplicate-delivery tests**

```python
def test_same_fos_object_delivery_is_claimed_once(metadata_db):
    identity = object_identity("bucket", "raw/a.gz", "etag-a", 123)
    assert claim_ingest_object(identity, "worker-1", 60) is True
    assert claim_ingest_object(identity, "worker-2", 60) is False
```

- [ ] **Step 2: Run the tests and confirm duplicate claims are possible**

Run: `uv run pytest tests/core/test_ingest.py::test_same_fos_object_delivery_is_claimed_once -q`

Expected: FAIL until the uniqueness and lease transition are implemented.

- [ ] **Step 3: Implement stable FOS identity and reconciliation**

Use object key plus immutable version/ETag/size metadata; never use `Last-Modified` alone. Keep the existing FOS list path as the reconciliation source of truth. Notification/queue consumers may enqueue hints, but a later list must recover missed notifications. Expired claims must be reclaimable, and failed objects must remain visible for quarantine/replay.

- [ ] **Step 4: Run stateful and ledger tests**

Run: `uv run pytest tests/core/test_ingest.py tests/core/test_ingest_stateful.py tests/cron/test_ledger_sweep.py -q`

Expected: duplicate deliveries remain one logical object and expired leases are recovered.

- [ ] **Step 5: Commit**

```bash
git add backend/core/metadata/ingest_log.py backend/core/ingest.py backend/cron/jobs/sync.py backend/cron/jobs/ledger_sweep.py tests/core/test_ingest.py tests/core/test_ingest_stateful.py tests/cron/test_ledger_sweep.py
git commit -m "feat: make FOS discovery replay safe"
```

### Task 3: Unify and bound DuckLake publication

**Files:**
- Modify: `backend/core/iceberg/buffer.py`
- Modify: `backend/core/ingest.py`
- Modify: `backend/core/metadata/quarantine.py`
- Modify: `backend/cron/jobs/commit.py`
- Test: `tests/test_e2e_pipeline.py`, `tests/core/test_ducklake_attach_concurrency.py`, `tests/cron/test_commit.py`

**Interfaces:**
- `publish_ingest_batch(source, batch) -> PublishResult`
- `flush_inlined_data_before_raw_delete(source) -> None`
- `PublishResult` reports source identities, files, rows, snapshot, and durability state.

- [ ] **Step 1: Write crash/replay/durability tests**

```python
def test_replaying_published_fos_object_does_not_duplicate_rows(pipeline_env):
    first = publish_ingest_batch(pipeline_env.source, pipeline_env.batch)
    second = publish_ingest_batch(pipeline_env.source, pipeline_env.batch)
    assert first.rows_published == second.rows_published
    assert pipeline_env.count_rows_for_source(pipeline_env.source_identity) == first.rows_published
```

- [ ] **Step 2: Run the tests and confirm the current paths do not share one contract**

Run: `uv run pytest tests/test_e2e_pipeline.py tests/cron/test_commit.py -q`

Expected: the new replay/durability assertions identify the differing sync and Celery publication behavior.

- [ ] **Step 3: Implement bounded microbatch publication**

Use one publication boundary for sync and Celery paths. Bound batches by both file count and bytes, retry catalog conflicts with bounded backoff, flush DuckLake inlined rows before raw deletion, and write quarantine state for permanent parse/schema failures. Do not increase writer concurrency until catalog lock/transaction metrics demonstrate capacity.

- [ ] **Step 4: Run pipeline and concurrency tests**

Run: `uv run pytest tests/test_e2e_pipeline.py tests/core/test_ducklake_attach_concurrency.py tests/cron/test_commit.py -q`

Expected: replay is idempotent, inlined data is durable before raw deletion, and concurrent attach remains safe.

- [ ] **Step 5: Commit**

```bash
git add backend/core/iceberg/buffer.py backend/core/ingest.py backend/core/metadata/quarantine.py backend/cron/jobs/commit.py tests/test_e2e_pipeline.py tests/core/test_ducklake_attach_concurrency.py tests/cron/test_commit.py
git commit -m "feat: bound DuckLake publication"
```

### Task 4: Remove correctness dependence on serving-pod-local files

**Files:**
- Modify: `backend/core/duckdb_pool.py`
- Modify: `backend/core/duckdb.py`
- Modify: `backend/repositories/_base.py`
- Modify: `backend/core/rollups/`
- Test: `tests/core/test_duckdb_pool.py`, `tests/core/test_rollups_recompute.py`, relevant repository tests

**Interfaces:**
- `open_serving_connection(source, mode="read_only")`
- `rollup_uri(source, partition) -> FOS-backed URI`
- Local cache reads return a cache miss and use durable shared state; they never silently suppress shared rows.

- [ ] **Step 1: Add the serving isolation regression tests**

```python
def test_serving_replica_can_answer_without_another_pod_local_file(shared_source):
    result = query_dashboard(shared_source, local_cache_dir=Path("/empty"))
    assert result.total_rows >= 0
```

- [ ] **Step 2: Run the tests and identify pod-local assumptions**

Run: `uv run pytest tests/core/test_duckdb_pool.py tests/core/test_rollups_recompute.py -q`

Expected: tests expose paths that require local Parquet or writable per-service DuckDB state.

- [ ] **Step 3: Implement explicit serving modes**

Make read-only/ephemeral serving the distributed mode. Retain local DuckDB pooling only for the single-node deployment. Store correctness-critical rollups in FOS or regenerate them from durable Parquet; local copies are disposable accelerators. Do not let shared metadata point at a local-only file.

- [ ] **Step 4: Run repository and rollup tests**

Run: `uv run pytest tests/core/test_duckdb_pool.py tests/core/test_rollups_recompute.py tests/repositories -q`

Expected: a clean serving replica can answer from shared durable state and local cache loss becomes a recoverable cache miss.

- [ ] **Step 5: Commit**

```bash
git add backend/core/duckdb_pool.py backend/core/duckdb.py backend/repositories/_base.py backend/core/rollups tests/core/test_duckdb_pool.py tests/core/test_rollups_recompute.py tests/repositories
git commit -m "feat: isolate distributed serving from local files"
```

### Task 5: Add measured FOS load and failure verification

**Files:**
- Create: `tests/load/fos_pipeline_load.py`
- Create: `tests/load/test_pipeline_load_contract.py`
- Modify: `Makefile`
- Create: `docs/runbooks/v3-fos-load-test.md`

**Interfaces:**
- Load driver records `traffic_sent`, `http_statuses`, `fos_objects_seen`, `files_discovered`, `rows_ingested`, `batches_published`, `catalog_conflicts`, `commit_duration_ms`, `dashboard_statuses`, `dashboard_latency_ms`, and `freshness_lag_s`.

- [ ] **Step 1: Add contract tests for stage accounting**

```python
def test_load_report_does_not_equate_http_requests_with_ingested_rows():
    report = LoadReport(http_requests=90, ingested_rows=62)
    assert report.http_requests != report.ingested_rows
    assert report.complete_pipeline is False
```

- [ ] **Step 2: Run the contract test**

Run: `uv run pytest tests/load/test_pipeline_load_contract.py -q`

Expected: PASS once the report distinguishes each pipeline stage.

- [ ] **Step 3: Implement the opt-in FOS load driver**

Require an explicit configured local endpoint and credential source; refuse to run with missing configuration. Generate unique request markers, use valid and intentionally invalid paths separately, and collect evidence at every stage. The driver must never print or persist the FOS access key. Add bounded concurrency/rate controls so the test can compare 1x, 2x, and 4x worker/load levels without becoming an uncontrolled flood.

- [ ] **Step 4: Add the runbook and Make target**

Document the exact sequence: baseline counts → send traffic to the configured Fastly logging service → verify HTTP responses → wait for FOS discovery → verify ledger/file/row deltas → wait for commit → query dashboard API → verify browser rendering. Define failure if any stage is missing, delayed beyond its SLO, or returns 5xx.

- [ ] **Step 5: Run local load validation**

Run: `make load-test-fos RATE=1 CONCURRENCY=4 DURATION=60`

Expected: the report contains non-zero stage deltas, no unexplained 5xx responses, completed commit status, current dashboard timestamps, and p95 dashboard latency within the documented local threshold.

- [ ] **Step 6: Commit**

```bash
git add tests/load/fos_pipeline_load.py tests/load/test_pipeline_load_contract.py Makefile docs/runbooks/v3-fos-load-test.md
git commit -m "test: measure FOS pipeline end to end"
```

### Task 6: Run release gates and clean deployment verification

**Files:**
- Modify: `docs/adr/19-fos-native-v3-topology.md` if measured results require a decision update.
- Modify: `docs/runbooks/v3-fos-load-test.md` with final measured evidence.

- [ ] **Step 1: Run targeted backend and frontend tests for changed areas**

Run: `uv run pytest tests/core tests/cron tests/repositories -q` and the relevant frontend test selectors.

Expected: PASS.

- [ ] **Step 2: Run repository gates**

Run: `make ci`

Expected: all existing gates pass without lowering coverage, ESLint, security, or import-contract floors.

- [ ] **Step 3: Recreate only the clean local v3 stack**

Stop the local stack, preserve explicitly named corrupt database backups, recreate the v3 runtime volumes, and start with the distributed topology configuration. Do not touch GCE or production v2.4.1.

- [ ] **Step 4: Run the FOS load test at increasing bounded levels**

Run the documented 1x, 2x, and 4x profiles and retain reports with timestamps. Compare discovery lag, commit throughput, catalog conflicts, dashboard p95, memory, and error rate.

- [ ] **Step 5: Commit the verified documentation**

```bash
git add docs/adr/19-fos-native-v3-topology.md docs/runbooks/v3-fos-load-test.md
git commit -m "docs: record verified v3 FOS deployment"
```
