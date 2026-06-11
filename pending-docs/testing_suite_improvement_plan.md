# Testing Suite Improvement Plan: Complete Coverage with Maximum Velocity

This document outlines the architectural blueprint and execution roadmap to elevate the `fastly-log-analytics` test suite to enterprise-grade production readiness. The dual mandate of this plan is **100% execution safety (catching all regressions during development) and extreme developer velocity (maintaining sub-minute local test runs).**

---

## 0. Execution status (2026-06-11)

Working pass on `refactor/cleanup`. Items below are scored against what actually ships in the repo today, not the plan's nominal phase boundaries.

### Done

| Plan item | Where it landed | Notes |
|---|---|---|
| §2 — `wait_until` helper + sleep cleanup | `tests/utils/polling.py`; sites in `test_admin_mutation_endpoints.py:1396`, `test_rollups_hour_bundling.py:135/157`, `test_duckdb_helpers.py:306/334` | Plan misclassified 4 of 5 sites as polling waits — they were FS-mtime bumps. Replaced with `os.utime()` or deleted (count-differs invariant made the sleep redundant). |
| §4.B.1 — MagicMock S3 overhaul | `tests/core/test_iceberg.py` (4 sites at L287/L322/L349/L372), `tests/routers/test_usage_endpoints.py:38` | Cache-TTL tests use `patch.object(s3_mock, "get_object", wraps=...)` to keep wire-call counting while running against real moto S3. |
| §3 Gap A — VCL stubs fixture | `tests/fixtures/fastly_stubs.vcl`; runner updated in `tests/core/test_vcl_semantics.py` | Uses `//<INJECT_FETCH_SNIPPET>` / `//<INJECT_DELIVER_SNIPPET>` marker comments (valid VCL on its own — IDE-friendly) instead of `{}` str.format escapes. Header doc points future authors at where to add `testing.inject_variable(...)` stubs for `fastly.ff.visits_this_service` / `fastly_info.state`. |
| §6 Custom Fields — SQL injection fuzzing | `tests/core/test_custom_field_fuzz.py` | 7 hypothesis property tests on `validate_custom_field` covering name regex (positive + negative), VCL injection-char guards, length cap, bytes_estimate range. Hypothesis surfaced one precedence quirk (empty-check fires before forbidden-char check) — documented inline. |
| §5 Pillar 3 — DuckDB pool stress (partial) | `tests/core/test_duckdb_pool.py` (3 new tests appended) | Covers saturation → `_PoolBusy` after `max_wait`, empty `_wait_stats` zero-shape, populated nearest-rank percentiles. |

### Skipped or deferred (with rationale)

| Plan item | Status | Why |
|---|---|---|
| §5 Pillar 4 — In-flight recovery regression | Already covered | `tests/core/test_ingest_in_flight.py` has 11 tests covering the recover paths the plan asks for. Plan's "crash after `write_to_buffer` but before SQLite tracking entry" misreads the mark-before-write protocol — SQLite tracking is *first*, then buffer write; existing tests already pin both halves of the actual race window. |
| §3 Gap B — FOS negative-cache wrapper | Skipped — premature | `backend/utils/retry.py` defines `http_api_retry` / `generic_network_retry` / `sqlite_busy_retry`, but the module docstring lists call sites as TODO. No FOS read-path currently retries (`_read_metadata_pointer` does a single `s3.get_object` with `except: continue`). Adding a 404-caching emulator before retry adoption tests nothing. Revisit once the retry decorators are wired into real call sites. |
| §3 Gap C / §7 Phase 1 — Schemathesis | POC complete; deferred | Installed schemathesis 4.21.5, wired `from_asgi` against `/openapi.json`, scoped to GET-only with `unsupported_method` check disabled (RFC 9110 `Allow`-header complaint). 79 GET endpoints collected; **~30 failed** on real findings — including `sqlite3.OperationalError: unable to open database file` triggered by a URL-encoded UTF-8 service_id (looks like a path-handling bug worth chasing). Each finding needs a scoped fix PR; shipping a broadly-failing test to a shared branch would be loud. The dep + the test file were reverted; pick this up as its own ticket. |
| §4.A — Pruning audit (`test_endpoints.py`, `test_admin_get_endpoints.py`, `AppLayout.test.tsx`) | Collapsed | The plan's "superseded by Schemathesis / Playwright" argument doesn't apply until those two are landed. Re-audit each candidate file after Schemathesis fuzz lands; check that the existing test catches something the fuzz doesn't before deleting. |

### Not started (need explicit signoff before starting)

| Plan item | Blast radius | What's needed first |
|---|---|---|
| §5 Pillar 1 / §7 Phase 2 — Playwright browser tests | Adds Chromium / Firefox / WebKit binaries (~500 MB), a new test runner config, a new CI step. Touches `frontend/` where other devs are actively committing. | Cut a dedicated branch (`tests/playwright-setup`) and confirm coordination with whoever owns the dashboard / map / filter-bar UI work. Start with a single smoke test (load `/dashboard`, screenshot, assert no console errors) before expanding to the four targets in §5 Pillar 1. |
| §5 Pillar 2 — Live-share lifecycle E2E | Depends on Playwright. Also requires understanding the `/api/remote-share/login` rate-limit/lockout semantics for the brute-force test. | After Playwright is in. Read `backend/access/remote_share.py` for the actual lockout contract, then write Playwright tests that drive a real invite → login → query flow against the in-process app. |
| §4.A — AppLayout pruning | Depends on Playwright covering the same layout assertions. | After Playwright is in. Diff the JSDOM assertions in `AppLayout.test.tsx` against what the equivalent Playwright test catches; delete only the overlap. |
| §6 — LocalStack + Terraform validation (Phase 3) | Adds a docker-compose profile, the `localstack/localstack` image (~200 MB), per-test setup/teardown via `tflocal`. | Confirm against the `infra-stays-local` memory which fixtures may hold real service IDs vs need scrubbing into local-only files. Then add a `test` profile to `docker-compose.yml` and a `tests/terraform_tests/test_localstack_apply.py` that runs `init` + `validate` + `apply` against the generated HCL. |
| §6 — VCL stubs: re-enable miss_pass test | Single test currently `pytest.skip`'d in `test_vcl_semantics.py:114` | Needs a `testing.inject_variable("fastly_info.state", ...)` stub in `tests/fixtures/fastly_stubs.vcl` once Falco fixes the `!~`-operator binding bug. Track Falco upstream rather than working around it locally. |

### Bug fixes surfaced this pass

The schemathesis POC was reverted, but its findings stayed actionable. Three real bugs got committed back to the branch:

| Commit | Bug | How it surfaced |
|---|---|---|
| `acf81f0` | `service_id` path parameter containing a 4-byte UTF-8 codepoint (or null byte / path traversal / over-cap length) crashed `sqlite3.connect` with `OSError(Errno 92): Illegal byte sequence` on APFS, surfaced as opaque 500 `unable to open database file`. | Schemathesis `curl -X GET '/api/services/%F0%B8%95%95%C2%A0d.../...'` — reduced manually to identify the codepoint class. |
| `b5b31dc` | The 422 body emitted by the new `_invalid_service_id_handler` had `detail` as a string, but FastAPI's `HTTPValidationError` schema says `detail` is an array of `ValidationError`. The fix from `acf81f0` itself violated the OpenAPI spec. | Re-running schemathesis after `acf81f0` shipped — `JsonSchemaError: "..." is not of type "array"`. |
| `abf3a61` | `test_usage_current_storage_success` was non-deterministically failing on the branch HEAD under `-n4` xdist with `DBBusyError` 500. The moto migration in `4d23079` had dropped `@patch("backend.core.duckdb.get_connection")`, exposing the test to a real DuckDB file lock race with peer xdist workers. | Pre-existing flakiness flagged in my own earlier session; root-caused by walking the migration's diff. |

### Not fixed this pass (catalogued, scoped for follow-up)

- **RFC 9110 — 405 responses lack `Allow` header**. FastAPI/Starlette default behavior; schemathesis's `unsupported_method` check flags it. Real but minor — Fastly often rewrites response headers anyway. Fix is a small middleware that traverses `app.routes` to populate the `Allow` header on 405s.
- **Schemathesis bulk audit** — both POC runs of the categorizer script crashed inside `schemathesis.openapi.from_asgi` startup before reaching the per-operation loop (FastAPI startup tracebacks about scheduler-already-running + interpreter-shutdown ExecutorPool flooded stdout; exit 0 but no summary printed). The audit needs a cleaner harness (probably an actual pytest test with the autouse sandbox + a per-op subtest) rather than a standalone script. The two findings above were extracted manually from the partial output.

### Commits on `refactor/cleanup` this pass

`7a278fe` sleep cleanup → `4d23079` moto S3 migration → `9dd2294` VCL stubs fixture → `8c553e3` custom-field hypothesis fuzz → `e7efc7b` pool saturation + wait-stats → `acf81f0` service_id 422 guard → `abf3a61` usage_endpoints xdist mock → `b5b31dc` 422 body matches HTTPValidationError schema.

---

## 1. Executive Summary & Core Philosophy

To support rapid iteration on log ingestion, analytical queries, and UI components without introducing flakiness or slow test pipelines, we adopt a tiered testing approach:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        TIERED TESTING STRATEGY                         │
├───────────────────┬───────────────────────────────┬────────────────────┤
│ Tier              │ Execution Trigger             │ Target Speed       │
├───────────────────┼───────────────────────────────┼────────────────────┤
│ Tier 1: Unit/Fast │ Local Commit & CI/CD          │ < 60 seconds total │
│ Tier 2: E2E & Sec │ Pull Request (CI) & On-Demand  │ < 5 minutes total  │
└───────────────────┴───────────────────────────────┴────────────────────┘
```

*   **Tier 1 (Unit & Fast Integration)**: Includes Python `pytest` (with database mocks) and Node `vitest` (with JSDOM and MSW). Runs on every local commit and CI commit.
*   **Tier 2 (Full E2E, Contract, & Cloud Integration)**: Includes Playwright browser tests, Schemathesis API contract fuzzing, and LocalStack Terraform dry-runs. Runs automatically in CI/CD on Pull Requests and can be triggered locally on-demand.

---

## 2. Speed Optimization: Eliminating Flaky Sleeps

### The Problem
Using `time.sleep(X)` in test files introduces non-determinism. Under high CI CPU load, the sleep might be too short (causing flaky failures); under local execution, it is often too long (wasting developer time).

### Adversarial Risk: CPU Starvation & Thread Deadlocks
A standard tight polling loop checking state every 1ms or 2ms via flat `time.sleep()` blocks can starve the CPU of thread-switching cycles on single-core CI runners. This causes Python's Global Interpreter Lock (GIL) to lock out background execution tasks (like scheduled syncs), resulting in paradoxically slower tests or deadlocks.

### Best-Practice Solution: Exponential-Backoff Polling
We will introduce a centralized polling helper (`wait_until`) inside a core pytest fixture. This helper starts checking the state at high frequency (1ms) and exponentially backs off (to 10ms, then 50ms) to release the CPU thread pool, returning **immediately** upon resolution and failing only if the maximum timeout is exceeded.

```python
# wait_until Exponential-Backoff Implementation (Best Practice)
import time
from typing import Callable

def wait_until(check_fn: Callable[[], bool], timeout: float = 1.0, initial_interval: float = 0.001, backoff_factor: float = 2.0, max_interval: float = 0.05) -> bool:
    """Active polling assertion helper with exponential backoff.

    Prevents CPU thread starvation under heavy CI loads while returning
    instantly (under 2ms) for fast-resolving events.
    """
    start = time.perf_counter()
    interval = initial_interval
    while time.perf_counter() - start < timeout:
        if check_fn():
            return True
        time.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)
    raise AssertionError(f"Timeout waiting for condition after {timeout}s")
```

---

## 3. Red-Team Adversarial Gaps & Countermeasures

### Gap A: The VCL/Falco Compiler Illusion
*   **The Vulnerability**: `falco` is a generic open-source Varnish/Fastly VCL parser. It does *not* perfectly emulate Fastly's actual closed-source edge compiler. In production, Fastly uses hyper-specific proprietary extensions (such as `fastly.ff.visits_this_service` or `fastly_info.state`). Running pure code-gen snippets against `falco` will either cause false-positive syntax errors or miss actual runtime failures (e.g., variables out of scope in fetch vs. deliver).
*   **The Countermeasure**: Build and maintain a strict VCL stubbing fixture (`tests/fixtures/fastly_stubs.vcl`) to mock proprietary Fastly variables. Currently, [`tests/core/test_vcl_semantics.py` (L36-57)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/core/test_vcl_semantics.py#L36-L57) relies on duplicate, inline VCL boilerplates for standard backend, fetch, and deliver definitions. Migrating these to `tests/fixtures/fastly_stubs.vcl` removes code duplication, allows reuse across other custom field tests, and provides an authoritative place to mock known edge anomalies (such as Falco's bug with `!~` operator checks on `fastly_info.state` at `test_falco_origin_field_miss_pass_only`).

### Gap B: LocalStack vs. Fastly Object Storage (FOS) Semantics
*   **The Vulnerability**: Fastly Object Storage (FOS) is S3-compatible but exhibits highly customized edge caching. FOS implements negative-caching traps (caching 404 responses for up to 1 second), which can break sequential REST actions during bucket setup. AWS-native LocalStack cannot natively mock Fastly edge-caching layers or token authorizations.
*   **The Countermeasure**: Ensure our Mock-S3 tests actively emulate these Fastly negative-cache traps inside the `ThreadedMotoServer` fixture wrapper, allowing us to assert that our retry loops (`tenacity`) are resilient under edge-specific 404 delays.

### Gap C: Schemathesis/Fuzzing Database Exhaustion
*   **The Vulnerability**: DuckDB enforces a strict single-writer lock per database file. Schemathesis sends hundreds of concurrent API requests. Simultaneous mutation calls (e.g., POST/DELETE custom fields) will exhaust the LIFO pool, causing massive "Database is locked" exceptions.
*   **The Countermeasure**: Restrict Schemathesis runs to a single worker thread (`--workers=1`) and wrap each test block in an auto-rollback transaction. This guarantees 100% route contract fuzzing without thread-locking contention.

---

## 4. Test Suite Pruning & Consolidation (Velocity Overhaul)

To keep tests exceptionally fast and low-maintenance, we must eliminate redundant files and refactor fragile mocking patterns.

### A. Redundant / Unnecessary Tests (Pruning Targets)
Once dynamic contract testing and E2E browser engines are integrated, the following manual test scripts can be pruned to save CPU pipeline runs:

1.  [`tests/routers/test_endpoints.py`](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/routers/test_endpoints.py): Basic route schema validations (checking JSON response root keys like `"status": "success"`). *Superceded entirely by Schemathesis fuzzer validation.*
2.  [`tests/routers/test_admin_get_endpoints.py`](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/routers/test_admin_get_endpoints.py): Standard read-only GET configurations. *Superceded by Schemathesis fuzz contract verification.*
3.  [`frontend/__tests__/components/AppLayout.test.tsx`](file:///Users/drew.michael/Projects/fastly-log-analytics/frontend/__tests__/components/AppLayout.test.tsx): Brittle JSDOM DOM-queries asserting that navigation panels collapse or render correctly. *Superceded by pixel-accurate Playwright layout validation.*

### B. Fragile Tests Requiring Refactoring (Robust Mocking)
The following test suites carry fragile mocking or timing constraints and must be refactored:

1.  **Legacy MagicMock S3 Overhaul**:
    *   *Target Files*: [`tests/core/test_iceberg.py` (L287, L322, L349, L372)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/core/test_iceberg.py) and [`tests/routers/test_usage_endpoints.py` (L38)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/routers/test_usage_endpoints.py).
    *   *Fragility*: These suites manually mock `boto3` calls with generic `MagicMock()`. If the internal ingestion engine alters its internal boto3 client calls, mock checks break.
    *   *Refactor*: Port to use our standardized, moto-backed `s3_mock` and `fos_source` fixtures defined in `conftest.py` to assert against a real, in-memory S3 API.
2.  **Timing & Contention Sleep Elimination**:
    *   *Target Files*: [`tests/core/test_duckdb_helpers.py`](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/core/test_duckdb_helpers.py), [`tests/services/test_service_manager.py` (L213)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/services/test_service_manager.py), [`tests/core/test_rollups_hour_bundling.py` (L135, L157)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/core/test_rollups_hour_bundling.py), and [`tests/routers/test_admin_mutation_endpoints.py` (L1396)](file:///Users/drew.michael/Projects/fastly-log-analytics/tests/routers/test_admin_mutation_endpoints.py).
    *   *Fragility*: Standard hardcoded sleeps (`time.sleep()`) ranging from 10ms to 100ms. In `test_admin_mutation_endpoints.py:1396`, `time.sleep(0.1)` is used to wait for background thread processing in an asynchronous SSE stream.
    *   *Refactor*: Replace with our custom exponential-backoff `wait_until` helper fixture to support instant exit, adaptive checking (e.g. polling the `thread_success` state list), and prevent CI deadlocks under resource-constrained runners.

---


## 5. Pillar-by-Pillar Action Plan

### Pillar 1: Test Coverage & Completeness
*   **Playwright Browser Tests**: We will add `playwright` to the frontend, restricted to a single worker locally to preserve RAM on developer machines.
*   **Test Targets**:
    *   Plotly analytical charts loading and responsiveness.
    *   Maplibre network traffic country maps.
    *   Interactive filter bar token addition and synchronization.
    *   Drag-and-drop customization of dashboard card visibility.

### Pillar 2: Integration & End-to-End (E2E) Flow
*   **API Contract Fuzzing**: Integrate single-threaded Schemathesis runs in the CI/CD pipeline to catch payload discrepancies.
*   **Live-Share Lifecycle Simulation**: Add E2E tests simulating:
    *   Invite creation, SSE client registration, read-only analytical queries, and TOS compliance.
    *   Brute-force security mitigation (rate-limiting and lockouts) on the `/api/remote-share/login` endpoint.

### Pillar 3: Reliability & Performance
*   **FOS Network Resilience**: Add network-delay simulation tests using `ThreadedMotoServer` wrapped in standard connection latency injects to test how connection timeouts behave under high latency or partial S3 failures.
*   **E2E Speed Guard**: Maintain the `perf_gate.sh` baseline performance gate to block commits that introduce a >10% regression in raw ingestion speed.

### Pillar 4: Database-Specific Testing (DuckDB & SQLite)
*   **In-Flight Recovery Regression**: Write tests in `test_ingest_in_flight.py` simulating crashes at multiple points of the pipeline:
    *   Crash after `write_to_buffer` but before SQLite tracking entry.
    *   Ensure the subsequent `sync()` run handles cleanup and recovers files with zero row loss.
*   **DuckDB Pool Stress Testing**: Verify queue checkout order, timeout limits, and telemetry metric rings when requests exceed `DUCKDB_POOL_MAX_SIZE`.

---

## 6. Provisioning & Custom Field Workflows

### Admin & Analyst Provisioning (Terraform Flow)
To ensure the generated Terraform HCL code is always deployable and developer-friendly, we adopt the following standard:

1.  **LocalStack Emulation via Docker Compose (Best Practice)**:
    We will provide a pre-configured `LocalStack` container under a dedicated profile in `docker-compose.yml` (e.g., `docker compose --profile test up`).
    *   *Why this is developer-friendly*: Developers don't need to configure real cloud credentials, nor do we pay for slow on-the-fly container downloads during unit-test runs.
2.  **HCL Validation Suite**:
    In CI/CD, the test suite will spin up LocalStack, execute `terraform init`, `terraform validate`, and `terraform apply` against the generated HCL, confirming the S3 bucket is successfully stood up and Fastly-compatible bucket policies are applied.

### Custom Fields Validation
*   **VCL Syntax Verification**: Verify that custom fields appending `vcl_log_expression` pass the `falco` linting process automatically when `falco` is installed, utilizing custom edge-variable stubs to prevent false-positive parser failures.
*   **SQL Injection Defenses**: Add aggressive fuzz-testing verifying that malicious custom field names and payload values (e.g., names with semicolons or SQL keywords) are rejected or safely escaped before DuckDB view-binding executes.

---

## 7. Phased Implementation Roadmap

To avoid disrupting active feature development, we suggest implementing this plan in three distinct phases:

### Phase 1: Speed, Safety, & Contract Foundation (Immediate)
*   **Action**: Create active polling helpers to replace flaky `time.sleep()` calls across the backend.
*   **Action**: Introduce `Schemathesis` API contract testing in CI/CD (single-worker) to eliminate frontend-backend API drift.
*   **Action**: Add custom-field SQL injection fuzzing tests.

### Phase 2: Complete Browser Verification (Short Term)
*   **Action**: Integrate `Playwright` to test interactive dashboard actions, drag-and-drop elements, and live-updating SSE components.
*   **Action**: Implement E2E simulation for Analyst Path A invitation and Analyst Path B Remote-Share logins.

### Phase 3: Cloud & Provisioning Integrity (Medium Term)
*   **Action**: Integrate `LocalStack` in a dedicated testing profile within `docker-compose.yml`.
*   **Action**: Add E2E tests validating the full Terraform generation and deployment lifecycle against LocalStack.
*   **Action**: Set up simulated S3 network latency tests to evaluate connection pool timeouts.
