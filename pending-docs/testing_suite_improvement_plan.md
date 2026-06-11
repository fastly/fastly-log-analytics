# Testing Suite Improvement Plan: Complete Coverage with Maximum Velocity

This document outlines the architectural blueprint and execution roadmap to elevate the `fastly-log-analytics` test suite to enterprise-grade production readiness. The dual mandate of this plan is **100% execution safety (catching all regressions during development) and extreme developer velocity (maintaining sub-minute local test runs).**

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
