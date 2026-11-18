# Observed Fastly Platform Usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add measured Object Storage, Compute, and NGWAF Platform Metrics to the Usage & Cost page while making Fastly's real usage authoritative over local fallback data.

**Architecture:** A deployment-independent `/api/usage/platform` JSON contract drives compact observed-usage cards. High-Scale implements it in FastAPI; Cloud implements the same contract through the browser adapter and a Compute admin route. Platform Object Storage gauges replace modeled/local storage cost inputs when available, while Compute and NGWAF counters remain informational or existing measured cost inputs.

**Tech Stack:** Python 3.13, FastAPI/Pydantic, Rust Fastly Compute, Next.js/React, TanStack Query, Vitest, pytest, cargo test, Ruff.

## Global Constraints

- Use Fastly Platform Metrics whenever real measured usage is available; local inventory and modeled values are fallback only.
- Never sum Object Storage gauge points.
- Label account-wide Object Storage scope and configured-workspace NGWAF scope in the UI.
- Keep per-install cost attribution explicit; never silently claim account-wide data belongs to one install.
- Preserve existing `/api/usage/operations`, `/api/usage/bandwidth`, `/api/usage/compute`, Real-Time Stats, and existing calculator behavior when Platform data is unavailable.
- Missing or unauthorized optional metrics produce independent unavailable states, never a page-wide failure.
- Do not expose tokens, service IDs, workspace IDs, raw Fastly bodies, cursors, or error bodies in the response.

---

### Task 1: Define the shared Platform usage contract

**Files:**
- Modify: `backend/models/admin/operations.py`
- Modify: `backend/models/admin/__init__.py`
- Test: `tests/models/test_models.py`

**Interfaces:**
- Produces: Pydantic `PlatformStorageUsage`, `PlatformComputeUsage`, `PlatformNgwafUsage`, and `PlatformUsageResponse`.
- The frontend generated types consume the FastAPI schema for High-Scale.

- [ ] **Step 1: Add contract tests**

```python
def test_platform_usage_response_preserves_unavailable_and_measured_sections():
    payload = {
        "source": "partial",
        "storage": {
            "status": "ok",
            "scope": "account",
            "observed_at": "2026-11-18T00:00:00Z",
            "bytes": 1073741824,
            "buckets": 3,
            "gb_hours": 24.0,
        },
        "compute": {"status": "not_configured"},
        "ngwaf": {
            "status": "ok",
            "scope": "workspace",
            "total": 100,
            "allowed": 80,
            "blocked": 10,
            "challenged": 5,
            "logged": 3,
            "timeout": 1,
            "unknown": 1,
        },
    }
    parsed = PlatformUsageResponse.model_validate(payload)
    assert parsed.storage.status == "ok"
    assert parsed.storage.bytes == 1073741824
    assert parsed.compute.status == "not_configured"
    assert parsed.ngwaf.blocked == 10
```

- [ ] **Step 2: Run the test to verify failure**

```bash
cd /Users/drew.michael/Projects/fastly-log-analytics
perl -e 'alarm 120; exec @ARGV' uv run pytest --confcutdir=/tmp tests/models/test_models.py::test_platform_usage_response_preserves_unavailable_and_measured_sections -q -x
```

Expected: FAIL with `NameError`/missing model import.

- [ ] **Step 3: Implement models**

Use string literal status types:

```python
PlatformSourceStatus = Literal["ok", "not_configured", "unavailable"]

class PlatformStorageUsage(BaseModel):
    status: PlatformSourceStatus
    scope: Literal["account"]
    observed_at: str | None = None
    bytes: int | None = None
    buckets: int | None = None
    gb_hours: float | None = None
    reason: str | None = None

class PlatformComputeUsage(BaseModel):
    status: PlatformSourceStatus
    scope: Literal["app_and_worker"] | None = None
    requests: int = 0
    billed_time_ms: float = 0
    backend_errors: int = 0
    cache_operations: int = 0
    sandboxes: int = 0
    resource_errors: int = 0
    reason: str | None = None

class PlatformNgwafUsage(BaseModel):
    status: PlatformSourceStatus
    scope: Literal["workspace"] | None = None
    total: int = 0
    allowed: int = 0
    blocked: int = 0
    challenged: int = 0
    logged: int = 0
    timeout: int = 0
    unknown: int = 0
    reason: str | None = None

class PlatformUsageResponse(BaseResponse):
    source: Literal["fastly", "partial", "unavailable"]
    storage: PlatformStorageUsage
    compute: PlatformComputeUsage
    ngwaf: PlatformNgwafUsage
```

- [ ] **Step 4: Export and regenerate types**

Export from `backend/models/admin/__init__.py`; run:

```bash
make gen-types
```

- [ ] **Step 5: Run Task 1 validation**

```bash
uv run ruff check backend/models/admin/operations.py backend/models/admin/__init__.py tests/models/test_models.py
uv run ruff format --check backend/models/admin/operations.py backend/models/admin/__init__.py tests/models/test_models.py
```

- [ ] **Step 6: Commit**

```bash
git add backend/models/admin/operations.py backend/models/admin/__init__.py tests/models/test_models.py frontend/openapi.json frontend/types/api.generated.ts
git commit -m "Define observed Platform usage contract"
```

### Task 2: Implement High-Scale Platform observations

**Files:**
- Modify: `backend/routers/usage.py`
- Test: `tests/routers/test_usage_platform.py` (new)

**Interfaces:**
- Consumes: `fastly_platform_metrics` and the models from Task 1.
- Produces: `GET /api/usage/platform?start=<ISO>&end=<ISO>`.

- [ ] **Step 1: Write source-isolation tests**

```python
def test_platform_usage_prefers_storage_gauges_and_keeps_partial_failures(monkeypatch):
    calls = []

    def metrics(**kwargs):
        calls.append(kwargs)
        if kwargs["entity_id"] is None:
            return {"data": [
                {"start_time": D0, "object_storage_bytes_count": 1_000},
                {"start_time": D1, "object_storage_bytes_count": 2_000,
                 "object_storage_buckets_count": 4},
            ]}
        raise RuntimeError("workspace unavailable")

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(
        start="2026-11-17T00:00:00Z",
        end="2026-11-19T00:00:00Z",
        source=source_with_ngwaf,
    )
    assert response.source == "partial"
    assert response.storage.bytes == 2_000
    assert response.storage.buckets == 4
    assert response.storage.gb_hours == pytest.approx(2_000 / 1024**3 * 24)
    assert response.ngwaf.status == "unavailable"
    assert all(call["token"] == "tok" for call in calls)
```

Add separate tests for:

- no API key => all network-backed sections unavailable and the endpoint still returns;
- no workspace => NGWAF `not_configured` with no workspace call;
- missing/sparse gauges preserve the prior observed value;
- no storage rows => unavailable, never zero;
- sanitized failure reason code and no token/cursor/body in output.

- [ ] **Step 2: Run tests to verify failure**

```bash
pytest --confcutdir=/tmp tests/routers/test_usage_platform.py -q -x
```

Expected: FAIL because the endpoint does not exist.

- [ ] **Step 3: Implement per-source fetches**

Use:

```python
STORAGE_PLATFORM_METRICS = (
    "object_storage_buckets_count",
    "object_storage_bytes_count",
)
NGWAF_PLATFORM_METRICS = (
    "ngwaf_requests_total_count",
    ...
)

def _platform_storage(records, from_epoch, to_epoch):
    bytes_by_day = sorted(
        (int(row["start_time"]), int(row["object_storage_bytes_count"]))
        for row in records
        if row.get("object_storage_bytes_count") is not None
    )
    bucket_rows = sorted(
        (int(row["start_time"]), int(row["object_storage_buckets_count"]))
        for row in records
        if row.get("object_storage_buckets_count") is not None
    )
    if not bytes_by_day:
        return None
    gb_hours = 0.0
    for index, (start, value) in enumerate(bytes_by_day):
        end = bytes_by_day[index + 1][0] if index + 1 < len(bytes_by_day) else to_epoch
        overlap = max(0, min(end, to_epoch) - max(start, from_epoch))
        gb_hours += value / 1024**3 * overlap / 3600
    observed_at, latest_bytes = bytes_by_day[-1]
    latest_buckets = bucket_rows[-1][1] if bucket_rows else None
    return PlatformStorageUsage(
        status="ok",
        scope="account",
        observed_at=datetime.fromtimestamp(observed_at, UTC).isoformat().replace("+00:00", "Z"),
        bytes=latest_bytes,
        buckets=latest_buckets,
        gb_hours=gb_hours,
    )
```

Wrap each external source in its own exception boundary. Derive source `fastly` when all configured sections succeed, `partial` when at least one succeeds and another fails, otherwise `unavailable`.

- [ ] **Step 4: Register the GET route**

Return `PlatformUsageResponse` from:

```python
@usage_router.get("/platform", response_model=PlatformUsageResponse)
@query_errors()
def platform_usage(...): ...
```

Resolve `ngwaf_workspace_id` from the active service config; never return it.

- [ ] **Step 5: Run tests and lint**

```bash
pytest --confcutdir=/tmp tests/routers/test_usage_platform.py tests/routers/test_usage_prefill.py -q -x
uv run ruff check backend/routers/usage.py tests/routers/test_usage_platform.py
uv run ruff format --check backend/routers/usage.py tests/routers/test_usage_platform.py
```

- [ ] **Step 6: Commit**

```bash
git add backend/routers/usage.py tests/routers/test_usage_platform.py
git commit -m "Add High-Scale Platform usage endpoint"
```

### Task 3: Add Cloud Platform observations

**Files:**
- Modify: `compute/cloud/src/telemetry/usage.rs`
- Modify: `compute/cloud/src/sdk.rs`
- Modify: `compute/cloud/src/admin/logs.rs`
- Modify: `compute/cloud/tests/ingest.rs`

**Interfaces:**
- Consumes: the existing `fetch_platform_metrics` helper.
- Produces: `GET /api/admin/usage/platform`.

- [ ] **Step 1: Add Rust request/parser tests**

```rust
#[test]
fn platform_usage_keeps_partial_optional_sections() {
    let body = build_platform_usage_response(
        Some(json!({
            "status": "ok",
            "scope": "account",
            "bytes": 2048,
            "buckets": 2,
            "gb_hours": 0.000001,
        })),
        None,
        Some(json!({
            "status": "ok",
            "scope": "workspace",
            "total": 5,
            "blocked": 3,
        })),
    );
    assert_eq!(body["storage"]["status"], "ok");
    assert_eq!(body["compute"]["status"], "unavailable");
    assert_eq!(body["ngwaf"]["blocked"], 3);
}
```

Add tests for:

- Compute fields are batched with the existing app/worker metrics call;
- Object Storage uses aggregate/daily;
- NGWAF uses the configured workspace entity and returns `not_configured` without a call;
- out-of-window, duplicate, malformed, and pagination-loop rows are source-local failures.

- [ ] **Step 2: Run focused Rust test to verify failure**

```bash
perl -e 'alarm 240; exec @ARGV' cargo test --manifest-path compute/cloud/Cargo.toml platform_usage --quiet
```

Expected: FAIL because route/helper do not exist.

- [ ] **Step 3: Implement metric helpers**

In `fetch_platform_metrics`, allow a `granularity` argument (`hourly` or `daily`) and an aggregate option. Keep the existing service calls unchanged by wrapping it with:

```rust
fetch_platform_entity(service_id, "hourly", from, to, metrics, fetch)
fetch_platform_aggregate("daily", from, to, STORAGE_FIELDS, fetch)
```

Add `platform_storage`, `platform_compute`, and `platform_ngwaf` builders that return sanitized JSON sections and never return config IDs.

- [ ] **Step 4: Register the route**

Add:

```rust
("/api/admin/usage/platform", GET) => platform_usage(runtime, query)
```

Validate `from`/`to` using the existing billing range limit. Return independent status objects; return HTTP 200 for partial Platform failures.

- [ ] **Step 5: Run Task 3 validation**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml platform_usage --quiet
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
cargo check --manifest-path compute/cloud/Cargo.toml --tests --target wasm32-wasip1 --quiet
```

- [ ] **Step 6: Commit**

```bash
git add compute/cloud/src/telemetry/usage.rs compute/cloud/src/sdk.rs compute/cloud/src/admin/logs.rs compute/cloud/tests/ingest.rs
git commit -m "Add Cloud Platform usage endpoint"
```

### Task 4: Expose the Cloud adapter contract

**Files:**
- Modify: `frontend/lib/cloud-engine/native-routes.ts`
- Modify: `frontend/lib/cloud-engine/cloud-api.generated.ts`
- Modify: `frontend/lib/cloud-engine/adapter.ts`
- Test: `frontend/lib/cloud-engine/adapter.test.ts`

**Interfaces:**
- Consumes: Cloud admin `/api/admin/usage/platform`.
- Produces: browser response at `/api/usage/platform`.

- [ ] **Step 1: Add failing adapter test**

```ts
it("answers /api/usage/platform from the Cloud admin route without changing cost inputs", async () => {
  fetchMock.mockResolvedValueOnce(jsonResponse({
    source: "partial",
    storage: { status: "ok", scope: "account", bytes: 2048 },
    compute: { status: "ok", scope: "app_and_worker", requests: 7 },
    ngwaf: { status: "not_configured" },
  }));
  const response = await load("/api/usage/platform?from=...&to=...");
  expect(response.ok).toBe(true);
  expect(fetchMock).toHaveBeenCalledWith(
    expect.stringContaining("/api/admin/usage/platform?"),
    expect.anything(),
  );
});
```

- [ ] **Step 2: Verify failure**

```bash
cd frontend
npx vitest run lib/cloud-engine/adapter.test.ts
```

- [ ] **Step 3: Register the route and pass-through**

Add the admin path to `native-routes.ts`, add `/api/usage/platform` adapter handling, preserve status and return body, and do not merge it into existing compute or prefill caches.

- [ ] **Step 4: Re-run adapter tests**

```bash
npx vitest run lib/cloud-engine/adapter.test.ts
npm run typecheck
```

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/cloud-engine/native-routes.ts frontend/lib/cloud-engine/cloud-api.generated.ts frontend/lib/cloud-engine/adapter.ts frontend/lib/cloud-engine/adapter.test.ts
git commit -m "Expose Cloud Platform usage in browser adapter"
```

### Task 5: Render observed usage and measured storage precedence

**Files:**
- Create: `frontend/app/usage/_sections/PlatformUsageCards.tsx`
- Modify: `frontend/app/usage/_sections/UsageClient.tsx`
- Modify: `frontend/app/usage/_sections/UsageStatCards.tsx`
- Test: `frontend/__tests__/app/usage-platform.test.tsx`

**Interfaces:**
- Consumes: generated High-Scale `PlatformUsageResponse`; identical Cloud adapter JSON.
- Produces: three optional cards and overrides for measured storage cost.

- [ ] **Step 1: Add failing card tests**

```tsx
it("uses Platform account storage as the measured cost source and labels its scope", () => {
  render(<PlatformUsageCards data={partialPlatform} />)
  expect(screen.getByText("Account-wide snapshot")).toBeInTheDocument()
  expect(screen.getByText("2 GB")).toBeInTheDocument()
})

it("does not render Compute on High-Scale and renders Not configured for NGWAF", () => {
  render(<PlatformUsageCards data={highScalePlatform} />)
  expect(screen.queryByText("Cloud Compute")).not.toBeInTheDocument()
  expect(screen.getByText("Not configured")).toBeInTheDocument()
})
```

Add a `UsageStatCards` test proving:

- successful Platform storage replaces local `total_billed_gb_hours` in the period storage cost,
- failed/unavailable Platform storage falls back to local inventory,
- the platform account scope label remains visible.

- [ ] **Step 2: Verify tests fail**

```bash
cd frontend
npx vitest run __tests__/app/usage-platform.test.tsx
```

- [ ] **Step 3: Implement `PlatformUsageCards`**

Use existing `StatCard`, `Skeleton`, `Tooltip`, `formatBytes`, and count formatters. Render:

- Storage primary: latest bytes; subtext buckets, "account-wide snapshot", measured Platform source;
- Compute primary: requests; subtext billed duration, backend/cache/resource errors;
- NGWAF primary: blocked/total; subtext allowed/challenged/logged/timeout/unknown.

Hide Compute when status is `not_configured` and the deployment is High-Scale. Keep NGWAF visible as "Not configured."

- [ ] **Step 4: Wire the query and source precedence**

In `UsageClient`:

```ts
const platform = useQuery({
  queryKey: ['usage','platform',activeServiceId,startTime,endTime],
  queryFn: ... client/admin adapter same URL,
  enabled: isReady,
  staleTime: 60_000,
})
```

Pass `platform.storage` into `UsageStatCards`. Compute:

```ts
const measuredStorageGbHours =
  platform?.storage.status === "ok" ? platform.storage.gb_hours : null
const billedGbHours = measuredStorageGbHours ?? storage?.total_billed_gb_hours ?? 0
```

Add an `observedStorageGbHours` prop to both `CostCalculator` and
`CloudCostCalculator`. Before rendering result lines, replace only the storage
line and total:

```ts
const observedStorageCost =
  observedStorageGbHours == null ? null
  : observedStorageGbHours / 720 * storageRate;
const displayedResult = observedStorageCost == null ? result : {
  ...result,
  costStorage: observedStorageCost,
  totalCost: result.totalCost - result.costStorage + observedStorageCost,
  storageTiers: [{
    label: 'Fastly Platform (account-wide)',
    gbMonths: observedStorageGbHours / 720,
    flagged: false,
  }],
};
```

For the High-Scale calculator, `CalcResults` uses the same storage and total
fields. For Cloud, replace the `storage` line in `result.lines` and add the delta
to `result.total`. Do not mutate cached API responses. Pass the source label so
the UI shows "Measured · Fastly Platform (account-wide)" rather than "Reference."

- [ ] **Step 5: Run focused frontend validation**

```bash
npx vitest run __tests__/app/usage-platform.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
npx tsc --noEmit
npm run lint -- app/usage/_sections/PlatformUsageCards.tsx app/usage/_sections/UsageClient.tsx app/usage/_sections/UsageStatCards.tsx
```

- [ ] **Step 6: Commit**

```bash
git add frontend/app/usage/_sections/PlatformUsageCards.tsx frontend/app/usage/_sections/UsageClient.tsx frontend/app/usage/_sections/UsageStatCards.tsx frontend/__tests__/app/usage-platform.test.tsx
git commit -m "Use measured Platform usage on cost page"
```

### Task 6: Integrate, verify, and document

**Files:**
- Modify: `docs/pages/usage-and-cost.md`
- Test: all focused suites from Tasks 1–5

- [ ] **Step 1: Document source precedence**

State that Platform Metrics Object Storage gauges are authoritative for the selected period when available, account-wide, never summed as counters, and local storage/model values are fallback. State Compute and NGWAF scope.

- [ ] **Step 2: Run focused backend tests**

```bash
perl -e 'alarm 240; exec @ARGV' uv run pytest -q -x \
  tests/models/test_models.py::test_platform_usage_response_preserves_unavailable_and_measured_sections \
  tests/routers/test_usage_platform.py \
  tests/routers/test_usage_prefill.py
```

- [ ] **Step 3: Run Cloud and frontend tests**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml platform_usage --quiet
cd frontend
npx vitest run lib/cloud-engine/adapter.test.ts __tests__/app/usage-platform.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
```

- [ ] **Step 4: Run lint, format, type, and drift gates**

```bash
uv run ruff check backend/models/admin/operations.py backend/routers/usage.py tests/models/test_models.py tests/routers/test_usage_platform.py
uv run ruff format --check backend/models/admin/operations.py backend/routers/usage.py tests/models/test_models.py tests/routers/test_usage_platform.py
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
cd frontend && npm run typecheck
make openapi-drift
```

- [ ] **Step 5: Commit docs and request final review**

```bash
git add docs/pages/usage-and-cost.md
git commit -m "Document measured Platform usage sources"
```

Review the accumulated rc3 diff, fix all Critical/Important findings, then stop before any live deployment. A combined Cloud + Elevation deploy/test requires a fresh explicit approval for that run.
