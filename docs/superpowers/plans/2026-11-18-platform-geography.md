# Usage Geographic Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add service-scoped request and bandwidth attribution by region or datacenter to the Usage & Cost page without changing billing totals.

**Architecture:** Extend the shared Platform Metrics client to accept `group_by`, aggregate daily grouped rows into a stable response in FastAPI and Compute, and render one independent TanStack Query card with a Plotly horizontal chart and accessible table.

**Tech Stack:** FastAPI/Pydantic, Rust Fastly Compute, Next.js/React, TanStack Query, Plotly, Vitest, pytest, cargo test.

## Global Constraints

- Default to `region`; fetch `datacenter` only after the user selects it.
- Query only the delivery service already used for Usage bandwidth.
- Sum counters only; never alter cost totals.
- Never return service IDs, tokens, cursors, or raw API bodies.
- Reject duplicate `(timestamp, group)` rows and malformed/out-of-window values.
- Missing/unavailable geography shows an unavailable card, never invented zero groups.

---

### Task 1: Grouped Platform Metrics support

**Files:**
- Modify: `backend/core/fastly/client.py`
- Test: `tests/core/test_fastly.py`

**Interfaces:**
- Consumes: existing `fastly_platform_metrics(...)`.
- Produces: optional `group_by: Literal["region", "datacenter"] | None`, preserving the group field on normalized records.

- [ ] **Step 1: Add a failing grouped request test**

```python
def test_platform_metrics_adds_supported_group_by_and_preserves_group():
    with patch.object(fastly_client, "fastly", return_value={
        "data": [{"timestamp": "2026-07-23T00:00:00Z", "region": "europe", "requests": 3}]
    }) as fetch:
        result = fastly_platform_metrics(..., group_by="region")
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(fetch.call_args.args[1]).query)
    assert query["group_by"] == ["region"]
    assert result["data"][0]["region"] == "europe"
```

- [ ] **Step 2: Verify failure**

```bash
pytest --confcutdir=tests/core tests/core/test_fastly.py::test_platform_metrics_adds_supported_group_by_and_preserves_group -q -x
```

- [ ] **Step 3: Implement validated grouping**

```python
group_by: Literal["region", "datacenter"] | None = None
...
if group_by is not None:
    if group_by not in ("region", "datacenter"):
        raise ValueError("Unsupported Platform Metrics group_by")
    params["group_by"] = group_by
```

Keep `timestamp` uniqueness as `(epoch, record[group_by])` when grouping; without grouping retain epoch-only uniqueness. Reject a missing/non-string/blank group.

- [ ] **Step 4: Run focused tests and lint**

```bash
pytest --confcutdir=tests/core tests/core/test_fastly.py -k platform_metrics -q -x
uv run ruff check backend/core/fastly/client.py tests/core/test_fastly.py
```

- [ ] **Step 5: Commit**

```bash
git add backend/core/fastly/client.py tests/core/test_fastly.py
git commit -m "Support grouped Platform Metrics queries"
```

### Task 2: High-Scale geography endpoint

**Files:**
- Modify: `backend/models/admin/operations.py`
- Modify: `backend/models/admin/__init__.py`
- Modify: `backend/routers/usage.py`
- Create: `tests/routers/test_usage_geography.py`

**Interfaces:**
- Consumes: grouped `fastly_platform_metrics`.
- Produces: `GET /api/usage/geography`.

- [ ] **Step 1: Add response model and endpoint tests**

```python
def test_geography_groups_regions_and_computes_shares(monkeypatch):
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", lambda **_: {
        "data": [
            {"start_time": D0, "region": "europe", "requests": 7, "bandwidth": 700},
            {"start_time": D1, "region": "usa", "requests": 3, "bandwidth": 300},
        ]
    })
    response = usage.geography(..., group_by="region", source=SOURCE)
    assert response.requests == 10
    assert response.bandwidth_bytes == 1000
    assert [(row.location, row.request_share) for row in response.data] == [
        ("europe", 0.7), ("usa", 0.3)
    ]
```

Also test datacenter, unknown group 422/400, no token/service, empty rows, malformed group, token-scoped caching, and no `_debug_calls` identifiers through the middleware.

- [ ] **Step 2: Verify the tests fail**

```bash
pytest --confcutdir=tests/routers tests/routers/test_usage_geography.py -q -x
```

- [ ] **Step 3: Implement aggregation**

Select `cdn_service_id or logging_service_id or service_id`. Request daily metrics `("requests", "bandwidth")`, group by the validated field. Aggregate with:

```python
groups[location]["requests"] += int(row.get("requests") or 0)
groups[location]["bandwidth_bytes"] += int(row.get("bandwidth") or 0)
```

Compute shares as `value / total` or `0.0` when total is zero. Sort by requests descending, then location. Return direct `PlatformGeographyResponse(...)` construction to suppress tracked entity paths in debug mode.

- [ ] **Step 4: Generate types and validate**

```bash
make gen-types
pytest --confcutdir=tests/routers tests/routers/test_usage_geography.py -q -x
uv run ruff check backend/models/admin/operations.py backend/routers/usage.py tests/routers/test_usage_geography.py
```

- [ ] **Step 5: Commit**

```bash
git add backend/models/admin/operations.py backend/models/admin/__init__.py backend/routers/usage.py tests/routers/test_usage_geography.py frontend/openapi.json frontend/types/api.generated.ts
git commit -m "Add High-Scale usage geography"
```

### Task 3: Cloud geography endpoint

**Files:**
- Modify: `compute/cloud/src/sdk.rs`
- Modify: `compute/cloud/src/telemetry/usage.rs`
- Modify: `compute/cloud/src/admin/logs.rs`
- Modify: `compute/cloud/tests/ingest.rs`

**Interfaces:**
- Consumes: front `service_id`, existing Platform fetch helper.
- Produces: `GET /api/admin/usage/geography`.

- [ ] **Step 1: Add grouped parser and route tests**

```rust
#[test]
fn geography_sums_regions_and_shares() {
    let out = platform_geography(
        &json!({"data": [
            {"timestamp":"2026-11-11T00:00:00Z","region":"europe","requests":7,"bandwidth":700},
            {"timestamp":"2026-11-11T00:00:00Z","region":"usa","requests":3,"bandwidth":300}
        ]}),
        1_794_355_200,
        1_794_441_600,
        "region",
    ).unwrap();
    assert_eq!(out["requests"], 10);
    assert_eq!(out["data"][0]["location"], "europe");
}
```

Add tests for datacenter, empty rows, missing group, duplicate `(timestamp, group)`, bad `group_by`, front service not configured, and one request call.

- [ ] **Step 2: Verify failure**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml geography --quiet
```

- [ ] **Step 3: Implement grouped fetch and parser**

Extend `fetch_platform_metrics` to accept an optional `group_by` and include exactly `group_by=region|datacenter` in the encoded query. Use the front service's configured ID, never the app/worker IDs. Aggregate by location in `BTreeMap`.

- [ ] **Step 4: Validate and commit**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml geography --quiet
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
cargo check --manifest-path compute/cloud/Cargo.toml --tests --target wasm32-wasip1 --quiet
git add compute/cloud/src/sdk.rs compute/cloud/src/telemetry/usage.rs compute/cloud/src/admin/logs.rs compute/cloud/tests/ingest.rs
git commit -m "Add Cloud usage geography"
```

### Task 4: Cloud adapter and geography card

**Files:**
- Modify: `frontend/lib/cloud-engine/native-routes.ts`
- Modify: `frontend/lib/cloud-engine/adapter.ts`
- Modify: `frontend/lib/cloud-engine/cloud-api.generated.ts`
- Create: `frontend/app/usage/_sections/GeographyCard.tsx`
- Modify: `frontend/app/usage/_sections/UsageClient.tsx`
- Test: `frontend/__tests__/app/usage-geography.test.tsx`

**Interfaces:**
- Consumes: `/api/usage/geography`.
- Produces: responsive chart and accessible table.

- [ ] **Step 1: Add failing adapter/UI tests**

```tsx
it("defaults to region and refetches datacenter rows when selected", async () => {
  render(<UsageGeographyCard ... />)
  expect(fetcher).toHaveBeenCalledWith("region")
  await user.click(screen.getByRole("button", {name: "Datacenter"}))
  expect(fetcher).toHaveBeenCalledWith("datacenter")
})
```

Also assert no data, unavailable, totals, and that the card emits no cost callback.

- [ ] **Step 2: Run the test to verify failure**

```bash
cd frontend
npx vitest run __tests__/app/usage-geography.test.tsx
```

- [ ] **Step 3: Implement adapter pass-through**

Register `/api/admin/usage/geography` in native routes and `/api/usage/geography` in the Cloud adapter. Preserve `start`, `end`, and `group_by` exactly.

- [ ] **Step 4: Implement card**

Use `AnalyticsCard`, `ButtonGroup`, `Button`, and `PlotlyChart`. Region/datacenter state stays local. Query with:

```ts
useQuery({
  queryKey: ["usage","geography", activeServiceId, startTime, endTime, groupBy],
  queryFn: () => client.GET("/api/usage/geography", ...),
  staleTime: 60_000,
})
```

Plot the top 10 by selected measure and render all returned rows in a keyboard-readable table below it. Include a toggle for Requests/Bandwidth and label shares. Do not pass geography into any calculator or stat-card cost prop.

- [ ] **Step 5: Run focused frontend gates**

```bash
npx vitest run __tests__/app/usage-geography.test.tsx lib/cloud-engine/adapter.test.ts
npx tsc --noEmit
npm run lint -- app/usage/_sections/GeographyCard.tsx app/usage/_sections/UsageClient.tsx
```

- [ ] **Step 6: Commit**

```bash
git add frontend/lib/cloud-engine/native-routes.ts frontend/lib/cloud-engine/adapter.ts frontend/lib/cloud-engine/cloud-api.generated.ts frontend/app/usage/_sections/GeographyCard.tsx frontend/app/usage/_sections/UsageClient.tsx frontend/__tests__/app/usage-geography.test.tsx
git commit -m "Add usage traffic geography"
```

### Task 5: Integrate and document

**Files:**
- Modify: `docs/pages/usage-and-cost.md`

- [ ] **Step 1: Run focused integrated verification**

```bash
pytest --confcutdir=tests/routers tests/routers/test_usage_geography.py -q -x
cargo test --manifest-path compute/cloud/Cargo.toml geography --quiet
cd frontend && npx vitest run __tests__/app/usage-geography.test.tsx lib/cloud-engine/adapter.test.ts
```

- [ ] **Step 2: Run static/drift gates**

```bash
make openapi-drift
uv run mypy backend
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
cd frontend && npx tsc --noEmit
```

- [ ] **Step 3: Update documentation**

Describe region/datacenter scope, daily grouped metrics, exact service selection, and that this visualization never feeds billing totals.

- [ ] **Step 4: Final review and commit docs**

```bash
git add docs/pages/usage-and-cost.md
git commit -m "Document usage geography"
```

Stop before any live deploy. The combined Cloud + Elevation run requires fresh explicit approval.
