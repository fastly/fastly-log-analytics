# Historical DDoS Usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add delivery-service DDoS request/action history to the Observed Fastly Platform section without duplicating Real-Time Control Room status or changing billing.

**Architecture:** Extend the existing `/api/usage/platform` contract with an independent `ddos` section and daily/hourly points. High-Scale and Cloud use one delivery-service Platform Metrics query; frontend renders the section and compact trend through the existing Platform cards/query.

**Tech Stack:** FastAPI/Pydantic, Rust Fastly Compute, Next.js/React, TanStack Query, Plotly, Vitest, pytest, cargo test.

## Global Constraints

- A successful empty DDoS Platform response is measured zero; parse/auth/request failures are unavailable.
- Action counters are separate from request totals and never added to detected/mitigated.
- No delivery service ID, token, cursor, raw response, or error body is exposed.
- Missing delivery configuration is `not_configured`.
- Existing storage, Compute, NGWAF, geography, Real-Time DDoS, and cost totals remain unchanged.

---

### Task 1: Extend the Platform usage contract and High-Scale query

**Files:**
- Modify: `backend/models/admin/operations.py`
- Modify: `backend/models/admin/__init__.py`
- Modify: `backend/routers/usage.py`
- Modify: `tests/models/test_models.py`
- Modify: `tests/routers/test_usage_platform.py`

**Interfaces:**
- Consumes: delivery `cdn_service_id` and `fastly_platform_metrics`.
- Produces: `PlatformUsageResponse.ddos` plus `GET /api/usage/platform`.

- [ ] **Step 1: Add failing response and behavior tests**

```python
def test_platform_ddos_empty_success_is_measured_zero(monkeypatch):
    configure(monkeypatch, workspace=None)
    def metrics(**kwargs):
        if kwargs["metrics"][0] == "ddos_protection_requests_allow_count":
            return {"data": []}
        return {"data": []}
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())
    assert response.ddos.status == "ok"
    assert response.ddos.detected == response.ddos.mitigated == response.ddos.allowed == 0
    assert response.ddos.data == []
```

```python
def test_platform_ddos_failure_is_unavailable_not_zero(monkeypatch):
    configure(monkeypatch)
    def metrics(**kwargs):
        if "ddos_protection_requests_allow_count" in kwargs["metrics"]:
            raise RuntimeError("upstream unavailable")
        return {"data": []}
    response = usage.platform_usage(...)
    assert response.ddos.status == "unavailable"
```

Add sparse positive rows and actions:

```python
assert ddos.actions["tarpit"] == 3
assert sum(ddos.request points) == totals
```

- [ ] **Step 2: Verify tests fail**

```bash
pytest --confcutdir=tests/routers tests/routers/test_usage_platform.py -q -x
```

- [ ] **Step 3: Add models**

```python
class PlatformDdosPoint(BaseModel):
    time: str
    detected: int = 0
    mitigated: int = 0
    allowed: int = 0

class PlatformDdosUsage(BaseModel):
    status: PlatformSourceStatus
    scope: Literal["delivery_service"] | None = None
    detected: int = 0
    mitigated: int = 0
    allowed: int = 0
    actions: dict[str, int] = Field(default_factory=dict)
    data: list[PlatformDdosPoint] = Field(default_factory=list)
    reason: str | None = None
```

Add `ddos: PlatformDdosUsage` to `PlatformUsageResponse`.

- [ ] **Step 4: Add independent High-Scale fetch**

Use `cdn_service_id` only. Select granularity `day` except ranges at most 48 hours use `hour`. Query:

```python
DDOS_PLATFORM_METRICS = (
    "ddos_protection_requests_allow_count",
    "ddos_protection_requests_detect_count",
    "ddos_protection_requests_mitigate_count",
    "ddos_action_blackhole",
    "ddos_action_close",
    "ddos_action_downgrade",
    "ddos_action_limit_streams_connections",
    "ddos_action_limit_streams_requests",
    "ddos_action_tarpit",
    "ddos_action_tarpit_accept",
)
```

For no rows return status `ok`, zero totals, and empty data. Sum request metrics by bucket, sum actions across records, and use sanitized `request_failed`/`invalid_data`.

- [ ] **Step 5: Validate and commit**

```bash
pytest --confcutdir=tests/routers tests/routers/test_usage_platform.py -q -x
uv run ruff check backend/models/admin/operations.py backend/routers/usage.py tests/routers/test_usage_platform.py
make gen-types
git add backend/models/admin/operations.py backend/models/admin/__init__.py backend/routers/usage.py tests/models/test_models.py tests/routers/test_usage_platform.py frontend/openapi.json frontend/types/api.generated.ts
git commit -m "Add High-Scale DDoS history"
```

### Task 2: Extend Cloud DDoS history

**Files:**
- Modify: `compute/cloud/src/telemetry/usage.rs`
- Modify: `compute/cloud/src/sdk.rs`
- Modify: `compute/cloud/tests/ingest.rs`

**Interfaces:**
- Produces: `ddos` section through `/api/admin/usage/platform`.

- [ ] **Step 1: Add failing parser/route tests**

```rust
#[test]
fn ddos_empty_response_is_measured_zero() {
    let body = platform_ddos(&json!({"data":[]}), from, to, "hourly").unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["detected"], 0);
    assert_eq!(body["data"], json!([]));
}
```

Test positive counters/actions, malformed values, invalid range, missing front service, and one independent fetch.

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml platform_ddos --quiet
```

- [ ] **Step 3: Implement parser and independent fetch**

Add `PLATFORM_DDOS_FIELDS`, choose hourly for ranges <= 48h otherwise daily, fetch the front entity only, and parse:

```rust
fn platform_ddos(body, from, to, bucket_seconds) -> Result<Value,String>
```

No rows => ok zero. Any returned row must have valid timestamps and numeric optional fields. Build request counters and action object.

- [ ] **Step 4: Integrate response source**

`build_platform_usage_response` considers DDoS `not_configured` neutral, `ok` configured, and `unavailable` a partial failure. Existing storage/Compute/NGWAF output remains byte-for-byte compatible except for the new section.

- [ ] **Step 5: Validate and commit**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml platform_ddos --quiet
cargo check --manifest-path compute/cloud/Cargo.toml --tests --target wasm32-wasip1 --quiet
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
git add compute/cloud/src/telemetry/usage.rs compute/cloud/src/sdk.rs compute/cloud/tests/ingest.rs
git commit -m "Add Cloud DDoS usage history"
```

### Task 3: Render DDoS usage history

**Files:**
- Modify: `frontend/app/usage/_sections/PlatformUsageCards.tsx`
- Create: `frontend/app/usage/_sections/DdosUsageChart.tsx`
- Modify: `frontend/app/usage/_sections/UsageClient.tsx`
- Modify: `frontend/__tests__/app/usage-platform.test.tsx`

**Interfaces:**
- Consumes: `PlatformUsageResponse.ddos`.
- Produces: fourth card plus compact trend.

- [ ] **Step 1: Add failing UI tests**

```tsx
it("renders a measured-zero DDoS card without calling it unavailable", () => {
  render(<PlatformUsageCards data={{...platform, ddos: okZero}} cloudMode />)
  expect(screen.getByText("0 detected")).toBeInTheDocument()
  expect(screen.queryByText("Unavailable")).not.toBeInTheDocument()
})
```

Add trend tests for detected/mitigated/allowed and action text. Assert no DDoS values enter cost callbacks.

- [ ] **Step 2: Verify failure**

```bash
cd frontend
npx vitest run __tests__/app/usage-platform.test.tsx
```

- [ ] **Step 3: Implement card and chart**

Extend the Platform card grid to four cards. Use `PlotlyChart` with three traces and the existing dark/light colors. Put action totals in the card subtitle, and keep the chart below Observed Platform cards. Do not show a chart if `status !== "ok"`; an ok empty series renders a zero-state chart/card, not unavailable.

- [ ] **Step 4: Validate and commit**

```bash
npx vitest run __tests__/app/usage-platform.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
npx tsc --noEmit
npm run lint -- app/usage/_sections/PlatformUsageCards.tsx app/usage/_sections/DdosUsageChart.tsx app/usage/_sections/UsageClient.tsx
git add frontend/app/usage/_sections/PlatformUsageCards.tsx frontend/app/usage/_sections/DdosUsageChart.tsx frontend/app/usage/_sections/UsageClient.tsx frontend/__tests__/app/usage-platform.test.tsx
git commit -m "Show historical DDoS usage"
```

### Task 4: Integrate and verify

**Files:**
- Modify: `docs/pages/usage-and-cost.md`

- [ ] **Step 1: Run focused integrated gates**

```bash
pytest --confcutdir=tests/routers tests/routers/test_usage_platform.py -q -x
cargo test --manifest-path compute/cloud/Cargo.toml platform_ddos --quiet
cd frontend && npx vitest run __tests__/app/usage-platform.test.tsx
```

- [ ] **Step 2: Run static gates**

```bash
make openapi-drift
uv run mypy backend
cargo fmt --manifest-path compute/cloud/Cargo.toml --check
cd frontend && npx tsc --noEmit
```

- [ ] **Step 3: Document and request review**

Document DDoS history, successful-zero semantics, action/request distinction, and why Control Room remains Real-Time. Final review before commit.

- [ ] **Step 4: Stop before deployment**

Do not deploy or send traffic without fresh explicit approval for a combined Cloud + Elevation run.
