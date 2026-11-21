# Cloud Dashboard XHR Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Cloud's startup metadata waterfall and operational-Parquet polling while preserving browser-side analytics, authorization, cache identity, and freshness.

**Architecture:** Compute will serve one private `/api/bootstrap` response by composing existing service/session/catalog/view helpers. The frontend will treat that route as native and hydrate dependent query caches. Cloud header freshness will switch from `readIngestEvents`' Parquet scan to the existing one-event `/api/ingest/events` endpoint; analytics file discovery and Parquet execution remain in the browser.

**Tech Stack:** Rust, Fastly Compute shell, React 19, TanStack React Query v5, TypeScript, Vitest, Cargo tests.

## Global Constraints

- Native bootstrap responses are private and identity-, role-, service-, and host-scoped.
- Existing non-admin service redaction and analyst-no-PII saved-view filtering remain authoritative.
- Mandatory bootstrap failures return a non-2xx response; never return a partial success-shaped object.
- `/api/files` and `/api/file` stay client-owned for range-specific browser analytics.
- DuckDB-WASM and summary aggregation stay in the browser.
- `/api/sync-status` and last-sync cron lookup keep their five-minute polling behavior.
- Static JS/CSS chunking and precompression are out of scope.

---

### Task 1: Serve native Cloud bootstrap

**Files:**
- Modify: `compute/cloud/src/shell.rs:780-843`
- Test: `compute/cloud/src/shell.rs`

**Interfaces:**
- Consumes: authenticated `User`, `crate::admin::handle`, `views::list_for_role`, existing `redact_services`, active service records, and `env!("FLP_BUILD_SHA")`.
- Produces: private `GET /api/bootstrap` with `user`, `services`, `active_service_id`, `active_log_field_ids`, `log_fields_catalog`, `views`, `build_sha`, capability flags, settings, and debug state.

- [ ] **Step 1: Add signed-out and signed-in bootstrap contract tests**

In the existing `shell.rs` test module, add tests using the standard `platform(true)`, `MemoryStore`, and signed-session helpers already used around the `/api/auth/session` tests. Assert:

```rust
let body: Value = serde_json::from_slice(&response.body).unwrap();
assert_eq!(body["runtime_mode"], "cloud");
assert_eq!(body["user"], Value::Null);
assert_eq!(body["services"], json!([]));
assert_eq!(body["active_service_id"], Value::Null);
assert_eq!(body["settings"]["needs_login"], true);
```

For a signed-in active service, assert the body includes a selected service, `active_log_field_ids`, `log_fields_catalog`, `views`, and a non-empty `build_sha`. Add an analyst-no-PII fixture containing one benign and one PII view and assert only the benign row is present. Add an analyst service assertion that storage and secret-reference fields are absent while `service_id`, capabilities, and active field IDs remain.

- [ ] **Step 2: Run the focused Rust tests and verify failure**

```bash
cargo test -p flp-cloud shell::tests:: -- --nocapture
```

Expected: the new bootstrap tests fail because `/api/bootstrap` is not handled.

- [ ] **Step 3: Add one native bootstrap handler**

Immediately after `/api/services` handling, add a branch for `GET /api/bootstrap`. Build synthetic admin requests by cloning the current headers:

```rust
if path == "/api/bootstrap" && request.method == "GET" {
    let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
    let mut base_headers = request.headers.clone();
    base_headers.insert("content-type".into(), "application/json".into());

    let service_request = Request {
        method: "GET".into(),
        path: "/api/services".into(),
        headers: base_headers.clone(),
        body: Vec::new(),
    };
    let service_response = crate::admin::handle(self.runtime, &user.id, &service_request);
    let service_response = if user.role == Role::Admin {
        service_response
    } else {
        redact_services(service_response)
    };
    if service_response.status != 200 {
        return Ok(service_response);
    }
    let service_body: Value = serde_json::from_slice(&service_response.body).map_err(|_| AuthError::Storage)?;
    let services = service_body["services"].as_array().cloned().unwrap_or_default();
    let requested = request
        .query_param("service_id")
        .or_else(|| request.header("x-service-id").map(str::to_owned));
    let active = services
        .iter()
        .find(|service| requested.as_deref() == service["service_id"].as_str())
        .or_else(|| services.first());
    let active_id = active.and_then(|service| service["service_id"].as_str()).map(str::to_owned);
    let catalog = if let Some(service_id) = active_id.as_deref() {
        let catalog_request = Request {
            method: "GET".into(),
            path: format!("/api/log-fields/catalog?service_id={service_id}"),
            headers: base_headers.clone(),
            body: Vec::new(),
        };
        let catalog_response = crate::admin::handle(self.runtime, &user.id, &catalog_request);
        if catalog_response.status != 200 {
            return Ok(catalog_response);
        }
        Some(serde_json::from_slice::<Value>(&catalog_response.body).map_err(|_| AuthError::Storage)?)
    } else {
        None
    };
    let views = active_id
        .as_deref()
        .map(|service_id| crate::views::list_for_role(self.runtime, service_id, user.role, crate::views::MAX_VIEWS))
        .transpose()?
        .unwrap_or_default();

    let active_fields = active
        .and_then(|service| service["active_log_field_ids"].as_array().cloned())
        .unwrap_or_default();
    return Ok(json_response(json!({
        "runtime_mode": "cloud",
        "user": user_json(user),
        "services": services,
        "active_service_id": active_id,
        "active_log_field_ids": active_fields,
        "log_fields_catalog": catalog,
        "views": views,
        "build_sha": env!("FLP_BUILD_SHA"),
        "ngwaf_configured": false,
        "fastly_bots_available": true,
        "settings": {
            "needs_login": false,
            "is_remote_analyst": matches!(user.role, Role::Analyst | Role::AnalystNoPii),
            "mask_ips": user.role == Role::AnalystNoPii,
            "initializing": false
        },
        "debug_state": {
            "query_debug_visibility": "disabled",
            "api_call_debug_visibility": "disabled"
        }
    })));
}
```

Keep stale/anonymous handling in the auth route contract. If the browser needs a login bootstrap, serve the same anonymous body from a native bootstrap branch before requiring `user`; do not make `401` the successful signed-out shape used by the current adapter.

- [ ] **Step 4: Preserve debug and cookie behavior**

For a valid signed-in session, read settings with the same `authenticate_with_settings` path used by `/api/auth/session`, and populate the two debug visibility strings. For an expired cookie, return the anonymous bootstrap with the existing `clear_session_and_cdn_cookies` calls. Add assertions that no mandatory response field is `null` due to a malformed synthetic request.

- [ ] **Step 5: Run focused tests**

```bash
cargo test -p flp-cloud shell::tests:: -- --nocapture
```

Expected: all bootstrap tests and existing shell auth tests pass.

- [ ] **Step 6: Commit**

```bash
git add compute/cloud/src/shell.rs
git commit -m "Serve Cloud bootstrap natively"
```

### Task 2: Switch the browser adapter to native bootstrap

**Files:**
- Modify: `frontend/lib/cloud-engine/native-routes.ts:1-30`
- Modify: `frontend/lib/cloud-engine/adapter.ts:862-871`
- Modify: `frontend/lib/cloud-engine/adapter.ts:989-1113`
- Test: `frontend/lib/cloud-engine/endpoint-bridges.test.ts`
- Test: `frontend/lib/cloud-engine/metadata-cache.test.ts`

**Interfaces:**
- Consumes: Task 1's native response.
- Produces: exactly one browser request to `/api/bootstrap`, with existing cache identity and React Query seeds.

- [ ] **Step 1: Write failing adapter contract tests**

In `endpoint-bridges.test.ts`, replace the synthesized-bootstrap test setup with a fetch mock that returns a complete native bootstrap for `/api/bootstrap`. Assert:

```ts
const response = await handleCloudAnalyticalRequest(
  new Request('http://localhost/api/bootstrap'),
)
expect(await response?.json()).toEqual(nativeBootstrap)
expect(fetcher).toHaveBeenCalledTimes(1)
expect(fetcher.mock.calls[0][0]).toBe('/api/bootstrap')
```

Add cache tests that call the hook with native bootstrap and assert service, catalog, and views keys are populated and no `/api/auth/session`, `/api/services`, or `/api/log-fields/catalog` follow-up is required.

- [ ] **Step 2: Verify the tests fail**

```bash
npx vitest run lib/cloud-engine/endpoint-bridges.test.ts lib/cloud-engine/metadata-cache.test.ts
```

Expected: current adapter still calls `/api/auth/session` and `/api/services`.

- [ ] **Step 3: Make bootstrap native**

Add:

```ts
["GET", "/api/bootstrap"],
```

to `NATIVE_ROUTES`. Change the native forwarding condition in `adapter.ts` so the bootstrap route is forwarded like other native routes:

```ts
if (stage && isNativeCloudRoute(request.method, path)) {
  return withLoading(stage, async () => {
    const response = await fetchCloudMetadata(path, request.signal)
    return response
  })
}
```

Remove the browser synthesis branch at lines 989-1113. Keep `setDashboardCacheIdentity` in a response-processing layer: after a successful bootstrap JSON, derive the user and rotate the dashboard identity before other queries can run. If the native response cannot be parsed or lacks required fields, throw; do not construct a fallback object.

- [ ] **Step 4: Run adapter tests and typecheck**

```bash
npx vitest run lib/cloud-engine/endpoint-bridges.test.ts lib/cloud-engine/metadata-cache.test.ts
npx tsc --noEmit
```

Expected: pass, including identity invalidation and cache seed parity.

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/cloud-engine/native-routes.ts frontend/lib/cloud-engine/adapter.ts frontend/lib/cloud-engine/endpoint-bridges.test.ts frontend/lib/cloud-engine/metadata-cache.test.ts
git commit -m "Use native Cloud bootstrap"
```

### Task 3: Remove the footer version request

**Files:**
- Modify: `frontend/components/CloudVersionFooter.tsx:1-32`
- Test: `frontend/__tests__/components/AppLayout.test.tsx` or create `frontend/__tests__/components/CloudVersionFooter.test.tsx`

**Interfaces:**
- Consumes: `BootstrapResponse.build_sha` from `queryKeys.bootstrap()`.
- Produces: the same footer with no `/version` fetch.

- [ ] **Step 1: Add a failing footer test**

Mock or seed the bootstrap query with `{ build_sha: "abc123" }`, render the footer inside `makeQueryWrapper`, and assert:

```tsx
expect(screen.getByText(/commit:abc123/)).toBeInTheDocument()
expect(global.fetch).not.toHaveBeenCalled()
```

- [ ] **Step 2: Verify failure**

```bash
npx vitest run __tests__/components/CloudVersionFooter.test.tsx
```

Expected: current component fetches `/version`.

- [ ] **Step 3: Read build SHA from React Query**

Replace the effect/state with:

```tsx
const bootstrap = useQueryClient().getQueryData<BootstrapResponse>(queryKeys.bootstrap())
const buildSha = bootstrap?.build_sha ?? 'unknown'
```

Subscribe through `useQuery({ queryKey, enabled: false })` if a non-reactive direct read does not update when hydration completes.

- [ ] **Step 4: Run test and commit**

```bash
npx vitest run __tests__/components/CloudVersionFooter.test.tsx
git add frontend/components/CloudVersionFooter.tsx frontend/__tests__/components/CloudVersionFooter.test.tsx
git commit -m "Read Cloud version from bootstrap"
```

### Task 4: Replace operational file polling with ingest events

**Files:**
- Modify: `frontend/hooks/useAdminStream.ts:60-151`
- Modify: `frontend/__tests__/hooks/useAdminStream.cloud.test.tsx`

**Interfaces:**
- Consumes: `GET /api/ingest/events?cursor=<last-id>`, one SSE frame with `id`, `event`, `data`, and `retry`.
- Produces: manifest observation and sync-status invalidation on `commit`/`resync`, without reading operational Parquet.

- [ ] **Step 1: Rewrite the hook tests around native events**

Replace the `readIngestEvents` mock with a `fetch` mock that returns SSE bodies:

```ts
const frame = (event: string, cursor: string, retry = 5000) =>
  `retry: ${retry}\nid: ${cursor}\nevent: ${event}\ndata: ${JSON.stringify({
    cursor,
    generation: 42,
    manifest: "processed/_catalog/current.json",
  })}\n\n`
```

Keep existing visibility, in-flight, cleanup, and service-isolation assertions. Add:

```ts
expect(fetch).toHaveBeenCalledWith(
  expect.stringContaining('/api/ingest/events?cursor=svc-1%3A42'),
  expect.objectContaining({ signal: expect.any(AbortSignal) }),
)
```

Assert `ready` does not invalidate; `commit` does; `resync` clears/invalidates through the existing catalog observation API.

- [ ] **Step 2: Run focused test and verify failure**

```bash
npx vitest run __tests__/hooks/useAdminStream.cloud.test.tsx
```

Expected: current hook calls `readIngestEvents` and does not parse SSE frames.

- [ ] **Step 3: Implement one-frame ingest polling**

In `useHeaderBadgeStream`, replace `browserOpsSource` and `readIngestEvents` with:

```ts
const url = new URL('/api/ingest/events', window.location.origin)
if (cursorRef.current) url.searchParams.set('cursor', cursorRef.current)
const response = await fetch(url, {
  headers: { Accept: 'text/event-stream', 'x-service-id': activeServiceId ?? '' },
  credentials: 'include',
  signal: controller.signal,
})
if (response.status === 401 || response.status === 403) return
if (!response.ok) throw new Error(`Ingest event read failed (${response.status})`)
const text = await response.text()
const parsed = parseIngestEventFrame(text)
```

The parser is responsible for `retry`, `id`, `event`, and the JSON `data`. Maintain the cursor as `string`, not a timestamp. Schedule the next read from the server's `retry` value, bounded to `1_000..30_000`. For the first request use no cursor; after each valid frame use its `id`. On `commit`, call:

```ts
observeCatalogCommit(
  undefined,
  { type: 'commit', generation: Number(data.generation), manifest: data.manifest },
)
```

If the helper cannot accept an undefined source, use the narrow existing source only for observation; do not call its Parquet read methods. On `resync`, call the same path with `type: 'resync'`.

- [ ] **Step 4: Run the hook tests and lint**

```bash
npx vitest run __tests__/hooks/useAdminStream.cloud.test.tsx
npx eslint hooks/useAdminStream.ts __tests__/hooks/useAdminStream.cloud.test.tsx
```

Expected: visibility, in-flight coalescing, service replacement, retry, commit, and resync tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/hooks/useAdminStream.ts frontend/__tests__/hooks/useAdminStream.cloud.test.tsx
git commit -m "Use Cloud ingest events for catalog updates"
```

### Task 5: Integration verification

**Files:**
- No implementation files.

- [ ] **Step 1: Run focused Cloud contract suites**

```bash
cargo test -p flp-cloud shell::tests:: views::tests:: -- --nocapture
cd frontend
npx vitest run lib/cloud-engine/endpoint-bridges.test.ts lib/cloud-engine/metadata-cache.test.ts __tests__/components/CloudVersionFooter.test.tsx __tests__/hooks/useAdminStream.cloud.test.tsx
```

Expected: all focused backend and frontend tests pass.

- [ ] **Step 2: Verify no startup waterfall regression**

In `endpoint-bridges.test.ts` and browser E2E instrumentation, assert the bootstrap request count is one and neither of these appears during initial Cloud dashboard mount:

```text
/api/auth/session
/api/services
/api/log-fields/catalog
/api/views/{service}
/version
```

The catalog/manifest analytics requests `/api/files` and `/api/file` remain expected.

- [ ] **Step 3: Verify recurring-request reduction**

In `useAdminStream.cloud.test.tsx`, assert each poll calls only:

```text
/api/ingest/events
```

and neither `/api/logs/cron_runs/live` nor `/api/files?dataset=ops:cron_runs` is called.

- [ ] **Step 4: Run type, lint, and browser cache invariants**

```bash
npx tsc --noEmit
npx eslint components/CloudVersionFooter.tsx hooks/useAdminStream.ts lib/cloud-engine/adapter.ts lib/cloud-engine/native-routes.ts __tests__/components/CloudVersionFooter.test.tsx __tests__/hooks/useAdminStream.cloud.test.tsx
npx vitest run lib/cloud-engine/cmcd.test.ts lib/cloud-engine/rollup-answer.test.ts lib/cloud-engine/dashboard/sources.test.ts
```

Expected: all pass; identity-scoped Cache Storage and browser-side CMCD/worker behavior remain unchanged.

## Plan Self-Review

- Spec coverage: Task 1 builds the private native response, Task 2 removes browser synthesis, Task 3 removes the footer request, Task 4 replaces operational file polling, and Task 5 proves analytics/cache invariants.
- Placeholder scan: every implementation task names concrete code boundaries, assertions, commands, and expected output.
- Type consistency: `build_sha` and bootstrap slices match the frontend bootstrap type and existing cache seed keys; ingest event cursor is consistently a `string` ID, while generation remains numeric.
