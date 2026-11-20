# SSR Bootstrap Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven implementation with a fresh worker per task and a verifier between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render config-gated shell/navigation from the canonical SSR bootstrap on first paint and move only proven-safe, default report seeds to SSR.

**Architecture:** The root layout remains the only bootstrap fetch owner. It passes a request-scoped shell snapshot and dehydrated React Query state into `AppLayout`; `AppLayout` uses that snapshot synchronously, while client refreshes and mutations update the same canonical cache and invalidate dependent metadata. A full-page audit adds SSR seeds only where an existing server helper exactly matches the initial client query key.

**Tech Stack:** Next.js App Router, React 19, TanStack React Query, Zustand, TypeScript, Vitest/Testing Library, Cloud adapter metadata cache.

## Global Constraints

- Use saved service config as the capability source of truth.
- High-Scale uses `rum_enabled` and `cmcd_enabled`.
- Cloud uses normalized `service.rum.enabled` and `service.cmcd.enabled` from `/api/services`.
- Do not infer navigation visibility from historical Parquet fields.
- Keep Cloud DuckDB/WASM scans, arbitrary filters, live streams, searches, polling, and mutations client-side.
- Never serialize credentials or privileged upstream headers.
- Keep the Cloud metadata cache at 15s sliding reuse and 60s hard max, identity-scoped and cleared on local writes.

---

### Task 1: Stabilize first-paint shell capability gates

**Files:**
- Modify: `frontend/app/layout.tsx`
- Modify: `frontend/components/AppLayout.tsx`
- Modify: `frontend/hooks/useBootstrap.ts`
- Test: `frontend/__tests__/components/AppLayout.test.tsx`

**Interfaces:**
- Consumes: `BootstrapResponse` from `frontend/lib/ssr/upstream.ts` and hydrated `queryKeys.bootstrap()`.
- Produces: synchronous `ssrShell` snapshot containing `activeServiceId`, `services`, `isAnalyst`, `isRemoteAnalyst`, `rumEnabled`, and `cmcdEnabled`.

- [ ] **Step 1: Add first-render regression tests**

Add two `AppLayout` tests in `frontend/__tests__/components/AppLayout.test.tsx`:

```tsx
test('renders RUM and Streaming on first paint from SSR capability props', () => {
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: undefined,
    isLoading: true,
    isSuccess: false,
  }))
  renderWithQueryClient(
    <AppLayout
      ssrActiveServiceId="svc-1"
      ssrIsRumEnabled
      ssrIsCmcdEnabled
    >
      <div>Content</div>
    </AppLayout>,
  )
  expect(screen.getByRole('link', { name: /^RUM$/ })).toHaveAttribute('href', '/rum')
  expect(screen.getByRole('link', { name: /^Streaming$/ })).toHaveAttribute('href', '/streaming')
})

test('does not wait for client bootstrap to hide disabled capability links', () => {
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: undefined,
    isLoading: true,
    isSuccess: false,
  }))
  renderWithQueryClient(
    <AppLayout ssrActiveServiceId="svc-1" ssrIsRumEnabled={false} ssrIsCmcdEnabled={false}>
      <div>Content</div>
    </AppLayout>,
  )
  expect(screen.queryByRole('link', { name: /^RUM$/ })).not.toBeInTheDocument()
  expect(screen.queryByRole('link', { name: /^Streaming$/ })).not.toBeInTheDocument()
})
```

- [ ] **Step 2: Run the focused test and verify failure**

Run:

```bash
npx vitest run frontend/__tests__/components/AppLayout.test.tsx --maxWorkers=1
```

Expected: the existing component does not accept `ssrIsCmcdEnabled`, and RUM may be absent while `useBootstrap` is pending.

- [ ] **Step 3: Implement an SSR shell snapshot**

In `frontend/app/layout.tsx`, after `fetchBootstrapServerSide()`, derive:

```tsx
const ssrShell = {
  activeServiceId: bootstrap?.active_service_id ?? null,
  isRumEnabled:
    !!bootstrap?.services?.find(s => s.service_id === bootstrap.active_service_id)?.rum_enabled,
  isCmcdEnabled:
    !!bootstrap?.services?.find(s => s.service_id === bootstrap.active_service_id)?.cmcd_enabled,
}
```

Pass `ssrShell` props into `AppLayout`:

```tsx
<AppLayout
  ssrActiveServiceId={ssrShell.activeServiceId}
  ssrIsRumEnabled={ssrShell.isRumEnabled}
  ssrIsCmcdEnabled={ssrShell.isCmcdEnabled}
  ...
>
```

In `frontend/components/AppLayout.tsx`, define:

```tsx
type AppLayoutProps = {
  ssrIsRumEnabled?: boolean
  ssrIsCmcdEnabled?: boolean
}
```

Derive:

```tsx
const activeSvc = bootstrapData?.services?.find(...)
const hasRum = activeSvc ? activeSvc.rum_enabled === true : ssrIsRumEnabled === true
const hasCmcd = activeSvc ? activeSvc.cmcd_enabled === true : ssrIsCmcdEnabled === true
```

Do not inspect historical schema for CMCD. Keep the client query as the eventual update source, not the only initial source.

- [ ] **Step 4: Run focused tests, typecheck, and lint**

```bash
npx vitest run __tests__/components/AppLayout.test.tsx --maxWorkers=1
npx tsc --noEmit
npx eslint components/AppLayout.tsx app/layout.tsx __tests__/components/AppLayout.test.tsx
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add app/layout.tsx components/AppLayout.tsx __tests__/components/AppLayout.test.tsx
git commit -m "Render feature-gated navigation from SSR bootstrap"
```

### Task 2: Canonicalize capability cache invalidation

**Files:**
- Modify: `frontend/lib/cloud-engine/metadata-cache.ts`
- Modify: `frontend/lib/cloud-engine/adapter.ts`
- Modify: `frontend/hooks/useBootstrap.ts`
- Test: `frontend/lib/cloud-engine/metadata-cache.test.ts`
- Test: `frontend/lib/cloud-engine/endpoint-bridges.test.ts`

**Interfaces:**
- Consumes: successful CMCD/RUM/logging/provision mutations handled by `handleCloudRequest`.
- Produces: invalidated `['bootstrap']`, `/api/services`, and metadata cache entries before dependent page state updates.

- [ ] **Step 1: Add failing invalidation test**

In `metadata-cache.test.ts`, add:

```ts
it('clears /api/services and bootstrap after a config mutation', async () => {
  const first = await handleCloudAnalyticalRequest(request('/api/bootstrap'))
  expect(first?.ok).toBe(true)
  await handleCloudAnalyticalRequest(
    new Request('http://localhost/api/services/fixture/logging-settings', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    }),
  )
  await handleCloudAnalyticalRequest(request('/api/bootstrap'))
  expect(fetcher).toHaveBeenCalledTimes(2)
})
```

- [ ] **Step 2: Run and verify failure**

```bash
npx vitest run lib/cloud-engine/metadata-cache.test.ts --maxWorkers=1
```

- [ ] **Step 3: Implement exact invalidation**

In `metadata-cache.ts`, add:

```ts
export function resetCloudMetadataPaths(paths: readonly string[]): void {
  for (const path of paths) metadataCache.delete(path)
}
```

In `adapter.ts`, after a successful config mutation:

```ts
resetCloudMetadataPaths(['/api/services', '/api/bootstrap', '/api/log-fields/catalog'])
queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
```

Keep the existing all-clear fallback for unknown mutations; use targeted invalidation for known config paths.

- [ ] **Step 4: Verify and commit**

```bash
npx vitest run lib/cloud-engine/metadata-cache.test.ts lib/cloud-engine/endpoint-bridges.test.ts --maxWorkers=1
npx tsc --noEmit
git add lib/cloud-engine/metadata-cache.ts lib/cloud-engine/adapter.ts lib/cloud-engine/metadata-cache.test.ts lib/cloud-engine/endpoint-bridges.test.ts
git commit -m "Invalidate canonical service capability metadata on writes"
```

### Task 3: Audit and seed core analytics defaults

**Files:**
- Audit/modify as proven: `frontend/app/{dashboard,network,insights,origin,performance,security,rum,sessions,streaming}/**`
- Audit/modify as proven: `frontend/lib/ssr/upstream.ts`
- Tests: existing page/test files under `frontend/__tests__/` and `frontend/lib/ssr/`

**Interfaces:**
- Produces: an audit table committed under `notes/` or session artifacts plus code changes only for safe SSR candidates.

- [ ] **Step 1: Build the audit matrix**

For every page, record:

```text
route | initial requests | available SSR helper | initial client query key | safe seed | reason
```

The classification must mark as client-only: DuckDB/WASM, filters, live streams, searches, range-dependent file planning, and mutations.

- [ ] **Step 2: Add query-key parity tests before each seed**

For every safe SSR candidate, add a test that proves the server seed key equals the client's first query key:

```ts
expect(serverSeed.queryKey).toEqual(clientInitialQueryKey({ serviceId, from, to, filters: {} }))
```

- [ ] **Step 3: Add only proven server seeds**

Reuse existing helpers in `lib/ssr/upstream.ts`. A page may call the helper only when:

```ts
resolveReportPageParams(...).isDefaultView === true
```

Dehydrate the response into the exact client query key; do not transform data on the server.

- [ ] **Step 4: Run page tests and typecheck**

Run the affected page suites plus:

```bash
npx tsc --noEmit
npx eslint app/{dashboard,network,insights,origin,performance,security,rum,sessions,streaming}
```

- [ ] **Step 5: Commit independently testable page batches**

Commit shell-safe seeds by page family, not as one broad audit commit.

### Task 4: Audit admin, control, and configuration defaults

**Files:**
- Audit/modify as proven: `frontend/app/{logs,usage,fastly-value,query,alerts,control-room,admin}/**`
- Test: matching `frontend/__tests__/app/**` and `frontend/__tests__/components/**`

- [ ] **Step 1: Build the admin/control audit matrix**

Classify setup/config, catalog, cron, usage, control-room, alert, query, and admin requests with the same safe/client-only rules as Task 3.

- [ ] **Step 2: Add SSR seeds only for stable default config**

Examples to evaluate, not assume:

```ts
// allowed candidates when exact key parity exists
log-fields catalog
cron/schedule snapshot
static usage configuration
service inventory
```

Do not SSR-seed live control-room streams, active jobs, arbitrary query execution, or mutations.

- [ ] **Step 3: Add auth-scope tests**

For each new seed, assert admin/analyst responses do not cross:

```ts
expect(await serverFetchAsAnalyst(...)).not.toContain(adminOnlyField)
expect(await serverFetchAsOtherUser(...)).toHaveBeenScopedTo(requestIdentity)
```

- [ ] **Step 4: Run focused suites and commit**

Run the changed page tests, typecheck, and lint; commit each independent page family.

### Task 5: Full integration and live verification

**Files:**
- Test: all changed files from Tasks 1–4
- Modify if needed: `docs/superpowers/specs/2026-11-20-ssr-bootstrap-shell-design.md`

- [ ] **Step 1: Run all focused suites together**

```bash
npx vitest run --maxWorkers=1
npx tsc --noEmit
npx eslint <changed frontend files>
```

- [ ] **Step 2: Run Cloud and High-Scale browser regressions**

Use existing page sweep/readiness tooling to verify:

```text
RUM and Streaming links present on first settled navigation
no nav pop-in/layout shift
default page data still exact after SSR seed
no duplicate initial API calls
```

- [ ] **Step 3: Deploy and verify all three environments**

Deploy the final SHA with the project orchestrator to all configured environments; verify each report separately and preserve explicit failures.

- [ ] **Step 4: Commit any audit documentation and final integration fix**

```bash
git add <exact audited files>
git commit -m "Integrate canonical SSR bootstrap audit"
```

## Self-review

- Spec coverage: Task 1 covers first-paint nav; Task 2 cache invalidation; Tasks 3–4 full app audit; Task 5 integration/live verification.
- Placeholder scan: all tasks name concrete files, commands, assertions, and commit boundaries.
- Type consistency: the plan uses `BootstrapResponse`, `ssrShell`, `AppLayoutProps`, and existing query helpers consistently.
