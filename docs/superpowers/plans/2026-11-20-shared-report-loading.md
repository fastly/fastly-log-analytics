# Shared Report Loading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every analytics card show a consistent, accessible loading state while service-query prerequisites are unresolved, including Streaming's Cloud CMCD reads.

**Architecture:** Extend the shared `useServiceQuery` result so a query waiting only for data readiness remains `isLoading`, then make `AnalyticsCard` derive its default announcement from the card title. Keep explicitly disabled queries, retained-data refreshes, browser file caching, and browser-side analytics unchanged; cover the contract in shared hook/card tests plus a Streaming integration test.

**Tech Stack:** React 19, TanStack React Query v5, TypeScript, Vitest, Testing Library, Next.js App Router.

## Global Constraints

- Loading, error, empty, and content precedence stays unchanged.
- Explicit `loadingMessage` values remain authoritative.
- An intentionally disabled query must not show as loading.
- Background refreshes with retained data must not replace visible data with the initial loader.
- Cloud analytics stay in the browser: summary decode/merge in the dashboard worker and raw fallback through DuckDB-WASM.
- Cloud reloads continue to reuse identity/service-scoped `CacheStorage` files.
- Do not add polling, retries, route-specific loading banners, or CMCD data-path changes.

---

### Task 1: Define the shared query-readiness contract

**Files:**
- Modify: `frontend/hooks/useDashboardCards.ts:155-169`
- Test: `frontend/__tests__/app/usage-sessions-streaming.test.tsx:1-25`
- Test: `frontend/__tests__/app/usage-sessions-streaming.test.tsx:278-305`

**Interfaces:**
- Consumes: `useIsDataReady()`, TanStack `UseQueryResult<T, Error>`, and optional `enabled: boolean`.
- Produces: the existing `useServiceQuery` return shape with `isLoading: true` while an otherwise-enabled query has no data and is waiting for `useIsDataReady()`.

- [ ] **Step 1: Add imports for the shared hook test**

In `frontend/__tests__/app/usage-sessions-streaming.test.tsx`, import the hook:

```tsx
import { useServiceQuery } from '@/hooks/useDashboardCards'
```

- [ ] **Step 2: Add failing readiness tests**

Inside the existing `describe('Usage, Sessions, Streaming & CostCalculator', () => { ... })`, before the page tests, add:

```tsx
test('treats a service query waiting for app data as initially loading', () => {
  mockIsDataReady = false
  const queryFn = vi.fn(async () => ({ rows: [] }))

  const { result } = renderHook(
    () => useServiceQuery(['loading-contract', 'test-svc'], queryFn),
    { wrapper: makeQueryWrapper(queryClient) },
  )

  expect(result.current.isLoading).toBe(true)
  expect(queryFn).not.toHaveBeenCalled()
})

test('does not mark an explicitly disabled service query as loading', () => {
  mockIsDataReady = false
  const queryFn = vi.fn(async () => ({ rows: [] }))

  const { result } = renderHook(
    () => useServiceQuery(['loading-contract-disabled', 'test-svc'], queryFn, { enabled: false }),
    { wrapper: makeQueryWrapper(queryClient) },
  )

  expect(result.current.isLoading).toBe(false)
  expect(queryFn).not.toHaveBeenCalled()
})
```

- [ ] **Step 3: Run the focused tests and verify failure**

Run from `frontend/`:

```bash
npx vitest run __tests__/app/usage-sessions-streaming.test.tsx --testNamePattern="service query"
```

Expected: the first test fails because TanStack reports a disabled pending query with `isLoading: false`; the explicit-disabled test passes.

- [ ] **Step 4: Implement the minimal shared readiness state**

In `frontend/hooks/useDashboardCards.ts`, replace the direct `useQuery` return with:

```ts
const query = useQuery<T, Error, T>({
  ...options,
  queryKey,
  queryFn,
  enabled: isReady && (options?.enabled ?? true),
  placeholderData: keepPreviousWithinService(queryKey, serviceIdIndex),
})
const waitingForDataReady =
  !isReady &&
  (options?.enabled ?? true) &&
  query.isPending &&
  query.data === undefined &&
  !query.isError

return waitingForDataReady ? { ...query, isLoading: true } : query
```

- [ ] **Step 5: Run the focused contract tests**

```bash
npx vitest run __tests__/app/usage-sessions-streaming.test.tsx --testNamePattern="service query"
```

Expected: 2 tests pass; the waiting query is loading and the explicitly disabled query is idle.

- [ ] **Step 6: Commit the shared query fix**

```bash
git add frontend/hooks/useDashboardCards.ts frontend/__tests__/app/usage-sessions-streaming.test.tsx
git commit -m "Treat service readiness wait as loading"
```

### Task 2: Make card loading copy contextual

**Files:**
- Modify: `frontend/components/AnalyticsCard.tsx:282-376`
- Create: `frontend/__tests__/components/AnalyticsCard.test.tsx`

**Interfaces:**
- Consumes: `AnalyticsCardProps.title`, optional `loadingMessage`, and `isLoading`.
- Produces: an accessible `role="status"` message of `Loading ${title}...` when `isLoading` is true and no explicit `loadingMessage` exists.

- [ ] **Step 1: Create focused failing card tests**

Create `frontend/__tests__/components/AnalyticsCard.test.tsx`:

```tsx
/** @vitest-environment jsdom */
import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { AnalyticsCard } from '@/components/AnalyticsCard'

describe('AnalyticsCard loading copy', () => {
  it('derives an accessible loading message from the card title', () => {
    render(
      <AnalyticsCard title="Active Viewers" isLoading>
        <div>loaded content</div>
      </AnalyticsCard>,
    )

    expect(screen.getByRole('status')).toHaveTextContent('Loading Active Viewers...')
  })

  it('preserves an explicit loading message', () => {
    render(
      <AnalyticsCard
        title="Active Viewers"
        isLoading
        loadingMessage="Reading streaming summaries..."
      >
        <div>loaded content</div>
      </AnalyticsCard>,
    )

    expect(screen.getByRole('status')).toHaveTextContent('Reading streaming summaries...')
  })
})
```

- [ ] **Step 2: Run the test and verify failure**

```bash
npx vitest run __tests__/components/AnalyticsCard.test.tsx
```

Expected: the first test fails because the current message is `Loading data...`; the explicit-message test passes.

- [ ] **Step 3: Derive the fallback from the title**

In `frontend/components/AnalyticsCard.tsx`, define a local fallback before the render:

```tsx
const effectiveLoadingMessage = loadingMessage ?? `Loading ${title}...`
```

Replace the loading text node with:

```tsx
<span className="text-xs text-foreground animate-pulse font-medium">
  {effectiveLoadingMessage}
</span>
```

- [ ] **Step 4: Run the focused card tests**

```bash
npx vitest run __tests__/components/AnalyticsCard.test.tsx
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit the card behavior**

```bash
git add frontend/components/AnalyticsCard.tsx frontend/__tests__/components/AnalyticsCard.test.tsx
git commit -m "Make analytics loading messages contextual"
```

### Task 3: Verify Streaming's initial loading state

**Files:**
- Test: `frontend/__tests__/app/usage-sessions-streaming.test.tsx:456-469`

**Interfaces:**
- Consumes: the Task 1 `useServiceQuery.isLoading` contract and Task 2 `AnalyticsCard` title fallback.
- Produces: a regression assertion that Streaming announces a loading card before `mockIsDataReady` becomes true.

- [ ] **Step 1: Add a failing Streaming prerequisite-wait test**

Immediately before the existing CMCD request test, add:

```tsx
test('shows a contextual loading state while Streaming prerequisites are pending', () => {
  mockIsDataReady = false

  renderWithQuery(<StreamingClient />, { queryClient })

  expect(screen.getByText('Loading Active Viewers...')).toBeInTheDocument()
  expect(screen.getByText('Loading Buffer Health...')).toBeInTheDocument()
  expect(mockPost).not.toHaveBeenCalled()
})
```

- [ ] **Step 2: Run the test and verify behavior**

```bash
npx vitest run __tests__/app/usage-sessions-streaming.test.tsx --testNamePattern="Streaming prerequisites"
```

Expected: PASS after Tasks 1 and 2. If it fails, fix the shared contract rather than adding a Streaming-only branch.

- [ ] **Step 3: Run all focused Streaming and shared-component tests**

```bash
npx vitest run __tests__/app/usage-sessions-streaming.test.tsx __tests__/components/AnalyticsCard.test.tsx
```

Expected: all tests in both files pass.

- [ ] **Step 4: Commit the integration regression test**

```bash
git add frontend/__tests__/app/usage-sessions-streaming.test.tsx
git commit -m "Cover streaming prerequisite loading"
```

### Task 4: Verify cache reuse and browser execution remain unchanged

**Files:**
- No implementation files.
- Test: `frontend/lib/cloud-engine/cmcd.test.ts`
- Test: `frontend/lib/cloud-engine/dashboard/sources.test.ts`
- Test: `frontend/lib/cloud-engine/rollup-answer.test.ts`

**Interfaces:**
- Consumes: the existing Cloud CMCD rollup path, `browserSource` persistent cache, and `answerFromRollup` worker boundary.
- Produces: evidence that the loading-state change did not move analytics server-side or alter reload cache reuse.

- [ ] **Step 1: Run the Cloud CMCD summary/raw fallback suite**

```bash
npx vitest run lib/cloud-engine/cmcd.test.ts
```

Expected: summary-first and raw-fallback CMCD tests pass.

- [ ] **Step 2: Run persistent browser-source cache tests**

```bash
npx vitest run lib/cloud-engine/dashboard/sources.test.ts -t "isolates manifest identities in memory and persistent file caches"
```

Expected: identity-scoped Cache Storage reuse tests pass.

- [ ] **Step 3: Run worker rollup tests**

```bash
npx vitest run lib/cloud-engine/rollup-answer.test.ts
```

Expected: `/api/cmcd/aggregates` remains in the browser rollup path, and summary computation stays in the dashboard engine worker.

- [ ] **Step 4: Run typecheck and lint**

```bash
npx tsc --noEmit
npx eslint components/AnalyticsCard.tsx hooks/useDashboardCards.ts __tests__/components/AnalyticsCard.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
```

Expected: both commands exit 0.

---

## Plan Self-Review

- Spec coverage: Task 1 implements readiness-aware loading; Task 2 implements contextual copy; Task 3 verifies Streaming; Task 4 verifies cache reuse and browser-side execution. Error and background-refresh behavior remain covered by explicit-disabled and retained-data constraints.
- Placeholder scan: every task contains concrete code, commands, and expected output.
- Type consistency: `useServiceQuery` retains its existing return type, `AnalyticsCard` keeps its existing props, and `loadingMessage` remains optional `string`.
