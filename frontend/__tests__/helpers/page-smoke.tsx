/**
 * Smoke-test scaffolding for the analytics page tests (R-6).
 *
 * Each page is a thin shell around `ReportLayout` + a few section
 * components. The smoke layer only needs to verify the page mounts,
 * the title renders, and no `console.error` fires. The section bodies
 * are exercised in their own component specs and (Phase 3) the
 * Playwright journeys.
 *
 * The cross-page mocks (serviceStore, filterStore, next/navigation,
 * PlotlyChart, ReportLayout) are exported here as factory functions
 * (`serviceStoreModuleMock`, `filterStoreModuleMock`, `navigationModuleMock`,
 * `plotlyChartModuleMock`, `reportLayoutModuleMock`). They cannot be installed
 * by a single helper call because `vi.mock` is hoisted above imports — so each
 * page test keeps its own top-level `vi.mock(path, async () => ...)` line and
 * delegates the factory body to the matching helper here. Individual page tests
 * still call `vi.mock` for their own section components.
 */

import { vi, type MockInstance } from 'vitest'
import React from 'react'

/**
 * Children-prop shape passed into the ReportLayout mock. Default values
 * cover the keys every page reads off the render-prop callback. Per-page
 * tests pass `dataOverride` when a section reads a specific field.
 */
export type ReportLayoutChildArgs = {
  data?: unknown
  isLoading?: boolean
  isFetching?: boolean
  config?: Record<string, unknown>
  setChartInterval?: (interval: string) => void
  trend?: string
  setTrend?: (trend: string) => void
  intervalButtons?: React.ReactNode
  bucketSeconds?: number
  startTime?: string | null
  endTime?: string | null
  timezone?: string
  activeServiceId?: string | null
  filterPayload?: Record<string, unknown>
}

export const REPORT_LAYOUT_DEFAULT_ARGS: Required<ReportLayoutChildArgs> = {
  data: undefined,
  isLoading: false,
  isFetching: false,
  config: {},
  setChartInterval: () => {},
  trend: 'flat',
  setTrend: () => {},
  intervalButtons: null,
  bucketSeconds: 3600,
  startTime: '2026-01-01T00:00:00Z',
  endTime: '2026-01-01T01:00:00Z',
  timezone: 'UTC',
  activeServiceId: 'test-svc',
  filterPayload: {},
}

/**
 * Drop-in spy on console.error. Returns the spy so the calling test can
 * `expect(spy).not.toHaveBeenCalled()` after render. Auto-restore via the
 * caller's afterEach.
 */
export function spyOnConsoleError(): MockInstance {
  return vi.spyOn(console, 'error').mockImplementation(() => {})
}

// ── Shared cross-page module mocks ───────────────────────────────────────────
// Each returns the module-shape object a page test's `vi.mock` factory should
// return. Usage (keeps vi.mock hoisting intact):
//   vi.mock('@/stores/serviceStore', async () =>
//     (await import('../helpers/page-smoke')).serviceStoreModuleMock())

/** `@/stores/serviceStore` — pass `accessLevel` for pages that read it (usage). */
export function serviceStoreModuleMock(opts: { accessLevel?: string } = {}) {
  const service: Record<string, unknown> = { id: 'test-svc', name: 'Test' }
  if (opts.accessLevel) service.accessLevel = opts.accessLevel
  const state = {
    activeServiceId: 'test-svc',
    isInitialized: true,
    services: [service],
    setServices: vi.fn(),
    setInitialized: vi.fn(),
    setActiveServiceId: vi.fn(),
  }
  const useServiceStore = vi.fn((selector?: (s: Record<string, unknown>) => unknown) => {
    return selector ? selector(state) : state
  })
  useServiceStore.getState = () => state
  return { useServiceStore }
}

/** `@/stores/filterStore`. */
export function filterStoreModuleMock() {
  return {
    useFilterStore: vi.fn((selector?: (s: Record<string, unknown>) => unknown) => {
      const state = {
        startTime: '2026-01-01T00:00:00Z',
        endTime: '2026-01-01T01:00:00Z',
        filters: [],
        isAutoRange: false,
        compareMode: false,
        setRange: vi.fn(),
        clearFilters: vi.fn(),
      }
      return selector ? selector(state) : state
    }),
  }
}

/** `next/navigation` — each page passes its own pathname. */
export function navigationModuleMock(pathname: string) {
  return {
    usePathname: vi.fn(() => pathname),
    useSearchParams: vi.fn(() => new URLSearchParams()),
    useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
  }
}

/** `@/components/PlotlyChart`. */
export function plotlyChartModuleMock() {
  return { PlotlyChart: () => <div data-testid="plotly-chart" /> }
}

/**
 * `@/components/ReportLayout` — the render-prop child args are page-specific,
 * so each caller passes the exact object its page reads (no defaults merged,
 * to keep the children() call byte-identical to the inlined mock).
 */
export function reportLayoutModuleMock(childArgs: ReportLayoutChildArgs) {
  return {
    ReportLayout: ({ children, title }: { children: (a: ReportLayoutChildArgs) => React.ReactNode; title: string }) => (
      <div>
        <h1>{title}</h1>
        {children(childArgs)}
      </div>
    ),
  }
}
