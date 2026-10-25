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
import React, { type ComponentProps } from 'react'
import type { PlotlyChart } from '@/components/PlotlyChart'
import { INITIAL_CONFIG } from '@/components/ProvisionWizard/types'

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
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state)),
    { getState: () => state },
  )
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
    useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn(), prefetch: vi.fn(), back: vi.fn() })),
  }
}

/** `@/components/PlotlyChart`. */
export function plotlyChartModuleMock(onRender?: (props: ComponentProps<typeof PlotlyChart>) => void) {
  return { PlotlyChart: (props: ComponentProps<typeof PlotlyChart>) => (onRender?.(props), <div data-testid="plotly-chart" />) }
}

/** `@/components/DataTable` smoke stub for page-level tests. */
export async function dataTableModuleMock() {
  const actual = await vi.importActual<typeof import('@/components/DataTable')>('@/components/DataTable')
  return { ...actual, DataTable: () => <div data-testid="data-table" />, ColumnVisibilityDropdown: () => null }
}

export type InteractiveTableRow = { name?: string; bot_name?: string; fingerprint?: string; ip?: string }
export type InteractiveDataTableMockProps = { data?: InteractiveTableRow[]; emptyMessage?: string }

/** Interactive `@/components/DataTable` mock for section tests that inspect rows + column toggles. */
export function interactiveDataTableModuleMock(onDataTable?: (props: InteractiveDataTableMockProps) => void) {
  return {
    DataTable: (props: InteractiveDataTableMockProps) => {
      onDataTable?.(props)
      return (
        <div data-testid={`data-table-${props.data?.length ?? 0}`}>
          {props.data?.length === 0 ? <span data-testid="empty">{props.emptyMessage}</span> : props.data?.map((row, idx) => {
            const label = row.name || row.bot_name || row.fingerprint || row.ip || String(idx)
            return <div key={label} data-testid="row">{label}</div>
          })}
        </div>
      )
    },
    ColumnVisibilityDropdown: (props: { columns: Array<{ id: string; label: string }>; onChange?: (id: string, visible: boolean) => void }) => (
      <button type="button" data-testid="col-vis" onClick={() => props.onChange?.(props.columns[0]?.id, false)}>cols</button>
    ),
  }
}

/** `@/components/FilterValueCell` passthrough mock for section tests. */
export function filterValueCellModuleMock() {
  return {
    FilterValueCell: (props: { display?: React.ReactNode; filters?: Array<{ value?: React.ReactNode }> }) => (
      <span>{props.display ?? props.filters?.[0]?.value}</span>
    ),
  }
}

/** `@/hooks/useLogFieldsCatalog` backed by a mutable `Set<string>` for section tests. */
export function activeLogFieldsSetModuleMock(activeFields: Set<string>) {
  return {
    useLogFieldsCatalog: () => ({ data: undefined, isLoading: false }),
    useFieldLabel: () => (id: string) => id,
    useFieldValues: () => ({ data: undefined, isLoading: false }),
    useActiveLogFields: () => ({
      ready: true,
      isFieldActive: (id: string) => activeFields.has(id),
      isGroupActive: () => false,
    }),
  }
}

/** `@/hooks/useDateFormat` smoke stub for page/component tests. */
export function dateFormatModuleMock(timezone = 'UTC') {
  return {
    useDateFormat: () => (d: unknown) => String(d ?? ''),
    useTimezone: () => timezone,
  }
}

/**
 * `@/components/ReportLayout` — the render-prop child args are page-specific,
 * so each caller passes the exact object its page reads (no defaults merged,
 * to keep the children() call byte-identical to the inlined mock).
 */
export function reportLayoutModuleMock(childArgs: ReportLayoutChildArgs) {
  return {
    ReportLayout: ({
      children,
      title,
      headerActions,
    }: {
      children: (a: ReportLayoutChildArgs) => React.ReactNode
      title: React.ReactNode
      headerActions?: React.ReactNode
    }) => (
      <div>
        <h1>{title}</h1>
        <div data-testid="header-actions">{headerActions}</div>
        {children(childArgs)}
      </div>
    ),
  }
}

/**
 * Shared `@/components/ui/select` mock factory for jsdom tests.
 */
export function selectMockFactory() {
  const SelectCtx = React.createContext<((v: string) => void) | null>(null)

  const Select = ({
    value,
    onValueChange,
    children,
  }: {
    value?: string
    onValueChange?: (v: string) => void
    children?: React.ReactNode
  }) => (
    <SelectCtx.Provider value={onValueChange ?? null}>
      <div data-testid="mock-select" data-select-value={value ?? ''}>
        {children}
      </div>
    </SelectCtx.Provider>
  )

  const SelectTrigger = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-trigger">{children}</div>
  )

  const SelectValue = ({ children }: { children?: React.ReactNode }) => (
    <span data-testid="mock-select-value">{children}</span>
  )

  const SelectContent = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-content">{children}</div>
  )

  const SelectSeparator = () => <hr data-testid="mock-select-separator" />

  const SelectItem = ({
    value,
    children,
  }: {
    value: string
    children?: React.ReactNode
  }) => {
    const onValueChange = React.useContext(SelectCtx)
    return (
      <button
        type="button"
        data-select-item-value={value}
        onClick={() => onValueChange?.(value)}
      >
        {children}
      </button>
    )
  }

  return {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
    SelectSeparator,
  }
}

export const API_BASE = 'http://127.0.0.1:8000'

export const FULL_CONFIG = {
  ...INITIAL_CONFIG,
  endpoint_name: 'My EP',
  fos_region: 'us-east-1',
  fos_bucket_name: 'my-bucket',
  fos_prefix: 'logs/',
  fos_access_key: 'AK',
  fos_secret_key: 'SK',
  sample_rate: 1,
  edge_only: true,
  custom_condition: '',
  log_period: 60,
  cdn_service_name: 'My CDN',
  cdn_prefix: 'mycdn',
  cdn_shield: 'iad-va-us',
  cdn_url: '',
  cdn_secret: '',
  enable_cron_sync: true,
  delete_after: false,
  commit_interval_mins: 5,
  enable_cron_compact: true,
}

export function serviceStoreMockFactory() {
  const state = { activeServiceId: 'svc-test' as string | null }
  const useServiceStore: unknown = Object.assign(
    (selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state),
    { getState: () => state },
  )
  return { useServiceStore }
}
