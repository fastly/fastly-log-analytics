/**
 * @vitest-environment jsdom
 *
 * URL → filterStore hydration for the legacy ?filter_<col>= short form,
 * absolute ?start_time/?end_time, and ?range= is handled synchronously
 * in lib/urlFilterHydration.ts (covered by urlFilterHydration.test.ts).
 *
 * useViewMetricUrlSync now only handles the two cases that can't run
 * pre-render: async ?view=<id> loading and ?metric=<val>. The legacy
 * tests for filter_/start_time/end_time hydration moved with the code.
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetMetric = vi.fn()
const mockClientGet = vi.fn()
const mockGetQueryData = vi.fn()

// The hook uses selector form — `useFilterStore(s => s.addFilter)` etc.
// Apply the selector to the mock store so each call returns the right
// slice instead of the whole object (which would TypeError when called
// like a function). Includes the toggle-state fields the restore-handler
// compares against, defaulting to the same store-init values.
const mockStore = {
  addFilter: mockAddFilter,
  clearFilters: mockClearFilters,
  setRange: mockSetRange,
  setRelativeRange: vi.fn(),
  toggleEdgeOnly: vi.fn(),
  toggleCompareMode: vi.fn(),
  setCompareRange: vi.fn(),
  edgeOnly: false,
  compareMode: false,
}
vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector?: (s: typeof mockStore) => unknown) =>
    typeof selector === 'function' ? selector(mockStore) : mockStore
  ),
}))

// useViewMetricUrlSync calls useQueryClient() to read the bootstrap-seeded
// views cache as a fast path before falling back to client.GET. The hook
// no longer needs a real QueryClientProvider in tests — we just stub the
// hook to return a query client with the methods we exercise.
vi.mock('@tanstack/react-query', () => ({
  useQueryClient: vi.fn(() => ({ getQueryData: mockGetQueryData })),
}))

vi.mock('@/hooks/useActiveService', () => ({
  useActiveService: vi.fn(() => ({ activeServiceId: 'test-service-id', services: [] })),
}))

vi.mock('@/hooks/useReportConfig', () => ({
  useReportConfig: vi.fn(() => ({ setMetric: mockSetMetric })),
}))

vi.mock('@/lib/api', () => ({
  client: { GET: mockClientGet },
}))

function setSearch(search: string) {
  window.history.pushState({}, '', search ? `?${search}` : '/')
}

describe('useViewMetricUrlSync', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
  })

  afterEach(() => {
    window.history.pushState({}, '', '/')
  })

  it('does nothing when there are no URL params', async () => {
    const { useViewMetricUrlSync } = await import('@/hooks/useViewMetricUrlSync')
    renderHook(() => useViewMetricUrlSync())
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockSetRange).not.toHaveBeenCalled()
    expect(mockSetMetric).not.toHaveBeenCalled()
  })

  it('parses metric param and calls setMetric', async () => {
    setSearch('metric=bandwidth')
    const { useViewMetricUrlSync } = await import('@/hooks/useViewMetricUrlSync')
    renderHook(() => useViewMetricUrlSync())
    expect(mockSetMetric).toHaveBeenCalledWith('bandwidth')
  })

  it('strips metric param from URL after processing while leaving unrelated params untouched', async () => {
    setSearch('metric=requests&other=keep')
    const { useViewMetricUrlSync } = await import('@/hooks/useViewMetricUrlSync')
    renderHook(() => useViewMetricUrlSync())
    const remaining = new URLSearchParams(window.location.search)
    expect(remaining.get('metric')).toBeNull()
    expect(remaining.get('other')).toBe('keep')
  })

  it('loads a saved view by id and clears URL view param', async () => {
    mockClientGet.mockResolvedValueOnce({
      data: [
        {
          id: 'view-123',
          start_time: '2026-01-01T00:00:00Z',
          end_time: '2026-01-02T00:00:00Z',
          filters_json: JSON.stringify([{ column: 'status', value: '404', mode: 'include' }]),
        },
      ],
    })
    setSearch('view=view-123')
    const { useViewMetricUrlSync } = await import('@/hooks/useViewMetricUrlSync')
    renderHook(() => useViewMetricUrlSync())
    // Give the async loadView a tick to run
    await new Promise(r => setTimeout(r, 0))
    expect(mockClientGet).toHaveBeenCalled()
    expect(mockSetRange).toHaveBeenCalledWith('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')
    expect(mockAddFilter).toHaveBeenCalledWith('status', '404', 'include')
    expect(new URLSearchParams(window.location.search).get('view')).toBeNull()
  })
})
