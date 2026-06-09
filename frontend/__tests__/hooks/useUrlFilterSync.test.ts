/**
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetMetric = vi.fn()
const mockClientGet = vi.fn()
const mockGetQueryData = vi.fn()

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn(() => ({
    addFilter: mockAddFilter,
    clearFilters: mockClearFilters,
    setRange: mockSetRange,
  })),
}))

// useUrlFilterSync calls useQueryClient() to read the bootstrap-seeded
// views cache as a fast path before falling back to client.GET. The hook
// no longer needs a real QueryClientProvider in tests — we just stub the
// hook to return a query client with the methods we exercise.
vi.mock('@tanstack/react-query', () => ({
  useQueryClient: vi.fn(() => ({ getQueryData: mockGetQueryData })),
}))

vi.mock('@/hooks/usePageContext', () => ({
  usePageContext: vi.fn(() => ({ activeServiceId: 'test-service-id' })),
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

describe('useUrlFilterSync', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
  })

  afterEach(() => {
    window.history.pushState({}, '', '/')
  })

  it('does nothing when there are no URL params', async () => {
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockSetRange).not.toHaveBeenCalled()
    expect(mockSetMetric).not.toHaveBeenCalled()
  })

  it('parses filter_col=val params and calls addFilter', async () => {
    setSearch('filter_status=200&filter_country=US')
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    expect(mockClearFilters).toHaveBeenCalledOnce()
    expect(mockAddFilter).toHaveBeenCalledWith('status', '200', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('country', 'US', 'include')
  })

  it('parses start_time / end_time params and calls setRange', async () => {
    setSearch('start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    expect(mockSetRange).toHaveBeenCalledWith('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')
  })

  it('parses metric param and calls setMetric', async () => {
    setSearch('metric=bandwidth')
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    expect(mockSetMetric).toHaveBeenCalledWith('bandwidth')
  })

  it('removes filter_, start_time, end_time, metric params from URL after processing', async () => {
    setSearch('filter_status=200&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&metric=requests&other=keep')
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    const remaining = new URLSearchParams(window.location.search)
    expect(remaining.get('filter_status')).toBeNull()
    expect(remaining.get('start_time')).toBeNull()
    expect(remaining.get('end_time')).toBeNull()
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
    const { useUrlFilterSync } = await import('@/hooks/useUrlFilterSync')
    renderHook(() => useUrlFilterSync())
    // Give the async loadView a tick to run
    await new Promise(r => setTimeout(r, 0))
    expect(mockClientGet).toHaveBeenCalled()
    expect(mockSetRange).toHaveBeenCalledWith('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')
    expect(mockAddFilter).toHaveBeenCalledWith('status', '404', 'include')
    expect(new URLSearchParams(window.location.search).get('view')).toBeNull()
  })
})
