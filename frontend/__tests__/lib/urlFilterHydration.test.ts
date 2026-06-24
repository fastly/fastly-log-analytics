/**
 * @vitest-environment jsdom
 *
 * Covers the URL → filterStore hydration that moved out of
 * useUrlFilterSync (commit c25a830) into a sync, pre-render path that
 * QueryProvider calls from a useState initializer.
 *
 * The legacy ?filter_<col>=, absolute ?start_time/?end_time, modern
 * ?filters JSON, and ?range= cases all live here now. After applying,
 * the consumed params get stripped from window.history so the post-
 * mount useUrlFilterSync effect doesn't see them.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'

const mockAddFilter = vi.fn()
const mockClearFilters = vi.fn()
const mockSetRange = vi.fn()
const mockSetRelativeRange = vi.fn()

vi.mock('@/stores/filterStore', () => ({
  useFilterStore: {
    getState: vi.fn(() => ({
      addFilter: mockAddFilter,
      clearFilters: mockClearFilters,
      setRange: mockSetRange,
      setRelativeRange: mockSetRelativeRange,
    })),
  },
}))

function setSearch(search: string) {
  window.history.pushState({}, '', search ? `?${search}` : '/')
}

async function hydrate() {
  // Re-import each test to honour the module-level `hydrated` guard reset.
  const mod = await import('@/lib/urlFilterHydration')
  mod._resetUrlHydrationFlag()
  mod.hydrateFilterStoreFromUrl()
}

describe('hydrateFilterStoreFromUrl', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.history.pushState({}, '', '/')
  })

  it('does nothing when there are no URL params', async () => {
    await hydrate()
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockSetRange).not.toHaveBeenCalled()
    expect(mockSetRelativeRange).not.toHaveBeenCalled()
    expect(mockClearFilters).not.toHaveBeenCalled()
  })

  it('parses legacy filter_<col>=val params and calls addFilter', async () => {
    setSearch('filter_status=200&filter_country=US')
    await hydrate()
    expect(mockClearFilters).toHaveBeenCalledOnce()
    expect(mockAddFilter).toHaveBeenCalledWith('status', '200', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('country', 'US', 'include')
  })

  it('parses absolute start_time / end_time params and calls setRange', async () => {
    setSearch('start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    await hydrate()
    expect(mockSetRange).toHaveBeenCalledWith(
      '2026-01-01T00:00:00Z',
      '2026-01-02T00:00:00Z',
    )
  })

  it('parses ?range= and calls setRelativeRange with computed start/end', async () => {
    setSearch('range=24h')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    const args = mockSetRelativeRange.mock.calls[0]
    expect(args[0]).toBe('24h')
    expect(typeof args[1]).toBe('string')
    expect(typeof args[2]).toBe('string')
  })

  it('parses modern ?filters JSON payload', async () => {
    const filters = {
      status: { values: ['200', '404'], mode: 'include' },
      country: { values: ['CA'], mode: 'exclude' },
    }
    setSearch(`filters=${encodeURIComponent(JSON.stringify(filters))}`)
    await hydrate()
    expect(mockClearFilters).toHaveBeenCalledOnce()
    expect(mockAddFilter).toHaveBeenCalledWith('status', '200', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('status', '404', 'include')
    expect(mockAddFilter).toHaveBeenCalledWith('country', 'CA', 'exclude')
  })

  it('?range= wins over absolute ?start_time/?end_time', async () => {
    setSearch('range=24h&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    expect(mockSetRange).not.toHaveBeenCalled()
  })

  it('strips consumed params from URL while leaving unrelated params untouched', async () => {
    setSearch('filter_status=200&start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&other=keep')
    await hydrate()
    const remaining = new URLSearchParams(window.location.search)
    expect(remaining.get('filter_status')).toBeNull()
    expect(remaining.get('start_time')).toBeNull()
    expect(remaining.get('end_time')).toBeNull()
    expect(remaining.get('other')).toBe('keep')
  })

  it('silently ignores malformed ?filters JSON', async () => {
    setSearch('filters=not-json')
    await hydrate()
    expect(mockAddFilter).not.toHaveBeenCalled()
    expect(mockClearFilters).not.toHaveBeenCalled()
  })
})
