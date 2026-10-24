/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { buildFiltersPayload } from '@/types/filters'
import type { FilterPill } from '@/types/filters'

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

describe('buildFiltersPayload', () => {
  it('returns empty object for no filters', () => {
    expect(buildFiltersPayload([])).toEqual({})
  })

  it('single include filter', () => {
    const pills: FilterPill[] = [{ id: '1', column: 'status', value: '200', mode: 'include' }]
    expect(buildFiltersPayload(pills)).toEqual({
      status: { mode: 'include', values: ['200'] },
    })
  })

  it('single exclude filter', () => {
    const pills: FilterPill[] = [{ id: '1', column: 'country', value: 'US', mode: 'exclude' }]
    expect(buildFiltersPayload(pills)).toEqual({
      country: { mode: 'exclude', values: ['US'] },
    })
  })

  it('two include filters on the same column merge into one entry', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '201', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(result.status.mode).toBe('include')
    expect(result.status.values).toContain('200')
    expect(result.status.values).toContain('201')
  })

  it('include and exclude on the same column get separate keys', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'status', value: '500', mode: 'exclude' },
    ]
    const result = buildFiltersPayload(pills)
    const keys = Object.keys(result)
    expect(keys.some(k => k === 'status' || k.startsWith('status_'))).toBe(true)
    expect(keys.length).toBe(2)
    const modes = keys.map(k => result[k].mode)
    expect(modes).toContain('include')
    expect(modes).toContain('exclude')
  })

  it('multiple different columns each get their own key', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'status', value: '200', mode: 'include' },
      { id: '2', column: 'country', value: 'US', mode: 'include' },
      { id: '3', column: 'pop', value: 'JFK', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(result.status).toBeDefined()
    expect(result.country).toBeDefined()
    expect(result.pop).toBeDefined()
  })

  it('produces the shape the backend FiltersDict expects', () => {
    const pills: FilterPill[] = [
      { id: '1', column: 'ip', value: '1.2.3.4', mode: 'include' },
    ]
    const result = buildFiltersPayload(pills)
    expect(typeof result).toBe('object')
    expect(typeof result.ip).toBe('object')
    expect(typeof result.ip.mode).toBe('string')
    expect(Array.isArray(result.ip.values)).toBe(true)
  })
})

function setSearch(search: string) {
  window.history.pushState({}, '', search ? `?${search}` : '/')
}

async function hydrate() {
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

  it('parses minute relative ranges such as ?range=5m', async () => {
    setSearch('range=5m')
    await hydrate()
    expect(mockSetRelativeRange).toHaveBeenCalledOnce()
    expect(mockSetRelativeRange.mock.calls[0][0]).toBe('5m')
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

function filterKeysFrom(url: string): string[] {
  const qs = url.includes('?') ? url.split('?')[1] : url
  return Array.from(new URLSearchParams(qs).keys()).filter(k => k.startsWith('filter_'))
}

function assertFilterFormat(url: string) {
  const search = url.includes('?') ? url.split('?')[1] : url
  const params = new URLSearchParams(search)
  for (const key of params.keys()) {
    if (key.startsWith('filter')) {
      expect(key).toMatch(/^filter_\w+/)
    }
  }
}

describe('Security page dashboard links', () => {
  it('bot id filter uses filter_ prefix', () => {
    const url = `/dashboard?filter__wellknown_bot_id=${encodeURIComponent('bot-123')}`
    const keys = filterKeysFrom(url)
    expect(keys).toContain('filter__wellknown_bot_id')
  })

  it('ngwaf bot filter uses filter_ prefix', () => {
    const url = `/dashboard?filter__ngwaf_bot_name=${encodeURIComponent('scraperbot')}`
    const keys = filterKeysFrom(url)
    expect(keys).toContain('filter__ngwaf_bot_name')
  })

  it('tls fingerprint filter uses filter_ prefix', () => {
    const url = `/dashboard?filter_tls_client_hello=${encodeURIComponent('abc123')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_tls_client_hello')
  })

  it('client IP filter uses filter_ prefix', () => {
    const url = `/dashboard?filter_client_ip=${encodeURIComponent('1.2.3.4')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_client_ip')
  })
})

describe('Network page dashboard links', () => {
  it('POP filter uses filter_pop', () => {
    const pop = 'JFK'
    const url = `/dashboard?filter_pop=${encodeURIComponent(pop)}`
    assertFilterFormat(url)
    expect(new URLSearchParams(url.split('?')[1]).get('filter_pop')).toBe(pop)
  })

  it('shield POP filter uses filter_shield_pop', () => {
    const url = `/dashboard?filter_shield_pop=${encodeURIComponent('IAD')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_shield_pop')
  })

  it('ASN filter uses filter_asn', () => {
    const url = `/dashboard?filter_asn=${encodeURIComponent('7922')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_asn')
  })

  it('city/region/country multi-filter all use filter_ prefix', () => {
    const city = 'London'
    const region = 'ENG'
    const country = 'GB'
    const url = `/dashboard?filter_city=${encodeURIComponent(city)}&filter_region=${encodeURIComponent(region)}&filter_country=${encodeURIComponent(country)}`
    assertFilterFormat(url)
    const params = new URLSearchParams(url.split('?')[1])
    expect(params.get('filter_city')).toBe(city)
    expect(params.get('filter_region')).toBe(region)
    expect(params.get('filter_country')).toBe(country)
    expect(params.has('city')).toBe(false)
    expect(params.has('region')).toBe(false)
  })
})

describe('Origin page dashboard links', () => {
  it('URL filter uses filter_url', () => {
    const url = `/dashboard?filter_url=${encodeURIComponent('/api/data')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_url')
  })

  it('POP filter uses filter_pop', () => {
    const url = `/dashboard?filter_pop=${encodeURIComponent('SYD')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_pop')
  })

  it('origin IP filter uses filter_oip (the actual schema column)', () => {
    const url = `/dashboard?filter_oip=${encodeURIComponent('10.0.0.1')}`
    assertFilterFormat(url)
    expect(filterKeysFrom(url)).toContain('filter_oip')
  })
})

describe('filter_ format is parseable by useViewMetricUrlSync', () => {
  it('all filter keys start with filter_ and are followed by at least one word char', () => {
    const sampleUrls = [
      `/dashboard?filter_status=200`,
      `/dashboard?filter_country=US&filter_pop=JFK`,
      `/dashboard?filter__wellknown_bot_id=bot-1`,
      `/dashboard?filter_city=London&filter_region=ENG&filter_country=GB`,
    ]
    for (const url of sampleUrls) {
      const params = new URLSearchParams(url.split('?')[1])
      for (const key of params.keys()) {
        expect(key.startsWith('filter_')).toBe(true)
        const col = key.substring(7)
        expect(col.length).toBeGreaterThan(0)
      }
    }
  })
})
