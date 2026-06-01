import { describe, it, expect } from 'vitest'
import { buildFiltersPayload } from '@/types/filters'
import type { FilterPill } from '@/types/filters'

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
    // One key gets the base name, the other gets a suffix
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
    // Backend expects: { [col]: { mode: 'include'|'exclude', values: string[] } }
    expect(typeof result).toBe('object')
    expect(typeof result.ip).toBe('object')
    expect(typeof result.ip.mode).toBe('string')
    expect(Array.isArray(result.ip.values)).toBe(true)
  })
})
