/**
 * useActiveLogFields answers "is this log field / field-group enabled for the
 * active service?" from bootstrap.active_log_field_ids (+ the catalog's
 * group→fields map). Pages gate their "Requires Group X to be enabled"
 * empty-state copy on it, so the not-ready guard (report everything active
 * until bootstrap lands, to avoid flashing a misleading message) and the
 * group "any field present" semantics both matter.
 *
 * Both upstream hooks are mocked at the module boundary so this stays a pure
 * unit test (no MSW / query client).
 *
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'

const useBootstrap = vi.fn()
const useLogFieldsCatalog = vi.fn()

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => useBootstrap(),
}))
vi.mock('@/hooks/useLogFieldsCatalog', () => ({
  useLogFieldsCatalog: () => useLogFieldsCatalog(),
}))

function loadHook() {
  return import('@/hooks/useActiveLogFields')
}

// Catalog with the two groups the tests exercise (mirrors the real
// group→field-ids mapping: L = Origin Metrics, D = Geolocation Basic).
const CATALOG = {
  groups: [
    { id: 'L', label: 'Origin Metrics', fields: ['ottfb', 'ottlb', 'oip', 'prid'] },
    { id: 'D', label: 'Geolocation', fields: ['country', 'city', 'region'] },
  ],
}

describe('useActiveLogFields', () => {
  beforeEach(() => {
    useBootstrap.mockReset()
    useLogFieldsCatalog.mockReset()
    useLogFieldsCatalog.mockReturnValue({ data: CATALOG })
  })

  it('reports not-ready and treats everything as active until bootstrap lands', async () => {
    useBootstrap.mockReturnValue({ data: undefined })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.ready).toBe(false)
    // Not-ready guard: never flash "requires X" during load.
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('anything')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(true)
  })

  it('treats an empty active_log_field_ids list as not-ready (everything active)', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: [] } })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.ready).toBe(false)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
  })

  it('honours active_log_field_ids when populated', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb', 'country'] } })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.ready).toBe(true)
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    expect(result.current.isFieldActive('ja3')).toBe(false)
  })

  it('isGroupActive is true when ANY field of the group is active', async () => {
    // Only 'country' (Group D) active; no Group L fields.
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['country'] } })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.isGroupActive('D')).toBe(true)
    expect(result.current.isGroupActive('L')).toBe(false)
  })

  it('isGroupActive is false for an unknown group id', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb'] } })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.isGroupActive('ZZ')).toBe(false)
  })

  it('does not throw when the catalog is missing (still resolves fields)', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['ottfb'] } })
    useLogFieldsCatalog.mockReturnValue({ data: undefined })
    const { useActiveLogFields } = await loadHook()
    const { result } = renderHook(() => useActiveLogFields())
    expect(result.current.isFieldActive('ottfb')).toBe(true)
    // No catalog → group lookup yields no match → false (ready path).
    expect(result.current.isGroupActive('L')).toBe(false)
  })
})
