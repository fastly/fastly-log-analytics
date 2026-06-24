/**
 * R-10: `useActiveService` selects a slim slice of the service store
 * (active id + services array) via `useShallow`. Mostly a thin
 * passthrough — pinned so a future refactor of the slice shape (or
 * removal of `useShallow`) doesn't silently change identity semantics
 * and re-render every page on unrelated state changes.
 *
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string }>
  unrelatedKey: number
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test' }],
  unrelatedKey: 0,
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

describe('useActiveService', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockState = {
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test' }],
      unrelatedKey: 0,
    }
  })

  it('returns the active service id and services array', async () => {
    const { useActiveService } = await import('@/hooks/useActiveService')
    const { result } = renderHook(() => useActiveService())
    expect(result.current.activeServiceId).toBe('svc-1')
    expect(result.current.services).toEqual([{ id: 'svc-1', name: 'Test' }])
  })

  it('returns null activeServiceId when nothing is selected', async () => {
    mockState = { activeServiceId: null, services: [], unrelatedKey: 0 }
    const { useActiveService } = await import('@/hooks/useActiveService')
    const { result } = renderHook(() => useActiveService())
    expect(result.current.activeServiceId).toBeNull()
    expect(result.current.services).toEqual([])
  })

  it('exposes only the active id + services slice — never the full store', async () => {
    const { useActiveService } = await import('@/hooks/useActiveService')
    const { result } = renderHook(() => useActiveService())
    expect(Object.keys(result.current).sort()).toEqual(['activeServiceId', 'services'])
  })
})
