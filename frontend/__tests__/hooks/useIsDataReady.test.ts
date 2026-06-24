/**
 * R-10: `useIsDataReady` reduces (effective service id) → boolean.
 * Drives the gate on every analytics page — false flashes "No service
 * selected"; true unblocks the data fetches.
 *
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, vi } from 'vitest'

let storedSid: string | null = null

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) => {
    const state = { activeServiceId: storedSid }
    return selector ? selector(state) : state
  })
  useServiceStore.getState = () => ({ activeServiceId: storedSid })
  return { useServiceStore }
})

function wrapperWithSeed(seedKey?: string, seedValue?: unknown) {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  if (seedKey) qc.setQueryData([seedKey], seedValue)
  return makeQueryWrapper(qc)
}

describe('useIsDataReady', () => {
  beforeEach(() => {
    storedSid = null
    vi.clearAllMocks()
  })

  it('returns false when no service id is set anywhere', async () => {
    const { useIsDataReady } = await import('@/hooks/useIsDataReady')
    const { result } = renderHook(() => useIsDataReady(), { wrapper: wrapperWithSeed() })
    expect(result.current).toBe(false)
  })

  it('returns true when the Zustand store has an active service id', async () => {
    storedSid = 'svc-from-store'
    const { useIsDataReady } = await import('@/hooks/useIsDataReady')
    const { result } = renderHook(() => useIsDataReady(), { wrapper: wrapperWithSeed() })
    expect(result.current).toBe(true)
  })

  it('falls back to bootstrap.active_service_id when the store is empty (SSR-hydrated first render)', async () => {
    storedSid = null
    const { useIsDataReady } = await import('@/hooks/useIsDataReady')
    const { result } = renderHook(() => useIsDataReady(), {
      wrapper: wrapperWithSeed('bootstrap', { active_service_id: 'svc-from-bootstrap' }),
    })
    expect(result.current).toBe(true)
  })
})
