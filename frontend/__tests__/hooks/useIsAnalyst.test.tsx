/**
 * @vitest-environment jsdom
 *
 * useIsAnalyst — gates admin-only UI surfaces and controls the 401
 * redirect target (analyst → /share-login, admin → surface error).
 *
 * Returns true when EITHER condition holds:
 *   1. activeService.accessLevel === 'read_only'
 *   2. bootstrap.settings.is_remote_analyst === true
 */
import { renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, it, expect, beforeEach } from 'vitest'
import React from 'react'

import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'

function makeWrapper(opts: { isRemoteAnalyst?: boolean } = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const settings: Record<string, unknown> = {}
  if (opts.isRemoteAnalyst !== undefined) {
    settings.is_remote_analyst = opts.isRemoteAnalyst
  }
  qc.setQueryData(queryKeys.bootstrap(), { settings })
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  }
}

beforeEach(() => {
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
})

describe('useIsAnalyst', () => {
  it('returns false for admin (no analyst signals)', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(false)
  })

  it('returns true when bootstrap.settings.is_remote_analyst is true', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: true }),
    })
    expect(result.current).toBe(true)
  })

  it('returns true when activeService.accessLevel is read_only', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(true)
  })

  it('returns true when both signals are present', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: true }),
    })
    expect(result.current).toBe(true)
  })

  it('returns false when activeServiceId has no matching service', () => {
    useServiceStore.setState({
      activeServiceId: 'svc-missing',
      services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
    })
    const { result } = renderHook(() => useIsAnalyst(), {
      wrapper: makeWrapper({ isRemoteAnalyst: false }),
    })
    expect(result.current).toBe(false)
  })
})
