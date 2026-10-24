/**
 * @vitest-environment jsdom
 *
 * Tests for useIsAnalyst, useMaskIps, and useEnforceMaskedFilters.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach } from 'vitest'

import { useIsAnalyst, useMaskIps, useEnforceMaskedFilters } from '@/hooks/useIsAnalyst'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'

function makeWrapper(opts: { isRemoteAnalyst?: boolean; maskIps?: boolean } = {}) {
  const qc = createTestQueryClient()
  const settings: Record<string, unknown> = {}
  if (opts.isRemoteAnalyst !== undefined) {
    settings.is_remote_analyst = opts.isRemoteAnalyst
  }
  if (opts.maskIps !== undefined) {
    settings.mask_ips = opts.maskIps
  }
  if (Object.keys(settings).length > 0 || opts.isRemoteAnalyst !== undefined) {
    qc.setQueryData(queryKeys.bootstrap(), { settings })
  }
  return makeQueryWrapper(qc)
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

describe('useMaskIps', () => {
  it('is true when bootstrap.settings.mask_ips is true', () => {
    const { result } = renderHook(() => useMaskIps(), { wrapper: makeWrapper({ maskIps: true }) })
    expect(result.current).toBe(true)
  })

  it('is false when mask_ips is false or absent', () => {
    const { result: off } = renderHook(() => useMaskIps(), { wrapper: makeWrapper({ maskIps: false }) })
    expect(off.current).toBe(false)
    const { result: missing } = renderHook(() => useMaskIps(), { wrapper: makeWrapper() })
    expect(missing.current).toBe(false)
  })
})

describe('useEnforceMaskedFilters', () => {
  beforeEach(() => {
    useFilterStore.getState().clearFilters()
  })

  it('strips IP-family filters (incl. dedup-suffixed) when masking', async () => {
    const store = useFilterStore.getState()
    store.addFilter('ip', '73.217.41.5', 'include')
    store.addFilter('country', 'US', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper({ maskIps: true }) })
    await waitFor(() => {
      const cols = useFilterStore.getState().filters.map(f => f.column)
      expect(cols).not.toContain('ip')
      expect(cols).toContain('country')
    })
  })

  it('leaves IP filters in place when NOT masking', async () => {
    useFilterStore.getState().addFilter('ip', '73.217.41.5', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper({ maskIps: false }) })
    await new Promise(r => setTimeout(r, 0))
    expect(useFilterStore.getState().filters.map(f => f.column)).toContain('ip')
  })
})
