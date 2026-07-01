/**
 * Tests for the PII-masking client signal and its enforcement guard.
 *
 *  - useMaskIps: reads bootstrap.settings.mask_ips from the query cache.
 *  - useEnforceMaskedFilters: strips IP-family filters from the store once
 *    masking is known (so a bookmarked ?filters={ip:...} URL doesn't 403).
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, it, expect, beforeEach } from 'vitest'
import React from 'react'

import { useMaskIps } from '@/hooks/useMaskIps'
import { useEnforceMaskedFilters } from '@/hooks/useEnforceMaskedFilters'
import { queryKeys } from '@/lib/query-keys'
import { useFilterStore } from '@/stores/filterStore'

function makeWrapper(maskIps: boolean | undefined) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  if (maskIps !== undefined) {
    qc.setQueryData(queryKeys.bootstrap(), { settings: { mask_ips: maskIps } })
  }
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  }
}

describe('useMaskIps', () => {
  it('is true when bootstrap.settings.mask_ips is true', () => {
    const { result } = renderHook(() => useMaskIps(), { wrapper: makeWrapper(true) })
    expect(result.current).toBe(true)
  })

  it('is false when mask_ips is false or absent', () => {
    const { result: off } = renderHook(() => useMaskIps(), { wrapper: makeWrapper(false) })
    expect(off.current).toBe(false)
    const { result: missing } = renderHook(() => useMaskIps(), { wrapper: makeWrapper(undefined) })
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
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper(true) })
    await waitFor(() => {
      const cols = useFilterStore.getState().filters.map(f => f.column)
      // ip removed, country kept.
      expect(cols).not.toContain('ip')
      expect(cols).toContain('country')
    })
  })

  it('leaves IP filters in place when NOT masking', async () => {
    useFilterStore.getState().addFilter('ip', '73.217.41.5', 'include')
    renderHook(() => useEnforceMaskedFilters(), { wrapper: makeWrapper(false) })
    // give the effect a tick; nothing should be stripped.
    await new Promise(r => setTimeout(r, 0))
    expect(useFilterStore.getState().filters.map(f => f.column)).toContain('ip')
  })
})
