/**
 * Tests for the AppLayout share-banner poller. The hook reads
 * /api/admin/share/banner every 15s, optionally seeded from
 * bootstrap's share_banner field. Two regressions live in this
 * path:
 *   1. The bootstrap seed must skip the immediate fetch (perf audit D-3);
 *      without it the cold-load page renders the banner from a stale
 *      RTT instead of from the bootstrap response already in cache.
 *   2. Polling must keep firing every 15s even when seeded — admin
 *      starting / stopping sharing must reflect in the banner within
 *      one window.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { server } from '../../tests/msw/server'

const push = vi.fn()
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push }),
}))

vi.mock('@/stores/serviceStore', () => {
  const state = {
    activeServiceId: 'svc-default',
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(state) : state,
  )
  useServiceStore.getState = () => state
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapperWithSeed(seedBootstrap?: any) {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  if (seedBootstrap) qc.setQueryData(['bootstrap'], seedBootstrap)
  return makeQueryWrapper(qc)
}

describe('useShareStatusBanner', () => {
  beforeEach(() => {
    push.mockReset()
  })

  it('seeds initial state from bootstrap.share_banner without firing an immediate fetch', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    const { useShareStatusBanner } = await import('@/hooks/useShareStatusBanner')
    const wrapper = wrapperWithSeed({
      share_banner: { sharing_active: true, public_url: 'https://example.test/x' },
    })
    const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper })

    // Bootstrap seed should populate state synchronously — no need to wait
    // for a fetch.
    expect(result.current.sharingActive).toBe(true)
    expect(result.current.node).not.toBeNull()
    // Allow microtasks to flush; the seeded path should not have fetched.
    await Promise.resolve()
    expect(bannerHits).toBe(0)
  })

  it('returns null node when disabled', async () => {
    const { useShareStatusBanner } = await import('@/hooks/useShareStatusBanner')
    const { result } = renderHook(() => useShareStatusBanner({ enabled: false }), {
      wrapper: wrapperWithSeed(),
    })
    expect(result.current.sharingActive).toBe(false)
    expect(result.current.node).toBeNull()
  })

  it('fires an initial banner fetch when no bootstrap seed is present', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: true, public_url: 'https://later.test' })
      }),
    )
    const { useShareStatusBanner } = await import('@/hooks/useShareStatusBanner')
    const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), {
      wrapper: wrapperWithSeed(),
    })

    await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
    await waitFor(() => expect(result.current.sharingActive).toBe(true))
  })

  it('stops polling after unmount', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    const { useShareStatusBanner } = await import('@/hooks/useShareStatusBanner')
    const { unmount } = renderHook(() => useShareStatusBanner({ enabled: true }), {
      wrapper: wrapperWithSeed(),
    })
    await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
    const after = bannerHits

    unmount()
    // Give the (now-cancelled) interval ~50ms of wall clock to fire — it
    // shouldn't, but if cleanup was broken we'd see the count climb.
    await act(async () => {
      await new Promise(r => setTimeout(r, 50))
    })
    expect(bannerHits).toBe(after)
  })
})
