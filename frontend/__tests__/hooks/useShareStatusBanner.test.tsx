/**
 * Tests for the AppLayout share-banner poller (`useShareStatusBanner`) and
 * analyst heartbeat (`useAnalystHeartbeat`).
 *
 * @vitest-environment jsdom
 */
import { renderHook, act, waitFor } from '@testing-library/react'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { useAnalystHeartbeat } from '@/hooks/useBootstrap'
import { __resetForTests } from '@/lib/utils'

const push = vi.fn()
const replace = vi.fn()
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push, replace }),
}))

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const state = {
    activeServiceId: 'svc-default',
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    setInitialized: vi.fn(),
  }
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state)),
    { getState: () => state },
  )
  return { ...actual, useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapperWithSeed(seedBootstrap?: unknown) {
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
    const { useShareStatusBanner } = await import('@/components/AppLayout')
    const wrapper = wrapperWithSeed({
      share_banner: { sharing_active: true, public_url: 'https://example.test/x' },
    })
    const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper })

    expect(result.current.sharingActive).toBe(true)
    expect(result.current.node).not.toBeNull()
    await Promise.resolve()
    expect(bannerHits).toBe(0)
  })

  it('returns null node when disabled', async () => {
    const { useShareStatusBanner } = await import('@/components/AppLayout')
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
    const { useShareStatusBanner } = await import('@/components/AppLayout')
    const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), {
      wrapper: wrapperWithSeed(),
    })

    await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
    await waitFor(() => expect(result.current.sharingActive).toBe(true))
  })

  it('does not fetch while the tab is hidden', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    Object.defineProperty(document, 'hidden', { configurable: true, value: true })
    try {
      const { useShareStatusBanner } = await import('@/components/AppLayout')
      renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper: wrapperWithSeed() })
      await act(async () => {
        await new Promise(r => setTimeout(r, 50))
      })
      expect(bannerHits).toBe(0)
    } finally {
      Object.defineProperty(document, 'hidden', { configurable: true, value: false })
    }
  })

  it('stops polling after unmount', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    const { useShareStatusBanner } = await import('@/components/AppLayout')
    const { unmount } = renderHook(() => useShareStatusBanner({ enabled: true }), {
      wrapper: wrapperWithSeed(),
    })
    await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
    const after = bannerHits

    unmount()
    await act(async () => {
      await new Promise(r => setTimeout(r, 50))
    })
    expect(bannerHits).toBe(after)
  })

  it('keeps polling on the 15s cadence after a seeded mount', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      let bannerHits = 0
      server.use(
        http.get(`${API_BASE}/api/admin/share/banner`, () => {
          bannerHits++
          return HttpResponse.json({ sharing_active: false, public_url: null })
        }),
      )
      const { useShareStatusBanner } = await import('@/components/AppLayout')
      const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), {
        wrapper: wrapperWithSeed({ share_banner: { sharing_active: true, public_url: null } }),
      })
      expect(result.current.sharingActive).toBe(true)
      expect(bannerHits).toBe(0)

      await act(async () => {
        await vi.advanceTimersByTimeAsync(15_100)
      })
      await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
      await waitFor(() => expect(result.current.sharingActive).toBe(false))
    } finally {
      vi.useRealTimers()
    }
  })

  it('aborts the in-flight banner request on unmount', async () => {
    let aborted = false
    let started = false
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, async ({ request }) => {
        started = true
        request.signal.addEventListener('abort', () => {
          aborted = true
        })
        await new Promise((r) => setTimeout(r, 300))
        return HttpResponse.json({ sharing_active: false, public_url: null })
      }),
    )
    const { useShareStatusBanner } = await import('@/components/AppLayout')
    const { unmount } = renderHook(() => useShareStatusBanner({ enabled: true }), {
      wrapper: wrapperWithSeed(),
    })
    await waitFor(() => expect(started).toBe(true))
    unmount()
    await waitFor(() => expect(aborted).toBe(true))
  })

  it('starts fetching once a hidden tab becomes visible', async () => {
    let bannerHits = 0
    server.use(
      http.get(`${API_BASE}/api/admin/share/banner`, () => {
        bannerHits++
        return HttpResponse.json({ sharing_active: true, public_url: 'https://v.test' })
      }),
    )
    Object.defineProperty(document, 'hidden', { configurable: true, value: true })
    try {
      const { useShareStatusBanner } = await import('@/components/AppLayout')
      const { result } = renderHook(() => useShareStatusBanner({ enabled: true }), { wrapper: wrapperWithSeed() })
      await act(async () => {
        await new Promise((r) => setTimeout(r, 50))
      })
      expect(bannerHits).toBe(0)

      Object.defineProperty(document, 'hidden', { configurable: true, value: false })
      await act(async () => {
        document.dispatchEvent(new Event('visibilitychange'))
      })
      await waitFor(() => expect(bannerHits).toBeGreaterThanOrEqual(1))
      await waitFor(() => expect(result.current.sharingActive).toBe(true))
    } finally {
      Object.defineProperty(document, 'hidden', { configurable: true, value: false })
    }
  })
})

const IDLE = 1_000
const INTERVAL = 5_000

function setLastActivityPast(ms: number) {
  vi.setSystemTime(Date.now() + ms)
}

describe('useAnalystHeartbeat', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    __resetForTests(Date.now())
    replace.mockReset()
    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('does not poll while the user is active inside the idle window', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )
    for (let i = 0; i < 3; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(INTERVAL - 1)
        window.dispatchEvent(new Event('mousemove'))
        await vi.advanceTimersByTimeAsync(1)
      })
    }
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('treats touch input as activity (touch-only analysts are not idle)', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )
    for (let i = 0; i < 3; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(INTERVAL - 1)
        window.dispatchEvent(new Event('touchstart'))
        await vi.advanceTimersByTimeAsync(1)
      })
    }
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('shows the disconnect overlay after two consecutive failures', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('network down'))

    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 2,
      }),
    )

    expect(result.current.disconnected).toBe(false)

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(false)

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(true)
  })

  it('clears the overlay after a successful tick following failures', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockRejectedValueOnce(new Error('blip'))
    fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))

    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 2,
      }),
    )

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(true)

    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(result.current.disconnected).toBe(false)
  })

  it('redirects to /share-login on a 401 response', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )
    setLastActivityPast(IDLE + 1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    expect(replace).toHaveBeenCalledWith('/share-login')
  })

  it('does not call fetch when enabled=false (even after multiple intervals)', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    renderHook(() =>
      useAnalystHeartbeat({ enabled: false, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )

    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 5)
      await vi.advanceTimersByTimeAsync(INTERVAL * 5)
    })

    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('stops polling after unmount', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
    )
    const { unmount } = renderHook(() =>
      useAnalystHeartbeat({ enabled: true, idleAfterMs: IDLE, intervalMs: INTERVAL }),
    )

    await act(async () => {
      vi.setSystemTime(Date.now() + IDLE + 1)
      await vi.advanceTimersByTimeAsync(INTERVAL)
    })
    const callsBeforeUnmount = fetchSpy.mock.calls.length

    unmount()

    await act(async () => {
      vi.setSystemTime(Date.now() + INTERVAL * 5)
      await vi.advanceTimersByTimeAsync(INTERVAL * 5)
    })

    expect(fetchSpy.mock.calls.length).toBe(callsBeforeUnmount)
  })

  it('swallows fetch errors silently (no unhandled rejection)', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('boom'))
    const unhandled = vi.fn()
    process.on('unhandledRejection', unhandled)

    const { result } = renderHook(() =>
      useAnalystHeartbeat({
        enabled: true,
        idleAfterMs: IDLE,
        intervalMs: INTERVAL,
        failuresBeforeOverlay: 999,
      }),
    )

    for (let i = 0; i < 3; i++) {
      await act(async () => {
        vi.setSystemTime(Date.now() + IDLE + 1)
        await vi.advanceTimersByTimeAsync(INTERVAL)
      })
    }

    expect(result.current.disconnected).toBe(false)
    expect(unhandled).not.toHaveBeenCalled()
    process.off('unhandledRejection', unhandled)
  })
})
