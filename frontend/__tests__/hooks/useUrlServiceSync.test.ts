/**
 * @vitest-environment jsdom
 *
 * useUrlServiceSync — bidirectional sync between the ?service= URL param
 * and the active service in the store.
 *
 * Rewritten for the nuqs migration (Phase 9a proof-of-concept). The hook
 * now reads/writes the URL via `useQueryState('service')` from nuqs
 * instead of the previous useSearchParams + router.replace dance.
 * Tests mock the nuqs binding directly so we exercise the hook's sync
 * semantics without spinning up an actual NuqsAdapter context.
 *
 * Behavior under test:
 *   - URL → store: if ?service=X differs from the store, write it in.
 *   - Store → URL: when activeServiceId changes (after init), push it
 *     to the URL via setUrlService.
 *   - If services list is empty, the URL must not carry a stale ?service.
 *   - Skip the store→URL push until isInitialized is true.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

// useQueryState mock — captures the current value + setter so each
// test can introspect what the hook wrote to the URL.
let mockUrlService: string | null = null
const mockSetUrlService = vi.fn((v: string | null) => {
  mockUrlService = v
})
vi.mock('nuqs', () => ({
  useQueryState: () => [mockUrlService, mockSetUrlService],
}))

const mockSetActiveServiceId = vi.fn()
let mockState = {
  activeServiceId: null as string | null,
  services: [] as Array<{ id: string; name: string }>,
  isInitialized: false,
  setActiveServiceId: mockSetActiveServiceId,
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: typeof mockState) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

beforeEach(() => {
  mockUrlService = null
  mockSetUrlService.mockReset()
  mockSetActiveServiceId.mockReset()
  mockState = {
    activeServiceId: null,
    services: [],
    isInitialized: false,
    setActiveServiceId: mockSetActiveServiceId,
  }
})

describe('useUrlServiceSync — URL → store', () => {
  it('writes ?service= URL param into the store on mount', async () => {
    mockUrlService = 'svc-from-url'
    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-from-url')
  })

  it('does nothing when there is no ?service= param', async () => {
    mockUrlService = null
    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
  })

  it('does not re-write when URL param matches store already', async () => {
    mockState.activeServiceId = 'svc-1'
    mockUrlService = 'svc-1'
    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
  })
})

describe('useUrlServiceSync — store → URL', () => {
  it('pushes activeServiceId into URL when it differs from current ?service', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [{ id: 'svc-new', name: 'New' }],
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-new')
  })

  it('strips ?service= from URL when services list is empty', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-orphan',
      services: [],
      isInitialized: true,
    }
    mockUrlService = 'svc-orphan'

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // No services → URL shouldn't carry a service param. Writes null,
    // which nuqs translates to removing the query string entirely.
    expect(mockSetUrlService).toHaveBeenCalledWith(null)
  })

  it('skips the store→URL sync until isInitialized is true', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'S' }],
      isInitialized: false,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('does not re-write the URL when it already matches the store', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'One' }],
      isInitialized: true,
    }
    mockUrlService = 'svc-1'

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('does NOT overwrite the URL when it names a different VALID service (prevents the #185 swap loop)', async () => {
    // Regression: switching to a freshly-added 2nd service. The URL names a
    // known service (svc-new) that differs from the store's activeServiceId
    // (svc-old). That is a deep-link / navigation the URL→store effect adopts.
    // The store→URL effect must DEFER — pushing svc-old back into the URL while
    // URL→store pushes svc-new makes the two effects swap values every render
    // and blow React's update-depth limit ("Maximum update depth exceeded",
    // React #185), which crashed the whole shell on switch.
    mockState = {
      ...mockState,
      activeServiceId: 'svc-old',
      services: [
        { id: 'svc-old', name: 'Old' },
        { id: 'svc-new', name: 'New' },
      ],
      isInitialized: true,
    }
    mockUrlService = 'svc-new'

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // URL→store adopts the new service; store→URL must NOT clobber the URL.
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-new')
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('DEFERS the stamp during a mid-switch (active id not yet in the list) to break the #185 swap', async () => {
    // The captured #185 was `Set.forEach → setActiveServiceId` — a store
    // fan-out looping from the render/commit phase. It fires in the window
    // where the switcher has written `activeServiceId = svc-new` but the
    // membership-validated `services` list has NOT yet caught up (still
    // [svc-old]); the URL has not been stamped yet (null). Old behaviour:
    // store→URL eagerly STAMPED svc-new into the URL even though the list
    // can't validate it. The companion useBootstrap reconcile, running
    // against the same stale snapshot, can revert activeServiceId back off
    // svc-new — and with the URL already mutated to svc-new the two writers
    // swap every render. The mid-switch guard must DEFER: stamp nothing
    // until activeServiceId is itself an established member of `services`,
    // mirroring useBootstrap's own "stale snapshot → defer, don't evict".
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [{ id: 'svc-old', name: 'Old' }], // list mid-update; svc-new not in yet
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // store→URL must stay silent: stamping svc-new before the list validates
    // it is the premature write that lets the reconcile-revert close the loop.
    expect(mockSetUrlService).not.toHaveBeenCalled()
  })

  it('converges in a single stamp once the list lands and the URL is blank', async () => {
    // Continuation of the mid-switch: the bootstrap reconcile lands the list
    // now carrying svc-new, and the URL has not yet been stamped (null). With
    // the active id finally an established member, the mid-switch guard no
    // longer defers — store→URL stamps svc-new exactly once and converges.
    // (When the URL instead already names a *different* valid service, the
    // L90 "deep-link / back-forward wins" guard takes over; that path is
    // covered by the VALID-service test above.)
    mockState = {
      ...mockState,
      activeServiceId: 'svc-new',
      services: [
        { id: 'svc-old', name: 'Old' },
        { id: 'svc-new', name: 'New' },
      ],
      isInitialized: true,
    }
    mockUrlService = null

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-new')
  })

  it('still clears a STALE ?service that names no known service', async () => {
    // A `?service=` left by a previous install / dead share link is not in the
    // services list, so URL→store won't adopt it. The store→URL effect should
    // still correct the URL to the real active service (no swap risk, since the
    // stale id can never come back through URL→store).
    mockState = {
      ...mockState,
      activeServiceId: 'svc-real',
      services: [{ id: 'svc-real', name: 'Real' }],
      isInitialized: true,
    }
    mockUrlService = 'svc-dead'

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    expect(mockSetUrlService).toHaveBeenCalledWith('svc-real')
  })
})
