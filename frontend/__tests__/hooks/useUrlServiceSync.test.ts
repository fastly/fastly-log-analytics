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

vi.mock('@/stores/serviceStore', () => ({
  useServiceStore: vi.fn((selector?: (s: typeof mockState) => any) =>
    selector ? selector(mockState) : mockState,
  ),
}))

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
})
