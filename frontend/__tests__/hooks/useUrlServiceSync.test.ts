/**
 * @vitest-environment jsdom
 *
 * useUrlServiceSync — bidirectional sync between the ?service= URL param and
 * the active service in the store. Sister to useUrlFilterSync.
 *
 * Behavior under test:
 *   - On mount, read ?service=X and write to the store.
 *   - When the store's activeServiceId changes (after init), push it to the URL.
 *   - If services list is empty, the URL must not carry a stale ?service.
 */
import { renderHook, act } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mockReplace = vi.fn()
const mockSearchParams = { get: vi.fn() }

vi.mock('next/navigation', () => ({
  useRouter: () => ({ replace: mockReplace, push: vi.fn() }),
  usePathname: () => '/dashboard',
  useSearchParams: () => mockSearchParams,
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
    selector ? selector(mockState) : mockState
  ),
}))

beforeEach(() => {
  mockReplace.mockReset()
  mockSetActiveServiceId.mockReset()
  mockSearchParams.get.mockReset()
  mockState = {
    activeServiceId: null,
    services: [],
    isInitialized: false,
    setActiveServiceId: mockSetActiveServiceId,
  }
})

describe('useUrlServiceSync — URL → store', () => {
  it('writes ?service= URL param into the store on mount', async () => {
    mockSearchParams.get.mockImplementation((key: string) => (key === 'service' ? 'svc-from-url' : null))
    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).toHaveBeenCalledWith('svc-from-url')
  })

  it('does nothing when there is no ?service= param', async () => {
    mockSearchParams.get.mockReturnValue(null)
    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockSetActiveServiceId).not.toHaveBeenCalled()
  })

  it('does not re-write when URL param matches store already', async () => {
    mockState.activeServiceId = 'svc-1'
    mockSearchParams.get.mockReturnValue('svc-1')
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
    mockSearchParams.get.mockReturnValue(null)

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    // The post-mount effect runs after isInitialMount.current flips on the
    // FIRST render; trigger another render to exercise the second effect.
    act(() => {
      rerender()
    })

    expect(mockReplace).toHaveBeenCalledWith('/dashboard?service=svc-new')
  })

  it('strips ?service= from URL when services list is empty', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-orphan',
      services: [],
      isInitialized: true,
    }
    mockSearchParams.get.mockReturnValue('svc-orphan')

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    const { rerender } = renderHook(() => useUrlServiceSync())
    act(() => {
      rerender()
    })

    // No services → URL shouldn't carry a service param
    expect(mockReplace).toHaveBeenCalledWith('/dashboard')
  })

  it('skips the store→URL sync until isInitialized is true', async () => {
    mockState = {
      ...mockState,
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'S' }],
      isInitialized: false, // not yet booted
    }
    mockSearchParams.get.mockReturnValue(null)

    const { useUrlServiceSync } = await import('@/hooks/useUrlServiceSync')
    renderHook(() => useUrlServiceSync())
    expect(mockReplace).not.toHaveBeenCalled()
  })
})
