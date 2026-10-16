/**
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useRumStatus } from '@/hooks/useRumStatus'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'

const adminFetch = vi.fn()
vi.mock('@/lib/api', () => ({ adminFetch: (...a: unknown[]) => adminFetch(...a) }))

beforeEach(() => adminFetch.mockReset())

describe('useRumStatus', () => {
  it('maps 403 to an unauthorized-disabled status', async () => {
    adminFetch.mockResolvedValue({ status: 403, ok: false })
    const { result } = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(result.current.data).toEqual({ enabled: false, unauthorized: true }))
  })

  it('returns parsed JSON on success and null on other failures', async () => {
    adminFetch.mockResolvedValue({ status: 200, ok: true, json: async () => ({ enabled: true }) })
    const ok = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(ok.result.current.data).toEqual({ enabled: true }))

    adminFetch.mockResolvedValue({ status: 500, ok: false })
    const bad = renderHook(() => useRumStatus('svc2'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(bad.result.current.isSuccess).toBe(true))
    expect(bad.result.current.data).toBeNull()
  })

  it('does not fetch without a service id', () => {
    renderHook(() => useRumStatus(null), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    expect(adminFetch).not.toHaveBeenCalled()
  })
})
