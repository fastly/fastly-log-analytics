/**
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { describe, expect, it } from 'vitest'
import { useRumStatus } from '@/hooks/useRumStatus'
import { server } from '@/__tests__/helpers/msw/server'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'

describe('useRumStatus', () => {
  it('maps 403 to an unauthorized-disabled status', async () => {
    server.use(http.get('*/api/services/svc/rum/status', () => HttpResponse.json({}, { status: 403 })))
    const { result } = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(result.current.data).toEqual({ enabled: false, unauthorized: true }))
  })

  it('returns parsed JSON on success and null on other failures', async () => {
    server.use(
      http.get('*/api/services/svc/rum/status', () => HttpResponse.json({ enabled: true })),
      http.get('*/api/services/svc2/rum/status', () => HttpResponse.json({}, { status: 500 })),
    )
    const ok = renderHook(() => useRumStatus('svc'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(ok.result.current.data).toEqual({ enabled: true }))

    const bad = renderHook(() => useRumStatus('svc2'), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await waitFor(() => expect(bad.result.current.isSuccess).toBe(true))
    expect(bad.result.current.data).toBeNull()
  })

  it('does not fetch without a service id', async () => {
    let hits = 0
    server.use(
      http.get('*/api/services/*/rum/status', () => {
        hits += 1
        return HttpResponse.json({})
      }),
    )
    renderHook(() => useRumStatus(null), { wrapper: makeQueryWrapper(createTestQueryClient()) })
    await new Promise((r) => setTimeout(r, 50))
    expect(hits).toBe(0)
  })
})
