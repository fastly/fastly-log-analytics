/**
 * Pins the SSR debug-query-visibility fix: toggling either switch must (1)
 * write the fla.debugResponses cookie as the OR of both toggles, so
 * lib/ssr/_transport.ts's SSR fetch can see it on the next navigation, and
 * (2) immediately invalidate the cache, so the CURRENT page's queries pick
 * up the header without needing a reload. No test existed for this
 * component before this fix.
 */
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, test, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { DiagnosticsPanel } from '@/app/admin/_sections/DiagnosticsPanel'
import { useDebugStore } from '@/stores/debugStore'
import { DEBUG_RESPONSES_COOKIE } from '@/lib/debug-cookie'

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => ({ data: { debug_state: { debug_responses_enabled: true } } }),
}))

function makeClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } })
}

function clearDebugCookie() {
  document.cookie = `${DEBUG_RESPONSES_COOKIE}=; path=/; max-age=0`
}

function readDebugCookie(): string | undefined {
  return document.cookie
    .split('; ')
    .find((c) => c.startsWith(`${DEBUG_RESPONSES_COOKIE}=`))
    ?.split('=')[1]
}

beforeEach(() => {
  act(() => {
    useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
  })
  clearDebugCookie()
})

describe('DiagnosticsPanel — debug cookie + refetch on toggle', () => {
  test('turning the query-debug switch on writes cookie=1 and invalidates the query cache', async () => {
    const qc = makeClient()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

    await userEvent.click(screen.getAllByRole('switch')[0])

    expect(readDebugCookie()).toBe('1')
    // Invalidation (not a point-in-time refetch) so enabled:false data
    // queries are marked stale and refetch when they switch on.
    expect(invalidateSpy).toHaveBeenCalled()
    expect(useDebugStore.getState().enabled).toBe(true)
  })

  test('turning the api-calls switch on ALSO sets the cookie (OR semantics)', async () => {
    const qc = makeClient()
    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

    const switches = screen.getAllByRole('switch')
    await userEvent.click(switches[1])

    expect(readDebugCookie()).toBe('1')
    expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
  })

  test('turning both switches off clears the cookie back to 0', async () => {
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: false })
    })
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/`

    const qc = makeClient()
    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

    const switches = screen.getAllByRole('switch')
    await userEvent.click(switches[0])

    expect(readDebugCookie()).toBe('0')
    expect(useDebugStore.getState().enabled).toBe(false)
  })
})
