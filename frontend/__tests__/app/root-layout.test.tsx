/**
 * Verifies RootLayout streams its shell immediately without awaiting /api/bootstrap
 * and hydrates streamed bootstrap data safely on the client.
 *
 * @vitest-environment jsdom
 */
import React from 'react'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider, dehydrate } from '@tanstack/react-query'
import { queryKeys } from '@/lib/api'
import { StreamedBootstrapSeed } from '@/components/QueryProvider'

const { fetchBootstrapMock, cookiesMock, headersMock } = vi.hoisted(() => ({
  fetchBootstrapMock: vi.fn(),
  cookiesMock: vi.fn(async () => ({ get: () => undefined })),
  headersMock: vi.fn(async () => new Headers({ host: 'fastly-log-analytics.example.com', 'x-proxied-by-caddy': 'true', 'x-nonce': 'nonce-123' })),
}))

vi.mock('next/font/google', () => ({
  Inter: () => ({ className: 'font-inter' }),
}))
vi.mock('next/headers', () => ({
  cookies: cookiesMock,
  headers: headersMock,
}))
vi.mock('next-themes', () => ({
  ThemeProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))
vi.mock('@/components/AppLayout', () => ({
  AppLayout: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="app-layout-shell">{children}</div>
  ),
}))
vi.mock('@/components/ServerFooter', () => ({
  ServerFooter: () => <footer data-testid="server-footer" />,
}))
vi.mock('@/components/QueryProvider', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/QueryProvider')>()
  return {
    ...actual,
    default: ({ children }: { children: React.ReactNode }) => <div data-testid="query-provider">{children}</div>,
    ReloadLoopGuard: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    StoreHydrator: () => null,
    WebVitalsReporter: () => null,
  }
})
vi.mock('@/components/AnalyticsCard', () => ({
  ErrorBoundaryWithRouteReset: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))
vi.mock('@/lib/ssr/upstream', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/ssr/upstream')>()
  return {
    ...actual,
    fetchBootstrapServerSide: fetchBootstrapMock,
  }
})

import RootLayout from '@/app/layout'
import { BootstrapHydration } from '@/components/BootstrapHydration'

describe('RootLayout streaming bootstrap shell', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('resolves RootLayout shell synchronously without waiting for fetchBootstrapServerSide', async () => {
    fetchBootstrapMock.mockImplementation(() => new Promise(() => {}))

    const tree = await RootLayout({ children: <div>Child page</div> })
    expect(tree).toBeTruthy()
    expect(fetchBootstrapMock).not.toHaveBeenCalled()
  })

  it('BootstrapHydration emits RUM script and StreamedBootstrapSeed when bootstrap resolves', async () => {
    const bootstrapPayload = {
      active_service_id: 'svc-1',
      services: [{ service_id: 'svc-1', name: 'Svc 1', rum_enabled: true }],
    }
    fetchBootstrapMock.mockResolvedValue(bootstrapPayload)

    const el = await BootstrapHydration({
      host: 'fastly-log-analytics.example.com',
      proxiedByCaddy: true,
      nonce: 'nonce-123',
    })
    const queryClient = new QueryClient()
    const { container, unmount } = render(
      <QueryClientProvider client={queryClient}>{el}</QueryClientProvider>,
    )

    try {
      const script =
        document.head.querySelector('script[src="/js/rum.js"]') ??
        container.querySelector('script[src="/js/rum.js"]')
      expect(script).not.toBeNull()
      expect(script?.getAttribute('nonce')).toBe('nonce-123')
      await waitFor(() => {
        expect(queryClient.getQueryData(queryKeys.bootstrap())).toEqual(bootstrapPayload)
      })
    } finally {
      unmount()
      for (const node of document.head.querySelectorAll('script[src="/js/rum.js"]')) {
        node.remove()
      }
    }
  })

  it('StreamedBootstrapSeed hydrates query cache without overwriting fresher client data', async () => {
    const serverClient = new QueryClient()
    serverClient.setQueryData(queryKeys.bootstrap(), { active_service_id: 'svc-ssr', services: [] })
    const dehydrated = dehydrate(serverClient)

    const clientA = new QueryClient()
    render(
      <QueryClientProvider client={clientA}>
        <StreamedBootstrapSeed state={dehydrated} />
      </QueryClientProvider>,
    )
    await waitFor(() => {
      expect(clientA.getQueryData(queryKeys.bootstrap())).toEqual({
        active_service_id: 'svc-ssr',
        services: [],
      })
    })

    const clientB = new QueryClient()
    clientB.setQueryData(queryKeys.bootstrap(), { active_service_id: 'svc-fresh', services: [] })
    render(
      <QueryClientProvider client={clientB}>
        <StreamedBootstrapSeed state={dehydrated} />
      </QueryClientProvider>,
    )
    expect(clientB.getQueryData(queryKeys.bootstrap())).toEqual({
      active_service_id: 'svc-fresh',
      services: [],
    })
  })
})
