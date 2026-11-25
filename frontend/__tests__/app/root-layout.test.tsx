/**
 * Verifies RootLayout awaits fetchBootstrapServerSide() synchronously before rendering
 * so SSR routes receive dehydrated bootstrap state and sidebar metadata in the initial HTML,
 * while guarding cookies()/headers() during STATIC_EXPORT builds.
 *
 * @vitest-environment jsdom
 */
import React from 'react'
import { describe, expect, it, vi, beforeEach } from 'vitest'

const { fetchBootstrapMock, cookiesMock, headersMock } = vi.hoisted(() => ({
  fetchBootstrapMock: vi.fn(),
  cookiesMock: vi.fn(async () => ({ get: () => undefined })),
  headersMock: vi.fn(
    async () =>
      new Headers({
        host: 'fastly-log-analytics.example.com',
        'x-proxied-by-caddy': 'true',
        'x-nonce': 'nonce-123',
      }),
  ),
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
vi.mock('@/components/QueryProvider', () => ({
  default: ({ children }: { children: React.ReactNode }) => <div data-testid="query-provider">{children}</div>,
  ReloadLoopGuard: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  StoreHydrator: () => null,
  WebVitalsReporter: () => null,
}))
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

describe('RootLayout synchronous SSR bootstrap', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    delete process.env.STATIC_EXPORT
  })

  it('hydrates bootstrap dehydratedState and RUM script before returning RootLayout HTML', async () => {
    fetchBootstrapMock.mockResolvedValue({
      active_service_id: 'svc-1',
      services: [{ service_id: 'svc-1', name: 'Svc 1', rum_enabled: true, cmcd_enabled: true }],
    })

    const tree = (await RootLayout({ children: <div>Child page</div> })) as React.ReactElement<{
      children: [React.ReactElement<{ children: React.ReactNode[] }>, React.ReactElement]
    }>
    expect(fetchBootstrapMock).toHaveBeenCalledTimes(1)

    const head = tree.props.children[0]
    const headChildren = React.Children.toArray(head.props.children) as React.ReactElement<{
      src?: string
      nonce?: string
    }>[]
    const rumScript = headChildren.find((child) => child?.props?.src === '/js/rum.js')
    expect(rumScript).toBeDefined()
    expect(rumScript?.props?.nonce).toBe('nonce-123')
  })

  it('skips cookies() and headers() when STATIC_EXPORT=1', async () => {
    process.env.STATIC_EXPORT = '1'
    fetchBootstrapMock.mockResolvedValue(null)

    const tree = await RootLayout({ children: <div>Child page</div> })
    expect(tree).toBeTruthy()
    expect(cookiesMock).not.toHaveBeenCalled()
    expect(headersMock).not.toHaveBeenCalled()
  })
})
