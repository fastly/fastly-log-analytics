import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Activity } from 'lucide-react'
import type { useRouter } from 'next/navigation'
import { SidebarNavList } from '@/components/AppLayoutChrome'
import { partial } from '@/__tests__/helpers/page-smoke'

vi.mock('next/navigation', () => ({
  usePathname: () => window.location.pathname,
  useSearchParams: () => new URLSearchParams(window.location.search),
  useRouter: () => ({}),
}))

const start = '2026-11-05T12:00:00Z'
const end = '2026-11-06T12:00:00Z'
const filters = JSON.stringify({ status: { mode: 'exclude', values: ['503'] } })

function renderNavigation(href: string, activeServiceId?: string | null) {
  const router = partial<ReturnType<typeof useRouter>>({ prefetch: vi.fn() })
  const view = render(
    <div onClickCapture={event => {
      const link = (event.target as Element).closest('a')
      if (!link) return
      event.preventDefault()
      window.history.pushState({}, '', link.href)
    }}>
      <SidebarNavList
        items={[{ name: 'Analytics', href, icon: Activity }]}
        ariaLabel="Primary"
        pathname={window.location.pathname}
        activeServiceId={activeServiceId}
        router={router}
      />
    </div>,
  )
  return { ...view, router, link: screen.getByRole('link', { name: 'Analytics' }) }
}

beforeEach(() => {
  window.history.replaceState({}, '', '/dashboard')
})

describe('canonical sidebar navigation', () => {
  it.each(['/dashboard', '/origin'])('preserves the selected absolute window and filters when clicking %s', href => {
    const params = new URLSearchParams({ service: 'url-service', start_time: start, end_time: end, filters })
    window.history.replaceState({}, '', `/dashboard?${params}`)
    const { link, router } = renderNavigation(href, 'selected-service')
    const expected = new URL(href, window.location.origin)
    expected.search = new URLSearchParams({ service: 'selected-service', start_time: start, end_time: end, filters }).toString()

    fireEvent.mouseEnter(link)
    expect(router.prefetch).toHaveBeenCalledWith(
      expect.stringContaining('start_time='),
    )
    const actual = new URL(link.getAttribute('href')!, window.location.origin)
    expect(actual.pathname).toBe(expected.pathname)
    expect(Object.fromEntries(actual.searchParams)).toEqual(Object.fromEntries(expected.searchParams))

    fireEvent.click(link)
    expect(window.location.pathname).toBe(href)
    expect(Object.fromEntries(new URLSearchParams(window.location.search))).toEqual({
      service: 'selected-service', start_time: start, end_time: end, filters,
    })
  })

  it('carries relative range and canonical filters but excludes source route tokens, sharing, auth and SQL', () => {
    const params = new URLSearchParams({
      service: 'url-service', range: '6h', filters,
      token: 'detail-SENTINEL', session_token: 'session-SENTINEL',
      auth: 'auth-SENTINEL', share: 'share-SENTINEL', code: 'code-SENTINEL',
      q: 'SELECT private_column FROM logs', mode: 'raw', tab: 'details', metric: 'requests',
    })
    window.history.replaceState({}, '', `/sessions/stream?${params}`)
    const { link } = renderNavigation('/performance', 'selected-service')

    fireEvent.click(link)

    expect(window.location.pathname).toBe('/performance')
    expect(Object.fromEntries(new URLSearchParams(window.location.search))).toEqual({
      service: 'selected-service', range: '6h', filters,
    })
    expect(window.location.href).not.toContain('SENTINEL')
  })

  it('keeps target route options and uses the selected service instead of a stale URL service', () => {
    window.history.replaceState({}, '', '/dashboard?service=stale&range=24h&metric=bytes&token=detail-SENTINEL')
    const { link } = renderNavigation('/network?metric=rtt#map', 'selected-service')

    fireEvent.click(link)

    expect(window.location.pathname).toBe('/network')
    expect(window.location.hash).toBe('#map')
    expect(Object.fromEntries(new URLSearchParams(window.location.search))).toEqual({
      metric: 'rtt', service: 'selected-service', range: '24h',
    })
  })

  it('preserves a URL service before service-store hydration', () => {
    window.history.replaceState({}, '', '/dashboard?service=url-service&range=1h')
    const { link } = renderNavigation('/origin')

    fireEvent.click(link)

    expect(Object.fromEntries(new URLSearchParams(window.location.search))).toEqual({
      service: 'url-service', range: '1h',
    })
  })

  it('leaves navigation without a service or canonical selection unchanged', () => {
    const { link } = renderNavigation('/origin', null)

    expect(link).toHaveAttribute('href', '/origin')
  })
})
