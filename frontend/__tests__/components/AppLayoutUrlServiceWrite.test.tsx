/**
 * @vitest-environment jsdom
 */
import { createTestQueryClient, makeQueryWrapper, partial, renderWithQueryClient, stubIntersectionObserver, stubPointerCaptureAndScroll, stubResizeObserver } from '@/__tests__/helpers/page-smoke'
import { hydrateRoot } from 'react-dom/client'
import { renderToString } from 'react-dom/server'
import { act, cleanup } from '@testing-library/react'
import { afterEach, beforeAll, expect, test, vi } from 'vitest'
import { AppLayout } from '@/components/AppLayout'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/filterStore'
import type { BootstrapResponse } from '@/types/api'

const { urlWrites } = vi.hoisted(() => ({ urlWrites: [] as Array<[string, unknown]> }))

vi.mock('nuqs', async (importOriginal) => ({
  ...(await importOriginal<typeof import('nuqs')>()),
  useQueryState: (key: string) => [null, (value: unknown) => { urlWrites.push([key, value]); return Promise.resolve(new URLSearchParams()) }],
}))
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/insights'))
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: vi.fn(),
  useIsAnalyst: () => false,
  useEnforceMaskedFilters: vi.fn(),
}))
vi.mock('@/components/FilterBar/FilterBar', () => ({ FilterBar: () => <div>FilterBar</div> }))
vi.mock('@/components/SyncStatusBadge/SyncStatusBadge', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/SyncStatusBadge/SyncStatusBadge')>()),
  ServiceSwitcher: () => <div>ServiceSwitcher</div>,
  SyncStatusBadge: () => <div>SyncStatusBadge</div>,
}))
vi.mock('@/components/DebugPanel', () => ({ DebugPanel: () => <div>DebugPanel</div> }))

beforeAll(() => {
  stubResizeObserver()
  stubIntersectionObserver()
  stubPointerCaptureAndScroll()
  if (!Element.prototype.getAnimations) Element.prototype.getAnimations = () => []
})
afterEach(cleanup)

// A URL update that reaches the page's Suspense boundary before it has hydrated
// makes React discard the server HTML and client-render the page behind a null
// fallback: a blank flash and a large layout shift on every cold load.
test('writes the active service into the URL only after the page content has rendered', async () => {
  urlWrites.length = 0
  useServiceStore.setState({
    services: [{ id: 'svc-1', name: 'Service' }] as ReturnType<typeof useServiceStore.getState>['services'],
    activeServiceId: 'svc-1',
    isInitialized: true,
  })
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: {
      runtime_mode: 'high_scale',
      user: { id: 'u1', role: 'admin' },
      services: [{ service_id: 'svc-1', name: 'Service' }],
      active_service_id: 'svc-1',
    } as unknown as BootstrapResponse,
    dataUpdatedAt: 1,
    isSuccess: true,
    isLoading: false,
  }))

  let release!: () => void
  const pending = new Promise<void>((resolve) => { release = resolve })
  let ready = false
  pending.then(() => { ready = true })
  function PageContent() {
    if (!ready) throw pending
    return <div>Content</div>
  }

  const view = renderWithQueryClient(<AppLayout><PageContent /></AppLayout>)
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 20)) })
  expect(urlWrites.filter(([key]) => key === 'service')).toEqual([])

  await act(async () => { release(); await pending; await new Promise((resolve) => setTimeout(resolve, 20)) })
  expect(view.getByText('Content')).toBeTruthy()
  const writes = urlWrites.filter(([key]) => key === 'service')
  expect(writes.length).toBeGreaterThan(0)
  expect(writes.every(([, value]) => value === 'svc-1')).toBe(true)
})

// The layout re-renders when bootstrap data or the service store changes. If
// that reaches the page's still-dehydrated Suspense boundary (its chunks are
// still loading), React gives up on hydrating it and client-renders the page:
// the server HTML disappears behind the null fallback until the chunks arrive.
test('layout re-renders before the page hydrates keep the server-rendered page', async () => {
  const services = [{ id: 'svc-1', name: 'Service' }] as ReturnType<typeof useServiceStore.getState>['services']
  useServiceStore.setState({ services, activeServiceId: 'svc-1', isInitialized: true })
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: {
      runtime_mode: 'high_scale',
      user: { id: 'u1', role: 'admin' },
      services: [{ service_id: 'svc-1', name: 'Service' }],
      active_service_id: 'svc-1',
    } as unknown as BootstrapResponse,
    dataUpdatedAt: 1,
    isSuccess: true,
    isLoading: false,
  }))

  let ready = true
  let release!: () => void
  const pending = new Promise<void>((resolve) => { release = resolve })
  function PageContent() {
    if (!ready) throw pending
    return <div data-testid="page-content">Content</div>
  }
  const Wrapper = makeQueryWrapper(createTestQueryClient())
  const tree = <Wrapper><AppLayout><PageContent /></AppLayout></Wrapper>
  const container = document.createElement('div')
  container.innerHTML = renderToString(tree)
  document.body.appendChild(container)
  const serverNode = container.querySelector('[data-testid="page-content"]')
  expect(serverNode).not.toBeNull()

  ready = false
  const recoverable: unknown[] = []
  let root!: ReturnType<typeof hydrateRoot>
  await act(async () => {
    root = hydrateRoot(container, tree, { onRecoverableError: (error) => recoverable.push(error) })
  })
  expect(container.querySelector('[data-testid="page-content"]'), 'hydration alone keeps the server page').toBe(serverNode)
  await act(async () => {
    useServiceStore.setState({ services: [...services] })
  })
  expect(container.querySelector('[data-testid="page-content"]'), 'a layout re-render keeps the server page').toBe(serverNode)

  ready = true
  await act(async () => { release(); await pending })
  expect(container.querySelector('[data-testid="page-content"]')).toBe(serverNode)
  expect(recoverable).toEqual([])
  act(() => root.unmount())
  container.remove()
})
