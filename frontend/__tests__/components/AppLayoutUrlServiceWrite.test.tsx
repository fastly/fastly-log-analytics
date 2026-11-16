/**
 * @vitest-environment jsdom
 */
import { partial, renderWithQueryClient, stubPointerCaptureAndScroll, stubResizeObserver } from '@/__tests__/helpers/page-smoke'
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
