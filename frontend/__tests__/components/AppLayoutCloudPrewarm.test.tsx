/**
 * @vitest-environment jsdom
 */
import { partial, renderWithQueryClient, stubPointerCaptureAndScroll, stubResizeObserver } from '@/__tests__/helpers/page-smoke'
import { act, cleanup } from '@testing-library/react'
import { afterEach, beforeAll, expect, test, vi } from 'vitest'
import { AppLayout } from '@/components/AppLayout'
import { useBootstrap } from '@/hooks/useBootstrap'
import { usePathname } from 'next/navigation'
import type { BootstrapResponse } from '@/types/api'

const { engineCalls } = vi.hoisted(() => ({ engineCalls: [] as string[] }))

vi.mock('@/lib/cloud-engine/mode', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/cloud-engine/mode')>()),
  IS_CLOUD_MODE: true,
}))
vi.mock('@/lib/cloud-engine/dashboard/client', () => ({
  prewarmDashboardEngine: () => engineCalls.push('prewarm'),
  setDashboardCacheIdentity: async (identity: string) => {
    if (!engineCalls.includes(`identity:${identity}`)) engineCalls.push(`identity:${identity}`)
  },
}))
vi.mock('@/lib/cloud-engine/telemetry/sources', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/cloud-engine/telemetry/sources')>()),
  syncOpsCacheForSession: vi.fn(async () => undefined),
}))
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/insights'))
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: vi.fn(),
  useUrlServiceSync: vi.fn(),
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

test('prewarms the Cloud dashboard engine only after the session cache identity is set', async () => {
  vi.mocked(usePathname).mockReturnValue('/insights')
  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({ isLoading: true }))
  const view = renderWithQueryClient(<AppLayout><div>Content</div></AppLayout>)
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 20)) })

  vi.mocked(useBootstrap).mockReturnValue(partial<ReturnType<typeof useBootstrap>>({
    data: {
      runtime_mode: 'cloud',
      user: { id: 'u1', role: 'admin' },
      services: [{ service_id: 'svc-1', name: 'Service' }],
    } as unknown as BootstrapResponse,
    dataUpdatedAt: 1,
    isSuccess: true,
    isLoading: false,
  }))
  view.rerender(<AppLayout><div>Content</div></AppLayout>)
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 20)) })

  expect(engineCalls).toEqual(['identity:u1:admin', 'prewarm'])
})
