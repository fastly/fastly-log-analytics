/**
 * CacheCollapseModal renders its "Recent Cache Misses" list through the shared
 * DataTableReadonly. Pins the column headers (including the timezone-suffixed
 * timestamp) and the cache/status badges.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubPointerCaptureAndScroll } from '@/__tests__/helpers/jsdom-shims'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient } from '../helpers/query'
import { server } from '../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { CacheCollapseModal } from '@/components/Insights/CacheCollapseModal'

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('@/components/charts/TimeSeriesChart', () => ({ TimeSeriesChart: () => <div data-testid="timeseries" /> }))

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: 'svc-cache', isInitialized: true })
})

describe('CacheCollapseModal recent misses', () => {
  it('lists recent misses with cache and status badges', async () => {
    server.use(
      http.post(`${getApiBase()}/api/insights/cache-collapse-detail`, () =>
        HttpResponse.json({
          timeline: [],
          recent_misses: [
            { timestamp: '2026-10-01T10:00:00Z', cache: 'MISS', pop: 'SJC', ip: '203.0.113.9', status: 200 },
            { timestamp: '2026-10-01T10:01:00Z', cache: 'PASS', pop: null, ip: null, status: 503 },
          ],
          baseline_hit_rate: 0.9,
          window_hit_rate: 0.1,
          baseline_pass_rate: 0,
          window_pass_rate: 0,
        }),
      ),
    )
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <CacheCollapseModal isOpen onOpenChange={() => {}} url="/hot" windowHours="1" baselineHours="24" />
      </QueryClientProvider>,
    )
    expect(await screen.findByText('SJC')).toBeTruthy()
    expect(screen.getByRole('columnheader', { name: /^Timestamp \(/ })).toBeTruthy()
    for (const name of ['Cache Status', 'POP', 'Client IP', 'HTTP Status']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('MISS')).toBeTruthy()
    expect(screen.getByText('PASS')).toBeTruthy()
    expect(screen.getByText('503')).toBeTruthy()
    expect(screen.getByText('203.0.113.9')).toBeTruthy()
  })
})
