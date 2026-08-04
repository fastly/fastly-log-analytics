/**
 * MSW-backed integration: the admin /api/services list flows through
 * openapi-fetch (with the request/response middleware) into the
 * ``ServicesTable`` component.
 *
 * Targets ``ServicesTable`` directly rather than ``AdminPage``: the
 * 2026-06-14 RSC conversion (commit 27a5c68) made the page a React
 * Server Component, which jsdom can't render — only the static
 * ``PageHeader`` chrome paints, the rest of the tree is server-side.
 * The test's load-bearing assertion has always been "services from
 * /api/services flow into the rendered table"; pointing at the
 * ``'use client'`` ``ServicesTable`` component delivers that
 * contract without depending on the RSC boundary.
 */
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'

import { useServiceStore } from '@/stores/serviceStore'
import { ServicesTable } from '@/app/admin/_sections/ServicesTable'
import { server } from '../../tests/msw/server'

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/admin'),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))


import { getApiBase } from '@/lib/api'

const API_BASE = getApiBase()

vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: (options: any) => ({
    getVirtualItems: () => {
      const count = options.count || 0
      return Array.from({ length: count }).map((_, i) => ({
        index: i,
        start: i * 40,
        size: 40,
      }))
    },
    getTotalSize: () => (options.count || 0) * 40,
  }),
}))

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true } as never)
})

test('ServicesTable renders services flowed through MSW + openapi-fetch', async () => {
  server.use(
    http.get(`${API_BASE}/api/services`, () =>
      HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Test Service',
            fos_bucket: 'test-bucket',
            access_level: 'read_write',
          },
        ],
      }),
    ),
  )

  // Per-test QueryClient avoids cross-test cache leakage and lets us
  // disable retries cleanly.
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  render(
    <QueryClientProvider client={queryClient}>
      <ServicesTable />
    </QueryClientProvider>,
  )

  await waitFor(
    () => {
      expect(screen.getByText('Test Service')).toBeInTheDocument()
      expect(screen.getByText('test-svc')).toBeInTheDocument()
      expect(screen.getByText('test-bucket')).toBeInTheDocument()
    },
    { timeout: 3000 },
  )
})

test('the row Manage menu offers Delete Data (left of/above Teardown) and opens its confirmation dialog', async () => {
  server.use(
    http.get(`${API_BASE}/api/services`, () =>
      HttpResponse.json({
        services: [
          {
            service_id: 'test-svc',
            name: 'Test Service',
            fos_bucket: 'test-bucket',
            access_level: 'read_write',
          },
        ],
      }),
    ),
  )

  const user = userEvent.setup()
  const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
  render(
    <QueryClientProvider client={queryClient}>
      <ServicesTable />
    </QueryClientProvider>,
  )

  await waitFor(() => expect(screen.getByText('Test Service')).toBeInTheDocument())

  await user.click(screen.getByRole('button', { name: /manage/i }))

  const deleteDataItem = await screen.findByText('Delete Data')
  const teardownItem = screen.getByText('Teardown Service')
  // Delete Data must be the less-destructive-first option, ahead of the
  // full-service Teardown, in DOM order (menu is rendered top-to-bottom).
  expect(
    deleteDataItem.compareDocumentPosition(teardownItem) & Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy()

  await user.click(deleteDataItem)
  expect(await screen.findByText('Delete Data: Test Service')).toBeInTheDocument()
})
