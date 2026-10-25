import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { describe, expect, it, test, vi, beforeEach } from 'vitest'
import { DeleteDataDialog } from '@/components/TeardownDialog'
import { TeardownDialog } from '@/components/TeardownDialog'
import { SSEProgressView } from '@/components/SSEModal'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'
import { createUseSSEMock } from '../helpers/page-smoke'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { server } from '@/__tests__/helpers/msw/handlers'
import type { ServiceConfig } from '@/types/api'
import type { SSELine } from '@/hooks/useSSE'
import React from 'react'

const API_BASE = getApiBase()

// ScrollArea is a portal-y shadcn wrapper; a passthrough keeps jsdom simple.
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

const { state: sseState, start: mockStart, stop: mockStop, resetState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()), useSSE: () => sseState }))

beforeEach(() => {
  resetState()
  useServiceStore.setState({ activeServiceId: 'some-other-active-service', isInitialized: true })
})

function renderDialog(props: Partial<React.ComponentProps<typeof DeleteDataDialog>> = {}) {
  const qc = createTestQueryClient()
  return render(
    <DeleteDataDialog
      service={{
        service_id: 'test-svc',
        name: 'Test Service',
        access_level: 'read_write',
        duckdb_size_bytes: 5_000_000,
        cache_file_count: 12,
      } as ServiceConfig}
      open={true}
      onOpenChange={vi.fn()}
      {...props}
    />,
    { wrapper: makeQueryWrapper(qc) },
  )
}

test('DeleteDataDialog shows local + cloud file counts and sizes before confirming', async () => {
  sseState.status = 'idle'
  server.use(
    http.get(`${API_BASE}/api/admin/iceberg-info`, ({ request }) => {
      const url = new URL(request.url)
      if (url.searchParams.get('service_id') !== 'test-svc') {
        return HttpResponse.json({ error: 'wrong service targeted' }, { status: 400 })
      }
      return HttpResponse.json({ data_files: 42, size_bytes: 123_456_789, snapshots: 3 })
    }),
  )

  renderDialog()

  expect(screen.getByText('Delete Data: Test Service')).toBeDefined()
  expect(await screen.findByText('12 files')).toBeInTheDocument()

  await waitFor(() => {
    expect(screen.getByText('42 files')).toBeInTheDocument()
  })
})

test('DeleteDataDialog keeps Delete Data disabled on a near-miss confirmation', async () => {
  sseState.status = 'idle'
  const user = userEvent.setup()

  renderDialog()

  const deleteBtn = screen.getByRole('button', { name: /^delete data$/i })
  const confirmInput = screen.getByLabelText(/type.*to confirm/i)

  await user.type(confirmInput, 'test service') // wrong case
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)

  await user.clear(confirmInput)
  await user.type(confirmInput, 'Test Servic') // missing trailing letter
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)

  await user.type(confirmInput, 'e') // now exact
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(false)
})

test('DeleteDataDialog explains the destructive scope and starts the SSE stream on confirm', async () => {
  sseState.status = 'idle'
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  renderDialog({ onOpenChange, onComplete })

  expect(screen.getByText('Delete Data: Test Service')).toBeDefined()
  expect(screen.getByText(/local.*and cloud-stored/i)).toBeDefined()
  expect(screen.getByText(/preserved:/i)).toBeDefined()

  const deleteBtn = screen.getByRole('button', { name: /^delete data$/i })
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)
  await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(deleteBtn)

  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/admin/reset-logs?service_id=test-svc',
      { confirm: 'test-svc' },
    )
  })
})

test('DeleteDataDialog blocks the Cancel/Close path while streaming and only offers Stop', async () => {
  sseState.status = 'idle'
  const user = userEvent.setup()
  const onOpenChange = vi.fn()

  const qc = createTestQueryClient()
  const { rerender } = render(
    <DeleteDataDialog
      service={{ service_id: 'test-svc', name: 'Test Service', access_level: 'read_write' } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
    />,
    { wrapper: makeQueryWrapper(qc) },
  )

  await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
  await user.click(screen.getByRole('button', { name: /^delete data$/i }))
  sseState.status = 'streaming'
  rerender(
    <DeleteDataDialog
      service={{ service_id: 'test-svc', name: 'Test Service', access_level: 'read_write' } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
    />,
  )

  expect(screen.queryByRole('button', { name: /cancel/i })).toBeNull()
  const stopBtn = screen.getByRole('button', { name: /^stop$/i })
  await user.click(stopBtn)
  expect(mockStop).toHaveBeenCalled()
})

test('TeardownDialog starts the SSE stream when the user enters a token and clicks Execute Teardown', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  render(
    <TeardownDialog
      service={{ service_id: 'test-svc', name: 'Test Service', rum_enabled: true } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
    />,
  )

  expect(screen.getByText('Teardown: Test Service')).toBeDefined()

  const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
  expect((teardownBtn as HTMLButtonElement).disabled).toBe(true)

  const tokenInput = screen.getByLabelText(/fastly token with the/i)
  await user.type(tokenInput, 'test-admin-token-value')

  expect((teardownBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(teardownBtn)

  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/teardown',
      {
        service_id: 'test-svc',
        remove_logging: true,
        remove_rum: true,
        remove_cdn: true,
        remove_bucket: true,
        remove_cloud_files: true,
        remove_cache: true,
        token: 'test-admin-token-value',
      },
    )
  })
})

test('TeardownDialog allows analyst (cache-only) teardown without a token', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  render(
    <TeardownDialog
      service={{ service_id: 'test-svc', name: 'Test Service', access_level: 'read_only' } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
    />,
  )

  const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
  expect((teardownBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(teardownBtn)

  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/teardown',
      {
        service_id: 'test-svc',
        remove_logging: false,
        remove_rum: false,
        remove_cdn: false,
        remove_bucket: false,
        remove_cache: true,
      },
    )
  })
})

describe('SSEProgressView error link', () => {
  it('renders a clickable Fastly link when the error event carries one', () => {
    const link = 'https://manage.fastly.com/products/compute'
    const lines: SSELine[] = [
      { type: 'status', message: 'Enabling…', _id: 1 },
      { type: 'error', message: 'Compute isn’t enabled', link, _id: 2 },
    ]
    render(
      <SSEProgressView lines={lines} status="error" error="Compute isn’t enabled" />,
    )

    const anchor = screen.getByRole('link', { name: /open fastly to enable it/i })
    expect(anchor).toHaveAttribute('href', link)
    expect(anchor).toHaveAttribute('target', '_blank')
    expect(screen.getByText(/Compute isn’t enabled/)).toBeInTheDocument()
  })

  it('renders no link for a plain error event', () => {
    const lines: SSELine[] = [{ type: 'error', message: 'Something broke', _id: 1 }]
    render(<SSEProgressView lines={lines} status="error" error="Something broke" />)

    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByText(/Something broke/)).toBeInTheDocument()
  })
})
