import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { expect, test, vi, beforeEach } from 'vitest'
import { DeleteDataDialog } from '@/components/DeleteDataDialog/DeleteDataDialog'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { server } from '../../tests/msw/server'
import React from 'react'

const API_BASE = getApiBase()

beforeEach(() => {
  // lib/api.ts's request middleware aborts every non-serviceless GET when
  // no service is active at all — set one (deliberately NOT the row being
  // deleted) so the cloud-stats fetch below proves the `?service_id=`
  // query override actually targets the row, not just whatever's active.
  useServiceStore.setState({ activeServiceId: 'some-other-active-service', isInitialized: true } as never)
})

const mockStart = vi.fn()
const mockStop = vi.fn()
// Mutable so the "streaming" test below can flip status without a second
// vi.mock factory — vi.mock is hoisted and can't read per-test state.
const sseState: { status: 'idle' | 'streaming' | 'done' | 'error' } = { status: 'idle' }

vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    get status() {
      return sseState.status
    },
    isDone: false,
    error: null,
    start: mockStart,
    stop: mockStop,
    reset: vi.fn()
  })
}))

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
      } as any}
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
      // Proves the row's id rides as ?service_id=, not whatever's active —
      // a real request would 400 server-side if this landed on the wrong
      // service (get_source's query-param-over-header precedence).
      if (url.searchParams.get('service_id') !== 'test-svc') {
        return HttpResponse.json({ error: 'wrong service targeted' }, { status: 400 })
      }
      return HttpResponse.json({ data_files: 42, size_bytes: 123_456_789, snapshots: 3 })
    }),
  )

  renderDialog()

  expect(screen.getByText('Delete Data: Test Service')).toBeDefined()

  // Local stats come straight off the service prop (no extra fetch needed).
  expect(await screen.findByText('12 files')).toBeInTheDocument()

  // Cloud stats are fetched scoped to this row's service_id.
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
  // Type-to-confirm gate (GitHub-repo-delete style): disabled until the
  // service name is typed exactly.
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)
  await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
  expect((deleteBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(deleteBtn)

  // service_id travels as a `?service_id=` query param (not the JSON body) —
  // this table lists every service, and backend.deps.get_source resolves
  // the target from the query param/header, not the body.
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
      service={{ service_id: 'test-svc', name: 'Test Service', access_level: 'read_write' } as any}
      open={true}
      onOpenChange={onOpenChange}
    />,
    { wrapper: makeQueryWrapper(qc) },
  )

  await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
  await user.click(screen.getByRole('button', { name: /^delete data$/i }))
  sseState.status = 'streaming'
  // Same mounted instance (rerender, not a second render) — isExecuting is
  // internal state set by the click above; a fresh render() would reset it.
  rerender(
    <DeleteDataDialog
      service={{ service_id: 'test-svc', name: 'Test Service', access_level: 'read_write' } as any}
      open={true}
      onOpenChange={onOpenChange}
    />,
  )

  expect(screen.queryByRole('button', { name: /cancel/i })).toBeNull()
  const stopBtn = screen.getByRole('button', { name: /^stop$/i })
  await user.click(stopBtn)
  expect(mockStop).toHaveBeenCalled()
})
