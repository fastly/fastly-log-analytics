/**
 * Scoring tab tables (audit log, labels, matrix history) render through the
 * shared DataTableReadonly. Pins header semantics, row content, empty state
 * and the Labels edit popover (its state must survive typing — the cell is
 * rendered through flexRender, so an unstable column identity would remount
 * it on every keystroke).
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubPointerCaptureAndScroll } from '@/__tests__/helpers/jsdom-shims'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient } from '../../helpers/query'
import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { AuditLogTab } from '@/components/SessionScoring/AuditLogTab'
import { LabelsTab } from '@/components/SessionScoring/LabelsTab'
import { MatrixVersionsCard } from '@/components/SessionScoring/MatrixVersionsCard'

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/virtual-mock')).reactVirtualModuleMock())

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

const API_BASE = getApiBase()
const SVC = 'svc-tables'

function renderWithClient(ui: React.ReactElement) {
  return render(<QueryClientProvider client={createTestQueryClient()}>{ui}</QueryClientProvider>)
}

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: SVC, isInitialized: true })
})

describe('AuditLogTab', () => {
  it('renders column headers and rows', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () =>
        HttpResponse.json({
          audit: [
            { id: 1, timestamp: '2026-10-01T00:00:00Z', action: 'retrain', actor: 'admin', details: { a: 1 } },
            { id: 2, timestamp: '2026-10-01T01:00:00Z', action: 'enable', actor: null, details: null },
          ],
        }),
      ),
    )
    renderWithClient(<AuditLogTab serviceId={SVC} />)
    expect(await screen.findByText('retrain')).toBeTruthy()
    for (const name of ['Timestamp', 'Action', 'Actor', 'Details']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('{"a":1}')).toBeTruthy()
    expect(screen.getByText('enable')).toBeTruthy()
  })

  it('shows the empty message when there are no events', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/audit`, () => HttpResponse.json({ audit: [] })),
    )
    renderWithClient(<AuditLogTab serviceId={SVC} />)
    expect(await screen.findByText(/No audit events yet/)).toBeTruthy()
  })
})

describe('MatrixVersionsCard', () => {
  it('flags the current version and disables its Restore button', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/matrix-versions`, () =>
        HttpResponse.json({
          current_version: 'v2',
          versions: [
            { version: 'v2', key: 'k2', size_bytes: 2048, last_modified: '2026-10-02T00:00:00Z' },
            { version: 'v1', key: 'k1', size_bytes: 1024, last_modified: '2026-10-01T00:00:00Z' },
          ],
        }),
      ),
    )
    renderWithClient(<MatrixVersionsCard serviceId={SVC} />)
    expect(await screen.findByText('current')).toBeTruthy()
    const buttons = screen.getAllByRole('button', { name: /Restore/ })
    expect(buttons).toHaveLength(2)
    expect((buttons[0] as HTMLButtonElement).disabled).toBe(true)
    expect((buttons[1] as HTMLButtonElement).disabled).toBe(false)
  })
})

describe('LabelsTab', () => {
  it('keeps the edit popover state while typing notes', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () =>
        HttpResponse.json({
          labels: [
            {
              id: 'l1',
              sid: 'sid-abc',
              label: 'bad',
              notes: 'orig',
              sample_url: '/x',
              sample_ip: '1.2.3.4',
              updated_at: '2026-10-01T00:00:00Z',
            },
          ],
          counts: { good: 0, neutral: 0, bad: 1 },
        }),
      ),
    )
    const user = userEvent.setup()
    renderWithClient(<LabelsTab serviceId={SVC} />)
    expect(await screen.findByText('sid-abc')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Edit label' }))
    const notes = await screen.findByPlaceholderText('Notes (optional)')
    await user.clear(notes)
    await user.type(notes, 'hello')
    await waitFor(() =>
      expect((screen.getByPlaceholderText('Notes (optional)') as HTMLTextAreaElement).value).toBe('hello'),
    )
  })

  it('shows the empty message with no labels', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/labels`, () =>
        HttpResponse.json({ labels: [], counts: { good: 0, neutral: 0, bad: 0 } }),
      ),
    )
    renderWithClient(<LabelsTab serviceId={SVC} />)
    expect(await screen.findByText(/No labels yet/)).toBeTruthy()
  })
})
