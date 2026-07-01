import { render, screen, waitFor } from '@testing-library/react'
import { expect, test, vi, beforeEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../../helpers/query'
import React from 'react'

// UX-5: SessionDetail destructured only { data, isLoading } from useQuery, so a
// 5xx on /api/sessions/detail rendered the DataTable's default "No results." —
// indistinguishable from an empty session. The fix surfaces an inline
// error + Retry above the table when isError.

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockRejectedValue(new Error('detail boom')), use: vi.fn() },
  extractApiError: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}))
vi.mock('@/components/DataTable', () => ({ DataTable: () => <div data-testid="data-table" /> }))
vi.mock('@/components/SessionScoring/FlagSessionPopover', () => ({ FlagSessionPopover: () => null }))
vi.mock('@/hooks/useDateFormat', () => ({
  useDateFormat: () => ({ full: (s: string) => s, relative: (s: string) => s, abbr: () => 'UTC' }),
}))
vi.mock('@/hooks/useFieldLabel', () => ({ useFieldLabel: () => (s: string) => s }))

import { SessionDetail } from '@/app/sessions/_sections/SessionDetail'

const selectedSession = {
  ip: '1.2.3.4',
  session_start: '2026-01-01T00:00:00Z',
  session_end: '2026-01-01T01:00:00Z',
} as never

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
beforeEach(() => queryClient.clear())

// `retry: false` (createTestQueryClient) makes the error immediate, so solo this
// settles in ~255ms. Under the full `make ci` vitest run (155 files in parallel
// saturating the box) the base-ui Dialog mount alone took ~2.7s, blowing
// waitFor's default 1s budget before the post-error render flushed — a CPU-
// starvation flake, not a logic bug. Generous waitFor + test timeouts absorb the
// load without masking a real regression (the alert still must appear).
test('UX-5: shows an inline error + Retry (not a bare empty table) when the detail query 5xxs', async () => {
  render(
    <QueryClientProvider client={queryClient}>
      <SessionDetail
        selectedSession={selectedSession}
        setSelectedSession={() => {}}
        activeServiceId="test-svc"
        data={undefined}
        labels={[]}
        labelBySid={new Map()}
        onFlagged={() => {}}
      />
    </QueryClientProvider>,
  )
  await waitFor(
    () => {
      expect(screen.getByRole('alert')).toHaveTextContent(/failed to load session detail/i)
    },
    { timeout: 10000 },
  )
  expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
}, 15000)
