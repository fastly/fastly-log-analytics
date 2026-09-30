import { QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, test, vi } from 'vitest'

import { QuarantineSection } from '@/app/admin/_sections/QuarantineSection'
import { useServiceStore } from '@/stores/serviceStore'
import { createTestQueryClient } from '../../helpers/query'

const { adminFetchMock } = vi.hoisted(() => ({ adminFetchMock: vi.fn() }))

vi.mock('@/lib/api', () => ({ adminFetch: adminFetchMock }))

const evidence = {
  id: 17,
  source_type: 'request',
  original_key: 'raw/request/example.log.gz',
  line_ordinal: 9,
  byte_offset: 128,
  byte_length: 4,
  error_category: 'invalid_json',
  error_text: 'Unexpected end of JSON input',
  sha256: 'a'.repeat(64),
  quarantined_at: '2026-09-29T12:00:00Z',
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
}

function renderSection() {
  const queryClient = createTestQueryClient({ queries: { retry: false } })
  return render(
    <QueryClientProvider client={queryClient}>
      <QuarantineSection />
    </QueryClientProvider>,
  )
}

describe('QuarantineSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useServiceStore.setState({ activeServiceId: 'test-service', isInitialized: true } as never)
    adminFetchMock.mockImplementation(async (input: string) => {
      if (input.includes('/summary')) {
        return jsonResponse({
          total_items: 1,
          total_bytes: 4,
          request_items: 1,
          rum_items: 0,
          oldest_at: evidence.quarantined_at,
          newest_at: evidence.quarantined_at,
          category_counts: { invalid_json: 1 },
        })
      }
      if (input.includes('/download/')) return new Response(new Uint8Array([0x00, 0xff, 0x7b, 0x7d]))
      if (input.includes('/purge')) return jsonResponse({ purged: 1 })
      return jsonResponse({ items: [evidence], total: 1 })
    })
  })

  test('shows evidence metadata and downloads the selected exact-byte item endpoint', async () => {
    const user = userEvent.setup()
    const createObjectURL = vi.fn(() => 'blob:quarantine-test')
    vi.stubGlobal('URL', { ...URL, createObjectURL, revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    renderSection()

    await user.click(await screen.findByRole('button', { name: 'Details' }))
    expect(await screen.findByText('raw/request/example.log.gz')).toBeInTheDocument()
    expect(screen.getByText('Unexpected end of JSON input')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Download' }))
    await waitFor(() => expect(adminFetchMock).toHaveBeenCalledWith('/api/admin/quarantine/download/17'))
    expect(createObjectURL).toHaveBeenCalledOnce()
    vi.unstubAllGlobals()
  })

  test('reports quarantine query failures with a retry action', async () => {
    adminFetchMock.mockRejectedValueOnce(new Error('metadata unavailable'))
    renderSection()

    expect(await screen.findByText('Quarantine unavailable')).toBeInTheDocument()
    expect(screen.getByText('metadata unavailable')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })

  test('purges an individual item through the item-scoped endpoint', async () => {
    const user = userEvent.setup()
    renderSection()

    await user.click(await screen.findByRole('button', { name: 'Details' }))
    await user.click(await screen.findByRole('button', { name: 'Remove' }))

    await waitFor(() => {
      expect(adminFetchMock).toHaveBeenCalledWith('/api/admin/quarantine/purge?item_id=17', { method: 'POST' })
    })
  })
})
