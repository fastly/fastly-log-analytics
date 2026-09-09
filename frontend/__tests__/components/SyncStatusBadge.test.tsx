import { render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { SyncStatusBadge } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'

const mocks = vi.hoisted(() => ({
  status: vi.fn(),
  bootstrap: vi.fn(),
  lastSync: vi.fn(),
  analyst: vi.fn(),
}))

vi.mock('@/hooks/useSyncStatus', () => ({
  useSyncStatus: () => mocks.status(),
  useIsAnalyst: () => mocks.analyst(),
}))
vi.mock('@/hooks/useBootstrap', () => ({ useBootstrap: () => mocks.bootstrap() }))
vi.mock('@/hooks/useLastSync', () => ({ useLastSync: () => mocks.lastSync() }))
vi.mock('@/hooks/useHeaderBadgeStream', () => ({ useHeaderBadgeStream: () => ({ state: 'idle' }) }))
vi.mock('@/hooks/useAdminEventStream', () => ({ useAdminEventStream: () => ({ state: 'idle' }) }))
vi.mock('@/hooks/useDateFormat', () => ({ useDateFormat: () => ({ full: 'yyyy-MM-dd', abbr: 'HH:mm' }) }))
vi.mock('@/hooks/useNowSeconds', () => ({ useNowMs: () => Date.parse('2026-09-08T18:00:00Z') }))
vi.mock('@/hooks/useMounted', () => ({ useMounted: () => true }))
vi.mock('next/navigation', () => ({ usePathname: () => '/dashboard' }))
vi.mock('@/stores/serviceStore', () => ({
  useServiceStore: (selector: (state: { activeServiceId: string }) => unknown) =>
    selector({ activeServiceId: 'test-service' }),
}))
vi.mock('@/components/TimeAgo', () => ({
  TimeAgo: ({ timestamp }: { timestamp: string }) => <time dateTime={timestamp}>{timestamp}</time>,
}))

const oldTimestamp = '2026-09-06T17:19:00Z'
const requestTimestamp = '2026-09-07T19:31:22Z'
const syncTimestamp = '2026-09-08T02:33:23Z'

beforeEach(() => {
  mocks.status.mockReturnValue({ data: undefined })
  mocks.analyst.mockReturnValue(false)
  mocks.lastSync.mockReturnValue({ data: { started_at: syncTimestamp, status: 'success' } })
  mocks.bootstrap.mockReturnValue({
    data: {
      services: [{ service_id: 'test-service', rum_enabled: false }],
      header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: oldTimestamp } },
    },
  })
})

function mountBadge() {
  return render(<SyncStatusBadge />, { wrapper: makeQueryWrapper(createTestQueryClient()) })
}

describe('request header timestamp', () => {
  it.each([
    [oldTimestamp, requestTimestamp],
    [requestTimestamp, oldTimestamp],
  ])('uses the request extent rather than flat summary %s', async (flat, canonical) => {
    mocks.status.mockReturnValue({ data: { latest_log_at: flat, request: { latest_log_at: canonical } } })
    mountBadge()
    await waitFor(() => expect(screen.getByText(canonical)).toBeInTheDocument())
    expect(screen.queryByText(flat)).not.toBeInTheDocument()
    expect(screen.getByText(syncTimestamp)).toBeInTheDocument()
  })

  it('uses the analyst request extent from bootstrap', async () => {
    mocks.analyst.mockReturnValue(true)
    mocks.bootstrap.mockReturnValue({
      data: { header_badge: { latest_log_at: oldTimestamp, request: { latest_log_at: requestTimestamp } } },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
  })

  it('accepts an older canonical extent from a replacement snapshot', async () => {
    mocks.status.mockReturnValue({ data: { request: { latest_log_at: requestTimestamp } } })
    const { rerender } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    mocks.status.mockReturnValue({
      data: { latest_log_at: requestTimestamp, request: { latest_log_at: oldTimestamp } },
    })
    rerender(<SyncStatusBadge />)
    await waitFor(() => expect(screen.getByText(oldTimestamp)).toBeInTheDocument())
    expect(screen.queryByText(requestTimestamp)).not.toBeInTheDocument()
  })

  it.each([null, { latest_log_at: null }])('honors a cleared request metric %s after an update', async (request) => {
    mocks.status.mockReturnValue({ data: { request: { latest_log_at: requestTimestamp } } })
    const { rerender } = mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    mocks.status.mockReturnValue({ data: { latest_log_at: oldTimestamp, request } })
    rerender(<SyncStatusBadge />)
    await waitFor(() => expect(screen.getByText('Never')).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
    expect(screen.queryByText(requestTimestamp)).not.toBeInTheDocument()
  })

  it('does not resurrect bootstrap data when the current flat extent is cleared', async () => {
    mocks.status.mockReturnValue({ data: { latest_log_at: null } })
    mountBadge()
    await waitFor(() => expect(screen.getByText('Never')).toBeInTheDocument())
    expect(screen.queryByText(oldTimestamp)).not.toBeInTheDocument()
  })

  it('uses flat-only payloads when request metrics are absent', async () => {
    mocks.status.mockReturnValue({ data: { latest_log_at: requestTimestamp } })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
  })

  it('keeps REQUEST and RUM timestamps separate', async () => {
    const rumTimestamp = '2026-09-08T17:00:00Z'
    mocks.bootstrap.mockReturnValue({ data: { services: [{ service_id: 'test-service', rum_enabled: true }] } })
    mocks.status.mockReturnValue({
      data: {
        latest_log_at: rumTimestamp,
        request: { latest_log_at: requestTimestamp },
        rum: { latest_log_at: rumTimestamp },
      },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText(requestTimestamp)).toBeInTheDocument())
    expect(screen.getByText(rumTimestamp)).toBeInTheDocument()
  })
})
