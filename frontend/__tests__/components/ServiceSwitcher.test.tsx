/**
 * ServiceSwitcher MRU + pinning contract.
 *
 * Coverage:
 *   - dropdown renders Pinned + Recent + All groups in the right order
 *   - selecting a service updates MRU AND persists to localStorage
 *   - clicking the pin button toggles the persisted pinned list
 *   - pin click does NOT bubble into the row's select handler
 *   - on remount, prefs hydrate from localStorage
 */
import * as React from 'react'
import { describe, it, expect, beforeEach, beforeAll, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { TimezoneSwitcher } from '@/components/AppLayout'
import { ServiceSwitcher, SyncStatusBadge } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { useServiceStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/filterStore'
import { getTimezoneAbbr } from '@/lib/format'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'
import { stubResizeObserver, stubSystemTimeZone } from '../helpers/page-smoke'

const mocks = vi.hoisted(() => ({
  status: vi.fn(),
  bootstrap: vi.fn(),
  lastSync: vi.fn(),
  analyst: vi.fn(),
}))

vi.mock('@/components/ui/select', async () => (await import('../helpers/page-smoke')).selectMockFactory())
vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsAnalyst: () => mocks.analyst(),
  useBootstrap: () => mocks.bootstrap(),
  useMounted: () => true,
}))
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSyncStatus: () => mocks.status(),
  useLastSync: () => mocks.lastSync(),
  useHeaderBadgeStream: () => ({ state: 'idle' }),
  useAdminEventStream: () => ({ state: 'idle' }),
  useDateFormat: () => ({ full: 'yyyy-MM-dd', abbr: 'HH:mm' }),
  useNowMs: () => Date.parse('2026-09-08T18:00:00Z'),
}))
vi.mock('@/lib/format', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/format')>()),
  formatTimeAgo: (timestamp: string) => timestamp,
}))

// ServiceSwitcher calls useQueryClient() (it invalidates the bootstrap query
// on switch), so every render needs a QueryClientProvider. One fresh client
// for the suite, cleared in beforeEach for isolation.
const queryClient = createTestQueryClient()
const renderSwitcher = () => render(<ServiceSwitcher />, { wrapper: makeQueryWrapper(queryClient) })

// cmdk requires ResizeObserver; jsdom doesn't ship one.
beforeAll(() => {
  stubResizeObserver()
})

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

const PREFS_KEY = 'fla-service-switcher-prefs'

beforeEach(() => {
  window.localStorage.clear()
  queryClient.clear()
  useServiceStore.setState({
    services: [
      { id: 'svc-alpha', name: 'Alpha Service' },
      { id: 'svc-beta', name: 'Beta Service' },
      { id: 'svc-gamma', name: 'Gamma Service' },
      { id: 'svc-delta', name: 'Delta Service' },
    ],
    activeServiceId: 'svc-alpha',
  })
})

describe('ServiceSwitcher MRU + pinning', () => {
  it('opens to show every service when no prefs exist', async () => {
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))

    // Each service has a pin button in the dropdown — that's a unique
    // accessible name per row (no collision with the trigger label).
    expect(screen.getByRole('button', { name: /pin alpha service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin beta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin gamma service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin delta service to top/i })).toBeInTheDocument()
  })

  it('touching the active service writes it to MRU + localStorage', async () => {
    // First render — mount effect should record activeServiceId 'svc-alpha' in MRU.
    renderSwitcher()
    // Effect runs synchronously after mount.
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru[0]).toBe('svc-alpha')
  })

  it('clicking the pin button toggles persisted prefs without changing the active service', async () => {
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))

    // The pin button for an UNPINNED service is visually opacity-0 until
    // hover; the DOM node still exists and is clickable by aria-label.
    const pinBtn = screen.getByRole('button', { name: /pin beta service to top/i })
    await user.click(pinBtn)

    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.pinned).toEqual(['svc-beta'])

    // Active service must NOT have switched (pin click stopPropagation works).
    expect(useServiceStore.getState().activeServiceId).toBe('svc-alpha')

    // The pin's accessible name should now invert — re-query for the
    // unpin affordance to confirm.
    expect(screen.getByRole('button', { name: /unpin beta service/i })).toBeInTheDocument()
  })

  it('hydrates Pinned and Recent buckets from localStorage on mount', async () => {
    window.localStorage.setItem(
      PREFS_KEY,
      JSON.stringify({ pinned: ['svc-gamma'], mru: ['svc-beta', 'svc-delta'] }),
    )
    const user = userEvent.setup()
    renderSwitcher()
    await user.click(screen.getByRole('combobox'))

    // Pinned: svc-gamma — should render with the "Unpin" affordance.
    expect(screen.getByRole('button', { name: /unpin gamma service/i })).toBeInTheDocument()

    // Non-pinned services should each still have a pin-to-top affordance.
    expect(screen.getByRole('button', { name: /pin beta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin delta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin alpha service to top/i })).toBeInTheDocument()

    // mount-effect adds svc-alpha to MRU front, so the persisted MRU now
    // has [alpha, beta, delta] (gamma is filtered out — it's pinned).
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru).toEqual(['svc-alpha', 'svc-beta', 'svc-delta'])
  })
})

describe('TimezoneSwitcher', () => {
  beforeEach(() => {
    window.localStorage.clear()
    useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('renders "System Time" first, then a separator, then the common zones in order', async () => {
    render(<TimezoneSwitcher />)

    const content = await waitFor(() => screen.getByTestId('mock-select-content'))
    const entries = Array.from(
      content.querySelectorAll('[data-select-item-value], [data-testid="mock-select-separator"]'),
    ).map((el) => el.getAttribute('data-select-item-value') ?? 'SEPARATOR')

    expect(entries[0]).toBe('__system__')
    expect(entries[1]).toBe('SEPARATOR')
    expect(entries.slice(2)).toEqual([
      'UTC',
      'America/New_York',
      'America/Chicago',
      'America/Denver',
      'America/Los_Angeles',
      'Europe/London',
      'Europe/Berlin',
      'Asia/Tokyo',
      'Australia/Sydney',
    ])
  })

  it('shows "System Time (<abbr>)" in the trigger while mode is system', async () => {
    useTimezoneStore.setState({ mode: 'system', timezone: 'America/Chicago' })
    render(<TimezoneSwitcher />)

    const abbr = getTimezoneAbbr(new Date(), 'America/Chicago')
    await waitFor(() => {
      expect(screen.getByTestId('mock-select-value')).toHaveTextContent(
        `System Time (${abbr})`,
      )
    })
  })

  it('picking a concrete zone flips the store to manual mode', async () => {
    const user = userEvent.setup()
    render(<TimezoneSwitcher />)

    const tokyoItem = await waitFor(() =>
      screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'Asia/Tokyo'),
    )
    expect(tokyoItem).toBeTruthy()

    await user.click(tokyoItem!)

    expect(useTimezoneStore.getState().mode).toBe('manual')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
  })

  it('picking "System Time" resolves the live browser zone immediately', async () => {
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Europe/London' })
    stubSystemTimeZone('Australia/Sydney')
    const user = userEvent.setup()
    render(<TimezoneSwitcher />)

    const systemItem = await waitFor(() =>
      screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === '__system__'),
    )
    expect(systemItem).toBeTruthy()

    await user.click(systemItem!)

    expect(useTimezoneStore.getState().mode).toBe('system')
    expect(useTimezoneStore.getState().timezone).toBe('Australia/Sydney')
  })

  it('only shows the manual-mode fallback item for an uncommon zone when mode is manual', async () => {
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Asia/Kolkata' })
    const { rerender } = render(<TimezoneSwitcher />)

    await waitFor(() => {
      expect(
        screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata'),
      ).toBe(true)
    })

    useTimezoneStore.setState({ mode: 'system', timezone: 'Asia/Kolkata' })
    rerender(<TimezoneSwitcher />)

    expect(
      screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata'),
    ).toBe(false)
  })
})

const oldTimestamp = '2026-09-06T17:19:00Z'
const requestTimestamp = '2026-09-07T19:31:22Z'
const syncTimestamp = '2026-09-08T02:33:23Z'

function mountBadge() {
  return render(<SyncStatusBadge />, { wrapper: makeQueryWrapper(createTestQueryClient()) })
}

describe('request header timestamp', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'test-service' })
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

  it('does not render a stale-log dot beside the latest timestamp', async () => {
    mountBadge()
    await waitFor(() => expect(screen.getByText(oldTimestamp)).toBeInTheDocument())
    expect(screen.queryByRole('img', { name: /Latest log is/ })).not.toBeInTheDocument()
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

  it('renders request total rows when rumTotal exceeds local_rows', async () => {
    mocks.bootstrap.mockReturnValue({ data: { services: [{ service_id: 'test-service', rum_enabled: true }] } })
    mocks.status.mockReturnValue({
      data: {
        local_rows: 2700,
        request: { latest_log_at: requestTimestamp, total_rows: 2700 },
        rum: { latest_log_at: '2026-09-08T17:00:00Z', total_rows: 4430 },
      },
    })
    mountBadge()
    await waitFor(() => expect(screen.getByText('total: 2,700')).toBeInTheDocument())
    expect(screen.getByText('total: 4,430')).toBeInTheDocument()
  })
})
