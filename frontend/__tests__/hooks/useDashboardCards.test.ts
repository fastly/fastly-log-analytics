/**
 * Tests for `useDashboardCards`, `useFilterUrlWriteback`, `useFilterPayload`, and `useTimeRange`.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  useFilterUrlWriteback,
  useFilterPayload,
  useTimeRange,
} from '@/hooks/useDashboardCards'
import { useFilterStore } from '@/stores/filterStore'

let currentPathname = '/dashboard'

vi.mock('next/navigation', () => ({
  usePathname: () => currentPathname,
}))

const useBootstrap = vi.fn()
const useLogFieldsCatalog = vi.fn()

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useBootstrap: () => useBootstrap(),
  useLogFieldsCatalog: () => useLogFieldsCatalog(),
}))
vi.mock('@/app/dashboard/_sections/CardGrid', () => ({
  CATEGORIZED_CARD_IDS: new Set([
    'status', 'method', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    'rid', 'prid', 'waf_sig', 'waf_req_id', 'ottfb', 'tcp_rtt'
  ]),
  CARD_CATEGORIES: [],
}))

function loadHook() {
  return import('@/hooks/useDashboardCards')
}

function resetStore() {
  act(() => {
    useFilterStore.setState({
      filters: [],
      edgeOnly: false,
      startTime: '2026-06-09T20:00:00.000Z',
      endTime: '2026-06-10T20:00:00.000Z',
      isAutoRange: true,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
    })
  })
}

beforeEach(() => {
  currentPathname = '/dashboard'
  window.history.replaceState({}, '', '/dashboard')
  resetStore()
})

describe('useDashboardCards', () => {
  beforeEach(() => {
    useBootstrap.mockReset()
    useLogFieldsCatalog.mockReset()
  })

  it('returns [] until both catalog and bootstrap have data', async () => {
    useBootstrap.mockReturnValue({ data: undefined })
    useLogFieldsCatalog.mockReturnValue({ data: undefined })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current).toEqual([])
  })

  it('excludes METRICS and INTERNAL groups', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: [] } })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'status', label: 'Status', group: 'EDGE' },
          { id: 'm1', label: 'Metric', group: 'METRICS' },
          { id: 'i1', label: 'Internal', group: 'INTERNAL' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['status'])
  })

  it('excludes the hard-coded id list (timestamp, elapsed, req_bytes…)', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: [] } })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'timestamp', label: 'T', group: 'EDGE' },
          { id: 'elapsed', label: 'E', group: 'EDGE' },
          { id: 'req_bytes', label: 'RB', group: 'EDGE' },
          { id: 'status', label: 'S', group: 'EDGE' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['status'])
  })

  it('marks all standard cards inActiveFormat=true when active_log_field_ids is empty', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: [] } })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'status', label: 'S', group: 'EDGE' },
          { id: 'method', label: 'M', group: 'EDGE' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.every(c => c.inActiveFormat)).toBe(true)
  })

  it('honours active_log_field_ids when non-empty', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['status'] } })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'status', label: 'S', group: 'EDGE' },
          { id: 'method', label: 'M', group: 'EDGE' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    const byId = Object.fromEntries(result.current.map(c => [c.id, c.inActiveFormat]))
    expect(byId).toEqual({ status: true, method: false })
  })

  it('force-shows supported VIRTUAL fields even when not in active_log_field_ids', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['status'], ngwaf_configured: true } })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'status', label: 'S', group: 'EDGE' },
          { id: '_bot_name', label: 'Bot', group: 'EDGE' },
          { id: '_ngwaf_bot_name', label: 'NgBot', group: 'EDGE' },
          { id: 'waf_sig_ind', label: 'Sig', group: 'EDGE' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    const byId = Object.fromEntries(result.current.map(c => [c.id, c.inActiveFormat]))
    expect(byId._bot_name).toBe(true)
    expect(byId._ngwaf_bot_name).toBe(true)
    expect(byId.waf_sig_ind).toBe(true)
  })

  it.each([false, undefined])('excludes NGWAF cards when attachment is %s, even with WAF fields', async (configured) => {
    useBootstrap.mockReturnValue({
      data: {
        ngwaf_configured: configured,
        active_log_field_ids: ['status', 'waf_req_id', 'waf_sig'],
        custom_dashboard_cards: [{ id: '_ngwaf_bot_name', label: 'Stale card' }],
      },
    })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: '_bot_name', label: 'Fastly Bots', group: 'VIRTUAL' },
          { id: '_ngwaf_bot_name', label: 'NGWAF Verified Bots', group: 'VIRTUAL' },
          { id: 'waf_sig_ind', label: 'NGWAF Signals', group: 'VIRTUAL' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result, rerender } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['_bot_name'])

    useBootstrap.mockReturnValue({ data: { ngwaf_configured: true, active_log_field_ids: [] } })
    rerender()
    expect(result.current.map(c => c.id)).toEqual(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'])

    useBootstrap.mockReturnValue({ data: { ngwaf_configured: false, active_log_field_ids: [] } })
    rerender()
    expect(result.current.map(c => c.id)).toEqual(['_bot_name'])
  })

  it('force-hides noisy IDs (rid, prid, waf_sig, waf_req_id) even when active', async () => {
    useBootstrap.mockReturnValue({
      data: { active_log_field_ids: ['rid', 'prid', 'waf_sig', 'waf_req_id', 'status'] },
    })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'rid', label: 'RID', group: 'EDGE' },
          { id: 'prid', label: 'PRID', group: 'EDGE' },
          { id: 'waf_sig', label: 'Sig', group: 'EDGE' },
          { id: 'waf_req_id', label: 'WafR', group: 'EDGE' },
          { id: 'status', label: 'S', group: 'EDGE' },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    const byId = Object.fromEntries(result.current.map(c => [c.id, c.inActiveFormat]))
    expect(byId.rid).toBeUndefined()
    expect(byId.prid).toBeUndefined()
    expect(byId.waf_sig).toBeUndefined()
    expect(byId.waf_req_id).toBeUndefined()
    expect(byId.status).toBe(true)
  })

  it('appends custom_dashboard_cards and dedupes by id', async () => {
    useBootstrap.mockReturnValue({
      data: {
        active_log_field_ids: [],
        custom_dashboard_cards: [
          { id: 'cf-1', label: 'Custom 1' },
          { id: 'status', label: 'dup-of-standard' },
        ],
      },
    })
    useLogFieldsCatalog.mockReturnValue({
      data: { fields: [{ id: 'status', label: 'S', group: 'EDGE' }] },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['status', 'cf-1'])
  })

  it('filters out system custom fields, uncategorized built-in fields, non-dashboard user custom fields, and non-top-N metric fields', async () => {
    useBootstrap.mockReturnValue({
      data: {
        active_log_field_ids: [],
        custom_dashboard_cards: [],
      },
    })
    useLogFieldsCatalog.mockReturnValue({
      data: {
        fields: [
          { id: 'status', label: 'S', group: 'EDGE' },
          { id: 'ottfb', label: 'Origin TTFB', group: 'L' },
          { id: 'tcp_rtt', label: 'TCP RTT', group: 'F' },
          { id: 'io_input_bytes', label: 'IO Input', group: 'M' },
          { id: 'edge_score', label: 'Edge Score', group: 'custom', is_custom: true },
          { id: 'cmcd_version', label: 'CMCD Version', group: 'custom', is_custom: true },
          { id: 'rum_cid', label: 'RUM Session ID', group: 'custom', is_custom: true },
          { id: 'fastly_req_id', label: 'Fastly Req ID', group: 'custom', is_custom: true },
          { id: 'user_cf_1', label: 'My Custom Field', group: 'custom', is_custom: true, show_in_dashboard: true },
          { id: 'user_cf_2', label: 'Other Custom Field', group: 'custom', is_custom: true, show_in_dashboard: false },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['status', 'user_cf_1'])
  })
})

describe('useFilterUrlWriteback', () => {
  it('re-hydrates filterStore on client-side pathname change (ingress)', () => {
    const { rerender } = renderHook(() => useFilterUrlWriteback())

    const filters = {
      country: { values: ['CA'], mode: 'include' },
    }
    window.history.replaceState({}, '', `/dashboard?filters=${encodeURIComponent(JSON.stringify(filters))}`)

    currentPathname = '/security'
    act(() => {
      rerender()
    })

    const store = useFilterStore.getState()
    expect(store.filters).toHaveLength(1)
    expect(store.filters[0]).toMatchObject({
      column: 'country',
      value: 'CA',
      mode: 'include',
    })
  })

  it('does not write start_time/end_time on fresh load (isAutoRange=true)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().toggleEdgeOnly()
    })

    const params = new URLSearchParams(window.location.search)
    expect(params.has('start_time')).toBe(false)
    expect(params.has('end_time')).toBe(false)
  })

  it('writes start_time/end_time after user picks a range (isAutoRange=false)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().setRange(
        '2026-06-09T17:36:00.000Z',
        '2026-06-10T17:36:00.000Z',
      )
    })

    const params = new URLSearchParams(window.location.search)
    expect(params.get('start_time')).toBe('2026-06-09T17:36:00.000Z')
    expect(params.get('end_time')).toBe('2026-06-10T17:36:00.000Z')
  })

  it('removes start_time/end_time on Reset (user-picked range → defaults)', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().setRange(
        '2026-06-09T17:36:00.000Z',
        '2026-06-10T17:36:00.000Z',
      )
    })
    expect(new URLSearchParams(window.location.search).has('start_time')).toBe(true)

    act(() => {
      useFilterStore.getState().resetAll()
    })
    const params = new URLSearchParams(window.location.search)
    expect(params.has('start_time')).toBe(false)
    expect(params.has('end_time')).toBe(false)
    expect(params.has('filters')).toBe(false)
  })

  it('removes filters from URL when filter list is cleared', () => {
    renderHook(() => useFilterUrlWriteback())

    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    expect(new URLSearchParams(window.location.search).has('filters')).toBe(true)

    act(() => {
      useFilterStore.getState().resetAll()
    })
    expect(new URLSearchParams(window.location.search).has('filters')).toBe(false)
  })
})

describe('useFilterPayload', () => {
  it('returns an empty object when no filters are set', () => {
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current).toEqual({})
  })

  it('reflects current filter pills as a backend-shaped payload', () => {
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
      useFilterStore.getState().addFilter('status', '500', 'exclude')
    })
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current.country).toEqual({ mode: 'include', values: ['US'] })
    const excludeKey = Object.keys(result.current).find(k => result.current[k].mode === 'exclude')
    expect(excludeKey).toBeTruthy()
    expect(result.current[excludeKey!].values).toContain('500')
  })

  it('does NOT include edge filter by default, even when edgeOnly is on', () => {
    act(() => {
      useFilterStore.setState({ edgeOnly: true })
    })
    const { result } = renderHook(() => useFilterPayload())
    expect(result.current).not.toHaveProperty('edge')
  })

  it('includes edge filter when called with includeEdgeOnly=true AND edgeOnly is on', () => {
    act(() => {
      useFilterStore.setState({ edgeOnly: true })
    })
    const { result } = renderHook(() => useFilterPayload(true))
    expect(result.current).toHaveProperty('edge')
    expect(result.current.edge).toEqual({ mode: 'include', values: ['true'] })
  })

  it('does not include edge filter when includeEdgeOnly=true but edgeOnly is off', () => {
    const { result } = renderHook(() => useFilterPayload(true))
    expect(result.current).not.toHaveProperty('edge')
  })

  it('memoises across renders when filters and edgeOnly are unchanged', () => {
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    const { result, rerender } = renderHook(() => useFilterPayload())
    const first = result.current
    rerender()
    expect(result.current).toBe(first)
  })

  it('returns a new object when filters change', () => {
    const { result } = renderHook(() => useFilterPayload())
    const first = result.current
    act(() => {
      useFilterStore.getState().addFilter('country', 'US', 'include')
    })
    expect(result.current).not.toBe(first)
  })
})

describe('useTimeRange', () => {
  beforeEach(() => {
    act(() => {
      useFilterStore.setState({
        startTime: '2026-07-01T00:00:00Z',
        endTime: '2026-07-02T00:00:00Z',
        compareMode: false,
        compareStartTime: null,
        compareEndTime: null,
      })
    })
  })

  it('returns the current time window from filterStore', () => {
    const { result } = renderHook(() => useTimeRange())
    expect(result.current.startTime).toBe('2026-07-01T00:00:00Z')
    expect(result.current.endTime).toBe('2026-07-02T00:00:00Z')
    expect(result.current.compareMode).toBe(false)
    expect(result.current.compareStartTime).toBeNull()
    expect(result.current.compareEndTime).toBeNull()
  })

  it('reflects filterStore changes', () => {
    const { result } = renderHook(() => useTimeRange())

    act(() => {
      useFilterStore.setState({
        startTime: '2026-06-01T00:00:00Z',
        endTime: '2026-06-15T00:00:00Z',
        compareMode: true,
        compareStartTime: '2026-05-01T00:00:00Z',
        compareEndTime: '2026-05-15T00:00:00Z',
      })
    })

    expect(result.current.startTime).toBe('2026-06-01T00:00:00Z')
    expect(result.current.compareMode).toBe(true)
    expect(result.current.compareStartTime).toBe('2026-05-01T00:00:00Z')
  })
})
