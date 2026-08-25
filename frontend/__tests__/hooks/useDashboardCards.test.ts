/**
 * useDashboardCards reduces (catalog × bootstrap.active_log_field_ids ×
 * custom_dashboard_cards) into the ordered card list the dashboard
 * page iterates over. The reducer logic is non-trivial — exclude
 * METRICS/INTERNAL groups, force VIRTUAL fields visible, force noisy
 * IDs hidden, then append custom cards. Each rule below has a test.
 *
 * Both upstream hooks are mocked at the module boundary so we don't
 * have to spin up MSW + a query client for what is effectively a pure
 * reducer.
 *
 * @vitest-environment jsdom
 */
import { renderHook } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'

const useBootstrap = vi.fn()
const useLogFieldsCatalog = vi.fn()

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => useBootstrap(),
}))
vi.mock('@/hooks/useLogFieldsCatalog', () => ({
  useLogFieldsCatalog: () => useLogFieldsCatalog(),
}))
vi.mock('@/app/dashboard/_sections/categories', () => ({
  CATEGORIZED_CARD_IDS: new Set([
    'status', 'method', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    'rid', 'prid', 'waf_sig', 'waf_req_id', 'ottfb', 'tcp_rtt'
  ]),
  CARD_CATEGORIES: [],
}))

function loadHook() {
  return import('@/hooks/useDashboardCards')
}

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

  it('force-shows VIRTUAL fields even when not in active_log_field_ids', async () => {
    useBootstrap.mockReturnValue({ data: { active_log_field_ids: ['status'] } })
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
    // status (standard) appears once; cf-1 appended; duplicate dropped.
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
          // Categorized standard field: should be shown
          { id: 'status', label: 'S', group: 'EDGE' },
          // Categorized but non-top-N fields: should be hidden
          { id: 'ottfb', label: 'Origin TTFB', group: 'L' },
          { id: 'tcp_rtt', label: 'TCP RTT', group: 'F' },
          // Uncategorized standard field (e.g. io_input_bytes, not in mocked categories): should be hidden
          { id: 'io_input_bytes', label: 'IO Input', group: 'M' },
          // System custom fields: should be hidden
          { id: 'edge_score', label: 'Edge Score', group: 'custom', is_custom: true },
          { id: 'cmcd_version', label: 'CMCD Version', group: 'custom', is_custom: true },
          { id: 'rum_cid', label: 'RUM Session ID', group: 'custom', is_custom: true },
          { id: 'fastly_req_id', label: 'Fastly Req ID', group: 'custom', is_custom: true },
          // User custom field with show_in_dashboard=true: should be shown
          { id: 'user_cf_1', label: 'My Custom Field', group: 'custom', is_custom: true, show_in_dashboard: true },
          // User custom field with show_in_dashboard=false: should be hidden
          { id: 'user_cf_2', label: 'Other Custom Field', group: 'custom', is_custom: true, show_in_dashboard: false },
        ],
      },
    })
    const { useDashboardCards } = await loadHook()
    const { result } = renderHook(() => useDashboardCards())
    expect(result.current.map(c => c.id)).toEqual(['status', 'user_cf_1'])
  })
})
