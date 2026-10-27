/**
 * @vitest-environment jsdom
 */
import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { NextResponse, NextRequest } from 'next/server'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  downloadAsCsv, IP_FAMILY_FIELDS, isIpFamilyField, buildSessionDashboardUrl, buildStreamDetailUrl,
  isUserActive, msSinceLastInteraction, __resetForTests, ACTIVE_WINDOW_MS, showReadOnlyToast, showToast,
  resolveRangeWire, DEFAULT_RANGE_TOKEN, ANCHOR_QUANTUM_SECONDS, quantizeAnchor, buildTrafficData,
  formatBytes, statusBadgeClass, toUTCDate, formatTimeAgo, formatLocalDateTime, formatValue,
  calculateDelta, formatCount, formatCountMaybe, formatAbbreviatedCount, ELAPSED_AGE_COMPACT,
  ELAPSED_FRESHNESS, ELAPSED_RELATIVE_AGO, ELAPSED_RELATIVE_AGO_JUST_NOW, formatBytesTiered,
  formatElapsed, TIME_HOVER_LAYOUT, makeTimeXAxis, denseTimeGrid, denseBarTrace, lineTrace,
  secondaryYAxis, withSecondaryAxis, type BuildTrafficDataParams,
} from '@/lib/utils'
import { buildTrafficDataAsync } from '@/app/dashboard/_sections/DashboardClient'
import { formatPopGeo } from '@/components/DataTable'
import { shouldLoadRumScript } from '@/lib/ssr/upstream'
import { invalidInput } from '../helpers/page-smoke'
import { handlers } from '@/__tests__/helpers/msw/handlers'
import { proxy as middleware } from '../../proxy'
import { proxyUpstreamRequest } from '@/lib/api'
import { GET as apiGet, POST as apiPost } from '@/app/api/[...path]/route'
import { GET as jsGet } from '@/app/js/[...path]/route'

describe('formatBytes', () => {
  it.each([
    [0, '0 B'], [1, '1 B'], [500, '500 B'], [1024, '1 KB'], [1536, '1.5 KB'],
    [1024 * 1024, '1 MB'], [1024 * 1024 * 2.5, '2.5 MB'], [1024 ** 3, '1 GB'], [1024 ** 4, '1 TB'],
  ] as const)('formats %d -> %s', (n, want) => expect(formatBytes(n)).toBe(want))
})

describe('downloadAsCsv', () => {
  let capturedCsv = ''
  beforeEach(() => {
    capturedCsv = ''
    vi.stubGlobal('Blob', class MockBlob { constructor(parts: BlobPart[]) { capturedCsv = (parts as string[]).join('') } })
    vi.stubGlobal('URL', { createObjectURL: vi.fn(() => 'blob:mock'), revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLElement.prototype, 'click').mockImplementation(() => {})
  })
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })

  const buildCsv = (rows: Record<string, unknown>[], cols: string[]) => (downloadAsCsv(rows, cols, 'test.csv'), capturedCsv)

  it('formats headers, rows, quoting, escaping, nulls, missing keys, and download attribute accurately', () => {
    expect(buildCsv([], ['name', 'count']).split('\n')[0]).toBe('name,count')
    expect(buildCsv([{ name: 'alice', count: 5 }, { name: 'bob', count: 3 }], ['name', 'count']).split('\n').slice(1, 3)).toEqual(['alice,5', 'bob,3'])
    expect(buildCsv([{ url: '/a,b', n: 1 }], ['url', 'n'])).toContain('"/a,b"')
    expect(buildCsv([{ label: 'say "hello"', n: 1 }], ['label', 'n'])).toContain('"say ""hello"""')
    expect(buildCsv([{ text: 'line1\nline2', n: 1 }], ['text', 'n'])).toContain('"line1\nline2"')
    expect(buildCsv([{ url: null, count: 5 }], ['url', 'count']).split('\n')[1]).toBe(',5')
    expect(buildCsv([{ count: 5 }], ['url', 'count']).split('\n')[1]).toBe(',5')
    expect(buildCsv([{ a: 1 }, { a: 2 }, { a: 3 }], ['a']).split('\n')).toHaveLength(4)

    const appendSpy = vi.spyOn(document.body, 'appendChild')
    downloadAsCsv([], ['col'], 'my-export.csv')
    expect((appendSpy.mock.calls[0][0] as HTMLAnchorElement).download).toBe('my-export.csv')
  })
})

describe('isIpFamilyField, formatPopGeo & shouldLoadRumScript', () => {
  it('matches client-IP-family columns and rejects non-IP / origin IP columns', () => {
    for (const col of ['ip', 'client_ip', 'ip_address', 'remote_addr']) expect(isIpFamilyField(col)).toBe(true)
    for (const col of ['oip', 'country', 'asn', 'city', 'url', 'ua', '']) expect(isIpFamilyField(col)).toBe(false)
    expect([...IP_FAMILY_FIELDS].sort()).toEqual(['client_ip', 'ip', 'ip_address', 'remote_addr'])
  })

  it.each([
    [{ city: 'Denver', region: 'co', country: 'us' }, 'Denver, CO - USA'], [{ city: 'Ashburn (Metro)', region: 'va', country: 'us' }, 'Ashburn, VA - USA'],
    [{ city: 'London', region: '', country: 'uk' }, 'London - UK'], [{ city: 'Frankfurt', region: '', country: 'de' }, 'Frankfurt - DE'],
    [{ city: 'Hyderabad', region: 'hyderabad', country: 'in' }, 'Hyderabad - IN'], [{ city: 'Sao Paulo', region: 'saopaulo', country: 'br' }, 'Sao Paulo - BR'],
    [{ city: 'New York City', region: 'ny', country: 'us' }, 'New York City, NY - USA'], [undefined, ''], [null, ''],
    [{ city: '', region: 'ca', country: 'us' }, ''], [{ city: 'Palo Alto', region: '', country: '' }, 'Palo Alto'],
  ] as const)('formatPopGeo %j -> %s', (input, want) => expect(formatPopGeo(input)).toBe(want))

  it('loads RUM only for an analyst request through the real proxy domain when enabled', () => {
    expect([
      shouldLoadRumScript({ host: 'fastly-log-analytics.example.com', proxiedByCaddy: true, rumEnabled: true }),
      shouldLoadRumScript({ host: 'fastly-log-analytics.example.com', proxiedByCaddy: false, rumEnabled: true }),
      shouldLoadRumScript({ host: 'fastly-log-analytics.example.com', proxiedByCaddy: true, rumEnabled: false }),
      ...['localhost:13002', '127.0.0.1:13002', '[::1]:13002'].map((host) => shouldLoadRumScript({ host, proxiedByCaddy: true, rumEnabled: true })),
    ]).toEqual([true, false, false, false, false, false])
  })
})

describe('buildSessionDashboardUrl & buildStreamDetailUrl', () => {
  const START = '2026-05-10T13:06:52+00:00'
  const END = '2026-05-10T13:07:15+00:00'

  it('builds /dashboard and /sessions/stream URLs with encoded parameters', () => {
    const url = buildSessionDashboardUrl('my-service', 'ip', '1.2.3.4', START, END)
    expect(url.startsWith('/dashboard?')).toBe(true)
    const params = new URLSearchParams(url.split('?')[1])
    expect(Object.fromEntries(params)).toEqual({ start_time: START, end_time: END, service: 'my-service', filter_ip: '1.2.3.4' })
    expect(new URLSearchParams(buildSessionDashboardUrl('svc', 'ja4', 'abc123', START, END).split('?')[1]).get('filter_ja4')).toBe('abc123')
    expect(new URLSearchParams(buildSessionDashboardUrl('svc', 'ua', 'Mozilla/5.0', START, END).split('?')[1]).get('filter_ua')).toBe('Mozilla/5.0')

    expect(buildStreamDetailUrl('token123')).toBe('/sessions/stream?token=token123')
    expect(buildStreamDetailUrl('token with spaces & symbols')).toBe('/sessions/stream?token=token%20with%20spaces%20%26%20symbols')
    expect(buildStreamDetailUrl('token123', 'svc-abc')).toBe('/sessions/stream?token=token123&service=svc-abc')
    expect(buildStreamDetailUrl('token123', 'svc with space')).toBe('/sessions/stream?token=token123&service=svc%20with%20space')
  })
})

describe('statusBadgeClass', () => {
  const G = 'bg-green-500/10 text-green-500 border-green-500/20', R = 'bg-red-500/10 text-red-500 border-red-500/20', M = 'bg-muted text-muted-foreground'
  it.each([
    [200, undefined, 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [302, undefined, 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'],
    [404, undefined, 'bg-amber-500/15 text-amber-800 dark:text-amber-400 border-amber-500/20'],
    [503, undefined, 'bg-destructive/15 text-destructive border-destructive/20 font-bold'],
    ['200', undefined, 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [null, undefined, M], ['x', undefined, M], [200, 'traffic-light', G], [299, 'traffic-light', G],
    [301, 'traffic-light', 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'],
    [404, 'traffic-light', R], [503, 'traffic-light', R], [100, 'traffic-light', R],
    [0, 'traffic-light', M], [null, 'traffic-light', M], [undefined, 'traffic-light', M],
  ] as const)('status=%s variant=%s', (status, variant, want) => expect(statusBadgeClass(status, variant)).toBe(want))
})

describe('userActivity & invalidInput', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    __resetForTests(0)
  })
  afterEach(() => vi.useRealTimers())

  it('tracks keydown/pointerdown gestures, honors custom idle windows, and passes invalidInput through', () => {
    expect(isUserActive()).toBe(false)
    expect(msSinceLastInteraction()).toBeGreaterThan(ACTIVE_WINDOW_MS)
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    expect(isUserActive()).toBe(true)
    expect(msSinceLastInteraction()).toBeLessThan(1_000)
    vi.advanceTimersByTime(ACTIVE_WINDOW_MS + 1)
    expect(isUserActive()).toBe(false)
    window.dispatchEvent(new Event('pointerdown'))
    expect(isUserActive()).toBe(true)
    vi.advanceTimersByTime(20_000)
    expect(isUserActive(15_000)).toBe(false)
    expect(isUserActive(120_000)).toBe(true)

    const describeFilters = (filters: string[]): string => typeof filters
    const bad = { not: 'an array' }
    expect(invalidInput(bad)).toBe(bad)
    expect(describeFilters(invalidInput(bad))).toBe('object')
    expect(describeFilters(invalidInput('str'))).toBe('string')
  })
})

describe('showToast & showReadOnlyToast', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date('2026-06-12T00:00:00Z')) })
  afterEach(() => { vi.useRealTimers(); document.body.innerHTML = '' })

  it.each([
    [undefined, 'status', 'polite', 'hello world'], ['error', 'alert', 'assertive', 'database is on fire'],
    ['warn', 'alert', 'assertive', 'careful now'], ['success', 'status', 'polite', 'saved'],
  ] as const)('renders kind=%s with role=%s and aria-live=%s', (kind, role, live, msg) => {
    showToast(msg, kind)
    const toast = document.querySelector(`[role="region"][aria-label="Notifications"] [role="${role}"]`)
    expect([toast?.textContent, toast?.getAttribute('aria-live')]).toEqual([msg, live])
  })

  it('dedupes identical messages within 1.5s, auto-removes on timer or click, and handles SSR/readOnly', () => {
    showToast('parallel 403')
    showToast('parallel 403')
    expect(document.querySelectorAll('[role="status"], [role="alert"]')).toHaveLength(1)
    vi.advanceTimersByTime(2000)
    showToast('parallel 403')
    showToast('second')
    expect(document.querySelectorAll('[role="status"]')).toHaveLength(3)
    vi.advanceTimersByTime(4000)
    expect(document.querySelector('[role="region"]')).toBeNull()

    showToast('serious', 'error')
    vi.advanceTimersByTime(3500)
    expect(document.querySelectorAll('[role="alert"]')).toHaveLength(1)
    vi.advanceTimersByTime(2200)
    expect(document.querySelectorAll('[role="alert"]')).toHaveLength(0)

    showToast('short-lived', 'info', { durationMs: 100 })
    vi.advanceTimersByTime(300)
    expect(document.querySelectorAll('[role="status"]')).toHaveLength(0)

    showToast('one')
    vi.advanceTimersByTime(50)
    showToast('two')
    expect(document.querySelectorAll('[role="status"]')).toHaveLength(2)
    vi.advanceTimersByTime(3650)
    expect(document.querySelectorAll('[role="status"]')).toHaveLength(1)
    ;(document.querySelector('[role="status"]') as HTMLElement).dispatchEvent(new MouseEvent('click', { bubbles: true }))
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]')).toHaveLength(0)

    const originalDocument = globalThis.document
    vi.stubGlobal('document', undefined)
    try { expect(() => showToast('ssr-safe')).not.toThrow() } finally { vi.stubGlobal('document', originalDocument) }

    showReadOnlyToast()
    const roToast = document.querySelector('[role="alert"]')
    expect(roToast?.textContent).toContain('Read-only access')
    expect(roToast?.getAttribute('aria-live')).toBe('assertive')
  })
})

describe('toUTCDate, formatTimeAgo & formatLocalDateTime', () => {
  const now = new Date('2024-06-15T12:00:00Z')
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(now) })
  afterEach(() => vi.useRealTimers())

  it('parses UTC dates, formats relative time ago, and formats local date time', () => {
    expect(toUTCDate('2024-01-01T00:00:00Z').toISOString()).toBe('2024-01-01T00:00:00.000Z')
    expect(toUTCDate('2024-01-01 12:00:00').toISOString()).toBe('2024-01-01T12:00:00.000Z')
    expect(formatTimeAgo(new Date(now.getTime() - 5000))).toBe('5s ago')
    expect(formatTimeAgo(new Date(now.getTime() - 75000))).toBe('1m 15s ago')
    expect(formatTimeAgo(new Date(now.getTime() - 3600000 * 2 - 60000))).toBe('2h 1m 0s ago')

    const iso = '2026-07-01T12:34:56Z', ms = 1_750_000_000_000, d = new Date('2026-01-15T00:00:00Z')
    const opts: Intl.DateTimeFormatOptions = { dateStyle: 'short' }
    expect(formatLocalDateTime(iso)).toBe(new Date(iso).toLocaleString())
    expect(formatLocalDateTime(ms)).toBe(new Date(ms).toLocaleString())
    expect(formatLocalDateTime(d)).toBe(d.toLocaleString())
    expect(formatLocalDateTime('not-a-date')).toBe(new Date('not-a-date').toLocaleString())
    expect(formatLocalDateTime(iso, opts)).toBe(new Date(iso).toLocaleString(undefined, opts))
  })
})

const ANCHOR = '2026-06-29T12:00:00Z', START = '2026-06-10T00:00:00Z', END = '2026-06-12T00:00:00Z', NOW = new Date(ANCHOR)

describe('resolveRangeWire & quantizeAnchor', () => {
  it('resolves cold-load auto default, preset pills, custom ranges, and 60s anchor quantization', () => {
    const auto = resolveRangeWire({ relativeRange: null, isAutoRange: true, startTime: START, endTime: END, anchor: ANCHOR })
    expect(auto).toEqual({
      rangeToken: DEFAULT_RANGE_TOKEN, rangeKey: '24h',
      rangeBody: { range_token: '24h', anchor: ANCHOR, start_time: START, end_time: END },
    })
    for (const preset of ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '30d']) {
      expect(resolveRangeWire({ relativeRange: preset, isAutoRange: false, startTime: START, endTime: END, anchor: ANCHOR })).toEqual({
        rangeToken: preset, rangeKey: preset, rangeBody: { range_token: preset, anchor: ANCHOR, start_time: START, end_time: END },
      })
    }
    const customA = resolveRangeWire({ relativeRange: null, isAutoRange: false, startTime: START, endTime: END, anchor: ANCHOR })
    const customB = resolveRangeWire({ relativeRange: null, isAutoRange: false, startTime: '2026-05-01T00:00:00Z', endTime: '2026-05-02T00:00:00Z', anchor: ANCHOR })
    expect(customA.rangeToken).toBeNull()
    expect(customA.rangeBody).toEqual({ start_time: START, end_time: END })
    expect(customA.rangeKey).not.toBe(customB.rangeKey)
    expect(resolveRangeWire({ relativeRange: '7d', isAutoRange: true, startTime: START, endTime: END, anchor: ANCHOR }).rangeToken).toBe('7d')

    expect(ANCHOR_QUANTUM_SECONDS).toBe(60)
    for (const input of ['2026-06-29T12:00:59Z', '2026-06-29T12:00:00Z', undefined, 'not-a-date', '2026-06-29T12:00:30.123Z']) {
      expect(quantizeAnchor(input, NOW)).toBe('2026-06-29T12:00:00Z')
    }
    expect(quantizeAnchor('2026-06-29T12:00:01Z', NOW)).toBe(quantizeAnchor('2026-06-29T12:00:58Z', NOW))
  })
})

describe('formatCount, formatValue, calculateDelta, formatCountMaybe, formatAbbreviatedCount', () => {
  it('formats counts, deltas, abbreviated counts, and domain field values accurately', () => {
    for (const n of [0, 5, 42, 1000, 1234567, -9876, 1234.5, 0.125]) expect(formatCount(n)).toBe(n.toLocaleString())
    expect(formatCount(3.14159, { maximumFractionDigits: 1 })).toBe((3.14159).toLocaleString(undefined, { maximumFractionDigits: 1 }))
    expect([calculateDelta(110, 100), calculateDelta(90, 100), calculateDelta(100, 0), calculateDelta(100, undefined)]).toEqual([10, -10, null, null])
    expect([formatCountMaybe(undefined), formatCountMaybe(null), formatCountMaybe(1234567), formatCountMaybe(0)]).toEqual([undefined, undefined, formatCount(1234567), '0'])
    expect([formatAbbreviatedCount(999), formatAbbreviatedCount(1500), formatAbbreviatedCount(2_345_678), formatAbbreviatedCount(2_345_678, 2), formatAbbreviatedCount(3_210_000_000, 2)]).toEqual(['999', '1.5K', '2.3M', '2.35M', '3.21B'])

    for (const [col, val, want] of [
      ['any', null, 'null'], ['any', undefined, 'null'], ['resp_bytes', 1024, '1 KB'], ['req_header_bytes', 500, '500 B'],
      ['requests', 1000, '1,000'], ['country', 'US', 'United States'], ['country', 'GB', 'United Kingdom'],
      ['city', 'new york', 'New York'], ['pop', 'jfk', 'JFK'], ['region', 'ca', 'CA'],
      ['proto', 3, '3'], ['tls', 1.3, '1.3'], ['proto', '2.0', '2'], ['tls', '1.3', '1.3'], ['proto', 'h2', 'h2'], ['tls', 'TLSv1.3', 'TLSv1.3'],
    ] as const) {
      expect(formatValue(col, val)).toBe(want)
    }
    const displayNames = vi.spyOn(Intl, 'DisplayNames').mockImplementation(function () { throw new Error('boom') })
    try { expect(formatValue('country', 'US')).toBe('US') } finally { displayNames.mockRestore() }
  })
})

type FormatRow<I> = [I, string]
const METADATA_BYTES: FormatRow<number | null>[] = [
  [0, '0 B'], [1, '1 B'], [1023, '1023 B'], [1024, '1.0 KB'], [1025, '1.0 KB'], [1536, '1.5 KB'], [1048575, '1024.0 KB'],
  [1048576, '1.0 MB'], [5366336, '5.1 MB'], [1073741823, '1024.0 MB'], [1073741824, '1.00 GB'], [3271225472, '3.05 GB'],
  [5497558138880, '5120.00 GB'], [null, '–'],
]
const QUARANTINE_BYTES: FormatRow<number>[] = [
  [0, '0 B'], [1, '1 B'], [1023, '1023 B'], [1024, '1.0 KB'], [1025, '1.0 KB'], [1536, '1.5 KB'], [1048575, '1024.0 KB'],
  [1048576, '1.0 MB'], [5366336, '5.1 MB'], [1073741823, '1024.0 MB'], [1073741824, '1024.0 MB'], [3271225472, '3119.7 MB'],
  [5497558138880, '5242880.0 MB'],
]
const BOT_SOURCES_RELATIVE: FormatRow<number>[] = [
  [-5000, 'just now'], [0, 'just now'], [1000, 'just now'], [44400, 'just now'], [44500, 'just now'], [45000, 'just now'],
  [59000, 'just now'], [59500, 'just now'], [60000, 'just now'], [89000, 'just now'], [89400, 'just now'], [89500, 'just now'],
  [90000, 'just now'], [119000, 'just now'], [120000, '2m ago'], [121000, '2m ago'], [179000, '2m ago'], [3599000, '59m ago'],
  [3599600, '59m ago'], [3600000, '1h ago'], [5399000, '1h ago'], [5400000, '1h ago'], [7200000, '2h ago'], [86399000, '23h ago'],
  [86400000, '1d ago'], [90000000, '1d ago'], [172799000, '1d ago'], [172800000, '2d ago'], [259200000, '3d ago'], [400000000, '4d ago'],
]
const RESUME_BANNER_RELATIVE: FormatRow<number>[] = [
  [-5000, '-5s ago'], [0, '0s ago'], [1000, '1s ago'], [45000, '45s ago'], [59000, '59s ago'], [60000, '1m ago'],
  [89000, '1m ago'], [90000, '1m ago'], [119000, '1m ago'], [120000, '2m ago'], [121000, '2m ago'], [179000, '2m ago'],
  [3599000, '59m ago'], [3600000, '1h ago'], [5399000, '1h ago'], [5400000, '1h ago'], [7200000, '2h ago'], [86399000, '23h ago'],
  [86400000, '1d ago'], [90000000, '1d ago'], [172799000, '1d ago'], [172800000, '2d ago'], [259200000, '3d ago'], [400000000, '4d ago'],
]
const SYSTEM_HEALTH_AGE: FormatRow<number>[] = [
  [-5, '-5s'], [0, '0s'], [1, '1s'], [44.4, '44s'], [44.5, '45s'], [45, '45s'], [59, '59s'], [59.5, '60s'], [60, '60s'],
  [89, '89s'], [89.4, '89s'], [89.5, '90s'], [90, '2m'], [119, '2m'], [120, '2m'], [121, '2m'], [179, '3m'], [3599, '60m'],
  [3599.6, '60m'], [3600, '60m'], [5399, '90m'], [5400, '1.5h'], [7200, '2.0h'], [86399, '24.0h'], [86400, '24.0h'],
  [90000, '25.0h'], [172799, '48.0h'], [172800, '2.0d'], [259200, '3.0d'], [400000, '4.6d'],
]
const HIGH_SCALE_FRESHNESS: FormatRow<number>[] = [
  [-5, '-5s'], [0, '0s'], [1, '1s'], [44.4, '44s'], [44.5, '45s'], [45, '45s'], [59, '59s'], [59.5, '60s'], [60, '1m'],
  [89, '1m'], [89.4, '1m'], [89.5, '1m'], [90, '2m'], [119, '2m'], [120, '2m'], [121, '2m'], [179, '3m'], [3599, '60m'],
  [3599.6, '60m'], [3600, '1.0h'], [5399, '1.5h'], [5400, '1.5h'], [7200, '2.0h'], [86399, '24.0h'], [86400, '24.0h'],
  [90000, '25.0h'], [172799, '48.0h'], [172800, '48.0h'], [259200, '72.0h'], [400000, '111.1h'],
]

describe('formatBytesTiered & formatElapsed presets', () => {
  it.each(METADATA_BYTES)('metadata card (GB cap) %s -> %s', (n, want) => expect(formatBytesTiered(n, { maxUnit: 'GB' })).toBe(want))
  it('renders the metadata card placeholder for undefined', () => expect(formatBytesTiered(undefined, { maxUnit: 'GB' })).toBe('–'))
  it.each(QUARANTINE_BYTES)('quarantine (MB cap) %s -> %s', (n, want) => expect(formatBytesTiered(n)).toBe(want))
  it.each(BOT_SOURCES_RELATIVE)('bot sources %s ms -> %s', (ms, want) => expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO_JUST_NOW)).toBe(want))
  it.each(RESUME_BANNER_RELATIVE)('resume banner %s ms -> %s', (ms, want) => expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO)).toBe(want))
  it.each(SYSTEM_HEALTH_AGE)('system health age %s s -> %s', (s, want) => expect(formatElapsed(s, ELAPSED_AGE_COMPACT)).toBe(want))
  it.each(HIGH_SCALE_FRESHNESS)('high-scale freshness %s s -> %s', (s, want) => expect(formatElapsed(s, ELAPSED_FRESHNESS)).toBe(want))
})

describe('chart-helpers: TIME_HOVER_LAYOUT, makeTimeXAxis, denseTimeGrid, denseBarTrace, lineTrace, secondaryYAxis', () => {
  it('exposes TIME_HOVER_LAYOUT and builds makeTimeXAxis in supplied timezone', () => {
    expect(TIME_HOVER_LAYOUT).toMatchObject({
      hovermode: 'x unified',
      legend: expect.objectContaining({ orientation: 'h', xanchor: 'center', yanchor: 'bottom' }),
    })
    const utc = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'UTC')
    const ny = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'America/New_York')
    expect(utc).toMatchObject({ type: 'date', tickformatstops: expect.anything() })
    expect(utc.range).toHaveLength(2)
    expect(utc.range[0]).toContain('12:00:00')
    expect(ny.range[0]).toContain('07:00:00')
    expect(makeTimeXAxis(null, null, 'UTC').range).toEqual(['', ''])
    expect(makeTimeXAxis(undefined, undefined, 'UTC').range).toEqual(['', ''])
    expect(makeTimeXAxis(null, '2026-01-01T00:00:00Z', 'UTC').range[0]).toBe('')
  })

  it('fills denseTimeGrid across Z, +00:00, naive UTC, and per-minute intervals, and no-ops when invalid/contiguous', () => {
    const g1 = denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'], 3600)
    expect(g1?.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'), Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'), Date.parse('2026-06-30T12:00:00Z'),
    ])
    expect(denseTimeGrid(['2026-06-30T09:00:00+00:00', '2026-06-30T11:00:00+00:00'], 3600)).toHaveLength(3)
    expect(denseTimeGrid(['2026-06-30 09:00:00', '2026-06-30 11:00:00'], 3600)).toHaveLength(3)
    expect(denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T09:03:00Z'], 60)).toHaveLength(4)

    for (const [times, step] of [
      [['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'], undefined], [['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'], 0],
      [['2026-06-30T09:00:00Z'], 3600], [['2026-06-30T09:00:00Z', '2026-06-30T09:00:00Z'], 3600],
      [['2026-06-30T09:00:00Z', '2026-06-30T10:00:00Z', '2026-06-30T11:00:00Z'], 3600],
      [['2026-06-30T09:00:00Z', '2026-06-30T09:30:00Z', '2026-06-30T12:00:00Z'], 3600],
      [['2026-06-30T09:00:00Z', '2026-06-30T11:00:01Z'], 1],
    ] as const) expect(denseTimeGrid(times as unknown as string[], step)).toBeNull()
  })

  it('builds denseBarTrace, lineTrace, secondaryYAxis, and withSecondaryAxis', () => {
    type TimeRow = { t: string | null; v: number | null }
    const rows: TimeRow[] = [{ t: '2026-10-01T00:00:00+00:00', v: 4 }, { t: '2026-10-01T03:00:00+00:00', v: 9 }, { t: null, v: 7 }]
    const base = { rows, getTime: (r: TimeRow) => r.t, getValue: (r: TimeRow) => r.v, intervalSeconds: 3600, color: '#111' }

    const t1 = denseBarTrace({ ...base, name: 'n' })
    expect(t1.y).toEqual([4, 0, 0, 9])
    expect('yaxis' in t1).toBe(false)

    const grid = ['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z']
    const t2 = denseBarTrace({ ...base, name: 'n', grid, yaxis: 'y2', showlegend: true })
    expect(t2).toMatchObject({ x: grid, y: [4, 0], yaxis: 'y2', showlegend: true })
    expect(denseBarTrace({ ...base, name: 'n', intervalSeconds: 0 }).y).toEqual([4, 9])

    const lt = lineTrace({ name: 'l', x: ['a'], y: [1], color: '#222' })
    expect(lt).toMatchObject({ type: 'scatter', mode: 'lines', line: { color: '#222', width: 2 } })
    expect('yaxis' in lt).toBe(false)

    expect(secondaryYAxis('Rate', { ticksuffix: '%' })).toEqual({
      title: 'Rate', overlaying: 'y', side: 'right', showgrid: false, rangemode: 'tozero', ticksuffix: '%',
    })
    const l = withSecondaryAxis({ margin: { l: 50, r: 20 } as { r?: number } }, 'Rate')
    expect(l.margin).toEqual({ l: 50, r: 50 })
    expect(l.yaxis2.title).toBe('Rate')
  })
})

function makeBaseParams(overrides: Partial<BuildTrafficDataParams> = {}): BuildTrafficDataParams {
  return {
    aggregates: {
      metric: 'requests', interval: '1 minute',
      time_series: [100, 200, 150, 250, 300, 400].map((value, i) => ({ time: `2026-10-20T10:0${i}:00Z`, value })),
    },
    compareAggregates: null, compareMode: false, compareStartTime: null,
    startTime: '2026-10-20T10:00:00Z', endTime: '2026-10-20T10:05:00Z',
    trend: 'off', timezone: 'UTC', metric: 'requests', effectiveInterval: '1 minute',
    hiddenCategories: new Set(), catalog: null, ...overrides,
  }
}

describe('buildTrafficData & buildTrafficDataAsync', () => {
  it('builds primary bar trace, baseline trace, sliding-window trend line, and categorized 5xx breakdowns', () => {
    const traces = buildTrafficData(makeBaseParams({
      trend: '5m', compareMode: true, compareStartTime: '2026-10-19T10:00:00Z',
      compareAggregates: {
        metric: 'requests', interval: '1 minute',
        time_series: [90, 180, 140, 220, 280, 350].map((value, i) => ({ time: `2026-10-19T10:0${i}:00Z`, value })),
      },
    }))
    expect(traces).toHaveLength(3)
    expect(traces[0].y).toEqual([100, 200, 150, 250, 300, 400])
    expect(traces[1].y).toEqual([90, 180, 140, 220, 280, 350])
    expect(traces[2].y).toEqual([null, null, null, null, 200, 260])

    const catTraces = buildTrafficData(makeBaseParams({
      metric: '5xx',
      aggregates: {
        metric: '5xx', interval: '1 minute',
        time_series: [
          { time: '2026-10-20T10:00:00Z', category: '500', value: 12 }, { time: '2026-10-20T10:00:00Z', category: '503', value: 4 },
          { time: '2026-10-20T10:01:00Z', category: '500', value: 15 }, { time: '2026-10-20T10:01:00Z', category: '503', value: 2 },
        ],
      },
      startTime: '2026-10-20T10:00:00Z', endTime: '2026-10-20T10:01:00Z', hiddenCategories: new Set(['503']),
    }))
    expect(catTraces).toHaveLength(2)
    expect([catTraces[0].visible, catTraces[1].visible]).toEqual([true, 'legendonly'])
  })

  it('resolves buildTrafficDataAsync for small, empty, large, and trend datasets', async () => {
    const makeAggregates = (rowCount: number) => ({
      time_series: Array.from({ length: rowCount }, (_, i) => ({ time: `2026-06-01T${String(i % 24).padStart(2, '0')}:00:00Z`, value: i })),
      metric: 'requests', interval: 'hour',
    })
    const defaultAsyncParams = {
      compareAggregates: null, compareMode: false, compareStartTime: null, startTime: null, endTime: null,
      trend: 'off', timezone: 'UTC', metric: 'requests', effectiveInterval: 'hour',
      hiddenCategories: new Set<string>(), catalog: { fields: [] },
    }
    expect((await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: makeAggregates(50) }))[0].x).toHaveLength(50)
    expect(await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: { time_series: [], metric: 'requests' } })).toEqual([])
    expect((await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: makeAggregates(2500) }))[0].x).toHaveLength(2500)
    const withTrend = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: makeAggregates(100), trend: 'auto' })
    expect(withTrend).toHaveLength(2)
    expect(withTrend[1].name).toContain('Trend')
  })
})

const ALLOWED_GAPS = new Set<string>([
  'POST /api/services/:service_id/cron-settings', 'DELETE /api/services/:service_id/time-range', 'POST /api/services/:service_id/ngwaf-sync', 'PATCH /api/services/:service_id/custom-fields/:field_name',
  'DELETE /api/cron-runs/:log_id', 'POST /api/assets/aggregates', 'GET /api/assets/aggregates', 'POST /api/sharing/deploy-frontend', 'POST /api/sharing/teardown-frontend',
  'GET /api/high-scale/request-facts', 'POST /api/high-scale/request-facts', 'GET /api/network/pop-health', 'GET /api/security/threat-intel', 'GET /api/cron-runs/:run_id/stream',
  'GET /api/log-extents/stream', 'GET /api/admin/events/stream', 'GET /api/usage/operations', 'GET /api/usage/bandwidth', 'GET /api/usage/log-activity', 'GET /api/usage/rum-breakdown',
  'GET /api/admin/compaction-stats', 'POST /api/admin/consolidate-rollups', 'POST /api/admin/compact/:service_id', 'GET /api/admin/compaction-status/:service_id', 'GET /api/admin/compaction-status',
  'POST /api/admin/rollups/compact/:service_id', 'POST /api/admin/rollups/compact', 'POST /api/admin/rollups/heal/:service_id', 'POST /api/admin/rollups/heal',
  'POST /api/admin/rollup-heal/:service_id', 'POST /api/admin/rollup-heal', 'GET /api/admin/rollups/status/:service_id', 'GET /api/admin/rollups/status',
  'POST /api/admin/rebuild-time-series-rollups', 'POST /api/admin/partial-hour-merge/:service_id', 'POST /api/admin/partial-hour-merge',
  'GET /api/admin/partial-hour-status/:service_id', 'GET /api/admin/partial-hour-status', 'GET /api/admin/metric-history', 'GET /api/admin/iceberg-tree',
  'GET /api/admin/usage-log/export', 'GET /api/admin/slow-queries/count', 'GET /api/admin/slow-queries', 'GET /api/download-folder', 'GET /api/download', 'GET /api/download-all',
  'GET /api/admin/vcl-health', 'POST /api/provision/reconcile', 'POST /api/admin/optimize-now', 'POST /api/admin/local-compact-now',
  'POST /api/admin/backfill-bundle-rollups', 'POST /api/admin/usage-logging', 'POST /api/admin/reset-rum', 'POST /api/provision/teardown',
  'PATCH /api/provision/services/:service_id/ngwaf-workspace', 'POST /api/admin/expire-snapshots/:service_id', 'POST /api/admin/metadata-cleanup/:service_id',
  'POST /api/admin/sync/:service_id', 'POST /api/admin/full-sweep/:service_id', 'POST /api/admin/gap-heal/:service_id', 'POST /api/admin/ledger/sweep/:service_id',
  'GET /api/admin/ledger/quarantine', 'POST /api/admin/rum/sync/:service_id', 'POST /api/admin/rum/discovery/:service_id', 'POST /api/services/:service_id/scoring/enable',
  'POST /api/services/:service_id/scoring/disable', 'GET /api/services/:service_id/scoring/status', 'GET /api/services/:service_id/scoring/labels', 'POST /api/services/:service_id/scoring/labels',
  'PATCH /api/services/:service_id/scoring/labels/:label_id', 'DELETE /api/services/:service_id/scoring/labels/:label_id', 'GET /api/services/:service_id/scoring/top-flagged',
  'GET /api/services/:service_id/scoring/score-distribution', 'GET /api/services/:service_id/scoring/latency-timeseries', 'GET /api/services/:service_id/scoring/compliance-breakdown',
  'GET /api/services/:service_id/scoring/evaluation', 'GET /api/services/:service_id/scoring/curves', 'GET /api/services/:service_id/scoring/threshold-preview',
  'POST /api/services/:service_id/scoring/retrain', 'GET /api/services/:service_id/scoring/sessions/:sid/events', 'GET /api/services/:service_id/scoring/enforce-threshold',
  'PUT /api/services/:service_id/scoring/enforce-threshold', 'GET /api/services/:service_id/scoring/exclude-regex', 'PUT /api/services/:service_id/scoring/exclude-regex',
  'POST /api/services/:service_id/scoring/exclude-regex/validate', 'GET /api/services/:service_id/scoring/enforce-status-code', 'PUT /api/services/:service_id/scoring/enforce-status-code',
  'PUT /api/services/:service_id/scoring/l2-enforce', 'GET /api/services/:service_id/scoring/matrix-versions', 'POST /api/services/:service_id/scoring/matrix-versions/:version/restore',
  'POST /api/services/:service_id/scoring/rotate-key', 'GET /api/services/:service_id/scoring/audit', 'GET /api/services/:service_id/scoring/threshold', 'PUT /api/services/:service_id/scoring/threshold',
  'GET /api/services/:service_id/scoring/evaluation/per-reason', 'GET /api/services/:service_id/scoring/dashboard', 'GET /api/security/proxies/export',
  'GET /api/services/:service_id/rum/analytics', 'GET /api/services/:service_id/rum/live-events', 'GET /api/services/:service_id/cmcd/status', 'POST /api/services/:service_id/cmcd/enable',
  'POST /api/services/:service_id/cmcd/disable', 'GET /api/services/:service_id/rum/status', 'POST /api/services/:service_id/rum/settings', 'POST /api/services/:service_id/rum/enable',
  'POST /api/services/:service_id/rum/disable', 'GET /api/services/:service_id/rum/beacon-health', 'GET /api/services/:service_id/rum/health', 'GET /api/services/:service_id/rum/vitals',
  'GET /api/services/:service_id/rum/errors', 'POST /api/services/:service_id/rum/upgrade', 'GET /api/services/:service_id/control-room/:tab', 'POST /api/services/:service_id/control-room/mitigations',
  'POST /api/services/:service_id/control-room/rules', 'POST /api/services/:service_id/control-room/allowlist', 'POST /api/services/:service_id/control-room/big-red-button',
  'POST /api/services/:service_id/control-room/cost-governor', 'GET /api/services/:service_id/control-room/wizard/state', 'POST /api/services/:service_id/control-room/wizard/step',
  'GET /api/services/:service_id/realtime-stream', 'GET /api/services/:service_id/log-field-audit', 'POST /api/services/:service_id/control-room/correlate',
  'GET /api/services/:service_id/control-room/error-stream', 'GET /api/debug/state', 'POST /api/share/logout', 'GET /api/share/heartbeat', 'POST /api/share/claim/:token',
  'POST /api/admin/share/invites/:invite_id/claim-token', 'POST /api/admin/share/backup/export', 'POST /api/admin/share/backup/import', 'POST /api/admin/share/gdpr/erase',
  'GET /js/rum.js', 'GET /js/faro-sdk.js', 'GET /rum-beacon', 'POST /rum-beacon', 'GET /api/debug/page-telemetry',
])

function describeHandler(h: unknown): { method: string; pathname: string } | null {
  const info = (h as { info?: { method?: string; path?: string } })?.info
  if (!info?.method || typeof info.path !== 'string') return null
  const method = String(info.method).toUpperCase()
  try { return { method, pathname: new URL(info.path).pathname } } catch { return { method, pathname: info.path } }
}

describe('MSW handler coverage vs openapi.json', () => {
  it('every documented (path × method) has an MSW handler or ALLOWED_GAPS entry, and ALLOWED_GAPS has no stale entries', () => {
    const spec = JSON.parse(fs.readFileSync(path.resolve(__dirname, '..', '..', 'openapi.json'), 'utf-8')) as { paths: Record<string, Record<string, unknown>> }
    const covered = new Set<string>()
    for (const h of handlers) {
      const d = describeHandler(h)
      if (d) covered.add(`${d.method} ${d.pathname}`)
    }
    const documented: string[] = []
    for (const [p, methods] of Object.entries(spec.paths)) {
      for (const m of Object.keys(methods)) {
        const method = m.toUpperCase()
        if (method !== 'HEAD' && method !== 'OPTIONS') documented.push(`${method} ${p.replace(/\{([^}]+)\}/g, ':$1')}`)
      }
    }
    expect(documented.filter((entry) => !covered.has(entry) && !ALLOWED_GAPS.has(entry))).toEqual([])
    expect([...ALLOWED_GAPS].filter((entry) => covered.has(entry))).toEqual([])
  })

  it('models disabled ClickHouse without inventing publication health or replay success', async () => {
    const status = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/status?service_id=svc-test')
    expect(await status.json()).toMatchObject({ service_id: 'svc-test', enabled: false, health: 'disabled', oldest_pending_age_seconds: null })
    const replay = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/replay', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ service_id: 'svc-test', dataset_id: 'dataset' }),
    })
    expect(replay.status).toBe(409)
    expect(await replay.json()).toMatchObject({ detail: { error: 'clickhouse_disabled' } })
  })
})

function makeReq(url: string, headers: Record<string, string> = {}): NextRequest {
  const u = new URL(url)
  return { nextUrl: { pathname: u.pathname, clone: () => new URL(u.toString()), toString: () => u.toString() }, headers: new Headers(headers), url } as NextRequest
}

describe('middleware /admin gate (security)', () => {
  it.each(['/admin', '/alerts', '/usage'])('gates %s and sub-paths behind Caddy while allowing local access', (prefix) => {
    const caddyRes = middleware(makeReq(`http://localhost${prefix}`, { 'x-proxied-by-caddy': 'true' }))
    expect(caddyRes.status).toBe(307)
    expect(new URL(caddyRes.headers.get('location') || '').pathname).toBe('/')

    const localRes = middleware(makeReq(`http://localhost${prefix}`))
    expect(localRes.status).toBe(200)
    expect(localRes.headers.get('location')).toBeNull()

    expect(middleware(makeReq(`http://localhost${prefix}/sub`, { 'x-proxied-by-caddy': 'true' })).status).toBe(307)
    expect(middleware(makeReq(`http://localhost${prefix}/sub`)).status).toBe(200)
    expect(middleware(makeReq(`http://localhost/_next/data/build-id${prefix}.json`, { 'x-proxied-by-caddy': 'true' })).status).toBe(403)
  })

  it('enforces spoof resistance, Server Action blocking, non-admin pass-through, and x-service-id propagation', () => {
    expect(middleware(makeReq('http://localhost/admin', { host: 'localhost', 'x-proxied-by-caddy': 'true' })).status).toBe(307)
    expect(middleware(makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'maybe' })).status).toBe(200)
    expect(middleware(makeReq('http://localhost/_next/data/build-id/admin/settings.json', { 'x-proxied-by-caddy': 'true' })).status).toBe(403)
    expect(middleware(makeReq('http://localhost/_next/data/build-id/admin.json')).status).toBe(200)
    expect(middleware(makeReq('http://localhost/', { 'x-proxied-by-caddy': 'true', 'next-action': 'some-action-id' })).status).toBe(403)
    expect(middleware(makeReq('http://localhost/', { 'next-action': 'some-action-id' })).status).toBe(200)
    for (const p of ['/dashboard', '/logs', '/_next/data/build-id/logs.json']) {
      expect(middleware(makeReq(`http://localhost${p}`, { 'x-proxied-by-caddy': 'true' })).status).toBe(200)
    }

    const nextSpy = vi.spyOn(NextResponse, 'next')
    middleware(makeReq('http://localhost/dashboard?service=my-service-id'))
    expect(nextSpy.mock.calls[0][0]?.request?.headers?.get('x-service-id')).toBe('my-service-id')
    nextSpy.mockClear()
    middleware(makeReq('http://localhost/dashboard'))
    expect(nextSpy.mock.calls[0][0]?.request?.headers?.get('x-service-id')).toBeNull()
    nextSpy.mockRestore()
  })
})

async function withUpstreamServer(handler: (req: IncomingMessage, res: ServerResponse) => void) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return { port, close: () => new Promise<void>((resolve) => server.close(() => resolve())) }
}

describe('upstream API/JS proxy route handler (ECONNRESET resilience)', () => {
  afterEach(() => {
    delete process.env.API_PROXY_URL
    delete process.env.NEXT_PUBLIC_API_PROXY_URL
  })

  it('middleware passes /api/* and /js/* to route handlers instead of NextResponse.rewrite', () => {
    for (const url of ['http://localhost:3002/api/admin/events/stream?service_id=demo', 'http://localhost:3002/js/rum.js']) {
      const res = middleware(new NextRequest(url))
      expect(res.status).toBe(200)
      expect(res.headers.get('x-middleware-rewrite')).toBeNull()
    }
  })

  it('forwards GET and POST requests with query params, headers, body, and Set-Cookie using connection: close', async () => {
    let seenMethod = '', seenUrl = '', seenConn = '', seenBody = ''
    const server = await withUpstreamServer((req, res) => {
      seenMethod = req.method || ''
      seenUrl = req.url || ''
      seenConn = req.headers.connection || ''
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        seenBody = Buffer.concat(chunks).toString('utf8')
        res.statusCode = 201
        res.setHeader('Content-Type', 'application/json')
        res.setHeader('Set-Cookie', ['a=1; Path=/', 'b=2; Path=/'])
        res.end(JSON.stringify({ ok: true, echoed: seenBody }))
      })
    })
    try {
      const res = await apiPost(new NextRequest('http://localhost:3002/api/query?range=1h', {
        method: 'POST', headers: { 'content-type': 'application/json', 'x-custom': 'val' }, body: JSON.stringify({ sql: 'SELECT 1' }),
      }))
      expect(res.status).toBe(201)
      expect([seenMethod, seenUrl, seenConn]).toEqual(['POST', '/api/query?range=1h', 'close'])
      expect(await res.json()).toEqual({ ok: true, echoed: '{"sql":"SELECT 1"}' })
      expect(res.headers.getSetCookie()).toEqual(['a=1; Path=/', 'b=2; Path=/'])
    } finally {
      await server.close()
    }
  })

  it('retries idempotent GET once when upstream socket resets with ECONNRESET before headers', async () => {
    let attempts = 0
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      attempts += 1
      if (attempts === 1) { req.socket.destroy(); return }
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"healed":true}')
    })
    try {
      const res = await apiGet(new NextRequest('http://localhost:3002/api/bootstrap'))
      expect(res.status).toBe(200)
      expect(await res.json()).toEqual({ healed: true })
      expect(attempts).toBe(2)
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('streams SSE responses and suppresses ECONNRESET logs on client abort or upstream disconnect, and proxies /js/* routes', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      if (req.url?.startsWith('/js/')) {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/javascript')
        res.end('window.__RUM__=1;')
        return
      }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' })
      res.write('data: {"tick":1}\n\n')
      setTimeout(() => req.socket.destroy(), 20)
    })
    try {
      const abortController = new AbortController()
      const res = await proxyUpstreamRequest(new Request('http://localhost:3002/api/admin/events/stream', { signal: abortController.signal }))
      expect(res.status).toBe(200)
      expect(res.headers.get('content-type')).toContain('text/event-stream')
      const reader = res.body!.getReader()
      expect(new TextDecoder().decode((await reader.read()).value)).toContain('data: {"tick":1}')
      expect((await reader.read()).done).toBe(true)
      abortController.abort()
      expect(errorSpy).not.toHaveBeenCalled()

      const jsRes = await jsGet(new NextRequest('http://localhost:3002/js/beacon.js'))
      expect(jsRes.status).toBe(200)
      expect(await jsRes.text()).toBe('window.__RUM__=1;')
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })
})
