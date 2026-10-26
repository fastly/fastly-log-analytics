/**
 * @vitest-environment jsdom
 */
import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { NextResponse, NextRequest } from 'next/server'
import { describe, it, expect, test, beforeEach, afterEach, vi } from 'vitest'
import {
  downloadAsCsv,
  IP_FAMILY_FIELDS,
  isIpFamilyField,
  buildSessionDashboardUrl,
  buildStreamDetailUrl,
  isUserActive,
  msSinceLastInteraction,
  __resetForTests,
  ACTIVE_WINDOW_MS,
  showReadOnlyToast,
  showToast,
  resolveRangeWire,
  DEFAULT_RANGE_TOKEN,
  ANCHOR_QUANTUM_SECONDS,
  quantizeAnchor,
  buildTrafficData,
  type BuildTrafficDataParams,
} from '@/lib/utils'
import {
  formatBytes,
  statusBadgeClass,
  toUTCDate,
  formatTimeAgo,
  formatLocalDateTime,
  formatValue,
  calculateDelta,
  formatCount,
  formatCountMaybe,
  formatAbbreviatedCount,
  ELAPSED_AGE_COMPACT,
  ELAPSED_FRESHNESS,
  ELAPSED_RELATIVE_AGO,
  ELAPSED_RELATIVE_AGO_JUST_NOW,
  formatBytesTiered,
  formatElapsed,
} from '@/lib/format'
import {
  TIME_HOVER_LAYOUT,
  makeTimeXAxis,
  denseTimeGrid,
  denseBarTrace,
  lineTrace,
  secondaryYAxis,
  withSecondaryAxis,
} from '@/lib/chart-helpers'
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
  it('returns "0 B" for zero', () => {
    expect(formatBytes(0)).toBe('0 B')
  })

  it('formats bytes', () => {
    expect(formatBytes(1)).toBe('1 B')
    expect(formatBytes(500)).toBe('500 B')
  })

  it('formats kilobytes', () => {
    expect(formatBytes(1024)).toBe('1 KB')
    expect(formatBytes(1536)).toBe('1.5 KB')
  })

  it('formats megabytes', () => {
    expect(formatBytes(1024 * 1024)).toBe('1 MB')
    expect(formatBytes(1024 * 1024 * 2.5)).toBe('2.5 MB')
  })

  it('formats gigabytes', () => {
    expect(formatBytes(1024 ** 3)).toBe('1 GB')
  })

  it('formats terabytes', () => {
    expect(formatBytes(1024 ** 4)).toBe('1 TB')
  })
})

describe('downloadAsCsv', () => {
  let capturedCsv = ''

  beforeEach(() => {
    capturedCsv = ''
    vi.stubGlobal('Blob', class MockBlob {
      constructor(parts: BlobPart[]) { capturedCsv = (parts as string[]).join('') }
    })
    vi.stubGlobal('URL', { createObjectURL: vi.fn(() => 'blob:mock'), revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLElement.prototype, 'click').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  function buildCsv(rows: Record<string, unknown>[], cols: string[]): string {
    downloadAsCsv(rows, cols, 'test.csv')
    return capturedCsv
  }

  it('first row is the header matching column order', () => {
    const csv = buildCsv([], ['name', 'count'])
    expect(csv.split('\n')[0]).toBe('name,count')
  })

  it('data rows follow column order', () => {
    const csv = buildCsv([{ name: 'alice', count: 5 }, { name: 'bob', count: 3 }], ['name', 'count'])
    const lines = csv.split('\n')
    expect(lines[1]).toBe('alice,5')
    expect(lines[2]).toBe('bob,3')
  })

  it('quotes values containing commas', () => {
    const csv = buildCsv([{ url: '/a,b', n: 1 }], ['url', 'n'])
    expect(csv).toContain('"/a,b"')
  })

  it('escapes double quotes by doubling them', () => {
    const csv = buildCsv([{ label: 'say "hello"', n: 1 }], ['label', 'n'])
    expect(csv).toContain('"say ""hello"""')
  })

  it('quotes values containing newlines', () => {
    const csv = buildCsv([{ text: 'line1\nline2', n: 1 }], ['text', 'n'])
    expect(csv).toContain('"line1\nline2"')
  })

  it('renders null as empty string', () => {
    const csv = buildCsv([{ url: null, count: 5 }], ['url', 'count'])
    expect(csv.split('\n')[1]).toBe(',5')
  })

  it('renders a missing column key as empty string', () => {
    const csv = buildCsv([{ count: 5 }], ['url', 'count'])
    expect(csv.split('\n')[1]).toBe(',5')
  })

  it('sets the download attribute to the given filename', () => {
    const appendSpy = vi.spyOn(document.body, 'appendChild')
    downloadAsCsv([], ['col'], 'my-export.csv')
    const anchor = appendSpy.mock.calls[0][0] as HTMLAnchorElement
    expect(anchor.download).toBe('my-export.csv')
  })

  it('produces correct row count including header', () => {
    const rows = [{ a: 1 }, { a: 2 }, { a: 3 }]
    const csv = buildCsv(rows, ['a'])
    expect(csv.split('\n')).toHaveLength(4)
  })
})

describe('isIpFamilyField', () => {
  it('matches the client-IP-family columns a masking analyst cannot filter on', () => {
    for (const col of ['ip', 'client_ip', 'ip_address', 'remote_addr']) {
      expect(isIpFamilyField(col)).toBe(true)
    }
  })

  it('does NOT match origin IP (oip) or non-IP columns', () => {
    for (const col of ['oip', 'country', 'asn', 'city', 'url', 'ua', '']) {
      expect(isIpFamilyField(col)).toBe(false)
    }
  })

  it('mirrors the backend forbidden-column set', () => {
    expect([...IP_FAMILY_FIELDS].sort()).toEqual(
      ['client_ip', 'ip', 'ip_address', 'remote_addr'],
    )
  })
})

describe('formatPopGeo', () => {
  it('formats a US PoP as "City, ST - USA"', () => {
    expect(formatPopGeo({ city: 'Denver', region: 'co', country: 'us' })).toBe('Denver, CO - USA')
    expect(formatPopGeo({ city: 'Ashburn (Metro)', region: 'va', country: 'us' })).toBe('Ashburn, VA - USA')
  })

  it('omits the state for non-US PoPs (2-part shield)', () => {
    expect(formatPopGeo({ city: 'London', region: '', country: 'uk' })).toBe('London - UK')
    expect(formatPopGeo({ city: 'Frankfurt', region: '', country: 'de' })).toBe('Frankfurt - DE')
  })

  it('drops a region that just repeats the city', () => {
    expect(formatPopGeo({ city: 'Hyderabad', region: 'hyderabad', country: 'in' })).toBe('Hyderabad - IN')
    expect(formatPopGeo({ city: 'Sao Paulo', region: 'saopaulo', country: 'br' })).toBe('Sao Paulo - BR')
  })

  it('keeps a real state that differs from the city', () => {
    expect(formatPopGeo({ city: 'New York City', region: 'ny', country: 'us' })).toBe('New York City, NY - USA')
  })

  it('degrades gracefully for missing / partial data', () => {
    expect(formatPopGeo(undefined)).toBe('')
    expect(formatPopGeo(null)).toBe('')
    expect(formatPopGeo({ city: '', region: 'ca', country: 'us' })).toBe('')
    expect(formatPopGeo({ city: 'Palo Alto', region: '', country: '' })).toBe('Palo Alto')
  })
})

describe('shouldLoadRumScript', () => {
  it('loads RUM for an analyst request through the real proxy domain', () => {
    expect(
      shouldLoadRumScript({
        host: 'fastly-log-analytics.example.com',
        proxiedByCaddy: true,
        rumEnabled: true,
      }),
    ).toBe(true)
  })

  it.each(['localhost:13002', '127.0.0.1:13002', '[::1]:13002'])(
    'does not load RUM on local host %s',
    (host) => {
      expect(
        shouldLoadRumScript({ host, proxiedByCaddy: true, rumEnabled: true }),
      ).toBe(false)
    },
  )

  it('does not load RUM when the request is not from the analyst proxy', () => {
    expect(
      shouldLoadRumScript({
        host: 'fastly-log-analytics.example.com',
        proxiedByCaddy: false,
        rumEnabled: true,
      }),
    ).toBe(false)
  })

  it('does not load RUM when the active service has it disabled', () => {
    expect(
      shouldLoadRumScript({
        host: 'fastly-log-analytics.example.com',
        proxiedByCaddy: true,
        rumEnabled: false,
      }),
    ).toBe(false)
  })
})

describe('buildSessionDashboardUrl', () => {
  const START = '2026-05-10T13:06:52+00:00'
  const END   = '2026-05-10T13:07:15+00:00'

  it('uses start_time and end_time params (not start/end)', () => {
    const url = buildSessionDashboardUrl('svc-1', 'ip', '1.2.3.4', START, END)
    const params = new URLSearchParams(url.split('?')[1])
    expect(params.get('start_time')).toBe(START)
    expect(params.get('end_time')).toBe(END)
    expect(params.has('start')).toBe(false)
    expect(params.has('end')).toBe(false)
  })

  it('sets the service param', () => {
    const url = buildSessionDashboardUrl('my-service', 'ip', '1.2.3.4', START, END)
    const params = new URLSearchParams(url.split('?')[1])
    expect(params.get('service')).toBe('my-service')
  })

  it('prefixes the filter key with filter_', () => {
    const ipUrl = buildSessionDashboardUrl('svc', 'ip', '1.2.3.4', START, END)
    expect(new URLSearchParams(ipUrl.split('?')[1]).get('filter_ip')).toBe('1.2.3.4')

    const ja4Url = buildSessionDashboardUrl('svc', 'ja4', 'abc123', START, END)
    expect(new URLSearchParams(ja4Url.split('?')[1]).get('filter_ja4')).toBe('abc123')

    const uaUrl = buildSessionDashboardUrl('svc', 'ua', 'Mozilla/5.0', START, END)
    expect(new URLSearchParams(uaUrl.split('?')[1]).get('filter_ua')).toBe('Mozilla/5.0')
  })

  it('routes to /dashboard', () => {
    const url = buildSessionDashboardUrl('svc', 'ip', '1.2.3.4', START, END)
    expect(url.startsWith('/dashboard?')).toBe(true)
  })
})

describe('buildStreamDetailUrl', () => {
  it('builds URL with only token when serviceId is omitted', () => {
    const url = buildStreamDetailUrl('token123')
    expect(url).toBe('/sessions/stream?token=token123')
  })

  it('encodes the token correctly', () => {
    const url = buildStreamDetailUrl('token with spaces & symbols')
    expect(url).toBe('/sessions/stream?token=token%20with%20spaces%20%26%20symbols')
  })

  it('appends service parameter when serviceId is provided', () => {
    const url = buildStreamDetailUrl('token123', 'svc-abc')
    expect(url).toBe('/sessions/stream?token=token123&service=svc-abc')
  })

  it('encodes the serviceId parameter correctly', () => {
    const url = buildStreamDetailUrl('token123', 'svc with space')
    expect(url).toBe('/sessions/stream?token=token123&service=svc%20with%20space')
  })
})

describe('statusBadgeClass', () => {
  it.each([
    [200, 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [302, 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'],
    [404, 'bg-amber-500/15 text-amber-800 dark:text-amber-400 border-amber-500/20'],
    [503, 'bg-destructive/15 text-destructive border-destructive/20 font-bold'],
    ['200', 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [null, 'bg-muted text-muted-foreground'],
    ['x', 'bg-muted text-muted-foreground'],
  ] as const)('default variant %s', (status, want) => {
    expect(statusBadgeClass(status)).toBe(want)
  })

  it.each([
    [200, 'bg-green-500/10 text-green-500 border-green-500/20'],
    [299, 'bg-green-500/10 text-green-500 border-green-500/20'],
    [301, 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'],
    [404, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [503, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [100, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [0, 'bg-muted text-muted-foreground'],
    [null, 'bg-muted text-muted-foreground'],
    [undefined, 'bg-muted text-muted-foreground'],
  ] as const)('traffic-light variant %s', (status, want) => {
    expect(statusBadgeClass(status, 'traffic-light')).toBe(want)
  })
})

describe('userActivity', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-05-27T00:00:00Z'))
    __resetForTests(0)
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('is inactive after reset with no gestures', () => {
    expect(isUserActive()).toBe(false)
    expect(msSinceLastInteraction()).toBeGreaterThan(ACTIVE_WINDOW_MS)
  })

  it('becomes active immediately after a keydown gesture', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    expect(isUserActive()).toBe(true)
    expect(msSinceLastInteraction()).toBeLessThan(1_000)
  })

  it('becomes active after a pointerdown gesture', () => {
    window.dispatchEvent(new Event('pointerdown'))
    expect(isUserActive()).toBe(true)
  })

  it('goes idle again once the active window elapses', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    expect(isUserActive()).toBe(true)
    vi.advanceTimersByTime(ACTIVE_WINDOW_MS + 1)
    expect(isUserActive()).toBe(false)
  })

  it('honors a caller-supplied window (the heartbeat idle-probe use)', () => {
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }))
    vi.advanceTimersByTime(20_000)
    expect(isUserActive(15_000)).toBe(false)
    expect(isUserActive(120_000)).toBe(true)
  })
})

function describeFilters(filters: string[]): string {
  return typeof filters
}

describe('invalidInput', () => {
  it('hands the exact malformed value through to the callee', () => {
    const bad = { not: 'an array' }
    expect(invalidInput(bad)).toBe(bad)
    expect(describeFilters(invalidInput(bad))).toBe('object')
    expect(describeFilters(invalidInput('str'))).toBe('string')
  })
})

describe('showToast & showReadOnlyToast', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-06-12T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    document.body.innerHTML = ''
  })

  it('renders the message into the DOM with the info role by default', () => {
    showToast('hello world')
    const region = document.querySelector('[role="region"][aria-label="Notifications"]')
    expect(region).not.toBeNull()
    const toast = region!.querySelector('[role="status"]')
    expect(toast).not.toBeNull()
    expect(toast!.textContent).toBe('hello world')
    expect(toast!.getAttribute('aria-live')).toBe('polite')
  })

  it('uses role=alert + aria-live=assertive for error toasts', () => {
    showToast('database is on fire', 'error')
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
    expect(toast!.textContent).toBe('database is on fire')
  })

  it('uses role=alert + aria-live=assertive for warn toasts', () => {
    showToast('careful now', 'warn')
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
  })

  it('uses role=status + aria-live=polite for success toasts', () => {
    showToast('saved', 'success')
    const toast = document.querySelector('[role="status"]')
    expect(toast).not.toBeNull()
    expect(toast!.getAttribute('aria-live')).toBe('polite')
  })

  it('dedupes identical messages within the 1.5s window', () => {
    showToast('parallel 403')
    showToast('parallel 403')
    showToast('parallel 403')
    const toasts = document.querySelectorAll('[role="status"], [role="alert"]')
    expect(toasts.length).toBe(1)
  })

  it('allows the same message after the dedupe window expires', () => {
    showToast('repeat me')
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    vi.advanceTimersByTime(2000)
    showToast('repeat me')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
  })

  it('does not dedupe different messages', () => {
    showToast('first')
    showToast('second')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
  })

  it('auto-removes after the default duration', () => {
    showToast('temporary')
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    vi.advanceTimersByTime(3500)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('keeps error toasts on screen for the longer 5500ms duration', () => {
    showToast('serious', 'error')
    vi.advanceTimersByTime(3500)
    expect(document.querySelectorAll('[role="alert"]').length).toBe(1)
    vi.advanceTimersByTime(2000)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="alert"]').length).toBe(0)
  })

  it('honours an explicit durationMs option', () => {
    showToast('short-lived', 'info', { durationMs: 100 })
    vi.advanceTimersByTime(100)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('removes the toast immediately on click', () => {
    showToast('clickable', 'info')
    const toast = document.querySelector('[role="status"]') as HTMLElement
    expect(toast).not.toBeNull()
    toast.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(0)
  })

  it('removes the container when the last toast goes away', () => {
    showToast('lonely')
    expect(document.querySelector('[role="region"]')).not.toBeNull()
    vi.advanceTimersByTime(3500)
    vi.advanceTimersByTime(200)
    expect(document.querySelector('[role="region"]')).toBeNull()
  })

  it('keeps the container while multiple toasts are stacked', () => {
    showToast('one')
    vi.advanceTimersByTime(50)
    showToast('two')
    expect(document.querySelectorAll('[role="status"]').length).toBe(2)
    vi.advanceTimersByTime(3500 - 50)
    vi.advanceTimersByTime(200)
    expect(document.querySelectorAll('[role="status"]').length).toBe(1)
    expect(document.querySelector('[role="region"]')).not.toBeNull()
  })

  it('is a no-op when document is undefined (SSR-safe)', () => {
    const originalDocument = globalThis.document
    vi.stubGlobal('document', undefined)
    try {
      expect(() => showToast('ssr-safe')).not.toThrow()
    } finally {
      vi.stubGlobal('document', originalDocument)
    }
  })

  it('renders the canonical read-only warning with the warn kind', () => {
    showReadOnlyToast()
    const toast = document.querySelector('[role="alert"]')
    expect(toast).not.toBeNull()
    expect(toast!.textContent).toContain('Read-only access')
    expect(toast!.getAttribute('aria-live')).toBe('assertive')
  })
})

describe('toUTCDate', () => {
  it('parses ISO strings', () => {
    const d = toUTCDate('2024-01-01T00:00:00Z')
    expect(d.toISOString()).toBe('2024-01-01T00:00:00.000Z')
  })

  it('handles space separator from backend', () => {
    const d = toUTCDate('2024-01-01 12:00:00')
    expect(d.toISOString()).toBe('2024-01-01T12:00:00.000Z')
  })
})

describe('relative time formatting', () => {
  const now = new Date('2024-06-15T12:00:00Z')

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('formatTimeAgo', () => {
    it('formats recent', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 5000))).toBe('5s ago')
    })

    it('formats minutes and seconds', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 75000))).toBe('1m 15s ago')
    })

    it('formats hours', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 3600000 * 2 - 60000))).toBe('2h 1m 0s ago')
    })
  })
})

describe('formatLocalDateTime', () => {
  it('matches new Date(x).toLocaleString() for ISO strings', () => {
    const iso = '2026-07-01T12:34:56Z'
    expect(formatLocalDateTime(iso)).toBe(new Date(iso).toLocaleString())
  })

  it('matches for epoch-millis numbers', () => {
    const ms = 1_750_000_000_000
    expect(formatLocalDateTime(ms)).toBe(new Date(ms).toLocaleString())
  })

  it('accepts a Date instance directly', () => {
    const d = new Date('2026-01-15T00:00:00Z')
    expect(formatLocalDateTime(d)).toBe(d.toLocaleString())
  })

  it('surfaces Invalid Date like the native call', () => {
    expect(formatLocalDateTime('not-a-date')).toBe(new Date('not-a-date').toLocaleString())
  })

  it('forwards Intl.DateTimeFormatOptions', () => {
    const iso = '2026-07-01T12:34:56Z'
    const opts: Intl.DateTimeFormatOptions = { dateStyle: 'short' }
    expect(formatLocalDateTime(iso, opts)).toBe(new Date(iso).toLocaleString(undefined, opts))
  })
})

const ANCHOR = '2026-06-29T12:00:00Z'
const START = '2026-06-10T00:00:00Z'
const END = '2026-06-12T00:00:00Z'

describe('resolveRangeWire', () => {
  it('cold-load / auto default → "24h" token (server-reproducible, SSR-seedable)', () => {
    const w = resolveRangeWire({
      relativeRange: null,
      isAutoRange: true,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBe(DEFAULT_RANGE_TOKEN)
    expect(w.rangeKey).toBe('24h')
    expect(w.rangeBody).toEqual({ range_token: '24h', anchor: ANCHOR, start_time: START, end_time: END })
  })

  it('explicit preset pill → forwards the label as the token + display bounds', () => {
    for (const preset of ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '30d']) {
      const w = resolveRangeWire({
        relativeRange: preset,
        isAutoRange: false,
        startTime: START,
        endTime: END,
        anchor: ANCHOR,
      })
      expect(w.rangeToken).toBe(preset)
      expect(w.rangeKey).toBe(preset)
      expect(w.rangeBody).toEqual({ range_token: preset, anchor: ANCHOR, start_time: START, end_time: END })
    }
  })

  it('custom absolute range (relativeRange null, not auto) → sends bounds, no token', () => {
    const w = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBeNull()
    expect(w.rangeBody).toEqual({ start_time: START, end_time: END })
    expect('range_token' in w.rangeBody).toBe(false)
  })

  it('distinct custom ranges produce distinct cache keys (no collision)', () => {
    const a = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    const b = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: '2026-05-01T00:00:00Z',
      endTime: '2026-05-02T00:00:00Z',
      anchor: ANCHOR,
    })
    expect(a.rangeKey).not.toBe(b.rangeKey)
  })

  it('a preset wins even when isAutoRange somehow lingers true', () => {
    const w = resolveRangeWire({
      relativeRange: '7d',
      isAutoRange: true,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBe('7d')
  })
})

const NOW = new Date('2026-06-29T12:00:00Z')

describe('quantizeAnchor floors to the 60s grid', () => {
  test('floors a sub-minute anchor to :00', () => {
    expect(quantizeAnchor('2026-06-29T12:00:59Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('leaves an exact-minute anchor unchanged', () => {
    expect(quantizeAnchor('2026-06-29T12:00:00Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('two anchors within the same quantum collapse to one value (key stability)', () => {
    const a = quantizeAnchor('2026-06-29T12:00:01Z', NOW)
    const b = quantizeAnchor('2026-06-29T12:00:58Z', NOW)
    expect(a).toBe(b)
    expect(a).toBe('2026-06-29T12:00:00Z')
  })
  test('a missing anchor falls back to quantized now', () => {
    expect(quantizeAnchor(undefined, NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('an unparseable anchor falls back to quantized now', () => {
    expect(quantizeAnchor('not-a-date', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('output carries NO milliseconds fraction (matches backend iso_z)', () => {
    expect(quantizeAnchor('2026-06-29T12:00:30.123Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
})

describe('token vocabulary + constants mirror the backend', () => {
  test('ANCHOR_QUANTUM_SECONDS is 60', () => {
    expect(ANCHOR_QUANTUM_SECONDS).toBe(60)
  })
})

// ---------------------------------------------------------------------------
// Folded from format.test.ts
// ---------------------------------------------------------------------------

describe('formatCount', () => {
  it('matches Number.prototype.toLocaleString() byte-for-byte', () => {
    for (const n of [0, 5, 42, 1000, 1234567, -9876, 1234.5, 0.125]) {
      expect(formatCount(n)).toBe(n.toLocaleString())
    }
  })

  it('passes through Intl.NumberFormatOptions', () => {
    expect(formatCount(3.14159, { maximumFractionDigits: 1 })).toBe(
      (3.14159).toLocaleString(undefined, { maximumFractionDigits: 1 }),
    )
  })
})

describe('formatValue', () => {
  it('formats null/undefined as "null"', () => {
    expect(formatValue('any', null)).toBe('null')
    expect(formatValue('any', undefined)).toBe('null')
  })

  it('formats bytes fields using formatBytes', () => {
    expect(formatValue('resp_bytes', 1024)).toBe('1 KB')
    expect(formatValue('req_header_bytes', 500)).toBe('500 B')
  })

  it('formats numeric fields with toLocaleString', () => {
    expect(formatValue('requests', 1000)).toBe('1,000')
  })

  it('formats country codes to names', () => {
    expect(formatValue('country', 'US')).toBe('United States')
    expect(formatValue('country', 'GB')).toBe('United Kingdom')
  })

  it('title cases city/region/pop', () => {
    expect(formatValue('city', 'new york')).toBe('New York')
    expect(formatValue('pop', 'jfk')).toBe('JFK')
    expect(formatValue('region', 'ca')).toBe('CA')
  })

  it('formats proto/tls to 1 decimal place', () => {
    expect(formatValue('proto', 3)).toBe('3')
    expect(formatValue('tls', 1.3)).toBe('1.3')
  })

  it('parses proto string values: valid numeric string → trimmed decimal', () => {
    expect(formatValue('proto', '2.0')).toBe('2')
    expect(formatValue('tls', '1.3')).toBe('1.3')
  })

  it('passes invalid proto/tls strings through unchanged', () => {
    expect(formatValue('proto', 'h2')).toBe('h2')
    expect(formatValue('tls', 'TLSv1.3')).toBe('TLSv1.3')
  })

  it('falls back to raw country code when Intl.DisplayNames throws', () => {
    const displayNames = vi.spyOn(Intl, 'DisplayNames').mockImplementation(function () {
      throw new Error('boom')
    })
    try {
      expect(formatValue('country', 'US')).toBe('US')
    } finally {
      displayNames.mockRestore()
    }
  })
})

describe('calculateDelta', () => {
  it('calculates percentage change', () => {
    expect(calculateDelta(110, 100)).toBe(10)
    expect(calculateDelta(90, 100)).toBe(-10)
  })

  it('returns null for zero or undefined baseline', () => {
    expect(calculateDelta(100, 0)).toBe(null)
    expect(calculateDelta(100, undefined)).toBe(null)
  })
})

describe('formatCountMaybe', () => {
  it('returns undefined for null/undefined and formats numbers like formatCount', () => {
    expect(formatCountMaybe(undefined)).toBeUndefined()
    expect(formatCountMaybe(null)).toBeUndefined()
    expect(formatCountMaybe(1234567)).toBe(formatCount(1234567))
    expect(formatCountMaybe(0)).toBe('0')
  })
})

describe('formatAbbreviatedCount', () => {
  it('abbreviates with K/M/B and configurable M/B precision', () => {
    expect(formatAbbreviatedCount(999)).toBe('999')
    expect(formatAbbreviatedCount(1500)).toBe('1.5K')
    expect(formatAbbreviatedCount(2_345_678)).toBe('2.3M')
    expect(formatAbbreviatedCount(2_345_678, 2)).toBe('2.35M')
    expect(formatAbbreviatedCount(3_210_000_000, 2)).toBe('3.21B')
  })
})

type FormatRow<I> = [I, string]

const METADATA_BYTES: FormatRow<number | null>[] = [
  [0, "0 B"],
  [1, "1 B"],
  [1023, "1023 B"],
  [1024, "1.0 KB"],
  [1025, "1.0 KB"],
  [1536, "1.5 KB"],
  [1048575, "1024.0 KB"],
  [1048576, "1.0 MB"],
  [5366336, "5.1 MB"],
  [1073741823, "1024.0 MB"],
  [1073741824, "1.00 GB"],
  [3271225472, "3.05 GB"],
  [5497558138880, "5120.00 GB"],
  [null, "–"],
]

const QUARANTINE_BYTES: FormatRow<number>[] = [
  [0, "0 B"],
  [1, "1 B"],
  [1023, "1023 B"],
  [1024, "1.0 KB"],
  [1025, "1.0 KB"],
  [1536, "1.5 KB"],
  [1048575, "1024.0 KB"],
  [1048576, "1.0 MB"],
  [5366336, "5.1 MB"],
  [1073741823, "1024.0 MB"],
  [1073741824, "1024.0 MB"],
  [3271225472, "3119.7 MB"],
  [5497558138880, "5242880.0 MB"],
]

const BOT_SOURCES_RELATIVE: FormatRow<number>[] = [
  [-5000, "just now"],
  [0, "just now"],
  [1000, "just now"],
  [44400, "just now"],
  [44500, "just now"],
  [45000, "just now"],
  [59000, "just now"],
  [59500, "just now"],
  [60000, "just now"],
  [89000, "just now"],
  [89400, "just now"],
  [89500, "just now"],
  [90000, "just now"],
  [119000, "just now"],
  [120000, "2m ago"],
  [121000, "2m ago"],
  [179000, "2m ago"],
  [3599000, "59m ago"],
  [3599600, "59m ago"],
  [3600000, "1h ago"],
  [5399000, "1h ago"],
  [5400000, "1h ago"],
  [7200000, "2h ago"],
  [86399000, "23h ago"],
  [86400000, "1d ago"],
  [90000000, "1d ago"],
  [172799000, "1d ago"],
  [172800000, "2d ago"],
  [259200000, "3d ago"],
  [400000000, "4d ago"],
]

const RESUME_BANNER_RELATIVE: FormatRow<number>[] = [
  [-5000, "-5s ago"],
  [0, "0s ago"],
  [1000, "1s ago"],
  [45000, "45s ago"],
  [59000, "59s ago"],
  [60000, "1m ago"],
  [89000, "1m ago"],
  [90000, "1m ago"],
  [119000, "1m ago"],
  [120000, "2m ago"],
  [121000, "2m ago"],
  [179000, "2m ago"],
  [3599000, "59m ago"],
  [3600000, "1h ago"],
  [5399000, "1h ago"],
  [5400000, "1h ago"],
  [7200000, "2h ago"],
  [86399000, "23h ago"],
  [86400000, "1d ago"],
  [90000000, "1d ago"],
  [172799000, "1d ago"],
  [172800000, "2d ago"],
  [259200000, "3d ago"],
  [400000000, "4d ago"],
]

const SYSTEM_HEALTH_AGE: FormatRow<number>[] = [
  [-5, "-5s"],
  [0, "0s"],
  [1, "1s"],
  [44.4, "44s"],
  [44.5, "45s"],
  [45, "45s"],
  [59, "59s"],
  [59.5, "60s"],
  [60, "60s"],
  [89, "89s"],
  [89.4, "89s"],
  [89.5, "90s"],
  [90, "2m"],
  [119, "2m"],
  [120, "2m"],
  [121, "2m"],
  [179, "3m"],
  [3599, "60m"],
  [3599.6, "60m"],
  [3600, "60m"],
  [5399, "90m"],
  [5400, "1.5h"],
  [7200, "2.0h"],
  [86399, "24.0h"],
  [86400, "24.0h"],
  [90000, "25.0h"],
  [172799, "48.0h"],
  [172800, "2.0d"],
  [259200, "3.0d"],
  [400000, "4.6d"],
]

const HIGH_SCALE_FRESHNESS: FormatRow<number>[] = [
  [-5, "-5s"],
  [0, "0s"],
  [1, "1s"],
  [44.4, "44s"],
  [44.5, "45s"],
  [45, "45s"],
  [59, "59s"],
  [59.5, "60s"],
  [60, "1m"],
  [89, "1m"],
  [89.4, "1m"],
  [89.5, "1m"],
  [90, "2m"],
  [119, "2m"],
  [120, "2m"],
  [121, "2m"],
  [179, "3m"],
  [3599, "60m"],
  [3599.6, "60m"],
  [3600, "1.0h"],
  [5399, "1.5h"],
  [5400, "1.5h"],
  [7200, "2.0h"],
  [86399, "24.0h"],
  [86400, "24.0h"],
  [90000, "25.0h"],
  [172799, "48.0h"],
  [172800, "48.0h"],
  [259200, "72.0h"],
  [400000, "111.1h"],
]

describe('formatBytesTiered', () => {
  it.each(METADATA_BYTES)('metadata card (GB cap) %s -> %s', (n, want) => {
    expect(formatBytesTiered(n, { maxUnit: 'GB' })).toBe(want)
  })
  it('renders the metadata card placeholder for undefined', () => {
    expect(formatBytesTiered(undefined, { maxUnit: 'GB' })).toBe('–')
  })
  it.each(QUARANTINE_BYTES)('quarantine (MB cap) %s -> %s', (n, want) => {
    expect(formatBytesTiered(n)).toBe(want)
  })
})

describe('formatElapsed presets', () => {
  it.each(BOT_SOURCES_RELATIVE)('bot sources %s ms -> %s', (ms, want) => {
    expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO_JUST_NOW)).toBe(want)
  })
  it.each(RESUME_BANNER_RELATIVE)('resume banner %s ms -> %s', (ms, want) => {
    expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO)).toBe(want)
  })
  it.each(SYSTEM_HEALTH_AGE)('system health age %s s -> %s', (s, want) => {
    expect(formatElapsed(s, ELAPSED_AGE_COMPACT)).toBe(want)
  })
  it.each(HIGH_SCALE_FRESHNESS)('high-scale freshness %s s -> %s', (s, want) => {
    expect(formatElapsed(s, ELAPSED_FRESHNESS)).toBe(want)
  })
})

// ---------------------------------------------------------------------------
// Folded from chart-helpers.test.ts
// ---------------------------------------------------------------------------

describe('TIME_HOVER_LAYOUT', () => {
  it('exposes the hover + legend keys callers spread into Plotly layouts', () => {
    expect(TIME_HOVER_LAYOUT).toMatchObject({
      hovermode: 'x unified',
      legend: expect.objectContaining({
        orientation: 'h',
        xanchor: 'center',
        yanchor: 'bottom',
      }),
    })
  })
})

describe('makeTimeXAxis', () => {
  it('returns a Plotly xaxis spec with type=date and a 2-element range', () => {
    const axis = makeTimeXAxis('2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z', 'UTC')
    expect(axis.type).toBe('date')
    expect(Array.isArray(axis.range)).toBe(true)
    expect(axis.range).toHaveLength(2)
    expect(axis.range[0]).not.toBe('')
    expect(axis.range[1]).not.toBe('')
    expect(axis.tickformatstops).toBeDefined()
  })

  it('formats range bounds in the supplied timezone', () => {
    const utc = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'UTC')
    const ny = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'America/New_York')
    expect(utc.range[0]).not.toBe(ny.range[0])
    expect(utc.range[0]).toContain('12:00:00')
    expect(ny.range[0]).toContain('07:00:00')
  })

  it('returns empty-string bounds for null/undefined start/end without throwing', () => {
    const both = makeTimeXAxis(null, null, 'UTC')
    expect(both.range).toEqual(['', ''])
    const undef = makeTimeXAxis(undefined, undefined, 'UTC')
    expect(undef.range).toEqual(['', ''])
    const mixed = makeTimeXAxis(null, '2026-01-01T00:00:00Z', 'UTC')
    expect(mixed.range[0]).toBe('')
    expect(mixed.range[1]).not.toBe('')
  })
})

describe('denseTimeGrid', () => {
  it('fills the gaps between the first and last bucket on the hourly grid', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid).toHaveLength(4)
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
      Date.parse('2026-06-30T12:00:00Z'),
    ])
  })

  it('treats a `+00:00` suffix (scoring `hour` format) the same as `Z`', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00+00:00', '2026-06-30T11:00:00+00:00'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00+00:00'),
      Date.parse('2026-06-30T10:00:00+00:00'),
      Date.parse('2026-06-30T11:00:00+00:00'),
    ])
  })

  it('treats space-separated naive timestamps (e.g. ClickHouse JSON) as UTC', () => {
    const grid = denseTimeGrid(
      ['2026-06-30 09:00:00', '2026-06-30 11:00:00'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
    ])
  })

  it('fills a per-minute grid for short scoring windows', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T09:03:00Z'],
      60,
    )
    expect(grid).toHaveLength(4)
  })

  it('no-ops (returns null) for unknown/zero interval', () => {
    const times = ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z']
    expect(denseTimeGrid(times, undefined)).toBeNull()
    expect(denseTimeGrid(times, 0)).toBeNull()
  })

  it('no-ops when there are fewer than 2 distinct buckets', () => {
    expect(denseTimeGrid(['2026-06-30T09:00:00Z'], 3600)).toBeNull()
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T09:00:00Z'], 3600),
    ).toBeNull()
  })

  it('no-ops when the series is already contiguous (no gaps)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T10:00:00Z', '2026-06-30T11:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when buckets are off the interval grid (mixed grain)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T09:30:00Z', '2026-06-30T12:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when the dense grid would exceed the safety cap', () => {
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T11:00:01Z'], 1),
    ).toBeNull()
  })
})

type TimeRow = { t: string | null; v: number | null }

describe('denseBarTrace', () => {
  const rows: TimeRow[] = [
    { t: '2026-10-01T00:00:00+00:00', v: 4 },
    { t: '2026-10-01T03:00:00+00:00', v: 9 },
    { t: null, v: 7 },
  ]
  const base = { rows, getTime: (r: TimeRow) => r.t, getValue: (r: TimeRow) => r.v, intervalSeconds: 3600, color: '#111' }

  it('zero-fills gaps and matches +00:00 rows to the Z grid by epoch ms', () => {
    const t = denseBarTrace({ ...base, name: 'n' })
    expect(t.x).toHaveLength(4)
    expect(t.y).toEqual([4, 0, 0, 9])
    expect(t).toMatchObject({ type: 'bar', name: 'n', marker: { color: '#111' } })
    expect('yaxis' in t).toBe(false)
  })

  it('uses a caller-supplied grid and passes yaxis/showlegend through', () => {
    const grid = ['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z']
    const t = denseBarTrace({ ...base, name: 'n', grid, yaxis: 'y2', showlegend: true })
    expect(t.x).toEqual(grid)
    expect(t.y).toEqual([4, 0])
    expect(t).toMatchObject({ yaxis: 'y2', showlegend: true })
  })

  it('falls back to present buckets when no grid can be built', () => {
    const t = denseBarTrace({ ...base, name: 'n', intervalSeconds: 0 })
    expect(t.x).toEqual(['2026-10-01T00:00:00+00:00', '2026-10-01T03:00:00+00:00'])
    expect(t.y).toEqual([4, 9])
  })
})

describe('lineTrace', () => {
  it('defaults to width 2 and omits optional keys', () => {
    const t = lineTrace({ name: 'l', x: ['a'], y: [1], color: '#222' })
    expect(t).toMatchObject({ type: 'scatter', mode: 'lines', line: { color: '#222', width: 2 } })
    expect('yaxis' in t).toBe(false)
    expect('connectgaps' in t).toBe(false)
  })
})

describe('secondary axis', () => {
  it('builds a right-hand overlaying axis', () => {
    expect(secondaryYAxis('Rate', { ticksuffix: '%' })).toEqual({
      title: 'Rate',
      overlaying: 'y',
      side: 'right',
      showgrid: false,
      rangemode: 'tozero',
      ticksuffix: '%',
    })
  })

  it('widens the right margin while keeping the others', () => {
    const l = withSecondaryAxis({ margin: { l: 50, r: 20 } as { r?: number } }, 'Rate')
    expect(l.margin).toEqual({ l: 50, r: 50 })
    expect(l.yaxis2.title).toBe('Rate')
  })
})

function makeBaseParams(overrides: Partial<BuildTrafficDataParams> = {}): BuildTrafficDataParams {
  return {
    aggregates: {
      metric: 'requests',
      interval: '1 minute',
      time_series: [
        { time: '2026-10-20T10:00:00Z', value: 100 },
        { time: '2026-10-20T10:01:00Z', value: 200 },
        { time: '2026-10-20T10:02:00Z', value: 150 },
        { time: '2026-10-20T10:03:00Z', value: 250 },
        { time: '2026-10-20T10:04:00Z', value: 300 },
        { time: '2026-10-20T10:05:00Z', value: 400 },
      ],
    },
    compareAggregates: null,
    compareMode: false,
    compareStartTime: null,
    startTime: '2026-10-20T10:00:00Z',
    endTime: '2026-10-20T10:05:00Z',
    trend: 'off',
    timezone: 'UTC',
    metric: 'requests',
    effectiveInterval: '1 minute',
    hiddenCategories: new Set(),
    catalog: null,
    ...overrides,
  }
}

describe('buildTrafficData golden characterization', () => {
  it('builds primary bar trace, baseline trace, and sliding-window trend line accurately', () => {
    const params = makeBaseParams({
      trend: '5m',
      compareMode: true,
      compareStartTime: '2026-10-19T10:00:00Z',
      compareAggregates: {
        metric: 'requests',
        interval: '1 minute',
        time_series: [
          { time: '2026-10-19T10:00:00Z', value: 90 },
          { time: '2026-10-19T10:01:00Z', value: 180 },
          { time: '2026-10-19T10:02:00Z', value: 140 },
          { time: '2026-10-19T10:03:00Z', value: 220 },
          { time: '2026-10-19T10:04:00Z', value: 280 },
          { time: '2026-10-19T10:05:00Z', value: 350 },
        ],
      },
    })

    const traces = buildTrafficData(params)
    expect(traces).toHaveLength(3)

    const [primary, baseline, trend] = traces
    expect(primary.type).toBe('bar')
    expect(primary.y).toEqual([100, 200, 150, 250, 300, 400])

    expect(baseline.name).toBe('Comparison Range')
    expect(baseline.x).toEqual(primary.x)
    expect(baseline.y).toEqual([90, 180, 140, 220, 280, 350])

    expect(trend.name).toBe('Trend')
    expect(trend.x).toEqual(primary.x)
    expect(trend.y).toEqual([null, null, null, null, 200, 260])
  })

  it('handles categorized 5xx status breakdown and hiddenCategories', () => {
    const params = makeBaseParams({
      metric: '5xx',
      aggregates: {
        metric: '5xx',
        interval: '1 minute',
        time_series: [
          { time: '2026-10-20T10:00:00Z', category: '500', value: 12 },
          { time: '2026-10-20T10:00:00Z', category: '503', value: 4 },
          { time: '2026-10-20T10:01:00Z', category: '500', value: 15 },
          { time: '2026-10-20T10:01:00Z', category: '503', value: 2 },
        ],
      },
      startTime: '2026-10-20T10:00:00Z',
      endTime: '2026-10-20T10:01:00Z',
      hiddenCategories: new Set(['503']),
    })

    const traces = buildTrafficData(params)
    expect(traces).toHaveLength(2)
    expect(traces[0].name).toBe('500')
    expect(traces[0].visible).toBe(true)
    expect(traces[1].name).toBe('503')
    expect(traces[1].visible).toBe('legendonly')
  })
})

function makeAggregates(rowCount: number) {
  const time_series = []
  for (let i = 0; i < rowCount; i++) {
    time_series.push({ time: `2026-06-01T${String(i % 24).padStart(2, '0')}:00:00Z`, value: i })
  }
  return { time_series, metric: 'requests', interval: 'hour' }
}

const defaultAsyncParams = {
  compareAggregates: null,
  compareMode: false,
  compareStartTime: null,
  startTime: null,
  endTime: null,
  trend: 'off',
  timezone: 'UTC',
  metric: 'requests',
  effectiveInterval: 'hour',
  hiddenCategories: new Set<string>(),
  catalog: { fields: [] },
}

describe('buildTrafficDataAsync (sync test path)', () => {
  it('resolves with the same trace shape the sync version produces for a small dataset', async () => {
    const aggregates = makeAggregates(50)
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates })
    expect(Array.isArray(traces)).toBe(true)
    expect(traces.length).toBeGreaterThan(0)
    expect(traces[0]).toMatchObject({ type: 'bar', name: 'requests' })
    expect(traces[0].x).toHaveLength(50)
    expect(traces[0].y).toHaveLength(50)
  })

  it('returns an empty array when there is no time_series data', async () => {
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates: { time_series: [], metric: 'requests' } })
    expect(traces).toEqual([])
  })

  it('handles a large dataset (above WORKER_THRESHOLD) via the sync fallback in test env', async () => {
    const aggregates = makeAggregates(2500)
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates })
    expect(traces[0].x).toHaveLength(2500)
  })

  it('propagates the trend overlay when trend != "off"', async () => {
    const aggregates = makeAggregates(100)
    const traces = await buildTrafficDataAsync({ ...defaultAsyncParams, aggregates, trend: 'auto' })
    expect(traces).toHaveLength(2)
    expect(traces[1].name).toContain('Trend')
  })
})

// ---------------------------------------------------------------------------
// Folded from msw-coverage.test.ts
// ---------------------------------------------------------------------------

const ALLOWED_GAPS = new Set<string>([
  'POST /api/services/:service_id/cron-settings',
  'DELETE /api/services/:service_id/time-range',
  'POST /api/services/:service_id/ngwaf-sync',
  'PATCH /api/services/:service_id/custom-fields/:field_name',
  'DELETE /api/cron-runs/:log_id',
  'POST /api/assets/aggregates',
  'GET /api/assets/aggregates',
  'POST /api/sharing/deploy-frontend',
  'POST /api/sharing/teardown-frontend',
  'GET /api/high-scale/request-facts',
  'POST /api/high-scale/request-facts',
  'GET /api/network/pop-health',
  'GET /api/security/threat-intel',
  'GET /api/cron-runs/:run_id/stream',
  'GET /api/log-extents/stream',
  'GET /api/admin/events/stream',
  'GET /api/usage/operations',
  'GET /api/usage/bandwidth',
  'GET /api/usage/log-activity',
  'GET /api/usage/rum-breakdown',
  'GET /api/admin/compaction-stats',
  'POST /api/admin/consolidate-rollups',
  'POST /api/admin/compact/:service_id',
  'GET /api/admin/compaction-status/:service_id',
  'GET /api/admin/compaction-status',
  'POST /api/admin/rollups/compact/:service_id',
  'POST /api/admin/rollups/compact',
  'POST /api/admin/rollups/heal/:service_id',
  'POST /api/admin/rollups/heal',
  'POST /api/admin/rollup-heal/:service_id',
  'POST /api/admin/rollup-heal',
  'GET /api/admin/rollups/status/:service_id',
  'GET /api/admin/rollups/status',
  'POST /api/admin/rebuild-time-series-rollups',
  'POST /api/admin/partial-hour-merge/:service_id',
  'POST /api/admin/partial-hour-merge',
  'GET /api/admin/partial-hour-status/:service_id',
  'GET /api/admin/partial-hour-status',
  'GET /api/admin/metric-history',
  'GET /api/admin/iceberg-tree',
  'GET /api/admin/usage-log/export',
  'GET /api/admin/slow-queries/count',
  'GET /api/admin/slow-queries',
  'GET /api/download-folder',
  'GET /api/download',
  'GET /api/download-all',
  'GET /api/admin/vcl-health',
  'POST /api/provision/reconcile',
  'POST /api/admin/optimize-now',
  'POST /api/admin/local-compact-now',
  'POST /api/admin/backfill-window',
  'POST /api/admin/backfill-bundle-rollups',
  'POST /api/admin/usage-logging',
  'POST /api/admin/reset-rum',
  'POST /api/provision/teardown',
  'PATCH /api/provision/services/:service_id/ngwaf-workspace',
  'POST /api/admin/expire-snapshots/:service_id',
  'POST /api/admin/metadata-cleanup/:service_id',
  'POST /api/admin/sync/:service_id',
  'POST /api/admin/full-sweep/:service_id',
  'POST /api/admin/gap-heal/:service_id',
  'POST /api/admin/ledger/sweep/:service_id',
  'GET /api/admin/ledger/quarantine',
  'POST /api/admin/rum/sync/:service_id',
  'POST /api/admin/rum/discovery/:service_id',
  'POST /api/services/:service_id/scoring/enable',
  'POST /api/services/:service_id/scoring/disable',
  'GET /api/services/:service_id/scoring/status',
  'GET /api/services/:service_id/scoring/labels',
  'POST /api/services/:service_id/scoring/labels',
  'PATCH /api/services/:service_id/scoring/labels/:label_id',
  'DELETE /api/services/:service_id/scoring/labels/:label_id',
  'GET /api/services/:service_id/scoring/top-flagged',
  'GET /api/services/:service_id/scoring/score-distribution',
  'GET /api/services/:service_id/scoring/latency-timeseries',
  'GET /api/services/:service_id/scoring/compliance-breakdown',
  'GET /api/services/:service_id/scoring/evaluation',
  'GET /api/services/:service_id/scoring/curves',
  'GET /api/services/:service_id/scoring/threshold-preview',
  'POST /api/services/:service_id/scoring/retrain',
  'GET /api/services/:service_id/scoring/sessions/:sid/events',
  'GET /api/services/:service_id/scoring/enforce-threshold',
  'PUT /api/services/:service_id/scoring/enforce-threshold',
  'GET /api/services/:service_id/scoring/exclude-regex',
  'PUT /api/services/:service_id/scoring/exclude-regex',
  'POST /api/services/:service_id/scoring/exclude-regex/validate',
  'GET /api/services/:service_id/scoring/enforce-status-code',
  'PUT /api/services/:service_id/scoring/enforce-status-code',
  'PUT /api/services/:service_id/scoring/l2-enforce',
  'GET /api/services/:service_id/scoring/matrix-versions',
  'POST /api/services/:service_id/scoring/matrix-versions/:version/restore',
  'POST /api/services/:service_id/scoring/rotate-key',
  'GET /api/services/:service_id/scoring/audit',
  'GET /api/services/:service_id/scoring/threshold',
  'PUT /api/services/:service_id/scoring/threshold',
  'GET /api/services/:service_id/scoring/evaluation/per-reason',
  'GET /api/services/:service_id/scoring/dashboard',
  'GET /api/security/proxies/export',
  'GET /api/services/:service_id/rum/analytics',
  'GET /api/services/:service_id/rum/live-events',
  'GET /api/services/:service_id/cmcd/status',
  'POST /api/services/:service_id/cmcd/enable',
  'POST /api/services/:service_id/cmcd/disable',
  'GET /api/services/:service_id/rum/status',
  'POST /api/services/:service_id/rum/settings',
  'POST /api/services/:service_id/rum/enable',
  'POST /api/services/:service_id/rum/disable',
  'GET /api/services/:service_id/rum/beacon-health',
  'GET /api/services/:service_id/rum/health',
  'GET /api/services/:service_id/rum/vitals',
  'GET /api/services/:service_id/rum/errors',
  'POST /api/services/:service_id/rum/upgrade',
  'GET /api/services/:service_id/control-room/:tab',
  'POST /api/services/:service_id/control-room/mitigations',
  'POST /api/services/:service_id/control-room/rules',
  'POST /api/services/:service_id/control-room/allowlist',
  'POST /api/services/:service_id/control-room/big-red-button',
  'POST /api/services/:service_id/control-room/cost-governor',
  'GET /api/services/:service_id/control-room/wizard/state',
  'POST /api/services/:service_id/control-room/wizard/step',
  'GET /api/services/:service_id/realtime-stream',
  'GET /api/services/:service_id/log-field-audit',
  'POST /api/services/:service_id/control-room/correlate',
  'GET /api/services/:service_id/control-room/error-stream',
  'GET /api/debug/state',
  'POST /api/share/logout',
  'GET /api/share/heartbeat',
  'POST /api/share/claim/:token',
  'POST /api/admin/share/invites/:invite_id/claim-token',
  'POST /api/admin/share/backup/export',
  'POST /api/admin/share/backup/import',
  'POST /api/admin/share/gdpr/erase',
  'GET /js/rum.js',
  'GET /js/faro-sdk.js',
  'GET /rum-beacon',
  'POST /rum-beacon',
  'GET /api/debug/page-telemetry',
])

function openapiToMsw(p: string): string {
  return p.replace(/\{([^}]+)\}/g, ':$1')
}

function describeHandler(h: unknown): { method: string; pathname: string } | null {
  const info = (h as { info?: { method?: string; path?: string } })?.info
  if (!info) return null
  const method = String(info.method ?? '').toUpperCase()
  const raw = info.path
  if (!method || typeof raw !== 'string') return null
  try {
    const u = new URL(raw)
    return { method, pathname: u.pathname }
  } catch {
    return { method, pathname: raw }
  }
}

describe('MSW handler coverage vs openapi.json', () => {
  it('every documented (path × method) has an MSW handler or an ALLOWED_GAPS entry', () => {
    const openapiPath = path.resolve(__dirname, '..', '..', 'openapi.json')
    if (!fs.existsSync(openapiPath)) {
      throw new Error(
        `openapi.json not found at ${openapiPath}. Run 'npm run gen:types' (or commit, ` +
          `which triggers the regen-openapi pre-commit hook).`,
      )
    }
    const spec = JSON.parse(fs.readFileSync(openapiPath, 'utf-8')) as {
      paths: Record<string, Record<string, unknown>>
    }

    const covered = new Set<string>()
    for (const h of handlers) {
      const d = describeHandler(h)
      if (d) covered.add(`${d.method} ${d.pathname}`)
    }

    const documented: string[] = []
    for (const [p, methods] of Object.entries(spec.paths)) {
      for (const m of Object.keys(methods)) {
        const method = m.toUpperCase()
        if (method === 'HEAD' || method === 'OPTIONS') continue
        documented.push(`${method} ${openapiToMsw(p)}`)
      }
    }

    const missing = documented.filter(
      (entry) => !covered.has(entry) && !ALLOWED_GAPS.has(entry),
    )

    expect(missing, missing.length > 0 ? buildMissingMessage(missing) : 'all paths covered').toEqual([])
  })

  it('ALLOWED_GAPS does not list any path that DOES have a handler', () => {
    const covered = new Set<string>()
    for (const h of handlers) {
      const d = describeHandler(h)
      if (d) covered.add(`${d.method} ${d.pathname}`)
    }
    const stale = [...ALLOWED_GAPS].filter((entry) => covered.has(entry))
    expect(stale, stale.length > 0 ? `Remove these ALLOWED_GAPS entries: ${stale.join(', ')}` : '').toEqual([])
  })

  it('models disabled ClickHouse without inventing publication health or replay success', async () => {
    const status = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/status?service_id=svc-test')
    expect(await status.json()).toMatchObject({
      service_id: 'svc-test',
      enabled: false,
      health: 'disabled',
      oldest_pending_age_seconds: null,
    })
    const replay = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/replay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ service_id: 'svc-test', dataset_id: 'dataset' }),
    })
    expect(replay.status).toBe(409)
    expect(await replay.json()).toMatchObject({ detail: { error: 'clickhouse_disabled' } })
  })
})

function buildMissingMessage(missing: string[]): string {
  return [
    'OpenAPI documents these endpoints with no MSW handler and no ALLOWED_GAPS entry:',
    ...missing.map((e) => `  - ${e}`),
    '',
    'Fix by EITHER:',
    '  1. Add a handler to frontend/tests/msw/handlers.ts (preferred), OR',
    '  2. Add the entry to ALLOWED_GAPS in this test with a short reason.',
  ].join('\n')
}

// ---------------------------------------------------------------------------
// Folded from middleware.test.ts
// ---------------------------------------------------------------------------

function makeReq(url: string, headers: Record<string, string> = {}): NextRequest {
  const u = new URL(url)
  const hdrMap = new Headers(headers)
  return {
    nextUrl: {
      pathname: u.pathname,
      clone: () => new URL(u.toString()),
      toString: () => u.toString(),
    },
    headers: hdrMap,
    url: url,
  } as NextRequest
}

describe('middleware /admin gate (security)', () => {
  it('redirects /admin to / when request came through Caddy', () => {
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
    expect(res.status).toBe(307)
    const loc = res.headers.get('location') || ''
    const p = new URL(loc).pathname
    expect(p).toBe('/')
  })

  it('allows /admin when request has no Caddy marker (SSH-tunnel path)', () => {
    const req = makeReq('http://localhost/admin')
    const res = middleware(req)
    expect(res.status).toBe(200)
    expect(res.headers.get('location')).toBeNull()
  })

  it('redirects regardless of Host header value (cannot spoof in)', () => {
    const req = makeReq('http://localhost/admin', {
      host: 'localhost',
      'x-proxied-by-caddy': 'true',
    })
    const res = middleware(req)
    expect(res.status).toBe(307)
  })

  it('does NOT gate non-/admin paths', () => {
    const req = makeReq('http://localhost/dashboard', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks /admin sub-paths (e.g. /admin/services)', () => {
    const req = makeReq('http://localhost/admin/services', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
    expect(res.status).toBe(307)
  })

  it('allows /admin sub-paths from local (no marker)', () => {
    const req = makeReq('http://localhost/admin/services')
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('treats a header value other than "true" as not-from-Caddy', () => {
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'maybe' })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks Next.js data requests targeting admin paths with a 403 status when from Caddy', () => {
    const req1 = makeReq('http://localhost/_next/data/build-id/admin.json', { 'x-proxied-by-caddy': 'true' })
    const res1 = middleware(req1)
    expect(res1.status).toBe(403)

    const req2 = makeReq('http://localhost/_next/data/build-id/admin/settings.json', { 'x-proxied-by-caddy': 'true' })
    const res2 = middleware(req2)
    expect(res2.status).toBe(403)
  })

  it('allows Next.js data requests targeting admin paths when local', () => {
    const req = makeReq('http://localhost/_next/data/build-id/admin.json')
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks Server Actions with a 403 status when from Caddy', () => {
    const req = makeReq('http://localhost/', {
      'x-proxied-by-caddy': 'true',
      'next-action': 'some-action-id',
    })
    const res = middleware(req)
    expect(res.status).toBe(403)
  })

  it('allows Server Actions when local', () => {
    const req = makeReq('http://localhost/', {
      'next-action': 'some-action-id',
    })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('does not block /logs for analysts behind Caddy', () => {
    const page = middleware(makeReq('http://localhost/logs', { 'x-proxied-by-caddy': 'true' }))
    expect(page.status).toBe(200)
    expect(page.headers.get('location')).toBeNull()
    const data = middleware(
      makeReq('http://localhost/_next/data/build-id/logs.json', { 'x-proxied-by-caddy': 'true' }),
    )
    expect(data.status).toBe(200)
  })

  describe.each(['/alerts', '/usage'])('analyst-blocked prefix %s', (prefix) => {
    it(`redirects ${prefix} to / when request came through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(307)
      const loc = res.headers.get('location') || ''
      const p = new URL(loc).pathname
      expect(p).toBe('/')
    })

    it(`allows ${prefix} when request has no Caddy marker (admin path)`, () => {
      const req = makeReq(`http://localhost${prefix}`)
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('location')).toBeNull()
    })

    it(`redirects ${prefix} sub-paths through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(307)
    })

    it(`allows ${prefix} sub-paths from local`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`)
      const res = middleware(req)
      expect(res.status).toBe(200)
    })

    it(`blocks Next.js data requests for ${prefix} with 403 when from Caddy`, () => {
      const req = makeReq(`http://localhost/_next/data/build-id${prefix}.json`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(403)
    })
  })

  describe('service ID propagation', () => {
    it('sets x-service-id header when service query parameter is present', () => {
      const nextSpy = vi.spyOn(NextResponse, 'next')
      const req = makeReq('http://localhost/dashboard?service=my-service-id')
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(nextSpy).toHaveBeenCalled()
      const callArgs = nextSpy.mock.calls[0][0]
      expect(callArgs?.request?.headers?.get('x-service-id')).toBe('my-service-id')
      nextSpy.mockRestore()
    })

    it('does not set x-service-id header when service query parameter is absent', () => {
      const nextSpy = vi.spyOn(NextResponse, 'next')
      const req = makeReq('http://localhost/dashboard')
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(nextSpy).toHaveBeenCalled()
      const callArgs = nextSpy.mock.calls[0][0]
      expect(callArgs?.request?.headers?.get('x-service-id')).toBeNull()
      nextSpy.mockRestore()
    })
  })
})

async function withUpstreamServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return {
    port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}

describe('upstream API/JS proxy route handler (ECONNRESET resilience)', () => {
  afterEach(() => {
    delete process.env.API_PROXY_URL
    delete process.env.NEXT_PUBLIC_API_PROXY_URL
  })

  it('middleware passes /api/* and /js/* to route handlers instead of NextResponse.rewrite', () => {
    const apiReq = new NextRequest('http://localhost:3002/api/admin/events/stream?service_id=demo')
    const apiRes = middleware(apiReq)
    expect(apiRes.status).toBe(200)
    expect(apiRes.headers.get('x-middleware-rewrite')).toBeNull()

    const jsReq = new NextRequest('http://localhost:3002/js/rum.js')
    const jsRes = middleware(jsReq)
    expect(jsRes.status).toBe(200)
    expect(jsRes.headers.get('x-middleware-rewrite')).toBeNull()
  })

  it('forwards GET and POST requests with query params, headers, body, and Set-Cookie using connection: close', async () => {
    let seenMethod = ''
    let seenUrl = ''
    let seenConn = ''
    let seenBody = ''
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
      const req = new NextRequest('http://localhost:3002/api/query?range=1h', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-custom': 'val' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      const res = await apiPost(req)
      expect(res.status).toBe(201)
      expect(seenMethod).toBe('POST')
      expect(seenUrl).toBe('/api/query?range=1h')
      expect(seenConn).toBe('close')
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
      if (attempts === 1) {
        req.socket.destroy()
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"healed":true}')
    })
    try {
      const req = new NextRequest('http://localhost:3002/api/bootstrap')
      const res = await apiGet(req)
      expect(res.status).toBe(200)
      expect(await res.json()).toEqual({ healed: true })
      expect(attempts).toBe(2)
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('streams SSE responses and suppresses ECONNRESET logs on client abort or upstream disconnect', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      })
      res.write('data: {"tick":1}\n\n')
      setTimeout(() => {
        req.socket.destroy()
      }, 20)
    })
    try {
      const abortController = new AbortController()
      const req = new Request('http://localhost:3002/api/admin/events/stream', {
        signal: abortController.signal,
      })
      const res = await proxyUpstreamRequest(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('content-type')).toContain('text/event-stream')
      const reader = res.body!.getReader()
      const first = await reader.read()
      expect(new TextDecoder().decode(first.value)).toContain('data: {"tick":1}')
      const second = await reader.read()
      expect(second.done).toBe(true)
      abortController.abort()
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('proxies /js/* routes via the JS route handler', async () => {
    const server = await withUpstreamServer((_req, res) => {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/javascript')
      res.end('window.__RUM__=1;')
    })
    try {
      const req = new NextRequest('http://localhost:3002/js/beacon.js')
      const res = await jsGet(req)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('window.__RUM__=1;')
    } finally {
      await server.close()
    }
  })
})
