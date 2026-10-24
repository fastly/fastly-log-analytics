/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, test, beforeEach, afterEach, vi } from 'vitest'
import { downloadAsCsv, IP_FAMILY_FIELDS, isIpFamilyField } from '@/lib/utils'
import { formatBytes, statusBadgeClass } from '@/lib/format'
import { formatPopGeo } from '@/lib/pop'
import { shouldLoadRumScript } from '@/lib/ssr/upstream'
import { buildSessionDashboardUrl, buildStreamDetailUrl } from '@/lib/navigation'
import {
  isUserActive,
  msSinceLastInteraction,
  __resetForTests,
  ACTIVE_WINDOW_MS,
} from '@/lib/userActivity'
import { showReadOnlyToast, showToast } from '@/lib/toast'
import { toUTCDate, formatTimeAgo, formatLocalDateTime } from '@/lib/date'
import { resolveRangeWire, DEFAULT_RANGE_TOKEN } from '@/lib/range-wire'
import { ANCHOR_QUANTUM_SECONDS, quantizeAnchor } from '@/lib/time-window'
import { invalidInput } from '../helpers/invalid-input'

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
