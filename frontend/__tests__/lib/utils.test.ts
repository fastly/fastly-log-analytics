/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { downloadAsCsv } from '@/lib/utils'
import { formatBytes, statusBadgeClass } from '@/lib/format'
import { IP_FAMILY_FIELDS, isIpFamilyField } from '@/lib/pii'
import { formatPopGeo } from '@/lib/pop'
import { shouldLoadRumScript } from '@/lib/ssr/upstream'
import { buildSessionDashboardUrl, buildStreamDetailUrl } from '@/lib/navigation'
import {
  isUserActive,
  msSinceLastInteraction,
  __resetForTests,
  ACTIVE_WINDOW_MS,
} from '@/lib/userActivity'
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
