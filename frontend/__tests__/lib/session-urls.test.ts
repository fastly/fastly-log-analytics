import { describe, it, expect } from 'vitest'
import { buildSessionDashboardUrl } from '@/lib/session-urls'

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
