import { NextRequest, NextResponse } from 'next/server'
import { describe, expect, it, vi } from 'vitest'
import { proxy } from '../proxy'
import { ACTIVE_SERVICE_COOKIE } from './utils'

describe('SEC-02 & SEC-10.3 frontend proxy gating and cookie hardening', () => {
  it('gates /admin when X-FLA-Public-Gateway is true while allowing local admin', () => {
    const pubReq = new NextRequest('https://share.example.com/admin', {
      headers: { 'x-fla-public-gateway': 'true' },
    })
    const pubRes = proxy(pubReq)
    expect(pubRes.status).toBe(307)
    expect(new URL(pubRes.headers.get('location') || '').pathname).toBe('/')

    const localReq = new NextRequest('http://localhost:3000/admin')
    expect(proxy(localReq).status).toBe(200)
  })

  it('treats both "1" and "true" on public-gateway, caddy, and frontend-proxy headers as proxied for /admin and Server Actions', () => {
    for (const [hdr, val] of [
      ['x-fla-public-gateway', '1'],
      ['x-fla-public-gateway', 'true'],
      ['x-proxied-by-caddy', '1'],
      ['x-proxied-by-caddy', 'true'],
      ['x-fla-frontend-proxy', '1'],
      ['x-fla-frontend-proxy', 'true'],
    ] as const) {
      const admReq = new NextRequest('https://share.example.com/admin/settings', {
        headers: { [hdr]: val },
      })
      expect(proxy(admReq).status).toBe(307)

      const actionReq = new NextRequest('https://share.example.com/dashboard', {
        method: 'POST',
        headers: { [hdr]: val, 'next-action': 'act_123' },
      })
      expect(proxy(actionReq).status).toBe(403)
    }
  })

  it('rejects malformed ?service= and cookie values and clears malformed analyst_session_id cookies', () => {
    const nextSpy = vi.spyOn(NextResponse, 'next')

    // Malformed service param with CRLF / path traversal is ignored
    const badSvcReq = new NextRequest('https://share.example.com/dashboard?service=../evil%0d%0ax:1', {
      headers: {
        cookie: `${ACTIVE_SERVICE_COOKIE}=../../bad; analyst_session_id=short`,
      },
    })
    const badRes = proxy(badSvcReq)
    const forwardedHeaders = nextSpy.mock.calls[0]?.[0]?.request?.headers as Headers | undefined
    expect(forwardedHeaders?.get('x-service-id') ?? null).toBeNull()

    // Malformed analyst_session_id cookie (< 16 chars) is cleared with Max-Age=0; HttpOnly; SameSite=strict
    const setCookies = badRes.headers.getSetCookie().join(' | ')
    expect(setCookies).toContain('analyst_session_id=;')
    expect(setCookies).toMatch(/Max-Age=0/i)
    expect(setCookies).toMatch(/HttpOnly/i)
    expect(setCookies).toMatch(/SameSite=strict/i)

    nextSpy.mockClear()

    // Valid service param sets x-service-id and writes ACTIVE_SERVICE_COOKIE
    const okReq = new NextRequest('https://share.example.com/dashboard?service=svc_prod-01')
    const okRes = proxy(okReq)
    const okHeaders = nextSpy.mock.calls[0]?.[0]?.request?.headers as Headers | undefined
    expect(okHeaders?.get('x-service-id')).toBe('svc_prod-01')
    expect(okRes.headers.getSetCookie().join(' | ')).toContain(`${ACTIVE_SERVICE_COOKIE}=svc_prod-01`)

    nextSpy.mockRestore()
  })
})
