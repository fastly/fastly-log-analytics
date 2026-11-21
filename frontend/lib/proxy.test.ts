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

  it('T5-FE-03: gates /admin, /logs, Server Actions, and _next/data under direct K8s Ingress or non-1 markers while allowing mTLS admin-gateway with XFF and localhost', () => {
    const prevSecret = process.env.ADMIN_GATEWAY_SECRET
    const prevReq = process.env.ADMIN_GATEWAY_REQUIRED
    try {
      delete process.env.ADMIN_GATEWAY_SECRET
      delete process.env.ADMIN_GATEWAY_REQUIRED

      // 1. Direct K8s Ingress request without X-Proxied-By-Caddy (X-Forwarded-For + non-loopback Host) -> 307
      const directAdmin = new NextRequest('https://analytics.example.com/admin', {
        headers: { host: 'analytics.example.com', 'x-forwarded-for': '203.0.113.5' },
      })
      expect(proxy(directAdmin).status).toBe(307)

      // 2. Server Action under direct K8s Ingress -> 403
      const directAction = new NextRequest('https://analytics.example.com/', {
        method: 'POST',
        headers: {
          host: 'analytics.example.com',
          'x-forwarded-for': '203.0.113.5',
          'next-action': 'act_evil',
        },
      })
      expect(proxy(directAction).status).toBe(403)

      // 3. /logs is gated for public/Caddy/Ingress callers -> 307 (and _next/data/.../logs.json -> 403), while /query stays accessible (200)
      const caddyLogs = new NextRequest('https://share.example.com/logs', {
        headers: { 'x-proxied-by-caddy': '1' },
      })
      expect(proxy(caddyLogs).status).toBe(307)
      const dataLogs = new NextRequest('https://share.example.com/_next/data/build123/logs.json', {
        headers: { 'x-proxied-by-caddy': '1' },
      })
      expect(proxy(dataLogs).status).toBe(403)
      const caddyQuery = new NextRequest('https://share.example.com/query', {
        headers: { 'x-proxied-by-caddy': '1' },
      })
      expect(proxy(caddyQuery).status).toBe(200)

      // 4. Non-1/true non-empty proxy marker ('yes') still gates /admin -> 307
      const nonBoolMarker = new NextRequest('https://localhost:3000/admin', {
        headers: { host: 'localhost:3000', 'x-proxied-by-caddy': 'yes' },
      })
      expect(proxy(nonBoolMarker).status).toBe(307)

      // 5. Valid mTLS admin-gateway request WITH Caddy X-Forwarded-For -> 200
      const secret = 'm'.repeat(40)
      process.env.ADMIN_GATEWAY_SECRET = secret
      process.env.ADMIN_GATEWAY_REQUIRED = '1'
      const mtlsAdmin = new NextRequest('https://admin.example.com/admin', {
        headers: {
          host: 'admin.example.com',
          'x-admin-gateway-token': secret,
          'x-forwarded-for': '10.0.0.5',
        },
      })
      expect(proxy(mtlsAdmin).status).toBe(200)

      // 6. Spoofed mTLS token alongside public marker -> 307
      const spoofedMtls = new NextRequest('https://admin.example.com/admin', {
        headers: {
          host: 'admin.example.com',
          'x-admin-gateway-token': secret,
          'x-fla-public-gateway': '1',
        },
      })
      expect(proxy(spoofedMtls).status).toBe(307)

      // 7. Local loopback admin without ADMIN_GATEWAY_REQUIRED or proxy headers -> 200
      delete process.env.ADMIN_GATEWAY_REQUIRED
      const localAdmin = new NextRequest('http://localhost:3000/admin')
      expect(proxy(localAdmin).status).toBe(200)
    } finally {
      if (prevSecret === undefined) delete process.env.ADMIN_GATEWAY_SECRET
      else process.env.ADMIN_GATEWAY_SECRET = prevSecret
      if (prevReq === undefined) delete process.env.ADMIN_GATEWAY_REQUIRED
      else process.env.ADMIN_GATEWAY_REQUIRED = prevReq
    }
  })
})
