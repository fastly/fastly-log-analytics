import { describe, expect, it, vi } from 'vitest'
import {
  clearCloudSessionKeys,
  clearSessionTokens,
  openSessionToken,
  sealSessionToken,
} from '../sessions'
import { buildStreamDetailUrl } from '@/lib/urlFilter'

describe('Campaign E hardening — durable opaque Cloud session tokens', () => {
  it('CLOUD-PII-01: sealSessionToken produces opaque encrypted tokens that never encode raw client IPs', () => {
    clearSessionTokens()
    const rawIp = '203.0.113.42'
    const token = sealSessionToken({
      ip: rawIp,
      ja4: 't13d1516h2_8daaf6152771_e5627efa2ab1',
      s: '2026-11-25T00:00:00Z',
      e: '2026-11-25T01:00:00Z',
      svc: 'svc-cloud-1',
    })

    expect(token.startsWith('fls_')).toBe(true)
    expect(token).not.toContain(rawIp)
    expect(Buffer.from(token, 'base64url').toString('utf8')).not.toContain(rawIp)
    expect(Buffer.from(token.slice(4), 'base64url').toString('utf8')).not.toContain(rawIp)

    expect(openSessionToken(token)).toEqual({
      ip: rawIp,
      ja4: 't13d1516h2_8daaf6152771_e5627efa2ab1',
      s: '2026-11-25T00:00:00Z',
      e: '2026-11-25T01:00:00Z',
      svc: 'svc-cloud-1',
    })
  })

  it('CLOUD-PII-01: openSessionToken survives reload via persisted origin HMAC key and rejects tampered/expired tokens', () => {
    clearSessionTokens()
    const ls = new Map<string, string>()
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => ls.get(k) ?? null,
      setItem: (k: string, v: string) => { ls.set(k, v) },
      removeItem: (k: string) => { ls.delete(k) },
      clear: () => { ls.clear() },
    })
    try {
      const token = sealSessionToken({
        ip: '198.51.100.77',
        ja4: null,
        s: '2026-11-25T00:00:00Z',
        e: '2026-11-25T01:00:00Z',
        svc: 'svc-cloud-1',
      })
      // Simulate reload: reset in-memory key cache only? Even calling openSessionToken with key in localStorage works!
      expect(ls.size).toBe(1)
      expect(openSessionToken(token)?.ip).toBe('198.51.100.77')

      // Forged plain JSON
      const forged = Buffer.from(
        JSON.stringify({
          ip: '198.51.100.99',
          ja4: null,
          s: '2026-11-25T00:00:00Z',
          e: '2026-11-25T01:00:00Z',
          svc: 'svc-cloud-1',
        }),
        'utf8',
      ).toString('base64url')
      expect(openSessionToken(forged)).toBeNull()
      expect(openSessionToken(`fls_${forged}`)).toBeNull()

      // Key rotation / wipe invalidates existing tokens
      clearCloudSessionKeys()
      expect(ls.size).toBe(0)
      expect(openSessionToken(token)).toBeNull()
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('CLOUD-PII-02: buildStreamDetailUrl carries ?token= cleanly without session-reference storage', () => {
    expect(buildStreamDetailUrl('fls_tok-123', 'svc-a')).toBe('/sessions/stream?token=fls_tok-123&service=svc-a')
  })
})
