import { describe, expect, it } from 'vitest'
import {
  clearSessionTokens,
  openSessionToken,
  sealSessionToken,
} from '../sessions'
import {
  clearCloudSessionReferences,
  readCloudSessionReference,
  saveCloudSessionReference,
} from '../session-reference'
import { buildStreamDetailUrl } from '@/lib/urlFilter'

describe('Campaign E hardening — opaque Cloud session tokens & stream ref URLs', () => {
  it('CLOUD-PII-01: sealSessionToken produces opaque tokens that never encode raw client IPs', () => {
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

    expect(openSessionToken(token)).toEqual({
      ip: rawIp,
      ja4: 't13d1516h2_8daaf6152771_e5627efa2ab1',
      s: '2026-11-25T00:00:00Z',
      e: '2026-11-25T01:00:00Z',
      svc: 'svc-cloud-1',
    })
  })

  it('CLOUD-PII-01: openSessionToken rejects forged base64url JSON tokens', () => {
    clearSessionTokens()
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
  })

  it('CLOUD-PII-01: clearCloudSessionReferences clears both stored refs and in-memory session tokens', () => {
    clearCloudSessionReferences()
    const token = sealSessionToken({
      ip: '198.51.100.77',
      ja4: null,
      s: '2026-11-25T00:00:00Z',
      e: '2026-11-25T01:00:00Z',
      svc: 'svc-cloud-1',
    })
    const refId = '11111111-2222-4333-8444-555555555555'
    saveCloudSessionReference(refId, token)
    expect(readCloudSessionReference(refId)).toBe(token)
    expect(openSessionToken(token)?.ip).toBe('198.51.100.77')

    clearCloudSessionReferences()
    expect(readCloudSessionReference(refId)).toBeNull()
    expect(openSessionToken(token)).toBeNull()
  })

  it('CLOUD-PII-02: buildStreamDetailUrl supports ref parameter so Cloud URLs never carry session tokens', () => {
    expect(buildStreamDetailUrl('11111111-2222-4333-8444-555555555555', 'svc-a', 'ref')).toBe(
      '/sessions/stream?ref=11111111-2222-4333-8444-555555555555&service=svc-a',
    )
    expect(buildStreamDetailUrl('tok-123', 'svc-a')).toBe('/sessions/stream?token=tok-123&service=svc-a')
  })
})
