import { describe, expect, it } from 'vitest'
import { generateOpaqueBucketName, generateOpaqueSubdomainPrefix } from './opaqueNames'

const OPAQUE_RE = /^fla-[0-9a-f]{24}$/
const DNS_S3_LABEL_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/

describe('opaqueNames', () => {
  it('generates DNS/S3-valid lowercase opaque bucket names of 28 chars (<= 63)', () => {
    const bucket = generateOpaqueBucketName()
    expect(bucket).toMatch(OPAQUE_RE)
    expect(bucket).toMatch(DNS_S3_LABEL_RE)
    expect(bucket.length).toBe(28)
    expect(bucket.length).toBeLessThanOrEqual(63)
    expect(bucket).toBe(bucket.toLowerCase())
    expect(bucket.startsWith('fos-')).toBe(false)
  })

  it('generates DNS-valid lowercase opaque subdomain prefixes of 28 chars (<= 63)', () => {
    const prefix = generateOpaqueSubdomainPrefix()
    expect(prefix).toMatch(OPAQUE_RE)
    expect(prefix).toMatch(DNS_S3_LABEL_RE)
    expect(prefix.length).toBe(28)
    expect(prefix.length).toBeLessThanOrEqual(63)
    expect(prefix).toBe(prefix.toLowerCase())
    expect(prefix.startsWith('fos-')).toBe(false)
  })

  it('produces independent values across calls and between bucket and subdomain helpers', () => {
    const buckets = new Set(Array.from({ length: 16 }, () => generateOpaqueBucketName()))
    const prefixes = new Set(Array.from({ length: 16 }, () => generateOpaqueSubdomainPrefix()))
    expect(buckets.size).toBe(16)
    expect(prefixes.size).toBe(16)
    for (const b of buckets) {
      expect(prefixes.has(b)).toBe(false)
    }
  })

  it('never correlates with any service ID or service name', () => {
    const serviceId = '7xKq2Mn9Pv4Wz1Rb'
    const serviceName = 'acme-prod-edge'
    const bucket = generateOpaqueBucketName()
    const prefix = generateOpaqueSubdomainPrefix()
    for (const generated of [bucket, prefix]) {
      expect(generated).not.toContain(serviceId.toLowerCase())
      for (const part of serviceName.split('-')) {
        expect(generated).not.toContain(part)
      }
    }
  })
})
