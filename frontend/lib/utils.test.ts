import { describe, expect, it } from 'vitest'
import { IP_FAMILY_FIELDS, isIpFamilyField } from './utils'

const SHARED_18_PII_KEYS = [
  'ip',
  'client_ip',
  'c_ip',
  'ip_address',
  'remote_addr',
  'oip',
  'x-forwarded-for',
  'x_forwarded_for',
  'true-client-ip',
  'true_client_ip',
  'fastly-client-ip',
  'fastly_client_ip',
  'cookie_session',
  'cid',
  'client_id',
  'edge_sid',
  'sid',
  'session_id',
] as const

describe('T5-FE-06: PII field guard covers all 18 canonical SHARED_18_PII_KEYS case-insensitively', () => {
  it('retains IP_FAMILY_FIELDS 4-key tuple while isIpFamilyField matches all 18 canonical PII keys', () => {
    expect(IP_FAMILY_FIELDS).toEqual(['ip', 'client_ip', 'ip_address', 'remote_addr'])
    for (const key of SHARED_18_PII_KEYS) {
      expect(isIpFamilyField(key), key).toBe(true)
      expect(isIpFamilyField(key.toUpperCase()), key.toUpperCase()).toBe(true)
      expect(isIpFamilyField(`  ${key}  `), `padded ${key}`).toBe(true)
    }
    for (const safe of ['url', 'status', 'country', 'pop', 'asn', 'ua', 'cache']) {
      expect(isIpFamilyField(safe), safe).toBe(false)
    }
  })
})
