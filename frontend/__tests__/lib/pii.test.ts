import { describe, expect, it } from 'vitest'

import { IP_FAMILY_FIELDS, isIpFamilyField } from '@/lib/pii'

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
