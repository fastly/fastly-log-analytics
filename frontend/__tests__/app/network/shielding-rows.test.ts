/**
 * Unit tests for the client-side min-requests recompute that backs the
 * "Min requests" dropdown on /network. The backend ships `anomaly_eligible`
 * (the latency verdict, sample-independent); this helper owns only the
 * sample-count comparison the user is adjusting.
 */
import { describe, it, expect } from 'vitest'
import {
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
} from '@/app/network/shielding-rows'

const eligible = (requests: number) => ({
  edge_pop: 'LAX',
  shield_pop: 'SJC',
  requests,
  efficiency_ratio: 20,
  anomaly_eligible: true,
})

describe('adjustShieldingRows', () => {
  it('marks a route below the floor low_sample and never flags it (even if eligible)', () => {
    const [row] = adjustShieldingRows([eligible(5)], 30)
    expect(row.low_sample).toBe(true)
    expect(row.anomaly_static).toBe(false)
  })

  it('flags an eligible route at/above the floor', () => {
    const [row] = adjustShieldingRows([eligible(200)], 30)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  it('treats the boundary value (requests === floor) as enough samples', () => {
    const [row] = adjustShieldingRows([eligible(30)], 30)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  it('raising the floor reclassifies a previously-flagged route as low_sample', () => {
    const [at30] = adjustShieldingRows([eligible(40)], 30)
    const [at100] = adjustShieldingRows([eligible(40)], 100)
    expect(at30.anomaly_static).toBe(true)
    expect(at100.anomaly_static).toBe(false)
    expect(at100.low_sample).toBe(true)
  })

  it('"No minimum" (0) disables the floor: nothing is low_sample, eligible routes flag', () => {
    const [row] = adjustShieldingRows([eligible(1)], 0)
    expect(row.low_sample).toBe(false)
    expect(row.anomaly_static).toBe(true)
  })

  it('never flags a route the backend deemed latency-ineligible, at any floor', () => {
    const notEligible = { ...eligible(500), anomaly_eligible: false }
    for (const floor of [0, 10, 30, 100]) {
      const [row] = adjustShieldingRows([notEligible], floor)
      expect(row.anomaly_static).toBe(false)
    }
  })

  it('default floor greys eligible routes below it and flags them at/above', () => {
    const rows = adjustShieldingRows(
      [eligible(SHIELDING_MIN_REQUESTS_DEFAULT - 1), eligible(SHIELDING_MIN_REQUESTS_DEFAULT)],
      SHIELDING_MIN_REQUESTS_DEFAULT,
    )
    expect(rows[0]).toMatchObject({ low_sample: true, anomaly_static: false })
    expect(rows[1]).toMatchObject({ low_sample: false, anomaly_static: true })
  })

  it('passes other fields through untouched and treats missing requests as 0', () => {
    const [row] = adjustShieldingRows(
      [{ edge_pop: 'CDG', shield_pop: 'FRA', p50_ms: 12.3, anomaly_eligible: false }],
      30,
    )
    expect(row.edge_pop).toBe('CDG')
    expect(row.p50_ms).toBe(12.3)
    expect(row.low_sample).toBe(true) // missing requests → 0 < 30
  })

  it('is null/undefined-safe', () => {
    expect(adjustShieldingRows(null, 30)).toEqual([])
    expect(adjustShieldingRows(undefined, 30)).toEqual([])
    expect(adjustShieldingRows([], 30)).toEqual([])
  })

  it('the default is one of the offered options', () => {
    const values = SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => o.value)
    expect(values).toContain(String(SHIELDING_MIN_REQUESTS_DEFAULT))
  })
})
