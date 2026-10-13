import { describe, test, expect } from 'vitest'
import { DEFAULTS, reducer, calculate, type PrefillResponse } from '../../../components/CostCalculator/calc'

describe('CostCalculator calc logic with RUM', () => {
  test('returns correct calculations for DEFAULTS', () => {
    const results = calculate(DEFAULTS)
    expect(results.totalCost).toBeGreaterThan(0)
    expect(results.rumBeaconsMonth).toBe(0)
    expect(results.rumParquetGBMonths).toBe(0)
    expect(results.costRum).toBe(0)
  })

  test('correctly calculates RUM beacon costs and storage when rumBeaconsDay is set', () => {
    const state = {
      ...DEFAULTS,
      rumBeaconsDay: 10000 // 10k beacons per day
    }
    const results = calculate(state)

    // 10k * 30 = 300,000 beacons per month
    expect(results.rumBeaconsMonth).toBe(300000)

    // RUM storage in GB-months:
    // 300,000 beacons * 1024 bytes (1 KB) = 307,200,000 bytes uncompressed
    // Compressed 4:1 to parquet = 76,800,000 bytes
    // 76,800,000 / (1024 * 1024 * 1024) = 0.071525 GB-months
    expect(results.rumParquetGBMonths).toBeCloseTo(0.071525, 4)

    // RUM Cost portion:
    // Class A cost: (300,000 / 1000) * 0.005 = $1.50
    // Storage cost: 0.0715255 * 0.02 = $0.00143
    // Total RUM portion: $1.50143
    expect(results.costRum).toBeCloseTo(1.50143, 4)

    // Verify RUM data storage is in storageTiers
    const rumTier = results.storageTiers.find(t => t.label === 'RUM data')
    expect(rumTier).toBeDefined()
    expect(rumTier?.gbMonths).toBeCloseTo(0.071525, 4)
  })

  test('reducer PREFILL handles rum_beacons_per_day correctly', () => {
    const prefill = {
      rum_beacons_per_day: 5000,
      requests_per_day: 1000000,
      edge_requests_per_day: 800000,
      log_period_seconds: 60,
      commit_interval_mins: 5,
      sample_rate: 100,
      edge_only: true,
      compaction_enabled: true,
      delete_after: true,
      log_retention_days: 90,
      class_a_rate_per_1k: 0.005,
      class_b_rate_per_10k: 0.01,
      cdn_egress_rate_per_gb: 0.12,
      storage_rate_per_gb_month: 0.02,
      min_billed_days: 30,
      avg_nodes_per_flush: 1,
    }
    const nextState = reducer(DEFAULTS, { type: 'PREFILL', prefill: prefill as any as PrefillResponse })
    expect(nextState.rumBeaconsDay).toBe(5000)
  })

  test('Billing Zones (MBZ100, MBZ200, Global) configure correct rates and calculate egress costs', () => {
    // 1. Default is MBZ200 ($0.12/GB)
    expect(DEFAULTS.billingZone).toBe('MBZ200')
    expect(DEFAULTS.rateEgress).toBe(0.12)

    // 2. Switch to MBZ100 ($0.08/GB)
    const mbz100State = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'MBZ100' })
    expect(mbz100State.billingZone).toBe('MBZ100')
    expect(mbz100State.rateEgress).toBe(0.08)

    // 3. Switch to Global ($0.18/GB)
    const globalState = reducer(DEFAULTS, { type: 'SET_ZONE', zone: 'Global' })
    expect(globalState.billingZone).toBe('Global')
    expect(globalState.rateEgress).toBe(0.18)

    // 4. Verify calculation differences across zones for identical traffic
    const resMBZ100 = calculate(mbz100State)
    const resMBZ200 = calculate(DEFAULTS)
    const resGlobal = calculate(globalState)

    expect(resMBZ100.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)
    expect(resGlobal.cdnEgressGB).toBe(resMBZ200.cdnEgressGB)

    expect(resMBZ100.costEgress).toBeCloseTo(resMBZ100.cdnEgressGB * 0.08, 4)
    expect(resMBZ200.costEgress).toBeCloseTo(resMBZ200.cdnEgressGB * 0.12, 4)
    expect(resGlobal.costEgress).toBeCloseTo(resGlobal.cdnEgressGB * 0.18, 4)

    expect(resMBZ100.costEgress).toBeLessThan(resMBZ200.costEgress)
    expect(resMBZ200.costEgress).toBeLessThan(resGlobal.costEgress)
  })

  test('PREFILL respects billing_zone and maps corresponding rate', () => {
    const prefill = {
      billing_zone: 'MBZ100',
      cdn_egress_rate_per_gb: 0.08,
    }
    const state = reducer(DEFAULTS, { type: 'PREFILL', prefill: prefill as any as PrefillResponse })
    expect(state.billingZone).toBe('MBZ100')
    expect(state.rateEgress).toBe(0.08)
  })
})
