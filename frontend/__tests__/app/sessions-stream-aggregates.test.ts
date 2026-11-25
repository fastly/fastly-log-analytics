import { describe, it, expect } from 'vitest'
import { computeStreamAggregates } from '@/app/sessions/stream/_components/streamAggregates'

describe('computeStreamAggregates', () => {
  it('filters to CMCD rows only (treating empty strings as null) and converts tcp_rtt from microseconds to ms', () => {
    const agg = computeStreamAggregates([
      // Non-CMCD HTML request before/after stream that must NOT skew duration, startup, or totals
      { timestamp: '2026-11-24T22:00:00Z', url: '/player.html', cmcd_ot: '', cmcd_br: null, cmcd_mtp: null, tcp_rtt: 15000, pop: 'SJC' },
      // CMCD manifest at start (startup)
      { timestamp: '2026-11-24T22:05:00Z', url: '/master.m3u8', cmcd_ot: 'm', cmcd_su: 1, cmcd_mtp: 8000, tcp_rtt: 24500, pop: 'IAD' },
      // CMCD video segments
      { timestamp: '2026-11-24T22:05:02Z', url: '/seg1.m4s', cmcd_ot: 'v', cmcd_br: 1500, cmcd_tb: 3000, cmcd_bl: 4000, cmcd_mtp: 9000, tcp_rtt: 22000, pop: 'IAD' },
      { timestamp: '2026-11-24T22:05:06Z', url: '/seg2.m4s', cmcd_ot: 'v', cmcd_br: 3000, cmcd_tb: 3000, cmcd_bl: 8000, cmcd_mtp: 11000, tcp_rtt: 20000, pop: 'IAD' },
      // Non-CMCD beacon after stream
      { timestamp: '2026-11-24T22:15:00Z', url: '/analytics', cmcd_ot: '', cmcd_br: null, tcp_rtt: 18000, pop: 'LAX' },
    ])

    expect(agg.summary.totalRequests).toBe(3)
    expect(agg.summary.videoRequests).toBe(2)
    expect(agg.summary.durationSeconds).toBe(6)
    expect(agg.summary.startupTimeMs).toBe(2000)
    expect(agg.summary.avgBitrate).toBe(2250)
    expect(agg.objectTypeDist.map((o) => o.type)).toEqual(['v', 'm'])
    expect(agg.pops).toEqual(['IAD'])
    expect(agg.networkTimeline).toEqual([
      { timestamp: '2026-11-24T22:05:00Z', throughput: 8000, rtt: 24.5 },
      { timestamp: '2026-11-24T22:05:02Z', throughput: 9000, rtt: 22 },
      { timestamp: '2026-11-24T22:05:06Z', throughput: 11000, rtt: 20 },
    ])
  })
})
