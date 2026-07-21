import { useMemo } from 'react'

type Row = Record<string, unknown>

function num(v: unknown): number | null {
  if (v == null) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function str(v: unknown): string | null {
  if (v == null) return null
  return String(v)
}

function bool(v: unknown): boolean {
  if (v === true || v === 1 || v === '1' || v === 'true') return true
  return false
}

export interface StreamSummary {
  durationSeconds: number | null
  avgBitrate: number | null
  topBitrate: number | null
  utilization: number | null
  rebufferCount: number
  startupTimeMs: number | null
  bufferHealthPct: number | null
  totalRequests: number
  videoRequests: number
}

export interface TimelinePoint {
  timestamp: string
  bitrate: number | null
  buffer: number | null
  throughput: number | null
  isRebuffer: boolean
  isStartup: boolean
}

export interface BitrateTier {
  bitrate: number
  count: number
  pct: number
}

export interface BitrateShifts {
  upshifts: number
  downshifts: number
}

export interface BufferBucket {
  label: string
  count: number
  pct: number
}

export interface StarvationEvent {
  timestamp: string
  bitrate: number | null
  buffer: number | null
  throughput: number | null
  url: string | null
}

export interface ObjectTypeDist {
  type: string
  label: string
  count: number
  pct: number
}

export interface NetworkPoint {
  timestamp: string
  throughput: number | null
  rtt: number | null
}

export interface StreamAggregates {
  summary: StreamSummary
  timeline: TimelinePoint[]
  bitrateTiers: BitrateTier[]
  bitrateShifts: BitrateShifts
  bufferBuckets: BufferBucket[]
  starvationEvents: StarvationEvent[]
  objectTypeDist: ObjectTypeDist[]
  networkTimeline: NetworkPoint[]
  pops: string[]
  contentIds: { id: string; count: number }[]
}

const OBJECT_TYPE_LABELS: Record<string, string> = {
  m: 'Manifest', a: 'Audio', v: 'Video', av: 'Muxed A/V',
  i: 'Init Segment', c: 'Caption', tt: 'Timed Text', k: 'Crypto Key', o: 'Other',
}

function computeAggregates(rows: Row[]): StreamAggregates {
  const sorted = [...rows].sort((a, b) => {
    const ta = str(a.timestamp) ?? ''
    const tb = str(b.timestamp) ?? ''
    return ta < tb ? -1 : ta > tb ? 1 : 0
  })

  const videoRows = sorted.filter(r => str(r.cmcd_ot) === 'v')

  // Summary KPIs
  const timestamps = sorted.map(r => str(r.timestamp)).filter(Boolean) as string[]
  let durationSeconds: number | null = null
  if (timestamps.length >= 2) {
    const first = new Date(timestamps[0]).getTime()
    const last = new Date(timestamps[timestamps.length - 1]).getTime()
    durationSeconds = (last - first) / 1000
  }

  const videoBitrates = videoRows.map(r => num(r.cmcd_br)).filter((v): v is number => v !== null)
  const avgBitrate = videoBitrates.length ? videoBitrates.reduce((a, b) => a + b, 0) / videoBitrates.length : null

  const videoTopBitrates = videoRows.map(r => num(r.cmcd_tb)).filter((v): v is number => v !== null)
  const topBitrate = videoTopBitrates.length ? Math.max(...videoTopBitrates) : null

  const utilization = avgBitrate != null && topBitrate != null && topBitrate > 0
    ? avgBitrate / topBitrate
    : null

  const rebufferCount = sorted.filter(r => bool(r.cmcd_bs)).length

  let startupTimeMs: number | null = null
  if (sorted.length > 0) {
    const firstTs = new Date(str(sorted[0].timestamp) ?? '').getTime()
    const firstNonStartup = sorted.find(r => !bool(r.cmcd_su) && str(r.cmcd_ot) === 'v')
    if (firstNonStartup) {
      const nsTs = new Date(str(firstNonStartup.timestamp) ?? '').getTime()
      startupTimeMs = nsTs - firstTs
    }
  }

  const videoBuffers = videoRows.map(r => num(r.cmcd_bl)).filter((v): v is number => v !== null)
  const bufferHealthPct = videoBuffers.length
    ? (videoBuffers.filter(b => b > 0).length / videoBuffers.length) * 100
    : null

  const summary: StreamSummary = {
    durationSeconds,
    avgBitrate,
    topBitrate,
    utilization,
    rebufferCount,
    startupTimeMs,
    bufferHealthPct,
    totalRequests: sorted.length,
    videoRequests: videoRows.length,
  }

  // Timeline
  const timeline: TimelinePoint[] = videoRows.map(r => ({
    timestamp: str(r.timestamp) ?? '',
    bitrate: num(r.cmcd_br),
    buffer: num(r.cmcd_bl),
    throughput: num(r.cmcd_mtp),
    isRebuffer: bool(r.cmcd_bs),
    isStartup: bool(r.cmcd_su),
  }))

  // Bitrate tiers
  const tierCounts = new Map<number, number>()
  for (const br of videoBitrates) {
    tierCounts.set(br, (tierCounts.get(br) ?? 0) + 1)
  }
  const bitrateTiers: BitrateTier[] = [...tierCounts.entries()]
    .sort((a, b) => b[0] - a[0])
    .map(([bitrate, count]) => ({
      bitrate,
      count,
      pct: videoBitrates.length ? (count / videoBitrates.length) * 100 : 0,
    }))

  // Bitrate shifts
  let upshifts = 0
  let downshifts = 0
  for (let i = 1; i < videoBitrates.length; i++) {
    if (videoBitrates[i] > videoBitrates[i - 1]) upshifts++
    else if (videoBitrates[i] < videoBitrates[i - 1]) downshifts++
  }

  // Buffer buckets
  const bucketDefs: [string, (v: number) => boolean][] = [
    ['0 (empty)', v => v === 0],
    ['< 2s', v => v > 0 && v < 2000],
    ['2–10s', v => v >= 2000 && v < 10000],
    ['10–30s', v => v >= 10000 && v < 30000],
    ['30s+', v => v >= 30000],
  ]
  const bufferBuckets: BufferBucket[] = bucketDefs.map(([label, pred]) => {
    const count = videoBuffers.filter(pred).length
    return {
      label,
      count,
      pct: videoBuffers.length ? (count / videoBuffers.length) * 100 : 0,
    }
  })

  // Starvation events
  const starvationEvents: StarvationEvent[] = sorted
    .filter(r => bool(r.cmcd_bs))
    .map(r => ({
      timestamp: str(r.timestamp) ?? '',
      bitrate: num(r.cmcd_br),
      buffer: num(r.cmcd_bl),
      throughput: num(r.cmcd_mtp),
      url: str(r.url),
    }))

  // Object type distribution
  const otCounts = new Map<string, number>()
  for (const r of sorted) {
    const ot = str(r.cmcd_ot) ?? 'unknown'
    otCounts.set(ot, (otCounts.get(ot) ?? 0) + 1)
  }
  const objectTypeDist: ObjectTypeDist[] = [...otCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([type, count]) => ({
      type,
      label: OBJECT_TYPE_LABELS[type] ?? type,
      count,
      pct: sorted.length ? (count / sorted.length) * 100 : 0,
    }))

  // Network timeline
  const networkTimeline: NetworkPoint[] = sorted
    .filter(r => num(r.cmcd_mtp) !== null || num(r.tcp_rtt) !== null)
    .map(r => ({
      timestamp: str(r.timestamp) ?? '',
      throughput: num(r.cmcd_mtp),
      rtt: num(r.tcp_rtt),
    }))

  // POPs
  const popSet = new Set<string>()
  for (const r of sorted) {
    const p = str(r.pop)
    if (p) popSet.add(p)
  }

  // Content IDs
  const cidCounts = new Map<string, number>()
  for (const r of sorted) {
    const cid = str(r.cmcd_cid)
    if (cid) cidCounts.set(cid, (cidCounts.get(cid) ?? 0) + 1)
  }
  const contentIds = [...cidCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([id, count]) => ({ id, count }))

  return {
    summary,
    timeline,
    bitrateTiers,
    bitrateShifts: { upshifts, downshifts },
    bufferBuckets,
    starvationEvents,
    objectTypeDist,
    networkTimeline,
    pops: [...popSet],
    contentIds,
  }
}

export function useStreamAggregates(rows: Row[] | undefined): StreamAggregates | null {
  return useMemo(() => {
    if (!rows?.length) return null
    return computeAggregates(rows)
  }, [rows])
}
