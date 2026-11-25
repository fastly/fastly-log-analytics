import { useMemo } from 'react'
import { OBJECT_TYPE_LABELS } from '@/app/streaming/_sections/StreamingClient'

export type StreamRow = Record<string, unknown>

const num = (v: unknown): number | null => {
  if (v == null) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

const str = (v: unknown): string | null => {
  if (v == null) return null
  const s = String(v).trim()
  return s === '' ? null : s
}

const bool = (v: unknown): boolean => v === true || v === 1 || v === '1' || v === 'true'

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

export function computeStreamAggregates(rows: StreamRow[]): StreamAggregates {
  const sorted = [...rows].sort((a, b) => {
    const ta = str(a.timestamp) ?? ''
    const tb = str(b.timestamp) ?? ''
    return ta < tb ? -1 : ta > tb ? 1 : 0
  })

  const cmcdRows = sorted.filter(
    (r) =>
      str(r.cmcd_ot) !== null ||
      str(r.cmcd_sid) !== null ||
      str(r.cmcd_cid) !== null ||
      num(r.cmcd_br) !== null ||
      num(r.cmcd_bl) !== null ||
      num(r.cmcd_mtp) !== null ||
      bool(r.cmcd_bs) ||
      bool(r.cmcd_su),
  )
  const traceRows = cmcdRows.length > 0 ? cmcdRows : sorted

  const explicitVideo = traceRows.filter((r) => {
    const ot = str(r.cmcd_ot)
    return ot === 'v' || ot === 'av'
  })
  const videoRows =
    explicitVideo.length > 0
      ? explicitVideo
      : traceRows.filter((r) => num(r.cmcd_br) !== null || num(r.cmcd_bl) !== null)

  const timestamps = traceRows.map((r) => str(r.timestamp)).filter(Boolean) as string[]
  const durationSeconds =
    timestamps.length >= 2
      ? (new Date(timestamps[timestamps.length - 1]).getTime() - new Date(timestamps[0]).getTime()) / 1000
      : null

  const videoBitrates = videoRows.map((r) => num(r.cmcd_br)).filter((v): v is number => v !== null)
  const avgBitrate = videoBitrates.length ? videoBitrates.reduce((a, b) => a + b, 0) / videoBitrates.length : null

  const videoTopBitrates = videoRows.map((r) => num(r.cmcd_tb)).filter((v): v is number => v !== null)
  const topBitrate = videoTopBitrates.length
    ? Math.max(...videoTopBitrates)
    : videoBitrates.length
      ? Math.max(...videoBitrates)
      : null

  const utilization = avgBitrate != null && topBitrate != null && topBitrate > 0 ? avgBitrate / topBitrate : null
  const rebufferCount = traceRows.filter((r) => bool(r.cmcd_bs)).length

  let startupTimeMs: number | null = null
  if (traceRows.length > 0) {
    const firstTs = new Date(str(traceRows[0].timestamp) ?? '').getTime()
    const firstNonStartup = traceRows.find(
      (r) => !bool(r.cmcd_su) && (str(r.cmcd_ot) === 'v' || str(r.cmcd_ot) === 'av' || num(r.cmcd_br) !== null),
    )
    if (firstNonStartup) startupTimeMs = new Date(str(firstNonStartup.timestamp) ?? '').getTime() - firstTs
  }

  const videoBuffers = videoRows.map((r) => num(r.cmcd_bl)).filter((v): v is number => v !== null)
  const bufferHealthPct = videoBuffers.length
    ? (videoBuffers.filter((b) => b > 0).length / videoBuffers.length) * 100
    : null

  const summary: StreamSummary = {
    durationSeconds,
    avgBitrate,
    topBitrate,
    utilization,
    rebufferCount,
    startupTimeMs,
    bufferHealthPct,
    totalRequests: traceRows.length,
    videoRequests: videoRows.length,
  }

  const timeline: TimelinePoint[] = videoRows.map((r) => ({
    timestamp: str(r.timestamp) ?? '',
    bitrate: num(r.cmcd_br),
    buffer: num(r.cmcd_bl),
    throughput: num(r.cmcd_mtp),
    isRebuffer: bool(r.cmcd_bs),
    isStartup: bool(r.cmcd_su),
  }))

  const tierCounts = new Map<number, number>()
  for (const br of videoBitrates) tierCounts.set(br, (tierCounts.get(br) ?? 0) + 1)
  const bitrateTiers: BitrateTier[] = [...tierCounts.entries()]
    .sort((a, b) => b[0] - a[0])
    .map(([bitrate, count]) => ({
      bitrate,
      count,
      pct: videoBitrates.length ? (count / videoBitrates.length) * 100 : 0,
    }))

  let upshifts = 0
  let downshifts = 0
  for (let i = 1; i < videoBitrates.length; i++) {
    if (videoBitrates[i] > videoBitrates[i - 1]) upshifts++
    else if (videoBitrates[i] < videoBitrates[i - 1]) downshifts++
  }

  const bucketDefs: [string, (v: number) => boolean][] = [
    ['0 (empty)', (v) => v === 0],
    ['< 2s', (v) => v > 0 && v < 2000],
    ['2–10s', (v) => v >= 2000 && v < 10000],
    ['10–30s', (v) => v >= 10000 && v < 30000],
    ['30s+', (v) => v >= 30000],
  ]
  const bufferBuckets: BufferBucket[] = bucketDefs.map(([label, pred]) => {
    const count = videoBuffers.filter(pred).length
    return { label, count, pct: videoBuffers.length ? (count / videoBuffers.length) * 100 : 0 }
  })

  const starvationEvents: StarvationEvent[] = traceRows
    .filter((r) => bool(r.cmcd_bs))
    .map((r) => ({
      timestamp: str(r.timestamp) ?? '',
      bitrate: num(r.cmcd_br),
      buffer: num(r.cmcd_bl),
      throughput: num(r.cmcd_mtp),
      url: str(r.url),
    }))

  const otCounts = new Map<string, number>()
  let otTotal = 0
  for (const r of traceRows) {
    const ot = str(r.cmcd_ot)
    if (!ot) continue
    otCounts.set(ot, (otCounts.get(ot) ?? 0) + 1)
    otTotal += 1
  }
  const objectTypeDist: ObjectTypeDist[] = [...otCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([type, count]) => ({
      type,
      label: OBJECT_TYPE_LABELS[type] ?? type,
      count,
      pct: otTotal ? (count / otTotal) * 100 : 0,
    }))

  const networkTimeline: NetworkPoint[] = traceRows
    .filter((r) => num(r.cmcd_mtp) !== null || num(r.tcp_rtt) !== null)
    .map((r) => {
      const rawRtt = num(r.tcp_rtt)
      return {
        timestamp: str(r.timestamp) ?? '',
        throughput: num(r.cmcd_mtp),
        rtt: rawRtt !== null ? rawRtt / 1000 : null,
      }
    })

  const popSet = new Set<string>()
  const cidCounts = new Map<string, number>()
  for (const r of traceRows) {
    const p = str(r.pop)
    if (p) popSet.add(p)
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

export function useStreamAggregates(rows: StreamRow[] | undefined): StreamAggregates | null {
  return useMemo(() => (rows?.length ? computeStreamAggregates(rows) : null), [rows])
}
