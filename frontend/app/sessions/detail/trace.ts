import type { SessionDetailResponse } from '@/types/api'

export type RequestRow = SessionDetailResponse['data'][number]
export type Lane = 'Edge' | 'Shield' | 'Unspecified'

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function positive(value: unknown): number | null {
  if (typeof value !== 'number' && typeof value !== 'string') return null
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null
}

export function durationMs(value: unknown): number | null {
  const us = positive(value)
  return us === null ? null : us / 1000
}

function urlKey(row: RequestRow): string | null {
  const url = text(row.url)
  if (!url) return null
  if (/^https?:\/\//i.test(url)) return url
  const host = text(row.host).toLowerCase()
  return host ? JSON.stringify([host, url]) : null
}

export function analyzeTrace(rows: RequestRow[]) {
  const requests = rows.map((row) => {
    const time = Date.parse(text(row.timestamp))
    const status = positive(row.status)
    const edge = String(row.edge).toLowerCase()
    const lane: Lane = edge === '1' || edge === 'true' ? 'Edge' : edge === '0' || edge === 'false' ? 'Shield' : 'Unspecified'
    const url = urlKey(row)
    const method = text(row.method).toUpperCase()
    return {
      row, time, lane, endpoint: method && url ? JSON.stringify([lane, method, url]) : null, label: text(row.url),
      status: status !== null && Number.isInteger(status) && status >= 100 && status < 600 ? status : null,
      elapsed: durationMs(row.elapsed),
    }
  }).sort((a, b) => (Number.isFinite(a.time) ? a.time : Infinity) - (Number.isFinite(b.time) ? b.time : Infinity))

  const timed = requests.filter((request) => Number.isFinite(request.time))
  const start = timed[0]?.time
  const end = timed.at(-1)?.time
  const statuses = requests.filter(({ status }) => status !== null).length
  const failures = requests.filter(({ status }) => status !== null && status >= 400)
  const firstFailure = timed.find(({ status }) => status !== null && status >= 400)
  const afterFailure = firstFailure?.endpoint
    ? timed.filter((request) => request.endpoint === firstFailure.endpoint && request.time > firstFailure.time)
    : []
  const recovery = afterFailure.find(({ status }) => status !== null && status >= 200 && status < 300)
  const completionOrdered = firstFailure && recovery && firstFailure.lane !== 'Unspecified'
    && firstFailure.elapsed !== null && recovery.elapsed !== null
    && recovery.time + recovery.elapsed > firstFailure.time + firstFailure.elapsed
  const evidence = firstFailure ? [firstFailure, ...afterFailure.filter((request) => {
    if (recovery && request.time >= recovery.time) return request === recovery
    return request.status !== null && request.status >= 400
  })] : []
  const retryCount = evidence.length - (firstFailure ? 1 : 0) - (recovery ? 1 : 0)
  const slowest = requests.reduce<(typeof requests)[number] | undefined>(
    (max, request) => request.elapsed !== null && (!max || request.elapsed > (max.elapsed ?? 0)) ? request : max,
    undefined,
  )
  const rtts = rows.flatMap(({ tcp_rtt }) => {
    const ms = durationMs(tcp_rtt)
    return ms === null ? [] : [ms]
  }).sort((a, b) => a - b)
  const mid = Math.floor(rtts.length / 2)
  const medianRtt = rtts.length % 2 ? rtts[mid] : rtts.length ? (rtts[mid - 1] + rtts[mid]) / 2 : null
  const urls = new Set(rows.flatMap((row) => {
    const key = urlKey(row) ?? text(row.url)
    return key ? [key] : []
  })).size
  const buckets = new Map<Lane, { slot: number; count: number; errors: number }[]>()
  const edgeCount = requests.filter(({ lane }) => lane === 'Edge').length
  const shieldCount = requests.filter(({ lane }) => lane === 'Shield').length
  if (start !== undefined && end !== undefined) {
    for (const { lane, time, status } of timed) {
      let laneBuckets = buckets.get(lane)
      if (!laneBuckets) {
        laneBuckets = Array.from({ length: 24 }, (_, slot) => ({ slot, count: 0, errors: 0 }))
        buckets.set(lane, laneBuckets)
      }
      const bin = laneBuckets[end === start ? 0 : Math.min(23, Math.floor(24 * (time - start) / (end - start)))]
      bin.count++
      if (status !== null && status >= 400) bin.errors++
    }
  }
  const peak = Math.max(1, ...Array.from(buckets.values()).flat().map(({ count }) => count))

  return { requests, start, end, statuses, failures, firstFailure, recovery, completionOrdered, evidence, retryCount, slowest, medianRtt, urls, buckets, peak, edgeCount, shieldCount }
}
