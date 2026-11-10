import { formatAbbreviatedCount } from '@/lib/utils'
import { SERVICE_DEFAULTS } from '@/lib/serviceDefaults'
import type { components } from '@/types/api.generated'

export type PrefillResponse = components['schemas']['PrefillResponse']
export type BillingZone = 'MBZ100' | 'MBZ200' | 'Global'

export const BILLING_ZONE_RATES: Record<BillingZone, number> = {
  MBZ100: 0.08,
  MBZ200: 0.12,
  Global: 0.18,
}

export const BILLING_ZONE_DESCRIPTIONS: Record<BillingZone, string> = {
  MBZ100: 'This zone includes North America, Hawaii and European POPs only. They have the lowest egress charges ($0.08/GB).',
  MBZ200: 'All Fastly POPs except Africa, India, and South Korea where egress charges are highest ($0.12/GB standard default).',
  Global: "All POPs throughout Fastly's entire global network ($0.18/GB).",
}

// Measured on a High-Scale deployment (2026-10-27, 165k request and 90k RUM archive artifacts, R^2
// 0.92-0.97): a request artifact is about 32.8 KB fixed plus 419 bytes per row, a RUM artifact about 22.5 KB fixed
// plus 350 bytes per row. The worker deletes artifacts one day after the service's retention window (data retention
// for requests, RUM retention for RUM), so they are billed for that window plus the margin.
const ARCHIVE_FIXED_BYTES = 32_766
const ARCHIVE_BYTES_PER_ROW = 419
const RUM_ARCHIVE_FIXED_BYTES = 22_500
const RUM_ARCHIVE_BYTES_PER_ROW = 350
const ARCHIVE_RETENTION_MARGIN_DAYS = 1
const ARCHIVE_TICK_SECONDS = 20
// ZSTD compresses structured, repetitive NDJSON edge logs by ~15× (~33% smaller than gzip's ~10×).
export const RAW_LOG_COMPRESSION_RATIO = 15
// Batched RUM beacons share page/session metadata and compress to ~128 B per metric row in ZSTD Parquet (~512 B raw / 4).
export const RUM_COMPRESSED_BYTES_PER_BEACON = 128
// RUM beacons are batched client-side (~4 metrics per /rum-beacon flush) and written as ZSTD-compressed
// objects (~0.11 S3 objects per unpacked beacon row, down from 0.44 before client-side batching).
const RUM_OBJECTS_PER_BEACON = 0.11
// A retention of 0 means keep forever; steady-state storage then has no bound, so model a year.
const KEEP_FOREVER_DAYS = 365

export interface CalcState {
  reqDay: number
  sampleRate: number
  edgeOnly: boolean
  edgeReqDay: number
  rumBeaconsDay: number
  logPeriod: number
  bytesPerLine: number
  logNodes: number
  userEditedNodes: boolean
  logsChecksPerDay: number
  cdnEnabled: boolean
  retentionDays: number
  rumRetentionDays: number
  deleteLogs: boolean
  archiveBatchObjects: number
  rateA: number
  rateB: number
  rateStorage: number
  rateEgress: number
  minDays: number
  billingZone: BillingZone
}

export type CalcAction =
  | { type: 'SET'; key: keyof CalcState; value: number | boolean | BillingZone }
  | { type: 'SET_ZONE'; zone: BillingZone }
  | { type: 'PREFILL'; prefill: PrefillResponse }
  | { type: 'AUTO_NODES' }

export const DEFAULTS: CalcState = {
  reqDay: 1_000_000, sampleRate: SERVICE_DEFAULTS.sampleRatePercent, edgeOnly: true, edgeReqDay: 800_000, rumBeaconsDay: 0,
  logPeriod: SERVICE_DEFAULTS.logPeriodSeconds, bytesPerLine: 500,
  logNodes: 1, userEditedNodes: false,
  logsChecksPerDay: 2,
  cdnEnabled: true, retentionDays: SERVICE_DEFAULTS.logRetentionDays, rumRetentionDays: SERVICE_DEFAULTS.rumRetentionDays,
  deleteLogs: true,
  archiveBatchObjects: 100,
  rateA: 0.005, rateB: 0.001, rateStorage: 0.02, rateEgress: 0.12, minDays: 30,
  billingZone: 'MBZ200',
}

function suggestNodes(reqDay: number) {
  return Math.min(120, Math.max(1, Math.ceil(reqDay / 8_000)))
}

export function reducer(state: CalcState, action: CalcAction): CalcState {
  switch (action.type) {
    case 'SET': {
      const next = { ...state, [action.key]: action.value }
      if (action.key === 'reqDay' && !state.userEditedNodes) next.logNodes = suggestNodes(action.value as number)
      if (action.key === 'logNodes') next.userEditedNodes = true
      if (action.key === 'billingZone') {
        const zone = action.value as BillingZone
        if (BILLING_ZONE_RATES[zone] !== undefined) next.rateEgress = BILLING_ZONE_RATES[zone]
      }
      return next
    }
    case 'SET_ZONE':
      return {
        ...state,
        billingZone: action.zone,
        rateEgress: BILLING_ZONE_RATES[action.zone] ?? state.rateEgress,
      }
    case 'PREFILL': {
      const p = action.prefill
      const req = p.requests_per_day ?? state.reqDay
      const edgeReq = p.edge_requests_per_day ?? state.edgeReqDay
      const lp = p.log_period_seconds ?? state.logPeriod

      let bpl = state.bytesPerLine
      if (p.avg_log_file_size_kb != null && req > 0) {
        const filesPerDay = (86400 / lp) * suggestNodes(req)
        bpl = (p.avg_log_file_size_kb * 1024 * RAW_LOG_COMPRESSION_RATIO * filesPerDay) / req
      } else if (p.estimated_bytes_per_line != null) {
        bpl = p.estimated_bytes_per_line
      }

      const rawZone = p.billing_zone
      const resolvedZone: BillingZone = rawZone && rawZone in BILLING_ZONE_RATES ? (rawZone as BillingZone) : state.billingZone
      const resolvedEgress = p.cdn_egress_rate_per_gb ?? BILLING_ZONE_RATES[resolvedZone] ?? state.rateEgress

      return {
        ...state,
        ...(p.sample_rate != null && { sampleRate: p.sample_rate }),
        reqDay: req,
        edgeReqDay: edgeReq,
        logPeriod: lp,
        bytesPerLine: Math.max(10, Math.round(bpl)),
        rumBeaconsDay: p.rum_beacons_per_day ?? state.rumBeaconsDay,
        ...(p.edge_only != null && { edgeOnly: p.edge_only }),
        ...(p.delete_after != null && { deleteLogs: p.delete_after }),
        ...(p.log_retention_days != null && { retentionDays: p.log_retention_days || KEEP_FOREVER_DAYS }),
        ...(p.rum_retention_days != null && { rumRetentionDays: p.rum_retention_days || KEEP_FOREVER_DAYS }),
        ...(p.class_a_rate_per_1k != null && { rateA: p.class_a_rate_per_1k }),
        ...(p.class_b_rate_per_10k != null && { rateB: p.class_b_rate_per_10k / 10 }),
        rateEgress: resolvedEgress,
        billingZone: resolvedZone,
        ...(p.storage_rate_per_gb_month != null && { rateStorage: p.storage_rate_per_gb_month }),
        ...(p.min_billed_days != null && { minDays: p.min_billed_days }),
        logNodes: p.avg_nodes_per_flush ?? suggestNodes(req),
        userEditedNodes: false,
      }
    }
    case 'AUTO_NODES':
      return !state.userEditedNodes ? { ...state, logNodes: suggestNodes(state.reqDay) } : state
    default:
      return state
  }
}

export interface CalcResults {
  classAPerMonth: number
  classBPerMonth: number
  totalGBStored: number
  cdnEgressGB: number
  costA: number
  costB: number
  costStorage: number
  costEgress: number
  totalCost: number
  logFilesPerMonth: number
  parquetFilesPerMonth: number
  syncsPerMonth: number
  logFilesPerSync: number
  reqDayEffective: number
  objectsPerDay: number
  objectsBilled: number
  classALogsPage: number
  storageTiers: { label: string; gbMonths: number; flagged: boolean }[]
  totalBytesPerMonth: number
  totalGzBytesPerMonth: number
  rumBeaconsMonth: number
  rumParquetGBMonths: number
  costRum: number
}

export interface MonthlyQuantities {
  classAPerMonth: number
  classBPerMonth: number
  gbMonthsStored: number
  cdnEgressGB: number
}

export interface PricedQuantities {
  costA: number
  costB: number
  costStorage: number
  costEgress: number
  totalCost: number
}

// Single pricing step shared by calculate() and the shared load-testing tool (the load-test config's `estimator`).
export function priceQuantities(s: CalcState, q: MonthlyQuantities): PricedQuantities {
  const costA = (q.classAPerMonth / 1000) * s.rateA
  const costB = (q.classBPerMonth / 1000) * s.rateB
  const costStorage = q.gbMonthsStored * s.rateStorage
  const costEgress = q.cdnEgressGB * s.rateEgress
  return { costA, costB, costStorage, costEgress, totalCost: costA + costB + costStorage + costEgress }
}

export function calculate(s: CalcState): CalcResults {
  const baseReqs = s.edgeOnly ? s.edgeReqDay : s.reqDay
  const reqDayEffective = baseReqs * (s.sampleRate / 100)

  const logFilesPerDay = (86400 / s.logPeriod) * s.logNodes
  const logFilesPerMonth = logFilesPerDay * 30

  const totalBytesPerDay = reqDayEffective * s.bytesPerLine
  const totalBytesPerMonth = totalBytesPerDay * 30
  const totalGzBytesPerDay = totalBytesPerDay / RAW_LOG_COMPRESSION_RATIO
  const totalGzBytesPerMonth = totalGzBytesPerDay * 30
  const logSizeKB = (totalGzBytesPerDay / logFilesPerDay) / 1024

  const ingestSeconds = Math.max(10, Math.floor(s.logPeriod / 2))
  const syncsPerDay = (24 * 60 * 60) / ingestSeconds
  const syncsPerMonth = syncsPerDay * 30
  const logFilesPerSync = logFilesPerDay / syncsPerDay

  const minChargeHours = s.minDays * 24
  // Stateless ingest worker archives each page of raw objects as one Parquet artifact plus a manifest and a commit marker.
  const objectsPerTick = (logFilesPerDay * ARCHIVE_TICK_SECONDS) / 86400
  const archivePagesPerDay = Math.ceil(logFilesPerDay / Math.min(Math.max(1, s.archiveBatchObjects), Math.max(1, objectsPerTick)))
  const archiveObjectsPerDay = archivePagesPerDay * 3
  const parquetFilesPerMonth = archivePagesPerDay * 30
  // RUM archive: one artifact per page of RUM objects, and RUM objects follow the beacon volume, not the log cadence.
  const rumObjectsPerDay = s.rumBeaconsDay * RUM_OBJECTS_PER_BEACON
  const rumPagesPerDay = rumObjectsPerDay > 0
    ? Math.ceil(rumObjectsPerDay / Math.min(Math.max(1, s.archiveBatchObjects), Math.max(1, (rumObjectsPerDay * ARCHIVE_TICK_SECONDS) / 86400)))
    : 0
  const objectsPerDay = logFilesPerDay + archiveObjectsPerDay

  const logSteadyStateDays = s.deleteLogs ? 1 / 24 : s.retentionDays
  const billedLogDays = Math.max(logSteadyStateDays, s.minDays)
  const billedArchiveDays = Math.max(s.retentionDays + ARCHIVE_RETENTION_MARGIN_DAYS, s.minDays)

  const objectsBilled = (logFilesPerDay * billedLogDays) + (archiveObjectsPerDay * billedArchiveDays)

  const rawFilesStored = s.deleteLogs ? logFilesPerDay / 24 : logFilesPerDay * s.retentionDays
  const listOpsClassA = Math.max(1, Math.ceil(rawFilesStored / 1000)) * syncsPerMonth
  const classALogsPage = s.logsChecksPerDay * 30
  const stateSyncClassA = ((24 * 60) / 5) * 30
  const rumBeaconsMonth = s.rumBeaconsDay * 30

  const classAPerMonth =
    logFilesPerMonth + listOpsClassA + classALogsPage +
    stateSyncClassA + rumBeaconsMonth + (archivePagesPerDay + rumPagesPerDay) * 30 * 3

  // Queries read ClickHouse, so FOS reads are raw .zst ingest plus archive publish verification reads.
  const classBPerMonth = logFilesPerMonth + (archivePagesPerDay + rumPagesPerDay) * 30 * 3

  const logGBPerFile = logSizeKB / (1024 * 1024)
  const logActualH = s.deleteLogs ? 1 : s.retentionDays * 24
  const logBilledH = Math.max(logActualH, minChargeHours)
  const rawLogGBMonths = (logFilesPerMonth * logGBPerFile * logBilledH) / 720

  // RUM data follows its own retention (rum_retention_days), not the request retention.
  const rumBilledH = Math.max(s.rumRetentionDays * 24, minChargeHours)
  const rumParquetGBMonths = ((s.rumBeaconsDay * 30 * RUM_COMPRESSED_BYTES_PER_BEACON) / (1024 * 1024 * 1024)) * (rumBilledH / 720)
  const archiveBilledH = Math.max((s.retentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24, minChargeHours)
  const archiveBytesPerMonth = archivePagesPerDay * 30 * ARCHIVE_FIXED_BYTES + reqDayEffective * 30 * ARCHIVE_BYTES_PER_ROW
  const archiveGBMonths = (archiveBytesPerMonth / (1024 * 1024 * 1024)) * (archiveBilledH / 720)
  const rumArchiveBilledH = Math.max((s.rumRetentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24, minChargeHours)
  const rumArchiveBytesPerMonth = rumPagesPerDay * 30 * RUM_ARCHIVE_FIXED_BYTES + s.rumBeaconsDay * 30 * RUM_ARCHIVE_BYTES_PER_ROW
  const rumArchiveGBMonths = rumObjectsPerDay > 0 ? (rumArchiveBytesPerMonth / (1024 * 1024 * 1024)) * (rumArchiveBilledH / 720) : 0
  const totalGBStored = rawLogGBMonths + rumParquetGBMonths + archiveGBMonths + rumArchiveGBMonths

  const storageTiers: CalcResults['storageTiers'] = []
  if (rawLogGBMonths > 0) storageTiers.push({ label: 'Raw logs', gbMonths: rawLogGBMonths, flagged: logBilledH > logActualH })
  if (archiveGBMonths > 0) storageTiers.push({ label: 'Archive Parquet', gbMonths: archiveGBMonths, flagged: archiveBilledH > (s.retentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24 })
  if (rumParquetGBMonths > 0) storageTiers.push({ label: 'RUM data', gbMonths: rumParquetGBMonths, flagged: false })
  if (rumArchiveGBMonths > 0) storageTiers.push({ label: 'RUM archive', gbMonths: rumArchiveGBMonths, flagged: rumArchiveBilledH > (s.rumRetentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24 })

  // With a CDN fronting FOS, the raw .zst objects read during ingest are the egress.
  const cdnEgressGB = s.cdnEnabled ? totalGzBytesPerMonth / (1024 * 1024 * 1024) : 0

  const { costA, costB, costStorage, costEgress, totalCost } = priceQuantities(s, {
    classAPerMonth, classBPerMonth, gbMonthsStored: totalGBStored, cdnEgressGB,
  })
  const costRum = (rumBeaconsMonth / 1000) * s.rateA + ((rumParquetGBMonths + rumArchiveGBMonths) * s.rateStorage)

  return {
    classAPerMonth, classBPerMonth, totalGBStored, cdnEgressGB,
    costA, costB, costStorage, costEgress, totalCost,
    logFilesPerMonth, parquetFilesPerMonth, syncsPerMonth, logFilesPerSync,
    reqDayEffective, objectsPerDay, objectsBilled, classALogsPage, storageTiers,
    totalBytesPerMonth, totalGzBytesPerMonth, rumBeaconsMonth, rumParquetGBMonths, costRum,
  }
}

export const fmtN = (n: number): string => formatAbbreviatedCount(n, 2)
export { estimateMonthlyUsage } from '@/lib/cloud-engine/usage/estimate'
export { evaluateCapacity } from '@/lib/cloud-engine/usage/capacity'
