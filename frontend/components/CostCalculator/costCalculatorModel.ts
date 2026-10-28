import { formatAbbreviatedCount } from '@/lib/utils'
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

export type DeploymentMode = 'standard' | 'high_scale'

// Measured on the Elevation dev High-Scale deployment (2026-10-27, 165k request and 90k RUM archive artifacts, R^2
// 0.92-0.97): a request artifact is about 32.8 KB fixed plus 419 bytes per row, a RUM artifact about 22.5 KB fixed
// plus 350 bytes per row. The worker deletes artifacts one day after the service's retention window (data retention
// for requests, RUM retention for RUM), so they are billed for that window plus the margin.
const ARCHIVE_FIXED_BYTES = 32_766
const ARCHIVE_BYTES_PER_ROW = 419
const RUM_ARCHIVE_FIXED_BYTES = 22_500
const RUM_ARCHIVE_BYTES_PER_ROW = 350
const ARCHIVE_RETENTION_MARGIN_DAYS = 1
const ARCHIVE_TICK_SECONDS = 20
// RUM beacons land as small raw objects: Elevation dev measured 7,104 RUM objects a day for 16,250 beacon rows.
const RUM_OBJECTS_PER_BEACON = 0.44
// A retention of 0 means keep forever; steady-state storage then has no bound, so model a year.
const KEEP_FOREVER_DAYS = 365

export interface CalcState {
  reqDay: number
  sampleRate: number
  edgeOnly: boolean
  edgeReqDay: number
  rumBeaconsDay: number
  logPeriod: number
  commitMins: number
  bytesPerLine: number
  parquetMB: number
  logNodes: number
  userEditedNodes: boolean
  logsChecksPerDay: number
  cdnEnabled: boolean
  retentionDays: number
  rumRetentionDays: number
  deleteLogs: boolean
  compactionEnabled: boolean
  deploymentMode: DeploymentMode
  archiveBatchObjects: number
  rateA: number
  rateB: number
  rateStorage: number
  rateEgress: number
  minDays: number
  billingZone: BillingZone
}

export type CalcAction =
  | { type: 'SET'; key: keyof CalcState; value: number | boolean | BillingZone | DeploymentMode }
  | { type: 'SET_ZONE'; zone: BillingZone }
  | { type: 'PREFILL'; prefill: PrefillResponse }
  | { type: 'AUTO_NODES' }

export const DEFAULTS: CalcState = {
  reqDay: 1_000_000, sampleRate: 100, edgeOnly: true, edgeReqDay: 800_000, rumBeaconsDay: 0,
  logPeriod: 60, commitMins: 5, bytesPerLine: 500, parquetMB: 20,
  logNodes: 1, userEditedNodes: false,
  logsChecksPerDay: 2,
  cdnEnabled: true, retentionDays: 90, rumRetentionDays: 90, deleteLogs: true,
  compactionEnabled: true, deploymentMode: 'standard', archiveBatchObjects: 100,
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
        bpl = (p.avg_log_file_size_kb * 1024 * 10 * filesPerDay) / req
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
        ...(p.commit_interval_mins != null && { commitMins: p.commit_interval_mins }),
        ...(p.edge_only != null && { edgeOnly: p.edge_only }),
        ...(p.delete_after != null && { deleteLogs: p.delete_after }),
        ...(p.log_retention_days != null && { retentionDays: p.log_retention_days || KEEP_FOREVER_DAYS }),
        ...(p.rum_retention_days != null && { rumRetentionDays: p.rum_retention_days || KEEP_FOREVER_DAYS }),
        ...(p.deployment_mode != null && { deploymentMode: p.deployment_mode === 'high_scale' ? 'high_scale' as const : 'standard' as const }),
        ...(p.compaction_enabled != null && { compactionEnabled: p.compaction_enabled }),
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

export function calculate(s: CalcState): CalcResults {
  const baseReqs = s.edgeOnly ? s.edgeReqDay : s.reqDay
  const reqDayEffective = baseReqs * (s.sampleRate / 100)

  const logFilesPerDay = (86400 / s.logPeriod) * s.logNodes
  const logFilesPerMonth = logFilesPerDay * 30

  const totalBytesPerDay = reqDayEffective * s.bytesPerLine
  const totalBytesPerMonth = totalBytesPerDay * 30
  const totalGzBytesPerDay = totalBytesPerDay / 10
  const totalGzBytesPerMonth = totalGzBytesPerDay * 30
  const logSizeKB = (totalGzBytesPerDay / logFilesPerDay) / 1024

  const syncsPerDay = (24 * 60) / s.commitMins
  const syncsPerMonth = syncsPerDay * 30
  const syncHrs = s.commitMins / 60
  const logFilesPerSync = logFilesPerDay * (syncHrs / 24)

  const rawBytesPerSync = totalBytesPerDay / syncsPerDay
  const parquetBytesPerSync = rawBytesPerSync / 4
  const parquetFilesPerSync = Math.max(1, Math.floor(parquetBytesPerSync / (s.parquetMB * 1024 * 1024)))
  const parquetFilesPerMonth = parquetFilesPerSync * syncsPerMonth
  const actualParquetBytesPerFile = parquetBytesPerSync / parquetFilesPerSync
  const parquetGBPerFile = actualParquetBytesPerFile / (1024 * 1024 * 1024)

  // Committed DuckLake Parquet is pushed to FOS after each commit (the cloud copy used to rebuild local data).
  const minChargeHours = s.minDays * 24
  const rawPqFilesPerDay = parquetFilesPerSync * syncsPerDay
  // High-Scale also archives each page of raw objects as one Parquet artifact plus a manifest and a commit marker.
  const highScale = s.deploymentMode === 'high_scale'
  // A page holds whatever the worker discovered in one tick (up to the page size), so quiet services archive one
  // object per page. Elevation dev measured 2.5 source objects per page on average.
  const objectsPerTick = (logFilesPerDay * ARCHIVE_TICK_SECONDS) / 86400
  const archivePagesPerDay = highScale ? Math.ceil(logFilesPerDay / Math.min(Math.max(1, s.archiveBatchObjects), Math.max(1, objectsPerTick))) : 0
  const archiveObjectsPerDay = archivePagesPerDay * 3
  // RUM archive: one artifact per page of RUM objects, and RUM objects follow the beacon volume, not the log cadence.
  const rumObjectsPerDay = s.rumBeaconsDay * RUM_OBJECTS_PER_BEACON
  const rumPagesPerDay = highScale && rumObjectsPerDay > 0
    ? Math.ceil(rumObjectsPerDay / Math.min(Math.max(1, s.archiveBatchObjects), Math.max(1, (rumObjectsPerDay * ARCHIVE_TICK_SECONDS) / 86400)))
    : 0
  const objectsPerDay = logFilesPerDay + rawPqFilesPerDay + archiveObjectsPerDay

  const logSteadyStateDays = s.deleteLogs ? syncHrs / 24 : s.retentionDays
  const billedLogDays = Math.max(logSteadyStateDays, s.minDays)
  const billedPqDays = Math.max(s.retentionDays, s.minDays)

  const objectsBilled = (logFilesPerDay * billedLogDays) + (rawPqFilesPerDay * billedPqDays) + (archiveObjectsPerDay * billedPqDays)

  const ingestSeconds = Math.max(10, Math.floor(s.logPeriod / 2))
  const ingestsPerMonth = ((24 * 60 * 60) / ingestSeconds) * 30
  const rawFilesStored = s.deleteLogs ? logFilesPerDay / 24 : logFilesPerDay * s.retentionDays
  const listOpsClassA = Math.max(1, Math.ceil(rawFilesStored / 1000)) * ingestsPerMonth
  const classALogsPage = s.logsChecksPerDay * 30
  const stateSyncClassA = syncsPerDay * 30
  const rumBeaconsMonth = s.rumBeaconsDay * 30

  const duckLakeParquetClassA = parquetFilesPerMonth + (s.compactionEnabled ? 30 + parquetFilesPerMonth : 0)
  const classAPerMonth =
    logFilesPerMonth + listOpsClassA + classALogsPage +
    stateSyncClassA + rumBeaconsMonth + duckLakeParquetClassA + (archivePagesPerDay + rumPagesPerDay) * 30 * 3

  // Queries read local data, so FOS reads are the raw .gz ingest only (restores are rare and not modelled).
  // Each archive publish is read back to verify the artifact, manifest and commit marker.
  const classBPerMonth = logFilesPerMonth + (archivePagesPerDay + rumPagesPerDay) * 30 * 3

  const logGBPerFile = logSizeKB / (1024 * 1024)
  const logActualH = s.deleteLogs ? Math.max(1, syncHrs) : s.retentionDays * 24
  const logBilledH = Math.max(logActualH, minChargeHours)
  const rawLogGBMonths = (logFilesPerMonth * logGBPerFile * logBilledH) / 720

  const pqActualH = s.retentionDays * 24
  const pqBilledH = Math.max(pqActualH, minChargeHours)
  const duckLakeDataGBMonths = (parquetFilesPerMonth * parquetGBPerFile * pqBilledH) / 720
  // RUM data follows its own retention (rum_retention_days), not the request retention.
  const rumBilledH = Math.max(s.rumRetentionDays * 24, minChargeHours)
  const rumParquetGBMonths = ((s.rumBeaconsDay * 30 * 1024 / 4) / (1024 * 1024 * 1024)) * (rumBilledH / 720)
  const archiveBilledH = Math.max((s.retentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24, minChargeHours)
  const archiveBytesPerMonth = archivePagesPerDay * 30 * ARCHIVE_FIXED_BYTES + reqDayEffective * 30 * ARCHIVE_BYTES_PER_ROW
  const archiveGBMonths = highScale ? (archiveBytesPerMonth / (1024 * 1024 * 1024)) * (archiveBilledH / 720) : 0
  const rumArchiveBilledH = Math.max((s.rumRetentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24, minChargeHours)
  const rumArchiveBytesPerMonth = rumPagesPerDay * 30 * RUM_ARCHIVE_FIXED_BYTES + s.rumBeaconsDay * 30 * RUM_ARCHIVE_BYTES_PER_ROW
  const rumArchiveGBMonths = highScale ? (rumArchiveBytesPerMonth / (1024 * 1024 * 1024)) * (rumArchiveBilledH / 720) : 0
  const totalGBStored = rawLogGBMonths + duckLakeDataGBMonths + rumParquetGBMonths + archiveGBMonths + rumArchiveGBMonths

  const storageTiers: CalcResults['storageTiers'] = []
  if (rawLogGBMonths > 0) storageTiers.push({ label: 'Raw logs', gbMonths: rawLogGBMonths, flagged: logBilledH > logActualH })
  if (duckLakeDataGBMonths > 0) storageTiers.push({ label: 'DuckLake data', gbMonths: duckLakeDataGBMonths, flagged: pqBilledH > pqActualH })
  if (rumParquetGBMonths > 0) storageTiers.push({ label: 'RUM data', gbMonths: rumParquetGBMonths, flagged: false })
  if (archiveGBMonths > 0) storageTiers.push({ label: 'High-Scale archive', gbMonths: archiveGBMonths, flagged: archiveBilledH > (s.retentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24 })
  if (rumArchiveGBMonths > 0) storageTiers.push({ label: 'High-Scale RUM archive', gbMonths: rumArchiveGBMonths, flagged: rumArchiveBilledH > (s.rumRetentionDays + ARCHIVE_RETENTION_MARGIN_DAYS) * 24 })

  // With a CDN fronting FOS, the raw .gz objects read during ingest are the egress.
  const cdnEgressGB = s.cdnEnabled ? totalGzBytesPerMonth / (1024 * 1024 * 1024) : 0

  const costA = (classAPerMonth / 1000) * s.rateA
  const costB = (classBPerMonth / 1000) * s.rateB
  const costStorage = totalGBStored * s.rateStorage
  const costEgress = cdnEgressGB * s.rateEgress
  const totalCost = costA + costB + costStorage + costEgress
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
