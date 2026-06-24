import type { components } from '@/types/api.generated'

export type PrefillResponse = components["schemas"]["PrefillResponse"]

// ─── State ────────────────────────────────────────────────────────────────────

export interface CalcState {
  // Traffic
  reqDay: number
  sampleRate: number
  edgeOnly: boolean
  edgeReqDay: number
  // Config
  logPeriod: number
  commitMins: number
  bytesPerLine: number
  parquetMB: number
  logNodes: number
  userEditedNodes: boolean
  cacheEnabled: boolean
  queriesDay: number
  logsChecksPerDay: number
  cdnEnabled: boolean
  retentionDays: number
  deleteLogs: boolean
  icebergOptimizeEnabled: boolean
  activeAnalysts: number
  analystFullSyncsPerMonth: number
  // Rates
  rateA: number
  rateB: number
  rateStorage: number
  rateEgress: number
  minDays: number
}

export type CalcAction =
  | { type: 'SET'; key: keyof CalcState; value: number | boolean }
  | { type: 'PREFILL'; prefill: PrefillResponse }
  | { type: 'AUTO_NODES' }

export const DEFAULTS: CalcState = {
  reqDay: 1_000_000, sampleRate: 100, edgeOnly: true, edgeReqDay: 800_000,
  logPeriod: 60, commitMins: 5, bytesPerLine: 500, parquetMB: 20,
  logNodes: 1, userEditedNodes: false,
  cacheEnabled: true, queriesDay: 50, logsChecksPerDay: 2,
  cdnEnabled: true, retentionDays: 90, deleteLogs: true,
  icebergOptimizeEnabled: true,
  activeAnalysts: 2, analystFullSyncsPerMonth: 1,
  rateA: 0.005, rateB: 0.001, rateStorage: 0.02, rateEgress: 0.12, minDays: 30,
}

function suggestNodes(reqDay: number) {
  // Fastly has ~120 POPs. Empirical data for this service shows ~34 nodes for 278k req/day.
  // 278,000 / 34 is roughly 8,000 requests per node.
  return Math.min(120, Math.max(1, Math.ceil(reqDay / 8_000)))
}

export function reducer(state: CalcState, action: CalcAction): CalcState {
  switch (action.type) {
    case 'SET': {
      const next = { ...state, [action.key]: action.value }
      if (action.key === 'reqDay' && !state.userEditedNodes) {
        next.logNodes = suggestNodes(action.value as number)
      }
      if (action.key === 'logNodes') next.userEditedNodes = true
      return next
    }
    case 'PREFILL': {
      const p = action.prefill
      const req = p.requests_per_day !== undefined && p.requests_per_day !== null ? p.requests_per_day : state.reqDay
      const edgeReq = p.edge_requests_per_day !== undefined && p.edge_requests_per_day !== null ? p.edge_requests_per_day : state.edgeReqDay
      const lp = p.log_period_seconds != null ? p.log_period_seconds : state.logPeriod

      let bpl = state.bytesPerLine
      if (p.avg_log_file_size_kb !== undefined && p.avg_log_file_size_kb !== null && req > 0) {
        const suggestedNodes = suggestNodes(req)
        const filesPerDay = (86400 / lp) * suggestedNodes
        bpl = (p.avg_log_file_size_kb * 1024 * 10 * filesPerDay) / req
      } else if (p.estimated_bytes_per_line !== undefined && p.estimated_bytes_per_line !== null) {
        bpl = p.estimated_bytes_per_line
      }

      return {
        ...state,
        ...(p.sample_rate !== undefined && p.sample_rate !== null && { sampleRate: p.sample_rate }),
        reqDay: req,
        edgeReqDay: edgeReq,
        logPeriod: lp,
        bytesPerLine: Math.max(10, Math.round(bpl)),
        ...(p.commit_interval_mins !== undefined && p.commit_interval_mins !== null && { commitMins: p.commit_interval_mins }),
        ...(p.edge_only !== undefined && p.edge_only !== null && { edgeOnly: p.edge_only }),
        ...(p.delete_after !== undefined && p.delete_after !== null && { deleteLogs: p.delete_after }),
        ...(p.log_retention_days !== undefined && p.log_retention_days !== null && { retentionDays: p.log_retention_days }),
        ...(p.compaction_enabled !== undefined && p.compaction_enabled !== null && { icebergOptimizeEnabled: p.compaction_enabled }),
        ...(p.class_a_rate_per_1k !== undefined && p.class_a_rate_per_1k !== null && { rateA: p.class_a_rate_per_1k }),
        ...(p.class_b_rate_per_10k !== undefined && p.class_b_rate_per_10k !== null && { rateB: p.class_b_rate_per_10k / 10 }), // Calculator uses per 1k rate
        ...(p.cdn_egress_rate_per_gb !== undefined && p.cdn_egress_rate_per_gb !== null && { rateEgress: p.cdn_egress_rate_per_gb }),
        ...(p.storage_rate_per_gb_month !== undefined && p.storage_rate_per_gb_month !== null && { rateStorage: p.storage_rate_per_gb_month }),
        ...(p.min_billed_days !== undefined && p.min_billed_days !== null && { minDays: p.min_billed_days }),
        logNodes: p.avg_nodes_per_flush !== undefined && p.avg_nodes_per_flush !== null ? p.avg_nodes_per_flush : suggestNodes(req),
        userEditedNodes: false,
      }
    }
    case 'AUTO_NODES':
      if (!state.userEditedNodes) return { ...state, logNodes: suggestNodes(state.reqDay) }
      return state
    default:
      return state
  }
}

// ─── Formula ──────────────────────────────────────────────────────────────────

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
}

export function calculate(s: CalcState): CalcResults {
  const baseReqs = s.edgeOnly ? s.edgeReqDay : s.reqDay
  const reqDayEffective = baseReqs * (s.sampleRate / 100)

  const logFilesPerDay = (86400 / s.logPeriod) * s.logNodes
  const logFilesPerMonth = logFilesPerDay * 30

  // Total raw uncompressed bytes per day
  const totalBytesPerDay = reqDayEffective * s.bytesPerLine
  const totalBytesPerMonth = totalBytesPerDay * 30
  // Assuming ~10:1 compression ratio for Fastly JSON to .gz
  const totalGzBytesPerDay = totalBytesPerDay / 10
  const totalGzBytesPerMonth = totalGzBytesPerDay * 30
  // Average .gz file size in KB
  const logSizeKB = (totalGzBytesPerDay / logFilesPerDay) / 1024

  const syncsPerDay = (24 * 60) / s.commitMins
  const syncsPerMonth = syncsPerDay * 30
  const syncHrs = s.commitMins / 60
  const logFilesPerSync = logFilesPerDay * (syncHrs / 24)

  // Use the calculated total bytes to determine parquet sizes
  const rawBytesPerSync = (totalBytesPerDay / syncsPerDay)
  // Parquet compression is roughly 4:1 from uncompressed JSON
  const parquetBytesPerSync = rawBytesPerSync / 4
  const parquetFilesPerSync = Math.max(1, Math.floor(parquetBytesPerSync / (s.parquetMB * 1024 * 1024)))

  const parquetFilesPerMonth = parquetFilesPerSync * syncsPerMonth

  // The actual size of each file is the total bytes per sync divided by the number of files we write,
  // converted to GB. It will never exceed parquetMB.
  const actualParquetBytesPerFile = parquetBytesPerSync / parquetFilesPerSync
  const parquetGBPerFile = actualParquetBytesPerFile / (1024 * 1024 * 1024)

  const minChargeHours = s.minDays * 24

  // Object counts
  const rawPqFilesPerDay = parquetFilesPerSync * syncsPerDay
  const icebergMetadataFilesPerDay = syncsPerDay * 4 // manifests, metadata.json, etc.
  const objectsPerDay = logFilesPerDay + rawPqFilesPerDay + icebergMetadataFilesPerDay

  const logSteadyStateDays = s.deleteLogs ? syncHrs / 24 : s.retentionDays
  const pqSteadyStateDays = s.retentionDays
  const billedLogDays = Math.max(logSteadyStateDays, s.minDays)
  const billedPqDays = Math.max(pqSteadyStateDays, s.minDays)
  const billedMetadataDays = Math.max(s.retentionDays, s.minDays)

  const objectsBilled = (logFilesPerDay * billedLogDays) + (rawPqFilesPerDay * billedPqDays) + (icebergMetadataFilesPerDay * billedMetadataDays)

  // Class A
  const ingestSeconds = Math.max(10, Math.floor(s.logPeriod / 2))
  const ingestsPerDay = (24 * 60 * 60) / ingestSeconds // ingest cron runs at half the log period cadence
  const ingestsPerMonth = ingestsPerDay * 30

  // If logs are deleted, the raw prefix only holds ~1 hour of logs before the commit job deletes them.
  // If not deleted, the prefix holds all logs for the entire retention period!
  const rawFilesStored = s.deleteLogs ? logFilesPerDay / 24 : logFilesPerDay * s.retentionDays
  const listOpsPerIngest = Math.max(1, Math.ceil(rawFilesStored / 1000))
  const listOpsClassA = listOpsPerIngest * ingestsPerMonth

  const classALogsPage = s.logsChecksPerDay * 30
  const stateSyncClassA = syncsPerDay * 30 // Admin writes state to FOS once per commit

  const classAPerMonth =
    logFilesPerMonth +
    parquetFilesPerMonth +
    listOpsClassA +
    classALogsPage +
    stateSyncClassA +
    (s.icebergOptimizeEnabled ? (30 + parquetFilesPerMonth) : 0) // monthly optimize + rewrites

  // Class B
  const cdnHitRate = s.cdnEnabled ? 0.8 : 0
  const cacheHitRate = s.cacheEnabled ? 1.0 : cdnHitRate
  const parquetFilesForQuery = Math.max(1, Math.round(parquetFilesPerMonth / syncsPerMonth))

  // Analyst sync checks FOS directly for metadata pointer (every 2 mins = 720/day)
  // then fetches new manifests and parquet files
  const analystSyncsPerMonth = s.activeAnalysts * 720 * 30
  const analystNewParquetDl = s.activeAnalysts * parquetFilesPerMonth * (1 - cdnHitRate)

  // Analysts occasionally trigger full historical imports (or new analysts join)
  const analystHistoricalDl = s.analystFullSyncsPerMonth * (rawPqFilesPerDay * s.retentionDays) * (1 - cdnHitRate)

  const classBPerMonth = logFilesPerMonth + (s.queriesDay * 30 * parquetFilesForQuery * (1 - cacheHitRate)) + analystSyncsPerMonth + analystNewParquetDl + analystHistoricalDl

  // Storage
  const logGBPerFile = logSizeKB / (1024 * 1024)
  const logActualH = s.deleteLogs ? Math.max(1, syncHrs) : s.retentionDays * 24
  const logBilledH = Math.max(logActualH, minChargeHours)
  const rawLogGBMonths = logFilesPerMonth * logGBPerFile * logBilledH / 720

  const pqActualH = s.retentionDays * 24
  const pqBilledH = Math.max(pqActualH, minChargeHours)
  const icebergDataGBMonths = parquetFilesPerMonth * parquetGBPerFile * pqBilledH / 720

  const metadataGBMonths = icebergMetadataFilesPerDay * 30 * (0.1 / 1024) * billedMetadataDays / 30 // Approx 100KB per metadata file

  const totalGBStored = rawLogGBMonths + icebergDataGBMonths + metadataGBMonths

  const storageTiers: CalcResults['storageTiers'] = []
  if (rawLogGBMonths > 0) storageTiers.push({ label: 'Raw logs', gbMonths: rawLogGBMonths, flagged: logBilledH > logActualH })
  if (icebergDataGBMonths > 0) storageTiers.push({ label: 'Iceberg data', gbMonths: icebergDataGBMonths, flagged: pqBilledH > pqActualH })
  if (metadataGBMonths > 0) storageTiers.push({ label: 'Metadata', gbMonths: metadataGBMonths, flagged: false })

  // CDN egress
  // Iceberg metadata files (manifest list, manifests, metadata.json) are fetched from CDN
  // on every sync check to detect new snapshots — ~4 small files (~5 KB each) per sync.
  const icebergMetaEgressGB = s.cdnEnabled ? (syncsPerMonth * 4 * 5) / (1024 * 1024) : 0
  let cdnEgressGB = 0
  if (s.cdnEnabled) {
    if (s.cacheEnabled) {
      // Local cache: each new parquet file is downloaded once from CDN when it is first seen.
      // Queries then read from local disk — no per-query CDN traffic.
      cdnEgressGB = parquetFilesPerMonth * parquetGBPerFile + icebergMetaEgressGB
    } else {
      // No local cache: every query reads parquet directly through CDN.
      // The CDN itself caches hot files (cdnHitRate), but egress is still charged for all reads.
      cdnEgressGB = (s.queriesDay * 30 * parquetFilesForQuery * parquetGBPerFile) + icebergMetaEgressGB
    }
  }

  const costA = (classAPerMonth / 1000) * s.rateA
  const costB = (classBPerMonth / 1000) * s.rateB
  const costStorage = totalGBStored * s.rateStorage
  const costEgress = cdnEgressGB * s.rateEgress
  const totalCost = costA + costB + costStorage + costEgress

  return {
    classAPerMonth, classBPerMonth, totalGBStored, cdnEgressGB,
    costA, costB, costStorage, costEgress, totalCost,
    logFilesPerMonth, parquetFilesPerMonth, syncsPerMonth, logFilesPerSync,
    reqDayEffective, objectsPerDay, objectsBilled, classALogsPage, storageTiers,
    totalBytesPerMonth, totalGzBytesPerMonth
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

export function fmtN(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString()
}

export function fmtUSD(n: number): string {
  if (n >= 1000) return '$' + n.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  if (n >= 1) return '$' + n.toFixed(2)
  return '$' + n.toFixed(4)
}
