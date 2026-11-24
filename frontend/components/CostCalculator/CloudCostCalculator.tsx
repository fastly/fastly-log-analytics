'use client'

import { useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { flexRender, useTable } from '@tanstack/react-table'
import { basicTableFeatures, type BasicColumnDef } from '@/lib/table'
import { buildServiceHref, cn, formatBytes, formatCount, formatUSD } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  DEFAULT_INPUTS,
  DEFAULT_UNITS,
  LIST_PRICE_RATES,
  estimateMonthly,
  type EstimateInputs,
  type EstimateRates,
} from '@/lib/cloud-engine/usage/estimate'
import type { MeasuredEstimate } from '@/lib/cloud-engine/usage/estimatePrefill'
import {
  BILLING_ZONE_DESCRIPTIONS,
  BILLING_ZONE_RATES,
  type BillingZone,
  NumInput,
  ReadOnlyValue,
  ResultRow,
  Row,
  fmtN,
} from './CostCalculator'

export function applyWhatIfs(base: EstimateInputs, edits: Partial<EstimateInputs>, lineBytes?: number): EstimateInputs {
  const inputs = { ...base, ...edits, deleteAfter: true }
  if (edits.requestsPerDay != null && !('edgeRequestsPerDay' in edits) && base.requestsPerDay > 0) {
    inputs.edgeRequestsPerDay = base.edgeRequestsPerDay * (edits.requestsPerDay / base.requestsPerDay)
  }
  const baseLineBytes = base.gzBytesPerLine * base.rawBytesPerGzByte
  if (lineBytes != null && baseLineBytes > 0) {
    const scale = lineBytes / baseLineBytes
    inputs.gzBytesPerLine = base.gzBytesPerLine * scale
    inputs.parquetBytesPerRow = base.parquetBytesPerRow * scale
  }
  return inputs
}

const CLOUD_RATE_ROWS: [keyof EstimateRates, string][] = [
  ['cdn_request_rate_per_10k', 'CDN requests ($/10k)'],
  ['compute_request_rate_per_million', 'Compute requests ($/million)'],
  ['compute_vcpu_rate_per_million_ms', 'Compute vCPU ($/million ms)'],
  ['compute_vcpu_free_ms_per_request', 'Included vCPU (ms/request)'],
  ['compute_vcpu_excess_rate_per_second', 'Excess vCPU ($/second)'],
  ['compute_vcpu_rounding_ms', 'vCPU billing increment (ms)'],
  ['kv_class_a_rate_per_million', 'KV Class A ($/million)'],
  ['kv_class_b_rate_per_million', 'KV Class B ($/million)'],
  ['secret_rate_per_month', 'Secret Store ($/secret-month)'],
]

const CLOUD_OPS_SECTIONS = [
  {
    title: 'Class A (writes & lists)',
    cls: 'py-1 font-semibold text-blue-600 dark:text-blue-400',
    rowCls: 'pl-3 py-0.5',
    valCls: 'text-right',
    rows: [
      ['Fastly writes each log file', '1 op/file'],
      ['Parquet window & summary bundle writes', 'per file + hour/day'],
      ['Hourly compaction & cleanup deletes', 'per batch'],
      ['DR state mirror (meta/cloud_dr_state.json)', 'on config update'],
    ],
  },
  {
    title: 'Class B (reads)',
    cls: 'py-1 mt-2 font-semibold text-amber-600 dark:text-amber-400',
    rowCls: 'pl-3 py-0.5',
    valCls: 'text-right',
    rows: [
      ['Worker reads each .zst during ingest', '1 op/file'],
      ['Summary bundle rollup & compaction reads', 'per window'],
    ],
  },
  {
    title: 'Egress (Transfer Out)',
    cls: 'py-1 mt-2 font-semibold text-orange-600 dark:text-orange-400',
    rowCls: 'pl-3 py-0.5',
    valCls: 'text-right',
    rows: [
      ['Raw .zst logs read through front CDN', 'compressed log volume'],
      ['Browser Parquet/summary fetches & clock probes', 'steady-state baseline'],
    ],
  },
  {
    title: 'Compute, KV & Secrets',
    cls: 'py-1 mt-2 font-semibold text-purple-600 dark:text-purple-400',
    rowCls: 'pl-3 py-0.5',
    valCls: 'text-right',
    rows: [
      ['Ingest workers, cron ticks & UI API', 'Compute reqs + vCPU'],
      ['Catalog, index & cron state', 'KV Class A/B'],
      ['Configured credentials', 'Secret Store / month'],
    ],
  },
] as const

type OpsSectionFlatRow =
  | { kind: 'header'; key: string; title: string; cls: string }
  | { kind: 'row'; key: string; label: string; value: string; rowCls: string; valCls: string }

function CloudOpsSectionsTable() {
  const data = useMemo<OpsSectionFlatRow[]>(() => {
    const out: OpsSectionFlatRow[] = []
    for (const sec of CLOUD_OPS_SECTIONS) {
      out.push({ kind: 'header', key: `hdr-${sec.title}`, title: sec.title, cls: sec.cls })
      for (const [l, r] of sec.rows) {
        out.push({ kind: 'row', key: `${sec.title}-${l}`, label: l, value: r, rowCls: sec.rowCls, valCls: sec.valCls })
      }
    }
    return out
  }, [])
  const columns = useMemo<BasicColumnDef<OpsSectionFlatRow>[]>(
    () => [
      { id: 'label', cell: ({ row }) => (row.original.kind === 'header' ? row.original.title : row.original.label) },
      { id: 'value', cell: ({ row }) => (row.original.kind === 'row' ? row.original.value : null) },
    ],
    [],
  )
  const table = useTable({ features: basicTableFeatures, data, columns })
  return (
    <Table scrollFocusable className="w-full text-xs text-muted-foreground">
      <TableBody>
        {table.getRowModel().rows.map((row) => {
          const item = row.original
          if (item.kind === 'header') {
            return (
              <TableRow key={item.key}>
                <TableCell colSpan={2} className={item.cls}>{item.title}</TableCell>
              </TableRow>
            )
          }
          const cells = row.getVisibleCells()
          return (
            <TableRow key={item.key}>
              <TableCell className={item.rowCls}>{flexRender(cells[0].column.columnDef.cell, cells[0].getContext())}</TableCell>
              <TableCell className={item.valCls}>{flexRender(cells[1].column.columnDef.cell, cells[1].getContext())}</TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

function zoneForRate(rate: number): BillingZone {
  if (Math.abs(rate - BILLING_ZONE_RATES.MBZ100) < 0.001) return 'MBZ100'
  if (Math.abs(rate - BILLING_ZONE_RATES.Global) < 0.001) return 'Global'
  return 'MBZ200'
}

export function CloudCostCalculator({ prefillData, prefillNote, rates: configuredRates, estimatedBytesPerLine }: {
  prefillData?: Partial<MeasuredEstimate>
  prefillNote?: string
  rates?: Partial<Record<keyof EstimateRates, number | null>>
  estimatedBytesPerLine?: number | null
  localStorageGbHours?: number | null
  localStoragePeriodHours?: number | null
}) {
  const router = useRouter()
  const activeServiceId = useServiceStore((st) => st.activeServiceId)
  const [edits, setEdits] = useState<Partial<EstimateInputs>>({})
  const [lineBytes, setLineBytes] = useState<number | undefined>(undefined)
  const [selectedZone, setSelectedZone] = useState<BillingZone | null>(null)

  const base: EstimateInputs = { ...DEFAULT_INPUTS }
  if (estimatedBytesPerLine != null && estimatedBytesPerLine > 0) {
    const defaultLineBytes = DEFAULT_INPUTS.gzBytesPerLine * DEFAULT_INPUTS.rawBytesPerGzByte
    if (defaultLineBytes > 0) {
      const scale = estimatedBytesPerLine / defaultLineBytes
      base.gzBytesPerLine = DEFAULT_INPUTS.gzBytesPerLine * scale
      base.parquetBytesPerRow = DEFAULT_INPUTS.parquetBytesPerRow * scale
    }
  }
  const prefillInputs = prefillData?.inputs
  if (prefillInputs) {
    if (prefillInputs.requestsPerDay !== undefined) base.requestsPerDay = prefillInputs.requestsPerDay
    if (prefillInputs.edgeRequestsPerDay !== undefined) base.edgeRequestsPerDay = prefillInputs.edgeRequestsPerDay
    else if (prefillInputs.requestsPerDay !== undefined && DEFAULT_INPUTS.requestsPerDay > 0) {
      base.edgeRequestsPerDay =
        DEFAULT_INPUTS.edgeRequestsPerDay *
        (prefillInputs.requestsPerDay / DEFAULT_INPUTS.requestsPerDay)
    }
    if (prefillInputs.samplePct !== undefined) base.samplePct = prefillInputs.samplePct
    if (prefillInputs.logPeriodS !== undefined) base.logPeriodS = prefillInputs.logPeriodS
    if (prefillInputs.edgeOnly !== undefined) base.edgeOnly = prefillInputs.edgeOnly
    if (prefillInputs.retentionDays !== undefined) base.retentionDays = prefillInputs.retentionDays
    if (prefillInputs.rumLogPeriodS !== undefined) base.rumLogPeriodS = prefillInputs.rumLogPeriodS
    if (prefillInputs.rumSamplePct !== undefined) base.rumSamplePct = prefillInputs.rumSamplePct
    if (prefillInputs.rumRetentionDays !== undefined) base.rumRetentionDays = prefillInputs.rumRetentionDays
    if (prefillData?.rumEnabled && prefillInputs.rumPageviewsPerDay !== undefined) {
      base.rumPageviewsPerDay = prefillInputs.rumPageviewsPerDay
    }
    if (prefillData?.measuredInputs?.filesPerPeriod && prefillInputs.filesPerPeriod !== undefined) {
      base.filesPerPeriod = prefillInputs.filesPerPeriod
    } else if (prefillInputs.requestsPerDay !== undefined || prefillInputs.edgeRequestsPerDay !== undefined) {
      const effectiveReqs = base.edgeOnly ? base.edgeRequestsPerDay : base.requestsPerDay
      if (effectiveReqs > 0) {
        base.filesPerPeriod = Math.max(1, Math.min(DEFAULT_INPUTS.filesPerPeriod, Math.ceil(effectiveReqs / 8000)))
      }
    }
  }

  const inputs = applyWhatIfs(base, edits, lineBytes)
  const rates = { ...LIST_PRICE_RATES }
  for (const key of Object.keys(rates) as (keyof EstimateRates)[]) {
    const value = prefillData?.rates?.[key] ?? configuredRates?.[key]
    if (value != null) rates[key] = value
  }
  const activeZone = selectedZone ?? zoneForRate(rates.cdn_egress_rate_per_gb)
  if (selectedZone) {
    rates.cdn_egress_rate_per_gb = BILLING_ZONE_RATES[selectedZone]
  }

  const result = estimateMonthly(inputs, DEFAULT_UNITS, rates, prefillData?.compaction, {
    relay: prefillData?.relay ?? 0,
  })
  const set = (key: keyof EstimateInputs) => (value: number | boolean) =>
    setEdits((current) => ({ ...current, [key]: value }))

  const lineBytesUncompressed = Math.round(inputs.gzBytesPerLine * inputs.rawBytesPerGzByte)
  const lineBytesCompressed = Math.max(1, Math.round(inputs.gzBytesPerLine))
  const objectsPerDay = result.filesPerDay + result.summaries.windowFilesPerDay + 25
  const objectsBilled = objectsPerDay * inputs.minBilledDays
  const rateBPer1k = rates.class_b_rate_per_10k / 10

  const bannerText = prefillNote ?? (prefillData?.measuredInputs?.requestsPerDay
    ? `Pre-filled with this service's live request volume (${formatCount(Math.round(base.requestsPerDay))} reqs/day) and logging configuration; all storage, egress, and platform projections use the canonical Cloud model.`
    : undefined)

  return (
    <div className="space-y-6">
      {bannerText && (
        <div className="text-xs text-emerald-700 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 rounded-md px-3 py-2">
          {bannerText}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="space-y-6">
          <section>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Your Traffic &amp; Config</h3>
            <div className="space-y-0">
              <Row label="Total requests per day" tooltip="Total requests your Fastly service handles per day (including both Edge and Shield).">
                <NumInput aria-label="Total requests per day" value={inputs.requestsPerDay} onChange={set('requestsPerDay')} wide />
              </Row>
              <Row label="Log sample rate (%)" tooltip="Percentage of requests logged. 100 = all requests, 1 = 1% of requests.">
                <NumInput aria-label="Log sample rate (%)" value={inputs.samplePct} onChange={set('samplePct')} min={1} max={100} />
              </Row>
              {prefillData?.rumEnabled && (
                <Row label="RUM pageviews per day" tooltip="Estimated Real User Monitoring (RUM) pageviews collected per day.">
                  <NumInput aria-label="RUM pageviews per day" value={inputs.rumPageviewsPerDay} onChange={set('rumPageviewsPerDay')} wide />
                </Row>
              )}
              <Row label="Edge-only logging" tooltip="Only log requests handled by the Edge, omitting Shield requests. Uses the Edge requests per day volume for calculation.">
                <Switch aria-label="Edge-only logging" checked={inputs.edgeOnly} onCheckedChange={set('edgeOnly')} />
              </Row>
              {inputs.edgeOnly && (
                <Row label="Edge requests per day" tooltip="Total requests handled directly by the Edge (excluding Shield requests).">
                  <NumInput aria-label="Edge requests per day" value={Math.round(inputs.edgeRequestsPerDay)} onChange={set('edgeRequestsPerDay')} wide />
                </Row>
              )}
              <Row label="Log file period (seconds)" tooltip="How often Fastly closes and uploads a log file. Controlled by the Fastly logging configuration.">
                <NumInput aria-label="Log file period (seconds)" value={inputs.logPeriodS} onChange={set('logPeriodS')} min={1} />
              </Row>
              <Row label="Log nodes / fan-out" tooltip="Active Fastly POPs/nodes flushing a log file per period. Higher traffic uses more nodes; derived from request volume (~1 node per 8k req/day) when pre-filled.">
                <NumInput aria-label="Log nodes / fan-out" value={inputs.filesPerPeriod} onChange={set('filesPerPeriod')} min={1} max={109} />
              </Row>
              <Row label="Average bytes per log line" tooltip="Average uncompressed bytes per logged request. Compressed in Object Storage (.zst) and Parquet.">
                <NumInput aria-label="Average bytes per log line" value={lineBytesUncompressed} onChange={setLineBytes} min={1} wide />
              </Row>
              <Row label="Data retention (days)" tooltip="How many days Parquet logs and summary bundles are kept in Object Storage. Deleted objects are still billed for the 30-day FOS minimum.">
                <NumInput aria-label="Data retention (days)" value={inputs.retentionDays} onChange={set('retentionDays')} min={1} />
              </Row>
              {prefillData?.rumEnabled && (
                <>
                  <Row label="RUM sample rate (%)" tooltip="Percentage of RUM pageviews sampled.">
                    <NumInput aria-label="RUM sample rate (%)" value={inputs.rumSamplePct} onChange={set('rumSamplePct')} min={1} max={100} />
                  </Row>
                  <Row label="RUM retention (days)" tooltip="How many days RUM Parquet artifacts are retained.">
                    <NumInput aria-label="RUM retention (days)" value={inputs.rumRetentionDays} onChange={set('rumRetentionDays')} min={1} />
                  </Row>
                </>
              )}
            </div>

            <div className="mt-4 p-3 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md space-y-2">
              <div className="text-xs font-semibold text-amber-700 dark:text-amber-400">Minimum Charge per Object</div>
              <Row label="Min. days billed per object (even if deleted early)">
                <ReadOnlyValue value={inputs.minBilledDays} />
              </Row>
              <div className="text-[11px] text-amber-700/80 dark:text-amber-500/80">
                Edit on the <a href={buildServiceHref('/admin', activeServiceId)} className="underline font-medium hover:text-amber-900 dark:hover:text-amber-300">admin page</a>.
              </div>
              <div className="text-xs text-amber-600 dark:text-amber-500 space-y-0.5">
                <div>Objects created/day: <strong>{fmtN(Math.round(objectsPerDay))}</strong></div>
                <div>Sustained billed footprint ({inputs.minBilledDays}d): <strong>{fmtN(Math.round(objectsBilled))}</strong> objects</div>
              </div>
            </div>
          </section>
        </div>

        <div className="space-y-6">
          <section>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Pricing (per 1,000 ops)</h3>
              <Button
                variant="link"
                size="sm"
                className="h-7 p-0 text-[10px] uppercase font-bold text-primary"
                onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}
              >
                Edit in Admin
              </Button>
            </div>

            <div className="mb-4 space-y-2 p-3 bg-muted/40 rounded-lg border border-border/50">
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Delivery Billing Zone
                </span>
                <span className="text-xs font-semibold text-primary">
                  ${rates.cdn_egress_rate_per_gb.toFixed(2)} / GB
                </span>
              </div>
              <div className="grid grid-cols-3 gap-1.5 p-1 bg-muted/80 rounded-md">
                {(['MBZ100', 'MBZ200', 'Global'] as const).map((zone) => {
                  const isSelected = activeZone === zone
                  return (
                    <TooltipProvider key={zone}>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <button
                            type="button"
                            onClick={() => setSelectedZone(zone)}
                            className={cn(
                              'px-2.5 py-1.5 text-xs font-medium rounded transition-all text-center',
                              isSelected
                                ? 'bg-background text-foreground shadow-sm font-semibold'
                                : 'text-muted-foreground hover:text-foreground hover:bg-background/50',
                            )}
                            aria-pressed={isSelected}
                          >
                            {zone}
                          </button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="max-w-[260px] text-xs">
                          <p className="font-semibold mb-1">{zone} (${BILLING_ZONE_RATES[zone].toFixed(2)}/GB)</p>
                          <p>{BILLING_ZONE_DESCRIPTIONS[zone]}</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  )
                })}
              </div>
              <p className="text-[11px] text-muted-foreground leading-normal">
                {BILLING_ZONE_DESCRIPTIONS[activeZone]}
              </p>
            </div>

            <div className="space-y-0">
              <Row label="Class A rate (writes, lists)"><ReadOnlyValue value={rates.class_a_rate_per_1k} /></Row>
              <Row label="Class B rate (reads/downloads)"><ReadOnlyValue value={rateBPer1k} /></Row>
              <Row label="Storage rate (per GB/month)"><ReadOnlyValue value={rates.storage_rate_per_gb_month} /></Row>
              <Row label="CDN egress rate (per GB)"><ReadOnlyValue value={rates.cdn_egress_rate_per_gb} /></Row>
              {CLOUD_RATE_ROWS.map(([key, label]) => (
                <Row key={key} label={label}><ReadOnlyValue value={rates[key]} /></Row>
              ))}
            </div>
            <div className="mt-2 text-[11px] text-muted-foreground">
              Rates are global defaults. Update them on the <a href={buildServiceHref('/admin', activeServiceId)} className="underline font-medium hover:text-foreground">admin page</a>.
            </div>
          </section>

          <section>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">What generates operations</h3>
            <CloudOpsSectionsTable />
          </section>
        </div>
      </div>

      <div className="bg-muted/30 border rounded-lg p-5">
        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4">Monthly Estimate Breakdown</h3>
        <div className="space-y-0">
          {result.lines.map((line) => (
            <ResultRow
              key={line.id}
              label={line.label}
              detail={line.detail}
              cost={formatUSD(line.usd)}
              parts={line.parts?.map((part) => ({ label: part.label, cost: formatUSD(part.usd) }))}
            />
          ))}
          <ResultRow label="Total Estimated Monthly Cost" cost={formatUSD(result.total)} highlight />
        </div>

        <div className="mt-5 pt-4 border-t border-border/40">
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Volume Estimates</h4>
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            {[
              ['Est. log line size (uncompressed)', `${lineBytesUncompressed} B`],
              ['Est. log line size (.zst compressed)', `${lineBytesCompressed} B`],
              ['Logged requests / month', fmtN(Math.round(result.linesPerDay * 30))],
              ['Raw volume / month (uncompressed)', formatBytes(result.linesPerDay * 30 * lineBytesUncompressed)],
              ['Est. volume / month (.zst compressed)', formatBytes(result.linesPerDay * 30 * inputs.gzBytesPerLine)],
              ['Log files written / month', fmtN(Math.round(result.filesPerDay * 30))],
              ['Window Parquet / summary files / month', fmtN(Math.round(result.summaries.windowFilesPerDay * 30))],
              ['Billed footprint (30d min)', `${fmtN(Math.round(objectsBilled))} objects`],
            ].map(([label, value]) => (
              <div key={label} className="flex justify-between text-xs py-0.5">
                <span className="text-muted-foreground">{label}</span>
                <span className="font-medium tabular-nums">{value}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <p className="text-xs text-muted-foreground leading-relaxed">
        * Estimates use this service&apos;s live request volume and logging configuration with the canonical Cloud steady-state model for storage, CDN delivery, Compute, KV, and Secret Store. Storage is billed in GB-hours (1 month = 720 hours) with a {inputs.minBilledDays}-day minimum per object regardless of when it is deleted. Cloud DR state mirroring (<code>meta/cloud_dr_state.json</code>) is event-driven on settings and saved view updates. Actual usage may vary.
      </p>
    </div>
  )
}
