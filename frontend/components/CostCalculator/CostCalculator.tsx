'use client'

import React, { useEffect, useMemo, useReducer } from 'react'
import { useRouter } from 'next/navigation'
import { cn, buildServiceHref } from '@/lib/utils'
import { formatBytes, formatUSD } from '@/lib/format'
import { useServiceStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  reducer,
  DEFAULTS,
  calculate,
  fmtN,
  BILLING_ZONE_RATES,
  BILLING_ZONE_DESCRIPTIONS,
} from './calc'
import type { CalcState, CalcResults, PrefillResponse, BillingZone } from './calc'

// ─── Shared Parts ────────────────────────────────────────────────────────────

function hasAccessibleName(el: React.ReactElement): boolean {
  const p = el.props as { 'aria-label'?: unknown; 'aria-labelledby'?: unknown }
  return p['aria-label'] != null || p['aria-labelledby'] != null
}

export function Row({ label, children, muted, tooltip }: { label: string; children: React.ReactNode; muted?: boolean; tooltip?: string }) {
  const labelledChild =
    React.isValidElement(children) && !hasAccessibleName(children)
      ? React.cloneElement(children as React.ReactElement<{ 'aria-label'?: string }>, { 'aria-label': label })
      : children
  return (
    <div className={cn('flex items-center justify-between py-1.5 border-b border-border/40 last:border-0 gap-4', muted && 'opacity-60')}>
      <LabelWithInfo
        label={label}
        info={tooltip}
        className="gap-1.5 text-sm text-muted-foreground flex-1 leading-tight"
        labelClassName="text-sm font-normal leading-tight"
      />
      <div className='flex-shrink-0'>{labelledChild}</div>
    </div>
  )
}

export function NumInput({ id, value, onChange, step, min, max, wide, 'aria-label': ariaLabel }: {
  id?: string; value: number; onChange: (v: number) => void
  step?: number; min?: number; max?: number; wide?: boolean
  'aria-label'?: string
}) {
  return (
    <Input
      id={id}
      aria-label={ariaLabel}
      type='number'
      value={value}
      step={step ?? 1}
      min={min ?? 0}
      max={max}
      onChange={(e) => { const v = parseFloat(e.target.value); if (!isNaN(v)) onChange(v) }}
      className={cn('text-right h-7 text-sm', wide ? 'w-32' : 'w-24')}
    />
  )
}

export function ReadOnlyValue({ value, wide }: { value: string | number; wide?: boolean }) {
  return (
    <div className={cn('text-right h-7 text-sm flex items-center justify-end px-3 rounded-md bg-muted/40 border border-transparent font-mono tabular-nums text-muted-foreground', wide ? 'w-32' : 'w-24')}>
      {value}
    </div>
  )
}

export function ResultRow({ label, detail, cost, highlight }: {
  label: string; detail?: string; cost: string; highlight?: boolean
}) {
  return (
    <div className={cn('flex items-center justify-between py-2 border-b border-border/40 last:border-0', highlight && 'border-t-2 border-border pt-3 mt-2')}>
      <div>
        <div className={cn('text-sm font-medium', highlight && 'text-base')}>{label}</div>
        {detail && <div className='text-xs text-muted-foreground mt-0.5'>{detail}</div>}
      </div>
      <div className={cn('font-bold tabular-nums', highlight ? 'text-xl text-emerald-500' : 'text-sm')}>{cost}</div>
    </div>
  )
}

// ─── Inputs ──────────────────────────────────────────────────────────────────

interface InputsProps {
  s: CalcState
  r: CalcResults
  set: (key: keyof CalcState) => (value: number | boolean) => void
}

export function Inputs({ s, r, set }: InputsProps) {
  const activeServiceId = useServiceStore((st) => st.activeServiceId)
  return (
    <section>
      <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>Your Traffic &amp; Config</h3>
      <div className='space-y-0'>
        <Row label='Total requests per day' tooltip="Total requests your Fastly service handles per day (including both Edge and Shield).">
          <NumInput value={s.reqDay} onChange={set('reqDay')} wide />
        </Row>
        <Row label='Log sample rate (%)' tooltip="Percentage of requests logged. 100 = all requests, 1 = 1% of requests.">
          <NumInput value={s.sampleRate} onChange={set('sampleRate')} min={1} max={100} />
        </Row>
        <Row label='Estimated RUM Beacons / day' tooltip="Estimated beacons collected by Real User Monitoring (RUM) scripts on client browsers. Each beacon generates 1 Class A FOS write.">
          <NumInput value={s.rumBeaconsDay} onChange={set('rumBeaconsDay')} wide />
        </Row>
        <Row label='Edge-only logging' tooltip="Only log requests handled by the Edge, omitting Shield requests. This uses the 'Edge requests per day' volume for calculation.">
          <Switch aria-label="Edge-only logging" checked={s.edgeOnly} onCheckedChange={set('edgeOnly')} />
        </Row>
        {s.edgeOnly && (
          <Row label='Edge requests per day' tooltip="Total requests handled directly by the Edge (excluding Shield requests). If 'Edge only logging' is enabled, this lower volume is used to calculate costs.">
            <NumInput value={s.edgeReqDay} onChange={set('edgeReqDay')} wide />
          </Row>
        )}
        <Row label='Log file period (seconds)' tooltip="How often Fastly closes and uploads a log file. Controlled by the Fastly logging configuration.">
          <NumInput value={s.logPeriod} onChange={set('logPeriod')} min={1} />
        </Row>
        <Row label='Cloud Commit Interval (minutes)' tooltip="How often the local buffer is flushed to the shared Iceberg table in FOS, creating new snapshots (e.g., 5).">
          <NumInput value={s.commitMins} onChange={set('commitMins')} min={1} />
        </Row>
        <Row label='Average bytes per log line' tooltip="The average uncompressed bytes per request based on your selected fields. Used to calculate storage sizes.">
          <NumInput value={s.bytesPerLine} onChange={set('bytesPerLine')} min={1} />
        </Row>
        <Row label='Parquet target size (MB)' tooltip="Target file size for the nightly FOS Iceberg optimize. Larger targets mean fewer rewritten objects and lower FOS storage cost.">
          <NumInput value={s.parquetMB} onChange={set('parquetMB')} min={1} />
        </Row>
        <Row label='Log nodes / fan-out' tooltip="Estimated number of Fastly aggregators/cache nodes flushing per period. Higher traffic = more nodes.">
          <NumInput value={s.logNodes} onChange={set('logNodes')} min={1} max={72} />
        </Row>
        <Row label='Local parquet cache enabled' tooltip="Download and cache Parquet files locally to avoid paying Class B operations on every query.">
          <Switch aria-label="Local parquet cache enabled" checked={s.cacheEnabled} onCheckedChange={set('cacheEnabled')} />
        </Row>
        <Row label='Dashboard page loads per day' tooltip="Page loads across dashboard, charts, etc. Costs FOS reads if local cache is disabled.">
          <NumInput value={s.queriesDay} onChange={set('queriesDay')} />
        </Row>
        <Row label='Manual log checks per day' tooltip="Each click of 'Refresh' on the Ingestion tab performs 1 Class A list operation against FOS.">
          <NumInput value={s.logsChecksPerDay} onChange={set('logsChecksPerDay')} />
        </Row>
        <Row label='CDN fronting FOS reads' tooltip="Use a Fastly CDN service to cache reads from Fastly Object Storage, reducing Class B operations.">
          <Switch aria-label="CDN fronting FOS reads" checked={s.cdnEnabled} onCheckedChange={set('cdnEnabled')} />
        </Row>
        <Row label='Data retention (days)' tooltip="How many days to keep data in Object Storage before deleting.">
          <NumInput value={s.retentionDays} onChange={set('retentionDays')} min={1} />
        </Row>
        <Row label='Auto-delete raw .gz logs after ingest' tooltip="Delete raw .gz log files immediately after ingesting them into Iceberg. They will still be billed for the FOS minimum 30 days, but doing this prevents redundant long-term storage since Iceberg writes its own optimized Parquet files.">
          <Switch aria-label="Auto-delete raw .gz logs after ingest" checked={s.deleteLogs} onCheckedChange={set('deleteLogs')} />
        </Row>
        <Row label='Iceberg table optimization enabled' tooltip="Periodically rewrite and merge small Parquet files into larger ones. This happens automatically but you can model the cost impact here.">
          <Switch aria-label="Iceberg table optimization enabled" checked={s.icebergOptimizeEnabled} onCheckedChange={set('icebergOptimizeEnabled')} />
        </Row>
        <Row label='Active remote analysts' tooltip="How many other team members have this service open on their computer simultaneously. Each analyst syncs metadata every 2 minutes.">
          <NumInput value={s.activeAnalysts} onChange={set('activeAnalysts')} min={0} />
        </Row>
        <Row label='Analyst full syncs / month' tooltip="How many times per month an analyst completely resets their local cache and re-downloads the entire historical Iceberg table.">
          <NumInput value={s.analystFullSyncsPerMonth} onChange={set('analystFullSyncsPerMonth')} min={0} />
        </Row>
      </div>

      {/* Minimum billing box */}
      <div className='mt-4 p-3 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md space-y-2'>
        <div className='text-xs font-semibold text-amber-700 dark:text-amber-400'>Minimum Charge per Object</div>
        <Row label='Min. days billed per object (even if deleted early)'>
          <ReadOnlyValue value={s.minDays} />
        </Row>
        <div className='text-[11px] text-amber-700/80 dark:text-amber-500/80'>
          Edit on the <a href={buildServiceHref('/admin', activeServiceId)} className='underline font-medium hover:text-amber-900 dark:hover:text-amber-300'>admin page</a>.
        </div>
        <div className='text-xs text-amber-600 dark:text-amber-500 space-y-0.5'>
          <div>Objects created/day: <strong>{fmtN(Math.round(r.objectsPerDay))}</strong></div>
          <div>Sustained billed footprint (30d): <strong>{fmtN(Math.round(r.objectsBilled))}</strong> objects</div>
        </div>
      </div>
    </section>
  )
}

// ─── Pricing ─────────────────────────────────────────────────────────────────

interface PricingProps {
  s: CalcState
  setZone?: (zone: BillingZone) => void
}

export function Pricing({ s, setZone }: PricingProps) {
  const router = useRouter()
  const activeServiceId = useServiceStore((st) => st.activeServiceId)
  return (
    <>
      <section>
        <div className="flex items-center justify-between mb-3">
          <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider'>Pricing (per 1,000 ops)</h3>
          <Button
            variant="link"
            size="sm"
            className="h-7 p-0 text-[10px] uppercase font-bold text-primary"
            onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}
          >
            Edit in Admin
          </Button>
        </div>

        {/* Delivery Billing Zone Selector */}
        <div className="mb-4 space-y-2 p-3 bg-muted/40 rounded-lg border border-border/50">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
              Delivery Billing Zone
            </span>
            <span className="text-xs font-semibold text-primary">
              ${s.rateEgress.toFixed(2)} / GB
            </span>
          </div>
          <div className="grid grid-cols-3 gap-1.5 p-1 bg-muted/80 rounded-md">
            {(['MBZ100', 'MBZ200', 'Global'] as const).map((zone) => {
              const isSelected = s.billingZone === zone
              return (
                <TooltipProvider key={zone}>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <button
                        type="button"
                        onClick={() => setZone?.(zone)}
                        className={cn(
                          'px-2.5 py-1.5 text-xs font-medium rounded transition-all text-center',
                          isSelected
                            ? 'bg-background text-foreground shadow-sm font-semibold'
                            : 'text-muted-foreground hover:text-foreground hover:bg-background/50'
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
            {BILLING_ZONE_DESCRIPTIONS[s.billingZone || 'MBZ200']}
          </p>
        </div>

        <div className='space-y-0'>
          <Row label='Class A rate (writes, lists)'>
            <ReadOnlyValue value={s.rateA} />
          </Row>
          <Row label='Class B rate (reads/downloads)'>
            <ReadOnlyValue value={s.rateB} />
          </Row>
          <Row label='Storage rate (per GB/month)'>
            <ReadOnlyValue value={s.rateStorage} />
          </Row>
          <Row label='CDN egress rate (per GB)'>
            <ReadOnlyValue value={s.rateEgress} />
          </Row>
        </div>
        <div className='mt-2 text-[11px] text-muted-foreground'>
          Rates are global defaults. Update them on the <a href={buildServiceHref('/admin', activeServiceId)} className='underline font-medium hover:text-foreground'>admin page</a>.
        </div>
      </section>

      {/* What generates ops reference table */}
      <section>
        <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>What generates operations</h3>
        {/* M-6 (audit, mobile UX): wrap the ops-reference table so its
            two-column labels (some long, e.g. "List during sync (per
            1,000 objects)") scroll horizontally instead of pushing the
            entire cost-calculator card off-screen on phones. */}
        <div className='overflow-x-auto'>
        <table className='w-full text-xs text-muted-foreground'>
          <tbody>
            <tr><td colSpan={2} className='py-1 font-semibold text-blue-600 dark:text-blue-400'>Class A (writes &amp; lists)</td></tr>
            {[
              ['Fastly writes each log file', '1 op/file'],
              ['Raw Parquet upload per sync', '1 op/file'],
              ['List during sync (per 1,000 objects)', '1 op/page'],
              ['Manual log checks (LIST)', '1 op/check'],
              ['Admin sync state to FOS', '1 op/sync'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 font-semibold text-blue-500 dark:text-blue-300 italic'>Iceberg Maintenance (if enabled)</td></tr>
            {[
              ['Iceberg commit (append data)', '1 op/sync'],
              ['Table optimization (rewrite)', '1 op/file'],
              ['Weekly snapshot expiry', '1 op/week'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5 text-muted-foreground/70'>{l}</td><td className='text-right text-muted-foreground/70'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 mt-2 font-semibold text-amber-600 dark:text-amber-400'>Class B (reads)</td></tr>
            {[
              ['Read each .gz during ingest', '1 op/file'],
              ['Analyst metadata pull (cached)', '1 op/min'],
              ['Query Parquet (no local cache)', '1 op/file/query'],
              ['CDN-cached reads', '0 ops'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 mt-2 font-semibold text-orange-600 dark:text-orange-400'>Egress (Transfer Out)</td></tr>
            {[
              ['Dashboard queries (uncached)', 'MBs'],
              ['Analyst metadata sync (cached)', '~10KB/min'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
          </tbody>
        </table>
        </div>
      </section>
    </>
  )
}

// ─── Results ─────────────────────────────────────────────────────────────────

interface ResultsProps {
  s: CalcState
  r: CalcResults
}

export function Results({ s, r }: ResultsProps) {
  return (
    <div className='bg-muted/30 border rounded-lg p-5'>
      <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4'>Monthly Estimate Breakdown</h3>
      <div className='space-y-0'>
        <ResultRow
          label='Class A Operations (writes, lists)'
          detail={`${fmtN(Math.round(r.classAPerMonth))} ops @ $${s.rateA}/1k`}
          cost={formatUSD(r.costA)}
        />
        <ResultRow
          label='Class B Operations (reads)'
          detail={`${fmtN(Math.round(r.classBPerMonth))} ops @ $${s.rateB}/1k`}
          cost={formatUSD(r.costB)}
        />
        <ResultRow
          label='Storage (GB-months billed)'
          detail={r.storageTiers.map(t => `${t.label}: ${Math.max(0.001, t.gbMonths).toFixed(3)} GB-mo${t.flagged ? '*' : ''}`).join(' · ')}
          cost={formatUSD(r.costStorage)}
        />
        <ResultRow
          label='CDN Egress'
          detail={`${r.cdnEgressGB.toFixed(3)} GB`}
          cost={formatUSD(r.costEgress)}
        />
        {s.rumBeaconsDay > 0 && (
          <ResultRow
            label='Real User Monitoring (RUM) Cost Portion'
            detail={`${fmtN(Math.round(r.rumBeaconsMonth))} beacons · ${Math.max(0.001, r.rumParquetGBMonths).toFixed(3)} GB RUM storage (included in Class A & Storage above)`}
            cost={formatUSD(r.costRum)}
          />
        )}
        <ResultRow
          label='Total Estimated Monthly Cost'
          cost={formatUSD(r.totalCost)}
          highlight
        />
      </div>

      {/* Volume estimates */}
      <div className='mt-5 pt-4 border-t border-border/40'>
        <h4 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>Volume Estimates</h4>
        <div className='grid grid-cols-2 gap-x-6 gap-y-1'>
          {[
            ['Est. log line size (uncompressed)', s.bytesPerLine + ' B'],
            ['Logged requests / month', fmtN(Math.round(r.reqDayEffective * 30))],
            ['Raw volume / month (uncompressed)', formatBytes(r.totalBytesPerMonth)],
            ['Est. volume / month (.gz compressed)', formatBytes(r.totalGzBytesPerMonth)],
            ['Log files written / month', fmtN(Math.round(r.logFilesPerMonth))],
            ['Iceberg data files created / month', fmtN(Math.round(r.parquetFilesPerMonth))],
            ['Billed footprint (30d min)', fmtN(Math.round(r.objectsBilled)) + ' objects'],
            ['Syncs / month', fmtN(Math.round(r.syncsPerMonth))],
            ...(s.rumBeaconsDay > 0 ? [
              ['RUM beacons / month', fmtN(Math.round(r.rumBeaconsMonth))],
              ['RUM footprint / month', Math.max(0.001, r.rumParquetGBMonths).toFixed(3) + ' GB-mo']
            ] : [])
          ].map(([label, value]) => (
            <div key={label} className='flex justify-between text-xs py-0.5'>
              <span className='text-muted-foreground'>{label}</span>
              <span className='font-medium tabular-nums'>{value}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

interface CostCalculatorProps {
  prefillData?: PrefillResponse
  prefillNote?: string
}

export function CostCalculator({ prefillData, prefillNote }: CostCalculatorProps) {
  const [s, dispatch] = useReducer(reducer, DEFAULTS)

  useEffect(() => {
    if (prefillData) {
      dispatch({ type: 'PREFILL', prefill: prefillData })
    }
  }, [prefillData])

  const set = (key: keyof CalcState) => (value: number | boolean) => {
    dispatch({ type: 'SET', key, value })
  }

  const setZone = (zone: import('./calc').BillingZone) => {
    dispatch({ type: 'SET_ZONE', zone })
  }

  const r = useMemo(() => calculate(s), [s])

  return (
    <div className='space-y-6'>
      {prefillNote && (
        <div className='text-xs text-emerald-700 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 rounded-md px-3 py-2'>
          {prefillNote}
        </div>
      )}

      <div className='grid grid-cols-1 lg:grid-cols-2 gap-8'>
        {/* ── Left: Inputs ── */}
        <div className='space-y-6'>
          <Inputs s={s} r={r} set={set} />
        </div>

        {/* ── Right: Pricing + What generates ops ── */}
        <div className='space-y-6'>
          <Pricing s={s} setZone={setZone} />
        </div>
      </div>

      {/* ── Results ── */}
      <Results s={s} r={r} />

      <p className='text-xs text-muted-foreground leading-relaxed'>
        * Estimates assume ~4× compression of log JSON to Parquet (ZSTD). Storage is billed in GB-hours;
        the calculator converts to GB-months (1 month = 720 hours). Each object is billed for a minimum
        of {s.minDays} days regardless of when it is deleted. CDN caching eliminates Class B ops for cached
        parquet reads; local cache eliminates them entirely for query reads. Actual usage may vary.
      </p>
    </div>
  )
}
