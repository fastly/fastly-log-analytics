'use client'

import { useState } from 'react'
import { Switch } from '@/components/ui/switch'
import { formatUSD } from '@/lib/utils'
import {
  DEFAULT_INPUTS,
  DEFAULT_UNITS,
  LIST_PRICE_RATES,
  estimateMonthly,
  type EstimateInputs,
  type EstimateRates,
} from '@/lib/cloud-engine/usage/estimate'
import type { MeasuredEstimate } from '@/lib/cloud-engine/usage/estimatePrefill'
import { NumInput, ReadOnlyValue, ResultRow, Row } from './CostCalculator'

const INPUT_ROWS: [keyof EstimateInputs, string][] = [
  ['requestsPerDay', 'Total requests per day'],
  ['edgeRequestsPerDay', 'Edge requests per day'],
  ['samplePct', 'Log sample rate (%)'],
  ['logPeriodS', 'Log file period (seconds)'],
  ['filesPerPeriod', 'Raw files per log period'],
  ['gzBytesPerLine', 'Compressed bytes per log line'],
  ['parquetBytesPerRow', 'Parquet bytes per row'],
  ['retentionDays', 'Log retention (days)'],
]

const RATE_ROWS: [keyof EstimateRates, string][] = [
  ['storage_rate_per_gb_month', 'Storage ($/GB-month)'],
  ['class_a_rate_per_1k', 'FOS Class A ($/1k)'],
  ['class_b_rate_per_10k', 'FOS Class B ($/10k)'],
  ['cdn_egress_rate_per_gb', 'CDN egress ($/GB)'],
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

export function CloudCostCalculator({ prefillData, rates: configuredRates }: {
  prefillData?: Partial<MeasuredEstimate>
  rates?: Partial<Record<keyof EstimateRates, number | null>>
}) {
  const [edits, setEdits] = useState<Partial<EstimateInputs>>({})
  const inputs = { ...DEFAULT_INPUTS, ...prefillData?.inputs, ...edits }
  const units = { ...DEFAULT_UNITS, ...prefillData?.units }
  const rates = { ...LIST_PRICE_RATES }
  for (const key of Object.keys(rates) as (keyof EstimateRates)[]) {
    const value = prefillData?.rates?.[key] ?? configuredRates?.[key]
    if (value != null) rates[key] = value
  }
  const measuredInputs = prefillData?.measuredInputs ?? {}
  const result = estimateMonthly(inputs, units, rates, prefillData?.compaction, {
    relay: prefillData?.relay ?? 0,
  })
  const set = (key: keyof EstimateInputs) => (value: number | boolean) =>
    setEdits((current) => ({ ...current, [key]: value }))
  const inputRow = ([key, label]: [keyof EstimateInputs, string]) => (
    <Row key={key} label={label}>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">
          {key in edits ? 'What-if' : measuredInputs[key] ? 'Measured' : 'Reference'}
        </span>
        <NumInput aria-label={label} value={inputs[key] as number} onChange={set(key)} wide />
      </div>
    </Row>
  )

  return (
    <div className="space-y-6">
      <p className="text-xs text-muted-foreground">
        Measured values use this install&apos;s traffic, billing and ingest records.
        Unavailable measurements retain the canonical Cloud reference assumptions, not observed zero usage.
        {prefillData?.usageLogging === false && ' FOS usage logging is off; storage operation units are reference estimates.'}
      </p>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <section>
          <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Your Traffic &amp; Config</h3>
          {INPUT_ROWS.map(inputRow)}
          <Row label="Edge-only logging">
            <Switch aria-label="Edge-only logging" checked={inputs.edgeOnly} onCheckedChange={set('edgeOnly')} />
          </Row>
          <Row label="Delete raw logs after ingest">
            <Switch aria-label="Delete raw logs after ingest" checked={inputs.deleteAfter} onCheckedChange={set('deleteAfter')} />
          </Row>
          {prefillData?.rumEnabled && ([
            ['rumPageviewsPerDay', 'RUM pageviews per day'],
            ['rumSamplePct', 'RUM sample rate (%)'],
            ['rumRetentionDays', 'RUM retention (days)'],
          ] as [keyof EstimateInputs, string][]).map(inputRow)}
        </section>
        <section>
          <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Contract Rates</h3>
          {RATE_ROWS.map(([key, label]) => (
            <Row key={key} label={label}><ReadOnlyValue value={rates[key]} /></Row>
          ))}
          <p className="text-xs text-muted-foreground mt-3">
            {prefillData?.rates || configuredRates ? 'Usage settings rates with list-price fallback where unavailable.' : 'List-price reference rates; configured rates are unavailable.'}
            {' '}Unmeasured fixed, per-file and per-line costs use the canonical Cloud reference install.
          </p>
        </section>
      </div>
      <div className="bg-muted/30 border rounded-lg p-5">
        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4">Monthly Estimate Breakdown</h3>
        {result.lines.map((line) => (
          <ResultRow key={line.id} label={line.label} detail={line.detail} cost={formatUSD(line.usd)} />
        ))}
        <ResultRow label="Total Estimated Monthly Cost" cost={formatUSD(result.total)} highlight />
      </div>
    </div>
  )
}
