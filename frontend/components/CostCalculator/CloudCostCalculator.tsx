'use client'

import { useState } from 'react'
import { measuredStorageGbMonths } from './measuredStorage'
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

const LINE_SIZE_LABEL = 'Log line size (bytes, uncompressed)'

/** Only what a user knows; edge requests, files per period and stored sizes are derived from these. */
const INPUT_ROWS: [keyof EstimateInputs, string][] = [
  ['requestsPerDay', 'Total requests per day'],
  ['samplePct', 'Log sample rate (%)'],
  ['logPeriodS', 'Log period (seconds)'],
]

/**
 * The install's measured inputs with the user's what-ifs applied. Edge requests keep their measured share of
 * total requests, and the zstd raw and Parquet sizes scale with the uncompressed line size at the install's measured
 * compression ratio (`rawBytesPerGzByte`); raw logs are always
 * deleted after ingest.
 */
export function applyWhatIfs(base: EstimateInputs, edits: Partial<EstimateInputs>, lineBytes?: number): EstimateInputs {
  const inputs = { ...base, ...edits, deleteAfter: true }
  if (edits.requestsPerDay != null && base.requestsPerDay > 0) {
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

export function CloudCostCalculator({ prefillData, rates: configuredRates, localStorageGbHours, localStoragePeriodHours }: {
  prefillData?: Partial<MeasuredEstimate>
  rates?: Partial<Record<keyof EstimateRates, number | null>>
  localStorageGbHours?: number | null
  localStoragePeriodHours?: number | null
}) {
  const [edits, setEdits] = useState<Partial<EstimateInputs>>({})
  const [lineBytes, setLineBytes] = useState<number | undefined>(undefined)
  const base = { ...DEFAULT_INPUTS, ...prefillData?.inputs }
  const inputs = applyWhatIfs(base, edits, lineBytes)
  const edited = lineBytes != null || Object.keys(edits).length > 0
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
  // Measured storage describes this install as it is; once the user asks "what if", the model prices storage.
  const localStorageGbMonths = edited ? null : measuredStorageGbMonths(localStorageGbHours, localStoragePeriodHours)
  const localStorageCost = localStorageGbMonths == null
    ? null
    : localStorageGbMonths * rates.storage_rate_per_gb_month
  // Measured storage replaces only the modelled GB-months; operations stay modelled.
  const modelledStorageUsd = result.lines
    .find((line) => line.id === 'storage')
    ?.parts?.find((part) => part.label === 'Storage')?.usd ?? 0
  const resultLines = localStorageCost == null
    ? result.lines
    : result.lines.map((line) => line.id === 'storage'
      ? {
          ...line,
          detail: `Measured · this install · ${(localStorageGbMonths ?? 0).toFixed(2)} GB-months`,
          usd: line.usd - modelledStorageUsd + localStorageCost,
          parts: line.parts?.map((part) => part.label === 'Storage' ? { ...part, usd: localStorageCost } : part),
        }
      : line)
  const resultTotal = localStorageCost == null
    ? result.total
    : result.total - modelledStorageUsd + localStorageCost
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
  const lineSizeRow = (
    <Row key="lineBytes" label={LINE_SIZE_LABEL}>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">
          {lineBytes != null
            ? 'What-if'
            : measuredInputs.gzBytesPerLine && measuredInputs.rawBytesPerGzByte ? 'Measured' : 'Reference'}
        </span>
        <NumInput
          aria-label={LINE_SIZE_LABEL}
          value={Math.round(inputs.gzBytesPerLine * inputs.rawBytesPerGzByte)}
          onChange={setLineBytes}
          wide
        />
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
          {lineSizeRow}
          {inputRow(['retentionDays', 'Log retention (days)'])}
          <Row label="Edge-only logging">
            <Switch aria-label="Edge-only logging" checked={inputs.edgeOnly} onCheckedChange={set('edgeOnly')} />
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
        {resultLines.map((line) => (
          <ResultRow
            key={line.id}
            label={line.label}
            detail={line.detail}
            cost={formatUSD(line.usd)}
            parts={line.parts?.map((part) => ({ label: part.label, cost: formatUSD(part.usd) }))}
          />
        ))}
        <ResultRow label="Total Estimated Monthly Cost" cost={formatUSD(resultTotal)} highlight />
      </div>
      <p className="text-xs text-muted-foreground leading-relaxed">
        Cloud DR state mirroring (<code>meta/cloud_dr_state.json</code>) is event-driven on service settings and saved view updates, classified under <code>pq_ops</code>, and automatically folded into measured fixed storage and Class A operations when FOS usage logging is enabled.
      </p>
    </div>
  )
}
