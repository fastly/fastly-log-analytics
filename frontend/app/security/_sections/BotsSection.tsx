import React from 'react'
import { Bot, Fingerprint, CheckCircle2, AlertTriangle, Clock, HelpCircle, Info, Scale } from 'lucide-react'
import type { Dispatch, SetStateAction } from 'react'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { FilterValueCell } from '@/components/FilterValueCell'
import { formatDate } from '@/lib/date'
import { formatBytes, formatCount, formatCountMaybe } from '@/lib/format'
import {
  SECURITY_INFO,
  NGWAF_BOT_COLUMN_IDS,
  BOT_COLUMN_IDS,
  BOT_EFFICIENCY_COLUMN_IDS,
  FINGERPRINT_COLUMN_IDS,
} from './securityInfo'
import { useActiveLogFields } from '@/hooks/useLogFieldsCatalog'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityAggregatesResponse']
type WellknownBot = components['schemas']['WellknownBotRow']
type BotEfficiency = components['schemas']['BotEfficiencyRow']
type NgwafVerifiedBot = components['schemas']['NgwafVerifiedBotRow']
type TlsFingerprint = components['schemas']['TlsFingerprintRow']

type Props = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
  intervalButtons: React.ReactNode
  bucketSeconds: number
  timezone: string
  commonTimeLayout: PlotlyLayout
  getFieldLabel: (id: string) => string
  ngwafBotVisibility: VisibilityState
  setNgwafBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onNgwafBotVisChange: (id: string, vis: boolean) => void
  botVisibility: VisibilityState
  setBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onBotVisChange: (id: string, vis: boolean) => void
  botEfficiencyVisibility?: VisibilityState
  setBotEfficiencyVisibility?: Dispatch<SetStateAction<VisibilityState>>
  onBotEfficiencyVisChange?: (id: string, vis: boolean) => void
  fingerprintVisibility: VisibilityState
  setFingerprintVisibility: Dispatch<SetStateAction<VisibilityState>>
  onFingerprintVisChange: (id: string, vis: boolean) => void
}

// Threshold below which we render a "low coverage" hint instead of letting
// an analyst stare at a 1-row leaderboard wondering whether the field is
// broken. 1% chosen because it's the floor below which a top-N is
// effectively unactionable (the visible rows represent <1 in 100 requests).
// Tuned for backend `fingerprint_coverage` values which are 0..1.
const LOW_COVERAGE_THRESHOLD = 0.01

// Per-field hint message: explains WHY a field is sparse so the analyst reads
// "TLS fingerprints aren't there because traffic is mostly shielded" instead
// of "TLS fingerprints are broken."
const COVERAGE_HINT_MESSAGE: Record<string, string> = {
  tls_ciphers_sha: 'TLS fingerprints are only captured when the request lands at the true edge PoP (not shielded). Sparse coverage typically means most traffic is shielded.',
}

function FingerprintCoverageHint({ coverage, field }: { coverage: number | undefined, field: string }) {
  // Undefined coverage = backend didn't return a value (older backend, or
  // field-not-in-schema branch). Don't render a hint we can't ground.
  if (coverage === undefined || coverage === null) return null
  if (coverage >= LOW_COVERAGE_THRESHOLD) return null
  const pct = coverage === 0 ? '0%' : coverage < 0.001 ? '<0.1%' : `${(coverage * 100).toFixed(2)}%`
  const msg = COVERAGE_HINT_MESSAGE[field] || `This field is populated for only a small fraction of requests in the current window.`
  return (
    <div className="flex items-start gap-2 px-3 py-2 text-[11px] text-muted-foreground bg-muted/30 border-b">
      <Info className="h-3.5 w-3.5 mt-0.5 shrink-0" />
      <span>
        <span className="font-medium text-foreground">Low coverage ({pct}).</span> {msg}
      </span>
    </div>
  )
}

export function BotsSection({
  data,
  isLoading,
  isFetching,
  error,
  intervalButtons,
  bucketSeconds,
  timezone,
  commonTimeLayout,
  getFieldLabel,
  ngwafBotVisibility,
  setNgwafBotVisibility,
  onNgwafBotVisChange,
  botVisibility,
  setBotVisibility,
  onBotVisChange,
  botEfficiencyVisibility = {},
  setBotEfficiencyVisibility,
  onBotEfficiencyVisChange,
  fingerprintVisibility,
  setFingerprintVisibility,
  onFingerprintVisChange,
}: Props) {
  // Distinguish "field group not enabled" from "enabled but no data yet" so a
  // low-traffic/fresh service doesn't read as misconfigured.
  const { isFieldActive } = useActiveLogFields()
  // Five sites below pass the same {id, label: getFieldLabel(id)} list to
  // ColumnVisibilityDropdown. Helper closes over the getFieldLabel prop so
  // a future change to label resolution lands once.
  const labeledColumns = React.useCallback(
    (ids: readonly string[]) => ids.map(id => ({ id, label: getFieldLabel(id) })),
    [getFieldLabel],
  )

  const ngwafBotsData = React.useMemo(() => {
    const timeseries = data?.ngwaf_verified_bots_ts
    if (!timeseries?.length) return []
    const byName: Record<string, { x: string[], y: number[] }> = {}

    const allTimesSet = new Set<string>()
    timeseries.forEach((d) => {
      allTimesSet.add(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    })
    const allTimes = Array.from(allTimesSet).sort()

    const names = Array.from(new Set(timeseries.map((d) => d.bot_name ?? '')))
    for (const n of names) {
      byName[n] = { x: [...allTimes], y: new Array(allTimes.length).fill(0) }
    }

    for (const d of timeseries) {
      const t = formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")
      const idx = allTimes.indexOf(t)
      if (idx !== -1) {
        byName[d.bot_name ?? ''].y[idx] = d.count
      }
    }

    return Object.entries(byName).map(([name, d], i): PlotlyTrace => ({
      x: d.x,
      y: d.y.map(val => val === 0 ? null : val),
      type: 'bar',
      name,
      width: bucketSeconds * 1000,
      hovertemplate: `${name}: %{y:,}<extra></extra>`,
      marker: { color: `hsl(${(i * 47 + 210) % 360}, 70%, 50%)` }
    }))
  }, [data, timezone, bucketSeconds])

  const botEfficiencyChartData = React.useMemo((): PlotlyTrace[] => {
    const rows = data?.bot_efficiency
    if (!rows?.length) return []
    const top = rows.slice(0, 10)
    return [
      {
        x: top.map((r) => r.bot_name),
        y: top.map((r) => r.request_count),
        name: 'Bot Requests',
        type: 'bar',
        marker: { color: '#f97316' },
      },
      {
        x: top.map((r) => r.bot_name),
        y: top.map((r) => r.referrals),
        name: 'Referred Traffic',
        type: 'bar',
        marker: { color: '#22c55e' },
      },
    ]
  }, [data?.bot_efficiency])

  const botColumns: ColumnDef<WellknownBot>[] = [
    {
      accessorKey: 'name',
      header: 'Bot',
      cell: ({ row }) => (
        <FilterValueCell
          filters={[{ column: '_wellknown_bot_id', value: row.original.id }]}
          display={row.original.name}
          className="font-medium"
          containerClassName="max-w-[200px]"
        />
      )
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: ({ row }) => <span className="capitalize">{row.original.category.replace(/-/g, ' ')}</span>
    },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCount(row.original.request_count) },
    {
      accessorKey: 'verified_count',
      header: 'Verified',
      cell: ({ row }) => row.original.verified_count > 0 ? (
        <span className="flex items-center gap-1 text-green-500">
          <CheckCircle2 className="h-3 w-3" />{formatCount(row.original.verified_count)}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'impersonator_count',
      header: 'Spoofed',
      cell: ({ row }) => row.original.impersonator_count > 0 ? (
        <span className="flex items-center gap-1 text-red-500">
          <AlertTriangle className="h-3 w-3" />{formatCount(row.original.impersonator_count)}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'unverified_count',
      header: 'Unverified',
      cell: ({ row }) => row.original.unverified_count > 0 ? (
        <span className="flex items-center gap-1 text-muted-foreground" title="Unverifiable (no IPs/domains in source)">
          <HelpCircle className="h-3 w-3" />{formatCount(row.original.unverified_count)}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'pending_count',
      header: 'Pending',
      cell: ({ row }) => row.original.pending_count > 0 ? (
        <span className="flex items-center gap-1 text-yellow-500" title="Pending rDNS lookup">
          <Clock className="h-3 w-3" />{formatCount(row.original.pending_count)}
        </span>
      ) : '—'
    }
  ]

  const ngwafBotColumns: ColumnDef<NgwafVerifiedBot>[] = [
    {
      accessorKey: 'bot_name',
      header: 'Bot Name',
      cell: ({ row }) => (
        <FilterValueCell
          filters={[{ column: '_ngwaf_bot_name', value: row.original.bot_name || '' }]}
          display={row.original.bot_name ?? undefined}
          className="font-medium"
          containerClassName="max-w-[200px]"
        />
      )
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: ({ row }) => row.original.category
        ? <span className="capitalize">{row.original.category.replace(/-/g, ' ')}</span>
        : <span className="text-muted-foreground">—</span>
    },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCount(row.original.request_count) },
  ]

  const fingerprintColumns: ColumnDef<TlsFingerprint>[] = [
    {
      accessorKey: 'fingerprint',
      header: 'Cipher Fingerprint (SHA)',
      cell: ({ row }) => (
        <FilterValueCell
          filters={[{ column: 'tls_client_hello', value: row.original.fingerprint ?? '' }]}
          className="font-mono text-[10px]"
          containerClassName="max-w-[200px]"
        />
      )
    },
    { accessorKey: 'ip_count', header: 'Unique IPs', cell: ({ row }) => formatCountMaybe(row.original.ip_count) ?? '—' },
    { accessorKey: 'request_count', header: 'Requests', cell: ({ row }) => formatCountMaybe(row.original.request_count) ?? '—' },
  ]

  const botEfficiencyColumns: ColumnDef<BotEfficiency>[] = [
    {
      accessorKey: 'bot_name',
      header: 'Bot / Crawler',
      cell: ({ row }) => <span className="font-medium">{row.original.bot_name}</span>,
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: ({ row }) => <span className="capitalize text-muted-foreground">{row.original.category.replace(/-/g, ' ')}</span>,
    },
    {
      accessorKey: 'request_count',
      header: 'Requests',
      cell: ({ row }) => formatCount(row.original.request_count),
    },
    {
      accessorKey: 'resp_bytes',
      header: 'Bandwidth',
      cell: ({ row }) => formatBytes(row.original.resp_bytes),
    },
    {
      accessorKey: 'bandwidth_pct',
      header: 'Site BW Share',
      cell: ({ row }) => `${row.original.bandwidth_pct.toFixed(1)}%`,
    },
    {
      accessorKey: 'referrals',
      header: 'Referred Traffic',
      cell: ({ row }) =>
        row.original.referrals === 0 ? (
          <span className="flex items-center gap-1 text-red-500 font-medium" title="Zero human visitors referred">
            <AlertTriangle className="h-3 w-3" />0
          </span>
        ) : (
          <span className="text-green-600 dark:text-green-400 font-medium">
            {formatCount(row.original.referrals)}
          </span>
        ),
    },
    {
      accessorKey: 'requests_per_referral',
      header: 'Requests / Referral',
      cell: ({ row }) => {
        const val = row.original.requests_per_referral
        const isHighCost = row.original.referrals === 0 || val >= 50
        return (
          <span className={isHighCost ? 'text-red-500 font-semibold' : undefined}>
            {formatCount(val, { maximumFractionDigits: 1 })}
          </span>
        )
      },
    },
    {
      accessorKey: 'bytes_per_referral',
      header: 'Bandwidth / Referral',
      cell: ({ row }) => {
        const isZeroRef = row.original.referrals === 0
        return (
          <span className={isZeroRef ? 'text-red-500 font-semibold' : undefined}>
            {formatBytes(row.original.bytes_per_referral)}
          </span>
        )
      },
    },
  ]

  return (
    <>
      {data?.ngwaf_configured !== false && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <AnalyticsCard
            title="Verified Bots (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={intervalButtons}
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF verified bots..."
            error={error as AnalyticsCardError | null}
            className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-2"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            {ngwafBotsData.length === 0 && !isLoading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
                {data?.ngwaf_configured
                  ? "No NGWAF bot detections in this time window."
                  : <>Set <code className="mx-1 text-xs bg-muted px-1 rounded">ngwaf_workspace_id</code> in service settings to enable NGWAF bot tracking.</>}
              </div>
            ) : (
              <PlotlyChart
                data={ngwafBotsData}
                layout={{
                  ...commonTimeLayout,
                  barmode: 'stack',
                  showlegend: true,
                  yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' }
                }}
                height="100%"
              />
            )}
          </AnalyticsCard>

          <AnalyticsCard
            title="Verified Bot Names (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={
              <ColumnVisibilityDropdown columns={labeledColumns(NGWAF_BOT_COLUMN_IDS)} visibility={ngwafBotVisibility} onChange={onNgwafBotVisChange} />
            }
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF bot names..."
            error={error as AnalyticsCardError | null}
            className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-0"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            <DataTable
              columns={ngwafBotColumns}
              data={data?.ngwaf_verified_bots || []}
              emptyMessage={isLoading ? "" : (data?.ngwaf_configured ? "No NGWAF bot detections in this time window." : "Set ngwaf_workspace_id in service settings to enable NGWAF bot tracking.")}
              hideToolbar
              columnVisibility={ngwafBotVisibility}
              onColumnVisibilityChange={setNgwafBotVisibility}
            />
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Well-Known Bots"
          icon={<Bot className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown columns={labeledColumns(BOT_COLUMN_IDS)} visibility={botVisibility} onChange={onBotVisChange} />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading well-known bots..."
          error={error as AnalyticsCardError | null}
          className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.wellknown_bots.title}
          helpContent={SECURITY_INFO.wellknown_bots.body}
        >
          <DataTable
            columns={botColumns}
            data={data?.wellknown_bots || []}
            emptyMessage={isLoading ? "" : "No known bots detected. Ensure bot sources are cached in Admin settings."}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botVisibility}
            onColumnVisibilityChange={setBotVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Top TLS Fingerprints"
          icon={<Fingerprint className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown columns={labeledColumns(FINGERPRINT_COLUMN_IDS)} visibility={fingerprintVisibility} onChange={onFingerprintVisChange} />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading TLS fingerprints..."
          error={error as AnalyticsCardError | null}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.fingerprints.title}
          helpContent={SECURITY_INFO.fingerprints.body}
        >
          <FingerprintCoverageHint
            coverage={data?.fingerprint_coverage?.tls_ciphers_sha}
            field="tls_ciphers_sha"
          />
          <DataTable
            columns={fingerprintColumns}
            data={data?.tls_fingerprints || []}
            emptyMessage={isLoading ? "" : ((isFieldActive('ja3') || isFieldActive('ja4')) ? "No TLS fingerprints in this time range." : "Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.")}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={fingerprintVisibility}
            onColumnVisibilityChange={setFingerprintVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Bot & Crawler Cost vs. Referrals"
          icon={<Scale className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot cost vs. referrals chart..."
          error={error as AnalyticsCardError | null}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          {botEfficiencyChartData.length === 0 && !isLoading ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
              {isFieldActive('ua')
                ? 'No bot or crawler traffic detected in this time window.'
                : 'Requires User-Agent (ua) field to be enabled in Fastly logging.'}
            </div>
          ) : (
            <PlotlyChart
              data={botEfficiencyChartData}
              layout={{
                barmode: 'group',
                showlegend: true,
                yaxis: { title: 'Count', separatethousands: true, exponentformat: 'none' },
              }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)"
          icon={<Scale className="h-4 w-4" />}
          headerAction={
            onBotEfficiencyVisChange ? (
              <ColumnVisibilityDropdown
                columns={labeledColumns(BOT_EFFICIENCY_COLUMN_IDS)}
                visibility={botEfficiencyVisibility}
                onChange={onBotEfficiencyVisChange}
              />
            ) : undefined
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot bandwidth & referral efficiency..."
          error={error as AnalyticsCardError | null}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          <DataTable
            columns={botEfficiencyColumns}
            data={data?.bot_efficiency || []}
            emptyMessage={
              isLoading
                ? ''
                : isFieldActive('ua')
                  ? 'No bot or crawler traffic detected in this time window.'
                  : 'Requires User-Agent (ua) field to be enabled in Fastly logging.'
            }
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botEfficiencyVisibility}
            onColumnVisibilityChange={setBotEfficiencyVisibility}
          />
        </AnalyticsCard>
      </div>
    </>
  )
}
