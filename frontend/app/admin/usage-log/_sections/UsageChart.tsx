'use client'

import React, { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Database, AlertTriangle, Loader2 } from 'lucide-react'

const LOG_ACCOUNTING_PRESETS = [
  { label: 'Last 1h', hours: 1, by: 'hour' as const },
  { label: 'Last 24h', hours: 24, by: 'hour' as const },
  { label: 'Last 7d', hours: 168, by: 'day' as const },
  { label: 'Last 30d', hours: 720, by: 'day' as const },
]

export function LogAccountingPanel() {
  const [presetIdx, setPresetIdx] = useState(1)
  const preset = LOG_ACCOUNTING_PRESETS[presetIdx]
  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['log-accounting', preset.hours, preset.by],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/log-accounting', { signal,
        params: { query: { hours: preset.hours, by: preset.by } },
      })
      if (error) throw new Error(extractApiError(error))
      return data
    },
    staleTime: 30_000,
    refetchInterval: 60_000,
  })

  const totals = data?.totals
  const buckets = data?.buckets ?? []
  const gapPct = totals?.gap_pct ?? 0
  const gapAbsPct = Math.abs(gapPct) * 100
  const gapColor =
    gapAbsPct <= 0.1 ? 'text-green-600 dark:text-green-400'
    : gapAbsPct <= 1 ? 'text-yellow-600 dark:text-yellow-400'
    : 'text-destructive'

  const catchup = data?.catchup
  const catchupBadge = useMemo(() => {
    if (!catchup) return null
    const fmtLag = (s: number | null | undefined) => {
      if (s == null) return ''
      if (s < 60) return `${s}s ago`
      if (s < 3600) return `${Math.floor(s / 60)}m ago`
      if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ago`
      return `${Math.floor(s / 86400)}d ago`
    }
    const palette: Record<string, { label: string; dot: string; text: string }> = {
      caught_up: { label: 'Caught up', dot: 'bg-green-500', text: 'text-green-700 dark:text-green-400' },
      backfilling: { label: `Backfilling${catchup.lag_seconds != null ? ` · ${fmtLag(catchup.lag_seconds)}` : ''}`, dot: 'bg-yellow-500', text: 'text-yellow-700 dark:text-yellow-400' },
      stalled: { label: `Stalled · ${fmtLag(catchup.lag_seconds)}`, dot: 'bg-red-500', text: 'text-destructive' },
      no_data: { label: 'No ingests yet', dot: 'bg-muted-foreground', text: 'text-muted-foreground' },
    }
    const p = palette[catchup.status] ?? palette.no_data
    return (
      <span className={`inline-flex items-center gap-1.5 text-[10px] font-medium ${p.text}`} title={catchup.latest_ingest_ts ? `Latest ingest: ${catchup.latest_ingest_ts}` : 'No ingests recorded'}>
        <span className={`h-1.5 w-1.5 rounded-full ${p.dot}`} />
        {p.label}
      </span>
    )
  }, [catchup])

  const chartData = useMemo(() => ([
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.fastly_logs),
      type: 'scatter',
      mode: 'lines',
      name: 'Fastly emitted (lines)',
      line: { color: '#3b82f6', width: 2 },
    },
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.our_rows),
      type: 'scatter',
      mode: 'lines',
      name: 'We ingested (lines)',
      line: { color: '#10b981', width: 2, dash: 'dot' },
    },
    {
      x: buckets.map((b: any) => b.ts),
      y: buckets.map((b: any) => b.file_count),
      type: 'scatter',
      mode: 'lines',
      name: 'Files ingested',
      yaxis: 'y2',
      line: { color: '#a855f7', width: 1.5, dash: 'dash' },
    },
  ]), [buckets])

  const chartLayout = useMemo(() => ({
    yaxis: { title: { text: 'log lines' } },
    yaxis2: { title: { text: 'files' }, overlaying: 'y', side: 'right', showgrid: false },
  }), [])

  return (
    <AnalyticsCard
      title="Log Line Accounting"
      description="Fastly's authoritative log-line emission counter (Stats API) vs our locally-ingested row_count, per bucket. A non-zero gap means lines were emitted by Fastly but never landed in our table."
      icon={<Database className="h-4 w-4" />}
      headerAction={
        <div className="flex items-center gap-2">
          {catchupBadge}
          {isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
          <Select value={String(presetIdx)} onValueChange={(v) => { if (v) setPresetIdx(parseInt(v)) }}>
            <SelectTrigger className="h-8 w-[120px] text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {LOG_ACCOUNTING_PRESETS.map((p, i) => (
                <SelectItem key={p.label} value={String(i)}>{p.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      }
    >
      {isLoading ? (
        <div className="text-xs text-muted-foreground italic px-1 py-4">Loading log accounting…</div>
      ) : error ? (
        <div className="text-xs text-destructive px-1 py-4">{(error as Error).message}</div>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Fastly emitted</div>
              <div className="font-mono text-base">{(totals?.fastly_logs ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">We ingested</div>
              <div className="font-mono text-base">{(totals?.our_rows ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Gap (lines)</div>
              <div className={`font-mono text-base ${gapColor}`}>{(totals?.gap ?? 0).toLocaleString()}</div>
            </div>
            <div className="rounded-md border border-muted bg-muted/20 px-3 py-2">
              <div className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">Gap %</div>
              <div className={`font-mono text-base ${gapColor}`}>{(gapPct * 100).toFixed(3)}%</div>
            </div>
          </div>
          {data?.sustained_loss && (
            <div className="mb-3 text-xs px-3 py-2 rounded-md border border-destructive/40 bg-destructive/10 text-destructive">
              <AlertTriangle className="h-3 w-3 inline mr-1.5" />
              Sustained loss: {data.sustained_loss.n_buckets} consecutive {preset.by === 'hour' ? 'hours' : 'days'} ≥5% gap since {data.sustained_loss.started_at} — peak {(data.sustained_loss.max_gap_pct * 100).toFixed(1)}%, {data.sustained_loss.total_lost_lines.toLocaleString()} lines missing
            </div>
          )}
          {totals?.worst_bucket_ts && (totals.worst_bucket_gap_pct ?? 0) > 0.01 && (
            <div className="mb-3 text-xs px-3 py-2 rounded-md border border-yellow-500/30 bg-yellow-500/10 text-yellow-700 dark:text-yellow-300">
              <AlertTriangle className="h-3 w-3 inline mr-1.5" />
              Worst bucket: {totals.worst_bucket_ts} — {((totals.worst_bucket_gap_pct ?? 0) * 100).toFixed(2)}% gap
            </div>
          )}
          {buckets.length > 0 ? (
            <TimeSeriesChart data={chartData} layout={chartLayout} timezone="UTC" height={240} />
          ) : (
            <div className="text-xs text-muted-foreground italic px-1 py-4">No data in this window yet.</div>
          )}
          {data?.fastly_field_used === null && (
            <div className="mt-2 text-[10px] text-muted-foreground italic">
              Note: Fastly Stats response did not contain a recognized log-count field; treating Fastly counts as 0.
            </div>
          )}
        </>
      )}
    </AnalyticsCard>
  )
}
