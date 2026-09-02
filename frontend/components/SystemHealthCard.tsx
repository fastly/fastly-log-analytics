'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Sparkline } from '@/components/Sparkline'
import { useMounted } from '@/hooks/useMounted'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']

// pool_wait is the one slot the backend response_model leaves loosely
// typed (generated as `{[k:string]: unknown}[]`), so we keep a narrow
// local shape and intersect it onto the generated snapshot. The other
// six slots come straight from HealthSnapshotResponse.
type PoolStats = {
  service: string
  max_size: number
  in_use: number
  idle: number
  created_total: number
  reused_total: number
  discarded_total: number
  // SRE-12: user-facing saturation rejects (→503) + recycle-drain rejects.
  saturated_rejects_total?: number
  drain_rejects_total?: number
  // SRE-16: wall-clock of the last completed writer-driven view warm.
  last_warmed_at?: string | null
  wait?: {
    count: number
    p50_ms: number
    p95_ms: number
    p99_ms: number
    max_ms: number
    mean_ms: number
  }
}

type HealthSnapshot = Omit<components['schemas']['HealthSnapshotResponse'], 'pool_wait'> & {
  pool_wait?: PoolStats[]
}

// SRE-14: 'unknown' is distinct from 'default'. A collector that errored
// (load/mem/disk → null on the backend) must NOT render as a healthy zero in
// the default foreground tone; it gets a muted tone + an explicit em-dash so
// "we couldn't measure this" reads differently from "this is a healthy low
// reading".
type Tone = 'default' | 'warn' | 'crit' | 'unknown'

/** Compact "time ago" for an age in seconds: 45s / 12m / 3.4h / 6.2d. */
function fmtAgeS(seconds: number): string {
  if (seconds < 90) return `${Math.round(seconds)}s`
  if (seconds < 5400) return `${Math.round(seconds / 60)}m`
  if (seconds < 172800) return `${(seconds / 3600).toFixed(1)}h`
  return `${(seconds / 86400).toFixed(1)}d`
}

function Stat({ label, value, sub, tone = 'default', trend, trendDomain, formatTrendValue }: {
  label: string
  value: React.ReactNode
  sub?: React.ReactNode
  tone?: Tone
  trend?: TrendPoint[]
  trendDomain?: [number | 'auto', number | 'auto']
  /** Per-stat value formatter for the hover tooltip on the sparkline.
   *  When absent, the tooltip shows the raw number to 2 decimals. */
  formatTrendValue?: (value: number) => string
}) {
  const valueClass =
    // WCAG AA (UX a11y gate): red-500/amber-500 on the white card fail 4.5:1 at
    // this 18px weight; darken in light mode only — dark mode keeps the brighter
    // shade (it sits on a dark card and already passes).
    tone === 'crit' ? 'text-red-600 dark:text-red-500' :
    tone === 'warn' ? 'text-amber-700 dark:text-amber-500' :
    tone === 'unknown' ? 'text-muted-foreground' :
    'text-foreground'
  return (
    <div className="flex flex-col gap-0.5 p-3 border rounded-lg">
      <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className={`text-lg font-semibold tabular-nums ${valueClass}`}>{value}</div>
      {sub && <div className="text-[11px] sm:text-[10px] text-muted-foreground">{sub}</div>}
      {trend && trend.length > 1 && (
        <div className={`mt-1 ${valueClass}`}>
          {/* Opt into the hover overlay even at 28px — the user wants
              to be able to inspect individual samples here, not just
              see the trend shape. */}
          <Sparkline
            points={trend}
            yDomain={trendDomain}
            interactive
            label={label}
            formatValue={formatTrendValue}
          />
        </div>
      )}
    </div>
  )
}

export function SystemHealthCard() {
  // SSR-safe: ['admin','health-snapshot'] is a live, client-only query that is
  // never seeded into the dehydrated SSR cache (the host metrics don't exist at
  // request time), so the server always renders the AnalyticsCard loading
  // overlay. If the client paints the resolved data on its first (hydration)
  // render — which happens once useAdminEventStream / the cache has primed
  // it — the markup diverges from the server's overlay and React throws a
  // hydration mismatch (#418, the /admin webkit failure). Force the first
  // client render to the server's loading state, then swap to data after mount.
  const mounted = useMounted()

  // Freshness is driven by useAdminEventStream (mounted globally
  // in SyncStatusBadge, gated to /admin and /logs paths). The 5-min
  // refetchInterval is a pure safety net for silently-dropped streams
  // — same shape as useSyncStatus / useLastSync.
  const { data: snap, isLoading, isFetching, error } = useQuery({
    queryKey: ['admin', 'health-snapshot'],
    queryFn: async () => {
      const { data } = await client.GET('/api/admin/health-snapshot')
      return data as HealthSnapshot
    },
    staleTime: 5_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  // Trend lines for the last hour. Backend sampler is 60s cadence,
  // so a 1h window gives ~60 points per series. SSE pushes via
  // useAdminEventStream; 5-min poll is the safety net.
  const { data: trends } = useQuery({
    queryKey: ['admin', 'metric-history-batch', '1h'],
    queryFn: async () => {
      const { data } = await client.GET('/api/admin/metric-history/batch', { params: { query: { since: '1h' } } })
      return data as TrendBatch
    },
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  if (!mounted || !snap) {
    return (
      <AnalyticsCard
        title="System Health"
        description="Live snapshot of the host machine — polls every 1s while this page is open."
        // Before mount, force the loading overlay so the first client render is
        // byte-identical to the server's (which never has data). The real query
        // flags take over once mounted.
        isLoading={!mounted || isLoading}
        isFetching={mounted ? isFetching : false}
        error={mounted ? (error as AnalyticsCardError | null) : null}
      >
        <div className="min-h-[120px]" />
      </AnalyticsCard>
    )
  }

  const series = trends?.series ?? {}
  // Aggregate per-service pool_wait into a single max-across-services
  // line so the card stays single-stat. Per-service breakdown stays in
  // the expandable table below.
  const poolKeys = Object.keys(series).filter((k) => k.startsWith('pool_wait_p95_ms|'))
  const poolTrend: TrendPoint[] = (() => {
    if (!poolKeys.length) return []
    const byTs = new Map<string, number>()
    for (const k of poolKeys) {
      for (const p of series[k] ?? []) {
        const prev = byTs.get(p.ts) ?? 0
        if (p.value > prev) byTs.set(p.ts, p.value)
      }
    }
    return Array.from(byTs.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ts, value]) => ({ ts, value }))
  })()

  const vcpus = snap.vcpus ?? 1
  // SRE-14: null = collector errored → 'unknown' (muted, em-dash), NOT a
  // green zero. `?? null` keeps the null distinct from a real 0 reading.
  const load1 = snap.load?.avg_1m ?? null
  // load > vCPU = backlog forming; >2× vCPU = serious overload
  const loadTone: Tone =
    load1 == null ? 'unknown' :
    load1 > vcpus * 2 ? 'crit' :
    load1 > vcpus ? 'warn' :
    'default'

  const memPct = snap.memory?.used_pct ?? null
  const memTone: Tone = memPct == null ? 'unknown' : memPct > 90 ? 'crit' : memPct > 75 ? 'warn' : 'default'

  const dataPct = snap.data_mount?.used_pct ?? null
  const dataTone: Tone = dataPct == null ? 'unknown' : dataPct > 90 ? 'crit' : dataPct > 75 ? 'warn' : 'default'

  const rootPct = snap.root_disk?.used_pct ?? null
  const rootTone: Tone = rootPct == null ? 'unknown' : rootPct > 90 ? 'crit' : rootPct > 80 ? 'warn' : 'default'

  // SRE-06: scheduler liveness. The metric sampler is a 60s global job, so a
  // stale tick age witnesses a dead scheduler (vs. healthy-idle, which the
  // "Active queries: 0" tile cannot distinguish). null = no samples yet
  // (fresh boot) → unknown, not an alarm.
  const tickAge = snap.scheduler_last_tick_age_s ?? null
  const tickTone: Tone =
    tickAge == null ? 'unknown' :
    tickAge > 180 ? 'crit' :
    tickAge > 90 ? 'warn' :
    'default'

  // SRE-03: any service+task whose latest terminal cron run errored.
  const cronFailures = snap.recent_cron_failures ?? []

  // SRE-11: config-backup freshness. null marker = never recorded → warn
  // (the only unrecoverable VM state has no captured copy). Age thresholds:
  // the recommended cadence is weekly, so >8d warns, >30d (past the bucket's
  // retention) is critical.
  const backup = snap.config_backup ?? null
  const backupAgeS = backup?.age_s ?? null
  const backupTone: Tone =
    backup == null || backupAgeS == null ? 'warn' :
    backupAgeS > 30 * 86400 ? 'crit' :
    backupAgeS > 8 * 86400 ? 'warn' :
    'default'

  const totalFiles = Object.values(snap.compaction ?? {}).reduce((acc, v) => acc + (v?.total_files ?? 0), 0)
  const aboveThreshold = Object.values(snap.compaction ?? {}).reduce((acc, v) => acc + (v?.partitions_above_3 ?? 0), 0)
  const fileTone: 'default' | 'warn' = aboveThreshold > 20 ? 'warn' : 'default'

  const inFlight = snap.in_flight_runs ?? []

  // Phase 6 in-process sampler — aggregate across services so the card
  // shows ONE p95 / p99 rather than a per-service breakdown. Per-service
  // detail is in the expandable section below.
  const pools = snap.pool_wait ?? []
  const poolMaxP95 = pools.reduce((acc, p) => Math.max(acc, p.wait?.p95_ms ?? 0), 0)
  const poolMaxP99 = pools.reduce((acc, p) => Math.max(acc, p.wait?.p99_ms ?? 0), 0)
  const poolSampleCount = pools.reduce((acc, p) => acc + (p.wait?.count ?? 0), 0)
  // SRE-12: saturation rejects (→ 503). A partial-reject cliff (<5% of
  // checkouts) can sit below the wait-p95 the tone keyed on, so p95 alone
  // missed it. Any saturation reject forces at least 'warn'.
  const poolRejects = pools.reduce((acc, p) => acc + (p.saturated_rejects_total ?? 0), 0)
  // ADR-03 escalation threshold: >50ms p95 → consider separate-process
  // cron isolation; <50ms → single-pool is sufficient.
  const poolTone: Tone =
    poolMaxP95 > 200 ? 'crit' :
    poolMaxP95 > 50 || poolRejects > 0 ? 'warn' :
    'default'

  const celery = snap.celery ?? null
  const queueDepth = celery?.queue_depth ?? null
  const activeWorkers = celery?.active_workers ?? null

  const celeryTone: Tone =
    celery == null ? 'unknown' :
    queueDepth != null && queueDepth > 500 ? 'crit' :
    queueDepth != null && queueDepth > 50 ? 'warn' :
    'default'

  return (
    <AnalyticsCard
      title="System Health"
      description="Live snapshot of the host machine — polls every 1s while this page is open."
      isFetching={isFetching}
      error={error as AnalyticsCardError | null}
    >
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat
          label="Load (1m)"
          value={load1 == null ? '–' : load1.toFixed(2)}
          sub={load1 == null
            ? 'collector unavailable'
            : `${snap.load?.avg_5m?.toFixed(2) ?? '–'} / ${snap.load?.avg_15m?.toFixed(2) ?? '–'} (5m/15m) · ${vcpus} vCPU`}
          tone={loadTone}
          trend={series['cpu_load_1m']}
          formatTrendValue={(v) => v.toFixed(2)}
        />
        <Stat
          label="Memory"
          value={memPct == null ? '–' : `${memPct.toFixed(1)}%`}
          sub={snap.memory ? `${snap.memory.available_mb}MB free / ${snap.memory.total_mb}MB` : 'collector unavailable'}
          tone={memTone}
          trend={series['mem_used_pct']}
          trendDomain={[0, 100]}
          formatTrendValue={(v) => `${v.toFixed(1)}%`}
        />
        <Stat
          label="Data disk"
          value={dataPct == null ? '–' : `${dataPct.toFixed(1)}%`}
          sub={snap.data_mount ? `${snap.data_mount.used_gb}GB / ${snap.data_mount.total_gb}GB` : 'collector unavailable'}
          tone={dataTone}
          trend={series['disk_used_pct']}
          trendDomain={[0, 100]}
          formatTrendValue={(v) => `${v.toFixed(1)}%`}
        />
        <Stat
          label="Boot disk"
          value={rootPct == null ? '–' : `${rootPct.toFixed(1)}%`}
          sub={snap.root_disk ? `${snap.root_disk.used_gb}GB / ${snap.root_disk.total_gb}GB` : 'collector unavailable'}
          tone={rootTone}
          trend={series['disk_used_pct_root']}
          trendDomain={[0, 100]}
          formatTrendValue={(v) => `${v.toFixed(1)}%`}
        />
        {celery != null && (
          <Stat
            label="Celery Queue"
            value={queueDepth == null ? '–' : `${queueDepth}`}
            sub={`${activeWorkers} Active Worker(s)`}
            tone={celeryTone}
          />
        )}
        <Stat
          label="Cache files"
          value={totalFiles}
          sub={aboveThreshold > 0 ? `${aboveThreshold} partition(s) above threshold` : 'all compacted'}
          tone={fileTone}
        />
        <Stat
          label="Active queries"
          value={inFlight.length}
          sub={inFlight.length > 0 ? `${inFlight.length} tasks running` : 'idle'}
          trend={series['active_query_count']}
          formatTrendValue={(v) => v.toFixed(0)}
        />
        <Stat
          label="Pool wait p95"
          value={poolSampleCount > 0 ? `${poolMaxP95.toFixed(1)}ms` : '–'}
          sub={poolSampleCount > 0
            ? `p99 ${poolMaxP99.toFixed(1)}ms · n=${poolSampleCount}`
            : 'no samples yet'}
          tone={poolTone}
          trend={poolTrend}
          formatTrendValue={(v) => `${v.toFixed(1)}ms`}
        />
        <Stat
          label="Pool in-use / idle"
          value={pools.reduce((acc, p) => acc + p.in_use, 0)}
          sub={pools.length > 0
            ? `${pools.reduce((acc, p) => acc + p.idle, 0)} idle · max ${pools.reduce((acc, p) => acc + p.max_size, 0)}`
            + (poolRejects > 0 ? ` · ${poolRejects} rejected` : '')
            : 'no pools yet'}
          tone={poolRejects > 0 ? 'warn' : 'default'}
        />
        <Stat
          label="Scheduler"
          value={tickAge == null ? '–' : `${fmtAgeS(tickAge)} ago`}
          sub={tickAge == null
            ? 'no ticks yet'
            : tickTone === 'default' ? 'cron loop alive' : 'last metric tick — overdue?'}
          tone={tickTone}
        />
        <Stat
          label="Config backup"
          value={backup == null || backupAgeS == null ? 'never' : `${fmtAgeS(backupAgeS)} ago`}
          sub={backup == null || backupAgeS == null
            ? 'no off-VM backup recorded'
            : (backup.last_backup_at ?? 'unknown')}
          tone={backupTone}
        />
      </div>

      {cronFailures.length > 0 && (
        // SRE-03: cross-service cron-failure glance. A non-sync per-service
        // cron erroring every tick was otherwise visible only by opening that
        // service's Cron History; surface it on the standing card.
        <div className="mt-3 rounded-lg border border-amber-500/40 bg-amber-500/5 p-3">
          <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-500 mb-1.5">
            Recent cron failures ({cronFailures.length})
          </div>
          <div className="flex flex-wrap gap-2">
            {cronFailures.slice(0, 12).map((f) => (
              <Badge
                key={`${f.service_id}:${f.task}`}
                variant="outline"
                className="text-xs border-amber-500/50"
                title={f.error_message ?? undefined}
              >
                {f.task}
                {f.service_id ? ` · ${f.service_id.slice(0, 8)}` : ''}
              </Badge>
            ))}
          </div>
        </div>
      )}

      {inFlight.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {inFlight.map(r => (
            <Badge key={String(r.run_id)} variant="outline" className="text-xs">
              {r.task}
              {r.service_id ? ` · ${r.service_id.slice(0, 8)}` : ''}
            </Badge>
          ))}
        </div>
      )}

      {pools.length > 0 && pools.some(p => (p.wait?.count ?? 0) > 0) && (
        <details className="mt-3 text-xs">
          <summary className="cursor-pointer text-muted-foreground hover:text-foreground select-none">
            Per-service pool wait (Phase 6 telemetry)
          </summary>
          <div className="mt-2 overflow-x-auto">
            <table className="w-full text-xs sm:text-[11px] tabular-nums">
              <thead className="text-muted-foreground">
                <tr>
                  <th className="text-left font-medium pr-3 pb-1">Service</th>
                  <th className="text-right font-medium px-2 pb-1">In-use</th>
                  <th className="text-right font-medium px-2 pb-1">Idle</th>
                  <th className="text-right font-medium px-2 pb-1">Samples</th>
                  <th className="text-right font-medium px-2 pb-1">p50</th>
                  <th className="text-right font-medium px-2 pb-1">p95</th>
                  <th className="text-right font-medium px-2 pb-1">p99</th>
                  <th className="text-right font-medium px-2 pb-1">max</th>
                  {/* SRE-12 + SRE-16 */}
                  <th className="text-right font-medium px-2 pb-1">503s</th>
                  <th className="text-right font-medium px-2 pb-1">Warmed</th>
                </tr>
              </thead>
              <tbody>
                {pools.map(p => (
                  <tr key={p.service} className="border-t border-muted/40">
                    <td className="pr-3 py-1 font-mono">{p.service.slice(0, 22)}</td>
                    <td className="text-right px-2 py-1">{p.in_use}/{p.max_size}</td>
                    <td className="text-right px-2 py-1">{p.idle}</td>
                    <td className="text-right px-2 py-1">{p.wait?.count ?? 0}</td>
                    <td className="text-right px-2 py-1">{p.wait?.p50_ms?.toFixed(1) ?? '–'}</td>
                    <td className="text-right px-2 py-1">{p.wait?.p95_ms?.toFixed(1) ?? '–'}</td>
                    <td className="text-right px-2 py-1">{p.wait?.p99_ms?.toFixed(1) ?? '–'}</td>
                    <td className="text-right px-2 py-1">{p.wait?.max_ms?.toFixed(1) ?? '–'}</td>
                    <td className={`text-right px-2 py-1 ${(p.saturated_rejects_total ?? 0) > 0 ? 'text-amber-700 dark:text-amber-500' : ''}`}>
                      {p.saturated_rejects_total ?? 0}
                    </td>
                    <td className="text-right px-2 py-1">{p.last_warmed_at ? p.last_warmed_at.slice(11, 19) : '–'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-1 text-[11px] sm:text-[10px] text-muted-foreground">
              Wait-time samples over the last ~1024 checkouts per service. ADR-03 escalation threshold: p95 &gt; 50ms ⇒ consider separate-process cron isolation.
              503s = pool-saturation rejects (SRE-12); Warmed = last writer-driven view rebuild (SRE-16).
              Same samples stream to OTel ``app.thread_wait_ms`` for off-box analysis.
            </p>
          </div>
        </details>
      )}

      {/* SRE-20: effective logging/telemetry mode — tells an incident
          responder whether a `jq`/`grep` over docker logs will match JSON
          or bracketed console text, and whether OTel trace fields exist. */}
      {snap.observability && (
        <p className="mt-3 text-[11px] sm:text-[10px] text-muted-foreground">
          Logs: <span className="font-mono">{snap.observability.log_format}</span>
          {' · '}OTel exporter: <span className="font-mono">{snap.observability.otel_exporter}</span>
          {snap.observability.otel_exporter === 'none' && ' (request ids are app-minted; trace fields empty)'}
        </p>
      )}
    </AnalyticsCard>
  )
}
