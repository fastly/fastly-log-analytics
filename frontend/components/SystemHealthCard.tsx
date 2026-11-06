'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { ELAPSED_AGE_COMPACT, formatElapsed } from "@/lib/utils"
import { useMounted } from '@/hooks/useBootstrap'
import { client, unwrapClient, queryKeys } from "@/lib/api";
import type { components } from '@/types/api.generated'

export const TICK_BACKEND_CALL_CEILING = 32
export const TICK_VCPU_BUDGET_MS = 1000
export const DEFAULT_CLOCK_INTERVAL_S = 120

export function summarizeTickHealth(runs: Array<{ started_at?: string; backend_calls?: number; vcpu_ms?: number; status?: string }>, nowMs = Date.now()) {
  const latest = runs[0]
  const last_tick_at = latest?.started_at ?? null
  const clock_interval_s = DEFAULT_CLOCK_INTERVAL_S
  const maxBackendCalls = runs.reduce((m, r) => Math.max(m, r.backend_calls ?? 0), 0)
  const maxVcpuMs = runs.reduce((m, r) => Math.max(m, r.vcpu_ms ?? 0), 0)
  const ageSec = last_tick_at ? Math.max(0, (nowMs - Date.parse(last_tick_at)) / 1000) : null
  const healthy =
    ageSec !== null &&
    ageSec <= clock_interval_s * 3 &&
    maxBackendCalls <= TICK_BACKEND_CALL_CEILING &&
    maxVcpuMs <= TICK_VCPU_BUDGET_MS
  return {
    tick_health: healthy ? 'healthy' : 'degraded',
    last_tick_at,
    clock_interval_s,
    maxBackendCalls,
    maxVcpuMs,
  }
}

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']
type HealthSnapshot = components['schemas']['HealthSnapshotResponse']

// SRE-14: 'unknown' is distinct from 'default'. A collector that errored
// (load/mem/disk → null on the backend) must NOT render as a healthy zero in
// the default foreground tone; it gets a muted tone + an explicit em-dash so
// "we couldn't measure this" reads differently from "this is a healthy low
// reading".
type Tone = 'default' | 'warn' | 'crit' | 'unknown'

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
    queryKey: queryKeys.admin.healthSnapshot(),
    queryFn: async () => {
      return (await unwrapClient(
        client.GET('/api/admin/health-snapshot'),
        'Failed to load system health snapshot',
      )) as HealthSnapshot
    },
    staleTime: 5_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  // Trend lines for the last hour. Backend sampler is 60s cadence,
  // so a 1h window gives ~60 points per series. SSE pushes via
  // useAdminEventStream; 5-min poll is the safety net.
  const { data: trends } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch('1h'),
    queryFn: async () => {
      return (await unwrapClient(
        client.GET('/api/admin/metric-history/batch', { params: { query: { since: '1h' } } }),
        'Failed to load metric history batch',
      )) as TrendBatch
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

  const inFlight = snap.in_flight_runs ?? []

  const hsDisk = snap.high_scale?.disk ?? null
  const hsTopology = snap.high_scale?.topology ?? null
  const chTone: Tone =
    hsDisk?.mode === 'emergency' || hsDisk?.mode === 'protective' ? 'crit' :
    hsDisk?.mode === 'warning' ? 'warn' :
    'default'

  const celery = snap.celery ?? null
  const queueDepth = celery?.queue_depth ?? null
  const activeWorkers = celery?.active_workers ?? null
  const brokerReachable = celery?.broker_reachable ?? true

  const celeryTone: Tone =
    celery == null ? 'unknown' :
    !brokerReachable ? 'crit' :
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
            value={!brokerReachable ? 'OFFLINE' : queueDepth == null ? '–' : `${queueDepth}`}
            sub={!brokerReachable ? 'Broker Unreachable' : `${activeWorkers} Active Worker(s)`}
            tone={celeryTone}
          />
        )}
        <Stat
          label="ClickHouse"
          value={hsDisk ? hsDisk.mode.toUpperCase() : 'ONLINE'}
          sub={hsTopology ? `${hsTopology.name} · ${hsTopology.shard_count} shard(s)` : 'analytical store'}
          tone={chTone}
        />
        <Stat
          label="Active ClickHouse queries"
          value={inFlight.length}
          sub={inFlight.length > 0 ? `${inFlight.length} tasks running` : 'idle'}
          trend={series['active_query_count']}
          formatTrendValue={(v) => v.toFixed(0)}
        />
        <Stat
          label="Scheduler"
          value={tickAge == null ? '–' : `${formatElapsed(tickAge, ELAPSED_AGE_COMPACT)} ago`}
          sub={tickAge == null
            ? 'no ticks yet'
            : tickTone === 'default' ? 'cron loop alive' : 'last metric tick — overdue?'}
          tone={tickTone}
        />
        <Stat
          label="Config backup"
          value={backup == null || backupAgeS == null ? 'never' : `${formatElapsed(backupAgeS, ELAPSED_AGE_COMPACT)} ago`}
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
