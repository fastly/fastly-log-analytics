'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { client } from '@/lib/api'

type PoolStats = {
  service: string
  max_size: number
  in_use: number
  idle: number
  created_total: number
  reused_total: number
  discarded_total: number
  wait?: {
    count: number
    p50_ms: number
    p95_ms: number
    p99_ms: number
    max_ms: number
    mean_ms: number
  }
}

type HealthSnapshot = {
  vcpus?: number | null
  load?: { avg_1m: number; avg_5m: number; avg_15m: number } | null
  memory?: { total_mb: number; available_mb: number; used_pct: number | null } | null
  data_mount?: { total_gb: number; used_gb: number; free_gb: number; used_pct: number | null } | null
  root_disk?: { total_gb: number; used_gb: number; free_gb: number; used_pct: number | null } | null
  in_flight_runs?: { run_id: string | number; service_id: string; task: string; started_at?: string }[]
  compaction?: Record<string, {
    total_files: number
    partitions: number
    partitions_above_3: number
    partitions_above_10: number
    daily_files: number
    avg_files_per_partition: number
  } | null>
  pool_wait?: PoolStats[]
}

function Stat({ label, value, sub, tone = 'default' }: {
  label: string
  value: React.ReactNode
  sub?: React.ReactNode
  tone?: 'default' | 'warn' | 'crit'
}) {
  const valueClass =
    tone === 'crit' ? 'text-red-500' :
    tone === 'warn' ? 'text-amber-500' :
    'text-foreground'
  return (
    <div className="flex flex-col gap-0.5 p-3 border rounded-lg">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className={`text-lg font-semibold tabular-nums ${valueClass}`}>{value}</div>
      {sub && <div className="text-[10px] text-muted-foreground">{sub}</div>}
    </div>
  )
}

export function SystemHealthCard() {
  const { data: snap } = useQuery({
    queryKey: ['admin', 'health-snapshot'],
    queryFn: async () => {
      const { data } = await client.GET('/api/admin/health-snapshot' as any, {} as any)
      return data as HealthSnapshot
    },
    // 1s polling. The endpoint is OS-level reads + per-service
    // compaction_stats (top-level os.listdir, NOT recursive); no DB,
    // no FOS, no network. Per-service cost is ~5-30ms; at 1-10
    // services per backend that's ~30-300ms per poll, well under one
    // worker's capacity. Gives operator-grade live feedback for the
    // "is the box healthy?" glance — useful during an attack or sync
    // backlog when load can climb second-to-second. Caveat: a future
    // change that grows N to 50+ services per backend, or that adds
    // a recursive walk inside compaction_stats, would need to revisit
    // this interval.
    refetchInterval: 1_000,
    refetchIntervalInBackground: false,
  })

  if (!snap) return null

  const vcpus = snap.vcpus ?? 1
  const load1 = snap.load?.avg_1m ?? 0
  // load > vCPU = backlog forming; >2× vCPU = serious overload
  const loadTone: 'default' | 'warn' | 'crit' =
    load1 > vcpus * 2 ? 'crit' :
    load1 > vcpus ? 'warn' :
    'default'

  const memPct = snap.memory?.used_pct ?? 0
  const memTone: 'default' | 'warn' | 'crit' = memPct > 90 ? 'crit' : memPct > 75 ? 'warn' : 'default'

  const dataPct = snap.data_mount?.used_pct ?? 0
  const dataTone: 'default' | 'warn' | 'crit' = dataPct > 90 ? 'crit' : dataPct > 75 ? 'warn' : 'default'

  const rootPct = snap.root_disk?.used_pct ?? 0
  const rootTone: 'default' | 'warn' | 'crit' = rootPct > 90 ? 'crit' : rootPct > 80 ? 'warn' : 'default'

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
  // ADR-03 escalation threshold: >50ms p95 → consider separate-process
  // cron isolation; <50ms → single-pool is sufficient.
  const poolTone: 'default' | 'warn' | 'crit' =
    poolMaxP95 > 200 ? 'crit' :
    poolMaxP95 > 50 ? 'warn' :
    'default'

  return (
    <AnalyticsCard title="System Health" description="Live snapshot of the host machine — polls every 1s while this page is open.">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat
          label="Load (1m)"
          value={load1.toFixed(2)}
          sub={`${snap.load?.avg_5m?.toFixed(2) ?? '–'} / ${snap.load?.avg_15m?.toFixed(2) ?? '–'} (5m/15m) · ${vcpus} vCPU`}
          tone={loadTone}
        />
        <Stat
          label="Memory"
          value={`${memPct.toFixed(1)}%`}
          sub={snap.memory ? `${snap.memory.available_mb}MB free / ${snap.memory.total_mb}MB` : '–'}
          tone={memTone}
        />
        <Stat
          label="Data disk"
          value={`${dataPct.toFixed(1)}%`}
          sub={snap.data_mount ? `${snap.data_mount.used_gb}GB / ${snap.data_mount.total_gb}GB` : '–'}
          tone={dataTone}
        />
        <Stat
          label="Boot disk"
          value={`${rootPct.toFixed(1)}%`}
          sub={snap.root_disk ? `${snap.root_disk.used_gb}GB / ${snap.root_disk.total_gb}GB` : '–'}
          tone={rootTone}
        />
        <Stat
          label="Cache files"
          value={totalFiles}
          sub={aboveThreshold > 0 ? `${aboveThreshold} partition(s) above threshold` : 'all compacted'}
          tone={fileTone}
        />
        <Stat
          label="In-flight crons"
          value={inFlight.length}
          sub={inFlight.length > 0 ? inFlight.slice(0, 2).map(r => r.task).join(', ') : 'idle'}
        />
        <Stat
          label="Pool wait p95"
          value={poolSampleCount > 0 ? `${poolMaxP95.toFixed(1)}ms` : '–'}
          sub={poolSampleCount > 0
            ? `p99 ${poolMaxP99.toFixed(1)}ms · n=${poolSampleCount}`
            : 'no samples yet'}
          tone={poolTone}
        />
        <Stat
          label="Pool in-use / idle"
          value={pools.reduce((acc, p) => acc + p.in_use, 0)}
          sub={pools.length > 0
            ? `${pools.reduce((acc, p) => acc + p.idle, 0)} idle · max ${pools.reduce((acc, p) => acc + p.max_size, 0)}`
            : 'no pools yet'}
        />
      </div>

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
            <table className="w-full text-[11px] tabular-nums">
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
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-1 text-[10px] text-muted-foreground">
              Wait-time samples over the last ~1024 checkouts per service. ADR-03 escalation threshold: p95 &gt; 50ms ⇒ consider separate-process cron isolation.
              Same samples stream to OTel ``app.thread_wait_ms`` for off-box analysis.
            </p>
          </div>
        </details>
      )}
    </AnalyticsCard>
  )
}
