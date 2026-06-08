'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { client } from '@/lib/api'

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
    // 10s polling. Pre-fix this was 2s for "live ping" feel — but the
    // endpoint that was claimed to be 20ms cheap routinely took
    // 1-1.7s when the backend was under sync load, which meant the
    // page was constantly waiting on health-snapshot. Combined with
    // navigation away from /admin (the old in-flight request kept
    // running because queryFns don't pass signal yet), clicks felt
    // sluggish. 10s is plenty for an operator glance — there's a
    // refresh button below if real-time matters.
    refetchInterval: 10_000,
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

  return (
    <AnalyticsCard title="System Health" description="Live snapshot of the host machine — polls every 15s while this page is open.">
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
    </AnalyticsCard>
  )
}
