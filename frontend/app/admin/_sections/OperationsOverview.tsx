'use client'

/**
 * Operations Overview — at-a-glance status row at the top of /admin so
 * operators see ingest health + live query activity without having to
 * navigate into the sub-pages. Cards are entirely clickable; each links
 * to the page that owns the full UI for that subsystem.
 *
 * The 2026-06-12 incident-debug session that motivated this: the
 * sustained_loss alert from log-accounting was buried in a sub-page
 * that nobody routinely visited. A 47% ingest gap sat undetected for
 * ~12 days. Surfacing the gap on /admin (the page operators DO open
 * regularly) closes that visibility gap.
 *
 * Polls every 10s (not every 1s like the dashboard) because the values
 * here change on a minutes-to-hours cadence; sub-second freshness costs
 * RSS + DB pressure for no benefit.
 */

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import Link from 'next/link'
import { Activity, Database, AlertTriangle } from 'lucide-react'

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { useBootstrap } from '@/hooks/useBootstrap'
import type { components } from '@/types/api.generated'
import { adminFetch } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'

const POLL_MS = 10_000

type SummaryResponse = components['schemas']['SummaryResponse']
type LogAccountingResponse = components['schemas']['LogAccountingResponse']

type OpsOverviewSeed = {
  queries_summary?: SummaryResponse
  log_accounting?: LogAccountingResponse
  slow_queries_count?: { count: number; since_hours: number; threshold_ms: number }
}

function useOpsOverviewSeed(): OpsOverviewSeed {
  const { data } = useBootstrap()
  return (data as { ops_overview?: OpsOverviewSeed } | undefined)?.ops_overview ?? {}
}

export function OperationsOverview() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
      <LiveQueriesCard />
      <IngestHealthCard />
      <SlowQueriesTeaser />
    </div>
  )
}

// ── Card 1: live query activity ───────────────────────────────────────────

function LiveQueriesCard() {
  const seed = useOpsOverviewSeed().queries_summary
  // Freshness via useSystemMetricsStream (mounted in SyncStatusBadge).
  // 5-min refetchInterval is a pure safety net.
  const { data, isError } = useQuery<SummaryResponse>({
    queryKey: ['admin', 'overview', 'queries-summary'],
    queryFn: async ({ signal }) => {
      const r = await adminFetch('/api/admin/queries/summary', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    initialData: seed,
    staleTime: POLL_MS,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  const active = data?.active_total ?? 0
  return (
    <OverviewCard
      href="/admin/queries"
      icon={<Activity className="h-4 w-4" />}
      title="Live Queries"
      primary={String(active)}
      primaryTone={active > 0 ? 'default' : 'muted'}
      secondary={
        data?.by_db_type
          ? Object.entries(data.by_db_type)
              .filter(([, n]) => n > 0)
              .map(([db, n]) => `${n} ${db}`)
              .join(' · ') || 'idle'
          : '—'
      }
      isStale={isError && !!data}
    />
  )
}

// ── Card 2: ingest gap ───────────────────────────────────────────────────

function IngestHealthCard() {
  // log-accounting is service-scoped (per-service ingest gap); the backend
  // 400s without a service. On a fresh install there is none yet, so gate
  // the fetch off — firing it just spams the console with 400s and the gap
  // is meaningless with nothing ingested.
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const seed = useOpsOverviewSeed().log_accounting
  const { data, isError } = useQuery<LogAccountingResponse>({
    queryKey: ['admin', 'overview', 'log-accounting'],
    queryFn: async ({ signal }) => {
      const r = await adminFetch('/api/admin/log-accounting?hours=24', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    initialData: seed,
    enabled: !!activeServiceId,
    // Freshness via useSystemMetricsStream. 5-min poll is the safety
    // net; the DuckDB COUNT(*) only runs on the safety tick now.
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  if (!activeServiceId) {
    return (
      <OverviewCard
        href="/admin/usage-log"
        icon={<Database className="h-4 w-4" />}
        title="Ingest Gap"
        primary="—"
        primaryTone="muted"
        secondary="no service yet"
      />
    )
  }
  const gapPct = data?.totals?.gap_pct ?? 0
  const sustained = data?.sustained_loss
  // gap_pct can be negative (we have more rows than Fastly — usually
  // in-flight bucket noise). Only POSITIVE gaps mean real loss; that's
  // what the tone should reflect.
  const tone: CardTone = sustained
    ? 'critical'
    : gapPct >= 0.1
      ? 'warning'
      : gapPct >= 0.02
        ? 'attention'
        : 'default'
  const primary = data?.totals
    ? `${(gapPct * 100).toFixed(gapPct === 0 ? 0 : 1)}%`
    : '—'
  const secondary = sustained
    ? `sustained: ${sustained.n_buckets} bucket(s), ${sustained.total_lost_lines.toLocaleString()} lost`
    : gapPct >= 0.02
      ? 'recent loss — check log accounting'
      : 'healthy · 24h'
  return (
    <OverviewCard
      href="/admin/usage-log"
      icon={
        tone === 'critical' || tone === 'warning' ? (
          <AlertTriangle className="h-4 w-4" />
        ) : (
          <Database className="h-4 w-4" />
        )
      }
      title="Ingest Gap"
      primary={primary}
      primaryTone={tone}
      secondary={secondary}
      isStale={isError && !!data}
    />
  )
}

// ── Card 3: slow queries teaser ──────────────────────────────────────────

function SlowQueriesTeaser() {
  // Use the persistent ``slow_queries`` SQLite via the dedicated count
  // endpoint instead of filtering the in-memory snapshot client-side.
  // Three wins: (1) the count is time-bounded ("in last 24 h") instead
  // of size-bounded ("in last 2000-query window"), which is what an
  // operator actually wants; (2) survives restarts; (3) single indexed
  // COUNT(*) instead of shipping a 2000-row JSON payload.
  // slow-queries/count is service-scoped (the backend 422s `service_id_required`
  // without one). Gate it off on a fresh install so we don't spam 422s before
  // any service — and any query exists — to count.
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const seed = useOpsOverviewSeed().slow_queries_count
  const { data, isError } = useQuery<{ count: number }>({
    queryKey: ['admin', 'overview', 'slow-queries-count'],
    queryFn: async ({ signal }) => {
      const r = await adminFetch(
        '/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000',
        { signal },
      )
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    initialData: seed,
    enabled: !!activeServiceId,
    // Freshness via useSystemMetricsStream. Safety-net poll only.
    staleTime: POLL_MS,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  if (!activeServiceId) {
    return (
      <OverviewCard
        href="/admin/queries?view=past&slow=1000"
        icon={<Activity className="h-4 w-4" />}
        title="Notable Slow Queries"
        primary="—"
        primaryTone="muted"
        secondary="no service yet"
      />
    )
  }
  const slowCount = data?.count ?? 0
  return (
    <OverviewCard
      href="/admin/queries?view=past&slow=1000"
      icon={<Activity className="h-4 w-4" />}
      title="Notable Slow Queries"
      primary={String(slowCount)}
      primaryTone={slowCount > 0 ? 'attention' : 'muted'}
      secondary={slowCount === 0 ? 'none ≥ 1s in last 24h' : '≥ 1s in last 24h'}
      isStale={isError && !!data}
    />
  )
}

// ── Shared primitive ─────────────────────────────────────────────────────

type CardTone = 'default' | 'muted' | 'attention' | 'warning' | 'critical'

function OverviewCard({
  href,
  icon,
  title,
  primary,
  primaryTone,
  secondary,
  isStale = false,
}: {
  href: string
  icon: React.ReactNode
  title: string
  primary: string
  primaryTone: CardTone
  secondary: string
  /** Refetch failed but seed/last-good data is still rendered. Surface a
   *  small "stale" badge so operators don't read the seeded value as live.
   *  Without this, a backend hiccup silently freezes the number with no
   *  signal — the exact failure mode the post-2026-06-12 OperationsOverview
   *  was built to prevent. */
  isStale?: boolean
}) {
  // Tone-driven classes for the primary metric. Keep these as literal
  // strings so Tailwind's content-scanner can see them — building class
  // names dynamically (e.g. `text-${tone}-600`) would be invisible to
  // the build and silently render as default colour.
  const primaryClass =
    primaryTone === 'critical'
      ? 'text-red-600 dark:text-red-400'
      : primaryTone === 'warning'
        ? 'text-orange-600 dark:text-orange-400'
        : primaryTone === 'attention'
          ? 'text-amber-600 dark:text-amber-400'
          : primaryTone === 'muted'
            ? 'text-muted-foreground'
            : 'text-foreground'
  return (
    <Link href={href} className="block group">
      <Card className="transition-colors group-hover:bg-muted/40 cursor-pointer">
        <CardContent className="p-4">
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-2">
            <span className="flex items-center gap-1.5">
              {icon}
              {title}
            </span>
            <div className="flex items-center gap-1.5">
              {isStale && (
                <Badge
                  variant="outline"
                  className="text-[10px] border-amber-500/40 text-amber-700 dark:text-amber-400"
                  title="Refetch failed — showing last known value"
                >
                  stale
                </Badge>
              )}
              <Badge variant="outline" className="text-[10px]">view →</Badge>
            </div>
          </div>
          <div className={`text-3xl font-semibold tabular-nums ${primaryClass}`}>
            {primary}
          </div>
          <div className="text-xs text-muted-foreground mt-1 truncate">{secondary}</div>
        </CardContent>
      </Card>
    </Link>
  )
}
