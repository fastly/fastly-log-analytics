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

const POLL_MS = 10_000

type SummaryResponse = {
  active_total: number
  by_db_type: Record<string, number>
  longest_ms: number
}

type LogAccountingTotals = {
  fastly_logs: number
  our_rows: number
  gap: number
  gap_pct: number
  worst_bucket_ts: string | null
  worst_bucket_gap_pct: number | null
}

type SustainedLossAlert = {
  started_at: string
  n_buckets: number
  max_gap_pct: number
  total_lost_lines: number
}

type LogAccountingResponse = {
  totals?: LogAccountingTotals
  sustained_loss?: SustainedLossAlert | null
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
  const { data } = useQuery<SummaryResponse>({
    queryKey: ['admin', 'overview', 'queries-summary'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/queries/summary', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    refetchInterval: POLL_MS,
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
    />
  )
}

// ── Card 2: ingest gap ───────────────────────────────────────────────────

function IngestHealthCard() {
  const { data } = useQuery<LogAccountingResponse>({
    queryKey: ['admin', 'overview', 'log-accounting'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/log-accounting?hours=24', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    // Refresh slower than the others — Fastly Stats lags by minutes and
    // this drives a DuckDB COUNT(*) on the per-service connection pool.
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
  })
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
  const { data } = useQuery<{ count: number }>({
    queryKey: ['admin', 'overview', 'slow-queries-count'],
    queryFn: async ({ signal }) => {
      const r = await fetch(
        '/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000',
        { signal },
      )
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    refetchInterval: POLL_MS,
    refetchIntervalInBackground: false,
  })
  const slowCount = data?.count ?? 0
  return (
    <OverviewCard
      href="/admin/queries?view=past&slow=1000"
      icon={<Activity className="h-4 w-4" />}
      title="Notable Slow Queries"
      primary={String(slowCount)}
      primaryTone={slowCount > 0 ? 'attention' : 'muted'}
      secondary={slowCount === 0 ? 'none ≥ 1s in last 24h' : '≥ 1s in last 24h'}
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
}: {
  href: string
  icon: React.ReactNode
  title: string
  primary: string
  primaryTone: CardTone
  secondary: string
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
            <Badge variant="outline" className="text-[10px]">view →</Badge>
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
