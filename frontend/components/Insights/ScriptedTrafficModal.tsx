'use client'

import React from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import { ScanSearch, Gauge, Timer, Activity, Repeat, Hash, Users, Clock } from 'lucide-react'
import { cn } from '@/lib/utils'
import { ScriptedTrafficData } from './types'

interface ScriptedTrafficModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ScriptedTrafficData | null
}

// Seconds → compact human string ("45s", "12m 30s", "2h 15m", "3d 4h").
function humanizeDuration(totalSeconds: number): string {
  const s = Math.max(0, Math.round(totalSeconds))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) {
    const rem = s % 60
    return rem ? `${m}m ${rem}s` : `${m}m`
  }
  const h = Math.floor(m / 60)
  if (h < 24) {
    const remM = m % 60
    return remM ? `${h}h ${remM}m` : `${h}h`
  }
  const d = Math.floor(h / 24)
  const remH = h % 24
  return remH ? `${d}d ${remH}h` : `${d}d`
}

// Inter-arrival gap: keep sub-90s readable in seconds, humanize longer cadences.
function formatInterval(sec: number | null): string {
  if (sec == null) return '—'
  if (sec < 90) return `${sec.toLocaleString(undefined, { maximumFractionDigits: 1 })}s`
  return humanizeDuration(sec)
}

interface StatCardProps {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

function StatCard({ icon, label, value, valueClass, hint }: StatCardProps) {
  return (
    <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
        {icon} {label}
      </div>
      <div className={cn('mt-1.5 font-mono text-xl font-bold', valueClass ?? 'text-foreground')}>
        {value}
      </div>
      {hint && <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>}
    </div>
  )
}

export function ScriptedTrafficModal({ isOpen, onOpenChange, data }: ScriptedTrafficModalProps) {
  if (!data) return null

  const modalPct = Math.round(data.modal_frac * 100)
  const span = humanizeDuration(data.span_s)
  // Score colour tracks the backend severity bands (≥90 critical, ≥70 warning).
  const scoreColor =
    data.score >= 90 ? 'text-red-500' : data.score >= 70 ? 'text-amber-500' : 'text-blue-500'

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            <ScanSearch className="h-5 w-5 text-primary shrink-0" />
            <span className="truncate max-w-[90%]">Why we flagged this: {data.label}</span>
          </DialogTitle>
          <DialogDescription>
            Evidence that this client&apos;s request timing is machine-regular rather than human.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">
          {/* Plain-language verdict assembled from the evidence below. */}
          <div className="p-4 rounded-lg bg-muted/30 border border-border">
            <p className="text-sm leading-relaxed">
              <span className="font-semibold">{data.label}</span> made{' '}
              <span className="font-semibold">{data.n_events.toLocaleString()}</span> requests over{' '}
              <span className="font-semibold">{span}</span> on a near-constant{' '}
              <span className="font-semibold">~{formatInterval(data.mean_interval_s)}</span> cadence
              {' '}— <span className="font-semibold">{modalPct}%</span> of the gaps between requests
              were identical, with only <span className="font-semibold">{formatInterval(data.stddev_s)}</span>{' '}
              of jitter (σ). A regularity score of{' '}
              <span className={cn('font-semibold', scoreColor)}>{data.score}/100</span> is far more
              consistent with an automated script — a scraper, poller, or cron-scheduled job — than
              with human browsing.
            </p>
          </div>

          {/* Measured signals. */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Regularity Score"
              value={`${data.score}/100`}
              valueClass={scoreColor}
              hint="higher = more machine-like"
            />
            <StatCard
              icon={<Timer className="h-3 w-3" />}
              label="Mean Interval"
              value={formatInterval(data.mean_interval_s)}
              hint="avg. gap between requests"
            />
            <StatCard
              icon={<Activity className="h-3 w-3" />}
              label="Jitter (σ)"
              value={formatInterval(data.stddev_s)}
              hint="std. dev. of gaps"
            />
            <StatCard
              icon={<Repeat className="h-3 w-3" />}
              label="Modal Gap"
              value={formatInterval(data.mode_gap_s)}
              hint={`${modalPct}% of gaps match`}
            />
            <StatCard
              icon={<Hash className="h-3 w-3" />}
              label="Total Requests"
              value={data.n_events.toLocaleString()}
              hint={`${data.n_gaps.toLocaleString()} gaps measured`}
            />
            <StatCard
              icon={<Clock className="h-3 w-3" />}
              label="Observation Span"
              value={span}
            />
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Requests / sec"
              value={data.rps.toLocaleString(undefined, { maximumFractionDigits: 3 })}
            />
            <StatCard
              icon={<Users className="h-3 w-3" />}
              label="Distinct UAs"
              value={data.distinct_ua.toLocaleString()}
            />
          </div>

          {/* Why each signal matters — keep the score from being a black box. */}
          <div className="rounded-lg bg-muted/10 border border-border p-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
            </h4>
            <ul className="space-y-2.5 text-xs leading-relaxed">
              <li>
                <span className="font-semibold">Cadence regularity (CV {data.cv.toFixed(2)}).</span>{' '}
                A low coefficient of variation means the time between requests barely changes — human
                traffic is bursty and irregular, scripts are not.
              </li>
              <li>
                <span className="font-semibold">Modal dominance ({modalPct}%).</span>{' '}
                The share of inter-arrival gaps that are exactly the modal interval
                {data.mode_gap_s != null ? <> (<span className="font-mono">{formatInterval(data.mode_gap_s)}</span>)</> : null}.
                A high share points to fixed-interval scheduling, like a cron job or polling loop.
              </li>
              <li>
                <span className="font-semibold">Volume &amp; span.</span>{' '}
                {data.n_events.toLocaleString()} requests over {span} ({data.rps.toLocaleString(undefined, { maximumFractionDigits: 3 })} req/s),
                sustained but below volumetric rate-limit thresholds — the pattern this insight is
                designed to catch.
              </li>
              <li>
                <span className="font-semibold">Distinct user-agents ({data.distinct_ua.toLocaleString()}).</span>{' '}
                Shown for context only — it is <span className="italic">not</span> a reason to dismiss
                the flag. A high UA count combined with this regular timing usually signals a
                UA-rotating scraper, which is <span className="font-semibold">more</span> suspicious,
                not less.
              </li>
            </ul>
          </div>
        </div>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}
