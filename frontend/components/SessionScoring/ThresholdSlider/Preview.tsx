'use client'

import * as React from 'react'

interface ThresholdPreviewStatsProps {
  precision: number | null
  recall: number | null
  totalScoredSessions: number
  sinceHours: number
}

/**
 * Precision/recall stat tiles + caption explaining the preview semantics.
 * Pure presentational; the matrix tiles live in <ThresholdMatrix/>.
 */
export function ThresholdPreviewStats({
  precision,
  recall,
  totalScoredSessions,
  sinceHours,
}: ThresholdPreviewStatsProps) {
  return (
    <>
      <div className="grid grid-cols-2 gap-3 text-xs">
        <Stat
          label="Precision"
          value={precision != null ? `${(precision * 100).toFixed(1)}%` : '—'}
          hint="of labeled flagged sessions, how many are bad"
        />
        <Stat
          label="Recall"
          value={recall != null ? `${(recall * 100).toFixed(1)}%` : '—'}
          hint="of all labeled-bad sessions, how many got flagged"
        />
      </div>

      <p className="text-[11px] text-muted-foreground italic">
        {totalScoredSessions.toLocaleString()} distinct scored sessions in the last{' '}
        {sinceHours}h. Precision/recall only count sessions you&apos;ve labeled —
        the &quot;unlabeled&quot; tally is everything else.
      </p>
    </>
  )
}

function Stat({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className="p-3 border rounded-md">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="text-lg font-mono font-semibold tabular-nums">{value}</div>
      <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>
    </div>
  )
}
