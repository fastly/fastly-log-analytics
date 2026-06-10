'use client'

import * as React from 'react'

interface BucketCounts {
  total: number
  good: number
  bad: number
  unlabeled: number
}

interface ThresholdMatrixProps {
  flagged: BucketCounts
  passed: Omit<BucketCounts, 'total'>
}

/**
 * 2x2 split of scored sessions at the previewed threshold:
 *   - Would FLAG (warn tint) vs Would PASS (good tint)
 *   - Within each, good / bad / unlabeled tallies
 *
 * Pure presentational — parent computes counts from the preview response.
 */
export function ThresholdMatrix({ flagged, passed }: ThresholdMatrixProps) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <Bucket
        title="Would FLAG"
        total={flagged.total}
        good={flagged.good}
        bad={flagged.bad}
        unlabeled={flagged.unlabeled}
        tone="warn"
      />
      <Bucket
        title="Would PASS"
        total={passed.good + passed.bad + passed.unlabeled}
        good={passed.good}
        bad={passed.bad}
        unlabeled={passed.unlabeled}
        tone="good"
      />
    </div>
  )
}

function Bucket({
  title,
  total,
  good,
  bad,
  unlabeled,
  tone,
}: {
  title: string
  total: number
  good: number
  bad: number
  unlabeled: number
  tone: 'warn' | 'good'
}) {
  const tint = tone === 'warn' ? 'border-amber-300 bg-amber-50/50' : 'border-emerald-300 bg-emerald-50/40'
  return (
    <div className={`p-3 border rounded-md ${tint}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{title}</div>
      <div className="text-xl font-mono font-semibold tabular-nums">{total.toLocaleString()}</div>
      <div className="mt-1 space-y-0.5 text-[11px]">
        <div className="flex justify-between">
          <span className="text-emerald-700">good</span>
          <span className="font-mono tabular-nums">{good.toLocaleString()}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-destructive">bad</span>
          <span className="font-mono tabular-nums">{bad.toLocaleString()}</span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>unlabeled</span>
          <span className="font-mono tabular-nums">{unlabeled.toLocaleString()}</span>
        </div>
      </div>
    </div>
  )
}
