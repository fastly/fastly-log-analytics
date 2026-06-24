import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

interface DeltaIndicatorProps {
  current: number
  baseline: number | null | undefined
}

export function DeltaIndicator({ current, baseline }: DeltaIndicatorProps) {
  if (baseline == null || baseline === 0) return null
  const pct = ((current - baseline) / Math.abs(baseline)) * 100
  const abs = current - baseline
  const absStr = abs > 0 ? `+${abs.toLocaleString(undefined, { maximumFractionDigits: 1 })}` : abs.toLocaleString(undefined, { maximumFractionDigits: 1 })

  if (Math.abs(pct) < 1) return <Minus className="h-3 w-3 text-muted-foreground" />

  if (pct > 0) return (
    <span className="flex items-center gap-0.5 text-red-500 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingUp className="h-3 w-3" />
      {absStr} (+{Math.round(pct)}%)
    </span>
  )

  return (
    <span className="flex items-center gap-0.5 text-green-500 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingDown className="h-3 w-3" />
      {absStr} ({Math.round(pct)}%)
    </span>
  )
}
