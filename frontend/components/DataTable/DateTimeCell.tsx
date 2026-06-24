import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'
import { TimeAgo } from '@/components/TimeAgo'

interface DateTimeCellProps {
  iso: string | null | undefined
  className?: string
  emptyFallback?: React.ReactNode
}

export function DateTimeCell({ iso, className, emptyFallback }: DateTimeCellProps) {
  // Absolute time (tooltip content) is static; the relative "X ago" trigger
  // ticks every second via the shared <TimeAgo> so table cells stop looking
  // frozen between query refetches. Only the inner text node re-renders.
  const { full, abbr } = useDateFormat()
  if (!iso) return <>{emptyFallback ?? <span className="text-muted-foreground/40">—</span>}</>
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className={
              className ??
              'text-muted-foreground whitespace-nowrap border-b border-dotted border-muted-foreground/30'
            }
          />
        }
      >
        <TimeAgo timestamp={iso} />
      </TooltipTrigger>
      <TooltipContent className="text-xs">
        {full(iso)} {abbr()}
      </TooltipContent>
    </Tooltip>
  )
}
