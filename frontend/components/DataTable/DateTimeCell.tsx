import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'

interface DateTimeCellProps {
  iso: string | null | undefined
  className?: string
  emptyFallback?: React.ReactNode
}

export function DateTimeCell({ iso, className, emptyFallback }: DateTimeCellProps) {
  const { timeAgo, full, abbr } = useDateFormat()
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
        {timeAgo(iso)}
      </TooltipTrigger>
      <TooltipContent className="text-xs">
        {full(iso)} {abbr()}
      </TooltipContent>
    </Tooltip>
  )
}
