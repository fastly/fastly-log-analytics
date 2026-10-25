import { Button } from '@/components/ui/button'

export function CardErrorState({
  icon,
  title,
  message,
  onRetry,
  variant = 'default',
}: {
  icon?: React.ReactNode
  title: string
  message: string
  onRetry?: () => void
  // 'default' lays the rows out in block flow (mt-1/mt-3 margins); 'stacked'
  // uses a flex column with gap-3 + items-start. Same content, two spacings —
  // ScoreDistChart needs the stacked look the rest of the cards don't.
  variant?: 'default' | 'stacked'
}) {
  const isStacked = variant === 'stacked'
  return (
    <div
      className={
        isStacked
          ? 'flex flex-col items-start gap-3 p-4 border border-destructive/20 bg-destructive/5 rounded-md'
          : 'border border-destructive/20 bg-destructive/5 rounded-md p-4'
      }
    >
      <div className="flex items-center gap-2 text-destructive">
        {icon}
        <span className="text-sm font-medium">{title}</span>
      </div>
      <p className={isStacked ? 'text-xs text-muted-foreground' : 'text-xs text-muted-foreground mt-1'}>{message}</p>
      {onRetry && (
        <Button size="sm" variant="outline" className={isStacked ? undefined : 'mt-3'} onClick={onRetry}>
          Retry
        </Button>
      )}
    </div>
  )
}

/** Shared chart-inner empty state (promoted from the security page's
 *  `_sections/` in the 2026-07-06 DRY pass so other chart bodies can adopt
 *  it). The wrapping `data.length === 0 && !isLoading` guard stays at each
 *  call site; this only owns the inner markup.
 *
 *  `requires` is OPTIONAL: pass it only when the underlying field group is
 *  genuinely NOT enabled (callers decide via `useActiveLogFields`). When the
 *  group IS enabled but there's simply no data in the window yet, omit it and
 *  this renders a neutral "no data" state instead of a misleading
 *  "Requires Group X to be enabled" message. */
export function ChartEmptyState({ requires }: { requires?: string }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
      <span className="text-sm font-medium mb-1">
        {requires ? 'No data available' : 'No data in this time range yet'}
      </span>
      {requires && <span className="text-[10px] opacity-70">Requires {requires}</span>}
    </div>
  )
}
