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
  onRetry: () => void
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
      <Button size="sm" variant="outline" className={isStacked ? undefined : 'mt-3'} onClick={onRetry}>
        Retry
      </Button>
    </div>
  )
}
