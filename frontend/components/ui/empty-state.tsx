import { cn } from '@/lib/utils'

interface EmptyStateProps {
  message?: string
  className?: string
}

export function EmptyState({ message = 'No data available', className }: EmptyStateProps) {
  return (
    <div className={cn('flex items-center justify-center h-full text-muted-foreground text-sm py-8', className)}>
      {message}
    </div>
  )
}
