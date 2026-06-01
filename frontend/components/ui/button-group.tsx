import { cn } from '@/lib/utils'

interface ButtonGroupProps {
  children: React.ReactNode
  className?: string
}

export function ButtonGroup({ children, className }: ButtonGroupProps) {
  return (
    <div className={cn('flex items-center flex-wrap gap-1 bg-muted/40 p-0.5 rounded-md', className)}>
      {children}
    </div>
  )
}
