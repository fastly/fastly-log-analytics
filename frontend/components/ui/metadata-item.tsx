import { cn } from '@/lib/utils'

interface MetadataItemProps {
  label: string
  children: React.ReactNode
  className?: string
}

export function MetadataItem({ label, children, className }: MetadataItemProps) {
  return (
    <div className={cn('flex flex-col gap-0.5', className)}>
      <span className="text-[10px] uppercase font-bold text-muted-foreground">{label}</span>
      <div className="text-xs">{children}</div>
    </div>
  )
}
