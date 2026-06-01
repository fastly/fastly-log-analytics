import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'

interface SkeletonGridProps {
  count: number
  height?: string
  className?: string
}

export function SkeletonGrid({ count, height = '120px', className }: SkeletonGridProps) {
  return (
    <>
      {Array.from({ length: count }, (_, i) => (
        <Skeleton key={i} className={cn('w-full rounded-xl', className)} style={{ height }} />
      ))}
    </>
  )
}
