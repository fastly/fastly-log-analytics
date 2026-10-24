import { cn } from "@/lib/utils"

function Skeleton({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="skeleton"
      className={cn("animate-pulse rounded-md bg-muted", className)}
      {...props}
    />
  )
}

interface SkeletonGridProps {
  count: number
  height?: string
  className?: string
}

function SkeletonGrid({ count, height = '120px', className }: SkeletonGridProps) {
  return (
    <>
      {Array.from({ length: count }, (_, i) => (
        <Skeleton key={i} className={cn('w-full rounded-xl', className)} style={{ height }} />
      ))}
    </>
  )
}

export { Skeleton, SkeletonGrid }
