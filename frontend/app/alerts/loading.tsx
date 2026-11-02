import { TableSkeleton } from '@/components/ui/skeleton'

export default function Loading() {
  return (
    <div className="min-h-[820px] [contain-intrinsic-size:auto_820px]">
      <TableSkeleton />
    </div>
  )
}
