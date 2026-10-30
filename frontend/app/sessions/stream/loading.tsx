import { TableSkeleton } from '@/components/ui/skeleton'

export default function Loading() {
  return (
    <TableSkeleton
      title="Stream Session Details"
      description="CMCD streaming quality metrics and timeline for this session."
    />
  )
}
