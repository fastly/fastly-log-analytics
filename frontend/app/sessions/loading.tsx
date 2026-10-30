import { TableSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <TableSkeleton
      title="User Sessions"
      description="Track IP addresses and JA4 fingerprints generating high request volumes or errors."
    />
  )
}
