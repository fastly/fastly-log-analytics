import { TableSkeleton } from '@/components/skeletons/PageSkeleton'

// title/description mirror the ReportLayout props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <TableSkeleton
      title="User Sessions"
      description="Track IP addresses and JA4 fingerprints generating high request volumes or errors."
    />
  )
}
