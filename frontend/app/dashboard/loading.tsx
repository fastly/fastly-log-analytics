import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// title/description mirror the ReportLayout props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Dashboard"
      description="Drill down into traffic details and analyze request trends."
    />
  )
}
