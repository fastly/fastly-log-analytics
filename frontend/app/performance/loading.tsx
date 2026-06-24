import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// title/description mirror the ReportLayout props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Performance"
      description="Analyze latency, cache efficiency, and origin vs edge processing time."
    />
  )
}
