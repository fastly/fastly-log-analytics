import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// title/description mirror the ReportLayout props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
    />
  )
}
