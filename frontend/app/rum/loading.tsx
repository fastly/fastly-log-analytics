import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

export default function Loading() {
  return (
    <DashboardSkeleton
      title="Real User Monitoring"
      description="Monitor real user performance metrics including Core Web Vitals, JavaScript errors, and session analytics."
    />
  )
}
