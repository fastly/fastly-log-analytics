import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

export default function Loading() {
  return (
    <DashboardSkeleton
      title="Live Log Streaming"
      description="Real-time CDN edge log tail via Server-Sent Events — interactive pause/resume, regex search, and deep JSON inspection."
    />
  )
}
